// Astinus session ledger (plan v3, §6). One SQLite file per router-served session key
// holding turn refs and — written by the sidecar pass — the model's topic mapping, marks
// and supersessions. The plugin writes only `turns` and `marks.applied_*`.
//
// Failure policy (R1/R2): the ledger can NEVER break a turn. Every entry point returns a
// result and never throws; a failure logs at warn and the caller continues.
import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { mkdirSync } from "fs";
import { join } from "path";
import {
  CHECKPOINT_SCHEMA_V2_DDL,
  CHECKPOINT_SCHEMA_VERSION,
  readCheckpointEntries,
  readContextState,
  type ContextStateRow,
} from "./checkpoint-store.js";
import { planRebase, type RebaseMark } from "./mark-rebase.js";

// v2 adds the durable checkpoint records (context_state / context_checkpoints /
// checkpoint_entries / ledger_bindings). Fresh ledgers are created at v2; an
// existing v1 ledger keeps working unchanged (no hot-path auto-migration) until
// the explicit v1 -> v2 migration step (checkpoint-store.migrateLedgerV1ToV2)
// runs. A future version is rejected.
export const LEDGER_SCHEMA_VERSION = CHECKPOINT_SCHEMA_VERSION;
export const LEDGER_SCHEMA_VERSION_V1 = 1;
const PLUGIN_BUSY_TIMEOUT_MS = 250;
const MAX_CACHED_LEDGERS = 200;

let ledgerRoot = "";

/** Called once from the plugin entry with the same workspace root the state document uses. */
export function configureSessionLedger(workspaceRoot: string): void {
  ledgerRoot = workspaceRoot;
}

/** Keys the router would not serve get no ledger file and no pass (R3). */
export function ledgerEligible(sessionKey: string | undefined | null): boolean {
  if (!sessionKey) return false;
  if (sessionKey.includes(":subagent:")) return false;
  if (sessionKey.includes(":heartbeat")) return false;
  if (sessionKey.includes(":cron")) return false;
  return true;
}

export function ledgerPath(sessionKey: string): string {
  const safe = String(sessionKey).replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 120);
  return join(ledgerRoot, "workspace", "state", "astinus", `${safe}.sqlite`);
}

/**
 * Context-only ledgers live in the same state directory under a HASH of the full
 * scoped key (design section 3), never a sanitized/truncated form. A router-served
 * ledger's sanitized name can collide on a long key; a hash of the full key cannot,
 * so the two namespaces can never merge. Readable and stable across runs.
 */
export function contextOnlyLedgerPath(sessionKey: string): string {
  const hash = createHash("sha256").update(String(sessionKey)).digest("hex").slice(0, 32);
  return join(ledgerRoot, "workspace", "state", "astinus", `ctx-${hash}.sqlite`);
}

// A context-only ledger carries the v2 context records and a meta row tagged
// 'context-only'. It deliberately has NO topics/passes/marks/turns: topic passes
// and retain are disabled for excluded keys, so only durable context/bindings live
// here (design section 3: "disable topic passes and retain there").
const CONTEXT_ONLY_SCHEMA = `
CREATE TABLE IF NOT EXISTS meta (
  session_key TEXT NOT NULL, agent_id TEXT NOT NULL, host_db_path TEXT NOT NULL,
  schema_version INTEGER NOT NULL, created_at INTEGER NOT NULL,
  ledger_kind TEXT NOT NULL DEFAULT 'context-only'
);
`;

// One row per committed prune (2026-11-03). An earlier epoch's drop removes tokens without any
// mark row being stamped, so the report reads the prune's own before/after from here (Astinus #6340).
const PRUNES_DDL = `CREATE TABLE IF NOT EXISTS prunes (
  epoch INTEGER PRIMARY KEY, at INTEGER NOT NULL,
  tokens_before INTEGER, tokens_after INTEGER,
  admitted INTEGER NOT NULL, dropped INTEGER NOT NULL, fallback_mark_id INTEGER
);`;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS meta (
  session_key TEXT NOT NULL, agent_id TEXT NOT NULL, host_db_path TEXT NOT NULL,
  schema_version INTEGER NOT NULL, created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS turns (
  turn_id      INTEGER PRIMARY KEY,
  session_id   TEXT NOT NULL,
  seq_start    INTEGER NOT NULL, seq_end INTEGER NOT NULL,
  gen          TEXT NOT NULL,
  advancement_key TEXT NOT NULL UNIQUE,
  heartbeat    INTEGER NOT NULL DEFAULT 0,
  committed_at INTEGER NOT NULL,
  classified_at INTEGER
);
CREATE TABLE IF NOT EXISTS topics (
  topic_id     INTEGER PRIMARY KEY,
  slug         TEXT NOT NULL, label TEXT NOT NULL,
  first_seen_seq INTEGER NOT NULL, last_seen_seq INTEGER NOT NULL, last_seen_at INTEGER NOT NULL,
  digest       TEXT,
  terminal_at  INTEGER,
  UNIQUE(slug)
);
CREATE TABLE IF NOT EXISTS turn_topics (
  turn_id INTEGER NOT NULL REFERENCES turns, topic_id INTEGER REFERENCES topics,
  class   TEXT NOT NULL CHECK (class IN ('topic','random')),
  confidence REAL, source TEXT NOT NULL CHECK (source IN ('model','fallback')),
  CHECK ((class = 'random') = (topic_id IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_turn_topics_topic ON turn_topics(turn_id, topic_id) WHERE topic_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_turn_topics_random ON turn_topics(turn_id) WHERE topic_id IS NULL;
CREATE TABLE IF NOT EXISTS marks (
  mark_id     INTEGER PRIMARY KEY,
  session_id  TEXT NOT NULL,
  seq_start   INTEGER NOT NULL, seq_end INTEGER NOT NULL,
  kind        TEXT NOT NULL CHECK (kind IN ('turn','tool_noise','recall_injection','enrich_injection')),
  action      TEXT NOT NULL CHECK (action IN ('digest','drop','placeholder')),
  digest      TEXT,
  reason      TEXT NOT NULL,
  stmt_age_s  INTEGER, topic_dormancy_s INTEGER, pressure REAL,
  proposed_at INTEGER NOT NULL, applied_gen TEXT, applied_at INTEGER,
  view_tokens_before INTEGER, view_tokens_after INTEGER,
  anchor_first_event TEXT, anchor_last_event TEXT,
  retired_at INTEGER
);
CREATE INDEX IF NOT EXISTS ix_marks_seq ON marks(session_id, seq_start);
CREATE TABLE IF NOT EXISTS supersessions (
  supersession_id INTEGER PRIMARY KEY,
  entity TEXT NOT NULL, attribute TEXT NOT NULL,
  old_turn_id INTEGER NOT NULL REFERENCES turns, new_turn_id INTEGER NOT NULL REFERENCES turns,
  old_value TEXT, new_value TEXT, seen_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS passes (
  pass_id INTEGER PRIMARY KEY, turn_id INTEGER REFERENCES turns,
  model TEXT, effort TEXT, elapsed_ms INTEGER, reasoning_tokens INTEGER,
  marks_proposed INTEGER, outcome TEXT NOT NULL,
  fallback_from TEXT, fallback_reason TEXT,
  started_at INTEGER NOT NULL, finished_at INTEGER,
  lag_turns INTEGER
);
CREATE TABLE IF NOT EXISTS rebases (
  rebase_id INTEGER PRIMARY KEY, session_id TEXT NOT NULL,
  old_gen TEXT, new_gen TEXT, at INTEGER NOT NULL, note TEXT
);
${PRUNES_DDL}
`;

const cache = new Map<string, DatabaseSync>();
// Caps-gap review item 6 (Claude, 2026-09-20): last-touch per cached ledger so an idle sweep can
// release handles - nothing else in production ever closes one, and archiving needs that.
const lastUsedAt = new Map<string, number>();
const IDLE_SWEEP_INTERVAL_MS = 60 * 60 * 1000;
const IDLE_LEDGER_MS = 2 * 60 * 60 * 1000;
let lastIdleSweepAt = 0;
// F10: a schema mismatch deserves to be sticky; a transient open failure (a momentary lock, a
// full disk) must not disable a session's ledger for the process lifetime, or nothing ever
// re-tries it. Transient entries carry a cooldown.
const disabled = new Map<string, { reason: string; until: number }>();
const OPEN_FAILED_COOLDOWN_MS = 5 * 60 * 1000;

export type LedgerOpen =
  | { ok: true; db: DatabaseSync }
  | { ok: false; reason: string };

/** Open (or create) a session's ledger. Cached; never throws. */
export function openSessionLedger(
  sessionKey: string,
  agentId: string,
  hostDbPath: string,
  warn: (msg: string) => void
): LedgerOpen {
  if (!ledgerEligible(sessionKey)) return { ok: false, reason: "ineligible-key" };
  const known = disabled.get(sessionKey);
  if (known) {
    if (known.until > Date.now()) return { ok: false, reason: known.reason };
    disabled.delete(sessionKey); // cooldown expired - try again
  }
  const cached = cache.get(sessionKey);
  if (cached) {
    lastUsedAt.set(sessionKey, Date.now());
    return { ok: true, db: cached };
  }
  if (!ledgerRoot) return { ok: false, reason: "not-configured" };
  const path = ledgerPath(sessionKey);
  let db: DatabaseSync;
  try {
    mkdirSync(join(ledgerRoot, "workspace", "state", "astinus"), { recursive: true });
    db = new DatabaseSync(path);
    db.exec(`PRAGMA journal_mode = WAL;`);
    db.exec(`PRAGMA busy_timeout = ${PLUGIN_BUSY_TIMEOUT_MS};`);
    // Schema version gate: a mismatch disables the ledger for this session rather than
    // migrating in the hot path (R2).
    const row = db.prepare("PRAGMA user_version").get() as { user_version?: number } | undefined;
    const v = Number(row?.user_version ?? 0);
    if (v === 0) {
      db.exec(SCHEMA);
      db.exec(CHECKPOINT_SCHEMA_V2_DDL);
      db.exec(`PRAGMA user_version = ${LEDGER_SCHEMA_VERSION};`);
      db.prepare(
        "INSERT INTO meta(session_key, agent_id, host_db_path, schema_version, created_at) VALUES(?,?,?,?,?)"
      ).run(sessionKey, agentId, hostDbPath, LEDGER_SCHEMA_VERSION, Date.now());
    } else if (v !== LEDGER_SCHEMA_VERSION && v !== LEDGER_SCHEMA_VERSION_V1) {
      const reason = `schema-version-mismatch:${v}`;
      db.close();
      disabled.set(sessionKey, { reason, until: Number.POSITIVE_INFINITY });
      warn(`[astinus-ledger] ${sessionKey}: ${reason}; ledger disabled for this session`);
      return { ok: false, reason };
    } else {
      // Never merge two keys into one file (design section 3): the sanitized
      // file name is lossy, so verify the stored key matches before trusting the
      // handle. A mismatch is a defect, not a rename - disable and surface it.
      const stored = db.prepare("SELECT session_key FROM meta LIMIT 1").get() as
        | { session_key?: string }
        | undefined;
      if (stored?.session_key !== undefined && stored.session_key !== sessionKey) {
        const reason = `ledger-key-mismatch:${stored.session_key}`;
        db.close();
        disabled.set(sessionKey, { reason, until: Number.POSITIVE_INFINITY });
        warn(`[astinus-ledger] ${sessionKey}: ${reason}; ledger disabled for this session`);
        return { ok: false, reason };
      }
    }
    if (cache.size >= MAX_CACHED_LEDGERS) {
      const oldest = cache.keys().next().value;
      if (oldest !== undefined) {
        try {
          cache.get(oldest)?.close();
        } catch {
          /* ignore */
        }
        cache.delete(oldest);
        lastUsedAt.delete(oldest);
      }
    }
    cache.set(sessionKey, db);
    lastUsedAt.set(sessionKey, Date.now());
    // Anchor columns (2026-11-02): a mark resolves by the host event id at its two
    // boundaries, because seq coordinates move on a trim or a header repair. Added in
    // place so an existing ledger keeps its rows; a failure leaves the ledger usable.
    try {
      const markCols = db.prepare("PRAGMA table_info(marks)").all() as Array<{ name?: string }>;
      const names = new Set(markCols.map((c) => c.name));
      if (!names.has("anchor_first_event")) {
        db.exec("ALTER TABLE marks ADD COLUMN anchor_first_event TEXT");
      }
      if (!names.has("anchor_last_event")) {
        db.exec("ALTER TABLE marks ADD COLUMN anchor_last_event TEXT");
      }
      if (!names.has("retired_at")) {
        db.exec("ALTER TABLE marks ADD COLUMN retired_at INTEGER");
      }
      // Prune cycle (2026-11-03): the epoch a mark was admitted in, and the session's current one.
      // NULL reads as 0, so marks applied before the cycle form epoch 0.
      if (!names.has("prune_epoch")) {
        db.exec("ALTER TABLE marks ADD COLUMN prune_epoch INTEGER");
      }
      const metaCols = db.prepare("PRAGMA table_info(meta)").all() as Array<{ name?: string }>;
      const metaNames = new Set(metaCols.map((c) => c.name));
      if (!metaNames.has("mark_rebase_done")) {
        db.exec("ALTER TABLE meta ADD COLUMN mark_rebase_done INTEGER");
      }
      if (!metaNames.has("prune_epoch")) {
        db.exec("ALTER TABLE meta ADD COLUMN prune_epoch INTEGER");
      }
      db.exec(PRUNES_DDL);
    } catch {
      /* a locked or read-only ledger keeps working without anchors */
    }
    // Item 6: hourly idle-close sweep, piggybacked on the per-turn open path.
    if (Date.now() - lastIdleSweepAt > IDLE_SWEEP_INTERVAL_MS) {
      lastIdleSweepAt = Date.now();
      sweepIdleLedgers();
    }
    return { ok: true, db };
  } catch (e) {
    const reason = `open-failed:${(e as Error)?.message ?? e}`;
    disabled.set(sessionKey, { reason, until: Date.now() + OPEN_FAILED_COOLDOWN_MS });
    warn(`[astinus-ledger] ${sessionKey}: ${reason}`);
    return { ok: false, reason };
  }
}

/**
 * Open (or create) a CONTEXT-ONLY ledger for a key the router excludes
 * (heartbeat / cron / subagent). Ownership is engine-wide (design section 3):
 * excluded keys still need durable context checkpoints/bindings so owner-mode
 * assembly and compaction cover every key the engine slot selects, but they run
 * no topic pass and retain. The file name is a hash of the full key
 * (contextOnlyLedgerPath) so it can never collide with, or merge into, a
 * router-served ledger. Cached like a served ledger; never throws.
 */
export function openContextOnlyLedger(
  sessionKey: string,
  agentId: string,
  hostDbPath: string,
  warn: (msg: string) => void
): LedgerOpen {
  if (!sessionKey) return { ok: false, reason: "missing-key" };
  const known = disabled.get(sessionKey);
  if (known) {
    if (known.until > Date.now()) return { ok: false, reason: known.reason };
    disabled.delete(sessionKey); // cooldown expired - try again
  }
  const cached = cache.get(sessionKey);
  if (cached) {
    lastUsedAt.set(sessionKey, Date.now());
    return { ok: true, db: cached };
  }
  if (!ledgerRoot) return { ok: false, reason: "not-configured" };
  const path = contextOnlyLedgerPath(sessionKey);
  let db: DatabaseSync;
  try {
    mkdirSync(join(ledgerRoot, "workspace", "state", "astinus"), { recursive: true });
    db = new DatabaseSync(path);
    db.exec("PRAGMA journal_mode = WAL;");
    db.exec(`PRAGMA busy_timeout = ${PLUGIN_BUSY_TIMEOUT_MS};`);
    const row = db.prepare("PRAGMA user_version").get() as { user_version?: number } | undefined;
    const v = Number(row?.user_version ?? 0);
    if (v === 0) {
      db.exec(CONTEXT_ONLY_SCHEMA);
      db.exec(CHECKPOINT_SCHEMA_V2_DDL);
      db.exec(`PRAGMA user_version = ${LEDGER_SCHEMA_VERSION};`);
      db.prepare(
        "INSERT INTO meta(session_key, agent_id, host_db_path, schema_version, created_at, ledger_kind) " +
          "VALUES(?,?,?,?,?,'context-only')"
      ).run(sessionKey, agentId, hostDbPath, LEDGER_SCHEMA_VERSION, Date.now());
      db.prepare(
        "INSERT OR IGNORE INTO context_state " +
          "(session_key, host_target, host_revision, read_cursor, view_epoch, ledger_revision, " +
          "active_checkpoint_id, status, last_rebuild_reason, updated_at) " +
          "VALUES (?, NULL, NULL, NULL, '0', 0, NULL, 'ready', 'context-only-ledger', ?)"
      ).run(sessionKey, Date.now());
    } else if (v !== LEDGER_SCHEMA_VERSION) {
      const reason = `schema-version-mismatch:${v}`;
      db.close();
      disabled.set(sessionKey, { reason, until: Number.POSITIVE_INFINITY });
      warn(`[astinus-context-ledger] ${sessionKey}: ${reason}; ledger disabled for this session`);
      return { ok: false, reason };
    }
    const stored = db.prepare("SELECT session_key FROM meta LIMIT 1").get() as
      | { session_key?: string }
      | undefined;
    if (stored?.session_key !== undefined && stored.session_key !== sessionKey) {
      const reason = `ledger-key-mismatch:${stored.session_key}`;
      db.close();
      disabled.set(sessionKey, { reason, until: Number.POSITIVE_INFINITY });
      warn(`[astinus-context-ledger] ${sessionKey}: ${reason}; ledger disabled for this session`);
      return { ok: false, reason };
    }
    if (cache.size >= MAX_CACHED_LEDGERS) {
      const oldest = cache.keys().next().value;
      if (oldest !== undefined) {
        try {
          cache.get(oldest)?.close();
        } catch {
          /* ignore */
        }
        cache.delete(oldest);
        lastUsedAt.delete(oldest);
      }
    }
    cache.set(sessionKey, db);
    lastUsedAt.set(sessionKey, Date.now());
    return { ok: true, db };
  } catch (e) {
    const reason = `open-failed:${(e as Error)?.message ?? e}`;
    disabled.set(sessionKey, { reason, until: Date.now() + OPEN_FAILED_COOLDOWN_MS });
    warn(`[astinus-context-ledger] ${sessionKey}: ${reason}`);
    return { ok: false, reason };
  }
}

/**
 * Close one session's cached ledger handle. Needed before the file can be archived or reset on
 * Windows, where an open handle blocks the unlink (see
 * research/ASTINUS-WINDOWS-FILE-LOCKING-2026-09-14.md). Never throws.
 */
export function closeSessionLedger(sessionKey: string): void {
  const db = cache.get(sessionKey);
  if (!db) return;
  try {
    db.close();
  } catch {
    /* ignore */
  }
  cache.delete(sessionKey);
  lastUsedAt.delete(sessionKey);
}

/**
 * Caps-gap review item 6 (Claude, 2026-09-20): close handles for sessions idle past `maxIdleMs`.
 * Without this, archiving or resetting a ledger file requires a gateway restart (the Windows
 * handle blocks the unlink). Best-effort; never throws. Returns the number of handles closed.
 */
export function sweepIdleLedgers(maxIdleMs: number = IDLE_LEDGER_MS): number {
  const cutoff = Date.now() - maxIdleMs;
  let closed = 0;
  for (const key of [...cache.keys()]) {
    const last = lastUsedAt.get(key);
    if (typeof last === "number" && last > cutoff) continue;
    closeSessionLedger(key);
    closed += 1;
  }
  return closed;
}

/**
 * Close every cached ledger handle — shutdown, or a test/archive pass that needs the files
 * free. Never throws.
 */
export function closeAllLedgers(): void {
  for (const key of [...cache.keys()]) {
    const db = cache.get(key);
    try {
      db?.close();
    } catch {
      /* ignore */
    }
    cache.delete(key);
  }
  lastUsedAt.clear();
}

/** Stamp marks as applied (best-effort; never throws). Plan §8: applied_gen/applied_at + before/after. */
export function markMarksApplied(
  sessionKey: string,
  markIds: Array<number | unknown>,
  gen: string,
  tokensBefore: number,
  tokensAfter: number
): void {
  const db = cache.get(sessionKey);
  if (!db || markIds.length === 0) return;
  try {
    const st = db.prepare(
      "UPDATE marks SET applied_gen = ?, applied_at = ?, view_tokens_before = ?, view_tokens_after = ? WHERE mark_id = ?"
    );
    for (const id of markIds) st.run(gen, Date.now(), tokensBefore, tokensAfter, id);
  } catch {
    /* never throws */
  }
}

/** The session's current prune epoch from an already-open ledger; 0 when never pruned or unreadable. */
export function readPruneEpochCached(sessionKey: string): number {
  const db = cache.get(sessionKey);
  if (!db) return 0;
  try {
    const row = db.prepare("SELECT prune_epoch AS e FROM meta LIMIT 1").get() as { e?: number | null } | undefined;
    return typeof row?.e === "number" && row.e > 0 ? row.e : 0;
  } catch {
    return 0;
  }
}

/** Commit one prune in a single transaction: the new epoch, the admitted marks stamped with it,
 *  and the oldest-turns fallback (if any) inserted as an applied mark of that epoch. All or
 *  nothing: a failed commit leaves the old epoch, and the next view recomputes the same prune.
 *  Returns false on failure; never throws. */
export function commitPrune(
  sessionKey: string,
  epoch: number,
  admittedIds: Array<number | unknown>,
  fallback: { session_id?: string; seq_start: number; seq_end: number; kind?: string; action: string; digest?: string | null; reason?: string } | null,
  gen: string,
  tokensBefore: number,
  tokensAfter: number,
  dropped = 0
): boolean {
  const db = cache.get(sessionKey);
  if (!db) return false;
  const now = Date.now();
  try {
    db.exec("BEGIN IMMEDIATE");
    try {
      db.prepare("UPDATE meta SET prune_epoch = ?").run(epoch);
      const st = db.prepare(
        "UPDATE marks SET applied_gen = ?, applied_at = ?, view_tokens_before = ?, view_tokens_after = ?, prune_epoch = ? WHERE mark_id = ?"
      );
      for (const id of admittedIds) st.run(gen, now, tokensBefore, tokensAfter, epoch, id as number);
      let fallbackId: number | null = null;
      if (fallback) {
        const ins = db.prepare(
          `INSERT INTO marks (session_id, seq_start, seq_end, kind, action, digest, reason, proposed_at,
                              applied_gen, applied_at, view_tokens_before, view_tokens_after, prune_epoch)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`
        ).run(
          fallback.session_id ?? "",
          fallback.seq_start,
          fallback.seq_end,
          fallback.kind ?? "turn",
          fallback.action,
          fallback.digest ?? null,
          fallback.reason ?? "prune_fallback",
          now,
          gen,
          now,
          tokensBefore,
          tokensAfter,
          epoch
        );
        fallbackId = Number(ins.lastInsertRowid);
      }
      db.prepare(
        "INSERT INTO prunes (epoch, at, tokens_before, tokens_after, admitted, dropped, fallback_mark_id) VALUES (?,?,?,?,?,?,?)"
      ).run(epoch, now, tokensBefore, tokensAfter, admittedIds.length, dropped, fallbackId);
      db.exec("COMMIT");
      return true;
    } catch {
      db.exec("ROLLBACK");
      return false;
    }
  } catch {
    return false;
  }
}

/** Union of ledger topic slugs over the given seq ranges, plus which ranges had no
 *  turn_topics rows (plan SS9.3: the retain path tags from the LEDGER, not
 *  classifyTurn(prompt)). Read-only from the in-process handle - after a gateway
 *  restart the first drain may read nothing until the session commits again (the
 *  commit path re-opens the handle); that drain falls back to the regex tags.
 *  Any failure reads as "all uncovered", which degrades to today's behaviour. */
export function readTopicsForRanges(
  sessionKey: string,
  ranges: Array<{ start: number; end: number }>
): { slugs: string[]; uncovered: Array<{ start: number; end: number }> } {
  const db = cache.get(sessionKey);
  if (!db || ranges.length === 0) return { slugs: [], uncovered: ranges.slice() };
  try {
    // A turn [seq_start, seq_end] covers a range [start, end] iff they overlap.
    const st = db.prepare(
      // Caps-gap review item 3 (Claude, 2026-09-20): this was the one consumer that never got the
      // source='model' filter - a drained range could tag the bank with the regex buckets.
      `SELECT DISTINCT top.slug FROM turns t
       JOIN turn_topics tt ON tt.turn_id = t.turn_id
       JOIN topics top ON top.topic_id = tt.topic_id
       WHERE t.seq_start <= ? AND t.seq_end >= ?
         AND EXISTS (SELECT 1 FROM turn_topics tt2
                     WHERE tt2.turn_id = t.turn_id AND tt2.topic_id = top.topic_id
                       AND tt2.source = 'model')`
    );
    const slugs = new Set<string>();
    const uncovered: Array<{ start: number; end: number }> = [];
    for (const r of ranges) {
      const rows = st.all(r.end, r.start) as Array<{ slug: string }>;
      if (rows.length === 0) uncovered.push(r);
      for (const row of rows) {
        if (typeof row.slug === "string" && row.slug) slugs.add(row.slug);
      }
    }
    return { slugs: [...slugs], uncovered };
  } catch {
    return { slugs: [], uncovered: ranges.slice() };
  }
}

/** R6: write the regex classification back for turns the pass has not reached,
 *  so the coverage gap is visible in Gate L's query (source='fallback'), never
 *  silent. Only touches turns that have NO turn_topics rows yet; INSERT OR
 *  IGNORE keeps it safe against the pass racing from its own process. The slug
 *  regex mirrors topicTag() in index.ts and the pass's Python slugify - keep the
 *  three in agreement or one topic splits into two rows. */
export function writeFallbackTopics(
  sessionKey: string,
  ranges: Array<{ start: number; end: number }>,
  labels: string[]
): number {
  let written = 0;
  if (labels.length === 0) return written;
  const db = cache.get(sessionKey);
  if (!db) return written;
  try {
    const findTurns = db.prepare(
      `SELECT turn_id, seq_start, seq_end, committed_at FROM turns
       WHERE seq_start <= ? AND seq_end >= ?`
    );
    const passRunning = db.prepare(
      `SELECT 1 AS x FROM passes WHERE turn_id = ? AND finished_at IS NULL LIMIT 1`
    );
    const hasRow = db.prepare(`SELECT 1 AS x FROM turn_topics WHERE turn_id = ? LIMIT 1`);
    const ensureTopic = db.prepare(
      `INSERT INTO topics (slug, label, first_seen_seq, last_seen_seq, last_seen_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(slug) DO UPDATE SET
         last_seen_seq = MAX(last_seen_seq, excluded.last_seen_seq),
         last_seen_at = excluded.last_seen_at`
    );
    const topicIdOf = db.prepare(`SELECT topic_id FROM topics WHERE slug = ?`);
    const insertTT = db.prepare(
      `INSERT OR IGNORE INTO turn_topics (turn_id, topic_id, class, confidence, source)
       VALUES (?, ?, 'topic', NULL, 'fallback')`
    );
    const now = Date.now();
    for (const label of labels) {
      const slug = String(label)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "");
      if (!slug) continue;
      for (const r of ranges) {
        const turns = findTurns.all(r.end, r.start) as Array<{
          turn_id: number;
          seq_start: number;
          seq_end: number;
          committed_at: number;
        }>;
        for (const t of turns) {
          if (hasRow.get(t.turn_id)) continue; // the pass already classified this turn
          // B1.ii (review 2026-09-17): never race the in-flight pass. A turn committed
          // under 180 s ago has not had its chance (the pass takes 3-78 s); a turn with
          // a running passes row is mid-classification. Stamping either would make
          // regex rows the steady state on every drain.
          if (Number(t.committed_at) > now - 180000) continue;
          if (passRunning.get(t.turn_id)) continue;
          ensureTopic.run(slug, String(label), Number(t.seq_start), Number(t.seq_end), now);
          const id = topicIdOf.get(slug) as { topic_id: number } | undefined;
          if (!id) continue;
          written += Number(insertTT.run(t.turn_id, id.topic_id).changes);
        }
      }
    }
    return written;
  } catch {
    return written;
  }
}

/** The session's LIVE topics (plan SS9.1): newest first by last_seen_seq, terminal
 *  topics excluded. Read-only from the in-process handle; empty when no ledger
 *  or no topics yet (young session, cold handle after a restart) - callers fall
 *  back to the regex classifier. */
/** Edit 4 (Vern, order 2026-09-23): the newest LIVE topic's label and digest - the
 *  desk's own classification of what this session is about, the content-relevance source
 *  for the recall query. Same liveness and classification constraints as readLiveTopics;
 *  null on a cold handle or when nothing is classified yet (the raw extraction is the
 *  fallback there). */
export function readNewestLiveTopic(
  sessionKey: string
): { slug: string; label: string; digest: string } | null {
  const db = cache.get(sessionKey);
  if (!db) return null;
  try {
    const row = db
      .prepare(
        `SELECT slug, label, digest FROM topics
         WHERE terminal_at IS NULL
           AND EXISTS (SELECT 1 FROM turn_topics tt
                       WHERE tt.topic_id = topics.topic_id AND tt.source = 'model')
         ORDER BY last_seen_seq DESC LIMIT 1`
      )
      .get() as { slug?: string; label?: string; digest?: string } | undefined;
    if (!row || typeof row.slug !== "string" || !row.slug) return null;
    return {
      slug: row.slug,
      label: typeof row.label === "string" ? row.label : "",
      digest: typeof row.digest === "string" ? row.digest : "",
    };
  } catch {
    return null;
  }
}

export function readLiveTopics(sessionKey: string, limit: number): string[] {
  const db = cache.get(sessionKey);
  if (!db || limit <= 0) return [];
  try {
    const rows = db
      .prepare(
        `SELECT slug FROM topics
         WHERE terminal_at IS NULL
           AND EXISTS (SELECT 1 FROM turn_topics tt
                       WHERE tt.topic_id = topics.topic_id AND tt.source = 'model')
         ORDER BY last_seen_seq DESC LIMIT ?`
      )
      .all(Math.floor(limit)) as Array<{ slug: string }>;
    return rows
      .map((r) => (typeof r.slug === "string" ? r.slug : ""))
      .filter((s) => s.length > 0);
  } catch {
    return [];
  }
}

/** Plan SS9.2 delta rule: the newest CLASSIFIED turn's topics vs the k turns
 *  before it. prevTop = the immediately previous classified turn's top topic
 *  (highest last_seen_seq first within each turn). null = no ledger or no
 *  classified turns yet - the caller fires as before (young session / cold
 *  handle; the regex classification still governs the query there). */
export function readTopicDelta(
  sessionKey: string,
  k: number
): { newest: string[]; prevTop: string | null; recent: string[] } | null {
  const db = cache.get(sessionKey);
  if (!db || k < 1) return null;
  try {
    const turnRows = db
      .prepare(
        `SELECT turn_id FROM turns
         WHERE turn_id IN (SELECT DISTINCT turn_id FROM turn_topics WHERE source = 'model')
         ORDER BY turn_id DESC LIMIT ?`
      )
      .all(k + 1) as Array<{ turn_id: number }>;
    if (turnRows.length === 0) return null;
    const slugsOf = db.prepare(
      `SELECT top.slug FROM turn_topics tt
       JOIN topics top ON top.topic_id = tt.topic_id
       WHERE tt.turn_id = ? AND tt.source = 'model'
       ORDER BY top.last_seen_seq DESC, tt.confidence DESC, top.slug`
    );
    const slugList = (turnId: number): string[] =>
      (slugsOf.all(turnId) as Array<{ slug: string }>)
        .map((r) => (typeof r.slug === "string" ? r.slug : ""))
        .filter((s) => s.length > 0);
    const newest = slugList(Number(turnRows[0].turn_id));
    const prevTop = turnRows.length > 1 ? slugList(Number(turnRows[1].turn_id))[0] ?? null : null;
    const recentSet = new Set<string>();
    for (let i = 1; i < turnRows.length; i++) {
      for (const s of slugList(Number(turnRows[i].turn_id))) recentSet.add(s);
    }
    return { newest, prevTop, recent: [...recentSet] };
  } catch {
    return null;
  }
}

/** Current-state subject/object roster for the session's LIVE topics (ruling
 *  2026-09-17, corrected same day: the axis is subject/OBJECT - "predicate" was
 *  shorthand for the object slot). Instances ride WITH their topic instead of
 *  fragmenting it. The table is pass-owned and may not exist yet on an older
 *  ledger - a missing table reads as empty. */
export type TopicSubject = { topic: string; subject: string; object: string };
export function readTopicSubjects(sessionKey: string, maxRows: number): TopicSubject[] {
  const db = cache.get(sessionKey);
  if (!db || maxRows <= 0) return [];
  try {
    const rows = db
      .prepare(
        `SELECT t.slug AS topic, s.subject AS subject, s.object AS object
         FROM topic_subjects s JOIN topics t ON t.topic_id = s.topic_id
         WHERE t.terminal_at IS NULL
         ORDER BY t.last_seen_seq DESC, s.updated_at DESC LIMIT ?`
      )
      .all(Math.floor(maxRows)) as Array<{ topic: string; subject: string; object: string }>;
    return rows
      .filter((r) => r.subject && typeof r.object === "string")
      .map((r) => ({ topic: String(r.topic), subject: r.subject, object: r.object }));
  } catch {
    return [];
  }
}

/** Subjects of NEW topics that are thin of content (ruling 2026-09-17): live topics
 *  with fewer than `thinTurnMax` classified turns. Their subjects may carry history in
 *  the shared bank from other desks - the recall thin-subject probe searches those
 *  directly. Empty when no ledger / table not yet created. */
export function readThinTopicSubjects(
  sessionKey: string,
  thinTurnMax: number,
  maxRows: number
): Array<{ topic: string; subject: string; object: string }> {
  const db = cache.get(sessionKey);
  if (!db || thinTurnMax < 1 || maxRows <= 0) return [];
  try {
    const rows = db
      .prepare(
        `SELECT t.slug AS topic, s.subject AS subject, s.object AS object
         FROM topics t
         JOIN topic_subjects s ON s.topic_id = t.topic_id
         WHERE t.terminal_at IS NULL
           AND (SELECT COUNT(*) FROM turn_topics tt
                WHERE tt.topic_id = t.topic_id AND tt.source = 'model') < ?
           AND EXISTS (
             SELECT 1 FROM turn_topics tt3
             WHERE tt3.topic_id = t.topic_id AND tt3.source = 'model'
               AND tt3.turn_id >= (
                 SELECT MIN(turn_id) FROM (
                   SELECT turn_id FROM turn_topics WHERE source = 'model'
                   GROUP BY turn_id ORDER BY turn_id DESC LIMIT 20
                 )
               )
           )
         ORDER BY t.last_seen_seq DESC, s.updated_at DESC LIMIT ?`
      )
      .all(thinTurnMax, maxRows) as Array<{
      topic: string;
      subject: string;
      object: string;
    }>;
    return rows
      .filter((r) => r.subject && typeof r.object === "string")
      .map((r) => ({ topic: String(r.topic), subject: r.subject, object: r.object }));
  } catch {
    return [];
  }
}

export type TurnInsert = {
  sessionKey: string;
  agentId: string;
  hostDbPath: string;
  sessionId: string;
  seqStart: number;
  seqEnd: number;
  generation: string;
  advancementKey: string;
  heartbeat: boolean;
};

/**
 * Best-effort `INSERT OR IGNORE` of one committed turn. Idempotent on `advancement_key`,
 * so a host retry of the same turn is a no-op. Never throws (R1).
 */
export function insertCommittedTurn(
  row: TurnInsert,
  warn: (msg: string) => void
): { ok: boolean; reason?: string } {
  const opened = openSessionLedger(row.sessionKey, row.agentId, row.hostDbPath, warn);
  if (!opened.ok) return { ok: false, reason: opened.reason };
  try {
    opened.db
      .prepare(
        `INSERT OR IGNORE INTO turns
           (session_id, seq_start, seq_end, gen, advancement_key, heartbeat, committed_at)
         VALUES (?,?,?,?,?,?,?)`
      )
      .run(
        row.sessionId,
        row.seqStart,
        row.seqEnd,
        row.generation,
        row.advancementKey,
        row.heartbeat ? 1 : 0,
        Date.now()
      );
    return { ok: true };
  } catch (e) {
    const reason = `insert-failed:${(e as Error)?.message ?? e}`;
    warn(`[astinus-ledger] ${row.sessionKey}: ${reason}`);
    return { ok: false, reason };
  }
}

/**
 * One-time mark rebase (2026-11-02).
 *
 * Marks admitted before anchors existed carry only numbers, and those numbers may
 * already point at different content. Run once per session: backfill anchors for
 * marks applied under the current generation, retire the ones applied under an
 * older generation (their transcript may have been rewritten in place or
 * reindexed, and the two cannot be told apart), and delete pending marks that
 * never got anchors so the pass proposes them again. One transaction, three
 * logged counts, and a meta flag so it never repeats.
 */
export function runMarkRebase(
  sessionKey: string,
  agentId: string,
  hostDbPath: string,
  currentGen: string,
  warn: (msg: string) => void
): { backfill: number; retire: number; remove: number } | null {
  const opened = openSessionLedger(sessionKey, agentId, hostDbPath, warn);
  if (!opened.ok) return null;
  const db = opened.db;
  try {
    const done = db
      .prepare("SELECT mark_rebase_done FROM meta LIMIT 1")
      .get() as { mark_rebase_done?: number | null } | undefined;
    if (done?.mark_rebase_done != null) return null;
    const marks = db
      .prepare(
        `SELECT mark_id, seq_start, seq_end, applied_gen, applied_at,
                anchor_first_event, anchor_last_event, session_id FROM marks`
      )
      .all() as unknown as Array<RebaseMark & { session_id: string }>;
    const sessionId = marks[0]?.session_id ?? "";
    // A2 (Codex 11-03): a saved prior observation is not proof of the CURRENT host
    // generation. Read the rewrite watermark for this session and treat anything we
    // cannot prove as unbackfillable, so old coordinates are retired, never blessed.
    let hostGen: string | null = null;
    try {
      const host = new DatabaseSync(hostDbPath, { readOnly: true });
      try {
        const row = host
          .prepare(
            "SELECT generation FROM transcript_rewrite_watermarks WHERE session_id = ?"
          )
          .get(sessionId) as { generation?: string | null } | undefined;
        hostGen = typeof row?.generation === "string" && row.generation ? row.generation : null;
      } finally {
        host.close();
      }
    } catch {
      hostGen = null;
    }
    const plan = planRebase(marks, hostGen ?? "\u0000unknown");
    let hostSeqs: Map<number, string> | null = null;
    const seqToEvent = (seq: number): string | null => {
      if (!hostSeqs) {
        hostSeqs = new Map();
        try {
          const host = new DatabaseSync(hostDbPath, { readOnly: true });
          try {
            const rows = host
              .prepare(
                "SELECT seq, event_id FROM transcript_event_identities WHERE session_id = ?"
              )
              .all(sessionId) as Array<{ seq: number; event_id: string }>;
            for (const row of rows) hostSeqs.set(Number(row.seq), String(row.event_id));
          } finally {
            host.close();
          }
        } catch {
          /* no identity table: nothing to backfill from */
        }
      }
      return hostSeqs.get(seq) ?? null;
    };
    db.exec("BEGIN IMMEDIATE");
    try {
      let backfilled = 0;
      for (const markId of plan.backfill) {
        const mark = marks.find((m) => m.mark_id === markId);
        if (!mark) continue;
        const first = seqToEvent(mark.seq_start);
        const last = seqToEvent(mark.seq_end);
        if (!first || !last) continue;
        db.prepare(
          "UPDATE marks SET anchor_first_event = ?, anchor_last_event = ? WHERE mark_id = ?"
        ).run(first, last, markId);
        backfilled += 1;
      }
      const retired = db
        .prepare("UPDATE marks SET retired_at = ? WHERE mark_id = ?");
      for (const markId of plan.retire) retired.run(Date.now(), markId);
      const remove = db.prepare("DELETE FROM marks WHERE mark_id = ?");
      for (const markId of plan.remove) remove.run(markId);
      db.prepare("UPDATE meta SET mark_rebase_done = ?").run(Date.now());
      db.exec("COMMIT");
      const counts = {
        backfill: backfilled,
        retire: plan.retire.length,
        remove: plan.remove.length,
      };
      warn(
        `[astinus-ledger] ${sessionKey}: mark rebase backfill=${counts.backfill} ` +
          `retire=${counts.retire} remove=${counts.remove}`
      );
      return counts;
    } catch (e) {
      try {
        db.exec("ROLLBACK");
      } catch {
        /* nothing to roll back */
      }
      throw e;
    }
  } catch (e) {
    warn(`[astinus-ledger] ${sessionKey}: mark rebase failed: ${(e as Error)?.message ?? e}`);
    return null;
  }
}

/** Read the session's marks; on failure the caller falls back to its cached index (R2). */
export function readMarks(
  sessionKey: string,
  agentId: string,
  hostDbPath: string,
  warn: (msg: string) => void
): { ok: boolean; marks: unknown[]; reason?: string } {
  const opened = openSessionLedger(sessionKey, agentId, hostDbPath, warn);
  if (!opened.ok) return { ok: false, marks: [], reason: opened.reason };
  try {
    const marks = opened.db
      .prepare(
        `SELECT mark_id, session_id, seq_start, seq_end, kind, action, digest, reason,
                applied_gen, applied_at, anchor_first_event, anchor_last_event, retired_at, prune_epoch
           FROM marks ORDER BY seq_start`
      )
      .all();
    return { ok: true, marks };
  } catch (e) {
    const reason = `marks-read-failed:${(e as Error)?.message ?? e}`;
    warn(`[astinus-ledger] ${sessionKey}: ${reason}`);
    return { ok: false, marks: [], reason };
  }
}

/**
 * Read the session's marks from an ALREADY-OPEN ledger only (assemble's hot path,
 * §6). assemble runs at turn start, before/without the meta insert a cold open
 * would need, so it must never create a file: if the ledger is not cached (a
 * cold start, or a key the router does not serve) this returns not-open and the
 * caller falls back to its in-memory index — or to no marks at all on a cold
 * start. Never throws.
 */
export function readMarksCached(
  sessionKey: string,
  warn: (msg: string) => void
): { ok: boolean; marks: unknown[]; reason?: string } {
  const known = disabled.get(sessionKey);
  if (known && known.until > Date.now()) {
    return { ok: false, marks: [], reason: known.reason };
  }
  const db = cache.get(sessionKey);
  if (!db) return { ok: false, marks: [], reason: "not-open" };
  try {
    const marks = db
      .prepare(
        `SELECT mark_id, session_id, seq_start, seq_end, kind, action, digest, reason,
                applied_gen, applied_at, anchor_first_event, anchor_last_event, retired_at, prune_epoch
           FROM marks ORDER BY seq_start`
      )
      .all();
    return { ok: true, marks };
  } catch (e) {
    const reason = `marks-read-failed:${(e as Error)?.message ?? e}`;
    warn(`[astinus-ledger] ${sessionKey}: ${reason}`);
    return { ok: false, marks: [], reason };
  }
}

/**
 * The highest committed turn seq_end in an already-open ledger (in-process
 * engine-serving check, §12.3). Returns null when the ledger is not open or has
 * no turns yet. Never throws.
 */
export function readMaxTurnSeq(sessionKey: string): number | null {
  const db = cache.get(sessionKey);
  if (!db) return null;
  try {
    const row = db.prepare("SELECT MAX(seq_end) AS m FROM turns WHERE heartbeat = 0").get() as
      | { m?: number | null }
      | undefined;
    return typeof row?.m === "number" ? row.m : null;
  } catch {
    return null;
  }
}
export function readMaxTurnId(sessionKey: string): number | null {
  const db = cache.get(sessionKey);
  if (!db) return null;
  try {
    const row = db.prepare("SELECT MAX(turn_id) AS m FROM turns WHERE heartbeat = 0").get() as
      | { m?: number | null }
      | undefined;
    return typeof row?.m === "number" ? row.m : null;
  } catch {
    return null;
  }
}
/**
 * Current durable context state for a session, read from the cached ledger handle
 * (the pass-payload fence needs it on the commit path, where only the cache is
 * guaranteed open). Never throws; null when the handle is closed or the ledger is
 * pre-v2 (no context_state table).
 */
export function readContextStateCached(sessionKey: string): ContextStateRow | null {
  const db = cache.get(sessionKey);
  if (!db) return null;
  return readContextState(db, sessionKey);
}

/**
 * Ordered source entry ids of the session's ACTIVE checkpoint, from the cached
 * handle (pass-payload fence). Empty when the handle is closed, there is no active
 * checkpoint, or the checkpoint has no coverage rows. Never throws.
 */
export function readActiveCheckpointEntryIdsCached(sessionKey: string): string[] {
  const db = cache.get(sessionKey);
  if (!db) return [];
  const st = readContextState(db, sessionKey);
  if (!st?.active_checkpoint_id) return [];
  return readCheckpointEntries(db, st.active_checkpoint_id)
    .map((r) => String((r as { source_entry_id?: unknown }).source_entry_id ?? ""))
    .filter((s) => s.length > 0);
}

