// Astinus session ledger (plan v3, §6). One SQLite file per router-served session key
// holding turn refs and — written by the sidecar pass — the model's topic mapping, marks
// and supersessions. The plugin writes only `turns` and `marks.applied_*`.
//
// Failure policy (R1/R2): the ledger can NEVER break a turn. Every entry point returns a
// result and never throws; a failure logs at warn and the caller continues.
import { DatabaseSync } from "node:sqlite";
import { mkdirSync } from "fs";
import { join } from "path";

export const LEDGER_SCHEMA_VERSION = 1;
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
  view_tokens_before INTEGER, view_tokens_after INTEGER
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
`;

const cache = new Map<string, DatabaseSync>();
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
  if (cached) return { ok: true, db: cached };
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
      db.exec(`PRAGMA user_version = ${LEDGER_SCHEMA_VERSION};`);
      db.prepare(
        "INSERT INTO meta(session_key, agent_id, host_db_path, schema_version, created_at) VALUES(?,?,?,?,?)"
      ).run(sessionKey, agentId, hostDbPath, LEDGER_SCHEMA_VERSION, Date.now());
    } else if (v !== LEDGER_SCHEMA_VERSION) {
      const reason = `schema-version-mismatch:${v}`;
      db.close();
      disabled.set(sessionKey, { reason, until: Number.POSITIVE_INFINITY });
      warn(`[astinus-ledger] ${sessionKey}: ${reason}; ledger disabled for this session`);
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
      }
    }
    cache.set(sessionKey, db);
    return { ok: true, db };
  } catch (e) {
    const reason = `open-failed:${(e as Error)?.message ?? e}`;
    disabled.set(sessionKey, { reason, until: Date.now() + OPEN_FAILED_COOLDOWN_MS });
    warn(`[astinus-ledger] ${sessionKey}: ${reason}`);
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
                applied_gen, applied_at
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
                applied_gen, applied_at
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
