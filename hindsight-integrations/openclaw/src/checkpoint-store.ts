// Durable checkpoint store for owner-mode compaction (design section 3, schema v2).
//
// This module owns the v2 durable records the publish path needs and the
// v1 -> v2 migration. It is deliberately separate from session-ledger.ts: the
// design warns "do not pretend the existing table implements rebasing", so the
// active projection lives in explicit ledger_bindings rows, never as an
// overload of marks or turns.
//
// Failure policy mirrors the ledger (R1/R2): every entry point returns a result
// and never throws. The atomic publish runs in ONE SQLite transaction and never
// holds a write lock across a model/network call - no network code exists here.
import { DatabaseSync } from "node:sqlite";

/** Current durable ledger/checkpoint schema version. */
export const CHECKPOINT_SCHEMA_VERSION = 2;
/** The legacy version this migration reads. */
export const CHECKPOINT_SCHEMA_VERSION_V1 = 1;

/** v2 durable records (design section 3). Additive: existing v1 tables are untouched. */
export const CHECKPOINT_SCHEMA_V2_DDL = [
  "CREATE TABLE IF NOT EXISTS context_state (",
  "  session_key        TEXT PRIMARY KEY,",
  "  host_target        TEXT,",
  "  host_revision      TEXT,",
  "  read_cursor        TEXT,",
  "  view_epoch         TEXT NOT NULL DEFAULT '0',",
  "  ledger_revision    INTEGER NOT NULL DEFAULT 0,",
  "  active_checkpoint_id TEXT,",
  "  status             TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('ready','rebuilding','blocked')),",
  "  last_rebuild_reason TEXT,",
  "  updated_at         INTEGER NOT NULL",
  ");",
  "CREATE TABLE IF NOT EXISTS context_checkpoints (",
  "  checkpoint_id      TEXT PRIMARY KEY,",
  "  session_key        TEXT NOT NULL,",
  "  parent_checkpoint_id TEXT,",
  "  idempotency_key    TEXT NOT NULL,",
  "  source_revision    TEXT NOT NULL,",
  "  view_epoch         TEXT NOT NULL,",
  "  ledger_revision    INTEGER NOT NULL,",
  "  summary            TEXT,",
  "  summary_provenance TEXT,",
  "  protected_boundary TEXT,",
  "  policy_version     TEXT,",
  "  model_version      TEXT,",
  "  estimator_version  TEXT,",
  "  before_tokens      INTEGER,",
  "  after_tokens       INTEGER,",
  "  created_at         INTEGER NOT NULL,",
  "  UNIQUE(session_key, idempotency_key)",
  ");",
  "CREATE TABLE IF NOT EXISTS checkpoint_entries (",
  "  checkpoint_id      TEXT NOT NULL REFERENCES context_checkpoints(checkpoint_id),",
  "  ordinal            INTEGER NOT NULL,",
  "  source_entry_id    TEXT NOT NULL,",
  "  source_entry_hash  TEXT,",
  "  relation           TEXT NOT NULL CHECK (relation IN ('represented','verbatim','protected')),",
  "  PRIMARY KEY (checkpoint_id, ordinal)",
  ");",
  "CREATE TABLE IF NOT EXISTS ledger_bindings (",
  "  binding_id         INTEGER PRIMARY KEY AUTOINCREMENT,",
  "  session_key        TEXT NOT NULL,",
  "  host_revision      TEXT NOT NULL,",
  "  view_epoch         TEXT NOT NULL,",
  "  rebuild_id         TEXT,",
  "  turn_id            INTEGER,",
  "  mark_id            INTEGER,",
  "  source_entry_anchor TEXT,",
  "  raw_seq            INTEGER,",
  "  disposition        TEXT NOT NULL CHECK (disposition IN ('verbatim','represented','inactive','unresolved')),",
  "  evidence           TEXT,",
  "  created_at         INTEGER NOT NULL",
  ");",
  "CREATE INDEX IF NOT EXISTS ix_checkpoint_entries_src ON checkpoint_entries(source_entry_id);",
  "CREATE INDEX IF NOT EXISTS ix_ledger_bindings_session ON ledger_bindings(session_key, view_epoch);",
].join("\n");

export type SchemaAudit = {
  ok: boolean;
  fromVersion: number;
  toVersion: number;
  reason?: string;
  rowCounts: Record<string, number>;
};

const V1_AUDIT_TABLES = [
  "meta",
  "turns",
  "topics",
  "turn_topics",
  "marks",
  "supersessions",
  "passes",
  "rebases",
];

function tableCount(db: DatabaseSync, table: string): number {
  try {
    const row = db.prepare('SELECT COUNT(*) AS c FROM "' + table + '"').get() as
      | { c?: number }
      | undefined;
    return Number(row?.c ?? 0);
  } catch {
    return 0;
  }
}

function auditCounts(db: DatabaseSync): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const t of V1_AUDIT_TABLES) counts[t] = tableCount(db, t);
  for (const t of [
    "context_state",
    "context_checkpoints",
    "checkpoint_entries",
    "ledger_bindings",
  ]) {
    counts[t] = tableCount(db, t);
  }
  return counts;
}

/** Read the file SQLite schema version (0 when unset). */
export function readSchemaVersion(db: DatabaseSync): number {
  const row = db.prepare("PRAGMA user_version").get() as { user_version?: number } | undefined;
  return Number(row?.user_version ?? 0);
}

/**
 * Reject a future schema version. A ledger written by a newer plugin must not be
 * mutated by this one (design gate G: a future schema version rejects).
 */
export function assertSupportedSchemaVersion(
  version: number
): { ok: true } | { ok: false; reason: string } {
  if (version > CHECKPOINT_SCHEMA_VERSION) {
    return { ok: false, reason: "future-schema-version:" + version };
  }
  return { ok: true };
}

/**
 * Migrate a v1 ledger file to schema v2 in ONE transaction. Preserves every v1
 * row (audit data); only adds the v2 tables and seeds a context_state row per
 * session key. Restartable: a fresh v2 file is a no-op; an unfinished run rolls
 * back as a unit. Never throws.
 */
export function migrateLedgerV1ToV2(db: DatabaseSync, now = Date.now()): SchemaAudit {
  const fromVersion = readSchemaVersion(db);
  const before = auditCounts(db);
  if (fromVersion === CHECKPOINT_SCHEMA_VERSION) {
    return { ok: true, fromVersion, toVersion: CHECKPOINT_SCHEMA_VERSION, rowCounts: before };
  }
  if (fromVersion > CHECKPOINT_SCHEMA_VERSION) {
    return {
      ok: false,
      fromVersion,
      toVersion: CHECKPOINT_SCHEMA_VERSION,
      reason: "future-schema-version:" + fromVersion,
      rowCounts: before,
    };
  }
  try {
    db.exec("BEGIN IMMEDIATE");
    db.exec(CHECKPOINT_SCHEMA_V2_DDL);
    const keys = db.prepare("SELECT DISTINCT session_key FROM meta").all() as Array<{
      session_key: string;
    }>;
    const insertState = db.prepare(
      "INSERT OR IGNORE INTO context_state " +
        "(session_key, host_target, host_revision, read_cursor, view_epoch, ledger_revision, " +
        "active_checkpoint_id, status, last_rebuild_reason, updated_at) " +
        "VALUES (?, NULL, NULL, NULL, '0', 0, NULL, 'ready', 'migrated-v1-to-v2', ?)"
    );
    const setRev = db.prepare(
      "UPDATE context_state SET host_revision = ?, read_cursor = ? WHERE session_key = ?"
    );
    const maxSeq = db.prepare("SELECT MAX(seq_end) AS m FROM turns");
    for (const k of keys) {
      const m = maxSeq.get() as { m?: number | null } | undefined;
      const revision = typeof m?.m === "number" ? "seq:" + m.m : "seq:0";
      insertState.run(k.session_key, now);
      setRev.run(revision, revision, k.session_key);
    }
    try {
      db.prepare("UPDATE meta SET schema_version = ?").run(CHECKPOINT_SCHEMA_VERSION);
    } catch {
      /* meta.schema_version is integer-typed on v1; tolerate a legacy shape */
    }
    db.exec("PRAGMA user_version = " + CHECKPOINT_SCHEMA_VERSION);
    db.exec("COMMIT");
    const after = auditCounts(db);
    for (const t of V1_AUDIT_TABLES) {
      if ((after[t] ?? 0) !== (before[t] ?? 0)) {
        return {
          ok: false,
          fromVersion,
          toVersion: CHECKPOINT_SCHEMA_VERSION,
          reason: "row-count-changed:" + t,
          rowCounts: after,
        };
      }
    }
    return { ok: true, fromVersion, toVersion: CHECKPOINT_SCHEMA_VERSION, rowCounts: after };
  } catch (e) {
    try {
      db.exec("ROLLBACK");
    } catch {
      /* already rolled back */
    }
    return {
      ok: false,
      fromVersion,
      toVersion: CHECKPOINT_SCHEMA_VERSION,
      reason: "migrate-failed:" + ((e as Error)?.message ?? e),
      rowCounts: before,
    };
  }
}

/** Open (or create) a checkpoint DB handle with the plugin pragmas. */
export function openCheckpointDb(path: string): DatabaseSync {
  const db = new DatabaseSync(path);
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA busy_timeout = 250;");
  return db;
}

/** Create the full v2 schema on a fresh file (no v1 history). */
export function createCheckpointSchemaV2(db: DatabaseSync): { ok: boolean; reason?: string } {
  try {
    db.exec(CHECKPOINT_SCHEMA_V2_DDL);
    db.exec("PRAGMA user_version = " + CHECKPOINT_SCHEMA_VERSION);
    return { ok: true };
  } catch (e) {
    return { ok: false, reason: "create-failed:" + ((e as Error)?.message ?? e) };
  }
}

export type ContextStateRow = {
  session_key: string;
  host_target: string | null;
  host_revision: string | null;
  read_cursor: string | null;
  view_epoch: string;
  ledger_revision: number;
  active_checkpoint_id: string | null;
  status: "ready" | "rebuilding" | "blocked";
  last_rebuild_reason: string | null;
  updated_at: number;
};

/** Cold-start read of the active context state for one session (never throws). */
export function readContextState(db: DatabaseSync, sessionKey: string): ContextStateRow | null {
  try {
    const row = db
      .prepare(
        "SELECT session_key, host_target, host_revision, read_cursor, view_epoch, " +
          "ledger_revision, active_checkpoint_id, status, last_rebuild_reason, updated_at " +
          "FROM context_state WHERE session_key = ?"
      )
      .get(sessionKey) as ContextStateRow | undefined;
    return row ?? null;
  } catch {
    return null;
  }
}

/** Cold-start read of one published checkpoint (never throws). */
export function readPublishedCheckpoint(
  db: DatabaseSync,
  sessionKey: string,
  checkpointId: string
): Record<string, unknown> | null {
  try {
    const row = db
      .prepare("SELECT * FROM context_checkpoints WHERE session_key = ? AND checkpoint_id = ?")
      .get(sessionKey, checkpointId) as Record<string, unknown> | undefined;
    return row ?? null;
  } catch {
    return null;
  }
}

/** Cold-start read of a checkpoint coverage rows (never throws). */
export function readCheckpointEntries(
  db: DatabaseSync,
  checkpointId: string
): Array<Record<string, unknown>> {
  try {
    return db
      .prepare("SELECT * FROM checkpoint_entries WHERE checkpoint_id = ? ORDER BY ordinal")
      .all(checkpointId) as Array<Record<string, unknown>>;
  } catch {
    return [];
  }
}

export type ActiveBindingsRead =
  | { status: "ok"; viewEpoch: string; bindings: Array<Record<string, unknown>> }
  | { status: "unavailable"; reason: string };

/**
 * R11a: read the ACTIVE bindings - the rows stamped for the CURRENT valid view
 * epoch of the session state. Historic epochs are preserved for audit but must
 * not reappear in the active view; reading every epoch is how an obsolete epoch
 * reinflates history. Distinguishes "unavailable" (no/unusable durable state)
 * from a genuinely empty active set. Never throws.
 */
export function readActiveBindingsState(db: DatabaseSync, sessionKey: string): ActiveBindingsRead {
  const state = readContextState(db, sessionKey);
  if (!state) {
    return { status: "unavailable", reason: "no-context-state" };
  }
  if (state.status !== "ready") {
    return { status: "unavailable", reason: "state-" + state.status };
  }
  try {
    const bindings = db
      .prepare(
        "SELECT * FROM ledger_bindings WHERE session_key = ? AND view_epoch = ? " +
          "ORDER BY binding_id"
      )
      .all(sessionKey, state.view_epoch) as Array<Record<string, unknown>>;
    return { status: "ok", viewEpoch: state.view_epoch, bindings };
  } catch (e) {
    return { status: "unavailable", reason: "read-failed:" + ((e as Error)?.message ?? e) };
  }
}

/**
 * R11a: the current-epoch active bindings as a plain array (backward-compatible
 * shape). Returns [] both when durable state is unavailable and when the active
 * set is empty; a caller that must tell those apart uses readActiveBindingsState.
 */
export function readActiveBindings(
  db: DatabaseSync,
  sessionKey: string
): Array<Record<string, unknown>> {
  const read = readActiveBindingsState(db, sessionKey);
  return read.status === "ok" ? read.bindings : [];
}

export type CheckpointEntryInput = {
  sourceEntryId: string;
  sourceEntryHash?: string;
  relation: "represented" | "verbatim" | "protected";
};

/** One rebuilt active binding row (wave 4 item 1/3). */
export type ActiveBindingInput = {
  turnId?: number | null;
  markId?: number | null;
  sourceEntryAnchor?: string | null;
  rawSeq?: number | null;
  disposition: "verbatim" | "represented" | "inactive" | "unresolved";
  evidence?: string | null;
};

export type CheckpointPublishInput = {
  checkpointId: string;
  parentCheckpointId?: string | null;
  idempotencyKey: string;
  sourceRevision: string;
  summary?: string | null;
  summaryProvenance?: string | null;
  protectedBoundary?: string | null;
  entries: CheckpointEntryInput[];
  beforeTokens?: number | null;
  afterTokens?: number | null;
  policyVersion?: string | null;
  modelVersion?: string | null;
  estimatorVersion?: string | null;
  /** Expected prior state - compare-and-set fence (design section 3). */
  expectedSourceRevision: string;
  expectedViewEpoch: string;
  expectedLedgerRevision: number;
  /**
   * Wave 4 item 1: the actively-rebuilt bindings to publish ATOMICALLY with the
   * checkpoint. When provided, these replace the per-coverage-entry bindings
   * derived below; when absent, the derived bindings are kept (backward
   * compatible with the wave-2/3 publish path).
   */
  activeBindings?: ActiveBindingInput[];
  /** Rebuild identity stamping this binding set (same-source or shadow rebuild). */
  rebuildId?: string;
  now?: number;
};

export type CheckpointPublishResult =
  | {
      ok: true;
      checkpointId: string;
      /** false when an identical idempotency key had already published. */
      published: boolean;
      viewEpoch: string;
      ledgerRevision: number;
      reason?: "idempotent-replay";
    }
  | {
      ok: false;
      code: "stale-source" | "stale-epoch" | "stale-revision" | "future-schema" | "busy" | "error";
      reason: string;
    };

/**
 * Atomically publish one immutable checkpoint: insert the checkpoint + coverage,
 * publish its active bindings and advance the active pointer/view epoch/ledger
 * revision in ONE transaction. The idempotency key publishes at most once. The
 * compare-and-set against the prior source revision/view epoch/ledger revision
 * is checked INSIDE the transaction; the host claim callback
 * (checkpoint-publish.ts) is validated by the caller BEFORE this runs. No
 * network or model call is made while the write lock is held.
 */
export function publishCheckpoint(
  db: DatabaseSync,
  sessionKey: string,
  input: CheckpointPublishInput
): CheckpointPublishResult {
  const now = input.now ?? Date.now();
  const version = readSchemaVersion(db);
  const supported = assertSupportedSchemaVersion(version);
  if (!supported.ok) {
    return { ok: false, code: "future-schema", reason: supported.reason };
  }
  if (version < CHECKPOINT_SCHEMA_VERSION) {
    return { ok: false, code: "error", reason: "schema-not-migrated:" + version };
  }
  try {
    db.exec("BEGIN IMMEDIATE");
  } catch (e) {
    return { ok: false, code: "busy", reason: "begin-failed:" + ((e as Error)?.message ?? e) };
  }
  try {
    const existing = db
      .prepare(
        "SELECT checkpoint_id FROM context_checkpoints WHERE session_key = ? AND idempotency_key = ?"
      )
      .get(sessionKey, input.idempotencyKey) as { checkpoint_id?: string } | undefined;
    if (existing?.checkpoint_id) {
      db.exec("ROLLBACK");
      // R12: return ONE internally consistent publication identity. A replay of an
      // older idempotency key must report that HISTORICAL checkpoint's own epoch and
      // ledger revision - never the CURRENT state's, which would label the wrong
      // assembled view (checkpointId=cp1 carrying the epoch/revision of cp2).
      const historical = db
        .prepare(
          "SELECT view_epoch, ledger_revision FROM context_checkpoints " +
            "WHERE session_key = ? AND checkpoint_id = ?"
        )
        .get(sessionKey, existing.checkpoint_id) as
        | { view_epoch?: string | null; ledger_revision?: number | null }
        | undefined;
      return {
        ok: true,
        checkpointId: existing.checkpoint_id,
        published: false,
        viewEpoch: historical?.view_epoch ?? "0",
        ledgerRevision: Number(historical?.ledger_revision ?? 0),
        reason: "idempotent-replay",
      };
    }

    const state = readContextState(db, sessionKey);
    const currentRevision = state?.host_revision ?? null;
    const currentEpoch = state?.view_epoch ?? "0";
    const currentLedgerRevision = state?.ledger_revision ?? 0;
    if (currentRevision !== null && currentRevision !== input.expectedSourceRevision) {
      db.exec("ROLLBACK");
      return {
        ok: false,
        code: "stale-source",
        reason:
          "source-revision-changed:expected=" +
          input.expectedSourceRevision +
          ":current=" +
          currentRevision,
      };
    }
    if (currentEpoch !== input.expectedViewEpoch) {
      db.exec("ROLLBACK");
      return {
        ok: false,
        code: "stale-epoch",
        reason:
          "view-epoch-changed:expected=" + input.expectedViewEpoch + ":current=" + currentEpoch,
      };
    }
    if (currentLedgerRevision !== input.expectedLedgerRevision) {
      db.exec("ROLLBACK");
      return {
        ok: false,
        code: "stale-revision",
        reason:
          "ledger-revision-changed:expected=" +
          input.expectedLedgerRevision +
          ":current=" +
          currentLedgerRevision,
      };
    }

    const nextLedgerRevision = currentLedgerRevision + 1;
    const nextViewEpoch = input.checkpointId;
    db.prepare(
      "INSERT INTO context_checkpoints " +
        "(checkpoint_id, session_key, parent_checkpoint_id, idempotency_key, source_revision, " +
        "view_epoch, ledger_revision, summary, summary_provenance, protected_boundary, " +
        "policy_version, model_version, estimator_version, before_tokens, after_tokens, created_at) " +
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    ).run(
      input.checkpointId,
      sessionKey,
      input.parentCheckpointId ?? state?.active_checkpoint_id ?? null,
      input.idempotencyKey,
      input.sourceRevision,
      nextViewEpoch,
      nextLedgerRevision,
      input.summary ?? null,
      input.summaryProvenance ?? null,
      input.protectedBoundary ?? null,
      input.policyVersion ?? null,
      input.modelVersion ?? null,
      input.estimatorVersion ?? null,
      input.beforeTokens ?? null,
      input.afterTokens ?? null,
      now
    );
    const insertEntry = db.prepare(
      "INSERT INTO checkpoint_entries " +
        "(checkpoint_id, ordinal, source_entry_id, source_entry_hash, relation) " +
        "VALUES (?,?,?,?,?)"
    );
    const insertBinding = db.prepare(
      "INSERT INTO ledger_bindings " +
        "(session_key, host_revision, view_epoch, rebuild_id, turn_id, mark_id, " +
        "source_entry_anchor, raw_seq, disposition, evidence, created_at) " +
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    );
    input.entries.forEach((entry, ordinal) => {
      insertEntry.run(
        input.checkpointId,
        ordinal,
        entry.sourceEntryId,
        entry.sourceEntryHash ?? null,
        entry.relation
      );
    });
    // Wave 4 item 1: publish the rebuilt active bindings in the SAME transaction.
    if (input.activeBindings && input.activeBindings.length > 0) {
      for (const binding of input.activeBindings) {
        insertBinding.run(
          sessionKey,
          input.sourceRevision,
          nextViewEpoch,
          input.rebuildId ?? input.checkpointId,
          binding.turnId ?? null,
          binding.markId ?? null,
          binding.sourceEntryAnchor ?? null,
          binding.rawSeq ?? null,
          binding.disposition,
          binding.evidence ?? null,
          now
        );
      }
    } else {
      for (const entry of input.entries) {
        const disposition = entry.relation === "represented" ? "represented" : "verbatim";
        insertBinding.run(
          sessionKey,
          input.sourceRevision,
          nextViewEpoch,
          input.checkpointId,
          null,
          null,
          entry.sourceEntryId,
          null,
          disposition,
          "relation:" + entry.relation,
          now
        );
      }
    }
    db.prepare(
      "UPDATE context_state " +
        "SET host_revision = ?, view_epoch = ?, ledger_revision = ?, active_checkpoint_id = ?, " +
        "status = 'ready', last_rebuild_reason = NULL, updated_at = ? " +
        "WHERE session_key = ?"
    ).run(
      input.sourceRevision,
      nextViewEpoch,
      nextLedgerRevision,
      input.checkpointId,
      now,
      sessionKey
    );
    const changed = db
      .prepare("SELECT 1 AS x FROM context_state WHERE session_key = ?")
      .get(sessionKey);
    if (!changed) {
      db.prepare(
        "INSERT INTO context_state " +
          "(session_key, host_target, host_revision, read_cursor, view_epoch, ledger_revision, " +
          "active_checkpoint_id, status, last_rebuild_reason, updated_at) " +
          "VALUES (?, NULL, ?, NULL, ?, ?, ?, 'ready', NULL, ?)"
      ).run(
        sessionKey,
        input.sourceRevision,
        nextViewEpoch,
        nextLedgerRevision,
        input.checkpointId,
        now
      );
    }
    db.exec("COMMIT");
    return {
      ok: true,
      checkpointId: input.checkpointId,
      published: true,
      viewEpoch: nextViewEpoch,
      ledgerRevision: nextLedgerRevision,
    };
  } catch (e) {
    try {
      db.exec("ROLLBACK");
    } catch {
      /* already rolled back */
    }
    return { ok: false, code: "error", reason: "publish-failed:" + ((e as Error)?.message ?? e) };
  }
}

/** Read the active bindings stamped for one published view epoch (never throws). */
export function readBindingsForEpoch(
  db: DatabaseSync,
  sessionKey: string,
  viewEpoch: string
): Array<Record<string, unknown>> {
  try {
    return db
      .prepare(
        "SELECT * FROM ledger_bindings WHERE session_key = ? AND view_epoch = ? ORDER BY binding_id"
      )
      .all(sessionKey, viewEpoch) as Array<Record<string, unknown>>;
  } catch {
    return [];
  }
}

/**
 * Append a structured rebase audit record on an external host revision change
 * (design section 3: "Existing rebases: Append an audit record on external host
 * revision change; add structured rebuild/view refs rather than relying on
 * free-text note"). Adds an `audit_json` column when absent. Never throws; a
 * context-only ledger without a rebases table is a tolerated no-op.
 */
export function recordRebase(
  db: DatabaseSync,
  params: {
    sessionKey: string;
    oldGen: string | null;
    newGen: string | null;
    reason: string;
    rebuildId?: string | null;
    viewEpoch?: string | null;
    ledgerRevision?: number | null;
    now?: number;
  }
): { ok: true; rebaseId: number | null } | { ok: false; reason: string } {
  const now = params.now ?? Date.now();
  try {
    const cols = db.prepare("PRAGMA table_info(rebases)").all() as Array<{ name?: string }>;
    if (cols.length === 0) {
      return { ok: false, reason: "no-rebases-table" };
    }
    if (!cols.some((c) => c.name === "audit_json")) {
      db.exec("ALTER TABLE rebases ADD COLUMN audit_json TEXT");
    }
    const audit = JSON.stringify({
      reason: params.reason,
      rebuildId: params.rebuildId ?? null,
      viewEpoch: params.viewEpoch ?? null,
      ledgerRevision: params.ledgerRevision ?? null,
    });
    const info = db
      .prepare(
        "INSERT INTO rebases (session_id, old_gen, new_gen, at, note, audit_json) VALUES (?,?,?,?,?,?)"
      )
      .run(params.sessionKey, params.oldGen, params.newGen, now, params.reason, audit);
    return { ok: true, rebaseId: Number(info.lastInsertRowid ?? 0) };
  } catch (e) {
    return { ok: false, reason: "rebase-audit-failed:" + ((e as Error)?.message ?? e) };
  }
}

/**
 * Garbage-collect ledger_bindings for view epochs no longer kept (orphan shadow
 * candidates). Only bindings are removed; the immutable checkpoint/coverage rows
 * and the raw transcript are never touched. Returns the number of orphan rows
 * removed and records an audit row when a rebases table exists.
 */
export function gcLedgerBindings(
  db: DatabaseSync,
  sessionKey: string,
  keepEpochs: readonly string[],
  now = Date.now()
): { ok: boolean; removed: number; reason?: string } {
  const keep = new Set(keepEpochs);
  try {
    const rows = db
      .prepare("SELECT binding_id, view_epoch FROM ledger_bindings WHERE session_key = ?")
      .all(sessionKey) as Array<{ binding_id: number; view_epoch: string }>;
    const orphans = rows.filter((r) => !keep.has(r.view_epoch));
    if (orphans.length === 0) {
      return { ok: true, removed: 0 };
    }
    const del = db.prepare("DELETE FROM ledger_bindings WHERE binding_id = ?");
    db.exec("BEGIN IMMEDIATE");
    for (const orphan of orphans) {
      del.run(orphan.binding_id);
    }
    db.exec("COMMIT");
    recordRebase(db, {
      sessionKey,
      oldGen: null,
      newGen: null,
      reason: "gc-orphan-bindings:removed=" + orphans.length,
      rebuildId: null,
      viewEpoch: keepEpochs[0] ?? null,
      now,
    });
    return { ok: true, removed: orphans.length };
  } catch (e) {
    try {
      db.exec("ROLLBACK");
    } catch {
      /* already rolled back */
    }
    return { ok: false, removed: 0, reason: "gc-failed:" + ((e as Error)?.message ?? e) };
  }
}
