import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  CHECKPOINT_SCHEMA_VERSION,
  assertSupportedSchemaVersion,
  createCheckpointSchemaV2,
  migrateLedgerV1ToV2,
  openCheckpointDb,
  publishCheckpoint,
  readActiveBindings,
  readCheckpointEntries,
  readContextState,
  readPublishedCheckpoint,
  readSchemaVersion,
} from "./checkpoint-store.js";

const V1_DDL = [
  "CREATE TABLE meta (session_key TEXT NOT NULL, agent_id TEXT NOT NULL, host_db_path TEXT NOT NULL, schema_version INTEGER NOT NULL, created_at INTEGER NOT NULL)",
  "CREATE TABLE turns (turn_id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, seq_start INTEGER NOT NULL, seq_end INTEGER NOT NULL, gen TEXT NOT NULL, advancement_key TEXT NOT NULL UNIQUE, heartbeat INTEGER NOT NULL DEFAULT 0, committed_at INTEGER NOT NULL, classified_at INTEGER)",
  "CREATE TABLE topics (topic_id INTEGER PRIMARY KEY, slug TEXT NOT NULL, label TEXT NOT NULL, first_seen_seq INTEGER NOT NULL, last_seen_seq INTEGER NOT NULL, last_seen_at INTEGER NOT NULL, digest TEXT, terminal_at INTEGER, UNIQUE(slug))",
  "CREATE TABLE marks (mark_id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, seq_start INTEGER NOT NULL, seq_end INTEGER NOT NULL, kind TEXT NOT NULL, action TEXT NOT NULL, digest TEXT, reason TEXT NOT NULL, proposed_at INTEGER NOT NULL, applied_gen TEXT, applied_at INTEGER)",
].join(";");

let dir: string;
let file: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "ckpt-store-"));
  file = join(dir, "session.sqlite");
});
afterEach(() => {
  try {
    rmSync(dir, { recursive: true, force: true });
  } catch {
    /* ignore windows lock */
  }
});

function createV1Ledger(): DatabaseSync {
  const db = openCheckpointDb(file);
  db.exec(V1_DDL);
  db.exec("PRAGMA user_version = 1");
  db.prepare(
    "INSERT INTO meta(session_key, agent_id, host_db_path, schema_version, created_at) VALUES(?,?,?,?,?)"
  ).run("agent:main:main", "main", "host", 1, 1);
  const t = db.prepare(
    "INSERT INTO turns(session_id, seq_start, seq_end, gen, advancement_key, heartbeat, committed_at) VALUES(?,?,?,?,?,?,?)"
  );
  t.run("s", 0, 4, "g0", "k1", 0, 1);
  t.run("s", 5, 9, "g0", "k2", 0, 2);
  t.run("s", 10, 14, "g0", "k3", 1, 3);
  db.prepare(
    "INSERT INTO marks(session_id, seq_start, seq_end, kind, action, reason, proposed_at) VALUES(?,?,?,?,?,?,?)"
  ).run("s", 0, 4, "turn", "digest", "r", 1);
  db.prepare(
    "INSERT INTO marks(session_id, seq_start, seq_end, kind, action, reason, proposed_at) VALUES(?,?,?,?,?,?,?)"
  ).run("s", 5, 9, "tool_noise", "drop", "r", 2);
  db.prepare(
    "INSERT INTO topics(slug, label, first_seen_seq, last_seen_seq, last_seen_at) VALUES(?,?,?,?,?)"
  ).run("t", "T", 0, 14, 3);
  return db;
}

describe("migrateLedgerV1ToV2", () => {
  it("migrates v1 -> v2 preserving every v1 row and the audit data", () => {
    const db = createV1Ledger();
    const audit = migrateLedgerV1ToV2(db, 5);
    expect(audit.ok).toBe(true);
    expect(audit.fromVersion).toBe(1);
    expect(audit.toVersion).toBe(CHECKPOINT_SCHEMA_VERSION);
    expect(audit.rowCounts.turns).toBe(3);
    expect(audit.rowCounts.marks).toBe(2);
    expect(audit.rowCounts.topics).toBe(1);
    expect(readSchemaVersion(db)).toBe(2);
    const state = readContextState(db, "agent:main:main");
    expect(state?.status).toBe("ready");
    expect(state?.host_revision).toBe("seq:14");
    expect(state?.ledger_revision).toBe(0);
    db.close();
  });

  it("is a no-op on an already-v2 file and restartable", () => {
    const db = createV1Ledger();
    expect(migrateLedgerV1ToV2(db).ok).toBe(true);
    const again = migrateLedgerV1ToV2(db);
    expect(again.ok).toBe(true);
    expect(again.fromVersion).toBe(2);
    expect(again.rowCounts.turns).toBe(3);
    db.close();
  });

  it("rejects a future schema version without touching the file", () => {
    const db = createV1Ledger();
    db.exec("PRAGMA user_version = 99");
    const audit = migrateLedgerV1ToV2(db);
    expect(audit.ok).toBe(false);
    expect(audit.reason).toBe("future-schema-version:99");
    expect(readSchemaVersion(db)).toBe(99);
    expect(assertSupportedSchemaVersion(99)).toMatchObject({ ok: false });
    db.close();
  });
});

describe("publishCheckpoint", () => {
  function migratedDb(): DatabaseSync {
    const db = createV1Ledger();
    migrateLedgerV1ToV2(db, 5);
    return db;
  }
  const base = {
    checkpointId: "cp-1",
    idempotencyKey: "idem-1",
    sourceRevision: "seq:14",
    expectedSourceRevision: "seq:14",
    expectedViewEpoch: "0",
    expectedLedgerRevision: 0,
    entries: [
      { sourceEntryId: "e1", relation: "represented" as const },
      { sourceEntryId: "e2", relation: "verbatim" as const },
    ],
    beforeTokens: 1000,
    afterTokens: 400,
    now: 7,
  };

  it("atomically inserts the checkpoint, bindings and advances the pointer", () => {
    const db = migratedDb();
    const res = publishCheckpoint(db, "agent:main:main", base);
    expect(res.ok).toBe(true);
    if (res.ok) {
      expect(res.published).toBe(true);
      expect(res.viewEpoch).toBe("cp-1");
      expect(res.ledgerRevision).toBe(1);
    }
    const state = readContextState(db, "agent:main:main");
    expect(state?.active_checkpoint_id).toBe("cp-1");
    expect(state?.view_epoch).toBe("cp-1");
    expect(state?.ledger_revision).toBe(1);
    const entries = readCheckpointEntries(db, "cp-1");
    expect(entries).toHaveLength(2);
    const bindings = readActiveBindings(db, "agent:main:main");
    expect(bindings).toHaveLength(2);
    expect(readPublishedCheckpoint(db, "agent:main:main", "cp-1")?.before_tokens).toBe(1000);
    db.close();
  });

  it("publishes at most once for a repeated idempotency key", () => {
    const db = migratedDb();
    expect(publishCheckpoint(db, "agent:main:main", base).ok).toBe(true);
    const second = publishCheckpoint(db, "agent:main:main", { ...base, checkpointId: "cp-2" });
    expect(second.ok).toBe(true);
    if (second.ok) {
      expect(second.published).toBe(false);
      expect(second.checkpointId).toBe("cp-1");
      expect(second.reason).toBe("idempotent-replay");
    }
    expect(readPublishedCheckpoint(db, "agent:main:main", "cp-2")).toBeNull();
    db.close();
  });

  it("refuses a stale view epoch (compare-and-set) and writes nothing", () => {
    const db = migratedDb();
    const res = publishCheckpoint(db, "agent:main:main", { ...base, expectedViewEpoch: "wrong" });
    expect(res.ok).toBe(false);
    if (!res.ok) expect(res.code).toBe("stale-epoch");
    expect(readPublishedCheckpoint(db, "agent:main:main", "cp-1")).toBeNull();
    expect(readContextState(db, "agent:main:main")?.view_epoch).toBe("0");
    db.close();
  });

  it("returns future-schema on a newer file and never writes", () => {
    const db = migratedDb();
    db.exec("PRAGMA user_version = 99");
    const res = publishCheckpoint(db, "agent:main:main", base);
    expect(res.ok).toBe(false);
    if (!res.ok) expect(res.code).toBe("future-schema");
    db.close();
  });
});

describe("createCheckpointSchemaV2", () => {
  it("creates a fresh v2 file readable on cold start", () => {
    const db = openCheckpointDb(file);
    expect(createCheckpointSchemaV2(db).ok).toBe(true);
    expect(readSchemaVersion(db)).toBe(CHECKPOINT_SCHEMA_VERSION);
    expect(readContextState(db, "none")).toBeNull();
    expect(readCheckpointEntries(db, "none")).toEqual([]);
    expect(readActiveBindings(db, "none")).toEqual([]);
    db.close();
  });
});
