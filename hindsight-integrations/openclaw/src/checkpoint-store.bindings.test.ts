import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  createCheckpointSchemaV2,
  gcLedgerBindings,
  migrateLedgerV1ToV2,
  openCheckpointDb,
  publishCheckpoint,
  readActiveBindings,
  readActiveBindingsState,
  readBindingsForEpoch,
  readCheckpointEntries,
  readContextState,
  recordRebase,
} from "./checkpoint-store.js";

let dir: string;
let file: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "ckpt-bind-"));
  file = join(dir, "session.sqlite");
});
afterEach(() => {
  try {
    rmSync(dir, { recursive: true, force: true });
  } catch {
    /* ignore windows lock */
  }
});

function freshV2(): DatabaseSync {
  const db = openCheckpointDb(file);
  createCheckpointSchemaV2(db);
  db.exec(
    "CREATE TABLE rebases (rebase_id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, old_gen TEXT, new_gen TEXT, at INTEGER NOT NULL, note TEXT)"
  );
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
  now: 7,
};

describe("publishCheckpoint with rebuilt active bindings (wave 4 item 1)", () => {
  it("publishes the rebuilt bindings atomically with the checkpoint", () => {
    const db = freshV2();
    const res = publishCheckpoint(db, "agent:main:main", {
      ...base,
      rebuildId: "rb-1",
      activeBindings: [
        {
          turnId: 4,
          sourceEntryAnchor: "e1",
          disposition: "represented",
          evidence: "covered:represented:cp-1",
        },
        {
          markId: 9,
          sourceEntryAnchor: "e2",
          disposition: "verbatim",
          evidence: "covered:protected:cp-1",
        },
        {
          turnId: 12,
          sourceEntryAnchor: null,
          disposition: "unresolved",
          evidence: "anchorless:no-resolvable-source-entry-id",
        },
      ],
    });
    expect(res.ok).toBe(true);
    const bindings = readActiveBindings(db, "agent:main:main");
    expect(bindings).toHaveLength(3);
    const byTurn = new Map(
      bindings.filter((b) => b.turn_id !== null).map((b) => [b.turn_id as number, b])
    );
    const markBinding = bindings.find((b) => b.mark_id === 9);
    expect(byTurn.get(4)?.disposition).toBe("represented");
    expect(byTurn.get(4)?.rebuild_id).toBe("rb-1");
    expect(markBinding?.disposition).toBe("verbatim");
    expect(byTurn.get(12)?.disposition).toBe("unresolved");
    expect(byTurn.get(12)?.evidence).toContain("anchorless");
    // Historical coverage rows are still present and untouched.
    expect(readCheckpointEntries(db, "cp-1")).toHaveLength(2);
    expect(readContextState(db, "agent:main:main")?.active_checkpoint_id).toBe("cp-1");
    db.close();
  });

  it("returns synchronously (no await inside the write transaction -> no model/network call holds the lock)", () => {
    const db = freshV2();
    const res = publishCheckpoint(db, "agent:main:main", base);
    // A synchronous function: it cannot interleave a network/model call under BEGIN IMMEDIATE.
    expect(typeof (res as unknown as { then?: unknown }).then).toBe("undefined");
    expect(res.ok).toBe(true);
    db.close();
  });

  it("publishes at most once for a repeated idempotency key (no extra bindings)", () => {
    const db = freshV2();
    expect(
      publishCheckpoint(db, "agent:main:main", {
        ...base,
        activeBindings: [{ turnId: 1, disposition: "represented" }],
      }).ok
    ).toBe(true);
    const second = publishCheckpoint(db, "agent:main:main", {
      ...base,
      checkpointId: "cp-2",
      activeBindings: [{ turnId: 2, disposition: "represented" }],
    });
    expect(second.ok).toBe(true);
    if (second.ok) expect(second.published).toBe(false);
    expect(readActiveBindings(db, "agent:main:main")).toHaveLength(1);
    db.close();
  });

  it("writes no bindings when the compare-and-set fence is stale", () => {
    const db = freshV2();
    const res = publishCheckpoint(db, "agent:main:main", {
      ...base,
      expectedViewEpoch: "wrong",
      activeBindings: [{ turnId: 1, disposition: "represented" }],
    });
    expect(res.ok).toBe(false);
    expect(readActiveBindings(db, "agent:main:main")).toEqual([]);
    db.close();
  });

  it("reads bindings scoped to one view epoch", () => {
    const db = freshV2();
    publishCheckpoint(db, "agent:main:main", {
      ...base,
      activeBindings: [{ turnId: 1, disposition: "represented" }],
    });
    publishCheckpoint(db, "agent:main:main", {
      ...base,
      checkpointId: "cp-2",
      idempotencyKey: "idem-2",
      expectedViewEpoch: "cp-1",
      expectedLedgerRevision: 1,
      activeBindings: [{ turnId: 2, disposition: "verbatim" }],
    });
    expect(readBindingsForEpoch(db, "agent:main:main", "cp-1")).toHaveLength(1);
    expect(readBindingsForEpoch(db, "agent:main:main", "cp-2")).toHaveLength(1);
    db.close();
  });
});

describe("recordRebase / gcLedgerBindings (wave 4 item 2 audit + orphan GC)", () => {
  it("appends a structured rebase audit record on an external host revision change", () => {
    const db = freshV2();
    const res = recordRebase(db, {
      sessionKey: "agent:main:main",
      oldGen: "g0",
      newGen: "g1",
      reason: "host-transcript-rewrite",
      rebuildId: "rb-shadow-1",
      viewEpoch: "cp-2",
      ledgerRevision: 2,
      now: 10,
    });
    expect(res.ok).toBe(true);
    const row = db.prepare("SELECT note, audit_json FROM rebases LIMIT 1").get() as {
      note: string;
      audit_json: string;
    };
    expect(row.note).toBe("host-transcript-rewrite");
    const audit = JSON.parse(row.audit_json) as { rebuildId: string; viewEpoch: string };
    expect(audit.rebuildId).toBe("rb-shadow-1");
    expect(audit.viewEpoch).toBe("cp-2");
    db.close();
  });

  it("GCs orphan bindings for dropped epochs but keeps live ones and the coverage rows", () => {
    const db = freshV2();
    publishCheckpoint(db, "agent:main:main", {
      ...base,
      activeBindings: [{ turnId: 1, disposition: "represented" }],
    });
    publishCheckpoint(db, "agent:main:main", {
      ...base,
      checkpointId: "cp-2",
      idempotencyKey: "idem-2",
      expectedViewEpoch: "cp-1",
      expectedLedgerRevision: 1,
      activeBindings: [{ turnId: 2, disposition: "verbatim" }],
    });
    const gc = gcLedgerBindings(db, "agent:main:main", ["cp-2"], 20);
    expect(gc.ok).toBe(true);
    expect(gc.removed).toBe(1);
    expect(readBindingsForEpoch(db, "agent:main:main", "cp-1")).toHaveLength(0);
    expect(readBindingsForEpoch(db, "agent:main:main", "cp-2")).toHaveLength(1);
    // Immutable checkpoint/coverage rows are never GC'd.
    expect(readCheckpointEntries(db, "cp-1")).toHaveLength(2);
    db.close();
  });
});

describe("R11a active-binding epoch filter + R12 replay identity", () => {
  it("R11a: filters to the CURRENT valid epoch (obsolete epochs do not reinflate)", () => {
    const db = freshV2();
    publishCheckpoint(db, "agent:main:main", {
      ...base,
      activeBindings: [{ turnId: 1, disposition: "represented" }],
    });
    publishCheckpoint(db, "agent:main:main", {
      ...base,
      checkpointId: "cp-2",
      idempotencyKey: "idem-2",
      expectedViewEpoch: "cp-1",
      expectedLedgerRevision: 1,
      activeBindings: [{ turnId: 2, disposition: "verbatim" }],
    });
    // Both epochs exist in the table, but only the current one is active.
    expect(readBindingsForEpoch(db, "agent:main:main", "cp-1")).toHaveLength(1);
    expect(readBindingsForEpoch(db, "agent:main:main", "cp-2")).toHaveLength(1);
    const active = readActiveBindings(db, "agent:main:main");
    expect(active).toHaveLength(1);
    expect(active[0]?.turn_id).toBe(2);
    const state = readActiveBindingsState(db, "agent:main:main");
    expect(state.status).toBe("ok");
    if (state.status === "ok") expect(state.viewEpoch).toBe("cp-2");
    db.close();
  });

  it("R11a: distinguishes unavailable state from an empty active set", () => {
    const db = freshV2();
    const unavailable = readActiveBindingsState(db, "agent:main:main");
    expect(unavailable.status).toBe("unavailable");
    expect(readActiveBindings(db, "agent:main:main")).toEqual([]);
    db.close();
  });

  it("R12: an idempotent replay returns the historical checkpoint's own epoch/revision", () => {
    const db = freshV2();
    publishCheckpoint(db, "agent:main:main", {
      ...base,
      activeBindings: [{ turnId: 1, disposition: "represented" }],
    });
    publishCheckpoint(db, "agent:main:main", {
      ...base,
      checkpointId: "cp-2",
      idempotencyKey: "idem-2",
      expectedViewEpoch: "cp-1",
      expectedLedgerRevision: 1,
      activeBindings: [{ turnId: 2, disposition: "verbatim" }],
    });
    const replay = publishCheckpoint(db, "agent:main:main", {
      ...base,
      activeBindings: [{ turnId: 1, disposition: "represented" }],
    });
    expect(replay.ok).toBe(true);
    if (replay.ok) {
      expect(replay.published).toBe(false);
      expect(replay.reason).toBe("idempotent-replay");
      expect(replay.checkpointId).toBe("cp-1");
      // Internally consistent: cp-1's OWN epoch/revision, never the current cp-2/2.
      expect(replay.viewEpoch).toBe("cp-1");
      expect(replay.ledgerRevision).toBe(1);
    }
    db.close();
  });
});
