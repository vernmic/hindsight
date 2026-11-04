// Ledger contract for the prune cycle (2026-11-03): commitPrune writes the epoch, stamps the
// admitted marks and inserts the fallback cut in one transaction, against the real ledger schema
// (its CHECK constraints included); the read path returns the epoch back.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import {
  closeAllLedgers,
  commitPrune,
  configureSessionLedger,
  insertCommittedTurn,
  ledgerPath,
  readMarksCached,
  readPruneEpochCached,
} from "./session-ledger.js";

const warn = (_m: string): void => {};
let root = "";
let n = 0;
const key = () => `agent:test:prune${++n}`;

function openWithOneMark(k: string): void {
  // a committed turn opens and caches the ledger, the same path production takes
  expect(
    insertCommittedTurn(
      { sessionKey: k, agentId: "test", hostDbPath: "C:\\none.sqlite", sessionId: "sid", seqStart: 1, seqEnd: 3,
        generation: "gen-1", advancementKey: `adv-${k}`, heartbeat: false },
      warn
    ).ok
  ).toBe(true);
  const db = new DatabaseSync(ledgerPath(k));
  db.prepare(
    "INSERT INTO marks (session_id, seq_start, seq_end, kind, action, digest, reason, proposed_at) VALUES (?,?,?,?,?,?,?,?)"
  ).run("sid", 5, 8, "tool_noise", "placeholder", null, "noise", 1);
  db.close();
}

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), "astinus-prune-"));
  configureSessionLedger(root);
});

afterEach(() => {
  closeAllLedgers();
  try { rmSync(root, { recursive: true, force: true }); } catch { /* best effort */ }
});

describe("commitPrune", () => {
  it("writes the epoch, stamps the admitted mark and inserts the fallback", () => {
    const k = key();
    openWithOneMark(k);
    expect(readPruneEpochCached(k)).toBe(0);
    const before = readMarksCached(k, warn).marks as Array<Record<string, unknown>>;
    expect(before).toHaveLength(1);
    expect(before[0].prune_epoch).toBeNull();
    const fallback = { session_id: "sid", seq_start: 2, seq_end: 4, kind: "turn", action: "digest",
                       digest: "1 earlier turn(s) removed at prune 1", reason: "prune_fallback" };
    expect(commitPrune(k, 1, [before[0].mark_id], fallback, "gen-1", 1000, 300, 4)).toBe(true);
    expect(readPruneEpochCached(k)).toBe(1);
    const after = readMarksCached(k, warn).marks as Array<Record<string, unknown>>;
    expect(after).toHaveLength(2);
    const stamped = after.find((m) => m.mark_id === before[0].mark_id)!;
    expect(stamped.prune_epoch).toBe(1);
    expect(stamped.applied_at).not.toBeNull();
    const fb = after.find((m) => m.reason === "prune_fallback")!;
    expect(fb.prune_epoch).toBe(1);
    expect(fb.applied_at).not.toBeNull();
    expect(fb.digest).toContain("removed at prune 1");
    // the prune's own record: the report accounts for epoch drops from it (Astinus #6340)
    const db = new DatabaseSync(ledgerPath(k), { readOnly: true });
    const prunes = db.prepare("SELECT * FROM prunes").all() as Array<Record<string, unknown>>;
    db.close();
    expect(prunes).toHaveLength(1);
    expect(prunes[0]).toMatchObject({ epoch: 1, tokens_before: 1000, tokens_after: 300, admitted: 1, dropped: 4, fallback_mark_id: fb.mark_id });
  });

  it("is all or nothing: a rejected fallback leaves the epoch and the marks as they were", () => {
    const k = key();
    openWithOneMark(k);
    const before = readMarksCached(k, warn).marks as Array<Record<string, unknown>>;
    // the marks table CHECK only admits digest/drop/placeholder
    const bad = { session_id: "sid", seq_start: 2, seq_end: 4, kind: "turn", action: "bogus", reason: "prune_fallback" };
    expect(commitPrune(k, 1, [before[0].mark_id], bad, "gen-1", 1000, 300)).toBe(false);
    expect(readPruneEpochCached(k)).toBe(0);
    const after = readMarksCached(k, warn).marks as Array<Record<string, unknown>>;
    expect(after).toHaveLength(1);
    expect(after[0].prune_epoch).toBeNull();
    expect(after[0].applied_at).toBeNull();
    const db = new DatabaseSync(ledgerPath(k), { readOnly: true });
    expect(db.prepare("SELECT COUNT(*) AS n FROM prunes").get()).toEqual({ n: 0 });
    db.close();
  });
});
