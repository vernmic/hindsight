import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { commitPruneIfNeeded, type PruneCommitInput } from "./context-engine.js";
import {
  closeAllLedgers,
  commitPrune,
  configureSessionLedger,
  insertCommittedTurn,
  ledgerPath,
  type TurnInsert,
} from "./session-ledger.js";

const warn = (_m: string): void => {};
let root = "";

const turn = (key: string, adv: string): TurnInsert => ({
  sessionKey: key,
  agentId: "test",
  hostDbPath: "C:\\nonexistent\\openclaw-agent.sqlite",
  sessionId: "sid-" + key,
  seqStart: 1,
  seqEnd: 3,
  generation: "gen-1",
  advancementKey: adv,
  heartbeat: false,
});

const input = (over: Partial<PruneCommitInput> = {}): PruneCommitInput => ({
  sessionKey: "agent:test:prune",
  epoch: 3,
  admitted: [],
  fallback: null,
  gen: "gen-1",
  tokensBefore: 1000,
  tokensAfter: 400,
  dropped: 5,
  ...over,
});

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), "astinus-prune-"));
  configureSessionLedger(root);
});

afterEach(() => {
  closeAllLedgers();
  try {
    rmSync(root, { recursive: true, force: true });
  } catch {
    /* best effort */
  }
});

describe("commitPruneIfNeeded", () => {
  it("does not write anything when there is nothing to commit", () => {
    const commitPruneSpy = vi.fn(() => true);
    const out = commitPruneIfNeeded({ commitPrune: commitPruneSpy }, input({ dropped: 0, fallback: null }));
    expect(out).toBe("skipped-nothing");
    expect(commitPruneSpy).not.toHaveBeenCalled();
  });

  it("commits when marks were dropped", () => {
    const commitPruneSpy = vi.fn(() => true);
    const out = commitPruneIfNeeded({ commitPrune: commitPruneSpy }, input({ dropped: 2 }));
    expect(out).toBe("committed");
    expect(commitPruneSpy).toHaveBeenCalledWith(
      "agent:test:prune",
      3,
      [],
      null,
      "gen-1",
      1000,
      400,
      2,
    );
  });

  it("commits when only the fallback cut exists", () => {
    const commitPruneSpy = vi.fn(() => true);
    const fallback = { seq_start: 1, seq_end: 9, action: "digest", reason: "prune_fallback" };
    const out = commitPruneIfNeeded({ commitPrune: commitPruneSpy }, input({ dropped: 0, fallback }));
    expect(out).toBe("committed");
    expect(commitPruneSpy).toHaveBeenCalledOnce();
  });

  it("reports a failed ledger write instead of pretending it committed", () => {
    const out = commitPruneIfNeeded({ commitPrune: () => false }, input());
    expect(out).toBe("failed");
  });
});

describe("commitPrune against a real ledger", () => {
  it("lands a prunes row and advances prune_epoch when dropped > 0", () => {
    const key = "agent:test:prune-real";
    expect(insertCommittedTurn(turn(key, "adv-p1"), warn).ok).toBe(true);
    const fallback = {
      session_id: "sid-" + key,
      seq_start: 1,
      seq_end: 3,
      kind: "turn",
      action: "digest",
      digest: "d",
      reason: "prune_fallback",
    };
    const out = commitPruneIfNeeded(
      { commitPrune },
      input({ sessionKey: key, epoch: 3, fallback, dropped: 4 }),
    );
    expect(out).toBe("committed");

    const db = new DatabaseSync(ledgerPath(key));
    const prunes = db.prepare("SELECT epoch, dropped, admitted FROM prunes").all() as Array<{
      epoch: number;
      dropped: number;
      admitted: number;
    }>;
    const meta = db.prepare("SELECT prune_epoch FROM meta LIMIT 1").get() as {
      prune_epoch: number | null;
    };
    const fallbackRow = db
      .prepare("SELECT seq_start, seq_end, prune_epoch FROM marks WHERE reason = 'prune_fallback'")
      .all() as Array<{ seq_start: number; seq_end: number; prune_epoch: number }>;
    db.close();

    expect(prunes).toHaveLength(1);
    expect(prunes[0]).toMatchObject({ epoch: 3, dropped: 4, admitted: 0 });
    expect(meta.prune_epoch).toBe(3);
    expect(fallbackRow).toHaveLength(1);
    expect(fallbackRow[0]).toMatchObject({ seq_start: 1, seq_end: 3, prune_epoch: 3 });
  });

  it("writes nothing for a dropped-0, no-fallback prune (the control)", () => {
    const key = "agent:test:prune-control";
    expect(insertCommittedTurn(turn(key, "adv-p2"), warn).ok).toBe(true);
    const out = commitPruneIfNeeded(
      { commitPrune },
      input({ sessionKey: key, epoch: 9, dropped: 0, fallback: null }),
    );
    expect(out).toBe("skipped-nothing");

    const db = new DatabaseSync(ledgerPath(key));
    const prunes = db.prepare("SELECT COUNT(*) AS n FROM prunes").get() as { n: number };
    const meta = db.prepare("SELECT prune_epoch FROM meta LIMIT 1").get() as {
      prune_epoch: number | null;
    };
    db.close();
    expect(prunes.n).toBe(0);
    expect(meta.prune_epoch ?? null).toBeNull();
  });
});
