import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import {
  closeAllLedgers,
  configureSessionLedger,
  insertCommittedTurn,
  ledgerPath,
  readMarksCached,
  type TurnInsert,
} from "./session-ledger.js";

const warn = (_m: string): void => {};
let root = "";

const turn = (key: string, adv: string, lo: number, hi: number): TurnInsert => ({
  sessionKey: key,
  agentId: "test",
  hostDbPath: "C:\\nonexistent\\openclaw-agent.sqlite",
  sessionId: "sid-" + key,
  seqStart: lo,
  seqEnd: hi,
  generation: "gen-1",
  advancementKey: adv,
  heartbeat: false,
});

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), "astinus-retire-"));
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

describe("legacy unanchored marks after the R36.1 gate was retired", () => {
  it("retires an old unanchored applied mark on reopen and leaves an anchored one live", () => {
    const key = "agent:test:retire1";
    expect(insertCommittedTurn(turn(key, "adv-r1", 1, 3), warn).ok).toBe(true);
    closeAllLedgers();

    const now = Date.now();
    const db = new DatabaseSync(ledgerPath(key));
    const ins = db.prepare(
      "INSERT INTO marks (session_id, seq_start, seq_end, kind, action, digest, reason, proposed_at, applied_at, anchor_first_event, anchor_last_event) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    );
    ins.run("sid-" + key, 4, 6, "turn", "digest", "d", "legacy-unanchored", now, now, null, null);
    ins.run("sid-" + key, 7, 9, "turn", "digest", "d", "anchored", now, now, "ev-7", "ev-9");
    db.close();

    // Reopening runs the idempotent migration, which retires the unanchored applied mark.
    expect(insertCommittedTurn(turn(key, "adv-r2", 10, 12), warn).ok).toBe(true);
    const read = readMarksCached(key, warn);
    expect(read.ok).toBe(true);
    const marks = read.marks as Array<{ seq_start: number; retired_at?: number | null }>;
    const legacy = marks.find((m) => m.seq_start === 4);
    const anchored = marks.find((m) => m.seq_start === 7);
    expect(legacy?.retired_at ?? null).not.toBeNull();
    expect(anchored?.retired_at ?? null).toBeNull();
  });
});
