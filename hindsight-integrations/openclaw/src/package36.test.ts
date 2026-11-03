import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { applyMarks, ensureMarkAnchorColumns, SeqIndex, type LedgerMark } from "./assemble-marks.js";
import {
  closeAllLedgers,
  configureSessionLedger,
  openSessionLedger,
  planMarkRebase,
  readMarksCached,
  runMarkRebaseCached,
} from "./session-ledger.js";
import { shouldQueueRetain } from "./retain-scope.js";

// Package 36: mark anchors, the one-time rebase, the seq-index reload (prod 667d86349,
// 7ace0c648, 40e6a16be, 7ee8730d8, 82a3d61d0) and retain scope (prod 484e299d6).

const dirs: string[] = [];
afterEach(() => {
  closeAllLedgers();
  for (const d of dirs.splice(0)) {
    try {
      rmSync(d, { recursive: true, force: true });
    } catch {
      /* Windows may hold a handle briefly */
    }
  }
});

const msg = (role: string, turn: number) => ({
  role,
  content: [{ type: "text", text: `${role} ${turn}` }],
  idempotencyKey: `ik-${role}-${turn}`,
});

/** Host store with event ids; turns [from..to] at seq 1.. (2 rows per turn). */
function host(from = 1, to = 12) {
  const dir = mkdtempSync(join(tmpdir(), "p36-"));
  dirs.push(dir);
  const dbPath = join(dir, "host.sqlite");
  const db = new DatabaseSync(dbPath);
  db.exec("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT)");
  write(db, from, to);
  db.close();
  return { dir, dbPath };
}
function write(db: DatabaseSync, from: number, to: number, startSeq = 1) {
  const ins = db.prepare("INSERT INTO transcript_events VALUES (?,?,?)");
  let seq = startSeq;
  for (let t = from; t <= to; t++)
    for (const role of ["user", "assistant"])
      ins.run("sid-1", seq++, JSON.stringify({ id: `ev-${role}-${t}`, message: msg(role, t) }));
}
/** Simulate a manual trim: drop turn 1, renumber the survivors from 1 (ids kept). */
function trimFirstTurn(dbPath: string, appendTurn?: number) {
  const db = new DatabaseSync(dbPath);
  db.exec("DELETE FROM transcript_events");
  write(db, 2, 12);
  if (appendTurn) write(db, appendTurn, appendTurn, 23);
  db.close();
}
const view = (from: number, to: number) => {
  const out: unknown[] = [];
  for (let t = from; t <= to; t++) out.push(msg("user", t), msg("assistant", t));
  return out;
};
const ctx = (dbPath: string, key: string) => ({
  sessionKey: key,
  hostDbPath: dbPath,
  sessionId: "sid-1",
  tokenBudget: 1,
  turnCounter: 100,
});
const applied = (lo: number, hi: number, extra: Partial<LedgerMark> = {}): LedgerMark =>
  ({ mark_id: lo, seq_start: lo, seq_end: hi, action: "digest", digest: "DIGEST", applied_at: 1, ...extra }) as LedgerMark;
const texts = (list: unknown[]) => JSON.stringify(list);

describe("B10 seq-index reload", () => {
  it("reloads when a trim renumbers the tail, so survivors and new rows map", () => {
    const { dbPath } = host();
    const ix = new SeqIndex(dbPath, "sid-1");
    ix.refresh();
    expect(ix.seqOf(msg("user", 4))).toBe(7);
    trimFirstTurn(dbPath, 13);
    ix.refresh();
    expect(ix.seqOf(msg("user", 4))).toBe(5); // stale index said 7
    expect(ix.seqOf(msg("user", 13))).toBe(23); // stale index never read it
    ix.close();
  });
});

describe("B9 anchors", () => {
  it("an anchored mark follows its content across a renumber; unanchored keeps old numbers", () => {
    const { dbPath } = host();
    trimFirstTurn(dbPath); // turn 4 moved from seq 7-8 to 5-6
    const anchored = applied(7, 8, { anchor_first_event: "ev-user-4", anchor_last_event: "ev-assistant-4" });
    const res = applyMarks(view(2, 12), [anchored], ctx(dbPath, "p36-a"));
    expect(texts(res.messages)).not.toContain("user 4");
    expect(texts(res.messages)).toContain("user 3");
  });

  it("skips a mark whose anchor is gone instead of re-aiming it, and never applies retired marks", () => {
    const { dbPath } = host();
    const gone = applied(3, 4, { anchor_first_event: "ev-user-99", anchor_last_event: "ev-assistant-99" });
    const retired = applied(5, 6, { retired_at: 1 });
    const res = applyMarks(view(1, 12), [gone, retired], ctx(dbPath, "p36-b"));
    expect(res.changed).toBeFalsy();
    expect(res.skipped.join("\n")).toContain("anchor gone");
    expect(texts(res.messages)).toContain("user 3");
  });

  it("migrates an old ledger in place and runs the one-time rebase once", () => {
    const { dir, dbPath } = host();
    configureSessionLedger(dir);
    const key = "agent:main:p36";
    const opened = openSessionLedger(key, "main", dbPath, () => {});
    expect(opened.ok).toBe(true);
    const db = (opened as { db: DatabaseSync }).db;
    // Simulate a pre-anchor ledger: drop the new columns' values by reinserting plain rows.
    const ins = db.prepare(
      "INSERT INTO marks(session_id,seq_start,seq_end,kind,action,digest,reason,proposed_at,applied_gen,applied_at) VALUES('sid-1',?,?,'turn','digest','d','r',1,?,?)"
    );
    ins.run(3, 4, "gen-now", 1); // backfill
    ins.run(5, 6, "gen-old", 1); // retire
    ins.run(7, 8, null, null); // remove (pending, unanchored)
    ins.run(40, 41, "gen-now", 1); // backfill impossible (no rows) -> retire
    ensureMarkAnchorColumns(db); // idempotent on a migrated ledger
    const warns: string[] = [];
    expect(runMarkRebaseCached(key, dbPath, "sid-1", "gen-now", (m) => warns.push(m))).toEqual({
      backfill: 1,
      retire: 2,
      remove: 1,
    });
    expect(runMarkRebaseCached(key, dbPath, "sid-1", "gen-now", () => {})).toBeNull(); // once
    const marks = readMarksCached(key, () => {}).marks as LedgerMark[];
    expect(marks.map((m) => [m.seq_start, m.anchor_first_event ?? null, m.retired_at != null])).toEqual([
      [3, "ev-user-2", false],
      [5, null, true],
      [40, null, true],
    ]);
  });

  it("plans: anchored marks untouched; no current generation retires every applied mark", () => {
    const plan = planMarkRebase(
      [
        { mark_id: 1, applied_at: 1, applied_gen: "g", anchor_first_event: "a", anchor_last_event: "b" },
        { mark_id: 2, applied_at: 1, applied_gen: "g" },
      ],
      ""
    );
    expect(plan).toEqual({ backfill: [], retire: [2], remove: [] });
  });
});

describe("B4 retain scope", () => {
  it("queues conversational sessions only, in both key forms", () => {
    expect(shouldQueueRetain("agent:main:telegram:group:-100:topic:22")).toBe(true);
    expect(shouldQueueRetain("agent:main:telegram:group:-100:topic:1201")).toBe(true);
    for (const k of [
      "agent:astinus:cron:cd29bb61:run:0d8e5b5c",
      "agent_astinus_cron_cd29bb61_run_0d8e5b5c",
      "agent:db:subagent:8cdb0690",
      "agent_gardener_main_heartbeat",
      "agent_main_dashboard_x",
      "  ",
      undefined,
    ])
      expect(shouldQueueRetain(k)).toBe(false);
  });
});