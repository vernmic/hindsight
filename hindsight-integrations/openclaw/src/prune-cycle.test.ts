// Proof suite: the prune cycle (Vern 2026-11-03; suits/astinus/analysis/PRUNE-CYCLE-DESIGN-20261103.md).
// Real temp host store, so the seq index resolves and marks actually apply. The cache claims are
// tested as bytes: between prunes the view must be identical whether the host carries it forward
// or rebuilds it from the transcript.
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { applyMarks, estimateTokensHost, type LedgerMark } from "./assemble-marks.js";

const dirs: string[] = [];
afterEach(() => {
  for (const d of dirs.splice(0)) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* Windows handle lag */ }
  }
});

type Msg = Record<string, unknown>;
const SID = "sid-pc";

/** Turn t = user, assistant(thinking + call), toolResult, assistant final: seqs 4t-3 .. 4t. */
function fixture(turns: number, size: (t: number) => number) {
  const dir = mkdtempSync(join(tmpdir(), "prune-cycle-"));
  dirs.push(dir);
  const dbPath = join(dir, "host.sqlite");
  const db = new DatabaseSync(dbPath);
  db.exec("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT)");
  const ins = db.prepare("INSERT INTO transcript_events (session_id, seq, event_json) VALUES (?,?,?)");
  const messages: Msg[] = [];
  let seq = 1;
  const push = (m: Msg) => {
    messages.push(m);
    ins.run(SID, seq, JSON.stringify({ message: m }));
    seq += 1;
  };
  for (let t = 1; t <= turns; t++) {
    const n = size(t);
    push({ role: "user", content: [{ type: "text", text: `user ${t}` }], idempotencyKey: `ik-u-${t}`, timestamp: 1000 + t });
    push({
      role: "assistant",
      content: [
        { type: "thinking", thinking: "r".repeat(n) },
        { type: "toolCall", id: `c${t}`, name: "exec", arguments: { command: "x".repeat(n) } },
      ],
      idempotencyKey: `ik-ac-${t}`,
      timestamp: 2000 + t,
    });
    push({ role: "toolResult", toolCallId: `c${t}`, content: [{ type: "text", text: "o".repeat(n) }], idempotencyKey: `ik-r-${t}`, timestamp: 3000 + t });
    push({ role: "assistant", content: [{ type: "text", text: `final ${t}` }], idempotencyKey: `ik-af-${t}`, timestamp: 4000 + t });
  }
  db.close();
  return { dbPath, messages };
}

const budgetAt = (messages: unknown[], pressure: number) => Math.max(1, Math.ceil(estimateTokensHost(messages) / pressure));

function ctx(dbPath: string, key: string, tokenBudget: number, pruneEpoch = 0) {
  return { sessionKey: key, hostDbPath: dbPath, sessionId: SID, tokenBudget, pruneEpoch };
}

let nextId = 1;
/** A pending digest over one whole turn t (seqs 4t-3 .. 4t). */
function digestTurn(t: number, text = `turn ${t} was a detour`): LedgerMark {
  return { mark_id: nextId++, session_id: SID, seq_start: 4 * t - 3, seq_end: 4 * t, kind: "turn",
           action: "digest", digest: text, reason: "off-topic", applied_at: null } as unknown as LedgerMark;
}

/** What the caller's commit does to the ledger rows: admitted marks get the epoch, the fallback is added. */
function committed(marks: LedgerMark[], res: ReturnType<typeof applyMarks>): LedgerMark[] {
  const epoch = res.prune!.epoch;
  const admitted = new Set(res.admitted.map((m) => m.mark_id));
  const rows = marks.map((m) => (admitted.has(m.mark_id) ? { ...m, applied_at: 5, prune_epoch: epoch } : m));
  if (res.prune!.fallback) rows.push({ ...res.prune!.fallback, mark_id: nextId++, applied_at: 5 });
  return rows;
}

const bytes = (list: unknown[]) => JSON.stringify(list);

describe("prune cycle", () => {
  it("holds pending marks below pruneAt and leaves the view untouched", () => {
    const { dbPath, messages } = fixture(14, () => 400);
    const res = applyMarks(messages, [digestTurn(3)], ctx(dbPath, "pc-hold", budgetAt(messages, 0.5)));
    expect(res.prune).toBeNull();
    expect(res.admitted).toEqual([]);
    expect(res.messages).toBe(messages);
    expect(res.skipped.join(" ")).toContain("prune held");
  });

  it("at pruneAt admits every pending mark at once and reaches pruneTo", () => {
    const { dbPath, messages } = fixture(14, (t) => (t <= 6 ? 4000 : 200));
    const marks = [digestTurn(2), digestTurn(4), digestTurn(5)];
    const res = applyMarks(messages, marks, ctx(dbPath, "pc-prune", budgetAt(messages, 0.65)));
    expect(res.prune).not.toBeNull();
    expect(res.prune!.epoch).toBe(1);
    expect(res.admitted.map((m) => m.mark_id).sort()).toEqual(marks.map((m) => m.mark_id).sort());
    expect(res.prune!.pressureAfter).toBeLessThanOrEqual(0.25);
    // first user message and the preserved tail (turns 7-14) survive as the host objects
    expect(res.messages[0]).toBe(messages[0]);
    const tail = messages.slice(24);
    expect(res.messages.slice(res.messages.length - tail.length)).toEqual(tail);
  });

  it("between prunes the view is byte-identical, rebuilt or carried forward", () => {
    const { dbPath, messages } = fixture(14, (t) => (t <= 6 ? 4000 : 200));
    const marks = [digestTurn(2), digestTurn(4)];
    const pruned = applyMarks(messages, marks, ctx(dbPath, "pc-stable", budgetAt(messages, 0.65)));
    expect(pruned.prune).not.toBeNull();
    const rows = committed(marks, pruned);
    const budget = budgetAt(messages, 0.65);
    // rebuilt: the host hands the raw transcript again
    const rebuilt = applyMarks(messages, rows, ctx(dbPath, "pc-stable", budget, 1));
    expect(rebuilt.prune).toBeNull();
    expect(bytes(rebuilt.messages)).toBe(bytes(pruned.messages));
    // carried: the host hands back the view it was given
    const carried = applyMarks(pruned.messages, rows, ctx(dbPath, "pc-stable", budget, 1));
    expect(carried.prune).toBeNull();
    expect(bytes(carried.messages)).toBe(bytes(pruned.messages));
  });

  it("a legacy APPLIED empty digest is inert before the prune and dropped by it (Codex N3)", () => {
    // turn 10 (seq 37-40) is outside the 8-turn tail of 20 and the only heavy turn, so dropping
    // it alone reaches pruneTo and no fallback cut can hide a missed drop
    const { dbPath, messages } = fixture(20, (t) => (t === 10 ? 20000 : 50));
    const legacy = { mark_id: nextId++, session_id: SID, seq_start: 37, seq_end: 40, kind: "turn", action: "digest",
                     digest: "", reason: "legacy", applied_at: 1 } as unknown as LedgerMark;
    const idle = applyMarks(messages, [legacy], ctx(dbPath, "pc-n3", budgetAt(messages, 0.1)));
    expect(idle.messages).toBe(messages); // item 1: an empty digest is not in effect
    const budget = budgetAt(messages, 0.65);
    const pruned = applyMarks(messages, [legacy], ctx(dbPath, "pc-n3", budget));
    expect(pruned.prune).not.toBeNull();
    expect(pruned.prune!.dropped).toBe(1);
    expect(pruned.prune!.fallback).toBeNull();
    expect(bytes(pruned.messages)).not.toContain("final 10");
    const rebuilt = applyMarks(messages, committed([legacy], pruned), ctx(dbPath, "pc-n3", budget, 1));
    expect(rebuilt.prune).toBeNull();
    expect(bytes(rebuilt.messages)).toBe(bytes(pruned.messages));
  });

  it("marks applied before the first prune (epoch 0) are dropped, and stay dropped on a rebuilt view", () => {
    // only turns 2 and 3 are heavy, so the marks reach pruneTo alone and no fallback cut can
    // hide what happens to the epoch-0 digest
    const { dbPath, messages } = fixture(14, (t) => (t === 2 || t === 3 ? 8000 : 50));
    const old = { ...digestTurn(3, "EPOCH-ZERO-DIGEST"), applied_at: 1 } as LedgerMark;
    const marks = [old, digestTurn(2)];
    const before = applyMarks(messages, marks, ctx(dbPath, "pc-epoch0", budgetAt(messages, 0.1)));
    expect(JSON.stringify(before.messages)).toContain("EPOCH-ZERO-DIGEST");
    // pressure is measured on the steady view, which already carries the epoch-0 digest
    const budget = budgetAt(before.messages, 0.65);
    const pruned = applyMarks(messages, marks, ctx(dbPath, "pc-epoch0", budget));
    expect(pruned.prune).not.toBeNull();
    expect(pruned.prune!.fallback).toBeNull();
    expect(JSON.stringify(pruned.messages)).not.toContain("EPOCH-ZERO-DIGEST");
    const rebuilt = applyMarks(messages, committed(marks, pruned), ctx(dbPath, "pc-epoch0", budget, 1));
    expect(rebuilt.prune).toBeNull();
    expect(JSON.stringify(rebuilt.messages)).toBe(JSON.stringify(pruned.messages));
  });

  it("the next prune removes everything the previous one left", () => {
    const { dbPath, messages } = fixture(20, (t) => (t <= 12 ? 3000 : 200));
    const first = [digestTurn(2, "FIRST-EPOCH-DIGEST")];
    const p1 = applyMarks(messages, first, ctx(dbPath, "pc-next", budgetAt(messages, 0.65)));
    expect(p1.prune).not.toBeNull();
    const rows = committed(first, p1);
    // a later pending mark, and pressure back over the threshold
    rows.push(digestTurn(10, "SECOND-EPOCH-DIGEST"));
    const p2 = applyMarks(p1.messages, rows, ctx(dbPath, "pc-next", budgetAt(p1.messages, 0.7), 1));
    expect(p2.prune).not.toBeNull();
    expect(p2.prune!.epoch).toBe(2);
    const text = bytes(p2.messages);
    expect(text).not.toContain("FIRST-EPOCH-DIGEST");
    if (p1.prune!.fallback) expect(text).not.toContain("removed at prune 1");
  });

  it("cuts the oldest whole turns only when the marks are not enough", () => {
    const { dbPath, messages } = fixture(16, (t) => (t <= 8 ? 4000 : 200));
    const res = applyMarks(messages, [], ctx(dbPath, "pc-fallback", budgetAt(messages, 0.7)));
    expect(res.prune).not.toBeNull();
    const fb = res.prune!.fallback!;
    expect(fb).not.toBeNull();
    expect(fb.reason).toBe("prune_fallback");
    expect(fb.prune_epoch).toBe(1);
    // the cut starts right after the first user message and takes user messages with it
    expect(fb.seq_start).toBe(2);
    const text = bytes(res.messages);
    expect(text).toContain("user 1");
    expect(text).not.toContain("\"user 2\"");
    expect(text).toContain("removed at prune 1");
    // no orphaned tool result
    const calls = new Set<string>();
    for (const m of res.messages as Msg[]) {
      if (m.role === "assistant" && Array.isArray(m.content)) {
        for (const p of m.content as Msg[]) if (p.type === "toolCall") calls.add(String(p.id));
      }
      if (m.role === "toolResult") expect(calls.has(String(m.toolCallId))).toBe(true);
    }
  });

  it("does not commit a prune that would remove under 5%", () => {
    // everything heavy sits in the preserved tail: nothing outside it is worth a cache miss
    const { dbPath, messages } = fixture(10, (t) => (t >= 3 ? 4000 : 10));
    const res = applyMarks(messages, [], ctx(dbPath, "pc-gain", budgetAt(messages, 0.9)));
    expect(res.prune).toBeNull();
    expect(res.skipped.join(" ")).toContain("prune skipped");
    expect(res.messages).toBe(messages);
  });

  it("never touches the preserved tail", () => {
    const { dbPath, messages } = fixture(14, () => 400);
    const res = applyMarks(messages, [digestTurn(13)], ctx(dbPath, "pc-tail", budgetAt(messages, 0.65)));
    expect(bytes(res.messages)).toContain("user 13");
    expect(bytes(res.messages)).toContain("final 13");
  });
});
