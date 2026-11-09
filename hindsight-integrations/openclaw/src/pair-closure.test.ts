// Proof suite: marks never leave half of a tool pair in the view (2026-11-08).
// Live finding on Fizban's session: an earlier-epoch drop that started on a lone toolResult
// absorbed the result before it but not that result's call, so 26 calls were left without
// results; fallbackCut then found no pair-closed boundary and the prune never reached pruneTo.
// turn_middle also kept a boundary call whose result it redacted. Real temp host store, so the
// seq index resolves and the marks actually apply.
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
const SID = "sid-pairs";

/** Builds a host store from message specs; returns the messages and the seq of each (1-based). */
function store(specs: Msg[]) {
  const dir = mkdtempSync(join(tmpdir(), "pair-closure-"));
  dirs.push(dir);
  const dbPath = join(dir, "host.sqlite");
  const db = new DatabaseSync(dbPath);
  db.exec("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT)");
  const ins = db.prepare("INSERT INTO transcript_events (session_id, seq, event_json) VALUES (?,?,?)");
  specs.forEach((m, i) => ins.run(SID, i + 1, JSON.stringify({ message: m })));
  db.close();
  return { dbPath, messages: specs };
}

let k = 0;
const user = (text: string): Msg => ({ role: "user", content: [{ type: "text", text }], idempotencyKey: `u-${k++}` });
const call = (id: string, n = 20): Msg => ({
  role: "assistant",
  content: [{ type: "thinking", thinking: "r".repeat(n) }, { type: "toolCall", id, name: "exec", arguments: { command: "x".repeat(n) } }],
  idempotencyKey: `c-${k++}`,
});
const result = (id: string, n = 20): Msg => ({ role: "toolResult", toolCallId: id, content: [{ type: "text", text: "o".repeat(n) }], idempotencyKey: `r-${k++}` });
const final = (text: string): Msg => ({ role: "assistant", content: [{ type: "text", text }], idempotencyKey: `f-${k++}` });

/** Eight light user turns: the preserved tail, so the marks under test sit outside it. */
function tail(): Msg[] {
  const out: Msg[] = [];
  for (let t = 1; t <= 8; t++) out.push(user(`tail ${t}`), final(`tail final ${t}`));
  return out;
}

function orphans(list: unknown[]) {
  const calls = new Set<string>();
  const results = new Set<string>();
  for (const m of list as Msg[]) {
    if (m.role === "assistant" && Array.isArray(m.content)) {
      for (const p of m.content as Msg[]) if (p.type === "toolCall") calls.add(String(p.id));
    }
    if (m.role === "toolResult") results.add(String(m.toolCallId));
  }
  return {
    calls: [...calls].filter((c) => !results.has(c)),
    results: [...results].filter((r) => !calls.has(r)),
  };
}

function mark(seqStart: number, seqEnd: number, extra: Partial<Msg> = {}): LedgerMark {
  return { mark_id: k++, session_id: SID, seq_start: seqStart, seq_end: seqEnd, kind: "tool_noise",
           action: "placeholder", digest: null, reason: "test", applied_at: 1, prune_epoch: 0,
           ...extra } as unknown as LedgerMark;
}

const ctx = (dbPath: string, key: string, tokenBudget: number, pruneEpoch: number) =>
  ({ sessionKey: key, hostDbPath: dbPath, sessionId: SID, tokenBudget, pruneEpoch });

describe("tool pairs stay closed", () => {
  it("an earlier-epoch drop on a lone result also takes the call of the result it absorbs", () => {
    // seq 1 user, 2 call c1, 3 result c1, 4 call c2, 5 result c2, 6 final, then the tail
    const { dbPath, messages } = store([user("first"), call("c1"), result("c1"), call("c2"), result("c2"), final("done"), ...tail()]);
    // applied in epoch 0, served in epoch 1: it drops its range outright
    const res = applyMarks(messages, [mark(5, 5)], ctx(dbPath, "pairs-absorb", 1e9, 1));
    expect(res.changed).toBe(true);
    expect(orphans(res.messages)).toEqual({ calls: [], results: [] });
    expect(JSON.stringify(res.messages)).not.toContain("\"c1\"");
  });

  it("turn_middle starting on a call does not keep that call without its result", () => {
    // seq 1 user, 2 call c1 (kept boundary), 3 result c1, 4 call c2, 5 result c2, 6 final
    const { dbPath, messages } = store([user("first"), call("c1"), result("c1"), call("c2"), result("c2"), final("done"), ...tail()]);
    const tm = mark(2, 6, { kind: "turn", action: "digest", reason: "turn_middle: test", prune_epoch: 1 });
    const res = applyMarks(messages, [tm], ctx(dbPath, "pairs-tm-call", 1e9, 1));
    expect(res.changed).toBe(true);
    expect(orphans(res.messages)).toEqual({ calls: [], results: [] });
    expect(JSON.stringify(res.messages)).toContain("trimmed with its result");
  });

  it("turn_middle ending on a result does not keep that result without its call", () => {
    // seq 2 final (kept boundary), 3 call c1, 4 result c1 (end): the call is interior
    const { dbPath, messages } = store([user("first"), final("start"), call("c1"), result("c1"), final("after"), ...tail()]);
    const tm = mark(2, 4, { kind: "turn", action: "digest", reason: "turn_middle: test", prune_epoch: 1 });
    const res = applyMarks(messages, [tm], ctx(dbPath, "pairs-tm-result", 1e9, 1));
    expect(orphans(res.messages)).toEqual({ calls: [], results: [] });
  });

  it("the oldest-turns cut still fires after a drop that started on a lone result", () => {
    // a heavy detour of call/result pairs, more heavy turns, then the light tail
    const specs: Msg[] = [user("first"), call("c1", 4000), result("c1", 4000), call("c2", 4000), result("c2", 4000), final("detour")];
    for (let t = 1; t <= 6; t++) specs.push(user(`old ${t}`), call(`o${t}`, 4000), result(`o${t}`, 4000), final(`old final ${t}`));
    specs.push(...tail());
    const { dbPath, messages } = store(specs);
    // The live shape: the mark is current-epoch (placeholder) in the steady view, and the prune
    // turns every applied mark into a drop before the oldest-turns cut runs.
    const marks = [mark(5, 5)];
    const steady = applyMarks(messages, marks, ctx(dbPath, "pairs-fallback", 1e9, 0));
    const budget = Math.ceil(steady.tokensAfter / 0.7);
    expect(estimateTokensHost(steady.messages)).toBe(steady.tokensAfter);
    const res = applyMarks(messages, marks, ctx(dbPath, "pairs-fallback", budget, 0));
    expect(res.prune).not.toBeNull();
    expect(res.prune!.fallback).not.toBeNull();
    expect(res.prune!.pressureAfter).toBeLessThanOrEqual(0.25);
    expect(orphans(res.messages)).toEqual({ calls: [], results: [] });
  });
});
