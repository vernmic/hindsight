// Proof suite: marks never leave half of a tool pair in the view.
// A drop that started on a lone toolResult absorbed the result before it but not that result's
// call, so the call was left without its result; turn_middle kept a boundary call whose result
// it redacted, and a boundary result whose call it redacted. Real temp host store, so the seq
// index resolves and the marks actually apply.
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { applyMarks, type LedgerMark } from "./assemble-marks.js";

const dirs: string[] = [];
afterEach(() => {
  for (const d of dirs.splice(0)) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* Windows handle lag */ }
  }
});

type Msg = Record<string, unknown>;
const SID = "sid-pairs";

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
const call = (id: string): Msg => ({
  role: "assistant",
  content: [{ type: "thinking", thinking: "reasoning" }, { type: "toolCall", id, name: "exec", arguments: { command: "x" } }],
  idempotencyKey: `c-${k++}`,
});
const result = (id: string): Msg => ({ role: "toolResult", toolCallId: id, content: [{ type: "text", text: "output" }], idempotencyKey: `r-${k++}` });
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
  return { calls: [...calls].filter((c) => !results.has(c)), results: [...results].filter((r) => !calls.has(r)) };
}

/** An already-applied mark: re-applied on every view, so admission policy does not gate it. */
function applied(seqStart: number, seqEnd: number, extra: Partial<Msg>): LedgerMark {
  return { mark_id: k++, session_id: SID, seq_start: seqStart, seq_end: seqEnd, kind: "turn", digest: null,
           reason: "test", applied_at: 1, pressure: 0.9, topic_dormancy_s: 10, stmt_age_s: 10,
           ...extra } as unknown as LedgerMark;
}

const ctx = (dbPath: string, key: string) =>
  ({ sessionKey: key, hostDbPath: dbPath, sessionId: SID, tokenBudget: 1e9, turnCounter: 500 });

describe("tool pairs stay closed", () => {
  it("a drop on a lone result also takes the call of the result it absorbs", () => {
    // seq 1 user, 2 call c1, 3 result c1, 4 call c2, 5 result c2, 6 final, then the tail
    const { dbPath, messages } = store([user("first"), call("c1"), result("c1"), call("c2"), result("c2"), final("done"), ...tail()]);
    const res = applyMarks(messages, [applied(5, 5, { action: "drop" })], ctx(dbPath, "pairs-absorb"));
    expect(res.changed).toBe(true);
    expect(orphans(res.messages)).toEqual({ calls: [], results: [] });
  });

  it("turn_middle starting on a call does not keep that call without its result", () => {
    const { dbPath, messages } = store([user("first"), call("c1"), result("c1"), call("c2"), result("c2"), final("done"), ...tail()]);
    const tm = applied(2, 6, { action: "digest", reason: "turn_middle: test" });
    const res = applyMarks(messages, [tm], ctx(dbPath, "pairs-tm-call"));
    expect(res.changed).toBe(true);
    expect(orphans(res.messages)).toEqual({ calls: [], results: [] });
    expect(JSON.stringify(res.messages)).toContain("trimmed with its result");
  });

  it("turn_middle ending on a result does not keep that result without its call", () => {
    const { dbPath, messages } = store([user("first"), final("start"), call("c1"), result("c1"), final("after"), ...tail()]);
    const tm = applied(2, 4, { action: "digest", reason: "turn_middle: test" });
    const res = applyMarks(messages, [tm], ctx(dbPath, "pairs-tm-result"));
    expect(orphans(res.messages)).toEqual({ calls: [], results: [] });
  });
});
