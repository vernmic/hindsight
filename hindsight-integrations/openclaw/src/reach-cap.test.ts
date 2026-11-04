// Proof suite: placeholder reach (item 3) and the hard cap (item 4), 2026-11-03.
// Same real-temp-host-store fixture as turn-middle.test.ts, so the seq index resolves.
// Messages use the host's block shapes: {type:"thinking", thinking} and
// {type:"toolCall", id, name, arguments} (packages/ai openai-completions-messages.ts).
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

/** Turn t = user, assistant(thinking + call), toolResult, assistant final; 4 seqs per turn.
 *  `big(t)` sizes the turn's reasoning, arguments and result. */
function fixture(session: string, turns: number, big: (t: number) => number, extra?: (t: number, push: (m: Msg, mapped?: boolean) => void) => void) {
  const dir = mkdtempSync(join(tmpdir(), "reach-cap-"));
  dirs.push(dir);
  const dbPath = join(dir, "host.sqlite");
  const db = new DatabaseSync(dbPath);
  db.exec("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT)");
  const ins = db.prepare("INSERT INTO transcript_events (session_id, seq, event_json) VALUES (?,?,?)");
  const messages: Msg[] = [];
  let seq = 1;
  const push = (m: Msg, mapped = true) => {
    messages.push(m);
    if (mapped) {
      ins.run(session, seq, JSON.stringify({ message: m }));
      seq += 1;
    }
  };
  for (let t = 1; t <= turns; t++) {
    const n = big(t);
    push({ role: "user", content: [{ type: "text", text: `user ${t}` }], idempotencyKey: `ik-u-${t}` });
    push({
      role: "assistant",
      content: [
        { type: "thinking", thinking: "r".repeat(n) },
        { type: "toolCall", id: `c${t}`, name: "exec", arguments: { command: "x".repeat(n) } },
      ],
      idempotencyKey: `ik-ac-${t}`,
    });
    push({ role: "toolResult", toolCallId: `c${t}`, content: [{ type: "text", text: "o".repeat(n) }], idempotencyKey: `ik-r-${t}` });
    push({ role: "assistant", content: [{ type: "text", text: `final ${t}` }], idempotencyKey: `ik-af-${t}` });
    extra?.(t, push);
  }
  db.close();
  return { dbPath, messages };
}

function ctx(dbPath: string, sessionKey: string, tokenBudget: number, policy?: Record<string, number>) {
  return { sessionKey, hostDbPath: dbPath, sessionId: "sid-rc", tokenBudget, turnCounter: 500, ...(policy ? { policy } : {}) };
}

const budgetFor = (messages: unknown[], pressure: number) => Math.max(1, Math.ceil(estimateTokensHost(messages) / pressure));

function placeholderMark(seqStart: number, seqEnd: number): LedgerMark {
  return { mark_id: 7, session_id: "sid-rc", seq_start: seqStart, seq_end: seqEnd, kind: "tool_noise",
           action: "placeholder", applied_at: 1, pressure: 0.5, topic_dormancy_s: 10, stmt_age_s: 10 } as unknown as LedgerMark;
}

describe("placeholder reach (item 3)", () => {
  it("drops reasoning and stubs large arguments on the call side, keeping the pair", () => {
    const { dbPath, messages } = fixture("sid-rc", 12, () => 2000);
    // turn 2 = seq 5-8 (indices 4-7); the mark names only the result, the pair pulls in the call.
    const res = applyMarks(messages, [placeholderMark(7, 7)], ctx(dbPath, "rc-reach", budgetFor(messages, 0.5)));
    expect(res.changed).toBe(true);
    const call = res.messages[5] as Msg;
    const parts = call.content as Msg[];
    expect(parts.some((p) => p.type === "thinking")).toBe(false);
    const tc = parts.find((p) => p.type === "toolCall") as Msg;
    expect(tc.id).toBe("c2");
    expect(tc.name).toBe("exec");
    expect(JSON.stringify(tc.arguments)).toContain("arguments pruned by Astinus");
    expect(call.idempotencyKey).toBe("ik-ac-2");
    expect(JSON.stringify((res.messages[6] as Msg).content)).toContain("tool output pruned by Astinus");
    // outside the range: untouched objects
    expect(res.messages[7]).toBe(messages[7]);
    expect(res.messages[9]).toBe(messages[9]);
    // the host objects were replaced, never mutated
    expect(((messages[5] as Msg).content as Msg[]).some((p) => p.type === "thinking")).toBe(true);
    expect(res.tokensAfter).toBeLessThan(res.tokensBefore);
  });

  it("keeps small arguments verbatim", () => {
    const { dbPath, messages } = fixture("sid-rc", 12, () => 50);
    const res = applyMarks(messages, [placeholderMark(7, 7)], ctx(dbPath, "rc-small", budgetFor(messages, 0.5)));
    const tc = ((res.messages[5] as Msg).content as Msg[]).find((p) => p.type === "toolCall") as Msg;
    expect(tc.arguments).toEqual({ command: "x".repeat(50) });
    expect(((res.messages[5] as Msg).content as Msg[]).some((p) => p.type === "thinking")).toBe(false);
  });
});

describe("hard cap (item 4)", () => {
  const big = (t: number) => (t <= 4 ? 20000 : 500);

  it("does nothing below the cap", () => {
    const { dbPath, messages } = fixture("sid-rc", 12, big);
    const res = applyMarks(messages, [], ctx(dbPath, "rc-below", budgetFor(messages, 0.85)));
    expect(res.hardCap).toBeNull();
    expect(res.messages).toBe(messages);
  });

  it("reduces the oldest turns until the target, keeping every user message and the tail", () => {
    const { dbPath, messages } = fixture("sid-rc", 12, big);
    const res = applyMarks(messages, [], ctx(dbPath, "rc-over", budgetFor(messages, 1.2)));
    expect(res.hardCap).not.toBeNull();
    expect(res.hardCap!.turns).toBeGreaterThan(0);
    expect(res.hardCap!.pressureAfter).toBeLessThanOrEqual(0.7);
    const users = (res.messages as Msg[]).filter((m) => m.role === "user").map((m) => m.idempotencyKey);
    expect(users).toEqual(Array.from({ length: 12 }, (_, i) => `ik-u-${i + 1}`));
    // turn 1's interior is the first to go
    expect(JSON.stringify((res.messages[1] as Msg).content)).toContain("Astinus hard cap");
    expect(JSON.stringify((res.messages[1] as Msg).content)).toContain("seq 2-4");
    // the last 8 user turns (5-12) are byte-for-byte the host objects
    const tail = messages.slice(16);
    expect(res.messages.slice(res.messages.length - tail.length)).toEqual(tail);
    // no orphaned tool result anywhere
    const callIds = new Set<string>();
    for (const m of res.messages as Msg[]) {
      if (m.role === "assistant" && Array.isArray(m.content)) {
        for (const p of m.content as Msg[]) if (p.type === "toolCall") callIds.add(String(p.id));
      }
      if (m.role === "toolResult") expect(callIds.has(String(m.toolCallId))).toBe(true);
    }
  });

  it("is off when hardCapPressure is 0", () => {
    const { dbPath, messages } = fixture("sid-rc", 12, big);
    const res = applyMarks(messages, [], ctx(dbPath, "rc-off", budgetFor(messages, 1.2), { hardCapPressure: 0 }));
    expect(res.hardCap).toBeNull();
    expect(res.messages).toBe(messages);
  });

  it("skips a turn holding unmapped host content and a turn whose pair crosses its edge", () => {
    const { dbPath, messages } = fixture("sid-rc", 12, big, (t, push) => {
      // turn 1: an unmapped host message (a compaction-summary stand-in)
      if (t === 1) push({ role: "assistant", content: [{ type: "text", text: "summary of earlier work" }] }, false);
      // turn 2: a call whose result lands after the next user message
      if (t === 2) push({ role: "assistant", content: [{ type: "toolCall", id: "cross", name: "exec", arguments: {} }], idempotencyKey: "ik-cross-a" });
      if (t === 3) push({ role: "toolResult", toolCallId: "cross", content: [{ type: "text", text: "late" }], idempotencyKey: "ik-cross-r" });
    });
    const res = applyMarks(messages, [], ctx(dbPath, "rc-skip", budgetFor(messages, 1.2)));
    expect(res.hardCap).not.toBeNull();
    const text = JSON.stringify(res.messages);
    expect(text).toContain("summary of earlier work");
    expect(text).toContain("\"id\":\"cross\"");
    expect(text).toContain("\"toolCallId\":\"cross\"");
    // turn 4 (no edge problems) was reduced instead
    expect(text).toContain("Astinus hard cap");
  });

  it("scales pressure by the count anchor", () => {
    const { dbPath, messages } = fixture("sid-rc", 12, big);
    // estimate says 1.2, but the provider sees half of it: no cap
    const res = applyMarks(messages, [], { ...ctx(dbPath, "rc-scale", budgetFor(messages, 1.2)), pressureScale: 0.5 });
    expect(res.hardCap).toBeNull();
  });
});
