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

describe("re-application is idempotent (a carried-forward view keeps its bytes)", () => {
  it("a placeholder already in place is left as it is", () => {
    const { dbPath, messages } = fixture("sid-rc", 12, () => 2000);
    const c = ctx(dbPath, "rc-idem-ph", budgetFor(messages, 0.5));
    const once = applyMarks(messages, [placeholderMark(7, 7)], c);
    const twice = applyMarks(once.messages, [placeholderMark(7, 7)], c);
    expect(JSON.stringify(twice.messages)).toBe(JSON.stringify(once.messages));
  });

  it("a turn_middle redaction already in place is left as it is", () => {
    const { dbPath, messages } = fixture("sid-rc", 12, () => 2000);
    const c = ctx(dbPath, "rc-idem-tm", budgetFor(messages, 0.5));
    const tm = { mark_id: 9, session_id: "sid-rc", seq_start: 5, seq_end: 8, kind: "turn", action: "digest",
                 digest: null, reason: "turn_middle: test", applied_at: 1 } as unknown as LedgerMark;
    const once = applyMarks(messages, [tm], c);
    expect(JSON.stringify(once.messages)).toContain("turn interior redacted");
    const twice = applyMarks(once.messages, [tm], c);
    expect(JSON.stringify(twice.messages)).toBe(JSON.stringify(once.messages));
  });
});
