// Follow-up increment: forced admission over budget (item 4 half) and boundary
// slimming for turn_middle (item 3 reach). Same real-temp-host fixture as reach-cap.
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

function fixture(session: string, turns: number, big: number) {
  const dir = mkdtempSync(join(tmpdir(), "followup-"));
  dirs.push(dir);
  const dbPath = join(dir, "host.sqlite");
  const db = new DatabaseSync(dbPath);
  db.exec("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT)");
  const ins = db.prepare("INSERT INTO transcript_events (session_id, seq, event_json) VALUES (?,?,?)");
  const messages: Msg[] = [];
  let seq = 1;
  const push = (m: Msg) => {
    messages.push(m);
    ins.run(session, seq, JSON.stringify({ message: m }));
    seq += 1;
  };
  for (let t = 1; t <= turns; t++) {
    push({ role: "user", content: [{ type: "text", text: `user ${t}` }], idempotencyKey: `ik-u-${t}` });
    push({
      role: "assistant",
      content: [
        { type: "thinking", thinking: "r".repeat(big) },
        { type: "toolCall", id: `c${t}`, name: "exec", arguments: { command: "x".repeat(big) } },
      ],
      idempotencyKey: `ik-ac-${t}`,
    });
    push({ role: "toolResult", toolCallId: `c${t}`, content: [{ type: "text", text: "o".repeat(big) }], idempotencyKey: `ik-r-${t}` });
    push({ role: "assistant", content: [{ type: "text", text: `final ${t}` }], idempotencyKey: `ik-af-${t}` });
  }
  db.close();
  return { dbPath, messages };
}

function ctx(dbPath: string, sessionKey: string, tokenBudget: number) {
  return { sessionKey, hostDbPath: dbPath, sessionId: "sid-fu", tokenBudget, turnCounter: 500 };
}

const budgetFor = (messages: unknown[], pressure: number) =>
  Math.max(1, Math.ceil(estimateTokensHost(messages) / pressure));

function pendingMark(seqStart: number, seqEnd: number): LedgerMark {
  return { mark_id: 21, session_id: "sid-fu", seq_start: seqStart, seq_end: seqEnd, kind: "tool_noise",
           action: "placeholder", applied_at: null, pressure: 0.9, topic_dormancy_s: 10, stmt_age_s: 10 } as unknown as LedgerMark;
}

describe("forced admission over budget", () => {
  it("admits a pending mark when pressure is above 1", () => {
    const { dbPath, messages } = fixture("sid-fu", 12, 200);
    const res = applyMarks(messages, [pendingMark(7, 7)], ctx(dbPath, "fu-over", budgetFor(messages, 2)));
    expect(res.changed).toBe(true);
    expect(res.skipped.join(" ")).toContain("admission forced");
  });

  it("still holds a pending mark when pressure is under the floor", () => {
    const { dbPath, messages } = fixture("sid-fu", 12, 200);
    const res = applyMarks(messages, [pendingMark(7, 7)], ctx(dbPath, "fu-idle", budgetFor(messages, 0.05)));
    expect(res.changed).toBe(false);
    expect(res.skipped.join(" ")).toContain("admission held");
  });
});

describe("turn_middle boundary reach", () => {
  it("trims reasoning and call arguments on the boundaries it keeps", () => {
    const { dbPath, messages } = fixture("sid-fu", 12, 400);
    // Start the range on the assistant message so a kept boundary carries thinking.
    const mark = { mark_id: 31, session_id: "sid-fu", seq_start: 6, seq_end: 16, kind: "turn",
                   action: "digest", digest: "interior digested", applied_at: 1,
                   reason: "turn_middle: interior redactable" } as unknown as LedgerMark;
    const res = applyMarks(messages, [mark], ctx(dbPath, "fu-tm", budgetFor(messages, 0.5)));
    expect(res.changed).toBe(true);
    const text = JSON.stringify(res.messages);
    // The kept boundary is slimmed in place, and the host objects are never mutated.
    expect(text).toContain("Astinus: reasoning trimmed");
    expect(JSON.stringify(messages)).toContain("r".repeat(400));
    expect(res.tokensAfter).toBeLessThan(res.tokensBefore);
  });
});
