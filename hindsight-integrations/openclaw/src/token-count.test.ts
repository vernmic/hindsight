import { describe, expect, it } from "vitest";
import { classifyMessage, countMarkRemoval, countPromptTokens, countText } from "./token-count.js";

// Deterministic stand-in for the tokenizer: 4 characters per token, rounded up.
const enc = (s: string) => Math.ceil(s.length / 4);

describe("token-count", () => {
  it("counts through the injected encoder and returns zero for empty text", () => {
    expect(countText("abcdefgh", { encoder: enc })).toBe(2);
    expect(countText("", { encoder: enc })).toBe(0);
  });

  it("classifies by role and exact customType, never by substring", () => {
    expect(classifyMessage({ role: "system", content: "x" })).toBe("system");
    expect(classifyMessage({ role: "user", content: "x" })).toBe("user");
    expect(classifyMessage({ role: "toolResult", content: "x" })).toBe("tool_result");
    expect(
      classifyMessage({ role: "assistant", content: "x", customType: "enrich" }, {
        enrichTypes: ["enrich"],
      })
    ).toBe("enrich_injection");
    // "enricher" must NOT match "enrich": exact types only.
    expect(
      classifyMessage({ role: "assistant", content: "x", customType: "enricher" }, {
        enrichTypes: ["enrich"],
      })
    ).toBe("assistant_text");
  });

  it("keeps block and category totals consistent and surfaces images", () => {
    const messages = [
      { role: "system", content: "rules of the house" },
      { role: "user", content: [{ type: "text", text: "hello there" }] },
      { role: "assistant", content: [{ type: "thinking", text: "thinking it over" }] },
      { role: "assistant", content: [{ type: "toolCall", name: "read", arguments: { path: "a" } }] },
      { role: "toolResult", content: [{ type: "text", text: "file contents" }] },
      { role: "user", content: [{ type: "image", source: { type: "base64", data: "" } }] },
    ];
    const r = countPromptTokens(messages as never, { encoder: enc });
    const blockSum = r.byBlock.reduce((a, b) => a + b.tokens, 0);
    const categorySum = Object.values(r.byCategory).reduce((a, b) => a + b, 0);
    expect(r.total).toBe(blockSum);
    expect(categorySum).toBe(r.total);
    expect(r.messageCount).toBe(messages.length);
    expect(r.imageBlockCount).toBe(1);
    expect(r.byCategory.assistant_reasoning).toBeGreaterThan(0);
    expect(r.byCategory.tool_call).toBeGreaterThan(0);
    expect(r.byCategory.tool_result).toBeGreaterThan(0);
  });
});

describe("countMarkRemoval", () => {
  it("attributes the removed tokens to the mark whose range was replaced", () => {
    const before = [
      { role: "user", content: "question one here", seq: 3 },
      { role: "assistant", content: [{ type: "text", text: "a long answer body" }], seq: 4 },
    ];
    const after = [{ role: "assistant", content: "[digest]", seq: 3 }];
    const r = countMarkRemoval(
      before as never,
      after as never,
      [{ markId: 7, seqStart: 3, seqEnd: 4 }],
      { encoder: enc }
    );
    const beforeCount = countPromptTokens(before as never, { encoder: enc }).total;
    const afterCount = countPromptTokens(after as never, { encoder: enc }).total;
    expect(r.total).toBe(beforeCount - afterCount);
    expect(r.byMark).toHaveLength(1);
    expect(r.byMark[0].markId).toBe(7);
    expect(r.byMark[0].tokens).toBe(r.total);
  });
});

