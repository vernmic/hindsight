import { describe, expect, it } from "vitest";
import { classifyTurn, qualityGateSkipReason } from "./retain-quality-gate.js";

describe("retain quality gate", () => {
  it("skips short chitchat and memory-meta noise", () => {
    expect(qualityGateSkipReason("[role: user]\nthanks!\n[role: assistant]\nyou're welcome")).toBe(
      "short chitchat"
    );
    expect(qualityGateSkipReason("[role: user]\nplease remember this\n[role: assistant]\nok")).toBe(
      "memory-meta noise"
    );
  });

  it("skips pure tool-call exchanges with no user message", () => {
    expect(qualityGateSkipReason("[role: tool]\n{...}\n[role: assistant]\ndone")).toBe("pure tool calls");
  });

  it("retains substantive turns", () => {
    const t =
      "[role: user]\nThe gateway restart fails because the config file has an invalid plugin entry; fix it.\n" +
      "[role: assistant]\nI removed the stale key and the gateway now starts.";
    expect(qualityGateSkipReason(t)).toBe("");
    expect(classifyTurn(t).topics).toContain("infrastructure");
  });
});
