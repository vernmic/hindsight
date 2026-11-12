import { describe, expect, it } from "vitest";
import { parseSweepArgs, preflightApply, preflightDatabases } from "./legacy-state-cli.js";

const args = { agents: "/tmp/a", state: "/tmp/s", apply: true, allowStaleLock: false };

describe("legacy sweep cli", () => {
  it("refuses to run without both directories", () => {
    expect(parseSweepArgs([]).ok).toBe(false);
    expect(parseSweepArgs(["--agents", "/tmp/a"]).ok).toBe(false);
    const ok = parseSweepArgs(["--agents", "/tmp/a", "--state", "/tmp/s", "--apply"]);
    expect(ok.ok).toBe(true);
    if (ok.ok) {
      expect(ok.args.apply).toBe(true);
      expect(ok.args.agents).toBe("/tmp/a");
    }
  });

  it("refuses --apply while a gateway lock is present", () => {
    const refused = preflightApply(args, true);
    expect(refused.ok).toBe(false);
    expect(refused.error).toContain("gateway lock");
    expect(preflightApply({ ...args, allowStaleLock: true }, true).ok).toBe(true);
    expect(preflightApply({ ...args, apply: false }, true).ok).toBe(true);
  });

  it("aborts --apply when an agent database could not be opened", () => {
    expect(preflightDatabases(true, ["worker: locked"]).ok).toBe(false);
    expect(preflightDatabases(false, ["worker: locked"]).ok).toBe(true);
    expect(preflightDatabases(true, []).ok).toBe(true);
  });
});
