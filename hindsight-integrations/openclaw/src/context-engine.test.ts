import { describe, expect, it, vi } from "vitest";
import { createContextEngine, registerHindsightContextEngine } from "./context-engine.js";

const log = () => ({ info: vi.fn(), warn: vi.fn() });

describe("context engine registration", () => {
  it("registers under the plugin id", () => {
    const registerContextEngine = vi.fn();
    expect(registerHindsightContextEngine({ registerContextEngine }, log())).toBe(true);
    expect(registerContextEngine).toHaveBeenCalledTimes(1);
    expect(registerContextEngine.mock.calls[0][0]).toBe("hindsight-openclaw");
    const engine = registerContextEngine.mock.calls[0][1]() as ReturnType<
      typeof createContextEngine
    >;
    expect(engine.info.id).toBe("hindsight-openclaw");
  });

  it("is a no-op on hosts without registerContextEngine", () => {
    expect(registerHindsightContextEngine({}, log())).toBe(false);
  });

  it("swallows a host registration failure", () => {
    const l = log();
    const registerContextEngine = vi.fn(() => {
      throw new Error("duplicate");
    });
    expect(registerHindsightContextEngine({ registerContextEngine }, l)).toBe(false);
    expect(l.warn).toHaveBeenCalled();
  });
});

describe("engine info (non-durable)", () => {
  it("never claims durable turns or compaction ownership", () => {
    const engine = createContextEngine(log()) as Record<string, unknown> & {
      info: Record<string, unknown>;
    };
    expect(engine.info.ownsCompaction).toBe(false);
    expect(engine.info.transcriptSemantics).toBeUndefined();
    expect(engine.info.hostRequirements).toBeUndefined();
    expect(engine.commitTurn).toBeUndefined();
    expect(Object.isFrozen(engine.info)).toBe(true);
  });
});

describe("assemble / ingest", () => {
  it("passes messages through with a char/4 estimate", async () => {
    const engine = createContextEngine(log());
    const messages = [{ role: "user", content: "x".repeat(48) }];
    const out = await engine.assemble({ messages });
    expect(out.messages).toBe(messages);
    expect(out.estimatedTokens).toBe(16);
    expect(out.promptAuthority).toBe("preassembly_may_overflow");
    expect((await engine.assemble({})).estimatedTokens).toBe(0);
    expect(await engine.ingest({})).toEqual({ ingested: false });
  });
});

describe("compact", () => {
  it("delegates to the runtime", async () => {
    const delegate = vi.fn(async () => ({ ok: true, compacted: true }));
    const engine = createContextEngine(log(), async () => delegate);
    expect(await engine.compact({ sessionId: "s" })).toEqual({ ok: true, compacted: true });
    expect(delegate).toHaveBeenCalledWith({ sessionId: "s" });
  });

  it("returns compacted=false when delegation is unavailable", async () => {
    const l = log();
    const engine = createContextEngine(l, async () => {
      throw new Error("no sdk");
    });
    const out = (await engine.compact({})) as { ok: boolean; compacted: boolean; reason: string };
    expect(out.ok).toBe(false);
    expect(out.compacted).toBe(false);
    expect(out.reason).toContain("no sdk");
    expect(l.warn).toHaveBeenCalled();
  });
});
