import { describe, expect, it, vi } from "vitest";
import { ENGINE_ID, isTurnOwnedByEngine } from "./engine-compat.js";
import plugin from "./index.js";

describe("engine id", () => {
  it("is the existing maintained id, equal to the plugin id", () => {
    expect(ENGINE_ID).toBe("hindsight-openclaw");
  });
});

describe("isTurnOwnedByEngine (exact host identity)", () => {
  it("owns a turn the host names for hindsight-openclaw", () => {
    expect(
      isTurnOwnedByEngine({
        contextEngineTurn: { logicalTurnId: "t1", enginePluginId: "hindsight-openclaw" },
      })
    ).toBe(true);
  });

  it("does not own unowned, other-engine or malformed turns", () => {
    for (const ctx of [
      undefined,
      {},
      { contextEngineTurn: { logicalTurnId: "t1", enginePluginId: "other-engine" } },
      { contextEngineTurn: { logicalTurnId: "t1" } },
      { contextEngineTurn: { enginePluginId: "hindsight-openclaw" } },
    ]) {
      expect(isTurnOwnedByEngine(ctx)).toBe(false);
    }
  });
});

describe("ordinary memory with the real slot value", () => {
  function fakeApi(config: Record<string, unknown>) {
    return {
      config,
      registerService: vi.fn(),
      on: vi.fn(),
      logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    };
  }

  for (const slots of [{ contextEngine: "hindsight-openclaw" }, {}]) {
    it(`registers memory hooks with slots=${JSON.stringify(slots)}`, () => {
      const api = fakeApi({ plugins: { slots } });
      plugin(api as any);
      expect(api.registerService).toHaveBeenCalled();
      expect(api.on).toHaveBeenCalled();
      expect(api.logger.error).not.toHaveBeenCalled();
    });
  }
});
