import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resolveHostStateRoot, resolveRetainQueuePath, sidecarEnvironment } from "./host-paths.js";
import type { MoltbotPluginAPI } from "./types.js";

afterEach(() => vi.unstubAllEnvs());
describe("host instance paths", () => {
  it("keeps default and relative retain queues inside the loaded instance", () => {
    const root = resolve("clone", ".openclaw");
    expect(resolveRetainQueuePath(root)).toBe(resolve(root, "data/hindsight-retain-queue.jsonl"));
    expect(resolveRetainQueuePath(root, "queues/retain.jsonl")).toBe(
      resolve(root, "queues/retain.jsonl")
    );
    const explicit = resolve("fixture", "queue.jsonl");
    expect(resolveRetainQueuePath(root, explicit)).toBe(explicit);
  });
  it("takes the clone root from its host despite an inherited production home", () => {
    vi.stubEnv("OPENCLAW_HOME", resolve("production"));
    const root = resolve("clone", ".openclaw");
    const api = { runtime: { state: { resolveStateDir: () => root } } } as MoltbotPluginAPI;
    expect(resolveHostStateRoot(api)).toBe(root);
    const env = sidecarEnvironment(root);
    expect(env.OPENCLAW_STATE_DIR).toBe(root);
    expect(env.OPENCLAW_HOME).toBeUndefined();
    expect(process.env.OPENCLAW_HOME).toBe(resolve("production"));
  });
  it.each([undefined, "", "relative-state"])(
    "refuses unavailable or relative host identity: %s",
    (root) => {
      const api = { runtime: { state: { resolveStateDir: () => root } } } as MoltbotPluginAPI;
      expect(() => resolveHostStateRoot(api)).toThrow("absolute instance root");
    }
  );
  it("requires host authority instead of falling back to process configuration", () => {
    vi.stubEnv("OPENCLAW_STATE_DIR", resolve("production"));
    expect(() => resolveHostStateRoot({} as MoltbotPluginAPI)).toThrow("host runtime.state");
  });
});
