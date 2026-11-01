import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { MoltbotPluginAPI, ServiceConfig } from "./types.js";

// P3 per-root lifecycle authority. OpenClaw evaluates a fresh plugin module copy per registry
// (#4828; tools.catalog loads another) and the hooks can run on a copy other than the started
// one. 10.3 clone run 2026-10-31: retain read its own copy's uninitialized client and logged
// "Retain client is unavailable" (hindsight 653219262, retain-registry-reload.test.ts). Every copy
// for one host state root now shares one client, init, lifecycle and queue.

let directory: string;
const AUTHORITIES = Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-authorities");
const ctx = {
  agentId: "main",
  sessionKey: "agent:main:telegram:direct:test-user",
  messageProvider: "telegram",
  channelId: "test-user",
  senderId: "test-user",
};
const turn = {
  messages: [
    { role: "user", content: "My favourite fruit is the persimmon." },
    { role: "assistant", content: "Noted, persimmons it is." },
  ],
};

interface PluginCopy {
  service: ServiceConfig;
  agentEnd: Parameters<MoltbotPluginAPI["on"]>[1];
  retain: ReturnType<typeof vi.fn>;
  engine: typeof import("./context-engine.js");
  getClient: () => unknown;
}

/** Evaluate a fresh copy of the plugin module, as a new OpenClaw registry does. */
async function loadCopy(root: string): Promise<PluginCopy> {
  vi.resetModules();
  const Client = (await import("@vectorize-io/hindsight-client")).HindsightClient;
  const retain = vi.fn().mockResolvedValue({ success: true });
  vi.spyOn(Client.prototype, "retain").mockImplementation(retain);
  const mod = await import("./index.js");
  const plugin = mod.default;
  const copy = {
    retain,
    engine: await import("./context-engine.js"),
    getClient: mod.getClient,
  } as PluginCopy;
  plugin({
    config: {
      plugins: {
        entries: {
          "hindsight-openclaw": {
            config: {
              hindsightApiUrl: "http://localhost:8888",
              dynamicBankGranularity: ["agent"],
              autoRecall: false,
              autoRetain: true,
              retainQueuePath: join(directory, "queue.jsonl"),
              logLevel: "error",
            },
          },
        },
      },
    },
    runtime: { state: { resolveStateDir: () => root } },
    registerService: (registered) => {
      copy.service = registered;
    },
    on: (name, handler) => {
      if (name === "agent_end") copy.agentEnd = handler;
    },
    registerContextEngine: () => {},
    logger: { info: () => {}, warn: () => {}, error: () => {} },
  } as unknown as MoltbotPluginAPI);
  return copy;
}

beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "hindsight-lifecycle-authority-"));
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(JSON.stringify({ api_version: "0.10.0", features: {} }), {
          headers: { "Content-Type": "application/json" },
        })
    )
  );
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete (globalThis as Record<symbol, unknown>)[AUTHORITIES];
  rmSync(directory, { recursive: true, force: true });
});

it("retains through an older never-started copy while a newer copy owns the global", async () => {
  const root = join(directory, "host");
  const older = await loadCopy(root);
  const newest = await loadCopy(root);
  await newest.service.start();

  await older.agentEnd(turn, ctx);

  // The older copy reads the one client the newest copy's start() created.
  await vi.waitFor(() => expect(newest.retain).toHaveBeenCalledTimes(1));
  expect(newest.retain.mock.calls[0][1]).toContain("persimmon");
  expect(older.getClient()).not.toBeNull();
  expect(older.getClient()).toBe(newest.getClient());
  await newest.service.stop();
});

it("retains through a newer never-started copy using the started copy's client", async () => {
  const root = join(directory, "host");
  const started = await loadCopy(root);
  await started.service.start();
  const reloaded = await loadCopy(root);

  await reloaded.agentEnd(turn, ctx);

  // Loaded last, the reloaded copy's spy receives the send; the client is the started one's.
  await vi.waitFor(() => expect(reloaded.retain).toHaveBeenCalledTimes(1));
  expect(reloaded.getClient()).not.toBeNull();
  expect(reloaded.getClient()).toBe(started.getClient());
  await started.service.stop();
});

it("ignores stop() from a registration that does not own the lifecycle", async () => {
  const root = join(directory, "host");
  const owner = await loadCopy(root);
  const other = await loadCopy(root);
  await owner.service.start();
  await other.service.stop();

  expect(other.getClient()).not.toBeNull();
  await other.agentEnd(turn, ctx);
  await vi.waitFor(() => expect(other.retain).toHaveBeenCalledTimes(1));

  await owner.service.stop();
  expect(other.getClient()).toBeNull();
  await other.agentEnd(turn, ctx);
  expect(other.retain).toHaveBeenCalledTimes(1);
});

it("does not share a lifecycle across host roots", async () => {
  const started = await loadCopy(join(directory, "host-a"));
  await started.service.start();
  const elsewhere = await loadCopy(join(directory, "host-b"));
  // host-b was never started: its hooks lazily initialize their own root's client instead.
  await elsewhere.agentEnd(turn, ctx);
  await vi.waitFor(() => expect(elsewhere.retain).toHaveBeenCalledTimes(1));
  expect(elsewhere.getClient()).not.toBeNull();
  expect(elsewhere.getClient()).not.toBe(started.getClient());
  await started.service.stop();
});

it("shares the context engine's assemble facts and marks index across module copies", async () => {
  const root = join(directory, "host");
  const a = await loadCopy(root);
  const b = await loadCopy(root);
  const log = { info: () => {}, warn: () => {} };
  await a.engine.createContextEngine(log, { hostStateRoot: root }).assemble({
    messages: [{ role: "user", content: "x".repeat(40) }],
    sessionKey: "agent:main:p3",
    tokenBudget: 900,
  });
  expect(b.engine.getLastAssembleFact(root, "agent:main:p3")?.tokenBudget).toBe(900);
  expect(b.engine.getLastAssembleFact(join(directory, "other"), "agent:main:p3")).toBeUndefined();
});
