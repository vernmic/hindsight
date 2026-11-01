import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { MoltbotPluginAPI, ServiceConfig } from "./types.js";

// P4 slice 2 ruling (Vern + Claude, 2026-11-01): agent_end returns once the turn's retain is
// durably queued on disk; the delivery pass (submit, terminal status) only runs in the
// background. retainNonBlocking is a deprecated, ignored key.

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

async function loadPlugin(root: string, extra: Record<string, unknown>) {
  vi.resetModules();
  const Client = (await import("@vectorize-io/hindsight-client")).HindsightClient;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  // A slow server: the submission does not answer until the test releases it.
  const retain = vi.fn(async () => {
    await gate;
    return { success: true, async: false };
  });
  vi.spyOn(Client.prototype, "retain").mockImplementation(retain as never);
  const mod = await import("./index.js");
  let service!: ServiceConfig;
  let agentEnd!: Parameters<MoltbotPluginAPI["on"]>[1];
  const queuePath = join(directory, "queue.jsonl");
  mod.default({
    config: {
      plugins: {
        entries: {
          "hindsight-openclaw": {
            config: {
              hindsightApiUrl: "http://localhost:8888",
              dynamicBankGranularity: ["agent"],
              autoRecall: false,
              autoRetain: true,
              retainQueuePath: queuePath,
              logLevel: "error",
              ...extra,
            },
          },
        },
      },
    },
    runtime: { state: { resolveStateDir: () => root } },
    registerService: (registered: ServiceConfig) => {
      service = registered;
    },
    on: (name: string, handler: Parameters<MoltbotPluginAPI["on"]>[1]) => {
      if (name === "agent_end") agentEnd = handler;
    },
    registerContextEngine: () => {},
    logger: { info: () => {}, warn: () => {}, error: () => {} },
  } as unknown as MoltbotPluginAPI);
  return { service, agentEnd: () => agentEnd, retain, release, queuePath };
}

beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "hindsight-retain-capture-"));
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
  try {
    rmSync(directory, { recursive: true, force: true });
  } catch {
    /* Windows may still hold the queue file */
  }
});

it.each([
  ["unset", {}],
  ["false", { retainNonBlocking: false }],
  ["true", { retainNonBlocking: true }],
])(
  "agent_end returns with the retain durably queued before a slow delivery pass finishes (retainNonBlocking %s)",
  async (_label, extra) => {
    const p = await loadPlugin(join(directory, "host"), extra);
    await p.service.start();
    const settled = await Promise.race([
      Promise.resolve(p.agentEnd()(turn, ctx)).then(() => "returned"),
      new Promise((resolve) => setTimeout(() => resolve("blocked"), 2000)),
    ]);
    expect(settled).toBe("returned");
    // The queue line is on disk when agent_end resolves, before the server has answered.
    expect(existsSync(p.queuePath)).toBe(true);
    expect(readFileSync(p.queuePath, "utf8")).toContain("persimmon");
    // The delivery pass is still running in the background.
    await vi.waitFor(() => expect(p.retain).toHaveBeenCalledTimes(1));
    p.release();
    await p.service.stop();
  }
);

it("keeps retainNonBlocking accepted by the strict config schema as a deprecated key", () => {
  const manifest = JSON.parse(
    readFileSync(new URL("../openclaw.plugin.json", import.meta.url), "utf8")
  ) as {
    configSchema: {
      additionalProperties?: boolean;
      properties: Record<string, { type?: string; description?: string }>;
    };
  };
  expect(manifest.configSchema.additionalProperties).toBe(false);
  const key = manifest.configSchema.properties.retainNonBlocking;
  expect(key?.type).toBe("boolean");
  expect(key?.description).toMatch(/deprecated/i);
});
