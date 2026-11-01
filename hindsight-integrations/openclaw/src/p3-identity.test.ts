import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { MoltbotPluginAPI, ServiceConfig } from "./types.js";

// P3 remainder (ebce1ee8c, 6ee66842b): endpoint capability state and the default retain queue are
// identified by the host instance root, not by the module copy or the inherited home directory.

let directory: string;
const AUTHORITIES = Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-authorities");

interface PluginCopy {
  service: ServiceConfig;
  isAppendModeSupported: () => boolean;
}

async function loadCopy(root: string, config: Record<string, unknown> = {}): Promise<PluginCopy> {
  vi.resetModules();
  const mod = await import("./index.js");
  const copy = { isAppendModeSupported: mod.isAppendModeSupported } as PluginCopy;
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
              logLevel: "error",
              ...config,
            },
          },
        },
      },
    },
    runtime: { state: { resolveStateDir: () => root } },
    registerService: (registered) => {
      copy.service = registered;
    },
    on: () => {},
    registerContextEngine: () => {},
    logger: { info: () => {}, warn: () => {}, error: () => {} },
  } as unknown as MoltbotPluginAPI);
  return copy;
}

beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "hindsight-p3-identity-"));
  // Keep any legacy home-relative default inside the sandbox.
  vi.stubEnv("USERPROFILE", join(directory, "home"));
  vi.stubEnv("HOME", join(directory, "home"));
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(
          JSON.stringify({ api_version: "0.10.0", features: { store_document_text: true } }),
          { headers: { "Content-Type": "application/json" } }
        )
    )
  );
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  delete (globalThis as Record<symbol, unknown>)[AUTHORITIES];
  try {
    rmSync(directory, { recursive: true, force: true });
  } catch {
    /* Windows may still hold the queue file */
  }
});

it("shares the started copy's endpoint capability probe with every copy for the root", async () => {
  const root = join(directory, "host");
  const older = await loadCopy(root, { retainQueuePath: join(directory, "q.jsonl") });
  const newest = await loadCopy(root, { retainQueuePath: join(directory, "q.jsonl") });
  await newest.service.start();
  expect(newest.isAppendModeSupported()).toBe(true);
  // The never-started copy retains with the same document-id mode as the started one (#3686).
  expect(older.isAppendModeSupported()).toBe(true);
  await newest.service.stop();
});

it("does not share endpoint capability state across host roots", async () => {
  const started = await loadCopy(join(directory, "host-a"), {
    retainQueuePath: join(directory, "a.jsonl"),
  });
  await started.service.start();
  const elsewhere = await loadCopy(join(directory, "host-b"), {
    retainQueuePath: join(directory, "b.jsonl"),
  });
  expect(started.isAppendModeSupported()).toBe(true);
  expect(elsewhere.isAppendModeSupported()).toBe(false);
  await started.service.stop();
});

it("places the default retain queue under the host instance root, not the home directory", async () => {
  const rootA = join(directory, "host-a");
  const rootB = join(directory, "host-b");
  const a = await loadCopy(rootA);
  await a.service.start();
  await a.service.stop();
  const b = await loadCopy(rootB);
  await b.service.start();
  await b.service.stop();
  expect(existsSync(join(rootA, "data"))).toBe(true);
  expect(existsSync(join(rootB, "data"))).toBe(true);
  expect(existsSync(join(directory, "home", ".openclaw"))).toBe(false);
});
