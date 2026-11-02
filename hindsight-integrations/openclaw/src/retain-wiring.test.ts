import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";

// Review 19 regressions, adapted from the reviewer probe (p4-wiring.test.ts). Synthetic
// transport/HTTP boundaries only; no hosted service is contacted.

let root = "";
const AUTHORITIES = Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-authorities");
beforeEach(() => {
  vi.resetModules();
  root = mkdtempSync(join(tmpdir(), "retain-wiring-"));
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete (globalThis as Record<symbol, unknown>)[AUTHORITIES];
  rmSync(root, { recursive: true, force: true });
});

async function flushFixture(withCompletion: boolean) {
  const { RetainQueue } = await import("./retain-queue.js");
  const { flushRetainQueue } = await import("./index.js");
  const queue = new RetainQueue({ filePath: join(root, "queue.jsonl") });
  queue.enqueue("synthetic-bank", {
    content: "synthetic pending capture",
    targetFingerprint: "synthetic-target",
    operationId: randomUUID(),
    ...(withCompletion
      ? {
          completion: {
            sessionKey: "agent:main:source-debt",
            generation: "generation-1",
            marker: { seq: 20, hashes: ["synthetic-hash"], ranges: [{ start: 1, end: 20 }] },
          },
        }
      : {}),
  } as never);
  const transport = {
    targetFingerprint: "synthetic-target",
    submit: vi.fn(async (_item: unknown, id: string) => ({
      success: true,
      async: true,
      operation_id: id,
    })),
    status: vi.fn(async (_bank: string, id: string) => ({ operation_id: id, status: "completed" })),
  };
  await flushRetainQueue(queue, undefined, "supported", undefined, undefined, transport as never);
  return { queue, transport };
}

it("R19.1 positive control: retires a completed ordinary row without a completion action", async () => {
  const { queue, transport } = await flushFixture(false);
  expect(transport.submit).toHaveBeenCalledTimes(1);
  expect(queue.size()).toBe(0);
});

it("R19.1: the real flush wrapper keeps a completion-bearing receipt it cannot apply (no state root)", async () => {
  const { queue, transport } = await flushFixture(true);
  expect(transport.submit).toHaveBeenCalledTimes(1);
  expect(queue.size()).toBe(1);
  const row = queue.peek()[0];
  expect(row.completion).toBeDefined();
  expect(row.deliveryState).toBe("completed");
  expect(String(row.lastError)).toContain("completion action held: no host state root");
});

it("R19.2: default-cadence session_end finishes a pending operation without a second submit", async () => {
  let status = "pending";
  let statusReads = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: unknown) => {
      const url =
        typeof input === "string" ? input : String((input as { url?: string })?.url ?? input);
      if (url.includes("/operations/")) {
        statusReads++;
        const id = decodeURIComponent(url.slice(url.lastIndexOf("/") + 1));
        return new Response(JSON.stringify({ operation_id: id, status }));
      }
      return new Response(JSON.stringify({ status: "ok", api_version: "0.10.0", features: {} }));
    })
  );
  const mod = await import("./index.js");
  const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
  const handlers = new Map<string, (event: unknown, ctx: unknown) => Promise<void>>();
  let service!: { start(): Promise<void>; stop(): Promise<void> };
  mod.default({
    config: {
      plugins: {
        entries: {
          "hindsight-openclaw": {
            config: {
              hindsightApiUrl: "http://synthetic-api.invalid:8888",
              autoRecall: false,
              autoRetain: true,
              dynamicBankGranularity: ["agent"],
              logLevel: "error",
            },
          },
        },
      },
    },
    runtime: { state: { resolveStateDir: () => root } },
    registerService: (value: typeof service) => {
      service = value;
    },
    registerContextEngine: () => {},
    on: (name: string, handler: (event: unknown, ctx: unknown) => Promise<void>) =>
      handlers.set(name, handler),
    logger: { info() {}, warn() {}, error() {} },
  } as never);
  try {
    await service.start();
    const authority = lifecycleAuthorityFor(root);
    const send = vi
      .spyOn(authority.client!, "retain")
      .mockImplementation(
        async (_bank, _content, options) =>
          ({ success: true, async: true, operation_id: options?.operationId }) as never
      );
    const ctx = { agentId: "main", sessionKey: "agent:main:review19", messageProvider: "telegram" };
    const turn = {
      messages: [
        { role: "user", content: "My favourite fruit is the persimmon." },
        { role: "assistant", content: "Noted, persimmons it is." },
      ],
    };
    await handlers.get("agent_end")!(turn, ctx);
    await vi.waitFor(() => {
      expect(authority.flushInProgress).toBe(false);
      expect(authority.queue?.peek()[0]?.deliveryState).toBe("submitted");
    });
    const before = statusReads;
    status = "completed";
    await handlers.get("session_end")!(turn, ctx);
    expect(send).toHaveBeenCalledTimes(1);
    expect(statusReads).toBeGreaterThan(before);
    expect(authority.queue?.size()).toBe(0);
  } finally {
    await service.stop();
  }
}, 15000);

it("R21.1: a cadence-2 session close makes one awaited pass and submits once", async () => {
  let statusReads = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: unknown) => {
      const url =
        typeof input === "string" ? input : String((input as { url?: string })?.url ?? input);
      if (url.includes("/operations/")) {
        statusReads++;
        await new Promise((resolve) => setTimeout(resolve, 3000));
        const id = decodeURIComponent(url.slice(url.lastIndexOf("/") + 1));
        return new Response(JSON.stringify({ operation_id: id, status: "pending" }));
      }
      return new Response(JSON.stringify({ status: "ok", api_version: "0.10.0", features: {} }));
    })
  );
  const mod = await import("./index.js");
  const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
  const handlers = new Map<string, (event: unknown, ctx: unknown) => Promise<void>>();
  let service!: { start(): Promise<void>; stop(): Promise<void> };
  mod.default({
    config: {
      plugins: {
        entries: {
          "hindsight-openclaw": {
            config: {
              hindsightApiUrl: "http://synthetic-api.invalid:8888",
              autoRecall: false,
              autoRetain: true,
              retainEveryNTurns: 2,
              dynamicBankGranularity: ["agent"],
              logLevel: "error",
            },
          },
        },
      },
    },
    runtime: { state: { resolveStateDir: () => root } },
    registerService: (value: typeof service) => {
      service = value;
    },
    registerContextEngine: () => {},
    on: (name: string, handler: (event: unknown, ctx: unknown) => Promise<void>) =>
      handlers.set(name, handler),
    logger: { info() {}, warn() {}, error() {} },
  } as never);
  try {
    await service.start();
    const authority = lifecycleAuthorityFor(root);
    const send = vi
      .spyOn(authority.client!, "retain")
      .mockImplementation(
        async (_bank, _content, options) =>
          ({ success: true, async: true, operation_id: options?.operationId }) as never
      );
    const ctx = { agentId: "main", sessionKey: "agent:main:review21", messageProvider: "telegram" };
    const turn = {
      messages: [
        { role: "user", content: "My favourite fruit is the persimmon." },
        { role: "assistant", content: "Noted, persimmons it is." },
      ],
    };
    await handlers.get("agent_end")!(turn, ctx);
    expect(authority.queue?.size()).toBe(0);
    await handlers.get("session_end")!(turn, ctx);
    expect(send).toHaveBeenCalledTimes(1);
    expect(statusReads).toBe(1);
    expect(authority.queue?.size()).toBe(1);
  } finally {
    await service.stop();
  }
}, 15000);
