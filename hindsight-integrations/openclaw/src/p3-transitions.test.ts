import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Review 17 (R17.1, R17.2), ported from Codex's probes. Endpoints and the daemon SDK are fakes.
const daemon = vi.hoisted(() => ({
  starts: 0,
  stops: 0,
  hold: null as null | { gate: Promise<void>; entered: () => void },
}));
vi.mock("@vectorize-io/hindsight-all", () => ({
  HindsightServer: class {
    async start() {
      daemon.starts++;
    }
    async stop() {
      daemon.stops++;
      const hold = daemon.hold;
      daemon.hold = null;
      if (hold) {
        hold.entered();
        await hold.gate;
      }
    }
    async checkHealth() {
      return true;
    }
    getBaseUrl() {
      return "http://synthetic-embedded.invalid:9077";
    }
  },
}));

const AUTHORITIES = Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-authorities");
type Service = { start(): Promise<void>; stop(): Promise<void> };
const dirs: string[] = [];

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete (globalThis as Record<symbol, unknown>)[AUTHORITIES];
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

async function loadCopy(root: string, config: Record<string, unknown>, services: Service[]) {
  vi.resetModules();
  const mod = await import("./index.js");
  mod.default({
    config: {
      plugins: {
        entries: {
          "hindsight-openclaw": {
            config: { autoRecall: false, autoRetain: true, logLevel: "off", ...config },
          },
        },
      },
    },
    runtime: { state: { resolveStateDir: () => root } },
    registerService: (service: Service) => services.push(service),
    registerContextEngine: () => {},
    on: () => {},
    logger: { info() {}, warn() {}, error() {} },
  } as never);
  return mod;
}

describe("R17.1: capability probes follow the root's active client", () => {
  it("an older copy's flush probes the active endpoint and keeps the operation id", async () => {
    const root = mkdtempSync(join(tmpdir(), "r17-endpoint-"));
    dirs.push(root);
    const endpointA = "http://synthetic-old.invalid:8888";
    const endpointB = "http://synthetic-owner.invalid:8889";
    const probes: string[] = [];
    const sent: Array<Record<string, unknown>> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
        const request = input instanceof Request ? input : new Request(input, init);
        if (request.url.endsWith("/version")) probes.push(request.url);
        if (request.url.includes("/operations/")) {
          return Response.json({
            operation_id: request.url.split("/").at(-1),
            status: "completed",
          });
        }
        if (request.method === "POST" && request.url.includes("/memories")) {
          const body = JSON.parse(await request.clone().text()) as Record<string, unknown>;
          sent.push(body);
          return Response.json({ success: true, async: true, operation_id: body.operation_id });
        }
        return Response.json({
          status: "ok",
          api_version: request.url.startsWith(endpointA) ? "0.8.0" : "0.10.0",
          features: { store_document_text: true },
        });
      })
    );
    const services: Service[] = [];
    try {
      const older = await loadCopy(root, { hindsightApiUrl: endpointA }, services);
      await loadCopy(root, { hindsightApiUrl: endpointB }, services);
      await services[1].start();
      const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
      const authority = lifecycleAuthorityFor(root);
      expect(authority.clientOptions?.baseUrl).toBe(endpointB);
      expect(authority.asyncRetainOperationIdCapability).toBe("supported");
      const { createRetainTransport } = await import("./retain-delivery.js");
      const { targetFingerprint } = createRetainTransport(
        authority.client!,
        { apiUrl: endpointB, apiToken: null },
        "test"
      );
      authority.queue!.enqueue("synthetic-bank", {
        content: "synthetic queued retain",
        targetFingerprint,
        documentId: "synthetic-document",
        operationId: "synthetic-stable-operation",
      } as never);
      probes.length = 0;
      await older.flushRetainQueue();
      expect(probes.some((url) => url.startsWith(endpointA))).toBe(false);
      expect(authority.asyncRetainOperationIdCapability).toBe("supported");
      // Review 20: a valid fingerprint so the row is really submitted, and exactly once.
      expect(sent).toHaveLength(1);
      expect(sent[0].operation_id).toBe("synthetic-stable-operation");
    } finally {
      for (let i = services.length - 1; i >= 0; i--) await services[i].stop();
    }
  }, 15_000);
});

describe("R17.2: embedded-to-external handoff disposes the earlier daemon manager", () => {
  it("stops the recorded daemon exactly once across the new owner and both stops", async () => {
    const root = mkdtempSync(join(tmpdir(), "r17-daemon-"));
    dirs.push(root);
    daemon.starts = 0;
    daemon.stops = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({
          status: "ok",
          api_version: "0.10.0",
          features: { store_document_text: true },
        })
      )
    );
    const services: Service[] = [];
    await loadCopy(root, { llmProvider: "openai", llmApiKey: "synthetic-key" }, services);
    await services[0].start();
    expect(daemon.starts).toBe(1);
    await loadCopy(root, { hindsightApiUrl: "http://synthetic-external.invalid:8888" }, services);
    await services[1].start();
    await services[0].stop(); // non-owner: ignored
    await services[1].stop();
    const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
    const authority = lifecycleAuthorityFor(root);
    expect(authority.owner).toBeNull();
    expect(daemon.stops).toBe(1);
    expect(authority.hindsightServer).toBeNull();
  }, 15_000);
});

describe("R20.3: an older stop does not tear down a newer owner", () => {
  it("keeps the new owner's client and queue when the old daemon stop resolves late", async () => {
    const root = mkdtempSync(join(tmpdir(), "r20-stop-"));
    dirs.push(root);
    daemon.starts = 0;
    daemon.stops = 0;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered!: () => void;
    const enteredStop = new Promise<void>((resolve) => {
      entered = resolve;
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({
          status: "ok",
          api_version: "0.10.0",
          features: { store_document_text: true },
        })
      )
    );
    const services: Service[] = [];
    let oldStop: Promise<void> | undefined;
    try {
      await loadCopy(root, { llmProvider: "openai", llmApiKey: "synthetic-key" }, services);
      await services[0].start();
      daemon.hold = { gate, entered };
      oldStop = services[0].stop();
      await enteredStop;
      const endpoint = "http://synthetic-new-owner.invalid:8888";
      await loadCopy(root, { hindsightApiUrl: endpoint }, services);
      await services[1].start();
      const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
      const authority = lifecycleAuthorityFor(root);
      const owner = authority.owner;
      const client = authority.client;
      const queue = authority.queue;
      expect(client).not.toBeNull();
      expect(queue).not.toBeNull();
      expect(authority.clientOptions?.baseUrl).toBe(endpoint);
      release();
      await oldStop;
      expect(authority.owner).toBe(owner);
      expect(authority.client).toBe(client);
      expect(authority.queue).toBe(queue);
      expect(authority.isInitialized).toBe(true);
      expect(daemon.stops).toBe(1);
    } finally {
      release();
      await oldStop?.catch(() => {});
      for (let i = services.length - 1; i >= 0; i--) await services[i].stop();
    }
  }, 15_000);
});
