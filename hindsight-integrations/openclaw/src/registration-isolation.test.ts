import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Review 14 (R14.1, R14.2): one evaluated module serving two roots, and a superseded lazy
// initialization racing a newer owner. Endpoints are synthetic; fetch is stubbed.
const AUTHORITIES = Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-authorities");
type Service = { start(): Promise<void>; stop(): Promise<void> };
type Handler = (event: unknown, ctx?: unknown) => Promise<unknown> | unknown;
const dirs: string[] = [];
const memoryCaps: Array<{ providerRuntime: { open(args: unknown): Promise<any> } }> = [];

afterEach(() => {
  vi.unstubAllGlobals();
  memoryCaps.length = 0;
  delete (globalThis as Record<symbol, unknown>)[AUTHORITIES];
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function tempRoot(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  dirs.push(dir);
  return dir;
}

function makeApi(root: string | undefined, endpoint: string, queueDir: string) {
  const services: Service[] = [];
  const hooks = new Map<string, Handler>();
  const api = {
    config: {
      plugins: {
        entries: {
          "hindsight-openclaw": {
            config: {
              hindsightApiUrl: endpoint,
              autoRecall: false,
              autoRetain: true,
              dynamicBankId: false,
              // R20.2: distinct banks - one shared bank hid the cross-root bank selection.
              bankId: endpoint.includes("synthetic-a.") ? "bank-a" : "bank-b",
              retainQueuePath: join(queueDir, "queue.jsonl"),
              logLevel: "off",
            },
          },
        },
      },
    },
    ...(root ? { runtime: { state: { resolveStateDir: () => root } } } : {}),
    registerService: (service: Service) => services.push(service),
    registerContextEngine: () => {},
    registerMemoryCapability: (cap: (typeof memoryCaps)[number]) => memoryCaps.push(cap),
    on: (event: string, handler: Handler) => hooks.set(event, handler),
    logger: { info() {}, warn() {}, error() {} },
  };
  return { api, services, hooks };
}

function stubServer(
  posts: string[],
  hold?: { url: string; gate: Promise<void>; entered: () => void }
) {
  let held = false;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      if (hold && request.url === hold.url && !held) {
        held = true;
        hold.entered();
        await hold.gate;
      }
      if (request.method === "POST") posts.push(request.url);
      if (request.url.includes("/operations/")) {
        const id = request.url.split("/").at(-1);
        return Response.json({ operation_id: id, status: "completed" });
      }
      if (request.method === "POST" && request.url.includes("/memories")) {
        const body = JSON.parse(await request.clone().text()) as { operation_id?: string };
        return Response.json({
          success: true,
          async: true,
          operation_id: body.operation_id ?? "op",
        });
      }
      // R20.1: the superseded endpoint lacks append and operation ids; the owner has both.
      return Response.json({
        status: "ok",
        api_version: request.url.includes("synthetic-old.") ? "0.4.0" : "0.10.0",
        features: { store_document_text: true },
      });
    })
  );
}

describe("R14.1: one module, two roots", () => {
  it("binds each registration's service and hooks to its own root", async () => {
    const posts: string[] = [];
    stubServer(posts);
    vi.resetModules();
    const { default: plugin } = await import("./index.js");
    const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
    const rootA = tempRoot("r14-a-");
    const rootB = tempRoot("r14-b-");
    const endpointA = "http://synthetic-a.invalid:8888";
    const endpointB = "http://synthetic-b.invalid:8889";
    const a = makeApi(rootA, endpointA, rootA);
    const b = makeApi(rootB, endpointB, rootB);
    plugin(a.api as never);
    plugin(b.api as never);
    const authA = lifecycleAuthorityFor(rootA);
    const authB = lifecycleAuthorityFor(rootB);

    await a.services[0].start();
    expect(authA.clientOptions?.baseUrl).toBe(endpointA);
    expect(authA.generation).toBe(1);
    expect(authB.client).toBeNull();
    expect(authB.generation).toBe(0);

    await b.services[0].start();
    expect(authB.clientOptions?.baseUrl).toBe(endpointB);
    expect(authA.clientOptions?.baseUrl).toBe(endpointA);

    // A's hook retains to A's endpoint even though B registered last.
    await a.hooks.get("agent_end")?.(
      {
        success: true,
        messages: [
          { role: "user", content: "Root A remembers the kumquat." },
          { role: "assistant", content: "Noted." },
        ],
      },
      { agentId: "main", sessionKey: "agent:main:discord:direct:a", messageProvider: "discord" }
    );
    await vi.waitFor(() => expect(posts.some((u) => u.includes("/memories"))).toBe(true));
    const retains = posts.filter((u) => u.includes("/memories"));
    expect(retains.every((u) => u.startsWith(endpointA))).toBe(true);
    expect(retains.every((u) => u.includes("/banks/bank-a/"))).toBe(true);

    // R20.2: A's native memory provider searches A's bank on A's server, not B's bank.
    const opened = await memoryCaps[0].providerRuntime.open({
      agentId: "main",
      context: { authority: { kind: "operator", scopes: [] }, assertCurrent() {} },
    });
    posts.length = 0;
    await opened.provider.search({ query: "kumquat" });
    const recalls = posts.filter((u) => u.includes("/memories/recall"));
    expect(recalls.length).toBeGreaterThan(0);
    expect(recalls.every((u) => u.startsWith(`${endpointA}/v1/default/banks/bank-a/`))).toBe(true);

    // Stopping B leaves A's lifecycle alone; stopping A ends only A.
    await b.services[0].stop();
    expect(authB.client).toBeNull();
    expect(authA.clientOptions?.baseUrl).toBe(endpointA);
    expect(authA.owner).not.toBeNull();
    await a.services[0].stop();
    expect(authA.client).toBeNull();
    expect(authA.owner).toBeNull();
  });

  it("does not let a root-less registration inherit a rooted registration's authority", async () => {
    stubServer([]);
    vi.resetModules();
    const { default: plugin } = await import("./index.js");
    const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
    const rootA = tempRoot("r14-rooted-");
    const queueC = tempRoot("r14-unrooted-");
    const a = makeApi(rootA, "http://synthetic-a.invalid:8888", rootA);
    const c = makeApi(undefined, "http://synthetic-c.invalid:8890", queueC);
    plugin(a.api as never);
    plugin(c.api as never);
    const authA = lifecycleAuthorityFor(rootA);
    await c.services[0].start();
    expect(authA.generation).toBe(0);
    expect(authA.client).toBeNull();
    await c.services[0].stop();
  });
});

describe("R14.2: a superseded lazy initialization does not publish", () => {
  it("keeps the newer owner's client when an older lazy init resumes", async () => {
    const root = tempRoot("r14-race-");
    const endpointOld = "http://synthetic-old.invalid:8888";
    const endpointOwner = "http://synthetic-owner.invalid:8889";
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered!: () => void;
    const enteredP = new Promise<void>((resolve) => {
      entered = resolve;
    });
    stubServer([], { url: `${endpointOld}/health`, gate, entered });
    const services: Service[] = [];
    let pending: Promise<void> | undefined;
    try {
      vi.resetModules();
      const older = await import("./index.js");
      const oldApi = makeApi(root, endpointOld, root);
      older.default(oldApi.api as never);
      services.push(...oldApi.services);
      pending = (globalThis as any).__hindsightClient.waitForReady();
      await enteredP;

      vi.resetModules();
      const newer = await import("./index.js");
      const ownerApi = makeApi(root, endpointOwner, root);
      newer.default(ownerApi.api as never);
      services.push(...ownerApi.services);
      await ownerApi.services[0].start();

      const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
      const authority = lifecycleAuthorityFor(root);
      expect(authority.clientOptions?.baseUrl).toBe(endpointOwner);
      expect(authority.supportsUpdateModeAppend).toBe(true);
      expect(authority.asyncRetainOperationIdCapability).toBe("supported");
      const ownerClient = authority.client;
      release();
      await pending;
      expect(authority.clientOptions?.baseUrl).toBe(endpointOwner);
      expect(authority.client).toBe(ownerClient);
      // R20.1: the old 0.4.0 endpoint's probe published nothing into the owner's flags.
      expect(authority.supportsUpdateModeAppend).toBe(true);
      expect(authority.asyncRetainOperationIdCapability).toBe("supported");
      expect(authority.isReinitInProgress).toBe(false);
    } finally {
      release();
      await pending?.catch(() => {});
      for (const service of services.reverse()) await service.stop();
    }
  }, 15_000);
});
