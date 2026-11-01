import { expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Review 22, ported from Codex's probes (cross-copy-recall, stale-health-recovery). Synthetic HTTP.
// R22.1: a hook bound by copy A recalls A's bank even when a later copy B owns the global facade.
// R22.2: a superseded start's failed health recovery neither resets nor reconfigures the new owner.
type Service = { start(): Promise<void>; stop(): Promise<void> };

it.each([false, true])(
  "recalls through root A when separate module copies = %s",
  async (separateCopies) => {
    vi.resetModules();
    const roots = [
      mkdtempSync(join(tmpdir(), "review22-copy-a-")),
      mkdtempSync(join(tmpdir(), "review22-copy-b-")),
    ];
    const endpoints = ["http://synthetic-a.invalid:8888", "http://synthetic-b.invalid:8889"];
    const services: Service[] = [];
    const hooks: Map<string, (event: any, ctx: any) => Promise<any>>[] = [];
    const recalls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
        const request = input instanceof Request ? input : new Request(input, init);
        if (request.url.includes("/memories/recall")) {
          recalls.push(request.url);
          return Response.json({
            results: [
              {
                id: "synthetic-memory",
                text: request.url.startsWith(endpoints[0])
                  ? "A private kumquat decision"
                  : "B private kumquat decision",
                type: "world",
              },
            ],
            entities: [],
          });
        }
        return Response.json({
          status: "ok",
          api_version: "0.10.0",
          features: { store_document_text: true },
        });
      })
    );
    let imported = await import("./index.js");
    try {
      for (let i = 0; i < 2; i++) {
        if (i === 1 && separateCopies) {
          vi.resetModules();
          imported = await import("./index.js");
        }
        const handlers = new Map<string, (event: any, ctx: any) => Promise<any>>();
        hooks.push(handlers);
        imported.default({
          config: {
            plugins: {
              entries: {
                "hindsight-openclaw": {
                  config: {
                    hindsightApiUrl: endpoints[i],
                    autoRecall: true,
                    autoRetain: false,
                    dynamicBankId: false,
                    bankId: i === 0 ? "bank-a" : "bank-b",
                    logLevel: "off",
                  },
                },
              },
            },
          },
          runtime: { state: { resolveStateDir: () => roots[i] } },
          registerService: (service: Service) => services.push(service),
          registerContextEngine() {},
          on: (name: string, handler: any) => handlers.set(name, handler),
          logger: { info() {}, warn() {}, error() {} },
        } as never);
        await services[i].start();
      }
      const result = await hooks[0].get("before_prompt_build")!(
        {
          prompt: "Please recall the persistent kumquat decision for this project.",
          messages: [
            {
              role: "user",
              content: "Please recall the persistent kumquat decision for this project.",
            },
          ],
        },
        {
          agentId: "main",
          sessionKey: "agent:main:discord:direct:a",
          messageProvider: "discord",
        }
      );
      console.log(JSON.stringify({ separateCopies, recalls, injected: result }));
      expect(recalls).toHaveLength(1);
      expect(recalls[0]).toBe(`${endpoints[0]}/v1/default/banks/bank-a/memories/recall`);
      expect(JSON.stringify(result)).toContain("A private kumquat decision");
      expect(JSON.stringify(result)).not.toContain("B private kumquat decision");
    } finally {
      for (const service of services.reverse()) await service.stop();
      vi.unstubAllGlobals();
      delete (globalThis as any)[
        Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-authorities")
      ];
      for (const root of roots) rmSync(root, { recursive: true, force: true });
    }
  },
  15_000
);

it("does not reset a newer owner when an older post-init health check fails", async () => {
  const root = mkdtempSync(join(tmpdir(), "review22-health-"));
  const endpointA = "http://synthetic-old.invalid:8888";
  const endpointB = "http://synthetic-owner.invalid:8889";
  let release!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const held = new Promise<void>((resolve) => {
    entered = resolve;
  });
  let oldHealthReads = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string | URL | Request) => {
      const url = input instanceof Request ? input.url : String(input);
      if (url === `${endpointA}/health`) {
        oldHealthReads++;
        if (oldHealthReads >= 2 && oldHealthReads <= 4) {
          if (oldHealthReads === 2) {
            entered();
            await gate;
          }
          throw new Error("synthetic superseded health failure");
        }
      }
      return Response.json({
        status: "ok",
        api_version: "0.10.0",
        features: { store_document_text: true },
      });
    })
  );
  const services: Service[] = [];
  let oldStart: Promise<void> | undefined;
  async function register(endpoint: string) {
    vi.resetModules();
    const { default: plugin } = await import("./index.js");
    plugin({
      config: {
        plugins: {
          entries: {
            "hindsight-openclaw": {
              config: {
                hindsightApiUrl: endpoint,
                autoRecall: false,
                autoRetain: true,
                logLevel: "off",
              },
            },
          },
        },
      },
      runtime: { state: { resolveStateDir: () => root } },
      registerService: (service: Service) => services.push(service),
      registerContextEngine() {},
      on() {},
      logger: { info() {}, warn() {}, error() {} },
    } as never);
  }
  try {
    await register(endpointA);
    oldStart = services[0].start();
    await held;
    await register(endpointB);
    await services[1].start();
    const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
    const authority = lifecycleAuthorityFor(root);
    const owner = authority.owner;
    const client = authority.client;
    expect(client).not.toBeNull();
    expect(authority.clientOptions?.baseUrl).toBe(endpointB);
    expect(authority.config?.hindsightApiUrl).toBe(endpointB);
    expect(authority.isInitialized).toBe(true);
    release();
    await oldStart;
    console.log(
      JSON.stringify({
        ownerPreserved: authority.owner === owner,
        clientPreserved: authority.client === client,
        clientIsNull: authority.client === null,
        initialized: authority.isInitialized,
        configuredEndpoint: authority.config?.hindsightApiUrl,
      })
    );
    expect(oldHealthReads).toBeGreaterThanOrEqual(4);
    expect(authority.owner).toBe(owner);
    expect(authority.client === client).toBe(true);
    expect(authority.isInitialized).toBe(true);
    expect(authority.config?.hindsightApiUrl).toBe(endpointB);
  } finally {
    release();
    await oldStart?.catch(() => {});
    for (const service of services.reverse()) await service.stop();
    vi.unstubAllGlobals();
    delete (globalThis as any)[
      Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-authorities")
    ];
    rmSync(root, { recursive: true, force: true });
  }
}, 15_000);
