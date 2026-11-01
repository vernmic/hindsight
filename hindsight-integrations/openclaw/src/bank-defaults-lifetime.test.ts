import { expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Review 23 (R23.1), ported from Codex's stale-bank-defaults probe. Synthetic HTTP only.
// finishLate=false is the serialized control; true is the overlapping old-defaults finish.
it.each([false, true])(
  "applies defaults on the new server when old defaults finish late = %s",
  async (finishLate) => {
    const root = mkdtempSync(join(tmpdir(), "review23-bank-defaults-"));
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
    const creates: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
        const request = input instanceof Request ? input : new Request(input, init);
        if (request.method === "PUT" && /\/banks\/[^/]+$/.test(new URL(request.url).pathname)) {
          creates.push(request.url);
          if (request.url === `${endpointA}/v1/default/banks/bank-a`) {
            entered();
            await gate;
          }
          return Response.json({});
        }
        return Response.json({
          status: "ok",
          api_version: "0.10.0",
          features: { store_document_text: true },
        });
      })
    );
    type Service = { start(): Promise<void>; stop(): Promise<void> };
    const services: Service[] = [];
    let oldStart: Promise<void> | undefined;
    async function register(endpoint: string, bank: string, mission: string) {
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
                  autoRetain: false,
                  dynamicBankId: false,
                  bankId: bank,
                  bankMission: mission,
                  agentBankMap: { legacy: "bank-a" },
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
      await register(endpointA, "bank-a", "Old server mission");
      oldStart = services[0].start();
      await held;
      if (!finishLate) {
        release();
        await oldStart;
      }
      await register(endpointB, "bank-b", "Current server mission");
      await services[1].start();
      const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
      const authority = lifecycleAuthorityFor(root);
      expect(authority.clientOptions?.baseUrl).toBe(endpointB);
      release();
      await oldStart;
      const client = await (globalThis as any).__hindsightClient.getClientForContext({
        agentId: "legacy",
      });
      expect(client.bankId).toBe("bank-a");
      const newServerBankAUpdates = creates.filter(
        (url) => url === `${endpointB}/v1/default/banks/bank-a`
      );
      expect(newServerBankAUpdates).toHaveLength(1);
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
  },
  15_000
);
