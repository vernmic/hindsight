import { expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Review 24 (R24.1), ported from Codex's concurrent-defaults probe. Synthetic HTTP only.
// concurrent=false is the serialized control; true is concurrent first use of two banks on one client.
it.each([false, true])(
  "keeps both banks cached with concurrent first use = %s",
  async (concurrent) => {
    vi.resetModules();
    const root = mkdtempSync(join(tmpdir(), "review24-defaults-"));
    const puts: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
        const req = input instanceof Request ? input : new Request(input, init);
        if (req.method === "PUT") {
          puts.push(req.url);
          if (concurrent && puts.length <= 2) {
            if (puts.length === 2) release();
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
    let service!: { start(): Promise<void>; stop(): Promise<void> };
    const { default: plugin } = await import("./index.js");
    plugin({
      config: {
        plugins: {
          entries: {
            "hindsight-openclaw": {
              config: {
                hindsightApiUrl: "http://synthetic.invalid:8888",
                autoRetain: false,
                autoRecall: false,
                bankMission: "Configured mission",
                agentBankMap: { a: "bank-a", b: "bank-b" },
                logLevel: "off",
              },
            },
          },
        },
      },
      runtime: { state: { resolveStateDir: () => root } },
      registerService: (s: typeof service) => {
        service = s;
      },
      registerContextEngine() {},
      on() {},
      logger: { info() {}, warn() {}, error() {} },
    } as never);
    try {
      await service.start();
      const facade = (globalThis as any).__hindsightClient;
      if (concurrent)
        await Promise.all([
          facade.getClientForContext({ agentId: "a" }),
          facade.getClientForContext({ agentId: "b" }),
        ]);
      else {
        await facade.getClientForContext({ agentId: "a" });
        await facade.getClientForContext({ agentId: "b" });
      }
      expect(puts).toHaveLength(2);
      await facade.getClientForContext({ agentId: "a" });
      await facade.getClientForContext({ agentId: "b" });
      expect(puts).toHaveLength(2);
    } finally {
      release();
      await service.stop();
      vi.unstubAllGlobals();
      delete (globalThis as any)[
        Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-authorities")
      ];
      rmSync(root, { recursive: true, force: true });
    }
  },
  15000
);
