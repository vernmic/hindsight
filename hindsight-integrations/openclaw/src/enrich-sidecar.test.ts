import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  resolveEnrichConfig,
  runEnrichSidecar,
  sessionFileName,
  type EnrichPayload,
  type EnrichServed,
  type EnrichSidecarConfig,
} from "./enrich-sidecar.js";

const dirs: string[] = [];
function root(): string {
  const d = mkdtempSync(join(tmpdir(), "astinus-enrich-"));
  dirs.push(d);
  return d;
}
afterEach(() => {
  for (const d of dirs.splice(0)) {
    rmSync(d, { recursive: true, force: true });
  }
});

const cfg = (raw: EnrichSidecarConfig) => resolveEnrichConfig(raw);

const topic = { slug: "token-accounting", label: "token accounting", digest: "prune replay" };
const mem = (id: string, text = "a memory that is comfortably longer than thirty characters") => ({
  document_id: id,
  text,
});

function payload(r: string, over: Partial<EnrichPayload> = {}): EnrichPayload {
  return {
    root: r,
    sessionKey: "agent:main:telegram:group:-100:topic:22",
    topics: ["agents", "models"],
    lastUserMessage: "continue the prune replay work",
    liveTopics: [topic],
    ...over,
  };
}

describe("enrich sidecar", () => {
  it("does nothing at all when the flag is off", async () => {
    const r = root();
    const gather = vi.fn(async () => [mem("d1")]);
    const synth = vi.fn(async () => ({ text: "Relevance: x" }));
    const out = await runEnrichSidecar(payload(r), {
      config: cfg({ enabled: false }),
      gather,
      synthesize: synth,
    });
    expect(out.status).toBe("disabled");
    expect(gather).not.toHaveBeenCalled();
    expect(synth).not.toHaveBeenCalled();
    expect(() => readFileSync(join(r, "workspace", "inject", "astinus", sessionFileName(payload(r).sessionKey) + ".md"))).toThrow();
  });

  it("writes the keyed inject file with the same header contract when on", async () => {
    const r = root();
    const out = await runEnrichSidecar(payload(r), {
      config: cfg({ enabled: true }),
      gather: async () => [mem("d1")],
      synthesize: async () => ({ text: "Relevance: it matters\n\n**[doc]** fact", model: "deepseek-flash", elapsedS: 3, reasoningTokens: 42 }),
    });
    expect(out.status).toBe("wrote");
    const path = join(r, "workspace", "inject", "astinus", sessionFileName(payload(r).sessionKey) + ".md");
    expect(out.path).toBe(path);
    const text = readFileSync(path, "utf-8");
    expect(text.startsWith("## Astinus Context - ")).toBe(true);
    expect(text).toContain("*Topics: agents, models*");
    expect(text).toContain("deepseek-flash, 3s, 42 reasoning tokens");
    expect(text).toContain("Relevance: it matters");
  });

  it("does not serve a document already served for a live topic", async () => {
    const r = root();
    const served: EnrichServed = {
      load: () => new Set(["d1"]),
      mark: vi.fn(),
      prune: vi.fn(),
    };
    const out = await runEnrichSidecar(payload(r), {
      config: cfg({ enabled: true }),
      gather: async () => [mem("d1")],
      synthesize: async () => ({ text: "should not be written" }),
      served,
    });
    expect(out.status).toBe("skipped-no-data");
    expect(served.mark).not.toHaveBeenCalled();
  });

  it("records the ids it served so the next run can skip them", async () => {
    const r = root();
    const mark = vi.fn();
    const out = await runEnrichSidecar(payload(r), {
      config: cfg({ enabled: true }),
      gather: async () => [mem("d9")],
      synthesize: async () => ({ text: "Relevance: ok" }),
      served: { load: () => new Set(), mark, prune: vi.fn() },
    });
    expect(out.status).toBe("wrote");
    expect(mark).toHaveBeenCalledWith([["token-accounting", "d9"]]);
  });

  it("sweeps keyed files older than the age limit", async () => {
    const r = root();
    const keyedDir = join(r, "workspace", "inject", "astinus");
    mkdirSync(keyedDir, { recursive: true });
    const old = join(keyedDir, "stale-session.md");
    writeFileSync(old, "old", "utf-8");
    const oldSecs = (Date.now() - 7 * 3600 * 1000) / 1000;
    utimesSync(old, oldSecs, oldSecs);
    const out = await runEnrichSidecar(payload(r), {
      config: cfg({ enabled: true, maxAgeHours: 6 }),
      gather: async () => [mem("d1")],
      synthesize: async () => ({ text: "Relevance: ok" }),
    });
    expect(out.status).toBe("wrote");
    expect(() => readFileSync(old, "utf-8")).toThrow();
  });

  it("falls back to the flat inject file when the payload carries no session key", async () => {
    const r = root();
    const out = await runEnrichSidecar(payload(r, { sessionKey: "" }), {
      config: cfg({ enabled: true }),
      gather: async () => [mem("d1")],
      synthesize: async () => ({ text: "Relevance: ok" }),
    });
    expect(out.status).toBe("wrote");
    expect(out.path).toBe(join(r, "workspace", "inject", "astinus-context.md"));
  });

  it("labels a synthesis failure instead of writing a bare dump", async () => {
    const r = root();
    const out = await runEnrichSidecar(payload(r), {
      config: cfg({ enabled: true }),
      gather: async () => [mem("d1")],
      synthesize: async () => {
        throw new Error("provider down");
      },
    });
    expect(out.status).toBe("wrote");
    const text = readFileSync(String(out.path), "utf-8");
    expect(text).toContain("raw hits -- synthesis unavailable: provider down");
  });

  it("does not gather when the payload has nothing to search for", async () => {
    const r = root();
    const gather = vi.fn(async () => [mem("d1")]);
    const out = await runEnrichSidecar(payload(r, { topics: [], lastUserMessage: "  " }), {
      config: cfg({ enabled: true }),
      gather,
      synthesize: async () => ({ text: "x" }),
    });
    expect(out.status).toBe("skipped-empty");
    expect(gather).not.toHaveBeenCalled();
  });

  it("defaults the config to off and the documented banks", () => {
    const c = resolveEnrichConfig(undefined);
    expect(c.enabled).toBe(false);
    expect(c.maxAgeHours).toBe(6);
    expect(c.banks.map((b) => b.bank)).toEqual(["main", "openclaw"]);
  });
});
