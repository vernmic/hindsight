import { expect, it } from "vitest";
import { readFileSync } from "fs";
import plugin, { getPluginConfig } from "./index.js";
import type { MoltbotPluginAPI } from "./types.js";

function apiWith(config: Record<string, unknown>, lines: string[] = []): MoltbotPluginAPI {
  return {
    config: { plugins: { entries: { "hindsight-openclaw": { config } } } },
    registerService: () => undefined,
    on: () => undefined,
    logger: {
      info: (msg: string) => lines.push(msg),
      warn: () => undefined,
      error: () => undefined,
    },
  } as unknown as MoltbotPluginAPI;
}

it('logLevel "debug" turns on debug lines without also setting debug: true', () => {
  const debugLines: string[] = [];
  plugin(apiWith({ hindsightApiUrl: "http://localhost:8888", logLevel: "debug" }, debugLines));
  expect(debugLines.some((l) => l.includes("Registering agent hooks..."))).toBe(true);

  const infoLines: string[] = [];
  plugin(apiWith({ hindsightApiUrl: "http://localhost:8888", logLevel: "info" }, infoLines));
  expect(infoLines.some((l) => l.includes("Registering agent hooks..."))).toBe(false);
});

it("keeps every manifest default through getPluginConfig", () => {
  // A field the whitelist forgets is silently dropped, the way logLevel and
  // logSummaryIntervalMs were. Defaults are valid values by construction.
  const ignored = new Set(["enableKnowledgeTools"]); // deprecated, read by nothing
  const props = JSON.parse(
    readFileSync(new URL("../openclaw.plugin.json", import.meta.url), "utf-8")
  ).configSchema.properties as Record<string, { default?: unknown }>;
  const defaults = Object.fromEntries(
    Object.entries(props)
      .filter(([key, prop]) => "default" in prop && !ignored.has(key))
      .map(([key, prop]) => [key, prop.default])
  );
  const parsed = getPluginConfig(apiWith(defaults)) as unknown as Record<string, unknown>;
  const dropped = Object.keys(defaults).filter((key) => parsed[key] === undefined);
  expect(dropped).toEqual([]);
});

it("keeps object fields without a top-level default through getPluginConfig", () => {
  // The default sweep above cannot see these: astinusMarks has per-property defaults only, and
  // it was dropped until 2026-11-03, hard cap included.
  const marks = { hardCapPressure: 0.8, hardCapTarget: 0.6, minPressure: 0.4 };
  const parsed = getPluginConfig(apiWith({ astinusMarks: marks, promptCapture: true })) as unknown as Record<string, unknown>;
  expect(parsed.astinusMarks).toEqual(marks);
  expect(parsed.promptCapture).toBe(true);
});
