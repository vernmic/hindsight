import { mkdtempSync, rmSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import { DatabaseSync } from "node:sqlite";
import * as zlib from "node:zlib";
import { afterAll, describe, expect, it } from "vitest";

import { providerUsageAfter } from "./assemble-marks.js";
import { anchoredFigure, calibrate, pressureScale, type AnchorState } from "./count-anchor.js";

const usage = (promptTokens: number, seq: number) => ({
  promptTokens,
  input: 0,
  cacheRead: promptTokens,
  cacheWrite: 0,
  seq,
});

function state(tokenizer: number, host: number, reported = tokenizer): AnchorState {
  return { sessionId: "s1", viewLastSeq: 100, tokenizer, host, reported };
}

// Live 11-03 pairs (engine tokenizer vs provider promptTokens): topic 2549 glm-5.3 1.55x
// over, topic 1959 glm-5.3 0.99x, topic 22 deepseek-flash 0.85x under.
const LIVE = [
  { name: "over-count (thinking replayed out)", tokenizer: 1_089_716, provider: 702_571, next: 1_090_650, nextProvider: 704_077 },
  { name: "neutral", tokenizer: 125_787, provider: 127_250, next: 126_900, nextProvider: 128_300 },
  { name: "under-count (deepseek)", tokenizer: 574_164, provider: 678_896, next: 593_888, nextProvider: 699_921 },
];

describe("count anchor", () => {
  for (const c of LIVE) {
    it(`lands within 15% of the provider on the next call: ${c.name}`, () => {
      const { state: s } = calibrate(state(c.tokenizer, c.tokenizer * 1.4), usage(c.provider, 101));
      const fig = anchoredFigure(c.next, s);
      expect(fig.source).toBe("derived");
      expect(Math.abs(fig.tokens - c.nextProvider) / c.nextProvider).toBeLessThan(0.15);
      // The uncalibrated tokenizer figure fails the same bar on the over-count desk.
      if (c.tokenizer / c.provider > 1.15) {
        expect(Math.abs(c.next - c.nextProvider) / c.nextProvider).toBeGreaterThan(0.15);
      }
    });
  }

  it("reports an estimate until a provider figure exists", () => {
    expect(anchoredFigure(1000, undefined)).toEqual({ tokens: 1000, source: "estimate" });
    expect(anchoredFigure(1000, state(1000, 1200))).toEqual({ tokens: 1000, source: "estimate" });
    expect(pressureScale(undefined)).toBe(1);
  });

  it("scales pressure by the provider/char-estimate ratio", () => {
    const { state: s } = calibrate(state(1_000_000, 1_400_000), usage(700_000, 101));
    expect(pressureScale(s)).toBeCloseTo(0.5, 5);
  });

  it("returns the accuracy check for the figure that was reported", () => {
    const { check } = calibrate(state(1_000_000, 1_000_000, 900_000), usage(800_000, 101));
    expect(check).toEqual({ reported: 900_000, provider: 800_000, error: 0.125 });
  });

  it("ignores an implausible pairing and keeps the previous calibration", () => {
    const first = calibrate(state(100_000, 100_000), usage(90_000, 101)).state;
    const bad = calibrate({ ...first, tokenizer: 100_000 }, usage(5, 140)).state;
    expect(bad.ratioTokenizer).toBeCloseTo(0.9, 5);
    expect(bad.ratioSeq).toBe(140);
  });

  it("does not fold the same provider event twice", () => {
    const first = calibrate(state(100_000, 100_000), usage(90_000, 101)).state;
    const again = calibrate({ ...first, tokenizer: 50_000 }, usage(90_000, 101));
    expect(again.check).toBeNull();
    expect(again.state.ratioTokenizer).toBeCloseTo(0.9, 5);
  });
});

describe("providerUsageAfter (host transcript)", () => {
  const dir = mkdtempSync(join(tmpdir(), "count-anchor-"));
  const dbPath = join(dir, "host.sqlite");
  const db = new DatabaseSync(dbPath);
  db.exec(
    "CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT, created_at INTEGER, event_zstd BLOB, event_utf8_bytes INTEGER, navigation_json TEXT)"
  );
  const ins = db.prepare("INSERT INTO transcript_events (session_id, seq, event_json, created_at, event_zstd) VALUES (?, ?, ?, 0, ?)");
  const ev = (role: string, usageObj?: unknown) =>
    JSON.stringify({ type: "message", id: `e${Math.random()}`, message: { role, content: [], ...(usageObj ? { usage: usageObj } : {}) } });
  ins.run("s1", 10, ev("assistant", { input: 1, cacheRead: 9 }), null);
  ins.run("s1", 11, ev("user"), null);
  // A host notice with all-zero usage is not a provider call.
  ins.run("s1", 12, ev("assistant", { input: 0, cacheRead: 0, contextUsage: { promptTokens: 0 } }), null);
  const z = (zlib as unknown as { zstdCompressSync?: (b: Buffer) => Buffer }).zstdCompressSync;
  const compressed = ev("assistant", { input: 7, cacheRead: 700, contextUsage: { promptTokens: 707 } });
  if (z) ins.run("s1", 13, null, z(Buffer.from(compressed)));
  else ins.run("s1", 13, compressed, null);
  db.close();
  afterAll(() => {
    // The cached SeqIndex keeps a read handle; Windows may refuse the delete. Temp dir, so best-effort.
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch {
      /* left in tmp */
    }
  });

  it("returns the first real provider call after the view, decoding compressed rows", () => {
    const u = providerUsageAfter(dbPath, "s1", 10);
    expect(u?.seq).toBe(13);
    expect(u?.promptTokens).toBe(707);
  });

  it("returns null when no call follows the view yet", () => {
    expect(providerUsageAfter(dbPath, "s1", 13)).toBeNull();
  });
});
