import { describe, expect, it } from "vitest";
import { buildAccountingRecord, findProviderUsage, usageFromEvent } from "./prompt-accounting.js";

const ev = (seq: number, body: unknown) => ({ seq, eventJson: JSON.stringify(body) });
const usageEvent = (seq: number, prompt: number) =>
  ev(seq, {
    message: {
      usage: {
        input: prompt,
        output: 5,
        cacheRead: 0,
        cacheWrite: 0,
        contextUsage: { promptTokens: prompt },
      },
    },
  });

const input = {
  sessionId: "s",
  generation: "g",
  fenceSeq: 1,
  marksInEffect: 2,
  admittedMarkIds: [1, 2],
  estimatedBefore: 1000,
  estimatedAfter: 800,
  now: 123,
};

describe("prompt-accounting", () => {
  it("returns null for unparseable JSON and for an all-zero host notice", () => {
    expect(usageFromEvent({ seq: 1, eventJson: "not json" })).toBeNull();
    expect(
      usageFromEvent(
        ev(2, {
          message: {
            usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, contextUsage: { promptTokens: 0 } },
          },
        })
      )
    ).toBeNull();
  });

  it("reads the provider prompt figure off an assistant event", () => {
    const u = usageFromEvent(usageEvent(10, 1234));
    expect(u?.promptTokens).toBe(1234);
    expect(u?.seq).toBe(10);
  });

  it("joins only the first usage at or after the fence", () => {
    const events = [usageEvent(5, 100), usageEvent(20, 200), usageEvent(30, 300)];
    expect(findProviderUsage(events, 10)?.promptTokens).toBe(200);
    expect(findProviderUsage(events, null)?.promptTokens).toBe(100);
  });

  it("labels the figure source and prefers the joined provider value", () => {
    const withUsage = buildAccountingRecord({
      ...input,
      usage: { promptTokens: 1500, seq: 9 } as never,
    });
    expect(withUsage.sources.after).toBe("provider");
    expect(withUsage.figures.after).toBe(1500);
    const estimateOnly = buildAccountingRecord({ ...input, usage: null });
    expect(estimateOnly.sources.after).toBe("estimate");
    expect(estimateOnly.figures.after).toBe(800);
  });
});
