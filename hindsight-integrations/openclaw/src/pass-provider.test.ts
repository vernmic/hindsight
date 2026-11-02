import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { HostTranscript } from "./assemble-marks.js";
import { callLlm } from "./pass-provider.js";

// Package 28a: the pass's host reads (one reader) and its provider calls (configured order,
// each provider's own key, no discovery). Synthetic store and faked fetch only.

const dirs: string[] = [];
afterEach(() => {
  for (const d of dirs.splice(0)) {
    try {
      rmSync(d, { recursive: true, force: true });
    } catch {
      /* Windows may still hold a sqlite handle */
    }
  }
});

describe("HostTranscript", () => {
  it("reads bounds, created_at and ranges, read-only", () => {
    const dir = mkdtempSync(join(tmpdir(), "host-transcript-"));
    dirs.push(dir);
    const dbPath = join(dir, "host.sqlite");
    const db = new DatabaseSync(dbPath);
    db.exec(
      "CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT, created_at INTEGER)"
    );
    const ins = db.prepare("INSERT INTO transcript_events VALUES (?,?,?,?)");
    for (let seq = 1; seq <= 6; seq++) ins.run("sid", seq, JSON.stringify({ seq }), 1000 * seq);
    ins.run("other", 99, "{}", 1);
    db.close();
    const ht = HostTranscript.open(dbPath, "sid");
    expect(ht.seqBounds()).toEqual({ min: 1, max: 6 });
    expect(ht.createdAt(4)).toBe(4000);
    expect(ht.createdAt(99)).toBeNull();
    expect(ht.events({ lo: 2, hi: 3, descending: true }).map((e) => e.seq)).toEqual([3, 2]);
    expect(ht.events({ after: 4, limit: 1 })).toEqual([{ seq: 5, eventJson: '{"seq":5}' }]);
    expect(() =>
      (ht as unknown as { db: DatabaseSync }).db.exec("DELETE FROM transcript_events")
    ).toThrow();
    ht.close();
  });
});

describe("callLlm", () => {
  const providers = [
    {
      name: "p1",
      url: "https://p1.invalid/c",
      model: "m1",
      apiKey: "k1",
      extra: { reasoning_effort: "max" },
    },
    { name: "p2", url: "https://p2.invalid/c", model: "m2", apiKey: "k2" },
  ];
  it("tries providers in order with each one's own key and records the fallback", async () => {
    const fetchMock = vi.fn(async (url: string) =>
      url.includes("p1")
        ? new Response("no", { status: 500, statusText: "boom" })
        : new Response(
            JSON.stringify({ choices: [{ message: { content: "hi" }, finish_reason: "stop" }] })
          )
    );
    const res = await callLlm(providers, "sys", "user", { fetch: fetchMock as never });
    const sent = fetchMock.mock.calls.map((c) => {
      const init = (c as unknown as [string, RequestInit])[1];
      return [
        (init.headers as Record<string, string>).Authorization,
        JSON.parse(String(init.body)),
      ];
    });
    expect(sent.map((s) => s[0])).toEqual(["Bearer k1", "Bearer k2"]);
    expect(sent[0][1]).toMatchObject({ model: "m1", reasoning_effort: "max", max_tokens: 16000 });
    expect(sent[1][1].reasoning_effort).toBeUndefined();
    expect(res).toMatchObject({ ok: true, text: "hi", model: "m2", fallbackFrom: "p1" });
    expect(res.fallbackReason).toContain("primary-failed: p1:HTTP Error 500");
  });
  it("applies an effort override only where a provider sets reasoning_effort", async () => {
    const fetchMock = vi.fn(async () => new Response("no", { status: 500 }));
    const res = await callLlm(providers, "s", "u", { fetch: fetchMock as never }, "high");
    const bodies = fetchMock.mock.calls.map((c) =>
      JSON.parse(String((c as unknown as [string, RequestInit])[1].body))
    );
    expect(bodies[0].reasoning_effort).toBe("high");
    expect("reasoning_effort" in bodies[1]).toBe(false);
    expect(res.ok).toBe(false);
  });
  it("reports no provider configured", async () => {
    expect((await callLlm([], "s", "u", {})).fallbackReason).toBe("no pass provider configured");
  });
});
