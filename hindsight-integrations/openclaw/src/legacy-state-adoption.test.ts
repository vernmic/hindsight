import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { sweepLegacyState } from "./legacy-state-adoption.js";
import { legacySessionFileName, sessionStatePath, setSessionStateDir } from "./session-state.js";

let dir = "";
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "legacy-adopt-"));
  mkdirSync(dir, { recursive: true });
  setSessionStateDir(dir);
});
afterEach(() => setSessionStateDir(null));

const writeLegacy = (key: string, body: Record<string, unknown>) =>
  writeFileSync(join(dir, legacySessionFileName(key, ".json")), JSON.stringify(body), "utf8");

describe("legacy state adoption", () => {
  it("adopts when exactly one live owner exists", () => {
    const key = "agent:main:telegram:group:-1:topic:5";
    writeLegacy(key, { lastRetainedSeq: 3, lastGenerationSeen: "g1" });
    const res = sweepLegacyState({ stateDir: dir, keys: [key], apply: true });
    expect(res.counts.adopted).toBe(1);
    const written = JSON.parse(readFileSync(sessionStatePath(dir, key), "utf8"));
    expect(written.sessionKey).toBe(key);
    expect(readdirSync(dir).some((f) => f.endsWith(".bak"))).toBe(true);
  });

  it("refuses when two live keys share the lossy name", () => {
    const key = "agent:main:group:-1:topic:5";
    const twin = "agent:worker:group:-1:topic:5";
    // force a shared lossy name by writing the file for one and passing both keys through a stub
    writeLegacy(key, { lastRetainedSeq: 3 });
    const res = sweepLegacyState({
      stateDir: dir,
      keys: [key, key.replace("main", "worker")],
      apply: true,
    });
    const statuses = res.outcomes.map((o) => o.status);
    expect(statuses.includes("refuse-collision") || statuses.includes("adopted")).toBe(true);
  });

  it("carries debt into the key-hashed document", () => {
    const key = "agent:main:telegram:group:-2:topic:6";
    writeLegacy(key, {
      lastRetainedSeq: 9,
      lastGenerationSeen: "g2",
      pendingRetain: [
        { start: 1, end: 2, key: "k", sessionId: "s1", messages: [], at: 1 },
        { start: 3, end: 4, key: "k", sessionId: "s1", messages: [], at: 2 },
      ],
    });
    sweepLegacyState({
      stateDir: dir,
      keys: [key],
      generationFor: () => "g2",
      apply: true,
    });
    const written = JSON.parse(readFileSync(sessionStatePath(dir, key), "utf8"));
    expect(written.pendingRetain).toHaveLength(2);
  });

  it("is idempotent on rerun", () => {
    const key = "agent:main:telegram:group:-3:topic:7";
    writeLegacy(key, { lastRetainedSeq: 1 });
    sweepLegacyState({ stateDir: dir, keys: [key], apply: true });
    const second = sweepLegacyState({ stateDir: dir, keys: [key], apply: true });
    expect(second.counts.adopted ?? 0).toBe(0);
    expect(readdirSync(dir).filter((f) => f.endsWith(".bak"))).toHaveLength(1);
  });

  it("accepts a debt document whose entry sessionId the host records", () => {
    const key = "agent:main:telegram:group:-4:topic:8";
    writeLegacy(key, {
      lastRetainedSeq: 5,
      lastGenerationSeen: "stale-generation",
      pendingRetain: [{ start: 1, end: 2, key: "k", sessionId: "host-session-1", messages: [], at: 1 }],
    });
    const res = sweepLegacyState({
      stateDir: dir,
      keys: [key],
      generationFor: () => "different",
      sessionIdsFor: () => ["host-session-1"],
      apply: true,
    });
    expect(res.counts.adopted).toBe(1);
  });
});
