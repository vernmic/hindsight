import { mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  legacySessionStatePath,
  loadSessionState,
  loadSessionStateCached,
  sessionStatePath,
  setSessionStateDir,
} from "./session-state.js";

let root = "";
function write(r: string, key: string, body: unknown, mtime?: number) {
  const p = sessionStatePath(r, key);
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, typeof body === "string" ? body : JSON.stringify(body));
  if (mtime !== undefined) utimesSync(p, mtime, mtime);
}

describe("session state document", () => {
  afterEach(() => {
    setSessionStateDir(null);
    if (root) rmSync(root, { recursive: true, force: true });
    root = "";
  });

  it("resolves under the host root with a readable prefix and a digest of the full key", () => {
    const p = sessionStatePath("/r", "agent:main:s/1");
    expect(dirname(p)).toBe(join("/r", "workspace", "state", "astinus"));
    expect(p).toMatch(/agent_main_s_1\.[0-9a-f]{32}\.json$/);
    expect(sessionStatePath("/r", "k".repeat(300))).toMatch(/[\\/]k{80}\.[0-9a-f]{32}\.json$/);
    expect(legacySessionStatePath("/r", "agent:main:s/1")).toBe(
      join("/r", "workspace", "state", "astinus", "agent_main_s_1.json")
    );
  });

  it("returns defaults for a missing, corrupt or mistyped file", () => {
    root = mkdtempSync(join(tmpdir(), "hs-state-"));
    expect(loadSessionState(root, "agent:main:none")).toMatchObject({
      lastGenerationSeen: null,
      lastCommittedSeq: 0,
    });
    write(root, "agent:main:bad", "{not json");
    expect(loadSessionState(root, "agent:main:bad").lastCommittedSeq).toBe(0);
    write(root, "agent:main:typed", { lastGenerationSeen: 5, lastCommittedSeq: "9" });
    expect(loadSessionState(root, "agent:main:typed")).toMatchObject({
      lastGenerationSeen: null,
      lastCommittedSeq: 0,
    });
  });

  it("reads the generation stamp and seq counter alongside the rest of the fork document", () => {
    root = mkdtempSync(join(tmpdir(), "hs-state-"));
    write(root, "agent:main:s", {
      lastGenerationSeen: "gen-7",
      lastCommittedSeq: 412,
      topics: { a: {} },
      pendingRetain: [],
    });
    expect(loadSessionState(root, "agent:main:s")).toMatchObject({
      lastGenerationSeen: "gen-7",
      lastCommittedSeq: 412,
    });
  });

  it("caches by mtime and invalidates on a changed mtime", () => {
    root = mkdtempSync(join(tmpdir(), "hs-state-"));
    write(root, "agent:main:c", { lastCommittedSeq: 1 }, 1000);
    expect(loadSessionStateCached(root, "agent:main:c").lastCommittedSeq).toBe(1);
    write(root, "agent:main:c", { lastCommittedSeq: 2 }, 1000);
    expect(loadSessionStateCached(root, "agent:main:c").lastCommittedSeq).toBe(1);
    write(root, "agent:main:c", { lastCommittedSeq: 3 }, 2000);
    expect(loadSessionStateCached(root, "agent:main:c").lastCommittedSeq).toBe(3);
  });

  it("keeps roots apart and honors the directory override", () => {
    root = mkdtempSync(join(tmpdir(), "hs-state-"));
    const a = join(root, "a");
    const b = join(root, "b");
    write(a, "agent:main:x", { lastCommittedSeq: 10 }, 1000);
    write(b, "agent:main:x", { lastCommittedSeq: 20 }, 1000);
    expect(loadSessionStateCached(a, "agent:main:x").lastCommittedSeq).toBe(10);
    expect(loadSessionStateCached(b, "agent:main:x").lastCommittedSeq).toBe(20);
    setSessionStateDir(join(root, "scratch"));
    expect(dirname(sessionStatePath(a, "agent:main:x"))).toBe(join(root, "scratch"));
    setSessionStateDir("  ");
    expect(dirname(sessionStatePath(a, "agent:main:x"))).toBe(
      join(a, "workspace", "state", "astinus")
    );
  });
});
