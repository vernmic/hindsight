import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { createContextEngine } from "./context-engine.js";
import {
  applyCompletedRetain,
  legacySessionStatePath,
  loadSessionState,
  sessionStatePath,
  updateSessionState,
} from "./session-state.js";

// P4 slice 3 (package 27): the durable turn writer, the completion writer and the owned-work
// retain drain, ported from 9c76cbda9. Faked host and synthetic HTTP only; no server contacted.

// Package 28: the pass spawn is captured, never run.
const spawned = vi.hoisted(() => [] as Array<{ cmd: string; args: string[]; stdin: string }>);
vi.mock("node:child_process", async (orig) => ({
  ...(await orig<typeof import("node:child_process")>()),
  spawn: vi.fn((cmd: string, args: string[]) => {
    const rec = { cmd, args, stdin: "" };
    spawned.push(rec);
    return { on() {}, unref() {}, stdin: { on() {}, end: (d: string) => (rec.stdin = d) } };
  }),
}));

let root = "";
const AUTHORITIES = Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-authorities");
const KEY = "agent:main:durable";
const quiet = { info() {}, warn() {} };
const okLedger =
  (calls: unknown[] = []) =>
  async () => ({
    configureSessionLedger: (r: string) => calls.push(["root", r]),
    insertCommittedTurn: (row: unknown) => (calls.push(row), { ok: true }),
  });
const commit = (n: number, extra: Record<string, unknown> = {}) => ({
  sessionKey: KEY,
  sessionId: "sid-1",
  advancementKey: `adv-${n}`,
  admission: { rawSeq: n * 10 + 1 },
  terminal: { rawSeq: n * 10 + 2, generation: "gen-1" },
  messages: [
    { role: "user", content: `question ${n}` },
    { role: "tool", content: "noise" },
    { role: "assistant", content: [{ type: "text", text: `answer ${n}` }] },
  ],
  ...extra,
});

beforeEach(() => {
  vi.resetModules();
  spawned.length = 0;
  root = mkdtempSync(join(tmpdir(), "durable-turns-"));
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete (globalThis as Record<symbol, unknown>)[AUTHORITIES];
  try {
    rmSync(root, { recursive: true, force: true });
  } catch {
    /* Windows: the session ledger opened by commitTurn may still hold its file */
  }
});

describe("engine declaration", () => {
  it("is non-durable by default: no turn contract, no commitTurn, no host requirement", () => {
    const engine = createContextEngine(quiet, { hostStateRoot: root }) as any;
    expect(engine.info.transcriptSemantics).toBeUndefined();
    expect(engine.info.hostRequirements).toBeUndefined();
    expect(engine.commitTurn).toBeUndefined();
  });

  it("durable mode declares the atomic-idempotent contract and requires the agent-end capability", () => {
    const engine = createContextEngine(quiet, { hostStateRoot: root, durableTurns: true }) as any;
    expect(engine.info.transcriptSemantics).toEqual({
      currentTurnFence: "before-current-turn-entry-v1",
      turnAdvancementIdempotency: "atomic-idempotent-v1",
    });
    expect(engine.info.hostRequirements).toEqual({
      "agent-run": { requiredCapabilities: ["agent-end-context-engine-turn-v1"] },
    });
    expect(typeof engine.commitTurn).toBe("function");
  });
});

describe("commitTurn", () => {
  it("records the turn, stamps the advancement key on its range, and is idempotent", async () => {
    const calls: unknown[] = [];
    const engine = createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      loadLedger: okLedger(calls),
    }) as any;
    expect(await engine.commitTurn(commit(1))).toEqual({ status: "committed" });
    expect(await engine.commitTurn(commit(1))).toEqual({ status: "duplicate" });
    const s = loadSessionState(root, KEY);
    expect(s.committedKeys).toEqual(["adv-1"]);
    expect(s.lastCommittedSeq).toBe(12);
    expect(s.lastCommittedRange).toEqual({ start: 11, end: 12 });
    expect(s.lastGenerationSeen).toBe("gen-1");
    expect(s.lastLedgerFailed).toBe(false);
    expect(s.pendingRetain).toHaveLength(1);
    expect(s.pendingRetain[0]).toMatchObject({
      start: 11,
      end: 12,
      key: "adv-1",
      sessionId: "sid-1",
      messages: [
        { role: "user", content: "question 1" },
        { role: "assistant", content: "answer 1" },
      ],
    });
    expect(typeof s.pendingRetain[0].at).toBe("number");
    expect(calls[0]).toEqual(["root", root]);
    expect(calls).toHaveLength(2);
  });

  it("a heartbeat commits without a pending range; a ledger failure is recorded, not thrown", async () => {
    const engine = createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      loadLedger: async () => {
        throw new Error("no sqlite");
      },
    }) as any;
    expect(await engine.commitTurn(commit(1, { isHeartbeat: true }))).toEqual({
      status: "committed",
    });
    const s = loadSessionState(root, KEY);
    expect(s.pendingRetain).toEqual([]);
    expect(s.lastLedgerFailed).toBe(true);
  });

  it("derives heartbeat from the session key only when the config asks for it", async () => {
    const rows: any[] = [];
    const cronKey = "agent:main:cron:nightly";
    const plain = createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      loadLedger: okLedger(rows),
    }) as any;
    await plain.commitTurn(commit(1, { sessionKey: cronKey }));
    const derived = createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      deriveHeartbeatFromSessionKey: true,
      loadLedger: okLedger(rows),
    }) as any;
    await derived.commitTurn(commit(2, { sessionKey: cronKey }));
    const beats = rows.filter((r) => r && r.heartbeat !== undefined).map((r) => r.heartbeat);
    expect(beats[0]).toBe(false);
    expect(beats[1]).toBe(true);
  });

  it("throws (host retries) without a key, or when the state document is corrupt", async () => {
    const engine = createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      loadLedger: okLedger(),
    }) as any;
    await expect(engine.commitTurn(commit(1, { advancementKey: undefined }))).rejects.toThrow(
      /advancementKey/
    );
    const p = sessionStatePath(root, KEY);
    mkdirSync(dirname(p), { recursive: true });
    writeFileSync(p, "{corrupt");
    await expect(engine.commitTurn(commit(2))).rejects.toThrow();
    expect(readFileSync(p, "utf8")).toBe("{corrupt");
  });
});

describe("commitTurn concurrency and identity (review 27)", () => {
  it("R27.1: overlapping retries across engine copies commit once; the loser is duplicate", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const slowLedger = async () => {
      await gate;
      return okLedger()();
    };
    const opts = { hostStateRoot: root, durableTurns: true, loadLedger: slowLedger };
    const a = createContextEngine(quiet, opts) as any;
    const b = createContextEngine(quiet, opts) as any;
    const both = Promise.all([a.commitTurn(commit(1)), b.commitTurn(commit(1))]);
    release();
    const statuses = (await both).map((r: { status: string }) => r.status).sort();
    expect(statuses).toEqual(["committed", "duplicate"]);
    const s = loadSessionState(root, KEY);
    expect(s.committedKeys).toEqual(["adv-1"]);
    expect(s.pendingRetain).toHaveLength(1);
  });

  it("R27.1: a retry after a failed write still commits", async () => {
    const engine = createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      loadLedger: okLedger(),
    }) as any;
    const p = sessionStatePath(root, KEY);
    mkdirSync(dirname(p), { recursive: true });
    writeFileSync(p, "{corrupt");
    await expect(engine.commitTurn(commit(1))).rejects.toThrow();
    unlinkSync(p);
    expect(await engine.commitTurn(commit(1))).toEqual({ status: "committed" });
    expect(loadSessionState(root, KEY).committedKeys).toEqual(["adv-1"]);
  });

  it("R27.2: keys that collide under the lossy name stay isolated", async () => {
    const engine = createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      loadLedger: okLedger(),
    }) as any;
    const pairs = [
      ["agent:main:a/b", "agent:main:a_b"],
      ["agent:main:" + "k".repeat(120) + "X", "agent:main:" + "k".repeat(120) + "Y"],
    ];
    for (const [x, y] of pairs) {
      expect(legacySessionStatePath(root, x)).toBe(legacySessionStatePath(root, y));
      expect(sessionStatePath(root, x)).not.toBe(sessionStatePath(root, y));
      await engine.commitTurn(commit(1, { sessionKey: x, advancementKey: "same" }));
      expect(await engine.commitTurn(commit(2, { sessionKey: y, advancementKey: "same" }))).toEqual(
        { status: "committed" }
      );
      expect(loadSessionState(root, x).pendingRetain[0].start).toBe(11);
      expect(loadSessionState(root, y).pendingRetain[0].start).toBe(21);
      applyCompletedRetain(root, {
        id: "q",
        completion: {
          sessionKey: x,
          generation: null,
          marker: { seq: 12, hashes: [], ranges: [{ start: 11, end: 12, key: "same" }] },
        },
      } as never);
      expect(loadSessionState(root, x).pendingRetain).toEqual([]);
      expect(loadSessionState(root, y).pendingRetain).toHaveLength(1);
    }
  });

  it("R27.2: a foreign or legacy document is never adopted; strict paths fail closed", async () => {
    const engine = createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      loadLedger: okLedger(),
    }) as any;
    await engine.commitTurn(commit(1));
    const other = "agent:main:other";
    const foreign = readFileSync(sessionStatePath(root, KEY), "utf8");
    writeFileSync(sessionStatePath(root, other), foreign);
    await expect(engine.commitTurn(commit(2, { sessionKey: other }))).rejects.toThrow(
      /another session/
    );
    expect(loadSessionState(root, other).pendingRetain).toEqual([]);
    expect(readFileSync(sessionStatePath(root, other), "utf8")).toBe(foreign);
    const legacyKey = "agent:main:legacy";
    const legacy = legacySessionStatePath(root, legacyKey);
    writeFileSync(legacy, JSON.stringify({ pendingRetain: [{ start: 1, end: 2, messages: [] }] }));
    await expect(engine.commitTurn(commit(3, { sessionKey: legacyKey }))).rejects.toThrow(/legacy/);
    expect(loadSessionState(root, legacyKey).pendingRetain).toEqual([]);
  });
});

describe("strict modeled-field validation (review 29)", () => {
  const seedDoc = (doc: Record<string, unknown>) => {
    const p = sessionStatePath(root, KEY);
    mkdirSync(dirname(p), { recursive: true });
    const text = JSON.stringify({ sessionKey: KEY, ...doc });
    writeFileSync(p, text);
    return { p, text };
  };
  const engine = () =>
    createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      loadLedger: okLedger(),
    }) as any;

  it("R29.1: a mistyped pending range in valid JSON refuses the commit and keeps the bytes", async () => {
    const { p, text } = seedDoc({
      committedKeys: ["old-key"],
      pendingRetain: [
        { start: 1, end: 2, key: 123, messages: [{ role: "user", content: "keep me" }] },
      ],
    });
    await expect(engine().commitTurn(commit(1))).rejects.toThrow(/pendingRetain is invalid/);
    expect(readFileSync(p, "utf8")).toBe(text);
    // the read-only view may still degrade
    expect(loadSessionState(root, KEY).pendingRetain).toEqual([]);
  });

  it("R29.1: mistyped scalars and legacy entries refuse too; keyless ranges are accepted", async () => {
    let seeded = seedDoc({ lastRetainedSeq: "7" });
    await expect(engine().commitTurn(commit(1))).rejects.toThrow(/lastRetainedSeq/);
    expect(readFileSync(seeded.p, "utf8")).toBe(seeded.text);
    seeded = seedDoc({ pendingLegacy: [{ id: 5, messages: [], at: 1 }] });
    expect(() =>
      applyCompletedRetain(root, {
        id: "q",
        completion: { sessionKey: KEY, generation: null },
      } as never)
    ).toThrow(/pendingLegacy/);
    expect(readFileSync(seeded.p, "utf8")).toBe(seeded.text);
    seedDoc({ pendingRetain: [{ start: 1, end: 2, messages: [] }], topics: { t: 1 } });
    expect(await engine().commitTurn(commit(1))).toEqual({ status: "committed" });
    const s = loadSessionState(root, KEY, true);
    expect(s.pendingRetain.map((r) => r.start)).toEqual([1, 11]);
  });
});

describe("applyCompletedRetain", () => {
  const seed = () =>
    updateSessionState(root, KEY, (s) => ({
      ...s,
      pendingRetain: [
        { start: 1, end: 2, messages: [{ role: "user", content: "a" }], key: "k1" },
        { start: 1, end: 2, messages: [{ role: "user", content: "b" }], key: "k2" },
        { start: 3, end: 4, messages: [{ role: "user", content: "c" }] },
      ],
      pendingLegacy: [{ id: "run-1", messages: [], at: 1 }],
    }));
  const item = (id: string, ranges: unknown[]) =>
    ({
      id,
      completion: {
        sessionKey: KEY,
        generation: null,
        marker: { seq: 4, hashes: ["h"], ranges, legacyIds: ["run-1"] },
      },
    }) as never;

  it("clears exactly the confirmed work (key when keyed, positions otherwise), once", () => {
    seed();
    applyCompletedRetain(
      root,
      item("q1", [
        { start: 1, end: 2, key: "k1" },
        { start: 3, end: 4 },
      ])
    );
    let s = loadSessionState(root, KEY);
    expect(s.pendingRetain.map((r) => r.key)).toEqual(["k2"]);
    expect(s.pendingLegacy).toEqual([]);
    expect(s.deliveredCaptureIds).toEqual(["run-1"]);
    expect(s.lastRetainedSeq).toBe(4);
    expect(s.lastAppliedRetainId).toBe("q1");
    applyCompletedRetain(root, item("q1", [{ start: 1, end: 2, key: "k2" }]));
    s = loadSessionState(root, KEY);
    expect(s.pendingRetain.map((r) => r.key)).toEqual(["k2"]);
    expect(s.deliveredCaptureIds).toEqual(["run-1"]);
  });

  it("keeps fields it does not model and throws on a corrupt document", () => {
    const p = sessionStatePath(root, KEY);
    mkdirSync(dirname(p), { recursive: true });
    writeFileSync(p, JSON.stringify({ topics: { t: { statements: 3 } }, retainTagList: ["x"] }));
    applyCompletedRetain(root, item("q2", []));
    const raw = JSON.parse(readFileSync(p, "utf8"));
    expect(raw.topics).toEqual({ t: { statements: 3 } });
    expect(raw.retainTagList).toEqual(["x"]);
    writeFileSync(p, "[]");
    expect(() => applyCompletedRetain(root, item("q3", []))).toThrow(/invalid shape/);
  });
});

async function plugin(config: Record<string, unknown>) {
  let status = "completed";
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: unknown) => {
      const url =
        typeof input === "string" ? input : String((input as { url?: string })?.url ?? input);
      if (url.includes("/operations/")) {
        const id = decodeURIComponent(url.slice(url.lastIndexOf("/") + 1));
        return new Response(JSON.stringify({ operation_id: id, status }));
      }
      return new Response(JSON.stringify({ status: "ok", api_version: "0.10.0", features: {} }));
    })
  );
  const mod = await import("./index.js");
  const { lifecycleAuthorityFor } = await import("./lifecycle-authority.js");
  const handlers = new Map<string, (event: unknown, ctx: unknown) => Promise<void>>();
  let factory: (() => any) | undefined;
  let service!: { start(): Promise<void>; stop(): Promise<void> };
  mod.default({
    config: {
      plugins: {
        entries: {
          "hindsight-openclaw": {
            config: {
              hindsightApiUrl: "http://synthetic-api.invalid:8888",
              autoRecall: false,
              autoRetain: true,
              dynamicBankGranularity: ["agent"],
              logLevel: "error",
              ...config,
            },
          },
        },
      },
    },
    runtime: { state: { resolveStateDir: () => root } },
    registerService: (value: typeof service) => {
      service = value;
    },
    registerContextEngine: (_id: string, f: () => any) => {
      factory = f;
    },
    on: (name: string, handler: (event: unknown, ctx: unknown) => Promise<void>) =>
      handlers.set(name, handler),
    logger: { info() {}, warn() {}, error() {} },
  } as never);
  await service.start();
  const authority = lifecycleAuthorityFor(root);
  const send = vi
    .spyOn(authority.client!, "retain")
    .mockImplementation(
      async (_bank, _content, options) =>
        ({ success: true, async: true, operation_id: options?.operationId }) as never
    );
  const settle = () => vi.waitFor(() => expect(authority.flushInProgress).toBe(false));
  return {
    handlers,
    engine: factory!(),
    authority,
    send,
    settle,
    service,
    setStatus: (s: string) => (status = s),
  };
}

const turn = {
  messages: [
    { role: "user", content: "My favourite fruit is the persimmon." },
    { role: "assistant", content: "Noted, persimmons it is." },
  ],
};

describe("faked host: owned-work retain", () => {
  it("durable OFF (default): the engine commits nothing and agent_end rows carry no completion", async () => {
    const h = await plugin({});
    try {
      expect(h.engine.commitTurn).toBeUndefined();
      h.setStatus("pending");
      await h.handlers.get("agent_end")!(turn, {
        agentId: "main",
        sessionKey: KEY,
        messageProvider: "telegram",
      });
      await h.settle();
      expect(h.authority.queue!.peek()[0].completion).toBeUndefined();
    } finally {
      await h.service.stop();
    }
  }, 15000);

  it("durable ON: a committed engine turn is delivered once and cleared on server completion", async () => {
    const h = await plugin({ durableTurns: true });
    try {
      expect(await h.engine.commitTurn(commit(1))).toEqual({ status: "committed" });
      const owned = {
        agentId: "main",
        sessionKey: KEY,
        messageProvider: "telegram",
        contextEngineTurn: { enginePluginId: "hindsight-openclaw", logicalTurnId: "t1" },
      };
      h.setStatus("pending");
      await h.handlers.get("agent_end")!(turn, owned);
      await h.settle();
      expect(h.send).toHaveBeenCalledTimes(1);
      const row = h.authority.queue!.peek()[0];
      expect(row.completion?.marker?.ranges).toEqual([{ start: 11, end: 12, key: "adv-1" }]);
      // Held: the queued row already carries the range, so a repeat does not resubmit it.
      await h.handlers.get("agent_end")!(turn, owned);
      await h.settle();
      expect(h.send).toHaveBeenCalledTimes(1);
      h.setStatus("completed");
      await h.handlers.get("session_end")!(
        { sessionId: "sid-1" },
        { agentId: "main", sessionKey: KEY, messageProvider: "telegram" }
      );
      await h.settle();
      expect(h.authority.queue!.size()).toBe(0);
      const s = loadSessionState(root, KEY);
      expect(s.pendingRetain).toEqual([]);
      expect(s.lastRetainedSeq).toBe(12);
    } finally {
      await h.service.stop();
    }
  }, 15000);

  it("durable ON: an unowned turn is captured by run id and a replayed agent_end is a no-op", async () => {
    const h = await plugin({ durableTurns: true });
    try {
      const ctx = {
        agentId: "main",
        sessionKey: KEY,
        messageProvider: "telegram",
        runId: "run-9",
        sessionId: "sid-1",
      };
      await h.handlers.get("agent_end")!(turn, ctx);
      await h.settle();
      expect(h.send).toHaveBeenCalledTimes(1);
      expect(h.authority.queue!.size()).toBe(0);
      const s = loadSessionState(root, KEY);
      expect(s.pendingLegacy).toEqual([]);
      expect(s.deliveredCaptureIds).toEqual(["run-9"]);
      await h.handlers.get("agent_end")!(turn, ctx);
      await h.settle();
      expect(h.send).toHaveBeenCalledTimes(1);
    } finally {
      await h.service.stop();
    }
  }, 15000);

  it("R27.3: a corrupt document stops capture and drain in the actual hooks, bytes preserved", async () => {
    const h = await plugin({ durableTurns: true });
    try {
      const p = sessionStatePath(root, KEY);
      mkdirSync(dirname(p), { recursive: true });
      writeFileSync(p, "{corrupt");
      const ctx = {
        agentId: "main",
        sessionKey: KEY,
        messageProvider: "telegram",
        runId: "run-c",
        sessionId: "sid-1",
      };
      await h.handlers.get("agent_end")!(turn, ctx);
      await h.settle();
      await h.handlers.get("session_end")!({ sessionId: "sid-1" }, ctx);
      await h.settle();
      expect(h.send).not.toHaveBeenCalled();
      expect(readFileSync(p, "utf8")).toBe("{corrupt");
    } finally {
      await h.service.stop();
    }
  }, 15000);

  it("R29.1: a malformed pending entry in valid JSON stops the actual hooks, bytes preserved", async () => {
    const h = await plugin({ durableTurns: true });
    try {
      const p = sessionStatePath(root, KEY);
      mkdirSync(dirname(p), { recursive: true });
      const text = JSON.stringify({
        sessionKey: KEY,
        pendingRetain: [
          { start: 1, end: 2, key: 123, messages: [{ role: "user", content: "keep me" }] },
        ],
      });
      writeFileSync(p, text);
      const ctx = {
        agentId: "main",
        sessionKey: KEY,
        messageProvider: "telegram",
        runId: "run-m",
        sessionId: "sid-1",
      };
      await h.handlers.get("agent_end")!(turn, ctx);
      await h.settle();
      await h.handlers.get("session_end")!({ sessionId: "sid-1" }, ctx);
      await h.settle();
      expect(h.send).not.toHaveBeenCalled();
      expect(readFileSync(p, "utf8")).toBe(text);
    } finally {
      await h.service.stop();
    }
  }, 15000);
});

describe("classification pass spawn (package 28)", () => {
  it("spawns once per newly committed turn; not for a duplicate, a heartbeat or a failed ledger row", async () => {
    const engine = createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      loadLedger: okLedger(),
      turnMiddleAgeTurns: 6,
      passProviders: [{ url: "https://p.invalid/c", model: "m", apiKey: "k" }],
    }) as any;
    const admission = { rawSeq: 11, agentId: "main", storePath: "/host.sqlite" };
    expect(await engine.commitTurn(commit(1, { admission }))).toEqual({ status: "committed" });
    expect(await engine.commitTurn(commit(1, { admission }))).toEqual({ status: "duplicate" });
    await engine.commitTurn(commit(2, { isHeartbeat: true }));
    expect(spawned).toHaveLength(1);
    expect(spawned[0].cmd).toBe(process.execPath);
    expect(spawned[0].args).toHaveLength(1); // the script only: the key never rides argv
    expect(spawned[0].args[0]).toMatch(/astinus-pass\.js$/);
    expect(JSON.parse(spawned[0].stdin)).toMatchObject({
      root,
      session_key: KEY,
      session_id: "sid-1",
      agent_id: "main",
      host_db_path: "/host.sqlite",
      range: [11, 12],
      turn_middle_age_turns: 6,
      providers: [{ apiKey: "k" }],
    });
    const failing = createContextEngine(quiet, {
      hostStateRoot: root,
      durableTurns: true,
      loadLedger: async () => ({
        configureSessionLedger() {},
        insertCommittedTurn: () => ({ ok: false, reason: "x" }),
      }),
    }) as any;
    expect(await failing.commitTurn(commit(3))).toEqual({ status: "committed" });
    expect(spawned).toHaveLength(1);
  });
});
