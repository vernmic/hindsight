import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { sessionStatePath } from "./session-state.js";
import { tmpdir } from "node:os";
import { DatabaseSync } from "node:sqlite";
import { zstdCompressSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import { applyMarks, estimateTokensHost } from "./assemble-marks.js";
import { getPluginConfig } from "./index.js";
import { closeAllLedgers, configureSessionLedger, openSessionLedger } from "./session-ledger.js";
import { dirname, join } from "node:path";
import {
  createContextEngine,
  getLastAssembleFact,
  hostDbPathFor,
  marksPolicyFromConfig,
  registerHindsightContextEngine,
  type MarksReadSide,
} from "./context-engine.js";

const log = () => ({ info: vi.fn(), warn: vi.fn() });

describe("context engine registration", () => {
  it("registers under the plugin id", () => {
    const registerContextEngine = vi.fn();
    expect(registerHindsightContextEngine({ registerContextEngine }, log())).toBe(true);
    expect(registerContextEngine).toHaveBeenCalledTimes(1);
    expect(registerContextEngine.mock.calls[0][0]).toBe("hindsight-openclaw");
    const engine = registerContextEngine.mock.calls[0][1]() as ReturnType<
      typeof createContextEngine
    >;
    expect(engine.info.id).toBe("hindsight-openclaw");
  });

  it("is a no-op on hosts without registerContextEngine", () => {
    expect(registerHindsightContextEngine({}, log())).toBe(false);
  });

  it("swallows a host registration failure", () => {
    const l = log();
    const registerContextEngine = vi.fn(() => {
      throw new Error("duplicate");
    });
    expect(registerHindsightContextEngine({ registerContextEngine }, l)).toBe(false);
    expect(l.warn).toHaveBeenCalled();
  });
});

describe("engine info (non-durable)", () => {
  it("never claims durable turns or compaction ownership", () => {
    const engine = createContextEngine(log()) as Record<string, unknown> & {
      info: Record<string, unknown>;
    };
    expect(engine.info.ownsCompaction).toBe(false);
    expect(engine.info.transcriptSemantics).toBeUndefined();
    expect(engine.info.hostRequirements).toBeUndefined();
    expect(engine.commitTurn).toBeUndefined();
    expect(Object.isFrozen(engine.info)).toBe(true);
  });
});

describe("assemble / ingest", () => {
  it("passes messages through with a char/4 estimate", async () => {
    const engine = createContextEngine(log());
    const messages = [{ role: "user", content: "x".repeat(48) }];
    const out = await engine.assemble({ messages });
    expect(out.messages).toBe(messages);
    expect(out.estimatedTokens).toBe(16);
    expect(out.promptAuthority).toBe("preassembly_may_overflow");
    expect((await engine.assemble({})).estimatedTokens).toBe(0);
    expect(await engine.ingest({})).toEqual({ ingested: false });
  });
});

describe("compact", () => {
  it("delegates to the runtime", async () => {
    const delegate = vi.fn(async () => ({ ok: true, compacted: true }));
    const engine = createContextEngine(log(), { loadDelegate: async () => delegate });
    expect(await engine.compact({ sessionId: "s" })).toEqual({ ok: true, compacted: true });
    expect(delegate).toHaveBeenCalledWith({ sessionId: "s" });
  });

  it("returns compacted=false when delegation is unavailable", async () => {
    const l = log();
    const engine = createContextEngine(l, {
      loadDelegate: async () => {
        throw new Error("no sdk");
      },
    });
    const out = (await engine.compact({})) as { ok: boolean; compacted: boolean; reason: string };
    expect(out.ok).toBe(false);
    expect(out.compacted).toBe(false);
    expect(out.reason).toContain("no sdk");
    expect(l.warn).toHaveBeenCalled();
  });
});

describe("assemble with marks (read side)", () => {
  const msgs = [{ role: "user", content: "x".repeat(40) }];
  function side(over: Partial<MarksReadSide> = {}): MarksReadSide {
    return {
      readMarksCached: vi.fn(() => ({ ok: false, marks: [], reason: "not-open" })),
      readMaxTurnId: vi.fn(() => 7),
      markMarksApplied: vi.fn(),
      applyMarks: vi.fn() as unknown as MarksReadSide["applyMarks"],
      estimateTokensHost: vi.fn(() => 3),
      ...over,
    };
  }
  const params = {
    messages: msgs,
    sessionKey: "agent:main:s1",
    sessionId: "uuid-1",
    tokenBudget: 100,
  };

  it("resolves the host store under the instance root", () => {
    expect(hostDbPathFor("/root", "agent:main:s1")).toBe(
      join("/root", "agents", "main", "agent", "openclaw-agent.sqlite")
    );
    expect(hostDbPathFor("/root", "plain")).toBe("");
    expect(hostDbPathFor("", "agent:main:s1")).toBe("");
  });

  it("does not load the sqlite read side without a host root", async () => {
    const loadMarks = vi.fn(async () => side());
    const out = await createContextEngine(log(), { loadMarks }).assemble(params);
    expect(loadMarks).not.toHaveBeenCalled();
    expect(out.promptAuthority).toBe("preassembly_may_overflow");
  });

  it("stays pass-through while the ledger is not open", async () => {
    const s = side();
    const l = log();
    const out = await createContextEngine(l, {
      hostStateRoot: "/root",
      loadMarks: async () => s,
    }).assemble(params);
    expect(out.messages).toBe(msgs);
    expect(out.promptAuthority).toBe("preassembly_may_overflow");
    expect(s.applyMarks).not.toHaveBeenCalled();
    expect(l.warn).not.toHaveBeenCalled();
  });

  it("applies marks, stamps admitted ones and claims assembled authority", async () => {
    const pruned = [{ role: "user", content: "y" }];
    const s = side({
      readMarksCached: vi.fn(() => ({
        ok: true,
        marks: [{ mark_id: 1, seq_start: 1, seq_end: 2, action: "drop" }],
      })),
      applyMarks: vi.fn(() => ({
        messages: pruned,
        admitted: [{ mark_id: 1, seq_start: 1, seq_end: 2, action: "drop" }],
        marksInEffect: 1,
        changed: true,
        pendingCount: 1,
        tokensBefore: 20,
        tokensAfter: 3,
        skipped: [],
        windowFirstSeq: 1,
      })) as unknown as MarksReadSide["applyMarks"],
    });
    const out = await createContextEngine(log(), {
      hostStateRoot: "/root",
      loadMarks: async () => s,
    }).assemble(params);
    expect(out.messages).toBe(pruned);
    expect(out.promptAuthority).toBe("assembled");
    expect(out.estimatedTokens).toBe(3);
    const ctx = (s.applyMarks as unknown as { mock: { calls: unknown[][] } }).mock
      .calls[0][2] as Record<string, unknown>;
    expect(ctx.sessionId).toBe("uuid-1");
    expect(ctx.turnCounter).toBe(7);
    expect(s.markMarksApplied).toHaveBeenCalledWith("agent:main:s1", [1], "", 20, 3);
  });

  it("degrades to pass-through when the read side fails to load", async () => {
    const l = log();
    const out = await createContextEngine(l, {
      hostStateRoot: "/root",
      loadMarks: async () => {
        throw new Error("No such built-in module: node:sqlite");
      },
    }).assemble(params);
    expect(out.messages).toBe(msgs);
    expect(out.promptAuthority).toBe("preassembly_may_overflow");
    expect(out.estimatedTokens).toBeGreaterThan(0);
    expect(l.warn).toHaveBeenCalled();
  });
});

describe("last-assemble bookkeeping and cached marks index (wave 11)", () => {
  const msgs = [{ role: "user", content: "x".repeat(40) }];
  const mark = { mark_id: 9, seq_start: 1, seq_end: 2, action: "drop" };
  const applied = (wfs: number | null) =>
    vi.fn(() => ({
      messages: [{ role: "user", content: "y" }],
      admitted: [],
      marksInEffect: 1,
      changed: true,
      pendingCount: 0,
      tokensBefore: 20,
      tokensAfter: 3,
      skipped: [],
      windowFirstSeq: wfs,
    })) as unknown as MarksReadSide["applyMarks"];
  function side(over: Partial<MarksReadSide>): MarksReadSide {
    return {
      readMarksCached: vi.fn(() => ({ ok: false, marks: [], reason: "not-open" })),
      readMaxTurnId: vi.fn(() => 0),
      markMarksApplied: vi.fn(),
      applyMarks: vi.fn() as unknown as MarksReadSide["applyMarks"],
      estimateTokensHost: vi.fn(() => 3),
      ...over,
    };
  }

  it("records a plugin-estimated fact for a pass-through assemble", async () => {
    await createContextEngine(log(), { hostStateRoot: "/w11a" }).assemble({
      messages: msgs,
      sessionKey: "agent:main:w11a",
      tokenBudget: 500,
    });
    const fact = getLastAssembleFact("/w11a", "agent:main:w11a");
    expect(fact).toMatchObject({ tokenBudget: 500, estimator: "plugin", windowFirstSeq: null });
    expect(fact?.viewTokens).toBeGreaterThan(0);
    expect(getLastAssembleFact("/other-root", "agent:main:w11a")).toBeUndefined();
  });

  it("records host estimator, view tokens and windowFirstSeq when marks apply", async () => {
    const s = side({
      readMarksCached: vi.fn(() => ({ ok: true, marks: [mark] })),
      applyMarks: applied(42),
    });
    await createContextEngine(log(), { hostStateRoot: "/w11b", loadMarks: async () => s }).assemble(
      { messages: msgs, sessionKey: "agent:main:w11b", sessionId: "u" }
    );
    expect(getLastAssembleFact("/w11b", "agent:main:w11b")).toMatchObject({
      viewTokens: 3,
      tokenBudget: null,
      estimator: "host",
      windowFirstSeq: 42,
    });
  });

  it("falls back to the cached marks index when a later ledger read fails", async () => {
    const reads = [
      { ok: true, marks: [mark] },
      { ok: false, marks: [], reason: "ledger-error" },
    ];
    const apply = applied(1);
    const s = side({ readMarksCached: vi.fn(() => reads.shift()!), applyMarks: apply });
    const l = log();
    const engine = createContextEngine(l, { hostStateRoot: "/w11c", loadMarks: async () => s });
    const p = { messages: msgs, sessionKey: "agent:main:w11c", sessionId: "u" };
    await engine.assemble(p);
    const out = await engine.assemble(p);
    expect(apply).toHaveBeenCalledTimes(2);
    expect(out.promptAuthority).toBe("assembled");
    expect(l.warn).toHaveBeenCalledWith(expect.stringContaining("using cached index (1)"));
  });

  it("never applies one host root's cached marks to another root (R11.1)", async () => {
    const key = "agent:main:r111-isolation";
    const base = mkdtempSync(join(tmpdir(), "hs-r111-"));
    const quiet = { info() {}, warn() {} };
    function fixture(name: string) {
      const root = join(base, name);
      const dbPath = hostDbPathFor(root, key);
      mkdirSync(dirname(dbPath), { recursive: true });
      const db = new DatabaseSync(dbPath);
      db.exec("CREATE TABLE transcript_events(session_id TEXT, seq INTEGER, event_json TEXT)");
      const insert = db.prepare("INSERT INTO transcript_events VALUES (?,?,?)");
      const messages: unknown[] = [];
      for (let turn = 1; turn <= 12; turn++) {
        for (const role of ["user", "assistant"]) {
          const message = {
            role,
            idempotencyKey: `${name}-${role}-${turn}`,
            content: [
              { type: "text", text: `${name} ${role} ${turn}: ` + "synthetic ".repeat(100) },
            ],
          };
          messages.push(message);
          insert.run(name, messages.length, JSON.stringify({ message }));
        }
      }
      db.close();
      return { root, messages, sessionId: name };
    }
    try {
      const a = fixture("host-a");
      const b = fixture("host-b");
      const m = {
        mark_id: 1,
        session_id: a.sessionId,
        seq_start: 2,
        seq_end: 2,
        action: "drop",
        applied_at: 1,
      };
      const realSide = (read: () => { ok: boolean; marks: unknown[]; reason?: string }) =>
        ({
          readMarksCached: read,
          readMaxTurnId: () => 100,
          markMarksApplied: () => {
            throw new Error("already-applied mark must not be admitted");
          },
          applyMarks,
          estimateTokensHost,
        }) as unknown as MarksReadSide;
      let aRead: { ok: boolean; marks: unknown[]; reason?: string } = { ok: true, marks: [m] };
      const engineA = createContextEngine(quiet, {
        hostStateRoot: a.root,
        loadMarks: async () => realSide(() => aRead),
      });
      const engineB = createContextEngine(quiet, {
        hostStateRoot: b.root,
        loadMarks: async () => realSide(() => ({ ok: false, marks: [], reason: "not-open" })),
      });
      const p = (f: typeof a) => ({
        messages: f.messages,
        sessionKey: key,
        sessionId: f.sessionId,
      });
      expect((await engineB.assemble(p(b))).messages.length).toBe(24);
      expect((await engineA.assemble(p(a))).messages.length).toBe(23);
      const afterA = await engineB.assemble(p(b));
      expect(afterA.messages.length).toBe(24);
      expect(afterA.promptAuthority).toBe("preassembly_may_overflow");
      expect(getLastAssembleFact(b.root, key)?.estimator).toBe("plugin");
      // Same-root reuse is preserved: A's own failed read still falls back to A's cached marks.
      aRead = { ok: false, marks: [], reason: "ledger-error" };
      const aAgain = await engineA.assemble(p(a));
      expect(aAgain.messages.length).toBe(23);
      expect(aAgain.promptAuthority).toBe("assembled");
    } finally {
      try {
        rmSync(base, { recursive: true, force: true });
      } catch {
        /* Windows may hold the read-only SeqIndex handle briefly */
      }
    }
  });

  it("stamps the state generation and falls back to lastCommittedSeq on a cold ledger", async () => {
    const r = mkdtempSync(join(tmpdir(), "hs-w13-"));
    try {
      const key = "agent:main:w13";
      const p = sessionStatePath(r, key);
      mkdirSync(dirname(p), { recursive: true });
      writeFileSync(p, JSON.stringify({ lastGenerationSeen: "gen-3", lastCommittedSeq: 77 }));
      const apply = vi.fn(() => ({
        messages: [{ role: "user", content: "y" }],
        admitted: [mark],
        marksInEffect: 1,
        changed: true,
        pendingCount: 1,
        tokensBefore: 20,
        tokensAfter: 3,
        skipped: [],
        windowFirstSeq: 1,
      })) as unknown as MarksReadSide["applyMarks"];
      const s = side({
        readMarksCached: vi.fn(() => ({ ok: true, marks: [mark] })),
        readMaxTurnId: vi.fn(() => null),
        applyMarks: apply,
      });
      await createContextEngine(log(), { hostStateRoot: r, loadMarks: async () => s }).assemble({
        messages: msgs,
        sessionKey: key,
        sessionId: "u",
      });
      const ctx = (apply as unknown as { mock: { calls: unknown[][] } }).mock.calls[0][2] as {
        turnCounter: number;
      };
      expect(ctx.turnCounter).toBe(77);
      expect(s.markMarksApplied).toHaveBeenCalledWith(key, [9], "gen-3", 20, 3);
    } finally {
      rmSync(r, { recursive: true, force: true });
    }
  });

  it("shares the fact map across engine instances of one state root", async () => {
    await createContextEngine(log(), { hostStateRoot: "/w11d" }).assemble({
      messages: msgs,
      sessionKey: "agent:main:w11d",
    });
    const a = getLastAssembleFact("/w11d", "agent:main:w11d");
    await createContextEngine(log(), { hostStateRoot: "/w11d" }).assemble({
      messages: [],
      sessionKey: "agent:main:w11d",
    });
    const b = getLastAssembleFact("/w11d", "agent:main:w11d");
    expect(a?.viewTokens).toBeGreaterThan(0);
    expect(b?.viewTokens).toBe(0);
  });

  it("warns when an assemble is slow", async () => {
    const now = vi.spyOn(Date, "now");
    now.mockReturnValueOnce(1000).mockReturnValue(1300);
    const l = log();
    try {
      await createContextEngine(l).assemble({ messages: msgs, sessionKey: "agent:main:w11e" });
    } finally {
      now.mockRestore();
    }
    expect(l.warn).toHaveBeenCalledWith(expect.stringContaining("assemble took 300ms"));
  });
});

// R7.1 composed: real lazy read side, real applyMarks, a deliberately opened temp plugin ledger
// (approved storage API; production never opens it in this wave) and a Zstd-compressed host store.
describe("composed assembly over a real ledger and compressed host store", () => {
  let root = "";
  afterEach(() => {
    closeAllLedgers();
    try {
      rmSync(root, { recursive: true, force: true });
    } catch {
      /* Windows may hold the handle briefly */
    }
  });

  it("prunes the view and stamps only admitted marks", async () => {
    root = mkdtempSync(join(tmpdir(), "hs-engine-composed-"));
    const sessionKey = "agent:main:composed";
    const hostDbPath = hostDbPathFor(root, sessionKey);
    mkdirSync(dirname(hostDbPath), { recursive: true });
    const host = new DatabaseSync(hostDbPath);
    host.exec(
      "CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT, event_zstd BLOB, event_utf8_bytes INTEGER)"
    );
    const insert = host.prepare("INSERT INTO transcript_events VALUES (?,?,?,?,?)");
    const messages: unknown[] = [];
    for (let turn = 1; turn <= 12; turn++) {
      for (const role of ["user", "assistant"]) {
        const message = {
          role,
          idempotencyKey: `${role}-${turn}`,
          content: [{ type: "text", text: `${role} ${turn}: ` + "synthetic content ".repeat(150) }],
        };
        messages.push(message);
        const bytes = Buffer.from(JSON.stringify({ id: `ev-${messages.length}`, message }), "utf8");
        insert.run("host-uuid", messages.length, null, zstdCompressSync(bytes), bytes.length);
      }
    }
    host.close();

    configureSessionLedger(root);
    const opened = openSessionLedger(sessionKey, "main", hostDbPath, () => {});
    expect(opened.ok).toBe(true);
    if (!opened.ok) return;
    const addMark = opened.db.prepare(
      "INSERT INTO marks (mark_id, session_id, seq_start, seq_end, kind, action, reason, proposed_at, anchor_first_event, anchor_last_event) VALUES (?,?,?,?,?,?,?,?,?,?)"
    );
    addMark.run(1, "host-uuid", 2, 2, "turn", "drop", "synthetic", 1, "ev-2", "ev-2");
    addMark.run(2, "host-uuid", 900, 900, "turn", "drop", "out of window", 1, "ev-900", "ev-900");

    const engine = createContextEngine(log(), { hostStateRoot: root });
    const out = await engine.assemble({
      messages,
      sessionKey,
      sessionId: "host-uuid",
      tokenBudget: Math.ceil(estimateTokensHost(messages) / 0.9),
    });
    expect(out.messages.length).toBe(23);
    expect(out.promptAuthority).toBe("assembled");
    const stamped = opened.db
      .prepare("SELECT mark_id, applied_at FROM marks ORDER BY mark_id")
      .all() as Array<{ mark_id: number; applied_at: number | null }>;
    expect(stamped[0].applied_at).not.toBeNull();
    expect(stamped[1].applied_at).toBeNull();
  });
  it("honours the configured astinusMarks policy (minPressure above current pressure)", async () => {
    root = mkdtempSync(join(tmpdir(), "hs-engine-composed-"));
    const sessionKey = "agent:main:composed-policy";
    const hostDbPath = hostDbPathFor(root, sessionKey);
    mkdirSync(dirname(hostDbPath), { recursive: true });
    const host = new DatabaseSync(hostDbPath);
    host.exec(
      "CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT, event_zstd BLOB, event_utf8_bytes INTEGER)"
    );
    const insert = host.prepare("INSERT INTO transcript_events VALUES (?,?,?,?,?)");
    const messages: unknown[] = [];
    for (let turn = 1; turn <= 12; turn++) {
      for (const role of ["user", "assistant"]) {
        const message = {
          role,
          idempotencyKey: `${role}-${turn}`,
          content: [{ type: "text", text: `${role} ${turn}: ` + "synthetic content ".repeat(150) }],
        };
        messages.push(message);
        const bytes = Buffer.from(JSON.stringify({ id: `ev-${messages.length}`, message }), "utf8");
        insert.run("host-uuid", messages.length, null, zstdCompressSync(bytes), bytes.length);
      }
    }
    host.close();

    configureSessionLedger(root);
    const opened = openSessionLedger(sessionKey, "main", hostDbPath, () => {});
    expect(opened.ok).toBe(true);
    if (!opened.ok) return;
    const addMark = opened.db.prepare(
      "INSERT INTO marks (mark_id, session_id, seq_start, seq_end, kind, action, reason, proposed_at, anchor_first_event, anchor_last_event) VALUES (?,?,?,?,?,?,?,?,?,?)"
    );
    addMark.run(1, "host-uuid", 2, 2, "turn", "drop", "synthetic", 1, "ev-2", "ev-2");
    addMark.run(2, "host-uuid", 900, 900, "turn", "drop", "out of window", 1, "ev-900", "ev-900");

    const engine = createContextEngine(log(), {
      hostStateRoot: root,
      // Through the real loader (R9.1): the override must survive getPluginConfig.
      getPolicy: () => marksPolicyFromConfig(loadConfig({ astinusMarks: { minPressure: 0.99 } })),
    });
    const out = await engine.assemble({
      messages,
      sessionKey,
      sessionId: "host-uuid",
      tokenBudget: Math.ceil(estimateTokensHost(messages) / 0.9),
    });
    expect(out.messages.length).toBe(24);
    expect(out.promptAuthority).toBe("preassembly_may_overflow");
    const stamped = opened.db
      .prepare("SELECT mark_id, applied_at FROM marks ORDER BY mark_id")
      .all() as Array<{ mark_id: number; applied_at: number | null }>;
    expect(stamped[0].applied_at).toBeNull();
    expect(stamped[1].applied_at).toBeNull();
  });
});

function loadConfig(raw: Record<string, unknown>) {
  return getPluginConfig({
    config: { plugins: { entries: { "hindsight-openclaw": { config: raw } } } },
  } as never);
}

describe("astinusMarks through the real getPluginConfig (R9.1)", () => {
  it("preserves all supported overrides", () => {
    const requested = {
      minPressure: 0.99,
      admitShare: false,
      debounceTurns: 3,
      criticalPressure: 0.8,
    };
    const parsed = loadConfig({ astinusMarks: requested });
    expect(parsed.astinusMarks).toEqual(requested);
    expect(marksPolicyFromConfig(parsed)).toEqual(requested);
  });

  it("keeps a partial override partial and drops mistyped fields", () => {
    const parsed = loadConfig({ astinusMarks: { minPressure: 0.5, debounceTurns: "x" } });
    expect(marksPolicyFromConfig(parsed)).toEqual({ minPressure: 0.5 });
  });

  it("leaves the policy undefined (defaults) when absent", () => {
    const parsed = loadConfig({});
    expect(parsed.astinusMarks).toBeUndefined();
    expect(marksPolicyFromConfig(parsed)).toBeUndefined();
  });
});

describe("astinusMarks policy wiring", () => {
  it("parses only typed overrides", () => {
    expect(marksPolicyFromConfig(undefined)).toBeUndefined();
    expect(marksPolicyFromConfig({})).toBeUndefined();
    expect(marksPolicyFromConfig({ astinusMarks: { minPressure: "x" } })).toBeUndefined();
    expect(
      marksPolicyFromConfig({
        astinusMarks: {
          minPressure: 0.4,
          admitShare: false,
          debounceTurns: 3,
          criticalPressure: 0.8,
        },
      })
    ).toEqual({ minPressure: 0.4, admitShare: false, debounceTurns: 3, criticalPressure: 0.8 });
  });

  it("re-reads the policy on every assemble and passes it to applyMarks", async () => {
    let cfg: unknown = { astinusMarks: { minPressure: 0.5 } };
    const registerContextEngine = vi.fn();
    registerHindsightContextEngine({ registerContextEngine }, log(), "/root", () =>
      marksPolicyFromConfig(cfg)
    );
    const applyMarks = vi.fn((messages: unknown[]) => ({
      messages,
      admitted: [],
      marksInEffect: 0,
      changed: false,
      pendingCount: 0,
      tokensBefore: 1,
      tokensAfter: 1,
      skipped: [],
      windowFirstSeq: null,
    }));
    const engine = registerContextEngine.mock.calls[0][1]() as ReturnType<
      typeof createContextEngine
    >;
    // The registered factory uses the real lazy read side; drive the option path directly instead.
    expect(engine.info.id).toBe("hindsight-openclaw");
    const direct = createContextEngine(log(), {
      hostStateRoot: "/root",
      getPolicy: () => marksPolicyFromConfig(cfg),
      loadMarks: async () => ({
        readMarksCached: () => ({
          ok: true,
          marks: [{ mark_id: 1, seq_start: 1, seq_end: 1, action: "drop" }],
        }),
        readMaxTurnId: () => 1,
        markMarksApplied: vi.fn(),
        applyMarks: applyMarks as unknown as MarksReadSide["applyMarks"],
        estimateTokensHost: () => 1,
      }),
    });
    const params = {
      messages: [{ role: "user", content: "a" }],
      sessionKey: "agent:main:p",
      sessionId: "u",
    };
    await direct.assemble(params);
    cfg = { astinusMarks: { debounceTurns: 2 } };
    await direct.assemble(params);
    const policies = applyMarks.mock.calls.map(
      (call) => (call as unknown[])[2] as { policy?: unknown }
    );
    expect(policies[0].policy).toEqual({ minPressure: 0.5 });
    expect(policies[1].policy).toEqual({ debounceTurns: 2 });
  });
});
