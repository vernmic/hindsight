import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { DatabaseSync } from "node:sqlite";
import { zstdCompressSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import { estimateTokensHost } from "./assemble-marks.js";
import { closeAllLedgers, configureSessionLedger, openSessionLedger } from "./session-ledger.js";
import { dirname, join } from "node:path";
import {
  createContextEngine,
  hostDbPathFor,
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
        const bytes = Buffer.from(JSON.stringify({ message }), "utf8");
        insert.run("host-uuid", messages.length, null, zstdCompressSync(bytes), bytes.length);
      }
    }
    host.close();

    configureSessionLedger(root);
    const opened = openSessionLedger(sessionKey, "main", hostDbPath, () => {});
    expect(opened.ok).toBe(true);
    if (!opened.ok) return;
    const addMark = opened.db.prepare(
      "INSERT INTO marks (mark_id, session_id, seq_start, seq_end, kind, action, reason, proposed_at) VALUES (?,?,?,?,?,?,?,?)"
    );
    addMark.run(1, "host-uuid", 2, 2, "turn", "drop", "synthetic", 1);
    addMark.run(2, "host-uuid", 900, 900, "turn", "drop", "out of window", 1);

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
});
