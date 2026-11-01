import { describe, expect, it, vi } from "vitest";
import { execFileSync } from "node:child_process";
import {
  adaptiveContextRuntimeError,
  createContextEngine,
  probeNodeSqlite,
} from "./context-engine.js";

describe("adaptive context runtime gate (PLAN section 4)", () => {
  it("accepts Node >= 22.5 and rejects older 22.x with a specific error", () => {
    expect(adaptiveContextRuntimeError("22.5.0")).toBeNull();
    expect(adaptiveContextRuntimeError("22.15.1")).toBeNull();
    expect(adaptiveContextRuntimeError("24.0.0")).toBeNull();
    expect(adaptiveContextRuntimeError("22.4.1")).toMatch(
      /requires Node >= 22\.5\.0 for node:sqlite \(running 22\.4\.1\)/
    );
  });

  it("disables marks without loading the ledger and warns once when unsupported", async () => {
    const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const loadMarks = vi.fn(async () => {
      throw new Error("ledger must not load");
    });
    const engine = createContextEngine(log as never, {
      hostStateRoot: "/tmp/hindsight-gate-root",
      nodeVersion: "22.4.0",
      loadMarks: loadMarks as never,
    });
    const messages = [{ role: "user", content: "hi" }];
    const params = { messages, tokenBudget: 1000, sessionKey: "agent:main:s1", sessionId: "s1" };
    const a = await engine.assemble(params);
    await engine.assemble(params);
    expect(loadMarks).not.toHaveBeenCalled();
    expect(a.messages).toEqual(messages);
    const gateWarns = log.warn.mock.calls.filter((c) => String(c[0]).includes("node:sqlite"));
    expect(gateWarns).toHaveLength(1);
  });

  it("does not warn when adaptive context is not selected (no state root)", async () => {
    const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const engine = createContextEngine(log as never, { nodeVersion: "22.0.0" });
    await engine.assemble({ messages: [], sessionKey: "k", sessionId: "s" });
    expect(log.warn.mock.calls.filter((c) => String(c[0]).includes("node:sqlite"))).toHaveLength(0);
  });
});

describe("node:sqlite availability (R16.1)", () => {
  // The real builtin, disabled the way Codex reproduced it: a child Node run with
  // --no-experimental-sqlite. Its actual import error drives the probe below.
  function disabledSqliteError(): { code: string; message: string } {
    const out = execFileSync(
      process.execPath,
      [
        "--no-experimental-sqlite",
        "-e",
        "import('node:sqlite').then(()=>console.log('{}'),e=>console.log(JSON.stringify({code:e.code,message:e.message})))",
      ],
      { encoding: "utf8" }
    );
    return JSON.parse(out.trim());
  }

  it("node:sqlite really fails to load under --no-experimental-sqlite", () => {
    expect(disabledSqliteError().code).toBe("ERR_UNKNOWN_BUILTIN_MODULE");
  });

  it("probeNodeSqlite reports the real unavailable-builtin failure specifically", async () => {
    const real = disabledSqliteError();
    const msg = await probeNodeSqlite(async () => {
      throw Object.assign(new Error(real.message), { code: real.code });
    });
    expect(msg).toMatch(/requires node:sqlite, which this runtime does not load/);
    expect(msg).toContain("ERR_UNKNOWN_BUILTIN_MODULE");
    expect(msg).toMatch(/22\.13\.0/);
    expect(await probeNodeSqlite()).toBeNull(); // this test process has it
  });

  it("a supported version with sqlite unavailable warns once and never loads the read side", async () => {
    const real = disabledSqliteError();
    const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const loadMarks = vi.fn(async () => {
      throw new Error("ledger must not load");
    });
    const probe = vi.fn(() =>
      probeNodeSqlite(async () => {
        throw Object.assign(new Error(real.message), { code: real.code });
      })
    );
    const engine = createContextEngine(log as never, {
      hostStateRoot: "/tmp/hindsight-gate-root",
      nodeVersion: "24.20.0",
      probeSqlite: probe,
      loadMarks: loadMarks as never,
    });
    const messages = [{ role: "user", content: "hi" }];
    const params = { messages, tokenBudget: 1000, sessionKey: "agent:main:s1", sessionId: "s1" };
    const a = await engine.assemble(params);
    await engine.assemble(params);
    expect(a.messages).toEqual(messages);
    expect(loadMarks).not.toHaveBeenCalled();
    expect(probe).toHaveBeenCalledTimes(1);
    const warns = log.warn.mock.calls.map((c) => String(c[0]));
    expect(warns.filter((w) => w.includes("does not load"))).toHaveLength(1);
    expect(warns.filter((w) => w.includes("mark application failed"))).toHaveLength(0);
  });

  it("does not probe when adaptive context is not selected", async () => {
    const probe = vi.fn(async () => "unavailable");
    const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const engine = createContextEngine(log as never, { probeSqlite: probe });
    await engine.assemble({ messages: [], sessionKey: "k", sessionId: "s" });
    expect(probe).not.toHaveBeenCalled();
    expect(log.warn).not.toHaveBeenCalled();
  });
});
