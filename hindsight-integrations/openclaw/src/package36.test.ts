import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { zstdCompressSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  applyMarks,
  ensureMarkAnchorColumns,
  HostTranscript,
  SeqIndex,
  type LedgerMark,
} from "./assemble-marks.js";
import { createContextEngine, estimateTokens } from "./context-engine.js";
import {
  closeAllLedgers,
  configureSessionLedger,
  openSessionLedger,
  planMarkRebase,
  readMarksCached,
  runMarkRebaseCached,
  ledgerPath,
} from "./session-ledger.js";
import { shouldQueueRetain } from "./retain-scope.js";
import plugin from "./index.js";
import { updateSessionState } from "./session-state.js";
import { lifecycleAuthorityFor } from "./lifecycle-authority.js";

// Package 36: mark anchors, the one-time rebase, the seq-index reload (prod 667d86349,
// 7ace0c648, 40e6a16be, 7ee8730d8, 82a3d61d0) and retain scope (prod 484e299d6).

const dirs: string[] = [];
afterEach(() => {
  closeAllLedgers();
  for (const d of dirs.splice(0)) {
    try {
      rmSync(d, { recursive: true, force: true });
    } catch {
      /* Windows may hold a handle briefly */
    }
  }
});

const msg = (role: string, turn: number) => ({
  role,
  content: [{ type: "text", text: `${role} ${turn}` }],
  idempotencyKey: `ik-${role}-${turn}`,
});

/** Host store with event ids; turns [from..to] at seq 1.. (2 rows per turn). */
function host(from = 1, to = 12, gen: string | null = "gen-now") {
  const dir = mkdtempSync(join(tmpdir(), "p36-"));
  dirs.push(dir);
  const dbPath = join(dir, "host.sqlite");
  const db = new DatabaseSync(dbPath);
  db.exec("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT)");
  write(db, from, to);
  db.exec("CREATE TABLE transcript_rewrite_watermarks (session_id TEXT PRIMARY KEY, generation TEXT, updated_at INTEGER)");
  if (gen) db.prepare("INSERT INTO transcript_rewrite_watermarks VALUES ('sid-1', ?, 1)").run(gen);
  db.close();
  return { dir, dbPath };
}
function write(db: DatabaseSync, from: number, to: number, startSeq = 1) {
  const ins = db.prepare("INSERT INTO transcript_events VALUES (?,?,?)");
  let seq = startSeq;
  for (let t = from; t <= to; t++)
    for (const role of ["user", "assistant"])
      ins.run("sid-1", seq++, JSON.stringify({ id: `ev-${role}-${t}`, message: msg(role, t) }));
}
/** Simulate a manual trim: drop turn 1, renumber the survivors from 1 (ids kept). */
function trimFirstTurn(dbPath: string, appendTurn?: number) {
  const db = new DatabaseSync(dbPath);
  db.exec("DELETE FROM transcript_events");
  write(db, 2, 12);
  if (appendTurn) write(db, appendTurn, appendTurn, 23);
  db.close();
}
const view = (from: number, to: number) => {
  const out: unknown[] = [];
  for (let t = from; t <= to; t++) out.push(msg("user", t), msg("assistant", t));
  return out;
};
const ctx = (dbPath: string, key: string) => ({
  sessionKey: key,
  hostDbPath: dbPath,
  sessionId: "sid-1",
  tokenBudget: 1,
  turnCounter: 100,
});
const applied = (lo: number, hi: number, extra: Partial<LedgerMark> = {}): LedgerMark =>
  ({ mark_id: lo, seq_start: lo, seq_end: hi, action: "digest", digest: "DIGEST", applied_at: 1, ...extra }) as LedgerMark;
const texts = (list: unknown[]) => JSON.stringify(list);

describe("B10 seq-index reload", () => {
  it("reloads when a trim renumbers the tail, so survivors and new rows map", () => {
    const { dbPath } = host();
    const ix = new SeqIndex(dbPath, "sid-1");
    ix.refresh();
    expect(ix.seqOf(msg("user", 4))).toBe(7);
    trimFirstTurn(dbPath, 13);
    ix.refresh();
    expect(ix.seqOf(msg("user", 4))).toBe(5); // stale index said 7
    expect(ix.seqOf(msg("user", 13))).toBe(23); // stale index never read it
    ix.close();
  });
});

describe("B9 anchors", () => {
  it("an anchored mark follows its content across a renumber; unanchored keeps old numbers", () => {
    const { dbPath } = host();
    trimFirstTurn(dbPath); // turn 4 moved from seq 7-8 to 5-6
    const anchored = applied(7, 8, { anchor_first_event: "ev-user-4", anchor_last_event: "ev-assistant-4" });
    const res = applyMarks(view(2, 12), [anchored], ctx(dbPath, "p36-a"));
    expect(texts(res.messages)).not.toContain("user 4");
    expect(texts(res.messages)).toContain("user 3");
  });

  it("skips a mark whose anchor is gone instead of re-aiming it, and never applies retired marks", () => {
    const { dbPath } = host();
    const gone = applied(3, 4, { anchor_first_event: "ev-user-99", anchor_last_event: "ev-assistant-99" });
    const retired = applied(5, 6, { retired_at: 1 });
    const res = applyMarks(view(1, 12), [gone, retired], ctx(dbPath, "p36-b"));
    expect(res.changed).toBeFalsy();
    expect(res.skipped.join("\n")).toContain("anchor gone");
    expect(texts(res.messages)).toContain("user 3");
  });

  it("migrates an old ledger in place and runs the one-time rebase once", () => {
    const { dir, dbPath } = host();
    configureSessionLedger(dir);
    const key = "agent:main:p36";
    const opened = openSessionLedger(key, "main", dbPath, () => {});
    expect(opened.ok).toBe(true);
    const db = (opened as { db: DatabaseSync }).db;
    // Simulate a pre-anchor ledger: drop the new columns' values by reinserting plain rows.
    const ins = db.prepare(
      "INSERT INTO marks(session_id,seq_start,seq_end,kind,action,digest,reason,proposed_at,applied_gen,applied_at) VALUES('sid-1',?,?,'turn','digest','d','r',1,?,?)"
    );
    ins.run(3, 4, "gen-now", 1); // backfill
    ins.run(5, 6, "gen-old", 1); // retire
    ins.run(7, 8, null, null); // remove (pending, unanchored)
    ins.run(40, 41, "gen-now", 1); // backfill impossible (no rows) -> retire
    ensureMarkAnchorColumns(db); // idempotent on a migrated ledger
    const warns: string[] = [];
    expect(runMarkRebaseCached(key, dbPath, "sid-1", (m) => warns.push(m))).toEqual({
      backfill: 1,
      retire: 2,
      remove: 1,
    });
    expect(runMarkRebaseCached(key, dbPath, "sid-1", () => {})).toBeNull(); // once
    const marks = readMarksCached(key, () => {}).marks as LedgerMark[];
    expect(marks.map((m) => [m.seq_start, m.anchor_first_event ?? null, m.retired_at != null])).toEqual([
      [3, "ev-user-2", false],
      [5, null, true],
      [40, null, true],
    ]);
  });

  it("plans: anchored marks untouched; no current generation retires every applied mark", () => {
    const plan = planMarkRebase(
      [
        { mark_id: 1, applied_at: 1, applied_gen: "g", anchor_first_event: "a", anchor_last_event: "b" },
        { mark_id: 2, applied_at: 1, applied_gen: "g" },
      ],
      ""
    );
    expect(plan).toEqual({ backfill: [], retire: [2], remove: [] });
  });
});

describe("Codex A2/A3/A4 parity", () => {
  // A2: the saved state says g-old (the mark's generation) but the host rewrote to g-new.
  for (const gen of ["g-new", null])
    it(`rebase never pins old numbers to new content (host generation ${gen ?? "unreadable"})`, () => {
      const { dir, dbPath } = host(1, 12, gen);
      trimFirstTurn(dbPath); // the rewrite: seq 3-4 now holds turn 3, not the marked turn 2
      configureSessionLedger(dir);
      const key = `agent:main:p36-a2-${gen}`;
      const db = (openSessionLedger(key, "main", dbPath, () => {}) as { db: DatabaseSync }).db;
      db.prepare(
        "INSERT INTO marks(session_id,seq_start,seq_end,kind,action,digest,reason,proposed_at,applied_gen,applied_at) VALUES('sid-1',3,4,'turn','digest','d','r',1,'g-old',1)"
      ).run();
      expect(runMarkRebaseCached(key, dbPath, "sid-1", () => {})).toEqual({ backfill: 0, retire: 1, remove: 0 });
      const [m] = readMarksCached(key, () => {}).marks as LedgerMark[];
      expect([m.anchor_first_event ?? null, m.retired_at != null]).toEqual([null, true]);
    });

  it("R36.1: a rebase that cannot take the write lock leaves legacy numeric marks unapplied, then retries", () => {
    const { dir, dbPath } = host(1, 12, "g-new");
    trimFirstTurn(dbPath);
    configureSessionLedger(dir);
    const key = "agent:main:p36-r361";
    const db = (openSessionLedger(key, "main", dbPath, () => {}) as { db: DatabaseSync }).db;
    db.exec("PRAGMA busy_timeout=1");
    db.prepare(
      "INSERT INTO marks(session_id,seq_start,seq_end,kind,action,digest,reason,proposed_at,applied_gen,applied_at) VALUES('sid-1',3,4,'turn','digest','d','r',1,'g-old',1)"
    ).run();
    const writer = new DatabaseSync(ledgerPath(key));
    writer.exec("BEGIN IMMEDIATE");
    try {
      expect(runMarkRebaseCached(key, dbPath, "sid-1", () => {})).toBeNull();
      const read = readMarksCached(key, () => {});
      expect(read.ok).toBe(true);
      expect(read.marks).toEqual([]);
      const res = applyMarks(view(2, 12), read.marks as never, ctx(dbPath, key));
      expect(texts(res.messages)).toContain("user 3");
    } finally {
      writer.exec("ROLLBACK");
      writer.close();
    }
    expect(runMarkRebaseCached(key, dbPath, "sid-1", () => {})).toEqual({ backfill: 0, retire: 1, remove: 0 });
  });
  it("A3: anchors come from compressed (event_zstd) rows", () => {
    const dir = mkdtempSync(join(tmpdir(), "p36-z-"));
    dirs.push(dir);
    const dbPath = join(dir, "host.sqlite");
    const db = new DatabaseSync(dbPath);
    db.exec("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT, event_zstd BLOB, event_utf8_bytes INTEGER)");
    const ins = db.prepare("INSERT INTO transcript_events VALUES ('sid-1',?,NULL,?,?)");
    for (let s = 1; s <= 6; s++) {
      const bytes = Buffer.from(JSON.stringify({ id: `e${s}`, message: msg("user", s) }), "utf8");
      ins.run(s, zstdCompressSync(bytes), bytes.length);
    }
    db.close();
    const ht = HostTranscript.open(dbPath, "sid-1");
    expect(ht.anchorsFor(4, 6)).toEqual({ first: "e4", last: "e6" });
    ht.close();
  });

  it("A4: with no marks the view count is the final view's count", async () => {
    const messages = view(1, 6);
    const out = await createContextEngine(
      { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} } as never,
      {}
    ).assemble({ messages, sessionKey: "agent:main:p36-a4", sessionId: "sid-1", tokenBudget: 100_000 } as never);
    expect(out.messages).toEqual(messages);
    expect(out.estimatedTokens).toBe(estimateTokens(messages));
    expect(out.estimatedTokens).toBeGreaterThan(0);
  });
});

describe("B4 retain scope", () => {
  it("default skips only heartbeat/cron/poll/status sessions, in both key forms", () => {
    for (const k of [
      "agent:main:telegram:group:-100:topic:22",
      "agent:main:telegram:group:-100:topic:1201",
      "agent:db:subagent:8cdb0690",
      "agent_main_subagent_8cdb0690",
      "agent:main:main",
    ])
      expect(shouldQueueRetain(k)).toBe(true);
    for (const k of [
      "agent:astinus:cron:cd29bb61:run:0d8e5b5c",
      "agent_astinus_cron_cd29bb61_run_0d8e5b5c",
      "agent_gardener_main_heartbeat",
      "agent_main_dashboard_x",
      "agent:main:keepalive",
      "agent:ops:status:poll",
      "  ",
      undefined,
    ])
      expect(shouldQueueRetain(k)).toBe(false);
  });

  it("config skip patterns replace the default; a non-empty allowlist overrides", () => {
    const skip = { skipSessionPatterns: ["subagent"] };
    expect(shouldQueueRetain("agent:db:subagent:1", skip)).toBe(false);
    expect(shouldQueueRetain("agent:main:heartbeat", skip)).toBe(true);
    const allow = { allowSessionPatterns: ["topic:(22|1201)$", "(bad"] };
    expect(shouldQueueRetain("agent:main:telegram:group:-100:topic:22", allow)).toBe(true);
    expect(shouldQueueRetain("agent:main:telegram:group:-100:topic:7", allow)).toBe(false);
    expect(shouldQueueRetain("agent:db:subagent:1", { allowSessionPatterns: [] })).toBe(true);
    // R36.2: all-invalid nonempty allowlist stays in allowlist mode and matches nothing.
    expect(shouldQueueRetain("agent:main:telegram:group:-100:topic:7", { allowSessionPatterns: ["("] })).toBe(false);
  });
});
describe("R36.3 subagent retention through the real session_end hook", () => {
  for (const exclude of [false, true])
    it(`defaults retain subagent work; ignoreSessionPatterns excludes it (exclude=${exclude})`, async () => {
      const root = mkdtempSync(join(tmpdir(), "p36-sub-"));
      dirs.push(root);
      const key = "agent:main:subagent:child-123";
      const sid = "subagent-sid";
      const ctx = { agentId: "main", sessionId: sid, sessionKey: key, messageProvider: "telegram", senderId: "u1",
        contextEngineTurn: { enginePluginId: "hindsight-openclaw", logicalTurnId: "turn-1" } };
      const config = { hindsightApiUrl: "http://synthetic.invalid:8888", autoRecall: false, autoRetain: true,
        durableTurns: true, retainEveryNTurns: 1, retainQueuePath: join(root, "queue.jsonl"), dynamicBankId: false,
        bankId: "synthetic-bank", logLevel: "error", ...(exclude ? { ignoreSessionPatterns: ["agent:*:subagent:**"] } : {}) };
      vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ status: "ok", api_version: "0.10.0", features: {} }))));
      let service: any;
      const handlers = new Map<string, any>();
      plugin({ config: { plugins: { entries: { "hindsight-openclaw": { config } } } }, runtime: { state: { resolveStateDir: () => root } },
        registerService: (s: any) => (service = s), registerContextEngine() {}, on: (n: string, fn: any) => handlers.set(n, fn),
        logger: { info() {}, warn() {}, error() {} } } as never);
      let calls = 0;
      try {
        await service.start();
        vi.spyOn(lifecycleAuthorityFor(root).client!, "retain").mockImplementation(async () => { calls++; return { success: true } as never; });
        updateSessionState(root, key, (s) => ({ ...s, committedKeys: ["work-1"], lastGenerationSeen: "g",
          pendingRetain: [{ start: 1, end: 2, key: "work-1", sessionId: sid, at: Date.now(), messages: [
            { role: "user", content: "The subagent researched the deployment issue and verified dependency versions." },
            { role: "assistant", content: "Finding: the deployment requires the host capability before durable turns." }] }] }) as never);
        await handlers.get("session_end")({ sessionId: sid, sessionKey: key }, ctx);
      } finally {
        await service.stop();
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
      }
      expect(calls).toBe(exclude ? 0 : 1);
    }, 15000);
});