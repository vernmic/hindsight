import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { applyMarks, type LedgerMark } from "./assemble-marks.js";
import { runPass, type PassPayload } from "./astinus-pass.js";
import {
  closeAllLedgers,
  configureSessionLedger,
  insertCommittedTurn,
  ledgerPath,
} from "./session-ledger.js";

// Package 28: the classification pass against a real temp ledger and a synthetic host store.
// Providers are faked fetches; no network, no real host.

const KEY = "agent:main:pass";
const SID = "sid-pass";
const dirs: string[] = [];
afterEach(() => {
  closeAllLedgers();
  for (const d of dirs.splice(0)) {
    try {
      rmSync(d, { recursive: true, force: true });
    } catch {
      /* Windows may still hold a sqlite handle */
    }
  }
});

/** n turns of [user, assistant interior, assistant final], seqs 3t+1..3t+3. */
function hostStore(dir: string, turns: number) {
  const dbPath = join(dir, "host.sqlite");
  const db = new DatabaseSync(dbPath);
  db.exec(
    "CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT, created_at INTEGER)"
  );
  const ins = db.prepare("INSERT INTO transcript_events VALUES (?,?,?,?)");
  const messages: Array<Record<string, unknown>> = [];
  let seq = 1;
  for (let t = 1; t <= turns; t++) {
    for (const [role, text] of [
      ["user", `question ${t} about HindsightBank`],
      ["assistant", `thinking ${t}`],
      ["assistant", `answer ${t}`],
    ]) {
      const m = { role, content: [{ type: "text", text }], idempotencyKey: `ik-${seq}` };
      messages.push(m);
      ins.run(SID, seq, JSON.stringify({ id: `ev-${seq}`, message: m }), 1_000_000 + seq * 1000);
      seq++;
    }
  }
  db.close();
  return { dbPath, messages };
}

function setup(turns: number) {
  const root = mkdtempSync(join(tmpdir(), "astinus-pass-"));
  dirs.push(root);
  const { dbPath, messages } = hostStore(root, turns);
  configureSessionLedger(root);
  for (let t = 0; t < turns; t++) {
    const row = {
      sessionKey: KEY,
      agentId: "main",
      hostDbPath: dbPath,
      sessionId: SID,
      seqStart: 3 * t + 1,
      seqEnd: 3 * t + 3,
      generation: "g",
      advancementKey: `adv-${t}`,
      heartbeat: false,
    };
    expect(insertCommittedTurn(row, () => {}).ok).toBe(true);
  }
  closeAllLedgers();
  const payload: PassPayload = {
    root,
    session_key: KEY,
    session_id: SID,
    agent_id: "main",
    host_db_path: dbPath,
    range: [3 * turns - 2, 3 * turns],
    view_tokens: 900,
    token_budget: 1000,
    estimator: "host",
    window_first_seq: null,
  };
  const ledger = () => new DatabaseSync(ledgerPath(KEY), { readOnly: true });
  return { root, dbPath, messages, payload, ledger };
}

const answer = (body: object) =>
  new Response(
    JSON.stringify({
      choices: [{ message: { content: JSON.stringify(body) }, finish_reason: "stop" }],
    }),
    { status: 200 }
  );

describe("turn_middle age (F2, turnMiddleAgeTurns = 6)", () => {
  it("the pass proposes a turn with 6 newer turns and not one with 5", async () => {
    const { payload, ledger } = setup(8); // turn i (1-based) has 8 - i newer turns
    expect(await runPass({ ...payload, turn_middle_age_turns: 6 })).toBe("error"); // no provider
    const db = ledger();
    const marks = db
      .prepare("SELECT seq_start, seq_end, kind, action, reason FROM marks ORDER BY seq_start")
      .all() as Array<Record<string, unknown>>;
    db.close();
    // turns 1 (7 newer) and 2 (6 newer); turn 3 has 5 newer
    expect(marks.map((m) => [m.seq_start, m.seq_end])).toEqual([
      [1, 3],
      [4, 6],
    ]);
    expect(marks[0]).toMatchObject({ kind: "turn", action: "digest" });
    expect(String(marks[0].reason).startsWith("turn_middle")).toBe(true);
  });

  it("applyMarks redacts a turn with 6 newer turns and not one with 5", () => {
    const root = mkdtempSync(join(tmpdir(), "astinus-apply-"));
    dirs.push(root);
    // turn 1 holds the first user message (never eligible), so the 6/5 pair is turns 2 and 3
    const { dbPath, messages } = hostStore(root, 8);
    const tm = (s: number): LedgerMark => ({
      mark_id: s,
      seq_start: s,
      seq_end: s + 2,
      kind: "turn",
      action: "digest",
      reason: "turn_middle: interior redactable; boundaries and steers preserved",
      pressure: 1,
      topic_dormancy_s: 10,
      stmt_age_s: 10,
    });
    const res = applyMarks(messages, [tm(4), tm(7)], {
      sessionKey: "agent:main:apply-f2",
      hostDbPath: dbPath,
      sessionId: SID,
      tokenBudget: 1,
      turnCounter: 1,
      turnMiddleAgeTurns: 6,
    });
    const text = JSON.stringify(res.messages);
    expect(text).not.toContain("thinking 2"); // turn 2: 6 newer turns - redacted
    expect(text).toContain("turn interior redacted");
    expect(text).toContain("thinking 3"); // turn 3: 5 newer turns - kept
    expect(text).toContain("question 2 about"); // boundaries preserved
    expect(text).toContain("answer 2");
  });
});

describe("classification pass", () => {
  it("writes the model's classification, filters marks, and never writes the host store", async () => {
    const { payload, dbPath, ledger } = setup(10);
    const hostHash = () => createHash("sha256").update(readFileSync(dbPath)).digest("hex");
    const before = hostHash();
    const fetchMock = vi.fn(async () =>
      answer({
        class: "topic",
        topics: [
          {
            slug: "bank-tuning",
            label: "Bank tuning",
            confidence: 0.9,
            subjects: [
              { subject: "HindsightBank", object: "recall" },
              { subject: "Invented", object: "x" },
            ],
          },
        ],
        supersessions: [],
        marks: [
          { range: [2, 2], action: "drop", reason: "old" },
          { range: [28, 30], action: "drop", reason: "newest turn" },
        ],
      })
    );
    const providers = [
      { name: "p1", url: "https://p1.invalid/v1/chat/completions", model: "m1", apiKey: "k1" },
    ];
    expect(
      await runPass({ ...payload, providers }, { fetch: fetchMock as never, now: () => 2_000_000 })
    ).toBe("ok");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(providers[0].url);
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer k1");
    const db = ledger();
    expect(db.prepare("SELECT slug FROM topics").all()).toEqual([{ slug: "bank-tuning" }]);
    expect(db.prepare("SELECT subject FROM topic_subjects").all()).toEqual([
      { subject: "HindsightBank" },
    ]);
    expect(db.prepare("SELECT source FROM turn_topics").all()).toEqual([{ source: "model" }]);
    const marks = db
      .prepare("SELECT seq_start, seq_end, stmt_age_s, pressure FROM marks WHERE reason = 'old'")
      .all();
    expect(marks).toEqual([{ seq_start: 2, seq_end: 2, stmt_age_s: 2000 - 1002, pressure: 0.9 }]);
    expect(db.prepare("SELECT COUNT(*) AS n FROM marks WHERE seq_start = 28").get()).toEqual({
      n: 0,
    });
    expect(db.prepare("SELECT outcome, model FROM passes").all()).toEqual([
      { outcome: "ok", model: "m1" },
    ]);
    db.close();
    expect(hostHash()).toBe(before);
  });

  it("falls back in order, each provider with its own key, and records why", async () => {
    const { payload, ledger } = setup(3);
    const fetchMock = vi.fn(async (url: string) =>
      url.includes("p1")
        ? new Response("no", { status: 500, statusText: "boom" })
        : answer({ class: "random", topics: [], marks: [] })
    );
    const providers = [
      { name: "p1", url: "https://p1.invalid/c", model: "m1", apiKey: "k1" },
      { name: "p2", url: "https://p2.invalid/c", model: "m2", apiKey: "k2" },
    ];
    expect(await runPass({ ...payload, providers }, { fetch: fetchMock as never })).toBe("ok");
    const auths = fetchMock.mock.calls.map(
      (c) =>
        ((c as unknown as [string, RequestInit])[1].headers as Record<string, string>).Authorization
    );
    expect(auths).toEqual(["Bearer k1", "Bearer k2"]);
    const db = ledger();
    const row = db
      .prepare("SELECT model, fallback_from, fallback_reason FROM passes")
      .get() as Record<string, string>;
    db.close();
    expect(row.model).toBe("m2");
    expect(row.fallback_from).toBe("p1");
    expect(row.fallback_reason).toContain("primary-failed: p1:HTTP Error 500");
  });

  it("refuses a ledger bound to another host store", async () => {
    const { payload, ledger } = setup(2);
    expect(await runPass({ ...payload, host_db_path: payload.host_db_path + ".other" })).toBe(
      "skipped-host-binding-mismatch"
    );
    const db = ledger();
    expect(db.prepare("SELECT COUNT(*) AS n FROM passes").get()).toEqual({ n: 0 });
    db.close();
  });
});

describe("package 31: incarnation attribution and terminal reservations", () => {
  const providers = [{ name: "p1", url: "https://p1.invalid/c", model: "m1", apiKey: "k1" }];
  const ok = () => vi.fn(async () => answer({ class: "random", topics: [], marks: [] }));
  const otherTurn = (dbPath: string, seqStart: number) => {
    expect(
      insertCommittedTurn(
        {
          sessionKey: KEY,
          agentId: "main",
          hostDbPath: dbPath,
          sessionId: "new-incarnation",
          seqStart,
          seqEnd: seqStart + 2,
          generation: "g",
          advancementKey: `adv-new-${seqStart}`,
          heartbeat: false,
        },
        () => {}
      ).ok
    ).toBe(true);
    closeAllLedgers();
  };

  it("R28b.1: reused coordinates classify the payload's own incarnation only", async () => {
    const { payload, dbPath, ledger } = setup(10);
    otherTurn(dbPath, 28); // same [28,30] as SID's newest turn
    // inside the 120s window: only a terminal row lets this one run
    expect(
      await runPass({ ...payload, providers }, { fetch: ok() as never, now: () => 2_000_100 })
    ).toBe("ok");
    const db = ledger();
    const rows = db
      .prepare("SELECT session_id, classified_at FROM turns WHERE seq_start = 28 ORDER BY turn_id")
      .all() as Array<Record<string, unknown>>;
    db.close();
    expect(rows[0]).toMatchObject({ session_id: SID }); // same-incarnation control
    expect(rows[0].classified_at).not.toBeNull();
    expect(rows[1]).toMatchObject({ session_id: "new-incarnation", classified_at: null });
  });

  it("R28b.1: a missing owned turn never attributes to another incarnation", async () => {
    const { payload, dbPath, ledger } = setup(3);
    otherTurn(dbPath, 40);
    await runPass({ ...payload, range: [40, 42], providers }, { fetch: ok() as never });
    const db = ledger();
    const other = db
      .prepare("SELECT turn_id FROM turns WHERE session_id = 'new-incarnation'")
      .get() as Record<string, number>;
    const passTurns = db.prepare("SELECT turn_id FROM passes").all() as Array<
      Record<string, number>
    >;
    db.close();
    expect(passTurns.map((p) => p.turn_id)).not.toContain(other.turn_id);
  });

  it("R28b.2: non-array marks fall back, finish the pass, and do not block the next", async () => {
    const { payload, ledger } = setup(3);
    for (const marks of [{}, "x", 5]) {
      const fetchMock = vi.fn(async () => answer({ class: "random", topics: [], marks }));
      expect(await runPass({ ...payload, providers }, { fetch: fetchMock as never })).toBe(
        "fallback"
      );
    }
    const db = ledger();
    const rows = db.prepare("SELECT outcome, finished_at FROM passes").all() as Array<
      Record<string, unknown>
    >;
    db.close();
    expect(rows.map((r) => r.outcome)).toEqual(["fallback", "fallback", "fallback"]);
    expect(rows.every((r) => r.finished_at !== null)).toBe(true);
  });

  it("R28b.2: an error after the reservation still records a terminal outcome", async () => {
    const { payload, ledger } = setup(3);
    let calls = 0;
    const now = () => {
      calls++;
      if (calls === 2) throw new Error("clock fault");
      return 2_000_000 + calls;
    };
    expect(await runPass({ ...payload, providers }, { fetch: ok() as never, now })).toBe("failed");
    // inside the 120s window: only a terminal row lets this one run
    expect(
      await runPass({ ...payload, providers }, { fetch: ok() as never, now: () => 2_000_100 })
    ).toBe("ok");
    const db = ledger();
    const rows = db
      .prepare("SELECT outcome, finished_at FROM passes ORDER BY pass_id")
      .all() as Array<Record<string, unknown>>;
    db.close();
    expect(rows.map((r) => r.outcome)).toEqual(["failed", "ok"]);
    expect(rows.every((r) => r.finished_at !== null)).toBe(true);
  });

  it("gate: proposals below 0.40 are discarded by code, not by the model (turn-203 case)", async () => {
    const { payload, ledger } = setup(3);
    const fetchMock = vi.fn(async () =>
      answer({
        class: "topic",
        topics: [],
        supersessions: [],
        marks: [{ range: [1, 3], action: "placeholder", digest: null, reason: "below gate" }],
      })
    );
    // 386/1000 = 0.386: the pressure that produced 26 marks in production parity.
    expect(
      await runPass(
        { ...payload, view_tokens: 386, token_budget: 1000, providers, turn_middle_age_turns: 99 },
        { fetch: fetchMock as never }
      )
    ).toBe("ok");
    const db = ledger();
    const row = db.prepare("SELECT count(*) AS c FROM marks").get() as { c: number };
    db.close();
    expect(row.c).toBe(0);
  });
});
