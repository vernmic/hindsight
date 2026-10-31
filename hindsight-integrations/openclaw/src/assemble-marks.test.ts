import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { zstdCompressSync } from "node:zlib";
import { afterEach, describe, expect, it } from "vitest";
import {
  applyMarks,
  decodeTranscriptEventJson,
  estimateTokensHost,
  type LedgerMark,
} from "./assemble-marks.js";

// Admission policy tests (Vern 2026-09-21; B1 fix per Claude's review). They run against a real temp
// host store so the seq index resolves and marks actually apply - admission and the debounce stamp
// both sit behind a successful application, so a storeless test would assert nothing. Each test uses
// its OWN session key: `lastAdmissionTurn` is module-level and keyed by session, so sharing a key
// leaks the debounce across tests.

const dirs: string[] = [];
afterEach(() => {
  for (const d of dirs.splice(0)) {
    // Windows can hold the sqlite handle a moment after close; a failed cleanup must not fail a
    // passing test (the EPERM-in-afterEach gotcha this box is known for).
    try {
      rmSync(d, { recursive: true, force: true });
    } catch {
      /* tolerated - the OS reaps its own temp dir */
    }
  }
});

function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "assemble-marks-"));
  dirs.push(dir);
  const dbPath = join(dir, "host.sqlite");
  const db = new DatabaseSync(dbPath);
  db.exec("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT)");
  const messages: Array<Record<string, unknown>> = [];
  const ins = db.prepare(
    "INSERT INTO transcript_events (session_id, seq, event_json) VALUES (?,?,?)"
  );
  let seq = 1;
  for (let turn = 1; turn <= 12; turn++) {
    for (const role of ["user", "assistant"] as const) {
      const m = {
        role,
        content: [{ type: "text", text: `${role} ${turn}` }],
        idempotencyKey: `ik-${role}-${turn}`,
      };
      messages.push(m);
      ins.run("sid-1", seq, JSON.stringify({ message: m }));
      seq += 1;
    }
  }
  db.close();
  return { dbPath, messages };
}

function mark(seqStart: number, pressure: number, dormancy = 10, age = 10): LedgerMark {
  // a digest mark MUST carry its one-line digest, or the application has nothing to put in place
  return {
    mark_id: seqStart,
    seq_start: seqStart,
    seq_end: seqStart + 1,
    action: "digest",
    digest: "digest line",
    pressure,
    topic_dormancy_s: dormancy,
    stmt_age_s: age,
  } as LedgerMark;
}

function ctxFor(
  dbPath: string,
  messages: unknown[],
  pressure: number,
  turnCounter: number,
  sessionKey: string
) {
  const before = estimateTokensHost(messages);
  return {
    sessionKey,
    hostDbPath: dbPath,
    sessionId: "sid-1",
    tokenBudget: Math.max(1, Math.ceil(before / pressure)),
    turnCounter,
  };
}

const skippedOf = (res: { skipped: string[] }) => res.skipped.join(" | ");

describe("marks admission policy", () => {
  it("admits the share rounded up: one pending at 0.30 admits one", () => {
    const { dbPath, messages } = fixture();
    const res = applyMarks(messages, [mark(1, 0.5)], ctxFor(dbPath, messages, 0.3, 100, "p-share"));
    expect(skippedOf(res)).toContain("admitted=1/1");
    expect(res.admitted.length).toBe(1);
  });

  it("holds a second admission inside the debounce window", () => {
    const { dbPath, messages } = fixture();
    applyMarks(messages, [mark(1, 0.5)], ctxFor(dbPath, messages, 0.4, 100, "p-holds"));
    const held = applyMarks(
      messages,
      [mark(3, 0.5)],
      ctxFor(dbPath, messages, 0.4, 101, "p-holds")
    );
    expect(skippedOf(held)).toContain("debounce 1/5");
    expect(held.admitted.length).toBe(0);
  });

  it("expires the debounce five turns after an admission and admits again", () => {
    const { dbPath, messages } = fixture();
    const first = applyMarks(
      messages,
      [mark(1, 0.5)],
      ctxFor(dbPath, messages, 0.4, 100, "p-expires")
    );
    expect(first.admitted.length).toBe(1);
    for (let t = 101; t <= 104; t++) {
      const held = applyMarks(
        messages,
        [mark(3, 0.5)],
        ctxFor(dbPath, messages, 0.4, t, "p-expires")
      );
      expect(skippedOf(held)).toContain("debounce");
    }
    const again = applyMarks(
      messages,
      [mark(3, 0.5)],
      ctxFor(dbPath, messages, 0.4, 105, "p-expires")
    );
    expect(again.admitted.length).toBe(1);
  });

  it("resets the debounce below the floor, so crossing 0.30 admits immediately", () => {
    const { dbPath, messages } = fixture();
    applyMarks(messages, [mark(1, 0.5)], ctxFor(dbPath, messages, 0.4, 100, "p-resets"));
    const idle = applyMarks(
      messages,
      [mark(3, 0.5)],
      ctxFor(dbPath, messages, 0.2, 101, "p-resets")
    );
    expect(skippedOf(idle)).toContain("idle; debounce reset");
    const next = applyMarks(
      messages,
      [mark(3, 0.5)],
      ctxFor(dbPath, messages, 0.4, 102, "p-resets")
    );
    expect(next.admitted.length).toBe(1);
  });

  it("never touches the preserved tail", () => {
    const { dbPath, messages } = fixture();
    const tailSeq = messages.length - 1;
    const res = applyMarks(
      messages,
      [mark(tailSeq, 0.9)],
      ctxFor(dbPath, messages, 0.9, 100, "p-tail")
    );
    expect(res.changed).toBe(false);
    expect(res.marksInEffect).toBe(0);
  });
});

// R7.1 regression: the host stores large events compressed (event_json NULL, event_zstd Zstd frame,
// event_utf8_bytes original length). Identity, compressed and mixed stores must map identically.
describe("host transcript payload codecs", () => {
  type Codec = "identity" | "zstd" | "mixed";
  function codecStore(codec: Codec) {
    const dir = mkdtempSync(join(tmpdir(), "assemble-marks-codec-"));
    dirs.push(dir);
    const dbPath = join(dir, "host.sqlite");
    const db = new DatabaseSync(dbPath);
    db.exec(
      "CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT, event_zstd BLOB, event_utf8_bytes INTEGER)"
    );
    const insert = db.prepare("INSERT INTO transcript_events VALUES (?,?,?,?,?)");
    const messages: Array<Record<string, unknown>> = [];
    for (let turn = 1; turn <= 12; turn++) {
      for (const role of ["user", "assistant"]) {
        const message = {
          role,
          idempotencyKey: `${role}-${turn}`,
          content: [{ type: "text", text: `${role} ${turn}: ` + "synthetic content ".repeat(150) }],
        };
        messages.push(message);
        const json = JSON.stringify({ message });
        const bytes = Buffer.from(json, "utf8");
        const zstd = codec === "zstd" || (codec === "mixed" && messages.length % 2 === 0);
        insert.run(
          "synthetic-session",
          messages.length,
          zstd ? null : json,
          zstd ? zstdCompressSync(bytes) : null,
          bytes.length
        );
      }
    }
    return { db, dbPath, messages };
  }
  function run(codec: Codec, dbPath: string, messages: unknown[], key = `codec-${codec}`) {
    return applyMarks(messages, [{ mark_id: 1, seq_start: 2, seq_end: 2, action: "drop" }], {
      sessionKey: key,
      hostDbPath: dbPath,
      sessionId: "synthetic-session",
      tokenBudget: Math.ceil(estimateTokensHost(messages) / 0.9),
      turnCounter: 100,
    });
  }

  for (const codec of ["identity", "zstd", "mixed"] as const) {
    it(`applies a seq-2 mark on ${codec} rows (24 -> 23)`, () => {
      const { db, dbPath, messages } = codecStore(codec);
      db.close();
      const res = run(codec, dbPath, messages);
      expect(res.readError).toBeUndefined();
      expect(res.changed).toBe(true);
      expect(res.admitted.map((m) => m.mark_id)).toEqual([1]);
      expect(messages.length).toBe(24);
      expect(res.messages.length).toBe(23);
      expect(res.windowFirstSeq).toBe(1);
    });
  }

  it("reports an undecodable row instead of indexing past it, and recovers once readable", () => {
    const { db, dbPath, messages } = codecStore("zstd");
    const good = db.prepare("SELECT event_zstd FROM transcript_events WHERE seq = 2").get() as {
      event_zstd: Uint8Array;
    };
    db.prepare("UPDATE transcript_events SET event_zstd = ? WHERE seq = 2").run(
      Buffer.from("not a zstd frame")
    );
    const bad = run("zstd", dbPath, messages, "codec-corrupt");
    expect(bad.readError).toMatch(/^seq 2: /);
    expect(bad.changed).toBe(false);
    expect(bad.admitted).toEqual([]);
    expect(bad.messages).toBe(messages);
    db.prepare("UPDATE transcript_events SET event_zstd = ? WHERE seq = 2").run(good.event_zstd);
    db.close();
    const fixed = run("zstd", dbPath, messages, "codec-corrupt");
    expect(fixed.readError).toBeUndefined();
    expect(fixed.messages.length).toBe(23);
  });

  it("decodes per the host bounds", () => {
    const json = JSON.stringify({ message: { role: "user" } });
    const bytes = Buffer.from(json, "utf8");
    expect(decodeTranscriptEventJson({ event_json: json })).toBe(json);
    expect(
      decodeTranscriptEventJson({
        event_json: null,
        event_zstd: zstdCompressSync(bytes),
        event_utf8_bytes: bytes.length,
      })
    ).toBe(json);
    expect(() =>
      decodeTranscriptEventJson({
        event_json: null,
        event_zstd: zstdCompressSync(bytes),
        event_utf8_bytes: bytes.length + 1,
      })
    ).toThrow(/length does not match/);
    expect(() => decodeTranscriptEventJson({ event_json: null, event_zstd: null })).toThrow();
    expect(() =>
      decodeTranscriptEventJson({
        event_json: null,
        event_zstd: new Uint8Array(0),
        event_utf8_bytes: 1,
      })
    ).toThrow(/bounds/);
  });
});
