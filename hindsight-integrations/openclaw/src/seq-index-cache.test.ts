import { afterEach, describe, expect, it } from "vitest";
import { existsSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { clearSeqIndexes, seqIndexDebug } from "./assemble-marks.js";

const dirs: string[] = [];
afterEach(() => {
  clearSeqIndexes();
  for (const d of dirs.splice(0)) {
    try {
      rmSync(d, { recursive: true, force: true });
    } catch {
      /* windows handle */
    }
  }
});

function hostDb(events: number): string {
  const dir = mkdtempSync(join(tmpdir(), "astinus-seq-"));
  dirs.push(dir);
  const dbPath = join(dir, "host.sqlite");
  const db = new DatabaseSync(dbPath);
  db.exec("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT, event_zstd BLOB)");
  db.exec(
    "CREATE TABLE transcript_event_identities (session_id TEXT, event_id TEXT, seq INTEGER, event_type TEXT, parent_id TEXT, message_idempotency_key TEXT, created_at INTEGER)"
  );
  const insEvent = db.prepare("INSERT INTO transcript_events (session_id, seq, event_json) VALUES (?,?,?)");
  const insId = db.prepare(
    "INSERT INTO transcript_event_identities (session_id, event_id, seq, event_type, message_idempotency_key) VALUES (?,?,?,?,?)"
  );
  for (let i = 1; i <= events; i++) {
    const id = "ev" + i;
    const body = JSON.stringify({
      id,
      message: { role: "user", idempotencyKey: "k" + i, timestamp: i },
    });
    insEvent.run("s1", i, body);
    insId.run("s1", id, i, "message", "k" + i);
  }
  db.close();
  return dbPath;
}

function cacheFiles(dir: string): string[] {
  try {
    return readdirSync(dir).filter((f) => f.endsWith(".json"));
  } catch {
    return [];
  }
}

describe("identity-index cache", () => {
  it("persists the map and reloads it in a later instance", () => {
    const dbPath = hostDb(50);
    const cacheDir = join(dbPath, "..", "seq-cache");
    const first = seqIndexDebug(dbPath, "s1", cacheDir);
    expect(first.fromCache).toBe(false);
    expect(first.size).toBeGreaterThanOrEqual(50);
    expect(cacheFiles(cacheDir)).toHaveLength(1);

    clearSeqIndexes();
    const second = seqIndexDebug(dbPath, "s1", cacheDir);
    expect(second.fromCache).toBe(true);
    expect(second.size).toBe(first.size);
    expect(second.lastSeq).toBe(first.lastSeq);
  });

  it("picks up new rows after a cached load", () => {
    const dbPath = hostDb(20);
    const cacheDir = join(dbPath, "..", "seq-cache");
    seqIndexDebug(dbPath, "s1", cacheDir);
    const before = seqIndexDebug(dbPath, "s1", cacheDir).size;

    const db = new DatabaseSync(dbPath);
    db.prepare("INSERT INTO transcript_events (session_id, seq, event_json) VALUES (?,?,?)").run(
      "s1",
      21,
      JSON.stringify({ id: "ev21", message: { role: "user", idempotencyKey: "k21", timestamp: 21 } })
    );
    db.prepare(
      "INSERT INTO transcript_event_identities (session_id, event_id, seq, event_type, message_idempotency_key) VALUES (?,?,?,?,?)"
    ).run("s1", "ev21", 21, "message", "k21");
    db.close();

    const after = seqIndexDebug(dbPath, "s1", cacheDir);
    expect(after.size).toBe(before + 1);
    expect(after.lastSeq).toBe(21);
  });

  it("rebuilds when the cache file is corrupt", () => {
    const dbPath = hostDb(30);
    const cacheDir = join(dbPath, "..", "seq-cache");
    seqIndexDebug(dbPath, "s1", cacheDir);
    const files = cacheFiles(cacheDir);
    expect(files).toHaveLength(1);
    writeFileSync(join(cacheDir, files[0]), "{not json", "utf-8");

    clearSeqIndexes();
    const rebuilt = seqIndexDebug(dbPath, "s1", cacheDir);
    expect(rebuilt.fromCache).toBe(false);
    expect(rebuilt.size).toBeGreaterThanOrEqual(30);
    expect(existsSync(join(cacheDir, files[0]))).toBe(true);
  });

  it("rebuilds when the transcript tail moved", () => {
    const dbPath = hostDb(10);
    const cacheDir = join(dbPath, "..", "seq-cache");
    seqIndexDebug(dbPath, "s1", cacheDir);

    // Rewrite the last row under a new event id: the watermark no longer matches.
    const db = new DatabaseSync(dbPath);
    db.prepare("UPDATE transcript_events SET event_json = ? WHERE seq = 10").run(
      JSON.stringify({ id: "ev10-replaced", message: { role: "user", idempotencyKey: "k10", timestamp: 10 } })
    );
    db.close();

    clearSeqIndexes();
    const rebuilt = seqIndexDebug(dbPath, "s1", cacheDir);
    expect(rebuilt.fromCache).toBe(true); // loaded, then invalidated by the watermark check
    expect(rebuilt.size).toBeGreaterThanOrEqual(10);
  });

  it("keeps the same answers with and without a cache", () => {
    const dbPath = hostDb(40);
    const cacheDir = join(dbPath, "..", "seq-cache");
    const cached = seqIndexDebug(dbPath, "s1", cacheDir);
    clearSeqIndexes();
    const uncached = seqIndexDebug(dbPath, "s1", undefined);
    expect(uncached.size).toBe(cached.size);
    expect(uncached.lastSeq).toBe(cached.lastSeq);
  });
});
