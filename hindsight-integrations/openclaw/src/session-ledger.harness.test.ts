// Gate L0 harness (plan §11a, Q1 option (i)): prove session-ledger.ts's contract — "returns a
// result, never throws" — without the gateway. Each case runs against a temp root with its own
// session key, because the module caches open ledgers and disabled entries per key for the life
// of the process.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { copyFileSync, mkdtempSync, mkdirSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";
import {
  closeAllLedgers,
  configureSessionLedger,
  insertCommittedTurn,
  ledgerPath,
  readMarksCached,
  type TurnInsert,
} from "./session-ledger.js";

const warn = (_m: string): void => {};
const HOST_DB = "C:\\nonexistent\\openclaw-agent.sqlite";

let root = "";
let n = 0;

function key(): string {
  n += 1;
  return `agent:test:case${n}`;
}

function row(sessionKey: string, advancementKey: string, lo = 1, hi = 3): TurnInsert {
  return {
    sessionKey,
    agentId: "test",
    hostDbPath: HOST_DB,
    sessionId: "sid-" + sessionKey,
    seqStart: lo,
    seqEnd: hi,
    generation: "gen-1",
    advancementKey,
    heartbeat: false,
  };
}

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), "astinus-ledger-"));
  configureSessionLedger(root);
});

afterEach(() => {
  // Release the cached handles first — on Windows an open handle blocks the unlink, so without
  // this the temp root survives and the case looks like a failure (see
  // research/ASTINUS-WINDOWS-FILE-LOCKING-2026-09-14.md).
  closeAllLedgers();
  try {
    rmSync(root, { recursive: true, force: true });
  } catch {
    /* best effort; the OS reaps what it can */
  }
});

describe("Gate L0 harness — session-ledger contract", () => {
  it("(a) a locked ledger degrades: returns ok:false and throws nothing", () => {
    const k = key();
    expect(insertCommittedTurn(row(k, "adv-a1"), warn).ok).toBe(true);
    const holder = new DatabaseSync(ledgerPath(k));
    holder.exec("PRAGMA busy_timeout = 0");
    holder.exec("BEGIN IMMEDIATE");
    let result: { ok: boolean; reason?: string } | undefined;
    expect(() => {
      result = insertCommittedTurn(row(k, "adv-a2", 4, 6), warn);
    }).not.toThrow();
    expect(result?.ok).toBe(false);
    holder.exec("ROLLBACK");
    holder.close();
  });

  it("(b) a missing file is created and the turn lands", () => {
    const k = key();
    expect(existsSync(ledgerPath(k))).toBe(false);
    const res = insertCommittedTurn(row(k, "adv-b1"), warn);
    expect(res.ok).toBe(true);
    expect(existsSync(ledgerPath(k))).toBe(true);
  });

  it("(c) a user_version mismatch disables the ledger for that session, no throw", () => {
    const k = key();
    const path = ledgerPath(k);
    mkdirSync(dirname(path), { recursive: true });
    const db = new DatabaseSync(path);
    db.exec("PRAGMA user_version = 99");
    db.close();
    let res: { ok: boolean; reason?: string } | undefined;
    expect(() => {
      res = insertCommittedTurn(row(k, "adv-c1"), warn);
    }).not.toThrow();
    expect(res?.ok).toBe(false);
    expect(res?.reason ?? "").toContain("schema-version-mismatch");
  });

  it("(d) a host retry of the same advancementKey yields exactly one row", () => {
    const k = key();
    expect(insertCommittedTurn(row(k, "adv-d1"), warn).ok).toBe(true);
    expect(insertCommittedTurn(row(k, "adv-d1"), warn).ok).toBe(true); // INSERT OR IGNORE
    const db = new DatabaseSync(ledgerPath(k), { readOnly: true });
    const count = (db.prepare("SELECT COUNT(*) AS c FROM turns").get() as { c: number }).c;
    db.close();
    expect(count).toBe(1);
  });

  it("(e) assemble's hot path never creates a file and never throws", () => {
    const k = key(); // never opened
    let res: { ok: boolean; reason?: string } | undefined;
    expect(() => {
      res = readMarksCached(k, warn);
    }).not.toThrow();
    expect(res?.ok).toBe(false);
    expect(res?.reason).toBe("not-open");
    expect(existsSync(ledgerPath(k))).toBe(false);
    // and a read while another connection holds the write lock must still not throw
    const k2 = key();
    insertCommittedTurn(row(k2, "adv-e1"), warn);
    const holder = new DatabaseSync(ledgerPath(k2));
    holder.exec("BEGIN IMMEDIATE");
    expect(() => readMarksCached(k2, warn)).not.toThrow();
    holder.exec("ROLLBACK");
    holder.close();
  });

  it("(f) an unwritable location degrades: ok:false, no throw", () => {
    const k = key();
    // make the 'workspace' path a FILE so the recursive mkdir of the ledger dir fails
    writeFileSync(join(root, "workspace"), "not a directory");
    let res: { ok: boolean; reason?: string } | undefined;
    expect(() => {
      res = insertCommittedTurn(row(k, "adv-f1"), warn);
    }).not.toThrow();
    expect(res?.ok).toBe(false);
  });

  it("(g) a transient open failure is per-session and cooling down, not sticky forever", () => {
    const k = key();
    writeFileSync(join(root, "workspace"), "not a directory");
    expect(insertCommittedTurn(row(k, "adv-g1"), warn).ok).toBe(false);
    // within the cooldown the same key still reports the failure rather than retrying hot
    const again = insertCommittedTurn(row(k, "adv-g2"), warn);
    expect(again.ok).toBe(false);
    // ...but the failure is scoped to that key (a different key is unaffected)
    const other = key();
    rmSync(join(root, "workspace"), { force: true });
    expect(insertCommittedTurn(row(other, "adv-g3"), warn).ok).toBe(true);
  });
});

describe("package 32 - injective ledger names (R27.2 naming)", () => {
  const dir = () => join(root, "workspace", "state", "astinus");
  const lossy = (k: string) => k.replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 120) + ".sqlite";

  it("keys that share a lossy name get separate ledgers, each recording its own key", () => {
    const k = key();
    const a = `${k}:a/b`;
    const b = `${k}:a_b`; // same sanitized name as a
    expect(insertCommittedTurn(row(a, "adv-p1"), warn).ok).toBe(true);
    expect(insertCommittedTurn(row(b, "adv-p2"), warn).ok).toBe(true);
    closeAllLedgers();
    expect(ledgerPath(a)).not.toBe(ledgerPath(b));
    for (const s of [a, b]) {
      const db = new DatabaseSync(ledgerPath(s), { readOnly: true });
      expect(db.prepare("SELECT session_key FROM meta").get()).toEqual({ session_key: s });
      db.close();
    }
  });

  it("a ledger whose stored key differs from the opening key refuses", () => {
    const k = key();
    const a = `${k}:owner`;
    const b = `${k}:other`;
    expect(insertCommittedTurn(row(a, "adv-p3"), warn).ok).toBe(true);
    closeAllLedgers();
    copyFileSync(ledgerPath(a), ledgerPath(b));
    const r = insertCommittedTurn(row(b, "adv-p4"), warn);
    expect(r.ok).toBe(false);
    expect(r.reason).toBe(`ledger-key-mismatch:${a}`);
  });

  it("an old lossy-named ledger fails closed and is never adopted", () => {
    const k = `${key()}:legacy`;
    mkdirSync(dir(), { recursive: true });
    const legacy = new DatabaseSync(join(dir(), lossy(k)));
    legacy.exec("CREATE TABLE meta (session_key TEXT)");
    legacy.close();
    const r = insertCommittedTurn(row(k, "adv-p5"), warn);
    expect(r.ok).toBe(false);
    expect(r.reason).toBe("legacy-ledger-unreconciled");
    expect(existsSync(ledgerPath(k))).toBe(false);
  });
});
