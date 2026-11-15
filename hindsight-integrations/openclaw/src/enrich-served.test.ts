import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import {
  closeAllLedgers,
  configureSessionLedger,
  insertCommittedTurn,
  ledgerPath,
  markEnrichServed,
  pruneEnrichServed,
  readEnrichServed,
  readLiveTopicsDetail,
  type TurnInsert,
} from "./session-ledger.js";

const warn = (_m: string): void => {};
let root = "";

const turn = (key: string): TurnInsert => ({
  sessionKey: key,
  agentId: "test",
  hostDbPath: "C:\\nonexistent\\openclaw-agent.sqlite",
  sessionId: "sid-" + key,
  seqStart: 1,
  seqEnd: 3,
  generation: "gen-1",
  advancementKey: "adv-" + key,
  heartbeat: false,
});

function seedTopic(key: string): void {
  const db = new DatabaseSync(ledgerPath(key));
  db.prepare(
    "INSERT INTO topics (slug, label, digest, first_seen_seq, last_seen_seq, last_seen_at, terminal_at) VALUES (?,?,?,?,?,?,?)"
  ).run("t1", "Token accounting", "prune replay", 1, 5, Date.now(), null);
  db.prepare(
    "INSERT INTO turn_topics (turn_id, topic_id, class, confidence, source) VALUES (?,?,?,?,?)"
  ).run(1, 1, "topic", 0.9, "model");
  db.close();
}

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), "astinus-served-"));
  configureSessionLedger(root);
});

afterEach(() => {
  closeAllLedgers();
  try {
    rmSync(root, { recursive: true, force: true });
  } catch {
    /* best effort */
  }
});

describe("enrich served store", () => {
  it("returns live topics with label and digest", () => {
    const key = "agent:test:topics";
    expect(insertCommittedTurn(turn(key), warn).ok).toBe(true);
    seedTopic(key);
    expect(readLiveTopicsDetail(key, 3)).toEqual([
      { slug: "t1", label: "Token accounting", digest: "prune replay" },
    ]);
  });

  it("records a pair once and reads it back", () => {
    const key = "agent:test:served";
    expect(insertCommittedTurn(turn(key), warn).ok).toBe(true);
    markEnrichServed(key, [
      ["t1", "d1"],
      ["t1", "d1"],
      ["t2", "d2"],
    ]);
    const map = readEnrichServed(key);
    expect(Array.from(map.get("t1") ?? [])).toEqual(["d1"]);
    expect(Array.from(map.get("t2") ?? [])).toEqual(["d2"]);
  });

  it("drops ids for topics that are no longer live", () => {
    const key = "agent:test:prune-served";
    expect(insertCommittedTurn(turn(key), warn).ok).toBe(true);
    markEnrichServed(key, [
      ["t1", "d1"],
      ["t9", "d9"],
    ]);
    pruneEnrichServed(key, ["t1"]);
    const map = readEnrichServed(key);
    expect(map.has("t1")).toBe(true);
    expect(map.has("t9")).toBe(false);
  });

  it("clears everything when no topic is live", () => {
    const key = "agent:test:clear-served";
    expect(insertCommittedTurn(turn(key), warn).ok).toBe(true);
    markEnrichServed(key, [["t1", "d1"]]);
    pruneEnrichServed(key, []);
    expect(readEnrichServed(key).size).toBe(0);
  });
});
