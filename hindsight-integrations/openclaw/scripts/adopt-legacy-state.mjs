#!/usr/bin/env node
import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { sweepLegacyState } from "../dist/legacy-state-adoption.js";
import { setSessionStateDir } from "../dist/session-state.js";
import {
  gatewayLockPath,
  parseSweepArgs,
  preflightApply,
  preflightDatabases,
} from "../dist/legacy-state-cli.js";

const parsed = parseSweepArgs(process.argv.slice(2));
if (!parsed.ok) {
  console.error(parsed.error);
  process.exit(2);
}
const args = parsed.args;

const lock = preflightApply(args, existsSync(gatewayLockPath(args.agents)));
if (!lock.ok) {
  console.error(lock.error);
  process.exit(3);
}

const keys = new Set();
const gen = new Map();
const ids = new Map();
const skipped = [];
for (const agentDir of readdirSync(args.agents)) {
  const dbPath = join(args.agents, agentDir, "agent", "openclaw-agent.sqlite");
  if (!existsSync(dbPath)) continue;
  let db;
  try {
    db = new DatabaseSync(dbPath, { readOnly: true });
    for (const row of db.prepare("select session_key, current_session_id from session_nodes").all()) {
      if (!row.session_key) continue;
      keys.add(row.session_key);
      if (!row.current_session_id) continue;
      const list = ids.get(row.session_key) ?? [];
      list.push(row.current_session_id);
      ids.set(row.session_key, list);
      const w = db.prepare("select generation from transcript_rewrite_watermarks where session_id = ?").get(row.current_session_id);
      if (w?.generation) gen.set(row.session_key, w.generation);
    }
  } catch (error) {
    skipped.push(`${agentDir}: ${String(error?.message ?? error).slice(0, 80)}`);
  } finally {
    db?.close();
  }
}

const dbCheck = preflightDatabases(args.apply, skipped);
if (!dbCheck.ok) {
  for (const s of skipped) console.error("db_skip", s);
  console.error(dbCheck.error);
  process.exit(4);
}
for (const s of skipped) console.error("db_skip", s);

setSessionStateDir(args.state);
const res = sweepLegacyState({
  stateDir: args.state,
  keys: [...keys],
  generationFor: (k) => gen.get(k) ?? null,
  sessionIdsFor: (k) => ids.get(k) ?? [],
  apply: args.apply,
});
console.log(JSON.stringify({ keys: res.keys, counts: res.counts, apply: args.apply }, null, 1));