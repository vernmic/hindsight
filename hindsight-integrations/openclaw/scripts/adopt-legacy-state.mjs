import { DatabaseSync } from "node:sqlite";
import { readdirSync, existsSync } from "node:fs";
import { join } from "node:path";
import { sweepLegacyState } from "./dist/legacy-state-adoption.js";
import { setSessionStateDir } from "./dist/session-state.js";

const AGENTS = process.env.SWEEP_AGENTS ?? "I:/OpenClaw/.openclaw/agents";
const STATE = process.env.SWEEP_STATE ?? "I:/OpenClaw/.openclaw/workspace/state/astinus";
const APPLY = process.argv.includes("--apply");
setSessionStateDir(STATE);

const keys = new Set();
const gen = new Map();
for (const agentDir of readdirSync(AGENTS)) {
  const dbPath = join(AGENTS, agentDir, "agent", "openclaw-agent.sqlite");
  if (!existsSync(dbPath)) continue;
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    for (const row of db.prepare("select session_key, current_session_id from session_nodes").all()) {
      if (!row.session_key) continue;
      keys.add(row.session_key);
      if (!row.current_session_id) continue;
      const w = db.prepare("select generation from transcript_rewrite_watermarks where session_id = ?").get(row.current_session_id);
      if (w?.generation) gen.set(row.session_key, w.generation);
    }
  } catch (e) {
    console.error("db_skip", agentDir, String(e.message).slice(0, 80));
  } finally {
    db.close();
  }
}

const res = sweepLegacyState({
  stateDir: STATE,
  keys: [...keys],
  generationFor: (k) => gen.get(k) ?? null,
  apply: APPLY,
});
console.log(JSON.stringify({ keys: res.keys, counts: res.counts, apply: APPLY }, null, 1));
const ev = res.outcomes.filter((o) => o.status === "refuse-evidence").slice(0, 3);
if (ev.length) console.log("evidence_samples", JSON.stringify(ev, null, 1));