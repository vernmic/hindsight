// One-time install migration: adopt old-name (lossy) Astinus session state files
// only when the HOST proves the owner. Run with the pinned Node, gateway STOPPED.
//   node migrate-legacy-state.mjs --root I:/OpenClaw2/.openclaw [--dry-run]
// Proof (per file F): F.lastGenerationSeen -> session id (transcript_rewrite_watermarks |
// session_transcript_archives) -> key K (session_windows); adopt iff legacy(K) === F and every
// pendingRetain[]/pendingLegacy[] sessionId also resolves to K. Adopted: write sessionFileName(K) with the same
// contents plus sessionKey, move F to legacy-adopted/. Everything else -> legacy-unowned/.
// Deletes nothing; archive moves never overwrite. Core check derived from Vern's read-only audit gen-own.mjs.
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const args = process.argv.slice(2);
const root = args[args.indexOf("--root") + 1];
const dry = args.includes("--dry-run");
if (!args.includes("--root") || !root) { console.error("usage: --root <openclaw home> [--dry-run]"); process.exit(2); }
const dir = path.join(root, "workspace", "state", "astinus");
const legacy = (k) => String(k).replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 120) + ".json";
const digestName = (k) => String(k).replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 80) + "." +
  createHash("sha256").update(String(k), "utf8").digest("hex").slice(0, 32) + ".json";

const agentsDir = path.join(root, "agents");
const dbs = fs.readdirSync(agentsDir).map((a) => path.join(agentsDir, a, "agent", "openclaw-agent.sqlite")).filter((p) => fs.existsSync(p));
const liveNames = new Set(), digestNames = new Set(), sidKey = new Map(), genSid = new Map();
for (const p of dbs) {
  const db = new DatabaseSync(p, { readOnly: true });
  const has = (t) => db.prepare("select 1 from sqlite_master where name=?").get(t);
  const keys = new Set();
  if (has("session_nodes")) for (const r of db.prepare("select session_key k from session_nodes").all()) keys.add(r.k);
  if (has("session_windows")) for (const r of db.prepare("select session_id s, session_key k from session_windows").all()) { keys.add(r.k); sidKey.set(r.s, r.k); }
  if (has("transcript_rewrite_watermarks")) for (const r of db.prepare("select session_id s, generation g from transcript_rewrite_watermarks").all()) genSid.set(r.g, r.s);
  if (has("session_transcript_archives")) for (const r of db.prepare("select session_id s, generation g from session_transcript_archives").all()) genSid.set(r.g, r.s);
  db.close();
  for (const k of keys) { liveNames.add(legacy(k)); digestNames.add(digestName(k)); }
}

const isDefault = (s) => !s.lastGenerationSeen && !s.lastRetainedSeq && !s.lastCommittedSeq &&
  !(s.pendingRetain || []).length && !(s.pendingLegacy || []).length && !(s.committedKeys || []).length;
const c = { adopted: 0, unownedLive: 0, defaultContent: 0, deadKeys: 0, skippedCurrent: 0 };
const moves = [];
for (const f of fs.readdirSync(dir).filter((f) => f.endsWith(".json"))) {
  const full = path.join(dir, f);
  let s;
  try { s = JSON.parse(fs.readFileSync(full, "utf8")); } catch { s = null; }
  if (s && typeof s === "object" && !Array.isArray(s) && s.sessionKey !== undefined) { c.skippedCurrent++; continue; }
  if (digestNames.has(f)) { c.skippedCurrent++; continue; }
  let verdict, key;
  if (!liveNames.has(f)) verdict = "dead-key";
  else if (!s || typeof s !== "object" || Array.isArray(s)) verdict = "unowned-live";
  else if (!s.lastGenerationSeen) verdict = isDefault(s) ? "default-content" : "unowned-live";
  else {
    key = sidKey.get(genSid.get(s.lastGenerationSeen));
    // R34.1: every pending work item that names a session (pendingRetain AND pendingLegacy) must
    // resolve through the host to the same key; foreign or unresolved -> unowned (bytes kept).
    const items = (v) => (Array.isArray(v) ? v : v && typeof v === "object" ? [v] : []);
    const pendOk = [...items(s.pendingRetain), ...items(s.pendingLegacy)].every((r) => !r || !r.sessionId || sidKey.get(r.sessionId) === key);
    verdict = key && legacy(key) === f && pendOk ? "adopt" : "unowned-live";
  }
  if (verdict === "adopt") {
    const target = path.join(dir, digestName(key));
    if (fs.existsSync(target)) { verdict = "unowned-live"; console.log(`[migrate] ${f}: target ${path.basename(target)} already exists; not adopted`); }
    else {
      c.adopted++;
      console.log(`[migrate] adopt ${f} -> ${path.basename(target)} (key ${key})`);
      if (!dry) {
        fs.writeFileSync(target + ".tmp", JSON.stringify({ ...s, sessionKey: String(key) }, null, 2));
        fs.renameSync(target + ".tmp", target);
        moves.push([full, "legacy-adopted", f]);
      }
      continue;
    }
  }
  if (verdict === "dead-key") c.deadKeys++; else if (verdict === "default-content") c.defaultContent++; else c.unownedLive++;
  console.log(`[migrate] ${verdict} ${f}`);
  if (!dry) moves.push([full, "legacy-unowned", f]);
}
// R34.2: an archive move never overwrites; an occupied name gets a unique .dupN suffix.
for (const [from, sub, f] of moves) {
  const d = path.join(dir, sub); fs.mkdirSync(d, { recursive: true });
  let dest = path.join(d, f);
  for (let n = 1; fs.existsSync(dest); n++) dest = path.join(d, f.replace(/\.json$/, "") + `.dup${n}.json`);
  if (dest !== path.join(d, f)) console.log(`[migrate] archive name ${sub}/${f} occupied; kept as ${path.basename(dest)}`);
  fs.renameSync(from, dest);
}
console.log(`[migrate] dbs=${dbs.length} dry=${dry} adopted=${c.adopted} unowned_live_keys=${c.unownedLive} default_content=${c.defaultContent} dead_keys=${c.deadKeys} skipped_current=${c.skippedCurrent}`);