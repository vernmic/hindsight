// Installer regressions for migrate-legacy-state.mjs (R34.1, R34.2). Run: node --test migrate-legacy-state.test.mjs
// Fixture shape follows Codex review 34's migration-probe.mjs: two keys whose lossy names collide.
import { test } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";

const script = join(dirname(fileURLToPath(import.meta.url)), "migrate-legacy-state.mjs");
const legacy = (k) => k.replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 120) + ".json";
const digest = (k) => k.replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 80) + "." + createHash("sha256").update(k).digest("hex").slice(0, 32) + ".json";
const A = "agent:main:a/b", B = "agent:main:a_b", NAME = legacy(B);

function fixture(state, { archive } = {}) {
  const root = mkdtempSync(join(tmpdir(), "mls-"));
  const dbFile = join(root, "agents", "main", "agent", "openclaw-agent.sqlite");
  mkdirSync(dirname(dbFile), { recursive: true });
  const db = new DatabaseSync(dbFile);
  db.exec("CREATE TABLE session_windows(session_id TEXT, session_key TEXT); CREATE TABLE transcript_rewrite_watermarks(session_id TEXT, generation TEXT)");
  db.prepare("INSERT INTO session_windows VALUES(?,?)").run("sid-a", A);
  db.prepare("INSERT INTO session_windows VALUES(?,?)").run("sid-b", B);
  db.prepare("INSERT INTO transcript_rewrite_watermarks VALUES(?,?)").run("sid-b", "gen-b");
  db.close();
  const dir = join(root, "workspace", "state", "astinus");
  mkdirSync(dir, { recursive: true });
  if (archive) { mkdirSync(join(dir, archive.sub)); writeFileSync(join(dir, archive.sub, NAME), "PRIOR ARCHIVED CONTENT"); }
  const bytes = JSON.stringify(state);
  writeFileSync(join(dir, NAME), bytes);
  const out = execFileSync(process.execPath, [script, "--root", root], { encoding: "utf8" });
  return { dir, bytes, out };
}
const base = () => ({ lastGenerationSeen: "gen-b", lastCommittedSeq: 6, pendingRetain: [], pendingLegacy: [] });
const capture = (sessionId) => ({ id: "capture-x", sessionId, at: 1, messages: [{ role: "user", content: "FOREIGN_A_PAYLOAD" }] });

test("matching owner adopts (control)", () => {
  const { dir, out } = fixture(base());
  assert.match(out, /adopted=1 /);
  assert.equal(JSON.parse(readFileSync(join(dir, digest(B)), "utf8")).sessionKey, B);
  assert.ok(existsSync(join(dir, "legacy-adopted", NAME)));
});

test("foreign pendingRetain sessionId -> unowned, bytes kept (control)", () => {
  const s = base(); s.pendingRetain.push({ start: 1, end: 3, sessionId: "sid-a", messages: [] });
  const { dir, bytes, out } = fixture(s);
  assert.match(out, /adopted=0 unowned_live_keys=1 /);
  assert.ok(!existsSync(join(dir, digest(B))));
  assert.equal(readFileSync(join(dir, "legacy-unowned", NAME), "utf8"), bytes);
});

test("R34.1: colliding key's pendingLegacy (sid-a in B's file) -> unowned, bytes kept", () => {
  const s = base(); s.pendingLegacy.push(capture("sid-a"));
  const { dir, bytes, out } = fixture(s);
  assert.match(out, /adopted=0 unowned_live_keys=1 /);
  assert.ok(!existsSync(join(dir, digest(B))), "no digest state may carry A's capture");
  assert.equal(readFileSync(join(dir, "legacy-unowned", NAME), "utf8"), bytes);
});

test("R34.1: unresolved pendingLegacy sessionId -> unowned", () => {
  const s = base(); s.pendingLegacy.push(capture("sid-unknown"));
  const { dir, out } = fixture(s);
  assert.match(out, /adopted=0 unowned_live_keys=1 /);
  assert.ok(!existsSync(join(dir, digest(B))));
});

test("R34.1: same-key pendingLegacy still adopts with the capture carried over (drain policy unchanged)", () => {
  const s = base(); s.pendingLegacy.push(capture("sid-b"));
  const { dir, out } = fixture(s);
  assert.match(out, /adopted=1 /);
  assert.equal(JSON.parse(readFileSync(join(dir, digest(B)), "utf8")).pendingLegacy[0].sessionId, "sid-b");
});

test("R34.2: occupied legacy-unowned archive is preserved; new archive gets a unique name", () => {
  const s = { lastCommittedSeq: 0, pendingRetain: [], pendingLegacy: [] };
  const { dir, bytes, out } = fixture(s, { archive: { sub: "legacy-unowned" } });
  assert.match(out, /default_content=1 /);
  assert.equal(readFileSync(join(dir, "legacy-unowned", NAME), "utf8"), "PRIOR ARCHIVED CONTENT");
  const dup = readdirSync(join(dir, "legacy-unowned")).filter((f) => f !== NAME);
  assert.deepEqual(dup, ["agent_main_a_b.dup1.json"]);
  assert.equal(readFileSync(join(dir, "legacy-unowned", dup[0]), "utf8"), bytes);
  assert.ok(!existsSync(join(dir, NAME)));
});

test("R34.2: occupied legacy-adopted archive is preserved on adoption", () => {
  const { dir, bytes } = fixture(base(), { archive: { sub: "legacy-adopted" } });
  assert.equal(readFileSync(join(dir, "legacy-adopted", NAME), "utf8"), "PRIOR ARCHIVED CONTENT");
  assert.equal(readFileSync(join(dir, "legacy-adopted", "agent_main_a_b.dup1.json"), "utf8"), bytes);
  assert.ok(existsSync(join(dir, digest(B))));
});