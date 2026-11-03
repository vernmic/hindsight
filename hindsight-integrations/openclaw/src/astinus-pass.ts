// Astinus classification pass (package 28), ported from the OpenClaw 10.3 companion
// skills/astinus/astinus_pass.py (sha 358928A3008E...). Spawned detached by commitTurn in durable
// mode after a successful ledger insert; reads one JSON payload on stdin. It classifies the newly
// committed turn into the session ledger (topics, subjects, supersessions, marks, passes row) and
// proposes turn_middle marks. Host tables are read ONLY through HostTranscript (read-only); the
// only file it writes is the plugin's own ledger. Providers come from the resolved plugin config
// (`passProviders`, PLAN section 2): no endpoint guessing, no compose/models.json scraping, and
// each provider uses only its own credential. Not ported: pass_fence, the session_nodes and
// watch-file pressure fallbacks, the stamp file.
import { DatabaseSync } from "node:sqlite";
import { appendFileSync, existsSync, mkdirSync, realpathSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import {
  DEFAULT_TURN_MIDDLE_AGE_TURNS,
  ensureMarkAnchorColumns,
  HostTranscript,
} from "./assemble-marks.js";
import { configureSessionLedger, ledgerPath } from "./session-ledger.js";
import { callLlm, type PassProvider, type ProviderDeps } from "./pass-provider.js";

export type { PassProvider };

export type PassPayload = {
  root: string;
  session_key: string;
  session_id: string;
  agent_id: string;
  host_db_path: string;
  range: [number, number];
  view_tokens?: number | null;
  token_budget?: number | null;
  estimator?: string;
  window_first_seq?: number | null;
  turn_middle_age_turns?: number;
  providers?: PassProvider[];
};

export type PassDeps = ProviderDeps & { now?: () => number };

const PASS_BUSY_TIMEOUT_MS = 5000;
const PRESERVE_USER_TURNS = 8;
const TURN_MIDDLE_REASON = "turn_middle: interior redactable; boundaries and steers preserved";

export const SYSTEM_PROMPT = `You are Astinus, the session-ledger keeper for an AI assistant.

You are given (a) the session's EXISTING ledger - its topic vocabulary, per-topic one-line
digests, open supersessions and the marks already proposed - and (b) the messages of ONE newly
committed turn. You classify that turn, keep the vocabulary consistent, and propose what the
model should NOT have to see next turn.

Rules, in order:

1. TOPICS. Prefer an existing topic slug when the turn belongs to it; introduce a new topic
   only when the turn genuinely starts one. Labels are short and specific ("hindsight fork
   upgrade", "recall quality tuning") - never a generic bucket like "code" or "agents".
   ZOOM LEVEL: a topic names the ongoing ACTIVITY or subject area, never its instances.
   Channel names, file names, symbols and per-item targets are instances - archiving many
   YouTube trading channels is ONE topic ("youtube-content-search"), not one per channel. A
   topic is too narrow when it would plausibly apply to a single item; too broad when unrelated
   desks' work would fit it. Instances live in the SUBJECTS list and in bank content search -
   never as separate topics.
2. The turn's \`class\` is "topic" when it matched (or created) a topic, "random" when it is a
   one-off question or chit-chat that belongs to no recurring topic.
3. MARKS - what the model should not see next turn, graded by two clocks x pressure:
   - a statement's age and its TOPIC's dormancy both matter; age alone is not enough
   - random-class turns are the most aggressive (a one-off answered once is worth nothing later)
   - old statement in a live topic: PROTECTED. recent statement in an old topic: PROTECTED
     until the topic is terminal.
   - a topic dormant ~60 days is terminal: its whole segment may be digested as one line
   - recall injections decay steepest; enrich injections age with their topic
   - action "digest" replaces the range with the one-line \`digest\` you write (it must stand
     alone); "drop" removes it; "placeholder" is ONLY for tool-result bodies (keep the call).
   - never mark the newest turn's own messages. Editing the current turn is not your job.
   - grade by the PRESSURE line: propose NOTHING below 0.30.
   - 0.30 and up: \`placeholder\` marks for the largest tool-result ranges listed as candidates
     outside the tail.
   - 0.45 and up: also \`digest\` marks for whole dormant-topic segments (a topic whose
     last_seen_seq is far behind the live end).
   - 0.65 and up: everything the two clocks allow.
   - NEVER propose a range that intersects the preserved tail - the prompt states its first seq.
     A mark inside it cannot apply for eight turns and only pads the pool.
   - BATCH SIZE: propose enough ranges that their token weight covers (pressure - 0.25) x the
     view tokens; that is what keeps the pending pool ahead of the window.
4. SUPERSESSIONS. When this turn asserts a different reality for an existing
   (entity, attribute) - a version, pid, port, count, path, model id - record it, naming the
   old and new values.
5. You PROPOSE; code enforces the invariants. Under-pruning is safe; over-pruning is not.
6. SUBJECTS. For each topic this turn touches, list its instances as subject/object pairs:
   the subject is an entity (a channel, a file, a service, a person); the object is the
   thing it is paired with right now - what it acted on or what is being said about it
   ("294 videos", "rate-limit error", "the session ledger") - omitted when there is none.
   Update an existing subject's object when this turn changes it - the list is current
   state, not history. Keep it short: the subjects that matter for the NEXT turn.

Return ONLY this JSON, no prose:

{"topics":[{"slug":"kebab-case","label":"Short label","confidence":0.0-1.0,
             "subjects":[{"subject":"EntityName","object":"paired object, or \\"\\""}]}],
 "class":"topic"|"random",
 "supersessions":[{"subject":{"entity":"...","attribute":"..."},"old_range":[s,e],
                   "old_value":"...","new_value":"..."}],
 "marks":[{"range":[s,e],"action":"digest"|"drop"|"placeholder",
           "digest":".one line (digest only).","reason":"clocks + class, human-readable"}]}`;

type Json = Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any

function eventOf(eventJson: string): Json {
  try {
    const ev = JSON.parse(eventJson);
    return ev && typeof ev === "object" ? ev : {};
  } catch {
    return {};
  }
}

function messageOf(ev: Json): Json {
  return ev.message && typeof ev.message === "object" ? ev.message : ev;
}

/** (role, text) of a transcript event (message_text). */
export function messageText(eventJson: string): [string, string] {
  const ev = eventOf(eventJson);
  let role = typeof ev.role === "string" ? ev.role : "";
  let content = ev.content;
  if (content === undefined && ev.message && typeof ev.message === "object") {
    role = ev.message.role ?? role;
    content = ev.message.content;
  }
  if (typeof content === "string") return [role, content];
  if (Array.isArray(content)) {
    const parts = content.filter((p) => typeof p?.text === "string").map((p) => p.text as string);
    return [role, parts.join("\n")];
  }
  return [role, ""];
}

/** The seq of the Nth-newest user event (0 when fewer exist). */
function preservedTailFloor(ht: HostTranscript, keep = PRESERVE_USER_TURNS): number {
  let seen = 0;
  for (const e of ht.events({ descending: true })) {
    if (messageOf(eventOf(e.eventJson)).role === "user" && ++seen >= keep) return e.seq;
  }
  return 0;
}

function lastCompactionSeq(ht: HostTranscript): number {
  for (const e of ht.events({ descending: true })) {
    if (eventOf(e.eventJson).type === "compaction") return e.seq;
  }
  return 0;
}

/** Largest contiguous toolResult runs in (floor, ceiling), by decoded character count. */
function toolResultRuns(ht: HostTranscript, ceiling: number, floor: number, limit = 40) {
  const runs: Array<[number, number, number]> = [];
  let cur: [number, number, number] | null = null;
  for (const e of ht.events({ lo: floor + 1, hi: ceiling - 1 })) {
    if (messageOf(eventOf(e.eventJson)).role === "toolResult") {
      cur = cur ? [cur[0], e.seq, cur[2] + e.eventJson.length] : [e.seq, e.seq, e.eventJson.length];
    } else if (cur) {
      runs.push(cur);
      cur = null;
    }
  }
  if (cur) runs.push(cur);
  return runs.sort((a, b) => b[2] - a[2]).slice(0, limit);
}

function readTurnMessages(ht: HostTranscript, lo: number, hi: number): string {
  const out: string[] = [];
  for (const e of ht.events({ lo, hi })) {
    const [role, text] = messageText(e.eventJson);
    if ((role === "user" || role === "assistant") && text) {
      out.push(`[${role} seq=${e.seq}] ${text.slice(0, 2000)}`);
    }
  }
  return out.join("\n\n");
}

function loadLedgerCompact(db: DatabaseSync): Json {
  const topics = db
    .prepare(
      "SELECT slug, label, last_seen_seq, digest, terminal_at, topic_id FROM topics t WHERE EXISTS" +
        " (SELECT 1 FROM turn_topics tt WHERE tt.topic_id = t.topic_id AND tt.source = 'model')" +
        " ORDER BY last_seen_seq DESC"
    )
    .all() as Json[];
  const subs = new Map<unknown, Json[]>();
  for (const r of db
    .prepare("SELECT topic_id, subject, object FROM topic_subjects ORDER BY updated_at DESC")
    .all() as Json[]) {
    const list = subs.get(r.topic_id) ?? [];
    list.push({ subject: r.subject, object: r.object });
    subs.set(r.topic_id, list);
  }
  return {
    topics: topics.map((r) => ({
      slug: r.slug,
      label: r.label,
      last_seen_seq: r.last_seen_seq,
      digest: r.digest,
      terminal: Boolean(r.terminal_at),
      subjects: (subs.get(r.topic_id) ?? []).slice(0, 12),
    })),
    marks: (
      db
        .prepare("SELECT seq_start, seq_end, kind, action, reason FROM marks ORDER BY seq_start")
        .all() as Json[]
    ).map((r) => ({
      range: [r.seq_start, r.seq_end],
      kind: r.kind,
      action: r.action,
      reason: r.reason,
    })),
    supersessions: db
      .prepare("SELECT entity, attribute, old_value, new_value FROM supersessions ORDER BY seen_at")
      .all(),
  };
}

/** Strict-ish JSON extraction; null when the answer cannot be trusted. */
export function parseAnswer(text: string): Json | null {
  let raw = (text ?? "").trim();
  if (!raw) return null;
  if (raw.startsWith("```"))
    raw = raw
      .replace(/^```[a-zA-Z]*\n?/, "")
      .replace(/\n?```$/, "")
      .trim();
  let data: unknown;
  try {
    data = JSON.parse(raw);
  } catch {
    const m = raw.match(/\{[\s\S]*\}/);
    if (!m) return null;
    try {
      data = JSON.parse(m[0]);
    } catch {
      return null;
    }
  }
  if (!data || typeof data !== "object" || Array.isArray(data)) return null;
  const d = data as Json;
  const cls = String(d.class ?? "")
    .trim()
    .toLowerCase();
  if (cls !== "topic" && cls !== "random") return null;
  if (!Array.isArray(d.topics)) return null;
  if (d.marks != null && !Array.isArray(d.marks)) return null;
  for (const m of d.marks ?? []) {
    if (!m || typeof m !== "object") return null;
    if (!["digest", "drop", "placeholder"].includes(String(m.action))) return null;
    if (!(Array.isArray(m.range) && m.range.length === 2)) return null;
  }
  return d;
}

/** Labelled fallback: coarse buckets, never mistakable for a model topic. */
export function fallbackBuckets(text: string): Array<{ slug: string; label: string }> {
  const s = text.toLowerCase();
  return (
    [
      ["agents", "Agents", /\b(agent|subagent|session|spawn)\b/],
      ["skills", "Skills", /\b(skill|workflow|automation)\b/],
      ["models", "Models", /\b(model|llm|gpt|claude|deepseek|glm)\b/],
      ["infrastructure", "Infrastructure", /\b(gateway|config|restart|service|container)\b/],
      ["planning", "Planning", /\b(task|todo|plan|priority)\b/],
      ["code", "Code", /\b(code|file|function|class|module|commit)\b/],
    ] as const
  )
    .filter(([, , re]) => re.test(s))
    .map(([slug, label]) => ({ slug, label }));
}

function samePath(a: string, b: string): boolean {
  const norm = (v: string) => {
    let p = resolve(v);
    try {
      p = realpathSync.native(p);
    } catch {
      /* compare the resolved form */
    }
    return process.platform === "win32" ? p.toLowerCase() : p;
  };
  return norm(a) === norm(b);
}

/** Deterministic turn_middle proposals (design A.11 + F2): a turn becomes redactable once
 *  `ageTurns` NEWER turns exist. Additive to the model's marks; skips overlaps. */
function proposeTurnMiddleMarks(
  db: DatabaseSync,
  ht: HostTranscript,
  sessionId: string,
  ageTurns: number,
  nowS: number
): number {
  const rows = db
    .prepare("SELECT seq_start, seq_end FROM turns WHERE session_id = ? ORDER BY turn_id")
    .all(sessionId) as Json[];
  const existing = db
    .prepare("SELECT seq_start, seq_end FROM marks WHERE session_id = ?")
    .all(sessionId) as Json[];
  const insert = db.prepare(
    "INSERT INTO marks(session_id,seq_start,seq_end,kind,action,digest,reason,proposed_at,anchor_first_event,anchor_last_event) VALUES(?,?,?,'turn','digest',NULL,?,?,?,?)"
  );
  let n = 0;
  rows.forEach((r, idx) => {
    if (rows.length - 1 - idx < ageTurns) return;
    if (existing.some((m) => m.seq_start <= r.seq_end && m.seq_end >= r.seq_start)) return;
    const a = ht.anchorsFor(r.seq_start, r.seq_end);
    if (!a) return; // an unanchorable range cannot survive a reindex
    insert.run(sessionId, r.seq_start, r.seq_end, TURN_MIDDLE_REASON, nowS, a.first, a.last);
    n++;
  });
  return n;
}

/** One pass. Returns the outcome written (or the reason nothing was). Never throws. */
export async function runPass(payload: PassPayload, deps: PassDeps = {}): Promise<string> {
  const now = deps.now ?? Date.now;
  const log = deps.log ?? (() => {});
  const key = String(payload?.session_key ?? "");
  const sessionId = String(payload?.session_id ?? "");
  const [lo, hi] = Array.isArray(payload?.range) ? payload.range.map(Number) : [0, 0];
  if (!payload?.root || !key || !sessionId || !(hi > 0)) return "skipped-incomplete-payload";
  const vt = payload.view_tokens;
  const tb = payload.token_budget;
  const pressure = typeof vt === "number" && typeof tb === "number" && tb > 0 ? vt / tb : null;
  const estimator = String(payload.estimator ?? "plugin");
  const windowFloor = Number(payload.window_first_seq) || 0;
  const ageTurns =
    Number.isInteger(payload.turn_middle_age_turns) && payload.turn_middle_age_turns! >= 1
      ? payload.turn_middle_age_turns!
      : DEFAULT_TURN_MIDDLE_AGE_TURNS;
  configureSessionLedger(payload.root);
  const path = ledgerPath(key);
  if (!existsSync(path)) return "skipped-no-ledger";

  const db = new DatabaseSync(path);
  let ht: HostTranscript | null = null;
  let passId: number | null = null;
  try {
    db.exec(`PRAGMA busy_timeout = ${PASS_BUSY_TIMEOUT_MS}; PRAGMA journal_mode = WAL;`);
    ensureMarkAnchorColumns(db);
    const meta = db
      .prepare("SELECT host_db_path, agent_id, session_key FROM meta LIMIT 1")
      .get() as Json | undefined;
    if (!meta?.host_db_path) return "skipped-no-meta";
    // Copied ledger metadata is data, not permission to read another instance.
    if (
      typeof payload.host_db_path !== "string" ||
      !payload.host_db_path ||
      !samePath(payload.host_db_path, meta.host_db_path) ||
      payload.agent_id !== meta.agent_id ||
      meta.session_key !== key
    ) {
      log(`${key}: host/agent binding mismatch; pass refused`);
      return "skipped-host-binding-mismatch";
    }
    let turn = db
      .prepare(
        "SELECT turn_id FROM turns WHERE session_id = ? AND seq_start = ? AND seq_end = ? ORDER BY turn_id DESC LIMIT 1"
      )
      .get(sessionId, lo, hi) as Json | undefined;
    if (!turn) {
      turn = db
        .prepare(
          "SELECT turn_id FROM turns WHERE session_id = ? AND seq_end <= ? ORDER BY turn_id DESC LIMIT 1"
        )
        .get(sessionId, hi) as Json | undefined;
      log(
        `${key}: exact turn range [${lo},${hi}] not found; attributing to turn ${turn?.turn_id ?? null}`
      );
    }
    const turnId: number | null = turn ? Number(turn.turn_id) : null;
    const startedAt = now();
    // Single-flight reservation: one short transaction, no model call holds it.
    db.exec("BEGIN IMMEDIATE");
    try {
      if (
        db
          .prepare(
            "SELECT pass_id FROM passes WHERE finished_at IS NULL AND started_at > ? LIMIT 1"
          )
          .get(startedAt - 120000)
      ) {
        db.exec("ROLLBACK");
        return "skipped-inflight";
      }
      passId = Number(
        db
          .prepare("INSERT INTO passes(turn_id, outcome, started_at) VALUES(?,?,?)")
          .run(turnId, "running", startedAt).lastInsertRowid
      );
      db.exec("COMMIT");
    } catch (e) {
      db.exec("ROLLBACK");
      throw e;
    }
    const finishWithoutModel = (outcome: string, error?: unknown) => {
      db.prepare("UPDATE passes SET outcome=?, finished_at=? WHERE pass_id=?").run(
        outcome,
        now(),
        passId
      );
      log(`${key}: ${outcome}${error ? ": " + ((error as Error)?.message ?? error) : ""}`);
      return outcome;
    };
    // topic_subjects is pass-owned (created on demand; TS readers tolerate its absence).
    db.exec(
      "CREATE TABLE IF NOT EXISTS topic_subjects (topic_id INTEGER NOT NULL REFERENCES topics," +
        " subject TEXT NOT NULL, object TEXT NOT NULL, first_seen_turn INTEGER, last_seen_turn INTEGER," +
        " updated_at INTEGER NOT NULL, UNIQUE(topic_id, subject))"
    );
    let turnText: string, ledger: Json, tailFloor: number, viewFloor: number;
    let runs: Array<[number, number, number]>, minTurnSeq: number;
    try {
      ht = HostTranscript.open(meta.host_db_path, sessionId);
      turnText = readTurnMessages(ht, lo, hi);
      ledger = loadLedgerCompact(db);
      tailFloor = preservedTailFloor(ht);
      viewFloor = Math.max(lastCompactionSeq(ht), windowFloor);
      runs = tailFloor > 0 ? toolResultRuns(ht, tailFloor, viewFloor) : [];
      minTurnSeq = ht.seqBounds().min ?? 0;
    } catch (e) {
      return finishWithoutModel("failed-source-read", e);
    }
    if (!turnText) return finishWithoutModel("skipped-no-messages");

    const pressureLine =
      pressure === null
        ? `PRESSURE: unknown (estimator=${estimator})`
        : `PRESSURE: view ${Math.trunc(vt!)} / budget ${Math.trunc(tb!)} = ${pressure.toFixed(2)} (estimator=${estimator})`;
    const tailLine =
      tailFloor > 0
        ? `PRESERVED TAIL: seq >= ${tailFloor} is kept verbatim; a mark ending at or above it cannot apply.`
        : "PRESERVED TAIL: fewer than 8 user turns - everything is preserved; propose NO marks.";
    const candText =
      runs.map(([a, b, c]) => `  seq ${a}-${b} (${c} chars)`).join("\n") ||
      "  (none outside the tail)";
    const userPrompt =
      `SESSION: ${key} (agent ${meta.agent_id}, seq ${lo}-${hi})\n${pressureLine}\n${tailLine}\n\n` +
      `=== EXISTING LEDGER ===\n${JSON.stringify(ledger).slice(0, 20000) || "{}"}\n\n` +
      `=== CANDIDATE TOOL-RESULT RANGES OUTSIDE THE TAIL (largest first) ===\n${candText}\n\n` +
      `=== NEW TURN (seq ${lo}-${hi}) ===\n${turnText.slice(0, 24000)}\n`;

    const providers = Array.isArray(payload.providers) ? payload.providers : [];
    let result = await callLlm(providers, SYSTEM_PROMPT, userPrompt, deps);
    // Reasoning can exhaust the cap: finish_reason=length with empty content retries ONCE at "high".
    if (result.ok && !result.text.trim() && result.finishReason === "length") {
      log(`${key}: finish_reason=length with empty content; retrying once at high`);
      const retry = await callLlm(providers, SYSTEM_PROMPT, userPrompt, deps, "high");
      if (retry.ok) result = retry;
    }
    const answer = result.ok ? parseAnswer(result.text) : null;
    const outcome = answer ? "ok" : result.ok ? "fallback" : "error";
    if (outcome !== "ok" && result.finishReason) {
      result.fallbackReason =
        `${result.fallbackReason ?? ""} finish_reason=${result.finishReason}`.trim();
    }
    const nowMs = now();
    const nowS = Math.floor(nowMs / 1000);
    const tid = turnId ?? 0;
    const upsertTopic = db.prepare(
      "INSERT INTO topics(slug,label,first_seen_seq,last_seen_seq,last_seen_at) VALUES(?,?,?,?,?)" +
        " ON CONFLICT(slug) DO UPDATE SET last_seen_seq=excluded.last_seen_seq, last_seen_at=excluded.last_seen_at"
    );
    const topicIdOf = (slug: string) =>
      Number((db.prepare("SELECT topic_id FROM topics WHERE slug = ?").get(slug) as Json).topic_id);
    const linkTopic = db.prepare(
      "INSERT OR REPLACE INTO turn_topics(turn_id,topic_id,class,confidence,source) VALUES(?,?,?,?,?)"
    );
    db.exec("BEGIN IMMEDIATE");
    try {
      // Retire unapplied marks below the assembled window, inside this transaction.
      if (windowFloor > 0) {
        const dead = db
          .prepare("DELETE FROM marks WHERE applied_at IS NULL AND seq_end < ?")
          .run(windowFloor).changes;
        if (dead)
          log(
            `${key}: retired ${dead} out-of-window pending marks (window starts at seq ${windowFloor})`
          );
      }
      if (answer) {
        if (turnId !== null)
          db.prepare("DELETE FROM turn_topics WHERE turn_id = ? AND source = 'fallback'").run(
            turnId
          );
        const topicIds = new Map<string, number>();
        const lowerText = turnText.toLowerCase();
        for (const t of answer.topics as Json[]) {
          const slug = String(t?.slug ?? "")
            .trim()
            .toLowerCase();
          if (!slug) continue;
          upsertTopic.run(slug, String(t.label || slug).trim(), lo, hi, nowMs);
          const topicId = topicIdOf(slug);
          topicIds.set(slug, topicId);
          for (const sub of (Array.isArray(t.subjects) ? t.subjects : []).slice(0, 12)) {
            const name = String(sub?.subject ?? "")
              .trim()
              .slice(0, 120);
            const obj = String(sub?.object ?? "")
              .trim()
              .slice(0, 240);
            // Keep only names that occur in the turn; case-insensitive identity.
            if (!name || !lowerText.includes(name.toLowerCase())) continue;
            const prev = db
              .prepare(
                "SELECT subject FROM topic_subjects WHERE topic_id = ? AND subject = ? COLLATE NOCASE"
              )
              .get(topicId, name) as Json | undefined;
            if (prev) {
              db.prepare(
                "UPDATE topic_subjects SET object = ?, last_seen_turn = ?, updated_at = ? WHERE topic_id = ? AND subject = ?"
              ).run(obj, tid, nowMs, topicId, prev.subject);
            } else {
              db.prepare(
                "INSERT INTO topic_subjects(topic_id,subject,object,first_seen_turn,last_seen_turn,updated_at) VALUES(?,?,?,?,?,?)"
              ).run(topicId, name, obj, tid, tid, nowMs);
            }
          }
        }
        if (turnId !== null) {
          if (String(answer.class) === "random" || topicIds.size === 0) {
            linkTopic.run(turnId, null, "random", null, "model");
          } else {
            for (const [slug, topicId] of topicIds) {
              const conf = (answer.topics as Json[]).find(
                (t) =>
                  String(t?.slug ?? "")
                    .trim()
                    .toLowerCase() === slug
              )?.confidence;
              linkTopic.run(
                turnId,
                topicId,
                "topic",
                typeof conf === "number" ? conf : null,
                "model"
              );
            }
          }
          db.prepare("UPDATE turns SET classified_at = ? WHERE turn_id = ?").run(nowMs, turnId);
        }
        for (const s of Array.isArray(answer.supersessions) ? answer.supersessions : []) {
          db.prepare(
            "INSERT INTO supersessions(entity,attribute,old_turn_id,new_turn_id,old_value,new_value,seen_at) VALUES(?,?,?,?,?,?,?)"
          ).run(
            String(s?.subject?.entity ?? ""),
            String(s?.subject?.attribute ?? ""),
            tid,
            tid,
            String(s?.old_value ?? ""),
            String(s?.new_value ?? ""),
            nowMs
          );
        }
        // Mark hygiene: code, not the prompt, enforces the invariants.
        const existing = db.prepare("SELECT seq_start, seq_end FROM marks").all() as Json[];
        const top = db
          .prepare("SELECT last_seen_at FROM topics ORDER BY last_seen_seq DESC LIMIT 1")
          .get() as Json | undefined;
        const dormancy = top?.last_seen_at
          ? Math.max(0, nowS - Math.floor(top.last_seen_at / 1000))
          : null;
        const insertMark = db.prepare(
          "INSERT INTO marks(session_id,seq_start,seq_end,kind,action,digest,reason,stmt_age_s,topic_dormancy_s,pressure,proposed_at,anchor_first_event,anchor_last_event) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        for (const m of (answer.marks ?? []) as Json[]) {
          const ms = Math.trunc(Number(m.range[0]));
          const me = Math.trunc(Number(m.range[1]));
          if (!Number.isFinite(ms) || !Number.isFinite(me)) continue;
          if (me >= lo) continue; // never the newest turn
          if (viewFloor && ms <= viewFloor) continue; // below the assembled window
          if (tailFloor <= 0 || me >= tailFloor) continue; // the preserved tail
          if (ms < minTurnSeq) continue; // outside this session's transcript
          if (existing.some((e) => ms <= e.seq_end && me >= e.seq_start)) continue;
          const action = String(m.action);
          const anchor = ht!.anchorsFor(ms, me);
          if (!anchor) continue; // unanchorable: would redact the wrong turn after a reindex
          let ageS: number | null = null;
          try {
            const created = ht!.createdAt(ms);
            if (created) ageS = Math.max(0, nowS - Math.floor(created / 1000));
          } catch {
            /* age unknown */
          }
          insertMark.run(
            sessionId,
            ms,
            me,
            action === "placeholder" ? "tool_noise" : "turn",
            action,
            action === "digest" ? String(m.digest ?? "") : null,
            String(m.reason ?? ""),
            ageS,
            dormancy,
            pressure,
            nowMs,
            anchor.first,
            anchor.last
          );
          existing.push({ seq_start: ms, seq_end: me });
        }
      } else if (turnId !== null) {
        // Labelled fallback rows - a regex topic can never be mistaken for a model one.
        const buckets = result.ok ? fallbackBuckets(turnText) : [];
        for (const b of buckets) {
          upsertTopic.run(b.slug, b.label, lo, hi, nowMs);
          linkTopic.run(turnId, topicIdOf(b.slug), "topic", null, "fallback");
        }
        if (buckets.length === 0) linkTopic.run(turnId, null, "random", null, "fallback");
        db.prepare("UPDATE turns SET classified_at = ? WHERE turn_id = ?").run(nowMs, turnId);
      }
      const lag = Number(
        (
          db
            .prepare("SELECT COUNT(*) AS n FROM turns WHERE committed_at > ?")
            .get(startedAt) as Json
        ).n
      );
      proposeTurnMiddleMarks(db, ht!, sessionId, ageTurns, nowS);
      db.prepare(
        "UPDATE passes SET outcome=?, model=?, effort=?, elapsed_ms=?, reasoning_tokens=?, marks_proposed=?," +
          " fallback_from=?, fallback_reason=?, finished_at=?, lag_turns=? WHERE pass_id=?"
      ).run(
        outcome,
        result.model,
        "max",
        result.elapsedMs,
        result.reasoningTokens,
        (answer?.marks ?? []).length,
        result.fallbackFrom,
        result.fallbackReason,
        nowMs,
        lag,
        passId
      );
      db.exec("COMMIT");
    } catch (e) {
      db.exec("ROLLBACK");
      return finishWithoutModel("failed-write", e);
    }
    log(
      `${key}: turn=${turnId} outcome=${outcome} model=${result.model} elapsed=${result.elapsedMs}ms marks=${(answer?.marks ?? []).length} pressure=${pressure === null ? "unknown" : pressure.toFixed(2)}`
    );
    return outcome;
  } catch (e) {
    log(`${key}: pass failed: ${(e as Error)?.message ?? e}`);
    // A reserved pass always ends terminal so the next pass is not blocked.
    if (passId !== null) {
      try {
        db.prepare("UPDATE passes SET outcome='failed', finished_at=? WHERE pass_id=?").run(
          now(),
          passId
        );
      } catch {
        /* ledger not writable; the 120s window still expires */
      }
    }
    return "failed";
  } finally {
    ht?.close();
    db.close();
  }
}

/** Process entry: one payload on stdin; log under <root>/workspace/logs/astinus-pass.log. */
async function main(): Promise<void> {
  const chunks: Buffer[] = [];
  for await (const c of process.stdin) chunks.push(c as Buffer);
  let payload: PassPayload;
  try {
    payload = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    return;
  }
  const logPath = join(String(payload?.root ?? ""), "workspace", "logs", "astinus-pass.log");
  const log = (msg: string) => {
    try {
      mkdirSync(dirname(logPath), { recursive: true });
      appendFileSync(logPath, `${new Date().toISOString()} ${msg}\n`);
    } catch {
      /* logging never fails a pass */
    }
  };
  log(`outcome ${await runPass(payload, { log })}`);
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  void main();
}
