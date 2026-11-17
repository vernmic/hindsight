// Astinus mark application (plan v3 §8 + v4.2 + the assemble-marks review B1-B4/M1-M5).
// Identity join (never position); admission vs re-application split (B2); tool pairs by
// role+toolCallId (B3); host-shaped estimator (B4); element replacement not mutation (M1);
// parts-array digest (M2); 8-user-turn preserved tail (M3); (dbPath,sessionId) index key (M4);
// one refresh per pass (M5). Any failure degrades to "less pruned", never an error.
import { DatabaseSync } from "node:sqlite";
import * as zlib from "node:zlib";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "fs";
import { join } from "node:path";
import { usageFromEvent, type ProviderUsage } from "./prompt-accounting.js";

export type LedgerMark = {
  mark_id?: number | unknown;
  session_id?: string;
  seq_start: number;
  seq_end: number;
  kind?: string;
  action: string;
  digest?: string | null;
  reason?: string;
  applied_gen?: string | null;
  applied_at?: number | null;
  // Written by the pass; kept for the ledger and the report, no longer an admission order.
  pressure?: number | null;
  topic_dormancy_s?: number | null;
  stmt_age_s?: number | null;
  /** The prune epoch the mark was admitted in (NULL = 0, the marks applied before the cycle). */
  prune_epoch?: number | null;
};

const PRESERVE_USER_TURNS = 8;
const LOAD_CHUNK = 5000;

/** Prune cycle (Vern 2026-11-03; design suits/astinus/analysis/PRUNE-CYCLE-DESIGN-20261103.md).
 *  Replaces the 2026-09-21 admission policy (floor, share, debounce, critical bypass) and the
 *  0.90/0.70 hard cap. At pruneAtPressure one prune runs and cuts the view to pruneToPressure;
 *  between prunes the view is a fixed function of the ledger, so the provider's prefix cache
 *  holds - the reason the debounce existed. Pressure is the count-anchored estimate over the
 *  host message budget (window - reserve - system prompt). Config: `astinusMarks.*`. */
export type PrunePolicy = {
  pruneAtPressure: number;
  pruneToPressure: number;
};

export const DEFAULT_PRUNE_POLICY: PrunePolicy = {
  pruneAtPressure: 0.6,
  pruneToPressure: 0.25,
};

/** A prune that would remove less than this share of the view is not committed: a session whose
 *  preserved tail alone sits over the threshold would otherwise re-prune, and miss the cache, on
 *  every request. */
const MIN_PRUNE_GAIN = 0.05;

/** Tool-call arguments up to this size stay verbatim under a placeholder mark: they are cheap
 *  and say what was asked. */
const ARGS_KEEP_CHARS = 200;

/** Canonical tool-call-id form. The host strict sanitizer strips characters from RUNTIME ids
 *  (measured 203 of 203 lose the underscore for openai-completions - Claude root cause
 *  2026-10-11), so a stored id and its runtime twin must collapse to one key. Stripping every
 *  non-alphanumeric survives whatever cosmetic transform the sanitizer picks next. Any
 *  resulting collision is caught by the tc-key ambiguity refusal (-1) in SeqIndex. */
export function canonicalId(raw: unknown): string {
  return typeof raw === "string" ? raw.replace(/[^a-zA-Z0-9]/g, "") : "";
}

export function identityKeyOf(m: unknown): string {
  const msg = (m ?? {}) as Record<string, unknown>;
  if (typeof msg.idempotencyKey === "string" && msg.idempotencyKey) return "ik:" + msg.idempotencyKey;
  const inner = (msg.message ?? {}) as Record<string, unknown>;
  const ik = typeof inner.idempotencyKey === "string" ? inner.idempotencyKey : "";
  if (ik) return "ik:" + ik;
  const role = typeof msg.role === "string" ? msg.role : "";
  const ts = typeof msg.timestamp === "number" ? msg.timestamp : 0;
  const tcRaw = (msg.toolCallId ?? msg.tool_call_id ?? inner.toolCallId) as unknown;
  const tc = canonicalId(tcRaw);
  return `rt:${role}:${ts}:${tc}`;
}

/** Guarded tool-call fallback key.
 *
 *  WHY IT EXISTS: toolResult events carry no idempotencyKey (census: 0 of 453 stored), so they
 *  depend entirely on the rt: composite key - and that key embeds the toolCallId, which the HOST
 *  REWRITES for openai-* apis. sanitizeToolCallIdsForCloudCodeAssist in strict mode strips
 *  punctuation (call_abc -> callabc, measured 203 of 203 on the live cohort), the rewritten list
 *  is written back to state.messages and handed to the engine, while SeqIndex is built from
 *  transcript_events holding the ORIGINAL ids - so no toolResult could resolve. canonicalId on
 *  BOTH sides is what restores the join; this key is additionally timestamp-free.
 *
 *  CORRECTED 2026-10-11 (Claude; Codex pre-build review item 6): an earlier version of this
 *  comment claimed the runtime objects "do not reproduce" the numeric timestamp. That is
 *  WITHDRAWN, and it is now disproved by the census in this same file - live readings are
 *  ts(num/str/undef)=200/0/0, i.e. every runtime toolResult DOES carry a numeric timestamp. The
 *  id rewrite was the cause, not the timestamp. That version also called activeSession.messages
 *  "the only variable"; membership is a second one - the assembled view is a subset of its own
 *  seq span.
 *
 *  BOUND (Codex item 2): canonicalId reverses PUNCTUATION STRIPPING ONLY. The host also clips ids
 *  to 40 chars and can hash/suffix on collision, and strict9 uses another transform - a clipped
 *  or suffixed id would still miss. Live cohort ids are 29/32 chars, so clipping is unreachable
 *  today. This is a CONTAINMENT; the durable fix is carrying the store seq onto the message.
 *
 *  AMBIGUITY GUARD (Claude delta review D2): register once, first-wins; a second registration
 *  with a different seq marks the key ambiguous (-1) and seqOf refuses it - a reused toolCallId
 *  can never apply a mark to the wrong source. */
export function toolCallKeyOf(m: unknown): string | null {
  const msg = (m ?? {}) as Record<string, unknown>;
  const inner = (msg.message ?? {}) as Record<string, unknown>;
  const role = typeof msg.role === "string" ? msg.role : typeof inner.role === "string" ? inner.role : "";
  if (role !== "toolResult") return null;
  const raw = (msg.toolCallId ?? msg.tool_call_id ?? inner.toolCallId) as unknown;
  const cid = canonicalId(raw);
  return cid ? "tc:" + cid : null;
}

function eventIdOf(eventJson: string): string | null {
  try {
    const ev = JSON.parse(eventJson) as Record<string, unknown>;
    return typeof ev.id === "string" && ev.id ? ev.id : null;
  } catch {
    return null;
  }
}

function eventIdentity(eventJson: string): string | null {
  try {
    const ev = JSON.parse(eventJson) as Record<string, unknown>;
    const msg = (ev.message ?? ev) as Record<string, unknown>;
    if (typeof msg.role !== "string" || !msg.role) return null;
    return identityKeyOf(msg);
  } catch {
    return null;
  }
}

/** Host-shaped token estimate (B4, after preemptive-compaction.ts:27-31): text/4, toolResult
 *  chars/2, other JSON/3, +12/message +6/block, x1.2. */
export function estimateTokensHost(list: unknown[]): number {
  let total = 0;
  for (const m of list) {
    const msg = (m ?? {}) as Record<string, unknown>;
    let blocks = 0;
    let chars = 0;
    const c = msg.content;
    if (typeof c === "string") {
      chars += c.length / 4; // string content is text — /4 like every other text block
      blocks += 1;
    } else if (Array.isArray(c)) {
      for (const p of c as Array<Record<string, unknown>>) {
        blocks += 1;
        const t = typeof p?.text === "string" ? p.text : "";
        if (msg.role === "toolResult") chars += t.length / 2;
        else if (t) chars += t.length / 4;
        else chars += JSON.stringify(p ?? {}).length / 3;
      }
    }
    if (msg.role === "thinking" && typeof c === "string") chars += c.length / 3;
    total += chars + 12 + blocks * 6;
  }
  return Math.ceil(total * 1.2);
}

function resolveMarkRange(
  mark: LedgerMark,
  ix: SeqIndex
): { lo: number; hi: number } | null {
  const first = (mark as { anchor_first_event?: unknown }).anchor_first_event;
  const last = (mark as { anchor_last_event?: unknown }).anchor_last_event;
  if (typeof first !== "string" || !first || typeof last !== "string" || !last) {
    return { lo: Number(mark.seq_start), hi: Number(mark.seq_end) };
  }
  const a = ix.eventSeq(first);
  const b = ix.eventSeq(last);
  if (a === null || b === null) return null;
  return { lo: Math.min(a, b), hi: Math.max(a, b) };
}

/**
 * Replace reasoning and tool-call payloads in one message, keeping its text and identity.
 * Tool-call arguments stay an empty object so the provider still receives valid JSON.
 */
function slimBlocks(msg: unknown, keptResults?: Set<string>): unknown {
  const m = (msg ?? {}) as Record<string, unknown>;
  if (!Array.isArray(m.content)) return msg;
  let changed = false;
  const content = (m.content as Array<Record<string, unknown>>).map((b) => {
    if (!b || typeof b !== "object") return b;
    const type = typeof b.type === "string" ? b.type : "";
    if (type === "thinking" || type === "reasoning") {
      // A text part, not a rewritten thinking block: a thinking block keeps its signature, and
      // a provider that verifies signatures rejects edited reasoning (Claude review 11-03).
      changed = true;
      return { type: "text", text: "[Astinus: reasoning trimmed]" };
    }
    if (
      type === "toolCall" ||
      type === "tool_call" ||
      type === "toolUse" ||
      type === "tool_use" ||
      type === "function_call"
    ) {
      changed = true;
      const id = b.id ?? b.toolCallId;
      if (keptResults && !(typeof id === "string" && keptResults.has(id))) {
        // Its result is redacted with the interior, so the call cannot stay a call.
        const name = typeof b.name === "string" ? b.name : "";
        return { type: "text", text: `[Astinus: tool call ${name} trimmed with its result]` };
      }
      return { ...b, arguments: {}, input: {} };
    }
    return b;
  });
  return changed ? { ...m, content } : msg;
}

export class SeqIndex {
  private db: DatabaseSync | null = null;
  private lastSeq = 0;
  /** Event id indexed at lastSeq. Its disappearance means the rows were reindexed. */
  private lastSeqId: string | null = null;
  private readonly map = new Map<string, number>();
  private loadedOnce = false;
  /** Whether the host store has the compressed payload columns (OpenClaw zstd transcript rows). */
  private zstdCols: boolean | null = null;
  failed = false;

  /** On-disk cache of the identity map (2026-11-17): a cold build walks every event of the
   *  session - decompressing and parsing each row - which froze the gateway for 15-48 s on a
   *  35k-event session. The map is a pure function of (dbPath, sessionId, transcript rows), so it
   *  can be persisted and reloaded; the tail watermark still validates it on every open. */
  private readonly cachePath: string | null;
  private loadedFromCache = false;

  constructor(readonly dbPath: string, readonly sessionId: string, cacheDir?: string | null) {
    const dir = typeof cacheDir === "string" && cacheDir ? cacheDir : null;
    this.cachePath = dir
      ? join(dir, createHash("sha1").update(dbPath + "|" + sessionId).digest("hex") + ".json")
      : null;
  }

  /** Size of the identity map (tests and diagnostics). */
  get size(): number {
    return this.map.size;
  }

  /** Whether this instance restored its map from the on-disk cache. */
  get fromCache(): boolean {
    return this.loadedFromCache;
  }

  /** Highest seq this index has processed (tests and diagnostics). */
  get lastSeqSeen(): number {
    return this.lastSeq;
  }

  private loadCache(): void {
    if (!this.cachePath || !existsSync(this.cachePath)) return;
    try {
      const parsed = JSON.parse(readFileSync(this.cachePath, "utf-8")) as {
        v?: number;
        lastSeq?: number;
        lastSeqId?: string | null;
        entries?: Array<[string, number]>;
      };
      if (parsed.v !== 1 || !Array.isArray(parsed.entries)) return;
      for (const pair of parsed.entries) {
        if (Array.isArray(pair) && typeof pair[0] === "string" && Number.isFinite(pair[1])) {
          this.map.set(pair[0], Number(pair[1]));
        }
      }
      this.lastSeq = Number.isFinite(parsed.lastSeq) ? Number(parsed.lastSeq) : -1;
      this.lastSeqId = typeof parsed.lastSeqId === "string" ? parsed.lastSeqId : null;
      this.loadedOnce = true;
      this.loadedFromCache = true;
    } catch {
      this.map.clear();
      this.loadedOnce = false;
    }
  }

  private saveCache(): void {
    if (!this.cachePath) return;
    try {
      mkdirSync(join(this.cachePath, ".."), { recursive: true });
      const entries: Array<[string, number]> = [];
      for (const [k, v] of this.map) entries.push([k, v]);
      const body = JSON.stringify({ v: 1, lastSeq: this.lastSeq, lastSeqId: this.lastSeqId, entries });
      const tmp = this.cachePath + ".tmp";
      writeFileSync(tmp, body, "utf-8");
      renameSync(tmp, this.cachePath);
    } catch {
      /* cache is an optimisation; never fail the assemble over it */
    }
  }

  /** Payload columns to select: compressed rows carry event_json NULL and the event in event_zstd. */
  private payloadCols(): string {
    if (this.zstdCols === null) {
      try {
        const cols = this.db!.prepare("PRAGMA table_info(transcript_events)").all() as Array<{ name?: unknown }>;
        this.zstdCols = cols.some((c) => String(c.name) === "event_zstd");
      } catch {
        this.zstdCols = false;
      }
    }
    return this.zstdCols ? "event_json, event_zstd" : "event_json";
  }

  private open(): boolean {
    if (this.db) return true;
    if (this.failed || !existsSync(this.dbPath)) {
      this.failed = true;
      return false;
    }
    try {
      this.db = new DatabaseSync(this.dbPath, { readOnly: true });
      this.loadCache();
      return true;
    } catch {
      this.failed = true;
      return false;
    }
  }

  /** One refresh per pass (M5); loop until a chunk comes back short so a fresh index converges. */
  /** True when the row at lastSeq still carries the id this index recorded for it. */
  private tailUnchanged(): boolean {
    if (this.lastSeqId === null) return true;
    try {
      const row = this.db!
        .prepare(`SELECT ${this.payloadCols()} FROM transcript_events WHERE session_id = ? AND seq = ?`)
        .get(this.sessionId, this.lastSeq) as { event_json: string | null; event_zstd?: Uint8Array | null } | undefined;
      if (!row) return false;
      const json = row.event_json ?? decodeZstd(row.event_zstd ?? null);
      return json !== null && eventIdOf(json) === this.lastSeqId;
    } catch {
      return false;
    }
  }

  refresh(): void {
    if (!this.open()) return;
    try {
      // A manual trim or a header repair keeps event ids but moves them, and the
      // incremental read below only walks seq > lastSeq, so every surviving row
      // would keep its old coordinate. Reload from zero when the tail moved.
      if (this.loadedOnce && !this.tailUnchanged()) {
        this.map.clear();
        this.lastSeq = -1;
        this.lastSeqId = null;
      }
      let rows: Array<{ seq: number; event_json: string | null; event_zstd?: Uint8Array | null }> = [];
      do {
        rows = this.db!
          .prepare(
            `SELECT seq, ${this.payloadCols()} FROM transcript_events WHERE session_id = ? AND seq > ? ORDER BY seq LIMIT ?`
          )
          .all(this.sessionId, this.lastSeq, LOAD_CHUNK) as typeof rows;
        for (const r of rows) {
          // A compressed row (event_json NULL) decodes from event_zstd; an undecodable row only
          // advances the cursor, as an identity-less row always did.
          const json = r.event_json ?? decodeZstd(r.event_zstd ?? null);
          // One parse per row (2026-11-17): eventIdentity and eventIdOf used to parse the same
          // string twice, which was ~7 s of the 15 s cold build on topic 2549's 35k events.
          let ev: Record<string, unknown> | null = null;
          if (json !== null) {
            try {
              ev = JSON.parse(json) as Record<string, unknown>;
            } catch {
              ev = null;
            }
          }
          const msg = ev === null ? null : ((ev.message ?? ev) as Record<string, unknown>);
          const key =
            msg !== null && typeof msg.role === "string" && msg.role ? identityKeyOf(msg) : null;
          if (Number(r.seq) > this.lastSeq) {
            this.lastSeq = Number(r.seq);
            this.lastSeqId =
              ev !== null && typeof ev.id === "string" && ev.id ? (ev.id as string) : null;
          }
          if (!key) continue;
          this.map.set(key, Number(r.seq));
          if (msg !== null) {
            try {
              const tcKey = toolCallKeyOf(msg);
              if (tcKey) {
                const prev = this.map.get(tcKey);
                if (prev === undefined) this.map.set(tcKey, Number(r.seq));
                else if (prev !== Number(r.seq)) this.map.set(tcKey, -1); // ambiguous: refuse
              }
            } catch {
              /* bonus key only */
            }
          }
        }
      } while (rows.length === LOAD_CHUNK);
      this.loadedOnce = true;
      this.saveCache();
    } catch {
      /* degrade: whatever is indexed still resolves */
    }
  }

  get ready(): boolean {
    return this.loadedOnce;
  }

  /** Current coordinate of a host event id, or null when it is gone. */
  eventSeq(eventId: string): number | null {
    if (!this.open()) return null;
    try {
      const row = this.db!
        .prepare(
          "SELECT seq FROM transcript_event_identities WHERE session_id = ? AND event_id = ?"
        )
        .get(this.sessionId, eventId) as { seq?: number } | undefined;
      const seq = Number(row?.seq);
      return Number.isFinite(seq) ? seq : null;
    } catch {
      return null;
    }
  }

  /** Release the read handle. Windows refuses to delete a directory holding an open DB. */
  close(): void {
    try {
      this.db?.close();
    } catch {
      /* already closed or unusable */
    }
    this.db = null;
  }

  seqOf(message: unknown): number | null {
    const direct = this.map.get(identityKeyOf(message));
    if (direct !== undefined && direct !== -1) return direct;
    const tcKey = toolCallKeyOf(message);
    const viaTc = tcKey ? this.map.get(tcKey) : undefined;
    return viaTc !== undefined && viaTc !== -1 ? viaTc : null;
  }

  /**
   * Provider usage of the first model call after `afterSeq`: the call that the view ending at
   * afterSeq fed (count anchor, 2026-11-03). Reads at most `limit` rows; compressed rows are
   * decoded when the runtime has zstd, else skipped.
   */
  firstUsageAfter(afterSeq: number, limit = 64): ProviderUsage | null {
    if (!this.open()) return null;
    try {
      const rows = this.db!
        .prepare(
          "SELECT seq, event_json, event_zstd FROM transcript_events WHERE session_id = ? AND seq > ? ORDER BY seq LIMIT ?"
        )
        .all(this.sessionId, afterSeq, limit) as Array<{
        seq: number;
        event_json: string | null;
        event_zstd: Uint8Array | null;
      }>;
      for (const r of rows) {
        const json = r.event_json ?? decodeZstd(r.event_zstd);
        if (!json) continue;
        const usage = usageFromEvent({ seq: Number(r.seq), eventJson: json });
        if (usage) return usage;
      }
    } catch {
      /* degrade: no anchor this call */
    }
    return null;
  }
}

function decodeZstd(blob: Uint8Array | null): string | null {
  if (!blob) return null;
  const fn = (zlib as unknown as { zstdDecompressSync?: (b: Buffer) => Buffer }).zstdDecompressSync;
  if (typeof fn !== "function") return null;
  try {
    return fn(Buffer.from(blob)).toString("utf8");
  } catch {
    return null;
  }
}

const seqIndexes = new Map<string, SeqIndex>();
function seqIndexFor(dbPath: string, sessionId: string, cacheDir?: string | null): SeqIndex {
  // M4: key by (dbPath, sessionId), not session key; the cache dir joins the key so two callers
  // with different cache roots never share an instance.
  const cacheKey = `${dbPath}|${sessionId}|${cacheDir ?? ""}`;
  let ix = seqIndexes.get(cacheKey);
  if (!ix) {
    ix = new SeqIndex(dbPath, sessionId, cacheDir);
    if (seqIndexes.size >= 200) {
      const oldest = seqIndexes.keys().next().value;
      if (oldest !== undefined) {
        seqIndexes.get(oldest)?.close();
        seqIndexes.delete(oldest);
      }
    }
    seqIndexes.set(cacheKey, ix);
  }
  return ix;
}

/** Test/diagnostic hook: instance state without exposing the class. */
export function seqIndexDebug(
  dbPath: string,
  sessionId: string,
  cacheDir?: string | null
): { size: number; fromCache: boolean; lastSeq: number } {
  const ix = seqIndexFor(dbPath, sessionId, cacheDir);
  ix.refresh();
  return { size: ix.size, fromCache: ix.fromCache, lastSeq: ix.lastSeqSeen };
}

/** Drop every cached instance (tests; also a way to release handles). */
export function clearSeqIndexes(): void {
  for (const ix of seqIndexes.values()) ix.close();
  seqIndexes.clear();
}

/** Provider usage of the first model call after `afterSeq` in this host session (count anchor). */
export function providerUsageAfter(dbPath: string, sessionId: string, afterSeq: number): ProviderUsage | null {
  return seqIndexFor(dbPath, sessionId).firstUsageAfter(afterSeq);
}

export type ApplyContext = {
  sessionKey: string;
  hostDbPath: string;
  sessionId: string; // B1: the HOST session UUID, never the session key
  tokenBudget: number;
  turnCounter?: number; // kept for callers; the prune cycle has no turn clock
  policy?: Partial<PrunePolicy>; // `astinusMarks.*`; DEFAULT_PRUNE_POLICY otherwise
  /** The session's current prune epoch (ledger meta.prune_epoch); 0 when it was never pruned. */
  pruneEpoch?: number;
  /** Provider/host-estimate ratio for this session (count anchor); pressure uses tokensBefore x scale. */
  pressureScale?: number;
  /** Where the identity-index cache lives (host state); omitted = build in memory every process. */
  indexCacheDir?: string | null;
};

export type PruneOutcome = {
  epoch: number; // the new epoch; the admitted marks and the fallback are stamped with it
  tokensBefore: number; // the steady view the prune cut (host-shaped estimate)
  pressureBefore: number;
  pressureAfter: number;
  dropped: number; // earlier-epoch marks whose ranges were removed outright
  fallback: LedgerMark | null; // the oldest-turns cut, for the caller to insert into the ledger
};

export type ApplyResult = {
  messages: unknown[];
  admitted: LedgerMark[]; // admitted by this call's prune (stamp these with prune.epoch)
  marksInEffect: number; // every applied mark of the view, own action or drop (authority basis)
  changed: boolean;
  pendingCount: number; // pending marks waiting for the next prune
  tokensBefore: number;
  tokensAfter: number;
  skipped: string[];
  windowFirstSeq: number | null; // min mapped seq of this view (2026-10-07): the pass bounds proposals to the assembled window
  windowLastSeq: number | null; // max mapped seq of this view: the next model call's usage follows it (count anchor)
  prune: PruneOutcome | null; // set when this call ran and committed a prune
};

/** All toolCall ids in an assistant message (P3: parallel calls defeat a single-id lookup). */
function toolCallIdsOf(m: unknown): string[] {
  const msg = (m ?? {}) as Record<string, unknown>;
  if (msg.role !== "assistant") return [];
  const c = msg.content;
  if (!Array.isArray(c)) return [];
  const ids: string[] = [];
  for (const p of c as Array<Record<string, unknown>>) {
    const id = (p?.id ?? p?.toolCallId) as unknown;
    if ((p?.type === "toolCall" || p?.type === "tool_use") && typeof id === "string") ids.push(id);
  }
  return ids;
}

function toolResultCallIds(m: unknown): string[] {
  const id = ((m ?? {}) as Record<string, unknown>).toolCallId as unknown;
  return typeof id === "string" ? [id] : [];
}

function isToolResult(m: unknown): boolean {
  return ((m ?? {}) as Record<string, unknown>).role === "toolResult";
}

/** Placeholder reach (2026-11-03, item 3): the assistant side of a placeholdered range. Reasoning
 *  is dropped and large tool-call arguments are stubbed; the call keeps its id and name so the
 *  pair stays valid. Measured on topic 2549: thinking 43% and call bodies 18% of the view, and
 *  the host replays reasoning_content for zai and deepseek (openai-completions-replay.ts), so
 *  both reach the wire. Returns null when nothing changes. */
function reduceAssistantBody(msg: Record<string, unknown>): Record<string, unknown> | null {
  if (msg.role !== "assistant" || !Array.isArray(msg.content)) return null;
  let changed = false;
  let reasoningChars = 0;
  const content: unknown[] = [];
  for (const b of msg.content as Array<Record<string, unknown>>) {
    if (b?.type === "thinking") {
      const t = typeof b.thinking === "string" ? b.thinking : typeof b.text === "string" ? b.text : "";
      if (t.length > 0) {
        reasoningChars += t.length;
        changed = true;
        continue;
      }
    }
    if ((b?.type === "toolCall" || b?.type === "tool_use") && b.arguments !== undefined) {
      const raw = JSON.stringify(b.arguments ?? {});
      if (raw.length > ARGS_KEEP_CHARS) {
        content.push({ ...b, arguments: { astinus_pruned: `arguments pruned by Astinus (${raw.length} chars)` } });
        changed = true;
        continue;
      }
    }
    content.push(b);
  }
  if (!changed) return null;
  // An assistant message left with no parts is skipped by the host converter; keep one line so
  // the turn shape is unchanged.
  if (content.length === 0) {
    content.push({ type: "text", text: `[reasoning pruned by Astinus (${reasoningChars} chars)]` });
  }
  return { ...msg, content };
}

/** A message Astinus wrote into the view (digest, tombstone, fallback cut): an assistant message
 *  whose first part opens with one of the engine's own prefixes. Callers also require it to map to
 *  no transcript seq, so a host message slimmed in place is never taken for one. */
function isAstinusMessage(m: unknown): boolean {
  const msg = (m ?? {}) as Record<string, unknown>;
  if (msg.role !== "assistant" || !Array.isArray(msg.content)) return false;
  const first = (msg.content as Array<Record<string, unknown>>)[0];
  const text = typeof first?.text === "string" ? first.text : "";
  return text.startsWith("[Astinus") || text.startsWith("[injected context");
}

function timestampOf(m: unknown): number {
  const ts = ((m ?? {}) as Record<string, unknown>).timestamp;
  return typeof ts === "number" ? ts : 0;
}

/** The prune's last resort: the oldest whole turns after the first user message, user messages
 *  included, as ONE range ending at a user-turn boundary, until pressure is at the target. Vern:
 *  "oldest is not best" - the pass's topic-based marks go first; this only guarantees the target.
 *  The range must be tool-pair-closed and stays clear of the preserved tail. Returns a digest mark
 *  of the new epoch (the caller writes it to the ledger, so a rebuilt view reproduces the cut). */
function fallbackCut(
  list: unknown[],
  ix: SeqIndex,
  tokens: number,
  pressureOf: (t: number) => number,
  target: number,
  epoch: number,
  sessionId: string
): LedgerMark | null {
  const firstUser = list.findIndex((m) => ((m ?? {}) as Record<string, unknown>).role === "user");
  if (firstUser < 0) return null;
  const tailFloor = preservedFloorIdx(list);
  const start = firstUser + 1;
  if (start >= tailFloor) return null;
  const cost = list.map((m) => estimateTokensHost([m]));
  let removed = 0;
  let best: { end: number; turns: number } | null = null;
  let turns = 0;
  const calls = new Set<string>();
  const results = new Set<string>();
  for (let i = start; i < tailFloor; i++) {
    removed += cost[i];
    for (const id of toolCallIdsOf(list[i])) calls.add(canonicalId(id));
    for (const id of toolResultCallIds(list[i])) results.add(canonicalId(id));
    const nextIsUser = ((list[i + 1] ?? {}) as Record<string, unknown>).role === "user";
    if (!nextIsUser) continue;
    turns++;
    let closed = calls.size === results.size;
    for (const id of calls) if (!results.has(id)) closed = false;
    if (!closed) continue;
    best = { end: i, turns };
    if (pressureOf(tokens - removed) <= target) break;
  }
  if (!best) return null;
  let lo: number | null = null;
  let hi: number | null = null;
  for (let i = start; i <= best.end; i++) {
    const s = ix.seqOf(list[i]);
    if (s === null) continue;
    if (lo === null || s < lo) lo = s;
    if (hi === null || s > hi) hi = s;
  }
  if (lo === null || hi === null) return null;
  return {
    session_id: sessionId,
    seq_start: lo,
    seq_end: hi,
    kind: "turn",
    action: "digest",
    digest:
      `${best.turns} earlier turn(s) removed at prune ${epoch}; recall, enrich or the transcript ` +
      `(seq ${lo}-${hi}) hold them`,
    reason: "prune_fallback",
    applied_at: 1,
    prune_epoch: epoch,
  };
}

/** The index of the first message inside the preserved tail: the last PRESERVE_USER_TURNS user
 *  turns (M3), not a fixed message count. */
function preservedFloorIdx(messages: unknown[]): number {
  let userTurns = 0;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (((messages[i] ?? {}) as Record<string, unknown>).role === "user") {
      userTurns++;
      if (userTurns >= PRESERVE_USER_TURNS) return i;
    }
  }
  return 0;
}

export function applyMarks(
  messages: unknown[],
  marks: LedgerMark[],
  ctx: ApplyContext
): ApplyResult {
  const tokensBefore = estimateTokensHost(messages);
  const out: ApplyResult = {
    messages,
    admitted: [],
    marksInEffect: 0,
    changed: false,
    tokensBefore,
    tokensAfter: tokensBefore,
    skipped: [],
    pendingCount: 0,
    windowFirstSeq: null,
    windowLastSeq: null,
    prune: null,
  };
  // 2026-10-07 (Vern: "so fix it"): the assembled window's first mapped seq. The pass bounds
  // its proposals to the window the host actually assembles - marks outside it can never
  // map ("no eligible message in view" forever) and the pools filled with unapplicable
  // marks while the in-window content sat unmarked. Computed before the early returns so
  // every session reports it, marks or not. Same cached SeqIndex the loop below uses.
  try {
    const wix = seqIndexFor(ctx.hostDbPath, ctx.sessionId, ctx.indexCacheDir);
    wix.refresh();
    let wfs: number | null = null;
    let wls: number | null = null;
    for (const m of messages) {
      const s = wix.seqOf(m);
      if (s !== null && (wfs === null || s < wfs)) wfs = s;
      if (s !== null && (wls === null || s > wls)) wls = s;
    }
    out.windowFirstSeq = wfs;
    out.windowLastSeq = wls;
  } catch {
    /* degrade: windowFirstSeq stays null; the pass falls back to the compaction bound */
  }
  // DECISIVE INSTRUMENTATION (eighth verdict E1 / view-ratchet, 2026-10-11): a per-assemble census
  // of the RUNTIME toolResult objects - settles (a) whether the rt: fallback resolves them,
  // (b) whether the runtime timestamp reproduces (typeof), (c) whether targets already carry an
  // Astinus stamp. Fail-soft; never affects the view. Logged at info by the caller. Runs BEFORE
  // the early returns so every assemble reports it, marks or not.
  try {
    const dix = seqIndexFor(ctx.hostDbPath, ctx.sessionId, ctx.indexCacheDir);
    dix.refresh();
    let dn = 0, dres = 0, dtsNum = 0, dtsStr = 0, dtsUndef = 0, dstamped = 0;
    for (const m of messages) {
      const msg = (m ?? {}) as Record<string, unknown>;
      const inner = (msg.message ?? {}) as Record<string, unknown>;
      const role = typeof msg.role === "string" ? msg.role : typeof inner.role === "string" ? inner.role : "";
      if (role !== "toolResult") continue;
      dn++;
      if (dix.seqOf(m) !== null) dres++;
      const ts = msg.timestamp;
      if (typeof ts === "number") dtsNum++;
      else if (typeof ts === "string") dtsStr++;
      else dtsUndef++;
      const c = msg.content;
      const text = typeof c === "string" ? c : JSON.stringify(c ?? "");
      if (text.indexOf("Astinus digest of seq") !== -1 || text.indexOf("tool output pruned by Astinus") !== -1) dstamped++;
    }
    (out as unknown as Record<string, unknown>).trDiag = { n: dn, resolved: dres, tsNum: dtsNum, tsStr: dtsStr, tsUndef: dtsUndef, stamped: dstamped };
  } catch { /* degrade: no diag; never affects the view */ }

  const policy = { ...DEFAULT_PRUNE_POLICY, ...(ctx.policy ?? {}) };
  const epoch = typeof ctx.pruneEpoch === "number" && ctx.pruneEpoch > 0 ? Math.floor(ctx.pruneEpoch) : 0;
  // Count anchor (2026-11-03): the char estimate is scaled by the session's measured
  // provider/estimate ratio, so pressure reflects what the model actually receives.
  const scale = typeof ctx.pressureScale === "number" && ctx.pressureScale > 0 ? ctx.pressureScale : 1;
  const pressureOf = (t: number) => (ctx.tokenBudget > 0 ? (t * scale) / ctx.tokenBudget : 0);
  // Valid marks: well formed and not retired by the one-time rebase (2026-11-02).
  const valid = marks.filter(
    (m) =>
      m &&
      Number.isFinite(m.seq_start) &&
      Number.isFinite(m.seq_end) &&
      (m as { retired_at?: number | null }).retired_at == null
  );
  // Actionable with its own action: a digest mark with no digest text replaces nothing (item 1),
  // except turn_middle, which redacts in place and carries no digest by design. This gates only
  // a mark's OWN action; a drop needs no text (Codex final review N3).
  const actionable = (m: LedgerMark) =>
    m.action !== "digest" ||
    (typeof m.digest === "string" && m.digest.length > 0) ||
    (typeof m.reason === "string" && m.reason.indexOf("turn_middle") === 0);
  // Every applied mark joins the epoch cycle, text or not: the legacy applied empty digests (52 on
  // topic 2549) must drop at the first prune like any other epoch-0 mark. Pending marks must be
  // actionable to be admitted.
  const applied = valid.filter((m) => m.applied_at != null);
  const pending = valid.filter((m) => m.applied_at == null && actionable(m));
  out.pendingCount = pending.length;
  const epochOf = (m: LedgerMark) => {
    const e = Number(m.prune_epoch ?? 0);
    return Number.isFinite(e) ? e : 0;
  };
  // An earlier epoch's mark removes its range outright: nothing a prune leaves lasts past the next
  // one (Vern: "whats the point of keeping tombstones forever ? memory is recall and enrich").
  const asDrop = (m: LedgerMark): LedgerMark => ({ ...m, action: "drop", reason: "prune_dropped" });

  // Steady view: the current epoch's actionable marks with their own action, earlier epochs
  // dropped, pending marks waiting. A fixed function of the ledger, so request after request it is
  // byte-identical. A current-epoch mark that cannot act stays inert (item 1: not in effect).
  const steadySet = applied.flatMap((m) =>
    epochOf(m) < epoch ? [asDrop(m)] : actionable(m) ? [m] : []
  );
  const steady = applySet(messages, steadySet, ctx);
  const steadyTokens = steady.changed ? estimateTokensHost(steady.list) : tokensBefore;
  const steadyPressure = pressureOf(steadyTokens);
  const settleSteady = () => {
    out.messages = steady.changed ? steady.list : messages;
    out.changed = steady.changed;
    out.tokensAfter = steadyTokens;
    out.marksInEffect = steadySet.length;
    out.skipped.push(...steady.skipped);
  };
  if (!(ctx.tokenBudget > 0) || steadyPressure < policy.pruneAtPressure) {
    settleSteady();
    if (pending.length > 0) {
      out.skipped.push(
        `prune held: pressure ${steadyPressure.toFixed(2)} < ${policy.pruneAtPressure}; ${pending.length} mark(s) wait`
      );
    }
    return out;
  }

  // The prune. Everything the previous prune left goes: its own messages (digests, tombstones,
  // fallback cuts) are stripped from a view the host carried forward, and every earlier mark
  // drops its range. Then all pending marks are admitted at once - the pass picked them for being
  // off-topic or incidental, so they go first - and only then the oldest-turns fallback.
  const next = epoch + 1;
  const ix = seqIndexFor(ctx.hostDbPath, ctx.sessionId, ctx.indexCacheDir);
  ix.refresh();
  const base = messages.filter((m) => !(isAstinusMessage(m) && ix.seqOf(m) === null));
  const pruned = applySet(base, [...applied.map(asDrop), ...pending], ctx);
  let list = pruned.changed ? pruned.list : base;
  let tokens = estimateTokensHost(list);
  const admitted = pruned.applied.filter((m) => m.applied_at == null);
  let fallback: LedgerMark | null = null;
  if (pressureOf(tokens) > policy.pruneToPressure) {
    fallback = fallbackCut(list, ix, tokens, pressureOf, policy.pruneToPressure, next, ctx.sessionId);
    if (fallback) {
      const cut = applySet(list, [fallback], ctx);
      if (cut.changed) {
        list = cut.list;
        tokens = estimateTokensHost(list);
      } else {
        fallback = null;
      }
    }
  }
  const gain = steadyTokens > 0 ? (steadyTokens - tokens) / steadyTokens : 0;
  if (gain < MIN_PRUNE_GAIN) {
    settleSteady();
    out.skipped.push(
      `prune skipped: pressure ${steadyPressure.toFixed(2)} but it would remove ${(gain * 100).toFixed(1)}% (< ${MIN_PRUNE_GAIN * 100}%)`
    );
    return out;
  }
  out.messages = list;
  out.changed = true;
  out.tokensAfter = tokens;
  out.admitted = admitted;
  out.marksInEffect = applied.length + admitted.length + (fallback ? 1 : 0);
  out.skipped.push(...pruned.skipped);
  out.prune = {
    epoch: next,
    tokensBefore: steadyTokens,
    pressureBefore: steadyPressure,
    pressureAfter: pressureOf(tokens),
    dropped: applied.length,
    fallback,
  };
  out.skipped.push(
    `prune ${next}: pressure ${steadyPressure.toFixed(2)} -> ${out.prune.pressureAfter.toFixed(2)}; ` +
      `${admitted.length} admitted, ${applied.length} dropped` +
      (fallback ? `, oldest turns cut at seq ${fallback.seq_start}-${fallback.seq_end}` : "")
  );
  return out;
}

type SetResult = { list: unknown[]; changed: boolean; skipped: string[]; applied: LedgerMark[] };

/** Apply one set of marks to a view. Idempotent: run again on its own output it changes nothing,
 *  so a view the host carries forward and a view rebuilt from the transcript come out the same.
 *  Invariants: never the first user message, tool pairs travel together, nothing in the preserved
 *  tail. The seq map and the tail index follow every splice, so overlapping ranges stay exact. */
function applySet(messages: unknown[], marks: LedgerMark[], ctx: ApplyContext): SetResult {
  const res: SetResult = { list: messages, changed: false, skipped: [], applied: [] };
  if (marks.length === 0) return res;
  const ix = seqIndexFor(ctx.hostDbPath, ctx.sessionId, ctx.indexCacheDir);
  ix.refresh();
  const list = messages.slice();
  const seqs: (number | null)[] = list.map((m) => ix.seqOf(m));
  const firstUser = list.findIndex((m) => ((m ?? {}) as Record<string, unknown>).role === "user");
  const floorIdx = firstUser < 0 ? 0 : firstUser;
  let tailFloor = preservedFloorIdx(list);
  const replace = (start: number, count: number, items: unknown[], itemSeqs: (number | null)[]) => {
    list.splice(start, count, ...items);
    seqs.splice(start, count, ...itemSeqs);
    tailFloor = preservedFloorIdx(list);
    res.changed = true;
  };

  const ordered = [...marks].sort((a, b) => b.seq_start - a.seq_start);
  for (const mark of ordered) {
    // Anchors are the authority; the numbers are a cache (2026-11-02). A mark whose
    // boundary events are gone must not fall back to numbers that now hold other content.
    const resolved = resolveMarkRange(mark, ix);
    if (resolved === null) {
      res.skipped.push(`mark ${mark.mark_id}: anchored content is gone`);
      continue;
    }
    const lo = resolved.lo;
    const hi = resolved.hi;
    // A mark may partially overlap the preserved tail; the in-view part applies, the tail
    // part is protected by construction (it has no eligible index below tailFloor).
    const idxs: number[] = [];
    for (let i = floorIdx + 1; i < list.length; i++) {
      if (i >= tailFloor) continue;
      const s = seqs[i];
      if (s !== null && s >= lo && s <= hi) idxs.push(i);
    }
    if (idxs.length === 0) {
      res.skipped.push(`seq ${lo}-${hi}: no eligible message in view (preserved/unmapped/already applied)`);
      continue;
    }
    let start = idxs[0];
    let end = idxs[idxs.length - 1];
    // B3/P1/P3: pairs travel together. Enter the block when EITHER boundary touches a tool
    // message — a result OR a call (P1: a range ending on the call skips this block entirely,
    // dropping the call and orphaning its result).
    const startIsResult = isToolResult(list[start]);
    const endIsResult = isToolResult(list[end]);
    const endIsCall = toolCallIdsOf(list[end]).length > 0;
    if (startIsResult || endIsResult || endIsCall) {
      const needIds = new Set<string>();
      for (let i = start; i <= end; i++) {
        for (const cid of toolResultCallIds(list[i])) needIds.add(cid);
        for (const cid of toolCallIdsOf(list[i])) needIds.add(cid);
      }
      // P3: consider EVERY call id in an assistant message — parallel tool calls mean the
      // first id alone is not the link.
      while (start - 1 > floorIdx) {
        const prev = list[start - 1];
        const linked = toolCallIdsOf(prev).some((id) => needIds.has(id));
        if (linked) {
          start--;
          for (const cid of toolResultCallIds(list[start + 1])) needIds.add(cid);
          for (const cid of toolCallIdsOf(list[start])) needIds.add(cid);
        } else if (isToolResult(prev) && start - 1 < tailFloor) {
          start--; // absorb an immediately preceding result so it is not orphaned
          // ...and its call: without its id the next step stops at that call and orphans it.
          for (const cid of toolResultCallIds(list[start])) needIds.add(cid);
        } else break;
      }
      // P2: forward extension NEVER crosses tailFloor — pruning inside the preserved tail is
      // worse than leaving a mark unapplied.
      while (end + 1 < list.length && end + 1 < tailFloor) {
        const next = list[end + 1];
        const linked = toolCallIdsOf(next).some((id) => needIds.has(id));
        if (linked) {
          end++;
          for (const cid of toolResultCallIds(list[end])) needIds.add(cid);
        } else if (isToolResult(next)) {
          end++; // an adjacent unlinked result would be orphaned — take it too
        } else break;
      }
      // P2 guard: if completing any needed pair would enter the tail, leave BOTH messages.
      let neededInTail = false;
      for (let i = tailFloor; i < list.length && !neededInTail; i++) {
        if (toolResultCallIds(list[i]).some((cid) => needIds.has(cid))) neededInTail = true;
        else if (toolCallIdsOf(list[i]).some((cid) => needIds.has(cid))) neededInTail = true;
      }
      if (neededInTail) {
        res.skipped.push(
          `seq ${lo}-${hi}: completing the tool pair would enter the preserved tail; left both messages`
        );
        continue;
      }
    }
    if (mark.action !== "placeholder" && !spanIsPairClosed(list, start, end)) {
      res.skipped.push(`seq ${lo}-${hi}: the range would split a tool pair; left as is`);
      continue;
    }
    if (mark.action === "drop") {
      replace(start, end - start + 1, [], []);
    } else if (mark.action === "turn_middle" || (typeof mark.reason === "string" && mark.reason.indexOf("turn_middle") === 0)) {
      // Turn-middle redaction (design A.11; Vern #5502/#5503): preserve the span boundaries
      // and every steer (user input) inside; replace the model interior - reasoning, calls,
      // results - with tombstone segments carrying counts and the ledger lookup pointer.
      const keep = new Set<number>();
      keep.add(start);
      keep.add(end);
      for (let i = start + 1; i < end; i++) {
        const r = (list[i] as Record<string, unknown> | null)?.role;
        if (r === "user") keep.add(i);
      }
      // A kept boundary must not keep half of a tool pair whose other half is redacted: a result
      // whose call goes joins the interior, and a kept call whose result goes becomes text.
      for (const i of [start, end]) {
        if (!isToolResult(list[i])) continue;
        const ids = toolResultCallIds(list[i]);
        const callKept = [...keep].some((k) => k !== i && toolCallIdsOf(list[k]).some((id) => ids.includes(id)));
        if (!callKept) keep.delete(i);
      }
      const keptResults = new Set<string>();
      for (const k of keep) for (const id of toolResultCallIds(list[k])) keptResults.add(id);
      let interiorCount = 0;
      let alreadyRedacted = true;
      for (let i = start; i <= end; i++) {
        if (keep.has(i)) continue;
        interiorCount++;
        if (!(isAstinusMessage(list[i]) && seqs[i] === null)) alreadyRedacted = false;
      }
      if (interiorCount === 0 || alreadyRedacted) {
        // Idempotence: a carried-forward view already holds this redaction.
        res.skipped.push(`seq ${lo}-${hi}: turn_middle found no interior to redact`);
        continue;
      }
      const replacements: unknown[] = [];
      const replacementSeqs: (number | null)[] = [];
      let segStart = -1;
      let segCalls = 0;
      let segReason = 0;
      let segResults = 0;
      const flushSeg = (segEnd: number) => {
        if (segStart < 0) return;
        const n = segEnd - segStart + 1;
        replacements.push({
          role: "assistant",
          timestamp: timestampOf(list[segStart]),
          content: [
            {
              type: "text",
              text:
                `[Astinus: turn interior redacted - ${segCalls} tool calls, ${segReason} reasoning, ` +
                `${segResults} tool results over ${n} messages | seq ${lo}-${hi}; recover the ` +
                `middle from the ledger by seq range]`,
            },
          ],
        });
        replacementSeqs.push(null);
        segStart = -1;
        segCalls = 0;
        segReason = 0;
        segResults = 0;
      };
      for (let i = start; i <= end; i++) {
        if (keep.has(i)) {
          flushSeg(i - 1);
          // Item 3 (2026-11-03): a preserved boundary message still carries its own reasoning
          // and tool-call argument bodies. Marks must be able to reach those, or turn_middle
          // only trims the messages it already deletes.
          replacements.push(slimBlocks(list[i], keptResults));
          replacementSeqs.push(seqs[i]);
          continue;
        }
        if (segStart < 0) segStart = i;
        const msg = list[i] as Record<string, unknown> | null;
        if (msg && isToolResult(msg)) segResults++;
        segCalls += msg ? toolCallIdsOf(msg).length : 0;
        const content = (msg as any)?.content;
        if (Array.isArray(content) && content.some((b: any) => b?.type === "thinking")) segReason++;
      }
      flushSeg(end);
      replace(start, end - start + 1, replacements, replacementSeqs);
    } else if (mark.action === "digest" && mark.digest) {
      const digestMsg = {
        role: "assistant",
        // Byte-stable: no clock in anything the engine writes, so a rebuilt view reproduces it.
        timestamp: timestampOf(list[start]),
        content: [
          {
            type: "text",
            text:
              `[injected context — earlier records of this session, digested; not current state; ` +
              `verify before acting] Astinus digest of seq ${lo}-${hi}: ${mark.digest}`,
          },
        ],
      };
      replace(start, end - start + 1, [digestMsg], [null]);
    } else if (mark.action === "placeholder") {
      let placeholdered = false;
      for (let i = start; i <= end; i++) {
        const msg = list[i] as Record<string, unknown> | null;
        if (!msg) continue;
        if (!isToolResult(msg)) {
          const reduced = reduceAssistantBody(msg);
          if (reduced) {
            list[i] = reduced;
            placeholdered = true;
          }
          continue;
        }
        const c = msg.content;
        const first = Array.isArray(c) ? (c[0] as Record<string, unknown> | undefined)?.text : c;
        // Idempotence: a placeholder already in place keeps its bytes (re-measuring it would
        // write its own length and change the text on every carried-forward request).
        if (typeof first === "string" && first.startsWith("[tool output pruned by Astinus")) continue;
        const text = typeof c === "string" ? c : JSON.stringify(c ?? "");
        // M1: replace the ELEMENT — never mutate the host's message object in place. The
        // placeholder is a parts array, the same shape as the digest message.
        list[i] = {
          ...msg,
          content: [
            {
              type: "text",
              text: `[tool output pruned by Astinus (${text.length} chars); the call above retains its shape]`,
            },
          ],
        };
        placeholdered = true;
      }
      if (placeholdered) res.changed = true;
      else {
        res.skipped.push(`seq ${lo}-${hi}: placeholder found nothing to prune`);
        continue;
      }
    } else {
      res.skipped.push(`seq ${lo}-${hi}: unknown action ${mark.action}`);
      continue;
    }
    res.applied.push(mark);
  }
  res.list = list;
  return res;
}

/** True when removing list[start..end] leaves no tool call without its result and no result
 *  without its call: every id the span holds on one side it also holds on the other. */
function spanIsPairClosed(list: unknown[], start: number, end: number): boolean {
  const calls = new Set<string>();
  const results = new Set<string>();
  for (let i = start; i <= end; i++) {
    for (const id of toolCallIdsOf(list[i])) calls.add(canonicalId(id));
    for (const id of toolResultCallIds(list[i])) results.add(canonicalId(id));
  }
  if (calls.size !== results.size) return false;
  for (const id of calls) if (!results.has(id)) return false;
  return true;
}
