// Astinus mark application (plan v3 §8 + v4.2 + the assemble-marks review B1-B4/M1-M5).
// Identity join (never position); admission vs re-application split (B2); tool pairs by
// role+toolCallId (B3); host-shaped estimator (B4); element replacement not mutation (M1);
// parts-array digest (M2); 8-user-turn preserved tail (M3); (dbPath,sessionId) index key (M4);
// one refresh per pass (M5). Any failure degrades to "less pruned", never an error.
import { DatabaseSync } from "node:sqlite";
import { existsSync } from "fs";
import zlib from "node:zlib";
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
  // Admission priority inputs (Vern 2026-09-21); written by the pass, read here in that order.
  pressure?: number | null;
  topic_dormancy_s?: number | null;
  stmt_age_s?: number | null;
  // Anchors: host event ids at the mark's first/last row. Seq numbers move on a trim or a
  // header repair; ids do not. A mark whose anchor is gone is skipped, never re-aimed.
  anchor_first_event?: string | null;
  anchor_last_event?: string | null;
  retired_at?: number | null; // one-time rebase: numbers no longer trustworthy
};

/** Adds the anchor columns in place; an old ledger keeps its rows. Idempotent; never throws. */
export function ensureMarkAnchorColumns(db: DatabaseSync): void {
  for (const [table, col, type] of [
    ["marks", "anchor_first_event", "TEXT"],
    ["marks", "anchor_last_event", "TEXT"],
    ["marks", "retired_at", "INTEGER"],
    ["meta", "mark_rebase_done", "INTEGER"],
  ]) {
    try {
      const cols = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name?: unknown }>;
      if (cols.length > 0 && !cols.some((c) => c.name === col))
        db.exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${type}`);
    } catch {
      /* a locked or read-only ledger keeps working without anchors */
    }
  }
}

function eventIdOf(eventJson: string): string | null {
  try {
    const ev = JSON.parse(eventJson) as Record<string, unknown>;
    return typeof ev.id === "string" && ev.id ? ev.id : null;
  } catch {
    return null;
  }
}

const PRESERVE_USER_TURNS = 8;
/** F2 (design A.14): one setting, `turnMiddleAgeTurns`, is both the pass's proposal cutoff and
 *  the tail floor turn_middle marks apply under. Other mark kinds keep the 8-turn tail. */
export const DEFAULT_TURN_MIDDLE_AGE_TURNS = 6;
const LOAD_CHUNK = 5000;

// Host transcript payload contract (OpenClaw v2026.10.3 / a20345f3
// src/config/sessions/transcript-payload.ts prepareTranscriptPayload + registerDecoder,
// src/infra/zstd-codec.ts): a row is either identity (event_json TEXT) or compressed
// (event_json NULL, event_zstd = one Zstd frame, event_utf8_bytes = original UTF-8 length).
// The decode below copies the host's bounds and checks; it reads, never rewrites, the store.
const MAX_COMPRESSED_EVENT_BYTES = 4 * 1024 * 1024;
const utf8Decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

/** Decode one transcript row to its event JSON text, exactly as the host's reader does. Throws
 *  on any row it cannot decode; the caller must not treat such a row as identity-less. */
export function decodeTranscriptEventJson(row: {
  event_json?: unknown;
  event_zstd?: unknown;
  event_utf8_bytes?: unknown;
}): string {
  if (typeof row.event_json === "string") return row.event_json;
  const bytes = row.event_zstd;
  const rawBytes = row.event_utf8_bytes;
  if (bytes === null || bytes === undefined) {
    throw new Error("Transcript row has neither event_json nor event_zstd");
  }
  if (
    !(bytes instanceof Uint8Array) ||
    bytes.byteLength === 0 ||
    bytes.byteLength > MAX_COMPRESSED_EVENT_BYTES ||
    typeof rawBytes !== "number" ||
    !Number.isSafeInteger(rawBytes) ||
    rawBytes < 1 ||
    rawBytes > MAX_COMPRESSED_EVENT_BYTES
  ) {
    throw new Error("Invalid compressed transcript payload bounds");
  }
  // Node >= 22.15 / 23.8 ship zlib Zstd; the installed @types/node predates it, hence the cast.
  const zstdDecompressSync = (
    zlib as unknown as {
      zstdDecompressSync?: (data: Uint8Array, options: { maxOutputLength: number }) => Buffer;
    }
  ).zstdDecompressSync;
  if (typeof zstdDecompressSync !== "function") {
    throw new Error(
      `Cannot decode compressed transcript payload: zlib Zstd requires Node >= 22.15.0 or >= 23.8.0 (running ${process.versions.node})`
    );
  }
  const decoded = zstdDecompressSync.call(zlib, bytes, { maxOutputLength: rawBytes });
  if (decoded.byteLength !== rawBytes) {
    throw new Error("Compressed transcript payload length does not match its recorded UTF-8 size");
  }
  return utf8Decoder.decode(decoded);
}

/** Marks admission policy (Vern 2026-09-21). Defaults below; the plugin config `astinus.marks.*`
 *  supplies overrides through ApplyContext.policy. Mark from 0.30, admit the share of the pending
 *  pool in priority order, hold five turns between admissions, and let 0.65 bypass the debounce -
 *  a desk over its window must not wait. */
export type MarksAdmissionPolicy = {
  minPressure: number;
  admitShare: boolean;
  debounceTurns: number;
  criticalPressure: number;
};

export const DEFAULT_MARKS_POLICY: MarksAdmissionPolicy = {
  minPressure: 0.3,
  admitShare: true,
  debounceTurns: 5,
  criticalPressure: 0.65,
};

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
  if (typeof msg.idempotencyKey === "string" && msg.idempotencyKey)
    return "ik:" + msg.idempotencyKey;
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
  const role =
    typeof msg.role === "string" ? msg.role : typeof inner.role === "string" ? inner.role : "";
  if (role !== "toolResult") return null;
  const raw = (msg.toolCallId ?? msg.tool_call_id ?? inner.toolCallId) as unknown;
  const cid = canonicalId(raw);
  return cid ? "tc:" + cid : null;
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

type TranscriptRow = {
  seq: number;
  event_json: string | null;
  event_zstd?: unknown;
  event_utf8_bytes?: unknown;
};
type RowRange = { after?: number; lo?: number; hi?: number; descending?: boolean; limit?: number };

/** THE host-transcript reader (assemble and the classification pass): read-only, scoped to one
 *  session, identity or Zstd rows. Nothing else in the plugin queries host tables. */
export class HostTranscript {
  private compressed: boolean | null = null;
  constructor(
    private readonly db: DatabaseSync,
    readonly sessionId: string
  ) {}
  static open(dbPath: string, sessionId: string): HostTranscript {
    return new HostTranscript(new DatabaseSync(dbPath, { readOnly: true }), sessionId);
  }
  /** Undecoded rows, so a caller can stop before the first row it cannot decode. */
  rows(range: RowRange = {}): TranscriptRow[] {
    if (this.compressed === null) {
      const cols = this.db.prepare("PRAGMA table_info(transcript_events)").all() as Array<{
        name?: unknown;
      }>;
      const names = new Set(cols.map((col) => String(col.name)));
      this.compressed = names.has("event_zstd") && names.has("event_utf8_bytes");
    }
    const where = ["session_id = ?"];
    const args: Array<string | number> = [this.sessionId];
    for (const [op, v] of [
      ["seq > ?", range.after],
      ["seq >= ?", range.lo],
      ["seq <= ?", range.hi],
    ] as const) {
      if (v !== undefined) (where.push(op), args.push(v));
    }
    const cols = this.compressed
      ? "seq, event_json, event_zstd, event_utf8_bytes"
      : "seq, event_json";
    const limit = range.limit !== undefined ? (args.push(range.limit), " LIMIT ?") : "";
    return this.db
      .prepare(
        `SELECT ${cols} FROM transcript_events WHERE ${where.join(" AND ")} ORDER BY seq ${range.descending ? "DESC" : "ASC"}${limit}`
      )
      .all(...args) as TranscriptRow[];
  }
  /** Decoded events; throws on a row it cannot decode. */
  events(range: RowRange = {}): Array<{ seq: number; eventJson: string }> {
    return this.rows(range).map((r) => ({
      seq: Number(r.seq),
      eventJson: decodeTranscriptEventJson(r),
    }));
  }
  seqBounds(): { min: number | null; max: number | null } {
    const r = this.db
      .prepare("SELECT MIN(seq) AS lo, MAX(seq) AS hi FROM transcript_events WHERE session_id = ?")
      .get(this.sessionId) as { lo?: number | null; hi?: number | null } | undefined;
    return { min: r?.lo ?? null, max: r?.hi ?? null };
  }
  createdAt(seq: number): number | null {
    const r = this.db
      .prepare("SELECT created_at FROM transcript_events WHERE session_id = ? AND seq = ?")
      .get(this.sessionId, seq) as { created_at?: unknown } | undefined;
    return typeof r?.created_at === "number" ? r.created_at : null;
  }
  /** Host event ids of the first and last identified rows in [lo, hi]; null when none. */
  anchorsFor(lo: number, hi: number): { first: string; last: string } | null {
    let first: string | null = null;
    let last: string | null = null;
    for (const r of this.rows({ lo, hi })) {
      let id: string | null = null;
      try {
        id = eventIdOf(decodeTranscriptEventJson(r));
      } catch {
        return null; // an undecodable row inside the range: do not guess
      }
      if (id) (first ??= id), (last = id);
    }
    return first && last ? { first, last } : null;
  }
  /** The host's current rewrite generation for this session (A2); null when it cannot be read. */
  generation(): string | null {
    try {
      const row = this.db
        .prepare("SELECT generation FROM transcript_rewrite_watermarks WHERE session_id = ?")
        .get(this.sessionId) as { generation?: unknown } | undefined;
      return typeof row?.generation === "string" && row.generation ? row.generation : null;
    } catch {
      return null;
    }
  }
  close(): void {
    this.db.close();
  }
}

export class SeqIndex {
  private db: HostTranscript | null = null;
  private lastSeq = 0;
  /** Event id at lastSeq; when that row changes, the transcript was reindexed. */
  private lastSeqId: string | null = null;
  private readonly map = new Map<string, number>();
  private readonly byEventId = new Map<string, number>();
  private loadedOnce = false;
  failed = false;
  /** Set when the last refresh stopped at a row it could not read; lastSeq stays before it. */
  readError: string | null = null;

  constructor(
    readonly dbPath: string,
    readonly sessionId: string
  ) {}

  private open(): boolean {
    if (this.db) return true;
    if (this.failed || !existsSync(this.dbPath)) {
      this.failed = true;
      return false;
    }
    try {
      this.db = HostTranscript.open(this.dbPath, this.sessionId);
      return true;
    } catch {
      this.failed = true;
      return false;
    }
  }

  /** One refresh per pass (M5); loop until a chunk comes back short so a fresh index converges.
   *  A row that cannot be decoded stops the refresh BEFORE it (lastSeq is not advanced past it)
   *  and sets readError, so the caller reports the store unreadable instead of unmapped. */
  refresh(): void {
    if (!this.open()) return;
    this.readError = null;
    try {
      // A trim or header repair keeps event ids but renumbers rows, and the incremental read
      // below only walks seq > lastSeq, so survivors would keep stale coordinates. Reload
      // from scratch when the row at lastSeq no longer carries the id recorded for it.
      if (this.loadedOnce && !this.tailUnchanged()) {
        this.map.clear();
        this.byEventId.clear();
        this.lastSeq = -1;
        this.lastSeqId = null;
      }
      let rows: TranscriptRow[] = [];
      do {
        rows = this.db!.rows({ after: this.lastSeq, limit: LOAD_CHUNK });
        for (const r of rows) {
          let eventJson: string;
          try {
            eventJson = decodeTranscriptEventJson(r);
          } catch (e) {
            this.readError = `seq ${Number(r.seq)}: ${(e as Error)?.message ?? e}`;
            return;
          }
          const key = eventIdentity(eventJson);
          const eid = eventIdOf(eventJson);
          if (eid) this.byEventId.set(eid, Number(r.seq));
          if (Number(r.seq) > this.lastSeq) (this.lastSeq = Number(r.seq)), (this.lastSeqId = eid);
          if (!key) continue;
          this.map.set(key, Number(r.seq));
          try {
            const ev = JSON.parse(eventJson) as Record<string, unknown>;
            const tcKey = toolCallKeyOf((ev.message ?? ev) as Record<string, unknown>);
            if (tcKey) {
              const prev = this.map.get(tcKey);
              if (prev === undefined) this.map.set(tcKey, Number(r.seq));
              else if (prev !== Number(r.seq)) this.map.set(tcKey, -1); // ambiguous: refuse
            }
          } catch {
            /* bonus key only */
          }
        }
      } while (rows.length === LOAD_CHUNK);
      this.loadedOnce = true;
    } catch (e) {
      this.readError = `transcript read failed: ${(e as Error)?.message ?? e}`;
    }
  }

  get ready(): boolean {
    return this.loadedOnce;
  }

  private tailUnchanged(): boolean {
    if (this.lastSeqId === null) return true;
    try {
      const row = this.db!.rows({ lo: this.lastSeq, hi: this.lastSeq })[0];
      return !!row && eventIdOf(decodeTranscriptEventJson(row)) === this.lastSeqId;
    } catch {
      return false;
    }
  }

  seqOfEventId(id: string): number | null {
    return this.byEventId.get(id) ?? null;
  }

  /** Release the read handle (Windows refuses to delete a directory with an open DB). */
  close(): void {
    try {
      this.db?.close();
    } catch {
      /* already closed */
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
      for (const r of this.db!.rows({ after: afterSeq, limit })) {
        let json: string;
        try {
          json = decodeTranscriptEventJson(r);
        } catch {
          // The host contract always records event_utf8_bytes, but an older compressed row
          // may not. The anchor path tolerates that (no length check) or skips the row.
          const bytes = (r as { event_zstd?: unknown }).event_zstd;
          const zstd = (zlib as unknown as { zstdDecompressSync?: (b: Uint8Array) => Buffer })
            .zstdDecompressSync;
          if (!(bytes instanceof Uint8Array) || typeof zstd !== "function") continue;
          try {
            json = zstd.call(zlib, bytes).toString("utf8");
          } catch {
            continue; // an undecodable row cannot carry usage; the next one may
          }
        }
        const usage = usageFromEvent({ seq: Number(r.seq), eventJson: json });
        if (usage) return usage;
      }
    } catch {
      /* degrade: no anchor this call */
    }
    return null;
  }
}

const seqIndexes = new Map<string, SeqIndex>();
function seqIndexFor(dbPath: string, sessionId: string): SeqIndex {
  const cacheKey = `${dbPath}|${sessionId}`; // M4: key by (dbPath, sessionId), not session key
  let ix = seqIndexes.get(cacheKey);
  if (!ix) {
    ix = new SeqIndex(dbPath, sessionId);
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

/** Provider usage of the first model call after `afterSeq` in this host session (count anchor). */
export function providerUsageAfter(dbPath: string, sessionId: string, afterSeq: number): ProviderUsage | null {
  return seqIndexFor(dbPath, sessionId).firstUsageAfter(afterSeq);
}

export type ApplyContext = {
  sessionKey: string;
  hostDbPath: string;
  sessionId: string; // B1: the HOST session UUID, never the session key
  tokenBudget: number;
  turnCounter: number; // monotonic (lastCommittedSeq), NOT the capped committedKeys length
  policy?: Partial<MarksAdmissionPolicy>; // `astinus.marks.*`; DEFAULT_MARKS_POLICY otherwise
  turnMiddleAgeTurns?: number; // F2: the turn_middle tail floor (default 6)
  /** Provider/host-estimate ratio for this session (count anchor); pressure uses tokensBefore x scale. */
  pressureScale?: number;
};

export type ApplyResult = {
  messages: unknown[];
  admitted: LedgerMark[]; // newly admitted this pass (stamp these)
  marksInEffect: number; // admitted-before + admitted-now (B2: authority basis)
  changed: boolean;
  pendingCount: number; // pending marks considered this call (the share denominator)
  tokensBefore: number;
  tokensAfter: number;
  skipped: string[];
  /** Host transcript could not be read/decoded; messages are unchanged and nothing admitted. */
  readError?: string;
  windowFirstSeq: number | null; // min mapped seq of this view (2026-10-07): the pass bounds proposals to the assembled window
  windowLastSeq: number | null; // max mapped seq of this view: the next model call's usage follows it (count anchor)
};

const lastAdmissionTurn = new Map<string, number>();

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

/** The index of the first message inside the preserved tail: the last PRESERVE_USER_TURNS user
 *  turns (M3), not a fixed message count. */
function preservedFloorIdx(messages: unknown[], keep = PRESERVE_USER_TURNS): number {
  let userTurns = 0;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (((messages[i] ?? {}) as Record<string, unknown>).role === "user") {
      userTurns++;
      if (userTurns >= keep) return i;
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
  };
  // 2026-10-07 (Vern: "so fix it"): the assembled window's first mapped seq. The pass bounds
  // its proposals to the window the host actually assembles - marks outside it can never
  // map ("no eligible message in view" forever) and the pools filled with unapplicable
  // marks while the in-window content sat unmarked. Computed before the early returns so
  // every session reports it, marks or not. Same cached SeqIndex the loop below uses.
  try {
    const wix = seqIndexFor(ctx.hostDbPath, ctx.sessionId);
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
    const dix = seqIndexFor(ctx.hostDbPath, ctx.sessionId);
    dix.refresh();
    let dn = 0,
      dres = 0,
      dtsNum = 0,
      dtsStr = 0,
      dtsUndef = 0,
      dstamped = 0;
    for (const m of messages) {
      const msg = (m ?? {}) as Record<string, unknown>;
      const inner = (msg.message ?? {}) as Record<string, unknown>;
      const role =
        typeof msg.role === "string" ? msg.role : typeof inner.role === "string" ? inner.role : "";
      if (role !== "toolResult") continue;
      dn++;
      if (dix.seqOf(m) !== null) dres++;
      const ts = msg.timestamp;
      if (typeof ts === "number") dtsNum++;
      else if (typeof ts === "string") dtsStr++;
      else dtsUndef++;
      const c = msg.content;
      const text = typeof c === "string" ? c : JSON.stringify(c ?? "");
      if (
        text.indexOf("Astinus digest of seq") !== -1 ||
        text.indexOf("tool output pruned by Astinus") !== -1
      )
        dstamped++;
    }
    (out as unknown as Record<string, unknown>).trDiag = {
      n: dn,
      resolved: dres,
      tsNum: dtsNum,
      tsStr: dtsStr,
      tsUndef: dtsUndef,
      stamped: dstamped,
    };
  } catch {
    /* degrade: no diag; never affects the view */
  }
  const wellFormed = marks.filter(
    (m) => m && Number.isFinite(m.seq_start) && Number.isFinite(m.seq_end)
  );
  if (wellFormed.length === 0) return out;
  // Count anchor (2026-11-03): the char estimate is scaled by the session's measured
  // provider/estimate ratio, so pressure reflects what the model actually receives.
  const scale = typeof ctx.pressureScale === "number" && ctx.pressureScale > 0 ? ctx.pressureScale : 1;
  const pressure = ctx.tokenBudget > 0 ? (tokensBefore * scale) / ctx.tokenBudget : 0;
  // B2: admission (NEW marks) is gated; re-application (admitted marks) is unconditional.
  // The original justification ("the view is rebuilt from the transcript every turn, so
  // admitted marks ARE the view") was FALSIFIED 2026-10-11 by the view-ratchet finding: the
  // host writes the assembled list back at attempt-history-prepare.ts:230-232, so the view is
  // CARRIED FORWARD, not rebuilt. Re-application still runs unconditionally; whether that is
  // redundant when the effect persists is unmeasured - the section-8 census will say.
  // A digest mark with no digest text cannot replace anything: it maps to a range, counts
  // as in effect, and changes nothing. turn_middle marks are the exception - they redact
  // in place and intentionally carry no digest.
  const actionable = wellFormed.filter(
    (m) =>
      m.action !== "digest" ||
      (typeof m.digest === "string" && m.digest.length > 0) ||
      (typeof m.reason === "string" && m.reason.indexOf("turn_middle") === 0)
  );
  const admitted = actionable.filter((m) => m.applied_at != null);
  let pending = actionable.filter((m) => m.applied_at == null);
  const policy = { ...DEFAULT_MARKS_POLICY, ...(ctx.policy ?? {}) };
  const pendingBefore = pending.length;
  out.pendingCount = pendingBefore;
  if (pending.length > 0) {
    if (pressure < policy.minPressure) {
      // Below the floor nothing is admitted and the debounce clock restarts from zero for the
      // next time it crosses 0.30. Already-applied marks are NOT reverted (B2).
      lastAdmissionTurn.delete(ctx.sessionKey);
      out.skipped.push(
        `admission held: pressure ${pressure.toFixed(2)} < ${policy.minPressure} (idle; debounce reset)`
      );
      pending = [];
    } else {
      const lastAt = lastAdmissionTurn.get(ctx.sessionKey);
      const since = lastAt === undefined ? Number.POSITIVE_INFINITY : ctx.turnCounter - lastAt;
      if (since < policy.debounceTurns && pressure < policy.criticalPressure) {
        out.skipped.push(
          `admission held: debounce ${since}/${policy.debounceTurns} turns (cache-hit ruling)`
        );
        pending = [];
      } else if (policy.admitShare) {
        // Admit the SHARE of the pending pool, in priority order: pressure desc, then topic
        // dormancy desc, then statement age desc. The rest stay pending for the next admission.
        const shareCount = Math.min(pending.length, Math.ceil(pressure * pending.length));
        pending = [...pending]
          .sort(
            (a, b) =>
              Number(b.pressure ?? 0) - Number(a.pressure ?? 0) ||
              Number(b.topic_dormancy_s ?? 0) - Number(a.topic_dormancy_s ?? 0) ||
              Number(b.stmt_age_s ?? 0) - Number(a.stmt_age_s ?? 0)
          )
          .slice(0, shareCount);
        out.skipped.push(
          `admission share: admitted=${pending.length}/${pendingBefore} share=${pressure.toFixed(2)}`
        );
      }
      // B1 (review 2026-09-21): do NOT stamp the debounce here. A held turn must not move the
      // clock, or `since` stays 1 forever and the debounce never expires below critical. The
      // stamp belongs after an actual admission - see `out.admitted.length > 0` below.
    }
  }
  const toApply = [...admitted, ...pending].filter((m) => m.retired_at == null);
  if (toApply.length === 0) return out;

  const ix = seqIndexFor(ctx.hostDbPath, ctx.sessionId);
  ix.refresh();
  if (ix.readError) {
    out.readError = ix.readError;
    out.skipped.push(`transcript unreadable: ${ix.readError}`);
    return out;
  }
  const seqs: (number | null)[] = messages.map((m) => ix.seqOf(m));
  const firstUser = messages.findIndex(
    (m) => ((m ?? {}) as Record<string, unknown>).role === "user"
  );
  const floorIdx = firstUser < 0 ? 0 : firstUser;
  const tailFloor = preservedFloorIdx(messages);
  const turnMiddleFloor = preservedFloorIdx(
    messages,
    ctx.turnMiddleAgeTurns ?? DEFAULT_TURN_MIDDLE_AGE_TURNS
  );
  let list = messages.slice();

  const ordered = [...toApply].sort((a, b) => b.seq_start - a.seq_start);
  for (const mark of ordered) {
    let lo = Number(mark.seq_start);
    let hi = Number(mark.seq_end);
    if (mark.anchor_first_event && mark.anchor_last_event) {
      const a = ix.seqOfEventId(mark.anchor_first_event);
      const b = ix.seqOfEventId(mark.anchor_last_event);
      if (a === null || b === null) {
        out.skipped.push(`seq ${lo}-${hi}: anchor gone from the transcript (not re-aimed)`);
        continue;
      }
      lo = Math.min(a, b);
      hi = Math.max(a, b);
    }
    const isTurnMiddle =
      mark.action === "turn_middle" ||
      (typeof mark.reason === "string" && mark.reason.indexOf("turn_middle") === 0);
    const floor = isTurnMiddle ? turnMiddleFloor : tailFloor;
    // A mark may partially overlap the preserved tail; the in-view part applies, the tail
    // part is protected by construction (it has no eligible index below tailFloor).
    const idxs: number[] = [];
    for (let i = floorIdx + 1; i < list.length; i++) {
      if (i >= floor) continue;
      const s = seqs[i];
      if (s !== null && s >= lo && s <= hi) idxs.push(i);
    }
    if (idxs.length === 0) {
      out.skipped.push(`seq ${lo}-${hi}: no eligible message in view (preserved/unmapped)`);
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
        } else if (isToolResult(prev) && start - 1 < floor) {
          start--; // absorb an immediately preceding result so it is not orphaned
          // ...and its call: without its id the next step stops at that call and orphans it.
          for (const cid of toolResultCallIds(list[start])) needIds.add(cid);
        } else break;
      }
      // P2: forward extension NEVER crosses tailFloor — pruning inside the preserved tail is
      // worse than leaving a mark unapplied.
      while (end + 1 < list.length && end + 1 < floor) {
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
      for (let i = floor; i < list.length && !neededInTail; i++) {
        if (toolResultCallIds(list[i]).some((cid) => needIds.has(cid))) neededInTail = true;
        else if (toolCallIdsOf(list[i]).some((cid) => needIds.has(cid))) neededInTail = true;
      }
      if (neededInTail) {
        out.skipped.push(
          `seq ${lo}-${hi}: completing the tool pair would enter the preserved tail; left both messages`
        );
        continue;
      }
    }
    if (mark.action !== "placeholder" && !spanIsPairClosed(list, start, end)) {
      out.skipped.push(`seq ${lo}-${hi}: the range would split a tool pair; left as is`);
      continue;
    }
    if (mark.action === "drop") {
      list.splice(start, end - start + 1);
      out.changed = true;
    } else if (isTurnMiddle) {
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
      for (let i = start; i <= end; i++) if (!keep.has(i)) interiorCount++;
      if (interiorCount === 0) {
        out.skipped.push(`seq ${lo}-${hi}: turn_middle found no interior to redact`);
        continue;
      }
      const replacements: unknown[] = [];
      let segStart = -1;
      let segCalls = 0;
      let segReason = 0;
      let segResults = 0;
      const flushSeg = (segEnd: number) => {
        if (segStart < 0) return;
        const n = segEnd - segStart + 1;
        replacements.push({
          role: "assistant",
          timestamp: Date.now(),
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
        segStart = -1;
        segCalls = 0;
        segReason = 0;
        segResults = 0;
      };
      for (let i = start; i <= end; i++) {
        if (keep.has(i)) {
          flushSeg(i - 1);
          replacements.push(withoutUnpairedCalls(list[i], keptResults));
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
      list.splice(start, end - start + 1, ...replacements);
      out.changed = true;
    } else if (mark.action === "digest" && mark.digest) {
      const digestMsg = {
        role: "assistant",
        timestamp: Date.now(),
        content: [
          {
            type: "text",
            text:
              `[injected context — earlier records of this session, digested; not current state; ` +
              `verify before acting] Astinus digest of seq ${lo}-${hi}: ${mark.digest}`,
          },
        ],
      };
      list.splice(start, end - start + 1, digestMsg);
      out.changed = true;
    } else if (mark.action === "placeholder") {
      let placeholdered = false;
      for (let i = start; i <= end; i++) {
        const msg = list[i] as Record<string, unknown> | null;
        if (!msg || !isToolResult(msg)) continue;
        const c = msg.content;
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
      if (placeholdered) out.changed = true;
      else {
        out.skipped.push(`seq ${lo}-${hi}: placeholder found no tool result`);
        continue;
      }
    } else {
      out.skipped.push(`seq ${lo}-${hi}: unknown action ${mark.action}`);
      continue;
    }
    if (mark.applied_at == null) out.admitted.push(mark);
  }
  out.marksInEffect = admitted.length + out.admitted.length;
  if (out.admitted.length > 0) lastAdmissionTurn.set(ctx.sessionKey, ctx.turnCounter);
  if (out.changed) {
    out.tokensAfter = estimateTokensHost(list);
    out.messages = list;
  }
  return out;
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

/** A kept turn_middle boundary whose tool-call results are redacted: each such call becomes a
 *  text line, so the view never holds a call without its result. Returns the message itself
 *  when nothing changes (M1: the host's object is never mutated). */
function withoutUnpairedCalls(msg: unknown, keptResults: Set<string>): unknown {
  const m = (msg ?? {}) as Record<string, unknown>;
  if (m.role !== "assistant" || !Array.isArray(m.content)) return msg;
  let changed = false;
  const content = (m.content as Array<Record<string, unknown>>).map((b) => {
    if (!b || typeof b !== "object" || (b.type !== "toolCall" && b.type !== "tool_use")) return b;
    const id = b.id ?? b.toolCallId;
    if (typeof id === "string" && keptResults.has(id)) return b;
    changed = true;
    const name = typeof b.name === "string" ? b.name : "";
    return { type: "text", text: `[Astinus: tool call ${name} trimmed with its result]` };
  });
  return changed ? { ...m, content } : msg;
}
