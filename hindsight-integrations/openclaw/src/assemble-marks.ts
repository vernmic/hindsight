// Astinus mark application (plan v3 §8 + the 2026-09-12 debounce ruling): turn the marks the
// pass proposed into the view the model actually sees. The join is by IDENTITY, never position —
// assemble's array has been through history limiting, tool-pair repair and custom-message
// stripping, so positions drift exactly when precision matters (L0 review Q2/M1).
//
// Hard invariants, enforced here in code regardless of what the model proposed:
//   * never touch the first user message or anything before it (bootstrap);
//   * tool_use/tool_result travel together — a range that would split a pair is skipped whole;
//   * nothing inside the last `preserveMessages` messages;
//   * apply only when view pressure exceeds `minPressure`;
//   * a session's view changes at most once per `debounceTurns` turns unless pressure is
//     critical (the Sept 12 cache-hit ruling — pruning that reshapes the view every turn
//     would tank cache hits; that is a hard constraint, not a preference);
//   * any failure degrades to "less pruned", never to an error (§6 failure policy).
import { DatabaseSync } from "node:sqlite";
import { existsSync } from "fs";
import { join } from "path";

export type LedgerMark = {
  mark_id?: number | unknown;
  session_id?: string;
  seq_start: number;
  seq_end: number;
  kind?: string;
  action: string; // digest | drop | placeholder
  digest?: string | null;
  reason?: string;
  applied_gen?: string | null;
  applied_at?: number | null;
};

const PRESERVE_MESSAGES = 6;
const MIN_PRESSURE = 0.4;
const CRITICAL_PRESSURE = 0.65;
const DEBOUNCE_TURNS = 10;
const MAX_SEQ_ROWS_PER_LOAD = 5000;

/** The identity of a message as it survives the host's transformations. */
export function identityKeyOf(m: unknown): string {
  const msg = (m ?? {}) as Record<string, unknown>;
  if (typeof msg.idempotencyKey === "string" && msg.idempotencyKey) return "ik:" + msg.idempotencyKey;
  const inner = (msg.message ?? {}) as Record<string, unknown>;
  const ik = typeof inner.idempotencyKey === "string" ? inner.idempotencyKey : "";
  if (ik) return "ik:" + ik;
  const role = typeof msg.role === "string" ? msg.role : "";
  const ts = typeof msg.timestamp === "number" ? msg.timestamp : 0;
  const tcRaw = (msg.toolCallId ?? msg.tool_call_id ?? inner.toolCallId) as unknown;
  const tc = typeof tcRaw === "string" ? tcRaw : "";
  return `rt:${role}:${ts}:${tc}`;
}

function eventIdentity(eventJson: string): { key: string; role: string } | null {
  try {
    const ev = JSON.parse(eventJson) as Record<string, unknown>;
    const msg = (ev.message ?? ev) as Record<string, unknown>;
    const role = typeof msg.role === "string" ? msg.role : "";
    if (!role) return null;
    return { key: identityKeyOf(msg), role };
  } catch {
    return null;
  }
}

/** Per-session identity→seq index over the host transcript, loaded read-only and incrementally. */
class SeqIndex {
  private db: DatabaseSync | null = null;
  private lastSeq = 0;
  private readonly map = new Map<string, number>();
  failed = false;

  constructor(readonly dbPath: string, readonly sessionId: string) {}

  private open(): boolean {
    if (this.db) return true;
    if (this.failed || !existsSync(this.dbPath)) return false;
    try {
      this.db = new DatabaseSync(this.dbPath, { readOnly: true });
      return true;
    } catch {
      this.failed = true;
      return false;
    }
  }

  seqOf(message: unknown): number | null {
    this.refresh();
    const hit = this.map.get(identityKeyOf(message));
    return hit ?? null;
  }

  private refresh(): void {
    if (!this.open()) return;
    try {
      const rows = this.db!
        .prepare(
          "SELECT seq, event_json FROM transcript_events WHERE session_id = ? AND seq > ? ORDER BY seq LIMIT ?"
        )
        .all(this.sessionId, this.lastSeq, MAX_SEQ_ROWS_PER_LOAD) as Array<{
        seq: number;
        event_json: string;
      }>;
      for (const r of rows) {
        const id = eventIdentity(r.event_json);
        if (!id) continue;
        this.map.set(id.key, Number(r.seq));
        if (Number(r.seq) > this.lastSeq) this.lastSeq = Number(r.seq);
      }
    } catch {
      // degrade: whatever is indexed so far still resolves; new messages return null
    }
  }
}

const seqIndexes = new Map<string, SeqIndex>();
function seqIndexFor(sessionKey: string, dbPath: string, sessionId: string): SeqIndex {
  let ix = seqIndexes.get(sessionKey);
  if (!ix || ix.dbPath !== dbPath) {
    ix = new SeqIndex(dbPath, sessionId);
    if (seqIndexes.size >= 200) {
      const oldest = seqIndexes.keys().next().value;
      if (oldest !== undefined) seqIndexes.delete(oldest);
    }
    seqIndexes.set(sessionKey, ix);
  }
  return ix;
}

export type ApplyContext = {
  sessionKey: string;
  hostDbPath: string;
  sessionId: string;
  viewTokens: number;
  tokenBudget: number;
  turnCount: number;
};

export type ApplyResult = {
  messages: unknown[];
  applied: LedgerMark[]; // the marks actually applied this pass
  changed: boolean;
  tokensBefore: number;
  tokensAfter: number;
  skipped: string[]; // human-readable skip reasons (one per skipped mark)
};

const lastAppliedTurn = new Map<string, number>();

/** Very rough token estimate — matches astinusEstimateTokens' char/4 shape. */
function estimate(list: unknown[]): number {
  let chars = 0;
  for (const m of list) {
    const msg = (m ?? {}) as Record<string, unknown>;
    const c = msg.content;
    if (typeof c === "string") chars += c.length;
    else if (Array.isArray(c))
      for (const p of c as Array<Record<string, unknown>>)
        if (typeof p?.text === "string") chars += p.text.length;
  }
  return Math.ceil(chars / 4);
}

function isToolSide(m: unknown): "call" | "result" | null {
  const msg = (m ?? {}) as Record<string, unknown>;
  const t = typeof msg.type === "string" ? msg.type : "";
  if (t === "toolCall" || t === "tool_use") return "call";
  if (t === "toolResult" || t === "tool_result") return "result";
  return null;
}

export function applyMarks(
  messages: unknown[],
  marks: LedgerMark[],
  ctx: ApplyContext
): ApplyResult {
  const tokensBefore = estimate(messages);
  const pressure = ctx.tokenBudget > 0 ? ctx.viewTokens / ctx.tokenBudget : 0;
  const out: ApplyResult = {
    messages,
    applied: [],
    changed: false,
    tokensBefore,
    tokensAfter: tokensBefore,
    skipped: [],
  };
  const usable = marks.filter(
    (m) => m && (m.applied_at == null) && Number.isFinite(m.seq_start) && Number.isFinite(m.seq_end)
  );
  if (usable.length === 0) return out;
  if (pressure < MIN_PRESSURE) {
    out.skipped.push(`pressure ${pressure.toFixed(2)} < ${MIN_PRESSURE} (idle)`);
    return out;
  }
  const lastAt = lastAppliedTurn.get(ctx.sessionKey) ?? -Infinity;
  if (ctx.turnCount - lastAt < DEBOUNCE_TURNS && pressure < CRITICAL_PRESSURE) {
    out.skipped.push(`debounce: ${ctx.turnCount - lastAt}/${DEBOUNCE_TURNS} turns since last apply (cache-hit ruling)`);
    return out;
  }
  const ix = seqIndexFor(ctx.sessionKey, ctx.hostDbPath, ctx.sessionId);
  // Resolve each message to a seq once. Messages with no seq are never marked (the invariant).
  const seqs: (number | null)[] = messages.map((m) => ix.seqOf(m));
  const firstUser = messages.findIndex(
    (m) => ((m ?? {}) as Record<string, unknown>).role === "user"
  );
  const floorIdx = firstUser < 0 ? 0 : firstUser; // never touch anything at or before the first user message
  const ceilingIdx = Math.max(floorIdx + 1, messages.length - PRESERVE_MESSAGES);
  let list = messages.slice();
  // Apply deepest (highest seq) first so indices stay valid as ranges are removed.
  const ordered = [...usable].sort((a, b) => b.seq_start - a.seq_start);
  for (const mark of ordered) {
    const lo = Number(mark.seq_start);
    const hi = Number(mark.seq_end);
    const idxs: number[] = [];
    for (let i = floorIdx + 1; i < ceilingIdx; i++) {
      const s = seqs[i];
      if (s !== null && s >= lo && s <= hi) idxs.push(i);
    }
    if (idxs.length === 0) {
      out.skipped.push(`seq ${lo}-${hi}: no eligible message in view (preserved/unmapped)`);
      continue;
    }
    const span = [idxs[0], idxs[idxs.length - 1]] as const;
    // Tool pairs travel together: if the boundary splits a pair, extend to include both sides.
    let start = span[0];
    let end = span[1];
    while (start - 1 > floorIdx && isToolSide(list[start - 1]) && isToolSide(list[start])) start--;
    while (end + 1 < list.length && isToolSide(list[end]) && isToolSide(list[end + 1])) end++;
    if (mark.action === "drop") {
      list.splice(start, end - start + 1);
      out.changed = true;
      out.applied.push(mark);
    } else if (mark.action === "digest" && mark.digest) {
      const digestMsg = {
        role: "assistant",
        content:
          `[injected context — earlier records of this session, digested; not current state; ` +
          `verify before acting] Astinus digest of seq ${lo}-${hi}: ${mark.digest}`,
      };
      list.splice(start, end - start + 1, digestMsg);
      out.changed = true;
      out.applied.push(mark);
    } else if (mark.action === "placeholder") {
      let placeholdered = false;
      for (let i = start; i <= end; i++) {
        const msg = list[i] as Record<string, unknown> | null;
        if (!msg || isToolSide(msg) !== "result") continue;
        const c = msg.content;
        const text = typeof c === "string" ? c : JSON.stringify(c ?? "");
        (msg as Record<string, unknown>).content =
          `[tool output pruned by Astinus (${text.length} chars); the call above retains its shape]`;
        placeholdered = true;
      }
      if (placeholdered) {
        out.changed = true;
        out.applied.push(mark);
      } else {
        out.skipped.push(`seq ${lo}-${hi}: placeholder found no tool result`);
      }
    } else {
      out.skipped.push(`seq ${lo}-${hi}: unknown action ${mark.action}`);
    }
  }
  if (out.changed) {
    out.tokensAfter = estimate(list);
    out.messages = list;
    lastAppliedTurn.set(ctx.sessionKey, ctx.turnCount);
  }
  return out;
}
