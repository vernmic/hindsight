// Astinus mark application (plan v3 §8 + v4.2 + the assemble-marks review B1-B4/M1-M5).
// Identity join (never position); admission vs re-application split (B2); tool pairs by
// role+toolCallId (B3); host-shaped estimator (B4); element replacement not mutation (M1);
// parts-array digest (M2); 8-user-turn preserved tail (M3); (dbPath,sessionId) index key (M4);
// one refresh per pass (M5). Any failure degrades to "less pruned", never an error.
import { DatabaseSync } from "node:sqlite";
import { existsSync } from "fs";

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
};

const PRESERVE_USER_TURNS = 8;
const LOAD_CHUNK = 5000;

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

class SeqIndex {
  private db: DatabaseSync | null = null;
  private lastSeq = 0;
  private readonly map = new Map<string, number>();
  private loadedOnce = false;
  failed = false;

  constructor(readonly dbPath: string, readonly sessionId: string) {}

  private open(): boolean {
    if (this.db) return true;
    if (this.failed || !existsSync(this.dbPath)) {
      this.failed = true;
      return false;
    }
    try {
      this.db = new DatabaseSync(this.dbPath, { readOnly: true });
      return true;
    } catch {
      this.failed = true;
      return false;
    }
  }

  /** One refresh per pass (M5); loop until a chunk comes back short so a fresh index converges. */
  refresh(): void {
    if (!this.open()) return;
    try {
      let rows: Array<{ seq: number; event_json: string }> = [];
      do {
        rows = this.db!
          .prepare(
            "SELECT seq, event_json FROM transcript_events WHERE session_id = ? AND seq > ? ORDER BY seq LIMIT ?"
          )
          .all(this.sessionId, this.lastSeq, LOAD_CHUNK) as Array<{ seq: number; event_json: string }>;
        for (const r of rows) {
          const key = eventIdentity(r.event_json);
          if (!key) continue;
          this.map.set(key, Number(r.seq));
          if (Number(r.seq) > this.lastSeq) this.lastSeq = Number(r.seq);
        }
      } while (rows.length === LOAD_CHUNK);
      this.loadedOnce = true;
    } catch {
      /* degrade: whatever is indexed still resolves */
    }
  }

  get ready(): boolean {
    return this.loadedOnce;
  }

  seqOf(message: unknown): number | null {
    return this.map.get(identityKeyOf(message)) ?? null;
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
      if (oldest !== undefined) seqIndexes.delete(oldest);
    }
    seqIndexes.set(cacheKey, ix);
  }
  return ix;
}

export type ApplyContext = {
  sessionKey: string;
  hostDbPath: string;
  sessionId: string; // B1: the HOST session UUID, never the session key
  tokenBudget: number;
  turnCounter: number; // monotonic (lastCommittedSeq), NOT the capped committedKeys length
  policy?: Partial<MarksAdmissionPolicy>; // `astinus.marks.*`; DEFAULT_MARKS_POLICY otherwise
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
  };
  const wellFormed = marks.filter(
    (m) => m && Number.isFinite(m.seq_start) && Number.isFinite(m.seq_end)
  );
  if (wellFormed.length === 0) return out;
  const pressure = ctx.tokenBudget > 0 ? tokensBefore / ctx.tokenBudget : 0;
  // B2: admission (NEW marks) is gated; re-application (admitted marks) is unconditional —
  // the view is rebuilt from the transcript every turn, so admitted marks ARE the view.
  const admitted = wellFormed.filter((m) => m.applied_at != null);
  let pending = wellFormed.filter((m) => m.applied_at == null);
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
  const toApply = [...admitted, ...pending];
  if (toApply.length === 0) return out;

  const ix = seqIndexFor(ctx.hostDbPath, ctx.sessionId);
  ix.refresh();
  const seqs: (number | null)[] = messages.map((m) => ix.seqOf(m));
  const firstUser = messages.findIndex(
    (m) => ((m ?? {}) as Record<string, unknown>).role === "user"
  );
  const floorIdx = firstUser < 0 ? 0 : firstUser;
  const tailFloor = preservedFloorIdx(messages);
  let list = messages.slice();

  const ordered = [...toApply].sort((a, b) => b.seq_start - a.seq_start);
  for (const mark of ordered) {
    const lo = Number(mark.seq_start);
    const hi = Number(mark.seq_end);
    // A mark may partially overlap the preserved tail; the in-view part applies, the tail
    // part is protected by construction (it has no eligible index below tailFloor).
    const idxs: number[] = [];
    for (let i = floorIdx + 1; i < list.length; i++) {
      if (i >= tailFloor) continue;
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
        } else if (isToolResult(prev) && start - 1 < tailFloor) {
          start--; // absorb an immediately preceding result so it is not orphaned
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
        out.skipped.push(
          `seq ${lo}-${hi}: completing the tool pair would enter the preserved tail; left both messages`
        );
        continue;
      }
    }
    if (mark.action === "drop") {
      list.splice(start, end - start + 1);
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
