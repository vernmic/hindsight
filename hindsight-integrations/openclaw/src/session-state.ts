import { existsSync, mkdirSync, readFileSync, renameSync, statSync, writeFileSync } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { join } from "node:path";
import { sameCommittedWork, type QueuedRetain } from "./retain-queue.js";

/**
 * The durable per-session state document, ported from the maintained plugin (9c76cbda9
 * AstinusSessionState, loadSessionState / updateSessionState / appendPendingRange /
 * appendPendingLegacy / applyCompletedRetain). Topic, tag-list and segment planning fields the
 * generic plugin does not compute are not modelled, but a write keeps every field it does not
 * model, so a document shared with the maintained plugin loses nothing.
 */
export type PendingRange = {
  start: number;
  end: number;
  messages: unknown[];
  at?: number;
  sessionId?: string;
  key?: string;
};
export type PendingLegacy = { id: string; sessionId?: string; messages: unknown[]; at: number };
export type SessionState = {
  lastRetainedSeq: number;
  lastAppliedRetainId?: string;
  lastGenerationSeen: string | null;
  lastRetainedAt: number;
  chunkHashes: string[];
  retainDocSegment: number | null;
  retainSegmentUnits: number;
  committedKeys: string[];
  lastCommittedSeq: number;
  lastCommittedRange: { start: number; end: number } | null;
  lastCommittedAt: number;
  lastLedgerFailed: boolean;
  pendingRetain: PendingRange[];
  pendingLegacy: PendingLegacy[];
  deliveredCaptureIds: string[];
};
/** Read-side name kept for assemble() callers. */
export type SessionStateView = SessionState;

const MAX_HASHES = 500;
const MAX_COMMITTED_KEYS = 2000;
const MAX_PENDING_TURNS = 500;

/** Overridable (e8cb review amendment 1c) so tests point at a scratch directory. */
let sessionStateDirOverride: string | null = null;
export function setSessionStateDir(dir: string | null): void {
  sessionStateDirOverride = dir && dir.trim().length > 0 ? dir : null;
}

/** e8cb `${WORKSPACE_ROOT}\\workspace\\state\\astinus`, generalized to the engine's host root. */
export function sessionStateDir(hostStateRoot: string): string {
  return sessionStateDirOverride ?? join(hostStateRoot, "workspace", "state", "astinus");
}

/**
 * R27.2: the readable prefix is lossy, so the name also carries a SHA-256 of the complete key
 * (injective in practice), and the document records the key it belongs to.
 */
export function sessionStatePath(hostStateRoot: string, sessionKey: string): string {
  const key = String(sessionKey);
  const safe = key.replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 80);
  const digest = createHash("sha256").update(key, "utf8").digest("hex").slice(0, 32);
  return join(sessionStateDir(hostStateRoot), `${safe}.${digest}.json`);
}

/** The maintained plugin's lossy name; several keys can share it, so it is never adopted. */
export function legacySessionStatePath(hostStateRoot: string, sessionKey: string): string {
  const safe = String(sessionKey)
    .replace(/[^a-zA-Z0-9_.-]/g, "_")
    .slice(0, 120);
  return join(sessionStateDir(hostStateRoot), `${safe}.json`);
}

function defaultSessionState(): SessionState {
  return {
    lastRetainedSeq: 0,
    lastGenerationSeen: null,
    lastRetainedAt: 0,
    chunkHashes: [],
    retainDocSegment: null,
    retainSegmentUnits: 0,
    committedKeys: [],
    lastCommittedSeq: 0,
    lastCommittedRange: null,
    lastCommittedAt: 0,
    lastLedgerFailed: false,
    pendingRetain: [],
    pendingLegacy: [],
    deliveredCaptureIds: [],
  };
}

const strings = (v: unknown): string[] =>
  Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];

function parseState(p: any): SessionState {
  const base = defaultSessionState();
  return {
    ...base,
    lastAppliedRetainId:
      typeof p?.lastAppliedRetainId === "string" ? p.lastAppliedRetainId : undefined,
    lastRetainedSeq: typeof p?.lastRetainedSeq === "number" ? p.lastRetainedSeq : 0,
    lastGenerationSeen: typeof p?.lastGenerationSeen === "string" ? p.lastGenerationSeen : null,
    lastRetainedAt: typeof p?.lastRetainedAt === "number" ? p.lastRetainedAt : 0,
    chunkHashes: strings(p?.chunkHashes),
    retainDocSegment:
      typeof p?.retainDocSegment === "number" && p.retainDocSegment >= 1
        ? Math.floor(p.retainDocSegment)
        : null,
    retainSegmentUnits:
      typeof p?.retainSegmentUnits === "number" && p.retainSegmentUnits >= 0
        ? p.retainSegmentUnits
        : 0,
    committedKeys: strings(p?.committedKeys),
    lastCommittedSeq: typeof p?.lastCommittedSeq === "number" ? p.lastCommittedSeq : 0,
    lastCommittedRange:
      p?.lastCommittedRange &&
      typeof p.lastCommittedRange.start === "number" &&
      typeof p.lastCommittedRange.end === "number"
        ? { start: p.lastCommittedRange.start, end: p.lastCommittedRange.end }
        : null,
    lastCommittedAt: typeof p?.lastCommittedAt === "number" ? p.lastCommittedAt : 0,
    lastLedgerFailed: p?.lastLedgerFailed === true,
    pendingRetain: Array.isArray(p?.pendingRetain)
      ? p.pendingRetain.filter(
          (r: any) =>
            r &&
            typeof r.start === "number" &&
            typeof r.end === "number" &&
            Array.isArray(r.messages) &&
            (r.key === undefined || typeof r.key === "string")
        )
      : [],
    pendingLegacy: Array.isArray(p?.pendingLegacy)
      ? p.pendingLegacy.filter(
          (l: any) =>
            l &&
            typeof l.id === "string" &&
            typeof l.at === "number" &&
            Array.isArray(l.messages) &&
            (l.sessionId === undefined || typeof l.sessionId === "string")
        )
      : [],
    deliveredCaptureIds: strings(p?.deliveredCaptureIds),
  };
}

/**
 * R29.1: strict callers refuse a document whose modeled fields the lenient parse would change -
 * a mistyped scalar, or a pending/known-id entry it would filter out. Absent fields are fine
 * (they default), as are keyless ranges and fields the plugin does not model.
 */
function assertModeled(p: any): void {
  const parsed = parseState(p) as Record<string, unknown>;
  for (const k of [...Object.keys(defaultSessionState()), "lastAppliedRetainId"]) {
    const raw = p[k];
    const got = parsed[k];
    if (raw === undefined) continue;
    const ok = Array.isArray(got)
      ? Array.isArray(raw) && raw.length === got.length
      : k === "lastCommittedRange"
        ? (raw === null) === (got === null)
        : raw === got;
    if (!ok) throw new Error(`Retain session state field ${k} is invalid; durable writes refused`);
  }
}

/**
 * strict (every durable mutation and drain): a corrupt, mistyped or foreign document THROWS and
 * is left untouched; a missing document initializes, unless a legacy lossy-named document exists
 * for the key - that debt may belong to another session, so it fails closed until reconciled.
 * Non-strict (assemble's view) reads all of those as defaults.
 */
function readRaw(hostStateRoot: string, sessionKey: string, strict: boolean): any {
  try {
    const p = JSON.parse(readFileSync(sessionStatePath(hostStateRoot, sessionKey), "utf8"));
    if (!p || typeof p !== "object" || Array.isArray(p)) {
      throw new Error("Retain session state has an invalid shape");
    }
    if (p.sessionKey !== undefined && p.sessionKey !== String(sessionKey)) {
      throw new Error("Retain session state belongs to another session");
    }
    if (strict) assertModeled(p);
    return p;
  } catch (error) {
    if (!strict) return {};
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    const legacy = legacySessionStatePath(hostStateRoot, sessionKey);
    if (existsSync(legacy)) {
      throw new Error(`legacy session state ${legacy} is unreconciled; durable writes refused`);
    }
    return {};
  }
}

/** strict: a corrupt document THROWS instead of reading as defaults (completion writer). */
export function loadSessionState(
  hostStateRoot: string,
  sessionKey: string,
  strict = false
): SessionState {
  return parseState(readRaw(hostStateRoot, sessionKey, strict));
}

/**
 * Load-modify-write the durable state atomically (unique tmp + rename). THROWS on mutate or
 * persist failure: a caller that must be durable (commitTurn, the completion writer) needs it.
 */
export function updateSessionState(
  hostStateRoot: string,
  sessionKey: string,
  mutate: (state: SessionState) => SessionState
): SessionState {
  // Always strict (R29.1): a leniently normalized view must never become the replacement.
  const raw = readRaw(hostStateRoot, sessionKey, true);
  const current = parseState(raw);
  const next = mutate(current);
  if (next === current) return current; // no-op decision (duplicate / already applied): no write
  const capped: SessionState = {
    ...next,
    chunkHashes: next.chunkHashes.slice(-MAX_HASHES),
    committedKeys: next.committedKeys.slice(-MAX_COMMITTED_KEYS),
    deliveredCaptureIds: next.deliveredCaptureIds.slice(-MAX_COMMITTED_KEYS),
  };
  const target = sessionStatePath(hostStateRoot, sessionKey);
  mkdirSync(sessionStateDir(hostStateRoot), { recursive: true });
  // Unique tmp name: two processes must never interleave on one tmp path (review 2026-09-13, Q2).
  const tmp = `${target}.${process.pid}.${randomUUID()}.tmp`;
  writeFileSync(
    tmp,
    JSON.stringify({ ...raw, ...capped, sessionKey: String(sessionKey) }, null, 2),
    "utf8"
  );
  renameSync(tmp, target);
  return capped;
}

/** Append under the cap; on overflow the OLDEST entries drop, never silently (review Q2). */
function capPending<T>(next: T[], what: string, warn: (msg: string) => void): T[] {
  if (next.length <= MAX_PENDING_TURNS) return next;
  warn(
    `[hindsight-engine] ${what} overflow: dropping ${next.length - MAX_PENDING_TURNS} oldest item(s); retains are not draining`
  );
  return next.slice(-MAX_PENDING_TURNS);
}
/** Stamps `at` so the age backstop can drain ranges the commit path never re-offers. */
export function appendPendingRange(
  queue: PendingRange[],
  range: PendingRange,
  warn: (msg: string) => void
): PendingRange[] {
  return capPending([...queue, { ...range, at: Date.now() }], "pendingRetain", warn);
}
export function appendPendingLegacy(
  queue: PendingLegacy[],
  item: PendingLegacy,
  warn: (msg: string) => void
): PendingLegacy[] {
  return capPending([...queue, item], "pendingLegacy", warn);
}

/** Apply a durable server completion once, before acknowledging its queue row. */
export function applyCompletedRetain(hostStateRoot: string, item: QueuedRetain): void {
  const action = item.completion;
  if (!action) return; // Older payload-only work has no local marker authority.
  updateSessionState(hostStateRoot, action.sessionKey, (state) => {
    if (state.lastAppliedRetainId === item.id) return state;
    const marker = action.marker;
    const segment = action.segment;
    const chargeCurrent =
      segment && (state.retainDocSegment === null || state.retainDocSegment === segment.segment);
    return {
      ...state,
      lastAppliedRetainId: item.id,
      ...(marker
        ? {
            lastRetainedSeq: Math.max(state.lastRetainedSeq, marker.seq),
            lastRetainedAt: Date.now(),
            chunkHashes: [...new Set([...state.chunkHashes, ...marker.hashes])],
            pendingRetain: state.pendingRetain.filter(
              (range) => !marker.ranges.some((covered) => sameCommittedWork(covered, range))
            ),
            pendingLegacy: state.pendingLegacy.filter(
              (legacy) => !marker.legacyIds?.includes(legacy.id)
            ),
            // Delivery dedupe, kept apart from commit acknowledgment: a replayed agent_end
            // for a delivered run is a no-op, restarts included.
            deliveredCaptureIds: [...state.deliveredCaptureIds, ...(marker.legacyIds ?? [])],
          }
        : {}),
      ...(chargeCurrent
        ? {
            retainDocSegment: segment.segment,
            retainSegmentUnits: state.retainSegmentUnits + segment.unitsDelta,
          }
        : {}),
    };
  });
}

// PERF (e8cb 2026-09-21): cache by (path, mtime); writers use tmp+rename, so a changed mtime is
// the invalidation signal. The path includes the host root, so roots never share an entry.
const sessionStateCache = new Map<string, { mtimeMs: number; state: SessionState }>();
const SESSION_STATE_CACHE_MAX = 2000;
export function loadSessionStateCached(hostStateRoot: string, sessionKey: string): SessionState {
  const path = sessionStatePath(hostStateRoot, sessionKey);
  let mtimeMs = -1;
  try {
    mtimeMs = statSync(path).mtimeMs;
  } catch {
    /* missing file: loadSessionState returns the default; do not cache a miss */
    return loadSessionState(hostStateRoot, sessionKey);
  }
  const hit = sessionStateCache.get(path);
  if (hit && hit.mtimeMs === mtimeMs) return hit.state;
  const state = loadSessionState(hostStateRoot, sessionKey);
  if (sessionStateCache.size >= SESSION_STATE_CACHE_MAX) {
    const oldest = sessionStateCache.keys().next().value;
    if (oldest !== undefined) sessionStateCache.delete(oldest);
  }
  sessionStateCache.set(path, { mtimeMs, state });
  return state;
}
