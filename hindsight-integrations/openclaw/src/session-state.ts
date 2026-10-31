import { readFileSync, statSync } from "node:fs";
import { join } from "node:path";

/**
 * Read side of the durable per-session state document (e8cb94b7 AstinusSessionState,
 * loadSessionState / loadSessionStateCached). Narrowed to the two fields assemble() consumes:
 * the generation stamp (lastGenerationSeen) and the durable seq counter (lastCommittedSeq, the
 * cold-ledger turnCounter fallback). Writers (retain loop, commitTurn) are not ported; with no
 * writer the file is absent and every read returns the defaults.
 */
export type SessionStateView = {
  lastGenerationSeen: string | null;
  lastCommittedSeq: number;
};

/** Overridable (e8cb review amendment 1c) so tests point at a scratch directory. */
let sessionStateDirOverride: string | null = null;
export function setSessionStateDir(dir: string | null): void {
  sessionStateDirOverride = dir && dir.trim().length > 0 ? dir : null;
}

/** e8cb `${WORKSPACE_ROOT}\\workspace\\state\\astinus`, generalized to the engine's host root. */
export function sessionStateDir(hostStateRoot: string): string {
  return sessionStateDirOverride ?? join(hostStateRoot, "workspace", "state", "astinus");
}

export function sessionStatePath(hostStateRoot: string, sessionKey: string): string {
  const safe = String(sessionKey)
    .replace(/[^a-zA-Z0-9_.-]/g, "_")
    .slice(0, 120);
  return join(sessionStateDir(hostStateRoot), `${safe}.json`);
}

function defaultSessionState(): SessionStateView {
  return { lastGenerationSeen: null, lastCommittedSeq: 0 };
}

export function loadSessionState(hostStateRoot: string, sessionKey: string): SessionStateView {
  const base = defaultSessionState();
  try {
    const p = JSON.parse(readFileSync(sessionStatePath(hostStateRoot, sessionKey), "utf8"));
    return {
      lastGenerationSeen:
        typeof p?.lastGenerationSeen === "string" ? p.lastGenerationSeen : base.lastGenerationSeen,
      lastCommittedSeq: typeof p?.lastCommittedSeq === "number" ? p.lastCommittedSeq : 0,
    };
  } catch {
    return base;
  }
}

// PERF (e8cb 2026-09-21): cache by (path, mtime); writers use tmp+rename, so a changed mtime is
// the invalidation signal. The path includes the host root, so roots never share an entry.
const sessionStateCache = new Map<string, { mtimeMs: number; state: SessionStateView }>();
const SESSION_STATE_CACHE_MAX = 2000;
export function loadSessionStateCached(
  hostStateRoot: string,
  sessionKey: string
): SessionStateView {
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
