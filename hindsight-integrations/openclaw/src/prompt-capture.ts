/**
 * Prompt capture: the two-turn rotating store and the wire-capture reader seam.
 *
 * Scope is exactly two complete turns per session key, replaced rather than
 * accumulated, and reconstructed from what the store already holds so a restart
 * cannot leave a third turn behind. The core is pure: it works against a small
 * store port, and the production adapter supplies compression and a directory.
 * No paths, session keys or desk names live here.
 */

import type { CategoryCounts } from "./token-count.js";

/** One model call inside a turn. Bodies live in the store, keyed by hash. */
export interface CapturedCall {
  index: number;
  requestSha256: string;
  responseSha256: string;
  requestBytes: number;
  responseBytes: number;
  /** Provider prompt tokens for this call, when the usage join had them. */
  providerPromptTokens?: number;
}

/** One complete turn. This is the manifest record; bodies are separate blobs. */
export interface CapturedTurn {
  turnId: string;
  startedAt: number;
  completedAt: number;
  messageCount: number;
  calls: CapturedCall[];
  /** Provider prompt tokens for the turn, when known. */
  providerPromptTokens?: number;
  marksInEffect: number;
  admittedMarkIds: Array<number | string>;
  /** Category counts of the engine view this turn (P4). */
  categories?: CategoryCounts;
  /** Engine view totals, labelled as the engine view rather than the wire. */
  engineView?: { beforeTotal: number; afterTotal: number };
  /**
   * The engine-view lists themselves, inside the turn record so rotating the turn
   * out removes them too (a separate blob store would grow without bound).
   */
  engineLists?: { before: unknown[]; after: unknown[] };
  /** Tokens the marks removed from the engine view, total (P4). */
  markRemovalTotal?: number;
}

/** Storage port. The production adapter owns paths, gzip and durability. */
export interface TurnStore {
  /** Turn ids already stored for this session key. Order is not assumed. */
  list(sessionKey: string): Promise<string[]>;
  /** Read one stored turn manifest, or null when absent. */
  read(sessionKey: string, turnId: string): Promise<CapturedTurn | null>;
  /** Write or replace one turn manifest and its blobs. */
  write(sessionKey: string, turn: CapturedTurn): Promise<void>;
  /** Remove one turn and its blobs. Missing is not an error. */
  remove(sessionKey: string, turnId: string): Promise<void>;
}

export interface RotationResult {
  kept: string[];
  removed: string[];
  /** True when the turn supplied was already stored and nothing changed. */
  unchanged: boolean;
}

export const DEFAULT_TURNS_KEPT = 2;

/**
 * Store one completed turn and reduce the session to the newest `keep` turns.
 *
 * Ordering is by completedAt, not by write order and not by memory: a restart
 * or an out-of-order completion cannot promote an older turn over a newer one.
 * The turn being written always counts as kept, even if its clock is behind.
 */
export async function rotateTurns(
  store: TurnStore,
  sessionKey: string,
  turn: CapturedTurn,
  keep: number = DEFAULT_TURNS_KEPT
): Promise<RotationResult> {
  const limit = Math.max(1, Math.trunc(keep));
  const existing = new Map<string, CapturedTurn>();
  for (const turnId of await store.list(sessionKey)) {
    const stored = await store.read(sessionKey, turnId);
    if (stored) existing.set(turnId, stored);
  }

  const prior = existing.get(turn.turnId);
  const unchanged =
    prior !== undefined &&
    prior.completedAt === turn.completedAt &&
    prior.calls.length === turn.calls.length;
  existing.set(turn.turnId, turn);

  const ordered = [...existing.values()].sort((a, b) => {
    if (a.completedAt !== b.completedAt) return b.completedAt - a.completedAt;
    return String(a.turnId) < String(b.turnId) ? 1 : -1;
  });

  const kept = ordered.slice(0, limit);
  const keptIds = new Set(kept.map((entry) => entry.turnId));
  keptIds.add(turn.turnId);

  if (!unchanged) await store.write(sessionKey, turn);

  const removed: string[] = [];
  for (const entry of ordered) {
    if (keptIds.has(entry.turnId)) continue;
    await store.remove(sessionKey, entry.turnId);
    removed.push(entry.turnId);
  }

  return {
    kept: ordered.filter((entry) => keptIds.has(entry.turnId)).map((entry) => entry.turnId),
    removed,
    unchanged,
  };
}

/** Turns currently retained for a session, newest first. For readers and tests. */
export async function retainedTurns(
  store: TurnStore,
  sessionKey: string,
  keep: number = DEFAULT_TURNS_KEPT
): Promise<CapturedTurn[]> {
  const limit = Math.max(1, Math.trunc(keep));
  const turns: CapturedTurn[] = [];
  for (const turnId of await store.list(sessionKey)) {
    const stored = await store.read(sessionKey, turnId);
    if (stored) turns.push(stored);
  }
  return turns.sort((a, b) => b.completedAt - a.completedAt).slice(0, limit);
}
