import { existsSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import {
  assertModeled,
  legacySessionFileName,
  legacySessionStatePath,
  parseState,
  sessionStatePath,
  type SessionState,
} from "./session-state.js";

export type AdoptOutcome =
  | { status: "adopted"; key: string; carried: number; backup: string }
  | { status: "already" | "absent"; key: string; legacyPath: string }
  | {
      status:
        | "refuse-collision"
        | "refuse-orphan"
        | "refuse-unparseable"
        | "refuse-evidence";
      key: string;
      legacyPath: string;
      detail: string;
    };

export type SweepCounts = Record<string, number>;

function debtOf(state: SessionState): number {
  return (state.pendingRetain?.length ?? 0) + (state.pendingLegacy?.length ?? 0);
}

/**
 * Adopt legacy lossy-named session state documents into the key-hashed scheme.
 *
 * Adoption requires exactly one live owner for the lossy name. A debt-bearing document must also
 * prove ownership: its lastGenerationSeen has to equal the host's transcript generation for that
 * key (transcript_rewrite_watermarks.generation via session_nodes.current_session_id). Debt-free
 * documents adopt on uniqueness alone. Anything else is reported; with apply it is moved aside,
 * never deleted.
 */
export function sweepLegacyState(opts: {
  stateDir: string;
  keys: readonly string[];
  generationFor?: (key: string) => string | null;
  /** Session ids the host records for this key (current_session_id plus watermark rows). */
  sessionIdsFor?: (key: string) => readonly string[];
  apply: boolean;
}): { outcomes: AdoptOutcome[]; counts: SweepCounts; keys: number } {
  const byLossy = new Map<string, string[]>();
  for (const key of opts.keys) {
    const name = legacySessionFileName(key, ".json");
    const list = byLossy.get(name) ?? [];
    list.push(key);
    byLossy.set(name, list);
  }
  const outcomes: AdoptOutcome[] = [];
  for (const [name, owners] of byLossy) {
    const legacyPath = legacySessionStatePath(opts.stateDir, owners[0]);
    if (!existsSync(legacyPath)) continue;
    const key = owners[0];
    if (owners.length > 1) {
      outcomes.push({ status: "refuse-collision", key, legacyPath, detail: `${owners.length} owners` });
      continue;
    }
    if (existsSync(sessionStatePath(opts.stateDir, key))) {
      outcomes.push({ status: "already", key, legacyPath });
      continue;
    }
    let state: SessionState;
    try {
      const raw = JSON.parse(readFileSync(legacyPath, "utf8"));
      assertModeled(raw);
      state = parseState(raw);
    } catch (error) {
      outcomes.push({
        status: "refuse-unparseable",
        key,
        legacyPath,
        detail: String((error as Error)?.message ?? error).slice(0, 160),
      });
      if (opts.apply) moveAside(legacyPath);
      continue;
    }
    const debt = debtOf(state);
    if (debt > 0) {
      const hostGen = opts.generationFor?.(key) ?? null;
      const genMatch = Boolean(hostGen) && hostGen === state.lastGenerationSeen;
      const hostIds = new Set(opts.sessionIdsFor?.(key) ?? []);
      const entries = [...(state.pendingRetain ?? []), ...(state.pendingLegacy ?? [])];
      const idMatch = entries.some((e: { sessionId?: string }) => e?.sessionId && hostIds.has(e.sessionId));
      if (!genMatch && !idMatch) {
        outcomes.push({
          status: "refuse-evidence",
          key,
          legacyPath,
          detail: `debt=${debt} host=${hostGen ?? "none"} doc=${state.lastGenerationSeen ?? "none"} ids=${hostIds.size}`,
        });
        if (opts.apply) moveAside(legacyPath);
        continue;
      }
    }
    if (opts.apply) {
      const target = sessionStatePath(opts.stateDir, key);
      writeFileSync(target, JSON.stringify({ ...state, sessionKey: key }, null, 2), "utf8");
      const backup = moveAside(legacyPath);
      outcomes.push({ status: "adopted", key, carried: debt, backup });
    } else {
      outcomes.push({ status: "adopted", key, carried: debt, backup: "" });
    }
  }
  const counts: SweepCounts = {};
  for (const o of outcomes) counts[o.status] = (counts[o.status] ?? 0) + 1;
  return { outcomes, counts, keys: opts.keys.length };
}

function moveAside(legacyPath: string): string {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const backup = `${legacyPath}.adopted-${stamp}.bak`;
  renameSync(legacyPath, backup);
  return backup;
}
