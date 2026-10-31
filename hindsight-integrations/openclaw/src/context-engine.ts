// Context engine registration, ported from the maintained plugin
// (e8cb94b7 src/index.ts:3130-3250 registration, :3286 assemble fallback,
// :3566-3587 compact delegation). This wave ports the NON-durable engine shell:
//
// - info: ownsCompaction false, no hostRequirements, and NO transcriptSemantics /
//   commitTurn. Without the durable-turn declaration no host hands this engine
//   turn ownership, so the ordinary agent_end capture stays the only capture on
//   every host and the wave-4 ownership predicate is intentionally not wired yet.
// - ingest: no-op (statement<->topic mapping lands with the session state port).
// - assemble: the maintained read side (e8cb :3274-3470): read the session's marks from an
//   ALREADY-OPEN ledger (readMarksCached), apply them to the view (assemble-marks.ts) and stamp
//   admitted marks. Authority is "assembled" only while a mark is in effect; otherwise the
//   maintained pass-through with preassembly_may_overflow. Until the commit path that opens the
//   ledger is ported, readMarksCached returns not-open and the view is the pass-through.
//   node:sqlite modules load lazily, so the plugin itself still loads where node:sqlite is absent.
// - compact: the maintained delegation to the runtime's built-in compaction.
//   Owner compaction is not ported and stays unavailable.
//
// TODO(host-capability): when commitTurn is ported, declare
//   hostRequirements["agent-run"].requiredCapabilities = ["agent-end-context-engine-turn-v1"]
// only after Vern confirms the host contract to Grokbot (see package 6).
//
// ENGINE ID = PLUGIN ID ("hindsight-openclaw"): the host reads
// plugins.slots.contextEngine both as the owning plugin id and as the engine id.
import { join } from "node:path";
import type { MarksAdmissionPolicy } from "./assemble-marks.js";
import { ENGINE_ID } from "./engine-compat.js";

type Logger = { info(msg: string): void; warn(msg: string): void };

export type RegisterContextEngine = (id: string, factory: () => unknown) => void;

function messageText(message: unknown): string {
  const content = (message as { content?: unknown } | null | undefined)?.content;
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .map((part) =>
        part && typeof part === "object" && typeof (part as { text?: unknown }).text === "string"
          ? (part as { text: string }).text
          : ""
      )
      .filter(Boolean)
      .join("\n");
  }
  return "";
}

/** Rough char/4 heuristic, as in the maintained engine's pass-through path. */
export function estimateTokens(messages: unknown[]): number {
  let chars = 0;
  for (const m of messages) chars += messageText(m).length + 16;
  return Math.ceil(chars / 4);
}

export type CompactionDelegate = (params: unknown) => Promise<unknown>;

async function loadRuntimeDelegate(): Promise<CompactionDelegate> {
  const sdkSpecifier = "openclaw/plugin-sdk/core";
  const sdk = (await import(sdkSpecifier)) as {
    delegateCompactionToRuntime?: CompactionDelegate;
  };
  if (typeof sdk.delegateCompactionToRuntime !== "function") {
    throw new Error("delegateCompactionToRuntime not exported by openclaw/plugin-sdk/core");
  }
  return sdk.delegateCompactionToRuntime;
}

/** The node:sqlite-backed read side, loaded lazily (runtime floor: node:sqlite, Node >= 22.5). */
export type MarksReadSide = {
  readMarksCached(
    sessionKey: string,
    warn: (msg: string) => void
  ): { ok: boolean; marks: unknown[]; reason?: string };
  readMaxTurnId(sessionKey: string): number | null;
  markMarksApplied(
    sessionKey: string,
    markIds: Array<number | unknown>,
    gen: string,
    tokensBefore: number,
    tokensAfter: number
  ): void;
  applyMarks: typeof import("./assemble-marks.js").applyMarks;
  estimateTokensHost(list: unknown[]): number;
};

async function loadMarksReadSide(): Promise<MarksReadSide> {
  const [ledger, marks] = await Promise.all([
    import("./session-ledger.js"),
    import("./assemble-marks.js"),
  ]);
  return {
    readMarksCached: ledger.readMarksCached,
    readMaxTurnId: ledger.readMaxTurnId,
    markMarksApplied: ledger.markMarksApplied,
    applyMarks: marks.applyMarks,
    estimateTokensHost: marks.estimateTokensHost,
  };
}

export type ContextEngineOptions = {
  loadDelegate?: () => Promise<CompactionDelegate>;
  /** Host instance state root (runtime.state.resolveStateDir); marks are skipped without it. */
  hostStateRoot?: string;
  loadMarks?: () => Promise<MarksReadSide>;
  /** Admission overrides from plugin config (`astinusMarks`); DEFAULT_MARKS_POLICY otherwise. */
  getPolicy?: () => Partial<MarksAdmissionPolicy> | undefined;
};

/** Plugin config `astinusMarks` -> admission overrides (e8cb index.ts astinusMarksPolicy). */
export function marksPolicyFromConfig(config: unknown): Partial<MarksAdmissionPolicy> | undefined {
  const raw = (config as { astinusMarks?: Record<string, unknown> } | undefined)?.astinusMarks;
  if (!raw || typeof raw !== "object") return undefined;
  const out: Partial<MarksAdmissionPolicy> = {};
  if (typeof raw.minPressure === "number") out.minPressure = raw.minPressure;
  if (typeof raw.admitShare === "boolean") out.admitShare = raw.admitShare;
  if (typeof raw.debounceTurns === "number") out.debounceTurns = raw.debounceTurns;
  if (typeof raw.criticalPressure === "number") out.criticalPressure = raw.criticalPressure;
  return Object.keys(out).length > 0 ? out : undefined;
}

/** Host transcript store for an agent session key (generalized from e8cb's fixed workspace path). */
export function hostDbPathFor(hostStateRoot: string, sessionKey: string): string {
  const agentId = sessionKey.startsWith("agent:") ? sessionKey.split(":")[1] : "";
  if (!hostStateRoot || !agentId) return "";
  return join(hostStateRoot, "agents", agentId, "agent", "openclaw-agent.sqlite");
}

/** Last-resort marks per host root + session (e8cb astinusMarksIndex, generalized): refilled by
 *  every successful ledger read and used when the ledger read fails, or not at all on a cold start.
 *  R11.1: keyed by host state root AND session key, so a failed read in one root can never reuse
 *  another root's sequence ranges (separate transcripts share seq numbers and session keys). */
const marksIndex = new Map<string, unknown[]>();
const MARKS_INDEX_MAX_SESSIONS = 2000;
const MAX_TRACKED_SESSIONS = 10_000;

/** What assemble() records per session (e8cb AstinusAssembleFact): the serving signal and the
 *  pass pressure inputs (view_tokens / token_budget / estimator / window_first_seq). */
export type AssembleFact = {
  at: number;
  viewTokens: number;
  tokenBudget: number | null;
  estimator: "host" | "plugin";
  windowFirstSeq: number | null;
};

// e8cb R2 (2026-10-25): the map is shared by every plugin copy of one host instance, so it lives in
// a process-wide registry keyed by host state root (e8cb lifecycle.lastAssemble via Symbol.for).
const LAST_ASSEMBLE = Symbol.for("@vectorize-io/hindsight-openclaw/last-assemble");
function lastAssembleFor(stateRoot: string): Map<string, AssembleFact> {
  const g = globalThis as unknown as Record<symbol, Map<string, Map<string, AssembleFact>>>;
  const all = (g[LAST_ASSEMBLE] ??= new Map());
  let m = all.get(stateRoot);
  if (!m) {
    m = new Map();
    all.set(stateRoot, m);
  }
  return m;
}

/** Read side for the future pass payload (commit path, not ported yet). */
export function getLastAssembleFact(
  stateRoot: string,
  sessionKey: string
): AssembleFact | undefined {
  return lastAssembleFor(stateRoot).get(sessionKey);
}

export function createContextEngine(log: Logger, options: ContextEngineOptions = {}) {
  const loadDelegate = options.loadDelegate ?? loadRuntimeDelegate;
  const loadMarks = options.loadMarks ?? loadMarksReadSide;
  const info = Object.freeze({
    id: ENGINE_ID,
    name: "Hindsight Context Engine",
    ownsCompaction: false,
  });
  return {
    info,
    async ingest(_params: unknown): Promise<{ ingested: boolean }> {
      return { ingested: false };
    },
    async assemble(params: {
      messages?: unknown[];
      tokenBudget?: number;
      sessionKey?: string;
      sessionId?: string;
    }): Promise<{
      messages: unknown[];
      estimatedTokens: number;
      promptAuthority: "assembled" | "preassembly_may_overflow";
    }> {
      const assembleStartedAt = Date.now();
      const list = Array.isArray(params.messages) ? params.messages : [];
      let outList = list;
      let windowFirstSeq: number | null = null;
      let marksSeen = 0;
      let estimatedTokens = 0;
      let marksInEffect = 0;
      const key = params.sessionKey ?? params.sessionId;
      const hostDbPath = key ? hostDbPathFor(options.hostStateRoot ?? "", key) : "";
      if (key && hostDbPath && typeof params.sessionId === "string" && params.sessionId) {
        try {
          const side = await loadMarks();
          const read = side.readMarksCached(key, (m) => log.warn(m));
          const cacheKey = JSON.stringify([options.hostStateRoot ?? "", key]);
          let marks: unknown[] = marksIndex.get(cacheKey) ?? [];
          if (read.ok) {
            marks = read.marks;
            marksIndex.set(cacheKey, marks);
            if (marksIndex.size > MARKS_INDEX_MAX_SESSIONS) {
              const oldest = marksIndex.keys().next().value;
              if (oldest !== undefined) marksIndex.delete(oldest);
            }
          } else if (read.reason !== "not-open") {
            log.warn(
              `[hindsight-engine] assemble: marks unavailable (${read.reason}); using cached index (${marks.length})`
            );
          }
          marksSeen = marks.length;
          if (marks.length > 0) {
            const res = side.applyMarks(list, marks as never, {
              sessionKey: key,
              hostDbPath,
              // B1: transcript rows are keyed by the HOST session UUID, never the session key.
              sessionId: params.sessionId,
              tokenBudget: params.tokenBudget ?? 0,
              // Review answer 1 (e8cb): the admission knobs ride the apply context from plugin config.
              policy: options.getPolicy?.(),
              // The session state document (lastCommittedSeq fallback) is not ported yet.
              turnCounter: side.readMaxTurnId(key) ?? 0,
            });
            if (res.readError) {
              log.warn(
                `[hindsight-engine] assemble: host transcript unreadable (${res.readError}); view unchanged`
              );
            }
            outList = res.messages;
            // Captured unconditionally (e8cb Q1 2026-10-07): the held-marks case needs it most.
            windowFirstSeq = res.windowFirstSeq ?? null;
            estimatedTokens = res.tokensBefore;
            marksInEffect = res.marksInEffect;
            const admittedIds = res.admitted.map((m) => m.mark_id);
            if (admittedIds.length > 0) {
              // Generation stamp comes from the unported state document; "" as e8cb's cold default.
              side.markMarksApplied(key, admittedIds, "", res.tokensBefore, res.tokensAfter);
            }
            if (res.changed || marksInEffect > 0) {
              log.info(
                `[hindsight-engine] view: ${res.messages.length} msgs, ~${res.tokensAfter} tok (from ${res.tokensBefore}; ${marksInEffect} marks in effect, ${admittedIds.length} admitted of ${res.pendingCount} pending; session=${key})`
              );
            }
            if (marksInEffect > 0 && outList !== list) {
              estimatedTokens = side.estimateTokensHost(outList);
            }
          }
        } catch (e) {
          // Any failure degrades to "less pruned", never an error.
          log.warn(
            `[hindsight-engine] assemble: mark application failed: ${(e as Error)?.message ?? e}`
          );
          outList = list;
          marksInEffect = 0;
          estimatedTokens = 0;
          windowFirstSeq = null;
        }
      }
      if (estimatedTokens === 0 && list.length > 0) estimatedTokens = estimateTokens(list);
      // PERF instrumentation (e8cb): assemble runs once per model call; name a slow one.
      const assembleMs = Date.now() - assembleStartedAt;
      if (assembleMs > 150) {
        log.warn(
          `[hindsight-engine] assemble took ${assembleMs}ms: ${list.length} msgs, ~${estimatedTokens} tok, marks=${marksSeen} (${marksInEffect} in effect)` +
            (key ? ` session=${key}` : "")
        );
      }
      if (key) {
        // Record this session's last assemble; touch-on-write so the cap behaves LRU (e8cb m1).
        const facts = lastAssembleFor(options.hostStateRoot ?? "");
        facts.delete(key);
        facts.set(key, {
          at: Date.now(),
          viewTokens: estimatedTokens,
          tokenBudget: typeof params.tokenBudget === "number" ? params.tokenBudget : null,
          estimator: marksInEffect > 0 ? "host" : "plugin",
          windowFirstSeq,
        });
        if (facts.size > MAX_TRACKED_SESSIONS) {
          const oldest = facts.keys().next().value;
          if (oldest !== undefined) facts.delete(oldest);
        }
      }
      return {
        messages: outList,
        estimatedTokens,
        promptAuthority: marksInEffect > 0 ? "assembled" : "preassembly_may_overflow",
      };
    },
    async compact(params: unknown): Promise<unknown> {
      try {
        const delegate = await loadDelegate();
        const result = await delegate(params);
        log.info("[hindsight-engine] compact: delegated to the runtime's built-in compaction");
        return result;
      } catch (e) {
        const msg = (e as Error)?.message ?? String(e);
        log.warn(
          `[hindsight-engine] compact: runtime delegation failed (${msg}); ` +
            "returning compacted=false - the host will report auto-compaction failed"
        );
        return {
          ok: false,
          compacted: false,
          reason: `hindsight: runtime compaction delegation failed: ${msg}`,
        };
      }
    },
  };
}

/** Registers the engine when the host exposes registerContextEngine; otherwise a no-op. */
export function registerHindsightContextEngine(
  api: { registerContextEngine?: RegisterContextEngine },
  log: Logger,
  hostStateRoot?: string,
  getPolicy?: () => Partial<MarksAdmissionPolicy> | undefined
): boolean {
  const register = api.registerContextEngine;
  if (typeof register !== "function") {
    log.info("[hindsight-engine] host does not expose registerContextEngine; skipping");
    return false;
  }
  try {
    register.call(api, ENGINE_ID, () => createContextEngine(log, { hostStateRoot, getPolicy }));
    return true;
  } catch (e) {
    log.warn(`[hindsight-engine] registerContextEngine failed: ${(e as Error)?.message ?? e}`);
    return false;
  }
}
