// Context engine registration, ported from the maintained plugin
// (e8cb94b7 src/index.ts:3130-3250 registration, :3286 assemble fallback,
// :3566-3587 compact delegation) and the durable turn commit (9c76cbda9 commitTurn):
//
// - info: ownsCompaction false. Durable turns are OFF by default: the engine then declares no
//   transcriptSemantics, no commitTurn and no hostRequirements, so no host hands it a turn and
//   the ordinary agent_end capture stays the only capture. With `durableTurns` on it declares
//   the atomic-idempotent turn contract and commitTurn, and requires the host capability
//   agent-end-context-engine-turn-v1 (agent-run), so a host that would also capture owned turns
//   at agent_end refuses the engine instead of double-capturing.
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
// ENGINE ID = PLUGIN ID ("hindsight-openclaw"): the host reads
// plugins.slots.contextEngine both as the owning plugin id and as the engine id.
import { spawn } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type { PassPayload, PassProvider } from "./astinus-pass.js";
import type { MarksAdmissionPolicy } from "./assemble-marks.js";
import { shouldQueueRetain } from "./retain-scope.js";
import { ENGINE_ID } from "./engine-compat.js";
import {
  appendPendingRange,
  loadSessionState,
  loadSessionStateCached,
  updateSessionState,
} from "./session-state.js";
import {
  lifecycleAuthorityFor,
  newLifecycleAuthority,
  type AssembleFact,
  type LifecycleAuthority,
} from "./lifecycle-authority.js";

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
  runMarkRebaseCached?: typeof import("./session-ledger.js").runMarkRebaseCached;
  estimateTokensHost(list: unknown[]): number;
};

/**
 * PLAN section 4 runtime gate: package engines stay ">=22", but adaptive context
 * (the host transcript ledger read side) needs node:sqlite, which Node ships from
 * 22.5.0. Returns a specific compatibility error, or null when supported. Zstd
 * frames are gated at decode time (assemble-marks.ts decodeTranscriptEventJson).
 */
export function adaptiveContextRuntimeError(
  nodeVersion: string = process.versions.node
): string | null {
  const [major, minor] = nodeVersion.split(".").map((p) => Number.parseInt(p, 10) || 0);
  if (major > 22 || (major === 22 && minor >= 5)) return null;
  return (
    `adaptive context requires Node >= 22.5.0 for node:sqlite (running ${nodeVersion}); ` +
    `marks are disabled, prompts pass through unchanged`
  );
}

/**
 * R16.1: a version number does not prove node:sqlite loads - it is unflagged only from 22.13.0
 * (22.5-22.12 need --experimental-sqlite) and --no-experimental-sqlite disables it on any version.
 * Probe the builtin itself, once per process; null when it loads, else a specific diagnostic.
 */
export async function probeNodeSqlite(
  importSqlite: () => Promise<unknown> = () => import("node:sqlite")
): Promise<string | null> {
  try {
    await importSqlite();
    return null;
  } catch (e) {
    const code = (e as { code?: unknown })?.code;
    const detail = `${typeof code === "string" ? code + ": " : ""}${(e as Error)?.message ?? e}`;
    return (
      `adaptive context requires node:sqlite, which this runtime does not load (${detail}); ` +
      `it is unflagged from Node 22.13.0 (22.5-22.12 need --experimental-sqlite) and ` +
      `--no-experimental-sqlite disables it; marks are disabled, prompts pass through unchanged`
    );
  }
}

let sqliteProbe: Promise<string | null> | null = null;
function cachedSqliteProbe(): Promise<string | null> {
  return (sqliteProbe ??= probeNodeSqlite());
}

async function loadMarksReadSide(): Promise<MarksReadSide> {
  const [ledger, marks] = await Promise.all([
    import("./session-ledger.js"),
    import("./assemble-marks.js"),
  ]);
  return {
    readMarksCached: ledger.readMarksCached,
    readMaxTurnId: ledger.readMaxTurnId,
    markMarksApplied: ledger.markMarksApplied,
    runMarkRebaseCached: ledger.runMarkRebaseCached,
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
  /** Runtime version override for the compatibility gate (tests). */
  nodeVersion?: string;
  /** node:sqlite availability probe (tests); default probes the real builtin once per process. */
  probeSqlite?: () => Promise<string | null>;
  /**
   * Durable turns (config `durableTurns`, default OFF). When on, the engine declares the
   * atomic-idempotent turn contract and commitTurn, and requires the host capability
   * agent-end-context-engine-turn-v1, so a host that would also capture owned turns at
   * agent_end refuses the engine.
   */
  durableTurns?: boolean;
  /** F2 (config `turnMiddleAgeTurns`, default 6, read once): the pass's turn_middle proposal
   *  cutoff and the tail floor turn_middle marks apply under. */
  turnMiddleAgeTurns?: number;
  /** Resolved config `passProviders`, in order; reaches the pass over stdin only. */
  passProviders?: PassProvider[];
  /** Pass spawner override (tests); default spawns dist/astinus-pass.js detached. */
  spawnPass?: (payload: PassPayload) => void;
  /** Ledger writer override (tests); default the lazily loaded session-ledger module. */
  loadLedger?: () => Promise<LedgerWriter>;
};

export type LedgerWriter = {
  configureSessionLedger(workspaceRoot: string): void;
  insertCommittedTurn: typeof import("./session-ledger.js").insertCommittedTurn;
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

const MARKS_INDEX_MAX_SESSIONS = 2000;
const MAX_TRACKED_SESSIONS = 10_000;

export type { AssembleFact };

// P3: the last-assemble facts (e8cb R2) and the last-resort marks index (e8cb astinusMarksIndex)
// live in the ONE per-host-root lifecycle authority shared by every module copy, so a fresh copy
// sees the facts and cached marks the serving copy recorded, and roots never share an entry
// (R11.1). An engine without a resolvable root keeps a private per-module record.
const unrootedAuthority: LifecycleAuthority = newLifecycleAuthority();
function authorityFor(stateRoot: string | undefined): LifecycleAuthority {
  return stateRoot ? lifecycleAuthorityFor(stateRoot) : unrootedAuthority;
}

/** Read side for the future pass payload (commit path, not ported yet). */
export function getLastAssembleFact(
  stateRoot: string,
  sessionKey: string
): AssembleFact | undefined {
  return authorityFor(stateRoot).lastAssemble.get(sessionKey);
}

export function createContextEngine(log: Logger, options: ContextEngineOptions = {}) {
  const loadDelegate = options.loadDelegate ?? loadRuntimeDelegate;
  const loadMarks = options.loadMarks ?? loadMarksReadSide;
  let runtimeError = adaptiveContextRuntimeError(options.nodeVersion ?? process.versions.node);
  const probeSqlite = options.probeSqlite ?? cachedSqliteProbe;
  let runtimeErrorReported = false;
  const durable = options.durableTurns === true;
  const info = Object.freeze({
    id: ENGINE_ID,
    name: "Hindsight Context Engine",
    ownsCompaction: false,
    ...(durable
      ? {
          transcriptSemantics: {
            currentTurnFence: "before-current-turn-entry-v1",
            turnAdvancementIdempotency: "atomic-idempotent-v1",
          },
          hostRequirements: {
            "agent-run": { requiredCapabilities: ["agent-end-context-engine-turn-v1"] },
          },
        }
      : {}),
  });
  return {
    info,
    ...(durable
      ? { commitTurn: (params: CommitTurnParams) => commitTurn(log, options, params) }
      : {}),
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
      const marksSelected =
        key && hostDbPath && typeof params.sessionId === "string" && params.sessionId;
      // R16.1: only a selected path probes, so unselected/ordinary memory stays load-safe.
      if (marksSelected && !runtimeError) runtimeError = await probeSqlite();
      if (marksSelected && runtimeError && !runtimeErrorReported) {
        runtimeErrorReported = true;
        log.warn(`[hindsight-engine] ${runtimeError}`);
      }
      if (
        !runtimeError &&
        key &&
        hostDbPath &&
        typeof params.sessionId === "string" &&
        params.sessionId
      ) {
        try {
          const side = await loadMarks();
          // One-time rebase before any mark applies by number (guarded; never breaks assemble).
          side.runMarkRebaseCached?.(
            key,
            hostDbPath,
            params.sessionId,
            loadSessionStateCached(options.hostStateRoot ?? "", key).lastGenerationSeen ?? "",
            (m) => log.warn(m)
          );
          const read = side.readMarksCached(key, (m) => log.warn(m));
          const marksIndex = authorityFor(options.hostStateRoot).marksIndex;
          let marks: unknown[] = marksIndex.get(key) ?? [];
          if (read.ok) {
            marks = read.marks;
            marksIndex.set(key, marks);
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
            // Durable state document (e8cb loadSessionStateCached): generation stamp + seq counter.
            const state = loadSessionStateCached(options.hostStateRoot ?? "", key);
            const res = side.applyMarks(list, marks as never, {
              sessionKey: key,
              hostDbPath,
              // B1: transcript rows are keyed by the HOST session UUID, never the session key.
              sessionId: params.sessionId,
              tokenBudget: params.tokenBudget ?? 0,
              // Review answer 1 (e8cb): the admission knobs ride the apply context from plugin config.
              policy: options.getPolicy?.(),
              turnMiddleAgeTurns: options.turnMiddleAgeTurns,
              // e8cb M1: the debounce counts TURNS off the ledger; the durable seq counter
              // (state.lastCommittedSeq) is only the cold-handle fallback.
              turnCounter: side.readMaxTurnId(key) ?? state.lastCommittedSeq,
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
              side.markMarksApplied(
                key,
                admittedIds,
                state.lastGenerationSeen ?? "",
                res.tokensBefore,
                res.tokensAfter
              );
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
        const facts = authorityFor(options.hostStateRoot).lastAssemble;
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

export type CommitTurnParams = {
  advancementKey?: string;
  messages?: unknown[];
  sessionKey?: string;
  sessionId?: string;
  admission?: { rawSeq?: number; agentId?: string; storePath?: string };
  terminal?: { rawSeq?: number; generation?: string };
  isHeartbeat?: boolean;
};

/**
 * Ported from 9c76cbda9 commitTurn: records the accepted turn (rawSeq + generation and the
 * admission..terminal range) for the retain loop to drain. THROWS on a state failure so the host
 * retries the same key; it never reports "committed" for a turn it did not persist. Not ported
 * here: the in-process F13 lag check. The classification pass (package 28) is spawned once per
 * newly committed non-heartbeat turn whose ledger row landed.
 */
async function commitTurn(
  log: Logger,
  options: ContextEngineOptions,
  params: CommitTurnParams
): Promise<{ status: "committed" | "duplicate" }> {
  const key = params.sessionKey ?? params.sessionId;
  const advancementKey = typeof params.advancementKey === "string" ? params.advancementKey : "";
  const root = options.hostStateRoot;
  if (!key || !advancementKey || !root) {
    // The host contract requires both keys; a rootless engine has nowhere durable to write.
    throw new Error("hindsight commitTurn: missing session key, advancementKey or state root");
  }
  if (loadSessionState(root, key, true).committedKeys.includes(advancementKey)) {
    return { status: "duplicate" };
  }
  const messages = Array.isArray(params.messages) ? params.messages : [];
  const heartbeat = params.isHeartbeat === true;
  const startSeq = typeof params.admission?.rawSeq === "number" ? params.admission.rawSeq : null;
  const endSeq = typeof params.terminal?.rawSeq === "number" ? params.terminal.rawSeq : null;
  const generation =
    typeof params.terminal?.generation === "string" ? params.terminal.generation : null;
  // 1. Ledger write FIRST - best-effort, never throws (R1). The state-document write below is
  //    the sole acknowledgement and the only thing that may throw.
  let ledgerOk = false;
  try {
    const ledger = await (options.loadLedger ?? loadLedgerWriter)();
    ledger.configureSessionLedger(root);
    ledgerOk = ledger.insertCommittedTurn(
      {
        sessionKey: key,
        agentId: typeof params.admission?.agentId === "string" ? params.admission.agentId : "",
        hostDbPath:
          typeof params.admission?.storePath === "string" ? params.admission.storePath : "",
        sessionId: typeof params.sessionId === "string" ? params.sessionId : key,
        seqStart: startSeq ?? 0,
        seqEnd: endSeq ?? 0,
        generation: generation ?? "",
        advancementKey,
        heartbeat,
      },
      (m) => log.warn(m)
    ).ok;
  } catch (e) {
    log.warn(`[hindsight-engine] commitTurn: ledger unavailable: ${(e as Error)?.message ?? e}`);
  }
  // Only conversational roles feed the pending-retain queue - tool output is noise (Q1.7).
  const conversational = messages
    .filter((m) => {
      const role = (m as { role?: string } | null | undefined)?.role;
      return role === "user" || role === "assistant";
    })
    .map((m) => ({ role: (m as { role: string }).role, content: messageText(m) }))
    .filter((m) => m.content.length > 0);
  const now = Date.now();
  // R27.1: the duplicate decision and the append are one synchronous read-modify-write. A retry
  // that overlapped this one across the awaited ledger load (in any engine copy in this process)
  // finds the key here and records nothing. A failed write records no key, so a retry still works.
  let duplicate = false;
  updateSessionState(root, key, (s) => {
    if (s.committedKeys.includes(advancementKey)) {
      duplicate = true;
      return s;
    }
    return {
      ...s,
      committedKeys: [...s.committedKeys, advancementKey],
      lastCommittedSeq: endSeq ?? s.lastCommittedSeq,
      lastCommittedRange:
        startSeq !== null && endSeq !== null
          ? { start: startSeq, end: endSeq }
          : s.lastCommittedRange,
      lastGenerationSeen: generation ?? s.lastGenerationSeen,
      lastCommittedAt: now,
      lastLedgerFailed: !ledgerOk,
      // Recorded for the retain loop to drain; cleared by the completion writer.
      pendingRetain:
        heartbeat || startSeq === null || endSeq === null || !shouldQueueRetain(key)
          ? s.pendingRetain
          : appendPendingRange(
              s.pendingRetain,
              {
                start: startSeq,
                end: endSeq,
                messages: conversational,
                key: advancementKey,
                // Owner tag: a detached predecessor close may deliver only its own work.
                ...(typeof params.sessionId === "string" ? { sessionId: params.sessionId } : {}),
              },
              (m) => log.warn(m)
            ),
    };
  });
  if (!duplicate && ledgerOk && !heartbeat && startSeq !== null && endSeq !== null) {
    const facts = authorityFor(root).lastAssemble.get(key);
    try {
      (options.spawnPass ?? spawnPassProcess)({
        root,
        session_key: key,
        session_id: typeof params.sessionId === "string" ? params.sessionId : key,
        agent_id: params.admission?.agentId ?? "",
        host_db_path: params.admission?.storePath ?? "",
        range: [startSeq, endSeq],
        view_tokens: facts?.viewTokens ?? null,
        token_budget: facts?.tokenBudget ?? null,
        estimator: facts?.estimator ?? "plugin",
        window_first_seq: facts?.windowFirstSeq ?? null,
        turn_middle_age_turns: options.turnMiddleAgeTurns,
        providers: options.passProviders ?? [],
      });
    } catch (e) {
      log.warn(`[hindsight-engine] commitTurn: pass spawn failed: ${(e as Error)?.message ?? e}`);
    }
  }
  return { status: duplicate ? "duplicate" : "committed" };
}

/** Detached pass process; the payload (provider keys included) goes over stdin only. */
function spawnPassProcess(payload: PassPayload): void {
  const script = fileURLToPath(new URL("./astinus-pass.js", import.meta.url));
  const child = spawn(process.execPath, [script], {
    detached: true,
    stdio: ["pipe", "ignore", "ignore"],
    windowsHide: true,
  });
  child.on("error", () => {});
  child.stdin?.on("error", () => {});
  child.stdin?.end(JSON.stringify(payload));
  child.unref();
}

async function loadLedgerWriter(): Promise<LedgerWriter> {
  const ledger = await import("./session-ledger.js");
  return {
    configureSessionLedger: ledger.configureSessionLedger,
    insertCommittedTurn: ledger.insertCommittedTurn,
  };
}

/** Registers the engine when the host exposes registerContextEngine; otherwise a no-op. */
export function registerHindsightContextEngine(
  api: { registerContextEngine?: RegisterContextEngine },
  log: Logger,
  hostStateRoot?: string,
  getPolicy?: () => Partial<MarksAdmissionPolicy> | undefined,
  durableTurns = false,
  pass: Pick<ContextEngineOptions, "turnMiddleAgeTurns" | "passProviders"> = {}
): boolean {
  const register = api.registerContextEngine;
  if (typeof register !== "function") {
    log.info("[hindsight-engine] host does not expose registerContextEngine; skipping");
    return false;
  }
  try {
    register.call(api, ENGINE_ID, () =>
      createContextEngine(log, { hostStateRoot, getPolicy, durableTurns, ...pass })
    );
    return true;
  } catch (e) {
    log.warn(`[hindsight-engine] registerContextEngine failed: ${(e as Error)?.message ?? e}`);
    return false;
  }
}
