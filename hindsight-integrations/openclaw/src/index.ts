import type {
  MoltbotPluginAPI,
  PluginConfig,
  PluginHookAgentContext,
  PluginToolContext,
  MemoryResult,
  RetainRequest,
} from "./types.js";
import { HindsightServer, type Logger } from "@vectorize-io/hindsight-all";
import {
  HindsightClient,
  type HindsightClientOptions,
  type MinScores,
} from "@vectorize-io/hindsight-client";
import { RetainQueue } from "./retain-queue.js";
import {
  configureSessionLedger,
  insertCommittedTurn,
  markMarksApplied,
  readLiveTopics,
  readMarksCached,
  readMaxTurnSeq,
  readThinTopicSubjects,
  readTopicDelta,
  readTopicSubjects,
  readTopicsForRanges,
  writeFallbackTopics,
} from "./session-ledger.js";
import { applyMarks, estimateTokensHost, type LedgerMark } from "./assemble-marks.js";
import { compileSessionPatterns, matchesSessionPattern } from "./session-patterns.js";
import { createHash, randomUUID } from "crypto";
import { dirname, join } from "path";
import * as log from "./logger.js";
import { configureLogger, setApiLogger, stopLogger } from "./logger.js";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "fs";
import { access, readFile, readdir, stat, unlink, writeFile } from "fs/promises";
import { spawn } from "child_process";
import { createRequire } from "module";
import { homedir } from "os";
import { createKnowledgeTools, TOOL_NAMES } from "@vectorize-io/hindsight-agent-sdk";
import {
  applyConfiguredBankDefaults,
  hasConfiguredBankDefaults,
  normalizeDispositionTrait,
  normalizeEntityLabels,
  normalizeRetainExtractionMode,
} from "./bank-defaults.js";

function loadPackageVersion(): string {
  try {
    const require = createRequire(import.meta.url);
    const pkg = require("../package.json") as { version?: string };
    return pkg.version ?? "0.0.0";
  } catch {
    return "0.0.0";
  }
}

const USER_AGENT = `hindsight-openclaw/${loadPackageVersion()}`;

export const DEFAULT_RETAIN_CONTEXT =
  "This content is an AI-assistant conversation transcript from OpenClaw. " +
  "Retain request metadata may include routing identifiers such as " +
  "'sender_id' (an opaque user ID, not a human name), 'channel_id' (a chat identifier), " +
  "and 'provider' (the messaging platform name). " +
  "These are operational routing metadata, not semantic actors or people. " +
  "Messages with role 'assistant' are from the AI assistant; first-person statements " +
  "in assistant messages refer to the AI, not the human user. " +
  "Messages with role 'user' are from the human user. " +
  "Bank IDs, session keys, agent IDs, thread IDs, source systems, " +
  "and tags in metadata are also operational routing identifiers, " +
  "not human names, project names, or organizations.";

// Logger adapter that routes the embed wrapper's output through openclaw's
// batched structured logger so messages share the same prefix and respect
// the configured log level.
const embedLogger: Logger = {
  debug: (msg) => log.verbose(msg),
  info: (msg) => log.info(msg),
  warn: (msg) => log.warn(msg),
  error: (msg) => log.error(msg),
};

// Debug logging: silent by default, enable with debug: true or logLevel: 'debug'
let debugEnabled = false;
const debug = (...args: unknown[]) => {
  if (debugEnabled)
    log.verbose(
      args
        .map((a) => (typeof a === "string" ? a.replace(/^\[Hindsight\]\s*/, "") : String(a)))
        .join(" ")
    );
};

// Module-level state
let hindsightServer: HindsightServer | null = null;
let client: HindsightClient | null = null;
let clientOptions: HindsightClientOptions | null = null;
let initPromise: Promise<void> | null = null;
let isInitialized = false;
let usingExternalApi = false; // Track if using external API (skip daemon management)

// Capability detected once per service.start() against `<apiUrl>/version`.
// `true` when the Hindsight API supports `update_mode: 'append'` (added in
// 0.5.0 — see vectorize-io/hindsight#932) and stores document text. When false,
// retain falls back to a per-turn document id so prior turns aren't silently
// overwritten or rejected by text-disabled deployments.
let supportsUpdateModeAppend = false;
let appendCapabilityProbed = false;
const MIN_VERSION_FOR_UPDATE_MODE_APPEND = "0.5.0";

/** Whether retain is currently using session-scoped documents + `update_mode: 'append'`. */
export function isAppendModeSupported(): boolean {
  return supportsUpdateModeAppend;
}

export type AsyncRetainOperationIdCapability = "supported" | "unsupported" | "unknown";
let asyncRetainOperationIdCapability: AsyncRetainOperationIdCapability = "unknown";
const MIN_VERSION_FOR_ASYNC_RETAIN_OPERATION_ID = "0.8.6";

// Store the current plugin config for bank ID derivation
let currentPluginConfig: PluginConfig | null = null;
let serviceGeneration = 0;
let serviceAbortController: AbortController | null = null;

// Track which banks have had configured defaults applied (missions + bank config).
const banksWithDefaultsApplied = new Set<string>();

// In-flight recall deduplication: concurrent recalls for the same bank reuse one promise
import type { RecallResponse } from "./types.js";

// A recall resolution remembers WHICH path served it — the topic-filtered
// query or the unfiltered fallback — so the hook can log the serving path even
// when the result came from a reused in-flight promise (plan v4 item 3).
export type RecallResolution = {
  response: RecallResponse;
  via: "topic" | "unfiltered" | "topic+unfiltered";
};
const inflightRecalls = new Map<string, Promise<RecallResolution>>();

// Lightweight bank-scoped facade over HindsightClient. Created per-request via
// getClientForContext() so hook bodies can keep their bankId-implicit style
// without going back to a stateful setBankId pattern. Also bridges the
// small shape differences (e.g. RetainRequest.metadata is Record<string, unknown>
// at build time; HindsightClient wants Record<string, string>).
export interface BankScopedClient {
  readonly bankId: string;
  retain(
    req: RetainRequest,
    capability?: AsyncRetainOperationIdCapability,
    signal?: globalThis.AbortSignal
  ): Promise<void>;
  recall(
    req: {
      query: string;
      maxTokens?: number;
      budget?: "low" | "mid" | "high";
      types?: Array<"world" | "experience" | "observation">;
      preferObservations?: boolean;
      minScores?: MinScores;
      /**
       * Server-side tag filter (plan v4 item 3). The recall QUERY stays
       * substance-first; the topic ledger only narrows the bank scan via
       * `topic:<slug>` tags. The generated client maps these fields
       * explicitly (tags/tags_match), so unknown keys would be dropped.
       */
      tags?: string[];
      tagsMatch?: "any" | "all" | "any_strict" | "all_strict" | "exact";
    },
    timeoutMs?: number
  ): Promise<RecallResponse>;
  setMissions(opts: BankMissionsUpdate): Promise<void>;
}

export interface BankMissionsUpdate {
  reflectMission?: string;
  retainMission?: string;
  observationsMission?: string;
}

export function scopeClient(c: HindsightClient, bankId: string): BankScopedClient {
  return {
    bankId,
    async retain(req, capability = asyncRetainOperationIdCapability, signal) {
      await c.retain(bankId, req.content, {
        documentId: req.documentId,
        context: req.context,
        metadata: toStringMetadata(req.metadata),
        tags: req.tags,
        updateMode: req.updateMode,
        async: true,
        signal,
        ...(capability === "supported" && req.operationId ? { operationId: req.operationId } : {}),
      });
    },
    async recall(req, timeoutMs) {
      const call = c.recall(bankId, req.query, {
        maxTokens: req.maxTokens,
        budget: req.budget,
        types: req.types,
        preferObservations: req.preferObservations,
        minScores: req.minScores,
        tags: req.tags,
        tagsMatch: req.tagsMatch,
      });
      if (!timeoutMs) return call;
      // The generated client doesn't accept a per-call AbortSignal, so we race
      // against a TimeoutError here. The before_prompt_build caller already
      // special-cases `DOMException { name: 'TimeoutError' }` from the old
      // bespoke client, so we preserve that contract.
      return Promise.race([
        call,
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new DOMException(`Recall timed out after ${timeoutMs}ms`, "TimeoutError")),
            timeoutMs
          )
        ),
      ]);
    },
    async setMissions(opts) {
      // createBank upserts each mission column the request explicitly sets;
      // unset fields are left untouched (server's get_config_updates() skips
      // None values). This means a per-bank mission previously written via
      // PATCH /banks/{id} survives unless the plugin is configured with the
      // matching bank* / retain* / observations* mission.
      await c.createBank(bankId, {
        reflectMission: opts.reflectMission,
        retainMission: opts.retainMission,
        observationsMission: opts.observationsMission,
      });
    },
  };
}

async function ensureBankDefaultsApplied(bankId: string, config: PluginConfig): Promise<void> {
  if (!client || !clientOptions || !hasConfiguredBankDefaults(config)) {
    return;
  }
  if (banksWithDefaultsApplied.has(bankId)) {
    return;
  }
  try {
    await applyConfiguredBankDefaults(client, bankId, config, clientOptions);
    banksWithDefaultsApplied.add(bankId);
    debug(`[Hindsight] Applied configured defaults for bank: ${bankId}`);
  } catch (error) {
    log.warn(
      `could not apply bank defaults for ${bankId}: ${error instanceof Error ? error.message : error}`
    );
  }
}

/**
 * Format a single perf line for the `debugPerfTiming` flag. Pure function so
 * the formatting can be unit-tested without standing up the full hook pipeline.
 * Caller is responsible for stringifying durations with the `ms` suffix —
 * counts and identifiers are rendered as-is.
 */
export function formatHookPerf(
  hook: string,
  hookTotalMs: number,
  fields: Record<string, string | number | undefined>
): string {
  const parts = [`hook_total=${hookTotalMs}ms`];
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined) continue;
    parts.push(`${k}=${v}`);
  }
  return `perf: ${hook} ${parts.join(" ")}`;
}

/**
 * The generated client's metadata type is `Record<string, string>`; the
 * openclaw builder uses `Record<string, unknown>` because some fields come
 * from optional plugin context. Drop undefined/null, stringify the rest.
 */
function toStringMetadata(
  input: Record<string, unknown> | undefined
): Record<string, string> | undefined {
  if (!input) return undefined;
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(input)) {
    if (v === undefined || v === null) continue;
    out[k] = typeof v === "string" ? v : String(v);
  }
  return out;
}
const turnCountBySession = new Map<string, number>();
const MAX_TRACKED_SESSIONS = 10_000;

// ---------------------------------------------------------------------------
// Astinus retain marker (plan v4): durable per-session retention position.
// The POSITION is the transcript sequence (lastRetainedSeq). The watermark's
// generation is the rewrite INVALIDATION token — a differing value means a
// rewrite happened and seq positions must be re-anchored before they are
// trusted (v0 records it; the invalidation trigger lands with the DB-access
// step). Written after every successful retain; read at flush time and by the
// pruner's retention check.
// ---------------------------------------------------------------------------
// Canonical workspace root for this deployment. Single module-scope literal;
// the marker module and the deployment customizations both reference it.
const WORKSPACE_ROOT = "I:\\OpenClaw\\.openclaw";
const RETAIN_MARKER_MAX_HASHES = 500;
const ASTINUS_MAX_COMMITTED_KEYS = 2000;
const ASTINUS_MAX_PENDING_TURNS = 500;

/**
 * One durable per-session state document (plan v4). It hosts all three pieces
 * so the retain loop (agent_end) and the context engine (commitTurn) share one
 * file: the retain marker, the durable statement<->topic mapping, and the
 * commit idempotency keys. Writes are load-modify-write + atomic (tmp+rename),
 * so the two writers never clobber each other's fields.
 */
type AstinusSessionState = {
  // --- retain marker (advanced only after a successful retain) ---
  lastRetainedSeq: number;
  lastGenerationSeen: string | null;
  lastRetainedAt: number;
  /**
   * Per-message content hashes recorded at retain time. AUDIT ONLY - not yet
   * consulted for idempotency. The seq marker already gives exactly-once on the
   * normal path, and content-hash filtering would drop legitimate turns that
   * repeat identical text (two "ok" turns hash the same). Capped as a size
   * bound on the audit trail.
   */
  chunkHashes: string[];
  // --- durable statement<->topic mapping (context engine) ---
  topics: Record<string, { firstSeenAt: number; lastSeenAt: number; statements: number }>;
  // --- durable-turn commit state (context engine) ---
  committedKeys: string[];
  /** Terminal anchor rawSeq of the last committed turn (durable seq source). */
  lastCommittedSeq: number;
  /** Inclusive [admission.rawSeq, terminal.rawSeq] of the last committed turn. */
  lastCommittedRange: { start: number; end: number } | null;
  lastCommittedAt: number;
  /** True when the last commit's ledger write failed (best-effort store; R1). */
  lastLedgerFailed: boolean;
  // --- committed-unretained ranges (option a; slot-day) ---
  /**
   * Accepted turns the engine committed but the retain loop has not yet
   * retained. The retain path drains these (the agent_end payload never carries
   * seqs). Holds only conversational roles, capped at ASTINUS_MAX_PENDING_TURNS.
   */
  pendingRetain: Array<{ start: number; end: number; messages: unknown[] }>;
};
function retainMarkerDir(): string {
  return `${WORKSPACE_ROOT}\\workspace\\state\\astinus`;
}
function retainMarkerPath(sessionKey: string): string {
  const safe = String(sessionKey).replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 120);
  return `${retainMarkerDir()}\\${safe}.json`;
}
function defaultSessionState(): AstinusSessionState {
  return {
    lastRetainedSeq: 0,
    lastGenerationSeen: null,
    lastRetainedAt: 0,
    chunkHashes: [],
    topics: {},
    committedKeys: [],
    lastCommittedSeq: 0,
    lastCommittedRange: null,
    lastCommittedAt: 0,
    lastLedgerFailed: false,
    pendingRetain: [],
  };
}
function loadSessionState(sessionKey: string): AstinusSessionState {
  const base = defaultSessionState();
  try {
    const p = JSON.parse(readFileSync(retainMarkerPath(sessionKey), "utf8"));
    return {
      ...base,
      lastRetainedSeq:
        typeof p?.lastRetainedSeq === "number" ? p.lastRetainedSeq : base.lastRetainedSeq,
      lastGenerationSeen:
        typeof p?.lastGenerationSeen === "string" ? p.lastGenerationSeen : base.lastGenerationSeen,
      lastRetainedAt: typeof p?.lastRetainedAt === "number" ? p.lastRetainedAt : base.lastRetainedAt,
      chunkHashes: Array.isArray(p?.chunkHashes)
        ? p.chunkHashes.filter((h: any) => typeof h === "string")
        : [],
      topics:
        p?.topics && typeof p.topics === "object" && !Array.isArray(p.topics) ? p.topics : {},
      committedKeys: Array.isArray(p?.committedKeys)
        ? p.committedKeys.filter((k: any) => typeof k === "string")
        : [],
      lastCommittedSeq:
        typeof p?.lastCommittedSeq === "number" ? p.lastCommittedSeq : 0,
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
              Array.isArray(r.messages)
          )
        : [],
    };
  } catch {
    return base;
  }
}
/**
 * Load-modify-write the durable state atomically (tmp + rename).
 * THROWS on mutate or persist failure: a caller that must be durable (the
 * engine's commitTurn) needs the failure, never a silent no-op.
 */
function updateSessionState(
  sessionKey: string,
  mutate: (state: AstinusSessionState) => AstinusSessionState
): AstinusSessionState {
  const current = loadSessionState(sessionKey);
  const next = mutate(current);
  mkdirSync(retainMarkerDir(), { recursive: true });
  const capped: AstinusSessionState = {
    ...next,
    chunkHashes: next.chunkHashes.slice(-RETAIN_MARKER_MAX_HASHES),
    committedKeys: next.committedKeys.slice(-ASTINUS_MAX_COMMITTED_KEYS),
  };
  const target = retainMarkerPath(sessionKey);
  // Unique tmp name: two processes must never interleave on one tmp path
  // (review 2026-09-13, Q2).
  const tmp = `${target}.${process.pid}.${randomUUID()}.tmp`;
  writeFileSync(tmp, JSON.stringify(capped, null, 2), "utf8");
  renameSync(tmp, target);
  return capped;
}
/**
 * Best-effort variant for the retain loop: a failed marker write must not abort
 * the retain hook (the next turn re-offers the same delta).
 */
function tryUpdateSessionState(
  sessionKey: string,
  mutate: (state: AstinusSessionState) => AstinusSessionState
): void {
  try {
    updateSessionState(sessionKey, mutate);
  } catch (e) {
    debug(`[Hindsight Hook] session state write failed: ${e}`);
  }
}
/**
 * Append a committed range, capping the queue. On overflow the OLDEST ranges are
 * dropped, but never silently: the drop is logged at warn (review 2026-09-13, Q2).
 */
function appendPendingRange(
  queue: AstinusSessionState["pendingRetain"],
  range: AstinusSessionState["pendingRetain"][number]
): AstinusSessionState["pendingRetain"] {
  const next = [...queue, range];
  if (next.length <= ASTINUS_MAX_PENDING_TURNS) return next;
  const dropped = next.length - ASTINUS_MAX_PENDING_TURNS;
  log.warn(
    `[astinus-engine] pendingRetain overflow: dropping ${dropped} oldest committed range(s); retains are not draining`
  );
  return next.slice(-ASTINUS_MAX_PENDING_TURNS);
}
const DEFAULT_RECALL_TIMEOUT_MS = 10_000;

type SessionIdentityRecord = Pick<
  PluginHookAgentContext,
  "senderId" | "messageProvider" | "channelId"
>;
export type IdentitySkipReason =
  | {
      kind: "retryable";
      detail: "missing stable message provider" | "missing stable sender identity";
    }
  | { kind: "final"; detail: string };

const sessionIdentityBySession = new Map<string, SessionIdentityRecord>();
const skipHindsightTurnBySession = new Map<string, IdentitySkipReason>();
const documentSequenceBySession = new Map<string, number>();

// Random token minted once per host process and mixed into fallback (non-append)
// document ids. `documentSequenceBySession` lives only in memory, so it restarts
// at 1 on every host restart — and it is FIFO-capped at MAX_TRACKED_SESSIONS, so
// a busy host can evict a live session's counter and recycle its ids without any
// restart at all. Either way the replayed id hits an existing server-side
// document, and retain's default `update_mode: 'replace'` *deletes* that
// document's memories before reprocessing: months of history collapsed to a
// single restart cycle's worth of turns. (#3686)
let documentIdBootToken: string | null = null;

/** Per-process token that keeps fallback document ids unique across restarts. */
export function getDocumentIdBootToken(): string {
  if (!documentIdBootToken) {
    documentIdBootToken = randomUUID().replace(/-/g, "").slice(0, 8);
  }
  return documentIdBootToken;
}

// Cooldown + guard to prevent concurrent reinit attempts
let lastReinitAttempt = 0;
let isReinitInProgress = false;
const REINIT_COOLDOWN_MS = 30_000;

// Retain queue (both external-API and local-daemon mode)
let retainQueue: RetainQueue | null = null;
let retainQueueFlushTimer: ReturnType<typeof setInterval> | null = null;
let isFlushInProgress = false;
const DEFAULT_FLUSH_INTERVAL_MS = 60_000; // 1 min

/**
 * Open the JSONL retain queue and start its periodic flush timer.
 *
 * Never throws: without the queue a failed retain is dropped exactly as it was
 * before the queue existed, which is worth far less than taking the whole plugin
 * down over an unwritable state directory.
 */
function initRetainQueue(
  pluginConfig: PluginConfig,
  expectedGeneration: number,
  signal: globalThis.AbortSignal
): void {
  // service.start() can run again without an intervening stop() (gateway
  // reloads); don't leak the previous generation's timer onto the new one.
  if (retainQueueFlushTimer) {
    clearInterval(retainQueueFlushTimer);
    retainQueueFlushTimer = null;
  }
  try {
    const queueDir = pluginConfig.retainQueuePath
      ? dirname(pluginConfig.retainQueuePath)
      : join(homedir(), ".openclaw", "data");
    mkdirSync(queueDir, { recursive: true });
    const queuePath =
      pluginConfig.retainQueuePath || join(queueDir, "hindsight-retain-queue.jsonl");
    const queueFlushInterval = pluginConfig.retainQueueFlushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    const queueMaxAge = pluginConfig.retainQueueMaxAgeMs ?? -1;
    retainQueue = new RetainQueue({ filePath: queuePath, maxAgeMs: queueMaxAge });
    const pending = retainQueue.size();
    if (pending > 0) {
      log.info(`retain queue: ${pending} items pending from previous session, will flush shortly`);
    }
    debug(`[Hindsight] Retain queue initialized: ${queuePath}`);

    // Periodic flush timer
    if (queueFlushInterval > 0) {
      retainQueueFlushTimer = setInterval(() => {
        void flushRetainQueue(undefined, undefined, undefined, expectedGeneration, signal);
      }, queueFlushInterval);
      retainQueueFlushTimer.unref?.();
    }
  } catch (error) {
    retainQueue = null;
    log.warn(`could not initialize retain queue, continuing without it: ${error}`);
  }
}

/**
 * Attempt to flush pending retains from the queue.
 * Each item is sent exactly as it would have been originally — same bank, payload, metadata.
 */
export async function flushRetainQueue(
  queueOverride?: RetainQueue,
  clientOverride?: HindsightClient,
  capabilityOverride?: AsyncRetainOperationIdCapability,
  expectedGeneration = serviceGeneration,
  signal: globalThis.AbortSignal | undefined = serviceAbortController?.signal
): Promise<void> {
  const activeQueue = queueOverride ?? retainQueue;
  const activeClient = clientOverride ?? client;
  if (
    !activeQueue ||
    isFlushInProgress ||
    expectedGeneration !== serviceGeneration ||
    signal?.aborted
  )
    return;

  isFlushInProgress = true;
  let flushed = 0;
  let failed = 0;

  try {
    // Nothing queued means nothing to be idempotent about, so don't spend a
    // /version round trip: this runs on a timer *and* after every successful
    // retain, and probing an empty queue put a request behind every turn. The
    // capability is re-read here whenever there is actually work to replay,
    // which is the only moment it changes the outcome.
    const pending = activeQueue.size();
    if (pending === 0) return;
    const capability =
      capabilityOverride ?? (await refreshQueueOperationIdCapability(expectedGeneration, signal));
    if (expectedGeneration !== serviceGeneration || signal?.aborted) return;
    if (capability === "unknown") {
      // Held back on purpose: a replay without operation_id is the one path that
      // can duplicate durable memories. Warn rather than debug — if /version
      // stays unreachable the queue grows without ever draining, and that must
      // not be silent.
      log.warn(
        `retain queue flush deferred (${pending} queued): server operation-id capability unknown`
      );
      return;
    }
    if (!activeClient) return; // no client yet — can't flush

    // Cleanup expired items first
    activeQueue.cleanup();

    const items = activeQueue.peek(50);
    for (const item of items) {
      try {
        if (expectedGeneration !== serviceGeneration || signal?.aborted) return;
        const operationId =
          capability === "supported"
            ? activeQueue.ensureOperationId(item.id, randomUUID())
            : undefined;
        await activeClient.retain(item.bankId, item.content, {
          documentId: item.documentId,
          context: item.context,
          metadata: toStringMetadata(item.metadata),
          tags: item.tags,
          updateMode: item.updateMode,
          async: true,
          signal,
          ...(operationId ? { operationId } : {}),
        });

        if (expectedGeneration !== serviceGeneration || signal?.aborted) return;
        // Checkpoint each acknowledgement before the next network await. A
        // later abort must not replay already-accepted work on legacy servers.
        activeQueue.remove(item.id);
        flushed++;
      } catch {
        if (expectedGeneration !== serviceGeneration || signal?.aborted) return;
        // API still down — stop trying this batch
        failed++;
        break;
      }
    }

    if (expectedGeneration !== serviceGeneration || signal?.aborted) return;
    const remaining = activeQueue.size();
    if (flushed > 0) {
      log.info(
        `queue flush: ${flushed} queued retains delivered${remaining > 0 ? `, ${remaining} still pending` : ", queue empty"}`
      );
    } else if (failed > 0) {
      debug(`[Hindsight] Queue flush: API still unreachable, ${remaining} retains pending`);
    }
  } finally {
    isFlushInProgress = false;
  }
}

const DEFAULT_RECALL_PROMPT_PREAMBLE =
  "Relevant memories from past conversations (prioritize recent when conflicting). Only use memories that are directly useful to continue this conversation; ignore the rest:";

export function formatCurrentTimeForRecall(date = new Date()): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hours = String(date.getUTCHours()).padStart(2, "0");
  const minutes = String(date.getUTCMinutes()).padStart(2, "0");
  // Suffix with " UTC" so the LLM doesn't misread the timestamp as local
  // time and make wrong recency judgments. Mirrors the fix landed for the
  // Claude Code integration in #1568. (#1789)
  return `${year}-${month}-${day} ${hours}:${minutes} UTC`;
}

/**
 * Lazy re-initialization after startup failure.
 * Called by waitForReady when initPromise rejected but API may now be reachable.
 * Throttled to one attempt per 30s to avoid hammering a down service.
 * Only works if initialization was attempted at least once (isInitialized guard).
 */
async function lazyReinit(configOverride?: PluginConfig): Promise<void> {
  const now = Date.now();
  if (now - lastReinitAttempt < REINIT_COOLDOWN_MS || isReinitInProgress) {
    return;
  }

  const config = configOverride ?? currentPluginConfig;
  if (!config) {
    debug("[Hindsight] lazyReinit skipped - no plugin config available");
    return;
  }

  // Persist config if we only have it from the live hook registration path.
  currentPluginConfig = config;

  isReinitInProgress = true;
  lastReinitAttempt = now;
  const externalApi = detectExternalApi(config);
  if (!externalApi.apiUrl) {
    isReinitInProgress = false;
    return; // Only external API mode supports lazy reinit
  }

  debug("[Hindsight] Attempting lazy re-initialization...");
  try {
    await checkExternalApiHealth(externalApi.apiUrl, externalApi.apiToken);
    await detectAppendCapability(externalApi.apiUrl, externalApi.apiToken);

    const llmConfig = detectLLMConfig(config);
    clientOptions = buildClientOptions(llmConfig, config, externalApi);
    banksWithDefaultsApplied.clear();
    client = new HindsightClient(clientOptions);

    if (usesStaticBank(config)) {
      await ensureBankDefaultsApplied(getStaticBankId(config), config);
    }

    usingExternalApi = true;
    isInitialized = true;
    // Replace the rejected initPromise with a resolved one
    initPromise = Promise.resolve();
    debug("[Hindsight] ✓ Lazy re-initialization succeeded");
  } catch (error) {
    log.warn(
      `lazy re-init failed (retry in ${REINIT_COOLDOWN_MS / 1000}s): ${error instanceof Error ? error.message : error}`
    );
  } finally {
    isReinitInProgress = false;
  }
}

// Global access for hooks (Moltbot loads hooks separately)
if (typeof global !== "undefined") {
  (global as any).__hindsightClient = {
    getClient: () => client,
    waitForReady: async () => {
      if (isInitialized) {
        return;
      }
      // If initPromise is null, it means service.start() hasn't been called yet
      // (CLI mode, not gateway mode). Hooks should gracefully no-op.
      if (!initPromise) {
        if (currentPluginConfig) {
          log.warn(
            "waitForReady called before service.start() — attempting lazy initialization fallback"
          );
          await lazyReinit(currentPluginConfig);
          return;
        }
        log.warn(
          "waitForReady called before service.start() — hooks will no-op (expected in CLI mode)"
        );
        return;
      }
      try {
        await initPromise;
      } catch {
        // Init failed (e.g., health check timeout at startup).
        // Attempt lazy re-initialization so Hindsight recovers
        // once the API becomes reachable again.
        if (!isInitialized) {
          await lazyReinit();
        }
      }
    },
    /**
     * Get a bank-scoped client handle for a specific agent context.
     * Derives the bank ID from the context for per-channel isolation and
     * ensures the bank mission is set on first use.
     */
    getClientForContext: async (
      ctx: PluginHookAgentContext | undefined
    ): Promise<BankScopedClient | null> => {
      if (!client) return null;
      const config = currentPluginConfig || {};
      const bankId = usesStaticBank(config) ? getStaticBankId(config) : deriveBankId(ctx, config);
      const scoped = scopeClient(client, bankId);

      // Stamp configured defaults onto this bank on first use (recall or retain).
      await ensureBankDefaultsApplied(bankId, config);

      return scoped;
    },
    getPluginConfig: () => currentPluginConfig,
  };
}

// Default bank name (fallback when channel context not available)
const DEFAULT_BANK_NAME = "openclaw";

// Default granularity fields used by deriveBankId when not explicitly configured.
// This constant is shared between getPluginConfig (normalisation) and deriveBankId
// (fallback) so the skip-reason check and the bank-routing logic always agree.
const DEFAULT_DYNAMIC_BANK_GRANULARITY: Array<"agent" | "provider" | "channel" | "user"> = [
  "agent",
  "channel",
  "user",
];

// Throttle set: log an info-level skip message at most once per (sessionKey) per
// process lifetime so operators can discover silent retention/recall skips without
// flooding the log on every turn.
const loggedSkipSessions = new Set<string>();

function getConfiguredBankId(pluginConfig: PluginConfig): string | undefined {
  if (typeof pluginConfig.bankId !== "string") {
    return undefined;
  }

  const trimmed = pluginConfig.bankId.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function usesStaticBank(pluginConfig: PluginConfig): boolean {
  return pluginConfig.dynamicBankId === false;
}

function getDefaultBankId(pluginConfig: PluginConfig): string {
  return pluginConfig.bankIdPrefix
    ? `${pluginConfig.bankIdPrefix}-${DEFAULT_BANK_NAME}`
    : DEFAULT_BANK_NAME;
}

function getStaticBankId(pluginConfig: PluginConfig): string {
  const configuredBankId = getConfiguredBankId(pluginConfig);
  const baseBankId = configuredBankId || DEFAULT_BANK_NAME;
  return pluginConfig.bankIdPrefix ? `${pluginConfig.bankIdPrefix}-${baseBankId}` : baseBankId;
}

/**
 * Strip plugin-injected memory tags from content to prevent retain feedback loop.
 * Removes <hindsight_memories> and <relevant_memories> blocks that were injected
 * during before_prompt_build so they don't get re-stored into the memory bank.
 */
export function stripMemoryTags(content: string): string {
  content = content.replace(/<hindsight_memories>[\s\S]*?<\/hindsight_memories>/g, "");
  content = content.replace(/<relevant_memories>[\s\S]*?<\/relevant_memories>/g, "");
  return content;
}

/**
 * Extract per-message retain tag overrides from inline user content.
 *
 * Supported forms:
 * - <retain_tags>tag:a, tag:b</retain_tags>
 * - <hindsight_retain_tags>tag:a, tag:b</hindsight_retain_tags>
 */
export function extractInlineRetainTags(content: string): string[] {
  if (!content) return [];

  const tags: string[] = [];
  const blockRe = /<(?:hindsight_)?retain_tags>([\s\S]*?)<\/(?:hindsight_)?retain_tags>/gi;
  let match: RegExpExecArray | null;

  while ((match = blockRe.exec(content)) !== null) {
    const normalized = normalizeRetainTags(match[1]);
    for (const tag of normalized) {
      if (!tags.includes(tag)) {
        tags.push(tag);
      }
    }
  }

  return tags;
}

/**
 * Remove inline retain tag directives from message content before storing it.
 */
export function stripInlineRetainTags(content: string): string {
  if (!content) return content;
  return content.replace(
    /<(?:hindsight_)?retain_tags>[\s\S]*?<\/(?:hindsight_)?retain_tags>/gi,
    ""
  );
}

/**
 * Strip OpenClaw's inline timestamp prefix (e.g. "[Wed 2026-04-15 10:44 GMT+2] ")
 * from the start of user-facing text. We lift this into a structured `timestamp`
 * field on the retained message instead, so facts aren't polluted by a weekday
 * prefix that varies per message.
 */
const INLINE_TIMESTAMP_PREFIX_RE =
  /^\s*\[(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}(?::\d{2})?\s+(?:GMT|UTC)(?:[+\-]\d{1,2}(?::\d{2})?)?\]\s*/;

export function stripInlineTimestampPrefix(content: string): string {
  if (!content) return content;
  return content.replace(INLINE_TIMESTAMP_PREFIX_RE, "");
}

/**
 * Provenance marker OpenClaw appends to every injected inbound context header
 * since 2026.8.1 — e.g. `Conversation info: ⟦openclaw:ctx⟧`. Older hosts label
 * the same blocks `Conversation info (untrusted metadata):` instead. Both forms
 * are recognised: the plugin has to keep working against hosts on either side
 * of that change.
 */
const INBOUND_CONTEXT_MARKER = "⟦openclaw:ctx⟧";

/** Matches a header line in either the marker (2026.8.1+) or legacy form. */
const INBOUND_META_HEADER_RE = new RegExp(
  `^[^\\n]*(?:${INBOUND_CONTEXT_MARKER}|\\(untrusted metadata\\))[^\\n]*$`
);

/** True when `line` opens an OpenClaw-injected inbound metadata block. */
function isInboundMetaHeaderLine(line: string): boolean {
  return INBOUND_META_HEADER_RE.test(line.trim());
}

interface InboundMetaBlock {
  /** Index of the header line's first character. */
  start: number;
  /** Index just past the block (header + body), i.e. where normal text resumes. */
  end: number;
  /** Parsed body when the block is a ```json fence, otherwise undefined. */
  json?: unknown;
}

/**
 * Locate every OpenClaw-injected inbound metadata block in `text`.
 *
 * Mirrors the host's own stripper: a header line is followed either by a
 * ```json fence (block ends at the closing fence) or by free-form lines that
 * end at the first blank line. Keying on the header rather than on a fenced
 * payload is what makes marker-form blocks like `Chat history since last
 * reply: ⟦openclaw:ctx⟧` strippable too.
 */
function findInboundMetaBlocks(text: string): InboundMetaBlock[] {
  if (!text) return [];
  const lines = text.split("\n");
  // Byte offset of the start of each line, plus a terminator past the end.
  const offsets: number[] = [];
  let cursor = 0;
  for (const line of lines) {
    offsets.push(cursor);
    cursor += line.length + 1;
  }
  offsets.push(cursor);

  const blocks: InboundMetaBlock[] = [];
  for (let i = 0; i < lines.length; i++) {
    if (!isInboundMetaHeaderLine(lines[i])) continue;
    const start = offsets[i];
    let end: number;
    let json: unknown;
    if (lines[i + 1]?.trim() === "```json") {
      let close = i + 2;
      while (close < lines.length && lines[close].trim() !== "```") close++;
      if (close < lines.length) {
        try {
          json = JSON.parse(lines.slice(i + 2, close).join("\n"));
        } catch {
          // Leave `json` undefined; the block is still stripped.
        }
      }
      end = offsets[Math.min(close + 1, lines.length)];
      i = close;
    } else {
      let blank = i + 1;
      while (blank < lines.length && lines[blank].trim() !== "") blank++;
      end = offsets[Math.min(blank + 1, lines.length)];
      i = blank;
    }
    blocks.push({ start, end, json });
  }
  return blocks;
}

/**
 * Extract sender_id from OpenClaw's injected inbound metadata blocks.
 * Reads both "Conversation info" and "Sender" blocks, in either the 2026.8.1+
 * `⟦openclaw:ctx⟧` marker form or the legacy "(untrusted metadata)" form.
 * Returns the first sender_id / id string found, or undefined if none.
 */
export function extractSenderIdFromText(text: string): string | undefined {
  if (!text) return undefined;
  for (const block of findInboundMetaBlocks(text)) {
    const obj = block.json as { sender_id?: unknown; id?: unknown } | undefined;
    const id = obj?.sender_id ?? obj?.id;
    if (typeof id === "string" && id) return id;
  }
  return undefined;
}

/**
 * Strip OpenClaw sender/conversation metadata envelopes from message content.
 * These blocks are injected by OpenClaw but are noise for memory storage and recall.
 */
export function stripMetadataEnvelopes(content: string): string {
  if (!content) return content;
  const blocks = findInboundMetaBlocks(content);
  if (blocks.length > 0) {
    const parts: string[] = [];
    let cursor = 0;
    for (const block of blocks) {
      parts.push(content.slice(cursor, Math.max(cursor, block.start)));
      cursor = Math.max(cursor, block.end);
    }
    parts.push(content.slice(cursor));
    content = parts.join("");
  }
  // Drop the `---` fences that wrapped the legacy envelope form.
  content = content.replace(/^---\n/, "").replace(/\n---$/, "");
  return stripRuntimeEnvelope(content).trim();
}

const RUNTIME_MESSAGE_ID_LINE_RE = /^\[message_id:\s*(?:om|ou|oc)_[A-Za-z0-9_-]+\]$/i;
const RUNTIME_OPAQUE_ID_LINE_RE = /^(?:om|ou|oc)_[A-Za-z0-9_-]+$/i;
const RUNTIME_OPAQUE_SENDER_PREFIX_RE = /^\s*(?:om|ou|oc)_[A-Za-z0-9_-]+\s*:\s*/i;

// Opt-in display-name prefix stripping (#3070). Some channels prepend a human
// display name to the user text ("Alice: today weather?"), which pollutes both
// the recall query and the retained transcript (the name gets extracted as a
// fact). No payload field carries the display name, and a generic `Word:`
// heuristic would eat ordinary user text ("计划: 今天修 retain 污染"), so the
// pattern is operator-supplied. Unset = byte-identical to the old behaviour.
// One process-global compiled pattern, armed from getPluginConfig(): the host
// holds a single hindsight-openclaw config, so every session in the process
// shares it. Compile per config if that ever stops being true.
let senderPrefixRe: RegExp | undefined;
let senderPrefixSource: string | undefined;

/**
 * Set (or clear) the display-name prefix pattern stripped by
 * {@link stripRuntimeEnvelope}. The pattern is the name alternation only
 * (e.g. `Alice|Bob` or `[A-Za-z ]{1,20}`); the anchor, surrounding whitespace
 * and the `:` separator are supplied here. An invalid regex fails closed:
 * stripping stays off and nothing throws.
 */
export function configureSenderPrefixStripping(pattern: string | undefined): void {
  if (pattern === senderPrefixSource) return; // no recompile, and no repeat warn
  senderPrefixSource = pattern;
  if (!pattern) {
    senderPrefixRe = undefined;
    return;
  }
  try {
    senderPrefixRe = new RegExp(`^\\s*(?:${pattern})\\s*:\\s*`);
  } catch (error) {
    senderPrefixRe = undefined;
    log.warn(`ignoring invalid senderPrefixPattern ${JSON.stringify(pattern)}: ${error}`);
  }
}

/**
 * Strip inline OpenClaw/Feishu runtime headers that can appear before user text.
 * These identifiers are routing/runtime metadata, not semantic conversation content.
 */
export function stripRuntimeEnvelope(content: string): string {
  if (!content) return content;

  const lines = content.split(/\r?\n/);
  const kept = lines.filter((line) => {
    const trimmed = line.trim();
    return !RUNTIME_MESSAGE_ID_LINE_RE.test(trimmed) && !RUNTIME_OPAQUE_ID_LINE_RE.test(trimmed);
  });

  const stripped = kept.join("\n").replace(RUNTIME_OPAQUE_SENDER_PREFIX_RE, "");
  return senderPrefixRe ? stripped.replace(senderPrefixRe, "") : stripped;
}

/**
 * Extract a recall query from a hook event's rawMessage or prompt.
 *
 * Prefers rawMessage (clean user text). Falls back to prompt, stripping
 * envelope formatting (System: lines, [Channel ...] headers, [from: X] footers).
 *
 * Returns null when no usable query (< 5 chars) can be extracted.
 */
export function extractRecallQuery(
  rawMessage: string | undefined,
  prompt: string | undefined
): string | null {
  // Reject known metadata/system message patterns — these are not user queries
  const METADATA_PATTERNS = [
    /^\s*conversation info\s*\(untrusted metadata\)/i,
    /^\s*\(untrusted metadata\)/i,
    /^\s*system:/i,
  ];
  const isMetadata = (s: string) =>
    METADATA_PATTERNS.some((p) => p.test(s)) ||
    // 2026.8.1+ marker form: a leftover header line is metadata, not a query.
    isInboundMetaHeaderLine(s.split("\n")[0] ?? "");

  let recallQuery = rawMessage;
  // Strip sender metadata envelope before any checks
  if (recallQuery) {
    recallQuery = stripRuntimeEnvelope(
      stripInlineTimestampPrefix(stripMetadataEnvelopes(recallQuery))
    );
  }
  if (
    !recallQuery ||
    typeof recallQuery !== "string" ||
    recallQuery.trim().length < 5 ||
    isMetadata(recallQuery)
  ) {
    recallQuery = prompt;
    // Strip metadata envelopes from prompt too, then check if anything useful remains
    if (recallQuery) {
      recallQuery = stripRuntimeEnvelope(
        stripInlineTimestampPrefix(stripMetadataEnvelopes(recallQuery))
      );
    }
    if (!recallQuery || recallQuery.length < 5) {
      return null;
    }

    // Strip envelope-formatted prompts from any channel
    let cleaned = recallQuery;

    // Remove leading "System: ..." lines (from prependSystemEvents)
    cleaned = cleaned.replace(/^(?:System:.*\n)+\n?/, "");

    // Remove session abort hint
    cleaned = cleaned.replace(/^Note: The previous agent run was aborted[^\n]*\n\n/, "");

    // Extract message after [ChannelName ...] envelope header
    const envelopeMatch = cleaned.match(/\[[A-Z][A-Za-z]*(?:\s[^\]]+)?\]\s*([\s\S]+)$/);
    if (envelopeMatch) {
      cleaned = envelopeMatch[1];
    }

    // Remove trailing [from: SenderName] metadata (group chats)
    cleaned = cleaned.replace(/\n\[from:[^\]]*\]\s*$/, "");

    // Strip metadata envelopes again after channel envelope extraction, in case
    // the metadata block appeared after the [ChannelName] header
    cleaned = stripRuntimeEnvelope(stripInlineTimestampPrefix(stripMetadataEnvelopes(cleaned)));

    recallQuery = cleaned.trim() || recallQuery;
  }

  const trimmed = recallQuery.trim();
  if (trimmed.length < 5 || isMetadata(trimmed)) return null;
  return trimmed;
}

export function composeRecallQuery(
  latestQuery: string,
  messages: any[] | undefined,
  recallContextTurns: number,
  recallRoles: Array<"user" | "assistant" | "system" | "tool"> = ["user", "assistant"]
): string {
  const latest = latestQuery.trim();
  if (recallContextTurns <= 1 || !Array.isArray(messages) || messages.length === 0) {
    return latest;
  }

  const allowedRoles = new Set(recallRoles);
  const contextualMessages = sliceLastTurnsByUserBoundary(messages, recallContextTurns);
  const contextLines = contextualMessages
    .map((msg: any) => {
      const role = msg?.role;
      if (!allowedRoles.has(role)) {
        return null;
      }

      let content = "";
      if (typeof msg?.content === "string") {
        content = msg.content;
      } else if (Array.isArray(msg?.content)) {
        content = msg.content
          .filter((block: any) => block?.type === "text" && typeof block?.text === "string")
          .map((block: any) => block.text)
          .join("\n");
      }

      content = stripMemoryTags(content).trim();
      content = stripMetadataEnvelopes(content);
      content = stripRuntimeEnvelope(stripInlineTimestampPrefix(content));
      if (!content) {
        return null;
      }
      if (role === "user" && content === latest) {
        return null;
      }
      return `${role}: ${content}`;
    })
    .filter((line: string | null): line is string => Boolean(line));

  if (contextLines.length === 0) {
    return latest;
  }

  return ["Prior context:", contextLines.join("\n"), latest].join("\n\n");
}

export function truncateRecallQuery(query: string, latestQuery: string, maxChars: number): string {
  if (maxChars <= 0) {
    return query;
  }

  const latest = latestQuery.trim();
  if (query.length <= maxChars) {
    return query;
  }

  const latestOnly = latest.length <= maxChars ? latest : latest.slice(0, maxChars);

  if (!query.includes("Prior context:")) {
    return latestOnly;
  }

  // New order: Prior context at top, latest user message at bottom.
  // Truncate by dropping oldest context lines first to preserve the suffix.
  const contextMarker = "Prior context:\n\n";
  const markerIndex = query.indexOf(contextMarker);
  if (markerIndex === -1) {
    return latestOnly;
  }

  const suffixMarker = "\n\n" + latest;
  const suffixIndex = query.lastIndexOf(suffixMarker);
  if (suffixIndex === -1) {
    return latestOnly;
  }

  const suffix = query.slice(suffixIndex); // \n\n<latest>
  if (suffix.length >= maxChars) {
    return latestOnly;
  }

  const contextBody = query.slice(markerIndex + contextMarker.length, suffixIndex);
  const contextLines = contextBody.split("\n").filter(Boolean);
  const keptContextLines: string[] = [];

  // Add context lines from newest (bottom) to oldest (top), stopping when we exceed maxChars
  for (let i = contextLines.length - 1; i >= 0; i--) {
    keptContextLines.unshift(contextLines[i]);
    const candidate = `${contextMarker}${keptContextLines.join("\n")}${suffix}`;
    if (candidate.length > maxChars) {
      keptContextLines.shift();
      break;
    }
  }

  if (keptContextLines.length > 0) {
    return `${contextMarker}${keptContextLines.join("\n")}${suffix}`;
  }

  return latestOnly;
}

/**
 * Parse the OpenClaw sessionKey to extract context fields.
 * Format: "agent:{agentId}:{provider}:{channelType}:{channelId}[:{extra}]"
 * Example: "agent:c0der:telegram:group:-1003825475854:topic:42"
 */
// Some OpenClaw hook contexts populate `ctx.channelId` with the provider name
// (e.g. "discord") instead of the actual channel ID. Treat those as missing so
// we fall through to the sessionKey-derived channel. See issue #854.
const PROVIDER_CHANNEL_ID_TOKENS = new Set([
  "discord",
  "telegram",
  "slack",
  "matrix",
  "whatsapp",
  "signal",
  "messenger",
  "sms",
  "email",
  "web",
  "cli",
]);

function sanitizeChannelId(channelId: string | undefined, provider?: string): string | undefined {
  if (!channelId) return undefined;
  if (provider && channelId === provider) return undefined;
  if (PROVIDER_CHANNEL_ID_TOKENS.has(channelId.toLowerCase())) return undefined;
  return channelId;
}

export interface ParsedSessionKey {
  agentId?: string;
  provider?: string;
  channel?: string;
}

export function parseSessionKey(sessionKey: string): ParsedSessionKey {
  const parts = sessionKey.split(":");
  if (parts[0] !== "agent") return {};
  if (parts.length === 3 && parts[2] === "main") {
    return {
      agentId: parts[1],
      provider: "main",
      channel: "main",
    };
  }
  // OpenClaw's Control UI creates `agent:<id>:dashboard:<opaque-id>` keys.
  // Recover only the agent identity: "dashboard" is a session namespace, not
  // the live message provider used for dynamic bank routing.
  if (parts.length === 4 && parts[2] === "dashboard") {
    return {
      agentId: parts[1],
    };
  }
  if (parts.length >= 4 && ["cron", "heartbeat", "subagent"].includes(parts[2])) {
    return {
      agentId: parts[1],
      provider: parts[2],
      channel: parts.slice(3).join(":"),
    };
  }
  if (parts.length < 5) return {};
  // parts[1] = agentId, parts[2] = provider, parts[3] = channelType, parts[4..] = channelId + extras
  return {
    agentId: parts[1],
    provider: parts[2],
    // Rejoin from channelType onward as the channel identifier (e.g. "group:-1003825475854:topic:42")
    channel: parts.slice(3).join(":"),
  };
}

export function extractTelegramDirectSenderId(channelId: string | undefined): string | undefined {
  if (typeof channelId !== "string") return undefined;
  const match = channelId.match(/^direct:([^:]+)$/);
  return match?.[1];
}

export function resolveSessionIdentity(
  ctx: PluginHookAgentContext | undefined
): PluginHookAgentContext | undefined {
  if (!ctx) return undefined;

  const sessionParsed = ctx.sessionKey ? parseSessionKey(ctx.sessionKey) : {};
  const messageProvider = ctx.messageProvider || sessionParsed.provider;
  const channelId = ctx.channelId || sessionParsed.channel;
  // direct:<id> channel ids carry the user id for any provider (telegram, msteams, …).
  const senderId = ctx.senderId || extractTelegramDirectSenderId(channelId);

  return {
    ...ctx,
    agentId: ctx.agentId || sessionParsed.agentId,
    messageProvider,
    channelId,
    senderId,
  };
}

function retryableSkipReason(
  detail: "missing stable message provider" | "missing stable sender identity"
): IdentitySkipReason {
  return { kind: "retryable", detail };
}

function finalSkipReason(detail: string): IdentitySkipReason {
  return { kind: "final", detail };
}

function formatIdentitySkipReason(reason: IdentitySkipReason | undefined): string | undefined {
  return reason?.detail;
}

function isRetryableIdentitySkipReason(reason: IdentitySkipReason | undefined): boolean {
  return reason?.kind === "retryable";
}

/**
 * Log an identity-skip event at info level, throttled to once per session key
 * per process lifetime. This makes silent skips visible to operators without
 * flooding the log on every turn.
 */
function logSkipOnce(
  operation: "recall" | "retain" | "dispatch",
  sessionKey: string | undefined,
  reason: IdentitySkipReason
): void {
  if (!sessionKey) return;
  const cacheKey = `${operation}:${sessionKey}`;
  if (loggedSkipSessions.has(cacheKey)) return;
  loggedSkipSessions.add(cacheKey);
  const hint =
    reason.kind === "final"
      ? ". If unexpected, set dynamicBankGranularity to ['agent','channel','user'] or use static banking (dynamicBankId: false + bankId: '<name>')"
      : "";
  log.info(`Skipping ${operation} on session '${sessionKey}': ${reason.detail}${hint}`);
}

function cacheSessionIdentity(
  sessionKey: string | undefined,
  resolvedCtx: PluginHookAgentContext | undefined
): void {
  if (!sessionKey || !resolvedCtx) return;
  if (!resolvedCtx.messageProvider && !resolvedCtx.channelId && !resolvedCtx.senderId) return;

  setCappedMapValue(sessionIdentityBySession, sessionKey, {
    senderId: resolvedCtx.senderId,
    messageProvider: resolvedCtx.messageProvider,
    channelId: resolvedCtx.channelId,
  });
}

export interface ResolveAndCacheIdentityOptions {
  sessionKey?: string;
  ctx?: PluginHookAgentContext;
  senderIdHint?: string;
  dispatchChannel?: string;
  pluginConfig?: PluginConfig;
}

export function resolveAndCacheIdentity(options: ResolveAndCacheIdentityOptions): {
  effectiveCtx: PluginHookAgentContext | undefined;
  resolvedCtx: PluginHookAgentContext | undefined;
  skipReason?: IdentitySkipReason;
} {
  const sessionKey = options.sessionKey ?? options.ctx?.sessionKey;
  const parsedSession = sessionKey ? parseSessionKey(sessionKey) : {};
  const cachedIdentity = sessionKey ? sessionIdentityBySession.get(sessionKey) : undefined;
  const baseCtx =
    options.ctx || (sessionKey ? ({ sessionKey } as PluginHookAgentContext) : undefined);
  // "main" is the synthetic provider produced by parseSessionKey for default
  // `agent:<id>:main` sessions — the *real* dispatch surface (telegram,
  // webchat, qqbot, …) is whatever the dispatcher provides. Treat it as if
  // the session key carried no provider so the dispatchChannel flows through
  // as the effective surface for downstream identity resolution. (#1541)
  const sessionProvider =
    parsedSession.provider && parsedSession.provider !== "main"
      ? parsedSession.provider
      : undefined;
  const effectiveCtx =
    baseCtx || cachedIdentity || options.senderIdHint || options.dispatchChannel || sessionKey
      ? {
          ...baseCtx,
          sessionKey: baseCtx?.sessionKey || sessionKey,
          agentId: baseCtx?.agentId || parsedSession.agentId,
          messageProvider: baseCtx?.messageProvider ?? cachedIdentity?.messageProvider,
          channelId: baseCtx?.channelId ?? cachedIdentity?.channelId,
          senderId: baseCtx?.senderId || cachedIdentity?.senderId || options.senderIdHint,
        }
      : undefined;
  const resolvedCtx = resolveSessionIdentity(
    effectiveCtx
      ? {
          ...effectiveCtx,
          messageProvider:
            effectiveCtx.messageProvider ?? sessionProvider ?? options.dispatchChannel,
          channelId: effectiveCtx.channelId ?? parsedSession.channel,
        }
      : undefined
  );

  // The dispatch-surface gate guards against retaining a turn into a bank
  // keyed by the *wrong* channel when the user has explicitly opted into
  // channel-scoped routing. Only fire when:
  //   - The session key carries a real (non-synthetic) provider.
  //   - The live dispatch surface actually differs from it.
  //   - Bank routing depends on the dispatch surface (granularity includes
  //     "channel" or "provider") AND the user has not pinned a static bank.
  // Without these guards, default `agent:<id>:main` sessions dispatched via
  // a real surface (telegram, webchat, …) and statically-banked setups were
  // silently skipped on every turn. (#1541)
  const granularity =
    options.pluginConfig?.dynamicBankGranularity ?? DEFAULT_DYNAMIC_BANK_GRANULARITY;
  const bankRoutingDependsOnSurface =
    granularity.includes("channel") || granularity.includes("provider");
  const staticBanking =
    options.pluginConfig?.dynamicBankId === false &&
    typeof options.pluginConfig?.bankId === "string" &&
    options.pluginConfig.bankId.length > 0;

  if (
    sessionProvider &&
    options.dispatchChannel &&
    sessionProvider !== options.dispatchChannel &&
    bankRoutingDependsOnSurface &&
    !staticBanking
  ) {
    const skipReason = finalSkipReason(
      `dispatch surface ${options.dispatchChannel} does not match session provider ${sessionProvider}`
    );
    if (sessionKey) {
      setCappedMapValue(skipHindsightTurnBySession, sessionKey, skipReason);
    }
    return { effectiveCtx, resolvedCtx, skipReason };
  }

  const { resolvedCtx: identityCtx, reason: skipReason } = getIdentitySkipReason(
    resolvedCtx,
    options.pluginConfig
  );
  cacheSessionIdentity(sessionKey, identityCtx);
  if (sessionKey) {
    if (skipReason) {
      setCappedMapValue(skipHindsightTurnBySession, sessionKey, skipReason);
    } else {
      skipHindsightTurnBySession.delete(sessionKey);
    }
  }

  return { effectiveCtx, resolvedCtx: identityCtx, skipReason };
}

export function getIdentitySkipReason(
  ctx: PluginHookAgentContext | undefined,
  pluginConfig?: PluginConfig
): { resolvedCtx: PluginHookAgentContext | undefined; reason?: IdentitySkipReason } {
  const resolvedCtx = resolveSessionIdentity(ctx);
  const sessionKey = resolvedCtx?.sessionKey;
  // The "internal main" / "operational provider main" / "anonymous sender" filters
  // exist to keep the default multi-tenant bank from being polluted by CLI/main
  // sessions that lack a stable identity. They should NOT fire when the user has
  // explicitly opted into a routing scheme that expects those sessions:
  //   - dynamicBankGranularity includes 'agent' (the default) → each agent
  //     (including 'main') gets its own bank
  //   - dynamicBankId === false with a configured bankId → user pinned a single
  //     named bank and wants every session retained into it
  // When dynamicBankGranularity is unset, the default is ["agent","channel","user"]
  // which includes "agent", so agentBanking defaults to true to match deriveBankId.
  const agentBanking = pluginConfig?.dynamicBankGranularity?.includes("agent") ?? true;
  const staticBanking =
    pluginConfig?.dynamicBankId === false &&
    typeof pluginConfig?.bankId === "string" &&
    pluginConfig.bankId.length > 0;
  const allowCliSessions = agentBanking || staticBanking;

  if (typeof sessionKey === "string") {
    if (/^agent:[^:]+:(cron|heartbeat|subagent):/.test(sessionKey)) {
      return { resolvedCtx, reason: finalSkipReason(`operational session ${sessionKey}`) };
    }
    if (!allowCliSessions && /^agent:[^:]+:main$/.test(sessionKey)) {
      return { resolvedCtx, reason: finalSkipReason(`internal main session ${sessionKey}`) };
    }
    if (/^temp:/.test(sessionKey)) {
      return { resolvedCtx, reason: finalSkipReason(`ephemeral temp session ${sessionKey}`) };
    }
  }

  const operationalProviders = allowCliSessions
    ? ["cron", "heartbeat", "subagent"]
    : ["cron", "heartbeat", "subagent", "main"];
  if (resolvedCtx?.messageProvider && operationalProviders.includes(resolvedCtx.messageProvider)) {
    return {
      resolvedCtx,
      reason: finalSkipReason(`operational provider ${resolvedCtx.messageProvider}`),
    };
  }
  if (!resolvedCtx?.messageProvider || resolvedCtx.messageProvider === "unknown") {
    return { resolvedCtx, reason: retryableSkipReason("missing stable message provider") };
  }
  if (!resolvedCtx?.senderId || resolvedCtx.senderId === "anonymous") {
    if (allowCliSessions && resolvedCtx?.agentId) {
      resolvedCtx.senderId = `agent-user:${resolvedCtx.agentId}`;
    } else {
      return { resolvedCtx, reason: retryableSkipReason("missing stable sender identity") };
    }
  }
  if (
    resolvedCtx.messageProvider === "telegram" &&
    typeof resolvedCtx.channelId === "string" &&
    resolvedCtx.channelId.startsWith("direct:")
  ) {
    const directSenderId = extractTelegramDirectSenderId(resolvedCtx.channelId);
    if (!directSenderId || directSenderId !== resolvedCtx.senderId) {
      return {
        resolvedCtx,
        reason: finalSkipReason(
          `telegram direct identity mismatch (${resolvedCtx.channelId} vs ${resolvedCtx.senderId})`
        ),
      };
    }
  }

  return { resolvedCtx, reason: undefined };
}

export function isEphemeralOperationalText(text: string | undefined): boolean {
  if (!text || typeof text !== "string") return false;

  const normalized = text
    .replace(/\[role:\s*[^\]]+\]\s*/gi, "")
    .replace(/\[[a-z]+:end\]\s*/gi, "")
    .trim();

  // These prefixes are OpenClaw-generated operational/session-bootstrap strings,
  // not user-authored content, so they should not create recall/retain entries.
  return [
    /^A new session was started via \/(?:new|reset)\./i,
    /^Based on this conversation, generate a short 1-2/i,
    /^This (?:script|task|job|workflow) updates .* index/i,
  ].some((pattern) => pattern.test(normalized));
}

function setCappedMapValue<K, V>(map: Map<K, V>, key: K, value: V): void {
  // FIFO cap, not LRU: updating an existing key keeps its original insertion order.
  map.set(key, value);
  if (map.size > MAX_TRACKED_SESSIONS) {
    const oldest = map.keys().next().value;
    if (oldest) map.delete(oldest);
  }
}

/**
 * Derive a bank ID from the agent context.
 * Uses configurable dynamicBankGranularity to determine bank segmentation.
 * Falls back to default bank when context is unavailable.
 */
export function deriveBankId(
  ctx: PluginHookAgentContext | undefined,
  pluginConfig: PluginConfig
): string {
  if (pluginConfig.dynamicBankId === false) {
    return getStaticBankId(pluginConfig);
  }

  // When no context is available, fall back to the static default bank.
  if (!ctx) {
    return getDefaultBankId(pluginConfig);
  }

  const resolvedCtx = resolveSessionIdentity(ctx);
  const fields = pluginConfig.dynamicBankGranularity?.length
    ? pluginConfig.dynamicBankGranularity
    : DEFAULT_DYNAMIC_BANK_GRANULARITY;

  // Validate field names at runtime — typos silently produce 'unknown' segments
  const validFields = new Set(["agent", "channel", "user", "provider"]);
  for (const f of fields) {
    if (!validFields.has(f)) {
      log.warn(
        `unknown dynamicBankGranularity field "${f}" — will resolve to "unknown". Valid: agent, channel, user, provider`
      );
    }
  }

  // Parse sessionKey as fallback when direct context fields are missing
  const sessionParsed = resolvedCtx?.sessionKey ? parseSessionKey(resolvedCtx.sessionKey) : {};

  // Warn when 'user' is in active fields but senderId is missing — bank ID will contain "anonymous"
  if (fields.includes("user") && resolvedCtx && !resolvedCtx.senderId) {
    debug(
      '[Hindsight] senderId not available in context — bank ID will use "anonymous". Ensure your OpenClaw provider passes senderId.'
    );
  }

  const fieldMap: Record<string, string> = {
    agent: resolvedCtx?.agentId || sessionParsed.agentId || "default",
    channel:
      sanitizeChannelId(
        resolvedCtx?.channelId,
        resolvedCtx?.messageProvider || sessionParsed.provider
      ) ||
      sessionParsed.channel ||
      "unknown",
    user: resolvedCtx?.senderId || "anonymous",
    provider: resolvedCtx?.messageProvider || sessionParsed.provider || "unknown",
  };

  const baseBankId = fields.map((f) => encodeURIComponent(fieldMap[f] || "unknown")).join("::");

  return pluginConfig.bankIdPrefix ? `${pluginConfig.bankIdPrefix}-${baseBankId}` : baseBankId;
}

function usesUserScopedBanking(pluginConfig: PluginConfig): boolean {
  if (usesStaticBank(pluginConfig)) {
    return false;
  }
  const granularity = pluginConfig.dynamicBankGranularity?.length
    ? pluginConfig.dynamicBankGranularity
    : DEFAULT_DYNAMIC_BANK_GRANULARITY;
  return granularity.includes("user");
}

export interface KnowledgeToolBankResolution {
  bankId: string;
  resolvedCtx: PluginHookAgentContext | undefined;
  identityError?: string;
}

/**
 * Resolve the Hindsight bank for knowledge tools using the same identity path as
 * auto-recall/retain. When user-scoped dynamic banking is enabled, unresolved
 * identity must not silently route to the shared default or anonymous bank.
 */
export function resolveBankIdForKnowledgeTools(
  toolCtx: PluginToolContext,
  pluginConfig: PluginConfig
): KnowledgeToolBankResolution {
  if (usesStaticBank(pluginConfig)) {
    return { bankId: getStaticBankId(pluginConfig), resolvedCtx: undefined };
  }

  const hookCtx: PluginHookAgentContext = {
    agentId: toolCtx.agentId,
    sessionKey: toolCtx.sessionKey,
    workspaceDir: toolCtx.workspaceDir,
  };

  const { resolvedCtx, skipReason } = resolveAndCacheIdentity({
    sessionKey: toolCtx.sessionKey,
    ctx: hookCtx,
    pluginConfig,
  });
  const { reason: identityReason } = getIdentitySkipReason(resolvedCtx, pluginConfig);
  const effectiveSkip = skipReason ?? identityReason;
  const bankId = deriveBankId(resolvedCtx, pluginConfig);

  if (usesUserScopedBanking(pluginConfig)) {
    const userSegment = resolvedCtx?.senderId || "anonymous";
    if (effectiveSkip) {
      return {
        bankId,
        resolvedCtx,
        identityError:
          `Hindsight knowledge tools skipped: ${formatIdentitySkipReason(effectiveSkip)}. ` +
          "Knowledge tools use the same per-user memory bank as auto-recall/retain.",
      };
    }
    if (userSegment === "anonymous") {
      return {
        bankId,
        resolvedCtx,
        identityError:
          "Hindsight knowledge tools skipped: missing stable sender identity. " +
          "Knowledge tools use the same per-user memory bank as auto-recall/retain.",
      };
    }
    if (bankId === getDefaultBankId(pluginConfig)) {
      return {
        bankId,
        resolvedCtx,
        identityError:
          "Hindsight knowledge tools skipped: could not resolve per-user memory bank. " +
          "Knowledge tools use the same per-user memory bank as auto-recall/retain.",
      };
    }
  }

  return { bankId, resolvedCtx };
}

/**
 * Render the event window a memory carries. `mentioned_at` says when the fact
 * was stated; `occurred_start`/`occurred_end` say when the event itself
 * happened, which is what the agent needs to order past events against each
 * other. Either bound can be absent, so each case gets its own wording rather
 * than an open-ended range the model has to guess at.
 */
function formatOccurredWindow(
  start: string | null | undefined,
  end: string | null | undefined
): string {
  if (start && end) {
    return start === end ? ` [occurred: ${start}]` : ` [occurred: ${start} → ${end}]`;
  }
  if (start) return ` [occurred from: ${start}]`;
  if (end) return ` [occurred until: ${end}]`;
  return "";
}

export function formatMemories(
  results: MemoryResult[],
  opts?: { markAsEarlierRecords?: boolean }
): string {
  if (!results || results.length === 0) return "";
  const earlierRecord = opts?.markAsEarlierRecords === true;
  return results
    .map((r) => {
      const type = r.type ? ` [${r.type}]` : "";
      const date = r.mentioned_at ? ` (${r.mentioned_at})` : "";
      const occurred = formatOccurredWindow(r.occurred_start, r.occurred_end);
      const doc = r.document_id ? ` [doc:${r.document_id}]` : "";
      // Injected-recall marking (plan v4 hard rule): a recalled item is a record
      // of something said EARLIER, never the current state of the thing. The
      // label rides the existing per-item date; only callers rendering into a
      // live prompt opt in, so other consumers keep the plain format.
      const staleness = earlierRecord ? " [earlier record — not current state; verify before acting]" : "";
      return `- ${r.text}${type}${date}${occurred}${doc}${staleness}`;
    })
    .join("\n\n");
}

// Providers that authenticate via OAuth or run locally — no API key needed.
const NO_KEY_REQUIRED_PROVIDERS = new Set([
  "ollama",
  "openai-codex",
  "claude-code",
  "github-copilot",
]);

export function detectLLMConfig(pluginConfig?: PluginConfig): {
  provider?: string;
  apiKey?: string;
  model?: string;
  baseUrl?: string;
  source: string;
} {
  // External API mode: the daemon handles LLM credentials, plugin doesn't need them.
  const externalApiCheck = detectExternalApi(pluginConfig);
  if (externalApiCheck.apiUrl) {
    return {
      provider: undefined,
      apiKey: undefined,
      model: undefined,
      baseUrl: undefined,
      source: "external-api-mode-no-llm",
    };
  }

  const provider = pluginConfig?.llmProvider;
  if (!provider) {
    throw new Error(
      `No LLM provider configured for the Hindsight memory plugin.\n\n` +
        `Set the provider via 'openclaw config set':\n` +
        `  openclaw config set plugins.entries.hindsight-openclaw.config.llmProvider openai\n\n` +
        `For providers that need an API key, configure it as a SecretRef so the value\n` +
        `is read from an env var (or file/exec source) at runtime instead of stored in plain text:\n` +
        `  openclaw config set plugins.entries.hindsight-openclaw.config.llmApiKey \\\n` +
        `      --ref-source env --ref-provider default --ref-id OPENAI_API_KEY\n\n` +
        `Providers that don't need an API key: ${[...NO_KEY_REQUIRED_PROVIDERS].join(", ")}.\n` +
        `Or point the plugin at an external Hindsight API by setting hindsightApiUrl instead.`
    );
  }

  const apiKey = pluginConfig?.llmApiKey ?? "";
  if (!apiKey && !NO_KEY_REQUIRED_PROVIDERS.has(provider)) {
    throw new Error(
      `llmProvider is set to "${provider}" but llmApiKey is empty.\n\n` +
        `Configure it via 'openclaw config set' as a SecretRef:\n` +
        `  openclaw config set plugins.entries.hindsight-openclaw.config.llmApiKey \\\n` +
        `      --ref-source env --ref-provider default --ref-id OPENAI_API_KEY`
    );
  }

  return {
    provider,
    apiKey,
    model: pluginConfig?.llmModel,
    baseUrl: pluginConfig?.llmBaseUrl,
    source: "plugin config",
  };
}

/**
 * Detect external Hindsight API configuration from plugin config.
 */
export function detectExternalApi(pluginConfig?: PluginConfig): {
  apiUrl: string | null;
  apiToken: string | null;
} {
  return {
    apiUrl: pluginConfig?.hindsightApiUrl ?? null,
    apiToken: pluginConfig?.hindsightApiToken ?? null,
  };
}

/**
 * The Hindsight API this plugin is currently talking to, whichever mode it is
 * in: the configured external API, or the local daemon we spawned ourselves.
 *
 * Capability probing must not care which one it is — the embedded daemon serves
 * the same `/version` endpoint as any other Hindsight API. Treating local-daemon
 * mode as a special case is precisely what left `supportsUpdateModeAppend` stuck
 * at `false` there, silently downgrading every retain to a per-turn document id.
 * (#3686)
 */
function getActiveApiEndpoint(): { apiUrl: string | null; apiToken: string | null } {
  const externalApi = detectExternalApi(currentPluginConfig ?? undefined);
  if (externalApi.apiUrl) return externalApi;
  return { apiUrl: hindsightServer?.getBaseUrl() ?? null, apiToken: null };
}

/**
 * Build HindsightClientOptions for the generated hindsight-client. In
 * external-API mode we use the configured URL/token; in local daemon mode
 * the caller overrides with the daemon's base URL after start().
 * The llmConfig parameter is currently only consumed by the daemon manager
 * (via env vars); it's kept on the client builder signature so callers
 * don't need to branch and so future features can forward it.
 */
export function buildClientOptions(
  _llmConfig: { provider?: string; apiKey?: string; model?: string },
  _pluginCfg: PluginConfig,
  externalApi: { apiUrl: string | null; apiToken: string | null }
): HindsightClientOptions {
  return {
    baseUrl: externalApi.apiUrl ?? "",
    apiKey: externalApi.apiToken ?? undefined,
  };
}

/**
 * Health check for external Hindsight API.
 * Retries up to 3 times with 2s delay — container DNS may not be ready on first boot.
 */
/**
 * Compare two semver-shaped strings ("0.5.0", "0.4.22"). Returns true when
 * `actual >= minimum`. Tolerates pre-release suffixes (treats them as the
 * same major.minor.patch as the bare version — good enough for capability
 * gating).
 */
export function meetsMinimumVersion(actual: string, minimum: string): boolean {
  const parse = (v: string): number[] =>
    v
      .split("-")[0]
      .split(".")
      .map((part) => Number.parseInt(part, 10))
      .map((n) => (Number.isFinite(n) ? n : 0));
  const a = parse(actual);
  const m = parse(minimum);
  for (let i = 0; i < Math.max(a.length, m.length); i++) {
    const av = a[i] ?? 0;
    const mv = m[i] ?? 0;
    if (av > mv) return true;
    if (av < mv) return false;
  }
  return true;
}

export interface HindsightApiCapabilities {
  version: string;
  storeDocumentText: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function parseHindsightApiCapabilities(payload: unknown): HindsightApiCapabilities | null {
  if (!isRecord(payload) || typeof payload.api_version !== "string") {
    return null;
  }

  let storeDocumentText = true;
  if ("features" in payload) {
    const features = payload.features;
    storeDocumentText = isRecord(features) && features.store_document_text === true;
  }

  return {
    version: payload.api_version,
    storeDocumentText,
  };
}

export function supportsAppendFromCapabilities(
  capabilities: HindsightApiCapabilities | null
): boolean {
  return (
    capabilities !== null &&
    capabilities.storeDocumentText &&
    meetsMinimumVersion(capabilities.version, MIN_VERSION_FOR_UPDATE_MODE_APPEND)
  );
}

export function supportsAsyncRetainOperationIdFromCapabilities(
  capabilities: HindsightApiCapabilities | null
): boolean {
  return asyncRetainOperationIdCapabilityFromCapabilities(capabilities) === "supported";
}

export function asyncRetainOperationIdCapabilityFromCapabilities(
  capabilities: HindsightApiCapabilities | null
): AsyncRetainOperationIdCapability {
  if (capabilities === null) {
    return "unknown";
  }
  const match = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.exec(capabilities.version);
  // JavaScript's `$` can match before a final line terminator, so require the
  // matched text to consume the complete payload as well as canonical digits.
  if (!match || match[0] !== capabilities.version) {
    return "unknown";
  }
  return meetsMinimumVersion(capabilities.version, MIN_VERSION_FOR_ASYNC_RETAIN_OPERATION_ID)
    ? "supported"
    : "unsupported";
}

/**
 * Allocate the identity before the first asynchronous request so a failed
 * acknowledgement and every durable-queue replay refer to the same server operation.
 */
export function createAsyncRetainOperationId(): string {
  return randomUUID();
}

/**
 * Probe `<apiUrl>/version` once at service.start to learn the running
 * Hindsight API capabilities. Returns `null` (treated as "no append support")
 * if the endpoint is unreachable or returns malformed payload — conservative
 * fallback path is the right call when we can't be sure.
 */
async function fetchHindsightApiCapabilities(
  apiUrl: string,
  apiToken?: string | null,
  serviceSignal?: globalThis.AbortSignal
): Promise<HindsightApiCapabilities | null> {
  const versionUrl = `${apiUrl.replace(/\/$/, "")}/version`;
  try {
    const headers: Record<string, string> = { "User-Agent": USER_AGENT };
    if (apiToken) headers["Authorization"] = `Bearer ${apiToken}`;
    const timeoutSignal = AbortSignal.timeout(5000);
    const response = await fetch(versionUrl, {
      signal: serviceSignal ? AbortSignal.any([serviceSignal, timeoutSignal]) : timeoutSignal,
      headers,
    });
    if (!response.ok) {
      debug(`[Hindsight] /version returned HTTP ${response.status}; assuming legacy`);
      return null;
    }
    const data = await response.json();
    const capabilities = parseHindsightApiCapabilities(data);
    if (!capabilities) {
      debug(`[Hindsight] /version payload missing api_version; assuming legacy`);
    }
    return capabilities;
  } catch (error) {
    debug(`[Hindsight] /version probe failed: ${String(error)}; assuming legacy`);
    return null;
  }
}

export async function refreshAsyncRetainOperationIdCapability(
  apiUrl: string,
  apiToken?: string | null,
  expectedGeneration = serviceGeneration,
  signal: globalThis.AbortSignal | undefined = serviceAbortController?.signal
): Promise<AsyncRetainOperationIdCapability> {
  const capabilities = await fetchHindsightApiCapabilities(apiUrl, apiToken, signal);
  const capability = asyncRetainOperationIdCapabilityFromCapabilities(capabilities);
  if (expectedGeneration !== serviceGeneration || signal?.aborted) return "unknown";
  asyncRetainOperationIdCapability = capability;
  return capability;
}

async function refreshQueueOperationIdCapability(
  expectedGeneration = serviceGeneration,
  signal: globalThis.AbortSignal | undefined = serviceAbortController?.signal
): Promise<AsyncRetainOperationIdCapability> {
  if (expectedGeneration !== serviceGeneration || signal?.aborted) return "unknown";
  if (!currentPluginConfig) {
    asyncRetainOperationIdCapability = "unknown";
    return "unknown";
  }
  const endpoint = getActiveApiEndpoint();
  if (!endpoint.apiUrl) {
    asyncRetainOperationIdCapability = "unknown";
    return "unknown";
  }
  return refreshAsyncRetainOperationIdCapability(
    endpoint.apiUrl,
    endpoint.apiToken,
    expectedGeneration,
    signal
  );
}

/**
 * Probe `/version` and update the module-level append and async-operation-id
 * capability flags. Logs a one-time WARN block when the append API is
 * older than 0.5.0 or cannot store document text — without
 * `update_mode: 'append'`, every retain on the same session id silently
 * overwrites prior turns server-side, and append itself requires stored
 * document text.
 *
 * Called wherever the plugin (re)connects to an API — external *and* local
 * daemon. It used to hang off the `checkExternalApiHealth` call sites only,
 * which is why local-daemon mode never probed at all and silently retained
 * with per-turn document ids. (#3686)
 */
async function detectAppendCapability(
  apiUrl: string,
  apiToken?: string | null,
  expectedGeneration = serviceGeneration,
  signal: globalThis.AbortSignal | undefined = serviceAbortController?.signal
): Promise<void> {
  const capabilities = await fetchHindsightApiCapabilities(apiUrl, apiToken, signal);
  if (expectedGeneration !== serviceGeneration || signal?.aborted) return;
  const supported = supportsAppendFromCapabilities(capabilities);
  asyncRetainOperationIdCapability = asyncRetainOperationIdCapabilityFromCapabilities(capabilities);
  const transitionedToUnsupported = supportsUpdateModeAppend && !supported;
  const firstProbe = !appendCapabilityProbed;
  appendCapabilityProbed = true;
  supportsUpdateModeAppend = supported;
  if (supported && capabilities) {
    debug(
      `[Hindsight] API version ${capabilities.version} supports update_mode=append with stored document text`
    );
    return;
  }
  // Warn on the first probe when unsupported, and on any transition from
  // supported -> unsupported. Stay silent on subsequent re-probes that
  // confirm the same unsupported state.
  if (!firstProbe && !transitionedToUnsupported) return;
  const version = capabilities?.version ?? null;
  const reason =
    capabilities !== null &&
    meetsMinimumVersion(capabilities.version, MIN_VERSION_FOR_UPDATE_MODE_APPEND) &&
    !capabilities.storeDocumentText
      ? `reports version "${capabilities.version}" but has features.store_document_text disabled`
      : `reports version "${version ?? "unknown"}", which is older than ${MIN_VERSION_FOR_UPDATE_MODE_APPEND}`;
  log.warn(
    `[Hindsight] ⚠️  API at ${apiUrl} ${reason}. ` +
      `Falling back to per-turn document ids — each retain becomes its own document instead of accumulating into one per-session document. ` +
      `Enable document text storage on Hindsight ${MIN_VERSION_FOR_UPDATE_MODE_APPEND} or newer to use session-scoped retention with update_mode=append.`
  );
}

async function checkExternalApiHealth(apiUrl: string, apiToken?: string | null): Promise<void> {
  const healthUrl = `${apiUrl.replace(/\/$/, "")}/health`;
  const maxRetries = 3;
  const retryDelay = 2000;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      debug(
        `[Hindsight] Checking external API health at ${healthUrl}... (attempt ${attempt}/${maxRetries})`
      );
      const headers: Record<string, string> = { "User-Agent": USER_AGENT };
      if (apiToken) {
        headers["Authorization"] = `Bearer ${apiToken}`;
      }
      const response = await fetch(healthUrl, { signal: AbortSignal.timeout(10000), headers });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      const data = (await response.json()) as { status?: string };
      debug(`[Hindsight] External API health: ${JSON.stringify(data)}`);
      return;
    } catch (error) {
      if (attempt < maxRetries) {
        debug(`[Hindsight] Health check attempt ${attempt} failed, retrying in ${retryDelay}ms...`);
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
      } else {
        throw new Error(`Cannot connect to external Hindsight API at ${apiUrl}: ${error}`, {
          cause: error,
        });
      }
    }
  }
}

export function normalizeRetainTags(value: unknown): string[] {
  if (value == null) return [];

  const rawItems = Array.isArray(value) ? value : typeof value === "string" ? value.split(",") : [];

  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const item of rawItems) {
    if (typeof item !== "string") continue;
    const tag = item.trim();
    if (!tag || seen.has(tag)) continue;
    seen.add(tag);
    normalized.push(tag);
  }
  return normalized;
}

export function getPluginConfig(api: MoltbotPluginAPI): PluginConfig {
  const config = api.config.plugins?.entries?.["hindsight-openclaw"]?.config || {};

  const senderPrefixPattern =
    typeof config.senderPrefixPattern === "string" && config.senderPrefixPattern.trim().length > 0
      ? config.senderPrefixPattern.trim()
      : undefined;
  // Arm the shared stripper used by every recall/retain text path (#3070).
  configureSenderPrefixStripping(senderPrefixPattern);

  // No default fallback for missions: if the user doesn't set one, the plugin
  // does not stamp anything. This lets per-bank missions written via the API
  // (PATCH /banks/{id}) survive gateway restarts. (#1270)
  return {
    bankMission:
      typeof config.bankMission === "string" && config.bankMission.length > 0
        ? config.bankMission
        : undefined,
    retainMission:
      typeof config.retainMission === "string" && config.retainMission.length > 0
        ? config.retainMission
        : undefined,
    observationsMission:
      typeof config.observationsMission === "string" && config.observationsMission.length > 0
        ? config.observationsMission
        : undefined,
    retainExtractionMode: normalizeRetainExtractionMode(config.retainExtractionMode),
    enableObservations:
      typeof config.enableObservations === "boolean" ? config.enableObservations : undefined,
    enableAutoConsolidation:
      typeof config.enableAutoConsolidation === "boolean"
        ? config.enableAutoConsolidation
        : undefined,
    dispositionSkepticism: normalizeDispositionTrait(config.dispositionSkepticism),
    dispositionLiteralism: normalizeDispositionTrait(config.dispositionLiteralism),
    dispositionEmpathy: normalizeDispositionTrait(config.dispositionEmpathy),
    entityLabels: normalizeEntityLabels(config.entityLabels),
    embedPort: config.embedPort || 0,
    daemonIdleTimeout: config.daemonIdleTimeout !== undefined ? config.daemonIdleTimeout : 0,
    embedVersion: config.embedVersion || "latest",
    embedPackagePath: config.embedPackagePath,
    llmProvider: config.llmProvider,
    llmModel: config.llmModel,
    llmApiKey: config.llmApiKey,
    llmBaseUrl: config.llmBaseUrl,
    hindsightApiUrl: config.hindsightApiUrl,
    hindsightApiToken: config.hindsightApiToken,
    apiPort: config.apiPort || 9077,
    // Dynamic bank ID options (default: enabled)
    dynamicBankId: config.dynamicBankId !== false,
    bankId:
      typeof config.bankId === "string" && config.bankId.trim().length > 0
        ? config.bankId.trim()
        : undefined,
    bankIdPrefix: config.bankIdPrefix,
    retainTags: normalizeRetainTags(config.retainTags),
    retainSource:
      typeof config.retainSource === "string" && config.retainSource.trim().length > 0
        ? config.retainSource.trim()
        : undefined,
    retainContext:
      typeof config.retainContext === "string" && config.retainContext.trim().length > 0
        ? config.retainContext.trim()
        : DEFAULT_RETAIN_CONTEXT,
    excludeProviders: Array.isArray(config.excludeProviders)
      ? Array.from(
          new Set([
            "heartbeat",
            ...config.excludeProviders.filter(
              (provider): provider is string => typeof provider === "string"
            ),
          ])
        )
      : ["heartbeat"],
    autoRecall: config.autoRecall !== false, // Default: true (on) — backward compatible
    dynamicBankGranularity: Array.isArray(config.dynamicBankGranularity)
      ? config.dynamicBankGranularity
      : DEFAULT_DYNAMIC_BANK_GRANULARITY,
    autoRetain: config.autoRetain !== false, // Default: true
    retainRoles: Array.isArray(config.retainRoles) ? config.retainRoles : undefined,
    retainFormat: config.retainFormat === "text" ? "text" : "json",
    retainToolCalls: config.retainToolCalls !== false,
    recallBudget: config.recallBudget || "mid",
    recallMaxTokens: config.recallMaxTokens || 1024,
    recallTypes: Array.isArray(config.recallTypes) ? config.recallTypes : ["observation"],
    preferObservations: config.preferObservations === true, // Default: false — backward compatible
    recallMinScores: config.recallMinScores,
    recallRoles: Array.isArray(config.recallRoles) ? config.recallRoles : ["user", "assistant"],
    retainEveryNTurns:
      typeof config.retainEveryNTurns === "number" && config.retainEveryNTurns >= 1
        ? config.retainEveryNTurns
        : 1,
    retainOverlapTurns:
      typeof config.retainOverlapTurns === "number" && config.retainOverlapTurns >= 0
        ? config.retainOverlapTurns
        : 0,
    recallTopK: typeof config.recallTopK === "number" ? config.recallTopK : undefined,
    recallTopicFilter: config.recallTopicFilter === true,
    recallContextTurns:
      typeof config.recallContextTurns === "number" && config.recallContextTurns >= 1
        ? config.recallContextTurns
        : 1,
    recallMaxQueryChars:
      typeof config.recallMaxQueryChars === "number" && config.recallMaxQueryChars >= 1
        ? config.recallMaxQueryChars
        : 800,
    recallPromptPreamble:
      typeof config.recallPromptPreamble === "string" &&
      config.recallPromptPreamble.trim().length > 0
        ? config.recallPromptPreamble
        : DEFAULT_RECALL_PROMPT_PREAMBLE,
    recallInjectionPosition:
      typeof config.recallInjectionPosition === "string" &&
      ["prepend", "append", "user"].includes(config.recallInjectionPosition)
        ? (config.recallInjectionPosition as PluginConfig["recallInjectionPosition"])
        : "user",
    recallTimeoutMs:
      typeof config.recallTimeoutMs === "number" && config.recallTimeoutMs >= 1000
        ? config.recallTimeoutMs
        : undefined,
    ignoreSessionPatterns: Array.isArray(config.ignoreSessionPatterns)
      ? config.ignoreSessionPatterns
      : [],
    statelessSessionPatterns: Array.isArray(config.statelessSessionPatterns)
      ? config.statelessSessionPatterns
      : [],
    skipRetainSessionPatterns: (() => {
      const raw = Array.isArray(config.skipRetainSessionPatterns)
        ? config.skipRetainSessionPatterns
        : [];
      // Convert bare tokens to glob patterns: "heartbeat" -> "**:heartbeat**"
      // Pass through patterns that already contain glob wildcards
      return raw.map((p: string) => (p.includes("*") ? p : `**:${p}**`));
    })(),
    skipStatelessSessions: config.skipStatelessSessions !== false,
    retainQualityGate: config.retainQualityGate === true,
    debug: config.debug ?? false,
    debugPerfTiming: config.debugPerfTiming === true,
    // Retain queue: kept off the strict whitelist before — user values were
    // silently dropped before queue init read them. (#1443)
    retainQueuePath:
      typeof config.retainQueuePath === "string" && config.retainQueuePath.trim().length > 0
        ? config.retainQueuePath
        : undefined,
    retainQueueMaxAgeMs:
      typeof config.retainQueueMaxAgeMs === "number" ? config.retainQueueMaxAgeMs : undefined,
    retainQueueFlushIntervalMs:
      typeof config.retainQueueFlushIntervalMs === "number" && config.retainQueueFlushIntervalMs > 0
        ? config.retainQueueFlushIntervalMs
        : undefined,
    retainNonBlocking: config.retainNonBlocking === true,
    enableKnowledgeTools: config.enableKnowledgeTools === true,
    senderPrefixPattern,
  };
}

// classifyTurn: lightweight regex-based turn classifier for the retention quality gate.
// Returns activity type and detected topics. Runs in <1ms, no LLM call.
export function classifyTurn(transcript: string): { activity: string; topics: string[] } {
  const sample = transcript.substring(0, 2000).toLowerCase();
  // Activity classification
  let activity = "general";
  if (
    /\b(hello|hi|hey|thanks|thank you|good morning|good evening|good night|bye)\b/.test(
      sample
    ) &&
    transcript.length < 500
  ) {
    activity = "chitchat";
  } else if (
    /\b(remember|recall|forget|memory|memories|retain|purge)\b/.test(sample)
  ) {
    activity = "memory-meta";
  } else if (
    /\b(edit|update|change|add|set|configure|patch|tweak)\b[\s\S]{0,40}\b(file|config|setting|variable)\b/.test(
      sample
    )
  ) {
    activity = "config-edit";
  } else if (/\b(fix|bug|error|issue|broken|crash|fail)\b/.test(sample)) {
    activity = "debugging";
  } else if (/\b(build|deploy|test|run|install|compile)\b/.test(sample)) {
    activity = "development";
  } else if (/\b(analyze|research|search|find|lookup|investigate)\b/.test(sample)) {
    activity = "research";
  }
  // Topic extraction — generic categories only; deployment-specific vocabulary
  // belongs in a downstream classifier, not this upstream-facing helper.
  const topics: string[] = [];
  if (/\b(agent|subagent|session|spawn)\b/.test(sample)) topics.push("agents");
  if (/\b(skill|workflow|automation)\b/.test(sample)) topics.push("skills");
  if (/\b(model|llm|gpt|claude)\b/.test(sample)) topics.push("models");
  if (/\b(gateway|config|restart|service)\b/.test(sample)) topics.push("infrastructure");
  if (/\b(task|todo|plan|priority)\b/.test(sample)) topics.push("planning");
  if (/\b(code|file|function|class|module)\b/.test(sample)) topics.push("code");
  return { activity, topics };
}

/**
 * Normalize a classifyTurn topic label into a stable bank tag (plan v4 item 3).
 * Lowercase, collapse non-alphanumerics to single dashes, trim the edges.
 * Returns null for an empty result so callers can drop it instead of emitting
 * a bare `topic:` tag.
 */
export function topicTag(label: string): string | null {
  const slug = String(label)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return slug ? `topic:${slug}` : null;
}

/** Map a classifyTurn topics[] to a de-duplicated `topic:<slug>` tag list. */
export function topicTags(labels: unknown): string[] {
  if (!Array.isArray(labels)) return [];
  const out = new Set<string>();
  for (const label of labels) {
    if (typeof label !== "string") continue;
    const tag = topicTag(label);
    if (tag) out.add(tag);
  }
  return [...out];
}

// Registration guard: WeakSet keyed by api instance to prevent double-registration
// on the same api object while allowing fresh registration on new api objects.
// Does not reintroduce issue #1029 because WeakSet.has() checks object identity,
// not a module-level boolean.
// ---------------------------------------------------------------------------
// Astinus context engine (skeleton - plan build step 2, review Gate 2)
//
// Registers the `astinus` context engine with the host. This skeleton is
// DELIBERATELY inert: it does NOT declare the durable-turn semantics
// (transcriptSemantics + commitTurn), so the host keeps using the legacy
// context path for real turns until that contract lands. Nothing here changes
// model context or the transcript, and the slot stays on legacy by default.
//
// What it proves now: registration works and the interface compiles.
// What it does once the slot is switched on AND commitTurn lands:
// `ingest` builds the per-session statement<->topic mapping, and `assemble`
// logs the prune plan it would apply - the dry-run inside the real surface.
// Kill switch: plugins.slots.contextEngine = "legacy".
// ENGINE ID = PLUGIN ID ("hindsight-openclaw"), deliberately. The host reads
// plugins.slots.contextEngine two ways: the plugin loader treats it as the owning
// PLUGIN id (and `plugins enable` auto-writes the plugin id into the slot for any
// kind: "context-engine" plugin), while resolveContextEngine looks the same string
// up as a registered ENGINE id. A different engine id ("astinus", 2026-09-12/13)
// therefore resolved as "not registered", was quarantined, and fell back to legacy
// silently. Log prefix and inject dir keep the astinus name; the id must not.
// ---------------------------------------------------------------------------

type AstinusTopicEntry = {
  topics: Set<string>;
  statements: number;
  lastMessageAt: number;
};

const astinusSessionMapping = new Map<string, AstinusTopicEntry>();
const ASTINUS_MAPPING_MAX_SESSIONS = 2000;

/**
 * Marks index — the fallback assemble reads when the ledger cannot be opened
 * (plan §6 failure policy: "on failure it uses the in-memory marks index from
 * the last successful read, or no marks at all on a cold start"). Refilled by
 * every successful ledger read.
 */
const astinusMarksIndex = new Map<string, unknown[]>();
const ASTINUS_MARKS_INDEX_MAX_SESSIONS = 2000;

function astinusMessageText(message: unknown): string {
  const content = (message as { content?: unknown } | null | undefined)?.content;
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .map((part) =>
        part && typeof part === "object" && typeof (part as { text?: unknown }).text === "string"
          ? ((part as { text: string }).text)
          : ""
      )
      .filter(Boolean)
      .join("\n");
  }
  return "";
}

function astinusEstimateTokens(messages: unknown[]): number {
  // Rough char/4 heuristic. A stand-in until the engine owns real accounting.
  let chars = 0;
  for (const m of messages) chars += astinusMessageText(m).length + 16;
  return Math.ceil(chars / 4);
}

/** The engine id; must equal the plugin id (see the note above the section). */
const ASTINUS_ENGINE_ID = "hindsight-openclaw";

function registerAstinusContextEngine(api: MoltbotPluginAPI): void {
  const register = (api as { registerContextEngine?: (id: string, factory: () => unknown) => void })
    .registerContextEngine;
  if (typeof register !== "function") {
    log.info("[astinus-engine] host does not expose registerContextEngine; skipping");
    return;
  }
  try {
    register(ASTINUS_ENGINE_ID, () => ({
      info: {
        id: ASTINUS_ENGINE_ID,
        name: "Astinus Context Engine",
        version: "0.2.0-durable-turn",
        // ownsCompaction false: compaction stays with the runtime.
        ownsCompaction: false,
        // Durable-turn contract (Gate 2 decision, 2026-09-12). Declaring the
        // fence + idempotency lets the host hand the engine the accepted turn
        // and lets commitTurn own the durable statement<->topic mapping.
        transcriptSemantics: {
          currentTurnFence: "before-current-turn-entry-v1",
          turnAdvancementIdempotency: "atomic-idempotent-v1",
        },
        // acceptedHostParams intentionally OMITTED until the host's field names
        // are read from src/context-engine/ - a wrong name silently drops a
        // field (review 2026-09-12, Gate 2).
      },
      async ingest(params: {
        sessionKey?: string;
        sessionId?: string;
        message?: unknown;
        isHeartbeat?: boolean;
      }): Promise<{ ingested: boolean }> {
        if (params.isHeartbeat) return { ingested: false };
        const key = params.sessionKey ?? params.sessionId;
        // No key -> skip: a shared "unknown" bucket would merge every session
        // into one mapping (review 2026-09-12, Gate 2 minor).
        if (!key) return { ingested: false };
        const text = astinusMessageText(params.message);
        if (!text) return { ingested: false };
        const entry =
          astinusSessionMapping.get(key) ??
          ({ topics: new Set<string>(), statements: 0, lastMessageAt: 0 } as AstinusTopicEntry);
        for (const tag of topicTags(classifyTurn(text).topics)) entry.topics.add(tag);
        entry.statements += 1;
        entry.lastMessageAt = Date.now();
        astinusSessionMapping.set(key, entry);
        if (astinusSessionMapping.size > ASTINUS_MAPPING_MAX_SESSIONS) {
          const oldest = astinusSessionMapping.keys().next().value;
          if (oldest !== undefined) astinusSessionMapping.delete(oldest);
        }
        return { ingested: true };
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
        const list = Array.isArray(params.messages) ? params.messages : [];
        const estimatedTokens = astinusEstimateTokens(list);
        const key = params.sessionKey ?? params.sessionId;
        // The durable state file is the SOURCE for the mapping; the in-memory
        // Map is only a cache (empty after a restart until ingest refills it) -
        // review 2026-09-13, Q5.5.
        const state = key ? loadSessionState(key) : null;
        const topics = state ? Object.keys(state.topics) : [];
        const statements = state
          ? Object.values(state.topics).reduce((n, t) => n + (t?.statements ?? 0), 0)
          : 0;
        // Read the model's marks from the ledger (plan §8). Best-effort: on any
        // failure fall back to the cached index, or to no marks on a cold start
        // (§6 failure policy). Still a pass-through — application is the next
        // step; this only surfaces what would be applied.
        let marks: unknown[] = key ? astinusMarksIndex.get(key) ?? [] : [];
        let marksSource = "cached";
        if (key) {
          const res = readMarksCached(key, (m) => log.warn(m));
          if (res.ok) {
            marks = res.marks;
            astinusMarksIndex.set(key, marks);
            if (astinusMarksIndex.size > ASTINUS_MARKS_INDEX_MAX_SESSIONS) {
              const oldest = astinusMarksIndex.keys().next().value;
              if (oldest !== undefined) astinusMarksIndex.delete(oldest);
            }
            marksSource = "ledger";
          } else if (res.reason !== "not-open") {
            log.warn(
              `[astinus-engine] assemble: marks unavailable (${res.reason}); using cached index (${marks.length})`
            );
          }
        }
        const marksApplied = marks.filter(
          (m) => (m as { applied_at?: unknown } | null)?.applied_at != null
        ).length;
        // Dry-run: report the plan. Pass-through - the view is unchanged.
        // debug (not info): this fires every turn of every session (Q5.3).
        debug(
          `[astinus-engine] assemble dry-run: ${list.length} msgs, ~${estimatedTokens} tok` +
            (params.tokenBudget ? ` / budget ${params.tokenBudget}` : "") +
            `, topics=[${topics.join(", ")}], statements=${statements}` +
            `, marks=${marks.length} (${marksApplied} applied, src=${marksSource})` +
            ` - pass-through (no selection applied)`
        );
        // A pass-through that trims nothing must report preassembly_may_overflow,
        // not "assembled", so the host keeps its pre-prompt overflow safeguard
        // when the slot goes live (review 2026-09-12, Gate 2). The char/4
        // estimate only feeds the host's compaction threshold, and the host's own
        // precheck still runs, so an under-estimate is safe.
        // Apply the marks to the VIEW (plan §8 + the Sept-12 cache-hit ruling). Invariants are
        // enforced in code inside applyMarks: never the first user message, tool pairs travel
        // together, nothing in the preserved tail, the pressure floor, and the 10-turn debounce
        // unless pressure is critical. Any failure degrades to "less pruned", never an error.
        const agentId = key && key.startsWith("agent:") ? key.split(":")[1] : "";
        const hostDbPath = agentId
          ? `${WORKSPACE_ROOT}\\agents\\${agentId}\\agent\\openclaw-agent.sqlite`
          : "";
        let marksInEffect = 0;
        let admittedIds: Array<number | unknown> = [];
        let outList = list;
        let pressure = 0;
        if (key && hostDbPath && Array.isArray(marks) && typeof params.sessionId === "string" && params.sessionId) {
          try {
            const res = applyMarks(list, marks as LedgerMark[], {
              sessionKey: key,
              hostDbPath,
              // B1: transcript rows are keyed by the HOST session UUID, never the session key.
              sessionId: params.sessionId,
              tokenBudget: params.tokenBudget ?? 0,
              // monotonic counter (lastCommittedSeq) — committedKeys is capped and stops advancing
              turnCounter: state ? state.lastCommittedSeq : 0,
            });
            outList = res.messages;
            marksInEffect = res.marksInEffect;
            admittedIds = res.admitted.map((m) => m.mark_id);
            pressure = res.tokensBefore > 0 && (params.tokenBudget ?? 0) > 0
              ? res.tokensBefore / (params.tokenBudget as number)
              : 0;
            if (admittedIds.length > 0) {
              markMarksApplied(
                key,
                admittedIds,
                state?.lastGenerationSeen ?? "",
                res.tokensBefore,
                res.tokensAfter
              );
            }
            if (res.changed || marksInEffect > 0) {
              log.info(
                `astinus view: ${res.messages.length} msgs, ~${res.tokensAfter} tok (from ${res.tokensBefore}; ${marksInEffect} marks in effect, ${admittedIds.length} admitted; pressure=${pressure.toFixed(2)})`
              );
            } else if (res.skipped.length > 0) {
              const line = `[astinus-engine] assemble: ${res.skipped.length} mark(s) held - ${res.skipped[0]}`;
              // Boundary case made visible (Claude's second pass): marks exist and are in
              // effect, but none mapped to this pass's view (post-compaction, post-/new, or
              // all inside the preserved tail) - info, not debug, so status.py can see it.
              if (marksInEffect > 0) log.info(line + ` (${marksInEffect} in effect, none applied)`);
              else debug(line);
            }
          } catch (e) {
            log.warn(`[astinus-engine] assemble: mark application failed: ${(e as Error).message}`);
          }
        }
        // B2/B4: authority is "assembled" whenever ANY mark is in effect (admitted marks are
        // re-applied every turn — they ARE the view); the estimate must then be host-shaped,
        // because it is no longer backstopped by the host's precheck.
        const authority = marksInEffect > 0 ? "assembled" : "preassembly_may_overflow";
        const finalEstimate = marksInEffect > 0 ? estimateTokensHost(outList) : estimatedTokens;
        // v4.2 (Claude, verified in the host): under "preassembly_may_overflow" the host's
        // precheck takes the LARGER of the assembled view and the unwindowed transcript
        // (preemptive-compaction.ts:405, attempt-history.ts:660), so a pruned view can never
        // lower the compaction decision. The moment marks actually apply, authority must be
        // "assembled" — and the estimate must then be honest, because it is no longer
        // backstopped. Compaction-boundary invariants are structural here: a summary message
        // carries no transcript seq, so the identity join never marks it, and marks below the
        // boundary match no in-view message and are skipped.
        return {
          messages: outList,
          estimatedTokens: finalEstimate,
          promptAuthority: authority as "assembled" | "preassembly_may_overflow",
        };
      },
      // INCIDENT 2026-09-17 00:21 (main desk wedged at 1.31M tokens): with ownsCompaction=false the
      // host STILL calls the active engine's compact() for /compact and provider overflow
      // recovery (docs/concepts/context-engine.md "ownsCompaction: false or unset"). Returning
      // "compacted: false" here made every overflow recovery on a served desk fail
      // ("auto-compaction failed for ...: astinus skeleton: compaction delegated to the
      // runtime"), so the session could never shrink and cycled through provider errors. The
      // documented implementation for a non-owning engine is to hand the request to the
      // runtime's built-in compaction via delegateCompactionToRuntime (plugin-sdk/core, line
      // 382 of that doc) - the same bridge the legacy engine uses. Loaded lazily so a resolver
      // failure degrades to a logged refusal instead of breaking plugin load.
      async compact(params: unknown): Promise<unknown> {
        try {
          const sdkSpecifier = "openclaw/plugin-sdk/core";
          const sdk = (await import(sdkSpecifier)) as {
            delegateCompactionToRuntime?: (p: unknown) => Promise<unknown>;
          };
          if (typeof sdk.delegateCompactionToRuntime !== "function") {
            throw new Error("delegateCompactionToRuntime not exported by openclaw/plugin-sdk/core");
          }
          const result = await sdk.delegateCompactionToRuntime(params);
          log.info("[astinus-engine] compact: delegated to the runtime's built-in compaction");
          return result;
        } catch (e) {
          log.warn(
            `[astinus-engine] compact: runtime delegation failed (${(e as Error)?.message ?? e}); ` +
              "returning compacted=false - the host will report auto-compaction failed"
          );
          return {
            ok: false,
            compacted: false,
            reason: `astinus: runtime compaction delegation failed: ${(e as Error)?.message ?? e}`,
          };
        }
      },
      // Durable-turn commit (Gate 2 decision): ONE atomic, idempotent write keyed
      // by advancementKey. Persists the turn's statement<->topic mapping into the
      // shared per-session state document and records the terminal anchor's
      // rawSeq + generation and the admission..terminal range - the durable seq
      // source the retain marker needs. THROWS on any failure so the host retries
      // the same key; it must never report "committed" for a turn it did not
      // persist (review 2026-09-13, Q1.2/Q1.3).
      async commitTurn(params: {
        advancementKey?: string;
        messages?: unknown[];
        sessionKey?: string;
        sessionId?: string;
        admission?: { rawSeq?: number; agentId?: string; storePath?: string };
        terminal?: { rawSeq?: number; generation?: string };
        isHeartbeat?: boolean;
      }): Promise<{ status: "committed" | "duplicate" }> {
        const key = params.sessionKey ?? params.sessionId;
        const advancementKey =
          typeof params.advancementKey === "string" ? params.advancementKey : "";
        if (!key || !advancementKey) {
          // The host contract requires both; a compliant host cannot reach here.
          throw new Error("astinus commitTurn: missing session key or advancementKey");
        }
        if (loadSessionState(key).committedKeys.includes(advancementKey)) {
          return { status: "duplicate" };
        }
        const messages = Array.isArray(params.messages) ? params.messages : [];
        const heartbeat = params.isHeartbeat === true;
        const startSeq =
          typeof params.admission?.rawSeq === "number" ? params.admission.rawSeq : null;
        const endSeq = typeof params.terminal?.rawSeq === "number" ? params.terminal.rawSeq : null;
        const generation =
          typeof params.terminal?.generation === "string" ? params.terminal.generation : null;
        // F13: capture the PREVIOUS turn's positions before this commit touches anything, so the
        // in-process check below compares N-1 rather than the turn it just wrote (which could
        // never lag by construction, and could only report the failure the first branch already
        // reports).
        const prevStateSeq = loadSessionState(key).lastCommittedSeq;
        const prevLedgerSeq = readMaxTurnSeq(key);

        // 1. Ledger write FIRST - best-effort, never throws (R1). The state-document write
        //    below remains the sole acknowledgement and the only thing that may throw.
        const ledgerResult = insertCommittedTurn(
          {
            sessionKey: key,
            agentId:
              typeof params.admission?.agentId === "string" ? params.admission.agentId : "",
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
        );
        const now = Date.now();
        // Only conversational roles feed the mapping and the pending-retain
        // queue - tool output is noise and would bloat the state file (Q1.7).
        const conversational = messages
          .filter((m) => {
            const role = (m as { role?: string } | null | undefined)?.role;
            return role === "user" || role === "assistant";
          })
          .map((m) => ({
            role: (m as { role: string }).role,
            content: astinusMessageText(m),
          }))
          .filter((m) => m.content.length > 0);
        updateSessionState(key, (s) => {
          const topics = heartbeat ? s.topics : { ...s.topics };
          if (!heartbeat) {
            for (const m of conversational) {
              const text = astinusMessageText(m);
              if (!text) continue;
              for (const tag of topicTags(classifyTurn(text).topics)) {
                const slug = tag.startsWith("topic:") ? tag.slice(6) : tag;
                const entry =
                  topics[slug] ?? { firstSeenAt: now, lastSeenAt: now, statements: 0 };
                entry.statements += 1;
                entry.lastSeenAt = now;
                topics[slug] = entry;
              }
            }
          }
          return {
            ...s,
            topics,
            committedKeys: [...s.committedKeys, advancementKey],
            lastCommittedSeq: endSeq ?? s.lastCommittedSeq,
            lastCommittedRange:
              startSeq !== null && endSeq !== null
                ? { start: startSeq, end: endSeq }
                : s.lastCommittedRange,
            lastGenerationSeen: generation ?? s.lastGenerationSeen,
            lastCommittedAt: now,
            lastLedgerFailed: !ledgerResult.ok,
            // Option (a): record the accepted turn for the retain loop to drain.
            // Cleared on a successful retain.
            pendingRetain:
              heartbeat || startSeq === null || endSeq === null
                ? s.pendingRetain
                : appendPendingRange(s.pendingRetain, {
                    start: startSeq,
                    end: endSeq,
                    messages: conversational,
                  }),
          };
        });

        // 3. Spawn the sidecar pass (detached, non-blocking) - only after a successful ledger
        //    insert, and only for router-served keys (plan v3 §5, R3).
        if (ledgerResult.ok && !heartbeat) {
          try {
            const passScript = `${WORKSPACE_ROOT}\\workspace\\skills\\astinus\\astinus_pass.py`;
            if (existsSync(passScript)) {
              const passPayload = JSON.stringify({
                session_key: key,
                session_id: typeof params.sessionId === "string" ? params.sessionId : key,
                range: [startSeq ?? 0, endSeq ?? 0],
                gen: generation ?? "",
                view_tokens: null,
                token_budget: null,
              });
              const passChild = spawn("python", [passScript], {
                detached: true,
                windowsHide: true,
                stdio: ["pipe", "ignore", "ignore"],
              });
              passChild.stdin?.end(passPayload);
              passChild.unref();
            }
          } catch (e) {
            log.warn(`[astinus-pass] spawn failed: ${(e as Error).message}`);
          }
        }

        // In-process checks (§12.3): the engine-serving and ledger-writer signals
        // only, as a warn line — so a regression is in the gateway log within one
        // turn rather than two hours (the 05-06 -> 09-10 lesson).
        try {
          if (!ledgerResult.ok) {
            log.warn(
              `[astinus-engine] in-process check: ledger writer failed (${ledgerResult.reason ?? "unknown"}) for ${key}`
            );
          } else {
            // F13: compare the PREVIOUS turn (captured before this commit). A ledger whose max
            // seq_end sits behind the state document's lastCommittedSeq means the previous
            // turn's ledger row was missed.
            if (
              !heartbeat &&
              prevStateSeq > 0 &&
              prevLedgerSeq !== null &&
              prevLedgerSeq < prevStateSeq
            ) {
              log.warn(
                `[astinus-engine] in-process check: engine-serving lag - previous state lastCommittedSeq=${prevStateSeq}, ledger max seq_end=${prevLedgerSeq}`
              );
            }
          }
        } catch (e) {
          log.warn(`[astinus-engine] in-process check failed: ${(e as Error).message}`);
        }

        return { status: "committed" };
      },
    }));
    log.info(
      `[astinus-engine] context engine registered (id: ${ASTINUS_ENGINE_ID}; inert until plugins.slots.contextEngine names it)`
    );
  } catch (e) {
    log.warn(`[astinus-engine] registration failed: ${(e as Error).message}`);
  }
}

const _registeredApis = new WeakSet<MoltbotPluginAPI>();

export default function (api: MoltbotPluginAPI) {
  if (_registeredApis.has(api)) {
    debug("[Hindsight] Plugin entry skipped (this api instance already registered)");
    return;
  }
  _registeredApis.add(api);
  try {
    log.info("plugin entry invoked");
    debug("[Hindsight] Plugin loading...");

    // Get plugin config first (needed for debug flag and service registration)
    const pluginConfig = getPluginConfig(api);
    // If logLevel is 'debug', also enable legacy debug flag
    debugEnabled = pluginConfig.debug ?? pluginConfig.logLevel === "debug";

    // Configure structured logger — route through OpenClaw's api.logger for consistent formatting
    if (api.logger) setApiLogger(api.logger);
    configureLogger({
      logLevel: pluginConfig.logLevel ?? (pluginConfig.debug ? "debug" : "info"),
      logSummaryIntervalMs: pluginConfig.logSummaryIntervalMs,
    });

    // Store config globally for bank ID derivation in hooks
    currentPluginConfig = pluginConfig;

    // Register the (inert) Astinus context engine. Safe no-op until the host
    // slot is switched on and the durable-turn contract lands.
    configureSessionLedger(WORKSPACE_ROOT);
    registerAstinusContextEngine(api);

    debug("[Hindsight] Plugin loaded successfully (deferred heavy init to gateway start)");

    // Register background service for cleanup
    // IMPORTANT: Heavy initialization (LLM detection, daemon start, API health checks)
    // happens in service.start() which is ONLY called on gateway start,
    // not on every CLI command.
    debug("[Hindsight] Registering service...");
    log.info("registering plugin service");
    api.registerService({
      id: "hindsight-memory",
      async start() {
        serviceAbortController?.abort();
        const serviceController = new AbortController();
        serviceAbortController = serviceController;
        const startGeneration = ++serviceGeneration;
        log.info("service.start invoked");
        debug("[Hindsight] Service start called - beginning heavy initialization...");

        // Detect LLM configuration (env vars > plugin config > auto-detect)
        debug("[Hindsight] Detecting LLM config...");
        const llmConfig = detectLLMConfig(pluginConfig);

        const baseUrlInfo = llmConfig.baseUrl ? `, base URL: ${llmConfig.baseUrl}` : "";
        const modelInfo = llmConfig.model || "default";

        if (llmConfig.provider === "ollama") {
          debug(
            `[Hindsight] ✓ Using provider: ${llmConfig.provider}, model: ${modelInfo} (${llmConfig.source})`
          );
        } else {
          debug(
            `[Hindsight] ✓ Using provider: ${llmConfig.provider}, model: ${modelInfo} (${llmConfig.source}${baseUrlInfo})`
          );
        }
        if (pluginConfig.bankMission) {
          debug(
            `[Hindsight] Custom bank mission configured: "${pluginConfig.bankMission.substring(0, 50)}..."`
          );
        }

        // Log bank ID mode
        if (pluginConfig.dynamicBankId) {
          const prefixInfo = pluginConfig.bankIdPrefix
            ? ` (prefix: ${pluginConfig.bankIdPrefix})`
            : "";
          debug(
            `[Hindsight] ✓ Dynamic bank IDs enabled${prefixInfo} - each channel gets isolated memory`
          );
        } else {
          const sourceInfo = getConfiguredBankId(pluginConfig) ? "configured" : "default";
          debug(
            `[Hindsight] Dynamic bank IDs disabled - using ${sourceInfo} static bank: ${getStaticBankId(pluginConfig)}`
          );
        }

        // Detect external API mode
        const externalApi = detectExternalApi(pluginConfig);
        usingExternalApi = Boolean(externalApi.apiUrl);

        // Get API port from config (default: 9077)
        const apiPort = pluginConfig.apiPort || 9077;

        // Both modes get the queue: a local daemon is unreachable while it is
        // still booting or after it has crashed, and a retain that fails then is
        // just as lost as one that fails against a remote API. (#3686)
        initRetainQueue(pluginConfig, startGeneration, serviceController.signal);

        if (externalApi.apiUrl) {
          // External API mode - skip local daemon
          usingExternalApi = true;
          debug(`[Hindsight] ✓ Using external API: ${externalApi.apiUrl}`);

          if (externalApi.apiToken) {
            debug("[Hindsight] API token configured");
          }
        } else {
          debug(`[Hindsight] API Port: ${apiPort}`);
        }

        // Initialize (runs synchronously in service.start())
        debug("[Hindsight] Starting initialization...");
        initPromise = (async () => {
          try {
            if (usingExternalApi && externalApi.apiUrl) {
              // External API mode - check health, skip daemon startup
              debug("[Hindsight] External API mode - skipping local daemon...");
              await checkExternalApiHealth(externalApi.apiUrl, externalApi.apiToken);
              await detectAppendCapability(externalApi.apiUrl, externalApi.apiToken);

              // Initialize client for external API
              debug("[Hindsight] Creating HindsightClient (external API)...");
              clientOptions = buildClientOptions(llmConfig, pluginConfig, externalApi);
              banksWithDefaultsApplied.clear();
              client = new HindsightClient(clientOptions);

              const defaultBankId = deriveBankId(undefined, pluginConfig);
              debug(`[Hindsight] Default bank: ${defaultBankId}`);

              // Defaults are stamped per-bank when dynamic bank IDs are enabled.
              // For static banks, stamp once here on init.
              if (usesStaticBank(pluginConfig)) {
                debug(`[Hindsight] Applying configured bank defaults...`);
                await ensureBankDefaultsApplied(defaultBankId, pluginConfig);
              }

              if (!isInitialized) {
                const mode = "external API";
                const autoRecall = pluginConfig.autoRecall !== false;
                const autoRetain = pluginConfig.autoRetain !== false;
                log.info(
                  `initialized (mode: ${mode}, bank: ${defaultBankId}, autoRecall: ${autoRecall}, autoRetain: ${autoRetain})`
                );
              }
              isInitialized = true;
              debug("[Hindsight] ✓ Ready (external API mode)");
            } else {
              // Local daemon mode - start hindsight-embed daemon
              debug("[Hindsight] Creating HindsightServer...");
              hindsightServer = new HindsightServer({
                profile: "openclaw",
                port: apiPort,
                embedVersion: pluginConfig.embedVersion,
                embedPackagePath: pluginConfig.embedPackagePath,
                env: {
                  HINDSIGHT_API_LLM_PROVIDER: llmConfig.provider || "",
                  HINDSIGHT_API_LLM_API_KEY: llmConfig.apiKey || "",
                  HINDSIGHT_API_LLM_MODEL: llmConfig.model,
                  HINDSIGHT_API_LLM_BASE_URL: llmConfig.baseUrl,
                  HINDSIGHT_EMBED_DAEMON_IDLE_TIMEOUT: String(pluginConfig.daemonIdleTimeout ?? 0),
                },
                logger: embedLogger,
              });

              // Start the embedded server
              debug("[Hindsight] Starting embedded server...");
              await hindsightServer.start();

              // The daemon is a Hindsight API like any other: probe it for the
              // same capabilities as an external one, or retains here silently
              // fall back to per-turn document ids. (#3686)
              await detectAppendCapability(hindsightServer.getBaseUrl());

              // Initialize client pointed at the local daemon URL
              debug("[Hindsight] Creating HindsightClient (local daemon)...");
              clientOptions = { baseUrl: hindsightServer.getBaseUrl() };
              banksWithDefaultsApplied.clear();
              client = new HindsightClient(clientOptions);

              const defaultBankId = deriveBankId(undefined, pluginConfig);
              debug(`[Hindsight] Default bank: ${defaultBankId}`);

              // Defaults are stamped per-bank when dynamic bank IDs are enabled.
              // For static banks, stamp once here on init.
              if (usesStaticBank(pluginConfig)) {
                debug(`[Hindsight] Applying configured bank defaults...`);
                await ensureBankDefaultsApplied(defaultBankId, pluginConfig);
              }

              if (!isInitialized) {
                const mode = "local daemon";
                const autoRecall = pluginConfig.autoRecall !== false;
                const autoRetain = pluginConfig.autoRetain !== false;
                log.info(
                  `initialized (mode: ${mode}, bank: ${defaultBankId}, autoRecall: ${autoRecall}, autoRetain: ${autoRetain})`
                );
              }
              isInitialized = true;
              debug("[Hindsight] ✓ Ready");
            }
          } catch (error) {
            log.error("initialization error", error);
            throw error;
          }
        })();

        // Wait for initialization to complete
        try {
          await initPromise;
        } catch (error) {
          log.error("initial initialization failed", error);
          // Continue to health check below
        }

        // External API mode: check external API health
        if (usingExternalApi) {
          const externalApi = detectExternalApi(pluginConfig);
          if (externalApi.apiUrl && isInitialized) {
            try {
              await checkExternalApiHealth(externalApi.apiUrl, externalApi.apiToken);
              await detectAppendCapability(externalApi.apiUrl, externalApi.apiToken);
              debug("[Hindsight] External API is healthy");
              return;
            } catch (error) {
              log.error("external API health check failed", error);
              // Reset state for reinitialization attempt
              client = null;
              clientOptions = null;
              banksWithDefaultsApplied.clear();
              isInitialized = false;
            }
          }
        } else {
          // Local daemon mode: check daemon health (handles SIGUSR1 restart case)
          if (hindsightServer && isInitialized) {
            const healthy = await hindsightServer.checkHealth();
            if (healthy) {
              // Same re-probe the external branch does after its health check:
              // the daemon may have been restarted (SIGUSR1) onto a different
              // embed version since we last looked. (#3686)
              await detectAppendCapability(hindsightServer.getBaseUrl());
              debug("[Hindsight] Daemon is healthy");
              return;
            }

            debug("[Hindsight] Daemon is not responding - reinitializing...");
            // Reset state for reinitialization
            hindsightServer = null;
            client = null;
            clientOptions = null;
            banksWithDefaultsApplied.clear();
            isInitialized = false;
          }
        }

        // Reinitialize if needed (fresh start or recovery)
        if (!isInitialized) {
          debug("[Hindsight] Reinitializing...");
          const reinitPluginConfig = getPluginConfig(api);
          currentPluginConfig = reinitPluginConfig;
          const llmConfig = detectLLMConfig(reinitPluginConfig);
          const externalApi = detectExternalApi(reinitPluginConfig);
          const apiPort = reinitPluginConfig.apiPort || 9077;

          if (externalApi.apiUrl) {
            // External API mode
            usingExternalApi = true;

            await checkExternalApiHealth(externalApi.apiUrl, externalApi.apiToken);
            await detectAppendCapability(externalApi.apiUrl, externalApi.apiToken);

            clientOptions = buildClientOptions(llmConfig, reinitPluginConfig, externalApi);
            banksWithDefaultsApplied.clear();
            client = new HindsightClient(clientOptions);
            const defaultBankId = deriveBankId(undefined, reinitPluginConfig);

            if (usesStaticBank(reinitPluginConfig)) {
              await ensureBankDefaultsApplied(defaultBankId, reinitPluginConfig);
            }

            isInitialized = true;
            debug("[Hindsight] Reinitialization complete (external API mode)");
          } else {
            // Local daemon mode
            hindsightServer = new HindsightServer({
              profile: "openclaw",
              port: apiPort,
              embedVersion: reinitPluginConfig.embedVersion,
              embedPackagePath: reinitPluginConfig.embedPackagePath,
              env: {
                HINDSIGHT_API_LLM_PROVIDER: llmConfig.provider || "",
                HINDSIGHT_API_LLM_API_KEY: llmConfig.apiKey || "",
                HINDSIGHT_API_LLM_MODEL: llmConfig.model,
                HINDSIGHT_API_LLM_BASE_URL: llmConfig.baseUrl,
                HINDSIGHT_EMBED_DAEMON_IDLE_TIMEOUT: String(
                  reinitPluginConfig.daemonIdleTimeout ?? 0
                ),
              },
              logger: embedLogger,
            });

            await hindsightServer.start();
            await detectAppendCapability(hindsightServer.getBaseUrl());

            clientOptions = { baseUrl: hindsightServer.getBaseUrl() };
            banksWithDefaultsApplied.clear();
            client = new HindsightClient(clientOptions);
            const defaultBankId = deriveBankId(undefined, reinitPluginConfig);

            if (usesStaticBank(reinitPluginConfig)) {
              await ensureBankDefaultsApplied(defaultBankId, reinitPluginConfig);
            }

            isInitialized = true;
            debug("[Hindsight] Reinitialization complete");
          }
        }
      },

      async stop() {
        try {
          serviceGeneration++;
          serviceAbortController?.abort();
          serviceAbortController = null;
          debug("[Hindsight] Service stopping...");

          // Only stop daemon if in local mode
          if (!usingExternalApi && hindsightServer) {
            await hindsightServer.stop();
            hindsightServer = null;
          }

          // Close retain queue
          if (retainQueueFlushTimer) {
            clearInterval(retainQueueFlushTimer);
            retainQueueFlushTimer = null;
          }
          if (retainQueue) {
            const pending = retainQueue.size();
            if (pending > 0) {
              debug(
                `[Hindsight] Service stopping with ${pending} queued retains (will resume on next start)`
              );
            }
            retainQueue.close();
            retainQueue = null;
          }

          client = null;
          clientOptions = null;
          asyncRetainOperationIdCapability = "unknown";
          usingExternalApi = false;
          banksWithDefaultsApplied.clear();
          isInitialized = false;

          stopLogger();
          debug("[Hindsight] Service stopped");
        } catch (error) {
          log.error("service stop error", error);
          throw error;
        }
      },
    });

    debug("[Hindsight] Plugin loaded successfully");

    // Register agent hooks for auto-recall and auto-retention.
    //
    // Why no module-level "already registered" guard: each plugin entry invocation
    // hands us a fresh `api` tied to a specific plugin registry. OpenClaw may call
    // the plugin entry multiple times per process (CLI vs gateway vs lazy reloads),
    // and the registry that's active when an agent actually runs is not guaranteed
    // to be the first one we saw. A process-global flag would let the first call
    // "win" and leave subsequent registries with zero hindsight hooks — which is
    // exactly how auto-recall/auto-retain silently stopped firing in 0.6.x.
    debug("[Hindsight] Registering agent hooks...");
    log.info("registering agent hooks");

    api.on("before_dispatch", async (event: any, ctx?: PluginHookAgentContext) => {
      try {
        const sessionKey =
          ctx?.sessionKey ?? (typeof event?.sessionKey === "string" ? event.sessionKey : undefined);
        if (!sessionKey) {
          return;
        }

        const dispatchChannel =
          (typeof event?.channel === "string" ? event.channel : undefined) ||
          ctx?.messageProvider ||
          parseSessionKey(sessionKey).provider;
        const { resolvedCtx, skipReason } = resolveAndCacheIdentity({
          sessionKey,
          ctx: {
            ...ctx,
            sessionKey,
            senderId:
              (typeof event?.senderId === "string" ? event.senderId : undefined) || ctx?.senderId,
          },
          dispatchChannel,
          pluginConfig,
        });

        if (skipReason) {
          debug(
            `[Hindsight] before_dispatch marked session ${sessionKey} to skip this turn: ${formatIdentitySkipReason(skipReason)}`
          );
          logSkipOnce("dispatch", sessionKey, skipReason);
          return;
        }
        if (!resolvedCtx?.senderId || typeof resolvedCtx.senderId !== "string") {
          return;
        }

        debug(
          `[Hindsight] before_dispatch cached identity for ${sessionKey}: ${resolvedCtx.messageProvider}/${resolvedCtx.channelId} sender=${resolvedCtx.senderId}`
        );
      } catch (error) {
        log.warn(`before_dispatch identity cache error: ${error}`);
      }
    });

    // No `before_agent_start` registration: the callback used to call
    // `resolveAndCacheIdentity()` and emit a debug log, but `before_dispatch`
    // already populates the identity cache earlier in the inbound path,
    // `before_prompt_build` re-resolves before recall (and can infer
    // `senderId` from prompt content when ctx is missing it), and `agent_end`
    // re-resolves before retain. Subscribing here was duplicate work on the
    // hot path. (#1354)

    // Auto-recall: Inject relevant memories before agent processes the message
    // Hook signature: (event, ctx) where event has {prompt, messages?} and ctx has agent context
    api.on("before_prompt_build", async (event: any, ctx?: PluginHookAgentContext) => {
      // Optional perf instrumentation (#1406). Captured here at hook entry so
      // the early-return paths below don't influence the measurement of slow
      // recall calls — perf lines are only emitted on the recall path.
      const perfHookStart = pluginConfig.debugPerfTiming ? Date.now() : 0;
      try {
        // Check if this provider is excluded
        if (ctx?.messageProvider && pluginConfig.excludeProviders?.includes(ctx.messageProvider)) {
          debug(`[Hindsight] Skipping recall for excluded provider: ${ctx.messageProvider}`);
          return;
        }

        // Session pattern filtering
        const sessionKey = ctx?.sessionKey;
        if (sessionKey) {
          const ignorePatterns = compileSessionPatterns(pluginConfig.ignoreSessionPatterns ?? []);
          if (ignorePatterns.length > 0 && matchesSessionPattern(sessionKey, ignorePatterns)) {
            debug(
              `[Hindsight] Skipping recall: session '${sessionKey}' matches ignoreSessionPatterns`
            );
            return;
          }
          const skipStateless = pluginConfig.skipStatelessSessions !== false;
          if (skipStateless) {
            const statelessPatterns = compileSessionPatterns(
              pluginConfig.statelessSessionPatterns ?? []
            );
            if (
              statelessPatterns.length > 0 &&
              matchesSessionPattern(sessionKey, statelessPatterns)
            ) {
              debug(
                `[Hindsight] Skipping recall: session '${sessionKey}' matches statelessSessionPatterns (skipStatelessSessions=true)`
              );
              return;
            }
          }
        }

        // Skip auto-recall when disabled (agent has its own recall tool)
        if (!pluginConfig.autoRecall) {
          debug("[Hindsight] Auto-recall disabled via config, skipping");
          return;
        }

        const sessionKeyForCache =
          ctx?.sessionKey ?? (typeof event?.sessionKey === "string" ? event.sessionKey : undefined);
        const skipTurnReason = sessionKeyForCache
          ? skipHindsightTurnBySession.get(sessionKeyForCache)
          : undefined;
        if (skipTurnReason && !isRetryableIdentitySkipReason(skipTurnReason)) {
          debug(
            `[Hindsight] Skipping recall for session ${sessionKeyForCache}: ${formatIdentitySkipReason(skipTurnReason)}`
          );
          logSkipOnce("recall", sessionKeyForCache, skipTurnReason);
          return;
        }

        const senderIdFromPrompt = !ctx?.senderId
          ? extractSenderIdFromText(event.prompt ?? event.rawMessage ?? "")
          : undefined;
        const { resolvedCtx: resolvedCtxForRecall, skipReason: identitySkipReason } =
          resolveAndCacheIdentity({
            sessionKey: sessionKeyForCache,
            ctx,
            senderIdHint: senderIdFromPrompt,
            pluginConfig,
          });
        if (identitySkipReason) {
          debug(
            `[Hindsight] Skipping recall for session ${sessionKeyForCache}: ${formatIdentitySkipReason(identitySkipReason)}`
          );
          logSkipOnce("recall", sessionKeyForCache, identitySkipReason);
          return;
        }

        const bankId = deriveBankId(resolvedCtxForRecall, pluginConfig);
        debug(
          `[Hindsight] before_prompt_build - bank: ${bankId}, channel: ${resolvedCtxForRecall?.messageProvider}/${resolvedCtxForRecall?.channelId}`
        );
        debug(`[Hindsight] event keys: ${Object.keys(event).join(", ")}`);
        debug(`[Hindsight] event.context keys: ${Object.keys(event.context ?? {}).join(", ")}`);

        // Get the user's latest message for recall — only the raw user text, not the full prompt
        // rawMessage is clean user text; prompt includes envelope, system events, media notes, etc.
        debug(
          `[Hindsight] extractRecallQuery input lengths - raw: ${event.rawMessage?.length ?? 0}, prompt: ${event.prompt?.length ?? 0}`
        );
        const extracted = extractRecallQuery(event.rawMessage, event.prompt);
        if (!extracted) {
          debug("[Hindsight] extractRecallQuery returned null, skipping recall");
          return;
        }
        if (isEphemeralOperationalText(extracted)) {
          debug("[Hindsight] Recall query is operational/ephemeral noise, skipping recall");
          return;
        }
        debug(`[Hindsight] extractRecallQuery result length: ${extracted.length}`);
        const recallContextTurns = pluginConfig.recallContextTurns ?? 1;
        const recallMaxQueryChars = pluginConfig.recallMaxQueryChars ?? 800;
        const sessionMessages = event.context?.sessionEntry?.messages ?? event.messages ?? [];
        const messageCount = sessionMessages.length;
        debug(
          `[Hindsight] event.messages count: ${messageCount}, roles: ${sessionMessages.map((m: any) => m.role).join(",")}`
        );
        if (recallContextTurns > 1 && messageCount === 0) {
          debug(
            "[Hindsight] recallContextTurns > 1 but event.messages is empty — prior context unavailable at before_agent_start for this provider"
          );
        }
        const recallRoles = pluginConfig.recallRoles ?? ["user", "assistant"];
        const composedPrompt = composeRecallQuery(
          extracted,
          sessionMessages,
          recallContextTurns,
          recallRoles
        );
        let prompt = truncateRecallQuery(composedPrompt, extracted, recallMaxQueryChars);

        // Final defensive cap
        if (prompt.length > recallMaxQueryChars) {
          prompt = prompt.substring(0, recallMaxQueryChars);
        }

        // Wait for client to be ready
        const clientGlobal = (global as any).__hindsightClient;
        if (!clientGlobal) {
          debug("[Hindsight] Client global not available, skipping auto-recall");
          return;
        }

        await clientGlobal.waitForReady();

        // Get client configured for this context's bank (async to handle mission setup)
        const client = await clientGlobal.getClientForContext(resolvedCtxForRecall);
        if (!client) {
          debug("[Hindsight] Client not initialized, skipping auto-recall");
          return;
        }

        debug(`[Hindsight] Auto-recall for bank ${bankId}, full query:\n---\n${prompt}\n---`);

        // Topic filter (plan v4 item 3): the ledger feeds a SERVER-SIDE tag
        // filter; the query itself stays substance-first (composeRecallQuery
        // above). One classification, the same helper the retain side uses.
        // Topic filter is OFF by default (review 2026-09-12, Gate 1 BLOCK):
        // Hindsight's tags_match "any" includes UNTAGGED memories but excludes
        // memories tagged with any other vocabulary, so a hard topic filter
        // would drop the retain:*/tier:* protected corpus and silently restrict
        // recall to the untagged minority. Gated behind
        // pluginConfig.recallTopicFilter (default false).
        const recallTopicTags = (() => {
          if (pluginConfig.recallTopicFilter !== true) return [] as string[];
          try {
            // Plan SS9.1 (ledger-first): the LIVE topics of THIS session drive the
            // topic tags - the pass's classifications, newest first, terminal
            // topics excluded. The query itself stays substance-first
            // (composeRecallQuery above); the tags ride the Gate 1 merge-not-filter
            // path (unfiltered primary + topic secondary, merged by id - never a
            // hard filter). No ledger / no topics yet (young session, cold handle
            // after a restart) -> the regex classification, exactly as before.
            const ledgerKey = (resolvedCtxForRecall as any)?.sessionKey;
            if (typeof ledgerKey === "string" && ledgerKey) {
              const slugs = readLiveTopics(ledgerKey, 2);
              if (slugs.length > 0) {
                debug(
                  `[Hindsight] recall topic tags from ledger: ${slugs.join(", ")}`
                );
                return slugs.map((s) => `topic:${s}`);
              }
            }
            return topicTags(classifyTurn(prompt).topics);
          } catch {
            return [] as string[];
          }
        })();
        const recallTimeoutMs = pluginConfig.recallTimeoutMs ?? DEFAULT_RECALL_TIMEOUT_MS;
        const recallOnce = (opts: {
          via: "topic" | "unfiltered" | "subject";
          tags?: string[];
          tagsMatch?: "any" | "all" | "any_strict" | "all_strict" | "exact";
          query?: string;
        }): Promise<RecallResolution> =>
          client
            .recall(
              {
                query: opts.query ?? prompt,
                maxTokens: pluginConfig.recallMaxTokens || 1024,
                budget: pluginConfig.recallBudget,
                types: pluginConfig.recallTypes,
                preferObservations: pluginConfig.preferObservations,
                minScores: pluginConfig.recallMinScores,
                ...(opts.tags ? { tags: opts.tags, tagsMatch: opts.tagsMatch } : {}),
              },
              recallTimeoutMs
            )
            .then((response: RecallResponse) => ({ response, via: opts.via }));

        // Merge the topic-filtered hits AHEAD of the unfiltered set (filtered
        // promoted, de-duplicated by result id). Never a hard filter.
        const mergeRecall = (
          unfiltered: RecallResolution,
          filtered: RecallResolution
        ): RecallResolution => {
          const seen = new Set<string>();
          const merged: RecallResponse["results"] = [];
          for (const r of filtered.response.results ?? []) {
            const id = (r as { id?: string }).id;
            if (id && seen.has(id)) continue;
            if (id) seen.add(id);
            merged.push(r);
          }
          for (const r of unfiltered.response.results ?? []) {
            const id = (r as { id?: string }).id;
            if (id && seen.has(id)) continue;
            if (id) seen.add(id);
            merged.push(r);
          }
          return {
            response: { ...unfiltered.response, results: merged },
            via: "topic+unfiltered",
          };
        };

        const resolveRecall = async (): Promise<RecallResolution> => {
          // Flag off (default): plain substance recall, unchanged behaviour.
          if (recallTopicTags.length === 0) {
            return recallOnce({ via: "unfiltered" });
          }
          // THIN-SUBJECT PROBE (ruling 2026-09-17): a NEW topic with little retained
          // content under it may still have subjects whose history lives in the shared
          // bank - all desks retain to the same bank, so searching a thin topic's
          // subjects directly surfaces other desks' material about them. Additive lane:
          // merged by id, never replaces the substance results. Capped at 3 probes.
          const probeKey = (resolvedCtxForRecall as any)?.sessionKey;
          const thinSubjects =
            typeof probeKey === "string" && probeKey
              ? readThinTopicSubjects(probeKey, 3, 4)
              : [];
          for (const s of thinSubjects.slice(0, 3)) {
            log.info(
              `astinus recall: thin-subject probe - "${s.subject}" (topic ${s.topic})`
            );
          }
          // Probe order (ruling 2026-09-17): the PAIR (subject + predicate) is the
          // highest-precision query - the current assertion disambiguates common-word
          // subjects. Single-axis searches are FALLBACKS, tried only when the previous
          // form returns nothing: subject alone (entity history), then predicate alone
          // (behavioral precedent - the email's asymmetric case).
          const probeSubject = async (
            s: { topic: string; subject: string; predicate: string }
          ): Promise<RecallResolution | null> => {
            const forms: Array<{ q: string; kind: string }> = [
              {
                q: `${s.subject} ${s.predicate}`.slice(0, 140),
                kind: `${s.subject} + predicate`,
              },
              { q: s.subject, kind: `subject only` },
              { q: s.predicate, kind: `predicate only` },
            ];
            for (const f of forms) {
              if (!f.q.trim()) continue;
              try {
                const res = await recallOnce({ via: "subject", query: f.q });
                if (res.response.results && res.response.results.length > 0) {
                  debug(
                    `[Hindsight] thin-subject probe "${s.subject}" hit on ${f.kind}`
                  );
                  return res;
                }
              } catch {
                /* try the next form */
              }
            }
            return null;
          };
          const probes = thinSubjects.slice(0, 3).map(probeSubject);
          const unfiltered = await recallOnce({ via: "unfiltered" });
          const [filtered, ...subjectResults] = await Promise.all([
            recallOnce({
              via: "topic",
              tags: recallTopicTags,
              tagsMatch: "any",
            }),
            ...probes,
          ]);
          // Fold the subject hits into the secondary (promoted) side, deduped by id.
          const secondaryResults: RecallResponse["results"] = [
            ...(filtered.response.results ?? []),
          ];
          const seenSecondary = new Set<string>();
          for (const r of secondaryResults) {
            const id = (r as { id?: string }).id;
            if (id) seenSecondary.add(id);
          }
          for (const res of subjectResults) {
            if (!res) continue;
            for (const r of res.response.results ?? []) {
              const id = (r as { id?: string }).id;
              if (id && seenSecondary.has(id)) continue;
              if (id) seenSecondary.add(id);
              secondaryResults.push(r);
            }
          }
          if (secondaryResults.length === 0) {
            debug(
              `[Hindsight] topic-filtered recall returned 0 results (${recallTopicTags.join(", ")}) - using unfiltered results`
            );
            return unfiltered;
          }
          return mergeRecall(unfiltered, {
            response: { ...filtered.response, results: secondaryResults },
            via: "topic+unfiltered",
          });
        };

        // Recall with deduplication: reuse in-flight request for same bank
        const normalizedPrompt = prompt.trim().toLowerCase().replace(/\s+/g, " ");
        const queryHash = createHash("sha256").update(normalizedPrompt).digest("hex").slice(0, 16);
        const recallKey = `${bankId}::${queryHash}`;
        const existing = inflightRecalls.get(recallKey);
        let recallPromise: Promise<RecallResolution>;
        if (existing) {
          debug(`[Hindsight] Reusing in-flight recall for bank ${bankId}`);
          recallPromise = existing;
        } else {
          recallPromise = resolveRecall();
          inflightRecalls.set(recallKey, recallPromise);
          void recallPromise.catch(() => {}).finally(() => inflightRecalls.delete(recallKey));
        }

        const recallStart = pluginConfig.debugPerfTiming ? Date.now() : 0;
        const resolution = await recallPromise;
        const response = resolution.response;
        const recallElapsedMs = pluginConfig.debugPerfTiming ? Date.now() - recallStart : 0;
        debug(
          `[Hindsight] Recall served via ${resolution.via} path (topic filter: ${
            recallTopicTags.length > 0 ? recallTopicTags.join(", ") : "none"
          })`
        );

        if (!response.results || response.results.length === 0) {
          if (pluginConfig.debugPerfTiming) {
            log.info(
              formatHookPerf("before_prompt_build", Date.now() - perfHookStart, {
                recall_main: `${recallElapsedMs}ms`,
                source: existing ? "reused" : "fresh",
                via: resolution.via,
                results: 0,
              })
            );
          }
          debug("[Hindsight] No memories found for auto-recall");
          return;
        }

        debug(
          `[Hindsight] Raw recall response (${response.results.length} results before topK):\n${response.results.map((r: any, i: number) => `  [${i}] score=${r.score?.toFixed(3) ?? "n/a"} type=${r.type ?? "n/a"}: ${JSON.stringify(r.content ?? r.text ?? r).substring(0, 200)}`).join("\n")}`
        );

        const results = pluginConfig.recallTopK
          ? response.results.slice(0, pluginConfig.recallTopK)
          : response.results;

        debug(
          `[Hindsight] After topK (${pluginConfig.recallTopK ?? "unlimited"}): ${results.length} results injected`
        );

        // Format memories as a bullet list (text + type + date + occurred window +
        // [doc:<document_id>], each part present only when the memory carries it)
        const memoriesFormatted = formatMemories(results, { markAsEarlierRecords: true });

        const contextMessage = `<hindsight_memories>
${pluginConfig.recallPromptPreamble || DEFAULT_RECALL_PROMPT_PREAMBLE}
Current time - ${formatCurrentTimeForRecall()}

${memoriesFormatted}
</hindsight_memories>`;

        debug(`[Hindsight] Auto-recall: Injecting ${results.length} memories from bank ${bankId}`);
        log.info(`injecting ${results.length} memories into context (bank: ${bankId})`);
        log.trackRecall(bankId, results.length);

        if (pluginConfig.debugPerfTiming) {
          log.info(
            formatHookPerf("before_prompt_build", Date.now() - perfHookStart, {
              recall_main: `${recallElapsedMs}ms`,
              source: existing ? "reused" : "fresh",
              via: resolution.via,
              results: results.length,
            })
          );
        }

        // Keep recalled memories outside the system prompt by default so the
        // provider can reuse its stable prompt prefix across turns. Users who
        // need system-level memory context can still opt into prepend or append.
        const position = pluginConfig.recallInjectionPosition ?? "user";
        switch (position) {
          case "append":
            return { appendSystemContext: contextMessage };
          case "user":
            return { prependContext: contextMessage };
          case "prepend":
          default:
            return { prependSystemContext: contextMessage };
        }
      } catch (error) {
        if (error instanceof DOMException && error.name === "TimeoutError") {
          log.warn(
            `[Hindsight] Auto-recall timed out after ${pluginConfig.recallTimeoutMs ?? DEFAULT_RECALL_TIMEOUT_MS}ms, skipping memory injection`
          );
        } else if (error instanceof Error && error.name === "AbortError") {
          log.warn(
            `[Hindsight] Auto-recall aborted after ${pluginConfig.recallTimeoutMs ?? DEFAULT_RECALL_TIMEOUT_MS}ms, skipping memory injection`
          );
        } else {
          log.error("auto-recall error", error);
        }
        return;
      }
    });

    // Shared retain path for `agent_end` (per-turn cadence) and `session_end`
    // (force-flush at session close, bypassing the cadence so short sessions
    // and the un-retained tail of long sessions still land on disk). See #1726.
    const runRetain = async (
      event: any,
      ctx: PluginHookAgentContext | undefined,
      retainOptions: { force?: boolean; hookName: "agent_end" | "session_end" } = {
        hookName: "agent_end",
      }
    ): Promise<void> => {
      const force = retainOptions.force === true;
      const hookName = retainOptions.hookName;
      const retainGeneration = serviceGeneration;
      const retainController = serviceAbortController;
      const retainSignal = retainController?.signal;
      const retainLifecycleIsCurrent = () =>
        retainController !== null &&
        serviceAbortController === retainController &&
        retainGeneration === serviceGeneration &&
        !retainSignal?.aborted;
      if (!retainLifecycleIsCurrent()) return;
      // Optional perf instrumentation (#1406). Only emitted when an actual
      // retain RPC fires; the many early-return skip paths are not measured.
      const perfHookStart = pluginConfig.debugPerfTiming ? Date.now() : 0;
      try {
        // Avoid cross-session contamination: only use context carried by this event.
        const eventSessionKey =
          typeof event?.sessionKey === "string" ? event.sessionKey : undefined;
        const effectiveCtx =
          ctx ||
          (eventSessionKey
            ? ({ sessionKey: eventSessionKey } as PluginHookAgentContext)
            : undefined);

        // Check if this provider is excluded
        if (
          effectiveCtx?.messageProvider &&
          pluginConfig.excludeProviders?.includes(effectiveCtx.messageProvider)
        ) {
          debug(
            `[Hindsight] Skipping retain for excluded provider: ${effectiveCtx.messageProvider}`
          );
          return;
        }

        // Session pattern filtering
        const agentEndSessionKey = effectiveCtx?.sessionKey;
        if (agentEndSessionKey) {
          const ignorePatterns = compileSessionPatterns(pluginConfig.ignoreSessionPatterns ?? []);
          if (
            ignorePatterns.length > 0 &&
            matchesSessionPattern(agentEndSessionKey, ignorePatterns)
          ) {
            debug(
              `[Hindsight] Skipping retain: session '${agentEndSessionKey}' matches ignoreSessionPatterns`
            );
            return;
          }
          const statelessPatterns = compileSessionPatterns(
            pluginConfig.statelessSessionPatterns ?? []
          );
          if (
            statelessPatterns.length > 0 &&
            matchesSessionPattern(agentEndSessionKey, statelessPatterns)
          ) {
            debug(
              `[Hindsight] Skipping retain: session '${agentEndSessionKey}' matches statelessSessionPatterns`
            );
            return;
          }
          const skipRetainPatterns = compileSessionPatterns(
            pluginConfig.skipRetainSessionPatterns ?? []
          );
          if (
            skipRetainPatterns.length > 0 &&
            matchesSessionPattern(agentEndSessionKey, skipRetainPatterns)
          ) {
            debug(
              `[Hindsight] Skipping retain - operational session pattern matched: ${agentEndSessionKey}`
            );
            return;
          }
        }

        const sessionKeyForLookup = effectiveCtx?.sessionKey;
        const skipTurnReason = sessionKeyForLookup
          ? skipHindsightTurnBySession.get(sessionKeyForLookup)
          : undefined;
        if (skipTurnReason && !isRetryableIdentitySkipReason(skipTurnReason)) {
          debug(
            `[Hindsight Hook] Skipping retain for session ${sessionKeyForLookup}: ${formatIdentitySkipReason(skipTurnReason)}`
          );
          logSkipOnce("retain", sessionKeyForLookup, skipTurnReason);
          if (sessionKeyForLookup) {
            skipHindsightTurnBySession.delete(sessionKeyForLookup);
          }
          return;
        }

        const {
          effectiveCtx: effectiveCtxForRetain,
          resolvedCtx: resolvedCtxForRetain,
          skipReason: identitySkipReason,
        } = resolveAndCacheIdentity({
          sessionKey: sessionKeyForLookup,
          ctx: effectiveCtx,
          pluginConfig,
        });

        if (identitySkipReason) {
          debug(
            `[Hindsight Hook] Skipping retain for session ${sessionKeyForLookup}: ${formatIdentitySkipReason(identitySkipReason)}`
          );
          logSkipOnce("retain", sessionKeyForLookup, identitySkipReason);
          if (sessionKeyForLookup) {
            skipHindsightTurnBySession.delete(sessionKeyForLookup);
          }
          return;
        }
        if (sessionKeyForLookup) {
          skipHindsightTurnBySession.delete(sessionKeyForLookup);
        }

        const bankId = deriveBankId(resolvedCtxForRetain, pluginConfig);
        debug(`[Hindsight Hook] ${hookName} triggered - bank: ${bankId}`);

        // `event.success === false` is a per-agent-end signal; session_end
        // doesn't carry it and a failed last turn shouldn't block the final
        // flush of preceding successful turns. (#1726)
        if (!force && event.success === false) {
          debug("[Hindsight Hook] Agent run failed, skipping retention");
          return;
        }

        if (
          !Array.isArray(event.context?.sessionEntry?.messages ?? event.messages) ||
          (event.context?.sessionEntry?.messages ?? event.messages ?? []).length === 0
        ) {
          debug("[Hindsight Hook] No messages in event, skipping retention");
          return;
        }

        if (pluginConfig.autoRetain === false) {
          debug("[Hindsight Hook] autoRetain is disabled, skipping retention");
          return;
        }

        // Chunked retention: skip non-Nth turns and use a sliding window when firing
        const retainEveryN = pluginConfig.retainEveryNTurns ?? 1;
        const allMessages = event.context?.sessionEntry?.messages ?? event.messages ?? [];
        let messagesToRetain = allMessages;
        let retainFullWindow = false;

        // Position-based delta selection (plan v4). The durable per-session
        // state carries the retained position; the context engine's commit
        // ranges are the delta (the agent_end payload never carries seqs). No
        // windows, no overlap, no duplicates, restart-safe. retainEveryNTurns
        // is only a cost gate (how often to commit); correctness lives in the
        // durable state.
        let markerAdvance: {
          sessionKey: string;
          seq: number;
          hashes: string[];
          clearPending?: boolean;
        } | null = null;

        const markerSessionKey = effectiveCtx?.sessionKey;
        // One durable read serves both the marker path and the committed-range
        // path (option a).
        const sessionStateForRetain = markerSessionKey
          ? loadSessionState(markerSessionKey)
          : null;
        // The engine "serves" a session once it has committed a durable turn.
        // While it serves, the legacy cadence must never run - it would re-retain
        // turns the commit-range path already covers (review 2026-09-13, Q1 major).
        const engineServing =
          !!sessionStateForRetain &&
          (sessionStateForRetain.lastCommittedAt > 0 ||
            sessionStateForRetain.committedKeys.length > 0);
        // Plan SS9.3 (retain tags from the ledger): the ranges being drained by
        // THIS retain, and the tags derived from them. Null = no ledger ranges in
        // this retain (legacy cadence / seq path) - the regex classification stays
        // exactly as before.
        let drainedLedgerRanges: Array<{ start: number; end: number }> | null = null;
        let ledgerTopicTags: string[] | null = null;

        if (
          markerSessionKey &&
          sessionStateForRetain &&
          sessionStateForRetain.pendingRetain.length > 0
        ) {
          // Option (a): the agent_end payload never carries transcript seqs, so
          // once the context engine is serving turns the durable commit ranges
          // are the only precise source. Drain them on the same cadence and
          // advance only on a successful retain (slot-day change).
          const pendingRanges = sessionStateForRetain.pendingRetain;
          const pendingMsgs = pendingRanges.flatMap((r) => r.messages);
          if (pendingMsgs.length === 0) {
            // Nothing conversational in the pending ranges: clear them without
            // touching the marker.
            tryUpdateSessionState(markerSessionKey, (s) => ({ ...s, pendingRetain: [] }));
            return;
          }
          const pendingTurns = pendingMsgs.filter((m: any) => m?.role === "user").length;
          const pendingEndSeq = Math.max(...pendingRanges.map((r) => r.end));
          if (!force && pendingTurns < retainEveryN) {
            debug(
              `[Hindsight Hook] commit-range: ${pendingTurns}/${retainEveryN} un-retained turns - holding (${pendingMsgs.length} msgs, ${pendingRanges.length} ranges)`
            );
            return;
          }
          messagesToRetain = pendingMsgs;
          retainFullWindow = true;
          drainedLedgerRanges = pendingRanges;
          markerAdvance = {
            sessionKey: markerSessionKey,
            seq: pendingEndSeq,
            hashes: pendingMsgs.map((m: any) =>
              createHash("sha256")
                .update(
                  typeof m?.content === "string" ? m.content : JSON.stringify(m?.content ?? "")
                )
                .digest("hex")
            ),
            clearPending: true,
          };
          log.info(
            `commit-range: retained ${pendingMsgs.length} msgs / ${pendingTurns} turns through seq ${pendingEndSeq}`
          );
          debug(
            `[Hindsight Hook] commit-range: draining ${pendingRanges.length} committed range(s)`
          );
        } else if (engineServing) {
          debug(
            `[Hindsight Hook] commit-range: engine serving, nothing committed-unretained - skipping retain`
          );
          return;
        } else if (retainEveryN > 1) {
          const sessionTrackingKey = `${bankId}:${effectiveCtx?.sessionKey || "session"}`;
          // session_end is a flush, not a turn — don't increment the counter.
          const turnCount = force
            ? turnCountBySession.get(sessionTrackingKey) || 0
            : (turnCountBySession.get(sessionTrackingKey) || 0) + 1;
          if (!force) {
            setCappedMapValue(turnCountBySession, sessionTrackingKey, turnCount);
          }

          const cadenceBoundary = turnCount > 0 && turnCount % retainEveryN === 0;
          const unretainedTurns = turnCount % retainEveryN;

          if (force) {
            // session_end: only flush if there are un-retained turns since the
            // last cadence boundary. The most recent agent_end either already
            // retained (cadenceBoundary) or accumulated `unretainedTurns` turns
            // that would otherwise be lost when the session closes. (#1726)
            if (turnCount === 0 || cadenceBoundary) {
              debug(
                `[Hindsight Hook] session_end: nothing un-retained (turnCount=${turnCount}, retainEveryN=${retainEveryN}), skipping flush`
              );
              return;
            }
            const overlapTurns = pluginConfig.retainOverlapTurns ?? 0;
            const windowTurns = unretainedTurns + overlapTurns;
            messagesToRetain = sliceLastTurnsByUserBoundary(allMessages, windowTurns);
            retainFullWindow = true;
            // Reset so a subsequent session_end (if the host re-emits) doesn't
            // re-retain the same window.
            turnCountBySession.delete(sessionTrackingKey);
            debug(
              `[Hindsight Hook] session_end: forced flush of ${unretainedTurns} un-retained turns (window: ${windowTurns} turns, ${messagesToRetain.length} messages)`
            );
          } else {
            if (!cadenceBoundary) {
              const nextRetainAt = Math.ceil(turnCount / retainEveryN) * retainEveryN;
              debug(
                `[Hindsight Hook] Turn ${turnCount}/${retainEveryN}, skipping retain (next at turn ${nextRetainAt})`
              );
              return;
            }

            // Sliding window in turns: N turns + configured overlap turns.
            // We slice by actual turn boundaries (user-role messages), so this
            // remains stable even when system/tool messages are present.
            const overlapTurns = pluginConfig.retainOverlapTurns ?? 0;
            const windowTurns = retainEveryN + overlapTurns;
            messagesToRetain = sliceLastTurnsByUserBoundary(allMessages, windowTurns);
            retainFullWindow = true;
            debug(
              `[Hindsight Hook] Turn ${turnCount}: chunked retain firing (window: ${windowTurns} turns, ${messagesToRetain.length} messages)`
            );
          }
        } else if (force) {
          // retainEveryN === 1 means every agent_end already retained — the
          // session_end flush would only duplicate work. (#1726)
          debug("[Hindsight Hook] session_end: retainEveryNTurns=1, nothing to flush");
          return;
        }

        const inlineRetainTags = normalizeRetainTags(
          messagesToRetain.flatMap((msg: any) => {
            if (msg?.role !== "user") {
              return [];
            }

            const content =
              typeof msg?.content === "string"
                ? msg.content
                : Array.isArray(msg?.content)
                  ? msg.content
                      .filter(
                        (block: any) => block?.type === "text" && typeof block?.text === "string"
                      )
                      .map((block: any) => block.text)
                      .join("\n")
                  : "";

            return extractInlineRetainTags(content);
          })
        );

        const retention = prepareRetentionTranscript(
          messagesToRetain,
          pluginConfig,
          retainFullWindow
        );
        if (!retention) {
          debug("[Hindsight Hook] No messages to retain (filtered/short/no-user)");
          return;
        }
        const { transcript, messageCount } = retention;

        if (isEphemeralOperationalText(transcript)) {
          debug("[Hindsight Hook] Transcript is operational/ephemeral noise, skipping retention");
          return;
        }

        // ONE classification per retain (plan v4 item 3): the quality gate and
        // the topic tags read the same result — classifyTurn is a cheap regex
        // pass, but there is no reason to run it twice.
        let turnClassification: { activity: string; topics: string[] } | null = null;
        const getTurnClassification = (): { activity: string; topics: string[] } => {
          if (!turnClassification) {
            try {
              turnClassification = classifyTurn(transcript);
            } catch {
              turnClassification = { activity: "general", topics: [] };
            }
          }
          return turnClassification;
        };

        if (pluginConfig.retainQualityGate) {
          try {
            const { activity, topics } = getTurnClassification();
            let skipReason = "";
            if (activity === "chitchat" && topics.length === 0 && transcript.length < 200) skipReason = "short chitchat";
            else if (activity === "memory-meta" && topics.length === 0) skipReason = "memory-meta noise";
            else if (/^(Assistant|User) (changed|requested|planned|explained)/i.test(transcript) && !/\b(decided|insight|important)\b/i.test(transcript) && transcript.length < 500) skipReason = "procedural noise";
            else if (/^(Assistant planned to|The assistant explained)/i.test(transcript) && topics.length === 0) skipReason = "narrative noise";
            else if (/^\[role: tool\][\s\S]*\[role: assistant\][\s\S]*$/.test(transcript) && !transcript.includes("[role: user]")) skipReason = "pure tool calls";

            if (skipReason) {
              debug(`[Hindsight] Skipping retain - quality gate: ${skipReason}`);
              return;
            }
          } catch (e) {
            debug(`[Hindsight] Quality gate error: ${e}`);
          }
        }

        // Wait for client to be ready
        const clientGlobal = (global as any).__hindsightClient;
        if (!clientGlobal) {
          log.warn("client global not found, skipping retain");
          return;
        }

        await clientGlobal.waitForReady();
        if (!retainLifecycleIsCurrent()) return;

        // Get client configured for this context's bank (async to handle mission setup)
        const client = await clientGlobal.getClientForContext(resolvedCtxForRetain);
        if (!retainLifecycleIsCurrent()) return;
        if (!client) {
          log.warn("client not initialized, skipping retain");
          return;
        }

        // Use the cached capability, and only pay for a /version round trip while
        // it is still unknown. Probing on every retain would put an extra
        // request in front of every turn, and the answer changes at most once
        // per server restart — the queue flush re-probes on its own timer.
        const retainOperationIdCapability =
          asyncRetainOperationIdCapability === "unknown"
            ? await refreshQueueOperationIdCapability(retainGeneration, retainSignal)
            : asyncRetainOperationIdCapability;
        if (!retainLifecycleIsCurrent()) return;
        // Ledger topics for the drained ranges (plan SS9.3). Computed HERE because
        // getTurnClassification is defined below this point - calling it inside the
        // drain branch would hit the temporal dead zone.
        if (drainedLedgerRanges && markerSessionKey) {
          const ledgerTopics = readTopicsForRanges(markerSessionKey, drainedLedgerRanges);
          if (ledgerTopics.uncovered.length > 0) {
            // R6: ranges the pass has not reached get the regex classification,
            // written back as source='fallback' so Gate L sees the coverage gap.
            const regexTopics = getTurnClassification().topics;
            writeFallbackTopics(markerSessionKey, ledgerTopics.uncovered, regexTopics);
            ledgerTopicTags = [
              ...ledgerTopics.slugs.map((s) => `topic:${s}`),
              ...topicTags(regexTopics),
            ];
          } else {
            ledgerTopicTags = ledgerTopics.slugs.map((s) => `topic:${s}`);
          }
        }
        const retainNow = Date.now();
        const retainRequest = buildRetainRequest(
          transcript,
          messageCount,
          effectiveCtxForRetain,
          pluginConfig,
          retainNow,
          {
            retentionScope: retainFullWindow ? "window" : "turn",
            windowTurns: retainFullWindow
              ? (pluginConfig.retainEveryNTurns ?? 1) + (pluginConfig.retainOverlapTurns ?? 0)
              : undefined,
            tags: [
              ...inlineRetainTags,
              ...(ledgerTopicTags ?? topicTags(getTurnClassification().topics)),
            ],
            appendSupported: supportsUpdateModeAppend,
            operationId: createAsyncRetainOperationId(),
          }
        );

        // Retain to Hindsight
        debug(
          `[Hindsight] Retaining to bank ${bankId}, document: ${retainRequest.documentId}, chars: ${transcript.length}\n---\n${transcript.substring(0, 500)}${transcript.length > 500 ? "\n...(truncated)" : ""}\n---`
        );

        const retainStart = pluginConfig.debugPerfTiming ? Date.now() : 0;

        const onRetainSettled = (outcome: "ok" | "queued" | "error") => {
          if (pluginConfig.debugPerfTiming) {
            log.info(
              formatHookPerf(hookName, Date.now() - perfHookStart, {
                retain: `${pluginConfig.debugPerfTiming ? Date.now() - retainStart : 0}ms`,
                outcome,
                bank: bankId,
                messages: messageCount,
              })
            );
          }
        };
        const onRetainOk = () => {
          if (!retainLifecycleIsCurrent()) return;
          log.trackRetain(bankId, messageCount);
          debug(
            `[Hindsight] Retained ${messageCount} messages to bank ${bankId} for session ${retainRequest.documentId}`
          );

          // Advance the durable marker (plan v4): the position moves only after
          // a successful retain, so a crash or a failed send re-offers the same
          // delta next turn — no duplicates, no gaps.
          if (markerAdvance) {
            const advance = markerAdvance;
            // Advance the durable marker (merge-safe + best-effort). The commit
            // path owns lastGenerationSeen, so this write does not touch it.
            tryUpdateSessionState(advance.sessionKey, (s) => ({
              ...s,
              lastRetainedSeq: advance.seq,
              lastRetainedAt: Date.now(),
              chunkHashes: [...s.chunkHashes, ...advance.hashes],
              // Clear only the drained ranges (end <= the advanced seq); a turn
              // committed during the retain RPC must survive (review 2026-09-13, Q1 blocker).
              pendingRetain: advance.clearPending
                ? s.pendingRetain.filter((r) => r.end > advance.seq)
                : s.pendingRetain,
            }));
            debug(
              `[Hindsight Hook] marker advanced to seq ${advance.seq} (+${advance.hashes.length} hashes)`
            );
          }
          // After a successful retain, try flushing any queued items
          if (retainQueue) {
            flushRetainQueue(undefined, undefined, undefined, retainGeneration, retainSignal).catch(
              () => {}
            );
          }
          onRetainSettled("ok");
        };
        const onRetainError = (retainError: unknown) => {
          if (!retainLifecycleIsCurrent()) return;
          // Queue the failed retain for later delivery
          if (retainQueue) {
            retainQueue.enqueue(bankId, retainRequest, retainRequest.metadata);
            const pending = retainQueue.size();
            log.warn(
              `API unreachable — retain queued (${pending} pending, bank: ${bankId}): ${retainError instanceof Error ? retainError.message : retainError}`
            );
            onRetainSettled("queued");
          } else {
            log.error("error retaining messages", retainError);
            onRetainSettled("error");
          }
        };

        // An unknown capability does not hold up the first send: there is
        // nothing on the server yet for it to duplicate, so omitting the wire
        // field is exactly today's behaviour. The id is still allocated and
        // persisted with the request, so a *replay* can be idempotent once the
        // capability is known — that is where duplicates actually come from.
        //
        // retainNonBlocking (opt-in) skips the await on agent_end only, so a
        // slow retain RPC doesn't hold up the hook return on every turn.
        // session_end always awaits: it is the last chance to flush a short
        // conversation before the process may exit (#1726), and an abandoned
        // fire-and-forget promise there would silently drop it.
        if (pluginConfig.retainNonBlocking && hookName === "agent_end") {
          client
            .retain(retainRequest, retainOperationIdCapability, retainSignal)
            .then(onRetainOk, onRetainError);
        } else {
          try {
            await client.retain(retainRequest, retainOperationIdCapability, retainSignal);
            onRetainOk();
          } catch (retainError) {
            onRetainError(retainError);
          }
        }
      } catch (error) {
        log.error("error retaining messages", error);
      }
    };

    // Hook signature: (event, ctx) where event has {messages, success, error?, durationMs?}
    api.on("agent_end", async (event: any, ctx?: PluginHookAgentContext) => {
      await runRetain(event, ctx, { hookName: "agent_end" });
      runAstinusTrigger(event, ctx);
    });

    // session_end fires once per OpenClaw session close. We force-flush so
    // short conversations (fewer turns than `retainEveryNTurns`) and the
    // un-retained tail of long conversations are not silently dropped. (#1726)
    api.on("session_end", async (event: any, ctx?: PluginHookAgentContext) => {
      await runRetain(event, ctx, { hookName: "session_end", force: true });
    });
    // ============================================================
    // Deployment customizations for this OpenClaw workspace. Registered as
    // independent hook handlers so they never touch Hindsight's own hook
    // logic above — OpenClaw runs multiple handlers per hook sequentially
    // and merges their results (prependContext/prependSystemContext/etc.
    // are concatenated, see mergeBeforePromptBuild in openclaw's hooks.ts),
    // so this is safe to run alongside the native recall/retain handlers
    // and never conflicts with them on rebase.
    // ============================================================
    const startupMandatedSessions = new Set<string>();
    // PER-SESSION ENRICH (edit 3/3, v4): module-scope Maps — survives entry-function
    // re-runs through jiti's module cache (the retain counter's pattern). The old
    // single variable was closure-scoped inside the register function and reset
    // to 0 on every invocation (Claude's re-review finding, 11:46).
    const astinusLastSpawnBySession = new Map<string, number>();
    const astinusActiveSpawns = new Map<number, number>(); // childPid -> startMs
    const ASTINUS_DEBOUNCE_MS = 90_000;
    const ASTINUS_SPAWN_CAP = 6;
// Plan v4.2 SS9.2: the enrich delta rule's lookback - a topic counts as novel
// when the newest classified turn introduces one unseen in the last K turns.
const ASTINUS_DELTA_TURNS = 8;
    const ASTINUS_SPAWN_GC_MS = 120_000;

    // ============================================================
    // SUIT ROUTER (build step 2, SUIT-ARCHITECTURE-PLAN v3):
    // session-key -> suit via workspace/suits/registry.json.
    //   before_prompt_build -> appendSystemContext (suit AGENTS.md/SOUL.md)
    //                          + toolsAllow (suit tools.json, opt-in)
    //   before_model_resolve -> modelOverride (suit MODEL.md `primary:`)
    // Exact key wins; then most-specific wildcard (fewest *, longest); then "*".
    // MAIN-AGENT ONLY (agent:main:*) — the fallback must not leak onto other
    // agents' sessions. One log line per resolution; missing files = no-op.
    // ============================================================
    const SUITS_ROOT = `${WORKSPACE_ROOT}\\workspace\\suits`;
    let suitRegistryCache: { mtimeMs: number; data: Record<string, string> } | null = null;

    const loadSuitRegistry = async (): Promise<Record<string, string>> => {
      const rp = join(SUITS_ROOT, "registry.json");
      try {
        const st = await stat(rp);
        if (suitRegistryCache && suitRegistryCache.mtimeMs === st.mtimeMs)
          return suitRegistryCache.data;
        const raw = JSON.parse(await readFile(rp, "utf8"));
        const data: Record<string, string> = {};
        for (const [k, v] of Object.entries(raw)) {
          if (k.startsWith("_") || typeof v !== "string") continue;
          data[k] = v;
        }
        suitRegistryCache = { mtimeMs: st.mtimeMs, data };
        return data;
      } catch {
        return {};
      }
    };

    const globToRegExp = (pat: string): RegExp =>
      new RegExp("^" + pat.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, "[^:]*") + "$");

    const resolveSuit = (
      registry: Record<string, string>,
      sessionKey: string
    ): { suit: string; pattern: string } | null => {
      if (!sessionKey) return null;
      if (registry[sessionKey]) return { suit: registry[sessionKey], pattern: sessionKey };
      let best: { suit: string; pattern: string; score: number } | null = null;
      for (const [pat, suit] of Object.entries(registry)) {
        if (pat === "*" || !pat.includes("*")) continue;
        if (!globToRegExp(pat).test(sessionKey)) continue;
        const score = pat.length - (pat.match(/\*/g) || []).length * 2;
        if (!best || score > best.score) best = { suit, pattern: pat, score };
      }
      if (best) return { suit: best.suit, pattern: best.pattern };
      if (registry["*"]) return { suit: registry["*"], pattern: "*" };
      return null;
    };

    const suitDirFor = (suit: string) =>
      join(SUITS_ROOT, suit.replace(/^[\\/]?suits[\\/]/, "").replace(/\\/g, "/"));

    // Error-to-inject (Vern 19:34): failures worth knowing immediately go to the
    // inject mailbox; the drain delivers them next turn and deletes the file.
    const writeInjectError = async (source: string, message: string): Promise<void> => {
      try {
        const dir = `${WORKSPACE_ROOT}\\workspace\\inject`;
        try {
          mkdirSync(dir, { recursive: true });
        } catch {}
        const stamp = new Date().toISOString().replace(/[:.]/g, "-");
        const safe = source.replace(/[^a-zA-Z0-9_-]/g, "-").slice(0, 40);
        await writeFile(
          join(dir, `err-${safe}-${stamp}-${Math.floor(Math.random() * 100000)}.md`),
          `## Error: ${source}\n**When:** ${new Date().toISOString()}\n\n\`\`\`\n${String(
            message
          ).slice(0, 2000)}\n\`\`\`\n`,
          "utf8"
        );
      } catch {}
    };

    api.on("before_prompt_build", async (event: any, ctx?: PluginHookAgentContext) => {
      // CANARY (probe finding): zero router log lines at debug level — the handlers
      // may never fire, or sessionKey may be unpopulated in this phase (it is OPTIONAL
      // on the hook context). Unconditional warn line BEFORE any gate settles it.
      log.warn(
        `[suit-router] canary(prompt): ctx.sessionKey=${JSON.stringify((ctx as any)?.sessionKey ?? null)} eventKeys=${Object.keys(event ?? {}).join("|")}`
      );
      try {
        const sessionKey =
          ctx?.sessionKey || (typeof event?.sessionKey === "string" ? event.sessionKey : "");
        if (!sessionKey || !sessionKey.startsWith("agent:main:")) return;
        if (sessionKey.includes(":subagent:")) return; // delegates get the suit at spawn, or none (plan v3)
        const registry = await loadSuitRegistry();
        const resolved = resolveSuit(registry, sessionKey);
        if (!resolved) return;
        log.info(`[suit-router] ${sessionKey} -> ${resolved.suit} (match: ${resolved.pattern})`);
        const dir = suitDirFor(resolved.suit);
        const parts: string[] = [];
        for (const f of ["AGENTS.md", "SOUL.md"]) {
          const content = await readFile(join(dir, f), "utf8").catch(() => null);
          if (content && content.trim())
            parts.push(`<suit_file name="${f}">\n${content.trim()}\n</suit_file>`);
        }
        const result: any = {};
        if (parts.length)
          result.appendSystemContext = `<suit name="${resolved.suit}">\n${parts.join("\n")}\n</suit>`;
        const toolsRaw = await readFile(join(dir, "tools.json"), "utf8").catch(() => null);
        if (toolsRaw) {
          try {
            const t = JSON.parse(toolsRaw);
            if (Array.isArray(t) && t.length > 0) result.toolsAllow = t;
          } catch {}
        }
        return result;
      } catch (e) {
        log.warn(`[suit-router] before_prompt_build failed: ${e}`);
        void writeInjectError("suit-router-prompt", String((e as any)?.stack || e));
      }
    });

    api.on("before_model_resolve", async (event: any, ctx?: PluginHookAgentContext) => {
      // CANARY (probe finding): see above. If this line never appears for a session
      // while the prompt canary does, the blocker is modelSelectionLocked upstream
      // (setup.ts returns before running this hook when model selection is locked).
      log.warn(
        `[suit-router] canary(model): ctx.sessionKey=${JSON.stringify((ctx as any)?.sessionKey ?? null)}`
      );
      try {
        const sessionKey =
          ctx?.sessionKey || (typeof event?.sessionKey === "string" ? event.sessionKey : "");
        if (!sessionKey || !sessionKey.startsWith("agent:main:")) return;
        if (sessionKey.includes(":subagent:")) return; // delegates get the suit at spawn, or none (plan v3)
        const registry = await loadSuitRegistry();
        const resolved = resolveSuit(registry, sessionKey);
        if (!resolved) return;
        const modelMd = await readFile(join(suitDirFor(resolved.suit), "MODEL.md"), "utf8").catch(
          () => null
        );
        if (!modelMd) return;
        const m = modelMd.match(/^primary:\s*(\S+)\s*$/m);
        if (m) {
          // The gateway applies modelOverride with the provider UNCHANGED unless
          // providerOverride is set (fork: run/setup.ts resolveHookModelSelection).
          // A bare "provider/model" string would ask the current provider (zai) for a
          // google model and fall through — split the ref (review finding 1).
          const ref = m[1];
          const slash = ref.indexOf("/");
          const provider = slash > 0 ? ref.slice(0, slash) : "";
          const modelId = slash > 0 ? ref.slice(slash + 1) : ref;
          const override: any = { modelOverride: modelId };
          if (provider) override.providerOverride = provider;
          log.info(
            `[suit-router] ${sessionKey} model -> ${provider ? provider + "/" : ""}${modelId} (suit: ${resolved.suit})`
          );
          // plugin-sdk type lag: the gateway honors modelOverride/providerOverride on
          // before_model_resolve (verified in the fork source) but the SDK .d.ts
          // narrows the result type — cast through any; inert if unsupported.
          return override;
        }
      } catch (e) {
        log.warn(`[suit-router] before_model_resolve failed: ${e}`);
        void writeInjectError("suit-router-model", String((e as any)?.stack || e));
      }
    });

    api.on("before_prompt_build", async (event: any, ctx?: PluginHookAgentContext) => {
      const prependParts: string[] = [];

      try {
        // Inject folder drain: reads workspace/inject/, injects as <injected file>,
        // deletes consumed files.
        const injectDir = `${WORKSPACE_ROOT}\\workspace\\inject`;
        const injectExists = await access(injectDir).then(() => true).catch(() => false);
        if (injectExists) {
          const files = (await readdir(injectDir, { withFileTypes: true })).filter(
            (e) => !e.name.startsWith(".")
          );
          const drainOne = async (name: string): Promise<{ name: string; content: string } | null> => {
            const fp = join(injectDir, name);
            try {
              const content = await readFile(fp, "utf8");
              await unlink(fp);
              return { name, content };
            } catch (e) {
              debug(`[Hindsight customizations] failed to drain ${name}: ${e}`);
              return null;
            }
          };
          // PER-SESSION ENRICH (edit 3/3): handle the astinus/ subdirectory —
          // keyed files deliver ONLY to their matching session, then delete.
          // Other directories are skipped (never readFile a directory entry).
          const drained: ({ name: string; content: string } | null)[] = [];
          for (const entry of files) {
            if (entry.isDirectory()) {
              if (entry.name === "astinus") {
                const sk = ctx?.sessionKey || "";
                if (sk) {
                  // IDENTICAL sanitizer to enrich.py: [^a-zA-Z0-9_.-], 120-char cap
                  const safe = sk.replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 120);
                  const keyed = join(injectDir, "astinus", `${safe}.md`);
                  try {
                    const content = await readFile(keyed, "utf8");
                    await unlink(keyed);
                    drained.push({ name: `astinus/${safe}.md`, content });
                  } catch (_) {
                    // no file for this session — normal, not an error
                  }
                  // sweep: delete keyed files older than 6h (same cutoff as enrich.py)
                  try {
                    const astinusDir = join(injectDir, "astinus");
                    const stale = (await readdir(astinusDir)).filter((f) => f.endsWith(".md"));
                    const cutoff = Date.now() - 6 * 3600 * 1000;
                    for (const f of stale) {
                      const fp = join(astinusDir, f);
                      const s = await stat(fp).catch(() => null);
                      if (s && s.mtimeMs < cutoff) await unlink(fp).catch(() => {});
                    }
                  } catch (_) {}
                }
              }
              continue; // never readFile a directory entry directly
            }
            // Double-delivery fix (2026-09-13): astinus-context.md is the
            // FALLBACK for sessions without a session key. A keyed session gets
            // its own file under astinus/, so delivering the flat one too would
            // inject the same content twice (double tokens every turn).
            if (entry.name === "astinus-context.md" && ctx?.sessionKey) {
              continue;
            }
            drained.push(await drainOne(entry.name));
          }
          for (const d of drained) {
            if (d)
              prependParts.push(
                `<injected file="${d.name}">\n${d.content}\n</injected file="${d.name}">`
              );
          }
        }
      } catch (e) {
        debug(`[Hindsight customizations] inject drain failed: ${e}`);
      }

      try {
        // Heartbeat injection: for heartbeat sessions, inject heartbeat-full.md.
        const sessionKey =
          ctx?.sessionKey || (typeof event?.sessionKey === "string" ? event.sessionKey : "");
        if (sessionKey.includes(":heartbeat")) {
          const hbPath = `${WORKSPACE_ROOT}\\workspace\\heartbeat-full.md`;
          if (existsSync(hbPath)) {
            const hbContent = await readFile(hbPath, "utf8");
            if (hbContent.trim()) {
              prependParts.push(
                `<injected file="heartbeat-full.md">\n${hbContent}\n</injected file="heartbeat-full.md">`
              );
            }
          }
        }
      } catch (e) {
        debug(`[Hindsight customizations] heartbeat inject failed: ${e}`);
      }

      try {
        // Startup mandate: on first turn of a new session, inject grounding instructions.
        const sessionKey =
          ctx?.sessionKey || (typeof event?.sessionKey === "string" ? event.sessionKey : undefined);
        if (sessionKey && !startupMandatedSessions.has(sessionKey)) {
          startupMandatedSessions.add(sessionKey);
          prependParts.push(
            [
              "<startup_mandate>",
              "Grounding context for this session:",
              "1. Read todos.md, NEXT_SESSION.md, and gateway.md before responding.",
              "2. These files are the canonical handoff between sessions.",
              "3. Verify everything. LLMs hallucinate directories, files, tool availability.",
              "</startup_mandate>",
            ].join("\n")
          );
        }
      } catch (e) {
        debug(`[Hindsight customizations] startup mandate failed: ${e}`);
      }

      try {
        // Wisdom sidecar: recall from the shared "openclaw" bank for derived
        // principles, 500ms hard timeout + 30-min cache.
        const clientGlobal = (global as any).__hindsightClient;
        const prompt = typeof event?.prompt === "string" ? event.prompt : "";
        if (clientGlobal && prompt) {
          const CACHE_TTL_MS = 30 * 60 * 1000;
          const HARD_TIMEOUT_MS = 500;
          const now = Date.now();
          const cacheKey = prompt.substring(0, 200);
          if (!(global as any).__hindsightWisdomCache)
            (global as any).__hindsightWisdomCache = new Map();
          const cache = (global as any).__hindsightWisdomCache as Map<
            string,
            { results: any[]; timestamp: number }
          >;
          const cached = cache.get(cacheKey);
          let wisdomResults: any[] | undefined;
          if (cached && now - cached.timestamp < CACHE_TTL_MS) {
            wisdomResults = cached.results;
          } else {
            try {
              await clientGlobal.waitForReady();
              const client = await clientGlobal.getClientForContext(ctx);
              if (client) {
                const wc = scopeClient(client, "openclaw");
                const resp = await Promise.race([
                  wc.recall({ query: cacheKey.substring(0, 400), maxTokens: 512 }),
                  new Promise<never>((_, rej) =>
                    setTimeout(() => rej(new Error("wisdom timeout 500ms")), HARD_TIMEOUT_MS)
                  ),
                ]);
                wisdomResults = ((resp as any)?.results ?? [])
                  .filter(
                    (r: any) =>
                      r.tags?.some((t: string) => t === "type:derived_principles") ||
                      r.context === "derived_learnings"
                  )
                  .slice(0, 5);
                cache.set(cacheKey, { results: wisdomResults ?? [], timestamp: now });
              }
            } catch (wte) {
              debug(`[Hindsight customizations] wisdom query failed or timed out: ${wte}`);
            }
          }
          if (wisdomResults && wisdomResults.length > 0) {
            prependParts.push(
              `<wisdom_context>\nDerived principles (${wisdomResults.length}):\n${wisdomResults
                .map((r: any) => `- ${r.content ?? r.text ?? r}`)
                .join("\n")}\n</wisdom_context>`
            );
          }
        }
      } catch (e) {
        debug(`[Hindsight customizations] wisdom sidecar failed: ${e}`);
      }

      try {
        // Interrupt check: spawns a deployment-specific Python script, 5s hard
        // kill cap so a hung/slow script never blocks the hook indefinitely.
        const sessionKey =
          ctx?.sessionKey || (typeof event?.sessionKey === "string" ? event.sessionKey : "unknown");
        const interruptScript = `${WORKSPACE_ROOT}\\workspace\\skills\\subagent-interrupt\\interrupt_check.py`;
        if (existsSync(interruptScript)) {
          const proc = spawn("python", [interruptScript, sessionKey], {
            stdio: ["ignore", "pipe", "ignore"],
            windowsHide: true,
          });
          let output = "";
          proc.stdout?.on("data", (d: Buffer) => {
            output += d.toString();
          });
          await new Promise<void>((resolve) => {
            const killTimer = setTimeout(() => {
              proc.kill();
              resolve();
            }, 5000);
            proc.on("close", () => {
              clearTimeout(killTimer);
              resolve();
            });
            proc.on("error", () => {
              clearTimeout(killTimer);
              resolve();
            });
          });
          const msg = output.trim();
          if (msg) {
            prependParts.push(`<system_interrupt>\n${msg}\n</system_interrupt>`);
          }
        }
      } catch (e) {
        debug(`[Hindsight customizations] interrupt check failed: ${e}`);
      }

      if (prependParts.length === 0) return;
      return { prependContext: prependParts.join("\n\n") };
    });

    // Astinus enrichment trigger: after a turn completes, spawn the enrichment
    // script (async, detached). PID-lockfile-style cooldown (90s) guards
    // against overlapping runs racing on the same output file. Called from
    // the native agent_end handler below (not a second api.on("agent_end", ...)
    // registration) — some hosts/test harnesses only keep the last handler
    // registered per event, so composing into the existing one is more robust
    // than relying on multi-handler merge support.
    const runAstinusTrigger = (event: any, ctx?: PluginHookAgentContext) => {
      try {
        const sessionKey =
          ctx?.sessionKey || (typeof event?.sessionKey === "string" ? event.sessionKey : "");
        // PER-SESSION ENRICH (review finding 3): gate on the CHANNEL segment, not
        // ':main:' — the old test matched the agent-id segment and admitted
        // subagent/cron/heartbeat sessions too (live log counts confirmed).
        // Desks = the main chat, telegram topics, and dashboard sessions.
        const seg = sessionKey.split(":");
        const isDeskSession =
          sessionKey === "agent:main:main" ||
          (seg[1] === "main" && (seg[2] === "telegram" || seg[2] === "dashboard"));
        if (!isDeskSession) return;
        const now = Date.now();
        // PER-SESSION ENRICH (edit 3/3, v4): per-session debounce + spawn cap.
        // Module-scope Maps persist through jiti's cache (the retain counter's pattern).
        const last = astinusLastSpawnBySession.get(sessionKey) ?? 0;
        if (now - last < ASTINUS_DEBOUNCE_MS) return;
        // GC dead spawns (killed/crashed enrich.py, >120s old)
        for (const [pid, startMs] of astinusActiveSpawns) {
          if (now - startMs > ASTINUS_SPAWN_GC_MS) astinusActiveSpawns.delete(pid);
        }
        if (astinusActiveSpawns.size >= ASTINUS_SPAWN_CAP) return;
        const enrichScript = `${WORKSPACE_ROOT}\\workspace\\skills\\astinus\\enrich.py`;
        if (!existsSync(enrichScript)) return;
        // DELTA RULE (plan v4.2 SS9.2): enrich fires on topic NOVELTY, not the
        // clock alone. The ledger's newest classified turn vs the previous K:
        // skip when every topic is already seen AND the top topic is unchanged.
        // A skipped check does NOT consume the debounce window (last-spawn is
        // only stamped on a real spawn). No ledger / no classified turns yet ->
        // fire, exactly as before (young sessions still get their first enrich).
        const delta = readTopicDelta(sessionKey, ASTINUS_DELTA_TURNS);
        if (delta && delta.newest.length > 0) {
          const novel = delta.newest.filter((t) => !delta.recent.includes(t));
          const topChanged = delta.prevTop != null && delta.newest[0] !== delta.prevTop;
          if (novel.length === 0 && !topChanged) {
            debug(
              `[Hindsight customizations] enrich skipped: no topic delta (top=${delta.newest[0]}, ${delta.recent.length} recent)`
            );
            return;
          }
          log.info(
            `astinus enrich: delta fire (novel: ${novel.join(", ") || "none"}; top ${delta.newest[0]})`
          );
        }
        astinusLastSpawnBySession.set(sessionKey, now);
        // Build the JSON payload enrich.py reads on stdin (topics + last turn text).
        let astinusPayload = "{}";
        try {
          const msgs: any[] =
            (event as any)?.context?.sessionEntry?.messages ?? (event as any)?.messages ?? [];
          const textOf = (m: any): string => {
            const c = m?.content;
            if (typeof c === "string") return c.trim();
            if (Array.isArray(c)) {
              return c
                .map((p: any) => (typeof p === "string" ? p : typeof p?.text === "string" ? p.text : ""))
                .join("\n")
                .trim();
            }
            return "";
          };
          let lastUser = "";
          let lastAssistant = "";
          for (let i = msgs.length - 1; i >= 0; i--) {
            const m = msgs[i];
            if (!m) continue;
            if (m.role === "user" && !lastUser) lastUser = textOf(m);
            if (m.role === "assistant" && !lastAssistant) lastAssistant = textOf(m);
            if (lastUser && lastAssistant) break;
          }
          let topics: string[] = [];
          try {
            const cls: any = classifyTurn(`${lastUser}\n${lastAssistant}`.slice(0, 2000));
            if (cls && Array.isArray(cls.topics)) topics = cls.topics.slice(0, 6);
          } catch (_) {}
          if (!topics.length) {
            const stop = new Set(["this", "that", "with", "from", "have", "will", "your", "what", "when", "were", "they", "them", "then", "than", "been", "into", "over", "just", "some", "more", "about", "these", "those", "which", "there", "their", "would", "could", "should", "please", "make", "sure", "like", "also", "only", "very", "much", "need"]);
            topics = Array.from(
              new Set(
                (lastUser.toLowerCase().match(/[a-z0-9][a-z0-9_.-]{3,}/g) || []).filter((w: string) => !stop.has(w))
              )
            ).slice(0, 5);
          }
          // Ledger-first topics (plan SS9.2): when the delta read classified
          // topics, they lead the payload (labels = slugs dashed back to words);
          // the regex topics follow, deduped, capped.
          if (delta && delta.newest.length > 0) {
            const ledgerTopics = delta.newest.map((s) => s.replace(/-/g, " "));
            topics = Array.from(new Set([...ledgerTopics, ...topics])).slice(0, 6);
          }
          astinusPayload = JSON.stringify({
            topics,
            // Ruling 2026-09-17: instances ride WITH their topic - the live topics'
          // subject/predicate roster travels the payload in parallel with recall,
          // so instance specificity survives the topic zoom-out without
          // polluting the topic vocabulary.
          topic_subjects: readTopicSubjects(sessionKey, 12),
          last_user_message: lastUser.slice(0, 800),
            conversation_summary: `${lastUser.slice(0, 300)}\n---\n${lastAssistant.slice(0, 500)}`.trim(),
            active_tasks: [],
            // PER-SESSION ENRICH (edit 1/3): carry the session identity so enrich.py
            // can key its output file. Agent id derives from the key (agent:<id>:...).
            session_key: sessionKey,
          });
        } catch (e) {
          debug(`[Hindsight customizations] Astinus payload build failed: ${e}`);
        }
        const child = spawn("python", [enrichScript], {
          stdio: ["pipe", "ignore", "ignore"],
          windowsHide: true,
          detached: true,
        });
        // PER-SESSION ENRICH (edit 3/3): track the spawn for the concurrent cap
        if (child.pid) {
          astinusActiveSpawns.set(child.pid, now);
          child.on("exit", () => astinusActiveSpawns.delete(child.pid!));
        }
        child.on("error", (err: any) => {
          debug(`[Hindsight customizations] Astinus spawn error: ${err.message}`);
        });
        child.unref();
        try {
          if (child.stdin) {
            child.stdin.write(astinusPayload);
            child.stdin.end();
          }
        } catch (_) {}
      } catch (e) {
        log.warn(`[Hindsight customizations] Astinus trigger failed: ${e}`);
        void writeInjectError("astinus-trigger", String((e as any)?.stack || e));
      }
    };

    debug("[Hindsight] Hooks registered");
    log.info("agent hooks registered");

    // Register knowledge tools (opt-in via enableKnowledgeTools config flag)
    if (pluginConfig.enableKnowledgeTools && typeof api.registerTool === "function") {
      try {
        const apiUrl = (() => {
          const ext = detectExternalApi(pluginConfig);
          return ext?.apiUrl || `http://localhost:${pluginConfig.apiPort || 9077}`;
        })();
        const apiToken = pluginConfig.hindsightApiToken || undefined;

        // Factory: called per session with agent context, returns tools scoped to that bank.
        // Identity is resolved the same way as auto-recall/retain so PluginToolContext
        // (which lacks senderId/messageProvider) still routes to the per-user bank.
        const factory = (ctx: PluginToolContext) => {
          const resolution = resolveBankIdForKnowledgeTools(ctx, pluginConfig);
          const tools = createKnowledgeTools({
            apiUrl,
            apiToken,
            bankId: resolution.bankId,
          });
          return tools.map((t) => ({
            name: t.name,
            label: t.label,
            description: t.description,
            parameters: t.parameters,
            async execute(_id: string, params: Record<string, unknown>) {
              if (resolution.identityError) {
                return {
                  content: [{ type: "text", text: resolution.identityError }],
                  details: {},
                };
              }
              const config = currentPluginConfig || pluginConfig;
              await ensureBankDefaultsApplied(resolution.bankId, config);
              return { ...(await t.execute(params)), details: {} };
            },
          }));
        };

        api.registerTool(factory, {
          names: [...TOOL_NAMES],
          optional: false,
        });
        log.info("knowledge tools registered");
      } catch (err) {
        log.warn(`knowledge tools registration failed: ${err}`);
      }
    }
  } catch (error) {
    log.error("plugin loading error", error);
    if (error instanceof Error) {
      log.error("error stack", error.stack);
    }
    throw error;
  }
}

// Export client getter for tools

function sanitizeDocumentIdPart(value: string | undefined, fallback: string): string {
  const normalized = (value || "").trim();
  if (!normalized) return fallback;
  return (
    normalized
      .replace(/[^a-zA-Z0-9:_-]+/g, "_")
      .replace(/_+/g, "_")
      .replace(/^_+|_+$/g, "") || fallback
  );
}

function getSessionDocumentBase(effectiveCtx: PluginHookAgentContext | undefined): string {
  const sessionKeyPart = sanitizeDocumentIdPart(effectiveCtx?.sessionKey, "session");
  return `openclaw:${sessionKeyPart}`;
}

function nextDocumentSequence(effectiveCtx: PluginHookAgentContext | undefined): number {
  const sequenceKey = effectiveCtx?.sessionKey || "session";
  const next = (documentSequenceBySession.get(sequenceKey) || 0) + 1;
  setCappedMapValue(documentSequenceBySession, sequenceKey, next);
  return next;
}

function extractThreadId(channelId: string | undefined): string | undefined {
  if (!channelId) return undefined;
  const match = channelId.match(/(?:^|:)topic:([^:]+)$/);
  return match?.[1];
}

export function buildRetainRequest(
  transcript: string,
  messageCount: number,
  effectiveCtx: PluginHookAgentContext | undefined,
  pluginConfig: PluginConfig,
  now = Date.now(),
  options?: {
    retentionScope?: "turn" | "window" | "manual";
    windowTurns?: number;
    turnIndex?: number;
    tags?: string[];
    /**
     * Whether the live Hindsight API supports `update_mode: 'append'`. When
     * true, the request gets a stable per-session document id and
     * `updateMode: 'append'` so each retain concatenates to the existing
     * document. When false, falls back to a unique per-turn document id so
     * prior turns aren't overwritten. Defaults to false (conservative).
     */
    appendSupported?: boolean;
    /** Stable UUID allocated before the initial asynchronous retain request. */
    operationId?: string;
  }
): RetainRequest {
  const resolvedCtx = resolveSessionIdentity(effectiveCtx);
  const parsedSession = resolvedCtx?.sessionKey ? parseSessionKey(resolvedCtx.sessionKey) : {};
  const turnIndex = options?.turnIndex ?? nextDocumentSequence(resolvedCtx);
  const retentionScope = options?.retentionScope || "turn";
  const documentBase = getSessionDocumentBase(resolvedCtx);
  const documentKind = retentionScope === "window" ? "window" : "turn";
  // Retains are session-scoped: all turns accumulate under one document id when
  // the API can append. On legacy APIs without append support, every retain on
  // the same id would silently overwrite prior turns (behavior pre-#932), so
  // fall back to per-turn ids there.
  const useSessionScopedDoc = options?.appendSupported === true;
  // The fallback id carries a per-process boot token because `turnIndex` comes
  // from an in-memory counter: without it, a host restart replays
  // `…:turn:000001` onto the previous cycle's document and `update_mode:
  // 'replace'` deletes what was there. (#3686)
  const documentId = useSessionScopedDoc
    ? documentBase
    : `${documentBase}:${documentKind}:${getDocumentIdBootToken()}:${String(turnIndex).padStart(6, "0")}`;
  const provider = effectiveCtx?.messageProvider || parsedSession.provider;
  const channelId = sanitizeChannelId(effectiveCtx?.channelId, provider) || parsedSession.channel;
  const channelType = effectiveCtx?.messageProvider;
  const threadId = extractThreadId(channelId);
  const mergedTags = normalizeRetainTags([
    ...(pluginConfig.retainTags ?? []),
    ...(options?.tags ?? []),
  ]);

  return {
    content: transcript,
    documentId: documentId,
    context:
      typeof pluginConfig.retainContext === "string" && pluginConfig.retainContext.trim().length > 0
        ? pluginConfig.retainContext.trim()
        : DEFAULT_RETAIN_CONTEXT,
    metadata: {
      retained_at: new Date(now).toISOString(),
      message_count: String(messageCount),
      source: pluginConfig.retainSource || "openclaw",
      retention_scope: retentionScope,
      turn_index: String(turnIndex),
      session_key: resolvedCtx?.sessionKey,
      agent_id: resolvedCtx?.agentId || parsedSession.agentId,
      provider,
      channel_type: channelType,
      channel_id: channelId,
      thread_id: threadId,
      sender_id: resolvedCtx?.senderId,
      ...(options?.windowTurns !== undefined ? { window_turns: String(options.windowTurns) } : {}),
    },
    tags: mergedTags.length > 0 ? mergedTags : undefined,
    ...(options?.operationId ? { operationId: options.operationId } : {}),
    updateMode: useSessionScopedDoc ? "append" : undefined,
  };
}

export function prepareRetentionTranscript(
  messages: any[],
  pluginConfig: PluginConfig,
  retainFullWindow = false
): { transcript: string; messageCount: number } | null {
  if (!messages || messages.length === 0) {
    return null;
  }

  let targetMessages: any[];
  if (retainFullWindow) {
    // Chunked retention: retain the full sliding window (already sliced by caller)
    targetMessages = messages;
  } else {
    // Default: retain only the last turn (user message + assistant responses)
    let lastUserIdx = -1;
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i].role === "user") {
        lastUserIdx = i;
        break;
      }
    }
    if (lastUserIdx === -1) {
      return null; // No user message found in turn
    }
    targetMessages = messages.slice(lastUserIdx);
  }

  const format = pluginConfig.retainFormat ?? "json";
  const includeToolCalls = format === "json" && pluginConfig.retainToolCalls !== false;

  if (includeToolCalls) {
    const structured = buildAnthropicStructuredMessages(targetMessages, pluginConfig);
    if (structured.length === 0) return null;
    const transcript = JSON.stringify(structured);
    if (!transcript.trim() || transcript.length < 10) return null;
    return { transcript, messageCount: structured.length };
  }

  // Role filtering (text-only path)
  const allowedRoles = new Set(pluginConfig.retainRoles || ["user", "assistant"]);
  const filteredMessages = targetMessages.filter((m: any) => allowedRoles.has(m.role));

  if (filteredMessages.length === 0) {
    return null; // No messages to retain
  }

  const normalized: Array<{ role: string; content: string; timestamp?: string }> = [];
  for (const msg of filteredMessages) {
    const role = msg.role || "unknown";
    let content = "";

    if (typeof msg.content === "string") {
      content = msg.content;
    } else if (Array.isArray(msg.content)) {
      content = msg.content
        .filter((block: any) => block.type === "text")
        .map((block: any) => block.text)
        .join("\n");
    }

    content = stripMemoryTags(content);
    content = stripInlineRetainTags(content);
    content = stripMetadataEnvelopes(content);
    content = stripInlineTimestampPrefix(content);
    content = stripRuntimeEnvelope(content).trim();

    if (content.trim()) {
      const timestamp = normalizeMessageTimestamp(msg);
      normalized.push(timestamp ? { role, content, timestamp } : { role, content });
    }
  }

  if (normalized.length === 0) return null;

  let transcript: string;
  let messageCount: number;
  if (format === "text") {
    transcript = normalized
      .map(({ role, content }) => `[role: ${role}]\n${content}\n[${role}:end]`)
      .join("\n\n");
    messageCount = normalized.length;
  } else {
    transcript = JSON.stringify(normalized);
    messageCount = normalized.length;
  }

  if (!transcript.trim() || transcript.length < 10) return null;

  return { transcript, messageCount };
}

// MCP tool name suffixes that are operational (recall/retain/search/CRUD) and
// shouldn't be retained — preserves agent reasoning without creating feedback
// loops on Hindsight's own MCP surface. Mirrors the claude-code integration.
const OPERATIONAL_TOOL_PATTERN =
  /(?:recall|retain|reflect|search|extract|create_|delete_|update_|get_|list_)/i;
const TOOL_RESULT_MAX_CHARS = 2000;

/**
 * Build an Anthropic-shaped message array from OpenClaw's session messages.
 *
 * OpenClaw stores assistant content as a block array that may contain
 * `text`, `thinking`, and `toolCall` entries, and emits tool results as
 * separate messages with `role: "toolResult"`. The Anthropic wire format
 * expected by Hindsight's Claude Code integration (and downstream consumers)
 * is: assistant messages carry `text` and `tool_use` blocks, and tool
 * results live in a following `user` message as `tool_result` blocks.
 * We translate to that shape here so stored documents are consistent
 * across integrations.
 */
function buildAnthropicStructuredMessages(
  messages: any[],
  pluginConfig: PluginConfig
): Array<{ role: string; content: any[]; timestamp?: string }> {
  const allowedRoles = new Set(pluginConfig.retainRoles || ["user", "assistant"]);
  const out: Array<{ role: string; content: any[]; timestamp?: string }> = [];

  for (const msg of messages) {
    const rawRole = msg?.role;
    if (rawRole === "toolResult") {
      const toolResultBlock = buildToolResultBlock(msg);
      if (!toolResultBlock) continue;
      // Fold tool_result into a synthetic user message (Anthropic convention),
      // merging with an immediately-preceding synthetic user if one exists so
      // consecutive tool results stay together.
      const last = out[out.length - 1];
      if (
        last &&
        last.role === "user" &&
        last.content.every((b: any) => b.type === "tool_result")
      ) {
        last.content.push(toolResultBlock);
      } else {
        out.push({ role: "user", content: [toolResultBlock] });
      }
      continue;
    }

    if (!allowedRoles.has(rawRole)) continue;

    const blocks = extractStructuredBlocks(msg.content, rawRole);
    if (blocks.length > 0) {
      const timestamp = normalizeMessageTimestamp(msg);
      out.push(
        timestamp
          ? { role: rawRole, content: blocks, timestamp }
          : { role: rawRole, content: blocks }
      );
    }
  }

  return out;
}

function normalizeMessageTimestamp(msg: any): string | undefined {
  const raw = msg?.timestamp;
  if (raw === undefined || raw === null) return undefined;
  const date =
    typeof raw === "number" ? new Date(raw) : typeof raw === "string" ? new Date(raw) : undefined;
  if (!date || Number.isNaN(date.getTime())) return undefined;
  return date.toISOString();
}

function extractStructuredBlocks(content: any, role: string): any[] {
  if (typeof content === "string") {
    const cleaned = stripRuntimeEnvelope(
      stripInlineTimestampPrefix(
        stripMetadataEnvelopes(stripInlineRetainTags(stripMemoryTags(content)))
      )
    ).trim();
    return cleaned ? [{ type: "text", text: cleaned }] : [];
  }
  if (!Array.isArray(content)) return [];

  const blocks: any[] = [];
  for (const block of content) {
    if (!block || typeof block !== "object") continue;
    const blockType = block.type;

    if (blockType === "text") {
      const cleaned = stripRuntimeEnvelope(
        stripInlineTimestampPrefix(
          stripMetadataEnvelopes(stripInlineRetainTags(stripMemoryTags(block.text ?? "")))
        )
      ).trim();
      if (cleaned) blocks.push({ type: "text", text: cleaned });
    } else if (blockType === "toolCall" && role === "assistant") {
      const name = typeof block.name === "string" ? block.name : "unknown";
      // Skip Hindsight's own MCP operational tools to avoid feedback loops.
      if (name.startsWith("mcp__") && OPERATIONAL_TOOL_PATTERN.test(name.split("__").pop() ?? ""))
        continue;
      const input = block.arguments && typeof block.arguments === "object" ? block.arguments : {};
      const id = typeof block.id === "string" ? block.id : undefined;
      const toolUse: any = { type: "tool_use", name, input };
      if (id) toolUse.id = id;
      blocks.push(toolUse);
    }
    // thinking / unknown types are dropped
  }
  return blocks;
}

function buildToolResultBlock(msg: any): any | null {
  const toolUseId = typeof msg.toolCallId === "string" ? msg.toolCallId : "";
  const raw = msg.content;
  let text = "";
  if (typeof raw === "string") {
    text = raw;
  } else if (Array.isArray(raw)) {
    text = raw
      .filter((b: any) => b && b.type === "text" && typeof b.text === "string")
      .map((b: any) => b.text)
      .join("\n");
  }
  text = text.trim();
  if (!text) return null;
  if (text.length > TOOL_RESULT_MAX_CHARS) {
    text = text.slice(0, TOOL_RESULT_MAX_CHARS) + "... (truncated)";
  }
  const block: any = { type: "tool_result", content: text };
  if (toolUseId) block.tool_use_id = toolUseId;
  return block;
}

export function sliceLastTurnsByUserBoundary(messages: any[], turns: number): any[] {
  if (!Array.isArray(messages) || messages.length === 0 || turns <= 0) {
    return [];
  }

  // Count only user messages that contain actual text content.
  // OpenClaw normalizes tool_result blocks into role:"user" messages with a
  // tool_result content block. Without this filter those synthetic messages
  // would be counted as real user turns, causing the window to exclude actual
  // user input from the retained transcript.
  function hasRealTextContent(msg: any): boolean {
    if (msg?.role !== "user") return false;
    const content = msg.content;
    if (typeof content === "string") return content.trim().length > 0;
    if (Array.isArray(content)) {
      return content.some(
        (b: any) => b?.type === "text" && typeof b?.text === "string" && b.text.trim().length > 0
      );
    }
    return false;
  }

  let userTurnsSeen = 0;
  let startIndex = -1;

  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i]?.role === "user" && hasRealTextContent(messages[i])) {
      userTurnsSeen += 1;
      if (userTurnsSeen >= turns) {
        startIndex = i;
        break;
      }
    }
  }

  if (startIndex === -1) {
    return messages;
  }

  return messages.slice(startIndex);
}

export function getClient() {
  return client;
}
