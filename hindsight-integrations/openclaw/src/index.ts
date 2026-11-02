import type {
  MoltbotPluginAPI,
  PluginConfig,
  PluginHookAgentContext,
  PluginHookEndedTranscript,
  MemoryResult,
  RetainRequest,
} from "./types.js";
import { HindsightServer, type Logger } from "@vectorize-io/hindsight-all";
import {
  HindsightClient,
  type HindsightClientOptions,
  type MinScores,
} from "@vectorize-io/hindsight-client";
import { RetainQueue, sameCommittedWork } from "./retain-queue.js";
import { isTurnOwnedByEngine } from "./engine-compat.js";
import {
  appendPendingLegacy,
  applyCompletedRetain,
  loadSessionState,
  updateSessionState,
} from "./session-state.js";
import {
  createRetainTransport,
  reconcileRetainQueue,
  type RetainTransport,
} from "./retain-delivery.js";
import { createMemoryProviderRuntime } from "./memory-provider.js";
import { createHindsightTools, HINDSIGHT_TOOL_NAMES } from "./tools.js";
import { compileSessionPatterns, matchesSessionPattern } from "./session-patterns.js";
import { parseSessionFile } from "./session-file.js";
import { createHash, randomUUID } from "crypto";
import { dirname, join } from "path";
import * as log from "./logger.js";
import { configureLogger, setApiLogger, stopLogger } from "./logger.js";
import { mkdirSync } from "fs";
import { createRequire } from "module";
import { homedir } from "os";
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

// Capability detected once per service.start() against `<apiUrl>/version`.
// `true` when the Hindsight API supports `update_mode: 'append'` (added in
// 0.5.0 — see vectorize-io/hindsight#932) and stores document text. When false,
// retain falls back to a per-turn document id so prior turns aren't silently
// overwritten or rejected by text-disabled deployments.
const MIN_VERSION_FOR_UPDATE_MODE_APPEND = "0.5.0";

/** Whether retain is currently using session-scoped documents + `update_mode: 'append'`. */
export function isAppendModeSupported(): boolean {
  return currentLifecycle().supportsUpdateModeAppend;
}

export type AsyncRetainOperationIdCapability = "supported" | "unsupported" | "unknown";
const MIN_VERSION_FOR_ASYNC_RETAIN_OPERATION_ID = "0.8.6";

// Store the current plugin config for bank ID derivation
// P3: the service lifecycle, client, init and retain queue live in the per-host-root authority
// (see lifecycle-authority.ts). R14.1: each registration binds its OWN authority - never a
// module-wide pointer a later registration could retarget. Its service, hooks, memory runtime and
// tools run inside lifecycleScope, so module-level helpers resolve that registration's authority.
// Registrations without a resolvable state root share this copy's private unrooted record (never a
// rooted one). Calls outside any registration scope (tests, the global facade from a foreign
// caller) fall back to the most recent registration's authority, as before.
const unrootedLifecycle: LifecycleAuthority = newLifecycleAuthority();
let lastRegisteredLifecycle: LifecycleAuthority = unrootedLifecycle;
const lifecycleScope = sharedLifecycleScope();
function currentLifecycle(): LifecycleAuthority {
  return lifecycleScope.getStore() ?? lastRegisteredLifecycle;
}
/** Run fn (now and for every async continuation) under one registration's authority. */
function bindLifecycle<A extends unknown[], R>(
  authority: LifecycleAuthority,
  fn: (...args: A) => R
): (...args: A) => R {
  return (...args: A) => lifecycleScope.run(authority, () => fn(...args));
}
// External-API hooks can lazy-initialize before the first service.start(). Keep
// that lifetime cancellable: the first start() or stop() aborts it.
//
// It covers retain as well as recall. It used to be recall-only, but OpenClaw
// re-evaluates this module for each plugin registry it loads and only ever calls
// service.start() on the first one, so the hooks that actually run often belong
// to a copy that is never started. Recall worked there and retain returned at its
// lifecycle guard without a log line, so memory silently stopped growing. (#4828)

/**
 * Snapshot the lifecycle a hook runs under: the started service's, or the
 * pre-start one when no copy for this host root has been started. `isCurrent()` turns
 * false once a later start() or stop() replaces it, so in-flight work can bail.
 * After an explicit stop() there is no lifecycle and hooks do nothing.
 */
function captureHookLifecycle(): { signal: AbortSignal | undefined; isCurrent: () => boolean } {
  const authority = currentLifecycle();
  const generation = authority.generation;
  const controller =
    authority.serviceController ?? (generation === 0 ? authority.preServiceController : null);
  return {
    signal: controller?.signal,
    isCurrent: () =>
      controller !== null &&
      (authority.serviceController ?? authority.preServiceController) === controller &&
      generation === authority.generation &&
      !controller.signal.aborted,
  };
}

// Track which banks have had configured defaults applied (missions + bank config).

// In-flight recall deduplication: concurrent recalls for the same bank reuse one promise
import type { RecallResponse } from "./types.js";
import { marksPolicyFromConfig, registerHindsightContextEngine } from "./context-engine.js";
import { resolveHostStateRoot, resolveRetainQueuePath } from "./host-paths.js";
import {
  lifecycleAuthorityFor,
  lifecycleScope as sharedLifecycleScope,
  newLifecycleAuthority,
  type LifecycleAuthority,
} from "./lifecycle-authority.js";
const inflightRecalls = new Map<string, Promise<RecallResponse>>();

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
    },
    timeoutMs?: number,
    signal?: globalThis.AbortSignal
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
    async retain(req, capability = currentLifecycle().asyncRetainOperationIdCapability, signal) {
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
    async recall(req, timeoutMs, signal) {
      signal?.throwIfAborted();
      // Two independent reasons to stop: the caller's service-owned signal (stop
      // or restart) and this call's own deadline. Compose them so the transport is
      // cancelled by whichever fires first, while the reason the caller classifies
      // on survives — a TimeoutError for the deadline, an AbortError for a stop.
      const deadline = new AbortController();
      const effective = signal ? AbortSignal.any([signal, deadline.signal]) : deadline.signal;
      let onAbort!: () => void;
      const cancelled = new Promise<never>((_, reject) => {
        onAbort = () => reject(effective.reason);
        effective.addEventListener("abort", onAbort, { once: true });
      });
      const timer = timeoutMs
        ? setTimeout(
            () =>
              deadline.abort(
                new DOMException(`Recall timed out after ${timeoutMs}ms`, "TimeoutError")
              ),
            timeoutMs
          )
        : undefined;
      try {
        // A race alone only stopped the hook's wait. Forward cancellation to the
        // client too, so its HTTP request receives the deadline and service stops.
        //
        // The race stays as a backstop for a transport that ignores the signal,
        // which is not hypothetical: from client 0.10.x recall routes through
        // `retryOnCapacity`, whose backoff sleeps in a plain setTimeout, so an
        // abort during a 429/503 wait does not interrupt it. (#4445)
        const response = await Promise.race([
          c.recall(bankId, req.query, {
            maxTokens: req.maxTokens,
            budget: req.budget,
            types: req.types,
            preferObservations: req.preferObservations,
            minScores: req.minScores,
            signal: effective,
          }),
          cancelled,
        ]);
        effective.throwIfAborted();
        return response;
      } finally {
        clearTimeout(timer);
        effective.removeEventListener("abort", onAbort);
      }
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
  const lifecycle = currentLifecycle();
  if (!lifecycle.client || !lifecycle.clientOptions || !hasConfiguredBankDefaults(config)) {
    return;
  }
  // R23.1: the record belongs to the client that did the work, so a late finish can never mark
  // a bank on a newer client. R24.1: install the client's one set before any await so concurrent
  // first uses of different banks all record into it.
  const client = lifecycle.client;
  let applied = lifecycle.banksWithDefaultsApplied.get(client);
  if (!applied) lifecycle.banksWithDefaultsApplied.set(client, (applied = new Set<string>()));
  if (applied.has(bankId)) {
    return;
  }
  try {
    await applyConfiguredBankDefaults(client, bankId, config, lifecycle.clientOptions);
    applied.add(bankId);
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
// Durable owned work older than this drains below cadence (9c76 ASTINUS_STUCK_RANGE_MS).
const DURABLE_STUCK_MS = 2 * 60 * 60 * 1000;
/** The per-message hash the completion marker records (9c76 retainMessageHash). */
function retainMessageHash(message: unknown): string {
  const content = (message as { content?: unknown } | null | undefined)?.content;
  return createHash("sha256")
    .update(typeof content === "string" ? content : JSON.stringify(content ?? ""))
    .digest("hex");
}
// Where the running agent run started in each session's transcript, recorded by
// before_prompt_build and consumed by agent_end. See runStartMarker.
const runStartBySession = new Map<string, string | null>();
const MAX_TRACKED_SESSIONS = 10_000;
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
const REINIT_COOLDOWN_MS = 30_000;

// Retain queue (both external-API and local-daemon mode)
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
  const lifecycle = currentLifecycle();
  // service.start() can run again without an intervening stop() (gateway
  // reloads); don't leak the previous generation's timer onto the new one.
  if (lifecycle.flushTimer) {
    clearInterval(lifecycle.flushTimer);
    lifecycle.flushTimer = null;
  }
  try {
    // ebce1ee8c: the default queue is instance-relative; an inherited home is not instance identity.
    // Hosts without runtime.state.resolveStateDir keep the legacy home default.
    const queuePath = lifecycle.stateRoot
      ? resolveRetainQueuePath(lifecycle.stateRoot, pluginConfig.retainQueuePath)
      : pluginConfig.retainQueuePath ||
        join(homedir(), ".openclaw", "data", "hindsight-retain-queue.jsonl");
    mkdirSync(dirname(queuePath), { recursive: true });
    const queueFlushInterval = pluginConfig.retainQueueFlushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    const queueMaxAge = pluginConfig.retainQueueMaxAgeMs ?? -1;
    lifecycle.queue = new RetainQueue({ filePath: queuePath, maxAgeMs: queueMaxAge });
    const pending = lifecycle.queue.size();
    if (pending > 0) {
      log.info(`retain queue: ${pending} items pending from previous session, will flush shortly`);
    }
    debug(`[Hindsight] Retain queue initialized: ${queuePath}`);

    // Periodic flush timer
    if (queueFlushInterval > 0) {
      lifecycle.flushTimer = setInterval(() => {
        void flushRetainQueue(undefined, undefined, undefined, expectedGeneration, signal);
      }, queueFlushInterval);
      lifecycle.flushTimer.unref?.();
    }
  } catch (error) {
    lifecycle.queue = null;
    log.warn(`could not initialize retain queue, continuing without it: ${error}`);
  }
}

/**
 * One bounded background reconciliation pass over the durable retain queue (e8cb94b7a,
 * ae3d09089): submit with the persisted operation id, poll terminal status, and acknowledge a
 * row only after the server reports it completed. Pending work stays queued for the next pass.
 */
export async function flushRetainQueue(
  queueOverride?: RetainQueue,
  clientOverride?: HindsightClient,
  capabilityOverride?: AsyncRetainOperationIdCapability,
  expectedGeneration = currentLifecycle().generation,
  signal: globalThis.AbortSignal | undefined = currentLifecycle().serviceController?.signal,
  transportOverride?: RetainTransport
): Promise<void> {
  const lifecycle = currentLifecycle();
  const activeQueue = queueOverride ?? lifecycle.queue;
  const activeClient = clientOverride ?? lifecycle.client;
  const isCurrent = () => expectedGeneration === lifecycle.generation && !signal?.aborted;
  if (!activeQueue || lifecycle.flushInProgress || !isCurrent()) return;
  lifecycle.flushInProgress = true;
  try {
    if (!activeQueue.size()) return;
    const capability =
      capabilityOverride ?? (await refreshQueueOperationIdCapability(expectedGeneration, signal));
    if (!isCurrent() || (!activeClient && !transportOverride)) return;
    if (capability === "unknown") {
      if (lifecycle.capabilityWarningGeneration !== expectedGeneration) {
        lifecycle.capabilityWarningGeneration = expectedGeneration;
        log.warn(
          "retain operation-id capability is unknown; new submissions remain durably queued while accepted operations reconcile"
        );
      }
    } else {
      lifecycle.capabilityWarningGeneration = null;
    }
    const transport =
      transportOverride ?? createRetainTransport(activeClient!, getActiveApiEndpoint(), USER_AGENT);
    const completed = await reconcileRetainQueue({
      queue: activeQueue,
      transport,
      capability,
      signal,
      isCurrent,
      // A row carrying a completion action is acknowledged only after its session-state write
      // succeeds; a failed write (or no state root to write to) keeps the receipt queued.
      applyCompletion: (item) => {
        if (item.completion) {
          if (!lifecycle.stateRoot) {
            throw new Error("completion action held: no host state root; receipt kept queued");
          }
          applyCompletedRetain(lifecycle.stateRoot, item);
        }
        const messages = Number(item.metadata.message_count ?? 0);
        log.trackRetain(item.bankId, Number.isFinite(messages) ? messages : 0);
      },
      onIssue: (message) => log.warn(message),
    });
    if (completed && isCurrent()) {
      log.info(`queue flush: ${completed} retains completed; ${activeQueue.size()} still pending`);
    }
  } catch (error) {
    if (isCurrent())
      log.warn(
        `retain reconciliation deferred; durable queue preserved: ${error instanceof Error ? error.message : String(error)}`
      );
  } finally {
    lifecycle.flushInProgress = false;
  }
}

const DEFAULT_RECALL_PROMPT_PREAMBLE =
  "Relevant memories from past conversations (prioritize recent when conflicting). Only use memories that are directly useful to continue this conversation; ignore the rest:";

/**
 * The messages a retain should work from, for a `session_end` event that carries none.
 *
 * OpenClaw's `buildSessionEndHookPayload()` sends `sessionId`, `sessionKey`,
 * `messageCount`, `durationMs`, `reason`, `sessionFile` and the next session's ids -
 * no `messages` array, and a `context` holding only ids. The forced flush added for
 * #1726 therefore ended at its own "no messages" guard on every session close, and the
 * turns after the last cadence boundary were never retained (#4341).
 *
 * The transcript the event points at is the source: one synchronous read, which is what
 * the shutdown drain's shared 2s budget allows, and extraction still happens
 * asynchronously in the bank's operation queue. `undefined` when there is no readable
 * transcript, which leaves the caller's existing guard to skip the flush as before.
 *
 * `/new` matters here: OpenClaw emits the previous session's `session_end` lazily, on
 * the first turn of its successor, so the old messages are gone from the live session
 * entry by then and the file is the only copy.
 */
export function sessionEndMessagesFromTranscript(
  event: unknown,
  read: typeof parseSessionFile = parseSessionFile
): unknown[] | undefined {
  const payload = (event ?? {}) as Record<string, any>;
  const sessionFile = typeof payload.sessionFile === "string" ? payload.sessionFile : undefined;
  if (!sessionFile) return undefined;
  const agentId = typeof payload.context?.agentId === "string" ? payload.context.agentId : "";
  try {
    const messages = read(sessionFile, agentId).messages;
    return messages.length > 0 ? messages : undefined;
  } catch {
    // A missing, truncated or unreadable transcript is not an error worth failing the
    // session close over; the caller skips the flush exactly as it did before.
    return undefined;
  }
}

// Generous bounds for one session's tail; OpenClaw applies its own caps on top.
const ENDED_TRANSCRIPT_MAX_MESSAGES = 10_000;
const ENDED_TRANSCRIPT_MAX_BYTES = 8 * 1024 * 1024;

/**
 * The messages of a session that just ended, for the `session_end` flush.
 *
 * OpenClaw 2026.9.4 moved transcripts into SQLite, so the JSONL file the event
 * points at no longer exists and the flush found nothing (#4594). 2026.10.1 added
 * `ctx.endedTranscript`, a bounded reader over that store; use it when the host
 * offers one and keep the file read for older hosts.
 */
export async function sessionEndMessages(
  event: unknown,
  endedTranscript: PluginHookEndedTranscript | undefined
): Promise<unknown[] | undefined> {
  if (endedTranscript === undefined) return sessionEndMessagesFromTranscript(event);
  if (!endedTranscript.available) {
    debug(`[Hindsight Hook] session_end: transcript unavailable (${endedTranscript.reason})`);
    return undefined;
  }
  try {
    const { messages, truncated } = await endedTranscript.readTail({
      maxMessages: ENDED_TRANSCRIPT_MAX_MESSAGES,
      maxBytes: ENDED_TRANSCRIPT_MAX_BYTES,
    });
    if (truncated) debug("[Hindsight Hook] session_end: transcript truncated to its tail");
    return messages.length > 0 ? [...messages] : undefined;
  } catch (error) {
    log.warn(`session_end: could not read the ended transcript: ${error}`);
    return undefined;
  }
}

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
  const lifecycle = currentLifecycle();
  const now = Date.now();
  if (now - lifecycle.lastReinitAttempt < REINIT_COOLDOWN_MS || lifecycle.isReinitInProgress) {
    return;
  }

  const config = configOverride ?? lifecycle.config;
  if (!config) {
    debug("[Hindsight] lazyReinit skipped - no plugin config available");
    return;
  }

  // Persist config if we only have it from the live hook registration path.
  lifecycle.config = config;

  // R14.2: capture the lifetime this attempt serves; a start()/stop() that supersedes it while it
  // awaits must not have its client/options/queue replaced, nor a newer attempt's flag cleared.
  const attempt = {};
  const generation = lifecycle.generation;
  const controller = lifecycle.serviceController ?? lifecycle.preServiceController;
  const attemptIsCurrent = () =>
    lifecycle.reinitAttempt === attempt &&
    lifecycle.generation === generation &&
    (lifecycle.serviceController ?? lifecycle.preServiceController) === controller &&
    !controller.signal.aborted;
  lifecycle.isReinitInProgress = true;
  lifecycle.reinitAttempt = attempt;
  lifecycle.lastReinitAttempt = now;
  const externalApi = detectExternalApi(config);
  if (!externalApi.apiUrl) {
    lifecycle.isReinitInProgress = false;
    lifecycle.reinitAttempt = null;
    return; // Only external API mode supports lazy reinit
  }

  debug("[Hindsight] Attempting lazy re-initialization...");
  try {
    await checkExternalApiHealth(externalApi.apiUrl, externalApi.apiToken);
    await detectAppendCapability(
      externalApi.apiUrl,
      externalApi.apiToken,
      attemptIsCurrent,
      controller.signal
    );
    if (!attemptIsCurrent()) {
      debug("[Hindsight] lazy re-init superseded by a newer lifecycle - not publishing");
      return;
    }

    const llmConfig = detectLLMConfig(config);
    lifecycle.clientOptions = buildClientOptions(llmConfig, config, externalApi);
    lifecycle.client = new HindsightClient(lifecycle.clientOptions);

    if (usesStaticBank(config)) {
      await ensureBankDefaultsApplied(getStaticBankId(config), config);
    }
    if (!attemptIsCurrent()) return;

    lifecycle.usingExternalApi = true;
    lifecycle.isInitialized = true;
    // A copy that was never started has no retain queue, so a failed retain
    // would be dropped instead of replayed. (#4537)
    // ponytail: the started copy may hold the same file, so both can replay one
    // item; the persisted operation id keeps that idempotent on API >= 0.8.6.
    // Share one queue per process via globalThis if duplicates show up on older APIs.
    if (
      !lifecycle.queue &&
      lifecycle.generation === 0 &&
      !lifecycle.preServiceController.signal.aborted
    ) {
      initRetainQueue(config, 0, lifecycle.preServiceController.signal);
    }
    // Replace the rejected initPromise with a resolved one
    lifecycle.initPromise = Promise.resolve();
    debug("[Hindsight] ✓ Lazy re-initialization succeeded");
  } catch (error) {
    log.warn(
      `lazy re-init failed (retry in ${REINIT_COOLDOWN_MS / 1000}s): ${error instanceof Error ? error.message : error}`
    );
  } finally {
    if (lifecycle.reinitAttempt === attempt) {
      lifecycle.isReinitInProgress = false;
      lifecycle.reinitAttempt = null;
    }
  }
}

// Global access for hooks (Moltbot loads hooks separately)
if (typeof global !== "undefined") {
  (global as any).__hindsightClient = {
    getClient: () => currentLifecycle().client,
    waitForReady: async () => {
      const lifecycle = currentLifecycle();
      if (lifecycle.isInitialized) {
        return;
      }
      // If initPromise is null, it means service.start() hasn't been called yet
      // (CLI mode, not gateway mode). Hooks should gracefully no-op.
      if (!lifecycle.initPromise) {
        if (lifecycle.config) {
          log.warn(
            "waitForReady called before service.start() — attempting lazy initialization fallback"
          );
          await lazyReinit(lifecycle.config);
          return;
        }
        log.warn(
          "waitForReady called before service.start() — hooks will no-op (expected in CLI mode)"
        );
        return;
      }
      try {
        await lifecycle.initPromise;
      } catch {
        // Init failed (e.g., health check timeout at startup).
        // Attempt lazy re-initialization so Hindsight recovers
        // once the API becomes reachable again.
        if (!lifecycle.isInitialized) {
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
      const lifecycle = currentLifecycle();
      if (!lifecycle.client) return null;
      const config = lifecycle.config || {};
      // deriveBankId already returns the static bank when dynamicBankId is false,
      // so the branch that used to stand here was redundant — and it routed around
      // the agentBankMap lookup for static configs. (#3890)
      const bankId = deriveBankId(ctx, config);
      const scoped = scopeClient(lifecycle.client, bankId);

      // Stamp configured defaults onto this bank on first use (recall or retain).
      await ensureBankDefaultsApplied(bankId, config);

      return scoped;
    },
    getPluginConfig: () => currentLifecycle().config,
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

/**
 * The bank an agent's memory lives in when it depends on the agent alone: an
 * `agentBankMap` entry, a static bank, or agent-only dynamic banking. Undefined
 * when the bank is keyed by user, channel or provider, which an agent id can't
 * pick.
 */
export function agentOnlyBankId(agentId: string, pluginConfig: PluginConfig): string | undefined {
  const ctx: PluginHookAgentContext = { agentId };
  const mapped = mappedBankIdForAgent(ctx, pluginConfig);
  if (mapped) return mapped;
  if (usesStaticBank(pluginConfig)) return getStaticBankId(pluginConfig);
  const fields = pluginConfig.dynamicBankGranularity?.length
    ? pluginConfig.dynamicBankGranularity
    : DEFAULT_DYNAMIC_BANK_GRANULARITY;
  return fields.every((field) => field === "agent") ? deriveBankId(ctx, pluginConfig) : undefined;
}

/** The initialized client and its options, initializing lazily like the hooks do. */
async function readyClient(): Promise<{
  client: HindsightClient;
  options: HindsightClientOptions;
} | null> {
  const lifecycle = currentLifecycle();
  await (global as any).__hindsightClient?.waitForReady();
  return lifecycle.client && lifecycle.clientOptions
    ? { client: lifecycle.client, options: lifecycle.clientOptions }
    : null;
}

function authHeaders(options: HindsightClientOptions): Record<string, string> {
  return options.apiKey ? { Authorization: `Bearer ${options.apiKey}` } : {};
}

function usesStaticBank(pluginConfig: PluginConfig): boolean {
  return pluginConfig.dynamicBankId === false;
}

/**
 * Normalise the optional `agentBankMap` (agentId -> bankId).
 *
 * The value comes from user-edited config, so an entry whose bank is not a
 * non-empty string is dropped rather than trusted — an empty one would otherwise
 * route that agent to a bank literally named "". A map left with no usable entry
 * is treated as unset. (#3890)
 */
export function normalizeAgentBankMap(input: unknown): Record<string, string> | undefined {
  if (!input || typeof input !== "object" || Array.isArray(input)) return undefined;

  const normalized: Record<string, string> = {};
  const dropped: string[] = [];
  for (const [agentId, bankId] of Object.entries(input as Record<string, unknown>)) {
    const trimmedAgentId = agentId.trim();
    const trimmedBank = typeof bankId === "string" ? bankId.trim() : "";
    if (!trimmedAgentId || !trimmedBank) {
      dropped.push(agentId);
      continue;
    }
    // Key on the trimmed id. Storing the raw key would keep an entry that can
    // never match a resolved agent id — inert rather than wrong, and silent.
    normalized[trimmedAgentId] = trimmedBank;
  }

  // Silently dropped config keys have bitten this plugin before (#1443), so say so.
  if (dropped.length > 0) {
    log.warn(
      `agentBankMap: ignoring ${dropped.length} entr${dropped.length === 1 ? "y" : "ies"} with a missing or blank bank id (${dropped.join(", ")})`
    );
  }

  return Object.keys(normalized).length > 0 ? normalized : undefined;
}

/**
 * The bank an agent is explicitly mapped to, if any.
 *
 * `agentBankMap` exists so one gateway can mix topologies: several agents share a
 * named bank while the rest keep their derived per-agent/channel/user banks
 * (#3890). The mapped name is used exactly as configured — `bankIdPrefix` is
 * deliberately not applied, because the operator named this bank themselves.
 */
function mappedBankIdForAgent(
  ctx: PluginHookAgentContext | undefined,
  pluginConfig: PluginConfig
): string | undefined {
  const map = pluginConfig.agentBankMap;
  if (!map || !ctx) return undefined;

  const resolvedCtx = resolveSessionIdentity(ctx);
  const agentId =
    resolvedCtx?.agentId ||
    (resolvedCtx?.sessionKey ? parseSessionKey(resolvedCtx.sessionKey).agentId : undefined);
  // Object.hasOwn, not a plain lookup: an agent literally called "toString" or
  // "constructor" would otherwise inherit a function from the prototype, which is
  // truthy and is not a bank id.
  if (!agentId || !Object.hasOwn(map, agentId)) return undefined;

  // Re-check the value instead of trusting the caller to have normalised it: the
  // backfill CLI builds its PluginConfig straight from openclaw.json and never
  // passes through normalizeAgentBankMap, so it would otherwise back-fill into a
  // differently-trimmed bank than the live gateway writes to.
  const mapped = map[agentId];
  return typeof mapped === "string" && mapped.trim().length > 0 ? mapped.trim() : undefined;
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
  // A mapped agent is pinned the same way a static bank is: its bank comes from
  // the map, not from the dispatch surface, so a surface mismatch cannot route
  // the turn into the wrong bank and must not skip it. (#3890)
  const mappedBanking =
    options.pluginConfig !== undefined &&
    mappedBankIdForAgent(resolvedCtx ?? effectiveCtx, options.pluginConfig) !== undefined;

  if (
    sessionProvider &&
    options.dispatchChannel &&
    sessionProvider !== options.dispatchChannel &&
    bankRoutingDependsOnSurface &&
    !staticBanking &&
    !mappedBanking
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
  //   - the agent has an explicit agentBankMap entry → the operator named that
  //     bank for this agent, so its sessions belong there too (#3890)
  const mappedBanking =
    pluginConfig !== undefined && mappedBankIdForAgent(resolvedCtx, pluginConfig) !== undefined;
  const allowCliSessions = agentBanking || staticBanking || mappedBanking;
  // Cron, heartbeat and subagent sessions are skipped unless the operator opts
  // them in; then ignoreSessionPatterns / statelessSessionPatterns decide (#5915).
  const allowOperationalSessions = pluginConfig?.skipOperationalSessions === false;

  if (typeof sessionKey === "string") {
    if (!allowOperationalSessions && /^agent:[^:]+:(cron|heartbeat|subagent):/.test(sessionKey)) {
      return { resolvedCtx, reason: finalSkipReason(`operational session ${sessionKey}`) };
    }
    if (!allowCliSessions && /^agent:[^:]+:main$/.test(sessionKey)) {
      return { resolvedCtx, reason: finalSkipReason(`internal main session ${sessionKey}`) };
    }
    if (/^temp:/.test(sessionKey)) {
      return { resolvedCtx, reason: finalSkipReason(`ephemeral temp session ${sessionKey}`) };
    }
  }

  const operationalProviders = [
    ...(allowOperationalSessions ? [] : ["cron", "heartbeat", "subagent"]),
    ...(allowCliSessions ? [] : ["main"]),
  ];
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
  // An explicit agent -> bank mapping wins over both the static bank and dynamic
  // derivation, so a gateway can give one group of agents a shared bank while the
  // rest keep derived ones (#3890). Resolved only when a map is configured, so the
  // common path is untouched.
  const mappedBankId = mappedBankIdForAgent(ctx, pluginConfig);
  if (mappedBankId) {
    return mappedBankId;
  }

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

export function formatMemories(results: MemoryResult[]): string {
  if (!results || results.length === 0) return "";
  return results
    .map((r) => {
      const type = r.type ? ` [${r.type}]` : "";
      const date = r.mentioned_at ? ` (${r.mentioned_at})` : "";
      const occurred = formatOccurredWindow(r.occurred_start, r.occurred_end);
      const doc = r.document_id ? ` [doc:${r.document_id}]` : "";
      return `- ${r.text}${type}${date}${occurred}${doc}`;
    })
    .join("\n\n");
}

// Providers that authenticate via OAuth or run locally — no API key needed.
const NO_KEY_REQUIRED_PROVIDERS = new Set([
  "ollama",
  "openai-codex",
  "claude-code",
  "cursor",
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
  // R17.1: the endpoint of the root authority's ACTIVE client - never this module copy's own
  // config, which may name a different endpoint than the client every copy is serving through.
  const lifecycle = currentLifecycle();
  if (lifecycle.client && lifecycle.clientOptions?.baseUrl) {
    return {
      apiUrl: lifecycle.clientOptions.baseUrl,
      apiToken: lifecycle.clientOptions.apiKey ?? null,
    };
  }
  return { apiUrl: lifecycle.hindsightServer?.getBaseUrl() ?? null, apiToken: null };
}

function sameEndpoint(a: string | null | undefined, b: string | null | undefined): boolean {
  return (a ?? "").replace(/\/+$/, "") === (b ?? "").replace(/\/+$/, "");
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
  expectedGeneration = currentLifecycle().generation,
  signal: globalThis.AbortSignal | undefined = currentLifecycle().serviceController?.signal
): Promise<AsyncRetainOperationIdCapability> {
  const lifecycle = currentLifecycle();
  const clientAtProbe = lifecycle.client;
  const capabilities = await fetchHindsightApiCapabilities(apiUrl, apiToken, signal);
  const capability = asyncRetainOperationIdCapabilityFromCapabilities(capabilities);
  if (expectedGeneration !== lifecycle.generation || signal?.aborted) return "unknown";
  // R17.1: publish only for the client this answer describes - the same live client, at this endpoint.
  if (
    lifecycle.clientOptions &&
    (lifecycle.client !== clientAtProbe || !sameEndpoint(lifecycle.clientOptions.baseUrl, apiUrl))
  ) {
    return "unknown";
  }
  lifecycle.asyncRetainOperationIdCapability = capability;
  return capability;
}

async function refreshQueueOperationIdCapability(
  expectedGeneration = currentLifecycle().generation,
  signal: globalThis.AbortSignal | undefined = currentLifecycle().serviceController?.signal
): Promise<AsyncRetainOperationIdCapability> {
  const lifecycle = currentLifecycle();
  if (expectedGeneration !== lifecycle.generation || signal?.aborted) return "unknown";
  if (!lifecycle.config) {
    lifecycle.asyncRetainOperationIdCapability = "unknown";
    return "unknown";
  }
  const endpoint = getActiveApiEndpoint();
  if (!endpoint.apiUrl) {
    lifecycle.asyncRetainOperationIdCapability = "unknown";
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
  apiToken: string | null | undefined,
  isCurrent: () => boolean,
  signal: globalThis.AbortSignal
): Promise<void> {
  // R20.1: the caller's captured lifetime - a superseded attempt neither probes nor publishes.
  const lifecycle = currentLifecycle();
  if (!isCurrent()) return;
  const capabilities = await fetchHindsightApiCapabilities(apiUrl, apiToken, signal);
  if (!isCurrent()) return;
  const supported = supportsAppendFromCapabilities(capabilities);
  lifecycle.asyncRetainOperationIdCapability =
    asyncRetainOperationIdCapabilityFromCapabilities(capabilities);
  const transitionedToUnsupported = lifecycle.supportsUpdateModeAppend && !supported;
  const firstProbe = !lifecycle.appendCapabilityProbed;
  lifecycle.appendCapabilityProbed = true;
  lifecycle.supportsUpdateModeAppend = supported;
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
    agentBankMap: normalizeAgentBankMap(config.agentBankMap),
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
    // "tools" is Hermes's tools-only mode: the agent recalls through hindsight_recall instead.
    autoRecall: config.autoRecall !== false && config.memoryMode !== "tools", // Default: true (on)
    memoryMode:
      config.memoryMode === "context" || config.memoryMode === "tools"
        ? config.memoryMode
        : "hybrid",
    allowKnowledgeDelete: config.allowKnowledgeDelete === true,
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
    durableTurns: config.durableTurns === true,
    retainOverlapTurns:
      typeof config.retainOverlapTurns === "number" && config.retainOverlapTurns >= 0
        ? config.retainOverlapTurns
        : 0,
    recallTopK: typeof config.recallTopK === "number" ? config.recallTopK : undefined,
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
    skipStatelessSessions: config.skipStatelessSessions !== false,
    skipOperationalSessions: config.skipOperationalSessions !== false,
    debug: config.debug ?? false,
    debugPerfTiming: config.debugPerfTiming === true,
    // Both were missing from this whitelist, so the logger always ran at
    // `info` with a 5-minute summary whatever the user configured.
    logLevel:
      config.logLevel && ["off", "error", "warning", "info", "debug"].includes(config.logLevel)
        ? config.logLevel
        : undefined,
    logSummaryIntervalMs:
      typeof config.logSummaryIntervalMs === "number" && config.logSummaryIntervalMs >= 0
        ? config.logSummaryIntervalMs
        : undefined,
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
    senderPrefixPattern,
    // Context engine admission overrides: only the typed, supported fields survive; absent ->
    // undefined, so applyMarks keeps DEFAULT_MARKS_POLICY. (e8cb's loader dropped this field too,
    // which left its configured knobs unreachable; review 9 R9.1.)
    astinusMarks: marksPolicyFromConfig(config),
  };
}

// Registration guard: WeakSet keyed by api instance to prevent double-registration
// on the same api object while allowing fresh registration on new api objects.
// Does not reintroduce issue #1029 because WeakSet.has() checks object identity,
// not a module-level boolean.
const _registeredApis = new WeakSet<MoltbotPluginAPI>();

export default function (api: MoltbotPluginAPI) {
  // P3 (e8cb F1): every copy for this host instance shares one lifecycle, client and queue.
  let hostStateRoot: string | undefined;
  try {
    hostStateRoot = resolveHostStateRoot(api);
  } catch {
    hostStateRoot = undefined;
  }
  // R14.1: this registration's own authority, captured lexically by everything registered below.
  const lifecycle = hostStateRoot ? lifecycleAuthorityFor(hostStateRoot) : unrootedLifecycle;
  lastRegisteredLifecycle = lifecycle;
  const scoped = <A extends unknown[], R>(fn: (...args: A) => R) => bindLifecycle(lifecycle, fn);
  const onScoped = (event: string, handler: Parameters<MoltbotPluginAPI["on"]>[1]) =>
    api.on(event, scoped(handler));
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
    // `debug` is always a boolean here (getPluginConfig defaults it, and so does
    // the schema), so `??` never reached logLevel and `logLevel: "debug"` was a no-op.
    debugEnabled = pluginConfig.debug === true || pluginConfig.logLevel === "debug";

    // Configure structured logger — route through OpenClaw's api.logger for consistent formatting
    if (api.logger) setApiLogger(api.logger);
    configureLogger({
      logLevel: pluginConfig.logLevel ?? (pluginConfig.debug ? "debug" : "info"),
      logSummaryIntervalMs: pluginConfig.logSummaryIntervalMs,
    });

    // R20.2: config (bank mapping, policy) is bound to this registration's root, like its client.
    lifecycle.config = pluginConfig;

    // Register the Hindsight context engine (non-durable shell; see context-engine.ts).
    // Inert unless plugins.slots.contextEngine selects "hindsight-openclaw".
    // Mark application needs the host instance root; without it assembly stays pass-through.
    const engineStateRoot = hostStateRoot;
    // Admission knobs ride the apply context from the live plugin config (re-read per assemble,
    // so a config reinit is honoured).
    registerHindsightContextEngine(
      api,
      log,
      engineStateRoot,
      () => marksPolicyFromConfig(lifecycle.config),
      pluginConfig.durableTurns === true
    );

    debug("[Hindsight] Plugin loaded successfully (deferred heavy init to gateway start)");

    // Register background service for cleanup
    // IMPORTANT: Heavy initialization (LLM detection, daemon start, API health checks)
    // happens in service.start() which is ONLY called on gateway start,
    // not on every CLI command.
    debug("[Hindsight] Registering service...");
    log.info("registering plugin service");
    // e8cb R1: the owner is this service registration, not the module copy - one evaluated
    // module can register for two plugin APIs.
    const serviceOwner = {};
    api.registerService({
      id: "hindsight-memory",
      start: scoped(async () => {
        lifecycle.preServiceController.abort();
        lifecycle.serviceController?.abort();
        inflightRecalls.clear();
        const serviceController = new AbortController();
        lifecycle.serviceController = serviceController;
        lifecycle.owner = serviceOwner;
        const startGeneration = ++lifecycle.generation;
        // R14.2: async init publishes only while this start still owns the lifecycle.
        const startIsCurrent = () =>
          lifecycle.generation === startGeneration &&
          lifecycle.serviceController === serviceController &&
          !serviceController.signal.aborted;
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
        lifecycle.usingExternalApi = Boolean(externalApi.apiUrl);
        // R17.2: a daemon manager an earlier owner started for this root is now ours to dispose -
        // an external-mode owner would otherwise leave it recorded and never stopped.
        if (lifecycle.usingExternalApi && lifecycle.hindsightServer) {
          const prior = lifecycle.hindsightServer;
          lifecycle.hindsightServer = null;
          await prior
            .stop()
            .catch((error: unknown) => log.warn(`prior daemon stop failed: ${error}`));
          if (!startIsCurrent()) return;
        }

        // Get API port from config (default: 9077)
        const apiPort = pluginConfig.apiPort || 9077;

        // Both modes get the queue: a local daemon is unreachable while it is
        // still booting or after it has crashed, and a retain that fails then is
        // just as lost as one that fails against a remote API. (#3686)
        initRetainQueue(pluginConfig, startGeneration, serviceController.signal);

        if (externalApi.apiUrl) {
          // External API mode - skip local daemon
          lifecycle.usingExternalApi = true;
          debug(`[Hindsight] ✓ Using external API: ${externalApi.apiUrl}`);

          if (externalApi.apiToken) {
            debug("[Hindsight] API token configured");
          }
        } else {
          debug(`[Hindsight] API Port: ${apiPort}`);
        }

        // Initialize (runs synchronously in service.start())
        debug("[Hindsight] Starting initialization...");
        lifecycle.initPromise = (async () => {
          try {
            if (lifecycle.usingExternalApi && externalApi.apiUrl) {
              // External API mode - check health, skip daemon startup
              debug("[Hindsight] External API mode - skipping local daemon...");
              await checkExternalApiHealth(externalApi.apiUrl, externalApi.apiToken);
              await detectAppendCapability(
                externalApi.apiUrl,
                externalApi.apiToken,
                startIsCurrent,
                serviceController.signal
              );
              if (!startIsCurrent()) return;

              // Initialize client for external API
              debug("[Hindsight] Creating HindsightClient (external API)...");
              lifecycle.clientOptions = buildClientOptions(llmConfig, pluginConfig, externalApi);
              lifecycle.client = new HindsightClient(lifecycle.clientOptions);

              const defaultBankId = deriveBankId(undefined, pluginConfig);
              debug(`[Hindsight] Default bank: ${defaultBankId}`);

              // Defaults are stamped per-bank when dynamic bank IDs are enabled.
              // For static banks, stamp once here on init.
              if (usesStaticBank(pluginConfig)) {
                debug(`[Hindsight] Applying configured bank defaults...`);
                await ensureBankDefaultsApplied(defaultBankId, pluginConfig);
              }
              if (!startIsCurrent()) return;

              if (!lifecycle.isInitialized) {
                const mode = "external API";
                const autoRecall = pluginConfig.autoRecall !== false;
                const autoRetain = pluginConfig.autoRetain !== false;
                log.info(
                  `initialized (mode: ${mode}, bank: ${defaultBankId}, autoRecall: ${autoRecall}, autoRetain: ${autoRetain})`
                );
              }
              lifecycle.isInitialized = true;
              debug("[Hindsight] ✓ Ready (external API mode)");
            } else {
              // Local daemon mode - start hindsight-embed daemon
              debug("[Hindsight] Creating HindsightServer...");
              lifecycle.hindsightServer = new HindsightServer({
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
              await lifecycle.hindsightServer.start();

              // The daemon is a Hindsight API like any other: probe it for the
              // same capabilities as an external one, or retains here silently
              // fall back to per-turn document ids. (#3686)
              await detectAppendCapability(
                lifecycle.hindsightServer.getBaseUrl(),
                null,
                startIsCurrent,
                serviceController.signal
              );
              if (!startIsCurrent()) return;

              // Initialize client pointed at the local daemon URL
              debug("[Hindsight] Creating HindsightClient (local daemon)...");
              lifecycle.clientOptions = { baseUrl: lifecycle.hindsightServer.getBaseUrl() };
              lifecycle.client = new HindsightClient(lifecycle.clientOptions);

              const defaultBankId = deriveBankId(undefined, pluginConfig);
              debug(`[Hindsight] Default bank: ${defaultBankId}`);

              // Defaults are stamped per-bank when dynamic bank IDs are enabled.
              // For static banks, stamp once here on init.
              if (usesStaticBank(pluginConfig)) {
                debug(`[Hindsight] Applying configured bank defaults...`);
                await ensureBankDefaultsApplied(defaultBankId, pluginConfig);
              }
              if (!startIsCurrent()) return;

              if (!lifecycle.isInitialized) {
                const mode = "local daemon";
                const autoRecall = pluginConfig.autoRecall !== false;
                const autoRetain = pluginConfig.autoRetain !== false;
                log.info(
                  `initialized (mode: ${mode}, bank: ${defaultBankId}, autoRecall: ${autoRecall}, autoRetain: ${autoRetain})`
                );
              }
              lifecycle.isInitialized = true;
              debug("[Hindsight] ✓ Ready");
            }
          } catch (error) {
            log.error("initialization error", error);
            throw error;
          }
        })();

        // Wait for initialization to complete
        try {
          await lifecycle.initPromise;
        } catch (error) {
          log.error("initial initialization failed", error);
          // Continue to health check below
        }
        if (!startIsCurrent()) return;

        // External API mode: check external API health
        if (lifecycle.usingExternalApi) {
          const externalApi = detectExternalApi(pluginConfig);
          if (externalApi.apiUrl && lifecycle.isInitialized) {
            try {
              await checkExternalApiHealth(externalApi.apiUrl, externalApi.apiToken);
              await detectAppendCapability(
                externalApi.apiUrl,
                externalApi.apiToken,
                startIsCurrent,
                serviceController.signal
              );
              debug("[Hindsight] External API is healthy");
              return;
            } catch (error) {
              if (!startIsCurrent()) return;
              log.error("external API health check failed", error);
              // Reset state for reinitialization attempt
              lifecycle.client = null;
              lifecycle.clientOptions = null;
              lifecycle.isInitialized = false;
            }
          }
        } else {
          // Local daemon mode: check daemon health (handles SIGUSR1 restart case)
          if (lifecycle.hindsightServer && lifecycle.isInitialized) {
            const healthy = await lifecycle.hindsightServer.checkHealth();
            if (!startIsCurrent()) return;
            if (healthy) {
              // Same re-probe the external branch does after its health check:
              // the daemon may have been restarted (SIGUSR1) onto a different
              // embed version since we last looked. (#3686)
              await detectAppendCapability(
                lifecycle.hindsightServer.getBaseUrl(),
                null,
                startIsCurrent,
                serviceController.signal
              );
              debug("[Hindsight] Daemon is healthy");
              return;
            }

            debug("[Hindsight] Daemon is not responding - reinitializing...");
            // Reset state for reinitialization
            lifecycle.hindsightServer = null;
            lifecycle.client = null;
            lifecycle.clientOptions = null;
            lifecycle.isInitialized = false;
          }
        }

        // Reinitialize if needed (fresh start or recovery)
        if (!lifecycle.isInitialized) {
          debug("[Hindsight] Reinitializing...");
          const reinitPluginConfig = getPluginConfig(api);
          lifecycle.config = reinitPluginConfig;
          const llmConfig = detectLLMConfig(reinitPluginConfig);
          const externalApi = detectExternalApi(reinitPluginConfig);
          const apiPort = reinitPluginConfig.apiPort || 9077;

          if (externalApi.apiUrl) {
            // External API mode
            lifecycle.usingExternalApi = true;

            await checkExternalApiHealth(externalApi.apiUrl, externalApi.apiToken);
            await detectAppendCapability(
              externalApi.apiUrl,
              externalApi.apiToken,
              startIsCurrent,
              serviceController.signal
            );
            if (!startIsCurrent()) return;

            lifecycle.clientOptions = buildClientOptions(
              llmConfig,
              reinitPluginConfig,
              externalApi
            );
            lifecycle.client = new HindsightClient(lifecycle.clientOptions);
            const defaultBankId = deriveBankId(undefined, reinitPluginConfig);

            if (usesStaticBank(reinitPluginConfig)) {
              await ensureBankDefaultsApplied(defaultBankId, reinitPluginConfig);
            }
            if (!startIsCurrent()) return;

            lifecycle.isInitialized = true;
            debug("[Hindsight] Reinitialization complete (external API mode)");
          } else {
            // Local daemon mode
            lifecycle.hindsightServer = new HindsightServer({
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

            await lifecycle.hindsightServer.start();
            await detectAppendCapability(
              lifecycle.hindsightServer.getBaseUrl(),
              null,
              startIsCurrent,
              serviceController.signal
            );
            if (!startIsCurrent()) return;

            lifecycle.clientOptions = { baseUrl: lifecycle.hindsightServer.getBaseUrl() };
            lifecycle.client = new HindsightClient(lifecycle.clientOptions);
            const defaultBankId = deriveBankId(undefined, reinitPluginConfig);

            if (usesStaticBank(reinitPluginConfig)) {
              await ensureBankDefaultsApplied(defaultBankId, reinitPluginConfig);
            }
            if (!startIsCurrent()) return;

            lifecycle.isInitialized = true;
            debug("[Hindsight] Reinitialization complete");
          }
        }
      }),

      stop: scoped(async () => {
        // e8cb F1/R1: only the registration that started the current lifecycle (or any
        // registration before a first start) ends it. Another registration's stop() is a no-op:
        // its hooks already run under the owner's lifecycle, and tearing down the shared client
        // would break the owner's retains.
        if (lifecycle.owner !== null && lifecycle.owner !== serviceOwner) {
          debug(
            "[Hindsight] Service stop on a registration that does not own the lifecycle - ignored"
          );
          return;
        }
        try {
          const stopGeneration = ++lifecycle.generation;
          lifecycle.preServiceController.abort();
          lifecycle.serviceController?.abort();
          lifecycle.serviceController = null;
          lifecycle.owner = null;
          inflightRecalls.clear();
          debug("[Hindsight] Service stopping...");

          // R20.3: detach everything this stop disposes before its one await, so a newer owner that
          // starts meanwhile keeps its own client, queue and daemon. R17.2: whatever the client mode.
          const server = lifecycle.hindsightServer;
          lifecycle.hindsightServer = null;

          // Close retain queue
          if (lifecycle.flushTimer) {
            clearInterval(lifecycle.flushTimer);
            lifecycle.flushTimer = null;
          }
          if (lifecycle.queue) {
            const pending = lifecycle.queue.size();
            if (pending > 0) {
              debug(
                `[Hindsight] Service stopping with ${pending} queued retains (will resume on next start)`
              );
            }
            lifecycle.queue.close();
            lifecycle.queue = null;
          }

          lifecycle.client = null;
          lifecycle.clientOptions = null;
          lifecycle.asyncRetainOperationIdCapability = "unknown";
          lifecycle.usingExternalApi = false;
          lifecycle.isInitialized = false;

          await server?.stop();
          if (lifecycle.generation === stopGeneration) stopLogger();
          debug("[Hindsight] Service stopped");
        } catch (error) {
          log.error("service stop error", error);
          throw error;
        }
      }),
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

    // Inbound identity capture: cache the sender and dispatch surface per session
    // before the agent runs, for banks keyed by user or channel.
    //
    // This used `before_dispatch`, which carries the same fields. OpenClaw refuses
    // to start or resume a Goal while any plugin holds `before_dispatch` (it can't
    // replay it after a restart), so the plugin blocked Goals. `message_received`
    // fires earlier on the same inbound path and is not on that list. (#5177)
    onScoped("message_received", async (event: any, ctx?: PluginHookAgentContext) => {
      try {
        const sessionKey =
          ctx?.sessionKey ?? (typeof event?.sessionKey === "string" ? event.sessionKey : undefined);
        if (!sessionKey) {
          return;
        }

        // A message hook's channelId is the surface ("telegram", "webchat").
        const dispatchChannel =
          (typeof ctx?.channelId === "string" ? ctx.channelId : undefined) ||
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
            `[Hindsight] message_received marked session ${sessionKey} to skip this turn: ${formatIdentitySkipReason(skipReason)}`
          );
          logSkipOnce("dispatch", sessionKey, skipReason);
          return;
        }
        if (!resolvedCtx?.senderId || typeof resolvedCtx.senderId !== "string") {
          return;
        }

        debug(
          `[Hindsight] message_received cached identity for ${sessionKey}: ${resolvedCtx.messageProvider}/${resolvedCtx.channelId} sender=${resolvedCtx.senderId}`
        );
      } catch (error) {
        log.warn(`message_received identity cache error: ${error}`);
      }
    });

    // No `before_agent_start` registration: the callback used to call
    // `resolveAndCacheIdentity()` and emit a debug log, but `message_received`
    // already populates the identity cache earlier in the inbound path,
    // `before_prompt_build` re-resolves before recall (and can infer
    // `senderId` from prompt content when ctx is missing it), and `agent_end`
    // re-resolves before retain. Subscribing here was duplicate work on the
    // hot path. (#1354)

    // Auto-recall: Inject relevant memories before agent processes the message
    // Hook signature: (event, ctx) where event has {prompt, messages?} and ctx has agent context
    onScoped("before_prompt_build", async (event: any, ctx?: PluginHookAgentContext) => {
      const recallLifecycle = captureHookLifecycle();
      const isCurrentRecall = recallLifecycle.isCurrent;
      if (!isCurrentRecall()) return;
      // Prompts are rebuilt inside a run (tool loops, a user message sent while the
      // agent works), so only the first build of a run marks where it started.
      const runSessionKey = ctx?.sessionKey;
      if (
        runSessionKey &&
        !runStartBySession.has(runSessionKey) &&
        Array.isArray(event?.messages)
      ) {
        setCappedMapValue(
          runStartBySession,
          runSessionKey,
          runStartMarker(event.messages, event.currentUserMessage)
        );
      }
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
        if (!isCurrentRecall()) return;

        // Get client configured for this context's bank (async to handle mission setup)
        const client = await clientGlobal.getClientForContext(resolvedCtxForRecall);
        if (!isCurrentRecall()) return;
        if (!client) {
          debug("[Hindsight] Client not initialized, skipping auto-recall");
          return;
        }

        debug(`[Hindsight] Auto-recall for bank ${bankId}, full query:\n---\n${prompt}\n---`);

        // Recall with deduplication: reuse in-flight request for same bank
        const normalizedPrompt = prompt.trim().toLowerCase().replace(/\s+/g, " ");
        const queryHash = createHash("sha256").update(normalizedPrompt).digest("hex").slice(0, 16);
        const recallKey = `${bankId}::${queryHash}`;
        const existing = inflightRecalls.get(recallKey);
        let recallPromise: Promise<RecallResponse>;
        if (existing) {
          debug(`[Hindsight] Reusing in-flight recall for bank ${bankId}`);
          recallPromise = existing;
        } else {
          const recallTimeoutMs = pluginConfig.recallTimeoutMs ?? DEFAULT_RECALL_TIMEOUT_MS;
          recallPromise = client.recall(
            {
              query: prompt,
              maxTokens: pluginConfig.recallMaxTokens || 1024,
              budget: pluginConfig.recallBudget,
              types: pluginConfig.recallTypes,
              preferObservations: pluginConfig.preferObservations,
              minScores: pluginConfig.recallMinScores,
            },
            recallTimeoutMs,
            recallLifecycle.signal
          );
          inflightRecalls.set(recallKey, recallPromise);
          void recallPromise
            .catch(() => {})
            .finally(() => {
              // An old generation can settle after start() installed a successor.
              if (inflightRecalls.get(recallKey) === recallPromise)
                inflightRecalls.delete(recallKey);
            });
        }

        const recallStart = pluginConfig.debugPerfTiming ? Date.now() : 0;
        const response = await recallPromise;
        if (!isCurrentRecall()) return;
        const recallElapsedMs = pluginConfig.debugPerfTiming ? Date.now() - recallStart : 0;

        if (!response.results || response.results.length === 0) {
          if (pluginConfig.debugPerfTiming) {
            log.info(
              formatHookPerf("before_prompt_build", Date.now() - perfHookStart, {
                recall_main: `${recallElapsedMs}ms`,
                source: existing ? "reused" : "fresh",
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
        const memoriesFormatted = formatMemories(results);

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
          // An AbortError now has two sources. The deadline is handled above as a
          // TimeoutError; this branch is reached when the service was stopped or
          // restarted mid-recall, which is not a timeout — quoting recallTimeoutMs
          // there reports a deadline that never elapsed, with a number unrelated
          // to the time actually spent. (#4450)
          if (recallLifecycle.signal?.aborted) {
            debug("[Hindsight] Auto-recall cancelled: service stopped, skipping memory injection");
          } else {
            log.warn(
              `[Hindsight] Auto-recall aborted after ${pluginConfig.recallTimeoutMs ?? DEFAULT_RECALL_TIMEOUT_MS}ms, skipping memory injection`
            );
          }
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
      const retainGeneration = lifecycle.generation;
      const { signal: retainSignal, isCurrent: retainLifecycleIsCurrent } = captureHookLifecycle();
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

        // Taken (and cleared) before any skip below, so the next run marks its own start.
        const runStartKey = effectiveCtx?.sessionKey;
        const runStart =
          !force && runStartKey && runStartBySession.has(runStartKey)
            ? runStartBySession.get(runStartKey)
            : undefined;
        if (!force && runStartKey) runStartBySession.delete(runStartKey);

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

        // Resolved once: `session_end` carries no transcript, so the forced flush
        // has to fetch it. Without this the guard below ended every session-close
        // flush before it began. (#4341)
        let eventMessages = event.context?.sessionEntry?.messages ?? event.messages;
        if (force && (!Array.isArray(eventMessages) || eventMessages.length === 0)) {
          eventMessages = await sessionEndMessages(event, ctx?.endedTranscript);
          if (Array.isArray(eventMessages)) {
            debug(`[Hindsight Hook] session_end: read ${eventMessages.length} messages`);
          }
        }

        // Durable turns (default OFF): every un-retained turn is a durable work item in the
        // session state, so a session_end with no transcript can still deliver it.
        const durableStateRoot = pluginConfig.durableTurns === true ? lifecycle.stateRoot : null;
        const markerSessionKey = durableStateRoot ? effectiveCtx?.sessionKey : undefined;
        if (!markerSessionKey && (!Array.isArray(eventMessages) || eventMessages.length === 0)) {
          debug("[Hindsight Hook] No messages in event, skipping retention");
          return;
        }

        if (pluginConfig.autoRetain === false) {
          debug("[Hindsight Hook] autoRetain is disabled, skipping retention");
          return;
        }

        // Chunked retention: skip non-Nth turns and use a sliding window when firing
        const retainEveryN = pluginConfig.retainEveryNTurns ?? 1;
        const allMessages: any[] = Array.isArray(eventMessages) ? eventMessages : [];
        let messagesToRetain = allMessages;
        let retainFullWindow = false;
        // Owned-work delivery (9c76cbda9): what this retain drains, cleared from the session
        // state by the completion writer once the server confirms the retain.
        let markerAdvance: { seq: number; hashes: string[]; generation: string | null } | null =
          null;
        let drainedRanges: Array<{ start: number; end: number; key?: string }> = [];
        let drainedLegacyIds: string[] = [];

        if (markerSessionKey && durableStateRoot) {
          // Every un-retained turn is a durable work item tagged with its owner - committed
          // ranges by commitTurn, captures by their own agent_end. A cadence boundary, the age
          // backstop or the owner's session_end delivers them; a detached predecessor close
          // delivers only its own work.
          const ownedByEngine = isTurnOwnedByEngine(ctx);
          const endedSessionId =
            force && typeof event?.sessionId === "string" ? event.sessionId : undefined;
          // R27.3: strict - a corrupt or unreconciled document throws here (caught below), so
          // nothing is captured, submitted or overwritten until it is reconciled.
          let state = loadSessionState(durableStateRoot, markerSessionKey, true);
          // Work already carried by a queued retain is not submitted again.
          const held = lifecycle.queue?.pendingForSession(markerSessionKey) ?? [];
          const heldRanges = held.flatMap((item) => item.completion?.marker?.ranges ?? []);
          const heldLegacy = new Set(
            held.flatMap((item) => item.completion?.marker?.legacyIds ?? [])
          );
          if (!force && !ownedByEngine) {
            // Keyed by the host runId, so a repeated delivery finds it (pending, queued or
            // delivered); nothing counts as captured until the durable append succeeds.
            const runId = typeof ctx?.runId === "string" ? ctx.runId : undefined;
            const itemId = runId ?? randomUUID();
            const turnMessages = sliceLastTurnsByUserBoundary(allMessages, 1)
              .filter((m: any) => m?.role === "user" || m?.role === "assistant")
              .map((m: any) => ({ role: m.role, content: messageText(m) }));
            const alreadyCaptured =
              state.deliveredCaptureIds.includes(itemId) ||
              heldLegacy.has(itemId) ||
              state.pendingLegacy.some((item) => item.id === itemId);
            if (!alreadyCaptured && turnMessages.length > 0) {
              state = updateSessionState(durableStateRoot, markerSessionKey, (s) => ({
                ...s,
                pendingLegacy: appendPendingLegacy(
                  s.pendingLegacy,
                  {
                    id: itemId,
                    ...(typeof ctx?.sessionId === "string" ? { sessionId: ctx.sessionId } : {}),
                    messages: turnMessages,
                    at: Date.now(),
                  },
                  (m) => log.warn(m)
                ),
              }));
            }
          }
          const ownedByCaller = (owner: string | undefined) =>
            !force ||
            owner === undefined ||
            endedSessionId === undefined ||
            owner === endedSessionId;
          let pendingRanges = state.pendingRetain.filter(
            (range) =>
              ownedByCaller(range.sessionId) && !heldRanges.some((h) => sameCommittedWork(h, range))
          );
          const emptyRanges = pendingRanges.filter((range) => range.messages.length === 0);
          if (emptyRanges.length > 0) {
            // Nothing conversational in these ranges: clear them without touching the marker.
            try {
              updateSessionState(durableStateRoot, markerSessionKey, (s) => ({
                ...s,
                pendingRetain: s.pendingRetain.filter(
                  (r) => !emptyRanges.some((empty) => sameCommittedWork(empty, r))
                ),
              }));
            } catch (e) {
              debug(`[Hindsight Hook] session state write failed: ${e}`);
            }
            pendingRanges = pendingRanges.filter((range) => range.messages.length > 0);
          }
          // Work older than DURABLE_STUCK_MS drains below cadence; unstamped ranges count as
          // stuck. Age only speeds delivery of work that exists - it never settles a turn.
          const isStuck = (at: number | undefined) =>
            typeof at !== "number" || Date.now() - at > DURABLE_STUCK_MS;
          const pendingLegacy = state.pendingLegacy.filter(
            (item) => ownedByCaller(item.sessionId) && !heldLegacy.has(item.id)
          );
          if (pendingRanges.length === 0 && pendingLegacy.length === 0) {
            debug(`[Hindsight Hook] owned work: nothing deliverable for ${markerSessionKey}`);
            return;
          }
          const stuckPending = [...pendingRanges, ...pendingLegacy].some((w) => isStuck(w.at));
          const pendingTurns =
            pendingLegacy.length +
            pendingRanges.flatMap((r) => r.messages).filter((m: any) => m?.role === "user").length;
          if (!force && !stuckPending && pendingTurns < retainEveryN) {
            debug(
              `[Hindsight Hook] owned work: ${pendingTurns}/${retainEveryN} un-retained turns - holding`
            );
            return;
          }
          // Deliver in capture order; ranges written before the stamp sort first.
          const deliverMsgs = [
            ...pendingRanges.map((r) => ({ at: r.at ?? 0, messages: r.messages })),
            ...pendingLegacy.map((l) => ({ at: l.at, messages: l.messages })),
          ]
            .sort((a, b) => a.at - b.at)
            .flatMap((w) => w.messages);
          messagesToRetain = deliverMsgs;
          retainFullWindow = true;
          drainedRanges = pendingRanges.map((r) => ({
            start: r.start,
            end: r.end,
            ...(r.key !== undefined ? { key: r.key } : {}),
          }));
          drainedLegacyIds = pendingLegacy.map((item) => item.id);
          markerAdvance = {
            // Legacy turns carry no transcript seq; only committed ranges advance the position.
            seq: pendingRanges.length > 0 ? Math.max(...pendingRanges.map((r) => r.end)) : 0,
            hashes: deliverMsgs.map(retainMessageHash),
            generation: state.lastGenerationSeen,
          };
          debug(
            `[Hindsight Hook] owned work: retaining ${deliverMsgs.length} msgs / ${pendingTurns} turns (${pendingRanges.length} ranges, ${pendingLegacy.length} legacy)`
          );
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

        const runStartIdx = retainFullWindow
          ? undefined
          : retainStartIndex(messagesToRetain, runStart);
        if (runStart !== undefined && runStartIdx === undefined) {
          debug(
            "[Hindsight Hook] Run start not found in transcript, retaining from the last user message"
          );
        }
        const retention = prepareRetentionTranscript(
          messagesToRetain,
          pluginConfig,
          retainFullWindow,
          runStartIdx
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
          lifecycle.asyncRetainOperationIdCapability === "unknown"
            ? await refreshQueueOperationIdCapability(retainGeneration, retainSignal)
            : lifecycle.asyncRetainOperationIdCapability;
        if (!retainLifecycleIsCurrent()) return;
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
            tags: inlineRetainTags,
            appendSupported: lifecycle.supportsUpdateModeAppend,
            operationId: createAsyncRetainOperationId(),
          }
        );

        // Retain to Hindsight
        debug(
          `[Hindsight] Retaining to bank ${bankId}, document: ${retainRequest.documentId}, chars: ${transcript.length}\n---\n${transcript.substring(0, 500)}${transcript.length > 500 ? "\n...(truncated)" : ""}\n---`
        );

        if (!lifecycle.queue) {
          throw new Error("Retain queue is unavailable; refusing an untracked memory submission");
        }
        const rawClient = lifecycle.client;
        if (!rawClient) throw new Error("Retain client is unavailable");
        const transport = createRetainTransport(rawClient, getActiveApiEndpoint(), USER_AGENT);
        if (!retainLifecycleIsCurrent()) return;
        // Durable capture precedes every network await (e8cb94b7a): the queue line is written
        // synchronously before the hook can return.
        lifecycle.queue.enqueue(
          bankId,
          {
            ...retainRequest,
            targetFingerprint: transport.targetFingerprint,
            ...(markerSessionKey && markerAdvance
              ? {
                  completion: {
                    sessionKey: markerSessionKey,
                    generation: markerAdvance.generation,
                    marker: {
                      seq: markerAdvance.seq,
                      hashes: markerAdvance.hashes,
                      ranges: drainedRanges,
                      ...(drainedLegacyIds.length > 0 ? { legacyIds: drainedLegacyIds } : {}),
                    },
                  },
                }
              : {}),
          },
          retainRequest.metadata
        );
        if (pluginConfig.debugPerfTiming) {
          log.info(
            formatHookPerf(hookName, Date.now() - perfHookStart, {
              outcome: "queued",
              bank: bankId,
              messages: messageCount,
            })
          );
        }
        // Ruling 2026-11-01 (Vern + Claude): agent_end never waits for delivery; the pass runs in
        // the background and pending work stays queued. session_end's one bounded awaited pass
        // (5 s budget) is owned by its hook, whether or not this close captured a tail (R21.1).
        if (hookName === "agent_end") {
          void flushRetainQueue(
            undefined,
            rawClient,
            retainOperationIdCapability,
            retainGeneration,
            retainSignal,
            transport
          ).catch(() => {});
        }
      } catch (error) {
        log.error("error retaining messages", error);
      }
    };

    // Hook signature: (event, ctx) where event has {messages, success, error?, durationMs?}
    onScoped("agent_end", async (event: any, ctx?: PluginHookAgentContext) => {
      await runRetain(event, ctx, { hookName: "agent_end" });
    });

    // session_end fires once per OpenClaw session close. We force-flush so
    // short conversations (fewer turns than `retainEveryNTurns`) and the
    // un-retained tail of long conversations are not silently dropped. (#1726)
    onScoped("session_end", async (event: any, ctx?: PluginHookAgentContext) => {
      await runRetain(event, ctx, { hookName: "session_end", force: true });
      // R19.2/R21.1: the close's single bounded awaited pass over everything queued, including a
      // tail captured just above. If a pass is already running this returns at once; that pass or
      // the timer finishes the row.
      await flushRetainQueue().catch(() => undefined);
    });
    debug("[Hindsight] Hooks registered");
    log.info("agent hooks registered");

    // OpenClaw's own memory surfaces read through this when Hindsight holds the
    // memory slot; without it they all report "memory plugin unavailable". (#4916)
    api.registerMemoryCapability?.({
      providerRuntime: createMemoryProviderRuntime({
        providerId: "hindsight-openclaw",
        agentBank: (agentId) => agentOnlyBankId(agentId, lifecycle.config || pluginConfig),
        recall: async (bankId, query, signal) => {
          const live = await scoped(readyClient)();
          if (!live) throw new Error("Hindsight client is not initialized");
          const config = lifecycle.config || pluginConfig;
          const response = await scopeClient(live.client, bankId).recall(
            {
              query,
              maxTokens: config.recallMaxTokens,
              budget: config.recallBudget,
              types: config.recallTypes,
            },
            config.recallTimeoutMs,
            signal
          );
          return (response.results ?? []).map((r) => ({
            id: r.id,
            text: r.text,
            score: r.scores?.final,
          }));
        },
        getMemory: async (bankId, memoryId, signal) => {
          const live = await scoped(readyClient)();
          if (!live) throw new Error("Hindsight client is not initialized");
          const res = await fetch(
            `${live.options.baseUrl.replace(/\/$/, "")}/v1/default/banks/${encodeURIComponent(bankId)}/memories/${encodeURIComponent(memoryId)}`,
            { headers: authHeaders(live.options), signal }
          );
          if (res.status === 404) return null;
          if (!res.ok) throw new Error(`Hindsight memory lookup failed: HTTP ${res.status}`);
          const body = (await res.json()) as { text?: unknown };
          return typeof body.text === "string" ? body.text : null;
        },
        healthy: async () => {
          const live = await scoped(readyClient)().catch(() => null);
          if (!live) return false;
          // Bounded: OpenClaw's status call waits on this, and a hung API must read
          // as unavailable rather than hang the Memory page.
          const res = await fetch(`${live.options.baseUrl.replace(/\/$/, "")}/health`, {
            headers: authHeaders(live.options),
            signal: AbortSignal.timeout(5000),
          }).catch(() => null);
          return res?.ok === true;
        },
      }),
    });

    // Hermes's agent tools on this session's own bank. (#5635)
    api.registerTool?.(
      (toolCtx) =>
        createHindsightTools(
          {
            bankFor: (ctx) => {
              const config = lifecycle.config || pluginConfig;
              // The same identity resolution auto-recall runs, so the tools land on its bank.
              const { resolvedCtx, skipReason } = resolveAndCacheIdentity({
                sessionKey: ctx.sessionKey,
                ctx: {
                  agentId: ctx.agentId,
                  sessionKey: ctx.sessionKey,
                  workspaceDir: ctx.workspaceDir,
                },
                senderIdHint: ctx.requesterSenderId,
                pluginConfig: config,
              });
              return skipReason
                ? { error: skipReason.detail }
                : { bankId: deriveBankId(resolvedCtx, config) };
            },
            clientFor: async (bankId) => {
              const live = await scoped(readyClient)();
              if (!live) throw new Error("Hindsight client is not initialized");
              await scoped(ensureBankDefaultsApplied)(bankId, lifecycle.config || pluginConfig);
              return live.client;
            },
            config: () => lifecycle.config || pluginConfig,
          },
          toolCtx
        ),
      { names: HINDSIGHT_TOOL_NAMES }
    );
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

/** A message's identity across hooks: the same message serializes the same way. */
function messageFingerprint(message: unknown): string {
  return createHash("sha256")
    .update(JSON.stringify(message) ?? "")
    .digest("hex");
}

function messageText(message: any): string {
  const content = message?.content;
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .filter((block: any) => block?.type === "text" && typeof block?.text === "string")
    .map((block: any) => block.text)
    .join("\n");
}

/**
 * Where a run starts, from the messages before_prompt_build sees: the fingerprint
 * of the last message the session already had, or null when it had none.
 *
 * Saving only from the last user message lost every message before it whenever
 * one run held more than one user message — a user writing again while the agent
 * was still working (OpenClaw steering) — so the first request and the agent's
 * work on it never reached memory (#5569). agent_end saves from this marker instead.
 * The current request may already be the last message; it belongs to this run.
 */
export function runStartMarker(messages: unknown[], currentUserMessage?: string): string | null {
  let end = messages.length;
  const last: any = messages[end - 1];
  if (
    last?.role === "user" &&
    typeof currentUserMessage === "string" &&
    messageText(last).trim() === currentUserMessage.trim()
  ) {
    end -= 1;
  }
  return end > 0 ? messageFingerprint(messages[end - 1]) : null;
}

/**
 * The index of the run's first message in `messages`, or undefined when the run
 * start is unknown or no longer in the transcript (compaction rewrote it).
 */
export function retainStartIndex(
  messages: unknown[],
  marker: string | null | undefined
): number | undefined {
  if (marker === undefined) return undefined;
  if (marker === null) return 0;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messageFingerprint(messages[i]) === marker) return i + 1;
  }
  return undefined;
}

export function prepareRetentionTranscript(
  messages: any[],
  pluginConfig: PluginConfig,
  retainFullWindow = false,
  runStartIdx?: number
): { transcript: string; messageCount: number } | null {
  if (!messages || messages.length === 0) {
    return null;
  }

  let targetMessages: any[];
  if (retainFullWindow) {
    // Chunked retention: retain the full sliding window (already sliced by caller)
    targetMessages = messages;
  } else {
    // Default: retain this run — from its start when known, else from the last
    // user message. Never later than the last user message.
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
    targetMessages = messages.slice(
      runStartIdx === undefined ? lastUserIdx : Math.min(runStartIdx, lastUserIdx)
    );
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
  return currentLifecycle().client;
}
