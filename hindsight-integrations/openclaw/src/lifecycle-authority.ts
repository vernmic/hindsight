import { AsyncLocalStorage } from "node:async_hooks";
import { resolve as resolvePath } from "node:path";
import type { HindsightServer } from "@vectorize-io/hindsight-all";
import type { HindsightClient, HindsightClientOptions } from "@vectorize-io/hindsight-client";
import type { RetainQueue } from "./retain-queue.js";
import type { PluginConfig } from "./types.js";

/** What assemble() records per session (e8cb AstinusAssembleFact). */
export type AssembleFact = {
  at: number;
  viewTokens: number;
  tokenBudget: number | null;
  /** "provider": viewTokens is calibrated to the session's provider-reported prompt tokens. */
  estimator: "host" | "plugin" | "provider";
  windowFirstSeq: number | null;
};

/**
 * The service lifecycle, client and retain queue, shared by every copy of this module that
 * serves one host instance (e8cb94b7 LifecycleAuthority, F1/R1/R2 2026-10-25).
 *
 * OpenClaw evaluates a fresh copy of the plugin module per plugin registry (#4828; tools.catalog
 * loads another), only starts one of them, and the hooks that run can belong to a different copy
 * than the started one - older or newer. Upstream kept this state per copy, so a hook copy read
 * its own never-initialized client ("Retain client is unavailable", 10.3 clone run 2026-10-31,
 * hindsight 653219262) and a never-started copy kept its own pre-start lifetime and queue. Every
 * copy now binds at plugin entry to the one record for its host state root: start() replaces the
 * lifecycle for all of them, the owning registration's stop() fences all of them, and one client,
 * one init promise, one queue and one flush flag serve them all.
 *
 * Generalized from e8cb: the client/options/init fields (e8cb kept them per copy and patched the
 * retain path to read the global owner instead) and the context engine's marks cache live here
 * too, so there is ONE per-root registry rather than one per piece of state.
 */
export interface LifecycleAuthority {
  generation: number;
  serviceController: AbortController | null;
  preServiceController: AbortController;
  /** The registration whose service.start() made the current lifecycle; null before any start and after its stop. */
  owner: object | null;
  /** R20.2: the effective plugin config (bank mapping, policy) for this root; reinit updates it. */
  config: PluginConfig | null;
  // --- client / init (shared so a hook copy never reads an uninitialized client) ---
  client: HindsightClient | null;
  clientOptions: HindsightClientOptions | null;
  initPromise: Promise<void> | null;
  isInitialized: boolean;
  lastReinitAttempt: number;
  isReinitInProgress: boolean;
  /** R14.2: token of the lazy re-init that owns isReinitInProgress; a superseded one never clears it. */
  reinitAttempt: object | null;
  // --- endpoint identity (P3 remainder): the API this root's client talks to and what it supports ---
  /** Embedded daemon started for this root; null in external-API mode. */
  hindsightServer: HindsightServer | null;
  usingExternalApi: boolean;
  /** Probed from the active endpoint (#3686); a hook copy must not retain with its own unprobed default. */
  supportsUpdateModeAppend: boolean;
  appendCapabilityProbed: boolean;
  asyncRetainOperationIdCapability: "supported" | "unsupported" | "unknown";
  /** Banks whose defaults were applied, per client (R23.1): a new client starts empty by construction. */
  banksWithDefaultsApplied: WeakMap<HindsightClient, Set<string>>;
  // --- retain queue ---
  /** Host instance root this record serves (ebce1ee8c: default queue path is root-relative); null when unrooted. */
  stateRoot: string | null;
  queue: RetainQueue | null;
  flushTimer: ReturnType<typeof setInterval> | null;
  flushInProgress: boolean;
  /** Lifecycle generation whose unknown-capability warning was already logged (e8cb). */
  capabilityWarningGeneration: number | null;
  // --- context engine (R2: the host can adopt one copy's engine while another copy's hook runs) ---
  lastAssemble: Map<string, AssembleFact>;
  /** Last-resort marks per session (e8cb astinusMarksIndex); per root by construction (R11.1). */
  marksIndex: Map<string, unknown[]>;
}

export function newLifecycleAuthority(): LifecycleAuthority {
  return {
    generation: 0,
    serviceController: null,
    preServiceController: new AbortController(),
    owner: null,
    config: null,
    client: null,
    clientOptions: null,
    initPromise: null,
    isInitialized: false,
    lastReinitAttempt: 0,
    isReinitInProgress: false,
    reinitAttempt: null,
    hindsightServer: null,
    usingExternalApi: false,
    supportsUpdateModeAppend: false,
    appendCapabilityProbed: false,
    asyncRetainOperationIdCapability: "unknown",
    banksWithDefaultsApplied: new WeakMap(),
    stateRoot: null,
    queue: null,
    flushTimer: null,
    flushInProgress: false,
    capabilityWarningGeneration: null,
    lastAssemble: new Map(),
    marksIndex: new Map(),
  };
}

const LIFECYCLE_AUTHORITIES = Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-authorities");

/** The process-wide lifecycle record for one host state root. */
export function lifecycleAuthorityFor(stateRoot: string): LifecycleAuthority {
  const store = globalThis as { [LIFECYCLE_AUTHORITIES]?: Map<string, LifecycleAuthority> };
  const authorities = (store[LIFECYCLE_AUTHORITIES] ??= new Map());
  const key = resolvePath(stateRoot);
  let authority = authorities.get(key);
  if (!authority) {
    authority = newLifecycleAuthority();
    authority.stateRoot = key;
    authorities.set(key, authority);
  }
  return authority;
}

const LIFECYCLE_SCOPE = Symbol.for("@vectorize-io/hindsight-openclaw/lifecycle-scope");

/**
 * R22.1: ONE process-wide scope beside the registry, so a hook bound by copy A keeps A's authority
 * even when it calls helpers (the global facade) owned by a later-evaluated copy B.
 */
export function lifecycleScope(): AsyncLocalStorage<LifecycleAuthority> {
  const store = globalThis as { [LIFECYCLE_SCOPE]?: AsyncLocalStorage<LifecycleAuthority> };
  return (store[LIFECYCLE_SCOPE] ??= new AsyncLocalStorage());
}
