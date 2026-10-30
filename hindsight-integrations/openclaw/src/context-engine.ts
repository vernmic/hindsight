// Context engine registration, ported from the maintained plugin
// (e8cb94b7 src/index.ts:3130-3250 registration, :3286 assemble fallback,
// :3566-3587 compact delegation). This wave ports the NON-durable engine shell:
//
// - info: ownsCompaction false, no hostRequirements, and NO transcriptSemantics /
//   commitTurn. Without the durable-turn declaration no host hands this engine
//   turn ownership, so the ordinary agent_end capture stays the only capture on
//   every host and the wave-4 ownership predicate is intentionally not wired yet.
// - ingest: no-op (statement<->topic mapping lands with the session state port).
// - assemble: the maintained pass-through fallback (no marks applied) with
//   preassembly_may_overflow authority, so the host keeps its overflow precheck.
// - compact: the maintained delegation to the runtime's built-in compaction.
//   Owner compaction is not ported and stays unavailable.
//
// TODO(host-capability): when commitTurn is ported, declare
//   hostRequirements["agent-run"].requiredCapabilities = ["agent-end-context-engine-turn-v1"]
// only after Vern confirms the host contract to Grokbot (see package 6).
//
// ENGINE ID = PLUGIN ID ("hindsight-openclaw"): the host reads
// plugins.slots.contextEngine both as the owning plugin id and as the engine id.
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

export function createContextEngine(
  log: Logger,
  loadDelegate: () => Promise<CompactionDelegate> = loadRuntimeDelegate
) {
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
    async assemble(params: { messages?: unknown[]; tokenBudget?: number }): Promise<{
      messages: unknown[];
      estimatedTokens: number;
      promptAuthority: "preassembly_may_overflow";
    }> {
      const list = Array.isArray(params.messages) ? params.messages : [];
      return {
        messages: list,
        estimatedTokens: list.length > 0 ? estimateTokens(list) : 0,
        promptAuthority: "preassembly_may_overflow",
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
  log: Logger
): boolean {
  const register = api.registerContextEngine;
  if (typeof register !== "function") {
    log.info("[hindsight-engine] host does not expose registerContextEngine; skipping");
    return false;
  }
  try {
    register.call(api, ENGINE_ID, () => createContextEngine(log));
    return true;
  } catch (e) {
    log.warn(`[hindsight-engine] registerContextEngine failed: ${(e as Error)?.message ?? e}`);
    return false;
  }
}
