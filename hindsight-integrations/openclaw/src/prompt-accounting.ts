/**
 * Prompt accounting: the provider usage join and the before/after record.
 *
 * The provider's per-call figures already exist in the host transcript
 * (assistant message.usage). This module turns those figures plus the engine's
 * own estimates into one record per assemble call, and labels where every
 * figure came from so a reader never has to guess.
 *
 * Pure: events and figures come in, a record goes out. No file system, no host
 * database, no environment, no session or desk names.
 */

export type FigureSource = "provider" | "estimate" | "derived";

export interface ProviderUsage {
  /** Provider-reported prompt tokens for the call that carried the view. */
  promptTokens: number;
  input: number;
  cacheRead: number;
  cacheWrite: number;
  /** Transcript seq of the assistant event the figures were read from. */
  seq: number;
}

export interface TranscriptEventLike {
  seq: number;
  eventJson: string;
}

export interface AccountingInput {
  sessionId: string;
  generation: string;
  /** First transcript seq the assembled view was built from. */
  fenceSeq: number | null;
  marksInEffect: number;
  admittedMarkIds: Array<number | string>;
  estimatedBefore: number;
  estimatedAfter: number;
  /** Joined provider figures for this call, when a later event carries them. */
  usage?: ProviderUsage | null;
  /** Prior record with zero marks in effect, when one exists. */
  baseline?: { promptTokens: number; at: number } | null;
  /** Tokens added since the baseline request, from the tokenizer. */
  deltaSinceBaseline?: number | null;
  now: number;
}

export interface AccountingRecord {
  sessionId: string;
  generation: string;
  fenceSeq: number | null;
  marksInEffect: number;
  admittedMarkIds: Array<number | string>;
  figures: {
    before: number;
    after: number;
  };
  sources: {
    before: FigureSource;
    after: FigureSource;
  };
  usage: ProviderUsage | null;
  recordedAt: number;
}

function asNumber(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

/**
 * Provider usage from one transcript event, or null.
 *
 * Assistant events carry message.usage with input, output, cacheRead,
 * cacheWrite, reasoningTokens and contextUsage.promptTokens. Some assistant
 * events carry every figure as zero (host-generated notices); those are not
 * provider calls and must never be joined.
 */
export function usageFromEvent(event: TranscriptEventLike): ProviderUsage | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(event.eventJson);
  } catch {
    return null;
  }
  const envelope = asRecord(parsed);
  if (!envelope) return null;
  const message = asRecord(envelope.message) ?? envelope;
  const usage = asRecord(message.usage);
  if (!usage) return null;

  const input = asNumber(usage.input);
  const cacheRead = asNumber(usage.cacheRead);
  const cacheWrite = asNumber(usage.cacheWrite);
  const contextUsage = asRecord(usage.contextUsage);
  const promptTokens = asNumber(contextUsage?.promptTokens) || input + cacheRead;
  if (promptTokens <= 0) return null;

  return { promptTokens, input, cacheRead, cacheWrite, seq: event.seq };
}

/** First provider figure at or after the fence. The call the view fed. */
export function findProviderUsage(
  events: readonly TranscriptEventLike[],
  fenceSeq: number | null
): ProviderUsage | null {
  const from = fenceSeq ?? Number.NEGATIVE_INFINITY;
  for (const event of events) {
    if (event.seq < from) continue;
    const usage = usageFromEvent(event);
    if (usage) return usage;
  }
  return null;
}

/**
 * Build the record for one assemble call.
 *
 * after: the provider figure when the call has one, else the engine estimate.
 * before: the baseline provider figure plus the tokenizer delta since it, when
 * a zero-mark baseline exists; otherwise the engine estimate. Never a provider
 * figure presented as exact when it was derived.
 */
export function buildAccountingRecord(input: AccountingInput): AccountingRecord {
  const afterSource: FigureSource = input.usage ? "provider" : "estimate";
  const after = input.usage ? input.usage.promptTokens : input.estimatedAfter;

  let before = input.estimatedBefore;
  let beforeSource: FigureSource = "estimate";
  if (input.baseline && typeof input.deltaSinceBaseline === "number") {
    before = input.baseline.promptTokens + input.deltaSinceBaseline;
    beforeSource = "derived";
  }

  return {
    sessionId: input.sessionId,
    generation: input.generation,
    fenceSeq: input.fenceSeq,
    marksInEffect: input.marksInEffect,
    admittedMarkIds: [...input.admittedMarkIds],
    figures: { before, after },
    sources: { before: beforeSource, after: afterSource },
    usage: input.usage ?? null,
    recordedAt: input.now,
  };
}

/**
 * How much of the after figure is accounted for by the provider's cache read.
 * Reported, never subtracted: a cached prompt is still prompt tokens.
 */
export function cachedFraction(usage: ProviderUsage | null): number | null {
  if (!usage || usage.promptTokens <= 0) return null;
  return usage.cacheRead / usage.promptTokens;
}
