/**
 * Count anchor: provider-calibrated token figures per session (2026-11-03).
 *
 * The tokenizer and the char estimate both miss what a provider actually counts, in
 * both directions: on 11-03, 1.55x over on a GLM desk whose view is 43% replayed-out
 * thinking, 0.85x under on a DeepSeek desk. Vern's bar is 15% of the model's own
 * reported value. So every figure is calibrated against the provider:
 *
 *   at assemble N:   record the view's last transcript seq and its two counts
 *                    (tokenizer T, char estimate H)
 *   at assemble N+1: the first provider usage after that seq is the call view N fed
 *                    (P); the ratios P/T and P/H become the session's calibration
 *   every figure:    count x ratio, labelled "derived"; no ratio yet -> "estimate"
 *
 * Scaling the count rather than adding a raw tokenizer delta to P keeps the delta in
 * the provider's units too: a delta full of thinking would otherwise carry the same
 * 1.55x error the anchor exists to remove.
 *
 * Pure: figures in, figures out. The host read lives in assemble-marks (SeqIndex).
 */
import type { ProviderUsage } from "./prompt-accounting.js";

/** A pairing outside this band is a mismatched join, not a calibration. */
export const MIN_RATIO = 0.2;
export const MAX_RATIO = 5;

export interface AnchorState {
  sessionId: string;
  /** Last mapped transcript seq of the most recent view; its call's usage follows it. */
  viewLastSeq: number | null;
  /** Tokenizer count of the most recent view (what the model was sent, by our count). */
  tokenizer: number;
  /** Char-estimate count of the most recent view. */
  host: number;
  /** The figure this module reported for the most recent view (for the accuracy check). */
  reported: number;
  /** Calibrations from the last good pairing; undefined until one exists. */
  ratioTokenizer?: number;
  ratioHost?: number;
  /** Seq of the provider event the ratios came from. */
  ratioSeq?: number;
}

export interface AnchorCheck {
  /** What we reported for the view. */
  reported: number;
  /** What the provider said for the same view. */
  provider: number;
  /** (reported - provider) / provider, as a fraction. */
  error: number;
}

/**
 * Fold the provider figure for the previous view into the state. Returns the new
 * state and the accuracy check for the figure reported for that view. A pairing with
 * an implausible ratio is ignored and the previous calibration kept.
 */
export function calibrate(
  prev: AnchorState,
  usage: ProviderUsage | null
): { state: AnchorState; check: AnchorCheck | null } {
  if (!usage || usage.promptTokens <= 0 || prev.tokenizer <= 0 || prev.host <= 0) {
    return { state: prev, check: null };
  }
  if (prev.ratioSeq !== undefined && usage.seq <= prev.ratioSeq) {
    // Already folded this provider event.
    return { state: prev, check: null };
  }
  const rt = usage.promptTokens / prev.tokenizer;
  const rh = usage.promptTokens / prev.host;
  const check: AnchorCheck = {
    reported: prev.reported,
    provider: usage.promptTokens,
    error: (prev.reported - usage.promptTokens) / usage.promptTokens,
  };
  if (rt < MIN_RATIO || rt > MAX_RATIO || rh < MIN_RATIO || rh > MAX_RATIO) {
    return { state: { ...prev, ratioSeq: usage.seq }, check };
  }
  return {
    state: { ...prev, ratioTokenizer: rt, ratioHost: rh, ratioSeq: usage.seq },
    check,
  };
}

/** The provider-calibrated figure for a tokenizer count, and where it came from. */
export function anchoredFigure(
  tokenizer: number,
  state: AnchorState | undefined
): { tokens: number; source: "derived" | "estimate" } {
  if (state?.ratioTokenizer !== undefined) {
    return { tokens: Math.round(tokenizer * state.ratioTokenizer), source: "derived" };
  }
  return { tokens: tokenizer, source: "estimate" };
}

/** The pressure scale for the char estimate: provider/estimate ratio, or 1 without one. */
export function pressureScale(state: AnchorState | undefined): number {
  return state?.ratioHost ?? 1;
}
