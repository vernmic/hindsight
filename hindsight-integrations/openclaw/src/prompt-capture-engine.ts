/**
 * Engine-view capture: the pre-mark and post-mark lists of one model call,
 * stored together with the marks that transformed them.
 *
 * This is the engine view, not the wire payload. It exists so the mark
 * transform is auditable (A2/A3/A5) and so the wire capture has something to
 * correlate against. Pure: lists and marks in, a turn record and blobs out.
 */

import { countMarkRemoval, countPromptTokens, type CountOptions, type MarkRange, type PromptMessage, type CategoryCounts } from "./token-count.js";
import type { CapturedTurn } from "./prompt-capture.js";

export interface EngineViewInput {
  turnId: string;
  startedAt: number;
  completedAt: number;
  before: readonly PromptMessage[];
  after: readonly PromptMessage[];
  marks: readonly MarkRange[];
  marksInEffect: number;
  admittedMarkIds: Array<number | string>;
  providerPromptTokens?: number;
  options?: CountOptions;
}

export interface EngineViewResult {
  turn: CapturedTurn;
  /** Exact bytes to store, content-addressed by the caller's blob store. */
  bodies: Array<{ name: "before" | "after"; bytes: Buffer }>;
  removal: { total: number; byCategory: CategoryCounts; byMark: Array<{ markId: number | string; tokens: number }> };
}

/**
 * Build the capture record for one assemble call.
 *
 * Both lists are serialized in full so a reader can reproduce the exact
 * transform; the manifest carries the totals, so a reader does not have to
 * tokenize to answer "how much did the marks remove".
 */
export function buildEngineViewCapture(input: EngineViewInput): EngineViewResult {
  const options = input.options ?? {};
  const before = countPromptTokens(input.before, options);
  const after = countPromptTokens(input.after, options);
  const removal = countMarkRemoval(input.before, input.after, input.marks, options);

  const beforeBytes = Buffer.from(JSON.stringify(input.before), "utf8");
  const afterBytes = Buffer.from(JSON.stringify(input.after), "utf8");

  return {
    turn: {
      turnId: input.turnId,
      startedAt: input.startedAt,
      completedAt: input.completedAt,
      messageCount: input.after.length,
      calls: [],
      providerPromptTokens: input.providerPromptTokens,
      marksInEffect: input.marksInEffect,
      admittedMarkIds: [...input.admittedMarkIds],
      categories: after.byCategory,
      engineView: { beforeTotal: before.total, afterTotal: after.total },
    },
    bodies: [
      { name: "before", bytes: beforeBytes },
      { name: "after", bytes: afterBytes },
    ],
    removal: { total: removal.total, byCategory: removal.byCategory, byMark: removal.byMark },
  };
}
