/**
 * Token accounting for an assembled prompt.
 *
 * One generic BPE tokenizer (cl100k_base) is used across every model. Per-model
 * exactness is not required: the accepted accuracy is within 10 percent of the
 * provider's reported prompt tokens, and no per-model calibration is maintained.
 *
 * This module is pure. It receives message lists and returns counts. It reads no
 * files, no host tables, no environment and no session state, and it knows no
 * desk names, session keys or private paths.
 */

// Package subpath export. tsconfig maps this specifier to the shipped
// declaration because this package compiles with moduleResolution "node", which
// does not read an "exports" map; the bundler-side resolver uses the export.
import { countTokens } from "gpt-tokenizer/encoding/cl100k_base";

/** Token categories reported for a prompt. */
export type TokenCategory =
  | "system"
  | "user"
  | "assistant_text"
  | "assistant_reasoning"
  | "tool_call"
  | "tool_result"
  | "recall_injection"
  | "enrich_injection"
  | "other";

export const TOKEN_CATEGORIES: readonly TokenCategory[] = [
  "system",
  "user",
  "assistant_text",
  "assistant_reasoning",
  "tool_call",
  "tool_result",
  "recall_injection",
  "enrich_injection",
  "other",
];

export type CategoryCounts = Record<TokenCategory, number>;

/** A message as the engine sees it: only the fields this module reads. */
export interface PromptMessage {
  role?: string;
  content?: unknown;
  customType?: string;
  /** Transcript sequence number, when the caller knows it. Used for mark ranges. */
  seq?: number;
  [key: string]: unknown;
}

/** A mark, reduced to what counting needs. */
export interface MarkRange {
  markId: number | string;
  seqStart: number;
  seqEnd: number;
}

/** A delimited span that belongs to an injection rather than to its message. */
export interface InjectionSpan {
  start: string;
  end: string;
}

/** Encoder returning a token count for a string. Injectable for tests. */
export type Encoder = (text: string) => number;

export interface CountOptions {
  /** Defaults to cl100k_base via countTokens. */
  encoder?: Encoder;
  /**
   * Approximate per-message framing cost added once per message. Zero by
   * default: the count is the tokenizer's count of the content the model sees,
   * with framing left to the caller's tolerance.
   */
  messageOverhead?: number;
  /** Recall spans. Defaults to the two tags this plugin writes. */
  recallSpans?: readonly InjectionSpan[];
  /** Enrich spans, supplied by the code that wrote them. No defaults. */
  enrichSpans?: readonly InjectionSpan[];
  /** Exact customType values that mean enrich. No substring matching. */
  enrichTypes?: readonly string[];
}

export interface BlockCount {
  messageIndex: number;
  seq?: number;
  category: TokenCategory;
  tokens: number;
}

export interface CountResult {
  total: number;
  byCategory: CategoryCounts;
  byBlock: BlockCount[];
  messageCount: number;
  /** Image blocks count as zero tokens; the count is surfaced, not hidden. */
  imageBlockCount: number;
}

export interface MarkedContentResult {
  total: number;
  byCategory: CategoryCounts;
  /** Tokens removed per mark, in the order the marks were supplied. */
  byMark: Array<{ markId: number | string; tokens: number }>;
  imageBlockCount: number;
}

export const DEFAULT_RECALL_SPANS: readonly InjectionSpan[] = [
  { start: "<hindsight_memories>", end: "</hindsight_memories>" },
  { start: "<relevant_memories>", end: "</relevant_memories>" },
];

export function emptyCategoryCounts(): CategoryCounts {
  const counts = {} as CategoryCounts;
  for (const category of TOKEN_CATEGORIES) counts[category] = 0;
  return counts;
}

/**
 * countTokens, not encode().length: the encoder refuses text that mentions a
 * special token unless every special is disallowed explicitly, and a transcript
 * can carry such text in any tool result. Allow nothing; count everything.
 */
const defaultEncoder: Encoder = (text: string) =>
  countTokens(text, { disallowedSpecial: new Set<string>() });

/** Token count of one string under the configured encoder. */
export function countText(text: string, options: CountOptions = {}): number {
  if (!text) return 0;
  const encoder = options.encoder ?? defaultEncoder;
  return encoder(text);
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function blockText(block: Record<string, unknown>): string {
  const text = block.text;
  if (typeof text === "string") return text;
  const reasoning = block.thinking ?? block.reasoning;
  if (typeof reasoning === "string") return reasoning;
  const args = block.arguments ?? block.input;
  if (args !== undefined) {
    try {
      return JSON.stringify(args);
    } catch {
      return String(args);
    }
  }
  return "";
}

function blockType(block: Record<string, unknown>): string {
  const type = block.type;
  return typeof type === "string" ? type : "";
}

function stringContent(message: PromptMessage): string | undefined {
  return typeof message.content === "string" ? message.content : undefined;
}

function collectText(message: PromptMessage): string {
  const flat = stringContent(message);
  if (flat !== undefined) return flat;
  if (!Array.isArray(message.content)) return "";
  const parts: string[] = [];
  for (const raw of message.content) {
    const block = asRecord(raw);
    if (block && typeof block.text === "string") parts.push(block.text);
  }
  return parts.join("\n");
}

/** Split text into plain and injected spans. Longest delimiters win on overlap. */
function splitSpans(
  text: string,
  spans: ReadonlyArray<{ span: InjectionSpan; category: TokenCategory }>
): Array<{ text: string; category: TokenCategory | null }> {
  if (!text) return [];
  const hits: Array<{ start: number; end: number; category: TokenCategory }> = [];
  for (const { span, category } of spans) {
    if (!span.start || !span.end) continue;
    let from = 0;
    for (;;) {
      const at = text.indexOf(span.start, from);
      if (at < 0) break;
      const close = text.indexOf(span.end, at + span.start.length);
      if (close < 0) break;
      hits.push({ start: at, end: close + span.end.length, category });
      from = close + span.end.length;
    }
  }
  if (hits.length === 0) return [{ text, category: null }];
  hits.sort((a, b) => a.start - b.start || b.end - a.end);

  const out: Array<{ text: string; category: TokenCategory | null }> = [];
  let cursor = 0;
  for (const hit of hits) {
    if (hit.start < cursor) continue; // contained in an earlier, longer span
    if (hit.start > cursor) out.push({ text: text.slice(cursor, hit.start), category: null });
    out.push({ text: text.slice(hit.start, hit.end), category: hit.category });
    cursor = hit.end;
  }
  if (cursor < text.length) out.push({ text: text.slice(cursor), category: null });
  return out;
}

function injectionSpans(
  options: CountOptions
): Array<{ span: InjectionSpan; category: TokenCategory }> {
  const recall = options.recallSpans ?? DEFAULT_RECALL_SPANS;
  const enrich = options.enrichSpans ?? [];
  return [
    ...recall.map((span) => ({ span, category: "recall_injection" as TokenCategory })),
    ...enrich.map((span) => ({ span, category: "enrich_injection" as TokenCategory })),
  ];
}

/**
 * Category for a message as a whole. Only an exact customType marks the whole
 * message; recall and enrich blocks are split at block level by span.
 */
export function classifyMessage(message: PromptMessage, options: CountOptions = {}): TokenCategory {
  const customType = typeof message.customType === "string" ? message.customType : "";
  const enrichTypes = options.enrichTypes ?? [];
  if (customType && enrichTypes.includes(customType)) return "enrich_injection";

  const role = typeof message.role === "string" ? message.role : "";
  if (role === "system") return "system";
  if (role === "toolResult" || role === "tool_result" || role === "tool") return "tool_result";
  if (role === "user") return "user";
  if (role === "assistant") return "assistant_text";
  return "other";
}

export interface ClassifiedBlock {
  category: TokenCategory;
  text: string;
}

/** Blocks of one message, with injection spans split out of their carrier. */
export function classifyBlocks(
  message: PromptMessage,
  options: CountOptions = {}
): ClassifiedBlock[] {
  const messageCategory = classifyMessage(message, options);
  const spans = injectionSpans(options);
  const flat = stringContent(message);
  const out: ClassifiedBlock[] = [];

  const pushText = (text: string, base: TokenCategory): void => {
    if (!text) return;
    for (const part of splitSpans(text, spans)) {
      out.push({ category: part.category ?? base, text: part.text });
    }
  };

  if (flat !== undefined) {
    pushText(flat, messageCategory);
    return out;
  }
  if (!Array.isArray(message.content)) return out;

  for (const raw of message.content) {
    const block = asRecord(raw);
    if (!block) continue;
    const type = blockType(block);
    if (type === "image" || type === "image_url" || type === "input_image") {
      out.push({ category: "other", text: "" });
      continue;
    }
    const text = blockText(block);
    if (type === "thinking" || type === "reasoning") {
      if (text) out.push({ category: "assistant_reasoning", text });
      continue;
    }
    if (
      type === "toolCall" ||
      type === "tool_call" ||
      type === "toolUse" ||
      type === "tool_use" ||
      type === "function_call"
    ) {
      if (text) out.push({ category: "tool_call", text });
      continue;
    }
    pushText(text, messageCategory);
  }
  return out;
}

function isImageBlock(raw: unknown): boolean {
  const block = asRecord(raw);
  if (!block) return false;
  const type = blockType(block);
  return type === "image" || type === "image_url" || type === "input_image";
}

/** Count every block of a prompt, by category, with a per-block breakdown. */
export function countPromptTokens(
  messages: readonly PromptMessage[],
  options: CountOptions = {}
): CountResult {
  const encoder = options.encoder ?? defaultEncoder;
  const overhead = options.messageOverhead ?? 0;
  const byCategory = emptyCategoryCounts();
  const byBlock: BlockCount[] = [];
  let total = 0;
  let imageBlockCount = 0;

  messages.forEach((message, messageIndex) => {
    if (Array.isArray(message.content)) {
      for (const raw of message.content) if (isImageBlock(raw)) imageBlockCount += 1;
    }
    for (const block of classifyBlocks(message, options)) {
      const tokens = block.text ? encoder(block.text) : 0;
      byCategory[block.category] += tokens;
      byBlock.push({ messageIndex, seq: message.seq, category: block.category, tokens });
      total += tokens;
    }
    if (overhead > 0) {
      total += overhead;
      byBlock.push({ messageIndex, seq: message.seq, category: "other", tokens: overhead });
      byCategory.other += overhead;
    }
  });

  return { total, byCategory, byBlock, messageCount: messages.length, imageBlockCount };
}

/** Tokens held by the messages a mark range covers, counting each message whole. */
function tokensInRange(
  messages: readonly PromptMessage[],
  marks: readonly MarkRange[],
  options: CountOptions
): { perMark: number[]; imageBlockCount: number } {
  const encoder = options.encoder ?? defaultEncoder;
  const perMark = marks.map(() => 0);
  let imageBlockCount = 0;

  messages.forEach((message) => {
    const seq = message.seq;
    if (typeof seq !== "number") return;
    const owner = marks.findIndex((mark) => seq >= mark.seqStart && seq <= mark.seqEnd);
    if (owner < 0) return;
    if (Array.isArray(message.content)) {
      for (const raw of message.content) if (isImageBlock(raw)) imageBlockCount += 1;
    }
    for (const block of classifyBlocks(message, options)) {
      if (block.text) perMark[owner] += encoder(block.text);
    }
  });

  return { perMark, imageBlockCount };
}

/**
 * Tokens a set of marks actually removed, computed as a diff of the pre-mark
 * and post-mark lists: marks leave placeholders, digests and redacted blocks
 * behind, so counting whole messages in a range overstates the removal.
 *
 * - total: count(before) - count(after), exact by construction.
 * - byCategory: the same subtraction per category.
 * - byMark: pre-mark tokens in the mark's range, minus post-mark tokens in the
 *   same range (the replacement text or digest that stayed). A message covered
 *   by more than one mark is attributed to the first covering mark.
 */
export function countMarkRemoval(
  before: readonly PromptMessage[],
  after: readonly PromptMessage[],
  marks: readonly MarkRange[],
  options: CountOptions = {}
): MarkedContentResult {
  const beforeCount = countPromptTokens(before, options);
  const afterCount = countPromptTokens(after, options);
  const byCategory = emptyCategoryCounts();
  for (const category of TOKEN_CATEGORIES) {
    byCategory[category] = beforeCount.byCategory[category] - afterCount.byCategory[category];
  }

  const pre = tokensInRange(before, marks, options);
  const post = tokensInRange(after, marks, options);
  const byMark = marks.map((mark, index) => ({
    markId: mark.markId,
    tokens: pre.perMark[index] - post.perMark[index],
  }));

  return {
    total: beforeCount.total - afterCount.total,
    byCategory,
    byMark,
    imageBlockCount: beforeCount.imageBlockCount - afterCount.imageBlockCount,
  };
}
