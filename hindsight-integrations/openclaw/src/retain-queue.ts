/**
 * JSONL-backed retain owner: requests, server operations and unapplied completion receipts.
 *
 * When the Hindsight API is unreachable, retain requests are stashed in a local
 * JSONL file and flushed later. Used in both modes: a locally spawned daemon is
 * just as unreachable while it boots or after it crashes as a remote API is.
 *
 * Zero runtime dependencies; uses only Node built-ins.
 */

import {
  readFileSync,
  writeFileSync,
  appendFileSync,
  existsSync,
  renameSync,
  unlinkSync,
} from "fs";
import { randomBytes } from "crypto";

/** Applied only after the server's terminal completion proof is durable. */
export type RetainCompletionAction = {
  sessionKey: string;
  generation: string | null;
  marker?: {
    seq: number;
    hashes: string[];
    /** Committed ranges this retain delivers; `key` (the host advancementKey) is their identity (9c76cbda9). */
    ranges: CommittedRange[];
    /** Owned legacy turns this retain delivers (fork, 2026-10-25); cleared on completion. */
    legacyIds?: string[];
  };
  segment?: { segment: number; unitsDelta: number };
};

/** A committed transcript range; `key` is absent on records written before 2026-11-01. */
export type CommittedRange = { start: number; end: number; key?: string };

/**
 * Whether two committed-range records name the same work (9c76cbda9). The host advancementKey is
 * the identity: a destructive transcript rewrite can reuse sequence numbers, so coordinates
 * identify only two keyless legacy records, and a keyless record never matches a keyed one.
 */
export function sameCommittedWork(a: CommittedRange, b: CommittedRange): boolean {
  if (a.key !== undefined || b.key !== undefined) return a.key === b.key;
  return a.start === b.start && a.end === b.end;
}

export type RetainDeliveryState = "submitting" | "submitted" | "completed" | "failed" | "uncertain";

/** The subset of a retain payload the queue needs to persist and replay. */
export interface QueuedRetainPayload {
  targetFingerprint?: string;
  completion?: RetainCompletionAction;
  content: string;
  documentId?: string;
  context?: string;
  metadata?: Record<string, unknown>;
  tags?: string[];
  operationId?: string;
  updateMode?: "replace" | "append";
}

export interface QueuedRetain {
  targetFingerprint?: string;
  completion?: RetainCompletionAction;
  deliveryState?: RetainDeliveryState;
  serverOperationIds?: string[];
  idempotentSubmission?: boolean;
  lastError?: string;
  id: string;
  bankId: string;
  content: string;
  documentId: string;
  context?: string;
  metadata: Record<string, unknown>;
  tags?: string[];
  operationId?: string;
  updateMode?: "replace" | "append";
  createdAt: string; // ISO 8601
}

export interface RetainQueueOptions {
  /** Path to the JSONL queue file. The parent directory must already exist. */
  filePath: string;
  /** Max age in ms for queued items. `-1` (default) keeps items forever. */
  maxAgeMs?: number;
}

export class RetainQueue {
  private readonly filePath: string;
  private readonly maxAgeMs: number;
  private cachedSize: number;

  constructor(opts: RetainQueueOptions) {
    this.filePath = opts.filePath;
    this.maxAgeMs = opts.maxAgeMs ?? -1;
    this.cachedSize = this.readAll().length;
  }

  /** Capture a retain durably before its first network submission. */
  enqueue(
    bankId: string,
    request: QueuedRetainPayload,
    metadata?: Record<string, unknown>
  ): string {
    const item: QueuedRetain = {
      id: `${Date.now()}-${randomBytes(4).toString("hex")}`,
      bankId,
      targetFingerprint: request.targetFingerprint,
      completion: request.completion,
      content: request.content,
      documentId: request.documentId || "conversation",
      context: request.context,
      metadata: metadata || request.metadata || {},
      tags: request.tags,
      operationId: request.operationId,
      updateMode: request.updateMode,
      createdAt: new Date().toISOString(),
    };
    appendFileSync(this.filePath, JSON.stringify(item) + "\n", "utf8");
    this.cachedSize++;
    return item.id;
  }

  /** Get up to `limit` oldest pending items (FIFO). */
  peek(limit = 50): QueuedRetain[] {
    return this.readAll().slice(0, limit);
  }

  /** Remove a single item by id. */
  remove(id: string): void {
    const items = this.readAll().filter((i) => i.id !== id);
    this.writeAll(items);
  }

  /** Remove multiple items by id in a single file rewrite. */
  removeMany(ids: string[]): void {
    const idSet = new Set(ids);
    const items = this.readAll().filter((i) => !idSet.has(i.id));
    this.writeAll(items);
  }

  /** Persist a replay identity before the request is sent. */
  ensureOperationId(id: string, operationId: string): string {
    const items = this.readAll();
    const item = items.find((queued) => queued.id === id);
    if (!item) {
      throw new Error(`queued retain not found: ${id}`);
    }
    if (typeof item.operationId === "string" && item.operationId.length > 0) {
      return item.operationId;
    }
    item.operationId = operationId;
    // The synchronous atomic rewrite must complete before the caller can send
    // the request; otherwise another lost acknowledgement would lose this ID.
    this.writeAll(items);
    return operationId;
  }

  /** Persist state before a network await or local completion application. */
  update(
    id: string,
    patch: Partial<
      Pick<
        QueuedRetain,
        "deliveryState" | "serverOperationIds" | "idempotentSubmission" | "lastError"
      >
    >
  ): QueuedRetain {
    const items = this.readAll();
    const item = items.find((candidate) => candidate.id === id);
    if (!item) throw new Error(`queued retain not found: ${id}`);
    Object.assign(item, patch);
    this.writeAll(items);
    return item;
  }

  /** Raw ranges already held by this owner must not be submitted again. */
  pendingForSession(sessionKey: string): QueuedRetain[] {
    // Held-work identity is the owner session (99ed79c61): on 10.3 the transcript generation
    // rotates every turn (openclaw/openclaw#172031), so keying on it hid work queued under the
    // previous generation and every drain re-submitted it.
    return this.readAll().filter((item) => item.completion?.sessionKey === sessionKey);
  }

  /** Number of items waiting (cached, O(1)). */
  size(): number {
    return this.cachedSize;
  }

  /** Expire only unambiguously unsent payloads; never erase operation/source debt. */
  cleanup(): number {
    if (this.maxAgeMs < 0) return 0;
    const cutoff = Date.now() - this.maxAgeMs;
    const items = this.readAll();
    const kept = items.filter(
      (i) =>
        i.deliveryState !== undefined ||
        i.completion !== undefined ||
        i.operationId !== undefined ||
        !i.targetFingerprint ||
        new Date(i.createdAt).getTime() >= cutoff
    );
    const removed = items.length - kept.length;
    if (removed > 0) this.writeAll(kept);
    return removed;
  }

  /** No-op — kept for API symmetry with DB-backed queues. */
  close(): void {
    /* nothing to close */
  }

  // -------------------------------------------------------------------------

  private readAll(): QueuedRetain[] {
    if (!existsSync(this.filePath)) return [];
    const content = readFileSync(this.filePath, "utf8").trim();
    if (!content) return [];
    const items: QueuedRetain[] = [];
    for (const line of content.split("\n")) {
      if (!line.trim()) continue;
      let item: QueuedRetain;
      try {
        item = JSON.parse(line) as QueuedRetain;
      } catch {
        throw new Error("Retain queue contains malformed JSON; reconciliation required");
      }
      if (
        !item ||
        typeof item !== "object" ||
        typeof item.id !== "string" ||
        typeof item.bankId !== "string" ||
        typeof item.content !== "string" ||
        typeof item.documentId !== "string" ||
        typeof item.createdAt !== "string" ||
        !Number.isFinite(Date.parse(item.createdAt)) ||
        !item.metadata ||
        typeof item.metadata !== "object" ||
        Array.isArray(item.metadata)
      ) {
        throw new Error("Retain queue row has an invalid shape; reconciliation required");
      }
      items.push(item);
    }
    return items;
  }

  /** Atomically rewrite the file with the given items. */
  private writeAll(items: QueuedRetain[]): void {
    if (items.length === 0) {
      try {
        unlinkSync(this.filePath);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
      this.cachedSize = 0;
      return;
    }
    const tmpPath = this.filePath + ".tmp";
    writeFileSync(tmpPath, items.map((i) => JSON.stringify(i)).join("\n") + "\n", "utf8");
    renameSync(tmpPath, this.filePath);
    this.cachedSize = items.length;
  }
}
