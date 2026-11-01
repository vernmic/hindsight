import { createHash, randomUUID } from "node:crypto";
import type { HindsightClient, RetainResponse } from "@vectorize-io/hindsight-client";
import { RetainQueue, type QueuedRetain } from "./retain-queue.js";

type Capability = "supported" | "unsupported" | "unknown";
type OperationStatus = { operation_id: string; status: string };
export type RetainTransport = {
  targetFingerprint: string;
  submit(item: QueuedRetain, operationId?: string, signal?: AbortSignal): Promise<RetainResponse>;
  status(bankId: string, operationId: string, signal?: AbortSignal): Promise<OperationStatus>;
};

export function createRetainTransport(
  client: HindsightClient,
  endpoint: { apiUrl: string | null; apiToken: string | null },
  userAgent: string
): RetainTransport {
  if (!endpoint.apiUrl) throw new Error("Retain destination is unavailable");
  const base = new URL(endpoint.apiUrl).href.replace(/\/$/, "");
  return {
    targetFingerprint: createHash("sha256")
      .update(JSON.stringify([base, endpoint.apiToken || ""]))
      .digest("hex"),
    submit: (item, operationId, signal) =>
      client.retain(item.bankId, item.content, {
        documentId: item.documentId,
        context: item.context,
        metadata: Object.fromEntries(
          Object.entries(item.metadata)
            .filter(([, value]) => value !== null && value !== undefined)
            .map(([key, value]) => [key, typeof value === "string" ? value : String(value)])
        ),
        tags: item.tags,
        updateMode: item.updateMode,
        async: true,
        signal: signal
          ? AbortSignal.any([signal, AbortSignal.timeout(5000)])
          : AbortSignal.timeout(5000),
        ...(operationId ? { operationId } : {}),
      }),
    async status(bankId, operationId, signal) {
      const timeout = AbortSignal.timeout(5000);
      const headers: Record<string, string> = { "User-Agent": userAgent };
      if (endpoint.apiToken) headers.Authorization = `Bearer ${endpoint.apiToken}`;
      const response = await fetch(
        `${base}/v1/default/banks/${encodeURIComponent(bankId)}/operations/${encodeURIComponent(operationId)}`,
        {
          signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
          headers,
        }
      );
      if (!response.ok) throw new Error(`Retain operation status HTTP ${response.status}`);
      const value = (await response.json()) as Partial<OperationStatus>;
      if (value.operation_id !== operationId || typeof value.status !== "string") {
        throw new Error("Retain operation status identity is invalid");
      }
      return value as OperationStatus;
    },
  };
}

/** One bounded pass by the existing queue owner; pending work is polled next pass. */
export async function reconcileRetainQueue(params: {
  queue: RetainQueue;
  transport: RetainTransport;
  capability: Capability;
  signal?: AbortSignal;
  isCurrent(): boolean;
  applyCompletion(item: QueuedRetain): void;
  onIssue(message: string): void;
}): Promise<number> {
  const { queue, transport } = params;
  const blocked = new Set<string>();
  const deadline = Date.now() + 5000;
  let completed = 0;
  const current = () => params.isCurrent() && !params.signal?.aborted;
  const issue = (item: QueuedRetain, state: "failed" | "uncertain", message: string) => {
    if (!current()) return;
    if (item.lastError !== message || item.deliveryState !== state) {
      queue.update(item.id, { deliveryState: state, lastError: message });
      params.onIssue(`retain ${item.id}: ${message}; payload and completion debt retained`);
    }
  };
  if (!current()) return completed;
  queue.cleanup();
  // Select session heads before the work cap so a blocked backlog cannot hide other sessions.
  const items = queue.peek(Number.MAX_SAFE_INTEGER);
  let visited = 0;
  for (let item of items) {
    if (!current() || Date.now() >= deadline) return completed;
    const session = item.completion?.sessionKey ?? `${item.bankId}:${item.documentId}`;
    if (blocked.has(session)) continue;
    if (++visited > 50) break;
    blocked.add(session);
    if (!item.targetFingerprint || item.targetFingerprint !== transport.targetFingerprint) {
      issue(item, "uncertain", "destination needs explicit reconciliation");
      continue;
    }
    try {
      if (
        (item.deliveryState === "uncertain" || item.deliveryState === "failed") &&
        !item.serverOperationIds?.length
      )
        continue;
      if (item.deliveryState !== "completed" && !item.serverOperationIds?.length) {
        if (item.deliveryState === "submitting" && item.idempotentSubmission !== true) {
          issue(
            item,
            "uncertain",
            "submission acknowledgement is unknown and cannot be safely replayed"
          );
          continue;
        }
        if (params.capability === "unknown") continue;
        // A previously supported attempt must not be replayed without its wire UUID on downgrade.
        if (item.deliveryState === "submitting" && params.capability !== "supported") continue;
        const operationId = queue.ensureOperationId(item.id, item.operationId ?? randomUUID());
        const idempotent = params.capability === "supported";
        item = queue.update(item.id, {
          deliveryState: "submitting",
          idempotentSubmission: idempotent,
        });
        const response = await transport.submit(
          item,
          idempotent ? operationId : undefined,
          params.signal
        );
        if (!current()) return completed;
        if (response.success !== true) {
          issue(item, "failed", "submission did not report success");
          continue;
        }
        if (response.async === false) {
          item = queue.update(item.id, { deliveryState: "completed", lastError: undefined });
        } else {
          const ids = [
            ...new Set([
              ...(response.operation_ids ?? []),
              ...(response.operation_id ? [response.operation_id] : []),
            ]),
          ];
          if (
            !ids.length ||
            ids.some((id) => typeof id !== "string" || !id) ||
            (idempotent && !ids.includes(operationId))
          ) {
            issue(
              item,
              "uncertain",
              "asynchronous acceptance lacks the matching operation identity"
            );
            continue;
          }
          item = queue.update(item.id, {
            deliveryState: "submitted",
            serverOperationIds: ids,
            lastError: undefined,
          });
        }
      }
      if (item.deliveryState !== "completed") {
        let allCompleted = true;
        let failure: "failed" | "uncertain" | undefined;
        let failedStatus = "";
        for (const id of item.serverOperationIds ?? []) {
          if (!current() || Date.now() >= deadline) return completed;
          const proof = await transport.status(item.bankId, id, params.signal);
          if (!current()) return completed;
          if (proof.operation_id !== id) throw new Error("Retain operation identity changed");
          if (proof.status !== "completed") allCompleted = false;
          if (proof.status === "failed" || proof.status === "cancelled") {
            failure = "failed";
            failedStatus = proof.status;
          } else if (!["pending", "processing", "completed"].includes(proof.status)) {
            failure ??= "uncertain";
            failedStatus = proof.status;
          }
        }
        if (failure) {
          issue(item, failure, `server operation is ${failedStatus}`);
          continue;
        }
        if (!allCompleted || !item.serverOperationIds?.length) {
          if (item.deliveryState !== "submitted")
            queue.update(item.id, { deliveryState: "submitted", lastError: undefined });
          continue;
        }
        item = queue.update(item.id, { deliveryState: "completed", lastError: undefined });
      }
      if (!current()) return completed;
      params.applyCompletion(item);
      queue.remove(item.id);
      completed++;
      blocked.delete(session);
    } catch (error) {
      if (!current()) return completed;
      if (item.deliveryState === "submitting" && !item.idempotentSubmission) {
        issue(
          item,
          "uncertain",
          "submission acknowledgement is unknown and cannot be safely replayed"
        );
      } else if (item.lastError !== String(error)) {
        queue.update(item.id, { lastError: String(error) });
        params.onIssue(
          `retain ${item.id}: reconciliation unavailable; payload and completion debt retained`
        );
      }
    }
  }
  return completed;
}
