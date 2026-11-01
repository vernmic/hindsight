import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import type { HindsightClient } from "@vectorize-io/hindsight-client";
import { buildRetainRequest, createAsyncRetainOperationId, flushRetainQueue } from "./index.js";
import { RetainQueue } from "./retain-queue.js";
import type { RetainTransport } from "./retain-delivery.js";
async function flushTestQueue(
  queue: RetainQueue,
  client: HindsightClient,
  capability: "supported" | "unsupported" | "unknown",
  generation = 0,
  signal?: AbortSignal
) {
  const transport: RetainTransport = {
    targetFingerprint: "unit",
    submit: (item, operationId, signal) =>
      client.retain(item.bankId, item.content, {
        async: true,
        signal,
        ...(operationId ? { operationId } : {}),
      }),
    status: async (_bank, id) => ({ operation_id: id, status: "completed" }),
  };
  return flushRetainQueue(queue, client, capability, generation, signal, transport);
}

const tempDirs: string[] = [];

function makeQueuePath(): string {
  const dir = mkdtempSync(join(tmpdir(), "hindsight-retain-queue-"));
  tempDirs.push(dir);
  return join(dir, "retains.jsonl");
}

afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

describe("RetainQueue operation id persistence", () => {
  it("reuses the initial operation id after a lost acknowledgement and restart", async () => {
    const queuePath = makeQueuePath();
    const operationId = createAsyncRetainOperationId();
    const request = buildRetainRequest("remember this", 1, {}, {}, 1_700_000_000_000, {
      turnIndex: 1,
      operationId,
    });
    const retain = vi
      .fn()
      .mockRejectedValueOnce(new Error("acknowledgement lost"))
      .mockResolvedValueOnce({ success: true, async: false });
    const rawClient = { retain } as unknown as HindsightClient;

    await expect(
      rawClient.retain("bank-1", request.content, { async: true, operationId })
    ).rejects.toThrow("acknowledgement lost");
    const initialOperationId = retain.mock.calls[0][2].operationId;

    const queue = new RetainQueue({ filePath: queuePath });
    queue.enqueue("bank-1", { ...request, targetFingerprint: "unit" });
    queue.close();

    const reloadedQueue = new RetainQueue({ filePath: queuePath });
    expect(reloadedQueue.peek()).toHaveLength(1);
    expect(reloadedQueue.peek()[0].operationId).toBe(initialOperationId);

    await flushTestQueue(reloadedQueue, rawClient, "supported");

    expect(retain).toHaveBeenCalledTimes(2);
    expect(retain.mock.calls[1][2].operationId).toBe(initialOperationId);
    expect(reloadedQueue.size()).toBe(0);
  });

  it("persists an id before the first supported replay of a legacy row", async () => {
    const queuePath = makeQueuePath();
    const queue = new RetainQueue({ filePath: queuePath });
    queue.enqueue("bank-1", {
      targetFingerprint: "unit",
      content: "legacy",
      documentId: "conversation",
    });
    const retain = vi
      .fn()
      .mockRejectedValueOnce(new Error("second acknowledgement lost"))
      .mockResolvedValueOnce({ success: true, async: false });
    const rawClient = { retain } as unknown as HindsightClient;

    await flushTestQueue(queue, rawClient, "supported");

    const firstReplayOperationId = retain.mock.calls[0][2].operationId;
    expect(firstReplayOperationId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
    );
    const afterFailure = new RetainQueue({ filePath: queuePath });
    expect(afterFailure.peek()[0].operationId).toBe(firstReplayOperationId);

    await flushTestQueue(afterFailure, rawClient, "supported");

    expect(retain.mock.calls[1][2].operationId).toBe(firstReplayOperationId);
    expect(afterFailure.size()).toBe(0);
  });

  it("holds an ambiguous unsupported send even after idempotency support returns", async () => {
    const queuePath = makeQueuePath();
    const operationId = createAsyncRetainOperationId();
    const queue = new RetainQueue({ filePath: queuePath });
    queue.enqueue("bank-1", { targetFingerprint: "unit", content: "queued", operationId });
    const retain = vi
      .fn()
      .mockRejectedValueOnce(new Error("old server unavailable"))
      .mockResolvedValueOnce({ success: true, async: false });
    const rawClient = { retain } as unknown as HindsightClient;

    await flushTestQueue(queue, rawClient, "unsupported");

    expect(retain.mock.calls[0][2]).not.toHaveProperty("operationId");
    const afterDowngrade = new RetainQueue({ filePath: queuePath });
    expect(afterDowngrade.peek()[0].operationId).toBe(operationId);

    await flushTestQueue(afterDowngrade, rawClient, "supported");

    expect(retain).toHaveBeenCalledOnce();
    expect(afterDowngrade.size()).toBe(1);
    expect(afterDowngrade.peek()[0].deliveryState).toBe("uncertain");
  });

  it("defers replay while the live capability probe is unknown", async () => {
    const queuePath = makeQueuePath();
    const queue = new RetainQueue({ filePath: queuePath });
    queue.enqueue("bank-1", { targetFingerprint: "unit", content: "queued" });
    const retain = vi.fn();
    const rawClient = { retain } as unknown as HindsightClient;

    await flushTestQueue(queue, rawClient, "unknown");

    expect(retain).not.toHaveBeenCalled();
    expect(queue.size()).toBe(1);
  });

  it("checkpoints an acknowledged legacy item before a later send is aborted", async () => {
    const queuePath = makeQueuePath();
    const queue = new RetainQueue({ filePath: queuePath });
    queue.enqueue("bank-1", { targetFingerprint: "unit", content: "first" });
    queue.enqueue("bank-1", { targetFingerprint: "unit", content: "second" });
    const controller = new AbortController();
    const retain = vi
      .fn()
      .mockResolvedValueOnce({ success: true, async: false })
      .mockImplementationOnce(async () => {
        controller.abort();
        throw new DOMException("service stopped", "AbortError");
      });
    const rawClient = { retain } as unknown as HindsightClient;

    await flushTestQueue(queue, rawClient, "unsupported", 0, controller.signal);

    expect(queue.peek().map((item) => item.content)).toEqual(["second"]);
  });

  it("refuses malformed legacy rows instead of silently dropping them on rewrite", () => {
    const queuePath = makeQueuePath();
    writeFileSync(
      queuePath,
      [
        JSON.stringify({
          id: "legacy-1",
          bankId: "bank-1",
          content: "legacy",
          documentId: "conversation",
          metadata: {},
          createdAt: "2026-08-01T00:00:00.000Z",
        }),
        "not-json",
      ].join("\n") + "\n",
      "utf8"
    );

    expect(() => new RetainQueue({ filePath: queuePath })).toThrow("malformed JSON");
    expect(readFileSync(queuePath, "utf8")).toContain("not-json");
  });
});
