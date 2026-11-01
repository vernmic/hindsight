// P4 slice 1 (ae3d09089, 92180499f; e8cb94b7a text): durable submission, terminal completion
// and acknowledgement in isolation. The index.ts completion application (applyCompletedRetain)
// and its session-state tests arrive with the state-writer slice.
import { randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { HindsightClient, RetainResponse } from "@vectorize-io/hindsight-client";
import { RetainQueue, type QueuedRetain } from "./retain-queue.js";
import {
  createRetainTransport,
  reconcileRetainQueue,
  type RetainTransport,
} from "./retain-delivery.js";

const dirs: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});
function harness() {
  const root = mkdtempSync(join(tmpdir(), "retain-completion-"));
  dirs.push(root);
  const filePath = join(root, "queue.jsonl");
  const queue = new RetainQueue({ filePath });
  let status = "pending";
  const applied = vi.fn();
  const issues = vi.fn();
  const transport: RetainTransport = {
    targetFingerprint: "target",
    submit: vi.fn(async (item, operationId) => {
      const persisted = new RetainQueue({ filePath })
        .peek(Number.MAX_SAFE_INTEGER)
        .find((row) => row.id === item.id)!;
      expect(persisted.deliveryState).toBe("submitting");
      expect(persisted.operationId).toBe(item.operationId);
      return {
        success: true,
        async: true,
        operation_id: operationId ?? randomUUID(),
      } as RetainResponse;
    }),
    status: vi.fn(async (_bank, id) => ({ operation_id: id, status })),
  };
  const add = (sessionKey = "session", content = "remember") =>
    queue.enqueue("bank", {
      content,
      operationId: randomUUID(),
      targetFingerprint: "target",
      completion: {
        sessionKey,
        generation: "g",
        marker: { seq: 10, hashes: [content], ranges: [{ start: 1, end: 10 }] },
      },
    });
  const pass = (overrides: Partial<Parameters<typeof reconcileRetainQueue>[0]> = {}) =>
    reconcileRetainQueue({
      queue,
      transport,
      capability: "supported",
      isCurrent: () => true,
      applyCompletion: applied,
      onIssue: issues,
      ...overrides,
    });
  return {
    root,
    filePath,
    queue,
    transport,
    add,
    pass,
    applied,
    issues,
    setStatus: (next: string) => {
      status = next;
    },
  };
}

describe("durable terminal retain reconciliation", () => {
  it("binds pending delivery to the credential scope without storing the credential", () => {
    const client = {} as HindsightClient;
    const first = createRetainTransport(
      client,
      { apiUrl: "https://isolated.test/", apiToken: "tenant-one-token" },
      "test"
    );
    const same = createRetainTransport(
      client,
      { apiUrl: "https://isolated.test", apiToken: "tenant-one-token" },
      "test"
    );
    const other = createRetainTransport(
      client,
      { apiUrl: "https://isolated.test", apiToken: "tenant-two-token" },
      "test"
    );
    expect(first.targetFingerprint).toBe(same.targetFingerprint);
    expect(first.targetFingerprint).not.toBe(other.targetFingerprint);
    expect(first.targetFingerprint).toMatch(/^[a-f0-9]{64}$/);
  });

  it("uses the configured bank endpoint/auth and preserves the native metadata conversion", async () => {
    const retain = vi.fn<HindsightClient["retain"]>(
      async () => ({ success: true, async: true }) as RetainResponse
    );
    const fetch = vi.fn(
      async () =>
        new Response(JSON.stringify({ operation_id: "id", status: "pending" }), { status: 200 })
    );
    vi.stubGlobal("fetch", fetch);
    const transport = createRetainTransport(
      { retain } as never,
      { apiUrl: "https://isolated.test/prefix", apiToken: "fixture-token" },
      "fixture-agent"
    );
    const item = {
      bankId: "bank with space",
      content: "payload",
      documentId: "document",
      metadata: {
        count: 2,
        absent: null,
        missing: undefined,
        tags: ["a", "b"],
        text: "literal",
      },
      tags: ["topic:test"],
      updateMode: "append",
    } as unknown as QueuedRetain;
    await transport.submit(item, "id");
    const options = retain.mock.calls[0][2];
    if (!options) throw new Error("Retain options were not passed");
    expect(options).toMatchObject({
      operationId: "id",
      metadata: { count: "2", tags: "a,b", text: "literal" },
      tags: ["topic:test"],
      updateMode: "append",
    });
    expect(options.metadata).not.toHaveProperty("absent");
    expect(options.signal).toBeInstanceOf(AbortSignal);
    await expect(transport.status(item.bankId, "id")).resolves.toEqual({
      operation_id: "id",
      status: "pending",
    });
    expect(fetch).toHaveBeenCalledWith(
      "https://isolated.test/prefix/v1/default/banks/bank%20with%20space/operations/id",
      expect.objectContaining({
        headers: { "User-Agent": "fixture-agent", Authorization: "Bearer fixture-token" },
        signal: expect.any(AbortSignal),
      })
    );
  });

  it("does not treat async acceptance as completion and polls after restart without resubmission", async () => {
    const h = harness();
    h.add();
    expect(await h.pass()).toBe(0);
    expect(h.applied).not.toHaveBeenCalled();
    expect(h.queue.peek()[0].deliveryState).toBe("submitted");
    const reloaded = new RetainQueue({ filePath: h.filePath });
    h.setStatus("completed");
    expect(await h.pass({ queue: reloaded })).toBe(1);
    expect(h.transport.submit).toHaveBeenCalledOnce();
    expect(h.applied).toHaveBeenCalledOnce();
    expect(reloaded.size()).toBe(0);
  });
  it("recovers a completed server operation after local application fails, without another server call", async () => {
    const h = harness();
    h.add();
    h.setStatus("completed");
    await h.pass({
      applyCompletion: () => {
        throw new Error("local write failed");
      },
    });
    expect(h.queue.peek()[0].deliveryState).toBe("completed");
    const reads = vi.mocked(h.transport.status).mock.calls.length;
    await h.pass({ queue: new RetainQueue({ filePath: h.filePath }) });
    expect(h.applied).toHaveBeenCalledOnce();
    expect(h.queue.peek()).toEqual([]);
    expect(h.transport.submit).toHaveBeenCalledOnce();
    expect(vi.mocked(h.transport.status).mock.calls.length).toBe(reads);
  });
  it.each(["failed", "cancelled", "not_found"])(
    "retains %s operations without advancing and permits external recovery",
    async (status) => {
      const h = harness();
      h.add();
      h.setStatus(status);
      await h.pass();
      await h.pass();
      expect(h.applied).not.toHaveBeenCalled();
      expect(h.queue.size()).toBe(1);
      expect(h.issues).toHaveBeenCalledOnce();
      expect(h.transport.submit).toHaveBeenCalledOnce();
      h.setStatus("completed");
      await h.pass();
      expect(h.applied).toHaveBeenCalledOnce();
      expect(h.queue.size()).toBe(0);
    }
  );
  it("replays a supported lost acknowledgement with the persisted wire identity", async () => {
    const h = harness();
    h.add();
    vi.mocked(h.transport.submit).mockRejectedValueOnce(new Error("lost ack"));
    await h.pass();
    const id = h.queue.peek()[0].operationId;
    await h.pass({ capability: "unsupported" });
    expect(h.transport.submit).toHaveBeenCalledOnce();
    h.setStatus("completed");
    await h.pass();
    expect(vi.mocked(h.transport.submit).mock.calls[1][1]).toBe(id);
    expect(h.queue.size()).toBe(0);
  });
  it("never turns an unsupported lost acknowledgement into a safe retry after upgrade", async () => {
    const h = harness();
    h.add();
    vi.mocked(h.transport.submit).mockRejectedValueOnce(new Error("lost ack"));
    await h.pass({ capability: "unsupported" });
    await h.pass();
    expect(h.transport.submit).toHaveBeenCalledOnce();
    expect(vi.mocked(h.transport.submit).mock.calls[0][1]).toBeUndefined();
    expect(h.queue.peek()[0].deliveryState).toBe("uncertain");
    expect(h.applied).not.toHaveBeenCalled();
  });
  it("holds unknown capability before first send, then submits once support is known", async () => {
    const h = harness();
    h.add();
    await h.pass({ capability: "unknown" });
    expect(h.transport.submit).not.toHaveBeenCalled();
    h.setStatus("completed");
    await h.pass();
    expect(h.queue.size()).toBe(0);
  });
  it("preserves per-session ordering while allowing a different session past pending work", async () => {
    const h = harness();
    h.add("first", "head");
    h.add("first", "tail");
    h.add("other", "independent");
    vi.mocked(h.transport.status).mockImplementation(async (_bank, id) => ({
      operation_id: id,
      status: id === h.queue.peek()[0].operationId ? "pending" : "completed",
    }));
    await h.pass();
    expect(vi.mocked(h.transport.submit).mock.calls.map(([item]) => item.content)).toEqual([
      "head",
      "independent",
    ]);
    expect(h.applied).toHaveBeenCalledOnce();
    expect(h.queue.peek().map((row) => row.content)).toEqual(["head", "tail"]);
  });
  it("does not let one blocked session consume the entire scan budget", async () => {
    const h = harness();
    for (let index = 0; index < 60; index++) h.add("blocked", `held-${index}`);
    h.add("other", "independent");
    const first = h.queue.peek()[0].operationId;
    vi.mocked(h.transport.status).mockImplementation(async (_bank, id) => ({
      operation_id: id,
      status: id === first ? "pending" : "completed",
    }));
    await h.pass();
    expect(h.applied).toHaveBeenCalledOnce();
    expect(vi.mocked(h.transport.submit).mock.calls.map(([item]) => item.content)).toEqual([
      "held-0",
      "independent",
    ]);
  });
  it("waits for every returned operation, including a failed batch child", async () => {
    const h = harness();
    h.add();
    const child = randomUUID();
    vi.mocked(h.transport.submit).mockImplementation(
      async (_item, id) =>
        ({
          success: true,
          async: true,
          operation_id: id,
          operation_ids: [id!, child],
        }) as RetainResponse
    );
    vi.mocked(h.transport.status).mockImplementation(async (_bank, id) => ({
      operation_id: id,
      status: id === child ? "failed" : "completed",
    }));
    await h.pass();
    expect(h.applied).not.toHaveBeenCalled();
    expect(h.queue.size()).toBe(1);
  });
  it.each(["different", undefined])(
    "does not replay work with an unverified destination: %s",
    async (target) => {
      const h = harness();
      h.queue.enqueue("bank", { content: "legacy", targetFingerprint: target });
      await h.pass();
      expect(h.transport.submit).not.toHaveBeenCalled();
      expect(h.applied).not.toHaveBeenCalled();
      expect(h.queue.size()).toBe(1);
    }
  );
  it("fences awaited submission when the service incarnation closes", async () => {
    const h = harness();
    h.add();
    let current = true;
    vi.mocked(h.transport.submit).mockImplementation(async (_item, id) => {
      current = false;
      return { success: true, async: true, operation_id: id } as RetainResponse;
    });
    await h.pass({ isCurrent: () => current });
    expect(h.queue.peek()[0].deliveryState).toBe("submitting");
    expect(h.applied).not.toHaveBeenCalled();
    expect(h.transport.status).not.toHaveBeenCalled();
  });
  it("yields after its pass budget and leaves later work durable", async () => {
    const h = harness();
    h.add("first");
    h.add("later");
    let now = 0;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    vi.mocked(h.transport.status).mockImplementation(async (_bank, id) => {
      now = 5001;
      return { operation_id: id, status: "pending" };
    });
    await h.pass();
    expect(h.transport.submit).toHaveBeenCalledOnce();
    expect(h.queue.size()).toBe(2);
    expect(h.applied).not.toHaveBeenCalled();
  });
  it("does not expire sent or source-bound completion debt", async () => {
    const h = harness();
    h.add();
    const old = h.queue.peek()[0];
    old.createdAt = "2000-01-01T00:00:00Z";
    writeFileSync(h.filePath, JSON.stringify(old) + "\n");
    const expiring = new RetainQueue({ filePath: h.filePath, maxAgeMs: 1 });
    expect(expiring.cleanup()).toBe(0);
    expect(expiring.size()).toBe(1);
  });
});
