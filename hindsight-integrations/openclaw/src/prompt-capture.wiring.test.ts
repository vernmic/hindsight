import { mkdtempSync, rmSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import { afterAll, describe, expect, it } from "vitest";

import { buildEngineViewCapture } from "./prompt-capture-engine.js";
import { retainedTurns, rotateTurns } from "./prompt-capture.js";
import { FsTurnStore } from "./prompt-capture-fs.js";

// The production wiring (index.ts captureEngineView) stores the engine lists INSIDE the turn
// record, so rotation removes them with the turn; this pins that contract end to end.
describe("engine-view capture store", () => {
  const root = mkdtempSync(join(tmpdir(), "prompt-capture-"));
  afterAll(() => rmSync(root, { recursive: true, force: true }));

  const view = (n: number) => [
    { role: "user", content: [{ type: "text", text: `question ${n}` }] },
    { role: "toolResult", toolCallId: `c${n}`, content: [{ type: "text", text: "x".repeat(400) }] },
  ];
  const marked = (n: number) => [view(n)[0], { ...view(n)[1], content: [{ type: "text", text: "[pruned]" }] }];

  async function capture(store: FsTurnStore, turn: number) {
    const c = buildEngineViewCapture({
      turnId: `after-turn-${turn}`,
      startedAt: turn * 1000,
      completedAt: turn * 1000 + 1,
      before: view(turn),
      after: marked(turn),
      marks: [],
      marksInEffect: 1,
      admittedMarkIds: [turn],
    });
    return rotateTurns(store, "agent:x:topic:1", {
      ...c.turn,
      engineLists: { before: view(turn), after: marked(turn) },
      markRemovalTotal: c.removal.total,
    });
  }

  it("keeps exactly the newest two turns, lists included", async () => {
    const store = new FsTurnStore(root);
    await capture(store, 1);
    await capture(store, 2);
    const third = await capture(store, 3);
    expect(third.removed).toEqual(["after-turn-1"]);
    const kept = await retainedTurns(store, "agent:x:topic:1");
    expect(kept.map((t) => t.turnId)).toEqual(["after-turn-3", "after-turn-2"]);
    expect(kept[0].engineLists?.before).toHaveLength(2);
    expect(kept[0].engineView!.beforeTotal).toBeGreaterThan(kept[0].engineView!.afterTotal);
    expect(kept[0].markRemovalTotal).toBe(kept[0].engineView!.beforeTotal - kept[0].engineView!.afterTotal);
    expect(await store.list("agent:x:topic:1")).toHaveLength(2);
  });
});
