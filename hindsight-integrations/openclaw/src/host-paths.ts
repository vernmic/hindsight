import { isAbsolute, resolve } from "node:path";
import type { MoltbotPluginAPI } from "./types.js";

/** The loaded host owns instance identity. Never infer it from an inherited home. */
export function resolveHostStateRoot(api: MoltbotPluginAPI): string {
  const state = api.runtime?.state;
  const root = state?.resolveStateDir?.();
  if (typeof root !== "string" || !root.trim() || !isAbsolute(root)) {
    throw new Error(
      "Hindsight requires the host runtime.state.resolveStateDir capability returning an absolute instance root"
    );
  }
  return resolve(root);
}

/** Explicit state routing for sidecars; an inherited production home is not authority. */
export function sidecarEnvironment(root: string): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { ...process.env, OPENCLAW_STATE_DIR: root };
  delete env.OPENCLAW_HOME;
  return env;
}

export function resolveRetainQueuePath(root: string, configured?: string): string {
  return resolve(root, configured || "data/hindsight-retain-queue.jsonl");
}
