import { join } from "node:path";

export type SweepArgs = {
  agents: string;
  state: string;
  apply: boolean;
  allowStaleLock: boolean;
};

export type Parsed =
  | { ok: true; args: SweepArgs }
  | { ok: false; error: string };

/** Both directories are required. There are deliberately no machine-path defaults. */
export function parseSweepArgs(argv: readonly string[]): Parsed {
  const get = (flag: string): string | undefined => {
    const i = argv.indexOf(flag);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  const agents = get("--agents");
  const state = get("--state");
  if (!agents || !state) {
    return {
      ok: false,
      error:
        "usage: adopt-legacy-state.mjs --agents <agents-dir> --state <astinus-state-dir> [--apply] [--allow-stale-lock]",
    };
  }
  return {
    ok: true,
    args: {
      agents,
      state,
      apply: argv.includes("--apply"),
      allowStaleLock: argv.includes("--allow-stale-lock"),
    },
  };
}

/** <root>/tmp/openclaw/gateway.state.lock, where root is the parent of the agents directory. */
export function gatewayLockPath(agentsDir: string): string {
  return join(agentsDir, "..", "tmp", "openclaw", "gateway.state.lock");
}

/**
 * Refuse --apply while a gateway may be running. A sweep under a live gateway reads and rewrites
 * documents mid-flight, which is how a ghost generation was produced once already.
 */
export function preflightApply(args: SweepArgs, lockExists: boolean): { ok: boolean; error?: string } {
  if (!args.apply || args.allowStaleLock || !lockExists) return { ok: true };
  return {
    ok: false,
    error:
      `gateway lock present at ${gatewayLockPath(args.agents)}; stop the gateway first, or pass --allow-stale-lock after checking the pid is gone`,
  };
}

/** With --apply, a database that could not be opened must abort the run before any write. */
export function preflightDatabases(apply: boolean, skipped: readonly string[]): { ok: boolean; error?: string } {
  if (!apply || skipped.length === 0) return { ok: true };
  return {
    ok: false,
    error: `agent database(s) could not be opened: ${skipped.join(", ")}; refusing to write with an incomplete key authority`,
  };
}
