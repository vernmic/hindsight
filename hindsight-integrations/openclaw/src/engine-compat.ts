/**
 * PREPARATORY - not imported by index.ts; no runtime path uses this module yet.
 *
 * Extracted unchanged from the maintained plugin (e8cb94b7, src/index.ts:3212
 * ASTINUS_ENGINE_ID and :5017-5019 ownedByEngine) so the context-engine port can
 * reuse it. Host compatibility is NOT checked here: when the engine factory is
 * ported, admission is left to OpenClaw's native ContextEngineInfo.hostRequirements
 * / assertContextEngineHostSupport. No plugin-side capability list exists.
 */

/** Context-engine id. Equal to the plugin id because the host slot is used for both. */
export const ENGINE_ID = "hindsight-openclaw";

/**
 * True when the host names this engine as owner of the turn
 * (agent_end ctx.contextEngineTurn). Exact identity only; no content matching.
 * Any other attempt keeps the ordinary agent_end capture.
 */
export function isTurnOwnedByEngine(ctx: unknown, engineId: string = ENGINE_ID): boolean {
  const turn = (
    ctx as { contextEngineTurn?: { logicalTurnId?: unknown; enginePluginId?: unknown } }
  )?.contextEngineTurn;
  return turn?.enginePluginId === engineId && typeof turn.logicalTurnId === "string";
}
