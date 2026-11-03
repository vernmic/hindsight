/**
 * Retain queue scope. pendingRetain is drained only by a later turn of the same session, so a
 * one-shot session would hold its range forever. The default skips only sessions whose content
 * can never inform a later decision: heartbeats, cron ticks, dashboard/status polls, keepalives.
 * Conversations, subagent sessions (they carry findings) and tool work are retained.
 * Patterns are case-insensitive regex sources tested against the session key (colon and
 * sanitized underscore forms). A non-empty allowSessionPatterns replaces the skip test: only
 * matching keys are queued (e.g. a deployment's desk allowlist).
 */
export interface RetainScopeConfig {
  skipSessionPatterns?: string[];
  allowSessionPatterns?: string[];
}

const SEP = "(^|[:_.-])";
const END = "([:_.-]|$)";
export const DEFAULT_SKIP_SESSION_PATTERNS = [
  `${SEP}(heartbeat|cron|keepalive|ping|poll|dashboard|status)${END}`,
];

function compile(list: unknown): RegExp[] {
  if (!Array.isArray(list)) return [];
  const out: RegExp[] = [];
  for (const p of list) {
    if (typeof p !== "string" || !p) continue;
    try {
      out.push(new RegExp(p, "i"));
    } catch {
      // invalid pattern: ignored
    }
  }
  return out;
}

export function shouldQueueRetain(
  sessionKey: string | undefined | null,
  scope: RetainScopeConfig = {}
): boolean {
  if (typeof sessionKey !== "string" || !sessionKey.trim()) return false;
  const key = sessionKey.trim();
  const allow = compile(scope.allowSessionPatterns);
  // R36.2: a supplied nonempty list stays in allowlist mode even when every entry is invalid.
  if (Array.isArray(scope.allowSessionPatterns) && scope.allowSessionPatterns.length)
    return allow.some((r) => r.test(key));
  const skip = compile(scope.skipSessionPatterns ?? DEFAULT_SKIP_SESSION_PATTERNS);
  return !skip.some((r) => r.test(key));
}
