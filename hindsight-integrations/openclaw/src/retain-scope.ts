/**
 * Retain queue scope. pendingRetain is drained only by a later turn of the same session, so a
 * one-shot session (cron run, subagent, heartbeat, dashboard) would hold its range forever.
 * Queue only conversational sessions. Matches both key forms (colon and sanitized underscore).
 */
const NON_CONVERSATIONAL = /(^|[:_.-])(cron|subagent|dashboard|heartbeat)([:_.-]|$)/i;

export function shouldQueueRetain(sessionKey: string | undefined | null): boolean {
  if (typeof sessionKey !== "string" || !sessionKey.trim()) return false;
  return !NON_CONVERSATIONAL.test(sessionKey.trim());
}
