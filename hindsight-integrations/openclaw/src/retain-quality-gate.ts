// Retain quality gate (opt-in `retainQualityGate`, default off). Carried from production
// fa8cd7a5d0 (2026-09-01) onto the stack: a lightweight regex classifier that skips retain for
// low-signal turns. Runs in under a millisecond, no LLM call.

/** Activity type and generic topics for a retain transcript. */
export function classifyTurn(transcript: string): { activity: string; topics: string[] } {
  const sample = transcript.substring(0, 2000).toLowerCase();
  let activity = "general";
  if (
    /\b(hello|hi|hey|thanks|thank you|good morning|good evening|good night|bye)\b/.test(sample) &&
    transcript.length < 500
  ) {
    activity = "chitchat";
  } else if (/\b(remember|recall|forget|memory|memories|retain|purge)\b/.test(sample)) {
    activity = "memory-meta";
  } else if (
    /\b(edit|update|change|add|set|configure|patch|tweak)\b[\s\S]{0,40}\b(file|config|setting|variable)\b/.test(
      sample
    )
  ) {
    activity = "config-edit";
  } else if (/\b(fix|bug|error|issue|broken|crash|fail)\b/.test(sample)) {
    activity = "debugging";
  } else if (/\b(build|deploy|test|run|install|compile)\b/.test(sample)) {
    activity = "development";
  } else if (/\b(analyze|research|search|find|lookup|investigate)\b/.test(sample)) {
    activity = "research";
  }
  // Generic categories only; deployment-specific vocabulary belongs in a downstream classifier.
  const topics: string[] = [];
  if (/\b(agent|subagent|session|spawn)\b/.test(sample)) topics.push("agents");
  if (/\b(skill|workflow|automation)\b/.test(sample)) topics.push("skills");
  if (/\b(model|llm|gpt|claude)\b/.test(sample)) topics.push("models");
  if (/\b(gateway|config|restart|service)\b/.test(sample)) topics.push("infrastructure");
  if (/\b(task|todo|plan|priority)\b/.test(sample)) topics.push("planning");
  if (/\b(code|file|function|class|module)\b/.test(sample)) topics.push("code");
  return { activity, topics };
}

/** Why the gate skips this transcript, or "" to retain it. */
export function qualityGateSkipReason(transcript: string): string {
  const { activity, topics } = classifyTurn(transcript);
  if (activity === "chitchat" && topics.length === 0 && transcript.length < 200) return "short chitchat";
  if (activity === "memory-meta" && topics.length === 0) return "memory-meta noise";
  if (
    /^(Assistant|User) (changed|requested|planned|explained)/i.test(transcript) &&
    !/\b(decided|insight|important)\b/i.test(transcript) &&
    transcript.length < 500
  ) {
    return "procedural noise";
  }
  if (/^(Assistant planned to|The assistant explained)/i.test(transcript) && topics.length === 0) {
    return "narrative noise";
  }
  if (/^\[role: tool\][\s\S]*\[role: assistant\][\s\S]*$/.test(transcript) && !transcript.includes("[role: user]")) {
    return "pure tool calls";
  }
  return "";
}
