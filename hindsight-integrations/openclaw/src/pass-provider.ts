// Classification-pass provider calls (package 28a), from 9c76 enrich-sidecar callOnce/callModel
// WITHOUT its discovery (no openclaw2 folder guess, no docker-compose.yml or models.json key
// scraping). Providers are the resolved plugin config `passProviders` (PLAN section 2): an ordered
// list where each entry owns its endpoint, model and credential; a credential is never reused
// across entries. Keys reach the pass process only over stdin.

type Json = Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any

export type PassProvider = {
  name?: string;
  url: string; // the full chat-completions endpoint
  model: string;
  apiKey: string;
  extra?: Record<string, unknown>; // merged into the request body (e.g. reasoning_effort)
};

export type ProviderDeps = {
  fetch?: typeof fetch;
  log?: (msg: string) => void;
  timeoutMs?: number;
};

const LLM_TIMEOUT_MS = 60_000;

export type LlmResult = {
  ok: boolean;
  text: string;
  model: string | null;
  elapsedMs: number;
  reasoningTokens: number | null;
  finishReason: string | null;
  fallbackFrom: string | null;
  fallbackReason: string | null;
};

/** Providers in configured order (enrich-sidecar callOnce/callModel, minus discovery). The cap is
 *  sized to the prompt (F1). `effort` overrides reasoning_effort only where a provider sets it. */
export async function callLlm(
  providers: PassProvider[],
  system: string,
  user: string,
  deps: ProviderDeps,
  effort?: string
): Promise<LlmResult> {
  const doFetch = deps.fetch ?? fetch;
  const log = deps.log ?? (() => {});
  let lastErr = "";
  for (let i = 0; i < providers.length; i++) {
    const p = providers[i];
    const name = p.name || p.model;
    const extra = { ...(p.extra ?? {}) };
    if (effort && "reasoning_effort" in extra) extra.reasoning_effort = effort;
    const body = {
      model: p.model,
      messages: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
      max_tokens: Math.max(
        16000,
        Math.min(32000, Math.floor((system.length + user.length) / 3) + 8000)
      ),
      ...extra,
    };
    const t0 = Date.now();
    try {
      const resp = await doFetch(p.url, {
        method: "POST",
        headers: { Authorization: `Bearer ${p.apiKey}`, "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(deps.timeoutMs ?? LLM_TIMEOUT_MS),
      });
      if (!resp.ok) throw new Error(`HTTP Error ${resp.status}: ${resp.statusText}`);
      const data = JSON.parse(await resp.text()) as Json;
      const choice = (data.choices ?? [{}])[0] ?? {};
      const usage = data.usage ?? {};
      const elapsedMs = Date.now() - t0;
      log(
        `${name} call: finish_reason=${choice.finish_reason} completion_tokens=${usage.completion_tokens} elapsed_ms=${elapsedMs}`
      );
      return {
        ok: true,
        text: typeof choice.message?.content === "string" ? choice.message.content : "",
        model: p.model,
        elapsedMs,
        reasoningTokens: usage.completion_tokens_details?.reasoning_tokens ?? null,
        finishReason: choice.finish_reason ?? null,
        fallbackFrom: i > 0 ? providers[0].name || providers[0].model : null,
        fallbackReason: i > 0 ? "primary-failed: " + lastErr.slice(0, 200) : null,
      };
    } catch (e) {
      lastErr = `${name}:${(e as Error)?.message ?? e}`;
      log(`${name} call failed: ${(e as Error)?.message ?? e}`);
    }
  }
  return {
    ok: false,
    text: "",
    model: null,
    elapsedMs: 0,
    reasoningTokens: null,
    finishReason: null,
    fallbackFrom: null,
    fallbackReason: lastErr || "no pass provider configured",
  };
}
