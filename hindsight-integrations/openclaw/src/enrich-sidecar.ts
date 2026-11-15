// Async context enrichment ("enrich") ported from workspace/skills/astinus/enrich.py.
//
// Why this file exists: the pre-cutover plugin spawned a Python sidecar (enrich.py) after each
// main-agent turn. The rewritten plugin spawns nothing, so enrichment silently stopped on
// 2026-11-15 (zero enrich_injection tokens in every captured turn). This is the TypeScript
// replacement: same delta rule (a document already served for a live topic is not served again
// while that topic stays live), same inject-file contract (keyed file under
// workspace/inject/astinus/<safe-session>.md, flat workspace/inject/astinus-context.md only as a
// fallback, 6 h sweep), default OFF behind enrichSidecar.enabled.
//
// Everything external is injected (gather, synthesize, served store, clock, fs) so the contract is
// testable without a bank, a model or a live gateway.
import { mkdirSync, readdirSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";

export type EnrichMem = {
  document_id?: string;
  text: string;
  mentioned_at?: string;
  occurred_start?: string;
};

export type EnrichLiveTopic = { slug: string; label: string; digest?: string | null };

export type EnrichPayload = {
  root: string;
  sessionKey: string;
  topics: string[];
  lastUserMessage: string;
  summary?: string;
  activeTasks?: string[];
  liveTopics: EnrichLiveTopic[];
};

export type EnrichSynthesis = {
  text: string;
  model?: string | null;
  elapsedS?: number;
  reasoningTokens?: number | null;
  fallbackFrom?: string | null;
  fallbackReason?: string | null;
};

export type EnrichBankCap = { bank: string; cap: number };

export type EnrichSidecarConfig = {
  enabled?: boolean;
  maxAgeHours?: number;
  topicsCap?: number;
  banks?: EnrichBankCap[];
  timeoutMs?: number;
};

export type ResolvedEnrichConfig = {
  enabled: boolean;
  maxAgeHours: number;
  topicsCap: number;
  banks: EnrichBankCap[];
  timeoutMs: number;
};

export type EnrichServed = {
  load(slug: string): Set<string>;
  mark(pairs: Array<[string, string]>): void;
  prune(liveSlugs: string[]): void;
};

export type EnrichDeps = {
  config: ResolvedEnrichConfig;
  gather?: (query: string, bank: string, cap: number) => Promise<EnrichMem[]>;
  synthesize?: (system: string, user: string) => Promise<EnrichSynthesis>;
  served?: EnrichServed;
  now?: () => number;
  fsx?: {
    mkdirSync: typeof mkdirSync;
    readdirSync: typeof readdirSync;
    statSync: typeof statSync;
    unlinkSync: typeof unlinkSync;
    writeFileSync: typeof writeFileSync;
  };
};

export type EnrichOutcome = {
  status:
    | "disabled"
    | "skipped-empty"
    | "skipped-no-data"
    | "skipped-empty-synthesis"
    | "wrote"
    | "error";
  path?: string;
  gathered?: number;
  elapsedMs?: number;
  reason?: string;
};

const DEFAULT_BANKS: EnrichBankCap[] = [
  { bank: "main", cap: 3 },
  { bank: "openclaw", cap: 2 },
];

export function resolveEnrichConfig(raw: EnrichSidecarConfig | undefined): ResolvedEnrichConfig {
  const banks =
    Array.isArray(raw?.banks) && raw.banks.length > 0
      ? raw.banks
          .filter((b) => typeof b?.bank === "string" && Number.isFinite(b?.cap) && b.cap > 0)
          .map((b) => ({ bank: b.bank, cap: Math.floor(b.cap) }))
      : DEFAULT_BANKS;
  return {
    enabled: raw?.enabled === true,
    maxAgeHours: Number.isFinite(raw?.maxAgeHours as number) ? Number(raw?.maxAgeHours) : 6,
    topicsCap: Number.isFinite(raw?.topicsCap as number) ? Number(raw?.topicsCap) : 3,
    banks,
    timeoutMs: Number.isFinite(raw?.timeoutMs as number) ? Number(raw?.timeoutMs) : 60000,
  };
}

/** The keyed inject-file name. The colon is replaced because it is the NTFS alternate-data-stream
 *  separator (the Python sidecar hit OSError 22 on every real session key before the fix). */
export function sessionFileName(sessionKey: string): string {
  return sessionKey.replace(/[^a-zA-Z0-9_.-]/g, "_").slice(0, 120);
}

const SYNTH_SYSTEM = [
  "You are Astinus, a knowledge retrieval agent. Your job is to surface the 3-5 most important",
  "facts from the provided sources that are directly relevant to the current conversation. Be",
  "precise, cite your source, and write in dense sentences. No padding. No intro. Max 500 tokens",
  "total output.",
  "",
  "For each fact, include a pointer -- a file path, doc section, or query the agent can follow for",
  "deeper context. Also write a single Relevance line that connects this enrichment to what the",
  "agent is actively working on right now. Frame the relevance so it provokes the agent to follow",
  "the pointers -- not a dry summary, but a reason to dig deeper.",
  "",
  "Format your output as:",
  "Relevance: {one sentence connecting to current conversation}",
  "",
  "{3-5 facts, each citing source in brackets}",
  "",
  "---",
  "Pointers:",
  "- {path or query} - {what the agent will find there}",
].join("\n");

export async function runEnrichSidecar(
  payload: EnrichPayload,
  deps: EnrichDeps,
): Promise<EnrichOutcome> {
  const cfg = deps.config;
  if (!cfg.enabled) return { status: "disabled" };
  const now = deps.now ?? (() => Date.now());
  const fsx = deps.fsx ?? { mkdirSync, readdirSync, statSync, unlinkSync, writeFileSync };
  const started = now();

  const hasPayload = payload.topics.length > 0 || payload.lastUserMessage.trim().length > 0;
  if (!hasPayload) return { status: "skipped-empty" };

  const gather = deps.gather;
  if (!gather) return { status: "skipped-no-data", reason: "no gather dependency" };

  const rawData: string[] = [];
  let gathered = 0;
  const servedNew: Array<[string, string]> = [];

  const live = payload.liveTopics.slice(0, cfg.topicsCap);
  if (live.length > 0) {
    for (const topic of live) {
      const query =
        (topic.label + ": " + (topic.digest ?? "")).trim().slice(0, 300) || topic.slug;
      const already = deps.served?.load(topic.slug) ?? new Set<string>();
      for (const entry of cfg.banks) {
        let mems: EnrichMem[] = [];
        try {
          mems = await gather(query, entry.bank, entry.cap);
        } catch {
          mems = [];
        }
        mems.sort((a, b) => recencyKey(b).localeCompare(recencyKey(a)));
        gathered += mems.length;
        let picked = 0;
        for (const m of mems) {
          const id = String(m.document_id ?? "");
          if (id && already.has(id)) continue;
          const text = (m.text ?? "").trim();
          if (text.length > 30) {
            rawData.push("[Hindsight " + entry.bank + " / " + topic.slug + "] " + text.slice(0, 300));
            if (id) servedNew.push([topic.slug, id]);
            picked += 1;
            if (picked >= entry.cap) break;
          }
        }
      }
    }
  } else {
    const query = payload.lastUserMessage.slice(0, 300) || payload.topics.slice(0, 3).join(" ");
    for (let i = 0; i < cfg.banks.length; i++) {
      const entry = cfg.banks[i];
      let mems: EnrichMem[] = [];
      try {
        mems = await gather(query, entry.bank, entry.cap);
      } catch {
        mems = [];
      }
      gathered += mems.length;
      const fallbackCap = i === 0 ? Math.max(entry.cap, 6) : Math.max(entry.cap, 4);
      for (const m of mems.slice(0, fallbackCap)) {
        const text = (m.text ?? "").trim();
        if (text.length > 30) rawData.push("[Hindsight " + entry.bank + "] " + text.slice(0, 300));
      }
    }
  }

  if (rawData.length === 0) return { status: "skipped-no-data", gathered };

  const userPrompt = [
    "Conversation context: " + (payload.summary || payload.lastUserMessage),
    "Active topics: " + payload.topics.join(", "),
    "Active tasks: " + (payload.activeTasks ?? []).slice(0, 3).join(", "),
    "",
    "Raw data from all archives:",
    "",
    rawData.slice(0, 12).join("\n\n---\n\n"),
    "",
    "Extract the 3-5 highest-value facts for the agent next response. Format each as:",
    "**[Source]** fact. Include a Pointers section at the end with file paths or queries. Start",
    "with a Relevance: line connecting to the current conversation. Skip anything not directly",
    "relevant.",
  ].join("\n");

  let synthesis: EnrichSynthesis | null = null;
  let modelNote = "no model -- raw fallback";
  try {
    synthesis = deps.synthesize ? await deps.synthesize(SYNTH_SYSTEM, userPrompt) : null;
  } catch (e) {
    synthesis = {
      text:
        "*(raw hits -- synthesis unavailable: " +
        String((e as Error)?.message ?? e).slice(0, 160).replace(/\n/g, " ") +
        ")*\n\n" +
        rawData
          .slice(0, 4)
          .map((d) => "- " + d.slice(0, 200))
          .join("\n"),
    };
  }
  if (!synthesis) return { status: "skipped-no-data", gathered, reason: "no synthesize dependency" };
  if (!synthesis.text.trim()) return { status: "skipped-empty-synthesis", gathered };

  if (synthesis.model) {
    modelNote =
      synthesis.model +
      ", " +
      (synthesis.elapsedS ?? 0) +
      "s, " +
      (synthesis.reasoningTokens ?? 0) +
      " reasoning tokens";
    if (synthesis.fallbackFrom) {
      modelNote +=
        "; FALLBACK from " +
        synthesis.fallbackFrom +
        ": " +
        String(synthesis.fallbackReason ?? "").slice(0, 80);
    }
  }

  const ts = new Date(now()).toISOString().replace("T", " ").slice(0, 19);
  const elapsed = Math.round((now() - started) / 1000);
  const output =
    "## Astinus Context - " +
    ts +
    " (" +
    elapsed +
    "s; " +
    modelNote +
    ")\n" +
    "*Topics: " +
    payload.topics.join(", ") +
    "*\n\n" +
    synthesis.text +
    "\n";

  const injectDir = join(payload.root, "workspace", "inject");
  let written: string | null = null;
  try {
    fsx.mkdirSync(injectDir, { recursive: true });
    if (payload.sessionKey) {
      const keyedDir = join(injectDir, "astinus");
      fsx.mkdirSync(keyedDir, { recursive: true });
      const path = join(keyedDir, sessionFileName(payload.sessionKey) + ".md");
      fsx.writeFileSync(path, output, "utf-8");
      written = path;
      const cutoff = now() - cfg.maxAgeHours * 3600 * 1000;
      for (const name of fsx.readdirSync(keyedDir)) {
        if (!name.endsWith(".md")) continue;
        const p = join(keyedDir, String(name));
        try {
          if (fsx.statSync(p).mtimeMs < cutoff) fsx.unlinkSync(p);
        } catch {
          /* best effort */
        }
      }
    }
  } catch {
    written = null;
  }
  if (!written) {
    const flat = join(injectDir, "astinus-context.md");
    try {
      fsx.mkdirSync(injectDir, { recursive: true });
      fsx.writeFileSync(flat, output, "utf-8");
      written = flat;
    } catch (e) {
      return { status: "error", reason: String((e as Error)?.message ?? e), gathered };
    }
  }

  deps.served?.prune(live.map((t) => t.slug));
  // One id can be returned for two live topics in the same run; the served store keys on
  // (slug, id), so mark each pair once (a duplicate pair made the store record twice).
  const seenPairs = new Set<string>();
  const uniquePairs = servedNew.filter(([slug, id]) => {
    const key = slug + String.fromCharCode(0) + id;
    if (seenPairs.has(key)) return false;
    seenPairs.add(key);
    return true;
  });
  deps.served?.mark(uniquePairs);
  return { status: "wrote", path: written, gathered, elapsedMs: now() - started };
}

function recencyKey(m: EnrichMem): string {
  return String(m.mentioned_at ?? m.occurred_start ?? "");
}

/** Default gatherer: the Hindsight recall endpoint, one query per bank. Read-only. */
export function createHindsightGather(opts: {
  apiUrl: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): (query: string, bank: string, cap: number) => Promise<EnrichMem[]> {
  const doFetch = opts.fetchImpl ?? fetch;
  return async (query, bank, cap) => {
    const url =
      opts.apiUrl.replace(/\/+$/, "") +
      "/v1/default/banks/" +
      encodeURIComponent(bank) +
      "/memories/recall";
    const resp = await doFetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, budget: "low", max_tokens: 1024 }),
      signal: AbortSignal.timeout(opts.timeoutMs ?? 20000),
    });
    if (!resp.ok) return [];
    const data = (await resp.json()) as { memories?: EnrichMem[]; results?: EnrichMem[] };
    const list = data.memories ?? data.results ?? [];
    return Array.isArray(list) ? list.slice(0, Math.max(cap, 6)) : [];
  };
}
