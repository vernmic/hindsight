"""B1: LLM fallback classifier for records Jev cannot judge (oversized, low-confidence).

Second-judge tier per the zero-loose-ends plan v2: an LLM produces the SAME
result shape as review_page, from the SAME taxonomy, with source discipline
(text truncated, never rewritten; verdicts additive-only downstream via
additive_page). The LLM sees: unit text (truncated), existing tags, and the
closed vocabularies. It returns disposition + tags with a confidence; we map
to the same result schema and mark judge="llm-fallback".

Cost discipline: one chat call per unit, max ~4k tokens in, tiny out.
Never called for records Jev judged fine.
"""
import json
import os
import re

# The fallback tier is optional and operator configured: endpoint, model and the
# secret name holding the API key. Nothing here is a deployment identity.
FALLBACK_ENDPOINT = os.environ.get("HERMES_OPS_FALLBACK_URL") or "https://openrouter.ai/api/v1/chat/completions"
FALLBACK_MODEL = os.environ.get("HERMES_OPS_FALLBACK_MODEL") or "deepseek/deepseek-chat"
FALLBACK_KEY_NAME = os.environ.get("HERMES_OPS_FALLBACK_KEY") or "OPENROUTER_API_KEY"
TRUNCATE_CHARS = 6000  # ~3k tokens of source text; enough for signal
FALLBACK_MAX_UNITS_PER_PASS = 50

SYSTEM_PROMPT = """You are a memory-unit tagger. You classify stored agent memories for a retrieval system.

Input: one memory unit's text and its existing tags.
Output: strict JSON only, no prose.

Vocabularies (closed sets):
- disposition: KEEP (has durable value), PRUNE (agent narration, telemetry, derived summary, no independent value)
- task (multi): design, build, patch, debug, validate, review, research, ingest, configure, ops
- value (multi): fact, constraint, decision, procedure, pitfall, reference, fix
- target (single): repo, bank, plugin, script, config, model, host, network, none

Rules:
1. Judge the CONTENT, not the narration. "I fixed X" is narration; "X was broken because Y" is a fact.
2. Assistant self-narration with no durable content -> PRUNE.
3. Telemetry, status counts, ack strings -> PRUNE.
4. Tag conservatively: only tag an axis when clearly supported; empty lists are valid.
5. Return: {"disposition": "...", "confidence": 0.0-1.0, "task": [...], "value": [...], "target": "...", "reason": "<one of: useful-knowledge, useful-procedure, useful-reference, agent-narration, telemetry, derived-content, inconsequential-fragment>"}
"""

ALLOWED_TASKS = {"design", "build", "patch", "debug", "validate", "review", "research", "ingest", "configure", "ops"}
ALLOWED_VALUES = {"fact", "constraint", "decision", "procedure", "pitfall", "reference", "fix"}
ALLOWED_TARGETS = {"repo", "bank", "plugin", "script", "config", "model", "host", "network", "none"}
ALLOWED_REASONS = {"useful-knowledge", "useful-procedure", "useful-reference", "agent-narration",
                   "telemetry", "derived-content", "inconsequential-fragment"}


def build_prompt(record):
    text = (record.get("text") or "")[:TRUNCATE_CHARS]
    tags = json.dumps(record.get("tags") or [])
    return (
        f"Existing tags: {tags}\n"
        f"Memory text (truncated to {TRUNCATE_CHARS} chars):\n{text}\n\n"
        'Return the JSON object now.'
    )


def parse_verdict(raw):
    """Parse + validate LLM output. Returns dict or None on malformed."""
    if not raw:
        return None
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if not m:
        return None
    try:
        v = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    disposition = v.get("disposition")
    if disposition not in ("KEEP", "PRUNE"):
        return None
    conf = v.get("confidence")
    conf = float(conf) if isinstance(conf, (int, float)) and 0.0 <= float(conf) <= 1.0 else None
    if conf is None:
        return None
    task = sorted({t for t in v.get("task", []) if t in ALLOWED_TASKS})
    value = sorted({x for x in v.get("value", []) if x in ALLOWED_VALUES})
    target = v.get("target") if v.get("target") in ALLOWED_TARGETS else "none"
    reason = v.get("reason") if v.get("reason") in ALLOWED_REASONS else None
    if reason is None:
        reason = "useful-knowledge" if disposition == "KEEP" else "inconsequential-fragment"
    return {"disposition": disposition, "confidence": conf, "task": task, "value": value,
            "target": target, "reason": reason}


def fallback_result(record, verdict, model=FALLBACK_MODEL):
    """Map an LLM verdict into the review_page result schema."""
    if verdict["disposition"] == "PRUNE":
        # PRUNE is not this pipeline's job (gardener owns pruning); hold for that path
        return {
            "id": record["id"], "disposition": "REVIEW",
            "reason": "llm-fallback:prune-candidate", "judge": "llm-fallback", "model": model,
            "draft_tags": [], "existing_tags": record.get("tags") or [], "applied": False,
            "llm_confidence": verdict["confidence"],
        }
    tags = [f"task:{t}" for t in verdict["task"]] + [f"value:{v}" for v in verdict["value"]]
    if verdict["target"] not in ("none", "other"):
        tags.append(f"target:{verdict['target']}")
    return {
        "id": record["id"], "disposition": "KEEP", "judge_disposition": "KEEP",
        "judge": "llm-fallback", "model": model,
        "reason": verdict["reason"], "draft_tags": tags,
        "existing_tags": record.get("tags") or [], "applied": False,
        "guards": [], "llm_confidence": verdict["confidence"],
    }


async def classify_fallback(records, chat_fn, model=FALLBACK_MODEL):
    """chat_fn(prompt) -> raw string. One call per record; failures return REVIEW holds."""
    results = []
    for record in records:
        try:
            raw = await chat_fn(build_prompt(record)) if _is_coroutine(chat_fn) else chat_fn(build_prompt(record))
            verdict = parse_verdict(raw)
            if verdict is None:
                results.append({"id": record["id"], "disposition": "REVIEW",
                                "reason": "llm-fallback:malformed", "judge": "llm-fallback",
                                "draft_tags": [], "existing_tags": record.get("tags") or [], "applied": False})
                continue
            results.append(fallback_result(record, verdict, model=model))
        except Exception as exc:
            results.append({"id": record["id"], "disposition": "REVIEW",
                            "reason": f"llm-fallback:error:{type(exc).__name__}",
                            "judge": "llm-fallback", "draft_tags": [],
                            "existing_tags": record.get("tags") or [], "applied": False})
    return {"policy": "llm-fallback-v1", "mode": "read-only", "results": results}


def _is_coroutine(fn):
    import inspect
    return inspect.iscoroutinefunction(fn)
