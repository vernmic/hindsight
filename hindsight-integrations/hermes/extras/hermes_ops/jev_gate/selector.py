"""One frozen recent-session state; batched skill/feed questions then result selection."""

import hashlib
import json
import time
from collections import Counter

from . import client, ledger
from .constants import POLICY_VERSION, QUESTIONS, config


def noul(text):
    return {"type": "noul", "instructions": text}


def choice(text, options):
    if len(options) > 255:
        raise ValueError("Choice exceeds 255 options")
    return {"type": "choice", "instructions": text, "criteria": dict.fromkeys(options)}


def recent_items(history, current_task, limit):
    from agent.conversation_compression import _is_real_user_message
    from agent.message_content import flatten_message_text
    from agent.skill_commands import extract_user_instruction_from_skill_message

    items = []
    for row in history:
        if row.get("role") not in ("user", "assistant") or row.get("tool_calls"):
            continue
        text = flatten_message_text(row.get("content", ""))
        if row["role"] == "user":
            text = extract_user_instruction_from_skill_message(text) or ""
            candidate = {**row, "content": text}
            if not _is_real_user_message(candidate):
                continue
            # Persisted gateway attribution can wrap an automatic user-role notice.
            prefix, separator, body = text.partition("] ")
            if text.startswith("[") and separator and not _is_real_user_message({**candidate, "content": body}):
                continue
        if text:
            items.append({"role": row["role"], "text": text, "message_id": row.get("_row_id")})
    if not items or items[-1]["role"] != "user" or items[-1]["text"] != current_task:
        items.append({"role": "user", "text": current_task})
    remaining = limit
    clipped = []
    for row in reversed(items[-10:]):
        if remaining <= 0:
            break
        text = row["text"][:remaining]
        clipped.append({**row, "text": text, "truncated": len(text) < len(row["text"])})
        remaining -= len(text)
    return list(reversed(clipped))


def skill_roster():
    from tools import skill_ledger
    from tools.skills_tool import skills_list

    rows = json.loads(skills_list()).get("skills", [])
    counts = Counter(row.get("skill") for row in skill_ledger.list_entries())
    from tools import skill_usage

    usage = skill_usage.load_usage()
    from tools.skill_manager_tool import _find_skill

    def features(row):
        found = _find_skill(row["name"])
        path = found["path"] / "SKILL.md" if found else None
        record = usage.get(row["name"]) or {}
        return {
            "bytes": path.stat().st_size if path and path.is_file() else None,
            "use_count": record.get("use_count", 0),
            "view_count": record.get("view_count", 0),
            "last_used_at": record.get("last_used_at"),
        }

    return [
        {
            "name": r["name"],
            "trigger": r.get("description", ""),
            "category": r.get("category", ""),
            "ledger_writes": counts[r["name"]],
            **features(r),
        }
        for r in rows
    ]


def prepare(
    *,
    session_id,
    conversation_history,
    user_message,
    turn_id="",
    parent_session_id="",
    platform="",
    turn_number=0,
    cfg=None,
    roster=None,
    **kwargs,
):
    from agent.memory_provider import is_trivial_prompt
    from agent.message_content import flatten_message_text
    from agent.skill_commands import extract_user_instruction_from_skill_message

    cfg = cfg or config()
    current = extract_user_instruction_from_skill_message(flatten_message_text(user_message)) or ""
    if cfg["mode"] == "off" or parent_session_id or platform in ("cron", "heartbeat") or is_trivial_prompt(current):
        return None
    if not turn_id:
        import uuid

        turn_id = f"{session_id}:{uuid.uuid4().hex}"
    deadline = time.monotonic() + cfg["turn_budget_seconds"]
    state = {
        "current_task": current,
        "recent_items": recent_items(conversation_history, current, cfg["max_context_chars"]),
        "skills": roster if roster is not None else skill_roster(),
        "session_id": session_id,
        "turn_ref": turn_id,
        "turn_number": turn_number,
        "registry": cfg["registry"],
        "policy_version": POLICY_VERSION,
    }
    questions = {
        f"skill_{i}": noul(QUESTIONS["skill"].format(name=skill["name"])) for i, skill in enumerate(state["skills"])
    }
    questions.update({f"seed_{i}": noul(QUESTIONS["seed"].format(index=i)) for i in range(len(state["recent_items"]))})
    questions.update(
        filter=noul(QUESTIONS["filter"]),
        depth=choice(QUESTIONS["depth"], ["3", "5", "10", "uncapped"]),
        discovery=choice(
            QUESTIONS["discovery"],
            ["recurring_multi_step", "one_off_task", "already_covered", "nothing_evident", "other"],
        ),
    )
    for axis in ("task", "target", "value"):
        options = list(cfg["registry"].get(axis, []))
        questions[axis] = choice(QUESTIONS["axis"].format(axis=axis), options + ["none"])
    try:
        result = client.decide(state, questions, cfg, timeout=deadline - time.monotonic())
    except client.Unavailable as exc:
        ledger.record(
            session_id,
            turn_id,
            "feed",
            state,
            questions,
            {"error": exc.disposition, "reason": str(exc)},
            mode=cfg["mode"],
            outcome={"baseline": True},
        )
        return None
    answers = result["answers"]
    ranked = sorted(
        ((answers[f"skill_{i}"]["noul"], skill) for i, skill in enumerate(state["skills"])),
        key=lambda row: (-row[0], row[1]["name"]),
    )
    selected = []
    chars = len("<skill-recommendations>\n\n</skill-recommendations>")
    for probability, skill in ranked:
        line = f"- {skill['name']}: {skill['trigger']}"
        if probability >= cfg["skill_threshold"] and chars + len(line) + 1 <= cfg["max_injection_chars"]:
            selected.append({"name": skill["name"], "probability": probability, "line": line})
            chars += len(line) + 1
    skill_context = (
        "<skill-recommendations>\n" + "\n".join(r["line"] for r in selected) + "\n</skill-recommendations>"
        if selected
        else ""
    )
    seeds = [(answers[f"seed_{i}"]["noul"], i) for i in range(len(state["recent_items"]))]
    best, index = max(seeds, default=(0, -1))
    feed = None
    if best >= cfg["seed_threshold"]:
        seed = state["recent_items"][index]["text"]
        query = current if seed == current else current + "\n\nRelevant recent context:\n" + seed
        groups = [
            {"tags": [answers[axis]["choice"]], "match": "any_strict"}
            for axis in ("task", "target", "value")
            if answers[axis]["choice"] != "none"
        ]
        depth = answers["depth"]["choice"]
        filtered = (
            cfg["tag_filters_enabled"]
            and cfg["registry"]["version"] != "unvalidated"
            and answers["filter"]["noul"] >= cfg["filter_threshold"]
        )
        feed = {
            "version": "struct_feed_v1",
            "query": query,
            "seed_index": index,
            "tag_groups": groups if filtered else [],
            "unfiltered_rescue": bool(filtered and groups),
            "max_results": 0 if depth == "uncapped" else int(depth),
            "budget": cfg["recall_budget"],
            "max_tokens": cfg["recall_max_tokens"],
            "timeout_seconds": cfg["recall_timeout_seconds"],
            "registry_version": cfg["registry"]["version"],
        }
        feed["feed_hash"] = hashlib.sha256(json.dumps(feed, sort_keys=True).encode()).hexdigest()
    outcome = {
        "skills": selected,
        "feed": feed,
        "discovery": answers["discovery"]["choice"],
        "skill_context": skill_context,
        "proposed_for_injection": cfg["mode"] == "enforced",
        "thresholds": {k: cfg[k] for k in ("skill_threshold", "seed_threshold", "filter_threshold")},
    }
    ledger.record(session_id, turn_id, "feed", state, questions, result, mode=cfg["mode"], outcome=outcome)
    if answers["discovery"]["choice"] in ("recurring_multi_step", "other"):
        ledger.discovery(session_id, turn_id, current, state["recent_items"], answers["discovery"])

    def select_results(batches):
        candidates = []
        seen = set()
        for batch in batches:
            if batch.get("error"):
                continue
            if batch.get("feed_hash") and feed and batch["feed_hash"] != feed["feed_hash"]:
                batch["error"] = "stale_feed"
                continue
            for row in batch["candidates"]:
                if row["id"] not in seen:
                    candidates.append(row)
                    seen.add(row["id"])
        providers = [
            {
                **{
                    key: batch[key]
                    for key in ("provider", "error", "feed_hash", "query_provenance", "query_errors")
                    if key in batch
                },
                "candidate_ids": [row["id"] for row in batch.get("candidates", [])],
            }
            for batch in batches
        ]
        candidate_state = {
            "current_task": current,
            "recent_items": state["recent_items"],
            "candidates": candidates,
            "provider_results": providers,
            "feed": feed,
        }
        result_questions = {f"memory_{i}": noul(QUESTIONS["memory"].format(index=i)) for i in range(len(candidates))}
        result_questions["coverage"] = noul(QUESTIONS["coverage"])
        if not candidates:
            ledger.record(
                session_id,
                turn_id,
                "results",
                candidate_state,
                {},
                {},
                mode=cfg["mode"],
                outcome={"selected": [], "injected_text": ""},
            )
            return ""
        try:
            judged = client.decide(candidate_state, result_questions, cfg, timeout=deadline - time.monotonic())
        except client.Unavailable as exc:
            ledger.record(
                session_id,
                turn_id,
                "results",
                candidate_state,
                result_questions,
                {"error": exc.disposition, "reason": str(exc)},
                mode=cfg["mode"],
                outcome={"selected": [], "injected_text": ""},
            )
            return ""
        ranked_memories = sorted(
            ((judged["answers"][f"memory_{i}"]["noul"], r) for i, r in enumerate(candidates)), key=lambda r: -r[0]
        )
        lines = []
        dispositions = []
        selected_text = {}
        for probability, row in ranked_memories:
            line = (
                "["
                + row["id"]
                + "|"
                + ",".join(row.get("tags") or [])
                + "|"
                + str(row.get("date") or "")
                + "] "
                + row["text"]
            )
            text_key = " ".join(row["text"].split())
            reason = "below_threshold" if probability < cfg["memory_threshold"] else "selected"
            duplicate_of = selected_text.get(text_key)
            if reason == "selected" and duplicate_of:
                reason = "duplicate_text"
            if reason == "selected" and sum(map(len, lines)) + len(line) > cfg["max_memory_chars"]:
                reason = "over_budget"
            if reason == "selected":
                lines.append(line)
                selected_text[text_key] = row["id"]
            dispositions.append(
                {
                    "id": row["id"],
                    "probability": probability,
                    "disposition": reason,
                    **({"duplicate_of": duplicate_of} if reason == "duplicate_text" else {}),
                }
            )
        text = "\n".join(lines)
        ledger.record(
            session_id,
            turn_id,
            "results",
            candidate_state,
            result_questions,
            judged,
            mode=cfg["mode"],
            outcome={
                "selected": dispositions,
                "proposed_text": text,
                "proposed_for_injection": cfg["mode"] == "enforced",
                "threshold": cfg["memory_threshold"],
            },
        )
        return text

    return {
        "apply": cfg["mode"] == "enforced",
        "feed": feed,
        "context": skill_context,
        "recall_timeout_seconds": cfg["recall_timeout_seconds"],
        "recall_deadline": deadline - cfg["judge_timeout_seconds"],
        "select_results": select_results,
    }
