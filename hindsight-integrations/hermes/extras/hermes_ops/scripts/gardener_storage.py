"""One independent keeper decision for previously retained observations."""

import asyncio
import json

import gardener_batch_v7 as worker


async def review_page(records, decide, model, journal, *, max_bytes=100000, concurrency=2):
    if (
        not 1 <= len(records) <= 100
        or len({r["id"] for r in records}) != len(records)
        or any(r["bank_id"] != "hermes-ops" for r in records)
    ):
        raise ValueError("Storage page scope refused")
    if concurrency not in (1, 2):
        raise ValueError("Storage concurrency refused")
    eligible = [r for r in records if r.get("canonical_peers")]
    batches, oversized = worker.split_requests(eligible, "storage", model, max_bytes)
    errors = {key: "oversized-record" for key in oversized}
    answers = {}
    calls = []
    semaphore = asyncio.Semaphore(concurrency)

    async def invoke(envelope):
        ids = list(envelope["state"]["memories"])
        cached = journal.lookup(envelope)
        try:
            if cached is None:
                async with semaphore:
                    result = await asyncio.to_thread(decide, envelope["state"], envelope["questions"])
            else:
                result = cached
            worker.validate_answers(envelope["questions"], result["answers"])
            if cached is None:
                journal.store(envelope, result)
            answers.update(result["answers"])
            calls.append({"ids": ids, "cached": cached is not None, "usage": result.get("usage")})
        except Exception as exc:
            code = getattr(exc, "disposition", type(exc).__name__)
            if code == "judge_unavailable_transport" and "HTTP 400" in str(exc) and len(ids) > 1:
                calls.append({"ids": ids, "retry": "split-batch", "detail": str(exc)})
                middle = len(ids) // 2
                for subset in (ids[:middle], ids[middle:]):
                    rows = [row for row in records if row["id"] in subset]
                    await invoke(worker.request(rows, "storage", model))
                return
            errors.update({key: code for key in ids})
            calls.append({"ids": ids, "error": code, "detail": str(exc)})

    await asyncio.gather(*(invoke(batch) for batch in batches))
    results = []
    for row in records:
        key = row["id"]
        result = {
            "id": key,
            "snapshot_version": row["snapshot_version"],
            "disposition": "KEEP",
            "draft_tags": [],
            "reason": "canonical-peer-adds-or-lacks-claim",
        }
        if key in errors:
            result.update(disposition="REVIEW", reason=errors[key])
        else:
            keeper = answers.get(key + "/keeper", {})
            selected = keeper.get("choice")
            probability = keeper.get("probabilities", {}).get(selected, 0)
            peer = next((p for p in row.get("canonical_peers", []) if p["id"] == selected), None)
            if peer and probability >= 0.80:
                disposition, guards = worker.guard_disposition(row, "DUPLICATE", "self-contained")
                result.update(
                    disposition=disposition,
                    guards=guards,
                    keeper_id=selected,
                    keeper_text=peer["text"],
                    keeper_probability=probability,
                    reason="confirmed-duplicate",
                )
        results.append(result)
    report = {
        "policy": "gardener-storage-v9",
        "records": len(records),
        "results": results,
        "requests": calls,
        "unavailable": errors,
        "complete": not errors,
    }
    journal.finish_page(records, report)
    return report
