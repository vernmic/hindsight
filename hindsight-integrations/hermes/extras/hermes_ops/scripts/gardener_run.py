"""Resumable 100-memory historical and incremental gardening, with a live off switch."""

import argparse
import asyncio
import datetime
import fcntl
import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

import gardener_batch_v7 as worker
import gardener_keeper as keeper
import gardener_repair as repair
import gardener_storage as storage
import gardener_writer as writer

ROOT = Path("/mnt/i/hermes")
CONTROL = ROOT / "reports/gardener-control-20261007.json"
RUN = ROOT / "reports/gardener-full-20261007"
BASE = "http://localhost:8888"
PAUSE = RUN / "consolidation-pause.json"


def enabled():
    return CONTROL.exists() and json.loads(CONTROL.read_text()).get("enabled") is True


def api(path, method="GET", body=None):
    request = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def inventory(incremental=False, retry_unavailable=False, storage_only=False):
    q = "SELECT json_build_object('id',id,'version',xmin::text) FROM memory_units WHERE bank_id='hermes-ops'"
    if not incremental:
        q += " AND fact_type IN ('experience','observation')"
    if retry_unavailable:
        q += " AND EXISTS(SELECT 1 FROM hermes_garden_reviews r WHERE r.memory_id=memory_units.id AND r.bank_id='hermes-ops' AND (r.recommendation->>'reason'='oversized-record' OR r.next_review_at IS NOT NULL))"
    if storage_only:
        q += " AND fact_type='observation' AND EXISTS(SELECT 1 FROM memory_units s WHERE s.bank_id='hermes-ops' AND s.fact_type='world' AND s.id=ANY(memory_units.source_memory_ids)) AND EXISTS(SELECT 1 FROM hermes_garden_reviews r WHERE r.memory_id=memory_units.id AND r.bank_id='hermes-ops' AND (r.recommendation->>'disposition' IN ('KEEP','RESCUE','REPAIR') OR (r.policy='gardener-storage-v9' AND (r.recommendation->>'reason' LIKE 'judge_unavailable_%' OR r.recommendation->>'reason'='ValueError'))) AND NOT(r.policy='gardener-storage-v9' AND r.recommendation->>'disposition'='KEEP' AND r.snapshot_hash=md5(to_jsonb(memory_units)::text)))"
    if incremental:
        q += " AND NOT EXISTS(SELECT 1 FROM hermes_garden_reviews r WHERE r.memory_id=memory_units.id AND r.bank_id='hermes-ops' AND r.snapshot_hash=md5(to_jsonb(memory_units)::text) AND (r.next_review_at IS NULL OR r.next_review_at>now()))"
    q += (
        " ORDER BY created_at DESC,id;"
        if incremental
        else " ORDER BY ('decay:prune-candidate'=ANY(tags)) DESC, CASE fact_type WHEN 'experience' THEN 0 ELSE 1 END,created_at,id;"
    )
    return [row["id"] for row in writer.query("BEGIN READ ONLY;" + q + "ROLLBACK;")]


def pause_consolidation():
    cfg = api("/v1/default/banks/hermes-ops/config")["config"]["enable_observations"]
    if cfg:
        PAUSE.write_text(json.dumps({"restore_enable_observations": True}))
        api("/v1/default/banks/hermes-ops/config", "PATCH", {"updates": {"enable_observations": False}})
    # Worker slots in progress must drain before pruning their source facts.
    for _ in range(30):
        rows = writer.query(
            "BEGIN READ ONLY;SELECT json_build_object('active',count(*)) FROM async_operations WHERE bank_id='hermes-ops' AND status='processing' AND operation_type='consolidation';ROLLBACK;"
        )
        if not rows[0]["active"]:
            return cfg
        time.sleep(2)
    restore_consolidation()
    raise RuntimeError("Active consolidation did not drain; no prune writes")


def restore_consolidation():
    if PAUSE.exists():
        state = json.loads(PAUSE.read_text())
        if state.get("restore_enable_observations"):
            api("/v1/default/banks/hermes-ops/config", "PATCH", {"updates": {"enable_observations": True}})
        PAUSE.unlink()


async def repair_page(records, report, decide, cfg, journal):
    # Observations are derived: rejudge their authoritative sources independently.
    source_ids = set()
    direct = set()
    for verdict in report["results"]:
        if verdict["disposition"] != "REPAIR":
            continue
        row = next(r for r in records if r["id"] == verdict["id"])
        if row["fact_type"] == "observation":
            source_ids.update(r["id"] for r in row.get("source_facts", []) if r["fact_type"] in ("world", "experience"))
        else:
            direct.add(row["id"])
    selected = sorted(source_ids | direct)
    if not selected:
        return []
    sources = []
    verdicts = []
    results = []
    for start in range(0, len(selected), 100):
        batch = worker.fetch_page(ids=selected[start : start + 100])
        sources.extend(batch)
        eligible = await worker.review_page(
            batch, decide, cfg["model"], journal, max_bytes=cfg["max_state_chars"], concurrency=2
        )
        verdicts.extend(eligible["results"])
    for verdict in verdicts:
        if verdict["disposition"] != "REPAIR":
            continue
        row = next(r for r in sources if r["id"] == verdict["id"])
        result = {"memory_id": row["id"], "eligibility": verdict}
        try:
            candidate = await asyncio.to_thread(repair.proposal, row)
            if not candidate or candidate.get("held"):
                result["held"] = "no-supported-connected-replacement"
            else:
                valid, answer = await asyncio.to_thread(repair.validate, row, candidate, decide)
                result["fidelity"] = answer
                if valid and enabled():
                    result["application"] = await asyncio.to_thread(repair.apply, row, candidate)
                else:
                    result["held"] = "fidelity-unavailable-or-below-0.80" if not valid else "gardener-disabled"
        except Exception as exc:
            result["held"] = "repair-unavailable:" + type(exc).__name__
        results.append(result)
    return results


def fetch_storage_page(ids):
    checked = [str(uuid.UUID(key)) for key in ids]
    if not 1 <= len(checked) <= 100 or len(set(checked)) != len(checked):
        raise ValueError("Storage snapshot scope refused")
    sql = (
        """BEGIN READ ONLY;SELECT json_build_object(
      'id',m.id,'bank_id',m.bank_id,'text',m.text,'context',m.context,'fact_type',m.fact_type,
      'tags',m.tags,'metadata',m.metadata,'document_id',m.document_id,'chunk_id',m.chunk_id,
      'created_at',m.created_at,'proof_count',m.proof_count,'source_memory_ids',m.source_memory_ids,
      'snapshot_version',m.xmin::text,'source_chunks','[]'::json,'source_documents','[]'::json,
      'causal_linked',EXISTS(SELECT 1 FROM memory_links l WHERE l.bank_id='hermes-ops' AND l.link_type IN ('causes','caused_by') AND (l.from_unit_id=m.id OR l.to_unit_id=m.id)),
      'referenced_by_observation',EXISTS(SELECT 1 FROM memory_units o WHERE o.bank_id='hermes-ops' AND o.source_memory_ids && ARRAY[m.id]),
      'source_facts',COALESCE((SELECT json_agg(json_build_object('id',f.id,'text',f.text,'fact_type',f.fact_type,'document_id',f.document_id,'chunk_id',f.chunk_id)) FROM memory_units f WHERE f.bank_id='hermes-ops' AND f.id=ANY(m.source_memory_ids)),'[]'::json))
      FROM memory_units m WHERE m.bank_id='hermes-ops' AND m.id IN ("""
        + ",".join(writer.quote(key) + "::uuid" for key in checked)
        + ");ROLLBACK;"
    )
    rows = writer.query(sql)
    if {r["id"] for r in rows} != set(checked):
        raise ValueError("Storage snapshot missing IDs")
    for row in rows:
        row["canonical_peers"] = [f for f in row.get("source_facts", []) if f["fact_type"] == "world"]
    return rows


def storage_resume_path(path):
    resolved = path.resolve()
    if (
        not resolved.is_relative_to(RUN.resolve())
        or not resolved.name.startswith("storage-")
        or resolved.parent != RUN.resolve()
    ):
        raise ValueError("Storage resume outside run directory")
    return resolved


async def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--max-pages", type=int, default=0)
    p.add_argument("--read-only", action="store_true")
    p.add_argument("--resume-storage", type=Path)
    group = p.add_mutually_exclusive_group()
    group.add_argument("--incremental", action="store_true")
    group.add_argument("--retry-unavailable", action="store_true")
    group.add_argument("--storage-only", action="store_true")
    args = p.parse_args()
    if args.resume_storage and not args.storage_only:
        p.error("--resume-storage requires --storage-only")
    RUN.mkdir(exist_ok=True)
    lock = (RUN / "worker.lock").open("w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("GARDENER_ALREADY_RUNNING")
        return
    restore_consolidation()
    signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(SystemExit("GARDENER_TERMINATED")))
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    mode = "storage" if args.storage_only else "retry-unavailable" if args.retry_unavailable else "incremental"
    directory = RUN / (mode + "-" + stamp) if args.incremental or args.retry_unavailable or args.storage_only else RUN
    if args.resume_storage:
        directory = storage_resume_path(args.resume_storage)
    directory.mkdir(exist_ok=True)
    inv = directory / "inventory.json"
    if not inv.exists():
        ids = inventory(args.incremental, args.retry_unavailable, args.storage_only)
        inv.write_text(
            json.dumps(
                {
                    "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "bank": "hermes-ops",
                    "ids": ids,
                    "primary_world_facts": "preserved as canonical source evidence",
                },
                indent=2,
            )
        )
    ids = json.loads(inv.read_text())["ids"]
    sys.path.insert(0, "/home/vern/.hermes/hermes-agent")
    plugin = Path("/home/vern/.hermes/plugins/jev_gate")
    spec = importlib.util.spec_from_file_location(
        "gardener_jev", plugin / "__init__.py", submodule_search_locations=[str(plugin)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["gardener_jev"] = module
    spec.loader.exec_module(module)
    from agent.secret_scope import build_profile_secret_scope, reset_secret_scope, set_secret_scope
    from gardener_jev import client, constants

    token = set_secret_scope(build_profile_secret_scope(Path("/home/vern/.hermes")), profile_home="/home/vern/.hermes")
    cfg = dict(constants.DEFAULTS)
    cfg["judge_timeout_seconds"] = 120
    cfg["max_state_chars"] = 100000
    worker.POLICY = "gardener-batch-v9-independent-keeper"
    worker.POLICY_TEXT += " Strict useful-knowledge standard: PRUNE inconsequential work/report packaging, probes, counts and unsupported universalized diagnoses after reading original context. No target keep/prune percentage. Preserve skill discovery and meaningful proposals with status. A date alone is not causal evidence."
    worker.POLICY_TEXT += " A promise to prepare review documents is work packaging, even when its surrounding project is important; do not manufacture a reusable reviewer lesson from it. Unsupported causal diagnoses of model behavior based solely on prompt wording are not reusable verified cause/fix lessons. A date is not causal evidence. Preserve meaningful proposals with actual status, and source-supported skill discovery. Every emitted/repaired memory must name its subject; sibling units will not accompany it."
    worker.POLICY_TEXT += " Injection contract: the future agent receives memory.text ONLY. Context, document IDs, tags and shared sources are evidence for review and are not guaranteed in its prompt. They cannot make anonymous text self-contained. If identity or applicability appears only in those fields, recommend source-supported REPAIR, not unchanged KEEP."
    journal = worker.Journal(directory / "judge.sqlite")
    overrides = {
        r["id"]: r
        for r in json.loads((ROOT / "reports/gardener-operator-efg-decisions-20261007.json").read_text())["decisions"]
    }
    pages = (len(ids) + 99) // 100
    processed = 0
    try:
        for index in range(pages):
            prefix = directory / ("page-%04d" % (index + 1))
            done = prefix.with_suffix(".applied.json")
            if done.exists():
                continue
            if args.max_pages and processed >= args.max_pages:
                break
            if not args.read_only and not enabled():
                print("GARDENER_DISABLED")
                break
            frozen = prefix.with_suffix(".input.json")
            if frozen.exists():
                records = json.loads(frozen.read_text())
            else:
                requested = ids[index * 100 : (index + 1) * 100]
                current = writer.query(
                    "BEGIN READ ONLY;SELECT json_build_object('id',id) FROM memory_units WHERE bank_id='hermes-ops' AND id IN ("
                    + ",".join(writer.quote(key) + "::uuid" for key in requested)
                    + ");ROLLBACK;"
                )
                requested = [r["id"] for r in current]
                if not requested:
                    done.write_text(json.dumps({"externally_absent": True}))
                    continue
                records = fetch_storage_page(requested) if args.storage_only else worker.fetch_page(ids=requested)
                frozen.write_text(json.dumps(records, ensure_ascii=True, indent=2))

            def decide(state, questions):
                return client.decide(state, questions, cfg)

            review = storage.review_page if args.storage_only else worker.review_page
            report = await review(
                records, decide, cfg["model"], journal, max_bytes=cfg["max_state_chars"], concurrency=2
            )
            if report["unavailable"]:
                report = await review(
                    records, decide, cfg["model"], journal, max_bytes=cfg["max_state_chars"], concurrency=2
                )
            report = await keeper.verify_report(
                records, report, decide, cfg["model"], journal, max_bytes=cfg["max_state_chars"], concurrency=2
            )
            # Protect a correction and a possible reusable diff lesson that
            # Codex disagreed with in the pilot. They stay in the feedback queue.
            for result in report["results"]:
                row = next(r for r in records if r["id"] == result["id"])
                if result["disposition"] == "PRUNE" and (
                    "correcting the assistant" in row["text"].lower() or "CRLF/whitespace noise" in row["text"]
                ):
                    result.update(
                        disposition="REVIEW",
                        reason="codex-usefulness-disagreement",
                        codex_feedback="Preserve meaningful corrections or source-supported newline/diff lessons pending contextual repair.",
                    )

            prefix.with_suffix(".review.json").write_text(json.dumps(report, ensure_ascii=True, indent=2))
            counts = {}
            for row in report["results"]:
                counts[row["disposition"]] = counts.get(row["disposition"], 0) + 1
            status = {
                "pages": pages,
                "page": index + 1,
                "memories": len(records),
                "dispositions": counts,
                "read_only": args.read_only,
                "complete": report["complete"],
            }
            if not args.read_only:
                if not enabled():
                    print("GARDENER_DISABLED_BEFORE_WRITE")
                    break
                previous = pause_consolidation()
                try:
                    written = prefix.with_suffix(".written.json")
                    if written.exists():
                        result = json.loads(written.read_text())
                    else:
                        result = writer.apply(
                            records,
                            report,
                            "full20261007-p%04d" % (index + 1)
                            if not (args.incremental or args.retry_unavailable or args.storage_only)
                            else ("storage" if args.storage_only else "retry" if args.retry_unavailable else "inc")
                            + stamp
                            + "-p%04d" % (index + 1),
                            overrides,
                        )
                        written.write_text(json.dumps(result, ensure_ascii=True, indent=2))
                    status["applied"] = sum(r["applied"] for r in result["actions"])
                    status["pruned"] = sum(r["applied"] and r["action"] == "PRUNE" for r in result["actions"])
                finally:
                    if previous:
                        restore_consolidation()
                result["repairs"] = await repair_page(records, report, decide, cfg, journal)
                done.write_text(json.dumps(result, ensure_ascii=True, indent=2))
                status["repaired"] = sum(r.get("application", {}).get("stage") == "applied" for r in result["repairs"])
            (directory / "status.json").write_text(json.dumps(status, indent=2))
            print(json.dumps(status), flush=True)
            processed += 1
    finally:
        try:
            restore_consolidation()
        finally:
            journal.db.close()
            reset_secret_scope(token)
            try:
                subprocess.run(
                    [sys.executable, str(ROOT / "scripts/gardener_status.py")], capture_output=True, timeout=120
                )
            finally:
                lock.close()


if __name__ == "__main__":
    asyncio.run(main())
