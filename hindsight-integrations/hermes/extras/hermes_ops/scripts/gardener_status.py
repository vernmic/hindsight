"""Refresh authoritative local progress from receipts and the live rollback journal."""

import datetime
import json
import os
from pathlib import Path

import gardener_writer as writer
import hermes_ops_config as config

ROOT = config.workspace()
REPORTS = config.reports_dir()
RUN = Path(os.environ.get("HERMES_OPS_RUN_DIR") or REPORTS / "gardener-run")


def refresh():
    inventory = json.loads((RUN / "inventory.json").read_text())
    pages = (len(inventory["ids"]) + 99) // 100
    receipts = list(RUN.glob("page-*.applied.json"))
    b = config.quote(config.bank())
    counts = writer.query(
        "BEGIN READ ONLY;SELECT json_build_object('action',action,'applied',count(*) FILTER(WHERE reverted_at IS NULL),'reverted',count(*) FILTER(WHERE reverted_at IS NOT NULL)) FROM hermes_garden_journal WHERE bank_id="
        + b
        + " GROUP BY action;SELECT json_build_object('recommendation',recommendation->>'disposition','count',count(*)) FROM hermes_garden_reviews r JOIN memory_units m ON m.id=r.memory_id AND m.bank_id=r.bank_id WHERE r.bank_id="
        + b
        + " GROUP BY recommendation->>'disposition';SELECT json_build_object('unavailable_live_records',count(*)) FROM hermes_garden_reviews r JOIN memory_units m ON m.id=r.memory_id AND m.bank_id=r.bank_id WHERE r.bank_id="
        + b
        + " AND (r.recommendation->>'reason'='oversized-record' OR r.recommendation->>'reason'='ValueError' OR r.recommendation->>'reason' LIKE 'judge_unavailable_%');ROLLBACK;"
    )
    repairs = [
        json.loads(p.read_text()) for p in (REPORTS / "gardener-repairs").glob("*/*/native-curation.json")
    ]
    state = {
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "bank": config.bank(),
        "extraction_deployed": True,
        "nightly_hardening_installed": True,
        "nightly_jev_gardening_installed": True,
        "ingestion_context_fix": "installed",
        "jev_traffic_authorized": True,
        "transactional_writer_installed": True,
        "source_repair_installed": True,
        "historical_records": len(inventory["ids"]),
        "historical_pages": pages,
        "historical_pages_finished": len(receipts),
        "historical_pass_complete": len(receipts) == pages,
        "all_historical_judgments_available": False,
        "journal_counts": counts,
        "native_repairs_applied": sum(r["bank"] == config.bank() and r["stage"] == "applied" for r in repairs),
        "page_size": 100,
        "concurrency": 2,
        "request_budget_bytes": 100000,
        "primary_world_policy": "preserve canonical world facts; repair source-supported incomplete text; incremental review includes world facts",
        "off_switch": "scripts/memory-quality-control.py",
        "progress": str((RUN / "status.json").relative_to(ROOT)) if (RUN / "status.json").is_relative_to(ROOT) else str(RUN / "status.json"),
        "pending": [
            "historical pass and unavailable retry"
            if len(receipts) < pages
            else "held contextual, source-budget and disagreement cases for operator feedback"
        ],
    }
    # Complete means every frozen page reached a receipt; unavailable records stay held.
    reports = [json.loads(p.read_text()) for p in RUN.glob("page-*.review.json")]
    state["first_pass_all_judgments_available"] = len(reports) == pages and all(r["complete"] for r in reports)
    state["unavailable_live_records"] = next(
        r["unavailable_live_records"] for r in counts if "unavailable_live_records" in r
    )
    state["all_historical_judgments_available"] = (
        state["historical_pass_complete"] and state["unavailable_live_records"] == 0
    )
    storage_dirs = sorted(RUN.glob("storage-*"))
    if storage_dirs:
        latest = storage_dirs[-1]
        storage_inventory = json.loads((latest / "inventory.json").read_text())
        storage_pages = (len(storage_inventory["ids"]) + 99) // 100
        state["storage_pass"] = {
            "directory": str(latest.relative_to(ROOT)),
            "records": len(storage_inventory["ids"]),
            "pages": storage_pages,
            "finished_pages": len(list(latest.glob("page-*.applied.json"))),
        }
        state["storage_pass"]["complete"] = state["storage_pass"]["pages"] == state["storage_pass"]["finished_pages"]
    (REPORTS / "gardener-worker-status.json").write_text(json.dumps(state, indent=2))
    return state


if __name__ == "__main__":
    print(json.dumps(refresh()))
