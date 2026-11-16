"""Durable per-attempt ledger for the tagger (plan 0.2).

Append-only JSONL. One row per attempt per unit per lifecycle moment:
'start' before the judge call, 'outcome' on completion, 'fail' on error.
Exceptions in the drain can no longer erase evidence — the start row is
already on disk. Cost is never fabricated: unknown stays null.

Batch judge calls share one request_id so downstream cost accounting
counts the call once, not once per unit."""
import json
import os
import threading
from pathlib import Path

_LOCK = threading.Lock()

# Reservation spent when a judged call reports no usage. An operator may pin a
# different drain budget through the environment; the queue meta row keeps the
# cumulative spend so the counter survives restarts.
DEFAULT_BUDGET_USD = float(os.environ.get("HERMES_OPS_DRAIN_BUDGET_USD") or 5.0)


def ledger_path(queue_db_path):
    return Path(str(queue_db_path)).parent / "ledger.jsonl"


def append(path, record):
    """Append one record; crash-safe per-line write. Caller fills ts if absent."""
    record = dict(record)
    record.setdefault("ts", _utcnow())
    line = json.dumps(record, ensure_ascii=True)
    with _LOCK:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())


def attempt_start(path, *, run_id, request_id, stage, queue_key, unit_id, unit_version,
                  judge, model, policy_version):
    append(path, {
        "event": "start", "run_id": run_id, "request_id": request_id, "stage": stage,
        "queue_key": queue_key, "unit_id": unit_id, "unit_version": unit_version,
        "judge": judge, "model": model, "policy_version": policy_version,
    })


def attempt_outcome(path, *, run_id, request_id, stage, queue_key, unit_id, unit_version,
                    judge, model, outcome, disposition=None, reason=None,
                    tags_applied=None, writer_result=None, cost_usd=None, latency_ms=None):
    append(path, {
        "event": "outcome", "run_id": run_id, "request_id": request_id, "stage": stage,
        "queue_key": queue_key, "unit_id": unit_id, "unit_version": unit_version,
        "judge": judge, "model": model, "outcome": outcome,
        "disposition": disposition, "reason": reason,
        "tags_applied": tags_applied, "writer_result": writer_result,
        "cost_usd": cost_usd, "latency_ms": latency_ms,
    })


def attempt_fail(path, *, run_id, request_id, stage, queue_key, unit_id, unit_version,
                 judge, model, error):
    append(path, {
        "event": "fail", "run_id": run_id, "request_id": request_id, "stage": stage,
        "queue_key": queue_key, "unit_id": unit_id, "unit_version": unit_version,
        "judge": judge, "model": model, "error": str(error)[:500],
    })


class Budget:
    """Cumulative spend reservation. Persists spent/budget in the queue meta table
    so the counter survives process restarts. Refusal is an outcome, never silent."""

    RESERVATION_PER_UNIT = 0.002  # used when a call reports no usage

    def __init__(self, queue, budget_usd):
        self.queue = queue
        self.budget_usd = float(budget_usd)
        queue.meta_set("drain_budget_usd", str(self.budget_usd))

    def spent(self):
        v = self.queue.meta_get("drain_spent_usd")
        return float(v) if v else 0.0

    def reserve(self, n_units, reported_cost=None):
        """Reserve before a call. Returns (ok, amount)."""
        amount = reported_cost if reported_cost is not None else n_units * self.RESERVATION_PER_UNIT
        if self.spent() + amount > self.budget_usd:
            return False, amount
        self.queue.meta_set("drain_spent_usd", str(self.spent() + amount))
        return True, amount

    def settle(self, reserved_amount, actual_cost):
        """After the call: correct the reservation toward actual usage."""
        delta = (actual_cost if actual_cost is not None else 0.0) - reserved_amount
        self.queue.meta_set("drain_spent_usd", str(max(0.0, self.spent() + delta)))


def _utcnow():
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
