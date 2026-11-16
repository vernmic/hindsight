"""Offline queue behavior: claims, ownership, terminal states, requeue and locks.

sqlite only, in a temporary directory. No Postgres, no docker, no network.
"""

import datetime
import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import hermes_ops_config as config
import post_extraction_tag_queue as queue_mod

BANK = "fixture-bank"
UNIT = "22222222-2222-2222-2222-222222222222"


def row(text="complete useful content", unit=UNIT, tags=None):
    return {
        "id": unit,
        "bank_id": BANK,
        "text": text,
        "document_id": "doc-1",
        "chunk_id": "chunk-1",
        "source_memory_ids": [],
        "tags": tags or [],
        "metadata": {},
        "snapshot_version": "7",
    }


class QueueCase(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, {"HERMES_OPS_BANK": BANK}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "queue.sqlite"
        self.queue = queue_mod.Queue(self.path)
        self.addCleanup(self.queue.close)

    def hash(self, **kwargs):
        """Return the same entry if it is already queued, else insert it."""
        return self.queue.enqueue(row(**kwargs), origin="test", cursor="new")


class SchemaTests(QueueCase):
    def test_claim_columns_and_history_exist(self):
        cols = {r["name"] for r in self.queue.db.execute("PRAGMA table_info(queue)").fetchall()}
        self.assertTrue({"claim_pid", "claim_at", "claim_run"} <= cols)
        tables = {r[0] for r in self.queue.db.execute("SELECT name FROM sqlite_master").fetchall()}
        self.assertIn("queue_history", tables)

    def test_schema_binds_configured_bank(self):
        sql = " ".join(queue_mod.schema())
        self.assertIn("CHECK(bank=%s)" % config.quote(BANK), sql)
        self.assertNotIn("hermes-ops", sql)

    def test_identity_refuses_other_bank(self):
        bad = row()
        bad["bank_id"] = "other-bank"
        with self.assertRaises(ValueError):
            queue_mod.identity(bad)


class ClaimTests(QueueCase):
    def test_acquire_claims_with_owner_metadata(self):
        self.hash()
        claimed = self.queue.acquire(run_id="run-1", pid=1234)
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0]["claim_pid"], 1234)
        self.assertEqual(claimed[0]["claim_run"], "run-1")
        self.assertEqual(claimed[0]["state"], "in_flight")

    def test_second_acquire_by_a_live_owner_gets_nothing(self):
        self.hash()
        self.queue.acquire(run_id="run-1", pid=os.getpid())
        self.assertEqual(self.queue.acquire(run_id="run-2", pid=os.getpid()), [])
        self.assertEqual(self.queue.acquire(run_id="run-3"), [])

    def test_stale_claim_by_a_dead_pid_returns_to_pending(self):
        entry = self.hash()
        self.queue.acquire(run_id="run-1", pid=424242)
        old = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=3)).isoformat()
        with self.queue.db:
            self.queue.db.execute("UPDATE queue SET claim_at=? WHERE key=?", (old, entry["key"]))
        self.queue._release_stale_claims()
        self.assertEqual(self.queue.get(entry["key"])["state"], "pending")
        history = self.queue.db.execute("SELECT from_state,to_state,reason FROM queue_history").fetchall()
        self.assertIn(("in_flight", "pending", "stale-claim-released"), [tuple(h) for h in history])

    def test_fresh_claim_is_not_released(self):
        entry = self.hash()
        self.queue.acquire(run_id="run-1", pid=424242)
        self.queue._release_stale_claims()
        self.assertEqual(self.queue.get(entry["key"])["state"], "in_flight")

    def test_mark_clears_the_claim_and_records_history(self):
        entry = self.hash()
        self.queue.acquire(run_id="run-1", pid=1)
        self.queue.mark(entry["key"], "held", reason="judge_unavailable_timeout", bump_attempts=True)
        row_after = self.queue.get(entry["key"])
        self.assertEqual(row_after["state"], "held")
        self.assertIsNone(row_after["claim_pid"])
        self.assertEqual(row_after["attempts"], 1)
        history = [tuple(h) for h in self.queue.db.execute("SELECT from_state,to_state FROM queue_history")]
        self.assertIn(("in_flight", "held"), history)


class TerminalTests(QueueCase):
    def test_terminal_entry_is_not_requeued(self):
        entry = self.hash()
        self.queue.mark(entry["key"], "done", reason="tagged")
        again = self.hash(text="different text now")
        self.assertFalse(again["enqueued"])
        self.assertTrue(again.get("skipped_terminal"))

    def test_completion_requires_a_new_identity_after_terminal(self):
        entry = self.hash()
        self.queue.mark(entry["key"], "no_op", reason="no-label-change")
        other = self.hash(unit="33333333-3333-3333-3333-333333333333")
        self.assertTrue(other["enqueued"])


class RequeueTests(QueueCase):
    def held(self, reason, attempts, age_hours):
        entry = self.hash(unit=str(uuid.uuid4()))
        self.queue.mark(entry["key"], "held", reason=reason, bump_attempts=True)
        for _ in range(max(0, attempts - 1)):
            self.queue.mark(entry["key"], "held", reason=reason, bump_attempts=True)
        stamp = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=age_hours)
        ).isoformat()
        with self.queue.db:
            self.queue.db.execute("UPDATE queue SET updated_at=? WHERE key=?", (stamp, entry["key"]))
        return entry

    def test_recent_failure_is_kept(self):
        self.held("judge_unavailable_timeout", 1, 0.1)
        receipt = queue_mod.requeue_held(self.queue)
        self.assertEqual(receipt["kept"], 1)
        self.assertEqual(receipt["requeued"], 0)

    def test_backoff_elapsed_failure_is_requeued(self):
        self.held("judge_unavailable_timeout", 1, 7)
        receipt = queue_mod.requeue_held(self.queue)
        self.assertEqual(receipt["requeued"], 1)

    def test_attempt_cap_blocks(self):
        self.held("judge_unavailable_timeout", 3, 100)
        receipt = queue_mod.requeue_held(self.queue)
        self.assertEqual(receipt["blocked"], 1)
        states = [entry["state"] for entry in self.queue.entries(state="blocked")]
        self.assertEqual(states, ["blocked"])

    def test_dry_run_path_reports_without_state_change(self):
        self.held("ValueError", 1, 100)
        before = self.queue.counts()
        receipt = queue_mod.requeue_held(self.queue, now_iso="2000-01-01T00:00:00+00:00")
        self.assertEqual(self.queue.counts(), before)
        self.assertEqual(receipt["kept"] + receipt["requeued"] + receipt["blocked"], 1)


class OutcomeTests(QueueCase):
    def test_no_label_change_is_no_op(self):
        entry = self.hash()
        receipt = queue_mod._outcomes(
            self.queue, [{"unit_id": UNIT, "key": entry["key"]}], {"held": [{"id": UNIT, "reason": "no-label-change"}]}, "run"
        )
        self.assertEqual(receipt.get("no_op"), 1)
        self.assertEqual(self.queue.get(entry["key"])["state"], "no_op")

    def test_confirmed_duplicate_is_terminal(self):
        entry = self.hash()
        receipt = queue_mod._outcomes(
            self.queue,
            [{"unit_id": UNIT, "key": entry["key"]}],
            {"held": [{"id": UNIT, "reason": "confirmed-duplicate:abc"}]},
            "run",
        )
        self.assertEqual(receipt.get("dup_resolved"), 1)
        self.assertEqual(self.queue.get(entry["key"])["state"], "dup_resolved")

    def test_plain_failure_stays_held(self):
        entry = self.hash()
        receipt = queue_mod._outcomes(
            self.queue, [{"unit_id": UNIT, "key": entry["key"]}], {"held": [{"id": UNIT, "reason": "oversized-record"}]}, "run"
        )
        self.assertEqual(receipt["held"], 1)
        self.assertEqual(self.queue.get(entry["key"])["state"], "held")


class RunnerLockTests(QueueCase):
    def test_second_runner_is_refused_while_the_first_holds(self):
        from tagger_lock import runner_lock

        with runner_lock(self.path) as first:
            self.assertTrue(first)
            with runner_lock(self.path) as second:
                self.assertFalse(second)

    def test_lock_is_reacquirable_after_release(self):
        from tagger_lock import runner_lock

        with runner_lock(self.path):
            pass
        with runner_lock(self.path) as again:
            self.assertTrue(again)


class LedgerTests(QueueCase):
    def test_budget_reserves_and_refuses(self):
        from tagger_ledger import Budget, ledger_path

        budget = Budget(self.queue, 0.003)
        ok, _amount = budget.reserve(1)
        self.assertTrue(ok)
        refused, _amount = budget.reserve(1)
        self.assertFalse(refused)
        self.assertTrue(str(ledger_path(self.path)).endswith("ledger.jsonl"))

    def test_attempt_rows_are_jsonl(self):
        from tagger_ledger import attempt_start, ledger_path

        path = ledger_path(self.path)
        attempt_start(
            path,
            run_id="run",
            request_id="req",
            stage="classify",
            queue_key="k",
            unit_id=UNIT,
            unit_version="7",
            judge="jev",
            model="m",
            policy_version="p",
        )
        record = json.loads(path.read_text().splitlines()[0])
        self.assertEqual(record["event"], "start")
        self.assertEqual(record["unit_id"], UNIT)


if __name__ == "__main__":
    unittest.main()
