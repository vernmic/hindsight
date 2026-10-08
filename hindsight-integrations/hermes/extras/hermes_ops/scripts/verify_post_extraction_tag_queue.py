"""Acceptance verification for the post-extraction fact/version tagging queue.

Deterministic and database-free: the reader, classifier and writer are injected
fakes, so this runs without the Hindsight container. The test-card suite
(t_0dfba636) owns the named test_ functions demanded by TAG-01; this script is
the implementation card's own acceptance receipt for:

  1. code builds cleanly (import, CLI help, no forked write logic),
  2. the queue persists across process boundaries and resumes without
     duplicate or lost tagging,
  3. the separate off switch is present and wired (disabled = queue only),
  4. no code path mutates or prunes sources (static plus runtime assertions).

Run: /home/vern/.hermes/hermes-agent/venv/bin/python scripts/verify_post_extraction_tag_queue.py
"""

import asyncio
import json
import re
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))
import post_extraction_tag_queue as tagger

SOURCE = Path(tagger.__file__).read_text()
FIVE = ("kind:", "scope:", "domain:", "confidence:", "source:")
PROVENANCE = ["kind:fact", "scope:hermes-ops", "confidence:provisional", "source:session"]
CHILD = """\
import json, sys
sys.path.insert(0, sys.argv[2])
import post_extraction_tag_queue as tagger
queue = tagger.Queue(sys.argv[1])
row = {'id': '00000000-0000-0000-0000-00000000000' + sys.argv[3], 'bank_id': 'hermes-ops',
    'text': 'fact ' + sys.argv[3], 'snapshot_version': '1', 'tags': ['confidence:high'],
    'document_id': 'doc-1', 'chunk_id': 'chunk-1', 'source_memory_ids': []}
print(json.dumps(queue.enqueue(row, origin='retain')))
queue.close()
"""


def child_row(number):
    return {
        "id": str(uuid.UUID(int=number)),
        "bank_id": "hermes-ops",
        "text": "fact " + str(number),
        "snapshot_version": "1",
        "tags": ["confidence:high"],
        "document_id": "doc-1",
        "chunk_id": "chunk-1",
        "source_memory_ids": [],
    }


def unit(number, **extra):
    row = {
        "id": str(uuid.UUID(int=number)),
        "bank_id": "hermes-ops",
        "text": "useful fact " + str(number),
        "snapshot_version": "1",
        "tags": ["confidence:high"],
        "document_id": "doc-1",
        "chunk_id": "chunk-1",
        "source_memory_ids": [],
        "fact_type": "experience",
    }
    row.update(extra)
    return row


def fake_classify(verdicts, seen):
    async def classify(records, decide, model, journal, *, max_bytes=100000, concurrency=2):
        seen.extend(records)
        return {
            "policy": tagger.policy_version(),
            "results": [
                {
                    "id": row["id"],
                    "snapshot_version": row["snapshot_version"],
                    "applied": False,
                    "disposition": verdicts[row["id"]],
                    "reason": verdicts[row["id"]].lower(),
                    "draft_tags": ["domain:tools", "task:debug", "value:fix"],
                }
                for row in records
            ],
        }

    return classify


def fake_writer(store, calls):
    def apply(records, report, run_id, operator=None):
        calls.append({"run_id": run_id, "ids": [row["id"] for row in records], "report": report})
        held, actions = [], []
        for result in report["results"]:
            row = store[result["id"]]
            tags = set(row["tags"]) | set(PROVENANCE) | set(result.get("draft_tags", []))
            if tags == set(row["tags"]):
                held.append({"id": row["id"], "action": "HOLD", "reason": "no-label-change"})
                continue
            store[row["id"]] = dict(row, tags=sorted(tags))
            actions.append({"id": row["id"], "action": "TAG", "blocked": None, "applied": True})
        return {
            "run_id": run_id,
            "bank": "hermes-ops",
            "considered": len(records),
            "actions": actions,
            "held": held,
            "committed": True,
        }

    return apply


class Acceptance(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.queue = tagger.Queue(self.root / "queue.sqlite")
        self.control = self.root / "tagger-control.json"
        self.telemetry = self.root / "telemetry.jsonl"
        self.store = {}
        self.calls = []
        self.seen = []
        self.journal = tagger.worker.Journal(self.root / "judge.sqlite")

    def tearDown(self):
        self.journal.db.close()
        self.queue.close()
        self.directory.cleanup()

    def drain(self, verdicts, entries=None, store=None, calls=None, **kwargs):
        store = self.store if store is None else store
        calls = self.calls if calls is None else calls
        classify = fake_classify(verdicts, self.seen)
        apply = fake_writer(store, calls)
        return asyncio.run(
            tagger.drain(
                self.queue,
                classify=classify,
                apply=apply,
                fetch=lambda ids=None: [dict(store[key]) for key in ids],
                exists=lambda ids: list(ids),
                journal=self.journal,
                telemetry=self.telemetry,
                control=self.control,
                **kwargs,
            )
        )

    # (1) builds cleanly -------------------------------------------------------
    def test_module_imports_and_cli_help(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPTS / "post_extraction_tag_queue.py"), "--help"], capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("control", result.stdout)

    # (4) no prune / no source mutation, static ---------------------------------
    def test_static_no_prune_no_repair_no_http_delete(self):
        self.assertIsNone(re.search(r"^\s*(import|from)\s+gardener_repair", SOURCE, re.M))
        self.assertNotIn("repair.apply", SOURCE)
        self.assertNotIn("gardener-control-20261007", SOURCE)
        self.assertNotIn("memory-quality-control", SOURCE)
        self.assertNotIn("DELETE FROM memory_units", SOURCE)
        self.assertNotIn("method='DELETE'", SOURCE)
        self.assertNotIn("urllib", SOURCE)

    # (2) durability ------------------------------------------------------------
    def test_queue_persists_across_process_boundaries(self):
        for number in (1, 2, 3):
            child = subprocess.run(
                [sys.executable, "-c", CHILD, str(self.root / "queue.sqlite"), str(SCRIPTS), str(number)],
                capture_output=True,
                text=True,
            )
            self.assertEqual(child.returncode, 0, child.stderr)
        reopened = tagger.Queue(self.root / "queue.sqlite")
        try:
            pending = reopened.pending("new")
            self.assertEqual(len(pending), 3)
            self.assertFalse(reopened.enqueue(child_row(1), origin="retain")["enqueued"])
            self.assertEqual(len(reopened.pending("new")), 3)
        finally:
            reopened.close()

    def test_resume_tags_each_pending_fact_once(self):
        for number in (1, 2, 3):
            row = unit(number)
            self.queue.enqueue(row, origin="retain")
        self.store.update({str(uuid.UUID(int=number)): unit(number) for number in (1, 2, 3)})
        tagger.set_enabled(True, self.control)
        first = self.drain({str(uuid.UUID(int=n)): "KEEP" for n in (1, 2, 3)})
        self.assertEqual(first["tagged"], 3)
        self.assertEqual(self.queue.pending("new"), [])
        second = self.drain({str(uuid.UUID(int=n)): "KEEP" for n in (1, 2, 3)})
        self.assertEqual(second["pages"], 0)
        self.assertEqual(len(self.calls), 1)

    # (3) off switch ------------------------------------------------------------
    def test_off_switch_is_separate_and_wired(self):
        self.assertFalse(tagger.enabled(self.control))
        self.queue.enqueue(unit(1), origin="retain")
        self.store[str(uuid.UUID(int=1))] = unit(1)
        disabled = self.drain({str(uuid.UUID(int=1)): "KEEP"})
        self.assertEqual(disabled["skipped"], "disabled")
        self.assertEqual(self.calls, [])
        self.assertEqual(len(self.queue.pending("new")), 1)
        self.assertTrue(tagger.set_enabled(True, self.control))
        self.assertTrue(tagger.enabled(self.control))
        self.assertEqual(json.loads(self.control.read_text())["bank"], "hermes-ops")
        enabled = self.drain({str(uuid.UUID(int=1)): "KEEP"})
        self.assertEqual(enabled["tagged"], 1)

    # additive-only -------------------------------------------------------------
    def test_prune_and_rescue_never_reach_a_destructive_write(self):
        tagger.set_enabled(True, self.control)
        verdicts = {
            str(uuid.UUID(int=1)): "PRUNE",
            str(uuid.UUID(int=2)): "RESCUE",
            str(uuid.UUID(int=3)): "KEEP",
            str(uuid.UUID(int=4)): "REVIEW",
        }
        for number in (1, 2, 3, 4):
            self.queue.enqueue(unit(number), origin="retain")
            self.store[str(uuid.UUID(int=number))] = unit(number)
        receipt = self.drain(verdicts)
        self.assertEqual(sorted(self.calls[0]["ids"]), sorted([str(uuid.UUID(int=n)) for n in (2, 3)]))
        self.assertTrue(all(result["disposition"] == "KEEP" for result in self.calls[0]["report"]["results"]))
        self.assertEqual(
            sorted(result["judge_disposition"] for result in self.calls[0]["report"]["results"]), ["KEEP", "RESCUE"]
        )
        self.assertEqual(receipt["tagged"], 2)
        self.assertEqual(receipt["held"], 2)
        self.assertNotIn("prune", json.dumps(receipt["runs"]).lower())
        reopened = tagger.Queue(self.root / "queue.sqlite")
        try:
            self.assertEqual(reopened.get(tagger.identity(unit(1))["key"])["state"], "held")
        finally:
            reopened.close()

    def test_five_provenance_dimensions_preserved_and_legacy_kept(self):
        tagger.set_enabled(True, self.control)
        self.queue.enqueue(unit(1), origin="retain")
        self.store[str(uuid.UUID(int=1))] = unit(1)
        self.drain({str(uuid.UUID(int=1)): "KEEP"})
        tags = self.store[str(uuid.UUID(int=1))]["tags"]
        for dimension in FIVE:
            self.assertTrue(any(tag.startswith(dimension) for tag in tags), dimension)
        self.assertIn("confidence:high", tags)
        self.assertIn("task:debug", tags)
        self.assertIn("value:fix", tags)

    # (4) source immutability, runtime -----------------------------------------
    def test_sources_byte_identical_before_and_after(self):
        tagger.set_enabled(True, self.control)
        row = unit(1, source_memory_ids=[str(uuid.UUID(int=9))])
        self.queue.enqueue(row, origin="retain")
        self.store[row["id"]] = dict(row)
        classify = fake_classify({row["id"]: "KEEP"}, self.seen)
        apply = fake_writer(self.store, self.calls)

        def fetch(ids=None):
            return [dict(self.store[key]) for key in ids]

        receipt = asyncio.run(
            tagger.drain(
                self.queue,
                classify=classify,
                apply=apply,
                fetch=fetch,
                exists=lambda ids: list(ids),
                journal=self.journal,
                telemetry=self.telemetry,
                control=self.control,
            )
        )
        self.assertEqual(receipt["source_immutability"], "verified")
        after = self.store[row["id"]]
        for field in ("text", "document_id", "chunk_id", "source_memory_ids"):
            self.assertEqual(after[field], row[field])
        self.assertTrue(set(row["tags"]).issubset(set(after["tags"])))

    def test_source_mutation_is_detected(self):
        before = unit(1)
        mutated = dict(unit(1), text="rewritten by a rogue writer")
        violations = tagger.verify_sources_unchanged([before], [mutated])
        self.assertTrue(any(violation["reason"] == "source-changed" for violation in violations))
        self.assertTrue(
            any(
                violation["reason"] == "legacy-label-removed"
                for violation in tagger.verify_sources_unchanged([before], [dict(unit(1), tags=[])])
            )
        )

    def test_protected_and_held_cases_unmodified(self):
        tagger.set_enabled(True, self.control)
        protected = unit(1, tags=["kind:decision", "retain:permanent", "confidence:high"])
        self.queue.enqueue(protected, origin="retain")
        self.store[protected["id"]] = dict(protected)
        receipt = self.drain({protected["id"]: "REVIEW"})
        self.assertEqual(self.calls, [])
        self.assertEqual(self.store[protected["id"]]["tags"], protected["tags"])
        self.assertEqual(receipt["held"], 1)

    def test_stale_version_is_refreshed_and_blocked_versions_are_marked(self):
        tagger.set_enabled(True, self.control)
        entry_row = unit(1)
        self.queue.enqueue(entry_row, origin="retain")
        self.store[entry_row["id"]] = dict(unit(1), snapshot_version="2")
        refreshed = self.drain({str(uuid.UUID(int=1)): "KEEP"})
        self.assertEqual(refreshed["tagged"], 0)
        self.assertEqual(refreshed["refreshed"], 1)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.queue.get(tagger.identity(entry_row)["key"])["state"], "superseded")
        self.assertEqual(len(self.queue.pending("new")), 1)
        # A commit-time version conflict is recorded as reconciliation state.
        entry = self.queue.pending("new")[0]
        conflict = {
            "run_id": "test",
            "bank": "hermes-ops",
            "considered": 1,
            "actions": [{"id": entry["unit_id"], "action": "TAG", "blocked": "version-conflict", "applied": False}],
            "held": [],
            "committed": True,
        }
        outcomes = tagger._outcomes(self.queue, [entry], conflict, "test")
        self.assertEqual(outcomes["stale"], 1)
        self.assertEqual(self.queue.get(entry["key"])["state"], "stale")

    def test_backfill_and_new_use_separate_cursors(self):
        self.queue.enqueue(unit(1), origin="retain", cursor="new")
        self.queue.enqueue(unit(2), origin="backfill", cursor="backfill")
        self.assertEqual([entry["unit_id"] for entry in self.queue.pending("new")], [unit(1)["id"]])
        self.assertEqual([entry["unit_id"] for entry in self.queue.pending("backfill")], [unit(2)["id"]])

    # real-writer integration (plan is pure: no database write) -----------------
    def test_real_writer_plan_only_ever_emits_additive_actions(self):
        row = unit(1, source_documents=[{"document_id": "doc-1", "text": "source evidence"}])
        keep = {
            "id": row["id"],
            "snapshot_version": "1",
            "disposition": "KEEP",
            "reason": "useful",
            "draft_tags": ["domain:tools", "task:debug", "value:fix"],
        }
        records, filtered, held = tagger.additive_page([row], {"policy": "gardener-batch-v7", "results": [keep]})
        self.assertEqual(held, [])
        plan = tagger.writer.plan(records, filtered)
        self.assertEqual(plan[0]["action"], "TAG")
        for dimension in FIVE:
            self.assertTrue(any(tag.startswith(dimension) for tag in plan[0]["tags"]), dimension)
        # RESCUE is coerced to KEEP so the writer never removes decay/tombstone labels.
        rescued = dict(keep, disposition="RESCUE")
        records, filtered, held = tagger.additive_page([row], {"policy": "gardener-batch-v7", "results": [rescued]})
        self.assertEqual(tagger.writer.plan(records, filtered)[0]["action"], "TAG")
        self.assertEqual(filtered["results"][0]["judge_disposition"], "RESCUE")
        # A destructive verdict is withheld from the writer entirely.
        pruned = dict(keep, disposition="PRUNE")
        records, filtered, held = tagger.additive_page([row], {"policy": "gardener-batch-v7", "results": [pruned]})
        self.assertEqual(records, [])
        self.assertEqual(filtered["results"], [])
        self.assertEqual(held[0]["disposition"], "PRUNE")


def main():
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(Acceptance)
    runner = unittest.TextTestRunner(stream=sys.stderr, verbosity=2)
    result = runner.run(suite)
    receipt = {
        "tests": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
        "passed": result.wasSuccessful(),
        "acceptance": {
            "builds_cleanly": "test_module_imports_and_cli_help",
            "queue_persists_across_processes": "test_queue_persists_across_process_boundaries",
            "resume_no_duplicate_or_loss": "test_resume_tags_each_pending_fact_once",
            "off_switch_wired": "test_off_switch_is_separate_and_wired",
            "no_prune_or_source_mutation": "test_static_no_prune_no_repair_no_http_delete",
            "source_byte_identical": "test_sources_byte_identical_before_and_after",
            "separate_cursors": "test_backfill_and_new_use_separate_cursors",
        },
    }
    print(json.dumps(receipt, indent=2))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
