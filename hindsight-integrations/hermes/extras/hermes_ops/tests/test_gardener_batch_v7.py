"""Meaningful batch coverage, size, fail-closed, resume and protection checks."""

import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path

import gardener_batch_v7 as worker


def records(count=100):
    return [
        {
            "id": "memory-%03d" % i,
            "bank_id": "hermes-ops",
            "text": "A useful decision and its reason.",
            "fact_type": "experience",
            "tags": [],
            "snapshot_version": "v1",
            "source_chunks": [{"chunk_id": "source-%03d" % i, "text": "source evidence"}],
        }
        for i in range(count)
    ]


def answers(questions):
    result = {}
    for key, q in questions.items():
        if q["type"] == "noul":
            result[key] = {"type": "noul", "noul": 0.9}
        else:
            options = list(q["criteria"])
            selected = "KEEP" if "KEEP" in options else options[0]
            result[key] = {
                "type": "choice",
                "choice": selected,
                "probabilities": {option: float(option == selected) for option in options},
            }
    return {"answers": result, "resolved_model": "test"}


class BatchTests(unittest.TestCase):
    def test_prune_skips_task_and_value_tag_calls(self):
        data = records(1)
        seen = []

        def decide(state, questions):
            seen.extend(questions)
            result = answers(questions)
            for key, q in questions.items():
                selected = (
                    "PRUNE"
                    if key.endswith("/disposition")
                    else "no-durable-value"
                    if key.endswith("/context")
                    else None
                )
                if selected:
                    result["answers"][key] = {
                        "type": "choice",
                        "choice": selected,
                        "probabilities": {option: float(option == selected) for option in q["criteria"]},
                    }
            return result

        with tempfile.TemporaryDirectory() as directory:
            journal = worker.Journal(Path(directory) / "j.db")
            try:
                report = asyncio.run(worker.review_page(data, decide, "test", journal))
            finally:
                journal.db.close()
        self.assertEqual(report["results"][0]["disposition"], "PRUNE")
        self.assertFalse(any("/task/" in key or "/value/" in key for key in seen))

    def test_duplicate_keeper_must_stand_alone(self):
        data = records(1)
        data[0]["canonical_peers"] = [{"id": "00000000-0000-0000-0000-000000001234", "text": "named subject"}]
        question = worker.request(data, "review", "test")["questions"][data[0]["id"] + "/keeper"]
        self.assertIn("text itself", question["instructions"])
        self.assertIn("self-contained", question["instructions"])

    def test_useful_observation_becomes_duplicate_when_complete_keeper_verified(self):
        row = records(1)[0]
        row["fact_type"] = "observation"
        peer = "00000000-0000-0000-0000-000000001234"
        row["canonical_peers"] = [{"id": peer, "text": row["text"]}]
        seen = []

        def decide(state, questions):
            seen.extend(questions)
            result = answers(questions)
            for key, q in questions.items():
                if key.endswith("/keeper"):
                    result["answers"][key] = {
                        "type": "choice",
                        "choice": peer,
                        "probabilities": {option: float(option == peer) for option in q["criteria"]},
                    }
            return result

        with tempfile.TemporaryDirectory() as d:
            journal = worker.Journal(Path(d) / "j.db")
            try:
                report = asyncio.run(worker.review_page([row], decide, "test", journal))
            finally:
                journal.db.close()
        self.assertEqual(report["results"][0]["disposition"], "DUPLICATE")
        self.assertEqual(report["results"][0]["keeper_id"], peer)
        self.assertFalse(any("/task/" in key or "/value/" in key for key in seen))

    def test_source_documents_are_shared_and_linked_without_truncation(self):
        data = records(2)
        document = {"document_id": "original-page", "text": "Full original task context. " * 1000}
        for row in data:
            row["source_documents"] = [document]
        envelope = worker.request(data, "review", "test")
        self.assertEqual(envelope["state"]["documents"], {"original-page": document})
        for row in envelope["state"]["memories"].values():
            self.assertEqual(row["source_document_ids"], ["original-page"])
            self.assertNotIn("source_documents", row)

    def test_conflicting_original_documents_are_refused(self):
        data = records(2)
        data[0]["source_documents"] = [{"document_id": "same", "text": "first"}]
        data[1]["source_documents"] = [{"document_id": "same", "text": "different"}]
        with self.assertRaises(ValueError):
            worker.request(data, "review", "test")

    def test_context_guard_prevents_unqualified_keep_or_missing_context_prune(self):
        row = records(1)[0]
        self.assertEqual(worker.guard_disposition(row, "KEEP", "needs-source-repair")[0], "REPAIR")
        self.assertEqual(worker.guard_disposition(row, "PRUNE", "needs-source-repair")[0], "REVIEW")
        self.assertEqual(worker.guard_disposition(row, "KEEP", "no-durable-value")[0], "REVIEW")
        self.assertEqual(worker.guard_disposition(row, "PRUNE", "missing-context")[0], "REVIEW")
        self.assertEqual(worker.guard_disposition(row, "PRUNE", "no-durable-value")[0], "PRUNE")

    def test_restored_register_has_existing_targets_and_subject_domains(self):
        self.assertTrue({"repo:hermes", "tool:jev", "tool:terminal", "none"} <= set(worker.TARGETS))
        self.assertTrue({"domain:security", "domain:models", "domain:hardware"} <= set(worker.DOMAINS))

    def test_none_target_does_not_become_a_tag(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = worker.Journal(Path(directory) / "journal.db")

            def decide(state, questions):
                result = answers(questions)
                for key, question in questions.items():
                    if key.endswith("/target"):
                        result["answers"][key] = {
                            "type": "choice",
                            "choice": "none",
                            "probabilities": {option: float(option == "none") for option in question["criteria"]},
                        }
                return result

            try:
                report = asyncio.run(worker.review_page(records(1), decide, "test", journal))
                self.assertTrue(report["complete"])
                self.assertFalse(any(tag.startswith("target:") for tag in report["results"][0]["draft_tags"]))
            finally:
                journal.db.close()

    def test_approved_axis_floors_and_top_three(self):
        scores = {"first": 0.95, "second": 0.8, "third": 0.7, "fourth": 0.65, "fifth": 0.49}
        self.assertEqual(worker.select_axis(scores, 0.7), ["first", "second", "third"])
        self.assertEqual(worker.select_axis({"a": 0.6, "b": 0.55, "c": 0.5, "d": 0.49}, 0.5), ["a", "b", "c"])

    def test_other_bank_input_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = worker.Journal(Path(directory) / "journal.db")
            data = records(1)
            data[0]["bank_id"] = "forbidden"
            try:
                with self.assertRaises(ValueError):
                    asyncio.run(worker.review_page(data, lambda state, questions: answers(questions), "test", journal))
                self.assertEqual(journal.db.execute("SELECT count(*) FROM requests").fetchone()[0], 0)
            finally:
                journal.db.close()

    def test_100_record_envelopes_preserve_every_id_and_full_text(self):
        data = records()
        data[0]["text"] = "Unicode \u263a " * 200
        batches, oversized = worker.split_requests(data, "review", "test", 20000)
        self.assertFalse(oversized)
        seen = [key for batch in batches for key in batch["state"]["memories"]]
        self.assertEqual(seen, [row["id"] for row in data])
        for batch in batches:
            self.assertLessEqual(len(json.dumps(batch, ensure_ascii=True).encode()), 20000)
        actual = next(
            batch["state"]["memories"][data[0]["id"]]["text"]
            for batch in batches
            if data[0]["id"] in batch["state"]["memories"]
        )
        self.assertEqual(actual, data[0]["text"])

    def test_oversized_record_is_explicit_not_truncated(self):
        data = records(2)
        data[0]["text"] = "x" * 120000
        batches, oversized = worker.split_requests(data, "review", "test", 100000)
        self.assertEqual(oversized, [data[0]["id"]])
        self.assertEqual(list(batches[0]["state"]["memories"]), [data[1]["id"]])

    def test_shared_sources_appear_once(self):
        data = records(2)
        data[1]["source_chunks"] = data[0]["source_chunks"]
        self.assertEqual(len(worker.request(data, "review", "test")["state"]["sources"]), 1)

    def test_answer_coverage_and_probability_validation(self):
        questions = worker.request(records(1), "review", "test")["questions"]
        valid = answers(questions)["answers"]
        worker.validate_answers(questions, valid)
        for invalid in (
            {},
            {**valid, "unexpected": {}},
            {
                **valid,
                next(iter(questions)): {"type": "choice", "choice": "PRUNE", "probabilities": {"PRUNE": float("nan")}},
            },
        ):
            with self.assertRaises(ValueError):
                worker.validate_answers(questions, invalid)

    def test_guards_cannot_be_overruled_by_prune(self):
        for patch in (
            {"tags": ["kind:decision"]},
            {"tags": ["retain:permanent"]},
            {"fact_type": "world"},
            {"causal_linked": True},
            {"referenced_by_observation": True},
        ):
            disposition, guards = worker.guard_disposition({**records(1)[0], **patch}, "PRUNE")
            self.assertEqual(disposition, "REVIEW")
            self.assertTrue(guards)

    def test_duplicate_requires_peer_evidence(self):
        self.assertEqual(worker.guard_disposition(records(1)[0], "DUPLICATE")[0], "REVIEW")

    def test_complete_page_and_resume_without_new_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = worker.Journal(Path(directory) / "journal.db")
            calls = []

            def decide(state, questions):
                calls.append(questions)
                return answers(questions)

            try:
                first = asyncio.run(worker.review_page(records(), decide, "test", journal, max_bytes=30000))
                self.assertTrue(first["complete"])
                self.assertEqual(len(first["results"]), 100)
                self.assertEqual(first["bank_writes"], 0)
                before = len(calls)
                second = asyncio.run(worker.review_page(records(), decide, "test", journal, max_bytes=30000))
                self.assertEqual(len(calls), before)
                self.assertTrue(all(call["cached"] for call in second["requests"]))
                self.assertEqual(journal.db.execute("SELECT count(*) FROM pages").fetchone()[0], 1)
                for row in second["results"]:
                    self.assertFalse(row["applied"])
                    self.assertLessEqual(sum(tag.startswith("task:") for tag in row["draft_tags"]), 3)
                    self.assertLessEqual(sum(tag.startswith("value:") for tag in row["draft_tags"]), 3)
                    self.assertTrue(any(tag.startswith("target:") for tag in row["draft_tags"]))
            finally:
                journal.db.close()

    def test_unavailable_judge_keeps_all_pending(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = worker.Journal(Path(directory) / "journal.db")

            def decide(state, questions):
                raise TimeoutError()

            try:
                report = asyncio.run(worker.review_page(records(), decide, "test", journal))
                self.assertFalse(report["complete"])
                self.assertEqual(len(report["unavailable"]), 100)
                self.assertTrue(
                    all(row["disposition"] == "REVIEW" and not row["draft_tags"] for row in report["results"])
                )
                self.assertEqual(journal.db.execute("SELECT count(*) FROM requests").fetchone()[0], 0)
            finally:
                journal.db.close()

    def test_value_stage_uses_completed_task_scores(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = worker.Journal(Path(directory) / "journal.db")
            stages = []

            def decide(state, questions):
                stage = next(iter(questions)).split("/")[1]
                stages.append(stage)
                if stage == "value":
                    self.assertEqual(
                        set(next(iter(state["memories"].values()))["task_probabilities"]), set(worker.TASKS)
                    )
                return answers(questions)

            try:
                asyncio.run(worker.review_page(records(1), decide, "test", journal))
                self.assertEqual(stages, ["context", "task", "value"])
            finally:
                journal.db.close()

    def test_parallel_work_has_a_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = worker.Journal(Path(directory) / "journal.db")
            import threading

            lock = threading.Lock()
            active = peak = 0

            def decide(state, questions):
                nonlocal active, peak
                with lock:
                    active += 1
                    peak = max(peak, active)
                time.sleep(0.005)
                with lock:
                    active -= 1
                return answers(questions)

            try:
                report = asyncio.run(
                    worker.review_page(records(10), decide, "test", journal, max_bytes=12000, concurrency=2)
                )
                self.assertTrue(report["complete"])
                self.assertEqual(peak, 2)
            finally:
                journal.db.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
