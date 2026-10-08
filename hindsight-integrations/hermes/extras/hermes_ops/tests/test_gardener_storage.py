import asyncio, tempfile, unittest
from pathlib import Path
import gardener_storage as s
import gardener_batch_v7 as w


class StorageTests(unittest.TestCase):
    def run_case(self, probability=0.95, protected=False, unavailable=False):
        peer = "00000000-0000-0000-0000-000000001234"
        key = "00000000-0000-0000-0000-000000001235"
        row = {
            "id": key,
            "bank_id": "hermes-ops",
            "fact_type": "observation",
            "snapshot_version": "1",
            "text": "Vega skill covers shipment estimates.",
            "tags": ["retain:permanent"] if protected else [],
            "canonical_peers": [{"id": peer, "text": "Vega skill covers shipment estimates."}],
        }

        def decide(state, questions):
            if unavailable:
                raise ValueError("unavailable")
            self.assertTrue(all(k.endswith("/keeper") for k in questions))
            return {
                "answers": {
                    k: {
                        "type": "choice",
                        "choice": peer,
                        "probabilities": {peer: probability, "no-complete-keeper": 1 - probability},
                    }
                    for k in questions
                }
            }

        with tempfile.TemporaryDirectory() as d:
            journal = w.Journal(Path(d) / "j.db")
            try:
                return asyncio.run(s.review_page([row], decide, "test", journal))["results"][0]
            finally:
                journal.db.close()

    def test_provider_token_limit_splits_without_deadlock(self):
        peer = "00000000-0000-0000-0000-000000001234"
        rows = [
            {
                "id": "00000000-0000-0000-0000-00000000123" + str(i),
                "bank_id": "hermes-ops",
                "fact_type": "observation",
                "snapshot_version": "1",
                "text": "Vega covers shipments.",
                "tags": [],
                "canonical_peers": [{"id": peer, "text": "Vega covers shipments."}],
            }
            for i in (5, 6)
        ]

        class Unavailable(RuntimeError):
            disposition = "judge_unavailable_transport"

        def decide(state, questions):
            if len(state["memories"]) > 1:
                raise Unavailable("Judge HTTP 400")
            return {
                "answers": {
                    k: {"type": "choice", "choice": peer, "probabilities": {peer: 1.0, "no-complete-keeper": 0.0}}
                    for k in questions
                }
            }

        with tempfile.TemporaryDirectory() as d:
            journal = w.Journal(Path(d) / "j.db")
            try:
                report = asyncio.run(asyncio.wait_for(s.review_page(rows, decide, "test", journal), 3))
            finally:
                journal.db.close()
        self.assertTrue(report["complete"])
        self.assertEqual([r["disposition"] for r in report["results"]], ["DUPLICATE", "DUPLICATE"])
        self.assertTrue(any(r.get("retry") == "split-batch" for r in report["requests"]))

    def test_storage_request_contains_only_injection_texts(self):
        row = {
            "id": "one",
            "text": "Nimbus skill covers shipments.",
            "canonical_peers": [
                {
                    "id": "00000000-0000-0000-0000-000000001234",
                    "text": "Nimbus skill covers shipments.",
                    "context": "secret identity",
                }
            ],
            "source_documents": [{"document_id": "huge", "text": "x" * 200000}],
        }
        request = w.request([row], "storage", "test")
        self.assertNotIn("documents", request["state"])
        self.assertEqual(set(request["state"]["memories"]["one"]), {"text", "canonical_peers"})
        self.assertNotIn("context", request["state"]["memories"]["one"]["canonical_peers"][0])

    def test_complete_canonical_keeper(self):
        self.assertEqual(self.run_case()["disposition"], "DUPLICATE")

    def test_uncertain_keeper_retained(self):
        self.assertEqual(self.run_case(0.65)["disposition"], "KEEP")

    def test_protected_copy_retained(self):
        self.assertEqual(self.run_case(protected=True)["disposition"], "REVIEW")

    def test_unavailable_unchanged(self):
        self.assertEqual(self.run_case(unavailable=True)["disposition"], "REVIEW")


if __name__ == "__main__":
    unittest.main()
