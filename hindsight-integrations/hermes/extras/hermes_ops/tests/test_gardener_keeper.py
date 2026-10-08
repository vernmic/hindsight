"""Assert the pair reviewer cannot use outside context and fails closed."""

import asyncio, tempfile, unittest
from pathlib import Path
import gardener_keeper as k
import gardener_batch_v7 as w


class KeeperTests(unittest.TestCase):
    def row(self):
        return {
            "id": "one",
            "text": "Nimbus Analysis skill uses three quote comparisons.",
            "keeper_id": "two",
            "keeper_text": "Use three quote comparisons.",
        }

    def answers(self, questions, complete=True):
        return {
            "answers": {
                key: {
                    "type": "choice",
                    "choice": list(q["criteria"])[0 if complete else 1],
                    "probabilities": {o: float(i == (0 if complete else 1)) for i, o in enumerate(q["criteria"])},
                }
                for key, q in questions.items()
            }
        }

    def test_only_injected_texts_are_visible(self):
        r = k.envelope([self.row()], "test")
        self.assertEqual(set(r["state"]["pairs"]["one"]), {"original_text", "candidate_text"})
        self.assertNotIn("sources", r["state"])
        self.assertIn("Reject a keeper missing an identity", r["state"]["instructions"])

    def test_failed_preservation_refuses_proof(self):
        r = self.row()
        a = self.answers(k.envelope([r], "test")["questions"], False)["answers"]
        self.assertIsNone(k.proof(r, a))

    def test_low_probability_refuses_proof(self):
        r = self.row()
        qs = k.envelope([r], "test")["questions"]
        a = self.answers(qs)["answers"]
        a["one/standalone-keeper"]["probabilities"]["complete-standalone"] = 0.7
        self.assertIsNone(k.proof(r, a))

    def test_proof_binds_exact_texts(self):
        r = self.row()
        a = self.answers(k.envelope([r], "test")["questions"])["answers"]
        p = k.proof(r, a)
        recommendation = {**r, "keeper_verification": p}
        self.assertTrue(k.valid_proof(r, recommendation))
        self.assertFalse(k.valid_proof({**r, "text": "later edit"}, recommendation))
        self.assertFalse(k.valid_proof(r, {**recommendation, "keeper_text": "later edit"}))

    def test_unavailable_duplicate_becomes_review(self):
        r = self.row()
        report = {
            "results": [{"id": "one", "disposition": "DUPLICATE", "keeper_id": "two", "keeper_text": r["keeper_text"]}]
        }

        def decide(*a):
            raise TimeoutError()

        with tempfile.TemporaryDirectory() as d:
            journal = w.Journal(Path(d) / "j.db")
            try:
                result = asyncio.run(k.verify_report([r], report, decide, "test", journal))
            finally:
                journal.db.close()
        self.assertEqual(result["results"][0]["disposition"], "REVIEW")


if __name__ == "__main__":
    unittest.main()
