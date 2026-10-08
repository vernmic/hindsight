import unittest
from unittest.mock import patch
import tempfile
from pathlib import Path
import gardener_repair as r


class RepairTests(unittest.TestCase):
    def test_other_bank_refused(self):
        with self.assertRaises(ValueError):
            r.api("main")

    def test_low_fidelity_held(self):
        def decide(state, q):
            return {
                "answers": {
                    k: {
                        "choice": "source-faithful",
                        "probabilities": {"source-faithful": 0.51, "unsupported-addition": 0.49},
                    }
                    for k in q
                }
            }

        valid, _ = r.validate({"id": "00000000-0000-0000-0000-000000001234"}, {"text": "repair"}, decide)
        self.assertFalse(valid)

    def test_observation_cannot_be_edited(self):
        with self.assertRaises(ValueError):
            r.proposal({"fact_type": "observation"})

    def test_protected_no_api(self):
        with patch.object(r, "api") as api:
            self.assertEqual(
                r.apply({"id": "00000000-0000-0000-0000-000000001234", "tags": ["kind:decision"]}, {"text": "changed"})[
                    "held"
                ],
                "protected-source-repair",
            )
            api.assert_not_called()

    def test_commit_resume_recovers_receipt(self):
        row = {"id": "00000000-0000-0000-0000-000000001234", "text": "old", "document_id": "doc"}
        candidate = {"text": "new"}
        with tempfile.TemporaryDirectory() as d, patch.object(r, "ROOT", Path(d)):
            with patch.object(
                r,
                "api",
                side_effect=[
                    {"id": row["id"], "text": "old", "document_id": "doc"},
                    {"id": row["id"], "text": "new", "document_id": "doc"},
                ],
            ):
                r.apply(row, candidate)
            journal = next(Path(d).rglob("native-curation.json"))
            import json

            record = json.loads(journal.read_text())
            record.pop("after")
            record["stage"] = "planned"
            journal.write_text(json.dumps(record))
            with patch.object(r, "api", return_value={"id": row["id"], "text": "new", "document_id": "doc"}) as api:
                restored = r.apply(row, candidate)
                self.assertTrue(restored["recovered_after_native_commit"])
                self.assertEqual(api.call_count, 1)

    def test_concurrent_text_not_overwritten(self):
        with (
            tempfile.TemporaryDirectory() as d,
            patch.object(r, "ROOT", Path(d)),
            patch.object(r, "api", return_value={"text": "another edit"}) as api,
        ):
            self.assertEqual(
                r.apply({"id": "00000000-0000-0000-0000-000000001234", "text": "old"}, {"text": "new"})["held"],
                "source-changed",
            )
            self.assertEqual(api.call_count, 1)


if __name__ == "__main__":
    unittest.main()
