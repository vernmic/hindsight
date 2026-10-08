import asyncio, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
import gardener_run as g


class ControllerTests(unittest.TestCase):
    def test_many_observation_sources_keep_100_memory_limit(self):
        source = [{"id": "source-%03d" % i, "fact_type": "world"} for i in range(205)]
        records = [{"id": "observation", "fact_type": "observation", "source_facts": source}]
        report = {"results": [{"id": "observation", "disposition": "REPAIR"}]}
        sizes = []

        def fetch(ids):
            sizes.append(len(ids))
            return [{"id": key} for key in ids]

        async def review(rows, *args, **kwargs):
            return {"results": [{"id": r["id"], "disposition": "KEEP"} for r in rows]}

        with (
            patch.object(g.worker, "fetch_page", side_effect=fetch),
            patch.object(g.worker, "review_page", side_effect=review),
        ):
            result = asyncio.run(
                g.repair_page(records, report, None, {"model": "test", "max_state_chars": 100000}, None)
            )
        self.assertEqual(sizes, [100, 100, 5])
        self.assertEqual(result, [])

    def test_recover_consolidation_pause_marker(self):
        with (
            tempfile.TemporaryDirectory() as d,
            patch.object(g, "PAUSE", Path(d) / "pause.json"),
            patch.object(g, "api") as api,
        ):
            g.PAUSE.write_text('{"restore_enable_observations":true}')
            g.restore_consolidation()
            self.assertFalse(g.PAUSE.exists())
            self.assertEqual(api.call_args.args[2], {"updates": {"enable_observations": True}})


class StorageSnapshotTests(unittest.TestCase):
    def test_resume_scope(self):
        self.assertEqual(g.storage_resume_path(g.RUN / "storage-test"), g.RUN / "storage-test")
        for path in (g.ROOT / "storage-test", g.RUN / "other-test", g.RUN / "storage-test" / "storage-child"):
            with self.assertRaises(ValueError):
                g.storage_resume_path(path)

    def test_storage_snapshot_retains_guards_and_peers_without_document_fetch(self):
        from unittest.mock import patch

        key = "00000000-0000-0000-0000-000000001235"
        rows = [
            {
                "id": key,
                "source_facts": [
                    {"id": "parent", "fact_type": "world", "text": "Nimbus skill supports quotes."},
                    {"id": "other", "fact_type": "experience", "text": "experience"},
                ],
            }
        ]
        with patch.object(g.writer, "query", return_value=rows) as q:
            actual = g.fetch_storage_page([key])
        sql = q.call_args[0][0]
        self.assertIn("m.bank_id='hermes-ops'", sql)
        self.assertIn("m.xmin::text", sql)
        self.assertIn("referenced_by_observation", sql)
        self.assertIn("causal_linked", sql)
        self.assertNotIn("FROM documents", sql)
        self.assertEqual([r["id"] for r in actual[0]["canonical_peers"]], ["parent"])

    def test_storage_snapshot_rejects_missing_or_duplicate_ids(self):
        from unittest.mock import patch

        key = "00000000-0000-0000-0000-000000001235"
        with self.assertRaises(ValueError):
            g.fetch_storage_page([key, key])
        with patch.object(g.writer, "query", return_value=[]):
            with self.assertRaises(ValueError):
                g.fetch_storage_page([key])


if __name__ == "__main__":
    unittest.main()
