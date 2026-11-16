"""Offline writer behavior: bound SQL, REFRESH semantics and fail-closed config.

Nothing here opens a database. `subprocess.run` is captured, so the assertions
are about the command and SQL text the writer would send.
"""

import json
import os
import unittest
import uuid
from unittest.mock import patch

import hermes_ops_config as config
import gardener_writer as writer

BANK = "fixture-bank"
ROW = "11111111-1111-1111-1111-111111111111"


def record(text="complete useful content", tags=None, **extra):
    row = {
        "id": ROW,
        "bank_id": BANK,
        "text": text,
        "snapshot_version": "42",
        "tags": tags or [],
        "metadata": {},
    }
    row.update(extra)
    return row


def report(disposition, reason, **extra):
    result = {"id": ROW, "disposition": disposition, "reason": reason, "snapshot_version": "42"}
    result.update(extra)
    return {"policy": "test-policy", "results": [result]}


class WriterTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"HERMES_OPS_BANK": BANK}, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_bank_comes_from_configuration(self):
        self.assertEqual(writer.bank(), BANK)

    def test_query_sends_configured_command(self):
        calls = []

        class Result:
            returncode = 0
            stdout = '{"ok": 1}\n'
            stderr = ""

        def fake_run(argv, **kwargs):
            calls.append((argv, kwargs))
            return Result()

        with patch.dict(os.environ, {"HINDSIGHT_PGPASSWORD": "temporary-secret"}):
            with patch("subprocess.run", fake_run):
                rows = writer.query("SELECT 1;")
        self.assertEqual(rows, [{"ok": 1}])
        argv, kwargs = calls[0]
        self.assertIn("PGPASSWORD=temporary-secret", argv)
        self.assertEqual(kwargs["input"], "SELECT 1;")

    def test_ddl_binds_the_configured_bank(self):
        ddl = writer.ddl()
        self.assertIn("CHECK(bank_id='%s')" % BANK, ddl)
        self.assertIn("'REFRESH'", ddl)
        self.assertNotIn("hermes-ops", ddl)

    def test_apply_sql_binds_bank_everywhere(self):
        items = writer.plan([record()], report("KEEP", "useful-knowledge"))
        sql = writer.apply_sql(items, "run-1")
        self.assertIn("bank_id='%s'" % BANK, sql)
        self.assertNotIn("hermes-ops", sql)
        self.assertIn("p.action='REFRESH'", sql)

    def test_rollback_sql_restores_refreshed_rows(self):
        sql = writer.rollback_sql("run-1")
        self.assertIn("('TAG','RESCUE','REFRESH')", sql)
        self.assertNotIn("hermes-ops", sql)

    def test_plan_prune_of_ordinary_text(self):
        items = writer.plan([record("probe tick 12 green")], report("PRUNE", "probe-or-telemetry"))
        self.assertEqual(items[0]["action"], "PRUNE")

    def test_plan_refreshes_a_durable_reference(self):
        text = "The retention policy is documented in reports/retention-policy.md for reference."
        items = writer.plan([record(text)], report("PRUNE", "useful-reference"))
        self.assertEqual(items[0]["action"], "REFRESH")

    def test_plan_refreshes_path_anchored_keep_reason(self):
        text = "Install notes live in docs/install.md and stay valid as the artifact persists."
        items = writer.plan([record(text)], report("PRUNE", "mixed-useful-and-noise"))
        self.assertEqual(items[0]["action"], "REFRESH")
        self.assertTrue(items[0]["reason"].endswith("->refreshed:durable-reference"))

    def test_plan_prunes_when_an_override_exists(self):
        text = "Install notes live in docs/install.md and stay valid as the artifact persists."
        items = writer.plan([record(text)], report("PRUNE", "useful-reference", override_evidence=ROW))
        self.assertEqual(items[0]["action"], "PRUNE")

    def test_plan_refuses_another_bank(self):
        row = record()
        row["bank_id"] = "other-bank"
        with self.assertRaises(ValueError):
            writer.plan([row], report("KEEP", "useful-knowledge"))

    def test_missing_bank_fails_closed(self):
        os.environ.pop("HERMES_OPS_BANK")
        with patch.object(config, "_policy_bank", return_value=None):
            with self.assertRaises(config.ConfigError):
                writer.bank()

    def test_run_ids_reject_unsafe_characters(self):
        with self.assertRaises(ValueError):
            writer.apply_sql([], "run'; DROP TABLE memory_units; --")

    def test_provenance_defaults_use_configured_scope(self):
        items = writer.plan(
            [record(document_id="doc-1", source_documents=[{"document_id": "doc-1"}])],
            report("KEEP", "useful-knowledge", draft_tags=["domain:memory-system"]),
        )
        self.assertIn("scope:%s" % BANK, items[0]["tags"])
        self.assertIn("source:document:doc-1", items[0]["tags"])


if __name__ == "__main__":
    unittest.main()
