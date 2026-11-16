"""Exercise writer SQL only on session-local tables cloned from the deployed schema."""

import json
import unittest
import uuid
import gardener_writer as w

DDL = """
CREATE TEMP TABLE hermes_garden_reviews(bank_id text,memory_id uuid,run_id text,policy text,snapshot_hash text,recommendation jsonb,reviewed_at timestamptz DEFAULT now(),next_review_at timestamptz,PRIMARY KEY(bank_id,memory_id));
CREATE TEMP TABLE memory_units (LIKE public.memory_units INCLUDING ALL);
CREATE TEMP TABLE memory_links (LIKE public.memory_links INCLUDING ALL);
CREATE TEMP TABLE unit_entities (LIKE public.unit_entities INCLUDING ALL);
CREATE TEMP TABLE observation_history (LIKE public.observation_history INCLUDING ALL);
ALTER TABLE memory_links ADD FOREIGN KEY(from_unit_id) REFERENCES memory_units(id) ON DELETE CASCADE;
ALTER TABLE memory_links ADD FOREIGN KEY(to_unit_id) REFERENCES memory_units(id) ON DELETE CASCADE;
ALTER TABLE unit_entities ADD FOREIGN KEY(unit_id) REFERENCES memory_units(id) ON DELETE CASCADE;
CREATE TEMP TABLE hermes_garden_journal(run_id text,memory_id uuid,bank_id text,action text,reason text,before_row jsonb,links jsonb,unit_entities jsonb,observation_history jsonb,after_row jsonb,applied_at timestamptz DEFAULT now(),reverted_at timestamptz,PRIMARY KEY(run_id,memory_id));
"""
A = "00000000-0000-0000-0000-000000009001"
B = "00000000-0000-0000-0000-000000009002"


def fixture(kind="experience", tags="{}"):
    return (
        "INSERT INTO memory_units(id,bank_id,text,fact_type,tags) VALUES ('"
        + A
        + "','hermes-ops','complete useful content','"
        + kind
        + "','"
        + tags
        + "');\n"
    )


def sql(action="PRUNE", version="current", tags=None):
    items = [
        {
            "id": A,
            "version": version,
            "action": action,
            "tags": tags
            or ["kind:note", "scope:project", "domain:tools", "confidence:medium", "source:session", "task:debug"],
            "reason": "test",
            "verdict": action,
        }
    ]
    text = w.apply_sql(items, "fixture")
    if version == "current":
        text = text.replace("m.xmin::text<>p.version", "false")
    return text


def run(body):
    return w.query(DDL + body)


class WriterTests(unittest.TestCase):
    def test_duplicate_requires_current_text_only_proof(self):
        import gardener_keeper as k

        row = {
            "id": A,
            "bank_id": "hermes-ops",
            "snapshot_version": "1",
            "text": "Nimbus skill supports quotes.",
            "canonical_peers": [{"id": B, "text": "Nimbus skill supports quotes."}],
        }
        recommendation = {
            "id": A,
            "snapshot_version": "1",
            "disposition": "DUPLICATE",
            "keeper_id": B,
            "keeper_text": "Nimbus skill supports quotes.",
        }
        self.assertEqual(w.plan([row], {"results": [recommendation]})[0]["action"], "HOLD")
        proof = {
            "policy": k.POLICY,
            "keeper_id": B,
            "original_sha256": k.digest(row["text"]),
            "keeper_sha256": k.digest(recommendation["keeper_text"]),
            "standalone_probability": 0.95,
            "preservation_probability": 0.95,
        }
        recommendation["keeper_verification"] = proof
        self.assertEqual(w.plan([row], {"results": [recommendation]})[0]["action"], "PRUNE")
        recommendation["keeper_text"] = "Changed keeper"
        self.assertEqual(w.plan([row], {"results": [recommendation]})[0]["action"], "HOLD")

    def test_prune_and_restore(self):
        r = run(
            fixture()
            + sql()
            + "SELECT json_build_object('remaining',count(*)) FROM memory_units;"
            + w.rollback_sql("fixture")
            + "SELECT json_build_object('restored_text',text,'id',id) FROM memory_units;"
        )
        self.assertEqual(r[1]["remaining"], 0)
        self.assertEqual(r[-1]["restored_text"], "complete useful content")
        self.assertEqual(r[-1]["id"], A)

    def test_graph_children_restored_after_real_cascade(self):
        body = (
            fixture()
            + "INSERT INTO memory_units(id,bank_id,text,fact_type) VALUES ('"
            + B
            + "','hermes-ops','other useful content','experience');"
        )
        body += (
            "INSERT INTO memory_links(from_unit_id,to_unit_id,link_type,bank_id) VALUES ('"
            + A
            + "','"
            + B
            + "','semantic','hermes-ops');"
        )
        body += (
            "INSERT INTO unit_entities(unit_id,entity_id) VALUES ('" + A + "','00000000-0000-0000-0000-000000009999');"
        )
        body += (
            "INSERT INTO observation_history(id,observation_id,bank_id,content) VALUES (1,'"
            + A
            + "','hermes-ops','{}');"
        )
        body += (
            sql()
            + "SELECT json_build_object('links',(SELECT count(*) FROM memory_links),'entities',(SELECT count(*) FROM unit_entities),'history',(SELECT count(*) FROM observation_history));"
        )
        body += (
            w.rollback_sql("fixture")
            + "SELECT json_build_object('links',(SELECT count(*) FROM memory_links),'entities',(SELECT count(*) FROM unit_entities),'history',(SELECT count(*) FROM observation_history));"
        )
        rows = run(body)
        self.assertEqual(rows[1], {"links": 0, "entities": 0, "history": 0})
        self.assertEqual(rows[-1], {"links": 1, "entities": 1, "history": 1})

    def test_embedding_round_trip(self):
        body = (
            fixture()
            + "UPDATE memory_units SET embedding=(SELECT embedding FROM public.memory_units WHERE bank_id='hermes-ops' AND embedding IS NOT NULL LIMIT 1);"
        )
        body += (
            "SELECT json_build_object('embedding',embedding::text) FROM memory_units;"
            + sql()
            + w.rollback_sql("fixture")
            + "SELECT json_build_object('embedding',embedding::text) FROM memory_units;"
        )
        rows = run(body)
        self.assertIsNotNone(rows[0]["embedding"])
        self.assertEqual(rows[0]["embedding"], rows[-1]["embedding"])

    def test_selected_rollback_restores_only_chosen_id(self):
        body = fixture() + fixture().replace(A, B) + sql() + sql().replace(A, B)
        body += w.rollback_sql("fixture", [A]) + "SELECT json_build_object('ids',array_agg(id)) FROM memory_units;"
        self.assertEqual(run(body)[-1]["ids"], [A])

    def test_keeper_change_blocks_duplicate_prune(self):
        body = (
            fixture("observation")
            + "INSERT INTO memory_units(id,bank_id,text,fact_type) VALUES ('"
            + B
            + "','hermes-ops','changed canonical text','world');"
        )
        items = [
            {
                "id": A,
                "version": "current",
                "action": "PRUNE",
                "tags": [],
                "reason": "confirmed-duplicate",
                "verdict": "DUPLICATE",
                "keeper_id": B,
                "keeper_text": "original canonical text",
            }
        ]
        statement = w.apply_sql(items, "fixture").replace("m.xmin::text<>p.version", "false")
        rows = run(body + statement + "SELECT json_build_object('copies',count(*)) FROM memory_units;")
        self.assertEqual(rows[0]["blocked"], "keeper-changed-or-missing")
        self.assertEqual(rows[-1]["copies"], 2)

    def test_protected_world(self):
        r = run(fixture("world") + sql())
        self.assertEqual(r[0]["blocked"], "protected-world")

    def test_protected_decision(self):
        r = run(fixture(tags="{kind:decision}") + sql())
        self.assertEqual(r[0]["blocked"], "protected-tag")

    def test_changed_version(self):
        r = run(fixture() + sql(version="0"))
        self.assertEqual(r[0]["blocked"], "version-conflict")

    def test_source_dependency(self):
        f = (
            fixture()
            + "INSERT INTO memory_units(id,bank_id,text,fact_type,source_memory_ids) VALUES ('"
            + B
            + "','hermes-ops','derived','observation',ARRAY['"
            + A
            + "'::uuid]);"
        )
        r = run(f + sql())
        self.assertEqual(r[0]["blocked"], "source-dependency")

    def test_tag_rollback_and_preservation(self):
        f = fixture(tags="{kind:note,scope:project,domain:tools,confidence:medium,source:session}")
        r = run(f + sql("TAG") + w.rollback_sql("fixture") + "SELECT json_build_object('tags',tags) FROM memory_units;")
        self.assertNotIn("task:debug", r[-1]["tags"])
        self.assertIn("confidence:medium", r[-1]["tags"])

    def test_idempotency(self):
        r = run(
            fixture()
            + sql("TAG")
            + sql("TAG")
            + "SELECT json_build_object('journal',count(*)) FROM hermes_garden_journal;"
        )
        self.assertEqual(r[-1]["journal"], 1)

    def test_rollback_does_not_overwrite_later_edit(self):
        r = run(
            fixture()
            + sql("TAG")
            + "UPDATE memory_units SET text='later correction';"
            + w.rollback_sql("fixture")
            + "SELECT json_build_object('text',text) FROM memory_units;"
        )
        self.assertEqual(r[-2]["restored"], 0)
        self.assertEqual(r[-1]["text"], "later correction")

    def test_refuse_other_bank(self):
        with self.assertRaises(ValueError):
            w.plan(
                [{"id": A, "bank_id": "main", "snapshot_version": "1"}],
                {"results": [{"id": A, "snapshot_version": "1", "disposition": "KEEP"}]},
            )

    def test_missing_provenance_held(self):
        p = w.plan(
            [{"id": A, "bank_id": "hermes-ops", "snapshot_version": "1", "tags": []}],
            {"results": [{"id": A, "snapshot_version": "1", "disposition": "KEEP"}]},
        )
        self.assertEqual(p[0]["action"], "HOLD")

    def test_draft_taxonomy_not_invented(self):
        r = {
            "id": A,
            "bank_id": "hermes-ops",
            "snapshot_version": "1",
            "tags": ["kind:note", "scope:project", "domain:tools", "confidence:medium", "source:session"],
        }
        p = w.plan(
            [r], {"results": [{"id": A, "snapshot_version": "1", "disposition": "KEEP", "draft_tags": ["task:debug"]}]}
        )
        self.assertIn("confidence:medium", p[0]["tags"])
        self.assertNotIn("confidence:high", p[0]["tags"])


if __name__ == "__main__":
    unittest.main()
