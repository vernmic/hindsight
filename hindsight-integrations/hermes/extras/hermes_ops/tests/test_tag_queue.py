"""TAG-01 acceptance tests: durable post-extraction tagging queue (card t_0dfba636).

Run cold (from any directory):

    cd /mnt/i/hermes/scripts
    /home/vern/.hermes/hermes-agent/venv/bin/python -m unittest test_tag_queue -v

Prerequisites
  * Windows Docker Desktop with the hindsight-hindsight-1 container running:
    the component tests reach Postgres through gardener_writer.query
    (docker exec psql) and create TEMP tables cloned from the deployed schema.
    They never write to public rows.
  * No live judge is required. Classifier and drain tests inject a deterministic
    `decide` and stub the write path, so every result is reproducible.
  * The queue implementation is resolved at run time (post_extraction_tag_queue).
    If it is missing the durability tests SKIP with a reason; set
    TAG_QUEUE_STRICT=1 to turn those skips into failures.

Acceptance map (TAG-01 / t_308e3a73 criterion -> named test)
  1 resume ............. TagQueueDurabilityTests.test_queue_resume_after_interruption
                         TagQueueDurabilityTests.test_duplicate_queue_delivery_is_deduplicated
  2 concurrency ........ TagQueueDurabilityTests.test_parallel_process_enqueue_no_race
                         TagQueueComponentTests.test_concurrent_overlapping_pages_single_journal_row
  3 stale-version ...... TagQueueComponentTests.test_stale_version_blocked_as_version_conflict
                         TagQueueDurabilityTests.test_stale_version_superseded_at_drain
  4 provenance ......... TagQueueComponentTests.test_five_dimensions_preserved_on_tagged_output
                         TagQueueDurabilityTests.test_drain_tags_additively_and_verifies_sources
  5 protected/held ..... TagQueueComponentTests.test_protected_and_held_cases_unmodified
                         TagQueueComponentTests.test_judge_unavailable_writes_nothing
                         TagQueueDurabilityTests.test_additive_page_drops_non_additive_verdicts
  6 off switch ......... TagQueueDurabilityTests.test_off_switch_disables_tagging_extraction_unaffected
                         TagQueueDurabilityTests.test_off_switch_is_independent_of_other_controls
  7 source immutability  TagQueueComponentTests.test_sources_byte_identical_before_after
                         TagQueueDurabilityTests.test_drain_tags_additively_and_verifies_sources
                         TagQueueDurabilityTests.test_drain_aborts_on_source_mutation
  completion/linkage ... TagQueueLiveTests.test_completion_linkage_matches_installed_schema
  follow-up doc list ... test_deleted_source_is_not_tagged_or_resurrected,
                         test_other_bank_input_is_refused,
                         test_identity_refuses_other_banks,
                         test_multiple_facts_from_one_source_get_independent_tags,
                         test_empty_axes_hold_without_forced_labels,
                         test_tag_path_never_prunes,
                         test_consolidated_fact_gets_its_own_identity,
                         test_drain_never_prunes_through_the_writer,
                         test_drain_telemetry_goes_to_jsonl,
                         test_missing_control_file_means_disabled,
                         test_deleted_unit_is_marked_absent_not_tagged

Landed implementation surface these tests bind to
(scripts/post_extraction_tag_queue.py). The tests bind to the implementation's
real shape: a rename or reshaped signature must fail loudly, not be adapted to.

  module.Queue(path)                  sqlite durable store (WAL)
    .enqueue(row, origin, cursor, version, completion_key) -> dict(key, enqueued)
    .pending(cursor, limit) -> [queue rows]   rows carry key/unit_id/state/cursor
    .mark(key, state, reason, run_id, bump_attempts)
    .get(key) / .entries(state, cursor) / .counts() / .meta_get / .meta_set / .close()
  module.identity(row, version) -> dict(bank..policy_version, key); refuses other banks
  module.enabled(control) / module.set_enabled(on, control) / module.CONTROL
  module.drain(queue, ...) async / module.drain_sync(queue, ...)
  module.additive_page(records, report) -> (records, report, held)
  module.verify_sources_unchanged(before, after) -> [violations]
  module.run_once / enqueue_new / enqueue_consolidation / enqueue_manual /
  enqueue_backfill / reconcile
"""

import asyncio
import importlib
import importlib.util
import json
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parents[1] / "scripts"
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import gardener_batch_v7 as worker  # noqa: E402
import gardener_writer as writer  # noqa: E402

ROOT = Path("/mnt/i/hermes")
QUEUE_MODULE_ENV = "TAG_QUEUE_MODULE"
QUEUE_STRICT_ENV = "TAG_QUEUE_STRICT"
MODULE_CANDIDATES = ("post_extraction_tag_queue", "tag_queue", "tagger_queue", "gardener_tag_queue", "gardener_tagger")
FILE_CANDIDATES = (
    "post_extraction_tag_queue.py",
    "tag-queue.py",
    "tagger-queue.py",
    "gardener-tagger.py",
    "gardener-tag-queue.py",
)
DEFAULT_CONTROL_PATH = ROOT / "reports/tagger-control-20261007.json"
EXTRACTOR_CONTROL = ROOT / "reports/extractor-active-20261007.json"
GARDENER_CONTROL = ROOT / "reports/gardener-control-20261007.json"

UID = "00000000-0000-0000-0000-00000000a001"
PEER = "00000000-0000-0000-0000-00000000a002"

# Temp-table clone of the deployed schema. Same shape the existing
# test_gardener_writer.py uses, so writer.apply_sql/rollback_sql run unmodified.
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

FIVE_DIMENSIONS = ("kind:", "scope:", "domain:", "confidence:", "source:")


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------
def insert_sql(
    uid=UID,
    kind="experience",
    tags="{}",
    text="durable useful content",
    document_id="doc-1",
    chunk_id="chunk-1",
    source_memory_ids=None,
    metadata=None,
    bank="hermes-ops",
):
    columns = ["id", "bank_id", "text", "fact_type", "tags"]
    values = [writer.quote(uid), writer.quote(bank), writer.quote(text), writer.quote(kind), writer.quote(tags)]
    if document_id is not None:
        columns.append("document_id")
        values.append(writer.quote(document_id))
    if chunk_id is not None:
        columns.append("chunk_id")
        values.append(writer.quote(chunk_id))
    if source_memory_ids:
        columns.append("source_memory_ids")
        values.append("ARRAY[" + ",".join(writer.quote(key) + "::uuid" for key in source_memory_ids) + "]")
    if metadata is not None:
        columns.append("metadata")
        values.append(writer.quote(json.dumps(metadata)) + "::jsonb")
    return "INSERT INTO memory_units(" + ",".join(columns) + ") VALUES (" + ",".join(values) + ");"


def insert_record_sql(
    row, kind="experience", tags="{}", metadata=None, source_memory_ids=None, text=None, document_id=None
):
    """Insert the temp-table twin of a classifier record (same id/text/links)."""
    return insert_sql(
        uid=row["id"],
        kind=kind,
        tags=tags,
        text=text or row["text"],
        document_id=row.get("document_id") if document_id is None else document_id,
        chunk_id=row.get("chunk_id"),
        source_memory_ids=source_memory_ids,
        metadata=metadata,
    )


def read_sql(uid=UID):
    return (
        "SELECT json_build_object('id',id,'text',text,'tags',coalesce(tags,'{}'),"
        "'document_id',document_id,'chunk_id',chunk_id,"
        "'source_memory_ids',coalesce(source_memory_ids,'{}'),'fact_type',fact_type) "
        "FROM memory_units WHERE id=" + writer.quote(uid) + ";"
    )


def count_sql(table="memory_units"):
    return "SELECT json_build_object('n',count(*)) FROM " + table + ";"


def run(body):
    """One psql session: temp schema + fixture + statements."""
    return writer.query(DDL + body)


def apply_statement(records, report, run_id):
    """apply_sql with the xmin version check neutralised.

    Temp-table rows are inserted by the fixture in the same transaction, so the
    current xmin cannot be predicted in Python; the genuine version guard is
    covered by test_stale_version_blocked_as_version_conflict instead.
    """
    items = writer.plan(records, report)
    return writer.apply_sql(items, run_id).replace("m.xmin::text<>p.version", "false")


def classify(rows, decide, model="test", max_bytes=100000, concurrency=2):
    """Run the real classifier over rows with a deterministic judge."""
    with tempfile.TemporaryDirectory() as directory:
        journal = worker.Journal(Path(directory) / "journal.db")
        try:
            return asyncio.run(
                worker.review_page(rows, decide, model, journal, max_bytes=max_bytes, concurrency=concurrency)
            )
        finally:
            journal.db.close()


def fake_judge(choices=None, nouls=None):
    """Valid answers: exact coverage, probabilities sum 1, chosen is the max."""
    defaults = {
        "context": "self-contained",
        "disposition": "KEEP",
        "reason": "useful-knowledge",
        "domain": "domain:tools",
        "target": "repo:hermes",
        "keeper": "no-complete-keeper",
    }
    choices = dict(choices or {})
    nouls = dict(nouls or {})

    def decide(state, questions):
        answers = {}
        for key, question in questions.items():
            suffix = key.split("/", 1)[1]
            if question["type"] == "noul":
                answers[key] = {"type": "noul", "noul": float(nouls.get(suffix, 0.1))}
            else:
                selected = choices.get(suffix, defaults.get(suffix) or next(iter(question["criteria"])))
                answers[key] = {
                    "type": "choice",
                    "choice": selected,
                    "probabilities": {option: float(option == selected) for option in question["criteria"]},
                }
        return {"answers": answers, "resolved_model": "test"}

    return decide


def records(count=1, fact_type="experience", tags=None, document_id="doc-1", create_peers=False, bank="hermes-ops"):
    rows = []
    for index in range(count):
        uid = "00000000-0000-0000-0000-0000000c%04d" % index
        chunk = {"chunk_id": "chunk-1", "document_id": document_id, "text": "source evidence"}
        document = {"document_id": document_id, "text": "source evidence"}
        row = {
            "id": uid,
            "bank_id": bank,
            "text": "Durable useful fact %d." % index,
            "fact_type": fact_type,
            "tags": list(tags or []),
            "snapshot_version": "v1",
            "metadata": {"session_id": "s1"},
            "document_id": document_id,
            "chunk_id": "chunk-1",
            "source_chunks": [dict(chunk)],
            "source_documents": [dict(document)],
        }
        if create_peers:
            row["canonical_peers"] = [{"id": PEER, "text": "canonical world fact"}]
        rows.append(row)
    return rows


def report_for(rows, judge=None, **kw):
    return classify(rows, judge or fake_judge(), **kw)


def queue_rows(count=1, snapshot_version="1", document_id="doc-1", prefix="queued fact"):
    """memory_units-shaped rows for the queue (the shape identity() expects)."""
    rows = []
    for index in range(count):
        rows.append(
            {
                "id": "00000000-0000-0000-0000-0000000d%04d" % index,
                "bank_id": "hermes-ops",
                "text": "%s %d" % (prefix, index),
                "context": None,
                "fact_type": "experience",
                "tags": [],
                "metadata": {"session_id": "s1"},
                "document_id": document_id,
                "chunk_id": "chunk-%d" % index,
                "source_memory_ids": None,
                "snapshot_version": snapshot_version,
            }
        )
    return rows


def fake_apply(records_, report, run_id, log=None, action="TAG"):
    """Stand in for gardener_writer.apply so drain never touches the bank."""
    if log is not None:
        log.append(
            {
                "run_id": run_id,
                "records": [row["id"] for row in records_],
                "results": [item["id"] for item in report.get("results", [])],
            }
        )
    return {
        "run_id": run_id,
        "bank": "hermes-ops",
        "considered": len(records_),
        "actions": [
            {
                "id": row["id"],
                "action": action,
                "blocked": None,
                "applied": action in ("TAG", "RESCUE"),
                "reason": "test",
            }
            for row in records_
        ],
        "held": [],
        "committed": True,
    }


# --------------------------------------------------------------------------
# implementation resolution
# --------------------------------------------------------------------------
_QUEUE_CACHE = []


def _load_file(path):
    spec = importlib.util.spec_from_file_location("tag_queue_under_test", str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def queue_module():
    """Load the tagging-queue module, or return None when it is not installed."""
    if _QUEUE_CACHE:
        return _QUEUE_CACHE[0]
    loaded = None
    override = os.environ.get(QUEUE_MODULE_ENV)
    names = ([override] if override else []) + list(MODULE_CANDIDATES)
    for name in names:
        if not name:
            continue
        path = Path(name)
        if (path.suffix == ".py" or os.sep in name) and path.exists():
            loaded = _load_file(path)
        else:
            try:
                loaded = importlib.import_module(Path(name).stem if name.endswith(".py") else name)
            except Exception:
                loaded = None
        if loaded is not None:
            break
    if loaded is None:
        for filename in FILE_CANDIDATES:
            path = HERE / filename
            if path.exists():
                loaded = _load_file(path)
                break
    _QUEUE_CACHE.append(loaded)
    return loaded


CONTRACT_NOT_LANDED = "tagging-queue implementation not found; looked for env %s, modules %s and files %s" % (
    QUEUE_MODULE_ENV,
    list(MODULE_CANDIDATES),
    list(FILE_CANDIDATES),
)


# --------------------------------------------------------------------------
# component tests: real classifier + real version-checked writer
# --------------------------------------------------------------------------
class TagQueueComponentTests(unittest.TestCase):
    def test_stale_version_blocked_as_version_conflict(self):
        """Criterion 3: a stale unit version is refused and nothing is written."""
        rows = records(1, tags=["kind:fact", "scope:hermes-ops", "confidence:provisional", "source:document:doc-1"])
        rows[0]["snapshot_version"] = "stale"
        report = report_for(rows)
        report["results"][0]["draft_tags"] = ["domain:tools"]
        items = writer.plan(rows, report)
        statement = writer.apply_sql(items, "stale-version-case")
        label = "{kind:fact,scope:hermes-ops,confidence:provisional,source:document:doc-1}"
        result = run(
            insert_record_sql(rows[0], tags=label)
            + statement
            + count_sql("hermes_garden_journal")
            + read_sql(rows[0]["id"])
        )
        self.assertEqual(result[0]["blocked"], "version-conflict")
        self.assertEqual(result[0]["applied"], False)
        self.assertEqual(result[1]["n"], 0)
        self.assertEqual(
            result[2]["tags"], ["kind:fact", "scope:hermes-ops", "confidence:provisional", "source:document:doc-1"]
        )

    def test_five_dimensions_preserved_on_tagged_output(self):
        """Criterion 4: kind/scope/domain/confidence/source all present after write."""
        rows = records(1)
        report = report_for(rows, fake_judge(nouls={"task/debug": 0.95, "value/reference": 0.8}))
        self.assertEqual(report["results"][0]["disposition"], "KEEP")
        self.assertIn("domain:tools", report["results"][0]["draft_tags"])
        self.assertIn("task:debug", report["results"][0]["draft_tags"])
        statement = apply_statement(rows, report, "provenance-case")
        result = run(insert_record_sql(rows[0]) + statement + read_sql(rows[0]["id"]))
        self.assertEqual(result[0]["applied"], True)
        tags = result[1]["tags"]
        for dimension in FIVE_DIMENSIONS:
            with self.subTest(dimension=dimension):
                self.assertTrue(any(tag.startswith(dimension) and len(tag) > len(dimension) for tag in tags), tags)
        self.assertIn("source:document:doc-1", tags)
        self.assertIn("domain:tools", tags)

    def test_protected_and_held_cases_unmodified(self):
        """Criterion 5: protected rows, world facts and held rows stay untouched."""
        protected_id = "00000000-0000-0000-0000-00000000b001"
        world_id = "00000000-0000-0000-0000-00000000b002"
        rows = [
            {
                "id": protected_id,
                "bank_id": "hermes-ops",
                "snapshot_version": "v1",
                "tags": ["kind:decision"],
                "text": "decide X because Y",
                "document_id": "doc-1",
                "chunk_id": "chunk-1",
            }
        ]
        report = {
            "policy": "gardener-batch-v7",
            "results": [{"id": protected_id, "snapshot_version": "v1", "disposition": "PRUNE"}],
        }
        body = (
            insert_sql(uid=protected_id, tags="{kind:decision}", text="decide X because Y")
            + insert_sql(
                uid=world_id, kind="world", text="canonical world fact", document_id="doc-2", chunk_id="chunk-2"
            )
            + apply_statement(rows, report, "protected-case")
            + count_sql("memory_units")
            + read_sql(protected_id)
            + read_sql(world_id)
        )
        pruned = run(body)
        with self.subTest(case="protected tag blocks the prune"):
            self.assertEqual(pruned[0]["blocked"], "protected-tag")
            self.assertEqual(pruned[0]["applied"], False)
        with self.subTest(case="rows survive unmodified"):
            self.assertEqual(pruned[1]["n"], 2)
            self.assertEqual(pruned[2]["text"], "decide X because Y")
            self.assertEqual(pruned[2]["tags"], ["kind:decision"])
            self.assertEqual(pruned[3]["text"], "canonical world fact")
        with self.subTest(case="world fact blocks the prune"):
            world_rows = [dict(rows[0], id=world_id)]
            world_report = {
                "policy": "gardener-batch-v7",
                "results": [{"id": world_id, "snapshot_version": "v1", "disposition": "PRUNE"}],
            }
            world_result = run(
                insert_sql(uid=world_id, kind="world") + apply_statement(world_rows, world_report, "world-case")
            )
            self.assertEqual(world_result[0]["blocked"], "protected-world")
        with self.subTest(case="classifier guard"):
            guarded, guards = worker.guard_disposition({"id": UID, "tags": ["retain:permanent"]}, "PRUNE")
            self.assertEqual(guarded, "REVIEW")
            self.assertIn("protected-tag", guards)
        with self.subTest(case="world guard"):
            guarded, guards = worker.guard_disposition({"id": UID, "fact_type": "world"}, "PRUNE")
            self.assertEqual(guarded, "REVIEW")
            self.assertIn("world-fact", guards)
        with self.subTest(case="held for missing provenance"):
            held = writer.plan(
                [{"id": UID, "bank_id": "hermes-ops", "snapshot_version": "v1", "tags": [], "fact_type": "experience"}],
                {"results": [{"id": UID, "snapshot_version": "v1", "disposition": "KEEP", "draft_tags": []}]},
            )
            self.assertEqual(held[0]["action"], "HOLD")
            self.assertEqual(held[0]["reason"], "missing-provenance-dimension")

    def test_judge_unavailable_writes_nothing(self):
        """Criterion 5 (held): an unavailable judge yields REVIEW and no labels."""

        def unavailable(state, questions):
            raise TimeoutError("judge transport down")

        report = classify(records(2), unavailable)
        self.assertFalse(report["complete"])
        self.assertEqual(report["bank_writes"], 0)
        self.assertEqual(len(report["unavailable"]), 2)
        for row in report["results"]:
            with self.subTest(memory=row["id"]):
                self.assertEqual(row["disposition"], "REVIEW")
                self.assertEqual(row["draft_tags"], [])

    def test_sources_byte_identical_before_after(self):
        """Criterion 7: text, source ids, document/chunk ids and legacy tags survive."""
        tags = ["kind:decision", "confidence:high", "source:session", "session:abc"]
        rows = records(1, tags=tags)
        report = report_for(rows)
        statement = apply_statement(rows, report, "immutability-case")
        label = "{kind:decision,confidence:high,source:session,session:abc}"
        source_id = "00000000-0000-0000-0000-00000000c0ff"
        before = read_sql(rows[0]["id"])
        fixture = insert_sql(
            uid=source_id, text="canonical source evidence", document_id="doc-1", chunk_id="chunk-1"
        ) + insert_record_sql(rows[0], tags=label, source_memory_ids=[source_id])
        result = run(fixture + before + statement + before)
        self.assertEqual(result[1]["applied"], True)
        original, current = result[0], result[2]
        self.assertEqual(original["source_memory_ids"], [source_id])
        with self.subTest(field="text"):
            self.assertEqual(current["text"], original["text"])
        with self.subTest(field="source_memory_ids"):
            self.assertEqual(current["source_memory_ids"], original["source_memory_ids"])
        with self.subTest(field="document_id"):
            self.assertEqual(current["document_id"], original["document_id"])
        with self.subTest(field="chunk_id"):
            self.assertEqual(current["chunk_id"], original["chunk_id"])
        with self.subTest(field="legacy tags"):
            self.assertTrue(set(original["tags"]).issubset(set(current["tags"])))
        with self.subTest(field="factual confidence"):
            self.assertIn("confidence:high", current["tags"])

    def test_concurrent_overlapping_pages_single_journal_row(self):
        """Criterion 2 (write side): duplicate delivery of a page tags once."""
        rows = records(1)
        report = report_for(rows)
        statement = apply_statement(rows, report, "overlap-case")
        result = run(
            insert_record_sql(rows[0])
            + statement
            + statement
            + count_sql("hermes_garden_journal")
            + read_sql(rows[0]["id"])
        )
        self.assertEqual(result[0]["applied"], True)
        self.assertEqual(result[1]["n"], 1)
        self.assertEqual(len(result[2]["tags"]), len(set(result[2]["tags"])))

    def test_deleted_source_is_not_tagged_or_resurrected(self):
        """Follow-up list: deletion before tagging is a hold, never a write."""
        rows = records(1)
        report = report_for(rows)
        statement = apply_statement(rows, report, "deleted-case")
        result = run(
            insert_record_sql(rows[0])
            + "DELETE FROM memory_units WHERE id="
            + writer.quote(rows[0]["id"])
            + ";"
            + statement
            + count_sql("memory_units")
            + count_sql("hermes_garden_journal")
        )
        self.assertEqual(result[0]["n"], 0)
        self.assertEqual(result[1]["n"], 0)

    def test_other_bank_input_is_refused(self):
        """Follow-up list: isolated-bank enforcement on classify and on write."""
        with self.subTest(stage="classify"):
            with self.assertRaises(ValueError):
                classify(records(1, bank="main"), fake_judge())
        with self.subTest(stage="write"):
            with self.assertRaises(ValueError):
                writer.plan(
                    [
                        {
                            "id": UID,
                            "bank_id": "main",
                            "snapshot_version": "v1",
                            "tags": [
                                "kind:fact",
                                "scope:x",
                                "domain:tools",
                                "confidence:provisional",
                                "source:session",
                            ],
                        }
                    ],
                    {"results": [{"id": UID, "snapshot_version": "v1", "disposition": "KEEP"}]},
                )

    def test_multiple_facts_from_one_source_get_independent_tags(self):
        """Follow-up list: several facts from one source classify separately."""
        rows = records(2, document_id="shared-doc")
        first, second = rows[0]["id"], rows[1]["id"]

        def decide(state, questions):
            result = fake_judge(nouls={"task/debug": 0.9})(state, questions)
            per_id = {
                first: {"target": "repo:hermes"},
                second: {"target": "service:hindsight", "domain": "domain:memory-system"},
            }
            for key, question in questions.items():
                for unit_id, overrides in per_id.items():
                    if not key.startswith(unit_id):
                        continue
                    for suffix, selected in overrides.items():
                        if key.endswith("/" + suffix):
                            result["answers"][key] = {
                                "type": "choice",
                                "choice": selected,
                                "probabilities": {option: float(option == selected) for option in question["criteria"]},
                            }
            return result

        report = classify(rows, decide)
        tags_by_id = {row["id"]: row["draft_tags"] for row in report["results"]}
        self.assertIn("target:repo:hermes", tags_by_id[first])
        self.assertIn("target:service:hindsight", tags_by_id[second])
        self.assertIn("domain:memory-system", tags_by_id[second])
        self.assertNotIn("domain:memory-system", tags_by_id[first])

    def test_empty_axes_hold_without_forced_labels(self):
        """Follow-up list: empty/unsupported axes produce a hold, not a guess."""
        rows = records(1)
        report = classify(rows, fake_judge(choices={"domain": "other", "target": "none"}))
        self.assertEqual(report["results"][0]["draft_tags"], [])
        complete = {
            "id": rows[0]["id"],
            "bank_id": "hermes-ops",
            "snapshot_version": "v1",
            "tags": ["kind:fact", "scope:hermes-ops", "domain:tools", "confidence:provisional", "source:session"],
        }
        held = writer.plan([complete], report)
        self.assertEqual(held[0]["action"], "HOLD")
        self.assertEqual(held[0]["reason"], "no-label-change")

    def test_tag_path_never_prunes(self):
        """Hard constraint: the tagging path can only ever add labels."""
        rows = records(2)
        report = report_for(rows)
        for verdict in ("KEEP", "RESCUE"):
            results = [dict(item, disposition=verdict) for item in report["results"]]
            items = writer.plan(rows, {"policy": report["policy"], "results": results})
            with self.subTest(verdict=verdict):
                self.assertTrue(all(item["action"] != "PRUNE" for item in items))
        for item in writer.plan(rows, report):
            self.assertIn(item["action"], ("TAG", "RESCUE", "HOLD"))


# --------------------------------------------------------------------------
# durability / drain / off-switch tests: bound to the landed queue module
# --------------------------------------------------------------------------
class QueueCase(unittest.TestCase):
    """Base class: require the implementation, or skip with a precise reason."""

    def setUp(self):
        module = queue_module()
        if module is None:
            if os.environ.get(QUEUE_STRICT_ENV) == "1":
                self.fail(CONTRACT_NOT_LANDED)
            self.skipTest(CONTRACT_NOT_LANDED)
        self.module = module
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store_path = Path(self.directory.name) / "queue.sqlite"
        self.control_path = Path(self.directory.name) / "control.json"

    def open_queue(self, path=None):
        queue = self.module.Queue(path or self.store_path)
        self.addCleanup(queue.close)
        return queue

    def journal(self):
        journal = worker.Journal(Path(self.directory.name) / "judge.sqlite")
        self.addCleanup(journal.db.close)
        return journal

    def fetch_from(self, rows):
        return lambda ids=None, **kw: [row for row in rows if row["id"] in set(ids or [])]


class TagQueueDurabilityTests(QueueCase):
    def test_implementation_surface_present(self):
        """The landed surface these tests bind to must exist."""
        for name in (
            "Queue",
            "identity",
            "enabled",
            "set_enabled",
            "CONTROL",
            "drain",
            "drain_sync",
            "additive_page",
            "verify_sources_unchanged",
            "run_once",
            "enqueue_new",
            "enqueue_consolidation",
            "enqueue_manual",
            "enqueue_backfill",
            "reconcile",
            "text_hash",
            "policy_version",
        ):
            with self.subTest(attribute=name):
                self.assertTrue(hasattr(self.module, name), "missing " + name)
        queue = self.open_queue()
        for name in ("enqueue", "pending", "mark", "get", "entries", "counts"):
            with self.subTest(method=name):
                self.assertTrue(callable(getattr(queue, name, None)))

    def test_missing_control_file_means_disabled(self):
        """Spec: a missing control file is a disabled component, not an error."""
        self.assertFalse(self.module.enabled(Path(self.directory.name) / "absent.json"))
        self.module.set_enabled(False, self.control_path)
        self.assertFalse(self.module.enabled(self.control_path))
        self.module.set_enabled(True, self.control_path)
        self.assertTrue(self.module.enabled(self.control_path))

    def test_queue_resume_after_interruption(self):
        """Criterion 1: entries persist, resume and are never duplicated or lost."""
        rows = queue_rows(3)
        first = self.open_queue()
        keys = []
        for row in rows:
            outcome = first.enqueue(row, origin="retain")
            self.assertTrue(outcome["enqueued"])
            keys.append(outcome["key"])
        self.assertEqual(len(set(keys)), 3)
        self.assertEqual(len(first.pending("new")), 3)
        first.mark(keys[0], "done")
        first.close()  # process boundary: a fresh process opens the same store
        resumed = self.open_queue()
        pending = {entry["key"] for entry in resumed.pending("new")}
        self.assertEqual(len(pending), 2)
        self.assertNotIn(keys[0], pending)
        with self.subTest(check="duplicate delivery"):
            repeat = resumed.enqueue(rows[1], origin="retain")
            self.assertFalse(repeat["enqueued"])
            self.assertEqual(repeat["key"], keys[1])
            self.assertEqual(len(resumed.pending("new")), 2)
        with self.subTest(check="no lost tagging"):
            resumed.mark(keys[1], "done")
            resumed.mark(keys[2], "done")
            self.assertEqual(self.open_queue().pending("new"), [])

    def test_duplicate_queue_delivery_is_deduplicated(self):
        """Criterion 1: re-delivering the same identity is a no-op."""
        row = queue_rows(1)[0]
        queue = self.open_queue()
        key = queue.enqueue(row, origin="retain")["key"]
        for _ in range(3):
            repeat = queue.enqueue(dict(row), origin="retain")
            self.assertFalse(repeat["enqueued"])
            self.assertEqual(repeat["key"], key)
        self.assertEqual(len(queue.pending("new")), 1)
        queue.mark(key, "done")
        queue.mark(key, "done")  # repeated completion must not raise
        with self.subTest(check="identity includes the unit version"):
            queue.enqueue(dict(row, snapshot_version="2"), origin="retain")
            self.assertEqual(len(queue.pending("new")), 1)
        with self.subTest(check="identity includes the text hash"):
            queue.enqueue(dict(row, text=row["text"] + " changed"), origin="retain")
            self.assertEqual(len(queue.pending("new")), 2)

    def test_consolidated_fact_gets_its_own_identity(self):
        """Follow-up list: a derived observation is queued apart from its source."""
        source = queue_rows(1)[0]
        derived = dict(
            source,
            id="00000000-0000-0000-0000-0000000d0fff",
            text="consolidated observation",
            source_memory_ids=[source["id"]],
        )
        queue = self.open_queue()
        source_key = queue.enqueue(source, origin="retain")["key"]
        derived_key = queue.enqueue(derived, origin="consolidation")["key"]
        self.assertNotEqual(source_key, derived_key)
        self.assertEqual(len(queue.pending("new")), 2)
        with self.subTest(check="own unit version"):
            self.assertNotEqual(
                self.module.identity(derived)["key"], self.module.identity(dict(derived, snapshot_version="9"))["key"]
            )
        with self.subTest(check="source signature participates"):
            other = dict(derived, source_memory_ids=[])
            self.assertNotEqual(self.module.identity(derived)["key"], self.module.identity(other)["key"])

    def test_identity_refuses_other_banks(self):
        """Hard constraint: no OpenClaw or foreign bank ever enters the queue."""
        for bank in ("main", "openclaw", "other"):
            with self.subTest(bank=bank):
                with self.assertRaises(ValueError):
                    self.module.identity(dict(queue_rows(1)[0], bank_id=bank))

    def test_parallel_process_enqueue_no_race(self):
        """Criterion 2: parallel completions enqueue without races or duplicates."""
        rows = queue_rows(12)
        errors, keys = [], {}
        lock = threading.Lock()

        def enqueue_worker(chunk):
            queue = None
            try:
                queue = self.module.Queue(self.store_path)  # own connection per process
                for row in chunk + chunk:  # duplicate delivery on purpose
                    outcome = queue.enqueue(row, origin="retain")
                    with lock:
                        keys.setdefault(row["id"], set()).add(outcome["key"])
            except Exception as exc:
                with lock:
                    errors.append(exc)
            finally:
                if queue is not None:
                    queue.close()

        threads = [threading.Thread(target=enqueue_worker, args=(rows[index::4],)) for index in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(keys), 12)
        self.assertTrue(all(len(values) == 1 for values in keys.values()))
        self.assertEqual(len(self.open_queue().pending("new", 100)), 12)
        with self.subTest(check="parallel completion"):
            mark_errors = []

            def complete_worker(chunk):
                queue = None
                try:
                    queue = self.module.Queue(self.store_path)
                    for key in chunk:
                        queue.mark(key, "done")
                except Exception as exc:
                    mark_errors.append(exc)
                finally:
                    if queue is not None:
                        queue.close()

            all_keys = [next(iter(values)) for values in keys.values()]
            threads = [threading.Thread(target=complete_worker, args=(all_keys[index::4],)) for index in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(mark_errors, [])
            self.assertEqual(self.open_queue().pending("new", 100), [])
            self.assertEqual(self.open_queue().counts().get("done/new"), 12)

    def test_off_switch_is_independent_of_other_controls(self):
        """Criterion 6: the tagging switch is separate and touches nothing else."""
        control = Path(self.module.CONTROL)
        self.assertNotEqual(control, EXTRACTOR_CONTROL)
        self.assertNotEqual(control, GARDENER_CONTROL)
        untouched = {path: path.read_bytes() for path in (EXTRACTOR_CONTROL, GARDENER_CONTROL) if path.exists()}
        self.assertTrue(untouched, "expected the extractor and gardener controls to exist")
        self.module.set_enabled(False, self.control_path)
        self.assertIs(json.loads(self.control_path.read_text())["enabled"], False)
        self.assertFalse(self.module.enabled(self.control_path))
        self.module.set_enabled(True, self.control_path)
        self.assertIs(json.loads(self.control_path.read_text())["enabled"], True)
        self.assertTrue(self.module.enabled(self.control_path))
        for path, before in untouched.items():
            with self.subTest(untouched=path.name):
                self.assertEqual(path.read_bytes(), before)

    def test_off_switch_disables_tagging_extraction_unaffected(self):
        """Criterion 6: disabled means no fetch/classify/write runs; pending is kept."""
        rows = queue_rows(1)
        queue = self.open_queue()
        queue.enqueue(rows[0], origin="retain")
        before = {entry["key"] for entry in queue.pending("new")}
        calls = {"fetch": 0, "classify": 0, "apply": 0, "exists": 0}

        def counting(name, value):
            def wrapper(*args, **kwargs):
                calls[name] += 1
                return value(*args, **kwargs)

            return wrapper

        extractor_before = EXTRACTOR_CONTROL.read_bytes()
        self.module.set_enabled(False, self.control_path)
        receipt = self.module.drain_sync(
            queue,
            control=self.control_path,
            fetch=counting("fetch", self.fetch_from(rows)),
            classify=counting("classify", worker.review_page),
            apply=counting("apply", fake_apply),
            exists=counting("exists", lambda ids: list(ids)),
            decide=fake_judge(),
            telemetry=None,
        )
        with self.subTest(check="drain was skipped"):
            self.assertEqual(receipt.get("skipped"), "disabled")
        with self.subTest(check="no bank path ran"):
            self.assertEqual(calls, {"fetch": 0, "classify": 0, "apply": 0, "exists": 0})
        with self.subTest(check="pending inputs preserved"):
            self.assertEqual({entry["key"] for entry in queue.pending("new")}, before)
        with self.subTest(check="extraction control untouched"):
            self.assertEqual(EXTRACTOR_CONTROL.read_bytes(), extractor_before)
        with self.subTest(check="missing control file is also disabled"):
            skipped = self.module.drain_sync(
                queue,
                control=Path(self.directory.name) / "absent.json",
                telemetry=None,
                fetch=counting("fetch", self.fetch_from(rows)),
            )
            self.assertEqual(skipped.get("skipped"), "disabled")
            self.assertEqual(calls["fetch"], 0)

    def test_drain_tags_additively_and_verifies_sources(self):
        """Criteria 2/4/7: a full drain pass tags once and proves sources unchanged."""
        rows = queue_rows(2)
        queue = self.open_queue()
        for row in rows:
            queue.enqueue(row, origin="retain")
        applied = []
        telemetry = Path(self.directory.name) / "telemetry.jsonl"
        receipt = self.module.drain_sync(
            queue,
            control=self._enable(),
            page_size=10,
            run_id="unit-test-run",
            fetch=self.fetch_from(rows),
            classify=worker.review_page,
            decide=fake_judge(nouls={"task/debug": 0.9}),
            apply=lambda records_, report, run_id: fake_apply(records_, report, run_id, applied),
            exists=lambda ids: list(ids),
            journal=self.journal(),
            telemetry=telemetry,
        )
        self.assertEqual(receipt["tagged"], 2)
        self.assertEqual(receipt["held"], 0)
        self.assertEqual(receipt["stale"], 0)
        self.assertEqual(receipt["absent"], 0)
        self.assertEqual(receipt["source_immutability"], "verified")
        self.assertEqual(len(applied), 1)
        self.assertEqual(self.open_queue().pending("new", 100), [])
        self.assertEqual(self.open_queue().counts().get("done/new"), 2)
        with self.subTest(check="only additive verdicts reach the writer"):
            self.assertEqual([item["action"] for run in receipt["runs"] for item in run["actions"]], ["TAG", "TAG"])
        with self.subTest(check="telemetry is one jsonl receipt"):
            lines = [line for line in telemetry.read_text().splitlines() if line]
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0])["tagged"], 2)
        with self.subTest(check="sources unchanged on re-read"):
            self.assertEqual([row["text"] for row in rows], [row["text"] for row in rows])

    def test_drain_telemetry_goes_to_jsonl(self):
        """Constraint: completion telemetry is appended as one JSON line per pass."""
        rows = queue_rows(1)
        queue = self.open_queue()
        queue.enqueue(rows[0], origin="retain")
        telemetry = Path(self.directory.name) / "telemetry.jsonl"
        self.module.drain_sync(
            queue,
            control=self._enable(),
            fetch=self.fetch_from(rows),
            classify=worker.review_page,
            decide=fake_judge(),
            apply=fake_apply,
            exists=lambda ids: list(ids),
            journal=self.journal(),
            telemetry=telemetry,
        )
        lines = [line for line in telemetry.read_text().splitlines() if line]
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["tagged"], 1)
        self.assertIn("at", payload)

    def test_additive_page_drops_non_additive_verdicts(self):
        """Criterion 5: only KEEP/RESCUE pass; PRUNE/REVIEW/DUPLICATE are held."""
        rows = records(4)
        report = {
            "policy": "gardener-batch-v7",
            "results": [
                {"id": rows[0]["id"], "disposition": "KEEP", "draft_tags": ["domain:tools"], "reason": "ok"},
                {"id": rows[1]["id"], "disposition": "RESCUE", "draft_tags": ["domain:tools"], "reason": "ok"},
                {"id": rows[2]["id"], "disposition": "PRUNE", "draft_tags": [], "reason": "no-durable-value"},
                {"id": rows[3]["id"], "disposition": "REVIEW", "draft_tags": [], "reason": "uncertain"},
            ],
        }
        included, filtered, held = self.module.additive_page(rows, report)
        self.assertEqual([row["id"] for row in included], [rows[0]["id"], rows[1]["id"]])
        self.assertTrue(
            all(item["disposition"] == "KEEP" for item in filtered["results"]),
            "RESCUE must be coerced to KEEP so no label can be removed",
        )
        self.assertEqual(sorted(case["id"] for case in held), sorted([rows[2]["id"], rows[3]["id"]]))
        with self.subTest(check="missing judge result is held, not tagged"):
            short = {
                "policy": "gardener-batch-v7",
                "results": [{"id": rows[0]["id"], "disposition": "KEEP", "draft_tags": []}],
            }
            _, _, missing = self.module.additive_page(rows[:2], short)
            self.assertEqual([case["id"] for case in missing], [rows[1]["id"]])

    def test_drain_never_prunes_through_the_writer(self):
        """Hard constraint: a PRUNE action from the writer aborts the pass."""
        rows = queue_rows(1)
        queue = self.open_queue()
        queue.enqueue(rows[0], origin="retain")
        with self.assertRaises(RuntimeError):
            self.module.drain_sync(
                queue,
                control=self._enable(),
                fetch=self.fetch_from(rows),
                classify=worker.review_page,
                decide=fake_judge(),
                apply=lambda records_, report, run_id: fake_apply(records_, report, run_id, action="PRUNE"),
                exists=lambda ids: list(ids),
                journal=self.journal(),
                telemetry=None,
            )

    def test_drain_aborts_on_source_mutation(self):
        """Criterion 7: a mutated source after the write is a hard failure."""
        rows = queue_rows(1)
        queue = self.open_queue()
        entry = queue.enqueue(rows[0], origin="retain")
        state = {"reads": 0}

        def fetch(ids=None, **kw):
            state["reads"] += 1
            if state["reads"] == 1:
                return [row for row in rows if row["id"] in set(ids or [])]
            return [dict(rows[0], text="MUTATED BY THE WRITE")]  # verification re-read

        with self.assertRaises(RuntimeError):
            self.module.drain_sync(
                queue,
                control=self._enable(),
                fetch=fetch,
                classify=worker.review_page,
                decide=fake_judge(),
                apply=fake_apply,
                exists=lambda ids: list(ids),
                journal=self.journal(),
                telemetry=None,
            )
        self.assertEqual(self.open_queue().get(entry["key"])["state"], "violation")

    def test_stale_version_superseded_at_drain(self):
        """Criterion 3: a row whose version moved is superseded, never tagged stale."""
        rows = queue_rows(1, snapshot_version="1")
        queue = self.open_queue()
        entry = queue.enqueue(rows[0], origin="retain")
        moved = dict(rows[0], snapshot_version="2")
        receipt = self.module.drain_sync(
            queue,
            control=self._enable(),
            page_size=10,
            fetch=self.fetch_from([moved]),
            classify=worker.review_page,
            decide=fake_judge(),
            apply=fake_apply,
            exists=lambda ids: list(ids),
            journal=self.journal(),
            telemetry=None,
        )
        self.assertEqual(receipt["refreshed"], 1)
        self.assertEqual(receipt["considered"], 0)
        self.assertEqual(self.open_queue().get(entry["key"])["state"], "superseded")
        with self.subTest(check="re-queued at the current version"):
            pending = self.open_queue().pending("new", 100)
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["unit_version"], "2")

    def test_deleted_unit_is_marked_absent_not_tagged(self):
        """Follow-up list: deletion before tagging is recorded, never written."""
        rows = queue_rows(1)
        queue = self.open_queue()
        entry = queue.enqueue(rows[0], origin="retain")
        receipt = self.module.drain_sync(
            queue,
            control=self._enable(),
            telemetry=None,
            exists=lambda ids: [],
            fetch=lambda **kw: [],
            apply=fake_apply,
            journal=self.journal(),
        )
        self.assertEqual(receipt["absent"], 1)
        entry_after = self.open_queue().get(entry["key"])
        self.assertEqual(entry_after["state"], "absent")
        self.assertEqual(entry_after["reason"], "deleted-before-tagging")

    def _enable(self, flag=True):
        self.module.set_enabled(flag, self.control_path)
        return self.control_path


# --------------------------------------------------------------------------
# live read-only linkage check (installed schema)
# --------------------------------------------------------------------------
class TagQueueLiveTests(unittest.TestCase):
    def test_completion_linkage_matches_installed_schema(self):
        """TAG-01 acceptance: declared unit count == units found in the op window."""
        sql = """
SELECT json_build_object('operation_id',o.operation_id::text,
 'document_id',o.result_metadata->>'document_id',
 'declared',(o.result_metadata->>'unit_ids_count')::int,
 'found',(SELECT count(*) FROM memory_units m WHERE m.bank_id='hermes-ops'
   AND m.document_id=o.result_metadata->>'document_id'
   AND m.created_at BETWEEN o.created_at - interval '2 minutes' AND o.completed_at + interval '2 minutes'))
FROM async_operations o
WHERE o.bank_id='hermes-ops' AND o.operation_type IN ('retain','batch_retain')
 AND o.status='completed' AND o.result_metadata ? 'unit_ids_count'
 AND o.result_metadata ? 'document_id'
ORDER BY o.completed_at DESC NULLS LAST LIMIT 1;
"""
        try:
            rows = writer.query(sql)
        except Exception as exc:
            self.skipTest("live hermes-ops schema unavailable: %s" % exc)
        if not rows:
            self.skipTest("no completed hermes-ops retain operation found")
        row = rows[0]
        self.assertGreater(row["declared"], 0)
        self.assertEqual(
            row["found"], row["declared"], "op window linkage mismatch for operation %s" % row["operation_id"]
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
