"""Durable post-extraction fact/version tagging queue for the hermes-ops bank.

Runs immediately after ACTUAL extraction completion: completed retain /
batch_retain operations are reconciled to the stored units they produced,
delayed consolidation is detected from observation_history, and each fact is
classified individually against its immutable source context by the existing
gardener v7 classifier and written by the existing version-checked gardener
writer (no classification or writing logic is forked here).

Hard guarantees (contract: reports/tag-01-implementation-brief-20261007.md):

* Additive only. Only KEEP/RESCUE verdicts reach the writer, and RESCUE is
  coerced to KEEP, so the writer can emit TAG or HOLD but never PRUNE, never
  remove a label and never edit a source. gardener_repair is never imported.
  Every write is followed by a source re-read that asserts text,
  source_memory_ids, document_id and chunk_id are unchanged and that legacy
  and confidence labels survive.
* Separate off switch at reports/tagger-control-20261007.json, independent of
  the extractor switch and of jev_gate.mode. A missing file means disabled.
  While disabled the queue still records pending facts but performs no
  classification and no write, preserving pending inputs for later.
* Durable, deduped queue (sqlite, WAL) keyed on the full identity: bank + unit
  id + text hash + source/document identity + unit version (xmin) + policy and
  registry version. Resume after a crash re-reads the same file.
* Separate cursors for new completions and historical backfill.

No OpenClaw bank (main, openclaw) is ever read or written.
"""

import argparse
import asyncio
import datetime
import hashlib
import importlib.util
import json
import sqlite3
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gardener_batch_v7 as worker
import gardener_writer as writer

BANK = "hermes-ops"
EXCLUDED_BANKS = {"main", "openclaw"}
ROOT = Path("/mnt/i/hermes")
QUEUE_DIR = ROOT / "reports/tag-queue-20261007"
QUEUE_DB = QUEUE_DIR / "queue.sqlite"
JOURNAL_DB = QUEUE_DIR / "judge.sqlite"
TELEMETRY = QUEUE_DIR / "telemetry.jsonl"
CONTROL = ROOT / "reports/tagger-control-20261007.json"
ADDITIVE = ("KEEP", "RESCUE")
PAGE_LIMIT = 100
WINDOW_MINUTES = 2

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS queue (
 key TEXT PRIMARY KEY, bank TEXT NOT NULL CHECK(bank='hermes-ops'),
 unit_id TEXT NOT NULL, text_hash TEXT NOT NULL, document_id TEXT, chunk_id TEXT,
 source_signature TEXT NOT NULL, unit_version TEXT NOT NULL, policy_version TEXT NOT NULL,
 origin TEXT NOT NULL, completion_key TEXT, cursor TEXT NOT NULL,
 state TEXT NOT NULL, reason TEXT, attempts INTEGER NOT NULL DEFAULT 0,
 run_id TEXT, enqueued_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS queue_pending ON queue(state,cursor,enqueued_at)",
    "CREATE INDEX IF NOT EXISTS queue_unit ON queue(unit_id)",
    """CREATE TABLE IF NOT EXISTS completions (
 completion_key TEXT PRIMARY KEY, bank TEXT NOT NULL, origin TEXT NOT NULL, document_id TEXT,
 declared INTEGER, found INTEGER, units INTEGER NOT NULL, processed_at TEXT NOT NULL)""",
    "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
)


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


def text_hash(text):
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def policy_version():
    """Versioned, not immutable: stamp the classifier policy and registry."""
    registry = {"tasks": worker.TASKS, "values": worker.VALUES, "domains": worker.DOMAINS, "targets": worker.TARGETS}
    return worker.POLICY + "/" + digest(registry)[:12]


def source_signature(row):
    return digest(
        {
            "document_id": row.get("document_id"),
            "chunk_id": row.get("chunk_id"),
            "source_memory_ids": sorted(str(value) for value in (row.get("source_memory_ids") or [])),
        }
    )


def identity(row, version=None):
    """The full dedup identity. Refuses any bank that is not hermes-ops."""
    if row.get("bank_id") != BANK:
        raise ValueError("Bank refused")
    unit_id = str(uuid.UUID(str(row["id"])))
    snapshot = row.get("snapshot_version", row.get("unit_version"))
    parts = {
        "bank": BANK,
        "unit_id": unit_id,
        "text_hash": text_hash(row.get("text")),
        "source_signature": source_signature(row),
        "unit_version": str(snapshot),
        "policy_version": version or policy_version(),
    }
    return dict(parts, key=digest(parts))


# --------------------------------------------------------------------------- control


def enabled(control=None):
    path = Path(control or CONTROL)
    if not path.exists():
        return False
    try:
        return json.loads(path.read_text()).get("enabled") is True
    except (ValueError, OSError):
        return False


def set_enabled(on, control=None):
    path = Path(control or CONTROL)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"enabled": bool(on), "bank": BANK, "updated_at": now()}, indent=2))
    return enabled(path)


# --------------------------------------------------------------------------- queue


class Queue:
    def __init__(self, path=QUEUE_DB):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=30000")
        for statement in SCHEMA:
            self.db.execute(statement)
        self.db.commit()

    def close(self):
        self.db.close()

    def meta_get(self, key, default=None):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def meta_set(self, key, value):
        with self.db:
            self.db.execute(
                "INSERT INTO meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value)
            )

    def enqueue(self, row, origin="manual", cursor="new", version=None, completion_key=None):
        record = identity(row, version)
        if self.db.execute("SELECT 1 FROM queue WHERE key=?", (record["key"],)).fetchone():
            return dict(record, enqueued=False)
        stamp = now()
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO queue (key,bank,unit_id,text_hash,document_id,"
                "chunk_id,source_signature,unit_version,policy_version,origin,completion_key,cursor,"
                "state,reason,attempts,enqueued_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    record["key"],
                    BANK,
                    record["unit_id"],
                    record["text_hash"],
                    row.get("document_id"),
                    row.get("chunk_id"),
                    record["source_signature"],
                    record["unit_version"],
                    record["policy_version"],
                    origin,
                    completion_key,
                    cursor,
                    "pending",
                    None,
                    0,
                    stamp,
                    stamp,
                ),
            )
        return dict(record, enqueued=True)

    def pending(self, cursor="new", limit=PAGE_LIMIT):
        rows = self.db.execute(
            "SELECT * FROM queue WHERE state=? AND cursor=? ORDER BY enqueued_at,key LIMIT ?",
            ("pending", cursor, min(max(1, limit), PAGE_LIMIT)),
        ).fetchall()
        return [dict(row) for row in rows]

    def entries(self, state=None, cursor=None, limit=100000):
        sql = "SELECT * FROM queue WHERE 1=1"
        params = []
        if state is not None:
            sql += " AND state=?"
            params.append(state)
        if cursor is not None:
            sql += " AND cursor=?"
            params.append(cursor)
        sql += " ORDER BY enqueued_at,key LIMIT ?"
        params.append(limit)
        return [dict(row) for row in self.db.execute(sql, params).fetchall()]

    def get(self, key):
        row = self.db.execute("SELECT * FROM queue WHERE key=?", (key,)).fetchone()
        return dict(row) if row else None

    def mark(self, key, state, reason=None, run_id=None, bump_attempts=False):
        with self.db:
            self.db.execute(
                "UPDATE queue SET state=?,reason=?,run_id=COALESCE(?,run_id),"
                "attempts=attempts+(CASE WHEN ? THEN 1 ELSE 0 END),updated_at=? WHERE key=?",
                (state, reason, run_id, 1 if bump_attempts else 0, now(), key),
            )

    def counts(self):
        rows = self.db.execute("SELECT state,cursor,count(*) AS n FROM queue GROUP BY state,cursor").fetchall()
        return {row["state"] + "/" + row["cursor"]: row["n"] for row in rows}

    def record_completion(self, completion_key, origin, document_id, declared, found, units):
        with self.db:
            self.db.execute(
                "INSERT INTO completions (completion_key,bank,origin,document_id,declared,"
                "found,units,processed_at) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(completion_key) DO NOTHING",
                (completion_key, BANK, origin, document_id, declared, found, units, now()),
            )


# --------------------------------------------------------------------------- read-only DB helpers


def existing_ids(ids):
    checked = [str(uuid.UUID(str(key))) for key in ids]
    if not checked:
        return []
    listing = ",".join(writer.quote(key) + "::uuid" for key in checked)
    sql = (
        "BEGIN READ ONLY;SELECT json_build_object('id',id) FROM memory_units "
        "WHERE bank_id='hermes-ops' AND id IN (" + listing + ");ROLLBACK;"
    )
    return [row["id"] for row in writer.query(sql)]


def unit_identities(ids):
    checked = [str(uuid.UUID(str(key))) for key in ids]
    if not checked:
        return []
    listing = ",".join(writer.quote(key) + "::uuid" for key in checked)
    sql = (
        "BEGIN READ ONLY;SELECT json_build_object('id',id,'bank_id',bank_id,'text',text,"
        "'document_id',document_id,'chunk_id',chunk_id,'source_memory_ids',source_memory_ids,"
        "'snapshot_version',xmin::text) FROM memory_units WHERE bank_id='hermes-ops' AND id IN ("
        + listing
        + ");ROLLBACK;"
    )
    return writer.query(sql)


def completed_retains(since):
    sql = (
        "BEGIN READ ONLY;SELECT json_build_object('operation_id',operation_id,"
        "'operation_type',operation_type,'document_id',result_metadata->>'document_id',"
        "'declared',(result_metadata->>'unit_ids_count')::int,'created_at',created_at,"
        "'completed_at',completed_at) FROM async_operations WHERE bank_id='hermes-ops' "
        "AND status='completed' AND operation_type IN ('retain','batch_retain') "
        "AND result_metadata ? 'document_id' AND completed_at > "
        + writer.quote(since)
        + "::timestamptz ORDER BY completed_at;ROLLBACK;"
    )
    return writer.query(sql)


def units_for_document(document_id, created_at, completed_at):
    sql = (
        "BEGIN READ ONLY;SELECT json_build_object('id',id,'bank_id',bank_id,'text',text,"
        "'context',context,'fact_type',fact_type,'tags',tags,'metadata',metadata,'document_id',document_id,"
        "'chunk_id',chunk_id,'created_at',created_at,'proof_count',proof_count,"
        "'source_memory_ids',source_memory_ids,'snapshot_version',xmin::text) FROM memory_units "
        "WHERE bank_id='hermes-ops' AND document_id="
        + writer.quote(document_id)
        + " AND created_at BETWEEN "
        + writer.quote(created_at)
        + "::timestamptz - interval '"
        + str(WINDOW_MINUTES)
        + " minutes' AND "
        + writer.quote(completed_at)
        + "::timestamptz + interval '"
        + str(WINDOW_MINUTES)
        + " minutes' ORDER BY created_at,id;ROLLBACK;"
    )
    return writer.query(sql)


def consolidation_changes(since):
    sql = (
        "BEGIN READ ONLY;SELECT json_build_object('observation_id',observation_id,"
        "'changed_at',changed_at) FROM observation_history WHERE bank_id='hermes-ops' AND changed_at > "
        + writer.quote(since)
        + "::timestamptz ORDER BY changed_at,id;ROLLBACK;"
    )
    return writer.query(sql)


def backfill_page(cursor, limit):
    sql = (
        "BEGIN READ ONLY;SELECT json_build_object('id',id,'bank_id',bank_id,'text',text,"
        "'document_id',document_id,'chunk_id',chunk_id,'source_memory_ids',source_memory_ids,"
        "'snapshot_version',xmin::text,'created_at',created_at) FROM memory_units m WHERE bank_id='hermes-ops' "
        "AND NOT EXISTS (SELECT 1 FROM unnest(m.tags) AS tag WHERE tag LIKE 'task:%' OR tag LIKE 'target:%' "
        "OR tag LIKE 'value:%')"
    )
    if cursor:
        sql += (
            " AND (m.created_at,m.id) > ("
            + writer.quote(cursor["created_at"])
            + "::timestamptz,"
            + writer.quote(cursor["id"])
            + "::uuid)"
        )
    sql += " ORDER BY m.created_at,m.id LIMIT " + str(min(max(1, limit), PAGE_LIMIT)) + ";ROLLBACK;"
    return writer.query(sql)


# --------------------------------------------------------------------------- enqueue paths


def enqueue_new(queue, since=None, version=None):
    """Enqueue units produced by confirmed completed retain/batch_retain ops."""
    if since is None:
        since = queue.meta_get("retain_cursor")
        if since is None:
            since = now()
            queue.meta_set("retain_cursor", since)
    version = version or policy_version()
    receipt = {
        "path": "retain",
        "since": since,
        "operations": 0,
        "declared": 0,
        "found": 0,
        "enqueued": 0,
        "duplicates": 0,
        "mismatches": [],
    }
    for operation in completed_retains(since):
        completion_key = str(operation["operation_id"])
        rows = units_for_document(operation["document_id"], operation["created_at"], operation["completed_at"])
        declared = operation.get("declared")
        if declared is not None and declared != len(rows):
            receipt["mismatches"].append(
                {"document_id": operation["document_id"], "declared": declared, "found": len(rows)}
            )
        receipt["operations"] += 1
        receipt["declared"] += declared or 0
        receipt["found"] += len(rows)
        for row in rows:
            outcome = queue.enqueue(
                row, origin=operation["operation_type"], cursor="new", version=version, completion_key=completion_key
            )
            receipt["enqueued"] += int(outcome["enqueued"])
            receipt["duplicates"] += int(not outcome["enqueued"])
        queue.record_completion(
            completion_key, operation["operation_type"], operation["document_id"], declared, len(rows), len(rows)
        )
        completed_at = operation.get("completed_at") or operation.get("created_at")
        queue.meta_set("retain_cursor", completed_at)
    return receipt


def enqueue_consolidation(queue, since=None, version=None):
    """Delayed consolidation: observations revised after their retain completed.

    There is no operation-level consolidation signal in the installed schema, so
    this reads observation_history.changed_at. Flagged UNVERIFIED by the brief:
    confirm the exact signal in the live canary before depending on it.
    """
    if since is None:
        since = queue.meta_get("consolidation_cursor")
        if since is None:
            since = now()
            queue.meta_set("consolidation_cursor", since)
    version = version or policy_version()
    changes = consolidation_changes(since)
    ids = sorted({str(change["observation_id"]) for change in changes})
    present = existing_ids(ids) if ids else []
    rows = unit_identities(present) if present else []
    receipt = {
        "path": "consolidation",
        "since": since,
        "changes": len(changes),
        "candidates": len(ids),
        "present": len(present),
        "enqueued": 0,
        "duplicates": 0,
        "signal": "UNVERIFIED:observation_history.changed_at",
    }
    for row in rows:
        outcome = queue.enqueue(
            row,
            origin="consolidation",
            cursor="new",
            version=version,
            completion_key="consolidation:" + row["snapshot_version"],
        )
        receipt["enqueued"] += int(outcome["enqueued"])
        receipt["duplicates"] += int(not outcome["enqueued"])
    for change in changes:
        queue.meta_set("consolidation_cursor", change["changed_at"])
    return receipt


def enqueue_backfill(queue, page_size=PAGE_LIMIT, version=None):
    """Historical backfill on its own cursor so nightly volume cannot starve it."""
    version = version or policy_version()
    cursor = json.loads(queue.meta_get("backfill_cursor") or "null")
    rows = backfill_page(cursor, page_size)
    receipt = {"path": "backfill", "after": cursor, "found": len(rows), "enqueued": 0, "duplicates": 0}
    for row in rows:
        outcome = queue.enqueue(row, origin="backfill", cursor="backfill", version=version)
        receipt["enqueued"] += int(outcome["enqueued"])
        receipt["duplicates"] += int(not outcome["enqueued"])
    if rows:
        last = rows[-1]
        queue.meta_set("backfill_cursor", json.dumps({"created_at": str(last["created_at"]), "id": last["id"]}))
        receipt["cursor"] = {"created_at": str(last["created_at"]), "id": last["id"]}
    return receipt


def enqueue_manual(queue, unit_ids=(), document_id=None, version=None):
    """Operator/manual retains that never appear as a plugin completion."""
    version = version or policy_version()
    ids = [str(value) for value in unit_ids]
    if document_id:
        sql = (
            "BEGIN READ ONLY;SELECT json_build_object('id',id) FROM memory_units "
            "WHERE bank_id='hermes-ops' AND document_id=" + writer.quote(document_id) + ";ROLLBACK;"
        )
        ids.extend(row["id"] for row in writer.query(sql))
    present = existing_ids(sorted(set(ids)))
    rows = unit_identities(present)
    receipt = {"path": "manual", "requested": len(set(ids)), "present": len(present), "enqueued": 0, "duplicates": 0}
    for row in rows:
        outcome = queue.enqueue(row, origin="manual", cursor="new", version=version)
        receipt["enqueued"] += int(outcome["enqueued"])
        receipt["duplicates"] += int(not outcome["enqueued"])
    return receipt


# --------------------------------------------------------------------------- additive write path


def additive_page(records, report):
    """Keep only additive verdicts; coerce RESCUE to KEEP so no label is removed."""
    by_id = {row["id"]: row for row in records}
    included_records, included_results, held = [], [], []
    seen = set()
    for result in report.get("results", []):
        key = result["id"]
        seen.add(key)
        disposition = result.get("disposition")
        if disposition in ADDITIVE and key in by_id:
            result = dict(result, judge_disposition=disposition, disposition="KEEP")
            included_results.append(result)
            included_records.append(by_id[key])
        else:
            held.append({"id": key, "disposition": disposition, "reason": result.get("reason")})
    for row in records:
        if row["id"] not in seen:
            held.append({"id": row["id"], "disposition": "REVIEW", "reason": "missing-judge-result"})
    return included_records, {"policy": report.get("policy"), "results": included_results}, held


def verify_sources_unchanged(before, after):
    """Post-write assertion: sources byte-identical, legacy and confidence labels kept."""
    current = {row["id"]: row for row in after}
    violations = []
    for row in before:
        updated = current.get(row["id"])
        if updated is None:
            violations.append({"id": row["id"], "reason": "source-absent-after-write"})
            continue
        for field in ("text", "document_id", "chunk_id"):
            if updated.get(field) != row.get(field):
                violations.append({"id": row["id"], "reason": "source-changed", "field": field})
        before_sources = sorted(str(value) for value in (row.get("source_memory_ids") or []))
        after_sources = sorted(str(value) for value in (updated.get("source_memory_ids") or []))
        if before_sources != after_sources:
            violations.append({"id": row["id"], "reason": "source-memory-ids-changed"})
        old_tags = set(row.get("tags") or [])
        new_tags = set(updated.get("tags") or [])
        if not old_tags.issubset(new_tags):
            violations.append(
                {"id": row["id"], "reason": "legacy-label-removed", "removed": sorted(old_tags - new_tags)}
            )
        for tag in old_tags:
            if tag.startswith("confidence:") and tag not in new_tags:
                violations.append({"id": row["id"], "reason": "confidence-rewritten"})
    return violations


def _outcomes(queue, entries, result, run_id):
    actions = {item["id"]: item for item in result.get("actions", [])}
    held = {item["id"]: item for item in result.get("held", [])}
    receipt = {"tagged": 0, "held": 0, "stale": 0}
    by_key = {entry["unit_id"]: entry for entry in entries}
    for unit_id, entry in by_key.items():
        action = actions.get(unit_id)
        if action is not None and action.get("applied") and action.get("action") == "TAG":
            queue.mark(entry["key"], "done", reason="tagged", run_id=run_id, bump_attempts=True)
            receipt["tagged"] += 1
            continue
        if action is not None and action.get("blocked"):
            state = "stale" if action["blocked"] == "version-conflict" else "held"
            queue.mark(entry["key"], state, reason=action["blocked"], run_id=run_id, bump_attempts=True)
            receipt["stale" if state == "stale" else "held"] += 1
            continue
        reason = (held.get(unit_id) or {}).get("reason") or (action or {}).get("reason") or "no-outcome"
        queue.mark(entry["key"], "held", reason=reason, run_id=run_id, bump_attempts=True)
        receipt["held"] += 1
    return receipt


async def drain(
    queue,
    *,
    cursor="new",
    page_size=PAGE_LIMIT,
    max_pages=1,
    decide=None,
    model="test",
    journal=None,
    fetch=None,
    classify=None,
    apply=None,
    exists=None,
    run_id=None,
    telemetry=TELEMETRY,
    max_bytes=100000,
    concurrency=2,
    control=None,
):
    """Classify and additively tag pending facts, one bounded page at a time."""
    if not enabled(control):
        return {"cursor": cursor, "skipped": "disabled", "pages": 0}
    page_size = min(max(1, page_size), PAGE_LIMIT)
    fetch = fetch or worker.fetch_page
    classify = classify or worker.review_page
    apply = apply or writer.apply
    exists = exists or existing_ids
    if journal is None:
        JOURNAL_DB.parent.mkdir(parents=True, exist_ok=True)
        journal = worker.Journal(JOURNAL_DB)
        owns_journal = True
    else:
        owns_journal = False
    receipt = {
        "cursor": cursor,
        "pages": 0,
        "considered": 0,
        "tagged": 0,
        "held": 0,
        "stale": 0,
        "absent": 0,
        "refreshed": 0,
        "source_immutability": "verified",
        "runs": [],
        "held_cases": [],
    }
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    try:
        for page in range(max(1, max_pages)):
            entries = queue.pending(cursor, page_size)
            if not entries:
                break
            run = run_id or ("tag-" + stamp + "-p%04d" % (page + 1))
            present = set(exists([entry["unit_id"] for entry in entries]))
            absent = [entry for entry in entries if entry["unit_id"] not in present]
            for entry in absent:
                queue.mark(entry["key"], "absent", reason="deleted-before-tagging", run_id=run)
            receipt["absent"] += len(absent)
            live = [entry for entry in entries if entry["unit_id"] in present]
            if not live:
                receipt["pages"] += 1
                continue
            rows = fetch(ids=[entry["unit_id"] for entry in live])
            by_id = {row["id"]: row for row in rows}
            eligible, refreshed = [], []
            for entry in live:
                row = by_id[entry["unit_id"]]
                current = identity(row)
                if current["key"] != entry["key"]:
                    refreshed.append((entry, row, current))
                else:
                    eligible.append((entry, row))
            for entry, row, current in refreshed:
                queue.mark(entry["key"], "superseded", reason="version-or-context-changed", run_id=run)
                queue.enqueue(
                    row, origin=entry["origin"], cursor=entry["cursor"], completion_key=entry["completion_key"]
                )
            receipt["refreshed"] += len(refreshed)
            receipt["considered"] += len(eligible)
            if not eligible:
                receipt["pages"] += 1
                continue
            records = [row for entry, row in eligible]
            report = await classify(records, decide, model, journal, max_bytes=max_bytes, concurrency=concurrency)
            included_records, filtered, held_cases = additive_page(records, report)
            for case in held_cases:
                entry = next(entry for entry, row in eligible if row["id"] == case["id"])
                queue.mark(
                    entry["key"], "held", reason=case["reason"] or case["disposition"], run_id=run, bump_attempts=True
                )
                receipt["held"] += 1
                receipt["held_cases"].append(case)
            if included_records:
                result = apply(included_records, filtered, run)
                prunes = [item for item in result.get("actions", []) if item.get("action") == "PRUNE"]
                if prunes:
                    raise RuntimeError("Refusing prune action from an additive tag queue")
                included_ids = {row["id"] for row in included_records}
                entries_by_id = {row["id"]: entry for entry, row in eligible if row["id"] in included_ids}
                outcomes = _outcomes(queue, list(entries_by_id.values()), result, run)
                receipt["tagged"] += outcomes["tagged"]
                receipt["held"] += outcomes["held"]
                receipt["stale"] += outcomes["stale"]
                applied = [
                    item["id"]
                    for item in result.get("actions", [])
                    if item.get("applied") and item.get("action") == "TAG"
                ]
                if applied:
                    after = fetch(ids=applied)
                    before = [row for entry, row in eligible if row["id"] in set(applied)]
                    violations = verify_sources_unchanged(before, after)
                    if violations:
                        for entry in entries_by_id.values():
                            if entry["unit_id"] in set(applied):
                                queue.mark(entry["key"], "violation", reason="source-immutability", run_id=run)
                        receipt["source_immutability"] = "violation"
                        raise RuntimeError("Source immutability violated: " + json.dumps(violations))
                receipt["runs"].append(
                    {
                        "run_id": run,
                        "considered": len(included_records),
                        "actions": result.get("actions", []),
                        "held": result.get("held", []),
                    }
                )
            receipt["pages"] += 1
    finally:
        if owns_journal:
            journal.db.close()
    if telemetry:
        Path(telemetry).parent.mkdir(parents=True, exist_ok=True)
        with open(telemetry, "a", encoding="ascii", errors="replace") as handle:
            handle.write(json.dumps(dict(receipt, at=now()), ensure_ascii=True) + "\n")
    return receipt


def drain_sync(queue, **kwargs):
    return asyncio.run(drain(queue, **kwargs))


def build_decide():
    """Judge wiring copied from gardener_run.py; caller owns the secret scope."""
    sys.path.insert(0, "/home/vern/.hermes/hermes-agent")
    plugin = Path("/home/vern/.hermes/plugins/jev_gate")
    spec = importlib.util.spec_from_file_location(
        "gardener_jev", plugin / "__init__.py", submodule_search_locations=[str(plugin)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["gardener_jev"] = module
    spec.loader.exec_module(module)
    from gardener_jev import client, constants

    cfg = dict(constants.DEFAULTS)
    cfg["judge_timeout_seconds"] = 45
    cfg["max_state_chars"] = 100000

    def decide(state, questions):
        return client.decide(state, questions, cfg)

    return decide, cfg


def reconcile(queue, version=None):
    """Re-queue the current identity of stale/superseded entries; report deletions."""
    version = version or policy_version()
    entries = [entry for state in ("stale", "superseded", "absent") for entry in queue.entries(state=state)]
    present = set(existing_ids([entry["unit_id"] for entry in entries])) if entries else set()
    receipt = {"candidates": len(entries), "present": len(present), "requeued": 0, "deleted": 0}
    for entry in entries:
        if entry["unit_id"] not in present:
            queue.mark(entry["key"], "absent", reason="deleted-before-tagging")
            receipt["deleted"] += 1
            continue
        rows = unit_identities([entry["unit_id"]])
        for row in rows:
            outcome = queue.enqueue(row, origin=entry["origin"], cursor=entry["cursor"], version=version)
            if outcome["enqueued"]:
                queue.mark(entry["key"], "superseded", reason="reconciled")
                receipt["requeued"] += 1
    return receipt


def run_once(
    queue,
    *,
    max_pages=1,
    page_size=PAGE_LIMIT,
    include_backfill=False,
    since=None,
    consolidation_since=None,
    manual_unit_ids=(),
    manual_document=None,
    decide=None,
    telemetry=TELEMETRY,
    control=None,
    decide_factory=None,
):
    """One polling pass: enqueue completions, then drain when the switch is on."""
    receipt = {"at": now(), "enabled": enabled(control)}
    receipt["retain"] = enqueue_new(queue, since=since)
    receipt["consolidation"] = enqueue_consolidation(queue, since=consolidation_since)
    if manual_unit_ids or manual_document:
        receipt["manual"] = enqueue_manual(queue, unit_ids=manual_unit_ids, document_id=manual_document)
    if include_backfill:
        receipt["backfill"] = enqueue_backfill(queue, page_size=page_size)
    if not receipt["enabled"]:
        receipt["drain"] = {"skipped": "disabled"}
        return receipt
    if decide is None:
        factory = decide_factory or build_decide
        sys.path.insert(0, "/home/vern/.hermes/hermes-agent")
        from agent.secret_scope import build_profile_secret_scope, reset_secret_scope, set_secret_scope

        home = Path("/home/vern/.hermes")
        token = set_secret_scope(build_profile_secret_scope(home), profile_home=str(home))
        try:
            decide, cfg = factory()
            receipt["drain"] = asyncio.run(
                drain(
                    queue,
                    cursor="new",
                    page_size=page_size,
                    max_pages=max_pages,
                    decide=decide,
                    model=cfg["model"],
                    control=control,
                    telemetry=telemetry,
                )
            )
        finally:
            reset_secret_scope(token)
    else:
        receipt["drain"] = asyncio.run(
            drain(
                queue,
                cursor="new",
                page_size=page_size,
                max_pages=max_pages,
                decide=decide,
                control=control,
                telemetry=telemetry,
            )
        )
    if include_backfill:
        receipt["drain_backfill"] = asyncio.run(
            drain(
                queue,
                cursor="backfill",
                page_size=page_size,
                max_pages=max_pages,
                decide=decide,
                control=control,
                telemetry=telemetry,
            )
        )
    return receipt


# --------------------------------------------------------------------------- CLI


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--queue", type=Path, default=QUEUE_DB)
    parser.add_argument("--control", type=Path, default=CONTROL)
    parser.add_argument("--telemetry", type=Path, default=TELEMETRY)
    sub = parser.add_subparsers(dest="command", required=True)
    control = sub.add_parser("control", help="separate tagging off switch")
    control.add_argument("mode", choices=("on", "off", "status"))
    scan = sub.add_parser("enqueue", help="enqueue confirmed completions (manual or hook)")
    scan.add_argument("--since")
    scan.add_argument("--unit", action="append", default=[])
    scan.add_argument("--document")
    drain_parser = sub.add_parser("drain", help="classify and tag pending facts")
    drain_parser.add_argument("--cursor", choices=("new", "backfill"), default="new")
    drain_parser.add_argument("--max-pages", type=int, default=1)
    drain_parser.add_argument("--page-size", type=int, default=PAGE_LIMIT)
    run = sub.add_parser("run", help="one polling pass: enqueue then tag when enabled")
    run.add_argument("--max-pages", type=int, default=1)
    run.add_argument("--page-size", type=int, default=PAGE_LIMIT)
    run.add_argument("--no-backfill", action="store_true")
    run.add_argument("--since")
    backfill = sub.add_parser("backfill", help="enqueue historical facts on the backfill cursor")
    backfill.add_argument("--page-size", type=int, default=PAGE_LIMIT)
    sub.add_parser("status", help="queue state and switch")
    sub.add_parser("reconcile", help="re-queue stale/superseded entries")
    linkage = sub.add_parser("linkage", help="verify completion/document/unit linkage")
    linkage.add_argument("--since")
    linkage.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()
    queue = Queue(args.queue)
    try:
        if args.command == "control":
            if args.mode == "status":
                print(
                    json.dumps(
                        {"component": "tagger", "enabled": enabled(args.control), "control": str(args.control)},
                        indent=2,
                    )
                )
            else:
                print(
                    json.dumps(
                        {
                            "component": "tagger",
                            "enabled": set_enabled(args.mode == "on", args.control),
                            "control": str(args.control),
                        },
                        indent=2,
                    )
                )
        elif args.command == "enqueue":
            print(
                json.dumps(
                    {
                        "retain": enqueue_new(queue, since=args.since),
                        "manual": enqueue_manual(queue, unit_ids=args.unit, document_id=args.document),
                    },
                    indent=2,
                )
            )
        elif args.command == "backfill":
            print(json.dumps(enqueue_backfill(queue, page_size=args.page_size), indent=2))
        elif args.command == "drain":
            print(
                json.dumps(
                    drain_sync(
                        queue,
                        cursor=args.cursor,
                        page_size=args.page_size,
                        max_pages=args.max_pages,
                        control=args.control,
                        telemetry=args.telemetry,
                    ),
                    indent=2,
                )
            )
        elif args.command == "run":
            print(
                json.dumps(
                    run_once(
                        queue,
                        max_pages=args.max_pages,
                        page_size=args.page_size,
                        include_backfill=not args.no_backfill,
                        since=args.since,
                        control=args.control,
                        telemetry=args.telemetry,
                    ),
                    indent=2,
                )
            )
        elif args.command == "status":
            print(
                json.dumps(
                    {
                        "enabled": enabled(args.control),
                        "counts": queue.counts(),
                        "retain_cursor": queue.meta_get("retain_cursor"),
                        "consolidation_cursor": queue.meta_get("consolidation_cursor"),
                        "backfill_cursor": queue.meta_get("backfill_cursor"),
                        "policy_version": policy_version(),
                    },
                    indent=2,
                )
            )
        elif args.command == "reconcile":
            print(json.dumps(reconcile(queue), indent=2))
        elif args.command == "linkage":
            since = args.since or queue.meta_get("retain_cursor") or now()
            checks = []
            for operation in completed_retains(since)[-max(1, args.limit) :]:
                rows = units_for_document(operation["document_id"], operation["created_at"], operation["completed_at"])
                checks.append(
                    {
                        "operation_id": str(operation["operation_id"]),
                        "operation_type": operation["operation_type"],
                        "document_id": operation["document_id"],
                        "declared": operation.get("declared"),
                        "found": len(rows),
                        "match": operation.get("declared") == len(rows),
                    }
                )
            print(
                json.dumps(
                    {"since": since, "checks": checks, "all_match": all(check["match"] for check in checks)}, indent=2
                )
            )
    finally:
        queue.close()


if __name__ == "__main__":
    main()
