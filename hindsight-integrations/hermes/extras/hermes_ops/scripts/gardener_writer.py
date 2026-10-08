"""Version-checked hermes-ops writer with an atomic, reversible PostgreSQL journal."""

import hashlib
import json
import subprocess
import uuid
from pathlib import Path

from gardener_keeper import valid_proof

BANK = "hermes-ops"
PROTECTED = {"retain:permanent", "kind:decision", "kind:preference", "kind:correction"}
D = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
PSQL = [
    D,
    "exec",
    "-i",
    "-e",
    "PGPASSWORD=hindsight",
    "-e",
    "LD_LIBRARY_PATH=/home/hindsight/.pg0/installation/18.1.0/lib",
    "hindsight-hindsight-1",
    "/home/hindsight/.pg0/installation/18.1.0/bin/psql",
    "-h",
    "127.0.0.1",
    "-U",
    "hindsight",
    "-d",
    "hindsight",
    "-At",
    "-q",
    "-v",
    "ON_ERROR_STOP=1",
]


def query(sql):
    r = subprocess.run(PSQL, input=sql, text=True, capture_output=True, timeout=120)
    if r.returncode:
        raise RuntimeError(r.stderr)
    return [json.loads(line) for line in r.stdout.splitlines() if line.startswith("{")]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


DDL = """
CREATE TABLE IF NOT EXISTS hermes_garden_reviews (
 bank_id text NOT NULL CHECK(bank_id='hermes-ops'), memory_id uuid NOT NULL,
 run_id text NOT NULL, policy text NOT NULL, snapshot_hash text NOT NULL,
 recommendation jsonb NOT NULL, reviewed_at timestamptz NOT NULL DEFAULT now(),
 next_review_at timestamptz, PRIMARY KEY(bank_id,memory_id)
);

CREATE TABLE IF NOT EXISTS hermes_garden_journal (
 run_id text NOT NULL, memory_id uuid NOT NULL, bank_id text NOT NULL CHECK(bank_id='hermes-ops'),
 action text NOT NULL CHECK(action IN ('PRUNE','TAG','RESCUE')),
 reason text NOT NULL, before_row jsonb NOT NULL, links jsonb NOT NULL,
 unit_entities jsonb NOT NULL, observation_history jsonb NOT NULL,
 after_row jsonb, applied_at timestamptz NOT NULL DEFAULT now(), reverted_at timestamptz,
 PRIMARY KEY(run_id,memory_id)
);
"""


def plan(records, report, operator=None):
    if not 1 <= len(records) <= 100:
        raise ValueError("Page must contain 1-100 memories")
    source = {row["id"]: row for row in records}
    if len(source) != len(records) or set(source) != {row["id"] for row in report["results"]}:
        raise ValueError("Coverage mismatch")
    operator = operator or {}
    result = []
    for recommendation in report["results"]:
        row = source[recommendation["id"]]
        if row["bank_id"] != BANK:
            raise ValueError("Bank refused")
        identity = str(uuid.UUID(row["id"]))
        if recommendation.get("snapshot_version") != row["snapshot_version"]:
            raise ValueError("Report version mismatch")
        verdict = operator.get(identity, {}).get("disposition", recommendation["disposition"])
        reason = operator.get(identity, {}).get("reason", recommendation.get("reason", "review"))
        action = "HOLD"
        tags = list(row.get("tags") or [])
        if verdict == "PRUNE":
            action = "PRUNE"
        elif verdict in ("KEEP", "RESCUE"):
            # These defaults describe known origin and unverified factual state,
            # not the probability of the classification. Existing labels survive.
            defaults = ["kind:fact", "scope:hermes-ops", "confidence:provisional"]
            if row.get("document_id") and row.get("source_documents"):
                defaults.append("source:document:" + row["document_id"])
            elif row.get("source_facts"):
                defaults.append("source:derived")
            elif (row.get("metadata") or {}).get("session_id") or any(t.startswith("session:") for t in tags):
                defaults.append("source:session")
            for default in defaults:
                dimension = default.split(":", 1)[0] + ":"
                if not any(t.startswith(dimension) and len(t) > len(dimension) for t in tags):
                    tags.append(default)
            for draft in recommendation.get("draft_tags", []):
                if draft.startswith("domain:") and not any(t.startswith("domain:") and len(t) > 7 for t in tags):
                    tags.append(draft)
            dims = ("kind:", "scope:", "domain:", "confidence:", "source:")
            if all(any(t.startswith(d) for t in tags) for d in dims):
                tags = sorted(set(tags + recommendation.get("draft_tags", [])))
                action = "TAG"
                if verdict == "RESCUE":
                    tags = [t for t in tags if t not in ("decay:prune-candidate", "retain:tombstone")]
                    action = "RESCUE"
            else:
                reason = "missing-provenance-dimension"
            if action == "TAG" and set(tags) == set(row.get("tags") or []):
                action = "HOLD"
                reason = "no-label-change"
        elif verdict == "DUPLICATE":
            peer = next(
                (
                    peer
                    for peer in row.get("canonical_peers", [])
                    if peer["id"] == recommendation.get("keeper_id")
                    and peer["text"] == recommendation.get("keeper_text")
                ),
                None,
            )
            if peer and peer["id"] != identity and valid_proof(row, recommendation):
                action = "PRUNE"
                reason = "confirmed-duplicate:" + peer["id"]
            else:
                reason = "keeper-selection-and-text-only-proof-required"
        elif verdict == "REPAIR":
            reason = "source-supported-replacement-required"
        # Frozen/protected and graph guards are rechecked in the transaction.
        result.append(
            {
                "id": identity,
                "version": str(row["snapshot_version"]),
                "action": action,
                "tags": tags,
                "reason": reason,
                "verdict": verdict,
                "keeper_id": recommendation.get("keeper_id") if verdict == "DUPLICATE" else None,
                "keeper_text": recommendation.get("keeper_text") if verdict == "DUPLICATE" else None,
                "recommendation": recommendation,
                "policy": report.get("policy", "operator-reviewed"),
            }
        )
    return result


def apply_sql(items, run_id):
    if (
        not run_id
        or len(run_id) > 100
        or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for c in run_id)
    ):
        raise ValueError("Invalid run")
    payload = quote(json.dumps(items, ensure_ascii=True))
    return (
        "BEGIN;\nSET LOCAL lock_timeout='5s';\n"
        + DDL
        + """
CREATE TEMP TABLE garden_input ON COMMIT DROP AS
SELECT * FROM jsonb_to_recordset("""
        + payload
        + """::jsonb)
 AS x(id uuid,version text,action text,tags varchar[],reason text,verdict text,keeper_id uuid,keeper_text text,recommendation jsonb,policy text);
-- Lock before evaluating row versions and graph dependencies.
SELECT m.id FROM memory_units m JOIN garden_input p ON p.id=m.id
 WHERE m.bank_id='hermes-ops' ORDER BY m.id FOR UPDATE OF m;
CREATE TEMP TABLE garden_eligible ON COMMIT DROP AS
SELECT p.*,to_jsonb(m) AS original,
 CASE WHEN m.xmin::text<>p.version THEN 'version-conflict'
 WHEN p.keeper_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM memory_units k WHERE k.id=p.keeper_id AND k.bank_id='hermes-ops' AND k.fact_type='world' AND k.text=p.keeper_text) THEN 'keeper-changed-or-missing'
 WHEN p.action='PRUNE' AND m.fact_type='world' THEN 'protected-world'
 WHEN p.action='PRUNE' AND m.tags && ARRAY['retain:permanent','kind:decision','kind:preference','kind:correction']::varchar[] THEN 'protected-tag'
 WHEN p.action='PRUNE' AND EXISTS(SELECT 1 FROM memory_links l WHERE l.bank_id='hermes-ops' AND l.link_type IN ('causes','caused_by') AND (l.from_unit_id=m.id OR l.to_unit_id=m.id)) THEN 'causal-dependency'
 WHEN p.action='PRUNE' AND EXISTS(SELECT 1 FROM memory_units o WHERE o.bank_id='hermes-ops' AND o.source_memory_ids && ARRAY[m.id]) THEN 'source-dependency'
 ELSE NULL END AS blocked
FROM garden_input p JOIN memory_units m ON m.id=p.id AND m.bank_id='hermes-ops'
WHERE p.action<>'HOLD' AND NOT EXISTS(SELECT 1 FROM hermes_garden_journal j WHERE j.run_id="""
        + quote(run_id)
        + """ AND j.memory_id=p.id);
INSERT INTO hermes_garden_journal(run_id,memory_id,bank_id,action,reason,before_row,links,unit_entities,observation_history)
SELECT """
        + quote(run_id)
        + """,p.id,'hermes-ops',p.action,p.reason,p.original,
 COALESCE((SELECT jsonb_agg(to_jsonb(l)) FROM memory_links l WHERE l.bank_id='hermes-ops' AND (l.from_unit_id=p.id OR l.to_unit_id=p.id)),'[]'::jsonb),
 COALESCE((SELECT jsonb_agg(to_jsonb(u)) FROM unit_entities u WHERE u.unit_id=p.id),'[]'::jsonb),
 COALESCE((SELECT jsonb_agg(to_jsonb(h)) FROM observation_history h WHERE h.bank_id='hermes-ops' AND h.observation_id=p.id),'[]'::jsonb)
FROM garden_eligible p WHERE blocked IS NULL;
UPDATE memory_units m SET tags=p.tags,updated_at=now()
FROM garden_eligible p WHERE m.id=p.id AND m.bank_id='hermes-ops' AND p.blocked IS NULL AND p.action IN ('TAG','RESCUE');
DELETE FROM observation_history h USING garden_eligible p
 WHERE h.bank_id='hermes-ops' AND h.observation_id=p.id AND p.blocked IS NULL AND p.action='PRUNE';
DELETE FROM memory_units m USING garden_eligible p
 WHERE m.id=p.id AND m.bank_id='hermes-ops' AND p.blocked IS NULL AND p.action='PRUNE';
UPDATE hermes_garden_journal j SET after_row=(SELECT to_jsonb(m) FROM memory_units m WHERE m.id=j.memory_id AND m.bank_id='hermes-ops')
 WHERE j.run_id="""
        + quote(run_id)
        + """ AND j.memory_id IN (SELECT id FROM garden_eligible WHERE blocked IS NULL);
INSERT INTO hermes_garden_reviews(bank_id,memory_id,run_id,policy,snapshot_hash,recommendation,next_review_at)
SELECT 'hermes-ops',p.id,"""
        + quote(run_id)
        + """,p.policy,md5(to_jsonb(m)::text),p.recommendation,
 CASE WHEN p.reason IN ('ValueError','judge_unavailable_transport','judge_unavailable_timeout') THEN now()+interval '1 day' ELSE NULL END
FROM garden_input p JOIN memory_units m ON m.id=p.id AND m.bank_id='hermes-ops'
WHERE (m.xmin::text=p.version OR EXISTS(SELECT 1 FROM hermes_garden_journal j WHERE j.run_id="""
        + quote(run_id)
        + """ AND j.memory_id=p.id AND j.after_row=to_jsonb(m)))
ON CONFLICT(bank_id,memory_id) DO UPDATE SET run_id=excluded.run_id,policy=excluded.policy,snapshot_hash=excluded.snapshot_hash,recommendation=excluded.recommendation,reviewed_at=now(),next_review_at=excluded.next_review_at;
SELECT json_build_object('id',p.id,'action',p.action,'blocked',p.blocked,'applied',p.blocked IS NULL) FROM garden_eligible p ORDER BY p.id;
COMMIT;
"""
    )


def apply(records, report, run_id, operator=None):
    items = plan(records, report, operator)
    output = query(apply_sql(items, run_id))
    return {
        "run_id": run_id,
        "bank": BANK,
        "considered": len(items),
        "actions": output,
        "held": [p for p in items if p["action"] == "HOLD"],
        "committed": True,
    }


def rollback_sql(run_id, memory_ids=None):
    restriction = ""
    if memory_ids is not None:
        if not memory_ids:
            raise ValueError("Empty rollback selection")
        restriction = (
            " AND j.memory_id IN (" + ",".join(quote(str(uuid.UUID(key))) + "::uuid" for key in memory_ids) + ")"
        )
    return (
        "BEGIN;\n"
        + DDL
        + """
-- Rows changed since gardening stay untouched; deleted IDs must still be absent.
CREATE TEMP TABLE garden_restore ON COMMIT DROP AS
SELECT j.* FROM hermes_garden_journal j LEFT JOIN memory_units m ON m.id=j.memory_id
 WHERE j.bank_id='hermes-ops' AND j.run_id="""
        + quote(run_id)
        + """ AND j.reverted_at IS NULL"""
        + restriction
        + """
 AND ((j.action='PRUNE' AND m.id IS NULL) OR (j.action IN ('TAG','RESCUE') AND to_jsonb(m)=j.after_row));
DO $restore$
DECLARE columns text; selections text;
BEGIN
 SELECT string_agg(quote_ident(attname),',' ORDER BY attnum),string_agg('m.'||quote_ident(attname),',' ORDER BY attnum) INTO columns,selections FROM pg_attribute
 WHERE attrelid='memory_units'::regclass AND attnum>0 AND NOT attisdropped AND attgenerated='' AND attidentity='';
 EXECUTE 'INSERT INTO memory_units ('||columns||') SELECT '||selections||' FROM garden_restore j CROSS JOIN LATERAL jsonb_populate_record(NULL::memory_units,j.before_row) m WHERE j.action=''PRUNE''';
END $restore$;
UPDATE memory_units m SET tags=x.tags,updated_at=x.updated_at
 FROM garden_restore j CROSS JOIN LATERAL jsonb_populate_record(NULL::memory_units,j.before_row) x
 WHERE j.action IN ('TAG','RESCUE') AND m.id=j.memory_id AND m.bank_id='hermes-ops';
INSERT INTO unit_entities SELECT u.* FROM garden_restore j CROSS JOIN LATERAL jsonb_populate_recordset(NULL::unit_entities,j.unit_entities) u ON CONFLICT DO NOTHING;
INSERT INTO memory_links SELECT l.* FROM garden_restore j CROSS JOIN LATERAL jsonb_populate_recordset(NULL::memory_links,j.links) l
 WHERE EXISTS(SELECT 1 FROM memory_units a WHERE a.id=l.from_unit_id) AND EXISTS(SELECT 1 FROM memory_units b WHERE b.id=l.to_unit_id) ON CONFLICT DO NOTHING;
INSERT INTO observation_history SELECT h.* FROM garden_restore j CROSS JOIN LATERAL jsonb_populate_recordset(NULL::observation_history,j.observation_history) h ON CONFLICT DO NOTHING;
UPDATE hermes_garden_journal SET reverted_at=now() WHERE run_id="""
        + quote(run_id)
        + """ AND bank_id='hermes-ops' AND memory_id IN (SELECT memory_id FROM garden_restore);
SELECT json_build_object('run_id',"""
        + quote(run_id)
        + """,'restored',count(*)) FROM garden_restore;
COMMIT;
"""
    )
