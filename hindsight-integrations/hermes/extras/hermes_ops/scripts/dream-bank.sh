#!/usr/bin/env bash
# dream-bank.sh -- nightly bank pass. Banks: hermes-ops, human-model.
# Never reads or writes main or openclaw. Never issues an HTTP DELETE.
# Default is the nightly path: execute only inside the caps. --dry-run deletes nothing.
# A missing 02:00 tar exits 0. A tool failure exits 1.
# Dry-run does not consolidate, flag, deduplicate, or require a backup.
# Automatic dedup requires matching complete content and source identity.
set -uo pipefail

DRY=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY=1 ;;
    --execute) DRY=0 ;;
    *) echo "aborted: unknown arg"; exit 1 ;;
  esac
done

DOCKER="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
API="http://host.docker.internal:8888"
LOG="/home/vern/.hermes/workspace/dream-bank-log.jsonl"
DEST="/mnt/i/hermes/backups"
TODAY="$(date +%Y%m%d)"
TAR="$DEST/hindsight-data-${TODAY}.tar.gz"

refuse_bank() {
  case "$1" in
    hermes-ops|human-model) ;;
    *) echo "aborted: bank refused" >&2; exit 1 ;;
  esac
  if [ "$1" = "main" ] || [ "$1" = "openclaw" ]; then
    echo "aborted: openclaw bank refused" >&2
    exit 1
  fi
}

psql_sql() {
  refuse_bank "$1"
  "$DOCKER" exec -i -e PGPASSWORD=hindsight hindsight-hindsight-1 \
    sh -c 'PATH=/home/hindsight/.pg0/installation/18.1.0/bin:$PATH psql -U hindsight -d hindsight -v ON_ERROR_STOP=1 -q -At'
}

victim_setup() {
  local bank="$1"
  refuse_bank "$bank"
  cat << SQL
BEGIN;
CREATE TEMP TABLE tmp_groups ON COMMIT DROP AS
WITH keyed AS (
  SELECT id, jsonb_build_object(
    'text', text, 'fact_type', fact_type, 'context', context,
    'document_id', document_id, 'chunk_id', chunk_id,
    'event_date', event_date, 'occurred_start', occurred_start,
    'occurred_end', occurred_end, 'metadata', metadata,
    'source_memory_ids', source_memory_ids,
    'observation_scopes', observation_scopes, 'attachment_ids', attachment_ids,
    'content_tags', ARRAY(SELECT t FROM unnest(tags) AS t
                          WHERE t NOT LIKE 'retain:%' ORDER BY t)
  ) AS identity
  FROM memory_units WHERE bank_id = '${bank}'
)
SELECT identity AS prefix, array_agg(id) AS all_ids
FROM keyed GROUP BY identity HAVING count(*) > 1;

CREATE TEMP TABLE tmp_ranked ON COMMIT DROP AS
SELECT m.id, g.prefix, g.all_ids,
       row_number() OVER (
         PARTITION BY g.prefix
         ORDER BY
           CASE WHEN m.tags && ARRAY['kind:preference','kind:correction','retain:permanent']::varchar[] THEN 0 ELSE 1 END,
           CASE m.fact_type WHEN 'world' THEN 1 WHEN 'experience' THEN 2 WHEN 'observation' THEN 3 ELSE 4 END,
           COALESCE(m.proof_count, 1) DESC,
           m.consolidated_at DESC NULLS LAST,
           m.created_at DESC, m.id
       ) AS rn
FROM memory_units m
JOIN tmp_groups g ON m.id = ANY(g.all_ids)
WHERE m.bank_id = '${bank}';

CREATE TEMP TABLE tmp_keepers ON COMMIT DROP AS
SELECT id AS keeper_id, all_ids FROM tmp_ranked WHERE rn = 1;

CREATE TEMP TABLE tmp_victims ON COMMIT DROP AS
SELECT v.victim_id, k.keeper_id
FROM tmp_keepers k
CROSS JOIN LATERAL unnest(k.all_ids) AS v(victim_id)
WHERE v.victim_id <> k.keeper_id
  AND NOT EXISTS (
    SELECT 1 FROM memory_units m
    WHERE m.id = v.victim_id
      AND (
        (m.fact_type = 'world' AND COALESCE(m.proof_count, 1) > 1)
        OR m.tags && ARRAY['kind:preference','kind:correction','retain:permanent']::varchar[]
      )
  )
  AND NOT EXISTS (
    SELECT 1 FROM memory_links ml
    WHERE ml.link_type IN ('caused_by', 'causes')
      AND (ml.from_unit_id = v.victim_id OR ml.to_unit_id = v.victim_id)
  )
  AND NOT EXISTS (
    SELECT 1 FROM memory_units src
    WHERE src.source_memory_ids && ARRAY[v.victim_id]
  );
SQL
}

count_nodes() {
  local bank="$1"
  printf 'SELECT count(*) FROM memory_units WHERE bank_id = %s;\n' "'$bank'" | psql_sql "$bank" | tr -d '[:space:]'
}

count_victims() {
  local bank="$1"
  {
    victim_setup "$bank"
    echo "SELECT count(*) FROM tmp_victims;"
    echo "ROLLBACK;"
  } | psql_sql "$bank"
}

flag_bank() {
  # Flags nodes decay:prune-candidate and returns "<count>|<json reason tally>".
  # Every flagged node also gets flag:reason:<code> so the reason travels with the node
  # and is visible in the Hindsight UI.
  local bank="$1"
  {
    cat << SQL
BEGIN;
WITH rules AS (
  SELECT id,
    CASE
      WHEN text ~* 'hook-probe' THEN 'telemetry'
      WHEN text ~* 'Memory stored successfully' THEN 'ack-text'
      WHEN text ~* '(^|\\n)(Assistant |User )(planned|requested|instructed|asked|offered|proposed|will|intends|decided|attempted|explained|summarized|confirmed|provided|recommended|completed|failed)'
        OR text ~* 'The assistant' THEN 'agent-narration'
      ELSE 'stale-mention'
    END AS reason
  FROM memory_units
  WHERE bank_id = '${bank}'
    AND mentioned_at > now() - interval '24 hours'
    AND fact_type IN ('experience', 'observation')
    AND 'decay:prune-candidate' <> ALL(tags)
    AND NOT (tags && ARRAY['kind:preference','kind:correction','retain:permanent']::varchar[])
    AND (
      text ~* '(^|\\n)(Assistant |User )(planned|requested|instructed|asked|offered|proposed|will|intends|decided|attempted|explained|summarized|confirmed|provided|recommended|completed|failed)'
      OR text ~* 'The assistant'
      OR text ~* 'hook-probe'
      OR text ~* 'Memory stored successfully'
    )
),
flagged AS (
  UPDATE memory_units m
  SET tags = array_append(array_append(tags, 'decay:prune-candidate'),
                          'flag:reason:' || r.reason)
  FROM rules r
  WHERE m.id = r.id
  RETURNING r.reason
)
SELECT (SELECT count(*) FROM flagged) || '|' ||
  COALESCE((SELECT json_object_agg(reason, n) FROM
    (SELECT reason, count(*) AS n FROM flagged GROUP BY reason) t), '{}');
COMMIT;
SQL
  } | psql_sql "$bank"
}

run_dedup() {
  local bank="$1"
  local abs_cap="$2"
  local batch="$3"
  case "$batch" in
    ''|*[!0-9]*) echo "dream-bank: bad batch" >&2; return 1 ;;
  esac
  {
    victim_setup "$bank"
    cat << SQL
CREATE TEMP TABLE tmp_batch ON COMMIT DROP AS
SELECT tv.victim_id, tv.keeper_id
FROM tmp_victims tv
JOIN memory_units m ON m.id = tv.victim_id AND m.bank_id = '${bank}'
ORDER BY m.mentioned_at ASC NULLS FIRST, tv.victim_id
LIMIT ${batch};

DO \$\$
DECLARE n int; nodes int; pct int; abs_cap int := ${abs_cap}; batch int := ${batch};
BEGIN
  SELECT count(*) INTO n FROM tmp_batch;
  SELECT count(*) INTO nodes FROM memory_units WHERE bank_id = '${bank}';
  pct := floor(nodes * 0.05);
  IF n > batch OR n > abs_cap OR n > pct THEN
    RAISE EXCEPTION 'batch breach n=% batch=% pct=% abs=%', n, batch, pct, abs_cap;
  END IF;
END \$\$;

INSERT INTO unit_entities (unit_id, entity_id)
SELECT DISTINCT tv.keeper_id, ue.entity_id
FROM tmp_batch tv
JOIN unit_entities ue ON ue.unit_id = tv.victim_id
JOIN entities e ON e.id = ue.entity_id
ON CONFLICT DO NOTHING;

INSERT INTO memory_links (from_unit_id, to_unit_id, link_type, entity_id, weight, created_at, bank_id)
SELECT DISTINCT tv.keeper_id, ml.to_unit_id, ml.link_type, ml.entity_id, ml.weight, ml.created_at, ml.bank_id
FROM tmp_batch tv
JOIN memory_links ml ON ml.from_unit_id = tv.victim_id
WHERE ml.link_type IN ('semantic', 'entity', 'temporal')
  AND ml.to_unit_id <> tv.keeper_id
  AND ml.bank_id = '${bank}'
  AND NOT EXISTS (SELECT 1 FROM tmp_batch o WHERE o.victim_id = ml.to_unit_id)
ON CONFLICT DO NOTHING;

INSERT INTO memory_links (from_unit_id, to_unit_id, link_type, entity_id, weight, created_at, bank_id)
SELECT DISTINCT ml.from_unit_id, tv.keeper_id, ml.link_type, ml.entity_id, ml.weight, ml.created_at, ml.bank_id
FROM tmp_batch tv
JOIN memory_links ml ON ml.to_unit_id = tv.victim_id
WHERE ml.link_type IN ('semantic', 'entity', 'temporal')
  AND ml.from_unit_id <> tv.keeper_id
  AND ml.bank_id = '${bank}'
  AND NOT EXISTS (SELECT 1 FROM tmp_batch o WHERE o.victim_id = ml.from_unit_id)
ON CONFLICT DO NOTHING;

UPDATE memory_units m
SET source_memory_ids = ARRAY(
      SELECT DISTINCT sid FROM (
        SELECT unnest(COALESCE(m.source_memory_ids, ARRAY[]::uuid[])) AS sid
        UNION ALL
        SELECT unnest(COALESCE(v.source_memory_ids, ARRAY[]::uuid[]))
        FROM tmp_batch tv JOIN memory_units v ON v.id = tv.victim_id
        WHERE tv.keeper_id = m.id
      ) actual_sources WHERE sid <> m.id ORDER BY sid
    ),
    tags = array_cat(
      m.tags,
      (SELECT COALESCE(array_agg(DISTINCT vt), ARRAY[]::character varying[])
       FROM (
         SELECT unnest(v.tags) AS vt
         FROM memory_units v
         JOIN tmp_batch tv2 ON v.id = tv2.victim_id
         WHERE tv2.keeper_id = m.id
       ) x
       WHERE vt LIKE 'retain:%' AND vt <> ALL(m.tags))
    )
WHERE m.bank_id = '${bank}'
  AND EXISTS (SELECT 1 FROM tmp_batch tv WHERE tv.keeper_id = m.id);

WITH deleted AS (
  DELETE FROM memory_units WHERE bank_id = '${bank}'
    AND id IN (SELECT victim_id FROM tmp_batch) RETURNING id
) SELECT count(*) FROM deleted;
COMMIT;
SQL
  } | psql_sql "$bank"
}

write_log() {
  python3 - "$LOG" "$1" << 'PY'
import json, sys
path, payload = sys.argv[1:]
row = json.loads(payload)
with open(path, "a", encoding="utf-8") as fh:
    fh.write(json.dumps(row, separators=(",", ":")) + "\n")
PY
}

# Sourcing exposes functions for isolated tests; it never runs the bank pass.
if [ "${BASH_SOURCE[0]}" != "$0" ]; then return 0; fi

if [ "$DRY" -eq 0 ] && [ ! -s "$TAR" ]; then
  write_log "{\"ts\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"aborted\":\"no fresh backup\",\"backup\":\"$TAR\",\"banks\":{}}"
  echo "dream-bank: aborted: no fresh backup"
  exit 0
fi
tar_day=""
if [ "$DRY" -eq 0 ]; then
  tar_day="$(date -d "@$(stat -c %Y "$TAR")" +%Y%m%d)"
fi
if [ "$DRY" -eq 0 ] && [ "$tar_day" != "$TODAY" ]; then
  write_log "{\"ts\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"aborted\":\"no fresh backup\",\"backup\":\"$TAR\",\"banks\":{}}"
  echo "dream-bank: aborted: no fresh backup"
  exit 0
fi

live_runs=0
if [ -f "$LOG" ]; then
  live_runs="$(python3 - "$LOG" << 'PY'
import json, sys
n = 0
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    line = line.strip()
    if not line:
        continue
    try:
        row = json.loads(line)
    except json.JSONDecodeError:
        continue
    if row.get("live") is True:
        n += 1
print(n)
PY
)"
fi
ABS_CAP=50
if [ "$live_runs" -ge 3 ]; then
  ABS_CAP=1000000
fi
case "$ABS_CAP" in
  ''|*[!0-9]*) echo "aborted: bad cap"; exit 1 ;;
esac

declare -A NODES_BEFORE PENDING VICTIMS BATCH DEDUPED REMAINING FLAGGED FLAGREASON ABORTED NODES_AFTER
SUMMARY=""
ANY_ABORT=""

for bank in hermes-ops human-model; do
  refuse_bank "$bank"
  st="$(curl -sS -m 30 "$API/v1/default/banks/${bank}/stats")" || {
    echo "dream-bank: stats failed for $bank"
    exit 1
  }
  nodes="$(count_nodes "$bank")"
  case "$nodes" in
    ''|*[!0-9]*) echo "dream-bank: bad node count for $bank: $nodes"; exit 1 ;;
  esac
  pending="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1]).get("pending_consolidation",-1))' "$st")"
  NODES_BEFORE[$bank]="$nodes"
  PENDING[$bank]="$pending"
  NODES_AFTER[$bank]="$nodes"
  DEDUPED[$bank]=0
  BATCH[$bank]=0
  REMAINING[$bank]=0
  FLAGGED[$bank]=0
  FLAGREASON[$bank]="{}"
  ABORTED[$bank]=""
  VICTIMS[$bank]=0

  if [ "$DRY" -eq 0 ] && [ "$pending" -gt 0 ]; then
    code="$(curl -sS -m 120 -o /tmp/dream-bank-consolidate.json -w '%{http_code}' -X POST "$API/v1/default/banks/${bank}/consolidate" || echo err)"
    if [ "$code" != "200" ] && [ "$code" != "202" ]; then
      ABORTED[$bank]="consolidate-http-${code}"
      ANY_ABORT="consolidate"
      SUMMARY="${SUMMARY}${bank} aborted: consolidate-http-${code}; "
      continue
    fi
    sleep 30
  fi

  victims="$(count_victims "$bank")" || {
    echo "dream-bank: count failed for $bank"
    exit 1
  }
  victims="$(printf '%s' "$victims" | tr -d '[:space:]')"
  case "$victims" in
    ''|*[!0-9]*) echo "dream-bank: bad victim count for $bank: $victims"; exit 1 ;;
  esac
  VICTIMS[$bank]="$victims"
  pct_cap=$(( nodes * 5 / 100 ))
  batch="$victims"
  if [ "$batch" -gt "$pct_cap" ]; then batch="$pct_cap"; fi
  if [ "$batch" -gt "$ABS_CAP" ]; then batch="$ABS_CAP"; fi
  BATCH[$bank]="$batch"
  REMAINING[$bank]="$victims"
  if [ "$DRY" -eq 1 ] || [ "$batch" -eq 0 ]; then
    SUMMARY="${SUMMARY}${bank} deduped=0 batch=${batch} remaining=${victims}; "
    if [ "$DRY" -eq 0 ]; then
      flagout="$(flag_bank "$bank")" || flagout="0|{}"
      flagged="$(printf '%s' "$flagout" | cut -d'|' -f1 | tr -d '[:space:]')"
      case "$flagged" in
        ''|*[!0-9]*) flagged=0 ;;
      esac
      FLAGGED[$bank]="$flagged"
      FLAGREASON[$bank]="$(printf '%s' "$flagout" | cut -d'|' -f2-)"
    fi
    continue
  fi

  if ! dropped="$(run_dedup "$bank" "$ABS_CAP" "$batch")"; then
    echo "dream-bank: dedup failed for $bank"
    exit 1
  fi
  st_after="$(curl -sS -m 30 "$API/v1/default/banks/${bank}/stats")" || {
    echo "dream-bank: stats-after failed for $bank"
    exit 1
  }
  NODES_AFTER[$bank]="$(count_nodes "$bank")"
  dropped="$(printf '%s' "$dropped" | tr -d '[:space:]')"
  case "$dropped" in
    ''|*[!0-9]*) echo "dream-bank: bad delete count" >&2; exit 1 ;;
  esac
  DEDUPED[$bank]="$dropped"
  remaining="$(count_victims "$bank")" || exit 1
  remaining="$(printf '%s' "$remaining" | tr -d '[:space:]')"
  case "$remaining" in
    ''|*[!0-9]*) echo "dream-bank: bad remaining count" >&2; exit 1 ;;
  esac
  REMAINING[$bank]="$remaining"
  flagout="$(flag_bank "$bank")" || flagout="0|{}"
  flagged="$(printf '%s' "$flagout" | cut -d'|' -f1 | tr -d '[:space:]')"
  case "$flagged" in
    ''|*[!0-9]*) flagged=0 ;;
  esac
  FLAGGED[$bank]="$flagged"
  FLAGREASON[$bank]="$(printf '%s' "$flagout" | cut -d'|' -f2-)"
  SUMMARY="${SUMMARY}${bank} deduped=${dropped} remaining=${remaining}; "
done

python3 - "$LOG" << PY
import json, sys
from datetime import datetime, timezone
path = sys.argv[1]
row = {
    "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "live": ${DRY} == 0,
    "dry_run": ${DRY} == 1,
    "backup": "${TAR}",
    "abs_cap": ${ABS_CAP},
    "aborted": "${ANY_ABORT}" or None,
    "banks": {
        "hermes-ops": {
            "nodes_before": int("${NODES_BEFORE[hermes-ops]}"),
            "nodes_after": int("${NODES_AFTER[hermes-ops]}"),
            "pending": int("${PENDING[hermes-ops]}"),
            "safe_victims": int("${VICTIMS[hermes-ops]}"),
            "batch": int("${BATCH[hermes-ops]}"),
            "deduped": int("${DEDUPED[hermes-ops]}"),
            "remaining": int("${REMAINING[hermes-ops]}"),
            "flagged": int("${FLAGGED[hermes-ops]}"),
            "flag_reason": json.loads('${FLAGREASON[hermes-ops]}' or '{}'),
            "aborted": "${ABORTED[hermes-ops]}" or None,
        },
        "human-model": {
            "nodes_before": int("${NODES_BEFORE[human-model]}"),
            "nodes_after": int("${NODES_AFTER[human-model]}"),
            "pending": int("${PENDING[human-model]}"),
            "safe_victims": int("${VICTIMS[human-model]}"),
            "batch": int("${BATCH[human-model]}"),
            "deduped": int("${DEDUPED[human-model]}"),
            "remaining": int("${REMAINING[human-model]}"),
            "flagged": int("${FLAGGED[human-model]}"),
            "flag_reason": json.loads('${FLAGREASON[human-model]}' or '{}'),
            "aborted": "${ABORTED[human-model]}" or None,
        },
    },
}
# The live flag above is a bash comparison pasted as Python. Fix it explicitly.
row["live"] = $( [ "$DRY" -eq 0 ] && echo True || echo False )
row["dry_run"] = $( [ "$DRY" -eq 1 ] && echo True || echo False )
if not row["aborted"]:
    row["aborted"] = None
for b in row["banks"].values():
    if not b["aborted"]:
        b["aborted"] = None
with open(path, "a", encoding="utf-8") as fh:
    fh.write(json.dumps(row, separators=(",", ":")) + "\n")
PY

echo "dream-bank: ${SUMMARY}"

# Jev gardening uses only hermes-ops and its own exclusive lock and off switch.
# Detach from the short cron wrapper; persistent journals permit safe resumption.
if [ "$DRY" -eq 0 ] && [ -z "${ANY_ABORT}" ]; then
  nohup /home/vern/.hermes/hermes-agent/venv/bin/python /mnt/i/hermes/scripts/gardener_run.py --incremental --max-pages 2 >> /home/vern/.hermes/workspace/gardener-nightly.log 2>&1 < /dev/null &
fi
exit 0
