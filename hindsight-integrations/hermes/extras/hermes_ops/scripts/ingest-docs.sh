#!/bin/bash
# =============================================================================
# ingest-docs.sh -- hermes-agent documentation -> Hindsight ingestion pipeline
#
# Versioned + replace-on-update. One retain item per doc page section group, all
# tagged with the doc-set identity `docset:hermes-agent-docs-<version-tag>`.
#
# SET IDENTITY IS THE TAG, not a document id (kanban t_9bfe1701): a retain POST
# that reuses an existing `document_id` REPLACES that whole document in
# Hindsight 0.9.2, so the previous "one document id per version" design kept only
# the last POST's items. ingest-docs.py now gives each page-part its own
# document_id and never writes one twice per run.
#
# Usage:
#   ingest-docs.sh <version-tag> [--limit N] [--dry-run] [--force]
#   ingest-docs.sh --verify <version-tag>
#   ingest-docs.sh --purge <version-tag>
#   ingest-docs.sh --list-versions
#
# Modes:
#   (default)        parse -> tag -> retain (idempotent per page; re-run resumes,
#                    and a design change re-posts every page)
#   --dry-run        parse only, print item/domain counts, retain nothing
#   --limit N        only the first N pages (smoke test)
#   --force          psql-purge the doc set first, then ingest from scratch
#   --verify         psql counts + recall probe + gates against the parse state
#   --purge          psql delete of the doc set BY TAG (units + derived
#                    observations + document rows). HARD RULE: never HTTP DELETE.
#   --list-versions  list doc sets present in the bank (for the version tick)
#
# Replacement lifecycle (install version changes):
#   ingest-docs.sh --purge <old-tag> && ingest-docs.sh <new-tag>
#
# Exit codes: 0 ok, 1 verify/failed-items failure, 2 usage, 3 preflight.
#
# Hard rules baked in: no HTTP DELETE against Hindsight (psql only); no config
# writes; no gateway restarts; nothing outside the state/report files below.
# =============================================================================
set -u

VERSION_TAG=""
MODE="ingest"
LIMIT=0
DRY_RUN=0
FORCE=0

while [ $# -gt 0 ]; do
  case "$1" in
    --verify) MODE="verify" ;;
    --purge) MODE="purge" ;;
    --list-versions) MODE="list" ;;
    --dry-run) DRY_RUN=1 ;;
    --force) FORCE=1 ;;
    --limit) shift; LIMIT="${1:-0}" ;;
    --limit=*) LIMIT="${1#*=}" ;;
    -h|--help) sed -n '2,42p' "$0"; exit 0 ;;
    -*) echo "unknown flag: $1" >&2; exit 2 ;;
    *) VERSION_TAG="$1" ;;
  esac
  shift
done

DOCS_ROOT="/home/vern/.hermes/hermes-agent/website/docs"
CHUNKS_DIR="/mnt/i/hermes/knowledge/upstream-docs-chunks"
BANK="hermes-ops"
HINDSIGHT_URL="${HINDSIGHT_URL:-http://host.docker.internal:8888}"
PY="/mnt/i/hermes/scripts/ingest-docs.py"
BACKUP_DIR="/mnt/i/hermes/backups"
LOG="$BACKUP_DIR/ingest-docs.log"
DKR_WIN="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
PG_VERSION_DIR="/home/hindsight/.pg0/installation/18.1.0/bin"

# Verify gates (kanban t_9bfe1701 item d). ratios are percent of the parse.
MIN_UNIT_RATIO=80      # retained raw units >= 80% of parsed chunk items
MIN_PAGE_RATIO=90      # pages represented >= 90% of parsed pages
MIN_FULL_SET_PAGES=100 # below this the set is a --limit smoke: probe gates are skipped

# Resolve the docker CLI + the container that serves this Hindsight URL.
docker_cli() {
  if [ -x "$DKR_WIN" ]; then echo "$DKR_WIN"; return 0; fi
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then echo docker; return 0; fi
  return 1
}
container_for_url() {
  case "$HINDSIGHT_URL" in
    *:8889*) echo "hindsight2-hindsight-1" ;;
    *:8888*) echo "hindsight-hindsight-1" ;;
    *) echo "${HINDSIGHT_CONTAINER:-hindsight-hindsight-1}" ;;
  esac
}
DKR="$(docker_cli || true)"
CT="$(container_for_url)"

PSQL_BIN="PATH=$PG_VERSION_DIR:\$PATH psql -U hindsight -d hindsight -A -F\"|\" -t"
sql() {  # sql "<one-line sql>" ; prints rows or empty on failure
  [ -n "$DKR" ] || { echo "NO_DOCKER"; return 1; }
  "$DKR" exec -i -e PGPASSWORD=hindsight -e SQL_TEXT="$1" "$CT" \
    sh -c "$PSQL_BIN -c \"\$SQL_TEXT\"" 2>&1
}

mkdir -p "$BACKUP_DIR"
exec > >(tee -a "$LOG") 2>&1
echo "=== ingest-docs.sh $(date -Iseconds) mode=$MODE version=${VERSION_TAG:-none} ==="

if [ "$MODE" = "list" ]; then
  echo "--- doc sets in bank $BANK (identity = docset: tag) ---"
  sql "SELECT t AS docset, count(*) AS units FROM memory_units u, unnest(u.tags) t WHERE u.bank_id='$BANK' AND t LIKE 'docset:hermes-agent-docs-%' GROUP BY 1 ORDER BY 1;"
  echo "--- document rows per docset prefix ---"
  sql "SELECT regexp_replace(id, '^(hermes-agent-docs-[A-Za-z0-9._]+?)(-p[0-9]+)?-.*$', '\\1') AS docset, count(*) FROM documents WHERE bank_id='$BANK' AND id LIKE 'hermes-agent-docs-%' GROUP BY 1 ORDER BY 1;"
  exit 0
fi

if [ -z "$VERSION_TAG" ]; then
  echo "usage: ingest-docs.sh <version-tag> [--limit N] [--dry-run] [--force] | --verify <tag> | --purge <tag> | --list-versions" >&2
  exit 2
fi

DOC_ID="hermes-agent-docs-$VERSION_TAG"
STATE="$BACKUP_DIR/ingest-docs-$VERSION_TAG.state.json"

# ---------------------------------------------------------------- preflight
echo "--- preflight ---"
HEALTH=$(curl -s -m 15 "$HINDSIGHT_URL/health" || true)
echo "health: ${HEALTH:-unreachable}"
case "$HEALTH" in
  *'"status":"healthy"'*) : ;;
  *) echo "FATAL: Hindsight not healthy at $HINDSIGHT_URL" >&2; exit 3 ;;
esac
if [ -z "$DKR" ]; then
  echo "WARN: no docker CLI reachable -- psql steps (purge/verify counts) unavailable"
else
  echo "docker: $DKR ; container: $CT"
fi
echo "docs_root: $DOCS_ROOT ; docset_tag: docset:$DOC_ID ; state: $STATE"

# ------------------------------------------------------------------- purge
purge_docset() {  # purge_docset <tag>  -- one psql session, tag-scoped CTE
  local tag="$1"; local did="hermes-agent-docs-$tag"
  echo "--- psql purge of docset:$did (tag-scoped units + observations + document rows) ---"
  echo "HARD RULE: psql only, never the HTTP DELETE endpoint."
  echo "before: units_tagged=$(sql "SELECT count(*) FROM memory_units WHERE bank_id='$BANK' AND 'docset:$did' = ANY(tags);") documents=$(sql "SELECT count(*) FROM documents WHERE bank_id='$BANK' AND (id='$did' OR id LIKE '$did-%');") chunks=$(sql "SELECT count(*) FROM chunks WHERE bank_id='$BANK' AND (document_id='$did' OR document_id LIKE '$did-%');")"
  local sql_text="WITH purged AS (SELECT id FROM memory_units WHERE bank_id='$BANK' AND ('docset:$did' = ANY(tags) OR document_id='$did' OR document_id LIKE '$did-%')), d_obs AS (DELETE FROM memory_units WHERE bank_id='$BANK' AND fact_type='observation' AND (source_memory_ids && (SELECT coalesce(array_agg(id),'{}'::uuid[]) FROM purged) OR 'version:$tag' = ANY(tags) OR 'docset:$did' = ANY(tags)) RETURNING 1), d_units AS (DELETE FROM memory_units WHERE bank_id='$BANK' AND ('docset:$did' = ANY(tags) OR document_id='$did' OR document_id LIKE '$did-%') RETURNING 1), d_chunks AS (DELETE FROM chunks WHERE bank_id='$BANK' AND (document_id='$did' OR document_id LIKE '$did-%') RETURNING 1), d_docs AS (DELETE FROM documents WHERE bank_id='$BANK' AND (id='$did' OR id LIKE '$did-%') RETURNING 1) SELECT 'observations_deleted|' || (SELECT count(*) FROM d_obs) || ' units_deleted|' || (SELECT count(*) FROM d_units) || ' chunks_deleted|' || (SELECT count(*) FROM d_chunks) || ' documents_deleted|' || (SELECT count(*) FROM d_docs);"
  local out
  out=$(sql "$sql_text")
  echo "$out"
  case "$out" in
    *observations_deleted*) : ;;
    *) echo "WARN: purge did not return counts (see line above)" ;;
  esac
  echo "after: units_tagged=$(sql "SELECT count(*) FROM memory_units WHERE bank_id='$BANK' AND 'docset:$did' = ANY(tags);") units_docidprefix=$(sql "SELECT count(*) FROM memory_units WHERE bank_id='$BANK' AND (document_id='$did' OR document_id LIKE '$did-%');") documents=$(sql "SELECT count(*) FROM documents WHERE bank_id='$BANK' AND (id='$did' OR id LIKE '$did-%');") chunks=$(sql "SELECT count(*) FROM chunks WHERE bank_id='$BANK' AND (document_id='$did' OR document_id LIKE '$did-%');") observations_tagged=$(sql "SELECT count(*) FROM memory_units WHERE bank_id='$BANK' AND fact_type='observation' AND 'version:$tag' = ANY(tags);")"
}

# -------------------------------------------------------------------- verify
verify_docset() {  # verify_docset <tag>
  local tag="$1"; local did="hermes-agent-docs-$tag"; local rc=0
  local state_json
  state_json=$(python3 "$PY" --version "$tag" --state "$STATE" --report)
  local chunks_lines pages_total failed_items pages_failed retried
  chunks_lines=$(printf '%s' "$state_json" | jq -r '.chunks_lines // 0')
  pages_total=$(printf '%s' "$state_json" | jq -r '.pages_total // 0')
  failed_items=$(printf '%s' "$state_json" | jq -r '.failed_items // 0')
  pages_failed=$(printf '%s' "$state_json" | jq -r '.pages_failed // 0')
  retried=$(printf '%s' "$state_json" | jq -r '.retried_batches // 0')

  echo "--- verify: psql counts (set identity = tag docset:$did) ---"
  local docs
  docs=$(sql "SELECT count(*) FROM documents WHERE bank_id='$BANK' AND (id='$did' OR id LIKE '$did-%');")
  echo "documents_row: ${docs:-unavailable} (per-page-part documents)"
  [ "${docs:-0}" -ge 1 ] 2>/dev/null || { echo "GATE FAIL documents_row < 1"; rc=1; }

  echo "units_by_fact_type:"
  sql "SELECT fact_type, count(*) FROM memory_units WHERE bank_id='$BANK' AND 'docset:$did' = ANY(tags) GROUP BY 1 ORDER BY 2 DESC;"
  echo "units_by_domain (top 12):"
  sql "SELECT t, count(*) FROM memory_units u, unnest(u.tags) t WHERE u.bank_id='$BANK' AND 'docset:$did' = ANY(u.tags) AND t LIKE 'domain:%' GROUP BY 1 ORDER BY 2 DESC LIMIT 12;"
  echo "units_by_source (top 10):"
  sql "SELECT t, count(*) FROM memory_units u, unnest(u.tags) t WHERE u.bank_id='$BANK' AND 'docset:$did' = ANY(u.tags) AND t LIKE 'source:%' GROUP BY 1 ORDER BY 2 DESC LIMIT 10;"

  local raw distinct_domains pages_covered tg messaging
  raw=$(sql "SELECT count(*) FROM memory_units WHERE bank_id='$BANK' AND fact_type IN ('world','experience') AND 'docset:$did' = ANY(tags);")
  raw=${raw:-0}
  distinct_domains=$(sql "SELECT count(DISTINCT t) FROM memory_units u, unnest(u.tags) t WHERE u.bank_id='$BANK' AND 'docset:$did' = ANY(u.tags) AND t LIKE 'domain:%';")
  pages_covered=$(sql "SELECT count(DISTINCT t) FROM memory_units u, unnest(u.tags) t WHERE u.bank_id='$BANK' AND fact_type IN ('world','experience') AND 'docset:$did' = ANY(u.tags) AND t LIKE 'source:docs/%';")
  tg=$(sql "SELECT count(*) FROM memory_units WHERE bank_id='$BANK' AND fact_type IN ('world','experience') AND 'source:docs/user-guide/messaging/telegram.md' = ANY(tags);")
  messaging=$(sql "SELECT count(*) FROM memory_units WHERE bank_id='$BANK' AND fact_type IN ('world','experience') AND EXISTS (SELECT 1 FROM unnest(tags) st WHERE st LIKE 'source:docs/user-guide/messaging/%');")
  pages_covered=${pages_covered:-0}; tg=${tg:-0}; messaging=${messaging:-0}
  echo "units_total_raw: $raw (parsed chunk items: $chunks_lines)"
  echo "distinct_domain_tags: ${distinct_domains:-0}"
  echo "pages_covered: $pages_covered (parsed pages: $pages_total)"
  echo "messaging_units: $messaging ; telegram_units: $tg"
  echo "derived observations carrying version:$tag (or docset:$did):"
  sql "SELECT count(*) FROM memory_units WHERE bank_id='$BANK' AND fact_type='observation' AND ('version:$tag' = ANY(tags) OR 'docset:$did' = ANY(tags));"

  # gates: the old "units > 0" check could not see the last-POST-only defect
  local min_units min_pages
  min_units=$(awk -v n="$chunks_lines" -v r="$MIN_UNIT_RATIO" 'BEGIN{printf "%d", (n*r)/100}')
  min_pages=$(awk -v n="$pages_total" -v r="$MIN_PAGE_RATIO" 'BEGIN{printf "%d", (n*r)/100}')
  if [ "$chunks_lines" -gt 0 ] 2>/dev/null; then
    if [ "$raw" -lt "$min_units" ] 2>/dev/null; then
      echo "GATE FAIL units_total_raw $raw < $min_units (${MIN_UNIT_RATIO}% of $chunks_lines chunk items)"; rc=1
    fi
  fi
  if [ "$pages_total" -gt 0 ] 2>/dev/null; then
    if [ "$pages_covered" -lt "$min_pages" ] 2>/dev/null; then
      echo "GATE FAIL pages_covered $pages_covered < $min_pages (${MIN_PAGE_RATIO}% of $pages_total parsed pages)"; rc=1
    fi
  fi
  [ "$tg" -ge 1 ] 2>/dev/null || { echo "GATE FAIL telegram_units < 1 (docs/user-guide/messaging/telegram.md absent)"; rc=1; }
  [ "$failed_items" = "0" ] || { echo "GATE FAIL failed_items=$failed_items"; rc=1; }
  [ "$pages_failed" = "0" ] || { echo "GATE FAIL pages_failed=$pages_failed"; rc=1; }
  echo "retried_batches: $retried (retry-on-500 path)"

  echo "--- verify: recall probe (raw fact layer, types=world+experience) ---"
  local probe='{"query":"what environment keys gate telegram group access","budget":"mid","max_tokens":1200,"types":["world","experience"]}'
  local resp
  resp=$(curl -s -m 120 -X POST "$HINDSIGHT_URL/v1/default/banks/$BANK/memories/recall" \
    -H 'Content-Type: application/json' -d "$probe")
  local hits thits
  hits=$(printf '%s' "$resp" | jq -r --arg d "docset:$did" '[.results[] | select((.tags // []) | index($d))] | length' 2>/dev/null)
  thits=$(printf '%s' "$resp" | jq -r '[.results[] | select((.tags // []) | index("source:docs/user-guide/messaging/telegram.md"))] | length' 2>/dev/null)
  echo "docset_hits_in_probe_top_results: ${hits:-0}"
  echo "telegram_source_hits_in_probe: ${thits:-0}"
  printf '%s' "$resp" | jq -r '.results[:5][] | "[\(.type)] doc=\(.document_id // "-") tags=\((.tags // []) | map(select(startswith("docset:") or startswith("source:"))) | join(",")) :: \((.text // "")[:180])"' 2>/dev/null
  if [ "$pages_total" -ge "$MIN_FULL_SET_PAGES" ] 2>/dev/null; then
    [ "${hits:-0}" -ge 1 ] 2>/dev/null || { echo "GATE FAIL probe returned no docset-tagged unit"; rc=1; }
    [ "${thits:-0}" -ge 1 ] 2>/dev/null || { echo "GATE FAIL probe returned no telegram.md unit"; rc=1; }
  else
    echo "NOTE: probe gates skipped (pages_total $pages_total < $MIN_FULL_SET_PAGES = --limit smoke)"
  fi

  echo "--- verify: state report ---"
  printf '%s\n' "$state_json"

  echo "--- verify: desk chunk artifact (same parse, other sink) ---"
  CHUNKS="$CHUNKS_DIR/$VERSION_TAG.jsonl"
  if [ -f "$CHUNKS" ]; then
    echo "chunks_file: $CHUNKS"
    echo "lines: $(wc -l < "$CHUNKS") ; bytes: $(wc -c < "$CHUNKS")"
    echo "domains_in_chunks:"
    jq -r '.domain' "$CHUNKS" 2>/dev/null | sort | uniq -c | sort -rn | head -12
    echo "first_sample:"; head -1 "$CHUNKS" | cut -c1-200
  else
    echo "chunks_file MISSING: $CHUNKS"; rc=1
  fi

  if [ "$rc" = "0" ]; then
    echo "VERIFY: PASS (docset:$did)"
  else
    echo "VERIFY: FAIL (docset:$did)"
  fi
  return $rc
}

# --------------------------------------------------------------- run modes
if [ "$MODE" = "purge" ]; then
  purge_docset "$VERSION_TAG"
  exit 0
fi

if [ "$MODE" = "verify" ]; then
  verify_docset "$VERSION_TAG"
  exit $?
fi

# ingest
if [ "$FORCE" = "1" ]; then
  echo "FORCE: purging existing doc set before re-ingest"
  purge_docset "$VERSION_TAG"
fi

ARGS=(--version "$VERSION_TAG" --docs-root "$DOCS_ROOT" --bank "$BANK" --url "$HINDSIGHT_URL" --state "$STATE")
[ "$LIMIT" -gt 0 ] 2>/dev/null && ARGS+=(--limit "$LIMIT")
[ "$DRY_RUN" = "1" ] && ARGS+=(--dry-run)

echo "--- parse + retain ---"
python3 "$PY" "${ARGS[@]}"
RC=$?
RC0=$RC
echo "python_rc=$RC"

if [ "$DRY_RUN" = "1" ]; then
  exit $RC
fi

# wrapper rc reflects the retainer's own failure accounting (t_9bfe1701 item c)
if [ -f "$STATE" ]; then
  FAILED_ITEMS=$(python3 "$PY" --version "$VERSION_TAG" --state "$STATE" --report | jq -r '.failed_items // 0')
  echo "state_failed_items=$FAILED_ITEMS"
  [ "$FAILED_ITEMS" = "0" ] || RC=1
fi

echo
verify_docset "$VERSION_TAG"
VRC=$?
[ "$VRC" = "0" ] || RC=1
echo "=== ingest-docs.sh finished $(date -Iseconds) rc=$RC (retainer=$RC0 verify=$VRC) ==="
exit $RC
