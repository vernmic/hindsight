#!/usr/bin/env bash
# Q8 database qualification: run the writer and queue suites against one owned,
# throwaway PostgreSQL container. No pull, no network, no published ports.
#
#   tests/run_db_qualification.sh            # run and remove the container
#   Q8_KEEP=1 tests/run_db_qualification.sh  # leave it for inspection
#
# The image must already be cached locally; --pull=never enforces that.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
OPS=$(cd "$HERE/.." && pwd)
DOCKER=${HINDSIGHT_DOCKER:-docker}
IMAGE=${Q8_IMAGE:-pgvector/pgvector:pg18}
NAME=${Q8_NAME:-"q8-ops-test-$(printf '%s' "$$$(date +%s)" | sha256sum | cut -c1-8)"}
PASS="q8-fixture-$(printf '%s' "$NAME$$" | sha256sum | cut -c1-16)"
DB="q8fixture"
ROLE="q8role"
PY=${HERMES_OPS_PYTHON:-python3}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/q8db.XXXXXX")
WS="$TMP/ws"
HOME_DIR="$TMP/home"
mkdir -p "$WS/reports" "$HOME_DIR/logs"

cleanup() {
  if [ "${Q8_KEEP:-0}" = "1" ]; then
    echo "Q8_KEEP=1: container $NAME left running for inspection"
    return
  fi
  "$DOCKER" rm -f "$NAME" >/dev/null 2>&1 && echo "removed owned container $NAME"
}
trap cleanup EXIT

if "$DOCKER" inspect "$NAME" >/dev/null 2>&1; then
  echo "== reuse existing owned container $NAME (Q8_NAME)"
else
  echo "== start $NAME from cached $IMAGE (no pull, no network, no ports)"
  "$DOCKER" run -d --rm --pull=never --network none --name "$NAME" \
    -e "POSTGRES_PASSWORD=$PASS" -e "POSTGRES_USER=$ROLE" -e "POSTGRES_DB=$DB" \
    "$IMAGE" >/dev/null || exit 3
fi
CID=$("$DOCKER" inspect -f '{{.Id}}' "$NAME")
LABEL=$("$DOCKER" inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$NAME" 2>/dev/null)
NET=$("$DOCKER" inspect -f '{{.HostConfig.NetworkMode}}' "$NAME")
PORTS=$("$DOCKER" inspect -f '{{json .NetworkSettings.Ports}}' "$NAME")
echo "container_id=$CID"
echo "network_mode=$NET ports=$PORTS compose_project=${LABEL:-none}"
[ "$NET" = "none" ] || { echo "FAIL: network mode is not none"; exit 4; }
case "$PORTS" in "{}"|"null"|"map[]"|"") ;; *) echo "FAIL: published ports present"; exit 4 ;; esac

echo "== wait for the server"
PSQL="/usr/lib/postgresql/18/bin/psql"
for _ in $(seq 1 90); do
  "$DOCKER" exec -e "PGPASSWORD=$PASS" "$NAME" "$PSQL" -h 127.0.0.1 -U "$ROLE" -d "$DB" -At -c "select 1" >/dev/null 2>&1 && break
  sleep 3
done
"$DOCKER" exec -e "PGPASSWORD=$PASS" "$NAME" "$PSQL" -h 127.0.0.1 -U "$ROLE" -d "$DB" -At -c "select 1" >/dev/null 2>&1 || { echo "FAIL: not ready"; exit 5; }

echo "== identity before any mutating statement"
"$DOCKER" exec -e "PGPASSWORD=$PASS" "$NAME" /usr/lib/postgresql/18/bin/psql -h 127.0.0.1 -U "$ROLE" -d "$DB" -At -c \
  "SELECT json_build_object('database',current_database(),'user',current_user,'container',current_setting('server_version'),'client_addr',coalesce((SELECT client_addr::text FROM pg_stat_activity WHERE pid=pg_backend_pid()),'local'))"
echo "container_hostname=$("$DOCKER" exec "$NAME" cat /etc/hostname)"

echo "== apply the minimal real public schema"
"$DOCKER" exec -i -e "PGPASSWORD=$PASS" "$NAME" /usr/lib/postgresql/18/bin/psql -h 127.0.0.1 -U "$ROLE" -d "$DB" \
  -v ON_ERROR_STOP=1 -q < "$HERE/sql/q8_fixture_schema.sql" || { echo "FAIL: schema"; exit 6; }
"$DOCKER" exec -e "PGPASSWORD=$PASS" "$NAME" /usr/lib/postgresql/18/bin/psql -h 127.0.0.1 -U "$ROLE" -d "$DB" -At -c \
  "SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_tables WHERE schemaname='public'"
"$DOCKER" exec -i -e "PGPASSWORD=$PASS" "$NAME" /usr/lib/postgresql/18/bin/psql -h 127.0.0.1 -U "$ROLE" -d "$DB" \
  -v ON_ERROR_STOP=1 -q < "$HERE/sql/q8_fixture_seed.sql" || { echo "FAIL: seed"; exit 6; }
BEFORE=$("$DOCKER" exec -e "PGPASSWORD=$PASS" "$NAME" /usr/lib/postgresql/18/bin/psql -h 127.0.0.1 -U "$ROLE" -d "$DB" -At -c \
  "SELECT count(*)||':'||md5(string_agg(id::text||text||coalesce(embedding::text,''), ',' ORDER BY id)) FROM memory_units")
echo "public_signature_before=$BEFORE"

# Staged, disabled controls: the recorded-page suites assert that the tag switch
# touches nothing else, so the extractor and gardener switches must exist. They
# are written into the temporary workspace, never into a live reports directory.
mkdir -p "$WS/reports"
[ -f "$WS/reports/extractor-active-20261007.json" ] || printf '{"enabled": false, "bank": "hermes-ops"}\n' > "$WS/reports/extractor-active-20261007.json"
[ -f "$WS/reports/gardener-control-20261007.json" ] || printf '{"enabled": false, "bank": "hermes-ops"}\n' > "$WS/reports/gardener-control-20261007.json"
[ -f "$WS/reports/tagger-control-20261007.json" ] || printf '{"enabled": false, "bank": "hermes-ops"}\n' > "$WS/reports/tagger-control-20261007.json"

relay_ok() {
  # The Windows docker CLI is reached over the WSL interop relay, which wedges
  # for seconds at a time ("UtilAcceptVsock ... accept4 failed 110"). Probe it
  # before the suites so a flap is not reported as a test failure.
  local attempt
  for attempt in 1 2 3 4 5; do
    "$DOCKER" exec "$NAME" /bin/true >/dev/null 2>&1 && return 0
    sleep 5
  done
  return 1
}
relay_ok || { echo "FAIL: docker relay unavailable"; exit 9; }

run_suite() {  # run_suite <pattern> <label> ; retries once on a relay flap only
  local pattern="$1" label="$2" log="$TMP/$2.log" rc
  env -i \
    PATH="/usr/local/bin:/usr/bin:/bin" HOME="$TMP" \
    HERMES_HOME="$HOME_DIR" HERMES_OPS_WORKSPACE="$WS" HERMES_OPS_REPORTS="$WS/reports" \
    HERMES_OPS_BANK="hermes-ops" \
    HERMES_OPS_TEST_DB="$NAME" HINDSIGHT_CONTAINER="$NAME" \
    HINDSIGHT_DB="$DB" HINDSIGHT_DB_USER="$ROLE" HINDSIGHT_DB_HOST="127.0.0.1" \
    HINDSIGHT_PG_BIN="/usr/lib/postgresql/18/bin/psql" HINDSIGHT_PG_LIB="/usr/lib/x86_64-linux-gnu" \
    HINDSIGHT_DOCKER="$DOCKER" HINDSIGHT_PGPASSWORD="$PASS" \
    PYTHONPATH="$OPS/scripts" TAG_QUEUE_STRICT=1 \
    "$PY" -m unittest discover -s "$HERE" -t "$HERE" -p "$pattern" -v > "$log" 2>&1
  rc=$?
  cat "$log"
  if [ "$rc" -ne 0 ] && grep -q "UtilAcceptVsock" "$log"; then
    echo "== relay flap detected in $label; retrying once"
    relay_ok || { echo "FAIL: docker relay unavailable on retry"; return 9; }
    env -i \
      PATH="/usr/local/bin:/usr/bin:/bin" HOME="$TMP" \
      HERMES_HOME="$HOME_DIR" HERMES_OPS_WORKSPACE="$WS" HERMES_OPS_REPORTS="$WS/reports" \
      HERMES_OPS_BANK="hermes-ops" \
      HERMES_OPS_TEST_DB="$NAME" HINDSIGHT_CONTAINER="$NAME" \
      HINDSIGHT_DB="$DB" HINDSIGHT_DB_USER="$ROLE" HINDSIGHT_DB_HOST="127.0.0.1" \
      HINDSIGHT_PG_BIN="/usr/lib/postgresql/18/bin/psql" HINDSIGHT_PG_LIB="/usr/lib/x86_64-linux-gnu" \
      HINDSIGHT_DOCKER="$DOCKER" HINDSIGHT_PGPASSWORD="$PASS" \
      PYTHONPATH="$OPS/scripts" TAG_QUEUE_STRICT=1 \
      "$PY" -m unittest discover -s "$HERE" -t "$HERE" -p "$pattern" -v > "$log.retry" 2>&1
    rc=$?
    cat "$log.retry"
  fi
  return "$rc"
}

echo "== run the two database suites"
cd "$OPS" || exit 7
run_suite "test_gardener_writer.py" "writer"
WRITER_RC=$?
run_suite "test_tag_queue.py" "queue"
QUEUE_RC=$?

echo "== public rows after the run (must be untouched)"
AFTER=$("$DOCKER" exec -e "PGPASSWORD=$PASS" "$NAME" /usr/lib/postgresql/18/bin/psql -h 127.0.0.1 -U "$ROLE" -d "$DB" -At -c \
  "SELECT count(*)||':'||md5(string_agg(id::text||text||coalesce(embedding::text,''), ',' ORDER BY id)) FROM memory_units")
echo "public_signature_after=$AFTER"
if [ "$BEFORE" = "$AFTER" ]; then echo "public_rows_unchanged=yes"; else echo "public_rows_unchanged=NO"; fi

echo "WRITER_RC=$WRITER_RC QUEUE_RC=$QUEUE_RC"
echo "TMP=$TMP NAME=$NAME"
[ "$BEFORE" = "$AFTER" ] || { echo "FAIL: public rows changed"; exit 8; }
[ "$WRITER_RC" -eq 0 ] && [ "$QUEUE_RC" -eq 0 ]
