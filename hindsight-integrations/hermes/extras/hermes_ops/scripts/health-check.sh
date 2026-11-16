#!/usr/bin/env bash
# health-check.sh -- infrastructure self-diagnosis for a Hermes host.
#
# Cron mode: --no-agent --deliver local, so this script is the only outbound
# lane. Everything it probes is operator-configured; nothing is hardcoded to one
# deployment. Unset ALERT_TARGET means no alert is sent: a RED tick is logged
# and printed with an explicit diagnostic instead.
#
# Probes
#   * Hindsight liveness (/health) and a bank-scoped memories page
#   * Firecrawl MCP /sse (status code only; an SSE stream never closes)
#   * dashboard /api/status
#   * configured banks present and non-empty via the bank LIST fact_count
#   * `hermes doctor` is INFORMATIONAL: it exits non-zero on soft advisories, so
#     it is RED only when the command produced no output at all.
#
# The tag queue holds an flock while its drain loads Postgres; during those
# windows a bank-scoped page can queue behind it while the service is healthy,
# so the data probe is deferred and recorded rather than reported RED.
#
# Output: one JSON line per tick appended to the log; nothing printed on green;
# the RED line printed only on a green -> red edge.
set -uo pipefail

HOME_DIR=${HERMES_HOME:-$HOME/.hermes}
WORKSPACE=${HERMES_OPS_WORKSPACE:-$HOME_DIR/workspace}
REPORTS=${HERMES_OPS_REPORTS:-$WORKSPACE/reports}
QUEUE_DIR=${HERMES_OPS_QUEUE_DIR:-$REPORTS/tag-queue}
LOG=${HERMES_OPS_HEALTH_LOG:-$WORKSPACE/health-check.jsonl}
HERMES_BIN=${HERMES_BIN:-$(command -v hermes 2>/dev/null || true)}
HERMES_PROFILE=${HERMES_ALERT_PROFILE:-default}
DASHBOARD_URL=${HERMES_DASHBOARD_URL:-http://127.0.0.1:9119}
TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# shellcheck disable=SC1091
set -a
[ -f "$HOME_DIR/.env" ] && . "$HOME_DIR/.env"
set +a

HINDSIGHT_URL=${HINDSIGHT_URL:-http://localhost:8888}
FIRECRAWL_MCP_URL=${FIRECRAWL_MCP_URL:-http://127.0.0.1:3200}
BANKS=${HERMES_OPS_BANKS:-${HERMES_OPS_BANK:-}}

mkdir -p "$(dirname "$LOG")" 2>/dev/null
mkdir -p "$HOME_DIR/logs" 2>/dev/null
RED=()

# Previous tick status, read before this tick appends any note or status line,
# so edge detection sees the real transition rather than our own note.
if command -v jq >/dev/null 2>&1; then
  PREV=$(tail -n 1 "$LOG" 2>/dev/null | jq -r '.status' 2>/dev/null)
else
  PREV=$(tail -n 1 "$LOG" 2>/dev/null | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin).get("status", ""))
except Exception:
    print("")' 2>/dev/null)
fi

probe_http() {  # any 2xx/3xx counts as reachable
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 6 "$1" 2>/dev/null)
  case "$code" in 2??|3??) return 0 ;; *) return 1 ;; esac
}

probe_http "$HINDSIGHT_URL/health" || RED+=("hindsight")
DRAIN_ACTIVE=0
if exec 9>>"$QUEUE_DIR/queue.sqlite.runner.lock" 2>/dev/null; then
  if ! flock -n 9 2>/dev/null; then DRAIN_ACTIVE=1; fi
  exec 9>&- 2>/dev/null || true
fi
if [ -z "$BANKS" ]; then
  echo "{\"ts\":\"$TS\",\"note\":\"bank probes skipped: set HERMES_OPS_BANKS\"}" >> "$LOG"
elif [ "$DRAIN_ACTIVE" = "1" ]; then
  echo "{\"ts\":\"$TS\",\"note\":\"hindsight-banks probe deferred: tagger drain active\"}" >> "$LOG"
else
  first_bank=${BANKS%%,*}
  if ! probe_http "$HINDSIGHT_URL/v1/default/banks/$first_bank/documents?limit=1"; then
    sleep 2
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 \
      "$HINDSIGHT_URL/v1/default/banks/$first_bank/documents?limit=1" 2>/dev/null)
    case "$code" in 2??|3??) ;; *) RED+=("hindsight-banks") ;; esac
  fi
fi
probe_http "$FIRECRAWL_MCP_URL/sse" || RED+=("firecrawl")
probe_http "$DASHBOARD_URL/api/status" || RED+=("dashboard")

# GET /v1/default/banks/<bank>/memories is not a valid route on Hindsight 0.9.2
# ("Method Not Allowed"), which parses as zero and raises a false empty-bank RED.
# The bank LIST carries fact_count per bank and is the supported probe.
BANKS_JSON=$(curl -s --max-time 30 "$HINDSIGHT_URL/v1/default/banks" 2>/dev/null)
if [ -z "$BANKS_JSON" ]; then
  sleep 3
  BANKS_JSON=$(curl -s --max-time 30 "$HINDSIGHT_URL/v1/default/banks" 2>/dev/null)
fi
if command -v jq >/dev/null 2>&1; then
  banks_parse() { printf '%s' "$BANKS_JSON" | jq -r --arg b "$1" '.banks[]? | select(.bank_id==$b) | .fact_count' 2>/dev/null; }
else
  banks_parse() {
    printf '%s' "$BANKS_JSON" | python3 -c 'import json,sys;b=sys.argv[1];d=json.load(sys.stdin);print(next((str(x.get("fact_count")) for x in d.get("banks",[]) if x.get("bank_id")==b),""))' "$1" 2>/dev/null
  }
fi
if [ -n "$BANKS_JSON" ] && [ -n "$BANKS" ]; then
  for BANK in ${BANKS//,/ }; do
    COUNT=$(banks_parse "$BANK")
    if [ -z "$COUNT" ] || [ "$COUNT" = "null" ]; then
      RED+=("$BANK-bank-missing")
    elif [ "$COUNT" = "0" ]; then
      RED+=("$BANK-bank-empty")
    fi
  done
fi

if [ -n "$HERMES_BIN" ] && [ -x "$HERMES_BIN" ]; then
  "$HERMES_BIN" doctor >"$HOME_DIR/logs/doctor-last.txt" 2>&1
  DOCTOR_RC=$?
  [ -s "$HOME_DIR/logs/doctor-last.txt" ] || RED+=("hermes-doctor")
  DOCTOR_ISSUES=$(grep -oE 'Found [0-9]+ issue' "$HOME_DIR/logs/doctor-last.txt" 2>/dev/null | grep -oE '[0-9]+' | head -n 1)
  DOCTOR_ISSUES=${DOCTOR_ISSUES:-0}
else
  DOCTOR_RC=127
  DOCTOR_ISSUES=0
  RED+=("hermes-cli-missing")
fi


if [ ${#RED[@]} -eq 0 ]; then
  printf '{"ts":"%s","status":"green","doctor_rc":%s,"doctor_issues":%s}\n' \
    "$TS" "$DOCTOR_RC" "$DOCTOR_ISSUES" >> "$LOG"
  exit 0
fi

BAD=""
FAILED_JSON=""
for r in "${RED[@]}"; do
  BAD="${BAD:+$BAD,}$r"
  FAILED_JSON="${FAILED_JSON:+$FAILED_JSON,}\"$r\""
done
printf '{"ts":"%s","status":"red","failed":[%s],"doctor_rc":%s,"doctor_issues":%s}\n' \
  "$TS" "$FAILED_JSON" "$DOCTOR_RC" "$DOCTOR_ISSUES" >> "$LOG"

if [ -z "${ALERT_TARGET:-}" ]; then
  echo "[hermes health] RED: $BAD at $TS (alert target unset: no delivery)"
  exit 0
fi
if [ "$PREV" != "red" ]; then
  if ! "$HERMES_BIN" -p "$HERMES_PROFILE" send -t "$ALERT_TARGET" "[hermes health] RED: $BAD at $TS" >/dev/null 2>&1; then
    echo "[hermes health] RED: $BAD at $TS (alert delivery FAILED)"
  fi
fi
exit 0
