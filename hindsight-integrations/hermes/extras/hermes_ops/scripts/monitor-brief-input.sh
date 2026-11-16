#!/usr/bin/env bash
# monitor-brief-input.sh -- O2 monitor source for the morning-brief job.
#
# Fingerprints the files the brief reads plus the newest Curator report, so an
# unchanged day suppresses the LLM run. Deterministic: content hashes and
# name+sorted listings only, no timestamps and no mtimes.
#
# Time-windowing: rows for execution-log.jsonl and recall-log.jsonl are filtered
# to YESTERDAY's UTC day (the brief's window). Rows the system writes today --
# including rows written by cron jobs after a fire -- therefore never change the
# fingerprint, which stops the fire-drives-new-rows-drives-refire loop. Falls
# back to the tail hash when date filtering yields nothing (older file).
#
# Every path derives from the resolved Hermes home and operator workspace:
#   HERMES_HOME            default $HOME/.hermes
#   HERMES_OPS_WORKSPACE   default $HERMES_HOME/workspace
#   HERMES_OPS_CURATOR_DIR default $HERMES_HOME/logs/curator
set -uo pipefail

HOME_DIR=${HERMES_HOME:-$HOME/.hermes}
W=${HERMES_OPS_WORKSPACE:-$HOME_DIR/workspace}
C=${HERMES_OPS_CURATOR_DIR:-$HOME_DIR/logs/curator}
Y=$(date -u -d 'yesterday' +%Y-%m-%d)

win() { # window a JSONL file to yesterday-UTC rows carrying "ts"
  if [ -f "$1" ]; then
    out=$(grep -F "\"ts\": \"$Y" "$1" 2>/dev/null || grep -F "\"ts\":\"$Y" "$1" 2>/dev/null || true)
    if [ -n "$out" ]; then printf '%s' "$out" | sha256sum | cut -c1-32; else tail -c 200000 "$1" | sha256sum | cut -c1-32; fi
  else printf 'absent'; fi
}
h() { if [ -f "$1" ]; then tail -c 200000 "$1" 2>/dev/null | sha256sum | cut -c1-32; else printf 'absent'; fi }

printf 'execution-log.jsonl=%s\n' "$(win "$W/execution-log.jsonl")"
printf 'recall-log.jsonl=%s\n' "$(win "$W/recall-log.jsonl")"
printf 'dream-log.jsonl=%s\n' "$(h "$W/dream-log.jsonl")"
printf 'coding-loop-results.jsonl=%s\n' "$(h "$W/coding-loop-results.jsonl")"
printf 'skill-failure-log.jsonl=%s\n' "$(h "$W/skill-failure-log.jsonl")"
printf 'expensive-sessions.txt=%s\n' "$(h "$W/expensive-sessions.txt")"
printf 'procedural-memory-candidates.jsonl=%s\n' "$(h "$W/procedural-memory-candidates.jsonl")"

if [ -d "$W/gepa-pr-candidates" ]; then
  printf 'gepa-pr-candidates=%s\n' "$(find "$W/gepa-pr-candidates" -maxdepth 1 -type f -printf '%f\n' 2>/dev/null | sort | sha256sum | cut -c1-32)"
else
  printf 'gepa-pr-candidates=absent\n'
fi

if [ -d "$C" ]; then
  latest=$(ls -1 "$C" 2>/dev/null | sort | tail -n 1)
  printf 'curator-latest=%s\n' "${latest:-none}"
  rpt="$C/${latest}/REPORT.md"
  [ -f "$rpt" ] || rpt="$C/${latest}/report.md"
  printf 'curator-report=%s\n' "$(h "$rpt")"
else
  printf 'curator-latest=none\ncurator-report=absent\n'
fi
