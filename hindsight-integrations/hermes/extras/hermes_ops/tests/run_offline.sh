#!/usr/bin/env bash
# Offline test runner for the maintained operator source.
#
#   HERMES_OPS_BANK=<bank> tests/run_offline.sh [-v]
#
# HERMES_OPS_BANK is required because the recorded-page suites replay fixtures
# from one real bank. The Q8 configuration, writer, queue and shell suites set
# their own fixture bank and run without it:
#
#   PYTHONPATH=scripts python3 -m unittest discover -s tests -t tests \
#       -p 'test_ops_config.py'      (likewise test_gardener_writer_offline.py,
#                                     test_tag_queue_offline.py, test_shell_operators.py)
#
# The database suites (test_gardener_writer.py, test_tag_queue.py) skip unless
# HERMES_OPS_TEST_DB is set, and are never pointed at a production bank.
# shellcheck is not installed on this host, so shell coverage here is `bash -n`
# plus stubbed execution only.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
: "${HERMES_OPS_BANK:?set HERMES_OPS_BANK to the bank those fixtures describe}"
export PYTHONPATH="$here/../scripts${PYTHONPATH:+:$PYTHONPATH}"
exec "${HERMES_OPS_PYTHON:-python3}" -m unittest discover -s "$here" -t "$here" "$@"
