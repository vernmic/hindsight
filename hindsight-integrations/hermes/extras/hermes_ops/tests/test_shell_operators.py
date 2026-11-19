"""Shell operator tests: syntax, stubbed health-check routing, monitor determinism.

All commands are stubs in a temporary PATH; nothing reaches a network, docker or
a live service. `shellcheck` is not installed on this host, so only `bash -n`
and executed behavior are claimed.
"""

import json
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
SHELL = "/bin/bash"


def write_exec(path, body):
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return path


class SyntaxTests(unittest.TestCase):
    def test_all_maintained_shell_scripts_parse(self):
        for name in ("dream-bank.sh", "monitor-brief-input.sh", "health-check.sh", "ingest-docs.sh"):
            result = subprocess.run([SHELL, "-n", str(SCRIPTS / name)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, "%s: %s" % (name, result.stderr))


class HealthCheckTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.home = self.root / "home"
        (self.home / "workspace").mkdir(parents=True)
        (self.home / "logs").mkdir(parents=True)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.root / "calls.log"
        # curl: 200 for the urls listed as healthy, otherwise 000.
        write_exec(
            self.bin / "curl",
            """#!/bin/bash
url=""
for arg in "$@"; do case "$arg" in http*) url="$arg" ;; esac; done
echo "curl $url" >> "%s"
case "$url" in
  *"$HEALTH_FAIL"*) printf '000' ;;
  *) printf '200' ;;
esac
"""
            % self.calls,
        )
        write_exec(
            self.bin / "hermes",
            """#!/bin/bash
echo "hermes $*" >> "%s"
case "$*" in
  *doctor*) echo "Found 0 issue(s)"; exit 0 ;;
  *send*) exit 0 ;;
esac
exit 0
""" % self.calls,
        )
        self.env = {
            "PATH": "%s:/usr/bin:/bin" % self.bin,
            "HERMES_HOME": str(self.home),
            "HERMES_OPS_WORKSPACE": str(self.home / "workspace"),
            "HERMES_BIN": str(self.bin / "hermes"),
            "HEALTH_FAIL": "/health",
            "HOME": str(self.root),
        }

    def run_check(self, **overrides):
        env = dict(self.env)
        env.update(overrides)
        result = subprocess.run(
            [SHELL, str(SCRIPTS / "health-check.sh")], capture_output=True, text=True, env=env, timeout=120
        )
        log = self.home / "workspace" / "health-check.jsonl"
        lines = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        return result, lines

    def test_green_tick_is_silent(self):
        result, lines = self.run_check(HEALTH_FAIL="-health-never-matches")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertEqual(lines[-1]["status"], "green")

    def test_red_tick_without_alert_target_logs_and_refuses_to_alert(self):
        result, lines = self.run_check()
        self.assertEqual(result.returncode, 0)
        self.assertIn("hindsight", lines[-1]["failed"])
        self.assertIn("alert target unset", result.stdout)
        calls = self.calls.read_text()
        self.assertNotIn("send", calls)

    def test_red_tick_with_alert_target_delivers_once_on_the_edge(self):
        result, lines = self.run_check(ALERT_TARGET="telegram:example")
        self.assertIn("send", self.calls.read_text(), "%s | %s" % (lines, result.stdout))
        self.assertEqual(lines[-1]["status"], "red")
        # a second red tick is not a new edge and must not alert again
        self.calls.write_text("")
        self.run_check(ALERT_TARGET="telegram:example")
        self.assertNotIn("send", self.calls.read_text())

    def test_missing_hermes_cli_is_reported(self):
        bare = self.root / "bare-bin"
        bare.mkdir()
        write_exec(bare / "curl", (self.bin / "curl").read_text())
        _result, lines = self.run_check(
            HERMES_BIN="", PATH="%s:/usr/bin:/bin" % bare, HEALTH_FAIL="-health-never-matches"
        )
        self.assertIn("hermes-cli-missing", lines[-1]["failed"])

    def test_unset_bank_list_skips_bank_probes(self):
        _result, lines = self.run_check(HEALTH_FAIL="-health-never-matches")
        notes = [line for line in lines if "note" in line]
        self.assertTrue(any("set HERMES_OPS_BANKS" in line["note"] for line in notes))


class MonitorBriefTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.ws = self.root / "workspace"
        self.ws.mkdir()
        self.env = {"HOME": str(self.root), "HERMES_HOME": str(self.root / "home"), "HERMES_OPS_WORKSPACE": str(self.ws)}

    def run_monitor(self):
        result = subprocess.run(
            [SHELL, str(SCRIPTS / "monitor-brief-input.sh")], capture_output=True, text=True, env=self.env, timeout=60
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def test_absent_files_are_stable(self):
        self.assertEqual(self.run_monitor(), self.run_monitor())

    def test_content_change_changes_the_fingerprint(self):
        target = self.ws / "dream-log.jsonl"
        target.write_text('{"a":1}\n', encoding="utf-8")
        first = self.run_monitor()
        target.write_text('{"a":2}\n', encoding="utf-8")
        self.assertNotEqual(first, self.run_monitor())

    def test_todays_rows_do_not_change_the_window(self):
        import datetime

        target = self.ws / "execution-log.jsonl"
        yesterday = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        target.write_text('{"ts":"%sT01:00:00Z","n":1}\n' % yesterday, encoding="utf-8")
        first = self.run_monitor()
        with target.open("a", encoding="utf-8") as handle:
            handle.write('{"ts":"%sT02:00:00Z","n":2}\n' % today)
        self.assertEqual(first, self.run_monitor())

    def test_workspace_defaults_under_home(self):
        env = {"HOME": str(self.root), "HERMES_HOME": str(self.root / "home2")}
        result = subprocess.run(
            [SHELL, str(SCRIPTS / "monitor-brief-input.sh")], capture_output=True, text=True, env=env, timeout=60
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("curator-latest=none", result.stdout)


class DreamBankTests(unittest.TestCase):
    def source(self, args, **env):
        base = {
            "HERMES_OPS_BANKS": "bank-a,bank-b",
            "HINDSIGHT_PGPASSWORD": "temporary-secret",
            "SCRIPT_PATH": str(SCRIPTS / "dream-bank.sh"),
        }
        base.update(env)
        return subprocess.run(
            [SHELL, "-c", 'set +u; source "$SCRIPT_PATH"; ' + args],
            capture_output=True,
            text=True,
            env={**os.environ, **base},
            timeout=60,
        )

    def test_configured_bank_is_accepted(self):
        result = self.source('refuse_bank bank-b; echo allowed')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("allowed", result.stdout)

    def test_unconfigured_bank_is_refused(self):
        result = self.source("refuse_bank other-bank")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not in HERMES_OPS_BANKS", result.stderr)

    def test_protected_banks_are_refused_even_when_configured(self):
        for name in ("main", "openclaw"):
            result = self.source("refuse_bank %s" % name, HERMES_OPS_BANKS="%s,bank-a" % name)
            self.assertNotEqual(result.returncode, 0, name)
            self.assertIn("protected bank refused", result.stderr)

    def test_psql_argv_uses_configured_container(self):
        # psql_sql now routes through hermes_ops_config.py psql; a stub
        # HINDSIGHT_PSQL_TRANSPORT with {container} proves the instance
        # selection still comes from HINDSIGHT_CONTAINER on the builder route.
        transport = json.dumps(["/bin/echo", "proxy:", "{container}"])
        result = self.source(
            'psql_sql bank-a <<< "select 1" ',
            HINDSIGHT_PSQL_TRANSPORT=transport,
            HINDSIGHT_CONTAINER="container-x",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("container-x", result.stdout)
        self.assertIn("proxy:", result.stdout)

    def test_missing_bank_list_aborts_execution(self):
        result = subprocess.run(
            [SHELL, str(SCRIPTS / "dream-bank.sh"), "--dry-run"],
            capture_output=True,
            text=True,
            env={**os.environ, "HINDSIGHT_PGPASSWORD": "x", "HERMES_OPS_BANKS": "", "HERMES_OPS_BANK": ""},
            timeout=60,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("set HERMES_OPS_BANKS", result.stderr)


if __name__ == "__main__":
    unittest.main()
