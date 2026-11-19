"""HINDSIGHT_PSQL_TRANSPORT seam tests: default-off parity and transport shape.

No database, no docker, no network. The default route is compared element by
element against the pre-transport construction; the transport route is
inspected as data; the CLI is exercised against a stub argv that copies stdin.
"""

import contextlib
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import hermes_ops_config as config

CLEAR = {
    "HERMES_HOME": None,
    "HERMES_OPS_AGENT_DIR": None,
    "HERMES_OPS_WORKSPACE": None,
    "HERMES_OPS_REPORTS": None,
    "HERMES_OPS_BANK": None,
    "HERMES_OPS_BANKS": None,
    "HERMES_OPS_PYTHON": None,
    "HINDSIGHT_DOCKER": None,
    "HINDSIGHT_URL": None,
    "HINDSIGHT_CONTAINER": None,
    "HINDSIGHT_DB": None,
    "HINDSIGHT_DB_USER": None,
    "HINDSIGHT_DB_HOST": None,
    "HINDSIGHT_PG_BIN": None,
    "HINDSIGHT_PG_LIB": None,
    "HINDSIGHT_PGPASSWORD": None,
    "HINDSIGHT_PSQL_TRANSPORT": None,
    "ALERT_TARGET": None,
    "HERMES_DASHBOARD_URL": None,
}


@contextlib.contextmanager
def clean_env(**overrides):
    saved = {key: os.environ.get(key) for key in CLEAR}
    for key in CLEAR:
        os.environ.pop(key, None)
    for key, value in overrides.items():
        if value is not None:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class TestTransportDefaultOff(unittest.TestCase):
    def test_unset_returns_today_argv_exactly(self):
        """The unset route is byte-identical to the pre-transport construction."""
        with clean_env(
            HINDSIGHT_DOCKER="dockerx",
            HINDSIGHT_CONTAINER="ctx",
            HINDSIGHT_PG_BIN="/opt/bin/psql",
            HINDSIGHT_DB="dbx",
            HINDSIGHT_DB_USER="ux",
            HINDSIGHT_DB_HOST="hx",
            HINDSIGHT_PGPASSWORD="pw",
        ):
            expected = [
                "dockerx", "exec", "-i", "-e", "PGPASSWORD",
                "-e", "LD_LIBRARY_PATH=/opt/lib",  # implied by /opt/bin/psql
                "ctx", "/opt/bin/psql",
                "-h", "hx", "-U", "ux", "-d", "dbx",
                "-v", "ON_ERROR_STOP=1", "-q", "-At",
            ]
            self.assertEqual(config.psql_argv(), expected)
            self.assertEqual(
                config.psql_command(),
                ["PGPASSWORD=pw" if item == "PGPASSWORD" else item for item in expected],
            )

    def test_none_when_unset_or_blank(self):
        with clean_env():
            self.assertIsNone(config.psql_transport())
        with clean_env(HINDSIGHT_PSQL_TRANSPORT="   "):
            self.assertIsNone(config.psql_transport())


class TestTransportSet(unittest.TestCase):
    TRANSPORT = json.dumps(
        ["/proxy/python", "/proxy/Hindsight-SQL-Proxy.py", "--instance", "{container}"]
    )

    def test_argv_is_transport_with_container_substituted(self):
        with clean_env(
            HINDSIGHT_PSQL_TRANSPORT=self.TRANSPORT,
            HINDSIGHT_CONTAINER="second",
        ):
            self.assertEqual(
                config.psql_argv(),
                ["/proxy/python", "/proxy/Hindsight-SQL-Proxy.py", "--instance", "second"],
            )

    def test_command_has_no_password_and_none_required(self):
        with clean_env(
            HINDSIGHT_PSQL_TRANSPORT=self.TRANSPORT,
            HINDSIGHT_CONTAINER="main",
        ):  # no HINDSIGHT_PGPASSWORD anywhere
            argv = config.psql_command()
            self.assertEqual(argv, config.psql_argv())
            self.assertFalse(any("PGPASSWORD" in item for item in argv))

    def test_no_discovery_call_on_transport_route(self):
        with patch.object(config, "_discover_psql") as discover, clean_env(
            HINDSIGHT_PSQL_TRANSPORT=self.TRANSPORT,
            HINDSIGHT_CONTAINER="main",
        ):
            config.psql_argv()
            config.psql_command()
            discover.assert_not_called()

    def test_bad_values_refuse(self):
        for bad in ("not json", "[1, 2]", "[]", '"str"', "[\"a\", 3]"):
            with self.assertRaises(config.ConfigError):
                with clean_env(HINDSIGHT_PSQL_TRANSPORT=bad):
                    config.psql_transport()

    def test_describe_reports_route(self):
        with clean_env(HINDSIGHT_PSQL_TRANSPORT=self.TRANSPORT):
            self.assertEqual(config.describe()["psql_transport"], "set")
        with clean_env():
            self.assertEqual(config.describe()["psql_transport"], "docker-exec")


class TestPsqlCli(unittest.TestCase):
    MODULE = Path(config.__file__).resolve()

    def _run(self, transport, stdin_text, args=()):
        env = dict(os.environ)
        env["HINDSIGHT_PSQL_TRANSPORT"] = transport
        env["HINDSIGHT_CONTAINER"] = "main"
        stub = "import sys; sys.stdout.write(sys.stdin.read())"
        return subprocess.run(
            [sys.executable, str(self.MODULE), "psql", *args],
            input=stdin_text,
            text=True,
            capture_output=True,
            env=env,
        )

    def test_stdin_passes_through_stub_transport(self):
        transport = json.dumps([sys.executable, "-c", "import sys; sys.stdout.write(sys.stdin.read())"])
        result = self._run(transport, "SELECT 1;\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "SELECT 1;\n")

    def test_binds_are_inlined_as_quoted_literals(self):
        stub = (
            "import sys; data=sys.stdin.read(); "
            "sys.stdout.write('OK' if \"bank_id='hermes-ops'\" in data and \"did'O'Brien'\" not in data else 'BAD:'+data)"
        )
        transport = json.dumps([sys.executable, "-c", stub])
        result = self._run(transport, "SELECT 1 FROM t WHERE bank_id=:'bank';", args=("bank=hermes-ops",))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "OK")

    def test_bad_bind_name_refuses(self):
        transport = json.dumps([sys.executable, "-c", "pass"])
        result = self._run(transport, "SELECT 1;", args=("1=2",))
        self.assertEqual(result.returncode, 2)
        self.assertIn("bad binding", result.stderr)

    def test_config_error_is_one_line_not_a_traceback(self):
        # Docker route with no password: the CLI must fail with one line and
        # rc 1, not leak a ConfigError traceback into captured SQL output.
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("HINDSIGHT_", "HERMES_"))
        }
        env["HINDSIGHT_PG_BIN"] = "/bin/true"  # skip in-container discovery
        result = subprocess.run(
            [sys.executable, str(self.MODULE), "psql"],
            input="SELECT 1;\n",
            text=True,
            capture_output=True,
            env=env,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("psql: HINDSIGHT_PGPASSWORD must be set", result.stderr)
        self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
