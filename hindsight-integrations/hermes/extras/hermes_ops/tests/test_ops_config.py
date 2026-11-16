"""Offline settings tests: precedence, refusal and command construction.

No database, no docker and no network: the psql argv is inspected as data, and
the password comes from a temporary environment value.
"""

import contextlib
import os
import unittest
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
    "ALERT_TARGET": None,
    "HERMES_DASHBOARD_URL": None,
}


@contextlib.contextmanager
def clean_env(**overrides):
    """Remove every setting under test, apply overrides, then restore exactly."""
    saved = {key: os.environ.get(key) for key in CLEAR}
    for key in CLEAR:
        os.environ.pop(key, None)
    for key, value in overrides.items():
        if value is not None:
            os.environ[key] = value
    try:
        yield
    finally:
        for key in CLEAR:
            os.environ.pop(key, None)
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value


class HomeAndWorkspaceTests(unittest.TestCase):
    def test_explicit_home_wins_and_workspace_derives(self):
        with clean_env(HERMES_HOME="/tmp/ignored"):
            home = config.hermes_home("/tmp/ops-home")
            self.assertEqual(str(home), "/tmp/ops-home")
            self.assertEqual(str(config.workspace("/tmp/ops-ws")), "/tmp/ops-ws")
            with clean_env(HERMES_OPS_WORKSPACE="/tmp/ops-ws"):
                self.assertEqual(str(config.workspace()), "/tmp/ops-ws")

    def test_workspace_defaults_under_resolved_home(self):
        with clean_env():
            with patch.object(config, "hermes_home", return_value=config.Path("/tmp/home-x")):
                self.assertEqual(str(config.workspace()), "/tmp/home-x/workspace")
                self.assertEqual(str(config.reports_dir()), "/tmp/home-x/workspace/reports")

    def test_reports_env_overrides_workspace(self):
        with clean_env(HERMES_OPS_REPORTS="/tmp/reports-x"):
            self.assertEqual(str(config.reports_dir()), "/tmp/reports-x")


class BankTests(unittest.TestCase):
    def test_missing_bank_refuses(self):
        with clean_env():
            with patch.object(config, "_policy_bank", return_value=None):
                with self.assertRaises(config.ConfigError):
                    config.bank()

    def test_policy_file_is_the_last_resort(self):
        with clean_env():
            with patch.object(config, "_policy_bank", return_value="policy-bank"):
                self.assertEqual(config.bank(), "policy-bank")

    def test_environment_beats_policy_file(self):
        with clean_env(HERMES_OPS_BANK="env-bank"):
            with patch.object(config, "_policy_bank", return_value="policy-bank"):
                self.assertEqual(config.bank(), "env-bank")

    def test_protected_banks_are_refused(self):
        for name in ("main", "openclaw"):
            with clean_env(HERMES_OPS_BANK=name):
                with self.assertRaises(config.ConfigError):
                    config.bank()

    def test_banks_list_defaults_to_single_bank(self):
        with clean_env(HERMES_OPS_BANK="one-bank"):
            self.assertEqual(config.banks(), ["one-bank"])
        with clean_env(HERMES_OPS_BANK="one-bank", HERMES_OPS_BANKS="a-bank,b-bank"):
            self.assertEqual(config.banks(), ["a-bank", "b-bank"])

    def test_bank_name_shape_is_enforced(self):
        for bad in ("", "1bank", "bank name", "bank'y"):
            with self.assertRaises(config.ConfigError):
                config.check_bank(bad)


class SqlCommandTests(unittest.TestCase):
    def test_psql_argv_uses_configured_identity(self):
        with clean_env(
            HINDSIGHT_CONTAINER="container-x",
            HINDSIGHT_DB="db-x",
            HINDSIGHT_DB_USER="user-x",
            HINDSIGHT_DB_HOST="127.0.0.9",
            HINDSIGHT_PG_BIN="/opt/bin/psql",
            HINDSIGHT_PG_LIB="/opt/lib",
            HINDSIGHT_DOCKER="/bin/echo",
            HINDSIGHT_PGPASSWORD="temporary-secret",
        ):
            argv = config.psql_command()
        self.assertEqual(argv[0], "/bin/echo")
        self.assertIn("container-x", argv)
        self.assertIn("db-x", argv)
        self.assertIn("user-x", argv)
        self.assertIn("127.0.0.9", argv)
        self.assertIn("/opt/bin/psql", argv)
        self.assertIn("LD_LIBRARY_PATH=/opt/lib", argv)

    def test_password_only_from_environment(self):
        with clean_env():
            with self.assertRaises(config.ConfigError):
                config.psql_command()
        with clean_env(HINDSIGHT_PGPASSWORD="  "):
            with self.assertRaises(config.ConfigError):
                config.psql_command()
        with clean_env(HINDSIGHT_PGPASSWORD="temporary-secret"):
            argv = config.psql_command()
        self.assertIn("PGPASSWORD=temporary-secret", argv)
        self.assertNotIn("PGPASSWORD", argv)

    def test_quote_escapes_single_quotes(self):
        self.assertEqual(config.quote("a'b"), "'a''b'")

    def test_alert_target_unset_is_none(self):
        with clean_env():
            self.assertIsNone(config.alert_target())
        with clean_env(ALERT_TARGET="telegram:example"):
            self.assertEqual(config.alert_target(), "telegram:example")

    def test_api_url_default_is_local_and_overridable(self):
        with clean_env():
            self.assertEqual(config.api_url(), "http://localhost:8888")
        with clean_env(HINDSIGHT_URL="http://host.docker.internal:8888/"):
            self.assertEqual(config.api_url(), "http://host.docker.internal:8888")


if __name__ == "__main__":
    unittest.main()
