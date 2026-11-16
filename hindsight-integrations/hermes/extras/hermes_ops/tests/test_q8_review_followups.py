"""Focused regressions for the independent R01 findings (F1-F7 + wrapper scope).

Each test is a red-on-the-old-code / green-on-this-head control:

* F3/F4 fence worker completions on the acquired claim and make the stale release
  a compare-and-set, so a late worker cannot finish a row another run owns.
* F1/F2 the ingest shell sends SQL on stdin with a configured database, role,
  psql path and library, and refuses a bank that is not the shared pattern.
* F5 the declared policy file is found in a synced layout.
* F6 psql location is discovered in the container, never a pinned version path.
* F7 / wrapper scope: no personal path in maintained source, wrappers use
  `wsl.exe -e` argv with configured paths.
"""

import importlib
import os
import re
import stat
import subprocess
import tempfile
import unittest
import unittest.mock
from pathlib import Path

import hermes_ops_config as config
import post_extraction_tag_queue as queue_mod

BANK = "fixture-bank"
UNIT = "44444444-4444-4444-4444-444444444444"
OPS = Path(__file__).resolve().parents[1]
SCRIPTS = OPS / "scripts"


def entry_row(unit=UNIT):
    return {
        "id": unit,
        "bank_id": BANK,
        "text": "complete useful content",
        "document_id": "doc-1",
        "chunk_id": "chunk-1",
        "source_memory_ids": [],
        "tags": [],
        "metadata": {},
        "snapshot_version": "7",
    }


class ClaimFencingTests(unittest.TestCase):
    def setUp(self):
        env = unittest.mock.patch.dict(os.environ, {"HERMES_OPS_BANK": BANK}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "queue.sqlite"
        self.queue = queue_mod.Queue(self.path)
        self.addCleanup(self.queue.close)

    def stale(self, entry, hours=3):
        old = (
            __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
            - __import__("datetime").timedelta(hours=hours)
        ).isoformat()
        with self.queue.db:
            self.queue.db.execute("UPDATE queue SET claim_at=? WHERE key=?", (old, entry["key"]))
        return old

    def test_late_worker_cannot_finish_a_row_another_run_owns(self):
        """F3: the peer probe on 02289 reproduced state=done under run-2's claim."""
        entry = self.queue.enqueue(entry_row(), origin="test")
        first = self.queue.acquire(run_id="run-1", pid=424242)[0]
        self.stale(first)
        self.queue._release_stale_claims()
        second = self.queue.acquire(run_id="run-2", pid=os.getpid())[0]
        self.assertEqual(second["claim_run"], "run-2")

        late = self.queue.mark(entry["key"], "done", reason="late-finish", run_id="run-1", claim=first)
        self.assertFalse(late, "a released claim must not be able to finish the row")
        row = self.queue.get(entry["key"])
        self.assertEqual(row["state"], "in_flight")
        self.assertEqual(row["claim_run"], "run-2")
        history = [tuple(h) for h in self.queue.db.execute("SELECT to_state,run_id FROM queue_history")]
        self.assertNotIn(("done", "run-1"), history)

        self.assertTrue(self.queue.mark(entry["key"], "done", reason="tagged", run_id="run-2", claim=second))
        self.assertEqual(self.queue.get(entry["key"])["state"], "done")

    def test_transition_without_the_live_claim_is_refused(self):
        entry = self.queue.enqueue(entry_row(), origin="test")
        held = self.queue.acquire(run_id="run-1", pid=os.getpid())[0]
        forged = dict(held, claim_run="run-9")
        self.assertFalse(self.queue.mark(entry["key"], "held", reason="x", claim=forged))
        self.assertEqual(self.queue.get(entry["key"])["state"], "in_flight")

    def test_stale_release_is_compare_and_set(self):
        """F4: a fresh reclaim between select and update must survive."""
        entry = self.queue.enqueue(entry_row(), origin="test")
        first = self.queue.acquire(run_id="run-1", pid=424242)[0]
        old_at = self.stale(first)
        # another process released it and a third claimed it under new metadata
        with self.queue.db:
            self.queue.db.execute(
                "UPDATE queue SET claim_pid=?,claim_at=?,claim_run=? WHERE key=?",
                (os.getpid(), "2999-01-01T00:00:00+00:00", "run-2", entry["key"]),
            )
        released = self.queue._release_claim(entry["key"], first["claim_pid"], old_at)
        self.assertFalse(released, "release must not clobber a different live claim")
        row = self.queue.get(entry["key"])
        self.assertEqual(row["state"], "in_flight")
        self.assertEqual(row["claim_run"], "run-2")
        history = [tuple(h) for h in self.queue.db.execute("SELECT reason FROM queue_history")]
        self.assertNotIn(("stale-claim-released",), history)

    def test_two_workers_claiming_at_once_never_share_a_row(self):
        import threading

        self.queue.enqueue(entry_row(), origin="test")
        results, barrier = [], threading.Barrier(2)

        def worker(run, pid):
            own = queue_mod.Queue(self.path)
            try:
                barrier.wait()
                claimed = own.acquire(run_id=run, pid=pid)
            finally:
                own.close()
            results.append((run, claimed))

        threads = [
            threading.Thread(target=worker, args=("run-a", 11111)),
            threading.Thread(target=worker, args=("run-b", 22222)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        winners = [run for run, claimed in results if claimed]
        self.assertEqual(len(winners), 1, "exactly one worker may claim the row")
        self.assertEqual(len(self.queue.entries(state="in_flight")), 1)


class UnclaimedTransitionTests(ClaimFencingTests):
    """R03 review F1: no unconditional unclaimed transition may clobber a claim."""

    def test_unclaimed_transition_on_a_pending_row_is_state_conditional(self):
        """A maintenance transition is allowed, but only against the observed state."""
        entry = self.queue.enqueue(entry_row(), origin="test")
        self.assertTrue(self.queue.mark(entry["key"], "done", reason="no-claim"))
        self.assertEqual(self.queue.get(entry["key"])["state"], "done")
        self.assertFalse(
            self.queue.mark(entry["key"], "done", reason="again", expect_state="pending"),
            "a stale expectation must lose even when the target state matches",
        )

    def test_unclaimed_transition_cannot_clear_an_active_claim(self):
        entry = self.queue.enqueue(entry_row(), origin="test")
        self.queue.acquire(run_id="run-1", pid=os.getpid())
        for expected in (None, "in_flight", "held"):
            with self.subTest(expect_state=expected):
                self.assertFalse(
                    self.queue.mark(entry["key"], "held", reason="x", expect_state=expected)
                )
        row = self.queue.get(entry["key"])
        self.assertEqual(row["state"], "in_flight")
        self.assertEqual(row["claim_run"], "run-1")
        self.assertEqual(list(self.queue.db.execute("SELECT * FROM queue_history")), [])

    def test_maintenance_cas_requires_the_expected_state(self):
        entry = self.queue.enqueue(entry_row(), origin="test")
        self.queue.mark(entry["key"], "held", reason="judge_unavailable_timeout", claim=None, expect_state="pending")
        self.assertTrue(self.queue.mark(entry["key"], "pending", reason="req", expect_state="held"))
        self.assertFalse(self.queue.mark(entry["key"], "pending", reason="req", expect_state="held"))

    def test_late_maintenance_loses_after_release_and_reclaim(self):
        entry = self.queue.enqueue(entry_row(), origin="test")
        first = self.queue.acquire(run_id="run-1", pid=424242)[0]
        old_at = self.stale(first)
        self.queue._release_claim(entry["key"], first["claim_pid"], old_at)
        second = self.queue.acquire(run_id="run-2", pid=os.getpid())[0]
        self.assertEqual(second["claim_run"], "run-2")
        self.assertFalse(self.queue.mark(entry["key"], "pending", reason="stale-sweep", expect_state="held"))
        row = self.queue.get(entry["key"])
        self.assertEqual(row["state"], "in_flight")
        self.assertEqual(row["claim_run"], "run-2")
        reasons = [row[0] for row in self.queue.db.execute("SELECT reason FROM queue_history")]
        self.assertNotIn("stale-sweep", reasons)

    def test_late_violation_override_cannot_clobber_a_reclaimed_row(self):
        """The terminal override must be owned by the run that finished the row."""
        entry = self.queue.enqueue(entry_row(), origin="test")
        claimed = self.queue.acquire(run_id="run-1", pid=os.getpid())[0]
        self.assertTrue(self.queue.mark(entry["key"], "done", reason="tagged", run_id="run-1", claim=claimed))
        # a later identity cycle re-queues and another run takes the row
        self.queue.mark(entry["key"], "pending", reason="requeued-by-class", expect_state="done")
        self.assertEqual(self.queue.acquire(run_id="run-2", pid=os.getpid())[0]["claim_run"], "run-2")
        self.assertFalse(
            self.queue.mark(
                entry["key"], "violation", reason="source-immutability", run_id="run-1",
                expect_state="done", expect_run="run-1",
            )
        )
        row = self.queue.get(entry["key"])
        self.assertEqual(row["state"], "in_flight")
        self.assertEqual(row["claim_run"], "run-2")
        self.assertNotIn("violation", [r[0] for r in self.queue.db.execute("SELECT state FROM queue")])

    def test_violation_override_applies_to_its_own_run(self):
        entry = self.queue.enqueue(entry_row(), origin="test")
        claimed = self.queue.acquire(run_id="run-1", pid=os.getpid())[0]
        self.assertTrue(self.queue.mark(entry["key"], "done", reason="tagged", run_id="run-1", claim=claimed))
        self.assertTrue(
            self.queue.mark(
                entry["key"], "violation", reason="source-immutability", run_id="run-1",
                expect_state="done", expect_run="run-1",
            )
        )
        self.assertEqual(self.queue.get(entry["key"])["state"], "violation")


class IngestShellTests(unittest.TestCase):
    """F1/F2: configured psql argv, SQL on stdin, bank refusal."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.root / "argv.log"
        self.stdin = self.root / "stdin.log"
        body = (
            "#!/bin/bash\n"
            "printf '%s\\n' \"$*\" >> " + str(self.calls) + "\n"
            "if [ \"$1\" = \"exec\" ]; then\n"
            "  cat >> " + str(self.stdin) + " 2>/dev/null\n"
            "  printf '0|docset-placeholder\\n'\n"
            "fi\n"
            "exit 0\n"
        )
        write_exec(self.bin / "docker", body)
        self.env = {
            "PATH": "%s:/usr/bin:/bin" % self.bin,
            "HOME": str(self.root),
            "HERMES_HOME": str(self.root / "home"),
            "HERMES_OPS_WORKSPACE": str(self.root / "ws"),
            "HERMES_OPS_BANK": BANK,
            "HINDSIGHT_DOCKER": str(self.bin / "docker"),
            "HINDSIGHT_CONTAINER": "container-x",
            "HINDSIGHT_DB": "db-x",
            "HINDSIGHT_DB_USER": "role-x",
            "HINDSIGHT_PG_BIN": "/opt/pg/bin/psql",
            "HINDSIGHT_PGPASSWORD": "temporary-secret",
        }

    def run_list(self, **overrides):
        env = dict(self.env)
        env.update(overrides)
        return subprocess.run(
            ["/bin/bash", str(SCRIPTS / "ingest-docs.sh"), "--list-versions"],
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        )

    def test_query_uses_configured_database_role_bin_and_library(self):
        result = self.run_list()
        self.assertEqual(result.returncode, 0, result.stderr)
        argv = self.calls.read_text()
        self.assertIn("-h 127.0.0.1", argv)
        self.assertIn("-U role-x", argv)
        self.assertIn("-d db-x", argv)
        self.assertIn("/opt/pg/bin/psql", argv)
        self.assertIn("LD_LIBRARY_PATH=/opt/pg/lib", argv)
        self.assertIn("-f -", argv)
        self.assertNotIn("sh -c", argv)
        self.assertNotIn("hindsight -d", argv)
        self.assertIn("bank_id='fixture-bank'", self.stdin.read_text())

    def test_bank_with_a_quote_is_refused_before_any_query(self):
        result = self.run_list(HERMES_OPS_BANK="bad'name")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("bank name must match", result.stderr)
        self.assertFalse(self.calls.exists() and self.calls.read_text().strip(), "no query may run")

    def test_protected_bank_is_refused(self):
        for name in ("main", "openclaw"):
            result = self.run_list(HERMES_OPS_BANK=name)
            self.assertNotEqual(result.returncode, 0, name)
            self.assertIn("protected bank refused", result.stderr)


class IngestVersionTagTests(IngestShellTests):
    """R03 review F2: a version tag is data, never SQL text."""

    def run_mode(self, *args, **overrides):
        env = dict(self.env)
        env.update(overrides)
        return subprocess.run(
            ["/bin/bash", str(SCRIPTS / "ingest-docs.sh"), *args],
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        )

    def test_supported_version_tag_is_accepted_and_bound(self):
        result = self.run_mode("--purge", "v2026.9.24")
        self.assertEqual(result.returncode, 0, result.stderr)
        argv = self.calls.read_text()
        self.assertIn("-v tag=v2026.9.24", argv)
        self.assertIn("-v did=hermes-agent-docs-v2026.9.24", argv)
        self.assertIn("-v bank=" + BANK, argv)
        stdin = self.stdin.read_text()
        self.assertIn(":'did'", stdin)
        self.assertIn(":'tag'", stdin)
        self.assertNotIn("v2026.9.24", stdin, "the tag value must travel as a binding, not as SQL text")

    def test_malicious_version_tags_are_refused_before_any_query(self):
        payloads = [
            "v1'; DROP TABLE memory_units; --",
            "v1' OR '1'='1",
            "v1$(id)",
            "v1`id`",
            "v1 2",
            "v1; DELETE FROM documents",
        ]
        for payload in payloads:
            with self.subTest(payload=payload):
                result = self.run_mode("--purge", payload)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn("version tag must match", result.stderr)
                self.assertFalse(
                    self.calls.exists() and self.calls.read_text().strip(),
                    "no query may be issued for %r" % payload,
                )

    def test_force_path_is_guarded_too(self):
        result = self.run_mode("--force", "v1'--")
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.calls.exists() and self.calls.read_text().strip())


class PolicyResolutionTests(unittest.TestCase):
    def test_declared_policy_file_is_found_from_the_package_root(self):
        """F5: the shipped file lives beside scripts/, not inside it."""
        path = config.policy_path()
        self.assertTrue(path.is_file(), path)
        self.assertEqual(path.name, "extraction-policy.json")
        self.assertEqual(path.parent, OPS)
        self.assertNotEqual(path.parent, SCRIPTS)

    def test_bank_falls_back_to_that_file_when_the_environment_is_unset(self):
        path = config.policy_path()
        declared = __import__("json").loads(path.read_text(encoding="utf-8"))["bank"]
        env = unittest.mock.patch.dict(os.environ, {"HERMES_OPS_BANK": ""}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        self.assertEqual(config.bank(), declared)

    def test_sync_map_carries_the_root_data_files(self):
        import sync_runtime

        plan = sync_runtime.targets(OPS, Path("/ws"), Path("/home"))
        mapped = {str(src.relative_to(OPS)): str(dst) for src, dst in plan.items()}
        for name in sync_runtime.ROOT_DATA:
            self.assertIn(name, mapped)
            self.assertEqual(mapped[name], str(Path("/ws") / name))


class PsqlDiscoveryTests(unittest.TestCase):
    def test_discovered_psql_and_derived_library(self):
        """F6: no pinned image path; the container is asked once."""
        def fake_run(argv, **kwargs):
            class Result:
                returncode = 0
                stdout = "/usr/lib/postgresql/18/bin/psql\n"
                stderr = ""

            return Result()

        env = unittest.mock.patch.dict(
            os.environ, {"HINDSIGHT_PG_BIN": "", "HINDSIGHT_PG_LIB": "", "HINDSIGHT_CONTAINER": "c"}, clear=False
        )
        env.start()
        self.addCleanup(env.stop)
        with unittest.mock.patch("subprocess.run", fake_run):
            reloaded = importlib.reload(config)
            self.assertEqual(reloaded.psql_bin(), "/usr/lib/postgresql/18/bin/psql")
            self.assertEqual(reloaded.psql_lib(), "/usr/lib/postgresql/18/lib")
        importlib.reload(config)

    def test_discovery_failure_still_yields_a_usable_default(self):
        def fake_run(argv, **kwargs):
            class Result:
                returncode = 1
                stdout = ""
                stderr = "no such container"

            return Result()

        env = unittest.mock.patch.dict(os.environ, {"HINDSIGHT_PG_BIN": "", "HINDSIGHT_PG_LIB": ""}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        with unittest.mock.patch("subprocess.run", fake_run):
            reloaded = importlib.reload(config)
            self.assertEqual(reloaded.psql_bin(), "psql")
            self.assertEqual(reloaded.psql_lib(), "")
            argv = reloaded.psql_argv()
            self.assertFalse([arg for arg in argv if arg.startswith("LD_LIBRARY_PATH=")])
        importlib.reload(config)


def write_exec(path, body):
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


class GenericSourceTests(unittest.TestCase):
    PERSONAL = re.compile(r"/home/[a-z][\w.-]*|/mnt/[a-z]/")

    def test_no_personal_path_in_maintained_source(self):
        """F7 + wrapper scope: the payload is generic, not one host's layout."""
        offenders = []
        for path in sorted(SCRIPTS.iterdir()) + sorted((OPS / "tests").iterdir()):
            if not path.is_file() or path.suffix not in {".py", ".sh", ".ps1", ".md"}:
                continue
            for number, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
                if self.PERSONAL.search(line):
                    offenders.append("%s:%d" % (path.name, number))
        for name in ("README.md", "tag-registry.json", "extraction-policy.json"):
            for number, line in enumerate((OPS / name).read_text(encoding="utf-8").splitlines(), 1):
                if self.PERSONAL.search(line):
                    offenders.append("%s:%d" % (name, number))
        self.assertEqual(offenders, [], "personal paths in public source: %s" % offenders)

    def test_ingest_shell_has_no_shell_sql_string(self):
        body = (SCRIPTS / "ingest-docs.sh").read_text(encoding="utf-8")
        self.assertNotIn("sh -c \"$PSQL_BIN", body)
        self.assertNotIn("-U hindsight -d hindsight", body)

    def test_powershell_wrappers_use_configured_argv(self):
        wrappers = sorted(SCRIPTS.glob("*.ps1"))
        self.assertEqual(len(wrappers), 5, [w.name for w in wrappers])
        for path in wrappers:
            body = path.read_text(encoding="utf-8")
            self.assertIn("wsl.exe -d $Distro -e $Python", body, path.name)
            self.assertIn("HERMES_HOME", body, path.name)
            self.assertNotIn("-u vern", body, path.name)
            self.assertNotIn("sh -c", body, path.name)


if __name__ == "__main__":
    unittest.main()
