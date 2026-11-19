"""Shared operator settings for the hermes_ops toolkit.

Every setting is resolved in one place so the maintained operator source carries
no personal host path, bank name, container name, chat target or credential.
Resolution order is always: explicit argument (a CLI flag) -> environment
variable -> a generic value derived from the resolved Hermes home -> refuse with
ConfigError. Nothing is written and no network call is made here.

Shell entry points resolve the same two roots the same way:
    HERMES_HOME            default $HOME/.hermes
    HERMES_OPS_WORKSPACE   default $HERMES_HOME/workspace

Python entry points use get_hermes_home() from the installed Hermes checkout
when that checkout is importable, which honours Hermes' own context-local
override and platform defaults; otherwise they fall back to HERMES_HOME.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

HOME_ENV = "HERMES_HOME"
AGENT_DIR_ENV = "HERMES_OPS_AGENT_DIR"
WORKSPACE_ENV = "HERMES_OPS_WORKSPACE"
REPORTS_ENV = "HERMES_OPS_REPORTS"
BANK_ENV = "HERMES_OPS_BANK"
BANKS_ENV = "HERMES_OPS_BANKS"
PYTHON_ENV = "HERMES_OPS_PYTHON"
DOCKER_ENV = "HINDSIGHT_DOCKER"
URL_ENV = "HINDSIGHT_URL"
TRANSPORT_ENV = "HINDSIGHT_PSQL_TRANSPORT"
ALERT_ENV = "ALERT_TARGET"
DASHBOARD_ENV = "HERMES_DASHBOARD_URL"

# Generic server defaults. database/role/container name the Hindsight compose
# service and its image's own role, and are overridable so a deployment that
# renamed them still works; the psql location is discovered rather than pinned.
# Nothing here names a host, a path outside the image or a bank.
DEFAULT_DB = "hindsight"
DEFAULT_DB_USER = "hindsight"
DEFAULT_DB_HOST = "127.0.0.1"
DEFAULT_CONTAINER = "hindsight"
DEFAULT_API = "http://localhost:8888"
DEFAULT_DASHBOARD = "http://127.0.0.1:9119"

# Hindsight servers that this toolkit must never read or write. Safety policy,
# not personalization: a deployment may add protected banks, never remove these.
REFUSED_BANKS = frozenset({"main", "openclaw"})

BANK_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,62}$")


class ConfigError(RuntimeError):
    """A required operator setting is missing or unusable."""


def _env(name):
    value = os.environ.get(name)
    return value.strip() if value and value.strip() else None


def _flag(value, name):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        raise ConfigError("%s must not be empty" % name)
    return text


def hermes_home(explicit=None):
    """Resolve the Hermes home: explicit, Hermes' own accessor, env, then ~/.hermes."""
    flag = _flag(explicit, "hermes_home")
    if flag:
        return Path(flag).expanduser()
    try:  # only when the installed checkout is already importable
        from hermes_constants import get_hermes_home  # type: ignore
    except Exception:
        pass
    else:
        try:
            resolved = get_hermes_home()
        except Exception:
            resolved = None
        if resolved:
            return Path(resolved)
    env = _env(HOME_ENV)
    return Path(env).expanduser() if env else Path.home() / ".hermes"


def agent_dir(explicit=None):
    """The Hermes checkout that supplies agent.secret_scope and hermes_constants."""
    flag = _flag(explicit, "agent_dir") or _env(AGENT_DIR_ENV)
    if flag:
        return Path(flag).expanduser()
    return hermes_home() / "hermes-agent"


def plugin_dir(explicit=None):
    """The installed copy of the jev_gate plugin (runtime entry for the judge)."""
    flag = _flag(explicit, "plugin_dir")
    if flag:
        return Path(flag).expanduser()
    return hermes_home() / "plugins" / "jev_gate"


def workspace(explicit=None):
    """Operator workspace root holding scripts/, reports/, backups/."""
    flag = _flag(explicit, "workspace")
    if flag:
        return Path(flag).expanduser()
    env = _env(WORKSPACE_ENV)
    if env:
        return Path(env).expanduser()
    return hermes_home() / "workspace"


def reports_dir(workspace_root=None, explicit=None):
    flag = _flag(explicit, "reports_dir") or _env(REPORTS_ENV)
    if flag:
        return Path(flag).expanduser()
    return workspace(workspace_root) / "reports"


def policy_path():
    """The maintained policy file.

    A synced install puts the scripts under <workspace>/scripts and the data
    files beside them at <workspace>/, so both locations are part of the
    declared contract; the package root wins when both exist.
    """
    scripts = Path(__file__).resolve().parent
    for candidate in (scripts.parent / "extraction-policy.json", scripts / "extraction-policy.json"):
        if candidate.is_file():
            return candidate
    return scripts.parent / "extraction-policy.json"


def _policy_bank():
    path = policy_path()
    try:
        value = json.loads(path.read_text(encoding="utf-8")).get("bank")
    except (OSError, ValueError):
        return None
    return value if isinstance(value, str) and value.strip() else None


def check_bank(name):
    """Validate and refuse the protected Hindsight banks."""
    if not isinstance(name, str) or not BANK_PATTERN.match(name or ""):
        raise ConfigError("bank name must match %s" % BANK_PATTERN.pattern)
    if name in REFUSED_BANKS:
        raise ConfigError("bank refused by safety policy: %s" % name)
    return name


def bank(explicit=None):
    """The single bank this toolkit gardens.

    Explicit setting, then HERMES_OPS_BANK, then the maintained policy file.
    There is deliberately no built-in bank name.
    """
    flag = _flag(explicit, "bank") or _env(BANK_ENV)
    if flag:
        return check_bank(flag)
    policy = _policy_bank()
    if policy:
        return check_bank(policy)
    raise ConfigError("no bank configured: set %s or --bank" % BANK_ENV)


def banks(explicit=None):
    """Banks for the nightly pass: explicit list, HERMES_OPS_BANKS, else the single bank."""
    if explicit:
        names = [str(name).strip() for name in explicit if str(name).strip()]
    else:
        raw = _env(BANKS_ENV)
        names = [name.strip() for name in raw.split(",") if name.strip()] if raw else [bank()]
    if not names:
        raise ConfigError("no banks configured: set %s" % BANKS_ENV)
    return [check_bank(name) for name in names]


def api_url(explicit=None, default=DEFAULT_API):
    flag = _flag(explicit, "api_url") or _env(URL_ENV) or default
    if not flag:
        raise ConfigError("no Hindsight URL configured: set %s" % URL_ENV)
    return flag.rstrip("/")


def dashboard_url(explicit=None):
    return _flag(explicit, "dashboard_url") or _env(DASHBOARD_ENV) or DEFAULT_DASHBOARD


def alert_target(explicit=None):
    """RED-alert delivery target. Unset means no delivery, never a default chat."""
    return _flag(explicit, "alert_target") or _env(ALERT_ENV)


def docker(explicit=None):
    """The docker client: explicit, HINDSIGHT_DOCKER, PATH lookup, then plain 'docker'."""
    flag = _flag(explicit, "docker") or _env(DOCKER_ENV)
    if flag:
        return flag
    return shutil.which("docker") or "docker"


def container(explicit=None):
    flag = _flag(explicit, "container") or _env("HINDSIGHT_CONTAINER")
    return flag or DEFAULT_CONTAINER


def database(explicit=None):
    flag = _flag(explicit, "database") or _env("HINDSIGHT_DB")
    return flag or DEFAULT_DB


def database_user(explicit=None):
    flag = _flag(explicit, "database_user") or _env("HINDSIGHT_DB_USER")
    return flag or DEFAULT_DB_USER


def database_host(explicit=None):
    flag = _flag(explicit, "database_host") or _env("HINDSIGHT_DB_HOST")
    return flag or DEFAULT_DB_HOST


_PSQL_DISCOVERY = {}


def _discover_psql(explicit=None):
    """Ask the container where psql is, once per process.

    There is no portable in-image path: the packaged image keeps psql under its
    pg0 installation, others put it on PATH. Discovery is best-effort and cached;
    a wrong or missing result surfaces as a failing psql call, never as a query
    against a different server.
    """
    if "value" in _PSQL_DISCOVERY:
        return _PSQL_DISCOVERY["value"]
    found = None
    try:
        probe = subprocess.run(
            [
                docker(explicit),
                "exec",
                container(explicit),
                "sh",
                "-c",
                "command -v psql || ls -d /usr/lib/postgresql/*/bin/psql "
                "/home/*/.pg0/installation/*/bin/psql 2>/dev/null | tail -n 1",
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
        if probe.returncode == 0 and probe.stdout.strip():
            candidate = probe.stdout.strip().splitlines()[-1].strip()
            if candidate.startswith("/"):
                found = candidate
    except (OSError, subprocess.SubprocessError):
        found = None
    _PSQL_DISCOVERY["value"] = found
    return found


def psql_bin(explicit=None):
    flag = _flag(explicit, "psql_bin") or _env("HINDSIGHT_PG_BIN")
    if flag:
        return flag
    return _discover_psql(explicit) or "psql"


def psql_lib(explicit=None):
    """The library directory for an in-image psql, when one is needed.

    Explicit setting wins; otherwise a psql under <prefix>/bin/psql implies
    <prefix>/lib. An empty result means no LD_LIBRARY_PATH is passed at all.
    """
    flag = _flag(explicit, "psql_lib") or _env("HINDSIGHT_PG_LIB")
    if flag:
        return flag
    binary = psql_bin(explicit)
    if binary.endswith("/bin/psql"):
        return str(Path(binary).parent.parent / "lib")
    return ""


def pg_password():
    """The database password, read only from the authorized environment."""
    value = (os.environ.get("HINDSIGHT_PGPASSWORD") or "").strip()
    if not value:
        raise ConfigError("HINDSIGHT_PGPASSWORD must be set")
    return value


def psql_transport():
    """Optional server-side SQL transport as a JSON argv list. Default: unset.

    When set, psql_argv() returns this list (with a literal "{container}"
    element replaced by container()) instead of the docker-exec route. The
    transport authenticates on the server side, so psql_command() neither
    requires nor injects HINDSIGHT_PGPASSWORD, and _discover_psql() is never
    called. Unset keeps today's behavior byte-for-byte.
    """
    raw = (os.environ.get(TRANSPORT_ENV) or "").strip()
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except ValueError as exc:
        raise ConfigError(f"{TRANSPORT_ENV} must be a JSON argv list: {exc}") from exc
    if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
        raise ConfigError(f"{TRANSPORT_ENV} must be a non-empty JSON list of strings")
    return value


def psql_argv(explicit=None):
    """The one psql command builder (dependency-free).

    Shared by the reader and the writer so the command is built once. Values are
    passed as argv elements; nothing is interpolated into a shell string.
    With HINDSIGHT_PSQL_TRANSPORT set, the configured argv list is returned
    instead (see psql_transport()); that route authenticates server-side.
    """
    transport = psql_transport()
    if transport is not None:
        return [container(explicit) if item == "{container}" else item for item in transport]
    argv = [docker(explicit), "exec", "-i", "-e", "PGPASSWORD"]  # value set by psql_command()
    library = psql_lib(explicit)
    if library:
        argv += ["-e", "LD_LIBRARY_PATH=" + library]
    return argv + [
        container(explicit),
        psql_bin(explicit),
        "-h",
        database_host(explicit),
        "-U",
        database_user(explicit),
        "-d",
        database(explicit),
        "-v",
        "ON_ERROR_STOP=1",
        "-q",
        "-At",
    ]


def psql_command(explicit=None):
    argv = psql_argv(explicit)
    if psql_transport() is not None:
        return argv  # server-side transport; no local password element
    return ["PGPASSWORD=" + pg_password() if arg == "PGPASSWORD" else arg for arg in argv]


def quote(value):
    """SQL string literal binding via the existing psql mechanism."""
    return "'" + str(value).replace("'", "''") + "'"


def ops_python(explicit=None):
    """Interpreter for the gardener and other optional tooling."""
    flag = _flag(explicit, "python") or _env(PYTHON_ENV)
    if flag:
        return flag
    candidate = hermes_home() / "venvs" / "ops" / "bin" / "python"
    if candidate.is_file() and os.access(candidate, os.X_OK):
        return str(candidate)
    return sys.executable


def describe():
    """Non-secret summary for diagnostics and receipts."""
    return {
        "hermes_home": str(hermes_home()),
        "workspace": str(workspace()),
        "bank": _env(BANK_ENV) or _policy_bank(),
        "container": container(),
        "database": database(),
        "api_url": api_url(),
        "psql_transport": "set" if psql_transport() else "docker-exec",
        "alert_target": "set" if alert_target() else "unset",
    }



def _cli():
    """Smallest CLI: `hermes_ops_config.py psql [name=value ...]` reads SQL on
    stdin, inlines psql-style :'name' bindings as safely quoted literals, and
    runs psql_command(), so shell operators have exactly one builder route.
    The binding inline is a plain text replace, so a :'name' sitting inside a
    string literal or a comment is rewritten too; that is safe for every
    statement these scripts send today (no binding name appears in a literal
    or a comment)."""
    if len(sys.argv) < 2 or sys.argv[1] != "psql":
        print("usage: hermes_ops_config.py psql [name=value ...]   (SQL on stdin)", file=sys.stderr)
        return 2
    statement = sys.stdin.read()
    for pair in sys.argv[2:]:
        name, sep, value = pair.partition("=")
        if not sep or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            print(f"psql: bad binding {pair!r} (expected name=value)", file=sys.stderr)
            return 2
        statement = statement.replace(
            ":'" + name + "'", "'" + value.replace("'", "''") + "'"
        )
    try:
        command = psql_command()
    except ConfigError as exc:
        print(f"psql: {exc}", file=sys.stderr)
        return 1
    result = subprocess.run(command, input=statement, text=True)
    return result.returncode


if __name__ == "__main__":  # pragma: no cover - thin shell entry
    raise SystemExit(_cli())
