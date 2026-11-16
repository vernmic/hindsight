# Hermes operator customizations

Canonical source for the October 7 Jev selector, journaled gardener, documentation
source parser and operator controls. This is a fork-maintained companion to the
Hermes integration, not an automatically loaded part of the Hindsight server.

* `jev_gate/`: external Hermes plugin, installed in `$HERMES_HOME/plugins/jev_gate`.
* `scripts/`: runtime tools mirrored to the operator workspace and `$HERMES_HOME/scripts`.
* `tests/`: gardener, queue and offline behavior tests. Jev tests are under `jev_gate/tests`.
* `extraction-policy.json`: bank-specific candidate and rollback fields.
* `source-manifest.json`: hashes of the maintained source; no raw memories.

## Approved Jev payload and its exception

`jev_gate/` (14 files) and the two Python controls `scripts/jev-control.py` and
`scripts/jev-write-control.py` are carried byte-identical to the approved
dependency `0aacb00a`. The four `scripts/jev*.ps1` wrappers are **not** identical:
under a root-approved scope exception they were parameterized to take their
distro, interpreter, controller path and Hermes home from `HERMES_HOME`,
`HERMES_OPS_WORKSPACE`, `HERMES_OPS_PYTHON`, `HERMES_WSL_DISTRO` and the
`HERMES_OPS_*_CONTROL` overrides, and to call `wsl.exe -e` with argv instead of
a quoted shell string. Plugin runtime behaviour is unchanged.

## Settings

Every deployment value is resolved at runtime by `scripts/hermes_ops_config.py`:
explicit CLI flag, then environment, then a generic value derived from the
resolved Hermes home, then refusal. There is no built-in bank, container, chat
target or host path.

| Setting | Default | Notes |
|---|---|---|
| `HERMES_HOME` | `~/.hermes` | resolved through `get_hermes_home()` when the checkout is importable |
| `HERMES_OPS_WORKSPACE` | `$HERMES_HOME/workspace` | holds `scripts/`, `reports/`, `backups/`, `knowledge/` |
| `HERMES_OPS_BANK` | none - required | the one garden bank; `main` and `openclaw` are refused |
| `HERMES_OPS_BANKS` | `HERMES_OPS_BANK` | comma list for the nightly pass |
| `HINDSIGHT_URL` | `http://localhost:8888` | set `host.docker.internal` only on Docker Desktop hosts |
| `HINDSIGHT_CONTAINER` | `hindsight` (the compose service name) | set it whenever your deployment renamed the service |
| `HINDSIGHT_DB` / `HINDSIGHT_DB_USER` / `HINDSIGHT_DB_HOST` | `hindsight` / `hindsight` / `127.0.0.1` | expected database identity |
| `HINDSIGHT_PG_BIN` / `HINDSIGHT_PG_LIB` | discovered inside the container | set explicitly when discovery cannot find psql; an explicit `<prefix>/bin/psql` implies `<prefix>/lib` |
| `HINDSIGHT_PGPASSWORD` | none - required for DB tools | read only from the authorized environment |
| `HERMES_OPS_PYTHON` | `$HERMES_HOME/venvs/ops/bin/python`, else the running interpreter | gardener and ingest subprocesses |
| `ALERT_TARGET` | unset | unset means no alert delivery, never a default chat |
| `HERMES_OPS_RUN_DIR`, `HERMES_OPS_CONTROL`, `HERMES_OPS_OVERRIDES`, `HERMES_OPS_QUEUE_DIR`, `HERMES_OPS_BACKUP_DIR` | under `reports/` and `backups/` | point these at existing operator state before installing |

Missing state is fail-closed: a missing gardener control file means disabled, a
missing run directory is created empty rather than silently reusing another
operator's, and an unset bank refuses rather than guessing.


Read selection has an off switch (`scripts/jev-control.py off`). The October 7 write gate and review delivery were deployed disabled. The
separate October 8 JEV-01 activation has since armed them; publishing this
source does not change their live settings. Tag-filter enforcement remains disabled pending validation.
The post-extraction tag queue is built and validated, with its independent
production switch parked off. Its approved implementation and legacy intake
gates are included here. Do not replay an apply journal as an installer.

The gardener uses only `hermes-ops`, checks row versions before writes, journals
changes and supports selected rollback. It is an instance customization and
requires Vern's existing database, Docker, workspace and model configuration.
Its workspace reports, review journals, backups, API keys and bank exports are
runtime data and must stay outside this repository.

Use `sync_runtime.py` to compare or mirror the plugin and tools after reviewing
an update. It defaults to comparison; `--apply` creates a backup before copying.
It does not change configuration, restart a gateway, launch gardening, or build
a server image. The Hindsight adapter and server must be installed through their
own integration/build procedures; do not overwrite their newer deployed files.

The source port targets upstream `fb11ddfea`. The October 7 runtime used an older
image and Hermes baseline. Source validation and live validation are separate:
a passing source test is not a receipt that this newer port was deployed.
