# Hermes operator customizations

Canonical source for the October 7 Jev selector, journaled gardener, documentation
source parser and operator controls. This is a fork-maintained companion to the
Hermes integration, not an automatically loaded part of the Hindsight server.

* `jev_gate/`: external Hermes plugin, installed in `$HERMES_HOME/plugins/jev_gate`.
* `scripts/`: runtime tools mirrored to `/mnt/i/hermes/scripts` on Vern's WSL host.
* `tests/`: gardener behavior tests. Jev behavior tests are under `jev_gate/tests`.
* `extraction-policy.json`: approved bank-specific candidate and rollback fields.
* `source-manifest.json`: hashes of the carried deployment sources; no raw memories.

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
