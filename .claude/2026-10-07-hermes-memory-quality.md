# Hermes memory quality source publication - October 7, 2026

The operator authorized publishing the previously deployed Hermes work to the
fork, following the established OpenClaw contribution/customization pattern.
Source base: refreshed upstream `fb11ddfea`. Preserve the unrelated OpenClaw
worktrees; this work is in `.worktrees/hermes-memory-quality`.

The server patch is opt-in through `HERMES_CONTEXT_BOUNDARIES_V1` in a custom
extraction prompt. It supplies neighboring source as evidence without changing
canonical chunks, retains document ownership, handles recursive overflow, and
disables delta extraction for this context-sensitive mode. The adapter adds
identified candidates and snapshots preceding source for automatic retain.
The newer upstream append buffer clears after a retain, so the port keeps one
previous retained turn and clears that reference when the session changes.

`hindsight-integrations/hermes/extras/hermes_ops` owns the instance-specific Jev,
gardener and controls. These are fork customizations, not a public integration
release. There is no version bump or registry release in this source pass.
No memory export, review journal, credential or backup belongs in this fork.

The October 7 live image remains `hindsight-fork:hermes-context-20261007`; the
Hermes gateway baseline remains `f97608f178`. A source commit is not deployment.
The workspace's `plan/PROJECTS.md` and dated instance records own live state,
open decisions and runtime rollback. Publication does not arm controls. The separate October 8 JEV-01 work has
since enabled the live write policy and review delivery; tag filtering and
production fact-tagging remain off. Use a separately validated update to deploy this newer
source port. Lint/test receipts are recorded in the local publication report.

The historical `feat/hermes-recall-max-results` commit `7baf8d1f5` is carried
forward in the adapter port. Current upstream lacks this feature; the cap now
applies after multi-bank merging and score filtering, with a zero default.

Validation: Hermes host 36; Jev companion 27; gardener/tag queue 90 plus 68
subtests; full adapter 114; extraction boundary/prompt/regression 15. The boundary
helper also passed 64 concurrent calls on Python 3.14.4 with its GIL disabled.
Real synthetic extraction and independent quality judgment have local receipts;
the `hs_llm_core` acceptance test guards against neighbor-only retention.

Review scope: the new server helper and payload are typed named structures.
Recovered operator scripts retain their deployed dictionary protocols and SQL
journal interfaces; they are archived fork-specific implementations, not
upstream-ready refactors. Modernizing those legacy interfaces is separate work
from this source recovery. No new database migration/table is introduced by the
server patch; operator journal DDL and recovery remain in the instance tools.

TAG-03 validation subsequently completed read-only: its published vocabulary
is preserved in the companion `tag-registry.json`. It is not wired into live
configuration and does not activate the selector's filtering gate.

A brief context-dependent deferral was omitted by the deployed default model
in a synthetic source canary. A separate deferral with a durable lock-contention
reason preserved its CargoLab subject, proposed approach, status and cause.
Keep that distinction in future quality feedback; do not treat syntax or the
three successful canary cases as a universal extraction-quality guarantee.
