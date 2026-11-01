# Skill-policy consumer: PR 57703 host contract (W01 map, W02 consumer)

Status: **proposal-dependent reference**. Mapped against NousResearch/hermes-agent PR 57703
head `117c521bed76631c0761e7925ff0977acfbe9be0` (base `085f562af53a6fedadac3681e2c498fc71316132`),
open and unmerged. No released Hermes version provides this contract.

Current-host integration (local only, not pushed): the PR head merged with authorship preserved
onto official Hermes main `c7da41dd522dc19cc08bbfcfa69d4b078224b45d` (merge
`d019776d384b9d5253813ff88e32ae341ac0311f`; with the host probe tests,
`5618e16704e3b18325e0aec56c7771859bc8263c` / tree `cec289e904cdcb8fcfce061368667ee282de1b08`). Only a
docs table conflicted; the hook, dispatch and skill-manager files merged cleanly, and the probes
below pass there. An earlier integration onto `d33d4fd9e7b57e16316ca953523bba596e4f6d5e` (tree
`c1a9b8219f8892c88cbae29be881e95d0b5e2b95`) and the pinned PR-head run are kept as historical
evidence, not current-main qualification.

## Host surface used

| Item | Actual behavior at the pinned head |
| --- | --- |
| Hook | `pre_skill_batch(operations, payload_digest, telemetry_schema_version)`; Python plugins only (shell hooks refused). |
| When | Before the write-approval gate, snapshots, locks and the first mutation. Skipped while an approved staged write is replayed (`_skill_gate_bypass`). See the call-count table below: it is **not** once per call for every shape. |
| Digest | SHA-256 of canonical JSON (sorted keys, no whitespace) of the ordered operations list the gate was given. |
| Approve | `{"action": "approve", "payload_digest": <echoed digest>}` (or `payload_hash`). A missing/mismatched echo is not an approval. |
| Block | `{"action": "block", "reason"\|"message": str}`. A block wins over any approval. This is the skill-gate contract only: the generic `pre_tool_call` parser ignores a block without a nonempty `message`, so a generic adapter must always send `message`. |
| Required mode | `skill_policy.required` in `config.yaml`, re-read on every call. The examples use `required: pre_skill_batch`, the hook it names. The PR treats **any non-blank string** as the switch; the value is not matched against a hook name (probed). |
| Refusal text | `Skill batch policy denied: ...` (decided no) vs `Skill batch policy unavailable: ...` (absent, raise, timeout, invalid/no decision, dispatch failure). A block accompanied by failed callbacks under required mode is reported as unavailable. |
| Optional mode | Only an explicit block refuses; every other outcome fails open. |
| Timeout | Bounded by the dispatcher's generic hook timeout (`plugins.hook_callback_timeout`); not fail-closed at dispatch, the gate decides from the failed-callback list. |
| Invalid names | If any op name fails validation the gate returns no decision and native validation rejects the call; the policy is never consulted. |
| Staged replay | A digest-bound staged record is re-hashed on `/skills approve`; a modified or subset payload is refused. With the W02 host fix, a staged record **without** a digest is refused while `skill_policy.required` is set (stage again for a fresh decision); with no required policy it replays unchanged as before. |
| Callback context | Arguments are only operations, digest and telemetry version. The `session_id` argument of `skill_manage` is **not** forwarded. The callback runs on a bounded worker thread started with `contextvars.copy_context()`, so host ContextVars are readable: task-local session id (`gateway.session_context`), write origin and attendedness (`tools.skill_provenance`), ledger actor (`tools.skill_ledger.derive_actor`). Probed A -> B -> A. There is **no evidence carrier**. |

## Shape coverage: flat calls versus `operations[]`

| Shape | Accepted actions | Policy judgments per call | Payload the policy sees |
| --- | --- | --- | --- |
| Flat `skill_manage(action=...)` | create, edit (legacy full-rewrite alias), patch (targeted or `content` full rewrite), write_file, remove_file, delete | **1** | One-element list: `{name, action, <non-null flat fields>, replace_all}` (`replace_all` always present) |
| `operations[]`, one or more of create / patch / write_file / remove_file | Only `_BATCH_OP_ACTIONS` = create, patch, write_file, remove_file | **1** (whole batch, before any effect) | The submitted operations, unchanged |
| `operations[]` full rewrite | `patch` with `content` (there is no batch `edit`) | 1 | As submitted |
| `operations=[{action: edit}]` | **Rejected** natively: `unknown action 'edit'` | 0 | none |
| `operations[]` mixing delete with anything | **Rejected** natively: `delete must be the SOLE op` | 0 | none |
| `operations=[{action: delete}]` (sole delete) | delete | **1** (W02 host fix) | The flat-equivalent op (`replace_all: false` added); that digest is bound into staging |

### Sole-delete batch: one judgment (W02-H1)

Host commit `79001dcf2d449cca5fc782468672028946f95fdf` (local branch `w02/skill-batch-consumer-c7da41dd`)
removes the batch-side `_gate_skill_batch` call from the sole-delete branch of `_skill_manage_batch`.
The delegated flat `delete` makes the single judgment on the flat-equivalent payload
(`_flat_gate_operation`), and the flat write gate binds that digest into staging, so replay
re-verifies the same digest. No `_skill_gate_bypass`, no hand-off flag; validation,
`_background_review_preflight`, locks, the recoverable archive and `post_skill_delete` are unchanged.
Host tests: one call, native staging plus `/skills approve` replay succeeds, a tampered staged record
is refused (`payload binding`), no-policy behavior unchanged.

## Entry-point boundary (W02 decision)

Required mode is enforced where `_gate_skill_batch` runs (`skill_manage`, flat and `operations[]`)
and, after host commit `a60c9f83355b027940aaf99f1897a063fb48fdee`, at undigested staged replay.

| Entry point | Mode | Behavior |
| --- | --- | --- |
| Agent `skill_manage` (flat, `operations[]`, sole delete) | **covered** | one `pre_skill_batch` judgment per call, digest bound into native staging |
| `/skills approve` replay of a digest-bound record | **covered** | re-hashed; mismatch refused; no second judgment (the staged decision is the one) |
| `/skills approve` replay of an undigested record while required | **covered (refused)** | refused with a re-stage instruction; no policy call, no silent replay |
| Undigested replay with no required policy | unchanged legacy | replays as before |
| Dashboard `POST /api/skills`, `PUT /api/skills/content` -> `_create_skill`/`_edit_skill` | **excluded: human edit** | authenticated dashboard write is the user; native approval unchanged; tested through the real create route (PUT by source inspection only, its profile scoping is not modeled) |
| `agent/learning_mutations.edit_node` -> `_edit_skill` | **excluded: human edit** | user-initiated journey edit; tested through the real caller |

Excluded modes make no policy call. No claim of unattended equivalence is made for them, and the
carried `pre_skill_write` gate is not retired by W02.


## Mapping from the existing Jev `pre_skill_write` consumer

| Jev today | Port requirement |
| --- | --- |
| `pre_skill_write(proposal=..., session_id=...)` | Register `pre_skill_batch`; build the proposal from `operations`. |
| Returns `{"action": "allow"}` | Must return `approve` with the echoed digest. `allow` is ignored: no decision (fail-open optional / unavailable required). |
| Returns `block` + `message` + `gate` | `block` + `reason`; the `gate` record is not consumed by the host and must be journaled by the consumer. |
| Uses `session_id` | Not passed. Readable from the task-local session ContextVar on the real dispatch path (probed); W02 must confirm it is bound on every caller path it relies on and report "unavailable" otherwise, not guess from `os.environ` or files. |
| Uses `derive_actor()` / `is_unattended_review()` | Readable in the callback through the copied context (probed). Attendedness decisions use `is_unattended_review()` only, never the actor label. |
| Uses `proposal.evidence` | No carrier. Do not add keys to `operations` (that changes the digest and fails native validation). A missing evidence input stays an explicit unavailable result; if needed, a separate optional host-context argument is a separately reviewed host change. |
| Legacy proposals with action `edit` | Flat `edit` is fine. When building or replaying an `operations[]` call, map a full rewrite to `patch` + `content`; never emit `edit` inside `operations[]`. |
| Previews skill files "under the host's mutation lock" | The hook fires **before** locks; a preview is advisory and can race a concurrent writer. |
| Required only for unattended actors (`skills.unattended_write_policy_required`) | Host required mode applies to every actor **on calls that reach `_gate_skill_batch`** (`skill_manage` flat and `operations[]`); the excluded human-edit modes above make no policy call; an unattended-only policy must decide inside the consumer (approve attended writes), or the operator accepts global enforcement. W02 must report this scope difference. |
| Shadow mode returns allow | Shadow must return `approve` + digest (or `None` in optional mode). |
| Recommendation on unavailable policy | An absent consumer cannot record its own absence, and a refused call fires no `post_skill_*`. W03 needs a capture owner that runs when policy is absent/failed (see W03 dependency in the promotion packet). |

## W02 consumer (`consumer.py`, native plugin `jev-skill-policy`)

- Loading: `plugin.yaml` + `__init__.py` make the package a directory plugin. Copied to a profile's
  `plugins/jev-skill-policy` and listed in `plugins.enabled`, the native loader calls `register(ctx)`,
  which reads `plugins.entries.jev-skill-policy.settings` for the active profile, validates them,
  builds the judge and registers the hook. Not enabled means not loaded. Tested from a disposable
  profile through `discover_plugins` (`tests/test_plugin_loading.py`); this is not a W04
  distribution package or an installed production plugin.
- Settings: `policy_revision` (required, non-empty), `required` (bool, default false),
  `timeout_seconds` (finite, 0 < t <= 120, default 10), `max_concurrent` (int 1..16, default 4),
  `unattended_only` (bool), `judge_callable` (`module:attr`, the deterministic injection seam; trusted
  profile config), `judge_provider` / `judge_model` (optional overrides, still gated by the host's
  `plugins.entries.<id>.llm.allow_*_override`), `instructions`. Any invalid value raises in `register`,
  so the plugin does not load; under host required mode the host then reports policy unavailable.
- Required mode: `required: true` also demands `skill_policy.required: pre_skill_batch` exactly and a
  host with the hook, else startup fails. Optional on a host without the hook registers nothing.
- Judge: by default the host's `ctx.llm.complete_structured` with a fixed JSON schema and
  `timeout=timeout_seconds`, so the provider call itself is bounded by the host. The judge receives a
  fresh detached dict; the engine keeps its own frozen `JudgeRequest` / `TrustedContext` (read-only
  availability mapping) and a read-only copy of the wire in each `Outcome`.
- Output: exactly `{"verdict": allow|block|modify|require_approval, "reason"?: str|null}`. Any other
  shape, key, or type (including unhashable verdicts) is `failed:invalid`.
- Wire: `allow` -> `{"action":"approve","payload_digest":<this call's digest>}`;
  `block`/`modify`/`require_approval` -> `{"action":"block","reason":...}` (nonempty; fallback names
  verdict, policy revision and request id). Every failure -> `None`, so the host decides
  (unavailable when required, native when optional). Nothing is cached across operations.
- Execution bounds (the pattern of `hermes_cli.approval_transport.invoke_approval_transport`): one
  daemon worker per call under `max_concurrent` slots and no queue. With every slot held, a call fails
  `busy` immediately and never starts later. A timed-out judge keeps its slot until it returns, then the
  slot recovers; its late result is discarded. Daemon workers do not hold up interpreter exit. Unload
  (`ctx.on_unload`) is one consumer-level gate: any call that finishes after close, on the judged path or
  the attended `unattended_only` shortcut, returns `failed:unloaded` (no approval), and no judge starts. A
  Python callable that never returns is not killed; it costs one slot until process exit.
- Attendedness: `unattended_only` skips the judge only when the host's `is_unattended_review()` is
  readable and false: foreground writes and explicitly attended background reviews (`/refine`,
  `set_review_attended(True)`). Unattended background reviews and an unreadable value are judged. The
  curator actor label is reported as context but not used for this decision. Writes whose origin is
  not a background review (for example cron or subagent writes) are not unattended for this host
  predicate, so `unattended_only` does not judge them; W02 does not redefine their authority.
- Context: task-local session id, write origin, attendedness and ledger actor through the copied
  ContextVars; an unbound value is reported unavailable (no `os.environ` fallback); evidence is always
  unavailable.
- Durability: only the in-memory `outcomes` list. Durable blocked/unavailable capture is W03.
