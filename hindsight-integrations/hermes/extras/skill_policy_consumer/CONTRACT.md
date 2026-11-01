# Skill-policy consumer: PR 57703 host contract (W01)

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
| Staged replay | A digest-bound staged record is re-hashed on `/skills approve`; a modified or subset payload is refused. |
| Callback context | Arguments are only operations, digest and telemetry version. The `session_id` argument of `skill_manage` is **not** forwarded. The callback runs on a bounded worker thread started with `contextvars.copy_context()`, so host ContextVars are readable: task-local session id (`gateway.session_context`), write origin and attendedness (`tools.skill_provenance`), ledger actor (`tools.skill_ledger.derive_actor`). Probed A -> B -> A. There is **no evidence carrier**. |

## Shape coverage: flat calls versus `operations[]`

| Shape | Accepted actions | Policy judgments per call | Payload the policy sees |
| --- | --- | --- | --- |
| Flat `skill_manage(action=...)` | create, edit (legacy full-rewrite alias), patch (targeted or `content` full rewrite), write_file, remove_file, delete | **1** | One-element list: `{name, action, <non-null flat fields>, replace_all}` (`replace_all` always present) |
| `operations[]`, one or more of create / patch / write_file / remove_file | Only `_BATCH_OP_ACTIONS` = create, patch, write_file, remove_file | **1** (whole batch, before any effect) | The submitted operations, unchanged |
| `operations[]` full rewrite | `patch` with `content` (there is no batch `edit`) | 1 | As submitted |
| `operations=[{action: edit}]` | **Rejected** natively: `unknown action 'edit'` | 0 | none |
| `operations[]` mixing delete with anything | **Rejected** natively: `delete must be the SOLE op` | 0 | none |
| `operations=[{action: delete}]` (sole delete) | delete | **2 (native gap, see below)** | First the submitted op; then the flat-shape op with `replace_all: false` added, so a different digest |

### Native contract gap: sole-delete batch is judged twice

`_skill_manage_batch` calls `_gate_skill_batch(operations)` for the sole-delete shape and then
delegates to flat `skill_manage(action="delete")`, whose flat gate runs `_gate_skill_batch` again
on the flat-equivalent payload. The first approval digest is discarded, so the decision bound into
staging is the second one. Consequences (probed): one user call produces two judgments with
different digests, and the first can approve while the second refuses (the refusal holds and
nothing is deleted).

**Bounded host correction required before W02 can claim one decision per call** (local design
requirement only; not submitted upstream):

1. The sole-delete `operations[]` call must make exactly one `pre_skill_batch` judgment.
2. That one decision must still feed the human write-approval staging gate, and the digest bound
   into the staged record must be the one replay re-verifies (`_verify_staged_batch_digest`). A flat
   staged delete is re-hashed from the flat shape, so the judged payload and the staged payload must
   be built by the same rule (`_flat_gate_operation`).
3. Native validation, `_background_review_preflight`, mutation locks, the recoverable-archive delete
   path and `post_skill_delete` bookkeeping must run unchanged.
4. It must **not** set the broad `_skill_gate_bypass` flag to suppress the second callback; that
   flag also bypasses the write-approval gate.
5. Acceptable shapes: either judge the sole delete once on the flat-equivalent payload and let the
   flat path carry that digest into its write gate (for example a narrow "policy already decided
   for digest D" hand-off consumed only by the flat policy step), or keep the batch-shape judgment
   and stage a batch-shape record. Either needs focused host tests: one call, staged replay of the
   approved delete succeeds, a tampered staged record is refused, no-policy behavior is unchanged.

Until that correction exists, a consumer must tolerate two calls for one sole-delete batch: treat
each call as an independent judgment of its own digest, never reuse the first decision for the
second, and record both. W02 must not count them as two user proposals.

## Mapping from the existing Jev `pre_skill_write` consumer

| Jev today | Port requirement |
| --- | --- |
| `pre_skill_write(proposal=..., session_id=...)` | Register `pre_skill_batch`; build the proposal from `operations`. |
| Returns `{"action": "allow"}` | Must return `approve` with the echoed digest. `allow` is ignored: no decision (fail-open optional / unavailable required). |
| Returns `block` + `message` + `gate` | `block` + `reason`; the `gate` record is not consumed by the host and must be journaled by the consumer. |
| Uses `session_id` | Not passed. Readable from the task-local session ContextVar on the real dispatch path (probed); W02 must confirm it is bound on every caller path it relies on and report "unavailable" otherwise, not guess from `os.environ` or files. |
| Uses `derive_actor()` / `is_unattended_review()` | Readable in the callback through the copied context (probed). |
| Uses `proposal.evidence` | No carrier. Do not add keys to `operations` (that changes the digest and fails native validation). A missing evidence input stays an explicit unavailable result; if needed, a separate optional host-context argument is a separately reviewed host change. |
| Legacy proposals with action `edit` | Flat `edit` is fine. When building or replaying an `operations[]` call, map a full rewrite to `patch` + `content`; never emit `edit` inside `operations[]`. |
| Previews skill files "under the host's mutation lock" | The hook fires **before** locks; a preview is advisory and can race a concurrent writer. |
| Required only for unattended actors (`skills.unattended_write_policy_required`) | Host required mode is global for all actors; an unattended-only policy must decide inside the consumer (approve attended writes), or the operator accepts global enforcement. W02 must report this scope difference. |
| Shadow mode returns allow | Shadow must return `approve` + digest (or `None` in optional mode). |
| Recommendation on unavailable policy | An absent consumer cannot record its own absence, and a refused call fires no `post_skill_*`. W03 needs a capture owner that runs when policy is absent/failed (see W03 dependency in the promotion packet). |