# Skill-policy consumer: PR 57703 host contract (W01)

Status: **proposal-dependent reference**. Mapped against NousResearch/hermes-agent PR 57703
head `117c521bed76631c0761e7925ff0977acfbe9be0` (base `085f562af53a6fedadac3681e2c498fc71316132`),
open and unmerged. No released Hermes version provides this contract.

## Host surface used

| Item | Actual behavior at the pinned head |
| --- | --- |
| Hook | `pre_skill_batch(operations, payload_digest, telemetry_schema_version)`; Python plugins only (shell hooks refused). |
| When | Once per `skill_manage` call: `operations[]` batches, the sole-delete batch shape, **and every flat single action** (wrapped as a one-element `operations` list). Before the write-approval gate, snapshots, locks and the first mutation. Skipped while an approved staged write is replayed (`_skill_gate_bypass`). |
| Actions | create, edit, patch, write_file, remove_file, delete. |
| Digest | SHA-256 of canonical JSON (sorted keys, no whitespace) of the ordered operations list. |
| Approve | `{"action": "approve", "payload_digest": <echoed digest>}` (or `payload_hash`). A missing/mismatched echo is not an approval. |
| Block | `{"action": "block", "reason"\|"message": str}`. A block wins over any approval. |
| Required mode | `skill_policy.required` in `config.yaml`; any non-blank string arms fail-closed. The value is not matched against a hook name. |
| Refusal text | `Skill batch policy denied: ...` (decided no) vs `Skill batch policy unavailable: ...` (absent, raise, timeout, invalid/no decision, dispatch failure). A block accompanied by failed callbacks under required mode is reported as unavailable. |
| Optional mode | Only an explicit block refuses; every other outcome fails open. |
| Timeout | Bounded by the dispatcher's generic hook timeout; not fail-closed at dispatch, the gate decides from the failed-callback list. |
| Invalid names | If any op name fails validation the gate returns no decision and native validation rejects the call; the policy is never consulted. |
| Staged replay | A digest-bound staged record is re-hashed on `/skills approve`; a modified or subset payload is refused. |

## Mapping from the existing Jev `pre_skill_write` consumer

| Jev today | Port requirement |
| --- | --- |
| `pre_skill_write(proposal=..., session_id=...)` | Register `pre_skill_batch`; build the proposal from `operations`. |
| Returns `{"action": "allow"}` | Must return `approve` with the echoed digest. `allow` is ignored: no decision (fail-open optional / unavailable required). |
| Returns `block` + `message` + `gate` | `block` + `reason`; the `gate` record is not consumed by the host and must be journaled by the consumer. |
| Uses `session_id` and `proposal.evidence` | The host passes neither. Session/actor/profile must come from host context available in-process, or a host addition. |
| Previews skill files "under the host's mutation lock" | The hook fires **before** locks; a preview is advisory and can race a concurrent writer. |
| Required only for unattended actors (`skills.unattended_write_policy_required`) | Host required mode is global for all actors; an unattended-only policy must decide inside the consumer. |
| Shadow mode returns allow | Shadow must return `approve` + digest (or `None` in optional mode). |
