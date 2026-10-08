# Jev turn gate

External Hermes plugin for per-turn skill recommendations, recall-feed selection,
post-retrieval memory selection, and unattended skill-write judgment. Host seams are maintained in `vernmic/hermes-agent` on
`codex/jev-turn-gate`. Operator rollout choices are documented in the companion
[maintenance record](../README.md) and Vern's local project register.

The first call scores the skill roster and recall seeds against the same frozen
recent-session state, anchored to the current task. The second call scores fresh,
identified Hindsight records. Recommendations are pointers in the current user
API message. Explicit skill reads and explicit memory-tool calls keep their usual
behavior. Existing transcript content and the system prompt stay stable.

`mode` accepts `"off"`, `"shadow"`, or `"enforced"`. Shadow mode records proposed
selections and keeps baseline injection. First-pass failure keeps baseline recall;
second-pass failure withholds structured candidates. Legacy providers keep their
string output. All worker calls inherit the active profile and secret scope.

The operator approved a longer Jev allowance while bank quality is repaired:
thirty seconds for structured retrieval and thirty-six seconds overall, reserving
three seconds for each judge call. These are upper limits; completed work returns
immediately. The manager's ordinary recall timeout stays unchanged. Cloud recall
uses a private request client so its transport limit does not affect concurrent
retain or explicit recall. Embedded mode keeps its existing transport settings.
Retrieval also respects the remaining turn deadline. Remeasure after cleanup.
Source: the companion [maintenance record](../README.md).

The separate `write_gate.mode` accepts the same values. In enforced mode,
unattended changes require verified message references from at least two
independent session lineages, a lesson, a measurable dimension and a check. Every
skill in an atomic batch must pass all applicable judgments. Judge failure or
missing evidence refuses the mutation. Attended writes keep existing approval
and ownership controls. Set `skills.unattended_write_policy_required: true` with
enforcement so failure to load a policy also refuses unattended writes.

Review capture is configured by `fallback_review`. The consumer groups by skill
and originating session, preserves distinct events and retries, and correlates
approved landings with existing reviews. Capture, delivery and reconciliation
share a cross-process lock. Background delivery retries are bounded; unresolved
events remain durable for the next service pass. A required `start_after` cutoff
prevents automatic historical backfill. Shadow rows are promoted only by a fresh
capture after arming, never by simply flipping the configuration.

Decisions are stored in the active profile's `state.db` in `jev_decisions`, with
stable turn references, state snapshots, typed questions/answers, feed/candidate
provenance and proposed selections. The `assembled` phase records the actual
current user API message and system-prompt hash; proposed selections alone are
not proof of injection. Positive discovery exits become evidence candidates in
`jev_discovery_candidates`; they do not automatically create skills.

Deployment sequence: merge the host patch after checking it, install this plugin
and the staged Hindsight adapter, enable the plugin while preserving existing
plugin configuration, then use shadow turn selection with write enforcement and
dry-run review capture. Merge `session_search` into the review fork's extra tools
when advertised by the parent; its results provide the real source message IDs.
Inspect the sample ladder before enabling selection and real card delivery.
Keep tag filters disabled until the tag registry and coverage are validated.
The [configuration example](config.example.yaml) ships disabled.

Thresholds are provisional. Typed protocol and behavior tests do not establish
real-world relevance improvements. There is no pre-write mutation journal, so a
crash after a mutation but before both ledger and recovery capture remains a
documented gap.
