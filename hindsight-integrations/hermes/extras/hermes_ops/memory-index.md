# Memory Index

Maps session/workflow keywords to (a) Hermes-owned Hindsight banks AND (b) tag filters.
Built-in auto-recall queries hermes-ops only. The agentic-recall plugin fans out to
agentic-design when the current message matches that row's keywords. Banks not listed
here are not queried (e.g., OpenClaw's main, openclaw).

## Tag schema (mandatory at every retain)

- kind:        docs | fact | decision | constraint | correction | directive |
               design-decision | build | dream-observation | status | incident |
               research-finding | user-profile | skill-distillation | finding |
               infra | preference | environment | implementation |
               skill-maintenance | event | process-rule
- scope:       hermes-agent | project | hermes-ops | harness
- domain:      skills | config | guides | messaging | models | install |
               dashboard | cli | security | kanban | knowledge | infrastructure |
               hermes-ecosystem | ...
- confidence:  high | provisional | medium | confirmed
- source:      session | derived | document:<id>

## Routing table

| Workflow / domain keywords                      | Primary banks  | Also recall    | Tag filter (AND; OR within {})              |
|-------------------------------------------------|----------------|----------------|---------------------------------------------|
| (default -- no keyword match)                   | hermes-ops     |                | confidence:{high,medium}                    |
| user, preference, style, like, prefer           | human-model    | hermes-ops     | scope:{project,harness}                     |
| coding, code, repo, github, skill               | hermes-ops     |                | domain:coding, confidence:{high,medium}     |
| install, config, version, infra, hindsight, wsl | hermes-ops     |                | scope:harness, kind:{decision,skill-distillation} |
| operator, vern, goal, career, pivot             | human-model    |                | (no filter -- bank is small)                |
| this project, hermes install, my setup          | hermes-ops     | human-model    | scope:project                               |
| agentic, design pattern, prompt chain, multi-agent, guardrail, grader, reconstruction, a2a | agentic-design | hermes-ops | domain:agentic-design, confidence:high | plugin agentic-recall; not routing/tool-use (too broad) |

## What NOT to retain (telemetry stays in JSONL only)

- Per-session cost, turn count, latency
- Retry / circuit-breaker events
- Coding-loop raw scores per project run
- GEPA per-run metadata
- Curator run reports
- Pre-tool hook block events
- Raw research-scan URLs/headlines (retain the finding, not the URL list)

Rule of thumb: if grep on ~/.hermes/workspace/*.jsonl would answer the same question, do not retain.
