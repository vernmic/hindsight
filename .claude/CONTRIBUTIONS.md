# Contribution inventory

Updated 2026-10-29. Current upstream checked at
[`8e8e9dc9cde79c4053befae35d84119d6b3b7cde`](https://github.com/vectorize-io/hindsight/commit/8e8e9dc9cde79c4053befae35d84119d6b3b7cde).
Latest release baseline: v1.1.0 (`a5a619b5ca59`). Classification, validation, source publication,
packaging, staging, deployment and upstream acceptance are independent states.

| ID | Capability | Disposition |
|---|---|---|
| S1 | Each append survivor keeps its own tags | Portable [reference candidate](https://github.com/vernmic/hindsight/tree/codex/promotion-append-tags), based on checked upstream. S4 is a separate contract. |
| S2 | Opt-in retain prefix preservation | Portable [reference candidate](https://github.com/vernmic/hindsight/tree/codex/promotion-protected-tags), off by default. |
| S3 | Opt-in consolidation fold prefix preservation | Companion on the S2 branch; shared configuration is included once; no S1 dependency. |
| S4 | Tag carry through full re-extraction | Deferred generic extraction/review: exact match, chunk overlap and ambiguous mapping need an attributable contract; preserving survivors does not preserve deleted/recreated identities. |
| S5 | Neighboring source evidence | Existing [issue2551 and our evidence](https://github.com/vectorize-io/hindsight/issues/2551#issuecomment-6055542023). Prompt-marker activation must become a supported option before portable promotion. |
| S6 | Unmatched mapping repair hints | Fork-only repair vocabulary/policy; generic uncertainty signal deferred. |
| P1 | Independent recall count/token limits | Existing [Hermes issue5426](https://github.com/vectorize-io/hindsight/issues/5426), public [reference](https://github.com/vernmic/hindsight/tree/codex/hermes-recall-result-limit) at `d9553b66c`. Custom OpenClaw knowledge tools are a distinct SDK surface. |
| P2 | Native OpenClaw memory provider | Already upstream via [#5535](https://github.com/vectorize-io/hindsight/pull/5535), before1.0.0. No duplicate request. |
| P3 | Host-root/lifecycle isolation | Integrated locally; portable extraction deferred until separated from sidecar/instance policy. |
| P4 | Durable retain completion/recovery | Integrated locally; generic extraction deferred until operation replay, destination identity and terminal local acknowledgement form a coherent stock-host contract. Acceptance is not extraction success. |
| P5 | Engine turn ownership handshake | Host-dependent; requires paired host ownership signal. Intermediate tie/clock/folding experiments are superseded. |
| P6 | Provider-bound credentials | Corrected custom route; not reported as a stock Hindsight vulnerability. No credentials/disclosure details published. |
| P7 | Filters, quality gate, nonblocking retain | Historical candidates require current-contract extraction. Upstream now has opt-in background memory support; do not blindly replay old filters. Nonblocking work still needs durable completion ownership. |
| P8 | Retain document segmentation | Secondary candidate, pending separation of stable identity/size policy from customization accounting. |

## Provenance and layers

S1 originates in `51cccfcae` / `1b01c31b2`; S2/S3 in `6f353946d`, `62079be73`, `998ab5d2e`
and `fa8f3d754`. Extracted branches retain meaningful authorship and provenance. Their tests and
candidate dossiers state exact identities and limits; no full deployment approval is implied.

Named desks, model/governance policy, prompt-pressure thresholds, local filesystem layout,
operator repair tools and retention vocabulary remain a customization layer. The entire Astinus
system is not presented as a small upstream patch. Historical branches remain recovery pointers,
including the separate undeployed derivation-inheritance experiment. Reverted predecessors are
not counted as additional deliverables. No production cutover is part of this publication.

## Dependency audit

| Assumption | Real | Necessary | Required |
|---|---|---|---|
| S1 needs protected-prefix settings | no | no | no |
| S2/S3 need shared typed configuration | yes | yes | included once |
| Portable features require the customization layer | no | no | no |
| Native provider needs a new upstream request | no, already native | no | no |
| Publication proves runtime installation | no | no | no |

Exact source identities and executed/unfinished checks: [candidate validation](CANDIDATE-VALIDATION.md).

## Published source and submission readback

- S1: [`821b46bea`](https://github.com/vernmic/hindsight/commit/821b46bea1e9cf0bf0fb89e1682bcece46bacc6f), request [#6206](https://github.com/vectorize-io/hindsight/issues/6206).
- S2/S3: [`eef396fcf`](https://github.com/vernmic/hindsight/commit/eef396fcf3851dde89cd6550461e973c7901e2f8), request [#6207](https://github.com/vectorize-io/hindsight/issues/6207).
- Documentation and standalone reproducer initially published at [`9490d7413`](https://github.com/vernmic/hindsight/commit/9490d74132e4cbfda5b00df8e5699214c5f4ad90).
- Both requests were verified open under the submitting account. Maintainer acceptance is unclaimed; candidate database/client limits remain unchanged. Raw integration branches were not published.
