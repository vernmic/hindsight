# Hermes upstream contribution preparation - October 8, 2026

The focused recall cap is separated from the combined operator branch:
[codex/hermes-recall-result-limit](https://github.com/vernmic/hindsight/tree/codex/hermes-recall-result-limit)
at [d9553b66c](https://github.com/vernmic/hindsight/commit/d9553b66ce203c4c19879c9147cb2519635bcba6),
based on upstream fb11ddfea. Three files cover the optional count setting, setup
schema, final post-filter/dedup cap, documentation and behavioral regressions.
Default zero is uncapped; reflect and bank order are unchanged.

Tests: two behavioral failures on untouched upstream; 116 integration tests pass
on Python 3.11, four focused cases pass on Python 3.14.4; full lint hook passes.
Focused code review found no must-fix findings. No dependency, SDK, API, schema
or production-state change. This is a fork reference, not an upstream PR.

Current [CONTRIBUTING](../CONTRIBUTING.md) rejects external PRs. The operator's
submission packet proposes a new feature request for the cap, and evidence on
[existing extraction issue 2551](https://github.com/vectorize-io/hindsight/issues/2551)
instead of a duplicate proposal. Hermes host proposals overlap
[PR 84412](https://github.com/NousResearch/hermes-agent/pull/84412) and
[PR 57703](https://github.com/NousResearch/hermes-agent/pull/57703); alignment is
requested before a current-main host port. Vern approved all four texts on October 8. The two Hermes comments are posted and verified: [Hermes PR 84412](https://github.com/NousResearch/hermes-agent/pull/84412#issuecomment-6055478808); [Hermes PR 57703](https://github.com/NousResearch/hermes-agent/pull/57703#issuecomment-6055483018). The Hindsight feature request and extraction comment remain approved but unposted: the connector returned HTTP 403 and browser sign-in is required. No upstream acceptance is claimed.

The local workspace owns the review packet and detailed test receipts under
reports/upstream-contribution-review-20261008.md and
reports/upstream-contribution-preparation-20261008.json. They contain synthetic
examples and validation only; private memory-bank data was not published.
