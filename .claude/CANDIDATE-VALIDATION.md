# Reference candidate validation (2026-10-29)

These are source-reference candidates, not turnkey releases or deployment approvals.
Both are based on upstream
[`8e8e9dc9cde79c4053befae35d84119d6b3b7cde`](https://github.com/vectorize-io/hindsight/commit/8e8e9dc9cde79c4053befae35d84119d6b3b7cde).
No model-service requests, production/test-instance bank writes, migrations or deployment were
part of the portable candidate checks.

## S1: append survivor annotations

Exact [source](https://github.com/vernmic/hindsight/commit/821b46bea1e9cf0bf0fb89e1682bcece46bacc6f):
30 production additions/1 deletion, plus132 regression-test lines. No new dependency, setting,
API field or migration. Original author/provenance retained from51cccfcae /1b01c31b2.

- Ruff on the three changed files, syntax and diff checks: passed.
- Actual-source helper/store-double comparison: upstream overwrites existing annotations;
candidate preserves them in three effective-tag cases. Non-append replacement remains native.
This executes the actual function AST; it is not a public-API or database proof. A portable
[reproducer](repro_append_tags.py) accepts the candidate checkout rather than an operator path.
- Real PostgreSQL regression: three cases collected, isolated PostgreSQL18.1 started, but
Alembic fixture startup exceeded the five-minute budget before assertions. The exact test
process was stopped/joined and its PostgreSQL instance gracefully stopped. **No DB pass.**

The supplied real-storage test can be run in a Linux checkout with the project's dependencies:

```sh
cd hindsight-api-slim
python -m pytest -n0 -q -p no:cacheprovider tests/test_retain_append_preserves_unit_tags.py
```

Its mock extraction avoids hosted LLM calls, but the project embedding/reranker fixtures need
their usual local dependencies/cache. A provided test is not a successful execution receipt.
Full re-extraction/deleted-unit mapping is outside this candidate.

## S2/S3: opt-in protected prefixes

Exact [source](https://github.com/vernmic/hindsight/commit/eef396fcf3851dde89cd6550461e973c7901e2f8):
345 production additions/6 deletions,530 test additions/1 deletion,100 documentation/schema additions.
Nineteen files; independent of S1 and the customization layer. Original authorship/provenance
retained from6f353946d /62079be73 /998ab5d2e /fa8f3d754.

-36 selected offline configuration/helper tests passed, serial, Python3.14; no database fixtures.
-All15 changed Python files parsed; diff check passed.
-OpenAPI and bank-template schemas regenerated successfully using owning tools.
-Ruff unavailable in that interpreter. Real database retain/fold assertions, concurrent stores,
Oracle, full client/build suites and doc-site build were not run.
-Generated Python/TypeScript/Go BankTemplateConfig client models and high-level TypeScript
updateBankConfig convenience options are **not refreshed**. Raw HTTP/environment configuration
is the reference surface. Complete typed-client work before a turnkey SDK release.

Source review preserves upstream document-before-memory lock order and bank predicates; it does
not establish runtime concurrency or persistence correctness. Prefixes should be chosen with the
bank's observation-scope policy in mind; preserving a tag is not proof that it cannot affect grouping.

## Publication/acceptance

Candidate source, public documentation, upstream submission, maintainer acceptance and deployed
runtime are separate states. Follow the [request ledger](UPSTREAM-REQUESTS.md). No raw operator
logs, credentials, transcripts or databases are needed to reproduce the source-level invariant.
