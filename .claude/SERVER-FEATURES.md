# Server reference contracts

These candidates are not upstream API promises. [Inventory](CONTRIBUTIONS.md) and
[request dispositions](UPSTREAM-REQUESTS.md) identify their scope.

## S1: append survivors

A surviving old unit keeps its complete tag set when a later append carries different tags,
omits tags or explicitly supplies an empty list. Newly inserted units receive the request's tags;
one sibling's classification must not spread to another. Metadata and observation-scope propagation
retain their documented behavior. The provided real-storage tests use `status:*` / `priority:*` annotations,
isolated PostgreSQL and mock extraction; their database assertions did not complete in this pass. Deleted/recreated units are separate S4 work.

## S2/S3: protected prefixes

Two bank-configurable lists are empty/off by default:

- `retain_preserve_tag_prefixes` / `HINDSIGHT_API_RETAIN_PRESERVE_TAG_PREFIXES`.
- `consolidation_preserve_tag_prefixes` / `HINDSIGHT_API_CONSOLIDATION_PRESERVE_TAG_PREFIXES`.

Environment lists accept JSON arrays or CSV. A prefix such as `protected:` is literal, not a
SQL wildcard or observation scope. With no prefixes configured, native behavior remains.

Retain carries matching existing unit tags during survivor relabel and matching document tags
across document upsert/full replacement. Document-level carry does not mark every newly extracted
unit and does not map identities for deleted units. Explicit document-tag PATCH remains exact
replacement, allowing intentional removal of obsolete classifications.

Consolidation carries attributable protected source tags onto surviving fold targets, with bank
isolation and upstream lock ordering. It does not imply inheritance on every derivation.
Observation grouping/globs are separate from tag protection. No concrete retention taxonomy,
classifier, repair workflow or schema migration is required.

The regression boundary includes off/default parity, per-unit ownership, configured carry,
literal special characters, bank isolation and storage-path effects. Mock helper results alone
are not database persistence proof. Each branch's dossier names executed checks and limitations.

Exact source identities and executed/unfinished checks: [candidate validation](CANDIDATE-VALIDATION.md).
