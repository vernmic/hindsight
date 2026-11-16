# ingest-docs -- hermes-agent documentation -> Hindsight (versioned, tag-scoped sets)

Built for kanban task t_929c2609; layout repaired under kanban t_9bfe1701.
Puts the local hermes-agent docs tree into the `hermes-ops` Hindsight bank so
config answers come from retained docs instead of being guessed at, and so the
harness desk can re-sync the same knowledge whenever the install version changes.

## Files

| File | Role |
|---|---|
| `ingest-docs.sh` | pipeline entry point: `<version-tag>` / `--verify` / `--purge` / `--list-versions` |
| `ingest-docs.py` | parser + retainer (frontmatter, MDX cleanup, section chunking, tagging, retry, state, JSONL emit) |
| `ingest-docs-launch.sh` | detached launcher (`setsid nohup`) for a full run that outlives the session |
| `../knowledge/upstream-docs-chunks/<version>.jsonl` | the SAME parse as a file artifact, for the OpenClaw desk to ingest into its own bank (amendment Vern #2246) |
| `../backups/ingest-docs-<version>.state.json` | per-page state: idempotency + resume |
| `../backups/ingest-docs.log`, `../backups/ingest-docs-run-<version>.log` | run logs |

## Set layout (design `part-docs-v2`)

A retain POST that reuses an existing `document_id` REPLACES that whole document
in Hindsight 0.9.2 (`engine/memories/pg/writes.py: delete_document` -- "called
when a document is replaced"), so the original "one document id per version"
design kept only the LAST POST's items: run 1 of v2026.9.24 POSTed 1686 items and
left `documents_row=1` with 210 surviving units, 0 of them from any messaging
page. The set identity is a TAG now:

* every item carries `docset:hermes-agent-docs-<version-tag>` (set identity),
  `version:<tag>`, `source:docs/<relpath>`, `domain:<topic>`, `kind:docs`,
  `scope:hermes-agent`, `confidence:high` (`confidence:high` is deliberate:
  `~/.hermes/workspace/memory-index.md` routes the default recall filter on
  `confidence:{high,medium}`);
* `document_id` is per page-part:
  `hermes-agent-docs-<version-tag>-<page-slug>-<sha1(relpath)[:6]>`
  (plus `-pNN` when a page needs more than 25 items). A page's items are cut into
  parts of at most 25 items and a POST never splits a part, so no `document_id` is
  written twice in one run -- which is what made the old design lossy;
* a part is stable across runs, so re-posting a failed part is an identical
  replace (idempotent retry).

`ingest-docs.py`'s `DESIGN` constant gates this: if the stored state was written
by an older layout, every page's `done` flag is cleared and the set is re-posted
(the old layout's documents cannot be trusted).

## What it produces

* Source: `/home/vern/.hermes/hermes-agent/website/docs/{getting-started,reference,guides,integrations,user-guide}`
  (387 pages, 6.6 MB, developer-guide/ deliberately out of scope per the card).
* ~1688 retain items (one per page section group, split at H2 boundaries, grouped
  to <=5 KB), posted in ~79 batches of <=25 items over 391 page-part documents.
* Items use `timestamp:"unset"` (docs are timeless reference material, so retained
  facts must not be dated to the ingest run).

## Usage

    ingest-docs.sh v2026.9.24              # parse -> tag -> retain -> verify (resumable)
    ingest-docs.sh v2026.9.24 --dry-run    # parse only, no retains, no state write
    ingest-docs.sh v2026.9.24 --limit 12   # smoke test (probe gates skipped)
    ingest-docs.sh --verify v2026.9.24     # psql counts + gates + recall probe + artifact
    ingest-docs.sh --list-versions         # doc sets present in the bank
    ingest-docs.sh --purge v2026.9.24      # tag-scoped delete (psql only)
    ingest-docs.sh v2026.9.24 --force      # purge then re-ingest from scratch

Exit codes: 0 ok, 1 `failed_items`/verify-gate failure, 2 usage, 3 preflight.
A batch that fails is retried twice (backoff 20s/40s) on HTTP 500 / deadlock /
socket errors, and `failed_items > 0` makes the wrapper exit non-zero.

## Version-change lifecycle (owned by the desk's daily docs-tick)

    INSTALLED=$(hermes --version | grep -oE 'v[0-9]{4}\.[0-9]+' | head -1)
    ingest-docs.sh --list-versions            # doc sets are listed by docset: tag
    # no units tagged docset:hermes-agent-docs-$INSTALLED -> ingest it
    for OLD in <doc sets older than $INSTALLED>; do ingest-docs.sh --purge "$OLD"; done
    ingest-docs.sh "$INSTALLED"

`--purge` is psql-only BY DESIGN (hard rule): never the HTTP `DELETE
/v1/.../documents/{id}` endpoint, which deletes whole documents by filter and has
wiped a bank before. The purge runs in one psql session as a single CTE:

1. delete `fact_type='observation'` units carrying `docset:<doc-id>` or
   `version:<tag>`, or whose `source_memory_ids` overlap the doc set's units
   (consolidation-derived facts are NOT document-scoped and would otherwise
   survive);
2. delete the doc set's `memory_units` -- by `docset:` tag, and by `document_id`
   (`<doc-id>` or `<doc-id>-%`) for rows whose tags were lost;
3. delete the doc set's `chunks` and `documents` rows by the same id prefix.

## Verify gates

`--verify` no longer passes on `units_total > 0` (that check could not see the
last-POST-only defect: 1686 items posted, 210 units, gate green). It now reads the
parse state (`--report`) and asserts, for a full set (>= 100 parsed pages):

* `documents_row >= 1`;
* raw `world`/`experience` units with the `docset:` tag >= 80% of parsed chunk
  items (`MIN_UNIT_RATIO`);
* distinct `source:docs/%` pages represented >= 90% of parsed pages
  (`MIN_PAGE_RATIO`);
* at least one unit from `docs/user-guide/messaging/telegram.md`;
* `failed_items = 0` and `pages_failed = 0`;
* the recall probe (`types=world+experience`) returns a docset-tagged item and a
  `telegram.md`-sourced item (skipped for `--limit` smokes).

## Operational notes and measured facts

* Retain is **synchronous** (`async: false`); the server splits a POST internally
  into ~10k-token sub-batches (`HINDSIGHT_API_RETAIN_BATCH_TOKENS` default
  10,000) and offsets chunk ids per document, so several documents per POST are
  safe. Batch of 25 items takes ~25-65 s (server LLM concurrency 12, groups of up
  to 8 documents in flight); a full run is ~30-45 min.
* Measured cost of the broken v2026.9.24 run (canary, do not repeat casually):
  6.97 M input / 2.47 M output DeepSeek tokens on Hindsight's own key, ~2.5 M
  output after the retry. Extraction is ~4.2 k input / ~1.3 k output tokens per
  item. Re-ingest only when the docs tree actually changed.
* Idempotent per version: a re-run skips pages marked `done` in the state file and
  re-posts identical page-part documents (replace, not append), so no duplicates.
* Docker access: the WSL2 docker socket is not available; the psql steps go through
  the Windows CLI (`/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe`)
  against container `hindsight-hindsight-1` (port 8888). Container is derived from
  the port in `HINDSIGHT_URL` (8889 -> `hindsight2-hindsight-1`).
* Recall surfaces doc knowledge at BOTH layers: raw `world`/`experience` units
  carry the `docset:`/`source:` tags; consolidation also emits observations tagged
  `version:<version>` / `docset:<doc-set>`, which is what the plugin's default
  observation-only auto-recall returns.
* Parser note: inline code spans are masked while MDX/JSX cleanup runs, so literal
  placeholders survive (`cron/output/<job_id>/`, `delivery_failed: <reason>`).
