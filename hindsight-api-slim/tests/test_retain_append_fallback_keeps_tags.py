"""An append must not lose earlier units' tags when it falls back to full re-extraction.

``update_mode="append"`` normally runs as a delta: unchanged chunks keep their units and
those units keep their tags (``test_retain_append_preserves_unit_tags.py``). But the delta
can find NO unchanged chunk - the chunker re-balances the boundaries once the appended text
lands (measured live on HS2 2026-10-14: "Chunk diff: 0 unchanged, 2 changed"), source-context
extraction disables delta entirely, and ``force_reextract`` skips it. The append then takes the
full path: every old unit is deleted and the whole document re-extracted, and the re-extracted
units used to come back with only the append request's tags - every per-unit classification
the earlier units carried was gone.

Contract (precision first, 2026-10-14): a re-extracted unit whose text matches an old unit
(exact, or clearly similar) inherits exactly that old unit's tags; every other unit - genuinely
new text, or re-extracted text with no confident match - gets only the request's tags. An old
unit with no confident match loses its unit-only tags rather than spreading them to siblings
(see ``test_append_tag_carry_precision.py`` for the leak this prevents).
"""

import uuid
from datetime import datetime, timezone

import pytest

from hindsight_api.engine.providers.mock_llm import MockLLM


@pytest.fixture(scope="session")
def db_url():
    """Isolated pg0 instance so this module never touches the dev database."""
    return "pg0://hsappendfallback:55474"


@pytest.fixture(autouse=True)
def one_fact_per_chunk(monkeypatch):
    def _facts(messages):
        user_text = ""
        for m in messages:
            if m.get("role") == "user":
                user_text = m.get("content", "")
                break
        return {
            "facts": [
                {
                    "what": f"Fact about {user_text[-40:]}",
                    "when": "N/A",
                    "where": "N/A",
                    "who": "N/A",
                    "why": "N/A",
                    "fact_kind": "conversation",
                    "fact_type": "world",
                }
            ]
        }

    monkeypatch.setattr(MockLLM, "_build_mock_facts", staticmethod(_facts))


# The live fixture's sentences and chunk size: the third append re-balances the chunks so
# that no chunk survives, and the delta falls back to a full retain.
_A = (
    "The fixture team decided on 2026-10-14 to keep the Redis cache TTL at 300 seconds for the "
    "billing service because shorter TTLs caused stampedes."
)
_B = (
    "The fixture team also chose PostgreSQL 18 for the reporting replica since it supports the "
    "needed logical replication filters."
)
_C = "Later the fixture team agreed to move nightly backups of the billing database to 02:00 UTC to avoid overlap with batch jobs."
_D = "Afterwards the fixture team confirmed the reporting replica lag stays under five seconds."

_ANNOTATIONS = [
    ["retain:permanent", "kind:decision"],
    ["retain:long", "task:upgrade"],
    ["tier:semantic", "confidence:high"],
]


async def _rows(memory, bank_id, doc):
    async with memory._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT mu.id, mu.tags, c.chunk_text FROM memory_units mu "
            "LEFT JOIN chunks c ON c.chunk_id = mu.chunk_id "
            "WHERE mu.bank_id = $1 AND mu.document_id = $2 AND mu.fact_type IN ('world', 'experience')",
            bank_id,
            doc,
        )
    return {str(r["id"]): {"tags": set(r["tags"] or []), "chunk": r["chunk_text"] or ""} for r in rows}


async def _annotate(memory, rows):
    async with memory._pool.acquire() as conn:
        for i, uid in enumerate(sorted(rows)):
            await conn.execute(
                "UPDATE memory_units SET tags = tags || $2::varchar[] WHERE id = $1",
                uuid.UUID(uid),
                _ANNOTATIONS[i % len(_ANNOTATIONS)],
            )


async def _retain(memory, bank_id, request_context, **item):
    await memory.retain_batch_async(bank_id=bank_id, contents=[item], request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.timeout(600)
@pytest.mark.parametrize("how", ["boundary_shift", "force_reextract"])
async def test_append_fallback_keeps_old_unit_tags(memory, request_context, how):
    bank_id = f"test_append_fallback_{how}_{datetime.now(timezone.utc).timestamp()}"
    doc = "fx-doc"
    try:
        await memory._ensure_bank_exists(bank_id, request_context)
        await memory._config_resolver.update_bank_config(bank_id, {"retain_chunk_size": 400})

        await _retain(memory, bank_id, request_context, content=f"{_A}\n\n{_B}", document_id=doc, tags=["topic:one"])
        await _retain(
            memory, bank_id, request_context, content=_C, document_id=doc, tags=["topic:two"], update_mode="append"
        )
        before = await _rows(memory, bank_id, doc)
        assert len(before) >= 2, before
        await _annotate(memory, before)
        before = await _rows(memory, bank_id, doc)
        old_text = "\n".join(r["chunk"] for r in before.values())

        item = {"content": _D, "document_id": doc, "tags": ["topic:three"], "update_mode": "append"}
        if how == "force_reextract":
            item["force_reextract"] = True
        await _retain(memory, bank_id, request_context, **item)
        after = await _rows(memory, bank_id, doc)
        assert after

        if how == "boundary_shift":
            # The scenario under test: nothing survived, everything was re-extracted.
            assert not (set(after) & set(before)), "a unit survived - the delta did not fall back"

        # Every unit carries either exactly one old unit's tag set (a matched re-extraction) or
        # exactly the append request's tags: never a union of several old units, never a mix.
        old_sets = [r["tags"] for r in before.values()]
        for uid, row in after.items():
            assert row["tags"] in old_sets or row["tags"] == {"topic:three"}, (uid, row, old_sets)
            if row["chunk"].strip() == _D:
                assert row["tags"] == {"topic:three"}, (uid, row)
        assert old_text  # keep the snapshot readable in failure output
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)
