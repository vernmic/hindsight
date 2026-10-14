"""Bank-gated tag-prefix preservation on consolidation folds.

A dedup fold absorbs source facts (create fold), or a whole observation and its sources
(update fold), into a surviving observation. Protection tags on what is absorbed
(``pin:critical``, ``pin:reference``) used to die with it. With the bank's
``consolidation_preserve_tag_prefixes`` set (default off), the fold unions the absorbed
rows' tags under those prefixes onto the survivor:

* existing survivor tags are never removed and keep their order; additions are sorted;
* only tags under the configured prefixes move (literal prefix, no LIKE wildcards), so a
  source's scope tags never contaminate an observation of another scope;
* the absorbed-row lookup is bank-scoped;
* off (the default) leaves the survivor's tags exactly as they were.

Runs the real fold functions against pg0 with hand-seeded rows; no LLM, no embeddings.
Adapted from protected-prefix fold regressions (2ecafddf7, 079a874db, 013d65eb6).
"""

import types
import uuid
from datetime import datetime, timezone

import pytest

from hindsight_api.engine.consolidation.consolidator import (
    _apply_dedup_create_fold,
    _apply_dedup_update_fold,
    _DedupOutcome,
    _preserve_tag_prefixes,
    _TemporalBounds,
)
from hindsight_api.engine.memories import get_memories
from hindsight_api.engine.memory_engine import fq_table

pytestmark = pytest.mark.memory_backend_incompatible


@pytest.fixture(scope="session")
def db_url():
    """Isolated pg0 instance so this module never touches the dev database."""
    return "pg0://hsfoldtags:55473"


_ENGINE = types.SimpleNamespace(embeddings=None)


def _config(prefixes):
    return types.SimpleNamespace(consolidation_dedup_threshold=0.97, consolidation_preserve_tag_prefixes=prefixes)


async def _new_bank(memory, request_context, name):
    bank_id = f"test_foldtags_{name}_{datetime.now(timezone.utc).timestamp()}"
    await memory._ensure_bank_exists(bank_id, request_context)
    return bank_id


async def _insert(conn, bank_id, text, fact_type, tags, source_ids=None):
    unit_id = uuid.uuid4()
    await conn.execute(
        """
        INSERT INTO memory_units (id, bank_id, text, fact_type, tags, source_memory_ids, proof_count)
        VALUES ($1, $2, $3, $4, $5::varchar[], $6::uuid[], $7)
        """,
        unit_id,
        bank_id,
        text,
        fact_type,
        tags,
        source_ids or [],
        len(source_ids or []) or 1,
    )
    return unit_id


async def _tags(conn, unit_id):
    return list(await conn.fetchval("SELECT tags FROM memory_units WHERE id = $1", unit_id) or [])


def test_preserve_tag_prefixes_reads_the_bank_config():
    assert _preserve_tag_prefixes(types.SimpleNamespace()) is None
    assert _preserve_tag_prefixes(_config(None)) is None
    assert _preserve_tag_prefixes(_config([])) is None
    assert _preserve_tag_prefixes(_config([""])) is None
    assert _preserve_tag_prefixes(_config(["pin:", "pin:"])) == ["pin:", "pin:"]


async def _create_fold(memory, request_context, prefixes, *, source_tags):
    bank_id = await _new_bank(memory, request_context, "create")
    try:
        async with memory._pool.acquire() as conn:
            twin = await _insert(conn, bank_id, "Twin text.", "observation", ["scope:mine"])
            sources = [await _insert(conn, bank_id, f"Source {i}.", "world", t) for i, t in enumerate(source_tags)]
            outcome = _DedupOutcome(
                best_id=str(twin), merged_text="Twin text.", should_merge=True, best_text="Twin text."
            )
            async with conn.transaction():
                folded = await _apply_dedup_create_fold(
                    conn,
                    _ENGINE,
                    bank_id,
                    outcome,
                    sources,
                    _TemporalBounds(),
                    preserve_tag_prefixes=_preserve_tag_prefixes(_config(prefixes)),
                )
            assert folded == str(twin)
            absorbed = await conn.fetchval("SELECT source_memory_ids FROM memory_units WHERE id = $1", twin)
            assert set(absorbed) == set(sources)
            return await _tags(conn, twin)
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


_SOURCES = [["pin:critical", "topic:other", "plain:x"], ["pin:reference", "scope:theirs"]]


@pytest.mark.asyncio
async def test_create_fold_off_leaves_tags_unchanged(memory, request_context):
    assert await _create_fold(memory, request_context, None, source_tags=_SOURCES) == ["scope:mine"]


@pytest.mark.asyncio
async def test_create_fold_on_unions_only_protected_tags(memory, request_context):
    tags = await _create_fold(memory, request_context, ["pin:"], source_tags=_SOURCES)
    # Survivor's own tags first, then the absorbed protected ones, sorted. No scope/topic leak.
    assert tags == ["scope:mine", "pin:reference", "pin:critical"]


@pytest.mark.asyncio
async def test_create_fold_nonmatching_prefix_adds_nothing(memory, request_context):
    assert await _create_fold(memory, request_context, ["pin:"], source_tags=_SOURCES) == ["scope:mine"]


@pytest.mark.asyncio
async def test_create_fold_prefix_is_literal_not_a_like_pattern(memory, request_context):
    tags = await _create_fold(
        memory, request_context, ["re_ain:", "x%"], source_tags=[["pin:wild", "re_ain:literal", "xyz:no", "x%:yes"]]
    )
    assert tags == ["scope:mine", "re_ain:literal", "x%:yes"]


@pytest.mark.asyncio
async def test_create_fold_is_idempotent_and_never_duplicates(memory, request_context):
    tags = await _create_fold(
        memory, request_context, ["scope:"], source_tags=[["scope:mine", "scope:mine"], ["scope:mine"]]
    )
    assert tags == ["scope:mine"]


@pytest.mark.asyncio
async def test_fold_lookup_is_bank_scoped(memory, request_context):
    """A source id from another bank contributes nothing, even if handed to the store fold."""
    bank_id = await _new_bank(memory, request_context, "iso")
    other = await _new_bank(memory, request_context, "iso_other")
    try:
        async with memory._pool.acquire() as conn:
            twin = await _insert(conn, bank_id, "Twin text.", "observation", ["scope:mine"])
            mine = await _insert(conn, bank_id, "Mine.", "world", ["pin:mine"])
            foreign = await _insert(conn, other, "Foreign.", "world", ["pin:foreign"])
            async with conn.transaction():
                ok = await get_memories().fold_sources_into_observation(
                    conn=conn,
                    fq_table=fq_table,
                    bank_id=bank_id,
                    observation_id=str(twin),
                    expected_text="Twin text.",
                    merged_text="Twin text.",
                    source_memory_ids=[mine, foreign],
                    bounds=_TemporalBounds(),
                    embeddings=None,
                    preserve_tag_prefixes=["pin:"],
                )
            assert ok
            assert await _tags(conn, twin) == ["scope:mine", "pin:mine"]
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)
        await memory.delete_bank(other, request_context=request_context)


async def _update_fold(memory, request_context, prefixes):
    bank_id = await _new_bank(memory, request_context, "update")
    try:
        async with memory._pool.acquire() as conn:
            twin = await _insert(conn, bank_id, "Twin text.", "observation", ["keep:base"])
            src = await _insert(conn, bank_id, "Source.", "world", ["pin:critical", "plain:x"])
            upd = await _insert(
                conn, bank_id, "Updated text.", "observation", ["pin:forever", "plain:z"], source_ids=[src]
            )
            outcome = _DedupOutcome(best_id=str(twin), merged_text="Merged.", should_merge=True, best_text="Twin text.")
            async with conn.transaction():
                folded = await _apply_dedup_update_fold(
                    conn, _ENGINE, bank_id, _config(prefixes), outcome, str(upd), "Updated text."
                )
            assert folded is True
            gone = await conn.fetchval("SELECT count(*) FROM memory_units WHERE id = $1", upd)
            assert gone == 0
            return await _tags(conn, twin)
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
async def test_update_fold_off_leaves_tags_unchanged(memory, request_context):
    assert await _update_fold(memory, request_context, None) == ["keep:base"]


@pytest.mark.asyncio
async def test_update_fold_on_keeps_deleted_row_and_source_protected_tags(memory, request_context):
    assert await _update_fold(memory, request_context, ["pin:"]) == [
        "keep:base",
        "pin:forever",
        "pin:critical",
    ]
