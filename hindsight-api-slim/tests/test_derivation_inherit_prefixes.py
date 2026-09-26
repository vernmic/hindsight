"""Derivation keepers inherit the protected tag prefixes of their source rows (T-117 fix A).

A consolidation CREATE mints a brand-new observation whose tags come from the batch's scope
tags, so a protection tier carried only by the source rows (retain:long, retain:permanent) is
lost the moment the keeper is written. Nothing downstream restores it: the tier classifier
selects candidates on mentioned_at and the protection sweep only diffs against prior-day
snapshots, so a row never tagged when first written stays untagged.

These tests pin the create-path union behind HINDSIGHT_API_RETAIN_PRESERVE_TAG_PREFIXES, that
only protected prefixes cross the boundary, and that the gate off leaves stock behaviour (and
issues no extra read).

They drive the real create path against a real database but without the memory engine's
embedder: the create path takes pre-embedded text, so passing embedding_str=None exercises
exactly the tag logic under test while keeping the module runnable on a host without the
optional local-ml stack.
"""

import uuid
from unittest.mock import patch

import asyncpg
import pytest
import pytest_asyncio

from hindsight_api.config import _get_raw_config
from hindsight_api.engine.consolidation import consolidator

#: The batch scope tag every keeper in these tests is created with.
SCOPE_TAG = "topic:scope"


class _StubOps:
    #: Postgres keeps observation sources in the source_memory_ids array, not a junction table.
    uses_observation_sources_table = False


class _StubBackend:
    ops = _StubOps()


class _StubEngine:
    """The create path reads only _backend.ops off the engine; the memories store comes from
    the process-wide get_memories(), which is the Postgres default here."""

    _backend = _StubBackend()


@pytest_asyncio.fixture
async def conn(pg0_db_url):
    """A raw connection to the pg0 test database, closed at the end of the test."""
    connection = await asyncpg.connect(pg0_db_url)
    yield connection
    await connection.close()


@pytest.fixture
def preserve_prefixes():
    """Pin the protected-prefix gate to retain: for one test, restoring the prior value after."""
    config = _get_raw_config()
    previous = config.retain_preserve_tag_prefixes
    config.retain_preserve_tag_prefixes = ["retain:"]
    yield config
    config.retain_preserve_tag_prefixes = previous


@pytest.fixture
def preserve_prefixes_off():
    """The gate's stock state: unset, so the create path must stay byte-identical."""
    config = _get_raw_config()
    previous = config.retain_preserve_tag_prefixes
    config.retain_preserve_tag_prefixes = None
    yield config
    config.retain_preserve_tag_prefixes = previous


async def _seed_source_fact(conn, bank_id: str, tags: list[str]) -> uuid.UUID:
    """Insert one plain source fact directly; the create path only needs the row to exist."""
    source_id = uuid.uuid4()
    await conn.execute(
        """
        INSERT INTO memory_units (id, bank_id, text, fact_type, tags, proof_count)
        VALUES ($1, $2, $3, 'experience', $4::varchar[], 1)
        """,
        source_id,
        bank_id,
        f"source fact {source_id}",
        tags,
    )
    return source_id


async def _create_keeper(conn, bank_id: str, source_ids: list[uuid.UUID], text: str) -> dict:
    """Mint one keeper from the given sources with only the scope tag, in its own transaction."""
    async with conn.transaction():
        return await consolidator._apply_create_observation(
            conn=conn,
            memory_engine=_StubEngine(),
            bank_id=bank_id,
            source_memory_ids=source_ids,
            observation_text=text,
            embedding_str=None,
            tags=[SCOPE_TAG],
        )


async def _keeper_tags(conn, bank_id: str) -> list[str]:
    """Tags of the single observation in the bank (there is exactly one per test)."""
    rows = await conn.fetch(
        "SELECT id, tags FROM memory_units WHERE bank_id = $1 AND fact_type = 'observation'",
        bank_id,
    )
    assert len(rows) == 1, f"expected exactly one keeper, got {[dict(r) for r in rows]}"
    return list(rows[0]["tags"] or [])


async def _cleanup(conn, bank_id: str) -> None:
    await conn.execute("DELETE FROM memory_units WHERE bank_id = $1", bank_id)


@pytest.mark.asyncio
async def test_keeper_inherits_retain_long(conn, preserve_prefixes):
    """A source row's retain:long must survive onto the keeper minted from it."""
    bank_id = f"test-derive-inherit-long-{uuid.uuid4().hex[:8]}"
    source = await _seed_source_fact(conn, bank_id, ["retain:long"])

    created = await _create_keeper(conn, bank_id, [source], "Alice keeps a long-term ledger.")
    assert created["action"] == "created"

    assert await _keeper_tags(conn, bank_id) == [SCOPE_TAG, "retain:long"]
    await _cleanup(conn, bank_id)


@pytest.mark.asyncio
async def test_keeper_inherits_retain_permanent(conn, preserve_prefixes):
    """A source row's retain:permanent must survive onto the keeper minted from it."""
    bank_id = f"test-derive-inherit-perm-{uuid.uuid4().hex[:8]}"
    source = await _seed_source_fact(conn, bank_id, ["retain:permanent"])

    created = await _create_keeper(conn, bank_id, [source], "Bob's security posture is permanent.")
    assert created["action"] == "created"

    assert await _keeper_tags(conn, bank_id) == [SCOPE_TAG, "retain:permanent"]
    await _cleanup(conn, bank_id)


@pytest.mark.asyncio
async def test_keeper_copies_only_protected_prefixes(conn, preserve_prefixes):
    """Non-protected source tags stay on their own rows; only protected prefixes cross over."""
    bank_id = f"test-derive-inherit-only-{uuid.uuid4().hex[:8]}"
    first = await _seed_source_fact(conn, bank_id, ["retain:long", "topic:ops"])
    second = await _seed_source_fact(conn, bank_id, ["retain:permanent", "decay:prune-candidate"])

    created = await _create_keeper(
        conn, bank_id, [first, second], "Carol's work spans the long and the permanent."
    )
    assert created["action"] == "created"

    tags = await _keeper_tags(conn, bank_id)
    assert tags == [SCOPE_TAG, "retain:long", "retain:permanent"]
    assert "topic:ops" not in tags, "a non-protected source tag must not be copied"
    assert "decay:prune-candidate" not in tags, "a non-protected source tag must not be copied"
    await _cleanup(conn, bank_id)


@pytest.mark.asyncio
async def test_gate_off_leaves_keeper_tags_unchanged(conn, preserve_prefixes_off):
    """With the gate unset the keeper keeps exactly the caller's tags and no source read happens."""
    bank_id = f"test-derive-inherit-off-{uuid.uuid4().hex[:8]}"
    source = await _seed_source_fact(conn, bank_id, ["retain:long"])

    with patch.object(
        consolidator,
        "_protected_prefix_tags_of_sources",
        side_effect=AssertionError("gate off must not read source tags"),
    ):
        created = await _create_keeper(conn, bank_id, [source], "Dave's tier is irrelevant while the gate is off.")
    assert created["action"] == "created"

    assert await _keeper_tags(conn, bank_id) == [SCOPE_TAG]
    await _cleanup(conn, bank_id)
