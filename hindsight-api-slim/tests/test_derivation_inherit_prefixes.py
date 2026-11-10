"""Derivation keepers inherit the protected tag prefixes of their source rows (T-117 fix A).

A consolidation CREATE mints a brand-new observation whose tags come from the batch scope
tags, so a protection tier carried only by the source rows (retain:long, retain:permanent)
is lost the moment the keeper is written. Nothing downstream restores it: the tier classifier
selects candidates on mentioned_at and the protection sweep only diffs against prior-day
snapshots, so a row never tagged when first written stays untagged.

The create path must use the bank's EFFECTIVE config resolved by the consolidation pass, not
the process-global raw env accessor (R2). These tests pass an explicit bank config while the
global raw value is set oppositely, so a regression to the global reader fails.

They drive the real create path against a real database but without the memory engine's
embedder: the create path takes pre-embedded text, so passing embedding_str=None exercises
exactly the tag logic under test.
"""

import copy
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
    """The create path reads only _backend.ops off the engine; the store is process-wide."""

    _backend = _StubBackend()


@pytest_asyncio.fixture
async def conn(pg0_db_url):
    """A raw connection to the pg0 test database, closed at the end of the test."""
    connection = await asyncpg.connect(pg0_db_url)
    yield connection
    await connection.close()


def _bank_config(prefixes):
    """A copy of the effective config with the bank's retain protection set."""
    cfg = copy.copy(_get_raw_config())
    cfg.retain_preserve_tag_prefixes = prefixes
    return cfg


@pytest.fixture
def preserve_prefixes():
    return _bank_config(["retain:"])


@pytest.fixture
def preserve_prefixes_off():
    return _bank_config(None)


@pytest.fixture
def global_off():
    """Set the process-global raw value off while a bank opts in."""
    cfg = _get_raw_config()
    previous = cfg.retain_preserve_tag_prefixes
    cfg.retain_preserve_tag_prefixes = None
    yield
    cfg.retain_preserve_tag_prefixes = previous


@pytest.fixture
def global_on():
    """Set the process-global raw value on while a bank disables or narrows it."""
    cfg = _get_raw_config()
    previous = cfg.retain_preserve_tag_prefixes
    cfg.retain_preserve_tag_prefixes = ["retain:"]
    yield
    cfg.retain_preserve_tag_prefixes = previous


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


async def _create_keeper(conn, bank_id: str, source_ids: list[uuid.UUID], text: str, config) -> dict:
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
            config=config,
        )


async def _keeper_tags(conn, bank_id: str) -> list[str]:
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
    bank_id = f"test-derive-inherit-long-{uuid.uuid4().hex[:8]}"
    source = await _seed_source_fact(conn, bank_id, ["retain:long"])

    created = await _create_keeper(conn, bank_id, [source], "Alice keeps a long-term ledger.", preserve_prefixes)
    assert created["action"] == "created"
    assert await _keeper_tags(conn, bank_id) == [SCOPE_TAG, "retain:long"]
    await _cleanup(conn, bank_id)


@pytest.mark.asyncio
async def test_keeper_inherits_retain_permanent(conn, preserve_prefixes):
    bank_id = f"test-derive-inherit-perm-{uuid.uuid4().hex[:8]}"
    source = await _seed_source_fact(conn, bank_id, ["retain:permanent"])

    created = await _create_keeper(conn, bank_id, [source], "Bob security posture is permanent.", preserve_prefixes)
    assert created["action"] == "created"
    assert await _keeper_tags(conn, bank_id) == [SCOPE_TAG, "retain:permanent"]
    await _cleanup(conn, bank_id)


@pytest.mark.asyncio
async def test_keeper_copies_only_protected_prefixes(conn, preserve_prefixes):
    bank_id = f"test-derive-inherit-only-{uuid.uuid4().hex[:8]}"
    first = await _seed_source_fact(conn, bank_id, ["retain:long", "topic:ops"])
    second = await _seed_source_fact(conn, bank_id, ["retain:permanent", "decay:prune-candidate"])

    created = await _create_keeper(
        conn, bank_id, [first, second], "Carol work spans the long and the permanent.", preserve_prefixes
    )
    assert created["action"] == "created"

    tags = await _keeper_tags(conn, bank_id)
    assert tags == [SCOPE_TAG, "retain:long", "retain:permanent"]
    assert "topic:ops" not in tags, "a non-protected source tag must not be copied"
    assert "decay:prune-candidate" not in tags, "a non-protected source tag must not be copied"
    await _cleanup(conn, bank_id)


@pytest.mark.asyncio
async def test_gate_off_leaves_keeper_tags_unchanged(conn, preserve_prefixes_off):
    bank_id = f"test-derive-inherit-off-{uuid.uuid4().hex[:8]}"
    source = await _seed_source_fact(conn, bank_id, ["retain:long"])

    with patch.object(
        consolidator,
        "_protected_prefix_tags_of_sources",
        side_effect=AssertionError("gate off must not read source tags"),
    ):
        created = await _create_keeper(
            conn, bank_id, [source], "Dave tier is irrelevant while the gate is off.", preserve_prefixes_off
        )
    assert created["action"] == "created"
    assert await _keeper_tags(conn, bank_id) == [SCOPE_TAG]
    await _cleanup(conn, bank_id)


@pytest.mark.asyncio
async def test_bank_on_global_off(conn, global_off):
    bank_id = f"test-derive-bank-on-{uuid.uuid4().hex[:8]}"
    source = await _seed_source_fact(conn, bank_id, ["retain:long"])
    bank = _bank_config(["retain:"])

    created = await _create_keeper(conn, bank_id, [source], "Bank opts in while global is off.", bank)
    assert created["action"] == "created"
    assert await _keeper_tags(conn, bank_id) == [SCOPE_TAG, "retain:long"]
    await _cleanup(conn, bank_id)


@pytest.mark.asyncio
async def test_bank_off_global_on(conn, global_on):
    bank_id = f"test-derive-bank-off-{uuid.uuid4().hex[:8]}"
    source = await _seed_source_fact(conn, bank_id, ["retain:long"])
    bank = _bank_config(None)

    created = await _create_keeper(conn, bank_id, [source], "Bank disables while global is on.", bank)
    assert created["action"] == "created"
    assert await _keeper_tags(conn, bank_id) == [SCOPE_TAG]
    await _cleanup(conn, bank_id)


@pytest.mark.asyncio
async def test_bank_prefixes_differ_from_global(conn, global_on):
    bank_id = f"test-derive-bank-diff-{uuid.uuid4().hex[:8]}"
    source = await _seed_source_fact(conn, bank_id, ["retain:long", "decision:approved"])
    bank = _bank_config(["decision:"])

    created = await _create_keeper(conn, bank_id, [source], "Bank protects a different prefix.", bank)
    assert created["action"] == "created"
    tags = await _keeper_tags(conn, bank_id)
    assert tags == [SCOPE_TAG, "decision:approved"]
    assert "retain:long" not in tags, "the global retain prefix must not leak into the bank"
    await _cleanup(conn, bank_id)
