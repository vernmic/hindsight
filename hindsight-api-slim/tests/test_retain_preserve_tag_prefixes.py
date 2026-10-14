"""Bank-gated tag-prefix preservation on retain (``retain_preserve_tag_prefixes``).

Some tags are written onto a document or onto individual memory units AFTER extraction:
a gardener marks a unit ``pin:critical``, an operator pins a document ``pin:doc``.
Upstream re-retain semantics replace a survivor's tags with the submission's document tags,
so the next replace-mode re-retain of the same document silently drops them. With the
bank's ``retain_preserve_tag_prefixes`` set (default off), tags under those prefixes
survive the retain write paths:

* a replace-mode (delta) re-pin: each SURVIVING unit keeps its OWN protected tags, after
  the new document tags. Nothing spreads to sibling units or to freshly extracted ones;
* the document row keeps its protected tags on an in-place upsert and, through a full
  replacement that deletes and re-inserts the row (including one that sends no tags), by
  carrying them forward at document level only;
* the explicit document tags update stays an exact replace, so a deliberate retag or
  removal is always possible;
* rescope detection still compares against what each unit ends with.

Append re-retains keep every survivor's whole tag set unconditionally; that is covered by
``test_retain_append_preserves_unit_tags.py``.
"""

import uuid
from datetime import datetime, timezone

import pytest

from hindsight_api.engine.providers.mock_llm import MockLLM
from hindsight_api.engine.retain.fact_storage import protected_tags


@pytest.fixture(scope="session")
def db_url():
    """Isolated pg0 instance so this module never touches the dev database."""
    return "pg0://hspreservetags:55472"


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


_A = "Alpha " * 20
_B = "Bravo " * 20
_C = "Charlie " * 20

_STAMP = datetime(2020, 1, 1, tzinfo=timezone.utc)

_PER_UNIT = [
    ["pin:critical", "label:one"],
    ["pin:reference", "label:two"],
]


# ---------------------------------------------------------------------------
# protected_tags: literal prefix match, the same on every store
# ---------------------------------------------------------------------------


def test_protected_tags_is_a_literal_prefix_match():
    tags = ["pin:x", "pi_n:y", "piXn:z", "re%:w", "rexx:q", "label:one"]
    assert protected_tags(tags, ["pin:"]) == ["pin:x"]
    # `_` and `%` are not wildcards: each prefix matches only itself.
    assert protected_tags(tags, ["pi_n:"]) == ["pi_n:y"]
    assert protected_tags(tags, ["re%"]) == ["re%:w"]
    assert protected_tags(tags, ["pin:", "tier:"]) == ["pin:x", "label:one"]


def test_protected_tags_off_and_empty():
    assert protected_tags(["pin:x"], None) == []
    assert protected_tags(["pin:x"], []) == []
    assert protected_tags(["pin:x"], [""]) == []  # an empty prefix never means "everything"
    assert protected_tags(None, ["pin:"]) == []


# ---------------------------------------------------------------------------
# Engine-level, against pg0
# ---------------------------------------------------------------------------


async def _bank(memory, request_context, name, *, gate):
    bank_id = f"test_preserve_{name}_{datetime.now(timezone.utc).timestamp()}"
    await memory._ensure_bank_exists(bank_id, request_context)
    cfg = {"retain_chunk_size": 60}
    if gate is not None:
        cfg["retain_preserve_tag_prefixes"] = gate
    await memory._config_resolver.update_bank_config(bank_id, cfg)
    return bank_id


async def _units(memory, bank_id, document_id):
    async with memory._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, tags, consolidated_at FROM memory_units "
            "WHERE bank_id = $1 AND document_id = $2 AND fact_type IN ('world', 'experience')",
            bank_id,
            document_id,
        )
    return {str(r["id"]): {"tags": list(r["tags"] or []), "consolidated_at": r["consolidated_at"]} for r in rows}


async def _doc_tags(memory, bank_id, document_id):
    async with memory._pool.acquire() as conn:
        tags = await conn.fetchval("SELECT tags FROM documents WHERE bank_id = $1 AND id = $2", bank_id, document_id)
    return list(tags or [])


async def _annotate(memory, bank_id, document_id, doc_extra, units):
    """Simulate the post-extraction tagger: per-unit tags plus a document-level pin."""
    async with memory._pool.acquire() as conn:
        for i, uid in enumerate(sorted(units)):
            await conn.execute(
                "UPDATE memory_units SET tags = tags || $2::varchar[] WHERE id = $1",
                uuid.UUID(uid),
                _PER_UNIT[i % len(_PER_UNIT)],
            )
        await conn.execute(
            "UPDATE documents SET tags = tags || $3::varchar[] WHERE bank_id = $1 AND id = $2",
            bank_id,
            document_id,
            doc_extra,
        )


async def _retain(memory, bank_id, request_context, **item):
    await memory.retain_batch_async(bank_id=bank_id, contents=[item], request_context=request_context)


async def _seed(memory, bank_id, doc, request_context):
    await _retain(memory, bank_id, request_context, content=_A + "\n\n" + _B, document_id=doc, tags=["topic:one"])
    first = await _units(memory, bank_id, doc)
    assert len(first) >= 2, first
    await _annotate(memory, bank_id, doc, ["pin:doc", "note:x"], first)
    return await _units(memory, bank_id, doc)


@pytest.mark.asyncio
@pytest.mark.timeout(600)
@pytest.mark.parametrize("gate", [["pin:"], None], ids=["gate_on", "gate_off"])
async def test_replace_rerentain_survivors_keep_only_their_own_protected_tags(memory, request_context, gate):
    bank_id = await _bank(memory, request_context, "delta", gate=gate)
    doc = "doc"
    try:
        before = await _seed(memory, bank_id, doc, request_context)

        # Replace-mode delta: chunk A survives, chunk B is replaced by C, tags change.
        await _retain(memory, bank_id, request_context, content=_A + "\n\n" + _C, document_id=doc, tags=["topic:two"])
        after = await _units(memory, bank_id, doc)
        survivors = {u: v for u, v in after.items() if u in before}
        fresh = {u: v for u, v in after.items() if u not in before}
        assert survivors and fresh

        for uid, unit in survivors.items():
            own_protected = [t for t in before[uid]["tags"] if t.startswith("pin:")]
            expected = ["topic:two", *own_protected] if gate else ["topic:two"]
            # Exact list: document tags first, then this unit's own protected tags only.
            assert unit["tags"] == expected, (uid, before[uid]["tags"], unit["tags"])
        for uid, unit in fresh.items():
            assert unit["tags"] == ["topic:two"], (uid, unit["tags"])

        # Document row: protected pin kept on the in-place upsert, the rest replaced.
        assert await _doc_tags(memory, bank_id, doc) == (["topic:two", "pin:doc"] if gate else ["topic:two"])
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.timeout(600)
@pytest.mark.parametrize("gate", [["pin:"], None], ids=["gate_on", "gate_off"])
async def test_tagless_full_replacement_carries_document_tags_only(memory, request_context, gate):
    bank_id = await _bank(memory, request_context, "full", gate=gate)
    doc = "doc"
    try:
        await _seed(memory, bank_id, doc, request_context)

        # Full replacement (row deleted and re-inserted), no tags field at all.
        await _retain(memory, bank_id, request_context, content=_A + "\n\n" + _C, document_id=doc, force_reextract=True)

        assert await _doc_tags(memory, bank_id, doc) == (["pin:doc"] if gate else [])
        # Every unit was re-extracted: none of them inherits the carried document pin or a
        # per-unit protected tag from the outgoing version.
        after = await _units(memory, bank_id, doc)
        assert after
        for uid, unit in after.items():
            assert unit["tags"] == [], (uid, unit["tags"])
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_only_configured_prefixes_are_preserved(memory, request_context):
    bank_id = await _bank(memory, request_context, "prefixes", gate=["tier:"])
    doc = "doc"
    try:
        before = await _seed(memory, bank_id, doc, request_context)
        await _retain(memory, bank_id, request_context, content=_A + "\n\n" + _C, document_id=doc, tags=["topic:two"])
        after = await _units(memory, bank_id, doc)
        for uid in set(before) & set(after):
            own_tier = [t for t in before[uid]["tags"] if t.startswith("tier:")]
            assert after[uid]["tags"] == ["topic:two", *own_tier], (uid, after[uid]["tags"])
        assert await _doc_tags(memory, bank_id, doc) == ["topic:two"]
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_explicit_document_tag_update_is_an_exact_replace(memory, request_context):
    """The tags PATCH is the deliberate retag: it can remove protected tags, gate or not."""
    bank_id = await _bank(memory, request_context, "patch", gate=["pin:"])
    doc = "doc"
    try:
        await _seed(memory, bank_id, doc, request_context)
        assert await memory.update_document(doc, bank_id, tags=["topic:new"], request_context=request_context)
        assert await _doc_tags(memory, bank_id, doc) == ["topic:new"]
        for uid, unit in (await _units(memory, bank_id, doc)).items():
            assert unit["tags"] == ["topic:new"], (uid, unit["tags"])
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_rescope_tracks_the_tags_units_end_with(memory, request_context):
    """Same document tags + kept protected tags: no rescope. New document tags: rescope."""
    bank_id = await _bank(memory, request_context, "rescope", gate=["pin:"])
    doc = "doc"
    try:
        before = await _seed(memory, bank_id, doc, request_context)
        # Bring the stored tags back to what the gate will compute (doc tags + own protected),
        # then stamp every unit consolidated at a fixed past time: a requeue clears that stamp
        # (and the consolidation run after the retain may then set a fresh one).
        async with memory._pool.acquire() as conn:
            for uid, unit in before.items():
                keep = ["topic:one", *[t for t in unit["tags"] if t.startswith("pin:")]]
                await conn.execute(
                    "UPDATE memory_units SET tags = $2, consolidated_at = $3 WHERE id = $1",
                    uuid.UUID(uid),
                    keep,
                    _STAMP,
                )

        # Re-retain identical content and tags: nothing moved, nothing is requeued.
        await _retain(memory, bank_id, request_context, content=_A + "\n\n" + _B, document_id=doc, tags=["topic:one"])
        same = await _units(memory, bank_id, doc)
        assert set(same) == set(before)
        assert all(u["consolidated_at"] == _STAMP for u in same.values()), same

        # Now the document tags change: every survivor is rescoped and requeued.
        await _retain(memory, bank_id, request_context, content=_A + "\n\n" + _B, document_id=doc, tags=["topic:two"])
        moved = await _units(memory, bank_id, doc)
        assert set(moved) == set(before)
        for uid, unit in moved.items():
            assert unit["tags"][0] == "topic:two" and "topic:one" not in unit["tags"], unit["tags"]
            assert any(t.startswith("pin:") for t in unit["tags"]), unit["tags"]
            assert unit["consolidated_at"] != _STAMP, (uid, unit)
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)
