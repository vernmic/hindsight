"""Append must not rewrite the tags of a document's surviving memory units.

``update_mode="append"`` adds content to a stable document (session transcripts:
OpenClaw and Hermes both retain every turn this way). The request's tags belong to
the units that submission INSERTS. Units that already exist were classified on
their own - by extraction, by a tagger, by an operator - and must come out of the
append with exactly the tag set they went in with, whatever the append carried:
different tags, no ``tags`` field, or ``tags=[]``. New units get the request's
tags and nothing borrowed from their older siblings.

This is unconditional: it does not depend on any tag-preservation setting.
"""

import uuid
from datetime import datetime, timezone

import pytest

from hindsight_api.engine.providers.mock_llm import MockLLM


@pytest.fixture(scope="session")
def db_url():
    """Isolated pg0 instance so this module never touches the dev database."""
    return "pg0://promotionappendtags:55479"


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

# Unit-specific annotations a classifier adds after extraction. Deliberately a mix of
# independently assigned classifications: preservation on append covers the whole set.
_ANNOTATIONS = [
    ["status:reviewed", "priority:high", "kind:decision"],
    ["status:draft", "priority:low", "confidence:high"],
]


async def _units(memory, bank_id, document_id, request_context):
    res = await memory.list_memory_units(bank_id, document_id=document_id, limit=200, request_context=request_context)
    return {u["id"]: set(u.get("tags") or []) for u in res["items"]}


async def _annotate(memory, unit_tags: dict[str, list[str]]):
    async with memory._pool.acquire() as conn:
        for unit_id, tags in unit_tags.items():
            await conn.execute("UPDATE memory_units SET tags = $2 WHERE id = $1", uuid.UUID(unit_id), tags)


def _append_item(content, document_id, tags_mode):
    item = {"content": content, "document_id": document_id, "update_mode": "append"}
    if tags_mode == "different":
        item["tags"] = ["topic:two"]
    elif tags_mode == "empty":
        item["tags"] = []
    return item


@pytest.mark.asyncio
@pytest.mark.timeout(600)
@pytest.mark.parametrize("tags_mode", ["different", "omitted", "empty"])
async def test_append_keeps_each_survivor_exact_tag_set(memory, request_context, tags_mode):
    bank_id = f"test_append_tags_{tags_mode}_{datetime.now(timezone.utc).timestamp()}"
    doc = "session-doc"
    try:
        await memory._ensure_bank_exists(bank_id, request_context)
        await memory._config_resolver.update_bank_config(bank_id, {"retain_chunk_size": 60})

        await memory.retain_batch_async(
            bank_id=bank_id,
            contents=[{"content": _A + "\n\n" + _B, "document_id": doc, "tags": ["topic:one"]}],
            request_context=request_context,
        )
        first = await _units(memory, bank_id, doc, request_context)
        assert len(first) >= 2, first

        # Each unit gets its own, different classification on top of the document tag.
        annotated = {
            uid: ["topic:one", *_ANNOTATIONS[i % len(_ANNOTATIONS)]] for i, uid in enumerate(sorted(first))
        }
        await _annotate(memory, annotated)
        before = await _units(memory, bank_id, doc, request_context)
        assert before == {uid: set(t) for uid, t in annotated.items()}

        await memory.retain_batch_async(
            bank_id=bank_id,
            contents=[_append_item(_C, doc, tags_mode)],
            request_context=request_context,
        )
        after = await _units(memory, bank_id, doc, request_context)

        survivors = {uid: t for uid, t in after.items() if uid in before}
        fresh = {uid: t for uid, t in after.items() if uid not in before}
        assert survivors, "append re-extracted every old chunk - nothing survived to check"
        assert fresh, "append extracted nothing new"

        # Every surviving unit keeps exactly its own set: nothing dropped, nothing added.
        for uid, tags in survivors.items():
            assert tags == before[uid], f"{tags_mode}: survivor {uid} {sorted(before[uid])} -> {sorted(tags)}"

        # New units carry the request's tags only - no old classification spreads to them.
        expected_new = {"topic:two"} if tags_mode == "different" else set()
        for uid, tags in fresh.items():
            assert tags == expected_new, f"{tags_mode}: new unit {uid} got {sorted(tags)}"
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)
