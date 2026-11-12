"""Append fallback tag carry is precise: no union spreading (Hermes HS2 leak, 2026-10-14).

Live on HS2 (v1.0.0-148f57cfe): a Hermes session document (a JSON message array, so the new
body is not a plain prefix extension of the stored one) had two facts annotated after
extraction - A with ``retain:permanent``/``task:release-ops``/``kind:decision``, B with
``target:storage``/``value:high``. Each later append re-extracted the whole document, and every
genuinely new fact (Wrenfield, Brindle, Osprey) came back with the UNION of A's and B's
labels, whatever tags the append sent (different, omitted, ``[]``) and whether the bank's
``retain_preserve_tag_prefixes`` gate was on or off.

Contract: a re-extracted unit inherits only the tags of the old unit it replaces; a unit from
genuinely new content gets only that request's tags (none when tags were omitted or ``[]``).
Asserted as exact per-unit tag sets after every append.
"""

import json
import re
from datetime import datetime, timezone

import pytest

from hindsight_api.engine.providers.mock_llm import MockLLM

from tests.ml_independent_fixtures import cross_encoder, embeddings  # noqa: F401


@pytest.fixture(scope="session")
def db_url():
    """Isolated pg0 instance so this module never touches the dev database."""
    return "pg0://hsappendprecision:55476"


_FIXTURE = re.compile(r"Fixture [A-Z]: [^\"\\\n]*?\.")


@pytest.fixture(autouse=True)
def one_fact_per_fixture_sentence(monkeypatch):
    """Deterministic extraction: one fact per distinct fixture sentence in the chunk, verbatim."""

    def _facts(messages):
        text = "\n".join(m.get("content", "") for m in messages if m.get("role") == "user")
        seen = []
        for s in _FIXTURE.findall(text):
            if s not in seen:
                seen.append(s)
        return {
            "facts": [
                {
                    "what": s,
                    "when": "N/A",
                    "where": "N/A",
                    "who": "N/A",
                    "why": "N/A",
                    "fact_kind": "conversation",
                    "fact_type": "world",
                }
                for s in seen
            ]
        }

    monkeypatch.setattr(MockLLM, "_build_mock_facts", staticmethod(_facts))


BASE = ["kind:fact", "scope:hermes-ops", "domain:knowledge", "confidence:provisional", "source:session"]
A = "Fixture A: the Quillfeather deploy window is every Tuesday at 14:00 UTC."
B = "Fixture B: the Larkspur storage bucket keeps objects for 30 days."
C = "Fixture C: the Wrenfield cron job runs at 03:00 UTC."
D = "Fixture D: the Brindle dashboard refreshes every 5 minutes."
E = "Fixture E: the Osprey queue is drained nightly."
A_EXTRA = ["retain:permanent", "task:release-ops", "kind:decision"]
B_EXTRA = ["target:storage", "value:high"]
_OMIT = object()


def _turn(*facts):
    msgs = []
    for f in facts:
        msgs += [{"role": "user", "content": f}, {"role": "assistant", "content": "Noted."}]
    return json.dumps(msgs)


async def _by_fixture(memory, bank_id, doc):
    async with memory._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT text, tags FROM memory_units WHERE bank_id=$1 AND document_id=$2 "
            "AND fact_type IN ('world','experience')",
            bank_id,
            doc,
        )
    out = {}
    for r in rows:
        m = re.search(r"Fixture ([A-Z])", r["text"])
        assert m, r["text"]
        assert m.group(1) not in out, f"duplicate unit for fixture {m.group(1)}"
        out[m.group(1)] = set(r["tags"] or [])
    return out


async def _retain(memory, bank_id, request_context, content, doc, tags, mode):
    item = {"content": content, "document_id": doc}
    if mode:
        item["update_mode"] = mode
    if tags is not _OMIT:
        item["tags"] = tags
    await memory.retain_batch_async(bank_id=bank_id, contents=[item], request_context=request_context)


@pytest.mark.asyncio
@pytest.mark.timeout(600)
@pytest.mark.parametrize("gate", [["retain:"], []], ids=["gate_on", "gate_off"])
async def test_append_new_facts_never_inherit_other_units_tags(memory, request_context, gate):
    bank_id = f"test_append_precision_{bool(gate)}_{datetime.now(timezone.utc).timestamp()}"
    doc = "hermes-session-doc"
    try:
        await memory._ensure_bank_exists(bank_id, request_context)
        await memory._config_resolver.update_bank_config(
            bank_id, {"retain_preserve_tag_prefixes": gate, "consolidation_preserve_tag_prefixes": gate}
        )
        first = ["session:one"] + BASE
        await _retain(memory, bank_id, request_context, _turn(A, B), doc, first, "append")
        units = await _by_fixture(memory, bank_id, doc)
        assert units == {"A": set(first), "B": set(first)}

        async with memory._pool.acquire() as conn:
            for text, extra in ((A, A_EXTRA), (B, B_EXTRA)):
                await conn.execute(
                    "UPDATE memory_units SET tags = tags || $3::varchar[] WHERE bank_id=$1 AND text LIKE $2",
                    bank_id,
                    text.split(":")[0] + ":%",
                    extra,
                )
        expected = {"A": set(first) | set(A_EXTRA), "B": set(first) | set(B_EXTRA)}
        assert await _by_fixture(memory, bank_id, doc) == expected

        steps = [
            ("C", C, ["session:two"] + BASE),  # different tags
            ("D", D, _OMIT),  # tags omitted
            ("E", E, []),  # explicit empty tags
        ]
        for key, fact, tags in steps:
            await _retain(memory, bank_id, request_context, _turn(fact), doc, tags, "append")
            expected[key] = set() if tags is _OMIT else set(tags)
            got = await _by_fixture(memory, bank_id, doc)
            assert got == expected, f"after appending {key}: {got} != {expected}"
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)