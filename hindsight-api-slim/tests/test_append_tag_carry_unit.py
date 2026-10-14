"""Pure unit tests for the append fallback tag mapping (``retain.append_tag_carry``)."""

from hindsight_api.engine.retain.append_tag_carry import (
    AppendTagCarry,
    OldDocumentSnapshot,
    OldUnit,
    locate_spans,
    new_chunk_spans,
)

BASE = "Alpha team picked Redis for caching.\nBeta team chose Postgres for reports."
NEW = BASE + "\nGamma team ships Friday."


def _carry(units, chunks, new_chunks, base=BASE, combined=NEW, labels=None):
    return AppendTagCarry(
        base_text=base,
        new_spans=new_chunk_spans(base, combined, new_chunks),
        snapshot=OldDocumentSnapshot(units=units, chunks=chunks),
        label_tag_keys=labels,
    )


OLD_CHUNKS = {0: "Alpha team picked Redis for caching.", 1: "Beta team chose Postgres for reports."}
U0 = OldUnit("Alpha team uses Redis for caching", "world", ("topic:one", "retain:permanent"), 0)
U1 = OldUnit("Beta team uses Postgres for reports", "world", ("topic:one", "kind:decision"), 1)


def test_locate_spans_in_order():
    assert locate_spans(["ab", "cd", "zz"], "abcd") == [(0, 2), (2, 4), None]


def test_exact_text_inherits_and_drops_request_tags():
    c = _carry([U0, U1], OLD_CHUNKS, [BASE, "Gamma team ships Friday."])
    assert c.tags_for("Beta team uses Postgres for reports", 0, ["topic:new"]) == ["topic:one", "kind:decision"]


def test_new_text_keeps_only_request_tags():
    c = _carry([U0, U1], OLD_CHUNKS, [BASE, "Gamma team ships Friday."])
    assert c.tags_for("Gamma team ships on Friday", 1, ["topic:new"]) == ["topic:new"]


def test_overlap_picks_best_jaccard_within_old_chunk():
    c = _carry(
        [U0, U1],
        OLD_CHUNKS,
        ["Alpha team picked Redis for caching.", "Beta team chose Postgres for reports.\nGamma team ships Friday."],
    )
    assert set(c.tags_for("Alpha team uses Redis for its caching", 0, ["topic:new"])) == {
        "topic:one",
        "retain:permanent",
    }


def test_no_clear_match_keeps_request_tags_only():
    c = _carry(
        [U0, U1],
        OLD_CHUNKS,
        ["Alpha team picked Redis for caching.", "Beta team chose Postgres for reports.\nGamma team ships Friday."],
    )
    assert c.tags_for("Something unrelated entirely", 0, ["topic:new"]) == ["topic:new"]


def test_straddling_chunk_without_match_keeps_request_tags_only():
    c = _carry(
        [U0, U1],
        OLD_CHUNKS,
        ["Alpha team picked Redis for caching.", "Beta team chose Postgres for reports.\nGamma team ships Friday."],
    )
    assert c.tags_for("Gamma ships", 1, ["topic:new"]) == ["topic:new"]
    assert c.tags_for("Gamma ships", 1, []) == []


def test_label_tags_are_not_inherited():
    u = OldUnit("Alpha team uses Redis for caching", "world", ("topic:one", "person:alice"), 0)
    c = _carry([u], OLD_CHUNKS, [BASE, "Gamma team ships Friday."], labels={"person"})
    assert set(c.tags_for("Alpha team uses Redis for caching", 0, ["topic:new", "person:bob"])) == {
        "topic:one",
        "person:bob",
    }


def test_unknown_positions_fall_back_to_text_similarity():
    c = _carry([U0, U1], OLD_CHUNKS, ["x"], combined='[{"different": "body"}]')
    assert set(c.tags_for("Beta team uses Postgres for its reports", 0, ["topic:new"])) == {
        "topic:one",
        "kind:decision",
    }
    assert c.tags_for("unrelated", 0, ["topic:new"]) == ["topic:new"]
    assert c.tags_for("unrelated", 0, []) == []


def test_assign_never_spreads_orphan_tags():
    c = _carry([U0, U1], OLD_CHUNKS, [BASE, "Gamma team ships Friday."])
    out = c.assign([("Beta team uses Postgres for reports", 0, ["topic:new"]), ("Gamma ships", 1, ["topic:new"])])
    assert out == [["topic:one", "kind:decision"], ["topic:new"]]


def test_ambiguous_tie_between_differently_tagged_units_does_not_carry():
    u0 = OldUnit("team uses Redis", "world", ("retain:permanent",), 0)
    u1 = OldUnit("team uses Redis", "world", ("target:storage",), 1)
    c = _carry([u0, u1], OLD_CHUNKS, ["x"], combined="[]")
    assert c.tags_for("team uses Redis", 0, ["topic:new"]) == ["topic:new"]


# Hermes HS2 leak, 2026-10-14: JSON message-array session doc, positions unknown.
_H_BASE = ["kind:fact", "scope:hermes-ops", "domain:knowledge", "confidence:provisional", "source:session"]
_HA = OldUnit(
    "The Quillfeather deploy window is every Tuesday at 14:00 UTC.",
    "world",
    tuple(["session:hs2acc", *_H_BASE, "retain:permanent", "task:release-ops", "kind:decision"]),
    0,
)
_HB = OldUnit(
    "The Larkspur storage bucket keeps objects for 30 days.",
    "world",
    tuple(["session:hs2acc", *_H_BASE, "target:storage", "value:high"]),
    0,
)
_NEW_FACTS = [
    "The Wrenfield cron job runs at 03:00 UTC.",
    "The Brindle dashboard refreshes every 5 minutes.",
    "The Osprey queue is drained nightly.",
]


import pytest  # noqa: E402


@pytest.mark.parametrize(
    "request_tags",
    [["session:hs2acc-2", *_H_BASE], [], None],
    ids=["different_tags", "empty_tags", "omitted_tags"],
)
def test_hermes_json_append_new_facts_get_only_request_tags(request_tags):
    c = AppendTagCarry(
        base_text='[{"role": "user", "content": "A"}]',
        new_spans=new_chunk_spans('[{"role": "user", "content": "A"}]', '[{"role": "user", "content": "A"}, {}]', ["x"]),
        snapshot=OldDocumentSnapshot(units=[_HA, _HB], chunks={0: "x"}),
    )
    current = list(request_tags or [])
    out = c.assign([(_HA.text, 0, current), (_HB.text, 0, current)] + [(f, 0, current) for f in _NEW_FACTS])
    assert set(out[0]) == set(_HA.tags)
    assert set(out[1]) == set(_HB.tags)
    for tags in out[2:]:
        assert tags == current


def test_no_old_units_is_a_no_op():
    c = _carry([], {}, [BASE])
    assert c.assign([("anything", 0, ["t"])]) == [["t"]]
