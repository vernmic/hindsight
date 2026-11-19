"""Structured recall shares the configured retrieval policy with ordinary recall (review F1),
the provider cap ceilings structured results (F5), and retain provenance is opt-in (F5).

The scenario is adapted from the reviewer's offline probe at
I:/hermes/reports/hindsight-integrations-review-20261118/probe-structured-policy.py.
"""

from __future__ import annotations

import types
from collections.abc import Callable


class Row:
    def __init__(self, row_id: str, text: str, tags: list[str], score: float):
        self.id = row_id
        self.text = text
        self.tags = tags
        self.occurred_start = None
        self.mentioned_at = None
        self.scores = {"semantic": score}
        self.metadata = {}

    def model_dump(self, **_kwargs):
        return vars(self)


class PolicyClient:
    """Applies the request's tags/min_scores like the real server, per bank; can fail one bank."""

    def __init__(self, rows: dict[str, list[Row]], fail_bank: str | None = None):
        self.rows = rows
        self.fail_bank = fail_bank
        self.recalls: list[dict] = []

    async def arecall(self, **kwargs):
        self.recalls.append(kwargs.copy())
        if kwargs["bank_id"] == self.fail_bank:
            raise RuntimeError("bank unavailable")
        out = list(self.rows.get(kwargs["bank_id"], []))
        if kwargs.get("tags"):
            out = [r for r in out if all(t in r.tags for t in kwargs["tags"])]
        if kwargs.get("tag_groups"):
            # Mirror the engine: every top-level group must match (tags_satisfy_groups
            # ANDs groups), so the fake exercises the narrowing rule honestly.
            # A group arrives as a dict (server leaf form) or a bare tag list.
            for group in kwargs["tag_groups"]:
                wanted = set(group.get("tags") or []) if isinstance(group, dict) else set(group)
                out = [r for r in out if wanted & set(r.tags)]
        if kwargs.get("min_scores"):
            out = [r for r in out if all(r.scores.get(k, 0) >= v for k, v in kwargs["min_scores"].items())]
        return types.SimpleNamespace(results=out)

    async def aclose(self):
        pass


ROWS = {
    "primary": [
        Row("weak", "weak", ["permitted"], 0.1),
        Row("denied", "wrong-tag", ["other"], 0.9),
        Row("good", "good", ["permitted"], 0.95),
    ],
    "extra": [Row("extra", "extra-good", ["permitted"], 0.99)],
}

POLICY = {
    "bank_id": "primary",
    "recall_tags": ["permitted"],
    "recall_tags_match": "all",
    "recall_types": ["observation"],
    "recall_min_scores": {"semantic": 0.8},
    "recall_additional_banks": ["extra"],
}


def _structured(provider: Callable, config: dict, feed: dict, client: PolicyClient | None = None):
    instance, _ = provider(config, client=client or PolicyClient(ROWS))
    try:
        return instance, instance.prefetch_candidates({"query": "q", "feed_hash": "h", **feed})
    except Exception:
        instance.shutdown()
        raise


def test_structured_recall_applies_configured_policy_and_banks(provider: Callable) -> None:
    # The probe scenario: permitted tag + 0.8 floor + observation types must filter
    # weak and wrong-tag rows, and the additional bank must be queried too.
    client = PolicyClient(ROWS)
    instance, result = _structured(provider, dict(POLICY), {}, client)
    try:
        assert [c["id"] for c in result["candidates"]] == ["primary/good", "extra/extra"]
        assert {call["bank_id"] for call in client.recalls} == {"primary", "extra"}
        for call in client.recalls:
            assert call["tags"] == ["permitted"]
            assert call["min_scores"] == {"semantic": 0.8}
            assert call["types"] == ["observation"]
    finally:
        instance.shutdown()


def test_feed_groups_narrow_within_configured_tags(provider: Callable) -> None:
    # The feed asks for the "other" tag, which would admit primary/denied - but the
    # configured [permitted] tags must still hold (AND), so nothing outside them
    # returns: a feed can never widen what the profile recalls.
    client = PolicyClient(ROWS)
    instance, result = _structured(provider, dict(POLICY), {"tag_groups": [["other"]]}, client)
    try:
        assert result["candidates"] == []
        for call in client.recalls:
            groups = call["tag_groups"]
            # The appended configured-tags leaf is a dict; the feed's own group may
            # be a bare list (the feed sent [["other"]] in this case).
            leaf_tags = [
                set(g.get("tags") or []) if isinstance(g, dict) else set(g) for g in groups
            ]
            assert {"permitted"} in leaf_tags
            assert "tags" not in call
            assert call["min_scores"] == {"semantic": 0.8}
    finally:
        instance.shutdown()


def test_feed_groups_and_configured_tags_both_hold(provider: Callable) -> None:
    # A row inside BOTH the feed's group and the configured tags is returned.
    rows = {"primary": [Row("both", "both", ["permitted", "extra"], 0.9)]}
    client = PolicyClient(rows)
    instance, result = _structured(provider, dict(POLICY), {"tag_groups": [["extra"]]}, client)
    try:
        assert [c["id"] for c in result["candidates"]] == ["primary/both"]
    finally:
        instance.shutdown()


def test_malformed_feed_numbers_are_coerced_not_fatal(provider: Callable) -> None:
    # A3: '' / null / non-numeric max_results and max_tokens read as absent
    # (uncapped depth, provider default tokens) instead of raising.
    client = PolicyClient(ROWS)
    instance, result = _structured(
        provider,
        dict(POLICY),
        {"max_results": "", "max_tokens": None},
        client,
    )
    try:
        assert [c["id"] for c in result["candidates"]] == ["primary/good", "extra/extra"]
        assert all(call["max_tokens"] == instance._recall_max_tokens for call in client.recalls)
    finally:
        instance.shutdown()

    client = PolicyClient(ROWS)
    instance, result = _structured(
        provider,
        dict(POLICY),
        {"max_results": "bogus", "max_tokens": "soon"},
        client,
    )
    try:
        assert len(result["candidates"]) == 2
    finally:
        instance.shutdown()


def test_absent_feed_override_keeps_configured_policy(provider: Callable) -> None:
    client = PolicyClient(ROWS)
    instance, _result = _structured(provider, dict(POLICY), {}, client)
    try:
        assert all(c.get("tags") == ["permitted"] and "tag_groups" not in c for c in client.recalls)
    finally:
        instance.shutdown()


def test_provider_cap_ceilings_structured_results(provider: Callable) -> None:
    config = dict(POLICY)
    config["recall_max_results"] = 1
    instance, result = _structured(provider, config, {})
    try:
        assert [c["id"] for c in result["candidates"]] == ["primary/good"]
    finally:
        instance.shutdown()


def test_feed_depth_applies_within_provider_cap(provider: Callable) -> None:
    config = dict(POLICY)
    config["recall_max_results"] = 3
    instance, result = _structured(provider, config, {"max_results": 1})
    try:
        assert len(result["candidates"]) == 1
    finally:
        instance.shutdown()


def test_uncapped_provider_cap_stays_uncapped(provider: Callable) -> None:
    instance, result = _structured(provider, dict(POLICY), {})
    try:
        assert len(result["candidates"]) == 2
    finally:
        instance.shutdown()


def test_partial_extra_bank_failure_keeps_primary_results(provider: Callable) -> None:
    client = PolicyClient(ROWS, fail_bank="extra")
    instance, result = _structured(provider, dict(POLICY), {}, client)
    try:
        assert [c["id"] for c in result["candidates"]] == ["primary/good"]
        assert result["error"] is None
        assert {e["bank"] for e in result["query_errors"]} == {"extra"}
    finally:
        instance.shutdown()


def test_duplicates_are_deduped_before_the_feed_cap(provider: Callable) -> None:
    rows = {"primary": [Row("dup", "dup", ["permitted"], 0.9), Row("good", "good", ["permitted"], 0.95), Row("dup", "dup", ["permitted"], 0.9)]}
    client = PolicyClient(rows)
    instance, result = _structured(provider, dict(POLICY), {"max_results": 2}, client)
    try:
        ids = [c["id"] for c in result["candidates"]]
        assert ids == ["primary/dup", "primary/good"]
    finally:
        instance.shutdown()


def test_row_without_id_is_skipped_not_fatal(provider: Callable) -> None:
    broken = Row(None, "broken", ["permitted"], 0.9)
    rows = {"primary": [broken, Row("good", "good", ["permitted"], 0.95)]}
    client = PolicyClient(rows)
    instance, result = _structured(provider, dict(POLICY), {}, client)
    try:
        assert [c["id"] for c in result["candidates"]] == ["primary/good"]
        assert result["query_errors"] == []
    finally:
        instance.shutdown()


def test_retain_provenance_is_off_by_default(provider: Callable) -> None:
    from conftest import FakeClient

    client = FakeClient()
    instance, _ = provider({"prefetch_waits_for_retain": False}, client=client)
    instance.sync_turn("first", "one")
    instance.sync_turn("second", "two")
    instance._retain_queue.join()
    instance.shutdown()
    tags = client.retains[0]["items"][0]["tags"]
    assert not any(t.startswith(("kind:", "scope:", "domain:", "confidence:", "source:")) for t in tags)
    assert "PREVIOUS TURN SOURCE DATA" not in (client.retains[0]["items"][0].get("context") or "")


def test_retain_provenance_opt_in_restores_dimensions(provider: Callable) -> None:
    from conftest import FakeClient

    client = FakeClient()
    instance, _ = provider(
        {"prefetch_waits_for_retain": False, "retain_provenance": True}, client=client
    )
    instance.sync_turn("first", "one")
    instance._retain_queue.join()
    instance.shutdown()
    tags = client.retains[0]["items"][0]["tags"]
    assert any(t.startswith("kind:") for t in tags)
    assert any(t.startswith("scope:") for t in tags)


def test_retain_provenance_opt_in_appends_previous_turn(provider: Callable) -> None:
    from conftest import FakeClient

    client = FakeClient()
    instance, _ = provider(
        {"prefetch_waits_for_retain": False, "retain_provenance": True}, client=client
    )
    instance.sync_turn("first", "one")
    instance._retain_queue.join()
    instance.sync_turn("second", "two")
    instance._retain_queue.join()
    instance.shutdown()
    context = client.retains[1]["items"][0].get("context") or ""
    assert "PREVIOUS TURN SOURCE DATA" in context
    assert "User: first" in context