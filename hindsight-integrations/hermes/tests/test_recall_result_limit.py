"""Count limits apply to the final recalled records, after floors and bank deduplication."""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest

from conftest import FakeClient, FakeRecallResponse


@pytest.mark.parametrize("configured", [None, 0, 2])
def test_cap_covers_tool_and_automatic_recall(provider: Callable, configured: int | None) -> None:
    config = {"recall_sync": True}
    if configured is not None:
        config["recall_max_results"] = configured
    instance, _ = provider(config, client=FakeClient(recall_texts=["one", "two", "three"]))
    try:
        expected = ["one", "two"] if configured == 2 else ["one", "two", "three"]
        tool = json.loads(instance.handle_tool_call("hindsight_recall", {"query": "q"}))["result"]
        injected = instance.prefetch("q")
        assert tool == "\n".join(f"{i}. {text}" for i, text in enumerate(expected, 1))
        assert all(text in injected for text in expected)
        assert ("three" in injected) == (configured != 2)
    finally:
        instance.shutdown()


def test_limit_follows_score_floors_and_cross_bank_dedup(provider: Callable) -> None:
    class Banks(FakeClient):
        async def arecall(self, *, bank_id: str, **kwargs: object) -> FakeRecallResponse:
            texts = [("weak", {"semantic": 0.1}), ("one", {"semantic": 0.9})]
            if bank_id != "primary":
                texts = [("one", {"semantic": 0.9}), ("two", {"semantic": 0.8}), ("three", {"semantic": 0.7})]
            return FakeRecallResponse(texts)

    instance, _ = provider(
        {
            "bank_id": "primary",
            "recall_additional_banks": ["vault"],
            "recall_min_scores": {"semantic": 0.5},
            "recall_max_results": 2,
        },
        client=Banks(),
    )
    try:
        assert [result.text for result in instance._recall("q")] == ["one", "two"]
    finally:
        instance.shutdown()
