"""Exercise the actual staged adapter with the installed plugin support modules."""

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def provider():
    root = Path(__file__).parents[4]
    support = root
    if not support.exists():
        pytest.skip("Installed Hindsight support modules are required for adapter validation")
    spec = importlib.util.spec_from_file_location(
        "hindsight_jev_validation", root / "__init__.py", submodule_search_locations=[str(support)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    obj = module.HindsightMemoryProvider()
    obj._bank_id, obj._budget, obj._recall_max_tokens = "hermes-ops", "mid", 4000
    obj._recall_disabled = lambda: False
    return obj


def test_current_feed_preserves_ids_filters_rescue_and_depth(provider):
    calls = []

    class Client:
        async def arecall(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                results=[
                    SimpleNamespace(
                        id=str(i),
                        text="fact " + str(i),
                        tags=["task:test"],
                        occurred_start="2026-10-06",
                        scores={"semantic": 0.9},
                    )
                    for i in range(1, 9)
                ]
            )

    provider._run_hindsight_operation = lambda operation: asyncio.run(operation(Client()))
    feed = {
        "query": "current task",
        "feed_hash": "frozen",
        "tag_groups": [{"tags": ["task:test"], "match": "any_strict"}],
        "unfiltered_rescue": True,
        "max_tokens": 2000,
        "max_results": 3,
    }
    result = provider.prefetch_candidates(feed)
    assert len(calls) == 2 and calls[0]["tag_groups"] == feed["tag_groups"]
    assert "tag_groups" not in calls[1] and sum(r["max_tokens"] for r in calls) == 2000
    assert result["feed_hash"] == "frozen"
    assert len(result["candidates"]) <= 3 and result["candidates"][0]["id"] == "hermes-ops/1"
    assert result["candidates"][0]["tags"] == ["task:test"]
    assert result["query_provenance"][1]["path"] == "unfiltered_rescue"
    # Uncapped results are not truncated by the existing provider's legacy five-record cap.
    result = provider.prefetch_candidates({"query": "current task", "max_results": 0})
    assert len(result["candidates"]) == 8


def test_failed_filtered_recall_keeps_unfiltered_rescue(provider):
    class Client:
        async def arecall(self, **kwargs):
            if kwargs.get("tag_groups"):
                raise ValueError("unsupported filter")
            return SimpleNamespace(results=[SimpleNamespace(id="one", text="rescue")])

    provider._run_hindsight_operation = lambda operation: asyncio.run(operation(Client()))
    result = provider.prefetch_candidates(
        {"query": "task", "tag_groups": [{"tags": ["task:test"], "match": "any_strict"}], "unfiltered_rescue": True}
    )
    assert result["error"] is None and result["candidates"][0]["text"] == "rescue"
    assert result["query_errors"] == [{"path": "filtered", "error": "ValueError"}]
    provider._bank_id = "openclaw"
    assert provider.prefetch_candidates({"query": "task"})["error"] == "bank_excluded"


def test_longer_structured_transport_uses_private_client_and_closes_it(provider, monkeypatch):
    calls = []

    class Client:
        async def arecall(self, **kwargs):
            calls.append(("recall", kwargs["query"]))
            return SimpleNamespace(results=[SimpleNamespace(id="one", text="useful")])

        async def aclose(self):
            calls.append(("closed",))

    provider._mode, provider._timeout = "cloud", 15
    original = provider._client
    monkeypatch.setattr(
        provider, "_new_cloud_client", lambda **kwargs: calls.append(("timeout", kwargs["timeout"])) or Client()
    )
    module = sys.modules[provider.__class__.__module__]
    monkeypatch.setattr(module, "_run_sync", lambda coro, timeout: asyncio.run(coro))
    result = provider.prefetch_candidates({"query": "task", "timeout_seconds": 30})
    assert result["candidates"][0]["text"] == "useful"
    assert calls == [("timeout", 30), ("recall", "task"), ("closed",)]
    assert provider._timeout == 15 and provider._client is original
