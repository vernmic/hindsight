"""``_recall_request_kwargs`` is the one override point for recall arguments.

The default build is the one ``_recall`` used to inline. Prefetch passes
``origin="auto"``; ``hindsight_recall`` passes ``origin="tool"``.
"""

import json

import hindsight_hermes as plugin
from conftest import FakeClient


def _pre_change_recall_kwargs(instance, query: str) -> dict:
    """The arguments ``_recall`` built inline, before this method existed."""
    kwargs: dict = {"query": query, "budget": instance._budget, "max_tokens": instance._recall_max_tokens}
    if instance._recall_tags:
        kwargs.update(tags=instance._recall_tags, tags_match=instance._recall_tags_match)
    if instance._recall_types:
        kwargs["types"] = instance._recall_types
    if instance._recall_min_scores:
        kwargs["min_scores"] = instance._recall_min_scores
    return kwargs


def _request_kwargs(call: dict) -> dict:
    """Drop ``bank_id``, which ``_recall`` adds beside the builder's arguments."""
    return {key: value for key, value in call.items() if key != "bank_id"}


def _started(cls, hermes_env, monkeypatch, config: dict, client: FakeClient):
    config_path = hermes_env / "hindsight" / "config.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps({"mode": "cloud", "apiKey": "test-key", **config}))
    instance = cls()
    monkeypatch.setattr(instance, "_new_cloud_client", lambda: client)
    monkeypatch.setattr(plugin, "_check_api_supports_update_mode_append", lambda *a, **k: True)
    instance.initialize("session-1")
    return instance


def test_default_builder_matches_the_pre_change_construction_on_both_paths(provider):
    configs = (
        {"recall_sync": True},
        {
            "recall_sync": True,
            "bank_id": "team",
            "recall_budget": "high",
            "recall_max_tokens": 128,
            "recall_tags": "team, project-x",
            "recall_tags_match": "all",
            "recall_types": "world, experience",
            "recall_min_scores": {"reranker": 0.25},
        },
    )
    for config in configs:
        instance, fake = provider(config, client=FakeClient(recall_texts=["fact"]))
        query = "who am I?"
        instance.handle_tool_call("hindsight_recall", {"query": query})
        instance.prefetch(query)
        expected = _pre_change_recall_kwargs(instance, query)
        assert len(fake.recalls) == 2
        for call in fake.recalls:
            assert call["bank_id"] == instance._bank_id
            assert list(_request_kwargs(call).items()) == list(expected.items())
        instance.shutdown()


def test_a_subclass_sees_auto_from_prefetch_and_tool_from_hindsight_recall(hermes_env, monkeypatch):
    seen: list[str] = []

    class Recording(plugin.HindsightMemoryProvider):
        def _recall_request_kwargs(self, query, *, origin):
            seen.append(origin)
            return super()._recall_request_kwargs(query, origin=origin)

    instance = _started(
        Recording, hermes_env, monkeypatch, {"recall_sync": True}, FakeClient(recall_texts=["fact"])
    )
    instance.prefetch("from prefetch")
    instance.handle_tool_call("hindsight_recall", {"query": "from the tool"})
    assert seen == ["auto", "tool"]
    instance.shutdown()


def test_an_override_returned_kwargs_reach_the_client_call(hermes_env, monkeypatch):
    custom = {"query": "narrowed", "budget": "low", "max_tokens": 3, "types": ["world"], "tags": ["override"]}

    class Overriding(plugin.HindsightMemoryProvider):
        def _recall_request_kwargs(self, query, *, origin):
            return dict(custom)

    client = FakeClient(recall_texts=["fact"])
    instance = _started(Overriding, hermes_env, monkeypatch, {"recall_sync": True, "bank_id": "team"}, client)
    instance.prefetch("ignored by the override")
    instance.handle_tool_call("hindsight_recall", {"query": "also ignored"})
    assert client.recalls
    for call in client.recalls:
        assert call["bank_id"] == "team"
        assert _request_kwargs(call) == custom
        assert _request_kwargs(call) != _pre_change_recall_kwargs(instance, "ignored by the override")
    instance.shutdown()
