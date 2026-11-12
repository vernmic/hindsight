"""Config-driven exclusion lists replace shipped bank/cron literals (generic defaults).

Positive/negative coverage for both corrected sites plus the old-host fallback:
absent settings behave exactly as upstream (nothing excluded).
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from conftest import FakeClient


def _provider_with_bank(provider: Callable, bank_id: str, extra: dict | None = None):
    config = {"bank_id": bank_id, "recall_sync": True}
    config.update(extra or {})
    return provider(config, client=FakeClient(recall_texts=["hit"]))


def test_prefetch_excludes_only_configured_banks(provider: Callable) -> None:
    instance, _ = _provider_with_bank(provider, "special", {"prefetch_excluded_banks": ["special"]})
    try:
        result = instance.prefetch_candidates({"query": "q", "feed_hash": "h"})
        assert result["error"] == "bank_excluded"
        assert result["candidates"] == []
    finally:
        instance.shutdown()


def test_prefetch_not_excluded_when_bank_absent_from_list(provider: Callable) -> None:
    instance, _ = _provider_with_bank(provider, "other", {"prefetch_excluded_banks": ["special"]})
    try:
        result = instance.prefetch_candidates({"query": "q", "feed_hash": "h"})
        assert result["error"] != "bank_excluded"
    finally:
        instance.shutdown()


def test_prefetch_no_shipped_default_exclusions(provider: Callable) -> None:
    # Old-host fallback: with the setting absent, even historically hard-coded
    # bank names proceed -- shipped code carries NO deployment bank names.
    instance, _ = _provider_with_bank(provider, "main")
    try:
        result = instance.prefetch_candidates({"query": "q", "feed_hash": "h"})
        assert result["error"] != "bank_excluded"
    finally:
        instance.shutdown()


def test_prefetch_exclusion_env_form(provider: Callable, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HINDSIGHT_PREFETCH_EXCLUDED_BANKS", " envbank , ,second ")
    instance, _ = _provider_with_bank(provider, "envbank")
    try:
        result = instance.prefetch_candidates({"query": "q", "feed_hash": "h"})
        assert result["error"] == "bank_excluded"
    finally:
        instance.shutdown()


def test_monitoring_retain_excludes_only_configured_prefixes(provider: Callable) -> None:
    instance, _ = _provider_with_bank(
        provider, "b", {"monitoring_retain_excluded_prefixes": ["cron_abc123_"]}
    )
    try:
        assert instance._monitoring_retain_excluded("cron_abc123_9") is True
        assert instance._monitoring_retain_excluded("cron_other_1") is False
        assert instance._monitoring_retain_excluded("") is False
    finally:
        instance.shutdown()


def test_monitoring_retain_default_excludes_nothing(provider: Callable) -> None:
    # Old-host fallback: absent setting = upstream behavior; the previously
    # hard-coded cron ids are deployment config now, never shipped defaults.
    instance, _ = _provider_with_bank(provider, "b")
    try:
        assert instance._monitoring_retain_excluded("cron_3a3da94c3a4b_1") is False
        assert instance._monitoring_retain_excluded("cron_1c7606c34c24_2") is False
        assert instance._monitoring_retain_excluded("cron_aab9d856059d_3") is False
    finally:
        instance.shutdown()


def test_prefetch_exclusion_preserves_recall_max_results(provider: Callable) -> None:
    # The two corrections are independent of the retained cap setting/name.
    instance, _ = _provider_with_bank(
        provider, "special", {"prefetch_excluded_banks": ["special"], "recall_max_results": 1}
    )
    try:
        assert instance._str_list_setting("prefetch_excluded_banks", "HINDSIGHT_PREFETCH_EXCLUDED_BANKS") == ("special",)
        assert instance._str_list_setting("monitoring_retain_excluded_prefixes", "HINDSIGHT_MONITORING_RETAIN_EXCLUDED_PREFIXES") == ()
    finally:
        instance.shutdown()
