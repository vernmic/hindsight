"""W02-R4: the consumer loads through native plugin discovery from a disposable profile.

A throwaway ``HERMES_HOME`` gets ``plugins/jev-skill-policy`` (a copy of this package) and a
``config.yaml`` with ``plugins.enabled`` and ``plugins.entries.jev-skill-policy.settings``. The
real loader imports the package and calls ``register(ctx)``; the host's ``skill_manage`` then
dispatches to it. Judges are deterministic (``judge_callable``) or a stubbed host LLM facade; no
model, network, live profile or installed plugin is used.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

plugins = pytest.importorskip("hermes_cli.plugins")
smt = pytest.importorskip("tools.skill_manager_tool")
pytestmark = pytest.mark.skipif("pre_skill_batch" not in plugins.VALID_HOOKS, reason="stock host")

from test_pr57703_host_probe import SKILL, skills_root, tree

PACKAGE = Path(__file__).resolve().parents[1]
JUDGES = """
CALLS = []

def block(request):
    CALLS.append(request)
    return {"verdict": "block", "reason": "jev loaded refusal"}

def crash(request):
    CALLS.append(request)
    raise RuntimeError("judge down")
"""


@pytest.fixture
def profile(tmp_path, monkeypatch):
    home = tmp_path / "profile"
    shutil.copytree(
        PACKAGE, home / "plugins" / "jev-skill-policy", ignore=shutil.ignore_patterns("tests", "__pycache__", "*.md")
    )
    (tmp_path / "jev_load_judges.py").write_text(JUDGES, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "jev_load_judges", raising=False)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(plugins, "get_bundled_plugins_dir", lambda: tmp_path / "no-bundled")
    monkeypatch.setattr(plugins.PluginManager, "_scan_entry_points", lambda self: [])
    plugins._reset_plugin_managers_for_tests()
    yield home
    plugins._reset_plugin_managers_for_tests()


def load(home, *, enabled=True, host_required=True, **settings):
    config = {
        "skill_policy": {"required": "pre_skill_batch"} if host_required else {},
        "plugins": {
            "enabled": ["jev-skill-policy"] if enabled else [],
            "entries": {
                "jev-skill-policy": {
                    "settings": {
                        "policy_revision": "rev-load",
                        "required": host_required,
                        "judge_callable": "jev_load_judges:block",
                        **settings,
                    }
                }
            },
        },
    }
    (home / "config.yaml").write_text(json.dumps(config), encoding="utf-8")
    plugins.discover_plugins(force=True)
    manager = plugins.get_plugin_manager()
    loaded = manager._plugins.get("jev-skill-policy")
    return manager._hooks.get("pre_skill_batch") or [], loaded


def batch(name="probe-skill"):
    operations = [{"name": name, "action": "create", "content": SKILL.replace("probe-skill", name)}]
    return json.loads(smt.skill_manage(action="", name="", operations=operations))


def test_required_profile_loads_registers_and_refuses_with_session_identity(profile, tmp_path):
    session_context = pytest.importorskip("gateway.session_context")
    hooks, loaded = load(profile, timeout_seconds=5)
    assert loaded is not None and loaded.enabled and not loaded.error, loaded
    assert len(hooks) == 1
    with skills_root(tmp_path) as root:
        with session_context.scoped_current_session_id("profile-session"):
            result = batch()
        assert tree(root) == []
    assert result["error"] == "Skill batch policy denied: jev loaded refusal", result
    import jev_load_judges

    (request,) = jev_load_judges.CALLS
    assert request["policy_revision"] == "rev-load" and request["deadline_ms"] == 5000
    assert request["context"]["session_id"] == "profile-session"
    assert request["mutation_digest"] == smt._skill_batch_payload_digest(
        [{"name": "probe-skill", "action": "create", "content": SKILL}]
    )


@pytest.mark.parametrize(
    "settings",
    [
        dict(timeout_seconds=0),
        dict(timeout_seconds="soon"),
        dict(max_concurrent=99),
        dict(policy_revision=""),
        dict(judge_callable="jev_load_judges:missing"),
    ],
)
def test_required_startup_failure_leaves_no_hook_and_host_refuses(profile, tmp_path, settings):
    hooks, loaded = load(profile, **settings)
    assert hooks == [] and loaded is not None and loaded.error, loaded
    with skills_root(tmp_path) as root:
        result = batch()
        assert tree(root) == []
    assert result["error"].startswith("Skill batch policy unavailable:"), result


def test_required_plugin_refuses_to_start_on_a_host_without_the_exact_switch(profile):
    hooks, loaded = load(profile, host_required=False, required=True)
    assert hooks == [] and "exactly 'pre_skill_batch'" in (loaded.error or ""), loaded


@pytest.mark.parametrize("host_required", [True, False])
def test_disabled_plugin_never_loads(profile, tmp_path, host_required):
    hooks, loaded = load(profile, enabled=False, host_required=host_required)
    assert hooks == [] and not (loaded and loaded.enabled)
    with skills_root(tmp_path):
        result = batch()
    import jev_load_judges

    assert jev_load_judges.CALLS == []
    if host_required:
        assert result["error"].startswith("Skill batch policy unavailable:"), result
    else:
        assert result["success"] is True, result


def test_optional_profile_judge_failure_is_native(profile, tmp_path):
    hooks, _ = load(profile, host_required=False, judge_callable="jev_load_judges:crash")
    assert len(hooks) == 1
    with skills_root(tmp_path) as root:
        result = batch()
        assert "probe-skill/SKILL.md" in tree(root)
    assert result["success"] is True, result


def test_default_judge_is_the_host_llm_facade_bounded_by_the_profile_timeout(profile, tmp_path, monkeypatch):
    plugin_llm = pytest.importorskip("agent.plugin_llm")
    seen = []

    def complete_structured(self, **kwargs):
        seen.append(kwargs)
        return plugin_llm.PluginLlmStructuredResult(
            text="",
            provider="stub",
            model="stub",
            agent_id="stub",
            parsed={"verdict": "block", "reason": "llm says no"},
        )

    monkeypatch.setattr(plugin_llm.PluginLlm, "complete_structured", complete_structured)
    hooks, loaded = load(profile, judge_callable=None, timeout_seconds=7.5)
    assert len(hooks) == 1, loaded
    with skills_root(tmp_path):
        result = batch()
    assert result["error"] == "Skill batch policy denied: llm says no", result
    (kwargs,) = seen
    assert kwargs["timeout"] == 7.5 and kwargs["json_schema"]["required"] == ["verdict"]
    assert json.loads(kwargs["input"][0].text)["policy_revision"] == "rev-load"
