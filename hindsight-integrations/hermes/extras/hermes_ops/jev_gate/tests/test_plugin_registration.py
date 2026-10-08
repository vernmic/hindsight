"""Load the external plugin through Hermes' real loader and dispatch its hooks."""

import importlib
import json
from pathlib import Path


def test_real_loader_shadow_selection_and_write_refusal(monkeypatch, tmp_path):
    from hermes_cli.plugins import PluginManager
    from tools import skill_provenance

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    (tmp_path / "config.yaml").write_text(
        json.dumps(
            {"jev_gate": {"mode": "shadow", "write_gate": {"mode": "enforced"}}, "plugins": {"enabled": ["jev_gate"]}}
        )
    )
    manager = PluginManager()
    manifest = next(m for m in manager._scan_directory(Path(__file__).parents[2], "user") if m.name == "jev_gate")
    manager._load_plugin(manifest)
    assert manager._plugins["jev_gate"].error is None
    callback = manager._hooks["pre_memory_recall"][0]
    module = importlib.import_module(callback.__module__)
    calls = []

    def decide(state, questions, cfg, **kwargs):
        calls.append(state)
        answers = {}
        for key, question in questions.items():
            if question["type"] == "noul":
                answers[key] = {"type": "noul", "noul": 0.95}
            else:
                selected = "5" if key == "depth" else "already_covered" if key == "discovery" else "none"
                answers[key] = {
                    "type": "choice",
                    "choice": selected,
                    "probabilities": {option: float(option == selected) for option in question["criteria"]},
                }
        return {"answers": answers}

    monkeypatch.setattr(module.client, "decide", decide)
    plans = manager.invoke_hook(
        "pre_memory_recall",
        session_id="s",
        turn_id="s:t",
        conversation_history=[],
        user_message="debug the current issue",
        roster=[{"name": "debug", "trigger": "Debugging"}],
    )
    assert len(plans) == 1 and plans[0]["apply"] is False and len(calls) == 1
    manager.invoke_hook(
        "pre_api_request",
        session_id="s",
        turn_id="s:t",
        conversation_history=[],
        request_messages=[{"role": "user", "content": "debug the current issue"}],
        system_prompt="unchanged",
    )
    directory = tmp_path / "skills/example"
    directory.mkdir(parents=True)
    (directory / "SKILL.md").write_text("Original rule.\n")
    token = skill_provenance.set_current_write_origin("background_review")
    try:
        decisions = manager.invoke_hook(
            "pre_skill_write",
            session_id="s",
            proposal={"action": "patch", "name": "example", "old_string": "Original rule.", "new_string": "New rule."},
        )
    finally:
        skill_provenance.reset_current_write_origin(token)
    assert decisions[0]["action"] == "block"
    assert decisions[0]["gate"]["disposition"] == "gate_refuse_recurrence"
    assert len(calls) == 1 and (directory / "SKILL.md").read_text() == "Original rule.\n"


def test_real_loader_off_restores_baseline_without_judge_or_structured_recall(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from hermes_cli.plugins import PluginManager
    from agent import turn_selection

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    (tmp_path / "config.yaml").write_text(
        json.dumps({"jev_gate": {"mode": "off"}, "plugins": {"enabled": ["jev_gate"]}})
    )
    manager = PluginManager()
    manifest = next(m for m in manager._scan_directory(Path(__file__).parents[2], "user") if m.name == "jev_gate")
    manager._load_plugin(manifest)
    assert manager._plugins["jev_gate"].error is None
    callback = manager._hooks["pre_memory_recall"][0]
    module = importlib.import_module(callback.__module__)

    def forbidden(*args, **kwargs):
        raise AssertionError("Off mode must not judge or retrieve structured candidates")

    monkeypatch.setattr(module.client, "decide", forbidden)
    monkeypatch.setattr(turn_selection, "_plans", lambda **kwargs: manager.invoke_hook("pre_memory_recall", **kwargs))
    agent = SimpleNamespace(
        session_id="s",
        platform="cli",
        _parent_session_id="",
        _user_turn_count=1,
        _persist_disabled=False,
        _memory_manager=SimpleNamespace(prefetch_structured=forbidden),
    )
    history = [{"role": "user", "content": "Work on the project"}]
    assert turn_selection.select_turn_context(
        agent, history, history[0]["content"], lambda: "original baseline memories"
    ) == ("original baseline memories", "")
    assert history == [{"role": "user", "content": "Work on the project"}]
