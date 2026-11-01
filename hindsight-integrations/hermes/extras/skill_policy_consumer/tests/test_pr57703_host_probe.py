"""W01 probe: a synthetic skill-policy consumer on the PR 57703 host path.

What is real: ``skill_manage`` (batch and flat), the host's ``_gate_skill_batch``,
``invoke_policy_hook`` dispatch with its timeout/failed-callback accounting,
``skill_policy.required`` read from a real ``config.yaml`` in a temporary
``HERMES_HOME``, path validation, locks, atomic rollback and the actual skill
files on a temporary root.

What is substituted: the separate human write-approval staging gate
(``_run_write_gate`` / ``_apply_skill_write_gate``) and success bookkeeping are
disabled exactly as the host's own contract tests do, so the policy gate is
observed in isolation. No model/provider is called; the consumer is a
deterministic test double. Plugin discovery is bypassed by registering the
callback on the real plugin manager (the same seam the host tests use); a
loadable plugin package is W02 scope.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

plugins = pytest.importorskip("hermes_cli.plugins")
smt = pytest.importorskip("tools.skill_manager_tool")

HAS_BATCH_HOOK = "pre_skill_batch" in getattr(plugins, "VALID_HOOKS", set())
proposed_host = pytest.mark.skipif(not HAS_BATCH_HOOK, reason="stock host: no pre_skill_batch")
stock_host = pytest.mark.skipif(HAS_BATCH_HOOK, reason="proposed host: pre_skill_batch present")

SKILL = """\
---
name: probe-skill
description: Synthetic W01 probe skill.
---

# Probe

Step 1: original.
"""

SKILL_V2 = SKILL.replace("original.", "rewritten.")

FLAT_ARGS = {
    "create": {"content": SKILL},
    "edit": {"content": SKILL_V2},
    "patch": {"old_string": "original.", "new_string": "patched."},
    "write_file": {"file_path": "references/note.md", "file_content": "synthetic"},
    "remove_file": {"file_path": "references/note.md"},
    "delete": {"absorbed_into": ""},
}


def batch_ops():
    return [
        {"name": "probe-skill", "action": "create", "content": SKILL},
        {"name": "probe-skill", "action": "write_file", "file_path": "references/note.md", "file_content": "synthetic"},
        {"name": "probe-skill", "action": "patch", "old_string": "original.", "new_string": "patched."},
    ]


@pytest.fixture(autouse=True)
def isolated_hooks(monkeypatch):
    manager = plugins.get_plugin_manager()
    monkeypatch.setattr(manager, "_hooks", {})
    monkeypatch.setattr(manager, "_discovered", True)
    return manager


@contextmanager
def skills_root(tmp_path: Path):
    root = tmp_path / "skills"
    root.mkdir()
    with (
        patch("tools.skill_manager_tool.SKILLS_DIR", root),
        patch("agent.skill_utils.get_all_skills_dirs", return_value=[root]),
        patch("tools.skill_manager_tool._apply_skill_write_gate", return_value=None),
        patch("tools.skill_manager_tool._run_write_gate", return_value=None),
        patch("tools.skill_manager_tool._record_success"),
    ):
        yield root


def set_required(home: Path, hook: str | None):
    text = f"skill_policy:\n  required: {hook}\n" if hook else "skill_policy: {}\n"
    (home / "config.yaml").write_text(text, encoding="utf-8")


def tree(root: Path):
    # Skill content only; the host's .locks/ lock files are not skill effects.
    return sorted(
        p.relative_to(root).as_posix()
        for p in root.rglob("*")
        if p.is_file() and p.relative_to(root).parts[0] != ".locks"
    )


def run_batch(ops):
    return json.loads(smt.skill_manage(action="", name="", operations=ops))


def run_flat(action, **kwargs):
    return json.loads(smt.skill_manage(action=action, name="probe-skill", **kwargs))


def seed(root, action):
    if action == "create":
        return
    assert smt._create_skill("probe-skill", SKILL)["success"]
    if action == "remove_file":
        f = root / "probe-skill" / "references" / "note.md"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("synthetic", encoding="utf-8")


class Consumer:
    """Deterministic policy double in the shape a ported Jev consumer must use."""

    def __init__(self, decision, root):
        self.decision, self.root, self.calls = decision, root, []

    def __call__(self, **kwargs):
        self.calls.append({"kwargs": kwargs, "tree_at_call": tree(self.root)})
        if self.decision == "approve":
            return {"action": "approve", "payload_digest": kwargs["payload_digest"]}
        if self.decision == "block":
            return {"action": "block", "reason": "synthetic policy declined"}
        if self.decision == "legacy_allow":  # current Jev pre_skill_write vocabulary
            return {"action": "allow"}
        if self.decision == "raise":
            raise RuntimeError("synthetic policy crash")
        return None


def register(consumer):
    plugins.get_plugin_manager()._hooks.setdefault("pre_skill_batch", []).append(consumer)


# ---------------------------------------------------------------- proposed host


@proposed_host
def test_no_policy_selected_is_native(tmp_path, hermes_home):
    set_required(hermes_home, None)
    with skills_root(tmp_path) as root:
        assert run_batch(batch_ops())["success"] is True
        assert "probe-skill/references/note.md" in tree(root)


@proposed_host
def test_allow_full_payload_one_judgment_before_any_effect(tmp_path, hermes_home):
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        consumer = Consumer("approve", root)
        register(consumer)
        result = run_batch(batch_ops())
        final = tree(root)
    assert result["success"] is True, result
    assert len(consumer.calls) == 1
    call = consumer.calls[0]
    assert call["tree_at_call"] == []  # judged before the first mutation
    assert call["kwargs"]["operations"] == batch_ops()  # complete ordered payload
    assert call["kwargs"]["payload_digest"] == smt._skill_batch_payload_digest(batch_ops())
    # Record the actual context the host supplies (no session/actor/profile identity).
    assert set(call["kwargs"]) - {"telemetry_schema_version"} == {"operations", "payload_digest"}
    assert final == ["probe-skill/SKILL.md", "probe-skill/references/note.md"]
    assert "patched." in (root / "probe-skill" / "SKILL.md").read_text(encoding="utf-8")


@proposed_host
def test_block_is_policy_denied_and_nothing_written(tmp_path, hermes_home):
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        consumer = Consumer("block", root)
        register(consumer)
        result = run_batch(batch_ops())
        assert tree(root) == []
    assert result["success"] is False
    assert result["error"].startswith("Skill batch policy denied:")
    assert len(consumer.calls) == 1


@proposed_host
def test_required_policy_absent_is_unavailable(tmp_path, hermes_home):
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        result = run_batch(batch_ops())
        assert tree(root) == []
    assert result["error"].startswith("Skill batch policy unavailable:")
    assert "not registered" in result["error"]


@proposed_host
def test_required_policy_crash_is_unavailable(tmp_path, hermes_home):
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        register(Consumer("raise", root))
        result = run_batch(batch_ops())
        assert tree(root) == []
    assert result["error"].startswith("Skill batch policy unavailable:")


@proposed_host
def test_legacy_jev_allow_vocabulary_is_not_consent_when_required(tmp_path, hermes_home):
    """Port requirement: the existing Jev ``{"action": "allow"}`` is no decision here."""
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        register(Consumer("legacy_allow", root))
        result = run_batch(batch_ops())
        assert tree(root) == []
    assert result["error"].startswith("Skill batch policy unavailable:")


@proposed_host
def test_optional_consumer_no_decision_fails_open(tmp_path, hermes_home):
    set_required(hermes_home, None)
    with skills_root(tmp_path) as root:
        register(Consumer("legacy_allow", root))
        assert run_batch(batch_ops())["success"] is True
        assert tree(root) != []


@proposed_host
def test_required_value_is_not_matched_to_a_hook_name(tmp_path, hermes_home):
    """Observed: any non-blank ``skill_policy.required`` arms the pre_skill_batch gate."""
    set_required(hermes_home, "some_other_hook")
    with skills_root(tmp_path) as root:
        consumer = Consumer("approve", root)
        register(consumer)
        assert run_batch(batch_ops())["success"] is True
    assert len(consumer.calls) == 1


@proposed_host
@pytest.mark.parametrize("action", list(FLAT_ARGS))
def test_single_flat_action_required_absent_is_unavailable(tmp_path, hermes_home, action):
    with skills_root(tmp_path) as root:
        set_required(hermes_home, None)
        seed(root, action)
        before = {p: (root / p).read_bytes() for p in tree(root)}
        set_required(hermes_home, "pre_skill_batch")
        result = run_flat(action, **FLAT_ARGS[action])
        after = {p: (root / p).read_bytes() for p in tree(root)}
    assert result["success"] is False
    assert result["error"].startswith("Skill batch policy unavailable:")
    assert after == before


@proposed_host
@pytest.mark.parametrize("action", list(FLAT_ARGS))
def test_single_flat_action_is_judged_once_as_one_op_payload(tmp_path, hermes_home, action):
    with skills_root(tmp_path) as root:
        set_required(hermes_home, None)
        seed(root, action)
        set_required(hermes_home, "pre_skill_batch")
        consumer = Consumer("approve", root)
        register(consumer)
        result = run_flat(action, **FLAT_ARGS[action])
    assert result["success"] is True, result
    assert len(consumer.calls) == 1
    ops = consumer.calls[0]["kwargs"]["operations"]
    assert len(ops) == 1 and ops[0]["action"] == action and ops[0]["name"] == "probe-skill"


@proposed_host
def test_changed_content_cannot_reuse_digest(tmp_path, hermes_home):
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        stale = smt._skill_batch_payload_digest(batch_ops())
        register(lambda **kw: {"action": "approve", "payload_digest": stale})
        changed = batch_ops()
        changed[2] = {**changed[2], "new_string": "tampered."}  # same shape, changed content
        result = run_batch(changed)
        assert tree(root) == []
    assert result["error"].startswith("Skill batch policy unavailable:")


# ------------------------------------------- operation shapes (review 1: W01-R1/R2)


class ScriptedConsumer(Consumer):
    """Returns the next scripted decision on each call (records like ``Consumer``)."""

    def __init__(self, decisions, root):
        super().__init__(None, root)
        self.decisions = list(decisions)

    def __call__(self, **kwargs):
        self.decision = self.decisions.pop(0) if self.decisions else None
        return super().__call__(**kwargs)


@proposed_host
def test_sole_delete_batch_is_judged_once(tmp_path, hermes_home):
    """W02-H1 corrected host: ``operations=[delete]`` is ONE judgment, on the flat-equivalent
    payload (``replace_all: false`` added) that the flat path also binds into staging."""
    with skills_root(tmp_path) as root:
        set_required(hermes_home, None)
        seed(root, "delete")
        set_required(hermes_home, "pre_skill_batch")
        consumer = Consumer("approve", root)
        register(consumer)
        result = run_batch([{"name": "probe-skill", "action": "delete"}])
        final = tree(root)
    assert result["success"] is True, result
    assert final == []
    (call,) = consumer.calls
    flat = [{"name": "probe-skill", "action": "delete", "replace_all": False}]
    assert call["kwargs"]["operations"] == flat
    assert call["kwargs"]["payload_digest"] == smt._skill_batch_payload_digest(flat)
    assert call["tree_at_call"] == ["probe-skill/SKILL.md"]  # judged before the deletion


@proposed_host
def test_flat_delete_is_judged_once(tmp_path, hermes_home):
    """Flat delete (not operations[]) is a single judgment over the flat-shape payload."""
    with skills_root(tmp_path) as root:
        set_required(hermes_home, None)
        seed(root, "delete")
        set_required(hermes_home, "pre_skill_batch")
        consumer = Consumer("approve", root)
        register(consumer)
        result = run_flat("delete")
    assert result["success"] is True, result
    assert [c["kwargs"]["operations"] for c in consumer.calls] == [
        [{"name": "probe-skill", "action": "delete", "replace_all": False}]
    ]


@proposed_host
def test_edit_inside_operations_is_rejected_before_policy(tmp_path, hermes_home):
    """W01-R2: ``edit`` is a flat legacy alias only; operations[] rejects it natively."""
    with skills_root(tmp_path) as root:
        set_required(hermes_home, None)
        seed(root, "edit")
        before = {p: (root / p).read_bytes() for p in tree(root)}
        set_required(hermes_home, "pre_skill_batch")
        consumer = Consumer("approve", root)
        register(consumer)
        result = run_batch([{"name": "probe-skill", "action": "edit", "content": SKILL_V2}])
        after = {p: (root / p).read_bytes() for p in tree(root)}
    assert result["success"] is False
    assert "unknown action 'edit'" in result["error"]
    assert consumer.calls == []
    assert after == before


@proposed_host
def test_delete_mixed_with_other_ops_is_rejected_before_policy(tmp_path, hermes_home):
    """W01-R2: delete is legal only as the sole operation."""
    with skills_root(tmp_path) as root:
        set_required(hermes_home, None)
        seed(root, "delete")
        before = {p: (root / p).read_bytes() for p in tree(root)}
        set_required(hermes_home, "pre_skill_batch")
        consumer = Consumer("approve", root)
        register(consumer)
        result = run_batch(
            [
                {"name": "probe-skill", "action": "patch", "old_string": "original.", "new_string": "x."},
                {"name": "probe-skill", "action": "delete"},
            ]
        )
        after = {p: (root / p).read_bytes() for p in tree(root)}
    assert result["success"] is False
    assert "delete must be the SOLE op" in result["error"]
    assert consumer.calls == []
    assert after == before


@proposed_host
def test_full_rewrite_inside_operations_uses_patch_content(tmp_path, hermes_home):
    """W01-R2: the supported operations[] full-rewrite shape is ``patch`` + ``content``;
    it is judged once with that exact shape."""
    ops = [{"name": "probe-skill", "action": "patch", "content": SKILL_V2}]
    with skills_root(tmp_path) as root:
        set_required(hermes_home, None)
        seed(root, "patch")
        set_required(hermes_home, "pre_skill_batch")
        consumer = Consumer("approve", root)
        register(consumer)
        result = run_batch(ops)
        text = (root / "probe-skill" / "SKILL.md").read_text(encoding="utf-8")
    assert result["success"] is True, result
    assert [c["kwargs"]["operations"] for c in consumer.calls] == [ops]
    assert "rewritten." in text


@proposed_host
@pytest.mark.parametrize("action", ["create", "patch", "write_file", "remove_file"])
def test_each_batchable_action_alone_in_operations_is_judged_once(tmp_path, hermes_home, action):
    """W01-R2: the actual ``_BATCH_OP_ACTIONS`` set, each as a one-op operations[] call."""
    op = {"name": "probe-skill", "action": action, **FLAT_ARGS[action]}
    with skills_root(tmp_path) as root:
        set_required(hermes_home, None)
        seed(root, action)
        set_required(hermes_home, "pre_skill_batch")
        consumer = Consumer("approve", root)
        register(consumer)
        result = run_batch([op])
    assert result["success"] is True, result
    assert [c["kwargs"]["operations"] for c in consumer.calls] == [[op]]


@proposed_host
def test_batchable_action_set_matches_the_map():
    from tools import skill_manager_batch as smb

    assert smb._BATCH_OP_ACTIONS == {"create", "patch", "write_file", "remove_file"}


# ------------------------------------------- host context reach (review 1 answer 2)


@proposed_host
def test_host_context_survives_into_the_bounded_policy_callback_a_b_a(tmp_path, hermes_home, monkeypatch):
    """Preparatory W02 check: which authoritative host context a ``pre_skill_batch``
    callback can read on the real bounded dispatch path, alternating A -> B -> A.

    The callback receives only operations/digest/telemetry version. Write origin,
    attendedness, ledger actor and the task-local session id are ContextVars that the
    dispatcher's worker thread inherits through ``contextvars.copy_context()``. The
    ``session_id`` argument of ``skill_manage`` itself is not forwarded.
    """
    session_context = pytest.importorskip("gateway.session_context")
    provenance = pytest.importorskip("tools.skill_provenance")
    ledger = pytest.importorskip("tools.skill_ledger")
    monkeypatch.delenv("HERMES_SESSION_ID", raising=False)
    seen = []

    def callback(**kwargs):
        import threading

        seen.append(
            {
                "session": session_context.get_session_env("HERMES_SESSION_ID"),
                "origin": provenance.get_current_write_origin(),
                "unattended": provenance.is_unattended_review(),
                "actor": ledger.derive_actor(),
                "thread": threading.current_thread().name,
                "keys": sorted(kwargs),
            }
        )
        return {"action": "approve", "payload_digest": kwargs["payload_digest"]}

    contexts = [
        ("session-A", provenance.BACKGROUND_REVIEW, "probe-a"),
        ("session-B", "foreground", "probe-b"),
        ("session-A", provenance.BACKGROUND_REVIEW, "probe-c"),
    ]
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path):
        register(callback)
        for session, origin, name in contexts:
            token = provenance.set_current_write_origin(origin)
            try:
                with session_context.scoped_current_session_id(session):
                    ops = [{"name": name, "action": "create", "content": SKILL.replace("probe-skill", name)}]
                    result = json.loads(
                        smt.skill_manage(action="", name="", operations=ops, session_id="arg-" + session)
                    )
            finally:
                provenance.reset_current_write_origin(token)
            assert result["success"] is True, result
    assert [(s["session"], s["origin"]) for s in seen] == [(c[0], c[1]) for c in contexts]
    assert [s["actor"] for s in seen] == ["curator", "agent", "curator"]
    assert [s["unattended"] for s in seen] == [True, False, True]
    # The skill_manage(session_id=...) argument never reaches the callback.
    assert all(s["keys"] == ["operations", "payload_digest", "telemetry_schema_version"] for s in seen)
    assert all("arg-" not in str(s) for s in seen)
    print(json.dumps({"w01_context_probe": seen}))


# ---------------------------------------------------------------- stock host


@stock_host
def test_stock_host_has_no_policy_gate_and_required_config_is_ignored(tmp_path, hermes_home):
    """On a host without the contract, setting skill_policy.required enforces nothing."""
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        result = run_batch(batch_ops())
        assert tree(root) != []
    assert result["success"] is True
    assert not hasattr(plugins, "invoke_policy_hook")
