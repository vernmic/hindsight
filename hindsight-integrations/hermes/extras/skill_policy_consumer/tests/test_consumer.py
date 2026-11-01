"""W02: the Jev skill-policy consumer (``consumer.py``) on the real host dispatch path.

Real: ``skill_manage`` flat and ``operations[]``, ``_gate_skill_batch`` -> ``invoke_policy_hook``
(bounded worker with copied ContextVars), ``skill_policy.required`` from a temporary
``config.yaml``, registration through a real ``PluginContext``, the skill files on a temporary
root. Substituted as in the W01 probe: the human write-approval gate and success bookkeeping
(the host suite covers those natively). The judge is a deterministic function; no model runs.
Native discovery/configuration is covered by ``test_plugin_loading.py``.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

plugins = pytest.importorskip("hermes_cli.plugins")
smt = pytest.importorskip("tools.skill_manager_tool")
pytestmark = pytest.mark.skipif("pre_skill_batch" not in plugins.VALID_HOOKS, reason="stock host")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import consumer as jev
from test_pr57703_host_probe import (  # noqa: F401 - fixture
    SKILL,
    isolated_hooks,
    seed,
    set_required,
    skills_root,
    tree,
)


def ops(name="probe-skill"):
    return [{"name": name, "action": "create", "content": SKILL.replace("probe-skill", name)}]


def batch(operations):
    return json.loads(smt.skill_manage(action="", name="", operations=operations))


def context():
    return plugins.PluginContext(
        plugins.PluginManifest(name="jev-skill-policy", source="user"), plugins.get_plugin_manager()
    )


def install(judge, *, required=True, **kwargs):
    consumer = jev.SkillPolicyConsumer(
        judge, jev.ConsumerSettings(policy_revision="rev-test", required=required, **kwargs)
    )
    assert jev.install(context(), consumer) is True
    return consumer


class Judge:
    def __init__(self, *outputs, delay=0.0):
        self.outputs, self.delay, self.requests = list(outputs), delay, []

    def __call__(self, request):
        self.requests.append(request)
        time.sleep(self.delay)
        out = self.outputs.pop(0) if len(self.outputs) > 1 else self.outputs[0]
        if isinstance(out, Exception):
            raise out
        return out


def test_allow_is_one_digest_bound_approve(tmp_path, hermes_home):
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        judge = Judge({"verdict": "allow"})
        consumer = install(judge)
        result = batch(ops())
        assert result["success"] is True, result
        assert "probe-skill/SKILL.md" in tree(root)
    (request,) = judge.requests
    digest = smt._skill_batch_payload_digest(ops())
    assert request["mutation_digest"] == digest and request["policy_revision"] == "rev-test"
    assert consumer.outcomes[0].wire == {"action": "approve", "payload_digest": digest}
    assert request["context"]["availability"]["evidence"] == "unavailable"


@pytest.mark.parametrize("verdict", ["block", "modify", "require_approval"])
@pytest.mark.parametrize("reason", ["judge said no", "", None])
def test_refusing_verdicts_are_denied_with_nonempty_reason(tmp_path, hermes_home, verdict, reason):
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        install(Judge({"verdict": verdict, "reason": reason}))
        result = batch(ops())
        assert tree(root) == []
    assert result["success"] is False
    assert result["error"].startswith("Skill batch policy denied:")
    assert (reason or f"({verdict}, policy rev-test") in result["error"]


FAILURES = {
    "crash": dict(output=RuntimeError("judge down")),
    "invalid": dict(output={"verdict": "maybe"}),
    "unhashable_verdict": dict(output={"verdict": []}),
    "not_a_dict": dict(output="allow"),
    "authority_only": dict(output={"action": "approve", "payload_digest": "x"}),
    "timeout_late_allow": dict(output={"verdict": "allow"}, delay=0.6),
}


@pytest.mark.parametrize("case", list(FAILURES))
def test_judge_failure_required_is_unavailable_optional_is_native(tmp_path, hermes_home, case):
    spec = FAILURES[case]
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        consumer = install(Judge(spec["output"], delay=spec.get("delay", 0.0)), timeout_s=0.2)
        required = batch(ops("req-skill"))
        time.sleep(spec.get("delay", 0.0))  # a late result must not change anything
        assert tree(root) == []
        set_required(hermes_home, None)
        optional = batch(ops("opt-skill"))
        assert "opt-skill/SKILL.md" in tree(root)
    assert required["error"].startswith("Skill batch policy unavailable:"), required
    assert optional["success"] is True, optional
    assert all(o.wire is None and o.result.status.startswith("failed") for o in consumer.outcomes)


def test_required_consumer_absent_is_unavailable(tmp_path, hermes_home):
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        result = batch(ops())
        assert tree(root) == []
    assert result["error"].startswith("Skill batch policy unavailable:")


def test_no_approval_reuse_across_operations(tmp_path, hermes_home):
    """The same payload twice is judged twice with fresh request ids; a later refusal holds."""
    set_required(hermes_home, "pre_skill_batch")
    with skills_root(tmp_path) as root:
        judge = Judge({"verdict": "allow"}, {"verdict": "block", "reason": "second look"})
        install(judge)
        assert batch(ops())["success"] is True
        smt._delete_skill("probe-skill")
        second = batch(ops())
        assert tree(root) == []
    assert second["error"].startswith("Skill batch policy denied: second look")
    assert len({r["request_id"] for r in judge.requests}) == 2


def test_trusted_identity_propagates_a_b_a_and_is_not_guessed(tmp_path, hermes_home, monkeypatch):
    session_context = pytest.importorskip("gateway.session_context")
    provenance = pytest.importorskip("tools.skill_provenance")
    monkeypatch.setenv("HERMES_SESSION_ID", "env-must-not-be-used")
    set_required(hermes_home, "pre_skill_batch")
    judge = Judge({"verdict": "allow"})
    plan = [
        ("session-A", provenance.BACKGROUND_REVIEW),
        ("session-B", "foreground"),
        ("session-A", provenance.BACKGROUND_REVIEW),
        (None, "foreground"),
    ]
    with skills_root(tmp_path):
        install(judge)
        for i, (session, origin) in enumerate(plan):
            token = provenance.set_current_write_origin(origin)
            try:
                if session is None:
                    assert batch(ops(f"s{i}"))["success"] is True
                else:
                    with session_context.scoped_current_session_id(session):
                        assert batch(ops(f"s{i}"))["success"] is True
            finally:
                provenance.reset_current_write_origin(token)
    seen = [(r["context"]["session_id"], r["context"]["write_origin"], r["context"]["actor"]) for r in judge.requests]
    assert seen == [
        ("session-A", provenance.BACKGROUND_REVIEW, "curator"),
        ("session-B", "foreground", "agent"),
        ("session-A", provenance.BACKGROUND_REVIEW, "curator"),
        (None, "foreground", "agent"),
    ]
    assert judge.requests[-1]["context"]["availability"]["session_id"] == "unavailable"


def _unavailable():
    raise LookupError("host attendedness not readable")


# case -> (background review?, explicitly attended?, judged?)
ATTENDANCE = {
    "foreground": (False, False, False),
    "unattended_background_review": (True, False, True),
    "attended_background_review": (True, True, False),
    "attendedness_unavailable": (False, False, True),
}


@pytest.mark.parametrize("case", list(ATTENDANCE))
def test_unattended_only_follows_host_attendedness(tmp_path, hermes_home, monkeypatch, case):
    """``unattended_only`` uses ``is_unattended_review()``, not the curator actor label: an explicitly
    attended ``/refine`` review keeps background origin and curator actor but is not judged."""
    provenance = pytest.importorskip("tools.skill_provenance")
    background, attended, judged = ATTENDANCE[case]
    if case == "attendedness_unavailable":
        monkeypatch.setitem(jev._CONTEXT_READERS, "unattended", _unavailable)
    set_required(hermes_home, "pre_skill_batch")
    origin = provenance.set_current_write_origin(provenance.BACKGROUND_REVIEW if background else "foreground")
    attendance = provenance.set_review_attended(attended)
    try:
        with skills_root(tmp_path) as root:
            judge = Judge({"verdict": "block", "reason": "judged and refused"})
            consumer = install(judge, unattended_only=True)
            result = batch(ops())
            written = "probe-skill/SKILL.md" in tree(root)
    finally:
        provenance.reset_review_attended(attendance)
        provenance.reset_current_write_origin(origin)
    assert len(judge.requests) == int(judged)
    assert consumer.outcomes[0].result.status == ("ok" if judged else "skipped")
    assert written is (not judged) and result["success"] is (not judged), result
    if judged:
        assert result["error"] == "Skill batch policy denied: judged and refused"


def test_sole_delete_through_consumer_is_one_judgment(tmp_path, hermes_home):
    with skills_root(tmp_path) as root:
        set_required(hermes_home, None)
        seed(root, "delete")
        set_required(hermes_home, "pre_skill_batch")
        judge = Judge({"verdict": "allow"})
        install(judge)
        assert batch([{"name": "probe-skill", "action": "delete"}])["success"] is True
    assert len(judge.requests) == 1


@pytest.mark.parametrize("value", [None, "true", "pre_skill_write"])
def test_required_mode_refuses_inexact_host_setting(hermes_home, value):
    set_required(hermes_home, value)
    consumer = jev.SkillPolicyConsumer(
        Judge({"verdict": "allow"}), jev.ConsumerSettings(policy_revision="r", required=True)
    )
    with pytest.raises(RuntimeError, match="exactly 'pre_skill_batch'"):
        jev.install(context(), consumer)
    assert not plugins.get_plugin_manager()._hooks.get("pre_skill_batch")
