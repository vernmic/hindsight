"""W02-R1/R3: bounded judge execution, typed judge output and an immutable engine identity.

These drive the consumer directly with deterministic judges; no host dispatch, model or network.
"""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE))
import consumer as jev

DIGEST = "d" * 64


def make(judge, **kwargs):
    return jev.SkillPolicyConsumer(judge, jev.ConsumerSettings(policy_revision="rev", **kwargs))


def call(consumer):
    return consumer(operations=[{"name": "s", "action": "create", "content": "x"}], payload_digest=DIGEST)


def test_saturated_slots_fail_fast_then_recover_and_expired_work_never_starts():
    release, lock, started = threading.Event(), threading.Lock(), []

    def judge(request):
        with lock:
            started.append(request["request_id"])
        release.wait(10)
        return {"verdict": "allow"}

    consumer = make(judge, timeout_s=0.05, max_concurrent=2)
    try:
        assert [call(consumer), call(consumer), call(consumer)] == [None, None, None]
        assert [o.result.status for o in consumer.outcomes] == ["failed:timeout", "failed:timeout", "failed:busy"]
        busy_id = consumer.outcomes[2].request.request_id
    finally:
        release.set()
    for _ in range(200):  # the held slots come back once the hung judges return
        if call(consumer) == {"action": "approve", "payload_digest": DIGEST}:
            break
        time.sleep(0.02)
    else:
        pytest.fail("judge slots did not recover after release")
    time.sleep(0.2)
    assert busy_id not in started, "a refused-busy request must never start later"


def test_hung_judge_does_not_block_process_exit():
    code = (
        "import json, sys, threading\n"
        f"sys.path.insert(0, {str(PACKAGE)!r})\n"
        "import consumer as jev\n"
        "never = threading.Event()\n"
        "c = jev.SkillPolicyConsumer(lambda r: never.wait(), jev.ConsumerSettings(policy_revision='r', timeout_s=0.05))\n"
        "print(json.dumps(c(operations=[], payload_digest='d')), c.outcomes[0].result.status)\n"
    )
    started = time.monotonic()
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=20)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.split() == ["null", "failed:timeout"]
    assert time.monotonic() - started < 20


def test_unloaded_consumer_refuses_new_work_without_calling_the_judge():
    calls = []
    consumer = make(lambda request: calls.append(request) or {"verdict": "allow"})
    consumer.close()
    assert call(consumer) is None
    assert consumer.outcomes[0].result.status == "failed:unloaded" and calls == []


@pytest.mark.parametrize(
    "output",
    [
        {"verdict": []},
        {"verdict": {}},
        {"verdict": 1},
        {"verdict": None},
        {"verdict": "allow", "reason": 5},
        {"verdict": "allow", "reason": ["x"]},
        {"verdict": "allow", "action": "approve"},
        ["allow"],
        "allow",
        None,
    ],
)
def test_malformed_judge_output_is_classified_invalid(output):
    consumer = make(lambda request: output)
    assert call(consumer) is None
    assert consumer.outcomes[0].result == jev.JudgeResult(None, None, "failed:invalid")


def test_judge_cannot_rewrite_the_engine_identity():
    def judge(request):
        request["request_id"] = "judge-supplied-id"
        request["mutation_digest"] = "e" * 64
        request["context"]["session_id"] = "judge-supplied-session"
        request["context"]["availability"]["session_id"] = "host"
        return {"verdict": "allow"}

    consumer = make(judge)
    assert call(consumer) == {"action": "approve", "payload_digest": DIGEST}
    (outcome,) = consumer.outcomes
    assert outcome.request.request_id != "judge-supplied-id"
    assert outcome.request.mutation_digest == DIGEST
    assert outcome.request.context.session_id != "judge-supplied-session"
    assert outcome.request.context.availability["session_id"] == "unavailable"
    with pytest.raises(FrozenInstanceError):
        outcome.request.request_id = "x"  # type: ignore[misc]
    with pytest.raises(TypeError):
        outcome.request.context.availability["session_id"] = "host"  # type: ignore[index]
    with pytest.raises(TypeError):
        outcome.wire["action"] = "block"  # type: ignore[index]


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(timeout_s=0),
        dict(timeout_s=-1),
        dict(timeout_s=float("inf")),
        dict(timeout_s=float("nan")),
        dict(timeout_s=121),
        dict(timeout_s=True),
        dict(timeout_s="10"),
        dict(max_concurrent=0),
        dict(max_concurrent=17),
        dict(max_concurrent=2.0),
        dict(required="yes"),
        dict(unattended_only=1),
        dict(policy_revision=""),
        dict(judge_callable=""),
        dict(instructions=" "),
    ],
)
def test_settings_reject_unbounded_or_mistyped_values(kwargs):
    with pytest.raises(ValueError, match="jev-skill-policy settings"):
        jev.ConsumerSettings(**{"policy_revision": "rev", **kwargs})
