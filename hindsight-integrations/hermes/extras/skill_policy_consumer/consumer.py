"""Jev skill-policy consumer for the proposed ``pre_skill_batch`` hook (W02, skill-only slice).

Trusted policy configuration and host context go in; a digest-bound wire response comes out.
The judge is advisory and injected: it returns ``{"verdict", "reason"?}`` only and holds no
authority field. Every call is judged afresh - no decision is cached or reused across
operations. ``outcomes`` is an in-memory audit of what this process decided; durable
blocked/unavailable recommendations and recovery are W03 and are NOT provided here.
"""

from __future__ import annotations

import contextvars
import copy
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from typing import Any, Callable

HOOK = "pre_skill_batch"
VERDICTS = frozenset({"allow", "block", "modify", "require_approval"})


def _session_id():
    # Task-local value only; get_session_env() would fall back to os.environ.
    from gateway import session_context as sc

    value = sc._VAR_MAP["HERMES_SESSION_ID"].get()
    return None if value is sc._UNSET else value


def _write_origin():
    from tools.skill_provenance import get_current_write_origin

    return get_current_write_origin()


def _unattended():
    from tools.skill_provenance import is_unattended_review

    return is_unattended_review()


def _actor():
    from tools.skill_ledger import derive_actor

    return derive_actor()


_CONTEXT_READERS = {"session_id": _session_id, "write_origin": _write_origin,
                    "unattended": _unattended, "actor": _actor}


def trusted_context() -> dict[str, Any]:
    """Host identity read on the bounded dispatch path (copied ContextVars). A field the host
    cannot supply is ``None`` and marked ``unavailable``; nothing is guessed. The host passes
    no evidence carrier, so ``evidence`` is always unavailable."""
    context: dict[str, Any] = {"evidence": None}
    availability = {"evidence": "unavailable"}
    for field, read in _CONTEXT_READERS.items():
        try:
            value = read()
        except Exception:
            value = None
        context[field] = value
        availability[field] = "unavailable" if value is None or value == "" else "host"
    context["availability"] = availability
    return context


class SkillPolicyConsumer:
    """Callable ``pre_skill_batch`` hook. ``judge(request) -> {"verdict", "reason"?}``."""

    def __init__(self, judge: Callable[[dict], Any], *, policy_revision: str,
                 deadline_s: float = 10.0, unattended_only: bool = False):
        self.judge, self.policy_revision = judge, policy_revision
        self.deadline_s, self.unattended_only = deadline_s, unattended_only
        self.outcomes: list[dict[str, Any]] = []
        self._pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="jev-skill-judge")

    def __call__(self, *, operations, payload_digest, **_ignored):
        context = trusted_context()
        request = {"request_id": uuid.uuid4().hex, "kind": "skill_batch",
                   "operation": {"skill_ops": copy.deepcopy(operations)},
                   "mutation_digest": payload_digest, "policy_revision": self.policy_revision,
                   "deadline_ms": int(self.deadline_s * 1000), "context": context}
        attended = (context["availability"]["unattended"] == "host" and not context["unattended"]
                    and context["actor"] != "curator")
        if self.unattended_only and attended:
            verdict, reason, status = "allow", None, "skipped"
        else:
            verdict, reason, status = self._judge(request)
        wire = self._wire(verdict, reason, payload_digest, request["request_id"])
        self.outcomes.append({"request_id": request["request_id"], "decision": verdict,
                              "judge_status": status, "mutation_digest": payload_digest,
                              "policy_revision": self.policy_revision, "wire": wire,
                              "context": context})
        return wire

    def _judge(self, request):
        """(verdict, reason, status). A raise, timeout or malformed result is a judge failure
        (verdict None); a result that arrives after the deadline is discarded."""
        future = self._pool.submit(contextvars.copy_context().run, self.judge, request)
        try:
            output = future.result(timeout=self.deadline_s)
        except FutureTimeout:
            return None, None, "failed:timeout"
        except Exception as exc:
            return None, None, f"failed:{type(exc).__name__}"
        if not isinstance(output, dict) or output.get("verdict") not in VERDICTS:
            return None, None, "failed:invalid"
        reason = output.get("reason")
        return output["verdict"], reason if isinstance(reason, str) else None, "ok"

    def _wire(self, verdict, reason, digest, request_id):
        if verdict is None:
            return None  # host decides: required -> policy unavailable, optional -> native
        if verdict == "allow":
            return {"action": "approve", "payload_digest": digest}
        # block; modify and require_approval also refuse (no skill rewrite or human route)
        if not (reason and reason.strip()):
            reason = (f"Jev policy refused skill batch ({verdict}, policy {self.policy_revision}, "
                      f"request {request_id})")
        return {"action": "block", "reason": reason}


def register(ctx, consumer: SkillPolicyConsumer, *, required: bool) -> bool:
    """Register on a host that has the hook. ``required=True`` also demands the host switch
    ``skill_policy.required: pre_skill_batch`` exactly, else refuses to claim enforcement.
    Returns whether the hook was registered (False: stock host in optional mode)."""
    from hermes_cli.plugins import VALID_HOOKS

    if HOOK not in VALID_HOOKS:
        if required:
            raise RuntimeError(f"required skill policy selected but this host has no {HOOK} hook")
        return False
    if required:
        from tools.skill_manager_tool import _required_skill_policy_hook

        if _required_skill_policy_hook() != HOOK:
            raise RuntimeError(f"required skill policy selected: set skill_policy.required to exactly '{HOOK}'")
    ctx.register_hook(HOOK, consumer)
    return True
