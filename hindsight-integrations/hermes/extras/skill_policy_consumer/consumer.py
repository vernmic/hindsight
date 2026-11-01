"""Jev skill-policy consumer for the proposed ``pre_skill_batch`` hook (W02, skill-only slice).

Loaded by the native plugin loader (``plugin.yaml`` + ``register(ctx)``) with settings from the active
profile's ``plugins.entries.jev-skill-policy.settings``. Trusted host context and the batch go in; a
digest-bound wire response comes out. The judge is advisory: it returns ``{"verdict", "reason"?}`` only
and holds no authority field. Every call is judged afresh. ``outcomes`` is an in-memory audit of what
this process decided; durable blocked/unavailable capture and recovery are W03 and NOT provided here.
"""

from __future__ import annotations

import contextvars
import importlib
import json
import math
import queue
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

HOOK = "pre_skill_batch"
PLUGIN_ID = "jev-skill-policy"
VERDICTS = frozenset({"allow", "block", "modify", "require_approval"})
MAX_TIMEOUT_S = 120.0
MAX_CONCURRENT = 16
JUDGE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["verdict"],
    "properties": {"verdict": {"type": "string", "enum": sorted(VERDICTS)}, "reason": {"type": "string"}},
}
DEFAULT_INSTRUCTIONS = (
    "You are Jev, a skill-change policy judge. The input is one JSON skill batch request. Reply with JSON "
    '{"verdict": "allow" | "block" | "modify" | "require_approval", "reason": "<short reason>"} and nothing else.'
)

Judge = Callable[[dict[str, Any]], object]


def _session_id():
    # Task-local value only; get_session_env() would fall back to os.environ.
    from gateway import session_context as sc

    value = sc._VAR_MAP["HERMES_SESSION_ID"].get()
    return None if value is sc._UNSET else value


def _write_origin():
    from tools.skill_provenance import get_current_write_origin

    return get_current_write_origin()


def _unattended():
    # The host's own attendedness: a background review that was not explicitly attended (/refine).
    from tools.skill_provenance import is_unattended_review

    return is_unattended_review()


def _actor():
    from tools.skill_ledger import derive_actor

    return derive_actor()


_CONTEXT_READERS = {
    "session_id": _session_id,
    "write_origin": _write_origin,
    "unattended": _unattended,
    "actor": _actor,
}


@dataclass(frozen=True)
class TrustedContext:
    """Host identity read on the dispatch path (copied ContextVars). A field the host cannot supply is
    ``None`` and marked ``unavailable``; nothing is guessed. There is no evidence carrier."""

    session_id: str | None
    write_origin: str | None
    unattended: bool | None
    actor: str | None
    availability: Mapping[str, str]

    @classmethod
    def read(cls) -> TrustedContext:
        values: dict[str, Any] = {}
        availability = {"evidence": "unavailable"}
        for name, reader in _CONTEXT_READERS.items():
            try:
                value = reader()
            except Exception:
                value = None
            values[name] = value
            availability[name] = "unavailable" if value is None or value == "" else "host"
        return cls(**values, availability=MappingProxyType(availability))

    def as_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "write_origin": self.write_origin,
            "unattended": self.unattended,
            "actor": self.actor,
            "evidence": None,
            "availability": dict(self.availability),
        }


@dataclass(frozen=True)
class JudgeRequest:
    """Engine-owned, immutable request. The judge only ever sees a fresh detached copy."""

    request_id: str
    operations_json: str
    mutation_digest: str
    policy_revision: str
    deadline_ms: int
    context: TrustedContext
    kind: str = "skill_batch"

    def as_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "kind": self.kind,
            "operation": {"skill_ops": json.loads(self.operations_json)},
            "mutation_digest": self.mutation_digest,
            "policy_revision": self.policy_revision,
            "deadline_ms": self.deadline_ms,
            "context": self.context.as_dict(),
        }


@dataclass(frozen=True)
class JudgeResult:
    verdict: str | None
    reason: str | None
    status: str


@dataclass(frozen=True)
class Outcome:
    request: JudgeRequest
    result: JudgeResult
    wire: Mapping[str, str] | None


def _failed(why: str) -> JudgeResult:
    return JudgeResult(None, None, f"failed:{why}")


def parse_judge_output(output: object) -> JudgeResult:
    """Fixed shape ``{"verdict": <known str>, "reason"?: str | null}``; anything else is invalid."""
    if not isinstance(output, Mapping) or not set(output) <= {"verdict", "reason"}:
        return _failed("invalid")
    verdict, reason = output.get("verdict"), output.get("reason")
    if not isinstance(verdict, str) or verdict not in VERDICTS or not (reason is None or isinstance(reason, str)):
        return _failed("invalid")
    return JudgeResult(verdict, reason or None, "ok")


@dataclass(frozen=True)
class ConsumerSettings:
    """Validated profile settings; any out-of-range value fails plugin startup."""

    policy_revision: str
    required: bool = False
    timeout_s: float = 10.0
    max_concurrent: int = 4
    unattended_only: bool = False
    judge_callable: str | None = None
    provider: str | None = None
    model: str | None = None
    instructions: str = DEFAULT_INSTRUCTIONS

    def __post_init__(self) -> None:
        def text(value: object) -> bool:
            return isinstance(value, str) and bool(value.strip())

        def number(value: object) -> bool:
            return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)

        checks = {
            "policy_revision must be a non-empty string": text(self.policy_revision),
            f"timeout_seconds must be a finite number in (0, {MAX_TIMEOUT_S:g}]": number(self.timeout_s)
            and 0 < self.timeout_s <= MAX_TIMEOUT_S,
            f"max_concurrent must be an integer in [1, {MAX_CONCURRENT}]": isinstance(self.max_concurrent, int)
            and not isinstance(self.max_concurrent, bool)
            and 1 <= self.max_concurrent <= MAX_CONCURRENT,
            "required and unattended_only must be booleans": isinstance(self.required, bool)
            and isinstance(self.unattended_only, bool),
            "judge_callable, judge_provider and judge_model must be unset or non-empty strings": all(
                v is None or text(v) for v in (self.judge_callable, self.provider, self.model)
            ),
            "instructions must be a non-empty string": text(self.instructions),
        }
        problems = [message for message, ok in checks.items() if not ok]
        if problems:
            raise ValueError(f"{PLUGIN_ID} settings: " + "; ".join(problems))

    @classmethod
    def from_plugin(cls, ctx) -> ConsumerSettings:
        get = ctx.get_config
        return cls(
            policy_revision=get("policy_revision"),
            required=get("required", False),
            timeout_s=get("timeout_seconds", 10.0),
            max_concurrent=get("max_concurrent", 4),
            unattended_only=get("unattended_only", False),
            judge_callable=get("judge_callable"),
            provider=get("judge_provider"),
            model=get("judge_model"),
            instructions=get("instructions", DEFAULT_INSTRUCTIONS),
        )


class BoundedJudgeRunner:
    """One daemon worker per call under a fixed number of slots, the pattern of the host's
    ``hermes_cli.approval_transport.invoke_approval_transport``. There is no queue: with every slot
    held a call fails ``busy`` at once, so expired work can never start later. A daemon worker never
    holds up interpreter exit. A hung judge keeps its slot until it returns, then the slot recovers;
    the provider call itself is bounded by the same timeout (see ``resolve_judge``)."""

    def __init__(self, judge: Judge, settings: ConsumerSettings):
        self._judge, self._timeout_s = judge, float(settings.timeout_s)
        self._slots = threading.BoundedSemaphore(settings.max_concurrent)
        self._closed = threading.Event()

    def close(self) -> None:
        """Unload: refuse new work; running workers are abandoned and their results discarded."""
        self._closed.set()

    def run(self, request: JudgeRequest) -> JudgeResult:
        if self._closed.is_set():
            return _failed("unloaded")
        if not self._slots.acquire(blocking=False):
            return _failed("busy")
        results: queue.Queue[tuple[bool, object, float]] = queue.Queue(maxsize=1)
        payload, context = request.as_dict(), contextvars.copy_context()
        deadline = time.monotonic() + self._timeout_s

        def work() -> None:
            try:
                results.put_nowait((True, context.run(self._judge, payload), time.monotonic()))
            except BaseException as exc:  # fail closed on any judge exit
                results.put_nowait((False, exc, time.monotonic()))
            finally:
                self._slots.release()

        try:
            threading.Thread(target=work, name=f"jev-skill-judge-{request.request_id[:8]}", daemon=True).start()
        except BaseException:
            self._slots.release()
            return _failed("error")
        try:
            ok, value, completed_at = results.get(timeout=self._timeout_s)
        except queue.Empty:
            return _failed("timeout")
        if completed_at > deadline or self._closed.is_set():
            return _failed("timeout" if completed_at > deadline else "unloaded")
        return parse_judge_output(value) if ok else _failed(type(value).__name__)


class SkillPolicyConsumer:
    """Callable ``pre_skill_batch`` hook. ``judge(request_dict) -> {"verdict", "reason"?}``."""

    def __init__(self, judge: Judge, settings: ConsumerSettings):
        self.settings = settings
        self.outcomes: list[Outcome] = []
        self._runner = BoundedJudgeRunner(judge, settings)

    def close(self) -> None:
        self._runner.close()

    def __call__(self, *, operations, payload_digest, **_ignored):
        request = JudgeRequest(
            request_id=uuid.uuid4().hex,
            operations_json=json.dumps(operations),
            mutation_digest=payload_digest,
            policy_revision=self.settings.policy_revision,
            deadline_ms=int(self.settings.timeout_s * 1000),
            context=TrustedContext.read(),
        )
        attended = request.context.availability["unattended"] == "host" and request.context.unattended is False
        if self.settings.unattended_only and attended:
            result = JudgeResult("allow", None, "skipped")
        else:
            result = self._runner.run(request)
        wire = self._wire(result, request)
        self.outcomes.append(Outcome(request, result, None if wire is None else MappingProxyType(dict(wire))))
        return wire

    def _wire(self, result: JudgeResult, request: JudgeRequest) -> dict[str, str] | None:
        if result.verdict is None:
            return None  # host decides: required -> policy unavailable, optional -> native
        if result.verdict == "allow":
            return {"action": "approve", "payload_digest": request.mutation_digest}
        # block; modify and require_approval also refuse (no skill rewrite or human route)
        reason = result.reason if result.reason and result.reason.strip() else None
        return {
            "action": "block",
            "reason": reason
            or f"Jev policy refused skill batch ({result.verdict}, policy {request.policy_revision}, "
            f"request {request.request_id})",
        }


def resolve_judge(ctx, settings: ConsumerSettings) -> Judge:
    """``judge_callable: "module:attr"`` is the deterministic injection seam (trusted profile config);
    otherwise the host's ``ctx.llm`` structured completion, bounded by ``timeout_seconds``."""
    if settings.judge_callable:
        module_name, _, attr = settings.judge_callable.partition(":")
        judge = getattr(importlib.import_module(module_name), attr, None) if attr else None
        if not callable(judge):
            raise ValueError(f"{PLUGIN_ID} settings: judge_callable must name a callable as 'module:attr'")
        return judge
    from agent.plugin_llm import PluginLlmTextInput

    llm = ctx.llm

    def llm_judge(request: dict[str, Any]) -> object:
        return llm.complete_structured(
            instructions=settings.instructions,
            input=[PluginLlmTextInput(json.dumps(request, sort_keys=True))],
            json_schema=JUDGE_SCHEMA,
            schema_name="jev_skill_verdict",
            timeout=settings.timeout_s,
            provider=settings.provider,
            model=settings.model,
            purpose=PLUGIN_ID,
        ).parsed

    return llm_judge


def install(ctx, consumer: SkillPolicyConsumer) -> bool:
    """Register on a host that has the hook. ``required`` also demands the host switch
    ``skill_policy.required: pre_skill_batch`` exactly, else refuses to claim enforcement.
    Returns whether the hook was registered (False: stock host in optional mode)."""
    from hermes_cli.plugins import VALID_HOOKS

    required = consumer.settings.required
    if HOOK not in VALID_HOOKS:
        if required:
            raise RuntimeError(f"required skill policy selected but this host has no {HOOK} hook")
        return False
    if required:
        from tools.skill_manager_tool import _required_skill_policy_hook

        if _required_skill_policy_hook() != HOOK:
            raise RuntimeError(f"required skill policy selected: set skill_policy.required to exactly '{HOOK}'")
    ctx.register_hook(HOOK, consumer)
    ctx.on_unload(consumer.close)
    return True


def register(ctx) -> None:
    """Native plugin entrypoint: bad settings or a required-mode mismatch fail startup, which the
    host's required mode then reports as policy unavailable."""
    settings = ConsumerSettings.from_plugin(ctx)
    install(ctx, SkillPolicyConsumer(resolve_judge(ctx, settings), settings))
