"""Structured decisions, validated answers and a hard caller deadline."""

import json
import math
import threading
import time
import urllib.error
import urllib.request

from .constants import POLICY_VERSION

_inflight = {}
_lock = threading.Lock()


class Unavailable(RuntimeError):
    def __init__(self, disposition, reason):
        super().__init__(reason)
        self.disposition = disposition


def validate_answers(questions, answers):
    if not isinstance(answers, dict) or set(answers) != set(questions):
        raise Unavailable("judge_unavailable_parse", "Missing or unexpected answer keys")
    for key, question in questions.items():
        answer = answers[key]
        if not isinstance(answer, dict) or answer.get("type") != question["type"]:
            raise Unavailable("judge_unavailable_parse", "Invalid answer type")
        if question["type"] == "noul":
            value = answer.get("noul")
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not 0 <= value <= 1
            ):
                raise Unavailable("judge_unavailable_parse", "Invalid noul probability")
        elif question["type"] == "choice":
            options = question["criteria"]
            probabilities = answer.get("probabilities")
            if (
                answer.get("choice") not in options
                or not isinstance(probabilities, dict)
                or set(probabilities) != set(options)
            ):
                raise Unavailable("judge_unavailable_parse", "Invalid choice options")
            if any(
                isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or not 0 <= v <= 1
                for v in probabilities.values()
            ):
                raise Unavailable("judge_unavailable_parse", "Invalid choice probabilities")
            if not 0.98 <= sum(probabilities.values()) <= 1.02:
                raise Unavailable("judge_unavailable_parse", "Choice probabilities do not sum to one")
            if "confidence" in answer:
                value = answer["confidence"]
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    or not 0 <= value <= 1
                ):
                    raise Unavailable("judge_unavailable_parse", "Invalid choice confidence")
    return answers


def decide(state, questions, cfg, *, timeout=None):
    from agent.memory_provider import spawn_context_thread
    from agent.secret_scope import get_secret
    from hermes_constants import hermes_home_key

    seconds = min(float(cfg["judge_timeout_seconds"]), timeout if timeout is not None else float("inf"))
    if seconds <= 0:
        raise Unavailable("judge_unavailable_timeout", "Total judge deadline exhausted")
    body = json.dumps({"model": cfg["model"], "state": state, "questions": questions}, ensure_ascii=True).encode()
    if len(body) > cfg["max_state_chars"]:
        raise Unavailable("judge_unavailable_parse", "Judge state exceeds configured budget")
    try:
        key = get_secret("OPENROUTER_API_KEY")
    except Exception as exc:
        raise Unavailable("judge_unavailable_auth", "Profile credential unavailable") from exc
    if not key:
        raise Unavailable("judge_unavailable_auth", "Profile credential missing")
    home_key = hermes_home_key()
    box = {}
    with _lock:
        if _inflight.get(home_key, 0) >= 2:
            raise Unavailable("judge_unavailable_timeout", "Previous judge work is still running")
        _inflight[home_key] = _inflight.get(home_key, 0) + 1
    request = urllib.request.Request(
        cfg["endpoint"], data=body, headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"}
    )
    started = time.monotonic()

    def run():
        try:
            with urllib.request.urlopen(request, timeout=seconds) as response:
                box["payload"] = json.loads(response.read(2_000_000))
        except urllib.error.HTTPError as exc:
            box["error"] = Unavailable(
                "judge_unavailable_auth" if exc.code in (401, 403) else "judge_unavailable_transport",
                f"Judge HTTP {exc.code}",
            )
        except (TimeoutError,):
            box["error"] = Unavailable("judge_unavailable_timeout", "Judge socket timeout")
        except urllib.error.URLError as exc:
            box["error"] = Unavailable(
                "judge_unavailable_timeout" if isinstance(exc.reason, TimeoutError) else "judge_unavailable_transport",
                "Judge connection failed",
            )
        except (ValueError, TypeError):
            box["error"] = Unavailable("judge_unavailable_parse", "Judge returned invalid JSON")
        except Exception:
            box["error"] = Unavailable("judge_unavailable_transport", "Judge transport failed")
        finally:
            with _lock:
                _inflight[home_key] -= 1

    worker = spawn_context_thread(run, name="jev-decision")
    worker.start()
    worker.join(seconds)
    elapsed = time.monotonic() - started
    if worker.is_alive() or elapsed > seconds:
        raise Unavailable("judge_unavailable_timeout", "Judge hard deadline exceeded; late response discarded")
    if "error" in box:
        raise box["error"]
    payload = box.get("payload")
    if not isinstance(payload, dict):
        raise Unavailable("judge_unavailable_parse", "Invalid decision envelope")
    answers = validate_answers(questions, payload.get("answers"))
    if not isinstance(payload.get("model"), str) or not payload["model"].startswith(cfg["model"]):
        raise Unavailable("judge_unavailable_parse", "Missing or unexpected resolved judge model")
    return {
        "answers": answers,
        "requested_model": cfg["model"],
        "resolved_model": payload.get("model"),
        "usage": payload.get("usage"),
        "elapsed_ms": round(elapsed * 1000),
        "request_bytes": body.decode(),
        "policy_version": POLICY_VERSION,
    }
