"""Native plugin ``jev-skill-recommendations``: durable capture of refused skill proposals (W03).

Separate from ``jev-skill-policy`` on purpose: it observes the host's own refusal on the upstream
``post_tool_call`` hook, so it records ``policy unavailable`` even when the policy plugin is absent,
disabled or failed to start. It never decides anything and returns nothing to the host.
"""

from __future__ import annotations

import hashlib
import importlib
import logging
import threading
from pathlib import Path

from .store import RecommendationStore, refusal_kind

PLUGIN_ID = "jev-skill-recommendations"
logger = logging.getLogger(__name__)


def _skill_hash(name: str) -> str | None:
    from tools.skill_manager_tool import _find_skill

    try:
        found = _find_skill(name)
        return hashlib.sha256((Path(found["path"]) / "SKILL.md").read_bytes()).hexdigest() if found else None
    except Exception:
        return None


def _setting(ctx, key, default, check, message):
    value = ctx.get_config(key, default)
    if not check(value):
        raise ValueError(f"{PLUGIN_ID} settings: {message}")
    return value


def build(ctx) -> tuple[RecommendationStore, object | None]:
    from hermes_constants import get_hermes_home

    root = _setting(ctx, "root", None, lambda v: v is None or (isinstance(v, str) and v.strip()), "root: path")
    bodies = _setting(ctx, "store_bodies", True, lambda v: isinstance(v, bool), "store_bodies: boolean")
    target = _setting(
        ctx,
        "board_callable",
        None,
        lambda v: v is None or (isinstance(v, str) and ":" in v),
        "board_callable: 'module:attr'",
    )
    store = RecommendationStore(
        Path(root) if root else get_hermes_home() / PLUGIN_ID, store_bodies=bodies, skill_hash=_skill_hash
    )
    board = None
    if target:
        module_name, _, attr = target.partition(":")
        board = getattr(importlib.import_module(module_name), attr)()
    return store, board


def register(ctx) -> None:
    store, board = build(ctx)
    delivering = threading.Lock()

    def deliver() -> None:
        if board is None or not delivering.acquire(blocking=False):
            return
        try:
            store.deliver_pending(board)
        except Exception:
            logger.warning("%s: delivery pass failed; records stay pending", PLUGIN_ID, exc_info=True)
        finally:
            delivering.release()

    def on_tool_result(*, tool_name=None, args=None, result=None, **ids) -> None:
        refusal = refusal_kind(result) if tool_name == "skill_manage" else None
        if refusal is None:
            return
        source = {k: ids.get(k) for k in ("session_id", "task_id", "tool_call_id", "turn_id")}
        try:
            store.capture(*refusal, args or {}, source)
        except Exception:  # the host refusal already stands; only capture is lost, and it says so
            logger.warning("%s: could not record %s skill proposal", PLUGIN_ID, refusal[0], exc_info=True)
            return
        threading.Thread(target=deliver, name=f"{PLUGIN_ID}-deliver", daemon=True).start()

    ctx.register_hook("post_tool_call", on_tool_result)
    threading.Thread(target=deliver, name=f"{PLUGIN_ID}-recover", daemon=True).start()
