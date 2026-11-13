"""Local Jev policy plugin; no mutation authority is delegated to the judge."""


def register(ctx):
    from .selector import prepare
    from .write_gate import judge_write

    ctx.register_hook("pre_memory_recall", prepare)
    ctx.register_hook("pre_skill_write", judge_write)
    ctx.register_hook("on_session_start", recover)
    ctx.register_hook("pre_api_request", record_assembly)


def recover(**kwargs):
    from tools.skill_review_delivery import schedule
    from tools.skill_review_trigger import load_trigger_config, trigger_armed

    cfg = load_trigger_config()
    if trigger_armed(cfg):
        schedule(cfg)


def record_assembly(*, session_id, turn_id, conversation_history, request_messages, system_prompt="", **kwargs):
    import hashlib

    from . import ledger
    from .constants import config

    cfg = config()
    if cfg["mode"] == "off":
        return
    user_rows = [row for row in request_messages if row.get("role") == "user"]
    current = user_rows[-1] if user_rows else {}
    history_users = [row for row in conversation_history if row.get("role") == "user"]
    message_id = history_users[-1].get("_row_id") if history_users else None
    ledger.record(
        session_id,
        turn_id,
        "assembled",
        {
            "current_user_api_message": current,
            "system_prompt_sha256": hashlib.sha256(str(system_prompt).encode()).hexdigest(),
        },
        {},
        {},
        mode=cfg["mode"],
        outcome={"message_id": message_id},
    )
    if isinstance(message_id, int):
        ledger.link_turn(turn_id, message_id)
