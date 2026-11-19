"""The feed ledger row carries stage timings on both the success and Unavailable path."""


def test_feed_row_records_stage_timings_on_success(monkeypatch):
    import time

    import jev_gate.selector as selector
    from jev_gate import client as jev_client
    from jev_gate import ledger

    recorded = {}
    monkeypatch.setattr(ledger, "record", lambda *a, **k: recorded.update(args=a, kwargs=k) or recorded.update({"n": recorded.get("n", 0) + 1}))
    monkeypatch.setattr(
        selector,
        "recent_items",
        lambda *a, **k: [{"role": "user", "text": "task", "message_id": 1}],
    )
    monkeypatch.setattr(selector, "skill_roster", lambda: [])
    def fake_decide(state, questions, cfg, timeout=None):
        return {
            "answers": {
                key: ({"noul": 0.0} if q["type"] == "noul" else {"choice": "none"})
                for key, q in questions.items()
            }
        }

    monkeypatch.setattr(jev_client, "decide", fake_decide)
    from jev_gate.constants import config

    cfg = config()
    cfg = {**cfg, "mode": "enforced"}
    selector.prepare(
        session_id="s",
        conversation_history=[],
        user_message="do the thing",
        turn_id="t1",
        cfg=cfg,
    )
    result = recorded["args"][5]
    assert set(result["timings_ms"]) == {"recent_items", "roster", "judge_wait", "total"}
    assert all(isinstance(v, (int, float)) and v >= 0 for v in result["timings_ms"].values())


def test_feed_row_records_stage_timings_on_unavailable(monkeypatch):
    import jev_gate.selector as selector
    from jev_gate import client as jev_client
    from jev_gate import ledger

    recorded = {}
    monkeypatch.setattr(ledger, "record", lambda *a, **k: recorded.update(args=a) or recorded.update({"n": recorded.get("n", 0) + 1}))
    monkeypatch.setattr(
        selector,
        "recent_items",
        lambda *a, **k: [{"role": "user", "text": "task", "message_id": 1}],
    )
    monkeypatch.setattr(selector, "skill_roster", lambda: [])

    def unavailable(*a, **k):
        raise jev_client.Unavailable("judge_unavailable_parse", "boom")

    monkeypatch.setattr(jev_client, "decide", unavailable)
    from jev_gate.constants import config

    cfg = {**config(), "mode": "enforced"}
    assert selector.prepare(session_id="s", conversation_history=[], user_message="x", turn_id="t2", cfg=cfg) is None
    result = recorded["args"][5]
    assert result["error"] == "judge_unavailable_parse"
    assert set(result["timings_ms"]) == {"recent_items", "roster", "judge_wait", "total"}