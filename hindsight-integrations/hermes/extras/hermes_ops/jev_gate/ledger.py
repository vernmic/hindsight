"""Decisions persist independently of session pruning; actual injections are explicit."""

import hashlib
import json
import sqlite3
import time


def record(session_id, turn_ref, phase, state, questions, result, *, mode, outcome=None):
    from hermes_constants import get_hermes_home

    path = get_hermes_home() / "state.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path, timeout=2) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS jev_decisions (id INTEGER PRIMARY KEY, ts REAL, session_id TEXT, turn_ref TEXT, turn_msg_id INTEGER, phase TEXT, mode TEXT, state_hash TEXT, state_json TEXT, questions_json TEXT, result_json TEXT, outcome_json TEXT, UNIQUE(turn_ref,phase))"
        )
        state_text = json.dumps(state, ensure_ascii=True, sort_keys=True)
        conn.execute(
            "INSERT INTO jev_decisions(ts,session_id,turn_ref,phase,mode,state_hash,state_json,questions_json,result_json,outcome_json) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(turn_ref,phase) DO UPDATE SET state_hash=excluded.state_hash,state_json=excluded.state_json,questions_json=excluded.questions_json,result_json=excluded.result_json,outcome_json=excluded.outcome_json",
            (
                time.time(),
                session_id,
                turn_ref,
                phase,
                mode,
                hashlib.sha256(state_text.encode()).hexdigest(),
                state_text,
                json.dumps(questions, ensure_ascii=True),
                json.dumps(result, ensure_ascii=True),
                json.dumps(outcome, ensure_ascii=True),
            ),
        )
        row = conn.execute("SELECT id FROM jev_decisions WHERE turn_ref=? AND phase=?", (turn_ref, phase)).fetchone()
        return row[0]


def link_turn(turn_ref, message_id):
    from hermes_constants import get_hermes_home

    with sqlite3.connect(get_hermes_home() / "state.db", timeout=2) as conn:
        conn.execute("UPDATE jev_decisions SET turn_msg_id=? WHERE turn_ref=?", (message_id, turn_ref))


def discovery(session_id, turn_ref, current_task, sources, answer):
    """Candidate evidence only; no automatic skill creation or one-off queue entries."""
    from hermes_constants import get_hermes_home

    with sqlite3.connect(get_hermes_home() / "state.db", timeout=2) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS jev_discovery_candidates (turn_ref TEXT PRIMARY KEY, session_id TEXT, ts REAL, task TEXT, sources_json TEXT, answer_json TEXT)"
        )
        conn.execute(
            "INSERT OR IGNORE INTO jev_discovery_candidates VALUES(?,?,?,?,?,?)",
            (
                turn_ref,
                session_id,
                time.time(),
                current_task,
                json.dumps(sources, ensure_ascii=True),
                json.dumps(answer, ensure_ascii=True),
            ),
        )
