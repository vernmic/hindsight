"""Preview complete operations under the host's mutation lock; judge verified evidence."""

import difflib
import hashlib
import json
import sqlite3
import uuid
from pathlib import Path

from . import client, ledger
from .constants import POLICY_VERSION, QUESTIONS, config
from .selector import noul


def manifest(files):
    return [{"path": name, "sha256": hashlib.sha256(text.encode()).hexdigest()} for name, text in sorted(files.items())]


def preview(proposal):
    from tools.skill_ledger import _skills_dir
    from tools.skill_manager_tool import _find_skill

    operations = proposal.get("operations") or [proposal]
    skills = {}
    for op in operations:
        name = op.get("name") or proposal.get("name")
        if not name:
            raise ValueError("Proposal has no skill identity")
        if name not in skills:
            found = _find_skill(name)
            directory = Path(found["path"]) if found else _skills_dir() / (op.get("category") or "") / name
            from tools.skill_ledger import TRANSIENT_DIRS

            paths = (
                [
                    p
                    for p in directory.rglob("*")
                    if p.is_file()
                    and not p.is_symlink()
                    and not any(part in TRANSIENT_DIRS for part in p.relative_to(directory).parts[:-1])
                ]
                if directory.exists()
                else []
            )
            files = {
                p.relative_to(directory).as_posix(): p.read_text(encoding="utf-8-sig", errors="replace") for p in paths
            }
            skills[name] = {
                "skill_id": directory.relative_to(_skills_dir()).as_posix()
                if directory.is_relative_to(_skills_dir())
                else name,
                "path": str(directory),
                "existing_files": files,
                "proposed_files": dict(files),
                "operations": [],
                "base_hashes": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
            }
        skill = skills[name]
        skill["operations"].append(dict(op))
        files = skill["proposed_files"]
        action = op["action"]
        path = op.get("file_path") or "SKILL.md"
        if Path(path).is_absolute() or ".." in Path(path).parts:
            raise ValueError("Proposal path escapes skill")
        if action in ("create", "edit") or (action == "patch" and op.get("content") is not None):
            files["SKILL.md"] = op["content"]
        elif action == "patch":
            old = op.get("old_string")
            if not old or old not in files.get(path, ""):
                raise ValueError("Patch does not match its base")
            files[path] = files[path].replace(old, op["new_string"], -1 if op.get("replace_all") else 1)
        elif action == "write_file":
            files[path] = op["file_content"]
        elif action == "remove_file":
            files.pop(path, None)
        elif action == "delete":
            files.clear()
        else:
            raise ValueError("Unsupported skill mutation")
    for skill in skills.values():
        # The mutation ledger hashes raw bytes and records absolute file paths.
        prefix = skill["path"] + "/"
        before = {prefix + p: t for p, t in skill["existing_files"].items()}
        after = {prefix + p: t for p, t in skill["proposed_files"].items()}
        changed = {p for p in before.keys() | after.keys() if before.get(p) != after.get(p)}
        skill["before"] = [{"path": p, "sha256": skill["base_hashes"][p]} for p in sorted(changed) if p in before]
        skill["after"] = manifest({p: after[p] for p in changed if p in after})
        diffs = [
            line
            for p in changed
            for line in difflib.ndiff(before.get(p, "").splitlines(), after.get(p, "").splitlines())
        ]
        skill["lines_added"] = sum(line.startswith("+ ") for line in diffs)
        skill["lines_removed"] = sum(line.startswith("- ") for line in diffs)
    return skills


def verified_marks(evidence):
    from hermes_constants import get_hermes_home

    marks = []
    path = get_hermes_home() / "state.db"
    if not path.exists():
        return marks
    with sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True, timeout=2) as conn:
        lineage_available = any(row[1] == "parent_session_id" for row in conn.execute("PRAGMA table_info(sessions)"))

        def lineage(identity):
            visited = set()
            while identity and identity not in visited and len(visited) < 64:
                visited.add(identity)
                row = conn.execute("SELECT parent_session_id FROM sessions WHERE id=?", (identity,)).fetchone()
                if not row or not row[0]:
                    return identity
                identity = row[0]
            # Cyclic/corrupt ancestry cannot manufacture independent evidence.
            return min(visited) if visited else None

        refs = evidence.get("marks", [])
        if not isinstance(refs, list) or len(refs) > 100:
            raise ValueError("Evidence marks must be a bounded list")
        for ref in refs:
            if not isinstance(ref, dict) or type(ref.get("message_id")) is not int:
                continue
            row = conn.execute(
                "SELECT content,role FROM messages WHERE id=? AND session_id=?",
                (ref["message_id"], ref.get("session_id")),
            ).fetchone()
            if row and row[1] in ("user", "assistant"):
                marks.append(
                    {
                        "session_id": ref["session_id"],
                        "message_id": ref["message_id"],
                        "text": row[0],
                        "lineage_session_id": lineage(ref["session_id"]) if lineage_available else ref["session_id"],
                    }
                )
    return marks


def judge_write(*, proposal, session_id=None, cfg=None, **kwargs):
    from tools.skill_ledger import derive_actor, list_entries
    from tools.skill_provenance import is_unattended_review
    from tools.skill_review_events import preserve_proposal

    cfg = cfg or config()
    mode = cfg["write_gate"]["mode"]
    if mode == "off" or not (is_unattended_review() or derive_actor() == "curator"):
        return {"action": "allow"}
    turn_ref = f"{session_id or 'unknown'}:write:{uuid.uuid4().hex}"
    gate = {
        "disposition": "gate_error",
        "reason": "",
        "gate_version": POLICY_VERSION,
        "judge_model": cfg["model"],
        "actor_unattended": True,
        "turn_ref": turn_ref,
    }
    state = {"proposal": proposal}
    questions = {}
    result = {}
    skills = {}
    try:
        skills = preview(proposal)
        evidence = proposal.get("evidence") or {}
        marks = verified_marks(evidence)
        independent = len({r["lineage_session_id"] for r in marks})
        state.update(
            skills=list(skills.values()),
            evidence=evidence,
            marks=marks,
            independent_sessions=independent,
            ledger_counts={name: len(list_entries(name)) for name in skills},
        )
        if independent < cfg["write_gate"]["min_independent_sessions"]:
            gate.update(
                disposition="gate_refuse_recurrence", reason="Fewer than two verified independent source sessions"
            )
        elif not evidence.get("lesson") or not evidence.get("dimension") or not evidence.get("check"):
            gate.update(disposition="gate_refuse_evidence", reason="Missing measurable dimension or check")
        else:
            questions = {
                f"{i}_{key}": noul("Judge only skills[" + str(i) + "]. " + QUESTIONS[key])
                for i, name in enumerate(skills)
                for key in (
                    "new_information",
                    "behavior_change",
                    "measurable",
                    "recurrence",
                    "contradiction",
                    "worth_tokens",
                )
            }
            result = client.decide(state, questions, cfg)
            answers = result["answers"]
            passing = all(
                answers[key]["noul"] <= cfg["write_gate"]["contradiction_max"]
                if key.endswith("_contradiction")
                else answers[key]["noul"] >= cfg["write_gate"]["threshold"]
                for key in questions
            )
            gate.update(
                disposition="gate_pass" if passing else "gate_refuse_evidence",
                reason="Evidence judgment passed" if passing else "Proposal did not meet evidence thresholds",
            )
    except client.Unavailable as exc:
        gate.update(disposition=exc.disposition, reason=str(exc))
    except Exception as exc:
        gate.update(disposition="gate_error", reason="Skill judgment failed: " + type(exc).__name__)
    try:
        gate["gate_call_ref"] = ledger.record(
            session_id, turn_ref, "write", state, questions, result, mode=mode, outcome=gate
        )
    except Exception:
        gate.update(disposition="gate_error", reason="Decision record could not be persisted")
    allow = gate["disposition"] == "gate_pass"
    if not allow:
        captured = preserve_proposal(proposal, session_id=session_id, gate=gate, previews=skills)
        if captured in ("outbox-write-failed", "error"):
            gate["reason"] += "; proposal review capture failed"
    if mode == "shadow":
        return {
            "action": "allow",
            "gate": {**gate, "observed_disposition": gate["disposition"], "disposition": "gate_bypassed"},
        }
    return {"action": "allow" if allow else "block", "message": gate["reason"], "gate": gate}
