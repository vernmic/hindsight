#!/usr/bin/env python3
"""TAG-02 bounded change applier: journal, resume, version checks, rollback.

Applies exactly the audit change list (C2, C2b, C3) recorded in
reports/legacy-routing-audit-20261007.json to three files. Each item carries a
before-image and an after-image plus md5s; the journal is the single source of
truth for state, so:

  * resume  - reruns skip items already in the target state;
  * version - any on-disk revision that is neither the audited before-image nor
              the intended after-image aborts the run before anything is written
              (tool/schema mismatch), and a replacement block that does not match
              exactly once also aborts.

usage: tag02_gate_apply.py {snapshot|apply|revert|verify} [--journal PATH]

Exit codes: 0 ok, 2 usage, 3 version/tool mismatch (aborted, nothing written),
4 state error.
"""

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

JOURNAL = Path("/mnt/i/hermes/reports/tag02-change-journal-20261007.json")
VERSION = "tag02-change-journal-v1"
TASK_ID = "t_076f1993"
AUDIT = "/mnt/i/hermes/reports/legacy-routing-audit-20261007.json"
SNAPSHOT_DIR = "/mnt/i/hermes/backups/tag02-20261007"

# ---- C2: retain-takeaway.py lines 34-35 (drop the tags+content blob) --------
C2_OLD = (
    '        tags = list(row.get("tags") or [])\n'
    '        blob = " ".join(tags) + " " + content\n'
    "        missing = [p for p in REQUIRED if p not in blob]\n"
)
C2_NEW = (
    '        tags = list(row.get("tags") or [])\n'
    "        missing = [p for p in REQUIRED if not any(t.startswith(p) and len(t) > len(p) for t in tags)]\n"
)

# ---- C2b: knowledge/index-extracts/index-part.py lines 38-42 (same class) ---
C2B_OLD = (
    '        blob = " ".join(r["tags"]) + " " + r["content"]\n'
    '        for p in ("kind:", "scope:", "domain:", "confidence:", "source:"):\n'
    "            if p not in blob:\n"
    '                print(f"row {i}: tag blob missing {p}", file=sys.stderr)\n'
    "                return 4\n"
)
C2B_NEW = (
    '        for p in ("kind:", "scope:", "domain:", "confidence:", "source:"):\n'
    '            if not any(t.startswith(p) and len(t) > len(p) for t in r["tags"]):\n'
    '                print(f"row {i}: tag missing {p}", file=sys.stderr)\n'
    "                return 4\n"
)

# ---- C3: /home/vern/.hermes/workspace/memory-index.md lines 10-15 (docs) ---
C3_OLD = (
    "- kind:        decision | skill-distillation | research-finding | dream-observation |\n"
    "               gepa-outcome | preference | correction | cadence | goal | seed\n"
    "- scope:       project | harness | general\n"
    "- domain:      coding | frontend | infrastructure | hermes-ecosystem | python-cli | ...\n"
    "- confidence:  high | medium | speculative\n"
    "- source:      session:<id> | file:<path> | web:<url> | dream-run:<ts> | gepa-run:<id>\n"
)
C3_NEW = (
    "- kind:        docs | fact | decision | constraint | correction | directive |\n"
    "               design-decision | build | dream-observation | status | incident |\n"
    "               research-finding | user-profile | skill-distillation | finding |\n"
    "               infra | preference | environment | implementation |\n"
    "               skill-maintenance | event | process-rule\n"
    "- scope:       hermes-agent | project | hermes-ops | harness\n"
    "- domain:      skills | config | guides | messaging | models | install |\n"
    "               dashboard | cli | security | kanban | knowledge | infrastructure |\n"
    "               hermes-ecosystem | ...\n"
    "- confidence:  high | provisional | medium | confirmed\n"
    "- source:      session | derived | document:<id>\n"
)

ITEMS = [
    {
        "id": "C2",
        "kind": "routing (intake gate)",
        "path": "/home/vern/.hermes/scripts/retain-takeaway.py",
        "before_md5": "10658b9c3725fa3724a83d176141e793",
        "bak": "/home/vern/.hermes/scripts/retain-takeaway.py.bak-20261007",
        "rollback": "journal revert, or copy the .bak back",
        "replacements": [(C2_OLD, C2_NEW)],
    },
    {
        "id": "C2b",
        "kind": "routing (intake gate, same class)",
        "path": "/mnt/i/hermes/knowledge/index-extracts/index-part.py",
        "before_md5": "74df7d853e76332a508c6430db3a5123",
        "bak": None,
        "rollback": "journal revert, or git checkout -- knowledge/index-extracts/index-part.py",
        "replacements": [(C2B_OLD, C2B_NEW)],
    },
    {
        "id": "C3",
        "kind": "documentation (NOT routing)",
        "path": "/home/vern/.hermes/workspace/memory-index.md",
        "before_md5": "3845baae4b8022e326166f7f2f93e00f",
        "bak": "/home/vern/.hermes/workspace/memory-index.md.bak-20261007",
        "rollback": "journal revert, or copy the .bak back",
        "replacements": [(C3_OLD, C3_NEW)],
    },
]


def md5_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def read_text(path):
    raw = Path(path).read_bytes()
    text = raw.decode("utf-8")
    if "\r\n" in text:
        raise SystemExit("tool/schema mismatch: CRLF line endings in %s" % path)
    return text


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_journal():
    if JOURNAL.exists():
        return json.loads(JOURNAL.read_text(encoding="utf-8"))
    return None


def save_journal(journal):
    JOURNAL.parent.mkdir(parents=True, exist_ok=True)
    tmp = JOURNAL.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(journal, indent=2), encoding="utf-8")
    os.replace(tmp, JOURNAL)


def build_after(item, before):
    after = before
    for old, new in item["replacements"]:
        n = after.count(old)
        if n != 1:
            raise SystemExit("tool/schema mismatch: block %s matched %d times in %s" % (item["id"], n, item["path"]))
        after = after.replace(old, new)
    if after == before:
        raise SystemExit("tool/schema mismatch: %s produced no change" % item["id"])
    return after


def do_snapshot(journal):
    if journal is not None:
        print("journal exists (%s); refusing to re-snapshot" % JOURNAL)
        return verify(journal)
    items = []
    SNAPSHOT_DIR_PATH = Path(SNAPSHOT_DIR)
    SNAPSHOT_DIR_PATH.mkdir(parents=True, exist_ok=True)
    for spec in ITEMS:
        before = read_text(spec["path"])
        got = md5_text(before)
        if got != spec["before_md5"]:
            raise SystemExit(
                "tool/schema mismatch: %s is at md5 %s, audited revision is %s"
                % (spec["path"], got, spec["before_md5"])
            )
        after = build_after(spec, before)
        Path(SNAPSHOT_DIR, Path(spec["path"]).name + ".before").write_bytes(before.encode("utf-8"))
        if spec["bak"]:
            Path(spec["bak"]).write_bytes(before.encode("utf-8"))
        items.append(
            {
                "id": spec["id"],
                "kind": spec["kind"],
                "path": spec["path"],
                "rollback": spec["rollback"],
                "bak": spec["bak"],
                "before_md5": got,
                "after_md5": md5_text(after),
                "state": "pending",
                "before": before,
                "after": after,
            }
        )
        print("snapshot %-4s %s  before=%s after=%s" % (spec["id"], spec["path"], got, md5_text(after)))
    journal = {
        "version": VERSION,
        "task_id": TASK_ID,
        "audit": AUDIT,
        "created_at": now(),
        "applier_md5": md5_text(Path(__file__).read_text(encoding="utf-8")),
        "snapshot_dir": SNAPSHOT_DIR,
        "items": items,
        "events": [{"ts": now(), "action": "snapshot", "item": "*"}],
    }
    save_journal(journal)
    print("journal written: %s" % JOURNAL)
    return 0


def do_apply(journal):
    if journal is None:
        print("no journal; run snapshot first")
        return 4
    changed = 0
    for it in journal["items"]:
        cur = md5_text(read_text(it["path"]))
        if cur == it["after_md5"]:
            if it["state"] != "applied":
                it["state"] = "applied"
                journal["events"].append({"ts": now(), "action": "resume-skip", "item": it["id"]})
            print("skip   %-4s already applied" % it["id"])
            continue
        if cur != it["before_md5"]:
            raise SystemExit(
                "version-conflict: %s is at md5 %s (neither before %s nor after %s); aborting"
                % (it["path"], cur, it["before_md5"], it["after_md5"])
            )
        Path(it["path"]).write_bytes(it["after"].encode("utf-8"))
        got = md5_text(read_text(it["path"]))
        if got != it["after_md5"]:
            raise SystemExit("post-write md5 mismatch on %s" % it["path"])
        it["state"] = "applied"
        journal["events"].append({"ts": now(), "action": "apply", "item": it["id"], "from_md5": cur, "to_md5": got})
        changed += 1
        print("apply  %-4s %s -> %s" % (it["id"], cur, got))
    save_journal(journal)
    print("applied=%d" % changed)
    return 0


def do_revert(journal):
    if journal is None:
        print("no journal; nothing to revert")
        return 4
    changed = 0
    for it in journal["items"]:
        cur = md5_text(read_text(it["path"]))
        if cur == it["before_md5"]:
            if it["state"] != "reverted":
                it["state"] = "reverted"
                journal["events"].append({"ts": now(), "action": "resume-skip", "item": it["id"]})
            print("skip   %-4s already at before-image" % it["id"])
            continue
        if cur != it["after_md5"]:
            raise SystemExit(
                "version-conflict: %s is at md5 %s (neither after %s nor before %s); aborting"
                % (it["path"], cur, it["after_md5"], it["before_md5"])
            )
        Path(it["path"]).write_bytes(it["before"].encode("utf-8"))
        got = md5_text(read_text(it["path"]))
        if got != it["before_md5"]:
            raise SystemExit("post-revert md5 mismatch on %s" % it["path"])
        it["state"] = "reverted"
        journal["events"].append({"ts": now(), "action": "revert", "item": it["id"], "from_md5": cur, "to_md5": got})
        changed += 1
        print("revert %-4s %s -> %s" % (it["id"], cur, got))
    save_journal(journal)
    print("reverted=%d" % changed)
    return 0


def verify(journal):
    if journal is None:
        print("no journal")
        return 4
    rc = 0
    for it in journal["items"]:
        cur = md5_text(read_text(it["path"]))
        if cur == it["before_md5"]:
            disk = "before"
        elif cur == it["after_md5"]:
            disk = "after"
        else:
            disk = "UNKNOWN"
            rc = 3
        print(
            "%-4s state=%-8s disk=%-7s before=%s after=%s"
            % (it["id"], it["state"], disk, it["before_md5"][:8], it["after_md5"][:8])
        )
    return rc


def main():
    global JOURNAL
    args = sys.argv[1:]
    if "--journal" in args:
        JOURNAL = Path(args[args.index("--journal") + 1])
        args = [a for a in args if a != "--journal" and a != str(JOURNAL)]
    if len(args) != 1 or args[0] not in ("snapshot", "apply", "revert", "verify"):
        print(__doc__)
        return 2
    journal = load_journal()
    action = args[0]
    if action == "snapshot":
        return do_snapshot(journal)
    if action == "apply":
        return do_apply(journal)
    if action == "revert":
        return do_revert(journal)
    return verify(journal)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit as exc:
        if isinstance(exc.code, str):
            print(exc.code, file=sys.stderr)
            raise SystemExit(3)
        raise
