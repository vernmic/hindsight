#!/usr/bin/env python3
"""Tests for the TAG-02 journaled applier guards (scripts/tag02_gate_apply.py).

Covered: snapshot md5 guard (wrong revision aborts), replacement-block guard,
resume (rerun skips applied items), version-conflict abort on an unexpected
on-disk revision, and the revert round trip. All work happens on temp copies of
the before-image, so no live file is touched.

Run: <venv python> scripts/test_tag02_gate_apply.py
"""

import hashlib
import importlib.util
import shutil
import sys
import tempfile
from pathlib import Path

APPLIER = str(Path(__file__).resolve().parents[1] / "scripts/tag02_gate_apply.py")
BEFORE_IMAGE = Path(__file__).resolve().parent / "fixtures/retain-takeaway-before.txt"

FAILED = []


def md5(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def load_applier():
    spec = importlib.util.spec_from_file_location("tag02_apply_mod", APPLIER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def call(fn):
    """Run an applier action, returning (rc, message)."""
    try:
        return fn(), ""
    except SystemExit as exc:
        return 3, str(exc)


def check(name, cond, detail=""):
    print("%-46s %s %s" % (name, "PASS" if cond else "FAIL", detail))
    if not cond:
        FAILED.append(name)


def fresh_item(mod, tmp, before_text, before_md5=None):
    live = Path(tmp, "live.py")
    live.write_text(before_text, encoding="utf-8")
    mod.JOURNAL = Path(tmp, "journal.json")
    mod.SNAPSHOT_DIR = str(Path(tmp, "snap"))
    mod.ITEMS = [
        {
            "id": "T",
            "kind": "test",
            "path": str(live),
            "bak": None,
            "rollback": "n/a",
            "before_md5": before_md5 if before_md5 is not None else md5(before_text),
            "replacements": [(mod.C2_OLD, mod.C2_NEW)],
        }
    ]
    return live


def main():
    before_text = BEFORE_IMAGE.read_text(encoding="utf-8")
    mod = load_applier()

    # 1. snapshot then resume
    with tempfile.TemporaryDirectory() as tmp:
        live = fresh_item(mod, tmp, before_text)
        rc, msg = call(lambda: mod.do_snapshot(mod.load_journal()))
        check("snapshot accepts the audited revision", rc == 0 and not msg, msg)
        j = mod.load_journal()
        check("journal records pending state", j is not None and j["items"][0]["state"] == "pending")
        rc, msg = call(lambda: mod.do_apply(mod.load_journal()))
        check(
            "apply writes the after-image",
            rc == 0 and md5(live.read_text(encoding="utf-8")) == j["items"][0]["after_md5"],
            msg,
        )
        rc, msg = call(lambda: mod.do_apply(mod.load_journal()))
        check(
            "rerun skips the applied item (resume)",
            rc == 0 and mod.load_journal()["items"][0]["state"] == "applied",
            msg,
        )

    # 2. version conflict: an unexpected on-disk revision aborts, no write
    with tempfile.TemporaryDirectory() as tmp:
        live = fresh_item(mod, tmp, before_text)
        call(lambda: mod.do_snapshot(mod.load_journal()))
        call(lambda: mod.do_apply(mod.load_journal()))
        tampered = "#!/usr/bin/env python3\n# changed by another tool\n"
        live.write_text(tampered, encoding="utf-8")
        rc, msg = call(lambda: mod.do_apply(mod.load_journal()))
        check("apply aborts on an unexpected revision", rc == 3 and "version-conflict" in msg, msg)
        check("abort left the tampered file untouched", live.read_text(encoding="utf-8") == tampered)

    # 3. snapshot guard: wrong audited revision aborts, no journal written
    with tempfile.TemporaryDirectory() as tmp:
        fresh_item(mod, tmp, before_text, before_md5="0" * 32)
        rc, msg = call(lambda: mod.do_snapshot(mod.load_journal()))
        check("snapshot aborts on a mismatched revision", rc == 3 and "tool/schema mismatch" in msg, msg)
        check("aborted snapshot wrote no journal", not mod.JOURNAL.exists())

    # 4. replacement guard: a block that does not match aborts
    with tempfile.TemporaryDirectory() as tmp:
        fresh_item(mod, tmp, "totally different content\n")
        rc, msg = call(lambda: mod.do_snapshot(mod.load_journal()))
        check("snapshot aborts when a block does not match", rc == 3 and "tool/schema mismatch" in msg, msg)

    # 5. revert round trip
    with tempfile.TemporaryDirectory() as tmp:
        live = fresh_item(mod, tmp, before_text)
        call(lambda: mod.do_snapshot(mod.load_journal()))
        call(lambda: mod.do_apply(mod.load_journal()))
        rc, msg = call(lambda: mod.do_revert(mod.load_journal()))
        after = md5(live.read_text(encoding="utf-8"))
        check("revert restores the before-image", rc == 0 and after == md5(before_text), msg)
        rc, msg = call(lambda: mod.do_revert(mod.load_journal()))
        check(
            "rerun skips the reverted item (resume)",
            rc == 0 and mod.load_journal()["items"][0]["state"] == "reverted",
            msg,
        )

    print("failures=%d" % len(FAILED))
    return 1 if FAILED else 0


if __name__ == "__main__":
    raise SystemExit(main())
