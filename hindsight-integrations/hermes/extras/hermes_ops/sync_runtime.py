"""Compare or mirror fork-maintained operator source; configuration stays untouched.

Roots come from hermes_ops_config: an explicit --workspace/--hermes-home, then
HERMES_OPS_WORKSPACE / HERMES_HOME, then the resolved Hermes home. Nothing is
copied and no configuration is changed unless --apply is given.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import shutil
from pathlib import Path

import hermes_ops_config as config


def digest(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


# Runtime-only tools: maintained here, installed by hand after review because a
# formatted copy would invalidate their own journals or gate receipts.
MANUAL_ONLY = (
    "retain-takeaway.py",
    "index-part.py",
    "tag02_gate_apply.py",
    "tag02_gate_dryrun.py",
    "verify_post_extraction_tag_queue.py",
)
# The tag queue and its helpers keep operator state (queue.sqlite, ledger,
# control switch) beside them and are installed by the operator, not mirrored.
QUEUE_FILES = ("post_extraction_tag_queue.py", "tagger_lock.py", "tagger_ledger.py", "tagger_fallback.py")
# Shell operators that live in the Hermes home rather than the workspace.
HOME_SCRIPTS = ("dream-bank.sh", "monitor-brief-input.sh", "health-check.sh")
# Data files the runtime reads from the package root; a synced install keeps them
# beside the scripts directory, which is where hermes_ops_config looks for them.
ROOT_DATA = ("extraction-policy.json", "tag-registry.json")


def targets(source: Path, workspace: Path, home: Path) -> dict[Path, Path]:
    scripts = {p: workspace / "scripts" / p.name for p in (source / "scripts").iterdir() if p.is_file()}
    for name in HOME_SCRIPTS:
        scripts[source / "scripts" / name] = home / "scripts" / name
    for name in MANUAL_ONLY + QUEUE_FILES:
        scripts.pop(source / "scripts" / name, None)
    data = {source / name: workspace / name for name in ROOT_DATA}
    plugin = {
        p: home / "plugins/jev_gate" / p.relative_to(source / "jev_gate")
        for p in (source / "jev_gate").iterdir()
        if p.is_file()
    }
    return {**scripts, **data, **plugin}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, help="operator workspace root (default: resolved, see module doc)")
    parser.add_argument("--hermes-home", type=Path, help="Hermes home (default: resolved)")
    parser.add_argument("--apply", action="store_true", help="back up and copy reviewed source files")
    args = parser.parse_args()
    source = Path(__file__).resolve().parent
    workspace, home = config.workspace(args.workspace).resolve(), config.hermes_home(args.hermes_home).resolve()
    if not workspace.is_dir() or not home.is_dir():
        parser.error("Both existing target directories must be specified correctly")
    plan = targets(source, workspace, home)
    changed = {a: b for a, b in plan.items() if digest(a) != digest(b)}
    backup = (
        workspace
        / "backups"
        / ("fork-source-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    )
    if args.apply and changed:
        backup.mkdir(parents=True, exist_ok=False)
        for a, b in changed.items():
            if b.exists():
                saved = (
                    backup
                    / ("workspace" if b.is_relative_to(workspace) else "hermes-home")
                    / b.relative_to(workspace if b.is_relative_to(workspace) else home)
                )
                saved.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(b, saved)
        for a, b in changed.items():
            b.parent.mkdir(parents=True, exist_ok=True)
            pending = b.with_name(b.name + ".fork-pending")
            shutil.copy2(a, pending)
            pending.replace(b)
    print(
        json.dumps(
            {
                "applied": args.apply,
                "changed": [str(b) for b in changed.values()],
                "backup": str(backup) if args.apply and changed else None,
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
