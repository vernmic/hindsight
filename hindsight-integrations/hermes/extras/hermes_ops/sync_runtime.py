"""Compare or mirror fork-maintained operator source; configuration stays untouched."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import shutil
from pathlib import Path


def digest(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def targets(source: Path, workspace: Path, home: Path) -> dict[Path, Path]:
    scripts = {p: workspace / "scripts" / p.name for p in (source / "scripts").iterdir() if p.is_file()}
    # dream-bank has always lived in the Hermes home, rather than the workspace.
    scripts[source / "scripts/dream-bank.sh"] = home / "scripts/dream-bank.sh"
    scripts.pop(source / "scripts/retain-takeaway.py", None)
    # Its source is maintained here; applying a formatted update needs a new
    # TAG-02 rollback receipt instead of invalidating the existing md5 journal.
    scripts.pop(source / "scripts/index-part.py", None)
    # Knowledge templates and journal-driven gate applicators require manual review.
    for name in ("tag02_gate_apply.py", "tag02_gate_dryrun.py", "verify_post_extraction_tag_queue.py"):
        scripts.pop(source / "scripts" / name, None)
    plugin = {
        p: home / "plugins/jev_gate" / p.relative_to(source / "jev_gate")
        for p in (source / "jev_gate").iterdir()
        if p.is_file()
    }
    return {**scripts, **plugin}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=Path("/mnt/i/hermes"))
    parser.add_argument("--hermes-home", type=Path, default=Path.home() / ".hermes")
    parser.add_argument("--apply", action="store_true", help="back up and copy reviewed source files")
    args = parser.parse_args()
    source = Path(__file__).resolve().parent
    workspace, home = args.workspace.resolve(), args.hermes_home.resolve()
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
