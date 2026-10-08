#!/usr/bin/env python3
"""Append one part's facts to the batch facts file, retain them, audit them.

usage: index-part.py --part PART.jsonl --facts FACTS.jsonl --audit AUDIT.jsonl
                     [--skips SKIPFILE]

--part   JSONL of fact rows ({content, context, source, tags})
--skips  optional text file, one path per line, recorded as skipped
Each line of PART must carry "source"; audit counts are per source.
"""

import json
import subprocess
import sys
from pathlib import Path

PY = "/home/vern/.hermes/hermes-agent/venv/bin/python"
RETAIN = "/home/vern/.hermes/scripts/retain-takeaway.py"


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def main() -> int:
    part = Path(arg("--part"))
    facts = Path(arg("--facts"))
    audit = Path(arg("--audit"))
    skips = arg("--skips")

    lines = [ln for ln in part.read_text(encoding="utf-8").splitlines() if ln.strip()]
    rows = [json.loads(ln) for ln in lines]
    # validate every row before touching anything
    for i, r in enumerate(rows, 1):
        for key in ("content", "context", "source", "tags"):
            if not r.get(key):
                print(f"row {i}: missing {key}", file=sys.stderr)
                return 4
        for p in ("kind:", "scope:", "domain:", "confidence:", "source:"):
            if not any(t.startswith(p) and len(t) > len(p) for t in r["tags"]):
                print(f"row {i}: tag missing {p}", file=sys.stderr)
                return 4

    with facts.open("a", encoding="utf-8") as fh:
        for ln in lines:
            fh.write(ln + "\n")

    proc = subprocess.run([PY, RETAIN, str(part)], capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    ok = proc.returncode == 0 and "ok=True" in proc.stdout

    counts = {}
    order = []
    for r in rows:
        if r["source"] not in counts:
            counts[r["source"]] = 0
            order.append(r["source"])
        counts[r["source"]] += 1

    with audit.open("a", encoding="utf-8") as fh:
        for src in order:
            fh.write(
                json.dumps(
                    {
                        "path": src,
                        "facts": counts[src],
                        "status": "retained" if ok else "retain_failed",
                    }
                )
                + "\n"
            )
        if skips and Path(skips).exists():
            for ln in Path(skips).read_text(encoding="utf-8").splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                fh.write(
                    json.dumps(
                        {
                            "path": ln,
                            "facts": 0,
                            "status": "skipped",
                            "reason": "no retainable facts",
                        }
                    )
                    + "\n"
                )

    print(f"part={part.name} rows={len(rows)} sources={len(order)} retain_ok={ok}")
    return 0 if ok else 5


if __name__ == "__main__":
    raise SystemExit(main())
