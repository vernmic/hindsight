#!/usr/bin/env python3
"""Retain one JSONL of takeaways into the hermes-ops Hindsight bank.

Refuses every other bank. Never deletes. Each line is
{"content","context","tags","source"}. Tags must include kind, scope,
domain, confidence, and source.
"""

import json
import sys
from pathlib import Path

BANK = "hermes-ops"
BASE = "http://host.docker.internal:8888"
REQUIRED = ("kind:", "scope:", "domain:", "confidence:", "source:")


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: retain-takeaway.py FACTS.jsonl", file=sys.stderr)
        return 2
    path = Path(sys.argv[1])
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    items = []
    for i, ln in enumerate(lines, 1):
        row = json.loads(ln)
        if row.get("bank") not in (None, BANK):
            print(f"line {i}: refused bank {row.get('bank')}", file=sys.stderr)
            return 3
        content = (row.get("content") or "").strip()
        if not content:
            print(f"line {i}: empty content", file=sys.stderr)
            return 3
        tags = list(row.get("tags") or [])
        missing = [p for p in REQUIRED if not any(t.startswith(p) and len(t) > len(p) for t in tags)]
        if missing:
            print(f"line {i}: missing {missing}", file=sys.stderr)
            return 3
        items.append(
            {
                "content": content,
                "context": row.get("context") or row.get("source") or "doc-index",
                "tags": tags,
            }
        )
    try:
        from hindsight_client import HindsightClient
    except ImportError:
        # hindsight_client >= 0.9 exposes the client as ``Hindsight``; the
        # constructor and retain_batch(bank_id, items) signature are unchanged.
        from hindsight_client import Hindsight as HindsightClient
    client = HindsightClient(base_url=BASE, timeout=120)
    resp = client.retain_batch(bank_id=BANK, items=items)
    print(f"retained={len(items)} bank={BANK} ok={getattr(resp, 'success', True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
