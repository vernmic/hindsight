#!/usr/bin/env python3
"""TAG-02 dry-run harness: exercise the real intake gates with no bank access.

Loads the two gate files as modules and runs their real main() validation path
against synthetic cases and against historical JSONL corpora. The Hindsight
client and the subprocess call are replaced by fakes, so nothing is retained and
no network call is made; the harness asserts that.

usage: tag02_gate_dryrun.py --label LABEL [--scan DIR] [--json OUT]
"""

import argparse
import hashlib
import importlib.util
import io
import json
import sys
import tempfile
from pathlib import Path

VENV_PY = "/home/vern/.hermes/hermes-agent/venv/bin/python"
RETAIN = Path("/home/vern/.hermes/scripts/retain-takeaway.py")
INDEX_PART = Path("/mnt/i/hermes/knowledge/index-extracts/index-part.py")
SCAN_DEFAULT = Path("/mnt/i/hermes/knowledge/index-extracts")

BANK_CALLS = []


def md5(p):
    return hashlib.md5(Path(p).read_bytes()).hexdigest()


class FakeClient:
    """Records retain requests instead of sending them."""

    def __init__(self, base_url=None, timeout=None):
        self.base_url = base_url

    def retain_batch(self, bank_id=None, items=None):
        BANK_CALLS.append({"bank_id": bank_id, "items": list(items or [])})
        return type("Resp", (), {"success": True})()


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_retain():
    fake = type(sys)("hindsight_client")
    fake.HindsightClient = FakeClient
    fake.Hindsight = FakeClient
    sys.modules["hindsight_client"] = fake
    return load_module("tag02_retain", RETAIN)


def run_retain_row(rt, row, tmpdir):
    p = Path(tmpdir, "row.jsonl")
    p.write_text(json.dumps(row) + "\n", encoding="utf-8")
    old = sys.argv
    old_err = sys.stderr
    old_out = sys.stdout
    sys.argv = ["retain-takeaway.py", str(p)]
    buf = io.StringIO()
    sys.stderr = buf
    sys.stdout = io.StringIO()
    try:
        rc = rt.main()
    except SystemExit as exc:
        rc = exc.code if isinstance(exc.code, int) else 1
    finally:
        sys.argv = old
        sys.stderr = old_err
        sys.stdout = old_out
    return rc, buf.getvalue().strip()


def run_index_part_row(ip, row, tmpdir):
    part = Path(tmpdir, "part.jsonl")
    part.write_text(json.dumps(row) + "\n", encoding="utf-8")
    facts = Path(tmpdir, "facts.jsonl")
    audit = Path(tmpdir, "audit.jsonl")
    for f in (facts, audit):
        if f.exists():
            f.unlink()

    class FakeProc:
        returncode = 0
        stdout = "retained=1 bank=hermes-ops ok=True\n"
        stderr = ""

    old_run = ip.subprocess.run
    ip.subprocess.run = lambda *a, **k: FakeProc()
    old = sys.argv
    old_out = sys.stdout
    sys.argv = ["index-part.py", "--part", str(part), "--facts", str(facts), "--audit", str(audit)]
    sys.stdout = io.StringIO()
    try:
        rc = ip.main()
    except SystemExit as exc:
        rc = exc.code if isinstance(exc.code, int) else 1
    finally:
        ip.subprocess.run = old_run
        sys.argv = old
        sys.stdout = old_out
    return rc


LEGIT = {
    "bank": "hermes-ops",
    "content": "2026-09-26: Hermes upgraded from v0.13.0 to v0.21.5 on the WSL2 host.",
    "context": "install",
    "source": "/mnt/i/hermes/docs/install.md",
    "tags": [
        "kind:fact",
        "scope:hermes-ops",
        "domain:install",
        "confidence:confirmed",
        "source:/mnt/i/hermes/docs/install.md",
    ],
}
PROSE_ONLY_SOURCE = {
    "bank": "hermes-ops",
    "content": "2026-09-26: Hermes upgraded; remember every fact needs a source: tag.",
    "context": "install",
    "source": "/mnt/i/hermes/docs/install.md",
    "tags": ["kind:fact", "scope:hermes-ops", "domain:install", "confidence:confirmed"],
}
VALUELESS_TAG = {
    "bank": "hermes-ops",
    "content": "2026-09-26: Hermes upgraded from v0.13.0 to v0.21.5.",
    "context": "install",
    "source": "/mnt/i/hermes/docs/install.md",
    "tags": [
        "kind:",
        "scope:hermes-ops",
        "domain:install",
        "confidence:confirmed",
        "source:/mnt/i/hermes/docs/install.md",
    ],
}
CASES = [
    ("legit", LEGIT, "PASS"),
    ("prose-only-source", PROSE_ONLY_SOURCE, "FAIL"),
    ("valueless-tag", VALUELESS_TAG, "FAIL"),
]


def scan(rt, directory, tmpdir):
    rows = passed = failed = not_input = 0
    reasons = {}
    verdicts = {}
    files = sorted(Path(directory).glob("*.jsonl"))
    for f in files:
        for i, ln in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if not ln.strip():
                continue
            try:
                row = json.loads(ln)
            except ValueError:
                not_input += 1
                continue
            if not isinstance(row, dict) or "tags" not in row or "content" not in row:
                not_input += 1
                continue
            rows += 1
            key = "%s:%d:%s" % (f.name, i, hashlib.md5(ln.encode("utf-8")).hexdigest()[:12])
            rc, reason = run_retain_row(rt, row, tmpdir)
            verdicts[key] = "PASS" if rc == 0 else "FAIL"
            if rc == 0:
                passed += 1
            else:
                failed += 1
                reasons[f.name] = reasons.get(f.name, 0) + 1
                verdicts[key] = "FAIL:" + reason
    return {
        "dir": str(directory),
        "files": len(files),
        "input_rows": rows,
        "passed": passed,
        "failed": failed,
        "not_input_rows": not_input,
        "failed_by_file": reasons,
        "verdicts": verdicts,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--scan", default=None)
    ap.add_argument("--json", default=None)
    a = ap.parse_args()

    rt = load_retain()
    ip = load_module("tag02_indexpart", INDEX_PART)

    out = {
        "label": a.label,
        "retain_md5": md5(RETAIN),
        "index_part_md5": md5(INDEX_PART),
        "cases": [],
        "scan": None,
        "bank_calls": 0,
    }
    with tempfile.TemporaryDirectory() as td:
        for name, row, expect in CASES:
            rrc, rreason = run_retain_row(rt, row, td)
            irc = run_index_part_row(ip, row, td)
            got = "PASS" if (rrc == 0 and irc == 0) else "FAIL"
            out["cases"].append(
                {
                    "case": name,
                    "expect": expect,
                    "got": got,
                    "retain_rc": rrc,
                    "index_part_rc": irc,
                    "retain_stderr": rreason,
                    "match": got == expect,
                }
            )
            print(
                "%-20s expect=%-4s retain_rc=%d index_part_rc=%d -> %s %s"
                % (name, expect, rrc, irc, got, "OK" if got == expect else "MISMATCH")
            )
        if a.scan:
            out["scan"] = scan(rt, a.scan, td)

    out["bank_calls"] = len(BANK_CALLS)
    out["bank_write_attempts"] = sum(len(c["items"]) for c in BANK_CALLS)
    print("bank_calls=%d bank_write_attempts=%d" % (out["bank_calls"], out["bank_write_attempts"]))
    if a.scan:
        s = out["scan"]
        print(
            "scan files=%d input_rows=%d passed=%d failed=%d not_input=%d"
            % (s["files"], s["input_rows"], s["passed"], s["failed"], s["not_input_rows"])
        )
    if a.json:
        Path(a.json).write_text(json.dumps(out, indent=2), encoding="utf-8")
        print("json=%s" % a.json)
    bad = [c for c in out["cases"] if not c["match"]]
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
