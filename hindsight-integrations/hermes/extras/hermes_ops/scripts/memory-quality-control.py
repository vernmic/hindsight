"""Restore or enable the extractor candidate; stop or enable journaled gardening."""

import argparse
import datetime
import json
import urllib.request
from pathlib import Path

ROOT = Path("/mnt/i/hermes")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("component", choices=["extractor", "gardener"])
    p.add_argument("mode", choices=["on", "off", "status"])
    a = p.parse_args()
    if a.component == "gardener":
        path = ROOT / "reports/gardener-control-20261007.json"
        if a.mode != "status":
            path.write_text(
                json.dumps(
                    {
                        "enabled": a.mode == "on",
                        "bank": "hermes-ops",
                        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    },
                    indent=2,
                )
            )
        print(path.read_text())
        return
    endpoint = "http://localhost:8888/v1/default/banks/hermes-ops/config"
    if a.mode != "status":
        record = json.loads((ROOT / "reports/extractor-active-20261007.json").read_text())
        updates = record["candidate"] if a.mode == "on" else record["rollback"]
        request = urllib.request.Request(
            endpoint,
            data=json.dumps({"updates": updates}).encode(),
            headers={"Content-Type": "application/json"},
            method="PATCH",
        )
        with urllib.request.urlopen(request, timeout=30) as f:
            json.load(f)
    with urllib.request.urlopen(endpoint, timeout=30) as f:
        data = json.load(f)
    print(
        json.dumps(
            {
                "bank": "hermes-ops",
                "mode": data["config"]["retain_extraction_mode"],
                "context_boundaries_enabled": "HERMES_CONTEXT_BOUNDARIES_V1"
                in (data["config"].get("retain_custom_instructions") or ""),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
