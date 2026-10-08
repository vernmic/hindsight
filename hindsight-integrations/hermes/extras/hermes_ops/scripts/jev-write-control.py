"""Operator off/on controls for the write gate and the fallback-review trigger.

Each control touches exactly one config key and preserves everything else,
including comments (atomic_config_write is ruamel round-trip based):

  write off|on  -> jev_gate.write_gate.mode  (off|enforced)
  review off|on -> fallback_review.enabled   (false|true)
  status        -> report both controls plus the read mode, no write

The read-selection switch stays scripts/jev-control.py; this helper never
changes jev_gate.mode. Mode changes take effect next turn without a restart.
"""

import argparse
import fcntl
import json
import sys
from pathlib import Path

sys.path.insert(0, "/home/vern/.hermes/hermes-agent")


def control(control_name, action, home):
    import yaml
    from hermes_cli.config import atomic_config_write

    home = Path(home)
    path = home / "config.yaml"
    if control_name not in ("write", "review", "status"):
        raise ValueError("Unknown control: " + control_name)
    with (home / ".jev-write-control.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        cfg = yaml.safe_load(path.read_text())
        if not isinstance(cfg, dict) or not isinstance(cfg.get("jev_gate"), dict):
            raise ValueError("Jev is not configured in this profile")
        if control_name == "write" and action != "status":
            if not isinstance(cfg["jev_gate"].get("write_gate"), dict):
                raise ValueError("jev_gate.write_gate is not configured in this profile")
            cfg["jev_gate"]["write_gate"]["mode"] = "enforced" if action == "on" else "off"
            atomic_config_write(path, cfg)
        elif control_name == "review" and action != "status":
            review = cfg.get("fallback_review")
            if not isinstance(review, dict):
                review = cfg["fallback_review"] = {}
            review["enabled"] = action == "on"
            atomic_config_write(path, cfg)
        gate = cfg["jev_gate"]
        review = cfg.get("fallback_review") or {}
        return {
            "write_gate_mode": (gate.get("write_gate") or {}).get("mode"),
            "fallback_review_enabled": bool(review.get("enabled", False)),
            "fallback_review_dry_run": bool(review.get("dry_run", False)),
            "fallback_review_armed": bool(review.get("enabled", False)) and not bool(review.get("dry_run", False)),
            "read_mode": gate.get("mode"),
            "read_mode_unchanged_by_this_tool": True,
            "takes_effect": "next turn; no restart needed for mode changes",
        }


def main():
    from hermes_constants import get_hermes_home

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("control_name", choices=("write", "review", "status"))
    parser.add_argument("action", choices=("on", "off", "status"), nargs="?", default="status")
    args = parser.parse_args()
    print(json.dumps(control(args.control_name, args.action, get_hermes_home()), ensure_ascii=True))


if __name__ == "__main__":
    main()
