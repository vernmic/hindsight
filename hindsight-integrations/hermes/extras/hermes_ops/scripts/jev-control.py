"""Switch live Jev selections on/off without changing write governance or other settings."""

import argparse
import fcntl
import json
import sys
from pathlib import Path

sys.path.insert(0, "/home/vern/.hermes/hermes-agent")


def control(action, home):
    import yaml
    from hermes_cli.config import atomic_config_write

    home = Path(home)
    path = home / "config.yaml"
    with (home / ".jev-control.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        cfg = yaml.safe_load(path.read_text())
        if not isinstance(cfg, dict) or not isinstance(cfg.get("jev_gate"), dict):
            raise ValueError("Jev is not configured in this profile")
        if action != "status":
            cfg["jev_gate"]["mode"] = {"off": "off", "on": "enforced"}[action]
            atomic_config_write(path, cfg)
        gate = cfg["jev_gate"]
        return {
            "read_mode": gate.get("mode"),
            "baseline_selections": gate.get("mode") == "off",
            "write_gate_mode": (gate.get("write_gate") or {}).get("mode"),
            "recall_timeout_seconds": gate.get("recall_timeout_seconds"),
            "turn_budget_seconds": gate.get("turn_budget_seconds"),
            "takes_effect": "next turn; no restart needed for mode changes",
        }


def main():
    from hermes_constants import get_hermes_home

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("on", "off", "status"))
    args = parser.parse_args()
    print(json.dumps(control(args.action, get_hermes_home()), ensure_ascii=True))


if __name__ == "__main__":
    main()
