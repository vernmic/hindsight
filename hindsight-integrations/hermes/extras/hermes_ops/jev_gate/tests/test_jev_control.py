"""The operator switch changes only selection mode and preserves configuration."""

import importlib.util
from pathlib import Path
import yaml

def _pin_runtime_dir(monkeypatch):
    """Pin the control scripts' runtime-path insert to the SAME core this
    suite runs against; without it the script inserts the operator's real
    <home>/hermes-agent into the shared test process and later lazy imports
    (pm.*) resolve to the live install instead of the pinned core."""
    import hermes_cli
    monkeypatch.setenv("HERMES_RUNTIME_DIR", str(Path(hermes_cli.__file__).resolve().parents[1]))


def test_off_on_preserves_other_settings_and_comments(tmp_path, monkeypatch):
    path = Path(__file__).parents[2] / "scripts/jev-control.py"
    _pin_runtime_dir(monkeypatch)
    spec = importlib.util.spec_from_file_location("jev_control_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    text = """# Preserve operator config comments
model:
  default: existing
plugins:
  enabled: [hindsight, jev_gate]
jev_gate:
  mode: enforced
  recall_timeout_seconds: 30
  turn_budget_seconds: 36
  write_gate:
    mode: enforced
fallback_review:
  enabled: true
  dry_run: false
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(text)
    original = yaml.safe_load(text)
    assert module.control("off", tmp_path)["baseline_selections"] is True
    off = yaml.safe_load(config_path.read_text())
    off["jev_gate"]["mode"] = "enforced"
    assert off == original
    assert "# Preserve operator config comments" in config_path.read_text()
    assert module.control("on", tmp_path)["read_mode"] == "enforced"
    assert yaml.safe_load(config_path.read_text()) == original
    assert module.control("status", tmp_path)["write_gate_mode"] == "enforced"
