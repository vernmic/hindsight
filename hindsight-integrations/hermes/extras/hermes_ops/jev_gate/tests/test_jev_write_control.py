"""The write/review controls each change exactly one key and preserve the rest."""

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


def _load(monkeypatch):
    path = Path(__file__).parents[2] / "scripts/jev-write-control.py"
    _pin_runtime_dir(monkeypatch)
    spec = importlib.util.spec_from_file_location("jev_write_control_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG = """# Preserve operator config comments
model:
  default: existing
plugins:
  enabled: [hindsight, jev_gate]
jev_gate:
  mode: enforced
  recall_timeout_seconds: 30
  write_gate:
    mode: enforced
    min_independent_sessions: 2
fallback_review:
  enabled: true
  dry_run: false
"""


def _write(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(CONFIG)
    return config_path


def test_write_off_changes_only_write_gate(tmp_path, monkeypatch):
    module = _load(monkeypatch)
    config_path = _write(tmp_path)
    original = yaml.safe_load(CONFIG)
    result = module.control("write", "off", tmp_path)
    assert result["write_gate_mode"] == "off"
    after = yaml.safe_load(config_path.read_text())
    expected = yaml.safe_load(CONFIG)
    expected["jev_gate"]["write_gate"]["mode"] = "off"
    assert after == expected
    assert after["jev_gate"]["mode"] == original["jev_gate"]["mode"] == "enforced"
    assert after["fallback_review"]["enabled"] is True
    assert "# Preserve operator config comments" in config_path.read_text()
    assert module.control("write", "on", tmp_path)["write_gate_mode"] == "enforced"
    assert yaml.safe_load(config_path.read_text()) == original


def test_review_off_changes_only_fallback_review(tmp_path, monkeypatch):
    module = _load(monkeypatch)
    config_path = _write(tmp_path)
    module.control("review", "off", tmp_path)
    after = yaml.safe_load(config_path.read_text())
    assert after["fallback_review"]["enabled"] is False
    assert after["jev_gate"]["write_gate"]["mode"] == "enforced"
    assert after["jev_gate"]["mode"] == "enforced"
    assert "# Preserve operator config comments" in config_path.read_text()
    module.control("review", "on", tmp_path)
    assert yaml.safe_load(config_path.read_text()) == yaml.safe_load(CONFIG)


def test_review_off_creates_block_when_absent(tmp_path, monkeypatch):
    module = _load(monkeypatch)
    config_path = tmp_path
    config_path.mkdir(exist_ok=True)
    (config_path / "config.yaml").write_text("jev_gate:\n  mode: enforced\n  write_gate:\n    mode: enforced\n")
    result = module.control("review", "on", tmp_path)
    assert result["fallback_review_enabled"] is True
    assert result["fallback_review_armed"] is True
    after = yaml.safe_load((config_path / "config.yaml").read_text())
    assert after["fallback_review"] == {"enabled": True}
    assert after["jev_gate"]["write_gate"]["mode"] == "enforced"
    module.control("review", "off", tmp_path)
    after = yaml.safe_load((config_path / "config.yaml").read_text())
    assert after["fallback_review"] == {"enabled": False}


def test_status_reports_both_and_writes_nothing(tmp_path, monkeypatch):
    module = _load(monkeypatch)
    config_path = _write(tmp_path)
    before = config_path.read_text()
    result = module.control("status", "status", tmp_path)
    assert result["write_gate_mode"] == "enforced"
    assert result["fallback_review_enabled"] is True
    assert result["fallback_review_armed"] is True
    assert result["read_mode"] == "enforced"
    assert config_path.read_text() == before


def test_read_switch_leaves_write_controls_alone(tmp_path, monkeypatch):
    # the selection-only switch must preserve both write controls
    control_path = Path(__file__).parents[2] / "scripts/jev-control.py"
    _pin_runtime_dir(monkeypatch)
    spec = importlib.util.spec_from_file_location("jev_control_test", control_path)
    control = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(control)
    module = _load(monkeypatch)
    _write(tmp_path)
    control.control("off", tmp_path)
    result = module.control("status", "status", tmp_path)
    assert result["read_mode"] == "off"
    assert result["write_gate_mode"] == "enforced"
    assert result["fallback_review_enabled"] is True
    control.control("on", tmp_path)
    assert module.control("status", "status", tmp_path)["read_mode"] == "enforced"
