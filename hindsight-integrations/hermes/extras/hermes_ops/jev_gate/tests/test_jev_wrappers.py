"""The four jev-*.ps1 wrappers are parameterized, not machine-pinned (review F3).

Static checks only: the wrappers are never executed against a live home here.
The profile-separation behaviour itself is covered by test_jev_control.py,
which proves a secondary-profile control() call cannot touch a primary config.
"""

from pathlib import Path

WRAPPERS = ["jev-on.ps1", "jev-off.ps1", "jev-write-on.ps1", "jev-write-off.ps1"]


def _source(name: str) -> str:
    return (Path(__file__).parents[2] / "scripts" / name).read_text(encoding="utf-8")


def test_every_wrapper_parameterizes_the_machine_values() -> None:
    for name in WRAPPERS:
        text = _source(name)
        for parameter in ("$Distro", "$User", "$Interpreter", "$Controller", "$HermesHome"):
            assert parameter in text, f"{name} lost {parameter}"
        assert text.index("param(") < text.index("wsl.exe"), name


def test_profile_selection_flows_through_hermes_home_argv() -> None:
    for name in WRAPPERS:
        text = _source(name)
        assert text.count("HERMES_HOME=$HermesHome") == 1, name
        # The invocation splats an argv array; no shell-built command string remains.
        assert "& wsl.exe @" in text, name


def test_wrappers_pin_an_explicit_action() -> None:
    assert "'on')" in _source("jev-on.ps1")
    assert "'off')" in _source("jev-off.ps1")
    assert "'write', 'on'" in _source("jev-write-on.ps1")
    assert "'review', 'off'" in _source("jev-write-off.ps1")