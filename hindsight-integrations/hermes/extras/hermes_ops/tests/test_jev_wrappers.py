"""Static checks for the jev control wrappers and memory quality control.

The wrappers are never executed. WSLENV is empty on the operator machine, so
HERMES_HOME reaches the controller only when each wsl.exe argv passes it.
A missing exit check, a compiled-in home or mount path, or a swapped action
fails here.
"""

from __future__ import annotations

import unittest
from pathlib import Path


WRAPPERS = (
    "jev-on.ps1",
    "jev-off.ps1",
    "jev-write-on.ps1",
    "jev-write-off.ps1",
    "memory-quality-control.ps1",
)

# Actions pinned to the wsl.exe lines, in call order.
ACTIONS = {
    "jev-on.ps1": ("on",),
    "jev-off.ps1": ("off",),
    "jev-write-on.ps1": ("write on", "review on", "status"),
    "jev-write-off.ps1": ("status", "write off", "review off"),
    "memory-quality-control.ps1": ("$Component $Mode",),
}


def _scripts() -> Path:
    """hermes_ops/scripts, whether this file sits in jev_gate/tests or hermes_ops/tests."""
    for ancestor in Path(__file__).resolve().parents:
        candidate = ancestor / "scripts" / "jev-on.ps1"
        if candidate.is_file():
            return candidate.parent
    raise AssertionError("jev wrapper scripts not found beside this test")


def _text(name: str) -> str:
    return (_scripts() / name).read_text(encoding="utf-8")


def _is_wsl_call(line: str) -> bool:
    stripped = line.strip()
    return stripped.startswith("& wsl.exe") or stripped.startswith("wsl.exe")


def _wsl_lines(name: str) -> list[str]:
    lines = [line.strip() for line in _text(name).splitlines() if _is_wsl_call(line)]
    if not lines:
        raise AssertionError(f"{name} has no wsl.exe call")
    return lines


class JevWrapperTests(unittest.TestCase):
    def test_hermes_home_is_in_each_wsl_argv(self) -> None:
        for name in WRAPPERS:
            for line in _wsl_lines(name):
                self.assertIn(
                    'env "HERMES_HOME=$HermesHome"',
                    line,
                    f"{name} does not pass HERMES_HOME into WSL: {line}",
                )

    def test_every_wsl_call_checks_exit(self) -> None:
        for name in WRAPPERS:
            lines = _text(name).splitlines()
            calls = [index for index, line in enumerate(lines) if _is_wsl_call(line)]
            self.assertGreater(len(calls), 0, name)
            for index, start in enumerate(calls):
                end = calls[index + 1] if index + 1 < len(calls) else len(lines)
                region = "\n".join(lines[start + 1 : end])
                self.assertIn(
                    "if ($LASTEXITCODE",
                    region,
                    f"{name} wsl.exe call at line {start + 1} has no exit check",
                )

    def test_no_machine_path_literal(self) -> None:
        for name in WRAPPERS:
            text = _text(name)
            self.assertNotIn("/home/", text, name)
            self.assertNotIn("/mnt", text, name)

    def test_each_wrapper_pins_its_action(self) -> None:
        for name, expected in ACTIONS.items():
            actions: list[str] = []
            for line in _wsl_lines(name):
                marker = "$Controller"
                self.assertIn(marker, line, name)
                actions.append(line.split(marker, 1)[1].strip())
            self.assertEqual(tuple(actions), expected, name)


if __name__ == "__main__":
    unittest.main()
