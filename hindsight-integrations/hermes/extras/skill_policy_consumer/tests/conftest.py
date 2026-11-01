"""Isolation for the PR 57703 host probe.

The probe imports the *proposed host* from a Hermes checkout named by the
``HERMES_AGENT_SRC`` environment variable (no path is hardcoded). Every test
runs against a throwaway ``HERMES_HOME`` and a temporary skills root; no live
profile, plugin directory or skill is read or written.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

_SRC = os.environ.get("HERMES_AGENT_SRC", "").strip()
_SANDBOX = Path(tempfile.mkdtemp(prefix="w01-hermes-home-"))


def pytest_configure(config):
    # Sandbox HERMES_HOME before any Hermes module is imported.
    os.environ["HERMES_HOME"] = str(_SANDBOX)
    if _SRC and _SRC not in sys.path:
        sys.path.insert(0, _SRC)


def pytest_collection_modifyitems(config, items):
    if _SRC:
        return
    skip = pytest.mark.skip(reason="HERMES_AGENT_SRC not set; host probe needs a Hermes checkout")
    for item in items:
        item.add_marker(skip)


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    home = tmp_path / "hermes-home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    return home
