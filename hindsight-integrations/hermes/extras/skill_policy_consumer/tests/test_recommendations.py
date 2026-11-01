"""W03: durable recommendations for declined and unavailable skill proposals.

Store tests use a disposable directory and a synthetic in-memory board. The integration tests load
``jev-skill-recommendations`` (and, where stated, ``jev-skill-policy``) from a throwaway profile and
drive the real ``model_tools.handle_function_call`` -> ``skill_manage`` path, so capture is observed
on the host's own ``post_tool_call``. No live profile, board, model or network is used.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

plugins = pytest.importorskip("hermes_cli.plugins")
model_tools = pytest.importorskip("model_tools")
pytestmark = pytest.mark.skipif("pre_skill_batch" not in plugins.VALID_HOOKS, reason="stock host")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "recommendations"))
from store import RecommendationStore, refusal_kind  # noqa: E402
from test_pr57703_host_probe import SKILL, skills_root, tree  # noqa: E402

PACKAGE = Path(__file__).resolve().parents[1]
DENIED = json.dumps({"success": False, "error": "Skill batch policy denied: synthetic no"})
UNAVAILABLE = json.dumps({"success": False, "error": "Skill batch policy unavailable: no decision"})
ARGS = {"action": "", "name": "", "operations": [{"name": "demo", "action": "create", "content": "synthetic"}]}
SRC = {"session_id": "s1", "task_id": None, "tool_call_id": "c1", "turn_id": None}


class Board:
    """Synthetic board: idempotent by key, can fail or lose the acknowledgement."""

    def __init__(self, *, fail=False, lose_ack=False):
        self.cards, self.fail, self.lose_ack, self.creates = {}, fail, lose_ack, 0

    def find(self, key):
        return {"card": self.cards[key]["id"]} if key in self.cards else None

    def create(self, key, card):
        self.creates += 1
        if self.fail:
            raise ConnectionError("board down")
        self.cards.setdefault(key, {"id": f"card-{len(self.cards) + 1}", **card})
        if self.lose_ack:
            raise TimeoutError("ack lost")
        return {"card": self.cards[key]["id"]}


@pytest.fixture
def hashes():
    return {"demo": "h1"}


@pytest.fixture
def store(tmp_path, hashes):
    return RecommendationStore(tmp_path / "recs", skill_hash=hashes.get)


def test_refusal_text_classification():
    assert refusal_kind(DENIED) == ("declined", "Skill batch policy denied: synthetic no")
    assert refusal_kind(UNAVAILABLE)[0] == "unavailable"
    assert refusal_kind(json.dumps({"success": True})) is None and refusal_kind("not json") is None


def test_decline_and_unavailable_are_distinct_and_persisted_before_return(store):
    declined = store.capture("declined", "d", ARGS, SRC)
    unavailable = store.capture("unavailable", "u", ARGS, SRC)
    assert declined["key"] != unavailable["key"]
    for record in (declined, unavailable):
        on_disk = json.loads((store.root / f"{record['key']}.json").read_text(encoding="utf-8"))
        assert on_disk == record and on_disk["status"] == "pending_delivery"
        assert on_disk["skill_sha256"] == {"demo": "h1"} and "not an approval" in on_disk["instruction"]
        assert on_disk["operations"] == ARGS["operations"]


def test_duplicates_reuse_the_record_and_changed_payloads_do_not(store):
    first = store.capture("declined", "d", ARGS, SRC)
    again = store.capture("declined", "d", ARGS, SRC)
    changed = store.capture("declined", "d", {"operations": [{**ARGS["operations"][0], "content": "x"}]}, SRC)
    assert again["key"] == first["key"] and again["sightings"] == 2
    assert changed["key"] != first["key"] and len(list(store.records())) == 2


def test_flat_calls_capture_their_non_null_fields(store):
    record = store.capture(
        "unavailable",
        "u",
        {"action": "patch", "name": "demo", "content": None, "old_string": "a", "new_string": "b", "session_id": "x"},
        SRC,
    )
    assert record["operations"] == [{"action": "patch", "name": "demo", "old_string": "a", "new_string": "b"}]


def test_redacted_bodies_keep_only_identity(tmp_path):
    store = RecommendationStore(tmp_path, store_bodies=False, skill_hash=lambda n: None)
    record = store.capture("declined", "d", ARGS, SRC)
    assert record["operations"] is None and len(record["operations_sha256"]) == 64


def test_delivery_retries_after_restart_and_reconciles_a_lost_ack(store, tmp_path, hashes):
    key = store.capture("declined", "d", ARGS, SRC)["key"]
    down = Board(fail=True)
    assert store.deliver_pending(down) == {key: "uncertain:ConnectionError"}
    assert store.load(key)["status"] == "pending_delivery"
    lossy = Board(lose_ack=True)
    assert store.deliver_pending(lossy)[key] == "uncertain:TimeoutError"
    restarted = RecommendationStore(store.root, skill_hash=hashes.get)
    assert restarted.deliver_pending(lossy) == {key: "delivered"}
    assert lossy.creates == 1 and len(lossy.cards) == 1
    assert restarted.deliver_pending(lossy) == {}
    record = restarted.load(key)
    assert record["status"] == "delivered" and record["receipts"] == [{"status": "delivered", "card": "card-1"}]


def test_status_needs_real_receipts_and_order(store):
    key = store.capture("declined", "d", ARGS, SRC)["key"]
    store.deliver_pending(Board())
    with pytest.raises(ValueError):
        store.mark(key, "implemented", {"by": "w"})
    with pytest.raises(ValueError):
        store.mark(key, "reviewed", {})
    store.mark(key, "reviewed", {"by": "reviewer"})
    assert store.mark(key, "implemented", {"commit": "abc"})["status"] == "implemented"
    reopened = store.capture("declined", "d", ARGS, SRC)
    assert reopened["status"] == "pending_delivery" and reopened["sightings"] == 1
    assert len(list(store.root.glob("*.json.implemented-*"))) == 1


def test_corrupt_or_foreign_record_is_set_aside_not_trusted(store):
    key = store.capture("declined", "d", ARGS, SRC)["key"]
    (store.root / f"{key}.json").write_text("{not json", encoding="utf-8")
    assert list(store.records()) == []
    assert store.capture("declined", "d", ARGS, SRC)["sightings"] == 1
    (store.root / f"{key}.json").write_text(json.dumps({"schema": "other/9", "key": key}), encoding="utf-8")
    assert store.load(key) is None
    assert len(list(store.root.glob(f"{key}.json.unreadable-*"))) == 2


def test_recording_failure_raises_to_the_caller_without_a_partial_record(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("x", encoding="utf-8")
    with pytest.raises(OSError):
        RecommendationStore(blocker / "recs", skill_hash=lambda n: None).capture("declined", "d", ARGS, SRC)


def test_worker_revalidation_flags_changed_skills(store, hashes):
    record = store.capture("declined", "d", ARGS, SRC)
    assert store.stale_skills(record) == []
    hashes["demo"] = "h2"
    assert store.stale_skills(record) == ["demo"]


# -- through the host --------------------------------------------------------------------------
JUDGES = """
CALLS = []

def block(request):
    CALLS.append(request)
    return {"verdict": "block", "reason": "jev says no"}
"""


@pytest.fixture
def profile(tmp_path, monkeypatch):
    home = tmp_path / "profile"
    ignore = shutil.ignore_patterns("tests", "__pycache__", "*.md", "recommendations")
    shutil.copytree(PACKAGE, home / "plugins" / "jev-skill-policy", ignore=ignore)
    shutil.copytree(
        PACKAGE / "recommendations",
        home / "plugins" / "jev-skill-recommendations",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    (tmp_path / "jev_w03_judges.py").write_text(JUDGES, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "jev_w03_judges", raising=False)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(plugins, "get_bundled_plugins_dir", lambda: tmp_path / "no-bundled")
    monkeypatch.setattr(plugins.PluginManager, "_scan_entry_points", lambda self: [])
    plugins._reset_plugin_managers_for_tests()
    yield home
    plugins._reset_plugin_managers_for_tests()


def load(home, enabled, **policy):
    config = {
        "skill_policy": {"required": "pre_skill_batch"},
        "plugins": {
            "enabled": enabled,
            "entries": {
                "jev-skill-policy": {
                    "settings": {
                        "policy_revision": "w03",
                        "required": True,
                        "judge_callable": "jev_w03_judges:block",
                        **policy,
                    }
                },
            },
        },
    }
    (home / "config.yaml").write_text(json.dumps(config), encoding="utf-8")
    plugins.discover_plugins(force=True)


def call(name="probe-skill"):
    ops = [{"name": name, "action": "create", "content": SKILL.replace("probe-skill", name)}]
    return json.loads(
        model_tools.handle_function_call(
            "skill_manage", {"action": "", "name": "", "operations": ops}, tool_call_id="tc-1", session_id="sess-1"
        )
    )


def recs(home):
    return [json.loads(p.read_text(encoding="utf-8")) for p in (home / "jev-skill-recommendations").glob("*.json")]


@pytest.mark.parametrize(
    ("enabled", "policy", "kind"),
    [
        (["jev-skill-policy", "jev-skill-recommendations"], {}, "declined"),
        (["jev-skill-recommendations"], {}, "unavailable"),  # policy plugin absent
        (["jev-skill-policy", "jev-skill-recommendations"], {"timeout_seconds": 0}, "unavailable"),  # failed
    ],
)
def test_host_refusals_are_captured_and_the_refusal_is_unchanged(profile, tmp_path, enabled, policy, kind):
    load(profile, enabled, **policy)
    with skills_root(tmp_path) as root:
        first, second = call(), call()
        assert tree(root) == []
    prefix = "Skill batch policy denied:" if kind == "declined" else "Skill batch policy unavailable:"
    assert first["error"].startswith(prefix) and second == first
    (record,) = recs(profile)
    assert record["kind"] == kind and record["sightings"] == 2 and record["status"] == "pending_delivery"
    assert record["source"]["tool_call_id"] == "tc-1" and record["operations"][0]["name"] == "probe-skill"


def test_no_board_configured_keeps_the_local_route_and_records_grant_nothing(profile, tmp_path):
    load(profile, ["jev-skill-policy", "jev-skill-recommendations"])
    with skills_root(tmp_path) as root:
        call()
        import jev_w03_judges

        assert len(jev_w03_judges.CALLS) == 1
        assert call()["error"].startswith("Skill batch policy denied:")
        assert len(jev_w03_judges.CALLS) == 2  # judged afresh; the record supplied no approval
        assert tree(root) == []
    (record,) = recs(profile)
    assert record["delivery"] is None and record["receipts"] == []
