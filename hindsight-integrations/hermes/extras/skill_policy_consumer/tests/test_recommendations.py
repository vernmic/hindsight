"""W03: durable recommendations for declined and unavailable skill proposals.

Store tests use a disposable directory and a synthetic in-memory board. The integration tests load
``jev-skill-recommendations`` (and, where stated, ``jev-skill-policy``) from a throwaway profile and
drive the real ``model_tools.handle_function_call`` -> ``skill_manage`` path, so capture is observed
on the host's own ``post_tool_call``. No live profile, board, model or network is used.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import threading
from dataclasses import asdict
from pathlib import Path

import pytest

plugins = pytest.importorskip("hermes_cli.plugins")
model_tools = pytest.importorskip("model_tools")
pytestmark = pytest.mark.skipif("pre_skill_batch" not in plugins.VALID_HOOKS, reason="stock host")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "recommendations"))
from store import SCHEMA, RecommendationStore, refusal_kind  # noqa: E402
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
    assert declined.key != unavailable.key
    for record in (declined, unavailable):
        on_disk = json.loads((store.root / f"{record.key}.json").read_text(encoding="utf-8"))
        assert on_disk == asdict(record) and on_disk["status"] == "pending_delivery"
        assert on_disk["skill_sha256"] == {"demo": "h1"} and "not an approval" in on_disk["instruction"]
        assert on_disk["operations"] == ARGS["operations"]


def test_duplicates_reuse_the_record_and_changed_payloads_do_not(store):
    first = store.capture("declined", "d", ARGS, SRC)
    again = store.capture("declined", "d", ARGS, SRC)
    changed = store.capture("declined", "d", {"operations": [{**ARGS["operations"][0], "content": "x"}]}, SRC)
    assert again.key == first.key and again.sightings == 2
    assert changed.key != first.key and len(list(store.records())) == 2


def test_flat_calls_capture_their_non_null_fields(store):
    record = store.capture(
        "unavailable",
        "u",
        {"action": "patch", "name": "demo", "content": None, "old_string": "a", "new_string": "b", "session_id": "x"},
        SRC,
    )
    assert record.operations == [{"action": "patch", "name": "demo", "old_string": "a", "new_string": "b"}]


def test_redacted_bodies_keep_only_identity(tmp_path):
    store = RecommendationStore(tmp_path, store_bodies=False, skill_hash=lambda n: None)
    record = store.capture("declined", "d", ARGS, SRC)
    assert record.operations is None and len(record.operations_sha256) == 64


def test_delivery_retries_after_restart_and_reconciles_a_lost_ack(store, tmp_path, hashes):
    key = store.capture("declined", "d", ARGS, SRC).key
    down = Board(fail=True)
    assert store.deliver_pending(down) == {key: "uncertain:ConnectionError"}
    assert store.load(key).status == "pending_delivery"
    lossy = Board(lose_ack=True)
    assert store.deliver_pending(lossy)[key] == "uncertain:TimeoutError"
    restarted = RecommendationStore(store.root, skill_hash=hashes.get)
    assert restarted.deliver_pending(lossy) == {key: "delivered"}
    assert lossy.creates == 1 and len(lossy.cards) == 1
    assert restarted.deliver_pending(lossy) == {}
    record = restarted.load(key)
    assert record.status == "delivered"
    assert record.receipts == [{"status": "delivered", "card": "card-1", "cycle": record.cycle}]


def test_status_needs_real_receipts_and_order(store):
    card_key = store.capture("declined", "d", ARGS, SRC).card_key
    store.deliver_pending(Board())
    with pytest.raises(ValueError):
        store.mark(card_key, "implemented", {"by": "w"})
    with pytest.raises(ValueError):
        store.mark(card_key, "reviewed", {})
    store.mark(card_key, "reviewed", {"by": "reviewer"})
    store.mark(card_key, "reviewed", {"by": "reviewer"})  # a retried acknowledgement changes nothing
    record = store.mark(card_key, "implemented", {"commit": "abc"})
    assert [r["status"] for r in record.receipts] == ["delivered", "reviewed", "implemented"]
    reopened = store.capture("declined", "d", ARGS, SRC)
    assert reopened.status == "pending_delivery" and reopened.sightings == 1
    assert len(list(store.root.glob("*.json.implemented-*"))) == 1


def test_stale_cycle_acknowledgements_are_rejected_without_change(store, hashes):
    first = store.capture("declined", "d", ARGS, SRC)
    store.deliver_pending(Board())
    other = RecommendationStore(store.root, skill_hash=hashes.get)  # a worker in another store instance
    held, go, outcome = other.load(first.key), threading.Event(), {}

    def slow_worker():  # still holds its cycle-A snapshot while A completes and B opens
        go.wait(10)
        try:
            other.mark(held.card_key, "reviewed", {"by": "slow worker"})
        except ValueError as exc:
            outcome["error"] = str(exc)

    worker = threading.Thread(target=slow_worker)
    worker.start()
    store.mark(first.card_key, "reviewed", {"by": "reviewer"})
    store.mark(first.card_key, "implemented", {"commit": "abc"})
    reopened = store.capture("declined", "d", ARGS, SRC)  # cycle B, not reviewed
    go.set()
    worker.join(10)
    assert "stale" in outcome["error"]
    for status in ("reviewed", "implemented"):  # duplicate cycle-A receipts through the public API
        with pytest.raises(ValueError, match="stale"):
            store.mark(first.card_key, status, {"commit": "abc"})
    current = store.load(first.key)
    assert current.cycle == reopened.cycle != first.cycle
    assert current.status == "pending_delivery" and current.receipts == []
    assert store.mark(reopened.card_key, "reviewed", {"by": "reviewer"}).status == "reviewed"


def test_corrupt_or_foreign_record_is_set_aside_not_trusted(store):
    key = store.capture("declined", "d", ARGS, SRC).key
    (store.root / f"{key}.json").write_text("{not json", encoding="utf-8")
    assert list(store.records()) == []
    assert store.capture("declined", "d", ARGS, SRC).sightings == 1
    (store.root / f"{key}.json").write_text(json.dumps({"schema": "other/9", "key": key}), encoding="utf-8")
    assert store.load(key) is None
    assert len(list(store.root.glob(f"{key}.json.invalid-*"))) == 2


def test_delivery_commits_forward_while_a_worker_and_capture_run_during_board_io(store, hashes):
    captured = store.capture("declined", "d", ARGS, SRC)
    other, board = RecommendationStore(store.root, skill_hash=hashes.get), Board()  # second instance
    create, done = board.create, threading.Event()

    def during(card_key, card):
        receipt = create(card_key, card)

        def worker():
            other.capture("declined", "d", ARGS, SRC)
            other.mark(card_key, "reviewed", {"by": "reviewer"})
            other.mark(card_key, "implemented", {"commit": "abc"})
            done.set()

        threading.Thread(target=worker).start()
        assert done.wait(10), "store lock must not be held during board I/O"
        return receipt

    board.create = during
    assert store.deliver_pending(board) == {captured.key: "delivered"}
    record = store.load(captured.key)
    assert record.status == "implemented" and record.sightings == 2 and record.delivery == "delivered"
    assert [r["status"] for r in record.receipts] == ["reviewed", "implemented", "delivered"]


def test_captures_from_several_processes_are_serialized(store):
    script = (
        f"import sys; sys.path[:0] = {sys.path!r}; from store import RecommendationStore\n"
        f"s = RecommendationStore({str(store.root)!r}, skill_hash=lambda n: None)\n"
        f"for _ in range(25): s.capture('declined', 'd', {ARGS!r}, {{}})"
    )
    procs = [subprocess.Popen([sys.executable, "-c", script]) for _ in range(4)]
    assert [p.wait(timeout=120) for p in procs] == [0] * 4
    (record,) = store.records()
    assert record.sightings == 100


def test_reopened_proposal_gets_a_new_card_with_refreshed_hashes(store, hashes):
    board = Board()
    first = store.capture("declined", "d", ARGS, SRC)
    store.deliver_pending(board)
    store.mark(first.card_key, "reviewed", {"by": "reviewer"})
    store.mark(first.card_key, "implemented", {"commit": "abc"})
    next(iter(board.cards.values()))["done"] = True  # the board models the completed task
    hashes["demo"] = "h2"
    store.capture("declined", "d", ARGS, SRC)
    assert store.capture("declined", "d", ARGS, SRC).sightings == 2  # an open cycle still dedups
    board.lose_ack = True
    assert store.deliver_pending(board) == {first.key: "uncertain:TimeoutError"}
    board.lose_ack = False
    restarted = RecommendationStore(store.root, skill_hash=hashes.get)
    assert restarted.deliver_pending(board) == {first.key: "delivered"}
    assert board.creates == 2 and len(board.cards) == 2
    new = [c for c in board.cards.values() if "done" not in c]
    assert len(new) == 1 and new[0]["skill_sha256"] == {"demo": "h2"}
    record = restarted.load(first.key)
    assert new[0]["card_key"] == record.card_key != first.card_key
    assert record.receipts == [{"status": "delivered", "card": "card-2", "cycle": record.cycle}]
    assert len(list(store.root.glob(f"{first.key}.json.implemented-*"))) == 1


def test_invalid_same_version_records_are_quarantined_and_recovery_continues(store, monkeypatch):
    good = store.capture("declined", "d", ARGS, SRC).key
    bare = "0" * 64  # sorts first: an early bad record must not block later ones
    (store.root / f"{bare}.json").write_text(json.dumps({"schema": SCHEMA, "key": bare}), encoding="utf-8")
    typed = store.capture("unavailable", "u", ARGS, SRC)
    typed_path = store.root / f"{typed.key}.json"
    typed_path.write_text(json.dumps({**asdict(typed), "sightings": "1"}), encoding="utf-8")
    denied = store.capture("declined", "d", {"operations": [{**ARGS["operations"][0], "content": "y"}]}, SRC).key
    read_bytes = Path.read_bytes

    def guarded(self, *args, **kwargs):
        if self.name == f"{denied}.json":
            raise PermissionError("synthetic denied")
        return read_bytes(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", guarded)
    assert store.deliver_pending(Board()) == {good: "delivered", denied: "error:PermissionError"}
    assert (store.root / f"{denied}.json").exists()  # an I/O failure is not mistaken for bad data
    assert len(list(store.root.glob("*.json.invalid-*"))) == 2


def test_missing_nullable_fields_are_quarantined_and_recovery_continues(store, hashes):
    def plant(valid):  # bad copies of a valid record, sorted ahead of it
        for bare, missing in (("0" * 64, "operations"), ("0" * 63 + "1", "delivery")):
            data = {**asdict(valid), "key": bare}
            del data[missing]
            (store.root / f"{bare}.json").write_text(json.dumps(data), encoding="utf-8")
        (store.root / f"{'0' * 62}ff.json").write_bytes(b"\xff")  # undecodable bytes, also ahead of it

    board, first = Board(), store.capture("declined", "d", ARGS, SRC)
    plant(first)
    assert store.deliver_pending(board) == {first.key: "delivered"}
    second = store.capture("unavailable", "u", ARGS, SRC)
    plant(second)
    restarted = RecommendationStore(store.root, skill_hash=hashes.get)
    assert restarted.deliver_pending(board) == {second.key: "delivered"}
    quarantined = sorted(store.root.glob("*.json.invalid-*"))
    assert len(quarantined) == 6 and board.creates == 2
    assert [q.read_bytes() for q in quarantined if q.name.startswith("0" * 62 + "ff")] == [b"\xff"] * 2
    assert {r.key for r in restarted.records()} == {first.key, second.key}


def test_non_json_board_receipt_leaves_the_record_pending(store):
    key = store.capture("declined", "d", ARGS, SRC).key
    board = Board()
    board.find = lambda card_key: {"card": object()}
    assert store.deliver_pending(board) == {key: "uncertain:TypeError"}
    assert store.load(key).status == "pending_delivery"


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
