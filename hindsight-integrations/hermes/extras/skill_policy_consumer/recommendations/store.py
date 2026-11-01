"""Durable local recommendations for refused skill proposals (W03).

One versioned JSON record per (kind, exact proposal), written atomically before capture is reported.
Records are recommendations only: nothing here returns, stores or replays an approval, and no outcome
of capture or delivery changes what the host already decided. A worker implementing one must obtain a
fresh policy judgment through the normal ``skill_manage`` path.

Every read-modify-write of a record runs under one store-wide OS file lock (the host's own
``agent.trajectory._lock_append_handle``), so capture, delivery acknowledgement and ``mark`` are
serialized across threads, store instances and processes sharing ``root``. The lock is never held
during board I/O: delivery snapshots the record, talks to the board unlocked, then commits against a
fresh read, so it only ever moves a record forward.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol

SCHEMA = "jev-skill-recommendation/2"
KINDS = {"Skill batch policy denied: ": "declined", "Skill batch policy unavailable: ": "unavailable"}
STATES = ("pending_delivery", "delivered", "reviewed", "implemented")
NEXT = {"pending_delivery": {"reviewed"}, "delivered": {"reviewed"}, "reviewed": {"implemented"}}
INSTRUCTION = (
    "Recommendation only, not an approval. Before implementing, re-read each named skill and compare its "
    "sha256 with skill_sha256; if it changed, re-plan against the current content. Apply the change through "
    "skill_manage so the current skill policy judges it afresh. Do not use any other write path."
)
_OPT = type(None)
FIELDS: dict[str, Any] = {  # the full record shape; a file that does not match it is never trusted
    "schema": str,
    "key": str,
    "cycle": str,
    "kind": str,
    "reason": str,
    "operations": (list, _OPT),
    "operations_sha256": str,
    "skill_sha256": dict,
    "source": dict,
    "instruction": str,
    "status": str,
    "receipts": list,
    "sightings": int,
    "delivery": (str, _OPT),
    "created_ms": int,
    "updated_ms": int,
}
logger = logging.getLogger(__name__)


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256(text: str | bytes) -> str:
    return hashlib.sha256(text.encode("utf-8") if isinstance(text, str) else text).hexdigest()


def refusal_kind(result: object) -> tuple[str, str] | None:
    """``(kind, reason)`` when a ``skill_manage`` result is the host batch-gate refusal, else ``None``."""
    try:
        error = (json.loads(result) if isinstance(result, str) else result).get("error")
    except (ValueError, AttributeError):
        return None
    for prefix, kind in KINDS.items():
        if isinstance(error, str) and error.startswith(prefix):
            return kind, error
    return None


def proposal_operations(args: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The proposed change exactly as submitted: ``operations[]`` or the flat call's non-null fields."""
    if isinstance(args.get("operations"), list):
        return [dict(op) for op in args["operations"]]
    return [{k: v for k, v in args.items() if v is not None and k != "session_id"}]


def invalid(record: object, key: str) -> str | None:
    """Why ``record`` is not a trustworthy ``SCHEMA`` record for ``key``, or ``None`` when it is."""
    if not isinstance(record, dict):
        return "not an object"
    for name, types in FIELDS.items():
        value = record.get(name)
        if not isinstance(value, types) or (types is int and isinstance(value, bool)):
            return f"field {name!r} missing or wrong type"
    checks = {
        "schema": record["schema"] == SCHEMA,
        "key": record["key"] == key,
        "kind": record["kind"] in KINDS.values(),
        "status": record["status"] in STATES,
        "sightings": record["sightings"] >= 1,
        "receipts": all(isinstance(r, dict) and r.get("status") in STATES for r in record["receipts"]),
        "skill_sha256": all(isinstance(v, (str, _OPT)) for v in record["skill_sha256"].values()),
        "operations": record["operations"] is None
        or sha256(canonical(record["operations"])) == record["operations_sha256"],
    }
    return next((f"field {name!r} invalid" for name, ok in checks.items() if not ok), None)


class Board(Protocol):
    """Optional task board. ``find`` must look a card up by the idempotency key ``create`` was given;
    an adapter that cannot do that only offers at-least-once delivery and must say so."""

    def find(self, key: str) -> Mapping[str, Any] | None: ...

    def create(self, key: str, card: Mapping[str, Any]) -> Mapping[str, Any]: ...


class RecommendationStore:
    def __init__(self, root: Path, *, store_bodies: bool = True, skill_hash: Callable[[str], str | None]):
        self.root, self.store_bodies, self._skill_hash = Path(root), store_bodies, skill_hash

    # -- persistence -------------------------------------------------------------------------------
    def _path(self, key: str) -> Path:
        return self.root / f"{key}.json"

    @contextmanager
    def _locked(self) -> Iterator[None]:
        from agent.trajectory import _lock_append_handle  # host helper: flock / msvcrt range lock

        self.root.mkdir(parents=True, exist_ok=True)
        with open(self.root / ".lock", "a", encoding="utf-8") as handle:
            _lock_append_handle(handle, True)
            try:
                yield
            finally:
                _lock_append_handle(handle, False)

    def _write(self, record: dict[str, Any], path: Path | None = None) -> None:
        record["updated_ms"] = int(time.time() * 1000)
        fd, tmp = tempfile.mkstemp(dir=self.root, prefix=".tmp-", suffix=".partial")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(canonical(record))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path or self._path(record["key"]))
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def load(self, key: str) -> dict[str, Any] | None:
        """The validated record, ``None`` if absent. Undecodable or invalid content is quarantined
        (renamed ``*.invalid-<ms>``, never deleted; the reason is logged) and reported as absent.
        Permission and other I/O errors are raised, not mistaken for bad data."""
        path = self._path(key)
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except ValueError as exc:
            problem = f"undecodable: {exc}"
        else:
            problem = invalid(record, key)
            if problem is None:
                return record
        quarantine = path.with_name(f"{path.name}.invalid-{int(time.time() * 1000)}")
        path.replace(quarantine)
        logger.warning("skill recommendation %s quarantined as %s: %s", key, quarantine.name, problem)
        return None

    def records(self) -> Iterator[dict[str, Any]]:
        for path in sorted(self.root.glob("*.json")):
            with self._locked():
                record = self.load(path.stem)
            if record is not None:
                yield record

    # -- capture -----------------------------------------------------------------------------------
    def capture(self, kind: str, reason: str, args: Mapping[str, Any], source: Mapping[str, Any]) -> dict:
        """Persist (or re-sight) the recommendation; returns it only after it is on disk. Re-refusing an
        implemented proposal opens a new review cycle (new ``cycle`` id, so a new board card) and keeps
        the finished cycle as ``<key>.json.implemented-<cycle>``."""
        operations = proposal_operations(args)
        key = sha256(canonical({"kind": kind, "operations": operations}))
        with self._locked():
            record = self.load(key)
            if record is not None and record["status"] == "implemented":
                self._write(record, self._path(key).with_name(f"{key}.json.implemented-{record['cycle']}"))
                record = None
            if record is None:
                names = sorted({str(op.get("name")) for op in operations if op.get("name")})
                record = {
                    "schema": SCHEMA,
                    "key": key,
                    "cycle": uuid.uuid4().hex,
                    "kind": kind,
                    "reason": reason,
                    "operations": operations if self.store_bodies else None,
                    "operations_sha256": sha256(canonical(operations)),
                    "skill_sha256": {name: self._skill_hash(name) for name in names},
                    "source": dict(source),
                    "instruction": INSTRUCTION,
                    "status": "pending_delivery",
                    "receipts": [],
                    "sightings": 0,
                    "delivery": None,
                    "created_ms": int(time.time() * 1000),
                }
            record["sightings"] += 1
            self._write(record)
        return record

    # -- delivery ----------------------------------------------------------------------------------
    def deliver_pending(self, board: Board) -> dict[str, str]:
        """At most one card per record cycle. Each attempt first reconciles by the cycle's idempotency key
        (``<key>:<cycle>``), so a crash or lost acknowledgement after ``create`` never yields a second card,
        while a reopened proposal gets its own. The result is committed to a fresh read under the lock,
        moving the record forward only. Failures leave it pending; an unreadable record is reported and
        skipped so later records still recover."""
        results = {}
        for path in sorted(self.root.glob("*.json")):
            key = path.stem
            try:
                with self._locked():
                    snapshot = self.load(key)
                if snapshot is None or snapshot["status"] != "pending_delivery":
                    continue
                card_key = f"{key}:{snapshot['cycle']}"
                card = {k: snapshot[k] for k in ("kind", "reason", "operations", "operations_sha256")}
                card.update({k: snapshot[k] for k in ("skill_sha256", "source", "instruction")})
                try:
                    receipt = board.find(card_key) or board.create(card_key, card)
                    if not receipt:
                        raise ValueError("board returned no receipt")
                    outcome = "delivered"
                except Exception as exc:  # timeout, outage, bad adapter: retry later, reconcile first
                    outcome = f"uncertain:{type(exc).__name__}"
                with self._locked():
                    record = self.load(key)
                    if record is None or record["cycle"] != snapshot["cycle"]:
                        results[key] = "superseded"
                        continue
                    if outcome == "delivered":
                        record["receipts"].append({"status": "delivered", **receipt})
                        if record["status"] == "pending_delivery":
                            record["status"] = "delivered"
                    if record["delivery"] != "delivered":
                        record["delivery"] = outcome
                    self._write(record)
                results[key] = outcome
            except OSError as exc:  # permission / storage failure: explicit, not treated as corrupt data
                results[key] = f"error:{type(exc).__name__}"
        return results

    def mark(self, key: str, status: str, receipt: Mapping[str, Any]) -> dict[str, Any]:
        """``reviewed`` / ``implemented`` only with an actual receipt; a card existing is not one."""
        with self._locked():
            record = self.load(key)
            if record is None or status not in NEXT.get(record["status"], ()) or not receipt:
                raise ValueError(f"cannot mark {key} {status}")
            record["status"] = status
            record["receipts"].append({"status": status, **receipt})
            self._write(record)
        return record

    def stale_skills(self, record: Mapping[str, Any]) -> list[str]:
        """Worker revalidation: skills whose current hash differs from the one recorded."""
        return [name for name, digest in record["skill_sha256"].items() if self._skill_hash(name) != digest]
