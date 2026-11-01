"""Durable local recommendations for refused skill proposals (W03).

One versioned JSON record per (kind, exact proposal), written atomically before capture is reported.
Records are recommendations only: nothing here returns, stores or replays an approval, and no outcome
of capture or delivery changes what the host already decided. A worker implementing one must obtain a
fresh policy judgment through the normal ``skill_manage`` path.

Every read-modify-write of a record runs under one store-wide OS file lock (the host's own
``agent.trajectory._lock_append_handle``), so capture, delivery acknowledgement and ``mark`` are
serialized across threads, store instances and processes sharing ``root``. The lock is never held
during board I/O: delivery snapshots the record, talks to the board unlocked, then commits against a
fresh read, so it only ever moves a record forward. Inside the store a record is a validated
``Recommendation``; JSON exists only at the file boundary.
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
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol, get_type_hints

SCHEMA = "jev-skill-recommendation/2"
KINDS = {"Skill batch policy denied: ": "declined", "Skill batch policy unavailable: ": "unavailable"}
STATES = ("pending_delivery", "delivered", "reviewed", "implemented")
NEXT = {"pending_delivery": {"reviewed"}, "delivered": {"reviewed"}, "reviewed": {"implemented"}}
INSTRUCTION = (
    "Recommendation only, not an approval. Before implementing, re-read each named skill and compare its "
    "sha256 with skill_sha256; if it changed, re-plan against the current content. Apply the change through "
    "skill_manage so the current skill policy judges it afresh. Do not use any other write path."
)
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


def plain(value: Mapping[str, Any]) -> dict[str, Any]:
    """A receipt as plain JSON data, or ``TypeError``/``ValueError``: nothing else is ever stored."""
    receipt = json.loads(canonical(dict(value)))
    if not receipt:
        raise ValueError("empty receipt")
    return receipt


@dataclass
class Recommendation:
    """One record, validated on construction. Every field is required, so a missing key fails
    construction and is never confused with a present ``null``."""

    schema: str
    key: str
    cycle: str
    kind: str
    reason: str
    operations: list | None
    operations_sha256: str
    skill_sha256: dict
    source: dict
    instruction: str
    status: str
    receipts: list
    sightings: int
    delivery: str | None
    created_ms: int
    updated_ms: int

    def __post_init__(self) -> None:
        for name, types in _TYPES.items():
            value = getattr(self, name)
            if not isinstance(value, types) or (types is int and isinstance(value, bool)):
                raise TypeError(f"field {name!r} has type {type(value).__name__}")
        checks = {
            "schema": self.schema == SCHEMA,
            "kind": self.kind in KINDS.values(),
            "status": self.status in STATES,
            "sightings": self.sightings >= 1,
            "receipts": all(
                isinstance(r, dict) and r.get("status") in STATES and r.get("cycle") == self.cycle
                for r in self.receipts
            ),
            "skill_sha256": all(isinstance(v, (str, type(None))) for v in self.skill_sha256.values()),
            "operations": self.operations is None or sha256(canonical(self.operations)) == self.operations_sha256,
        }
        bad = next((name for name, ok in checks.items() if not ok), None)
        if bad:
            raise ValueError(f"field {bad!r} invalid")

    @property
    def card_key(self) -> str:
        """Board idempotency key and worker handle: one per review cycle of this proposal."""
        return f"{self.key}:{self.cycle}"

    @classmethod
    def from_json(cls, text: str, key: str) -> Recommendation:
        data = json.loads(text)
        if not isinstance(data, dict) or data.get("key") != key:
            raise ValueError("not a record object for this key")
        return cls(**data)  # a missing or unknown field raises TypeError


_TYPES = get_type_hints(Recommendation)


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

    def _write(self, record: Recommendation, path: Path | None = None) -> None:
        record.updated_ms = int(time.time() * 1000)
        fd, tmp = tempfile.mkstemp(dir=self.root, prefix=".tmp-", suffix=".partial")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(canonical(asdict(record)))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path or self._path(record.key))
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def load(self, key: str) -> Recommendation | None:
        """The validated record, ``None`` if absent. Content that is not a valid record for ``key`` (any
        decoding, presence, type or consistency failure) is quarantined (renamed ``*.invalid-<ms>``, never
        deleted; the reason is logged) and reported as absent, so it can never abort recovery. Permission
        and other I/O errors are raised, not mistaken for bad data."""
        path = self._path(key)
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            return Recommendation.from_json(text, key)
        except Exception as exc:  # malformed content of any shape: never trusted, never fatal
            problem = f"{type(exc).__name__}: {exc}"
        quarantine = path.with_name(f"{path.name}.invalid-{int(time.time() * 1000)}")
        path.replace(quarantine)
        logger.warning("skill recommendation %s quarantined as %s: %s", key, quarantine.name, problem)
        return None

    def records(self) -> Iterator[Recommendation]:
        for path in sorted(self.root.glob("*.json")):
            with self._locked():
                record = self.load(path.stem)
            if record is not None:
                yield record

    # -- capture -----------------------------------------------------------------------------------
    def capture(self, kind: str, reason: str, args: Mapping[str, Any], source: Mapping[str, Any]) -> Recommendation:
        """Persist (or re-sight) the recommendation; returns it only after it is on disk. Re-refusing an
        implemented proposal opens a new review cycle (new ``cycle`` id, so a new board card) and keeps
        the finished cycle as ``<key>.json.implemented-<cycle>``."""
        operations = proposal_operations(args)
        key = sha256(canonical({"kind": kind, "operations": operations}))
        with self._locked():
            record = self.load(key)
            if record is not None and record.status == "implemented":
                self._write(record, self._path(key).with_name(f"{key}.json.implemented-{record.cycle}"))
                record = None
            if record is None:
                names = sorted({str(op.get("name")) for op in operations if op.get("name")})
                now = int(time.time() * 1000)
                record = Recommendation(
                    schema=SCHEMA,
                    key=key,
                    cycle=uuid.uuid4().hex,
                    kind=kind,
                    reason=reason,
                    operations=operations if self.store_bodies else None,
                    operations_sha256=sha256(canonical(operations)),
                    skill_sha256={name: self._skill_hash(name) for name in names},
                    source=dict(source),
                    instruction=INSTRUCTION,
                    status="pending_delivery",
                    receipts=[],
                    sightings=1,
                    delivery=None,
                    created_ms=now,
                    updated_ms=now,
                )
            else:
                record.sightings += 1
            self._write(record)
        return record

    # -- delivery ----------------------------------------------------------------------------------
    def deliver_pending(self, board: Board) -> dict[str, str]:
        """At most one card per record cycle. Each attempt first reconciles by the cycle's idempotency key
        (``card_key``), so a crash or lost acknowledgement after ``create`` never yields a second card,
        while a reopened proposal gets its own. The result is committed to a fresh read under the lock,
        moving the record forward only. Failures leave it pending; an unreadable record is reported and
        skipped so later records still recover."""
        results = {}
        for path in sorted(self.root.glob("*.json")):
            key = path.stem
            try:
                with self._locked():
                    snapshot = self.load(key)
                if snapshot is None or snapshot.status != "pending_delivery":
                    continue
                fields = ("kind", "reason", "operations", "operations_sha256", "skill_sha256", "source")
                card = {k: getattr(snapshot, k) for k in (*fields, "instruction", "card_key")}
                try:  # timeout, outage, bad adapter or a non-JSON receipt: retry later, reconcile first
                    receipt = plain(board.find(snapshot.card_key) or board.create(snapshot.card_key, card))
                    outcome = "delivered"
                except Exception as exc:
                    outcome = f"uncertain:{type(exc).__name__}"
                with self._locked():
                    record = self.load(key)
                    if record is None or record.cycle != snapshot.cycle:
                        results[key] = "superseded"
                        continue
                    if outcome == "delivered":
                        record.receipts.append({**receipt, "status": "delivered", "cycle": record.cycle})
                        if record.status == "pending_delivery":
                            record.status = "delivered"
                    if record.delivery != "delivered":
                        record.delivery = outcome
                    self._write(record)
                results[key] = outcome
            except OSError as exc:  # permission / storage failure: explicit, not treated as corrupt data
                results[key] = f"error:{type(exc).__name__}"
        return results

    def mark(self, card_key: str, status: str, receipt: Mapping[str, Any]) -> Recommendation:
        """Record a worker's ``reviewed`` / ``implemented`` for the one review cycle named by ``card_key``
        (the card's idempotency key ``<key>:<cycle>``). Compare-and-set under the store lock: a stale or
        unknown cycle is rejected and nothing changes. Needs an actual receipt; a card existing is not one.
        Repeating the cycle's current status is an idempotent retry that changes nothing."""
        key, _, cycle = card_key.partition(":")
        receipt = plain(receipt)
        with self._locked():
            record = self.load(key)
            if record is None or record.cycle != cycle:
                raise ValueError(f"stale or unknown review cycle {card_key}")
            if status in ("reviewed", "implemented") and status == record.status:
                return record
            if status not in NEXT.get(record.status, ()):
                raise ValueError(f"cannot mark {card_key} {status}")
            record.status = status
            record.receipts.append({**receipt, "status": status, "cycle": cycle})
            self._write(record)
        return record

    def stale_skills(self, record: Recommendation) -> list[str]:
        """Worker revalidation: skills whose current hash differs from the one recorded."""
        return [name for name, digest in record.skill_sha256.items() if self._skill_hash(name) != digest]
