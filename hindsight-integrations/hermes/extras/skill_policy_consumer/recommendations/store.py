"""Durable local recommendations for refused skill proposals (W03).

One versioned JSON record per (kind, exact proposal), written atomically before capture is reported.
Records are recommendations only: nothing here returns, stores or replays an approval, and no outcome
of capture or delivery changes what the host already decided. A worker implementing one must obtain a
fresh policy judgment through the normal ``skill_manage`` path.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any, Protocol

SCHEMA = "jev-skill-recommendation/1"
KINDS = {"Skill batch policy denied: ": "declined", "Skill batch policy unavailable: ": "unavailable"}
OPEN = ("pending_delivery", "delivered", "reviewed")
NEXT = {"pending_delivery": {"reviewed"}, "delivered": {"reviewed"}, "reviewed": {"implemented"}}
INSTRUCTION = (
    "Recommendation only, not an approval. Before implementing, re-read each named skill and compare its "
    "sha256 with skill_sha256; if it changed, re-plan against the current content. Apply the change through "
    "skill_manage so the current skill policy judges it afresh. Do not use any other write path."
)


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

    def _write(self, record: dict[str, Any]) -> None:
        record["updated_ms"] = int(time.time() * 1000)
        self.root.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.root, prefix=".tmp-", suffix=".partial")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(canonical(record))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self._path(record["key"]))
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def load(self, key: str) -> dict[str, Any] | None:
        """The record, ``None`` if absent. A corrupt or other-schema file is set aside (renamed, never
        deleted) and reported as absent so a fresh capture can replace it."""
        path = self._path(key)
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(record, dict) and record.get("schema") == SCHEMA and record.get("key") == key:
                return record
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            pass
        path.replace(path.with_name(f"{path.name}.unreadable-{int(time.time() * 1000)}"))
        return None

    def records(self) -> Iterator[dict[str, Any]]:
        for path in sorted(self.root.glob("*.json")):
            record = self.load(path.stem)
            if record is not None:
                yield record

    # -- capture -----------------------------------------------------------------------------------
    def capture(self, kind: str, reason: str, args: Mapping[str, Any], source: Mapping[str, Any]) -> dict:
        """Persist (or re-sight) the recommendation; returns it only after it is on disk."""
        operations = proposal_operations(args)
        key = sha256(canonical({"kind": kind, "operations": operations}))
        record = self.load(key)
        if record is not None and record["status"] == "implemented":
            path = self._path(key)
            path.replace(path.with_name(f"{path.name}.implemented-{record['updated_ms']}"))
            record = None
        if record is None:
            names = sorted({str(op.get("name")) for op in operations if op.get("name")})
            record = {
                "schema": SCHEMA,
                "key": key,
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
        """At most one card per record. Each attempt first reconciles by idempotency key, so a crash or
        lost acknowledgement after ``create`` never yields a second card. Failures leave it pending."""
        results = {}
        for record in self.records():
            if record["status"] != "pending_delivery":
                continue
            card = {
                k: record[k]
                for k in ("kind", "reason", "operations", "operations_sha256", "skill_sha256", "source", "instruction")
            }
            try:
                receipt = board.find(record["key"])
                if receipt is None:
                    record["delivery"] = "attempting"
                    self._write(record)
                    receipt = board.create(record["key"], card)
                if not receipt:
                    raise ValueError("board returned no receipt")
            except Exception as exc:  # timeout, outage, bad adapter: retry later, reconcile first
                record["delivery"] = f"uncertain:{type(exc).__name__}"
                self._write(record)
                results[record["key"]] = record["delivery"]
                continue
            record.update(status="delivered", delivery="delivered")
            record["receipts"].append({"status": "delivered", **receipt})
            self._write(record)
            results[record["key"]] = "delivered"
        return results

    def mark(self, key: str, status: str, receipt: Mapping[str, Any]) -> dict[str, Any]:
        """``reviewed`` / ``implemented`` only with an actual receipt; a card existing is not one."""
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
