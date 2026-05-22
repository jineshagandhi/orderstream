"""Hash-chained, append-only Event Spine.

Every event references the previous event's hash:
    hash_i = SHA-256(prev_hash || canonical_json(payload))

This produces a tamper-evident chain — any retroactive edit invalidates
every subsequent hash, detectable by /audit/verify.

Why this matters for APT's domain:
The SEBI 2025 algo trading framework mandates an immutable audit trail
for every order lifecycle event. A hash chain provides cryptographic
proof of integrity without external infrastructure.

The chain also doubles as the source of truth for the outbox pattern:
- Watcher writes to spine FIRST.
- Broadcast is downstream from the spine.
- On crash + restart, the spine is replayable via seq.

Canonicalization (the round-trip problem)
-----------------------------------------
MongoDB's BSON stores datetimes at millisecond precision; Python
datetimes have microsecond precision. Pydantic v2's `model_dump(mode="json")`
may also format datetimes inside `Any`-typed fields differently from
plain `datetime.isoformat()`. Either source of drift would break the hash.

Solution: BEFORE storing, we run the whole event through `_to_canonical()`
which converts every datetime/UUID/bytes value to a plain string. The
stored document already contains plain strings — there is no datetime
to round-trip — so re-reading and re-hashing produces the identical
canonical JSON.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from datetime import date, datetime, time, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase

from .config import get_settings
from .db import EVENT_LOG
from .models import Event, Intent, Priority, DiffEntry


# Fields of the event document that participate in the hash.
# Anything outside this set (resume_token, cluster_time, coalesced flags)
# can change without invalidating the chain.
HASHED_FIELDS: tuple[str, ...] = (
    "event_id",
    "seq",
    "op",
    "intent",
    "priority",
    "order_id",
    "diff",
    "before",
    "after",
    "ts",
    "schema_version",
    "correlation_id",
)


def _to_canonical(obj: Any) -> Any:
    """Recursively convert a value to a JSON-stable form.

    The output contains ONLY str/int/float/bool/None/list/dict — never
    datetime, UUID, bytes, or any other type. This makes the canonical
    representation BSON-round-trip-safe by construction: there's nothing
    left for BSON to convert.
    """
    if obj is None or isinstance(obj, (str, int, bool)):
        return obj
    if isinstance(obj, float):
        return obj
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, dict):
        return {k: _to_canonical(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_canonical(v) for v in obj]
    return str(obj)


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    canonical = _to_canonical(payload)
    return json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")


def compute_hash(prev_hash: str, payload: dict[str, Any]) -> str:
    h = hashlib.sha256()
    h.update(prev_hash.encode("utf-8"))
    h.update(_canonical_bytes(payload))
    return h.hexdigest()


def _hash_payload_from_doc(doc: dict[str, Any]) -> dict[str, Any]:
    """Extract only the hashed fields from a stored document.

    Verification reads what was stored — there is no reconstruction step,
    so round-trip drift cannot break the hash.
    """
    return {k: doc.get(k) for k in HASHED_FIELDS}


class EventSpine:
    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self._db = db
        self._lock = asyncio.Lock()
        self._last_hash: str = get_settings().audit_genesis_hash
        self._last_seq: int = 0

    @property
    def col(self):
        return self._db[EVENT_LOG]

    async def bootstrap(self) -> None:
        """Load the chain tail from the database on startup."""
        last = await self.col.find_one(sort=[("seq", -1)])
        if last:
            self._last_hash = last["hash"]
            self._last_seq = int(last["seq"])

    async def append(
        self,
        *,
        op: str,
        intent: Intent,
        priority: Priority,
        order_id: str,
        diff: list[DiffEntry],
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
        resume_token: dict[str, Any] | None,
        cluster_time: dict[str, Any] | None,
        correlation_id: str | None = None,
    ) -> Event:
        async with self._lock:
            self._last_seq += 1
            seq = self._last_seq
            prev_hash = self._last_hash
            ts = datetime.now(timezone.utc)
            event_id = str(uuid.uuid4())

            # Canonicalize ALL dynamic fields up front. After this step,
            # there are no datetime/UUID/bytes left anywhere — only the
            # plain JSON primitive types Mongo stores and returns identically.
            canon_before = _to_canonical(before)
            canon_after = _to_canonical(after)
            canon_diff = [_to_canonical(d.model_dump()) for d in diff]

            payload_for_hash = {
                "event_id": event_id,
                "seq": seq,
                "op": op,
                "intent": intent.value,
                "priority": priority.value,
                "order_id": order_id,
                "diff": canon_diff,
                "before": canon_before,
                "after": canon_after,
                "ts": ts.isoformat(),
                "schema_version": 1,
                "correlation_id": correlation_id,
            }
            h = compute_hash(prev_hash, payload_for_hash)

            # Storage document: the hashed fields verbatim + auxiliary metadata.
            # No Pydantic serialization. No surprise conversions. What we hashed
            # is exactly what we store.
            storage_doc: dict[str, Any] = dict(payload_for_hash)
            storage_doc.update({
                "resume_token": _to_canonical(resume_token),
                "cluster_time": _to_canonical(cluster_time),
                "coalesced": False,
                "coalesced_count": 1,
                "prev_hash": prev_hash,
                "hash": h,
            })
            await self.col.insert_one(storage_doc)

            event = Event(
                event_id=event_id,
                seq=seq,
                op=op,  # type: ignore[arg-type]
                intent=intent,
                priority=priority,
                order_id=order_id,
                diff=[DiffEntry(**d) for d in canon_diff],
                before=canon_before,
                after=canon_after,
                ts=ts,
                resume_token=resume_token,
                cluster_time=cluster_time,
                correlation_id=correlation_id,
                prev_hash=prev_hash,
                hash=h,
            )

            self._last_hash = h
            return event

    async def replay_since(self, *, seq: int | None = None, event_id: str | None = None, limit: int = 1000) -> list[dict[str, Any]]:
        query: dict[str, Any] = {}
        start_seq = seq
        if start_seq is None and event_id is not None:
            doc = await self.col.find_one({"event_id": event_id})
            if doc is not None:
                start_seq = int(doc["seq"])
        if start_seq is not None:
            query["seq"] = {"$gt": int(start_seq)}
        cursor = self.col.find(query).sort("seq", 1).limit(limit)
        return [d async for d in cursor]

    async def verify_chain(self, limit: int | None = None) -> dict[str, Any]:
        """Walk the chain and return a structural integrity report.

        For each stored event, re-hash the same fields that were hashed
        at append time. Since we stored canonical (plain-string) values,
        no type round-trip can shift the hash.
        """
        prev = get_settings().audit_genesis_hash
        verified = 0
        broken_at: int | None = None
        broken_event_id: str | None = None

        cursor = self.col.find().sort("seq", 1)
        if limit is not None:
            cursor = cursor.limit(limit)
        async for doc in cursor:
            payload = _hash_payload_from_doc(doc)
            expected = compute_hash(prev, payload)
            if doc.get("prev_hash") != prev or doc.get("hash") != expected:
                broken_at = int(doc["seq"])
                broken_event_id = doc["event_id"]
                break
            prev = doc["hash"]
            verified += 1

        return {
            "verified": verified,
            "head_hash": prev,
            "intact": broken_at is None,
            "broken_at_seq": broken_at,
            "broken_event_id": broken_event_id,
        }

    @property
    def head_hash(self) -> str:
        return self._last_hash

    @property
    def last_seq(self) -> int:
        return self._last_seq
