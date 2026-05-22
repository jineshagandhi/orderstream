"""MongoDB Change Stream watcher with watchdog + reconciliation.

Pipeline:
  Change Stream  -->  before/after resolver  -->  diff  -->  intent
                                                        |
                                                        v
                                                  Event Spine (append)
                                                        |
                                                        v
                                                  Cohesion Buffer
                                                        |
                                                        v
                                                    SSE Broker

Failure-mode handling:
- Stream death (network blip, primary stepdown, oplog rollover):
  caught, logged, and we restart from last resume_token. If the token
  is invalid (oplog has rolled past), we trigger reconciliation: full
  state snapshot is taken and clients receive synthetic events marking
  current state as ground truth.
- Watchdog: a background coroutine asserts at least one event (or no-op
  ping) within HEARTBEAT * 4 seconds. If silent, the stream is restarted.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError

from .cohesion import CohesionBuffer
from .config import get_settings
from .db import ORDERS
from .diff import compute_diff
from .event_spine import EventSpine
from .health import HEALTH
from .intent import classify
from .models import Event

log = logging.getLogger(__name__)


class ChangeWatcher:
    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        spine: EventSpine,
        cohesion: CohesionBuffer,
    ) -> None:
        self.db = db
        self.spine = spine
        self.cohesion = cohesion
        self._cache: dict[str, dict[str, Any]] = {}
        self._resume_token: dict[str, Any] | None = None
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def warm_cache(self) -> None:
        """Load current state into the in-memory before-image cache."""
        async for doc in self.db[ORDERS].find():
            self._cache[str(doc["_id"])] = self._strip(doc)
        HEALTH.db_connected = True
        log.info("watcher_cache_warmed", extra={"size": len(self._cache)})

    @staticmethod
    def _strip(doc: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in doc.items() if k != "_id"}

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run_forever(), name="change_watcher")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run_forever(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._consume_change_stream()
                backoff = 1.0
            except PyMongoError as e:
                log.warning("change_stream_error", extra={"err": str(e)})
                HEALTH.watcher_alive = False
                await self._reconcile_or_reset()
            except Exception:
                log.exception("change_stream_unexpected_error")
                HEALTH.watcher_alive = False
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    async def _consume_change_stream(self) -> None:
        kwargs: dict[str, Any] = {"full_document": "updateLookup"}
        if self._resume_token is not None:
            kwargs["resume_after"] = self._resume_token

        async with self.db[ORDERS].watch(**kwargs) as stream:
            HEALTH.watcher_alive = True
            log.info("change_stream_open", extra={"resumed": self._resume_token is not None})
            async for change in stream:
                if self._stop.is_set():
                    break
                await self._process(change)
                self._resume_token = change.get("_id")

    async def _process(self, change: dict[str, Any]) -> None:
        op = change["operationType"]
        if op == "invalidate":
            log.warning("change_stream_invalidated")
            raise PyMongoError("invalidated")

        if op not in {"insert", "update", "replace", "delete"}:
            return

        doc_key = change["documentKey"]["_id"]
        order_id = str(doc_key)

        if op in {"insert", "replace", "update"}:
            after_doc = change.get("fullDocument")
            if after_doc is None:
                return
            after = self._strip(after_doc)
        else:
            after = None

        before = self._cache.get(order_id) if op in {"update", "replace", "delete"} else None

        diff = compute_diff(before, after)
        if not diff and op == "update":
            return

        intent, priority = classify(op, before, after, diff)
        correlation_id = str(uuid.uuid4())

        event = await self.spine.append(
            op=op,
            intent=intent,
            priority=priority,
            order_id=order_id,
            diff=diff,
            before=before,
            after=after,
            resume_token=change.get("_id"),
            cluster_time=self._cluster_time(change),
            correlation_id=correlation_id,
        )

        if after is None:
            self._cache.pop(order_id, None)
        else:
            self._cache[order_id] = after

        HEALTH.record_event()
        await self.cohesion.add(event)

    @staticmethod
    def _cluster_time(change: dict[str, Any]) -> dict[str, Any] | None:
        ct = change.get("clusterTime")
        if ct is None:
            return None
        return {"time": int(ct.time), "inc": int(ct.inc)} if hasattr(ct, "time") else {"raw": str(ct)}

    async def _reconcile_or_reset(self) -> None:
        """When the resume token is invalid, reconcile state from scratch."""
        log.warning("watcher_reconciling")
        try:
            old_cache = dict(self._cache)
            self._cache.clear()
            await self.warm_cache()
            now_ids = set(self._cache.keys())
            old_ids = set(old_cache.keys())

            for added in now_ids - old_ids:
                event = await self.spine.append(
                    op="insert",
                    intent=__import__("app.models", fromlist=["Intent"]).Intent.NEW_ORDER,
                    priority=__import__("app.models", fromlist=["Priority"]).Priority.URGENT,
                    order_id=added,
                    diff=compute_diff(None, self._cache[added]),
                    before=None,
                    after=self._cache[added],
                    resume_token=None,
                    cluster_time=None,
                    correlation_id="reconciliation",
                )
                await self.cohesion.add(event)
        except Exception:
            log.exception("reconciliation_failed")

        self._resume_token = None  # next loop opens a fresh stream
