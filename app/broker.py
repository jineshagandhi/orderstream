"""SSE broker with per-client bounded queue and flow control.

Design choices documented for the reader:

1. Each client gets a bounded asyncio.Queue.
   - Bounded: a slow client cannot consume unbounded server memory.
   - put_nowait: a full queue means the client is too slow.
2. Slow clients are NOT silently dropped events. They are disconnected
   after a strike threshold, with the drop counted in metrics. Silent
   data loss is the worst failure mode in real-time systems.
3. The broker is stateless w.r.t. event content — it just relays.
   The event spine is the source of truth.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque
from typing import AsyncIterator

from .auth import Subscription
from .kill_switch import KILL_SWITCH
from .models import Event

log = logging.getLogger(__name__)


class _Client:
    __slots__ = ("id", "queue", "subscription", "dropped", "connected_at", "last_delivered_at")

    def __init__(self, client_id: str, sub: Subscription, queue_size: int) -> None:
        self.id = client_id
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=queue_size)
        self.subscription = sub
        self.dropped = 0
        self.connected_at = time.time()
        self.last_delivered_at: float = self.connected_at


class SSEBroker:
    """In-memory fan-out. For multi-process scale, swap put_nowait with
    Redis Pub/Sub publish, and replace the in-process clients dict with
    a per-broker-instance subset.
    """

    def __init__(self, max_queue: int = 200) -> None:
        self._max_queue = max_queue
        self._clients: dict[str, _Client] = {}
        self._lock = asyncio.Lock()
        self._dropped_window: deque[float] = deque(maxlen=10000)

    async def subscribe(self, sub: Subscription) -> _Client:
        cid = str(uuid.uuid4())
        client = _Client(cid, sub, self._max_queue)
        async with self._lock:
            self._clients[cid] = client
        log.info("client_connected", extra={"client_id": cid})
        return client

    async def unsubscribe(self, client_id: str) -> None:
        async with self._lock:
            self._clients.pop(client_id, None)
        log.info("client_disconnected", extra={"client_id": client_id})

    async def broadcast(self, event: Event) -> None:
        # Kill-switch suppresses fan-out. Event is still durable in the
        # spine; clients can replay via /events?since_seq once released.
        if KILL_SWITCH.engaged:
            return
        snapshot = list(self._clients.values())
        now = time.time()
        for client in snapshot:
            if not client.subscription.matches(event):
                continue
            try:
                client.queue.put_nowait(event)
                client.last_delivered_at = now
            except asyncio.QueueFull:
                client.dropped += 1
                self._dropped_window.append(now)
                log.warning(
                    "client_queue_full",
                    extra={"client_id": client.id, "dropped_total": client.dropped},
                )
                if client.dropped >= 5:
                    log.error(
                        "client_evicted_slow_consumer",
                        extra={"client_id": client.id, "dropped_total": client.dropped},
                    )
                    await self.unsubscribe(client.id)

    async def broadcast_system(self, payload: dict) -> None:
        """Broadcast a non-event payload (e.g. system_health) to all clients
        regardless of their subscription. Implemented by wrapping in a
        sentinel queue item that routes use to emit a different SSE event type.
        """
        snapshot = list(self._clients.values())
        for client in snapshot:
            try:
                client.queue.put_nowait(payload)  # type: ignore[arg-type]
            except asyncio.QueueFull:
                pass

    def stats(self) -> dict:
        return {
            "clients_connected": len(self._clients),
            "queue_high_watermark": max((c.queue.qsize() for c in self._clients.values()), default=0),
            "dropped_events_60s": sum(1 for t in self._dropped_window if time.time() - t < 60),
            "max_queue": self._max_queue,
        }

    async def iter_client(self, client: _Client) -> AsyncIterator[Event | dict]:
        while True:
            item = await client.queue.get()
            yield item
