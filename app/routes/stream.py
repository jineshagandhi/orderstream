"""SSE stream endpoint.

Each connection holds a per-client bounded queue. The server emits:
- `event: change` — domain events from the watcher
- `event: system_health` — periodic system state
- `event: heartbeat` — keep-alive ping every N seconds

Clients should set `Last-Event-ID` to a resume_token (handled separately
via /events?since= for explicit replay). The browser EventSource will
auto-reconnect; the dashboard reconnect handler queries /events?since=
to bridge the gap.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncIterator

from fastapi import APIRouter, Query, Request
from sse_starlette.sse import EventSourceResponse

from ..auth import Subscription
from ..config import get_settings
from ..models import Event

log = logging.getLogger(__name__)
router = APIRouter()


def _serialize_event(ev: Event) -> str:
    return ev.model_dump_json()


def _serialize_system(payload: dict) -> str:
    return json.dumps(payload, default=str)


@router.get("/stream")
async def stream(
    request: Request,
    intents: str | None = Query(default=None, description="comma-separated intent filter"),
    customer: str | None = Query(default=None, description="restrict to a single customer"),
):
    broker = request.app.state.broker
    settings = get_settings()
    sub = Subscription.parse(intents, customer)
    client = await broker.subscribe(sub)

    async def event_gen() -> AsyncIterator[dict]:
        try:
            yield {
                "event": "connected",
                "data": json.dumps(
                    {
                        "client_id": client.id,
                        "subscription": {
                            "intents": [i.value for i in client.subscription.intents]
                            if client.subscription.intents
                            else None,
                            "customer": client.subscription.customer,
                        },
                    }
                ),
            }

            heartbeat_task = asyncio.create_task(_heartbeat_loop(client, settings.heartbeat_interval))

            try:
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        item = await asyncio.wait_for(client.queue.get(), timeout=settings.heartbeat_interval)
                    except asyncio.TimeoutError:
                        continue
                    if isinstance(item, dict) and item.get("__type__") == "system_health":
                        yield {"event": "system_health", "data": _serialize_system(item)}
                    elif isinstance(item, dict) and item.get("__type__") == "ping":
                        yield {"event": "heartbeat", "data": _serialize_system({"t": item.get("t")})}
                    else:
                        yield {
                            "event": "change",
                            "id": item.event_id if hasattr(item, "event_id") else None,
                            "data": _serialize_event(item),
                        }
            finally:
                heartbeat_task.cancel()
        finally:
            await broker.unsubscribe(client.id)

    return EventSourceResponse(event_gen(), ping=None)


async def _heartbeat_loop(client, interval: int) -> None:
    import time as _time
    try:
        while True:
            await asyncio.sleep(interval)
            try:
                client.queue.put_nowait({"__type__": "ping", "t": _time.time()})
            except asyncio.QueueFull:
                pass
    except asyncio.CancelledError:
        return
