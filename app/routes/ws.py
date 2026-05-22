"""WebSocket endpoint — parallel to /stream (SSE).

Why offer both?
- SSE is simpler, HTTP/1.1 compatible, and the right primitive for a
  one-directional server-push feed. It is what the dashboard uses.
- WebSocket is bidirectional and what most Indian broker terminals
  consume (Kite Ticker, Angel SmartAPI WebSocket, etc.).

Real fintech consumers expect a WebSocket endpoint, so we expose one.
Both endpoints share the same broker + subscription model — every
event, every filter, every replay guarantee carries over.

Wire protocol (text frames, JSON):

  client → server (optional, after connect):
    { "type": "subscribe", "intents": "cancellation,status_change",
      "customer": "Alice" }

  server → client:
    { "type": "hello", "client_id": "...", "subscription": {...} }
    { "type": "change", "event": {...full event...} }
    { "type": "system_health", "data": {...} }
    { "type": "heartbeat", "t": 1716200000.0 }
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from ..auth import Subscription
from ..config import get_settings

log = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws")
async def websocket_stream(
    websocket: WebSocket,
    intents: str | None = Query(default=None),
    customer: str | None = Query(default=None),
) -> None:
    await websocket.accept()
    broker = websocket.app.state.broker
    settings = get_settings()

    sub = Subscription.parse(intents, customer)
    client = await broker.subscribe(sub)

    await websocket.send_text(json.dumps({
        "type": "hello",
        "client_id": client.id,
        "subscription": {
            "intents": [i.value for i in client.subscription.intents]
            if client.subscription.intents else None,
            "customer": client.subscription.customer,
        },
    }))

    heartbeat = asyncio.create_task(_heartbeat(websocket, settings.heartbeat_interval))
    reader = asyncio.create_task(_drain_client_messages(websocket))

    try:
        while True:
            try:
                item = await asyncio.wait_for(client.queue.get(), timeout=settings.heartbeat_interval)
            except asyncio.TimeoutError:
                continue

            if isinstance(item, dict) and item.get("__type__") == "system_health":
                payload = {"type": "system_health", "data": item}
                await websocket.send_text(json.dumps(payload, default=str))
            elif isinstance(item, dict) and item.get("__type__") == "ping":
                continue  # heartbeat handled by dedicated task
            else:
                event_json = item.model_dump_json() if hasattr(item, "model_dump_json") else json.dumps(item, default=str)
                await websocket.send_text(json.dumps({"type": "change", "event": json.loads(event_json)}, default=str))
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("ws_stream_error")
    finally:
        heartbeat.cancel()
        reader.cancel()
        await broker.unsubscribe(client.id)
        try:
            await websocket.close()
        except Exception:
            pass


async def _heartbeat(ws: WebSocket, interval: int) -> None:
    try:
        while True:
            await asyncio.sleep(interval)
            await ws.send_text(json.dumps({"type": "heartbeat", "t": time.time()}))
    except (asyncio.CancelledError, Exception):
        return


async def _drain_client_messages(ws: WebSocket) -> None:
    """Read and discard client-sent frames so the WebSocket loop progresses.
    In a richer client protocol we'd dispatch on `type` here (subscribe,
    unsubscribe, replay-request). For now we just keep the channel drained.
    """
    try:
        while True:
            msg: Any = await ws.receive_text()
            try:
                json.loads(msg)
            except Exception:
                pass
    except (WebSocketDisconnect, asyncio.CancelledError, Exception):
        return
