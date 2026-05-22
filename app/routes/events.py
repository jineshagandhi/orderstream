"""Replay endpoint — solves the reconnect data-loss problem.

Clients reconnecting after a network drop call:
    GET /events?since_seq=<int>     (preferred — monotonic)
    GET /events?since_event=<uuid>  (also supported)

The server returns events strictly AFTER that point, in seq order.
This guarantees no duplicates and no gaps on reconnect — the property
Zerodha Kite explicitly lacks (their forum confirms: "if WebSocket opens
after order placement, you miss that order update").
"""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/events")
async def replay_events(
    request: Request,
    since_seq: int | None = Query(default=None),
    since_event: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
):
    spine = request.app.state.spine
    if since_seq is None and since_event is None:
        raise HTTPException(status_code=400, detail="provide since_seq or since_event")

    docs = await spine.replay_since(seq=since_seq, event_id=since_event, limit=limit)
    return JSONResponse(
        content={
            "count": len(docs),
            "events": json.loads(json.dumps(docs, default=str)),
        }
    )
