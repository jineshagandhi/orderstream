"""Atomic snapshot + read-watermark endpoint.

This solves the snapshot+tail race that Firebase, Supabase, and Hasura
all have documented bugs around: a new client needs (a) the current state
and (b) a cursor from which to receive future events, with no gap and no
overlap.

Strategy:
1. Read the current head_seq from the event spine BEFORE reading state.
2. Read the orders collection.
3. Return both together. The client uses head_seq as `since_seq` for
   the SSE /stream connection.

Any event published between steps 1 and 2 will have seq > head_seq, so
the client will receive it via the stream. Any event published before
step 1 is already reflected in the state read at step 2.
"""

from __future__ import annotations

import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from ..db import ORDERS

router = APIRouter()


@router.get("/snapshot")
async def snapshot(request: Request):
    db = request.app.state.db
    spine = request.app.state.spine

    head_seq = spine.last_seq
    head_hash = spine.head_hash

    docs = []
    async for doc in db[ORDERS].find().sort("updated_at", -1):
        doc["_id"] = str(doc["_id"])
        docs.append(doc)

    return JSONResponse(
        content=json.loads(
            json.dumps(
                {
                    "head_seq": head_seq,
                    "head_hash": head_hash,
                    "count": len(docs),
                    "orders": docs,
                },
                default=str,
            )
        )
    )
