"""Audit endpoints — cryptographic integrity of the event spine.

GET /audit/verify
    Walks the full chain from genesis. Returns intact=true/false.
    Anyone with read access can independently verify the audit trail.

GET /audit/order/{order_id}
    Returns the full lifecycle of one order — every state change with
    its hash. Useful for compliance investigations or debugging.

Why this matters: the SEBI 2025 framework mandates a tamper-evident
order audit trail. A blockchain-style hash chain provides
mathematical proof of integrity without external infrastructure.
"""

from __future__ import annotations

import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/audit/verify")
async def audit_verify(request: Request):
    spine = request.app.state.spine
    report = await spine.verify_chain()
    return JSONResponse(content=report)


@router.get("/audit/order/{order_id}")
async def audit_order(order_id: str, request: Request):
    spine = request.app.state.spine
    cursor = spine.col.find({"order_id": order_id}).sort("seq", 1)
    events = [d async for d in cursor]
    payload = json.loads(json.dumps({"order_id": order_id, "count": len(events), "events": events}, default=str))
    return JSONResponse(content=payload)
