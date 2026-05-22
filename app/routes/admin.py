"""Admin endpoints — kill-switch, reset, operator controls."""

from __future__ import annotations

import os

from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import JSONResponse

from ..db import EVENT_LOG, ORDERS
from ..kill_switch import KILL_SWITCH

router = APIRouter()


@router.get("/admin/kill-switch")
async def get_kill_switch(request: Request):
    return JSONResponse(content=KILL_SWITCH.to_dict())


@router.post("/admin/kill-switch")
async def engage_kill_switch(
    request: Request,
    payload: dict = Body(default_factory=dict),
):
    action = (payload.get("action") or "engage").lower()
    if action == "release":
        was = KILL_SWITCH.engaged
        KILL_SWITCH.release()
        broker = request.app.state.broker
        if was:
            await broker.broadcast_system({
                "__type__": "system_health",
                "status": "nominal",
                "kill_switch_released": True,
            })
        return JSONResponse(content={"action": "released", "state": KILL_SWITCH.to_dict()})

    reason = payload.get("reason") or "manual operator action"
    by = payload.get("by") or "operator"
    KILL_SWITCH.engage(reason, by)

    broker = request.app.state.broker
    await broker.broadcast_system({
        "__type__": "system_health",
        "status": "critical",
        "kill_switch_engaged": True,
        "reason": reason,
    })

    return JSONResponse(content={"action": "engaged", "state": KILL_SWITCH.to_dict()})


@router.post("/admin/reset")
async def reset_demo(request: Request, payload: dict = Body(default_factory=dict)):
    """Reset the orders and event_log collections. The demo simulator
    (if DEMO_MODE is on) will re-seed automatically.

    Protected by ADMIN_TOKEN env var if set — otherwise open (for demo use).
    """
    required = os.environ.get("ADMIN_TOKEN")
    if required:
        provided = payload.get("token")
        if provided != required:
            raise HTTPException(status_code=401, detail="invalid admin token")

    db = request.app.state.db
    spine = request.app.state.spine

    orders_deleted = (await db[ORDERS].delete_many({})).deleted_count
    events_deleted = (await db[EVENT_LOG].delete_many({})).deleted_count

    # Reset the spine's in-memory tail so the next event starts at seq=1.
    await spine._refresh_head()

    # Reset the watcher cache so diffs against deleted orders don't leak.
    watcher = request.app.state.watcher
    watcher._cache.clear()

    return JSONResponse(
        content={
            "action": "reset",
            "orders_deleted": orders_deleted,
            "events_deleted": events_deleted,
            "head_hash": spine.head_hash,
            "last_seq": spine.last_seq,
        }
    )
