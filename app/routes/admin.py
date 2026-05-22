"""Admin endpoints — kill-switch and operator controls."""

from __future__ import annotations

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse

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
