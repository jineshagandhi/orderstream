"""/health — JSON metrics for ops dashboards and uptime probes."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from ..health import HEALTH

router = APIRouter()


@router.get("/health")
async def health(request: Request):
    broker = request.app.state.broker
    spine = request.app.state.spine
    stats = broker.stats()
    lag = HEALTH.stream_lag_ms()
    status = HEALTH.classify(lag, stats["dropped_events_60s"])

    return JSONResponse(
        content={
            "status": status,
            "uptime_seconds": HEALTH.uptime(),
            "db_connected": HEALTH.db_connected,
            "watcher_alive": HEALTH.watcher_alive,
            "stream_lag_ms": lag,
            "events_emitted_60s": HEALTH.events_in_window(60),
            "clients_connected": stats["clients_connected"],
            "queue_high_watermark": stats["queue_high_watermark"],
            "dropped_events_60s": stats["dropped_events_60s"],
            "spine": {
                "last_seq": spine.last_seq,
                "head_hash": spine.head_hash,
            },
        }
    )
