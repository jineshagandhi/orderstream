"""FastAPI entry point + application lifecycle.

Startup:
  1. Connect Mongo, ensure indexes.
  2. Bootstrap event spine (load tail of hash chain).
  3. Warm the watcher cache.
  4. Start the change stream watcher task.
  5. Start the system_health emitter task.

Shutdown:
  1. Cancel emitter + watcher tasks (graceful).
  2. Flush cohesion buffer (no in-flight data loss).
  3. Close Mongo client.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .broker import SSEBroker
from .cohesion import CohesionBuffer
from .config import get_settings
from .db import close_client, ensure_indexes, get_client, get_db
from .demo import DemoSimulator
from .event_spine import EventSpine
from .health import HEALTH, periodic_health_emitter
from .routes import admin, audit, events, health, orders, snapshot, stream, ws
from .watcher import ChangeWatcher


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    _configure_logging(settings.log_level)
    log = logging.getLogger("orderstream")
    log.info("starting orderstream v1.0.0")

    client = get_client()
    await client.admin.command("ping")
    HEALTH.db_connected = True

    db = get_db()
    await ensure_indexes()

    spine = EventSpine(db)
    await spine.bootstrap()

    broker = SSEBroker(max_queue=settings.max_client_queue)

    cohesion = CohesionBuffer(
        emit=broker.broadcast,
        window_status=settings.cohesion_window_status,
        window_price=settings.cohesion_window_price,
        window_default=settings.cohesion_window_default,
    )

    watcher = ChangeWatcher(db=db, spine=spine, cohesion=cohesion)
    await watcher.warm_cache()
    await watcher.start()

    health_task = asyncio.create_task(periodic_health_emitter(broker, settings.health_event_interval))

    demo: DemoSimulator | None = None
    if os.environ.get("DEMO_MODE", "").lower() in ("true", "1", "yes", "on"):
        demo = DemoSimulator(db)
        await demo.start()
        log.info("demo_mode_enabled — background traffic generator running")

    app.state.db = db
    app.state.broker = broker
    app.state.spine = spine
    app.state.cohesion = cohesion
    app.state.watcher = watcher
    app.state.demo = demo

    log.info("orderstream ready on http://%s:%d", settings.app_host, settings.app_port)

    try:
        yield
    finally:
        log.info("shutting down orderstream")
        if demo is not None:
            await demo.stop()
        health_task.cancel()
        await watcher.stop()
        await cohesion.flush_all()
        await close_client()


app = FastAPI(
    title="orderstream",
    description=(
        "Real-time, audit-grade order event spine. "
        "MongoDB Change Streams → field-level diff → intent classification → "
        "tamper-evident hash chain → SSE fan-out."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(stream.router, tags=["stream"])
app.include_router(ws.router, tags=["stream"])
app.include_router(events.router, tags=["replay"])
app.include_router(snapshot.router, tags=["snapshot"])
app.include_router(health.router, tags=["ops"])
app.include_router(audit.router, tags=["audit"])
app.include_router(orders.router, tags=["orders"])
app.include_router(admin.router, tags=["admin"])


CLIENT_DIR = Path(__file__).resolve().parent.parent / "client"
if CLIENT_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(CLIENT_DIR)), name="static")


@app.get("/", include_in_schema=False)
async def index():
    dashboard = CLIENT_DIR / "dashboard.html"
    if dashboard.exists():
        return FileResponse(str(dashboard))
    return JSONResponse(content={"service": "orderstream", "status": "ok"})


@app.get("/api", include_in_schema=False)
async def api_root():
    return JSONResponse(
        content={
            "service": "orderstream",
            "version": "1.0.0",
            "endpoints": {
                "stream_sse": "GET /stream?intents=...&customer=...",
                "stream_ws": "WS /ws?intents=...&customer=...",
                "snapshot": "GET /snapshot",
                "replay": "GET /events?since_seq=N",
                "health": "GET /health",
                "audit_verify": "GET /audit/verify",
                "audit_order": "GET /audit/order/{order_id}",
                "kill_switch_status": "GET /admin/kill-switch",
                "kill_switch_engage": "POST /admin/kill-switch",
                "orders_list": "GET /orders",
                "orders_create": "POST /orders",
                "orders_update": "PATCH /orders/{order_id}",
                "orders_delete": "DELETE /orders/{order_id}",
            },
        }
    )
