"""System health tracking + periodic /system_health SSE emission.

Why a proactive health signal?
Zerodha Kite's Feb 2026 outage left users staring at frozen prices with no
indication the system was degraded. By pushing a periodic system_health
event into the SSE stream, clients can show "data may be stale" before the
user notices values stopped moving.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class HealthState:
    started_at: float = field(default_factory=time.time)
    watcher_alive: bool = False
    db_connected: bool = False
    last_event_at: datetime | None = None
    events_window: deque[float] = field(default_factory=lambda: deque(maxlen=5000))
    last_resume_token_ts: float | None = None

    def record_event(self) -> None:
        now = time.time()
        self.events_window.append(now)
        self.last_event_at = datetime.now(timezone.utc)
        self.last_resume_token_ts = now

    def uptime(self) -> int:
        return int(time.time() - self.started_at)

    def events_in_window(self, seconds: int = 60) -> int:
        cutoff = time.time() - seconds
        return sum(1 for t in self.events_window if t >= cutoff)

    def stream_lag_ms(self) -> int:
        if self.last_event_at is None:
            return 0
        delta = (datetime.now(timezone.utc) - self.last_event_at).total_seconds() * 1000
        return int(max(0, delta))

    def classify(self, stream_lag_ms: int, dropped_60s: int) -> str:
        if not self.db_connected or not self.watcher_alive:
            return "critical"
        if stream_lag_ms > 30_000 or dropped_60s > 50:
            return "degraded"
        return "nominal"


HEALTH = HealthState()


async def periodic_health_emitter(broker, interval: int) -> None:
    """Push system_health into broker every `interval` seconds."""
    while True:
        try:
            await asyncio.sleep(interval)
            stats = broker.stats()
            lag = HEALTH.stream_lag_ms()
            status = HEALTH.classify(lag, stats["dropped_events_60s"])
            payload = {
                "__type__": "system_health",
                "status": status,
                "stream_lag_ms": lag,
                "clients_connected": stats["clients_connected"],
                "events_emitted_60s": HEALTH.events_in_window(60),
                "queue_high_watermark": stats["queue_high_watermark"],
                "dropped_events_60s": stats["dropped_events_60s"],
                "uptime_seconds": HEALTH.uptime(),
                "watcher_alive": HEALTH.watcher_alive,
                "db_connected": HEALTH.db_connected,
            }
            await broker.broadcast_system(payload)
        except asyncio.CancelledError:
            break
        except Exception:
            import logging
            logging.getLogger(__name__).exception("health emitter failed")
