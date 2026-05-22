"""Intent-aware Temporal Cohesion Buffer.

Coalesces rapid same-order writes into a single emission per logical event.
- Urgent intents (cancellation, new_order, deletion) bypass the buffer.
- Status changes use a short window.
- Price corrections use a longer window.

Crash safety: caller is expected to persist to the event spine BEFORE
buffering, so a process death loses at most the coalescing benefit, not data.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .models import Event, Intent, Priority
from .diff import merge_diffs

log = logging.getLogger(__name__)

EmitCallback = Callable[[Event], Awaitable[None]]


@dataclass
class _Pending:
    initial: Event
    latest: Event
    timer: asyncio.Task | None = None
    count: int = 1


class CohesionBuffer:
    def __init__(
        self,
        emit: EmitCallback,
        window_status: float,
        window_price: float,
        window_default: float,
    ) -> None:
        self._emit = emit
        self._windows: dict[Intent, float] = {
            Intent.CANCELLATION: 0.0,
            Intent.NEW_ORDER: 0.0,
            Intent.DELETION: 0.0,
            Intent.STATUS_CHANGE: window_status,
            Intent.PRICE_CORRECTION: window_price,
            Intent.OTHER: window_default,
        }
        self._pending: dict[str, _Pending] = {}
        self._lock = asyncio.Lock()

    def window_for(self, intent: Intent) -> float:
        return self._windows.get(intent, 0.0)

    async def add(self, event: Event) -> None:
        window = self.window_for(event.intent)
        if window <= 0 or event.priority == Priority.URGENT:
            await self._emit(event)
            return

        order_id = event.order_id
        async with self._lock:
            existing = self._pending.get(order_id)
            if existing is None:
                pending = _Pending(initial=event, latest=event)
                pending.timer = asyncio.create_task(self._flush_after(order_id, window))
                self._pending[order_id] = pending
            else:
                merged_diff = merge_diffs(existing.initial.diff, event.diff)
                merged = event.model_copy(
                    update={
                        "diff": merged_diff,
                        "before": existing.initial.before,
                        "coalesced": True,
                        "coalesced_count": existing.count + 1,
                    }
                )
                existing.latest = merged
                existing.count += 1

    async def _flush_after(self, order_id: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        async with self._lock:
            pending = self._pending.pop(order_id, None)
        if pending is None:
            return
        try:
            await self._emit(pending.latest)
        except Exception:
            log.exception("cohesion flush emit failed for order %s", order_id)

    async def flush_all(self) -> None:
        """Drain pending entries on shutdown."""
        async with self._lock:
            entries = list(self._pending.values())
            self._pending.clear()
        for pending in entries:
            if pending.timer:
                pending.timer.cancel()
            try:
                await self._emit(pending.latest)
            except Exception:
                log.exception("cohesion shutdown flush failed")
