import asyncio
import uuid
from datetime import datetime, timezone

import pytest

from app.cohesion import CohesionBuffer
from app.models import DiffEntry, Event, Intent, Priority


def make_event(*, intent: Intent, priority: Priority, order_id: str, status_from: str, status_to: str, seq: int) -> Event:
    diff = [DiffEntry(field="status", before=status_from, after=status_to)]
    return Event(
        event_id=str(uuid.uuid4()),
        seq=seq,
        op="update",
        intent=intent,
        priority=priority,
        order_id=order_id,
        diff=diff,
        before={"status": status_from},
        after={"status": status_to},
        ts=datetime.now(timezone.utc),
        prev_hash="0" * 64,
        hash="a" * 64,
    )


@pytest.mark.asyncio
async def test_urgent_bypasses_buffer():
    emitted: list[Event] = []
    async def emit(e: Event) -> None:
        emitted.append(e)

    buf = CohesionBuffer(emit=emit, window_status=0.05, window_price=0.2, window_default=0.1)
    ev = make_event(
        intent=Intent.CANCELLATION, priority=Priority.URGENT,
        order_id="ord_1", status_from="pending", status_to="cancelled", seq=1,
    )
    await buf.add(ev)
    assert len(emitted) == 1


@pytest.mark.asyncio
async def test_status_changes_coalesce_within_window():
    emitted: list[Event] = []
    async def emit(e: Event) -> None:
        emitted.append(e)

    buf = CohesionBuffer(emit=emit, window_status=0.05, window_price=0.2, window_default=0.1)
    a = make_event(
        intent=Intent.STATUS_CHANGE, priority=Priority.NORMAL,
        order_id="ord_1", status_from="pending", status_to="validating", seq=1,
    )
    b = make_event(
        intent=Intent.STATUS_CHANGE, priority=Priority.NORMAL,
        order_id="ord_1", status_from="validating", status_to="shipped", seq=2,
    )
    await buf.add(a)
    await buf.add(b)
    assert emitted == []
    await asyncio.sleep(0.12)
    assert len(emitted) == 1
    out = emitted[0]
    assert out.coalesced is True
    assert out.coalesced_count == 2
    assert any(d.field == "status" and d.before == "pending" and d.after == "shipped" for d in out.diff)


@pytest.mark.asyncio
async def test_different_orders_dont_coalesce():
    emitted: list[Event] = []
    async def emit(e: Event) -> None:
        emitted.append(e)

    buf = CohesionBuffer(emit=emit, window_status=0.05, window_price=0.2, window_default=0.1)
    a = make_event(intent=Intent.STATUS_CHANGE, priority=Priority.NORMAL, order_id="A", status_from="p", status_to="v", seq=1)
    b = make_event(intent=Intent.STATUS_CHANGE, priority=Priority.NORMAL, order_id="B", status_from="p", status_to="v", seq=2)
    await buf.add(a)
    await buf.add(b)
    await asyncio.sleep(0.12)
    assert len(emitted) == 2
    assert {e.order_id for e in emitted} == {"A", "B"}
