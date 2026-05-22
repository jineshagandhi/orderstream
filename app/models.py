from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, ConfigDict


class OrderStatus(str, Enum):
    PENDING = "pending"
    VALIDATING = "validating"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class Intent(str, Enum):
    NEW_ORDER = "new_order"
    STATUS_CHANGE = "status_change"
    CANCELLATION = "cancellation"
    PRICE_CORRECTION = "price_correction"
    DELETION = "deletion"
    OTHER = "other"


class Priority(str, Enum):
    URGENT = "urgent"
    NORMAL = "normal"
    LOW = "low"


INTENT_PRIORITY: dict[Intent, Priority] = {
    Intent.CANCELLATION: Priority.URGENT,
    Intent.NEW_ORDER: Priority.URGENT,
    Intent.DELETION: Priority.URGENT,
    Intent.STATUS_CHANGE: Priority.NORMAL,
    Intent.PRICE_CORRECTION: Priority.LOW,
    Intent.OTHER: Priority.NORMAL,
}


class Order(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str = Field(alias="_id")
    customer_name: str
    product_name: str
    status: OrderStatus
    price: float | None = None
    updated_at: datetime


class OrderCreate(BaseModel):
    customer_name: str
    product_name: str
    status: OrderStatus = OrderStatus.PENDING
    price: float | None = None
    algo_id: str | None = None
    broker_id: str | None = None


class OrderUpdate(BaseModel):
    customer_name: str | None = None
    product_name: str | None = None
    status: OrderStatus | None = None
    price: float | None = None
    algo_id: str | None = None
    broker_id: str | None = None


class DiffEntry(BaseModel):
    field: str
    before: Any
    after: Any


class Event(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: str
    seq: int
    schema_version: int = 1
    op: Literal["insert", "update", "delete", "replace"]
    intent: Intent
    priority: Priority
    order_id: str
    diff: list[DiffEntry]
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    coalesced: bool = False
    coalesced_count: int = 1
    resume_token: dict[str, Any] | None = None
    cluster_time: dict[str, Any] | None = None
    ts: datetime
    correlation_id: str | None = None
    prev_hash: str
    hash: str


class HealthSnapshot(BaseModel):
    status: Literal["nominal", "degraded", "critical"]
    stream_lag_ms: int
    clients_connected: int
    events_emitted_60s: int
    queue_high_watermark: int
    dropped_events_60s: int
    oldest_pending_ms: int
    db_connected: bool
    watcher_alive: bool
    last_event_at: datetime | None
    uptime_seconds: int
