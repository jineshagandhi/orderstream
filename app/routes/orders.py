"""CRUD endpoints for orders — these trigger the Change Stream.

Kept minimal: the point of this service is the real-time pipeline,
not order management. But we need create/update/delete to demo it.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from ..db import ORDERS
from ..models import OrderCreate, OrderUpdate

router = APIRouter()


def _serialize(doc: dict) -> dict:
    return json.loads(json.dumps(doc, default=str))


@router.get("/orders")
async def list_orders(
    request: Request,
    customer: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
):
    db = request.app.state.db
    query: dict = {}
    if customer:
        query["customer_name"] = customer
    if status:
        query["status"] = status

    cursor = db[ORDERS].find(query).sort("updated_at", -1).limit(limit)
    docs = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return JSONResponse(content={"count": len(docs), "orders": _serialize(docs)})


@router.post("/orders")
async def create_order(payload: OrderCreate, request: Request):
    db = request.app.state.db
    now = datetime.now(timezone.utc)
    doc = {
        "_id": f"ord_{uuid.uuid4().hex[:12]}",
        "customer_name": payload.customer_name,
        "product_name": payload.product_name,
        "status": payload.status.value,
        "price": payload.price,
        "algo_id": payload.algo_id,
        "broker_id": payload.broker_id,
        "created_at": now,
        "updated_at": now,
    }
    await db[ORDERS].insert_one(doc)
    return JSONResponse(content=_serialize(doc), status_code=201)


@router.patch("/orders/{order_id}")
async def update_order(order_id: str, payload: OrderUpdate, request: Request):
    db = request.app.state.db
    updates = {k: v for k, v in payload.model_dump(exclude_none=True).items()}
    if not updates:
        raise HTTPException(status_code=400, detail="no fields provided")
    if "status" in updates:
        updates["status"] = updates["status"].value if hasattr(updates["status"], "value") else updates["status"]
    updates["updated_at"] = datetime.now(timezone.utc)

    result = await db[ORDERS].update_one({"_id": order_id}, {"$set": updates})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="order not found")
    doc = await db[ORDERS].find_one({"_id": order_id})
    return JSONResponse(content=_serialize(doc))


@router.delete("/orders/{order_id}", status_code=204)
async def delete_order(order_id: str, request: Request):
    db = request.app.state.db
    result = await db[ORDERS].delete_one({"_id": order_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="order not found")
    return None
