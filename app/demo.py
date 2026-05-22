"""DEMO_MODE — auto-seed and background traffic generator.

Why this exists:
A live demo with an empty dashboard ("No orders yet") looks broken to a
reviewer who clicks the link without reading the README. When DEMO_MODE=true
is set in the environment, the app:

  1. Ensures the orders collection has a baseline (20 orders), seeding any
     deficit on boot.
  2. Runs a conservative background loop that produces realistic order
     activity (~1 op every 5-12 seconds) — new orders, status transitions,
     occasional cancellations, occasional price corrections.

The rate is intentionally low so a free-tier Atlas cluster (512 MB) is
not exhausted within a reasonable submission window. To stop activity
entirely without killing the app, engage the kill-switch:
    POST /admin/kill-switch  {"reason": "halt demo"}

This is for the deployed environment ONLY. For local development run
scripts/simulator.py instead.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorDatabase

log = logging.getLogger(__name__)


PRODUCTS = [
    "RELIANCE 2500 CE", "TCS 4100 PE", "HDFC BANK 1700 FUT", "NIFTY 50 INDEX",
    "BANKNIFTY 51000 CE", "INFY 1500 PE", "WIPRO 590 FUT", "TATAMOTORS 1100 CE",
    "ADANI ENT 3200 PE", "ITC 480 FUT", "SBIN 820 CE", "AXIS BANK 1180 PE",
]

ALGO_IDS = ["APT-MOMENTUM-V2", "APT-RSI-MEAN-REVERT", "APT-BB-SQUEEZE", "APT-VWAP-SCALP"]
BROKER_IDS = ["zerodha", "angelone", "fyers", "upstox", "dhan"]

CUSTOMERS = [
    "Rahul Sharma", "Priya Krishnan", "Amit Reddy", "Sneha Patel",
    "Arjun Nair", "Anjali Iyer", "Vikram Singh", "Pooja Joshi",
    "Karthik Menon", "Divya Rao", "Rohan Kapoor", "Meera Banerjee",
]

STATUS_PROGRESSION: dict[str, list[str]] = {
    "pending": ["validating", "cancelled"],
    "validating": ["shipped", "cancelled"],
    "shipped": ["delivered"],
    "delivered": [],
    "cancelled": [],
}


class DemoSimulator:
    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self.db = db
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="demo_simulator")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        try:
            await self._ensure_baseline(target=20)
        except Exception:
            log.exception("demo baseline seed failed")

        while not self._stop.is_set():
            try:
                roll = random.random()
                if roll < 0.20:
                    await self._insert_one()
                elif roll < 0.30:
                    await self._cancel_one()
                else:
                    await self._update_one()
            except Exception:
                log.exception("demo step failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=random.uniform(5, 12))
            except asyncio.TimeoutError:
                continue

    async def _ensure_baseline(self, *, target: int) -> None:
        existing = await self.db["orders"].count_documents({})
        deficit = max(0, target - existing)
        for _ in range(deficit):
            await self._insert_one()
            await asyncio.sleep(0.05)

    async def _insert_one(self) -> None:
        now = datetime.now(timezone.utc)
        doc = {
            "_id": f"ord_{uuid.uuid4().hex[:12]}",
            "customer_name": random.choice(CUSTOMERS),
            "product_name": random.choice(PRODUCTS),
            "status": "pending",
            "price": round(random.uniform(150, 4500), 2),
            "algo_id": random.choice(ALGO_IDS),
            "broker_id": random.choice(BROKER_IDS),
            "created_at": now,
            "updated_at": now,
        }
        await self.db["orders"].insert_one(doc)

    async def _update_one(self) -> None:
        cursor = self.db["orders"].find({"status": {"$nin": ["delivered", "cancelled"]}})
        candidates = await cursor.to_list(length=200)
        if not candidates:
            return
        order = random.choice(candidates)
        options = STATUS_PROGRESSION.get(order["status"], [])
        if not options:
            return
        non_cancel = [s for s in options if s != "cancelled"]
        new_status = random.choice(non_cancel or options)
        updates = {"status": new_status, "updated_at": datetime.now(timezone.utc)}

        if random.random() < 0.15:
            updates["price"] = round(order.get("price", 1000) * random.uniform(0.95, 1.05), 2)

        await self.db["orders"].update_one({"_id": order["_id"]}, {"$set": updates})

    async def _cancel_one(self) -> None:
        cursor = self.db["orders"].find({"status": {"$in": ["pending", "validating"]}})
        candidates = await cursor.to_list(length=100)
        if not candidates:
            return
        order = random.choice(candidates)
        await self.db["orders"].update_one(
            {"_id": order["_id"]},
            {"$set": {"status": "cancelled", "updated_at": datetime.now(timezone.utc)}},
        )
