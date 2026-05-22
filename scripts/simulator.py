"""Realistic traffic simulator.

Produces a stream of inserts, status walks, price corrections, and
occasional cancellations against the running app. Used to demonstrate
the system end-to-end without needing a real broker feed.

Patterns generated:
- New orders at a steady rate.
- Burst writes on existing orders (tests the cohesion buffer).
- Random cancellations (tests urgent-bypass).
- Price corrections (tests longer coalescing window).

Usage:
    python scripts/simulator.py                 # default rate
    SIM_NEW_PER_SEC=2 SIM_UPDATE_PER_SEC=8 python scripts/simulator.py
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from faker import Faker
from motor.motor_asyncio import AsyncIOMotorClient

from app.config import get_settings


PRODUCTS = [
    "RELIANCE 2500 CE", "TCS 4100 PE", "HDFC BANK 1700 FUT", "NIFTY 50 INDEX",
    "BANKNIFTY 51000 CE", "INFY 1500 PE", "WIPRO 590 FUT", "TATAMOTORS 1100 CE",
]

ALGO_IDS = ["APT-MOMENTUM-V2", "APT-RSI-MEAN-REVERT", "APT-BB-SQUEEZE", "APT-VWAP-SCALP"]
BROKER_IDS = ["zerodha", "angelone", "fyers", "upstox", "dhan"]

STATUS_PROGRESSION = {
    "pending": ["validating", "cancelled"],
    "validating": ["shipped", "cancelled"],
    "shipped": ["delivered"],
    "delivered": [],
    "cancelled": [],
}


class Simulator:
    def __init__(self, db):
        self.db = db
        self.fake = Faker()
        self.new_per_sec    = float(os.environ.get("SIM_NEW_PER_SEC", "1.5"))
        self.update_per_sec = float(os.environ.get("SIM_UPDATE_PER_SEC", "5"))
        self.burst_chance   = float(os.environ.get("SIM_BURST_CHANCE", "0.2"))
        self.cancel_chance  = float(os.environ.get("SIM_CANCEL_CHANCE", "0.05"))
        self.stop_event = asyncio.Event()

    async def run(self) -> None:
        await asyncio.gather(
            self._loop_new(),
            self._loop_update(),
        )

    async def _loop_new(self) -> None:
        interval = 1.0 / self.new_per_sec if self.new_per_sec > 0 else 99999
        while not self.stop_event.is_set():
            try:
                await self._insert_one()
            except Exception as e:
                print(f"insert failed: {e}", file=sys.stderr)
            await asyncio.sleep(random.uniform(interval * 0.5, interval * 1.5))

    async def _loop_update(self) -> None:
        interval = 1.0 / self.update_per_sec if self.update_per_sec > 0 else 99999
        while not self.stop_event.is_set():
            try:
                await self._update_one()
            except Exception as e:
                print(f"update failed: {e}", file=sys.stderr)
            await asyncio.sleep(random.uniform(interval * 0.5, interval * 1.5))

    async def _insert_one(self) -> None:
        now = datetime.now(timezone.utc)
        doc = {
            "_id": f"ord_{uuid.uuid4().hex[:12]}",
            "customer_name": self.fake.name(),
            "product_name": random.choice(PRODUCTS),
            "status": "pending",
            "price": round(random.uniform(100, 5000), 2),
            "algo_id": random.choice(ALGO_IDS),
            "broker_id": random.choice(BROKER_IDS),
            "created_at": now,
            "updated_at": now,
        }
        await self.db["orders"].insert_one(doc)
        print(f"+ insert  {doc['_id']:<22} {doc['product_name']}")

    async def _update_one(self) -> None:
        candidates = await self.db["orders"].find(
            {"status": {"$nin": ["delivered", "cancelled"]}}
        ).to_list(length=200)
        if not candidates:
            return
        order = random.choice(candidates)

        if random.random() < self.cancel_chance:
            new_status = "cancelled"
        else:
            options = STATUS_PROGRESSION.get(order["status"], [])
            if not options:
                return
            non_cancel = [s for s in options if s != "cancelled"]
            new_status = random.choice(non_cancel or options)

        updates: dict = {
            "status": new_status,
            "updated_at": datetime.now(timezone.utc),
        }

        if random.random() < self.burst_chance:
            await self.db["orders"].update_one({"_id": order["_id"]}, {"$set": {"updated_at": datetime.now(timezone.utc)}})
            await asyncio.sleep(0.01)
            updates["price"] = round(order.get("price", 1000) * random.uniform(0.95, 1.05), 2)
            await asyncio.sleep(0.01)

        await self.db["orders"].update_one({"_id": order["_id"]}, {"$set": updates})
        tag = "CANCEL" if new_status == "cancelled" else "update"
        print(f"~ {tag:<6} {order['_id']:<22} → {new_status}")


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    sim = Simulator(db)
    print(f"simulator running — new/sec={sim.new_per_sec}, update/sec={sim.update_per_sec}")
    print("press Ctrl+C to stop")
    try:
        await sim.run()
    except KeyboardInterrupt:
        pass
    client.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
