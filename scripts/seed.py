"""Seed a small number of orders for an initial demo state."""

from __future__ import annotations

import asyncio
import os
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
    "ADANI ENT 3200 PE", "ITC 480 FUT", "SBIN 820 CE", "AXIS BANK 1180 PE",
]

STATUSES = ["pending", "validating", "shipped", "delivered"]

ALGO_IDS = ["APT-MOMENTUM-V2", "APT-RSI-MEAN-REVERT", "APT-BB-SQUEEZE", "APT-VWAP-SCALP"]
BROKER_IDS = ["zerodha", "angelone", "fyers", "upstox", "dhan"]


async def main() -> int:
    settings = get_settings()
    fake = Faker()
    client = AsyncIOMotorClient(settings.mongo_uri)
    db = client[settings.mongo_db]

    count = int(os.environ.get("SEED_COUNT", "20"))
    docs = []
    now = datetime.now(timezone.utc)
    for _ in range(count):
        docs.append({
            "_id": f"ord_{uuid.uuid4().hex[:12]}",
            "customer_name": fake.name(),
            "product_name": fake.random_element(PRODUCTS),
            "status": fake.random_element(STATUSES),
            "price": round(float(fake.pyfloat(min_value=100, max_value=5000)), 2),
            "algo_id": fake.random_element(ALGO_IDS),
            "broker_id": fake.random_element(BROKER_IDS),
            "created_at": now,
            "updated_at": now,
        })

    result = await db["orders"].insert_many(docs)
    print(f"inserted {len(result.inserted_ids)} orders")
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
