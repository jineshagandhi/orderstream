from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from .config import get_settings

_client: AsyncIOMotorClient | None = None


def get_client() -> AsyncIOMotorClient:
    global _client
    if _client is None:
        settings = get_settings()
        _client = AsyncIOMotorClient(settings.mongo_uri)
    return _client


def get_db() -> AsyncIOMotorDatabase:
    settings = get_settings()
    return get_client()[settings.mongo_db]


async def close_client() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None


ORDERS = "orders"
EVENT_LOG = "event_log"


async def ensure_indexes() -> None:
    db = get_db()
    await db[ORDERS].create_index("customer_name")
    await db[ORDERS].create_index("status")
    await db[ORDERS].create_index("updated_at")

    await db[EVENT_LOG].create_index("event_id", unique=True)
    await db[EVENT_LOG].create_index("order_id")
    await db[EVENT_LOG].create_index("ts")
    await db[EVENT_LOG].create_index("seq")
