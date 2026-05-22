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
    # UNIQUE on seq — guards against any scenario where two events could
    # be appended with the same seq (e.g. brief deploy overlap, stale
    # in-memory _last_seq after out-of-band DB modification).
    try:
        await db[EVENT_LOG].create_index("seq", unique=True)
    except Exception:
        # Existing duplicates would block index creation. Fall back to
        # non-unique so the app still boots; the spine's retry logic
        # will still recover from any conflict on subsequent appends.
        await db[EVENT_LOG].create_index("seq")
