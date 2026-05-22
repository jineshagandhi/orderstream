"""One-shot replica set initialization.

Use this if your MongoDB instance is running but hasn't been initialized as
a replica set yet. The docker-compose healthcheck attempts this automatically,
but you can run this script as a fallback or for non-Docker setups.

Usage:
    python scripts/init_replica.py
"""

from __future__ import annotations

import sys
from urllib.parse import urlparse

from pymongo import MongoClient
from pymongo.errors import OperationFailure


def main() -> int:
    uri = "mongodb://localhost:27017/?directConnection=true"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)

    try:
        status = client.admin.command("replSetGetStatus")
        print(f"replica set already initialized: {status.get('set')} (members: {len(status.get('members', []))})")
        return 0
    except OperationFailure as e:
        if "no replset config" not in str(e).lower() and e.code != 94:
            print(f"unexpected error checking replSetGetStatus: {e}", file=sys.stderr)

    cfg = {
        "_id": "rs0",
        "members": [{"_id": 0, "host": "localhost:27017"}],
    }
    print("initiating replica set rs0…")
    try:
        result = client.admin.command("replSetInitiate", cfg)
        print(f"OK: {result}")
        return 0
    except OperationFailure as e:
        print(f"replSetInitiate failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
