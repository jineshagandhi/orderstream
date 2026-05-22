from datetime import datetime, timezone

from app.event_spine import HASHED_FIELDS, _hash_payload_from_doc, _to_canonical, compute_hash


def test_hash_is_deterministic():
    a = compute_hash("0" * 64, {"event_id": "x", "seq": 1, "op": "insert"})
    b = compute_hash("0" * 64, {"event_id": "x", "seq": 1, "op": "insert"})
    assert a == b


def test_hash_changes_when_prev_changes():
    a = compute_hash("0" * 64, {"seq": 1})
    b = compute_hash("1" * 64, {"seq": 1})
    assert a != b


def test_hash_changes_when_payload_changes():
    a = compute_hash("0" * 64, {"seq": 1, "op": "insert"})
    b = compute_hash("0" * 64, {"seq": 1, "op": "update"})
    assert a != b


def test_hash_payload_key_order_invariant():
    """Canonical serialization sorts keys — order should not change the hash."""
    a = compute_hash("0" * 64, {"seq": 1, "op": "insert"})
    b = compute_hash("0" * 64, {"op": "insert", "seq": 1})
    assert a == b


def test_chain_simulation():
    prev = "0" * 64
    seq = []
    for i in range(10):
        h = compute_hash(prev, {"seq": i, "data": f"event-{i}"})
        seq.append((prev, h))
        prev = h

    for i, (p, h) in enumerate(seq):
        recomputed = compute_hash(p, {"seq": i, "data": f"event-{i}"})
        assert recomputed == h


def test_hash_stable_across_datetime_roundtrip():
    """The key invariant: hash must be identical whether a datetime field
    appears as a Python datetime (pre-Mongo) or as an ISO string (post-Mongo
    BSON round-trip via Pydantic mode='json')."""
    dt = datetime(2026, 5, 21, 18, 0, tzinfo=timezone.utc)
    before_pre  = {"customer_name": "Alice", "updated_at": dt}
    before_post = {"customer_name": "Alice", "updated_at": dt.isoformat()}
    h_pre  = compute_hash("0" * 64, {"before": before_pre,  "seq": 1})
    h_post = compute_hash("0" * 64, {"before": before_post, "seq": 1})
    assert h_pre == h_post


def test_to_canonical_handles_nested_datetimes():
    dt = datetime(2026, 5, 21, 18, 0, tzinfo=timezone.utc)
    raw = {
        "after": {
            "status": "shipped",
            "created_at": dt,
            "nested": {"updated_at": dt, "tags": [dt, "x"]},
        }
    }
    canon = _to_canonical(raw)
    iso = dt.isoformat()
    assert canon["after"]["created_at"] == iso
    assert canon["after"]["nested"]["updated_at"] == iso
    assert canon["after"]["nested"]["tags"] == [iso, "x"]


def test_end_to_end_hash_then_verify_via_stored_doc():
    """Simulate the storage round-trip: build a payload (with datetime),
    hash it, store the canonicalized version, then verify by re-hashing
    only the HASHED_FIELDS extracted from the stored doc. This is exactly
    what /audit/verify does."""
    dt = datetime(2026, 5, 22, 18, 0, tzinfo=timezone.utc)
    canon = _to_canonical({
        "event_id": "ev-1",
        "seq": 1,
        "op": "insert",
        "intent": "new_order",
        "priority": "urgent",
        "order_id": "ord_x",
        "diff": [{"field": "status", "before": None, "after": "pending"}],
        "before": None,
        "after": {"customer_name": "Alice", "created_at": dt, "updated_at": dt},
        "ts": dt.isoformat(),
        "schema_version": 1,
        "correlation_id": "corr-1",
    })
    h = compute_hash("0" * 64, canon)

    stored_doc = {**canon, "prev_hash": "0" * 64, "hash": h, "resume_token": None}

    extracted = _hash_payload_from_doc(stored_doc)
    recomputed = compute_hash("0" * 64, extracted)
    assert recomputed == h
    assert set(extracted.keys()) == set(HASHED_FIELDS)


def test_chain_detects_tamper():
    prev = "0" * 64
    chain = []
    for i in range(5):
        h = compute_hash(prev, {"seq": i, "data": f"event-{i}"})
        chain.append({"prev": prev, "payload": {"seq": i, "data": f"event-{i}"}, "hash": h})
        prev = h

    chain[2]["payload"]["data"] = "tampered"
    recomputed = compute_hash(chain[2]["prev"], chain[2]["payload"])
    assert recomputed != chain[2]["hash"]
