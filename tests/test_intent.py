from app.diff import compute_diff
from app.intent import classify
from app.models import Intent, Priority


def test_insert_is_new_order_urgent():
    intent, priority = classify("insert", None, {"status": "pending"}, compute_diff(None, {"status": "pending"}))
    assert intent == Intent.NEW_ORDER
    assert priority == Priority.URGENT


def test_delete_is_deletion_urgent():
    intent, priority = classify("delete", {"status": "shipped"}, None, compute_diff({"status": "shipped"}, None))
    assert intent == Intent.DELETION
    assert priority == Priority.URGENT


def test_status_to_cancelled_is_cancellation_urgent():
    before = {"status": "pending"}
    after  = {"status": "cancelled"}
    intent, priority = classify("update", before, after, compute_diff(before, after))
    assert intent == Intent.CANCELLATION
    assert priority == Priority.URGENT


def test_status_change_is_normal():
    before = {"status": "pending"}
    after  = {"status": "shipped"}
    intent, priority = classify("update", before, after, compute_diff(before, after))
    assert intent == Intent.STATUS_CHANGE
    assert priority == Priority.NORMAL


def test_price_change_is_price_correction_low():
    before = {"status": "pending", "price": 100}
    after  = {"status": "pending", "price": 110}
    intent, priority = classify("update", before, after, compute_diff(before, after))
    assert intent == Intent.PRICE_CORRECTION
    assert priority == Priority.LOW
