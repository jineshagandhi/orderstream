from app.diff import compute_diff, merge_diffs
from app.models import DiffEntry


def test_diff_insert_emits_every_field():
    after = {"customer_name": "Alice", "status": "pending", "price": 100.0}
    out = compute_diff(None, after)
    assert {d.field for d in out} == {"customer_name", "status", "price"}
    for d in out:
        assert d.before is None
        assert d.after == after[d.field]


def test_diff_delete_emits_every_field():
    before = {"customer_name": "Bob", "status": "shipped", "price": 50.0}
    out = compute_diff(before, None)
    assert {d.field for d in out} == {"customer_name", "status", "price"}
    for d in out:
        assert d.after is None
        assert d.before == before[d.field]


def test_diff_update_only_changed_fields():
    before = {"customer_name": "Alice", "status": "pending", "price": 100.0}
    after  = {"customer_name": "Alice", "status": "shipped", "price": 100.0}
    out = compute_diff(before, after)
    assert len(out) == 1
    assert out[0].field == "status"
    assert out[0].before == "pending"
    assert out[0].after == "shipped"


def test_diff_ignores_internal_fields():
    before = {"_id": "x", "updated_at": "t1", "status": "a"}
    after  = {"_id": "x", "updated_at": "t2", "status": "b"}
    out = compute_diff(before, after)
    assert len(out) == 1
    assert out[0].field == "status"


def test_merge_diffs_collapses_to_canonical_before_after():
    initial = [DiffEntry(field="status", before="pending", after="validating")]
    latest  = [DiffEntry(field="status", before="validating", after="shipped")]
    merged = merge_diffs(initial, latest)
    assert len(merged) == 1
    assert merged[0].before == "pending"
    assert merged[0].after == "shipped"


def test_merge_diffs_drops_noop():
    initial = [DiffEntry(field="status", before="pending", after="shipped")]
    latest  = [DiffEntry(field="status", before="shipped", after="pending")]
    merged = merge_diffs(initial, latest)
    assert merged == []
