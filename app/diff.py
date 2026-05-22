from typing import Any

from .models import DiffEntry


IGNORED_FIELDS = {"_id", "updated_at"}


def compute_diff(before: dict[str, Any] | None, after: dict[str, Any] | None) -> list[DiffEntry]:
    """Field-level diff. Returns only changed fields.

    Insert: before=None, after=doc -> every field shows as new.
    Delete: before=doc, after=None -> every field shows as removed.
    Update: only changed fields appear.
    """
    if before is None and after is None:
        return []

    if before is None:
        return [
            DiffEntry(field=k, before=None, after=v)
            for k, v in (after or {}).items()
            if k not in IGNORED_FIELDS
        ]

    if after is None:
        return [
            DiffEntry(field=k, before=v, after=None)
            for k, v in before.items()
            if k not in IGNORED_FIELDS
        ]

    out: list[DiffEntry] = []
    keys = (set(before.keys()) | set(after.keys())) - IGNORED_FIELDS
    for k in sorted(keys):
        b = before.get(k)
        a = after.get(k)
        if b != a:
            out.append(DiffEntry(field=k, before=b, after=a))
    return out


def merge_diffs(initial: list[DiffEntry], latest: list[DiffEntry]) -> list[DiffEntry]:
    """When coalescing N updates, the effective diff is from the initial 'before' to the latest 'after'.
    Caller passes the diff(before=window_initial, after=event1.after) and the latest event's full diff.
    This merges to one canonical diff per field.
    """
    by_field: dict[str, DiffEntry] = {}
    for entry in initial:
        by_field[entry.field] = entry
    for entry in latest:
        if entry.field in by_field:
            by_field[entry.field] = DiffEntry(
                field=entry.field, before=by_field[entry.field].before, after=entry.after
            )
        else:
            by_field[entry.field] = entry
    return [e for e in by_field.values() if e.before != e.after]
