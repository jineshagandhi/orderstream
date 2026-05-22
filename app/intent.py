from typing import Any

from .models import Intent, Priority, INTENT_PRIORITY, DiffEntry


def classify(
    op: str,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    diff: list[DiffEntry],
) -> tuple[Intent, Priority]:
    """Classify a change into a business intent + priority.

    Order matters: cancellation must be detected BEFORE generic status_change.
    """
    if op == "insert":
        intent = Intent.NEW_ORDER
    elif op == "delete":
        intent = Intent.DELETION
    else:
        changed_fields = {d.field for d in diff}

        new_status = (after or {}).get("status")
        old_status = (before or {}).get("status")

        if "status" in changed_fields and new_status == "cancelled" and old_status != "cancelled":
            intent = Intent.CANCELLATION
        elif "status" in changed_fields:
            intent = Intent.STATUS_CHANGE
        elif "price" in changed_fields:
            intent = Intent.PRICE_CORRECTION
        else:
            intent = Intent.OTHER

    return intent, INTENT_PRIORITY[intent]
