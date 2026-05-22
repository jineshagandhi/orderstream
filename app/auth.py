"""Authorization filter, evaluated at the broadcast layer per client per event.

Change Streams have no concept of identity, so authorization cannot live in
the database. It lives here, between the event spine and the SSE fan-out.

This implementation supports a simple subscription scope:
- intents: which intent types the client cares about
- customer: limit to one customer's orders (None = all)

In production this would extend to per-role/per-tenant rules.
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import Event, Intent


@dataclass(frozen=True)
class Subscription:
    intents: frozenset[Intent] | None = None  # None = all
    customer: str | None = None  # None = all

    def matches(self, event: Event) -> bool:
        if self.intents is not None and event.intent not in self.intents:
            return False
        if self.customer is not None:
            doc = event.after or event.before or {}
            if doc.get("customer_name") != self.customer:
                return False
        return True

    @classmethod
    def parse(cls, intents_param: str | None, customer_param: str | None) -> "Subscription":
        intents = None
        if intents_param:
            parts = [p.strip() for p in intents_param.split(",") if p.strip()]
            try:
                intents = frozenset(Intent(p) for p in parts)
            except ValueError:
                intents = None
        return cls(intents=intents, customer=customer_param or None)
