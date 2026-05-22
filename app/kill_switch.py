"""Platform kill-switch — modeled on SEBI 2025's required emergency-stop primitive.

When engaged:
- All broadcasts are SUPPRESSED at the broker fan-out layer.
- The watcher KEEPS writing to the event spine — audit trail is never broken.
- Clients receive a `system_health` event indicating the kill-switch is engaged.
- /events?since_seq=… continues to work — operators can inspect the chain.

When disengaged:
- Broadcasts resume immediately.
- Connected clients receive a `kill_switch_released` event.
- Catch-up replay via /events bridges the suppression window.

This is exactly the kill-switch contract APT's JD references: a safe pause
that preserves the audit trail and supports operator-controlled recovery.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class KillSwitchState:
    engaged: bool = False
    reason: str | None = None
    engaged_at: float | None = None
    engaged_by: str | None = None

    def engage(self, reason: str, by: str = "operator") -> None:
        self.engaged = True
        self.reason = reason
        self.engaged_at = time.time()
        self.engaged_by = by

    def release(self) -> None:
        self.engaged = False
        self.reason = None
        self.engaged_at = None
        self.engaged_by = None

    def to_dict(self) -> dict:
        return {
            "engaged": self.engaged,
            "reason": self.reason,
            "engaged_at": self.engaged_at,
            "engaged_by": self.engaged_by,
            "engaged_for_seconds": int(time.time() - self.engaged_at) if self.engaged_at else None,
        }


KILL_SWITCH = KillSwitchState()
