"""CLI SSE listener — proves programmatic consumption works.

Usage:
    python client/cli_listener.py
    python client/cli_listener.py --intents cancellation,status_change
    python client/cli_listener.py --url http://localhost:8000/stream
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

import httpx


COLORS = {
    "new_order": "\033[32m",
    "status_change": "\033[36m",
    "cancellation": "\033[31m",
    "price_correction": "\033[33m",
    "deletion": "\033[35m",
    "other": "\033[37m",
}
RESET = "\033[0m"
DIM = "\033[2m"
BOLD = "\033[1m"


def render_event(ev: dict) -> str:
    intent = ev.get("intent", "other")
    color = COLORS.get(intent, RESET)
    seq = ev.get("seq", "?")
    op = ev.get("op", "?")
    order = ev.get("order_id", "?")
    ts = ev.get("ts", "")
    coalesced = ev.get("coalesced", False)
    count = ev.get("coalesced_count", 1)
    tag = f" {BOLD}×{count} coalesced{RESET}" if coalesced else ""

    diff_parts = []
    for d in ev.get("diff", []):
        diff_parts.append(f"{d['field']}: {DIM}{d.get('before')}{RESET} → {color}{d.get('after')}{RESET}")

    return (
        f"{DIM}#{seq:<5} {ts[:23]}{RESET}  "
        f"{color}{intent:<18}{RESET} "
        f"{op:<8} {order}{tag}\n"
        + ("".join(f"          {p}\n" for p in diff_parts) if diff_parts else "")
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000/stream")
    parser.add_argument("--intents", default=None, help="comma-separated intent filter")
    parser.add_argument("--customer", default=None)
    args = parser.parse_args()

    params: dict = {}
    if args.intents:  params["intents"] = args.intents
    if args.customer: params["customer"] = args.customer

    print(f"{BOLD}orderstream CLI listener{RESET}")
    print(f"connecting to {args.url} (filters={params or 'none'})")
    print("-" * 80)

    try:
        with httpx.Client(timeout=None) as client:
            with client.stream("GET", args.url, params=params, headers={"Accept": "text/event-stream"}) as r:
                event_name = "message"
                data_lines: list[str] = []
                for line in r.iter_lines():
                    if line == "":
                        if data_lines and event_name == "change":
                            try:
                                ev = json.loads("\n".join(data_lines))
                                print(render_event(ev), end="")
                            except Exception as e:
                                print(f"parse error: {e}", file=sys.stderr)
                        elif data_lines and event_name == "system_health":
                            try:
                                h = json.loads("\n".join(data_lines))
                                print(f"{DIM}[health] status={h['status']} clients={h['clients_connected']} lag={h['stream_lag_ms']}ms{RESET}")
                            except Exception:
                                pass
                        event_name = "message"
                        data_lines = []
                        continue
                    if line.startswith("event: "):
                        event_name = line[7:].strip()
                    elif line.startswith("data: "):
                        data_lines.append(line[6:])
    except KeyboardInterrupt:
        print("\nstopped.")
        return 0
    except httpx.RequestError as e:
        print(f"connection error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
