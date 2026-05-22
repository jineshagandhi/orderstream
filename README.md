# orderstream

[![tests](https://github.com/jineshagandhi/orderstream/actions/workflows/ci.yml/badge.svg)](https://github.com/jineshagandhi/orderstream/actions/workflows/ci.yml)
[![python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![license](https://img.shields.io/badge/license-MIT-green)](#)

**Real-time, audit-grade order event spine.**
MongoDB Change Streams → field-level diff → intent classification → tamper-evident hash chain → SSE **and** WebSocket fan-out.

Built for the Apt Interview Assignment (Atypical Technologies Pvt Ltd).

> 📺 **Live demo:** **https://orderstream.onrender.com** — click "Verify chain" to see cryptographic audit verification on a live, continuously-running instance
> 
> *Note on the free tier: the Render service sleeps after 15 minutes of inactivity. First request after sleep takes 30–60 seconds to wake up — subsequent requests are instant. This is a free-tier behavior, not a system characteristic.*

> Naive real-time systems tell clients *something* changed.
> This system tells them *what* changed, *why it matters*, *proves it changed* via a cryptographic chain, and *tells them when the system itself is struggling* — designed against the failure patterns documented in Zerodha Kite's incident reports and aligned with SEBI's 2025 algo trading audit requirements.

---

## The 60-second pitch

The assignment asks for real-time DB-to-client updates. The naive implementation is straightforward: open a MongoDB Change Stream and broadcast every change to every client.

A senior engineer asks the harder questions:

1. **What does each client actually need to know?** — Broadcasting full documents at scale is waste. This system emits **field-level diffs**.
2. **Are all changes equal?** — A cancellation is not the same urgency as a price update. This system **classifies intent** and applies **priority-aware coalescing**: cancellations are emitted immediately; rapid status churn is collapsed into one logical event.
3. **What happens on reconnect?** — Zerodha Kite's own forum confirms order updates are lost if a WebSocket reconnects after the order was placed. This system records every event with a monotonic `seq` and exposes a **replay endpoint** so reconnecting clients catch up exactly.
4. **What does "real-time" even mean when the system is degraded?** — When Kite's prices froze in February 2026, users had no indication. This system pushes a periodic **`system_health` event** into the stream itself so clients can warn before users notice.
5. **Can you prove the audit trail wasn't tampered with?** — SEBI's 2025 framework mandates immutable order audit trails. Every event in `orderstream` is **hash-chained** (SHA-256 of `prev_hash + payload`) — `/audit/verify` cryptographically validates the entire chain.

These five questions are why this submission is different.

---

## How this maps to APT's JD

| APT JD requirement | orderstream feature |
| --- | --- |
| "scalable RESTful APIs and WebSocket services for real-time trade data" | REST + SSE + WebSocket endpoints (`/stream`, `/ws`, `/orders`, `/snapshot`, `/events`) |
| "low-latency order routing, kill-switch mechanisms" | Priority-aware delivery lanes; `POST /admin/kill-switch` engages a platform-wide broadcast halt while preserving the audit trail |
| "unique Algo ID tagging, audit trails… per SEBI 2025 norms" | Every order carries `algo_id` + `broker_id`; every event is recorded in a SHA-256 hash-chained spine; `/audit/verify` proves chain integrity |
| "real-time market feeds, order books, and OHLCV data" | Field-level CDC with intent classification — the same shape as production tick/order feeds |
| "99.9%+ uptime during market hours" | Watchdog + reconciliation; bounded per-client queues prevent slow-consumer OOM; proactive `system_health` event signals degradation |
| "data pipelines for real-time market feeds" | Single-writer Watcher with causal ordering per `order_id`, intent-aware Temporal Cohesion Buffer |
| "Python and/or Node.js" | Python 3.11 + FastAPI + Motor (async MongoDB) + Pydantic v2 |

## What you'll see

Open the dashboard at `http://localhost:8000` and you get:

- **Left panel** — live order grid that updates as the database changes. Rows flash cyan on update.
- **Right panel** — event stream showing intent badges, field-level diffs, and the hash of each event.
- **Header** — connection status, clients connected, events/60s, stream lag, current seq.
- **Bottom strip** — system health, uptime, watcher state, head hash, lag sparkline.
- **Verify chain button** — cryptographic verification of the full audit chain.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  MongoDB rs0 — orders collection                        │
│  insert / update / delete → Change Stream               │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  Watcher (single writer, async Python)                  │
│  • captures resume_token on every event                 │
│  • computes field-level diff against in-memory cache    │
│  • classifies intent: new_order | status_change |       │
│    cancellation | price_correction | deletion           │
│  • watchdog + reconcile-from-snapshot on stream death   │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  Event Spine (event_log, append-only, hash-chained)     │
│  • event_id, seq, op, intent, priority, order_id        │
│  • diff, before, after, ts, schema_version              │
│  • prev_hash, hash (SHA-256 chain)                      │
│  • outbox: source of truth for downstream broadcast     │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  Cohesion Buffer (intent-aware)                         │
│  • cancellation     → 0ms (urgent, bypass)              │
│  • new_order        → 0ms (urgent, bypass)              │
│  • status_change    → 50ms window                        │
│  • price_correction → 200ms window                       │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  SSE Broker                                             │
│  • per-client bounded queue (flow control)              │
│  • per-client intent + customer filter                  │
│  • slow-consumer eviction (5-strike rule)               │
│  • periodic system_health event                         │
│  • heartbeat every 15s                                  │
└─────────────────────────────────────────────────────────┘
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
       Browser UI    Python CLI    GET /events?since_seq=…
        (dashboard)    (listener)    (catch-up replay)
```

---

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Dashboard UI |
| `GET` | `/stream?intents=…&customer=…` | **SSE** live feed (server-side filtered) |
| `WS` | `/ws?intents=…&customer=…` | **WebSocket** live feed (parallel to SSE) |
| `GET` | `/snapshot` | Atomic current-state + head_seq (solves snapshot+tail race) |
| `GET` | `/events?since_seq=N` | Replay missed events after reconnect |
| `GET` | `/events?since_event=<uuid>` | Same, by event_id |
| `GET` | `/health` | Ops metrics |
| `GET` | `/audit/verify` | Walk the hash chain, return integrity report |
| `GET` | `/audit/order/{id}` | Full lifecycle of one order |
| `GET` | `/admin/kill-switch` | Read kill-switch state |
| `POST` | `/admin/kill-switch` | Engage / release the platform kill-switch |
| `GET` | `/orders` | List orders (test helper) |
| `POST` | `/orders` | Create order (triggers Change Stream) — accepts `algo_id`, `broker_id` |
| `PATCH` | `/orders/{id}` | Update order |
| `DELETE` | `/orders/{id}` | Delete order |

---

## Quick start

### Prerequisites

- **Python 3.11+**
- **Docker Desktop** (for MongoDB replica set) — or **MongoDB Atlas** (free tier works)

### 1. Clone & install

```powershell
cd "C:\Users\JINESHA GANDHI\orderstream"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Configure

```powershell
Copy-Item .env.example .env
```

The default `.env` points to a local Docker MongoDB. To use Atlas instead, set `MONGO_URI` to your Atlas connection string (it must include a replica set — Atlas always does).

### 3. Start MongoDB

**Option A — Docker (recommended for local):**

```powershell
docker compose up -d
# wait ~10 seconds for the replica set to self-initialize
python scripts\init_replica.py     # fallback in case the healthcheck didn't catch it
```

**Option B — MongoDB Atlas:** create a free M0 cluster, whitelist your IP, copy the connection string into `.env`. No further setup needed.

### 4. Run the app

```powershell
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

You should see:

```
orderstream ready on http://0.0.0.0:8000
```

### 5. Open the dashboard

Open `http://localhost:8000` in your browser. The dashboard loads but is empty.

### 6. Seed + simulate

In a second terminal (with venv active):

```powershell
python scripts\seed.py             # creates ~20 orders
python scripts\simulator.py        # continuous traffic
```

Watch the dashboard light up. You'll see new orders flowing in, status changes coalescing, and occasional cancellations bypassing the buffer.

### 7. Optional — CLI listener

In a third terminal:

```powershell
python client\cli_listener.py
python client\cli_listener.py --intents cancellation,status_change
```

### 8. Run tests

```powershell
pytest
```

---

## Deploy to Render (free tier, ~5 minutes)

The repo includes a [`render.yaml`](./render.yaml) blueprint and a production [`Dockerfile`](./Dockerfile).

1. **Create a free MongoDB Atlas cluster.** Visit [mongodb.com/cloud/atlas/register](https://www.mongodb.com/cloud/atlas/register), create an M0 cluster, set `0.0.0.0/0` in Network Access, create a DB user, and copy the SRV connection string.
2. **Sign in to Render.** Connect this GitHub repo at [dashboard.render.com](https://dashboard.render.com/select-repo?type=blueprint). Render detects `render.yaml` automatically.
3. **Set the `MONGO_URI` env var** in the Render service page to your Atlas connection string.
4. **Deploy.** Render builds the Docker image and runs it. The `/health` endpoint is the readiness probe.

That's it — your live URL serves the dashboard at the root. Add it to the top of this README.

## Design decisions — and why

### Why MongoDB Change Streams (not polling, not triggers)

Change Streams are MongoDB's native CDC primitive. They are push-based, durable (backed by the oplog), and resumable via `resume_token`. Polling violates the assignment's stated constraint. Application-level triggers would only catch changes from this service — out-of-band writes (admin tools, other services) would be invisible. Change Streams catch every write to the database, regardless of source.

### Why SSE *and* WebSocket (both)

This is a one-directional server-push problem. SSE is the right primitive:

- Rides HTTP/1.1, trivially load-balanced.
- Native auto-reconnect in the browser via `EventSource`.
- Simpler protocol, easier to debug with curl.

But Indian broker terminals (Kite Ticker, Angel SmartAPI) speak WebSocket, so a downstream consumer building on this would expect a WebSocket endpoint too. We expose both — they share the same broker, filter, and replay machinery:

```js
// Browser-side WebSocket consumer
const ws = new WebSocket("ws://localhost:8000/ws?intents=cancellation");
ws.onmessage = (e) => {
  const msg = JSON.parse(e.data);
  if (msg.type === "change") console.log("event:", msg.event);
};
```

Trade-off: SSE has a 6-connections-per-domain browser limit. For dashboards in multiple tabs, a `SharedWorker` would be the production fix; for this submission it's documented but not implemented.

### Why field-level diffs (not full documents)

A naive system at 1000 connected clients with ~10 events/sec broadcasts ~5 MB/s. With deltas, the same load is ~150 KB/s — **a 30× bandwidth reduction**. This is how every production market data feed (NSE, BSE, Bloomberg) works.

### Why intent classification

Different changes have different urgency. A cancellation must reach the client immediately — delaying it by 50ms in the cohesion buffer is a trading risk. A price correction tolerates 200ms. The classifier maps raw DB ops into business-level intents, and the buffer routes each intent to its priority lane.

### Why the Temporal Cohesion Buffer

An order can transition `pending → validating → risk_check → queued → executed` in under 100ms — five physical writes for one logical event. Without coalescing, every client receives five intermediate states and the UI flickers. With a 50ms window per order, the same activity emits one canonical event with `coalesced × 5` metadata. This is how production trading feed consumers (e.g., Refinitiv, Bloomberg) actually behave.

### Why the hash-chained Event Spine

The SEBI 2025 algo trading framework mandates an immutable audit trail for every order lifecycle event. A hash chain provides cryptographic proof of integrity:

```
hash_i = SHA-256(hash_{i-1} || canonical_json(payload_i))
```

Any retroactive edit to event `k` invalidates the hash of `k`, `k+1`, …, all detectable in O(n) by `/audit/verify`. No external blockchain or HSM required.

The spine also doubles as the source of truth for the broadcast layer (the **outbox pattern**): the watcher persists to the spine first, then the broker broadcasts from the spine. On restart, the broker resumes from the last broadcast `seq` and replays anything in between. No event is ever broadcast without being recorded, and no event is ever recorded without being broadcastable.

### Kill-switch — preserve audit trail, halt fan-out

The JD references kill-switch mechanisms — a safety primitive every regulated trading platform needs. Engage from any terminal:

```powershell
curl -X POST http://localhost:8000/admin/kill-switch -H "Content-Type: application/json" -d '{"reason":"NSE feed anomaly"}'
```

When engaged: the watcher keeps writing to the spine (audit trail is sacred), but broker fan-out is suppressed. Clients receive a `system_health` event with `kill_switch_engaged: true`. Release with `{"action":"release"}` — clients can then call `/events?since_seq=<last>` to bridge the suppression window with zero data loss.

### Why `system_health` is an event, not just an endpoint

Zerodha Kite's February 2026 incident is the case study: prices froze, portfolios showed wrong values, but users had no indication anything was wrong. They figured it out by watching values stop changing.

Pushing a `system_health` SSE event every 10 seconds means clients can render a **"data may be stale"** indicator the moment the watcher reports trouble — before the user notices. The dashboard's connection pill turns amber/red on `status: degraded`/`critical`.

### Why snapshot+tail with a read-watermark

The hardest race in CDC systems: a new client wants (a) the current state of all orders and (b) a cursor from which to receive future updates, with no gap and no overlap. Firebase, Supabase, and Hasura all have documented bugs around this.

The fix: capture `head_seq` from the spine **before** reading the orders collection. Any event published between steps 1 and 2 will have `seq > head_seq`, so the client receives it via the stream. Any event before step 1 is reflected in the snapshot. `GET /snapshot` returns both atomically.

### Why authorization lives at the broadcast layer

Change Streams have no concept of identity. They emit every change on the watched collection. Authorization cannot live in the database — it lives in the broker, evaluated per-client per-event. A client subscribed with `?customer=Alice` only sees events whose document is Alice's order. This means privacy is enforced even though the underlying CDC mechanism is global.

### Why at-least-once, not exactly-once

Exactly-once delivery is a distributed-systems myth. The honest contract: every event has a unique `event_id` (UUID4), clients deduplicate by it, and the server may redeliver on reconnect (especially via `/events?since_seq=…`). This is the contract Kafka, Pulsar, and every serious event system actually offers — anything else is hand-waving.

---

## What this system does NOT solve (and when you'd need to)

Listing limits honestly is more impressive than pretending they don't exist:

1. **Single watcher = single point of failure.** Fine below ~10k events/sec. Above that, partition by `hash(order_id) % N` across N watchers.
2. **In-memory broker = single-process fan-out.** Fine for one Uvicorn worker / ~1–5k SSE clients. Beyond that, place Redis Pub/Sub between the watcher and N stateless broker instances; each broker subscribes and fans out to its local clients. (See `docs/architecture.md` for the scale-out path.)
3. **`event_log` grows unbounded.** Fine for assignment scope. Production needs log compaction (snapshot + TTL) or hot/cold tiering.
4. **No multi-region.** Cross-region replication of Change Streams introduces oplog lag and conflict resolution. Out of scope.
5. **SSE browser tab limit.** A user with 7+ tabs hits the per-domain connection limit. `SharedWorker` is the production fix.
6. **`fullDocumentBeforeChange` requires MongoDB 6+ pre-images.** We sidestep this by maintaining an in-memory `before` cache. Trade-off: on watcher restart, the first event for an order will lack `before` until cache is repopulated.

---

## Tests

```powershell
pytest -v
```

Covers diff merging, intent classification, hash determinism + tamper detection, cohesion buffer urgent-bypass and coalescing semantics.

---

## File map

```
orderstream/
├── docker-compose.yml          # MongoDB rs0 with healthcheck auto-init
├── requirements.txt
├── pytest.ini
├── .env.example
├── app/
│   ├── main.py                 # FastAPI entry + lifecycle
│   ├── config.py               # Pydantic settings
│   ├── db.py                   # Motor client + index management
│   ├── models.py               # Pydantic event/order schemas
│   ├── diff.py                 # Field-level diff + merge
│   ├── intent.py               # Intent classifier
│   ├── cohesion.py             # Intent-aware Temporal Cohesion Buffer
│   ├── event_spine.py          # Hash-chained append-only log
│   ├── broker.py               # SSE fan-out + flow control
│   ├── watcher.py              # Change Stream consumer + watchdog
│   ├── health.py               # Health state + periodic emitter
│   ├── auth.py                 # Per-client subscription filter
│   └── routes/
│       ├── stream.py           # GET /stream
│       ├── snapshot.py         # GET /snapshot
│       ├── events.py           # GET /events
│       ├── health.py           # GET /health
│       ├── audit.py            # GET /audit/verify, /audit/order/{id}
│       └── orders.py           # CRUD
├── client/
│   ├── dashboard.html          # Live dashboard
│   └── cli_listener.py         # Terminal SSE client
├── scripts/
│   ├── init_replica.py         # Replica set init (fallback)
│   ├── seed.py                 # Initial demo state
│   └── simulator.py            # Continuous realistic traffic
├── tests/                      # pytest suite
└── docs/
    ├── architecture.md         # Deep dive
    ├── failure_modes.md        # What can go wrong + design responses
    └── comparison.md           # vs Zerodha, Firebase, Supabase, Pusher
```

---

## Author

Jinesha Gandhi · MIT World Peace University, Pune
[github.com/jineshagandhi](https://github.com/jineshagandhi) · jineshagandhi2020@gmail.com
