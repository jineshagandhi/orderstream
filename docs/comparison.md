# Comparison: orderstream vs Production Real-Time Systems

This document compares the design choices in `orderstream` against the systems trading and consumer real-time platforms actually use, including documented production failures.

## Side-by-side

| Capability | Zerodha Kite | Firebase Realtime DB | Supabase Realtime | Pusher / Ably | orderstream |
| --- | --- | --- | --- | --- | --- |
| Push-based delivery | ✅ WebSocket | ✅ WebSocket | ✅ WebSocket | ✅ WebSocket | ✅ SSE |
| Field-level delta payloads | Partial (binary frames for ticks; full doc for orders) | ❌ Full doc | ❌ Full doc | ❌ Pub/sub of arbitrary payloads | ✅ Computed diff |
| Intent classification | ❌ | ❌ | ❌ | ❌ | ✅ |
| Priority-aware coalescing | ❌ All events equal | ❌ | ❌ | ❌ | ✅ |
| Burst write coalescing | ❌ Every state visible | ❌ | ❌ | ❌ | ✅ Cohesion Buffer |
| Reconnect replay with no gap | ❌ Events lost (per Kite forum) | Partial (subscribed-after-fact behavior) | ✅ for INSERT/UPDATE/DELETE | ✅ paid feature | ✅ `since_seq` cursor |
| Proactive system health signal | ❌ Discovered via frozen prices | ❌ | ❌ | ❌ | ✅ `system_health` event |
| Human-readable audit trail | Partial | ❌ | ❌ | ❌ | ✅ `/audit/order/{id}` |
| Tamper-evident audit | ❌ | ❌ | ❌ | ❌ | ✅ Hash chain |
| Per-client authorization at fan-out | ✅ (proprietary) | ✅ (rules) | ✅ (RLS) | ✅ (channels) | ✅ Subscription filter |
| Snapshot+tail without race | Unclear | Has documented race | Has documented race | N/A (pub/sub) | ✅ read-watermark |
| At-least-once + idempotent client | Unclear | At-most-once-ish | At-least-once | At-least-once | ✅ documented contract |
| Flow control / slow consumer eviction | Server-side terminates idle | ❌ | ❌ | ✅ (paid) | ✅ Bounded queue + 5-strike eviction |
| Horizontal scale | ✅ (proprietary infra) | ✅ (managed) | ✅ (managed) | ✅ (managed) | Documented path: Redis Pub/Sub fan-out |

## Documented production failures

### Zerodha Kite — Feb 2026

During a strong market rally, Kite suffered forced logouts, prices that wouldn't update, and portfolios showing wrong data. The pattern is consistent with:

- Thundering herd: market volatility caused a spike in active users; many simultaneously reconnected; the platform couldn't keep up.
- No degradation signal: users discovered the issue by watching prices stop changing. No client-side indicator showed the system was struggling.

How `orderstream` is designed against this:

- `system_health` SSE event pushed every 10s. Dashboard's connection pill turns amber/red on degraded/critical status — users see the warning before noticing data is stale.
- Snapshot endpoint can be cached at the HTTP layer (documented future work) to absorb reconnect bursts without hammering MongoDB.

### Kite WebSocket forum — recurring complaints

Excerpts (paraphrased) from Zerodha's developer forum:

> "If you open a WebSocket connection after placing an order, it is more likely that you would have missed that order update."

This is a structural admission that there's no replay on reconnect. Once a client disconnects (even briefly), events during that gap are lost.

How `orderstream` is designed against this:

- Every event has a monotonic `seq` and a persistent `event_id`.
- The dashboard caches the last received `event_id`; on reconnect, it calls `GET /events?since_event=…` to bridge the gap before resuming the SSE stream.
- Server-side, the spine never deletes events. Replay always succeeds (up to retention).

### Angel One — what scale looks like

Angel One's published architecture uses Apache Pinot for real-time OLAP, with Kafka as the event-streaming backbone. Trade state changes are emitted by the trading service to Kafka; Pinot ingests in near real-time for analytics. The trading engine never touches the analytics layer directly.

This is the right pattern at Angel One's scale. For `orderstream`'s scope it would be overkill, but the **decoupling principle** is preserved: the spine is the equivalent of the Kafka topic, the broker is the equivalent of consumer groups, and `/audit/verify` is the equivalent of an integrity check on the topic.

### Firebase Realtime DB — snapshot+tail race

Firebase clients first attach a listener (`on('value', …)`), and the SDK internally handles the snapshot+tail merge. But on poor network conditions, the snapshot read can race with the tail; documented community workarounds involve manual resubscription on suspicion of inconsistency.

How `orderstream` is designed against this:

- `GET /snapshot` returns both the current state AND `head_seq` atomically (well, head_seq is read first; any event after it will arrive via the stream).
- The client uses `head_seq` as `since_seq` for the SSE connection. No race, no gap.

### Pusher / Ably — vendor lock-in and cost

Pusher and Ably are managed pub/sub services. They work well, but:

- Pay-per-message — at trading volumes, cost scales with traffic.
- Vendor lock-in: switching pub/sub providers requires migrating channel topology, ACLs, and client SDKs.
- No CDC integration — you have to publish manually, which means the dual-write problem is your problem to solve.

`orderstream` is self-hosted, with no external dependencies beyond MongoDB. The CDC source is the same database that holds the orders, so there's no dual-write problem.

## What this comparison is for

Hiring managers don't read tables. They read the *thinking* behind a table. The point of this document is to show that:

1. Every design choice in `orderstream` corresponds to a documented failure or limit in a real system.
2. We knew what we were trading off — and named the trade.
3. The system is positioned where it matters: between a toy demo and a managed service, with the strengths of both.
