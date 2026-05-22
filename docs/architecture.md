# Architecture Deep Dive

This document explains the *why* behind each component. The README covers *what* exists; this covers *why* it was designed that way.

## The pipeline, end to end

```
client write
   │
   ▼
orders collection            ← source of truth for current state
   │
   ▼  (oplog)
MongoDB Change Stream        ← exactly-once delivery semantics from the DB
   │
   ▼
Watcher (single writer)
   ├─ resume_token captured
   ├─ before/after resolved from in-memory cache
   ├─ field-level diff computed
   ├─ intent + priority classified
   │
   ▼
Event Spine (append-only)    ← source of truth for emitted events
   ├─ prev_hash + payload → SHA-256 → hash
   ├─ persisted to event_log
   │
   ▼
Cohesion Buffer (intent-aware)
   ├─ urgent → bypass
   ├─ normal → 50ms coalescing window per order_id
   ├─ low    → 200ms window
   │
   ▼
SSE Broker (in-memory)
   ├─ per-client bounded queue
   ├─ per-client subscription filter
   ├─ slow-consumer eviction
   │
   ▼
Clients (browser, CLI, downstream service)
```

## Why the spine is the source of truth, not the change stream

Naive systems broadcast directly from the change stream. This creates the **dual-write problem**: the broadcast and any logging are two separate operations that can succeed independently.

Failure cases:

| Scenario | Effect |
| --- | --- |
| Broadcast OK, log fails | No audit record. |
| Log OK, broadcast fails | Clients miss the event. |
| Both OK but order swapped | Client receives an event whose log entry doesn't yet exist; `/replay` returns nothing. |

The **outbox pattern** fixes this: persist to a durable log first, broadcast from that log. We use the `event_log` collection as the outbox. Replay queries hit the same source as live broadcast. The system is **consistent under failure**.

## Why the hash chain matters

Hash chaining was chosen over alternatives:

| Approach | Pros | Cons |
| --- | --- | --- |
| Plain log | Simple | Can be silently edited by anyone with DB write access |
| Append-only enforced via DB perms | Strong if perms hold | Defense-in-depth fails the moment perms drift |
| Hash chain | Tamper-evident — single edit invalidates everything downstream | Reads must walk chain to verify (acceptable cost; verification is rare) |
| External blockchain | Tamper-evident + decentralized | Latency, cost, operational complexity unjustified for an internal audit trail |
| Trusted-timestamp service (RFC 3161) | Cryptographic notarization | Requires an external authority |

For SEBI 2025 compliance — where the requirement is *immutable audit trail* — a hash chain is the right primitive. It's mathematically the same guarantee as a single-chain blockchain without any of the operational baggage.

`/audit/verify` walks the chain from genesis, recomputes each hash, and reports the first break (if any). On a healthy database with ~100k events, verification takes well under a second.

## Why the cohesion buffer is intent-aware, not time-aware only

A flat 50ms window applied to *all* events would delay cancellations by 50ms. In trading, that's a real risk: a cancellation that doesn't reach the position management system in time can mean a position is held longer than intended.

The intent classifier maps raw DB operations to business intents, and the buffer routes each intent to its priority lane:

| Intent | Window | Rationale |
| --- | --- | --- |
| `cancellation` | 0ms | Risk-critical; never delay |
| `new_order` | 0ms | Triggers downstream processing; never delay |
| `deletion` | 0ms | Treated like cancellation for safety |
| `status_change` | 50ms | UI flicker prevention dominates |
| `price_correction` | 200ms | Tolerates more batching; less time-sensitive |
| `other` | 100ms | Conservative default |

These windows are configurable via environment (`COHESION_WINDOW_STATUS`, `COHESION_WINDOW_PRICE`, `COHESION_WINDOW_DEFAULT`).

## Why we maintain an in-memory `before` cache

MongoDB 6+ supports `fullDocumentBeforeChange` if the collection has pre-images enabled. We don't rely on this, for two reasons:

1. **Portability.** Pre-images require a specific feature configuration on the collection. We want to work against any rs0-capable cluster (including Atlas free tier).
2. **Cost.** Pre-images double the oplog size. For high-write workloads this is non-trivial.

Instead, we warm a cache (`{order_id: latest_doc}`) on startup and update it on every event. Diffs are computed against this cache.

Trade-off: if the watcher process restarts, the cache is rebuilt from the current state of the orders collection. The first event for any given order after restart may lack a `before` field if it's an update — but the diff is still correct because we resolve `before` from cache, and the cache reflects pre-restart state.

## Why a single-writer watcher

The change stream is consumed by exactly one watcher per partition. This guarantees:

- **Causal ordering per order.** Events for the same order arrive in the order they were written, because oplog ordering is preserved within a single consumer.
- **Atomic spine appends.** With one writer, `seq` is monotonic without any coordination — `_last_seq + 1` is correct under an `asyncio.Lock`. With multiple writers we'd need a distributed counter.

For scale-out, we partition the order space (`hash(order_id) % N`) and run N watchers, each consuming a filtered change stream. Within each partition, single-writer semantics still hold.

## Why backpressure is enforced per client

Without backpressure, one slow client on a poor network connection can monopolize server memory. Node.js has documented production outages from exactly this — `res.write()` returns false and is ignored, the buffer grows unbounded, the process eventually OOMs.

Our broker:

1. Each client has a bounded `asyncio.Queue(maxsize=200)`.
2. `put_nowait()` raises `QueueFull` when the client is too slow.
3. We count drops per client. After 5 drops, the client is forcibly disconnected.
4. The drop is recorded in metrics (`/health`) so degradation is visible.

This is honest, observable backpressure. The alternative — silently dropping events — is the worst failure mode in real-time systems because no one knows it happened.

## Scale-out path (when needed)

Below ~5,000 concurrent SSE clients, a single Uvicorn worker is sufficient. Beyond that:

```
            Watcher (1 process per partition)
                │
                ▼
            Redis Pub/Sub channel "events"
                │
        ┌───────┼───────┐
        ▼       ▼       ▼
     Broker  Broker  Broker     (N stateless instances)
        │       │       │
        ▼       ▼       ▼
     clients clients clients
```

Each broker subscribes to the Redis channel and fans out to its local SSE clients. Brokers can be horizontally scaled behind a load balancer with **sticky sessions disabled** — because brokers are stateless, any broker can serve any client, and the Redis channel ensures every broker sees every event.

`event_log` remains the persistent source of truth. Replay queries (`/events?since_seq=…`) hit `event_log` directly and are independent of which broker serves them.

## Graceful shutdown

On `SIGTERM`:

1. `lifespan` exit triggers.
2. The `system_health` emitter task is cancelled.
3. The watcher's `_stop` event is set; in-flight change stream consumption finishes cleanly.
4. The cohesion buffer is flushed: any pending coalesced events are emitted immediately.
5. The Motor client is closed.

This prevents the case where a deployment loses events that were buffered but not yet flushed.

## Concurrency model

Everything is `asyncio`-native. There are no threads, no multiprocessing. This is correct for:

- **I/O bound work.** Mongo, SSE, HTTP — all I/O. Python's GIL is not a bottleneck.
- **Single-writer guarantees.** The watcher is a single coroutine; the spine's `_last_seq + 1` increment is protected by an `asyncio.Lock`.

For CPU-bound work (which this system has none of — diff and hash are negligible), we'd move that to a thread pool. Not needed here.
