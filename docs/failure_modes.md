# Failure Modes & Design Responses

A working real-time system isn't one that works on a happy path. It's one that fails *loudly* and *recoverably*. This document enumerates every failure mode we've considered and how the design responds.

## Failure mode catalog

### F1. Change Stream dies

**Causes:** primary stepdown during replica set election; network blip; oplog rollover; cursor timeout on memory pressure; long GC pause in the watcher.

**Symptom if unhandled:** stream silently stops delivering. The watcher process is still alive, the SSE broker is still alive, but no events flow. This is the worst failure mode: *invisible incorrectness*.

**Our response:**
- The watcher runs `_consume_change_stream()` inside a `_run_forever()` loop with exponential backoff.
- `PyMongoError` is caught; we log, mark `HEALTH.watcher_alive = False`, and restart.
- The dashboard's `system_health` indicator turns critical the moment `watcher_alive` flips false — operators see it before clients notice missing events.

### F2. Resume token invalid

**Cause:** the watcher was offline longer than the oplog retention window; old oplog entries have been overwritten.

**Symptom if unhandled:** `resume_after` throws `ChangeStreamHistoryLost`. The watcher cannot resume.

**Our response:**
- On `PyMongoError`, we call `_reconcile_or_reset()`.
- Reconciliation re-reads the entire `orders` collection into the cache.
- Any orders present now that weren't in the previous cache emit a synthetic `new_order` event with `correlation_id="reconciliation"`.
- The resume token is cleared so the next loop opens a fresh stream from "now."
- Clients receive synthetic events. Combined with their existing snapshot, they reconverge on the correct state.

### F3. Outbox dual-write inconsistency

**Cause:** the spine append and the broker broadcast are two operations. Network or process death between them creates inconsistency.

**Symptom if unhandled:** clients see an event that isn't in the log, or vice versa.

**Our response:**
- Spine append happens BEFORE broker broadcast. Both happen inside the same watcher task, no network between them.
- If the watcher crashes after spine append but before broadcast, on restart the broker's broadcast position lags spine's `last_seq`. (Future work: persist broadcast cursor.)
- Clients reconnecting via `/events?since_seq=…` always read from the spine, never from in-memory broker state.

### F4. Slow consumer memory leak

**Cause:** a client on a slow network connection stops draining its SSE buffer. Server keeps writing.

**Symptom if unhandled:** Node.js-style production OOM. The infamous "we deployed and the box ran out of memory" outage.

**Our response:**
- Each client gets a bounded `asyncio.Queue(maxsize=200)`.
- `put_nowait()` raises `QueueFull` rather than blocking the broadcast loop.
- After 5 cumulative drops, the client is forcibly disconnected.
- Dropped count is exposed in `/health` and the `system_health` SSE event.

### F5. Thundering herd on restart

**Cause:** the app restarts. All connected SSE clients reconnect simultaneously. Each calls `/snapshot` and `/events?since_seq=…`. MongoDB sees a burst.

**Symptom if unhandled:** DB overload at restart precisely when the system is most vulnerable.

**Our response (partial):**
- The dashboard's reconnect handler waits 1.5s before reconnecting — small jitter, but a start.
- `EventSource` reconnect timing is browser-controlled, but the deliberate delay in the snapshot+replay path provides natural smoothing.
- Production-grade fix: serve `/snapshot` from a short-lived in-memory cache (~5s TTL) so 1000 simultaneous reconnects produce one DB read. Documented but not implemented.

### F6. Clock skew between Mongo and the app

**Cause:** wall-clock time on the Mongo server and the application server differ by hundreds of milliseconds (clock drift, NTP misconfiguration).

**Symptom if unhandled:** wall-clock timestamps in events lie. `since=<timestamp>` queries miss events or replay duplicates.

**Our response:**
- We use the monotonic `seq` field as the canonical cursor, not wall-clock time.
- `seq` is incremented by exactly one writer (the watcher) and is purely application-internal.
- `ts` is informational only — it's the watcher's wall-clock view, used for display.
- For cross-database ordering we'd switch to MongoDB's `clusterTime`, which is captured in every event but not currently used as a cursor.

### F7. Ordering across orders

**Cause:** events for different orders can arrive in any order in the change stream consumer if processing is parallelized. Even with a single watcher, downstream consumers may apply events out of order.

**Symptom if unhandled:** a client sees order #100 update before order #99, even though #99 was written first.

**Our response:**
- We provide **causal ordering per `order_id`** but not **total ordering** across orders.
- Causal ordering is enforced by the single-writer property: one watcher, one async loop, one `seq` counter.
- For consumers that need total ordering, sort received events by `seq` (monotonic across the entire system).

### F8. Browser SSE tab limit

**Cause:** HTTP/1.1 allows 6 connections per domain. A user with 7+ open tabs on the dashboard hits the limit. The 7th tab's SSE connection silently hangs.

**Symptom if unhandled:** real-time updates appear broken in some tabs but not others.

**Our response:**
- Documented limitation. The production fix is a `SharedWorker` that holds one SSE connection per origin and broadcasts to all tabs via `postMessage`.
- Not implemented in the assignment scope.

### F9. SSE buffering through HTTP/2 proxies

**Cause:** Some HTTP/2 proxies (Cloudflare default, some corporate proxies) buffer SSE frames and flush in batches. This destroys the real-time property.

**Symptom if unhandled:** events arrive in bursts every few seconds rather than as they happen.

**Our response:**
- `EventSourceResponse` from `sse-starlette` includes appropriate `X-Accel-Buffering: no` and `Cache-Control: no-cache` headers.
- For Nginx, the documented setup is `proxy_buffering off` and `proxy_read_timeout` extended.
- For Cloudflare, SSE works correctly when "Caching: Bypass" is configured for the `/stream` path.

### F10. `event_log` grows unbounded

**Cause:** every change appends a document. At 100 events/sec for 30 days, that's 260 million documents.

**Symptom if unhandled:** database bloat, slower queries, increased backup size.

**Our response:**
- Documented limitation. Production options:
  - **TTL index** on the `ts` field with retention matching audit requirements (e.g., 7 years for SEBI).
  - **Hot/cold tiering**: recent events in MongoDB, older in S3-backed cold storage.
  - **Compaction**: periodic snapshot of state at boundaries, prune events before that boundary.
- Hash chain remains verifiable as long as the genesis hash and the chain are intact. Compaction would require re-anchoring the chain at the snapshot point.

### F11. Auth filter evaluation cost

**Cause:** Every event is evaluated against every connected client's subscription. With 10,000 clients and 1,000 events/sec, that's 10M evaluations/sec.

**Symptom if unhandled:** broker CPU saturates.

**Our response:**
- Current implementation: O(N_clients × N_events) — fine to ~5K clients.
- Production-grade optimization: index clients by their filter criteria. For `customer=X`, look up only clients subscribed to X. For `intents=[cancellation]`, look up only clients subscribing to that intent. Reduces evaluation to O(matching clients × N_events).
- Documented but not implemented.

### F12. Multi-region replication lag

**Cause:** running this system across regions (e.g., Mumbai primary, Singapore secondary) introduces oplog replication lag of 50–200ms. Change streams on the secondary lag the primary.

**Symptom if unhandled:** clients connected to Singapore see events later than clients connected to Mumbai. Cross-client consistency is violated.

**Our response:**
- Out of scope. Multi-region active-active CDC is a hard distributed-systems problem with no clean solution short of CRDTs.
- Documented.

## What this list is for

This list is not exhaustive — production systems discover failure modes for years. But cataloging the ones we know, with our responses, demonstrates that the design was made with failure in mind, not just function.

> A system that fails loudly and recoverably is better than one that fails silently and looks fine.
