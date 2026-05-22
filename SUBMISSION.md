# Submission Materials

This file contains the **application email**, **resume entry**, and **submission form note** for the APT Backend Developer assignment. Edit the placeholders, then send.

---

## 1. Application email

> **To:** business@apt.trading
> **Subject:** Backend Developer Assignment — Jinesha Gandhi (orderstream)

```
Hi APT team,

Following up on the backend assignment shortlist. I read the role carefully and noticed your emphasis on three things that are unusual for a fresher brief — SEBI 2025 audit-trail mandates, real-time order propagation guarantees, and low-latency execution with kill-switch primitives. So I built a project that engages with those problems head-on rather than a generic CRUD API.

orderstream — a real-time, audit-grade order event spine for high-throughput order management:

  •  MongoDB Change Streams → field-level diff → intent classification →
     tamper-evident hash chain → SSE + WebSocket fan-out
  •  Hash-chained Event Spine (SHA-256, blockchain-style) — every order
     lifecycle event is cryptographically chained, /audit/verify proves
     integrity in O(n). Direct mapping to SEBI 2025's immutable audit-trail
     mandate.
  •  Intent-aware Temporal Cohesion Buffer — cancellations bypass coalescing
     (urgent), status changes coalesce in 50ms, price corrections in 200ms.
     Modeled on the "missed cancellation = trading risk" principle.
  •  Proactive system_health SSE event — pushes degradation signal to
     clients before users notice frozen data. Designed against the
     pattern documented in Zerodha Kite's February 2026 outage.
  •  Snapshot+tail with read-watermark — solves the reconnect race that
     Firebase, Supabase, and Hasura all have documented bugs around.
  •  Per-client bounded queue + 5-strike slow-consumer eviction — observable
     backpressure, no silent data loss.
  •  Platform kill-switch (/admin/kill-switch) — halts broadcasts while
     preserving the audit trail. Clients bridge the suppression window
     via /events?since_seq replay.
  •  Algo ID and broker ID tagging on every order and event.
  •  23 tests passing in CI (GitHub Actions); full architecture docs;
     failure-modes catalogue with 12 named scenarios and design responses.

GitHub:    https://github.com/jineshagandhi/orderstream
Live demo: https://orderstream.onrender.com
Resume:    attached

I chose Python + FastAPI + MongoDB + SSE (with parallel WebSocket) for
specific reasons documented in README.md. Happy to walk through those
trade-offs in an interview — including alternatives I rejected (Kafka,
Redis pub/sub, full-document broadcast) and exactly when each becomes
the right choice.

About me: final-year B.Tech CSE (AI & Data Science) at MIT-WPU, Pune.
CGPA 8.60. 100+ DSA problems solved. Python-focused backend developer
with prior projects in Spring Boot, Firebase Realtime DB, and async
data pipelines. Genuinely interested in algorithmic trading and the
SEBI 2025 regulatory shift — this assignment was the most engaging
brief I've worked on this year.

Looking forward to a conversation.

Best,
Jinesha Gandhi
+91 9373318901
github.com/jineshagandhi
linkedin.com/in/jinesha-gandhi
```

---

## 2. Resume entry (add as the **first project** in your CV)

Drop this into the Projects section above FreelanceFlow:

```latex
\textbf{orderstream — Real-Time, Audit-Grade Order Event Spine}
\hfill \textit{Python, FastAPI, MongoDB Change Streams, SSE/WebSocket, Docker}
\hfill \href{https://github.com/jineshagandhi/orderstream}{GitHub}
\hfill \href{https://orderstream.onrender.com}{Live}
\hfill 2026

\begin{itemize}[leftmargin=*]
  \item Designed and built a CDC-driven real-time order propagation system
    with field-level diffs, intent-aware priority lanes, snapshot+tail
    read-watermark, and an SHA-256 hash-chained event spine — aligned with
    SEBI 2025 audit-trail requirements
  \item Implemented Temporal Cohesion Buffer that coalesces same-order
    burst writes within configurable windows per intent (cancellation =
    0ms / status = 50ms / price = 200ms), plus a platform kill-switch
    primitive that preserves the audit trail while halting fan-out
  \item Modeled proactive degradation signal (system\_health SSE event)
    against the Zerodha Kite Feb 2026 outage pattern; documented 12 failure
    modes with design responses; 23 tests passing in CI; full architecture,
    failure-mode, and comparison docs
\end{itemize}
```

Plain-text version for ATS / non-LaTeX resumes:

```
orderstream — Real-Time, Audit-Grade Order Event Spine                                 2026
Python, FastAPI, MongoDB Change Streams, SSE/WebSocket, Docker | GitHub | Live demo

• Designed and built a CDC-driven, real-time order propagation system with
  field-level diffs, intent-aware priority lanes, snapshot+tail read-watermark,
  and an SHA-256 hash-chained event spine aligned with SEBI 2025 audit-trail
  requirements

• Implemented a Temporal Cohesion Buffer that coalesces same-order burst writes
  per intent (cancellation 0ms / status 50ms / price 200ms), plus a kill-switch
  primitive that halts fan-out while preserving the audit trail

• Modeled proactive degradation signal (system_health SSE event) against the
  Zerodha Kite Feb 2026 outage pattern; documented 12 failure modes with design
  responses; 23 tests passing in CI; full architecture, failure-mode, and
  comparison docs
```

---

## 3. Google Form submission note

The shortlist email referenced [forms.gle/eT7mVyrWAWuz4K7t6](https://forms.gle/eT7mVyrWAWuz4K7t6). Most likely the form asks for a GitHub URL and optionally a description. If there's a free-text field, paste this:

```
Project: orderstream — real-time, audit-grade order event spine

Beyond the assignment's literal requirements, this submission engages
APT-specific concerns:

  •  SEBI 2025 audit mandate    → SHA-256 hash-chained event spine,
                                  /audit/verify endpoint
  •  Real-time + WebSocket      → both SSE and WebSocket endpoints
                                  share the same broker + replay path
  •  Kill-switch                → /admin/kill-switch halts fan-out
                                  while preserving the audit chain
  •  Algo ID / broker ID        → first-class fields on every order
  •  Documented failure modes   → docs/failure_modes.md catalogues
                                  12 scenarios with design responses
  •  Comparison to production   → docs/comparison.md analyzes Zerodha
                                  Kite (Feb 2026 outage), Angel One,
                                  Firebase, Supabase, Pusher

GitHub:     https://github.com/jineshagandhi/orderstream
Live demo:  https://orderstream.onrender.com
Tests:      23 passing in GitHub Actions CI
Stack:      Python 3.11 + FastAPI + Motor + Pydantic v2 + MongoDB rs0
```

---

## 4. Checklist before submitting

- [ ] Resume updated with `orderstream` as first project
- [ ] Project pushed to GitHub at `github.com/jineshagandhi/orderstream`
- [ ] GitHub repo is **public**
- [ ] CI badge in README turns green (GitHub Actions ran successfully)
- [ ] Live URL deployed on Render and added to README + email
- [ ] Demo GIF or screenshot at the top of README (optional but high-impact)
- [ ] `.env` file is NOT in the repo (verify with `git ls-files | grep .env`)
- [ ] Audit chain screenshot saved (proof of integrity)
- [ ] Tests passing screenshot saved (`23 passed`)
- [ ] Application email reviewed and sent to business@apt.trading
- [ ] Google Form submitted: https://forms.gle/eT7mVyrWAWuz4K7t6
