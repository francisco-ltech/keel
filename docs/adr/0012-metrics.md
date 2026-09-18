# ADR 0012 — Metrics

**Status:** accepted · **Date:** 2026-09-18 · **Phase:** 5 (slice four, the last)

## Context

Three slices of Phase 5 answered questions about one request: which log lines
it produced (ADR 0009), whether the process could serve it (ADR 0010), and what
it did (ADR 0011). None of them answered the questions a service is run by:
how many requests, at what latency, with what error rate; how many jobs
succeeded and how many were dead-lettered; whether the cache misses more than
it hits; how far behind the worker is. ADR 0010 had already pointed one class
of fact here — "a dependency whose failure should not take the process out of
rotation belongs in metrics" — and ADR 0011 declined OpenTelemetry as "the
remaining slice of this phase". This is it.

## Decisions

### 1. Metrics is a second Observer over the inspector's sources

`Metrics` subscribes to exactly what `Inspector` subscribes to — the engine's
cursor events, the dispatcher the cache and `dispatch()` announce on, and the
worker's lifecycle events when a worker runs in the process — and to nothing
those subsystems do not already say. The two observers share their sources and
share nothing else: a trace is sampled, kept briefly and off in production; a
counter must count every request. Deriving one from the other was declined for
that reason.

The bookkeeping both need — attach to a source once however many times asked,
detach harmlessly however many times asked — was written once as
`keel.support.events.Subscriptions`, because the inspector's review had found
both rules missing and the second observer would have re-derived them. The
cache's event vocabulary likewise moved to one place, `keel.cache.events.verb`,
so a timeline's word and a counter's label cannot drift.

### 2. `prometheus_client`, as an optional extra

The one place in Phase 5 where a dependency was taken rather than declined.
ADR 0009 refused `structlog` because the standard library had the hook logging
needed. It has nothing for this: the exposition format has escaping rules,
histograms are `_bucket`, `_sum` and `_count` series with a `+Inf` bucket,
label names have a character set, and every one of those is a scraper's parser
written a second time, wrongly. The process, platform and garbage-collector
collectors come with it, and they are the memory and CPU line every dashboard
starts with.

Optional (`keel[metrics]`) and imported only when enabled, because a process
nothing scrapes should not carry a registry — the review found the first draft
importing it before consulting `enabled`, so a process with metrics off could
not start without the extra. A missing extra is a `ConfigurationError` naming
it, at the lifespan rather than at the first scrape.

The worker's events are matched by class name under a listener on `object`,
never by importing `keel.queue.worker`: that import brings SAQ with it, and the
first draft did exactly that in every API process, against the lazy-export rule
`keel.queue` keeps.

### 3. Every label is bounded by construction

A route *template*, never a path; a method from a set of nine, never what the
parser let through; a SQL *operation*, never a statement; a job *name*, never
an id; a store *name* from configuration; a readiness check's name. A metric
with an unbounded label is a memory leak with a dashboard, and each bound is
applied in `Metrics` rather than trusted to the caller: the review sent 500
invented methods through uvicorn's fallback parser and got 9,500 permanent
series from one unauthenticated client, and ten WebDAV verbs through the
pinned parser. `OTHER` now.

The template's middleware reads the matched route's template off the scope,
prefixed with the mount's `root_path` so a sub-application's `/items/{id}`
does not alias onto the parent's, and labels a request no route claimed
`unmatched`, so a scan of guessed URLs is one series. A statement whose first
word is not a known operation is `OTHER`. The scraper's own `GET /metrics` is
not counted as a request, or the most frequent request on every dashboard
would be the dashboard.

The one label a caller decides is the queue on `keel_jobs_dispatched_total`: it
is whatever `dispatch(on=…)` said. An application that derives a queue name
from data has made a label out of that data, and the docstring says so rather
than claiming a bound the library cannot enforce.

### 4. What nothing announces is handed in

An HTTP request and a readiness probe are web-framework vocabulary that core
does not carry, so the application calls `Metrics.request(method, route,
status, duration)` and `Metrics.readiness(report)`. Two methods, both
one-liners at the call site, and the alternative — a middleware in Keel — is
the precedent ADR 0009 already refused.

### 5. One delivery is one terminal outcome, and the gauges are read, not tracked

The worker reports one job under two events when an unroutable job expires:
`JobUnroutable` with no `retry_in`, then `JobDeadLettered`. `keel_jobs_total`
counts the second and skips the first, and `JobStarted` has its own
`keel_jobs_started_total`, so `sum(rate(keel_jobs_total))` is the delivery rate
and `dead_lettered / total` is a ratio of one thing to itself. The first draft
counted `started` as an outcome and both halves of the pair, so every
successful job counted twice.

`keel_worker_jobs_in_flight` and `keel_worker_seconds_since_queue_answered`
are read off the `Worker` when the registry is rendered, not tracked by
incrementing on `JobStarted` and decrementing on each terminal event — the
same double report would make that arithmetic drift by one per expired job
for the life of the process. Reading the worker's own `in_flight` and
`last_success` cannot drift, and the second gauge is the number that climbs
through a Redis outage while liveness, correctly, stays green (ADR 0006,
decision 10). Both are registered only when a worker is watched, so an API
process's exposition carries no worker line.

**A statement that raised is counted too**, on `keel_db_queries_total` and
`keel_db_errors_total`, through SQLAlchemy's `handle_error` hook.
`after_cursor_execute` never fires for it, so the first draft's throughput
*dropped* during the one incident a database dashboard exists to show.

### 6. The worker opens one listener, and only when asked

A scrape is a pull, and there is no file-shaped substitute for it the way the
heartbeat file substitutes for a liveness endpoint. `WORKER_METRICS_PORT`
starts `prometheus_client`'s own thread-based listener serving nothing but the
exposition; zero, the default, opens nothing. The objection the healthcheck
module raised to a listener — a second port, a second graceful shutdown, in a
process whose job is not to listen — still holds for *liveness*; for metrics
the alternative is no metrics.

## What was declined

| Declined | What would change it |
|---|---|
| A backend seam — StatsD, OTLP, a push gateway — and with it a `Manager`, a fake and a contract suite | A second exporter. One implementation behind a protocol is the ceremony ADR 0000 refuses; an OTLP exporter would earn the seam and the suite. |
| A hand-rolled registry and exposition | Decision 2. A fake is not needed either: a test reads the real exposition back, which is what a scraper does. |
| Metrics derived from the inspector's traces | Decision 1. |
| Timing inside the worker loop | The worker's events already carry durations; counting is a listener's job, not the loop's. |
| Per-instrument buckets, a name prefix, label configuration | Every Keel service drawing on one dashboard is worth more than a knob. A deployment that wants more has the registry and can add its own instruments. |
| Counting the scrape as a request | Decision 3. |
| A home for a non-critical dependency's failures | ADR 0010 sent them here, and nothing produces them yet: a check outside `READINESS` is never probed, so there is nothing to count. The first draft's docstring claimed the readiness counter was that home; it is not, and the instrument arrives with its first caller. |

## Consequences

**`METRICS_ENABLED` defaults to true**, unlike the inspector's switch, because
counters by template, operation and name carry no identifiers and cost a
`labels().inc()` per event. Off skips every subscription and leaves `/metrics`
unmounted.

**The template's `/metrics` is served to whoever asks.** Nothing in it
identifies a user, a row or a statement. A deployment that wants it private
puts the scraper on the same network and the port behind it.

**One dispatcher per process feeds three consumers**: the log listeners, the
inspector under `DEBUG`, and metrics. Each of the template's entrypoints builds
it once and hands it to the cache, the queue and both observers. The worker's
did not, in the first draft — it bound the cache and the queue with no
dispatcher and built another for metrics, so a worker's cache and dispatch
counters could never move. `app/worker.py` now wires everything in one
`wired()` context manager that a generated test enters exactly as `serve` does.

**`Subscriptions` counts holders.** Two observers, or one lifespan and one
test, may watch the same engine; the source is detached when the last of them
lets go, not the first.

**Phase 5 is complete** with this slice. Tracing — exported spans with sampling
and retention — is not part of it and has no caller.

## What the review caught

Ten findings, the Postgres-backed ones reproduced live:

- **The method label was unbounded**: 500 invented methods, 9,500 series.
  Decision 3.
- **`METRICS_ENABLED=false` still required the extra**, and still built every
  instrument. Decision 2.
- **Every API process imported the worker runtime** through `WorkerEvent`,
  against the lazy-export rule. Decision 2.
- **The worker template's cache and dispatch counters could never move**: two
  dispatchers, the wrong one watched. Consequences.
- **`keel_jobs_total` counted `started` as an outcome and an expiring
  unroutable job twice.** Decision 5.
- **A statement that raised was not counted.** Decision 5.
- **`Metrics.readiness` promised a home for non-critical failures** that had no
  producer. Declined table.
- **A mounted sub-application's routes aliased onto the parent's.** Decision 3.
- **`Subscriptions` had no holder count**, so the first detach silenced the
  second holder. Consequences.
- **Dead code and false docstrings**: a `__contains__` nobody called, a
  write-only gauge list, buckets justified by a cache histogram that does not
  exist, `BEGIN` and `COMMIT` in an operation set they can never reach.

## Verification

- `test_a_request_is_counted_and_timed_by_route_template` — decision 3, from
  the exposition text, buckets included.
- `test_statements_are_counted_and_timed_by_operation` — against Postgres, and
  the statement is asserted absent from the exposition.
- `test_cache_round_trips_are_counted_by_store_and_outcome` — through a real
  manager, single flight included, and no key is a label.
- `test_worker_events_are_counted_by_job_and_outcome` — and no job id is a
  label.
- `test_a_watched_worker_exposes_its_liveness_as_gauges` — decision 5, and the
  gauges leave with the worker.
- `test_two_instances_do_not_share_a_registry` — the process-global registry
  `prometheus_client` offers is not used.
- `test_a_missing_extra_says_what_to_install` and
  `test_a_disabled_instance_subscribes_to_nothing_and_needs_no_extra` —
  decision 2, both halves.
- `test_a_dispatch_only_process_does_not_import_the_worker_runtime` — in a
  fresh interpreter, so the test module's own imports cannot mask it.
- `test_an_unknown_method_is_one_series`, `test_a_failed_statement_is_still_counted`,
  `test_one_delivery_is_one_terminal_outcome`,
  `test_one_holder_letting_go_does_not_stop_another` — one per review finding.
- In the generated project: a `POST /users` is counted under `/users`, a
  guessed URL under `unmatched`, three WebDAV verbs under `OTHER`, a mounted
  sub-application under its prefix, the INSERT under its operation, `/ready`'s
  checks by result, the scrape not at all, `METRICS_ENABLED=false` leaves
  `/metrics` unmounted — and the worker's wiring, entered as `serve` enters
  it, moves its cache and dispatch counters.
