# ADR 0011 — The request inspector

**Status:** accepted · **Date:** 2026-09-18 · **Phase:** 5 (slice three)

## Context

Structured logs (ADR 0009) answer "what happened to request `r-42`" from an
aggregator, after the fact, one line at a time. Nothing answered the question a
developer asks at the desk: *what did this request just do?* How many queries,
how long each took, which cache keys it missed, which jobs it dispatched and
whether they waited for the commit. "The endpoint is slow" stayed a guess until
someone added print statements. Laravel ships Telescope for this; Keel had the
event vocabulary — `keel.support.events` names "the development request
inspector" as its intended subscriber — and no subscriber.

ADR 0001 also left a question open on purpose: whether cache events should come
from the `Store` or the `Repository`. It deferred the decision to "the first
consumer with a real opinion". This is that consumer.

## Decisions

### 1. A trace is a context variable; the inspector is an Observer

`Inspector.trace(name)` binds a `Trace` on a `ContextVar` for the block. Every
source — SQLAlchemy's `before_cursor_execute`/`after_cursor_execute`, the cache
and queue's `EventDispatcher`, a `logging.Handler` on the root logger — reads
that variable first and returns if it is unset. So an entry recorded five frames
below the middleware lands on the right request, concurrent requests cannot see
each other's, a task started with `start_soon` inherits its parent's trace, and
an installed-but-idle inspector costs one `ContextVar.get` per event.

The engine hook runs inside SQLAlchemy's greenlet. Whether a context variable
set in the task is visible there is exactly the kind of thing a fake gets right
by accident, so the test runs against Postgres and asserts the statement text
and a positive duration.

### 2. Cache events stay on the `Store` — ADR 0001's question, closed

The inspector wants every round trip, timed and complete, including code that
bypasses the repository. `EventfulStore` gives it that. `remember` shows up as a
miss followed by a write with the recompute visible between them; `put_many`
shows as one entry per key, which is what a timeline should show. A second,
intent-level vocabulary emitted from the `Repository` would double the events to
add one word. Declined, and the question is closed rather than left open for a
third consumer.

The one thing the store level could not show was the single-flight lock, and
the review found the first draft claiming a picture it did not produce: two
identical misses with the whole wait unexplained between them. `LockAcquired`
had existed since Phase 1 and nothing emitted it. `EventfulStore.lock` now
returns an `EventfulLock` that announces `LockAcquired`, with how long `block()`
waited, and `LockReleased`, so a single-flight `remember` reads miss, lock,
re-check, write, unlock — and a request that spent five seconds behind another
caller's recompute says so.

### 3. The queue announces from `dispatch()`, not from a decorator over the driver

`QueueManager` and `queue_lifespan` take an `EventDispatcher` the way the cache
does, and `dispatch()` emits one `JobDispatched` per envelope. It is emitted from
the facade rather than from an `EventfulQueue` wrapping the driver, because only
the facade knows whether the push was deferred to the commit — the driver sees
the push after the commit and cannot say.

It is emitted **when the push is made**, not when `dispatch()` is called. The
first draft announced "dispatched, waiting for the commit" at the call, and the
review showed a rolled-back unit of work leaving that entry on the timeline with
nothing on the queue — for a tool whose purpose is "what did this request do",
the one fact it got wrong. A deferred dispatch is now announced at the commit,
a rollback announces nothing, and an immediate push is announced *before* the
driver runs it, so under the `sync` driver the job's own entries follow their
cause rather than precede it.

### 4. A development tool, off by default, and honest about what it exposes

Traces hold SQL text, cache keys and log messages, in memory, for whoever can
reach the endpoint that serves them. So `InspectorConfig.enabled` defaults to
false; the template ties it to `DEBUG` with no second switch to leave on by
mistake; the endpoint is mounted only under `DEBUG` and excluded from the
OpenAPI document; bind parameters are recorded only when asked, because a
password hash and a bearer token travel as one; a cache event's `value` and a
lock's `owner` are never recorded. A bearer check on the endpoint was
considered and rejected: it narrows "whoever" to "any signed-in user", which is
still everyone's queries.

`parameters` governs the SQL source. The review found the log source undoing
it: `DB_ECHO=true` prints every bind parameter through `sqlalchemy.engine`, and
those lines landed on the trace. They are skipped now (`ECHO_LOGGERS`), and the
docstrings say the narrower true thing: a message an application chose to log
is recorded as written.

Retention is a ring buffer of `retain` traces, `max_entries` per trace, and
`STATEMENT_LIMIT` characters per detail value once rendered as JSON — the last
added after the review held 2.5 MB in one entry from a single `executemany` with
`parameters=True`. The worst case is now the product of three numbers rather
than a function of the request, and a runaway loop turns into a `dropped` count
rather than a leak.

### 5. The bound trace is application-facing

`current_trace()` returns the open trace or `None`, and `Trace.record(kind,
summary, **detail)` takes any kind. An application that calls an outbound API
records it in one line, under its own kind, without Keel listing the kinds
first — and whatever it hands over is coerced to plain JSON and clipped, because
the first draft invited `response=whatever` and then served a 500 for it.
`Trace.tag(**fields)` is how the middleware writes the status code, which it
alone knows, onto a trace everything else filled.

A closed trace refuses further entries. A task started with `start_soon` inherits
the trace, which is the point, and it may outlive the request; without the
refusal it kept writing into a trace that had already been served.

## What was declined

| Declined | What would change it |
|---|---|
| A storage seam, and with it a `Manager`, a fake and a contract suite | A reader that outlives the process — a UI paging through yesterday. Telescope writes to the database for that; an in-memory buffer answers "the last few requests, on this machine" with nothing to migrate or clean up. |
| An entry class per kind | The inspector displays entries and never dispatches on them. Five classes with no behaviour that a JSON renderer flattens is a hierarchy for its own sake. |
| A Null Object inspector for the disabled case | `current_trace()` returning `None` is the same no-op with no object to carry it, and `trace()` yielding `None` lets a caller tell. |
| Tracing jobs in the worker | The worker would have to open a trace it has no reason to know about. A job's timeline is real, and it arrives with metrics, when there is a consumer for a trace that is not a request. |
| A UI | JSON from two endpoints is what a developer's tools and a `curl` read; a page is a frontend. |
| OpenTelemetry | Production wants exported spans with sampling and retention. That is a different tool with a different audience, and it is the remaining slice of this phase. |

## Consequences

**`cache_lifespan`, `queue_lifespan` and the template share one dispatcher.**
The template's lifespan builds an `EventDispatcher`, hands it to the cache and
the queue, and the inspector reads it. A listener that raises is logged by
`on_listener_error` and never fails the request that emitted.

**The inspector enters the lifespan last.** It watches what the others bound —
`current_database()` and the shared dispatcher — so it cannot come first.

**The log handler goes on the root logger directly.** Behind a `QueueHandler` it
would run in another thread with an empty context, for the reason ADR 0009 gives
for choosing a record factory over a filter. The access line the middleware
writes lands on the trace too, which is right: it is part of what the request
did.

**The inspector's own requests are not recorded**, or the listing would fill
with itself.

**`DEBUG` now does something.** It had no reader before this slice.

## What the review caught

Nine confirmed defects in a draft that passed every gate, all reproduced against
the live Postgres:

- **`DB_ECHO` put bind parameters on the trace with `parameters=False`**, through
  the log source. Decision 4.
- **One `executemany` held 2.5 MB in one entry**; the memory claim had no byte
  bound. Decision 4.
- **A rolled-back unit of work showed a job as dispatched.** Decision 3.
- **Decision 2 described a single-flight picture the code did not produce**, and
  the lock vocabulary it relied on was never emitted. Decision 2.
- **A task outliving its request kept writing into the served trace**, and a
  trace exited in another task was lost because the reset ran before the
  retention. Decision 5, and the order is now retain then reset.
- **`Entry.detail` claimed "JSON-safe by construction"** while `Trace.record`
  accepted anything; the detail endpoint returned 500. Decision 5.
- **Detaching twice raised**, and watching the same engine twice recorded every
  statement twice. Both idempotent now.
- **Under the `sync` driver the dispatch entry followed the job's own entries.**
  Decision 3.
- **The generated test's password assertion could not fail**: the plaintext
  never reaches a bind parameter. It asserts on the hash now, in both positions
  of the switch.

Also from the review: the trace name is bounded like a summary, `/_inspectorate`
is no longer skipped as if it were the inspector, and the prefix constant lives
with the middleware rather than the middleware importing a router.

## Verification

- `test_queries_are_recorded_with_their_duration` — the greenlet claim, against
  Postgres.
- `test_concurrent_traces_do_not_see_each_other` — the reason it is a context
  variable.
- `test_cache_operations_are_recorded_without_their_values` — through a real
  manager, and the password in the cached value is not in the trace.
- `test_a_dispatch_inside_a_unit_of_work_is_recorded_as_deferred` — decision 3.
- `test_parameters_are_recorded_only_when_asked` — decision 4.
- `test_the_lifespan_watches_everything_and_undoes_it` — nothing survives the
  lifespan's exit: not the engine hooks, not the listeners, not the handler.
- `test_a_disabled_inspector_records_and_subscribes_to_nothing` — the cost of
  the default.
- `test_echoed_statements_do_not_smuggle_parameters_onto_the_trace`,
  `test_a_bulk_statement_cannot_blow_the_trace_budget`,
  `test_a_dispatch_on_a_rolled_back_unit_of_work_is_not_shown_as_dispatched`,
  `test_single_flight_shows_the_lock_and_its_wait`,
  `test_a_task_that_outlives_its_trace_cannot_write_into_it`,
  `test_a_trace_is_retained_even_when_its_reset_fails`,
  `test_an_application_recorded_detail_is_coerced`,
  `test_detaching_twice_is_harmless_and_watching_twice_records_once`,
  `test_a_synchronously_run_job_is_announced_before_its_own_entries` — one per
  review finding.
- In the generated project: a `POST /users` is one timeline with its status
  and request id on it, the inspector does not record itself, `/_inspector` is
  a 404 without `DEBUG`, and the password hash is on the trace exactly when
  `INSPECTOR_PARAMETERS` says so.
