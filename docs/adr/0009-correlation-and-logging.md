# ADR 0009 — Correlation and structured logging

**Status:** accepted · **Date:** 2026-09-14 · **Phase:** 5 (slice one)

## Context

`Envelope.context` has carried this docstring since Phase 3: *"Ambient data
travelling with the job — correlation id, request id, tenant. This is the field
that makes a worker's logs joinable to the request that caused the work."*

Nothing ever populated it. It was a parameter a caller could pass to `dispatch()`
and nothing else, so the promise was unkept. Separately there was no logging
setup at all: three modules called `getLogger` and that was the whole story — no
formatter, no configuration, nothing carrying a request id.

This slice closes that loop. It deliberately does **not** build the request
inspector, metrics or health checks, and does not answer ADR 0001's open
question about whether cache events come from the `Store` or the `Repository`.

## Decisions

### 1. A ContextVar of fields, merged rather than replaced

`correlate(**fields)` binds, `correlation()` reads. The precedent is
`acting_as` and `publish_session`, and the difference from `acting_as` is the
interesting part: **that replaces, this merges.**

There is exactly one principal, so an inner `acting_as` naming a different one
*means* it. Fields are a set, and the question an inner scope asks is "also
record the tenant", never "forget the request id" — which is exactly what the
worker binding `job_id` would otherwise have done to the field this whole slice
exists to carry.

An inner scope may overwrite a field, and the outer value returns on exit.
Refusing would leave the worker unable to bind `job_id` under a scope that had
one; accepting silently and keeping the outer value would lie to the caller.

### 2. Values are coerced at the bind site, not at the write site

A field lands in two places that cannot hold an arbitrary object: a JSON line,
and `Envelope.context`, which is serialised onto Redis and stored in
`keel_failed_jobs.context`. Both fail *late*, frames away from the `correlate()`
responsible. So non-strings are coerced with `str` where they are bound, and
`None` is dropped — `correlate(tenant=maybe)` means "nothing to add", and an
outer value survives it.

### 3. `refuse=` splits programmer error from wire data

A keyword argument is written by a programmer, so a refused name is a mistake to
fix at the call site: it raises. A *mapping* is data — an envelope from Redis,
an inbound header — so a refused name is dropped. A field name chosen by an
older release must not be able to kill the worker that reads it.

Without that split, one poisoned envelope takes down a replica's task group.

### 4. `setLogRecordFactory`, not a `logging.Filter`

This is the load-bearing choice, and both halves of the argument were verified
rather than assumed.

A factory runs inside `Logger.makeRecord`, in the task that made the call, so
the fields are the ones in effect at the log statement. A filter runs on the
handler's side — which under `QueueHandler`/`QueueListener`, the standard way to
keep logging off an event loop, is a **different thread with an empty context**.
The filter version passes every test and loses the request id in the deployment
that most needs it. A filter on the root *logger* has a second, quieter failure:
filters do not run for records that merely propagate up from a child, so
`keel.queue.failed`'s lines would carry nothing.

The factory chains over whatever was installed before it. Re-capturing that
base on a second `configure_logging` is **wrong** — a vendor factory wraps
whatever it finds, so re-capturing makes the two wrap each other and the next
log call is a `RecursionError`. It probes with a throwaway record instead.

### 5. No new dependency, and the reason is not dependency count

`structlog` was declined, but not because Keel has only two runtime
dependencies. The standard library has the exact hook this needs, and a wrapper
would not improve it — it would push its own vocabulary into every application
and would still fail to reach libraries that log through the standard module.
That last point is not theoretical: SAQ's own lines carry the request id, for
free, because the factory reaches every logger in the process.

### 6. Only `Identity.id` reaches a log record

`roles` is an authorization input with no per-line diagnostic value. `claims` is
untyped and application-populated — ADR 0007 offers "a tenant, a scope, a token
id" as examples — so copying it onto every record makes the aggregator a
permanent mirror of whatever an author stashed on a principal. ADR 0008 removed
an email claim from `Identity` for the same reason.

The id is attached automatically rather than left to the application, because
the point of a record factory is that a module which knows nothing about any of
this still carries context. A worker is precisely where nobody remembers.

### 7. `SECRET_MARKERS` refuses a name, and says so

A denylist of credential-shaped field names (`password`, `token`, `cookie`,
`bearer`, …). This was challenged as being in the same family as the
`runtime_checkable` that ADR 0000 removed, and the distinction is narrow but
real: `runtime_checkable` *claimed* to check a protocol and actually checked
attribute names — the claim and the check differed. This claims to check a name
and checks a name.

`session_id` is deliberately **not** in the list, and a test pins its absence: it
is a legitimate correlation field, and refusing it would push people to spell it
something worse.

**The guarantee this does not give:** a secret in a *value* under an innocent
name, a secret in the log *message*, or a driver echoing a DSN in an exception
message all reach the aggregator untouched. Establishing "no secret reaches a
log record" would need a value-side check, and that belongs in the aggregator.
What actually moves the needle here is narrower and was done: only `Identity.id`
reaches a record, and the access line logs the path without the query string.

## What was declined

| Declined | What would change it |
|---|---|
| A `Manager[T]` and a contract suite for logging | A second implementation of the formatting seam — an OTLP exporter. Today it is a dict with two entries, and a `Manager` over that is the pattern-itis ADR 0000 refuses. |
| A fake in `keel.testing` | Nothing yet. A test binds real fields and reads them back. A recording double would be a second Formatter, at which point the contract suite arrives too. |
| A `logging_lifespan` | Nothing. The interesting failures happen *during* start-up, and a lifespan configures logging too late to record them. It is a function called first, not a context manager. |
| `structlog` | See decision 5. |
| A `replace=` mode on `correlate` | A caller that must *hide* an outer field. `dispatch(context_override=)` already covers "carry nothing". |
| The request-id middleware in Keel | ASGI is web-framework vocabulary, and core imports none. Same precedent as ADR 0008's bearer dependency. |

## Consequences

**`dispatch(context_override=)` replaces where `correlate` merges**, and carries
that in its name rather than in a docstring nobody opens. The alternative
considered was refusing an override that drops an ambient field, and it was
rejected: that turns a call which worked in a test into one that raises in
production because an unrelated request happened to bind a field.

**A job dispatched from a handler seals its parent's identifiers as
`parent_job`/`parent_job_id`.** They were being sealed under their own names, so
a dead-lettered child stored a `job_id` that was not its own.

**`configure_logging` adopts a server's loggers only if they already have
handlers.** Uvicorn decides whether to write an access line at all via
`getLogger("uvicorn.access").hasHandlers()`, and `--no-access-log` answers no by
emptying handlers and clearing propagate — so forcing `propagate = True` would
resurrect the access log that flag exists to silence. The rule is: take over
output that exists, never create output that does not.

**A 500's traceback from uvicorn carries no request id, and cannot.** The
contextvar has unwound by the time uvicorn's `except` runs, and leaving it bound
would break the invariant this is built on. So the request middleware logs the
failure itself, with `exc_info` and the id, a few milliseconds earlier. The cost
is one duplicate traceback per 500, which beats a request-id middleware becoming
the last word on every unhandled exception in the process.

**JSON is UTC and text is local time.** A machine correlating across hosts needs
one zone; a person reading a terminal needs their wall clock.

## What the reviews caught

**A 500 response carried no `X-Request-ID`.** The middleware sat inside
Starlette's `ServerErrorMiddleware` and nothing handled bare `Exception`, so the
one response a caller quotes in a ticket was the one without an id. The existing
test asserted the header on a 404 — which passes, and was active false
assurance.

**`--no-access-log` removed the structured half of uvicorn's output and left the
unstructured half**, so the JSON stream contained exactly one class of
non-JSON line: the ASGI exception traceback, the line most worth joining.

**`SAFE_REQUEST_ID` used `$`**, which matches before a trailing newline, so
`'abc\n'` and 129 characters were both accepted against a docstring promising
neither.

**A lone surrogate lost the whole line** — `json.dumps` emits it and the stream
write then raises — in a formatter whose docstring promises a logging call
cannot throw.

**`extra=` fields were silently dropped**, which is the exact failure
`RESERVED_FIELDS` exists to refuse. The two halves of the slice were applying
opposite rules to the same mistake. They are merged now, under the structural
lock.

**A log call could raise where it previously worked**: the record attribute was
unnamespaced, so `extra={"correlation": ...}` hit `logging`'s overwrite guard.

**Five one-line mutations survived the suite**, all of them the *contents* of the
denylists. The tests are parametrised over the constants now, so they cannot go
stale when someone edits a tuple — the same fix as looping over `__slots__`
rather than remembering two places.

## Verification

- A request id chosen by the caller appears on the API's log line, on SAQ's own
  enqueue line, and on the worker's lines for the job it dispatched.
- A retried dead letter keeps its original `request_id` rather than acquiring
  today's. This falls out of `FailedJobs.retry` bypassing `dispatch` to keep the
  replay verbatim (ADR 0006 §8), and is pinned because the obvious tidy-up is to
  route retries through `dispatch()`.
- A log line from inside `anyio.to_thread.run_sync` — the sign-in path — carries
  the request id.
- Concurrent tasks never see each other's fields; the binding restores on the
  exception path.
- A forged `request_id` containing `"}\n{"level":"CRITICAL"` produces one
  physical line of valid JSON.
- A 500 carries the header and a problem document; every uvicorn line parses as
  JSON.
