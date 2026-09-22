# Observability

Structured logs that carry the request id, a readiness probe that names the
dependency that failed, a request inspector for the desk, and Prometheus
metrics for the dashboard. Without it, a worker's log lines cannot be joined
to the request that caused them, and "the endpoint is slow" stays a guess.
There is no manager, driver seam or fake here, for the reasons in
[ADR 0009](adr/0009-correlation-and-logging.md).

## Logging and the request id

Configure logging once, first, before anything that might log. It is a
function and not a lifespan, because the failures worth reading happen during
start-up.

```python
from keel.observability import LoggingConfig, configure_logging

configure_logging(LoggingConfig.from_env())
```

| Variable | Default | Meaning |
|---|---|---|
| `LOG_LEVEL` | `INFO` | The root logger's level. Checked at load, so a typo fails at start-up. |
| `LOG_FORMAT` | `json` | `json` for an aggregator, `text` for a terminal. |

Bind a field wherever it becomes known. Every line logged inside the block
carries it, whichever module wrote the line, third-party ones included.

```python
from keel.observability import correlate

with correlate(request_id=request_id):
    await handle(request)
```

Fields merge with the surrounding scope, and the outer value returns on exit.
Values are coerced to `str` and `None` is dropped.

A JSON line carries `time`, `level`, `logger` and `message`, then the
correlation fields, then anything passed as `extra=`, then `exception` when
there is one. The bound principal's id is added as `user_id`; its roles and
claims are not. `RESERVED_FIELDS` and `SECRET_MARKERS` name what `correlate`
refuses: the members a record writes itself, and anything that looks like a
credential, such as `token`, `password` or `cookie`. This is a check on the
name, not the value. A secret bound under an innocent name reaches the log.

`dispatch()` seals the fields in effect onto the job's envelope, and the
worker binds them again around the attempt. A job's log lines carry the
request id that dispatched it, plus a `job` and `job_id` of their own.

## Readiness

`probe` runs named checks concurrently, each under its own deadline, and
returns a report. It never raises for a failing check.

```python
from keel.observability import check_cache, check_database, check_tokens, probe

report = await probe({"database": check_database, "cache": check_cache, "tokens": check_tokens})
report.as_dict()  # {"status": ..., "checks": {name: {"ok", "duration_ms"[, "error"]}}}
```

A check is any `async () -> object` that raises on failure. The shipped ones
are `check_database`, which runs `SELECT 1`; `check_cache`, which reads a key
that is never written; `check_tokens`, which resolves a token no store can
know; and `check_queue`, which counts the default lane. The last three take a
store or connection name, so `functools.partial(check_cache, "sessions")`
probes a second store.

`DEFAULT_TIMEOUT` is 0.8 seconds, under the one-second probe timeout
Kubernetes ships with, and `probe(..., timeout=)` changes it. An empty mapping
raises, because a probe that checks nothing is always ready. The body carries
each failure's exception class; the message, which can name a host, goes to
the log.

`/health` is liveness and touches nothing: a restart does not fix a database,
and it loses in-flight work. `/ready` asks every dependency a request can
need. A dependency bound in the lifespan belongs in `READINESS` too, or the
probe says ready while requests fail. Mail is the exception. No request needs
the relay to answer, and probing it would pull the API out of rotation for a
dead relay. Every check gates; a dependency that should not remove the
process from rotation is not a readiness check.

## The request inspector

The inspector records what one request did, as one timeline: its statements
and their durations, cache hits and misses, dispatches, and log lines. It is
a development tool, off by default. Enter it after the subsystems it watches,
and wrap each unit of work in `trace`.

```python
from keel.observability import InspectorConfig, inspector_lifespan, trace

async with inspector_lifespan(
    InspectorConfig.from_env(), database=current_database(), events=events
):
    ...

with trace(f"{method} {path}") as recorded:
    ...
```

`trace` yields the open `Trace`, or `None` when no inspector is bound or it is
disabled. A `Trace` holds `Entry` values. Each has a `kind` (`query`, `cache`,
`job` or `log`), a one-line `summary`, `at` seconds since the trace began, a
`duration` when timed, and a JSON-safe `detail` mapping. Application code
adds its own entries through `current_trace()`.

| Variable | Default | Meaning |
|---|---|---|
| `INSPECTOR_ENABLED` | off | Whether anything is recorded. Off subscribes to nothing. |
| `INSPECTOR_RETAIN` | `100` | Finished traces kept in memory. The oldest are dropped first. |
| `INSPECTOR_MAX_ENTRIES` | `2000` | Entries one trace keeps. The rest are counted as `dropped`. |
| `INSPECTOR_PARAMETERS` | off | Whether SQL bind parameters are recorded. |

Bind parameters are not recorded unless asked for, because a password hash
and a bearer token travel as one. Cache values, lock owners and request or
response bodies are never recorded. A log line is kept as written, so an
application that logs a secret has logged it. The traces hold SQL text and
log messages for whoever can reach the endpoint, which is why the switch is off.

## Metrics

Counters and histograms over the same sources, rendered for a Prometheus
scraper. Install the `keel[metrics]` extra, then enter the lifespan after the
subsystems it watches.

```python
from keel.observability import MetricsConfig, current_metrics, metrics_lifespan

async with metrics_lifespan(MetricsConfig.from_env(), database=current_database(), events=events):
    ...

body, content_type = current_metrics().render()
```

| Variable | Default | Meaning |
|---|---|---|
| `METRICS_ENABLED` | `true` | Off imports nothing, builds nothing and subscribes to nothing, so the extra is not needed. |

The instruments, with their labels:

- `keel_http_requests_total` (`method`, `route`, `status`) and `keel_http_request_duration_seconds` (`method`, `route`), fed by `Metrics.request()`.
- `keel_db_queries_total`, `keel_db_query_duration_seconds` and `keel_db_errors_total` (`operation`).
- `keel_cache_operations_total` (`store`, `operation`).
- `keel_jobs_dispatched_total` (`queue`, `deferred`).
- `keel_jobs_started_total` (`job`), `keel_jobs_total` (`job`, `outcome`) and `keel_job_duration_seconds` (`job`).
- `keel_jobs_recovered_total` (`lane`) and `keel_worker_faults_total` (`activity`).
- `keel_readiness_checks_total` (`check`, `result`) and `keel_readiness_check_duration_seconds` (`check`), fed by `Metrics.readiness()`.
- `keel_worker_jobs_in_flight` and `keel_worker_seconds_since_queue_answered`, gauges read off the `worker` handed to the lifespan.

A label is a route template, an operation, a store name or a job name, never
an id and never a path. A metric with an unbounded label is a memory leak with
a dashboard, so `Metrics` applies each bound itself. A method outside
`HTTP_METHODS` or a statement outside `SQL_OPERATIONS` counts as `OTHER`, and
a request no route claimed is `UNMATCHED_ROUTE`.

## In the template

`app/observability.py` holds `RequestContext`, a pure ASGI middleware. It
honours a valid inbound `X-Request-ID` or mints one, binds it with
`correlate`, opens a `trace`, echoes the id on the response, and hands the
route template and status to `Metrics.request()`. `app/main.py` calls
`configure_logging` first, declares `READINESS`, serves `/health` and
`/ready`, and mounts `/metrics` when metrics are enabled. `app/inspector.py`
serves `GET /_inspector/requests` and `GET /_inspector/requests/{id}` only
under `DEBUG`, which is the template's only switch for the inspector.
`app/worker.py` hands the running `Worker` to `metrics_lifespan`, serves the
exposition on `WORKER_METRICS_PORT` when that is set, and refreshes the
heartbeat file `app/healthcheck.py` reads as the worker's liveness probe.

## Limits

- No tracing. Exported spans with sampling and retention have no caller.
  Declined in [ADR 0011](adr/0011-the-request-inspector.md), and the phase
  closed without it in [ADR 0012](adr/0012-metrics.md).
- No backend seam for metrics. StatsD, OTLP or a push gateway would need a
  second exporter before a protocol earns its place. [ADR 0012](adr/0012-metrics.md).
- No home for a non-critical dependency's failures.
  [ADR 0010](adr/0010-readiness-checks.md) sent them to metrics, and nothing
  produces them yet. [ADR 0012](adr/0012-metrics.md).

## Further reading

- [ADR 0009 — correlation and logging](adr/0009-correlation-and-logging.md)
- [ADR 0010 — readiness checks](adr/0010-readiness-checks.md)
- [ADR 0011 — the request inspector](adr/0011-the-request-inspector.md)
- [ADR 0012 — metrics](adr/0012-metrics.md)
