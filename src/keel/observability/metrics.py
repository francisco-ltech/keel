"""Metrics: what this process has done, counted, for something else to read.

The inspector answers "what did this request just do", at the desk, from a
buffer that dies with the process. This answers the production questions —
how many requests, at what latency, with what error rate; how many jobs
succeeded and how many were dead-lettered; whether the cache is missing more
than it hits; how far behind the worker is — as numbers a scraper reads on a
schedule and a dashboard draws over weeks. The two share their sources and
nothing else.

**Pattern: Observer, on the consuming side**, the same arrangement as
:mod:`keel.observability.inspector` and for the same reason: every subsystem
already announces what it does and none of them should know a counter exists.
:class:`Metrics` subscribes to the engine's cursor events, the shared
:class:`~keel.support.events.EventDispatcher` — cache events, dispatches, and
the worker's lifecycle events when it is running in this process — and reads a
:class:`~keel.queue.worker.Worker`'s own liveness numbers straight off it. The
two things nothing announces, an HTTP request and a readiness probe, are
handed in by the application through :meth:`Metrics.request` and
:meth:`Metrics.readiness`, because both are web-framework vocabulary that core
does not carry.

**The registry and the exposition format are ``prometheus_client``'s**, an
optional extra, and this is the one place in Phase 5 where a dependency was
taken rather than declined. ADR 0009 refused ``structlog`` because the
standard library had the exact hook logging needed. It has nothing for this:
the text exposition format has escaping rules, histograms need ``_bucket``,
``_sum`` and ``_count`` series with a ``+Inf`` bucket, label values have a
character set, and every one of those is a way to write a scraper's parser a
second time, wrongly. The process, platform and garbage-collector collectors
come with it, which is the memory and CPU line every dashboard starts with.

**Every label is bounded, and each is bounded here rather than by trusting the
caller.** A route *template*, never a path; a method from :data:`HTTP_METHODS`
or ``OTHER``, because a parser that accepts ``PROPFIND`` accepts a series per
word a client invents; a SQL *operation* from :data:`SQL_OPERATIONS` or
``OTHER``; a job *name*, never an id; a store name from configuration. The one
label a caller decides is the queue on ``keel_jobs_dispatched_total``: it is
whatever ``dispatch(on=…)`` said, and an application that derives it from data
has made a label out of that data. A metric with an unbounded label is a memory
leak with a dashboard.

**Off means off.** A disabled instance imports nothing, builds nothing and
subscribes to nothing, so a process nothing scrapes need not even install the
extra. Worker events are matched by class name rather than by importing
:mod:`keel.queue.worker`, because that import brings the worker runtime with
it, and a dispatch-only process must not pay for one (the rule ``keel.queue``
keeps with its lazy exports).

**Declined, with what would change each:**

* **A backend seam — StatsD, OTLP, a push gateway.** One implementation, so
  a protocol over it is the ceremony ADR 0000 refuses. An OTLP exporter
  would earn the seam, and with it a contract suite over both.
* **A hand-rolled registry.** See above; and a fake is not needed either,
  because a test reads the real exposition back.
* **Metrics from the inspector's traces.** A trace is sampled, retained
  briefly, and off in production; a counter must count every request. Two
  observers over one set of sources is the honest shape.
* **Timing in the worker itself.** The worker emits events with durations
  in them already; counting is a listener's job, not the loop's.
* **A home for a non-critical dependency's failures.** ADR 0010 sent them
  here, and nothing produces them yet: a check outside ``READINESS`` is not
  probed, so there is nothing to count. The instrument arrives with its first
  caller.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import event as sqla_event

from keel.exceptions import ConfigurationError
from keel.support.binding import Binding
from keel.support.events import Subscriptions

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping

    from prometheus_client import CollectorRegistry, Counter, Histogram

    from keel.database.engine import Database
    from keel.observability.health import HealthReport
    from keel.queue.worker import Worker
    from keel.support.events import EventDispatcher

SETTING_VARS: Final = "METRICS_"
"""Prefix for this module's knobs. ``METRICS_ENABLED``."""

INSTALL_HINT: Final = "install the metrics extra: keel[metrics]"

HTTP_METHODS: Final[frozenset[str]] = frozenset(
    {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "CONNECT"}
)
"""The methods a request may be labelled by. Anything else a parser lets through is ``OTHER``."""

DURATION_BUCKETS: Final[tuple[float, ...]] = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)
"""Histogram buckets for every duration here, in seconds.

One millisecond to ten seconds, roughly doubling: a query sits in the first
few, a request in the middle, a job or a readiness check that ran out in the
last. One set rather than one per instrument, so two histograms can be
compared on one axis.
"""

SQL_OPERATIONS: Final[frozenset[str]] = frozenset(
    {
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "WITH",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "SET",
        "CREATE",
        "ALTER",
        "DROP",
    }
)
"""The first words a statement may be labelled by. Anything else is ``OTHER``.

No ``BEGIN`` or ``COMMIT``: asyncpg issues those on the connection rather than
through a cursor, so they never reach the hook. ``ROLLBACK`` is here for
``ROLLBACK TO SAVEPOINT``, which does.
"""

UNMATCHED_ROUTE: Final = "unmatched"
"""The route label for a request no route claimed, so a 404 scan is one series."""

_TIMER_KEY: Final = "keel_metrics_began"

_JOB_OUTCOMES: Final[Mapping[str, str]] = {
    "JobSucceeded": "succeeded",
    "JobRetrying": "retrying",
    "JobDeadLettered": "dead_lettered",
    "JobUnroutable": "unroutable",
    "JobUnsettled": "unsettled",
}
"""The worker's terminal job events by class name, and the outcome each counts under.

``JobStarted`` is not here: it is counted on its own instrument, so one delivery
is one outcome. A ``JobUnroutable`` that is about to be dead-lettered — its
``retry_in`` is ``None`` — is skipped for the same reason, since the
``JobDeadLettered`` that follows it is the outcome.
"""

_WORKER_MODULE: Final = "keel.queue.worker"
"""Where the worker's events are defined. Matched by name so it is never imported here."""


@dataclass(frozen=True, slots=True)
class MetricsConfig:
    """Whether this process keeps metrics at all.

    One knob. Buckets, names and labels are fixed so that every Keel service
    draws on the same dashboard; a deployment that wants different ones has
    the registry and can add its own.

    Attributes:
            enabled: On by default: counters are cheap and safe to expose. Off
            imports nothing, builds nothing and subscribes to nothing, so a
            process nothing scrapes need not install the extra.
    """

    enabled: bool = True

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, prefix: str = "") -> MetricsConfig:
        """Build a configuration from environment variables.

        Reads ``METRICS_ENABLED``; absent means enabled.

        Args:
            env: The mapping to read; defaults to :data:`os.environ`.
            prefix: Prepended to every variable name.

        Returns:
            The configuration.
        """
        source = os.environ if env is None else env
        raw = source.get(f"{prefix}{SETTING_VARS}ENABLED", "true").strip().lower()
        return cls(enabled=raw not in {"0", "false", "no", "off"})


class Metrics:
    """The instruments, the registry they live in, and the subscriptions that feed them.

    Args:
        config: Whether to build and subscribe to anything.

    Raises:
        ConfigurationError: If enabled and ``prometheus_client`` is not
            installed.
    """

    __slots__ = (
        "_cache_operations",
        "_config",
        "_db_duration",
        "_db_errors",
        "_db_queries",
        "_http_duration",
        "_http_requests",
        "_job_duration",
        "_jobs",
        "_jobs_dispatched",
        "_jobs_recovered",
        "_jobs_started",
        "_readiness",
        "_readiness_duration",
        "_registry",
        "_subscriptions",
        "_worker_faults",
    )

    def __init__(self, config: MetricsConfig) -> None:
        self._config = config
        self._subscriptions = Subscriptions()
        self._registry: CollectorRegistry | None = None
        if config.enabled:
            self._build()

    def _build(self) -> None:
        try:
            import prometheus_client as prometheus
        except ImportError as exc:
            raise ConfigurationError(f"metrics need prometheus_client; {INSTALL_HINT}") from exc

        registry = self._registry = prometheus.CollectorRegistry()
        # Memory, CPU, open files, GC: the lines every dashboard starts with.
        prometheus.ProcessCollector(registry=registry)
        prometheus.PlatformCollector(registry=registry)
        prometheus.GCCollector(registry=registry)

        def counter(name: str, doc: str, labels: tuple[str, ...]) -> Counter:
            return prometheus.Counter(name, doc, labels, registry=registry)

        def histogram(name: str, doc: str, labels: tuple[str, ...]) -> Histogram:
            return prometheus.Histogram(
                name, doc, labels, buckets=DURATION_BUCKETS, registry=registry
            )

        self._http_requests = counter(
            "keel_http_requests_total",
            "Requests served, by route template.",
            ("method", "route", "status"),
        )
        self._http_duration = histogram(
            "keel_http_request_duration_seconds",
            "Request latency, by route template.",
            ("method", "route"),
        )
        self._db_queries = counter(
            "keel_db_queries_total", "Statements executed, failed ones included.", ("operation",)
        )
        self._db_duration = histogram(
            "keel_db_query_duration_seconds", "Latency of statements that returned.", ("operation",)
        )
        self._db_errors = counter(
            "keel_db_errors_total", "Statements the database refused.", ("operation",)
        )
        self._cache_operations = counter(
            "keel_cache_operations_total", "Cache round trips, by outcome.", ("store", "operation")
        )
        self._jobs_dispatched = counter(
            "keel_jobs_dispatched_total", "Jobs handed to the queue.", ("queue", "deferred")
        )
        self._jobs_started = counter("keel_jobs_started_total", "Deliveries begun.", ("job",))
        self._jobs = counter(
            "keel_jobs_total", "Deliveries ended, one terminal outcome each.", ("job", "outcome")
        )
        self._job_duration = histogram(
            "keel_job_duration_seconds", "Handler time for a successful job.", ("job",)
        )
        self._jobs_recovered = counter(
            "keel_jobs_recovered_total", "Jobs re-queued after their worker died.", ("lane",)
        )
        self._worker_faults = counter(
            "keel_worker_faults_total", "Driver failures a worker loop survived.", ("activity",)
        )
        self._readiness = counter(
            "keel_readiness_checks_total", "Readiness checks, by result.", ("check", "result")
        )
        self._readiness_duration = histogram(
            "keel_readiness_check_duration_seconds", "Readiness check latency.", ("check",)
        )

    @property
    def config(self) -> MetricsConfig:
        """The configuration this was built from."""
        return self._config

    @property
    def enabled(self) -> bool:
        """Whether :meth:`watch` subscribes to anything."""
        return self._config.enabled

    @property
    def registry(self) -> CollectorRegistry:
        """The registry every instrument lives in, for a listener that serves it.

        Raises:
            ConfigurationError: If disabled; there is no registry.
        """
        if self._registry is None:
            raise ConfigurationError("metrics are disabled; there is no registry")
        return self._registry

    # -- what the application hands in ------------------------------------

    def request(self, method: str, route: str, status: int, duration: float) -> None:
        """Count one served request.

        Args:
            method: The HTTP method as the server parsed it. Anything outside
                :data:`HTTP_METHODS` is counted as ``OTHER``, because the label
                is bounded here and not by what a parser lets through.
            route: The route *template* — ``/users/{id}`` — or
                :data:`UNMATCHED_ROUTE`. Never the path.
            status: The response status.
            duration: Seconds from receipt to the last byte.
        """
        if not self._config.enabled:
            return
        verb = method.upper() if method.upper() in HTTP_METHODS else "OTHER"
        self._http_requests.labels(verb, route, str(status)).inc()
        self._http_duration.labels(verb, route).observe(duration)

    def readiness(self, report: HealthReport) -> None:
        """Count a readiness probe's results, per check.

        Args:
            report: What :func:`~keel.observability.health.probe` returned.
        """
        if not self._config.enabled:
            return
        for result in report.checks:
            self._readiness.labels(result.name, "ok" if result.ok else "failed").inc()
            self._readiness_duration.labels(result.name).observe(result.duration)

    def render(self) -> tuple[bytes, str]:
        """Return the exposition body and its content type.

        Returns:
            What a ``/metrics`` route sends, and the ``Content-Type`` to send
            it with.

        Raises:
            ConfigurationError: If disabled; there is no registry to render.
        """
        registry = self.registry
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        return generate_latest(registry), CONTENT_TYPE_LATEST

    # -- sources ----------------------------------------------------------

    def watch(
        self,
        *,
        database: Database | None = None,
        events: EventDispatcher | None = None,
        worker: Worker | None = None,
    ) -> Callable[[], None]:
        """Subscribe to the sources given, and return what undoes it.

        Watching a source twice is watching it once, and detaching twice is
        harmless — :class:`~keel.support.events.Subscriptions` keeps both rules.

        Args:
            database: Its engine's statements are counted and timed, by
                operation.
            events: The dispatcher the cache, ``dispatch()`` and a worker
                announce on.
            worker: A worker running in this process. Its jobs in flight and
                the age of its last successful queue call become gauges, read
                off the worker at scrape time rather than tracked by event
                arithmetic, which a doubly-reported event would corrupt.

        Returns:
            A function that removes every subscription this call made.
        """
        keys: list[object] = []
        if not self._config.enabled:
            return lambda: None
        held = self._subscriptions
        if database is not None:
            engine = database.engine.sync_engine
            keys.append(held.add(("engine", id(engine)), lambda: self._watch_engine(database)))
        if events is not None:
            keys.append(held.add(("events", id(events)), lambda: self._watch_events(events)))
        if worker is not None:
            keys.append(held.add(("worker", id(worker)), lambda: self._watch_worker(worker)))

        def detach() -> None:
            for key in keys:
                held.remove(key)

        return detach

    def detach(self) -> None:
        """Remove every subscription made through :meth:`watch`."""
        self._subscriptions.clear()

    def _watch_engine(self, database: Database) -> Callable[[], None]:
        engine = database.engine.sync_engine
        queries, durations, errors = self._db_queries, self._db_duration, self._db_errors

        def before(conn: Any, *_: Any) -> None:
            conn.info[_TIMER_KEY] = time.perf_counter()

        def after(conn: Any, _cursor: Any, statement: str, *_: Any) -> None:
            began = conn.info.pop(_TIMER_KEY, None)
            operation = _operation(statement)
            queries.labels(operation).inc()
            if began is not None:
                durations.labels(operation).observe(time.perf_counter() - began)

        def failed(context: Any) -> None:
            # after_cursor_execute never fires for a statement that raised, and a
            # dashboard whose throughput drops during an incident is lying.
            statement = getattr(context, "statement", None)
            if statement is None:
                return
            operation = _operation(statement)
            queries.labels(operation).inc()
            errors.labels(operation).inc()

        sqla_event.listen(engine, "before_cursor_execute", before)
        sqla_event.listen(engine, "after_cursor_execute", after)
        sqla_event.listen(engine, "handle_error", failed)

        def remove() -> None:
            sqla_event.remove(engine, "before_cursor_execute", before)
            sqla_event.remove(engine, "after_cursor_execute", after)
            sqla_event.remove(engine, "handle_error", failed)

        return remove

    def _watch_events(self, events: EventDispatcher) -> Callable[[], None]:
        # Imported here so configure_logging, the first thing a process runs,
        # does not pull the cache and the queue in behind it.
        from keel.cache.events import CacheEvent
        from keel.queue.dispatch import JobDispatched

        # The worker's events are matched by name under `object`, because importing
        # their classes would load the worker runtime into a dispatch-only process.
        removers = [
            events.listen(CacheEvent, self._on_cache_event),
            events.listen(JobDispatched, self._on_job_dispatched),
            events.listen(object, self._on_worker_event),
        ]

        def remove() -> None:
            for one in removers:
                one()

        return remove

    def _watch_worker(self, worker: Worker) -> Callable[[], None]:
        from prometheus_client import Gauge

        in_flight = Gauge(
            "keel_worker_jobs_in_flight", "Jobs this worker is running now.", registry=self.registry
        )
        in_flight.set_function(lambda: float(worker.in_flight))
        since = Gauge(
            "keel_worker_seconds_since_queue_answered",
            "Age of this worker's last successful queue call; climbs through an outage.",
            registry=self.registry,
        )
        since.set_function(lambda: time.time() - worker.last_success)
        registry = self.registry

        def remove() -> None:
            registry.unregister(in_flight)
            registry.unregister(since)

        return remove

    def _on_cache_event(self, event: Any) -> None:
        from keel.cache.events import verb

        self._cache_operations.labels(event.store, verb(event)).inc()

    def _on_job_dispatched(self, event: Any) -> None:
        self._jobs_dispatched.labels(event.envelope.queue, str(event.deferred).lower()).inc()

    def _on_worker_event(self, event: Any) -> None:
        kind = type(event)
        if kind.__module__ != _WORKER_MODULE:
            return
        name = kind.__name__
        outcome = _JOB_OUTCOMES.get(name)
        if name == "JobStarted":
            self._jobs_started.labels(event.envelope.job).inc()
        elif outcome is not None:
            if name == "JobUnroutable" and event.retry_in is None:
                return  # the JobDeadLettered that follows is this delivery's outcome
            self._jobs.labels(event.envelope.job, outcome).inc()
            if name == "JobSucceeded":
                self._job_duration.labels(event.envelope.job).observe(event.duration)
        elif name == "WorkerFaulted":
            self._worker_faults.labels(event.activity).inc()
        elif name == "JobRecovered":
            self._jobs_recovered.labels(event.lane).inc()


def _operation(statement: str) -> str:
    """Return the label a statement is counted under.

    Args:
        statement: The SQL as executed.

    Returns:
        Its first word uppercased when that is a known operation, else ``OTHER``.
    """
    head = statement.lstrip().split(None, 1)
    word = head[0].upper() if head else ""
    return word if word in SQL_OPERATIONS else "OTHER"


# -- binding --------------------------------------------------------------

_binding: Binding[Metrics] = Binding(
    "metrics",
    "enter keel.observability.metrics_lifespan(...) during startup",
)


def set_metrics(metrics: Metrics | None) -> None:
    """Install the process-wide metrics.

    Args:
        metrics: The instance, or ``None`` to unbind.
    """
    _binding.set(metrics)


def current_metrics() -> Metrics:
    """Return the metrics in effect.

    Returns:
        The bound instance.

    Raises:
        ConfigurationError: If none is bound.
    """
    return _binding.current()


def bound_metrics() -> Metrics | None:
    """Return the metrics in effect, or ``None`` when nothing is bound.

    What a middleware calls, so a process configured without metrics pays a
    lookup and nothing else.

    Returns:
        The instance, or ``None``.
    """
    try:
        return _binding.current()
    except ConfigurationError:
        return None


@contextmanager
def use_metrics(metrics: Metrics) -> Iterator[Metrics]:
    """Override the bound metrics for the duration of a block.

    Args:
        metrics: The instance to use.

    Yields:
        The same instance.
    """
    with _binding.use(metrics) as bound:
        yield bound


@asynccontextmanager
async def metrics_lifespan(
    config: MetricsConfig,
    *,
    database: Database | None = None,
    events: EventDispatcher | None = None,
    worker: Worker | None = None,
) -> AsyncIterator[Metrics]:
    """Bind metrics for the life of the process, watching the sources given.

    Entered after the subsystems it watches are bound. Restores whatever was
    bound before, like every other lifespan.

    Args:
        config: Whether to subscribe to anything.
        database: The database whose statements to count, if any.
        events: The dispatcher the subsystems announce on, if any.
        worker: The worker running in this process, if any.

    Yields:
        The bound instance.

    Raises:
        ConfigurationError: If enabled and ``prometheus_client`` is not
            installed.
    """
    previous = _binding.peek()
    metrics = Metrics(config)
    metrics.watch(database=database, events=events, worker=worker)
    set_metrics(metrics)
    try:
        yield metrics
    finally:
        metrics.detach()
        set_metrics(previous)


__all__ = [
    "DURATION_BUCKETS",
    "HTTP_METHODS",
    "SQL_OPERATIONS",
    "UNMATCHED_ROUTE",
    "Metrics",
    "MetricsConfig",
    "bound_metrics",
    "current_metrics",
    "metrics_lifespan",
    "set_metrics",
    "use_metrics",
]
