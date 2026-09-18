"""The request inspector: what one unit of work did, for the developer who wrote it.

Structured logs answer "what happened to request ``r-42``" after the fact, from
an aggregator, one line at a time. This answers a different question, at the
desk: *what did this request just do?* The queries it ran and how long each
took, the cache keys it hit and missed, the jobs it dispatched and whether they
waited for the commit, and the log lines it wrote — as one timeline, kept in the
process, readable from a debug endpoint or a test. Laravel calls this Telescope.
It is the tool that turns "the endpoint is slow" into "it runs one query per
item and misses the cache every time".

**Pattern: Observer, on the consuming side.** Every subsystem already announces
what it does — the cache through :class:`~keel.support.events.EventDispatcher`,
the queue's dispatch side likewise, SQLAlchemy through its engine events, and
``logging`` through a handler. :class:`Inspector` subscribes to all four and
none of them imports it. That is the arrangement ``keel.support.events`` was
written for, and the inspector is the "development request inspector" its
docstring names as the intended subscriber.

**A trace is bound on a context variable**, the mechanism ``correlate`` and
``acting_as`` use and for the same reasons: an entry recorded five frames below
the middleware lands on the right request, concurrent requests cannot see each
other, and a task started with ``start_soon`` inherits its parent's trace. When
no trace is bound, every observer returns before doing anything, so the cost of
an inspector that is installed but idle is one ``ContextVar.get`` per event.

**It is off in production, and it is a development tool by design.** The
recorded traces hold SQL text, cache keys and log messages, in memory, for
whoever can reach the endpoint that serves them. ``enabled`` therefore defaults
to false, the template mounts its endpoint only under ``DEBUG``, and bind
parameters are not recorded unless asked for, because they are where passwords
and tokens travel. That switch governs the SQL source; a log line is recorded
as it was written, so SQLAlchemy's own echo lines — which print the parameters
— are skipped, and an application that logs a secret has logged it. Memory is
bounded per trace by ``max_entries`` and per detail value by
:data:`STATEMENT_LIMIT`, so the worst case is arithmetic rather than a leak.
Production wants metrics and a tracing exporter, which are different tools
with different retention, and are Phase 5's remaining slice.

**Declined, with what would change each:**

* **A storage seam.** Telescope writes entries to the database so a UI can
  page through history. An in-memory ring buffer answers the question this is
  for — the last few requests, on this developer's machine — with nothing to
  migrate or clean up. A second reader that outlives the process is what
  would earn a driver, and a contract suite with it.
* **An entry class per kind.** :class:`Entry` is one shape with a ``kind``: the
  inspector displays entries, it never dispatches on them, and a hierarchy
  would be five classes with no behaviour that a JSON renderer flattens anyway.
* **A Null Object inspector for the disabled case.** ``current_trace()``
  returning ``None`` is the same no-op with no object to carry it, and
  :func:`trace` yields ``None`` so a caller can tell.
* **Emitting cache events from the ``Repository`` rather than the ``Store``**
  — ADR 0001's open question, answered here by the first consumer with an
  opinion. The inspector wants every round trip, timed and complete, including
  code that bypasses the repository; ``remember`` shows up as a miss followed
  by a write with the recompute between them, and under single flight as a
  miss, the lock with how long it waited, the re-check, and the write. A
  second, intent-level vocabulary would double the events for one extra word.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from collections import Counter, deque
from collections.abc import Callable, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import event as sqla_event

from keel.exceptions import ConfigurationError
from keel.support.binding import Binding
from keel.support.correlation import correlation

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from keel.database.engine import Database
    from keel.support.events import EventDispatcher

SETTING_VARS: Final = "INSPECTOR_"
"""Prefix for this module's knobs. ``INSPECTOR_ENABLED``, ``INSPECTOR_RETAIN``, …"""

TRUTHY: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on"})
"""What an environment variable may say to mean ``True``, case-insensitively."""

SUMMARY_LIMIT: Final = 500
"""Characters an entry's one-line summary may hold. The full text stays in ``detail``."""

STATEMENT_LIMIT: Final = 10_000
"""Characters any one detail value may hold once rendered as JSON.

A generated ``IN (…)`` can run to megabytes, and so can an ``executemany``'s
parameter list; a value past this is kept as its clipped rendering and the
original length. With ``max_entries`` this is what makes a trace's memory a
product of two numbers rather than a function of the request.
"""

ECHO_LOGGERS: Final = ("sqlalchemy.engine",)
"""Loggers whose lines are the statements and parameters again, and are not recorded.

``DB_ECHO`` prints every bind parameter through them, which would put on the
trace exactly what ``parameters=False`` keeps off it.
"""

_TIMER_KEY: Final = "keel_inspector_began"
"""Where the statement's start time is parked on the connection between the two engine hooks."""

_WHITESPACE: Final = re.compile(r"\s+")

_CACHE_VERBS: Final[Mapping[str, str]] = {
    "CacheHit": "hit",
    "CacheMissed": "miss",
    "KeyWritten": "write",
    "KeyForgotten": "forget",
    "CounterIncremented": "increment",
    "CacheFlushed": "flush",
    "LockAcquired": "lock",
    "LockReleased": "unlock",
}
"""How each cache event reads on the timeline. Keyed by class name so nothing is imported."""

_NEVER_RECORDED: Final[frozenset[str]] = frozenset({"value", "owner"})
"""Cache event fields kept off the trace: a value may be a secret, an owner is a lock token."""


# -- configuration --------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InspectorConfig:
    """Whether and how much to record.

    Attributes:
        enabled: Off by default, for the reason the module docstring gives.
        retain: How many finished traces to keep. Oldest are dropped first.
        max_entries: Entries one trace may hold before the rest are counted
            rather than kept, so a runaway loop cannot turn a debug tool into
            a memory leak.
        parameters: Whether SQL bind parameters are recorded. Off by default:
            a password hash, a token and a card number all travel as
            parameters, and a timeline of statements is diagnostic without them.
            Governs the SQL source only; see :data:`ECHO_LOGGERS` for the one
            log source that would have leaked them, and note that a message an
            application chose to log is recorded as written.
    """

    enabled: bool = False
    retain: int = 100
    max_entries: int = 2000
    parameters: bool = False

    def __post_init__(self) -> None:
        """Reject limits that would keep nothing.

        Raises:
            ConfigurationError: If ``retain`` or ``max_entries`` is below one.
        """
        if self.retain < 1:
            raise ConfigurationError(f"INSPECTOR_RETAIN must be at least 1, got {self.retain}")
        if self.max_entries < 1:
            raise ConfigurationError(
                f"INSPECTOR_MAX_ENTRIES must be at least 1, got {self.max_entries}"
            )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, prefix: str = "") -> InspectorConfig:
        """Build a configuration from environment variables.

        Reads ``INSPECTOR_ENABLED``, ``INSPECTOR_RETAIN``, ``INSPECTOR_MAX_ENTRIES``
        and ``INSPECTOR_PARAMETERS``.

        Args:
            env: The mapping to read; defaults to :data:`os.environ`.
            prefix: Prepended to every variable name.

        Returns:
            The configuration.

        Raises:
            ConfigurationError: If a count is not an integer, or is below one.
        """
        source = os.environ if env is None else env

        def flag(name: str) -> bool:
            return source.get(f"{prefix}{SETTING_VARS}{name}", "").strip().lower() in TRUTHY

        def count(name: str, default: int) -> int:
            raw = source.get(f"{prefix}{SETTING_VARS}{name}")
            if raw is None or not raw.strip():
                return default
            try:
                return int(raw)
            except ValueError as exc:
                raise ConfigurationError(
                    f"{prefix}{SETTING_VARS}{name} must be an integer, got {raw!r}"
                ) from exc

        return cls(
            enabled=flag("ENABLED"),
            retain=count("RETAIN", 100),
            max_entries=count("MAX_ENTRIES", 2000),
            parameters=flag("PARAMETERS"),
        )


# -- what is recorded -----------------------------------------------------


@dataclass(frozen=True, slots=True)
class Entry:
    """One thing a unit of work did.

    Attributes:
        kind: ``query``, ``cache``, ``job`` or ``log``. A string rather than an
            enum so an application can record its own kinds through
            :meth:`Trace.record` without Keel listing them first.
        summary: One line, at most :data:`SUMMARY_LIMIT` characters.
        at: Seconds after the trace began.
        duration: Seconds the operation took, when it was timed.
        detail: Everything else worth showing. JSON-safe and size-bounded by
            construction: :meth:`Trace.record` coerces what it is given.
    """

    kind: str
    summary: str
    at: float
    duration: float | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Return the entry as plain data, for a JSON response.

        Returns:
            The entry's fields.
        """
        return {
            "kind": self.kind,
            "summary": self.summary,
            "at": self.at,
            "duration": self.duration,
            "detail": dict(self.detail),
        }


class Trace:
    """The record of one unit of work: a request, a job, a command.

    Mutable while open — that is its job — and left alone once
    :attr:`duration` is set. Application code reaches it through
    :func:`current_trace` to record something of its own, or to tag the trace
    with an outcome the middleware knows and nothing below it does.

    Args:
        name: What the work was, as a person would say it: ``GET /users``.
        max_entries: Entries to keep before counting the rest as dropped.
    """

    __slots__ = (
        "_began",
        "dropped",
        "duration",
        "entries",
        "fields",
        "id",
        "max_entries",
        "name",
        "started",
    )

    def __init__(self, name: str, *, max_entries: int) -> None:
        self.id = uuid.uuid4().hex[:12]
        self.name = name[:SUMMARY_LIMIT]
        self.started = time.time()
        self._began = time.perf_counter()
        self.max_entries = max_entries
        self.entries: list[Entry] = []
        self.dropped = 0
        self.fields: dict[str, str] = dict(correlation())
        self.duration: float | None = None

    @property
    def elapsed(self) -> float:
        """Seconds since the trace began, or its final duration once closed."""
        return self.duration if self.duration is not None else time.perf_counter() - self._began

    def record(
        self,
        kind: str,
        summary: str,
        *,
        duration: float | None = None,
        **detail: Any,
    ) -> Entry | None:
        """Append an entry.

        Args:
            kind: See :attr:`Entry.kind`.
            summary: One line; truncated to :data:`SUMMARY_LIMIT`.
            duration: Seconds the operation took, if known.
            **detail: Anything else. Reduced to JSON-safe values, and each
                clipped to :data:`STATEMENT_LIMIT` once rendered, so an
                application can hand over whatever it has.

        Returns:
            The entry, or ``None`` when it was not kept: the trace is full and
            it was counted instead, or the trace has already closed — a task
            that outlives its request must not keep writing into a trace that
            has been served.
        """
        if self.duration is not None:
            return None
        if len(self.entries) >= self.max_entries:
            self.dropped += 1
            return None
        entry = Entry(
            kind=kind,
            summary=summary[:SUMMARY_LIMIT],
            at=time.perf_counter() - self._began,
            duration=duration,
            detail={key: _bounded(value) for key, value in detail.items()},
        )
        self.entries.append(entry)
        return entry

    def tag(self, **values: object) -> None:
        """Attach facts about the work as a whole: a status code, a route name.

        Coerced to strings like correlation fields, and for the same reason —
        they are rendered, never computed with. ``None`` is dropped.

        Args:
            **values: The facts.
        """
        for name, value in values.items():
            if value is not None:
                self.fields[name] = str(value)

    def counts(self) -> dict[str, int]:
        """Return how many entries of each kind were recorded.

        Returns:
            Kind to count, in first-seen order.
        """
        return dict(Counter(entry.kind for entry in self.entries))

    def as_dict(self, *, entries: bool = True) -> dict[str, Any]:
        """Return the trace as plain data.

        Args:
            entries: Whether to include the entries, or only the summary a
                listing wants.

        Returns:
            The trace's fields.
        """
        data: dict[str, Any] = {
            "id": self.id,
            "name": self.name,
            "started": self.started,
            "duration": self.duration,
            "fields": dict(self.fields),
            "counts": self.counts(),
            "dropped": self.dropped,
        }
        if entries:
            data["entries"] = [entry.as_dict() for entry in self.entries]
        return data

    def __repr__(self) -> str:
        """Identify the trace by id, name and size."""
        return f"<Trace {self.id} {self.name!r} entries={len(self.entries)}>"


_current: ContextVar[Trace | None] = ContextVar("keel_inspector_trace", default=None)


def current_trace() -> Trace | None:
    """Return the trace in effect, or ``None`` when nothing is being recorded.

    The check every observer makes first, and the way application code adds an
    entry of its own::

        if (trace := current_trace()) is not None:
            trace.record("http", f"GET {url}", duration=elapsed, status=status)

    Returns:
        The bound trace, or ``None``.
    """
    return _current.get()


# -- the inspector --------------------------------------------------------


class Inspector:
    """Records traces, and keeps the last few.

    Args:
        config: What to record and how much to keep.
    """

    __slots__ = ("_config", "_detach", "_watched", "recent")

    def __init__(self, config: InspectorConfig) -> None:
        self._config = config
        self._detach: dict[object, Callable[[], None]] = {}
        self._watched: set[object] = set()
        self.recent: deque[Trace] = deque(maxlen=config.retain)

    @property
    def config(self) -> InspectorConfig:
        """The configuration this inspector records under."""
        return self._config

    @property
    def enabled(self) -> bool:
        """Whether :meth:`trace` records anything."""
        return self._config.enabled

    @contextmanager
    def trace(self, name: str) -> Iterator[Trace | None]:
        """Record everything that happens in the block as one trace.

        Nested traces are not merged: an inner block gets its own trace, and
        the outer one resumes when it exits. That is what a job dispatched and
        run inline wants, and it is the same rule ``correlate`` follows.

        Args:
            name: What the work is, for the listing.

        Yields:
            The open trace, or ``None`` when the inspector is disabled.
        """
        if not self._config.enabled:
            yield None
            return
        opened = Trace(name, max_entries=self._config.max_entries)
        token = _current.set(opened)
        try:
            yield opened
        finally:
            # Retained before the reset: a block exited in another task fails the
            # reset, and the trace must not be lost with it.
            opened.duration = opened.elapsed
            self.recent.append(opened)
            _current.reset(token)

    def find(self, trace_id: str) -> Trace | None:
        """Return a retained trace by id.

        Args:
            trace_id: The id a listing showed.

        Returns:
            The trace, or ``None`` if it was never recorded or has been dropped.
        """
        for retained in self.recent:
            if retained.id == trace_id:
                return retained
        return None

    def clear(self) -> None:
        """Forget every retained trace."""
        self.recent.clear()

    # -- sources ----------------------------------------------------------

    def watch(
        self,
        *,
        database: Database | None = None,
        events: EventDispatcher | None = None,
        logs: bool = False,
    ) -> Callable[[], None]:
        """Subscribe to the sources given, and return what undoes it.

        A disabled inspector subscribes to nothing, so the process pays no
        per-statement hook for a tool it is not using. Watching a source twice
        is watching it once: a second lifespan over the same engine must not
        record every statement twice.

        Args:
            database: Its engine's statements are timed and recorded. The
                readiness probe's own engine is not it, and is not watched.
            events: The dispatcher the cache and the queue announce on.
            logs: Whether to record log records. The handler goes on the root
                logger directly, so it runs in the emitting task and sees the
                trace — behind a ``QueueHandler`` it would not, for the reason
                ADR 0009 gives for the record factory.

        Returns:
            A function that removes every subscription this call made. Safe to
            call more than once, and after :meth:`detach`.
        """
        keys: list[object] = []
        if not self._config.enabled:
            return lambda: None
        if database is not None:
            engine = database.engine.sync_engine
            keys.append(
                self._subscribe(("engine", id(engine)), lambda: self._watch_engine(database))
            )
        if events is not None:
            keys.append(self._subscribe(("events", id(events)), lambda: self._watch_events(events)))
        if logs:
            keys.append(self._subscribe("logs", self._watch_logging))

        def detach() -> None:
            for key in keys:
                self._unsubscribe(key)

        return detach

    def detach(self) -> None:
        """Remove every subscription made through :meth:`watch`."""
        for key in list(self._detach):
            self._unsubscribe(key)

    def _subscribe(self, key: object, attach: Callable[[], Callable[[], None]]) -> object:
        if key not in self._watched:
            self._watched.add(key)
            self._detach[key] = attach()
        return key

    def _unsubscribe(self, key: object) -> None:
        remover = self._detach.pop(key, None)
        if remover is not None:
            self._watched.discard(key)
            remover()

    def _watch_engine(self, database: Database) -> Callable[[], None]:
        engine = database.engine.sync_engine
        record_parameters = self._config.parameters

        def before(
            conn: Any, _cursor: Any, _statement: str, _parameters: Any, _context: Any, _many: bool
        ) -> None:
            if _current.get() is not None:
                conn.info[_TIMER_KEY] = time.perf_counter()

        def after(
            conn: Any, cursor: Any, statement: str, parameters: Any, _context: Any, many: bool
        ) -> None:
            trace = _current.get()
            began = conn.info.pop(_TIMER_KEY, None)
            if trace is None:
                return
            detail: dict[str, Any] = {
                "statement": statement[:STATEMENT_LIMIT],
                "executemany": many,
            }
            rows = getattr(cursor, "rowcount", None)
            if isinstance(rows, int) and rows >= 0:
                detail["rows"] = rows
            if record_parameters:
                detail["parameters"] = parameters
            trace.record(
                "query",
                _WHITESPACE.sub(" ", statement).strip(),
                duration=None if began is None else time.perf_counter() - began,
                **detail,
            )

        sqla_event.listen(engine, "before_cursor_execute", before)
        sqla_event.listen(engine, "after_cursor_execute", after)

        def remove() -> None:
            sqla_event.remove(engine, "before_cursor_execute", before)
            sqla_event.remove(engine, "after_cursor_execute", after)

        return remove

    def _watch_events(self, events: EventDispatcher) -> Callable[[], None]:
        # Imported here so configure_logging, the first thing a process runs,
        # does not pull the cache and the queue in behind it.
        from keel.cache.events import CacheEvent
        from keel.queue.dispatch import JobDispatched

        forget_cache = events.listen(CacheEvent, self._on_cache_event)
        forget_jobs = events.listen(JobDispatched, self._on_job_dispatched)

        def remove() -> None:
            forget_cache()
            forget_jobs()

        return remove

    def _watch_logging(self) -> Callable[[], None]:
        handler = _TraceHandler()
        root = logging.getLogger()
        root.addHandler(handler)
        return lambda: root.removeHandler(handler)

    @staticmethod
    def _on_cache_event(event: Any) -> None:
        trace = _current.get()
        if trace is None:
            return
        name = type(event).__name__
        verb = _CACHE_VERBS.get(name, name.lower())
        subject = getattr(event, "key", None) or getattr(event, "name", None)
        summary = f"{verb} {subject}" if subject else verb
        waited = getattr(event, "waited", 0.0)
        if waited >= 0.001:
            summary += f" after waiting {waited * 1000:.0f}ms"
        detail = {
            column.name: getattr(event, column.name)
            for column in fields(event)
            if column.name not in _NEVER_RECORDED
        }
        trace.record("cache", summary, **detail)

    @staticmethod
    def _on_job_dispatched(event: Any) -> None:
        trace = _current.get()
        if trace is None:
            return
        envelope = event.envelope
        summary = f"dispatch {envelope.job} to {envelope.queue}"
        if event.deferred:
            summary += " after commit"
        if envelope.delay:
            summary += f" in {envelope.delay:g}s"
        trace.record(
            "job",
            summary,
            job=envelope.job,
            job_id=envelope.id,
            queue=envelope.queue,
            connection=event.connection,
            deferred=event.deferred,
            delay=envelope.delay,
        )


class _TraceHandler(logging.Handler):
    """Copies each log record onto the trace in effect, if there is one."""

    def emit(self, record: logging.LogRecord) -> None:
        """Record the line.

        Args:
            record: The record being logged.
        """
        trace = _current.get()
        if trace is None or record.name.startswith(ECHO_LOGGERS):
            return
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 — a bad format string is the caller's bug, not ours
            message = str(record.msg)
        trace.record(
            "log",
            f"{record.levelname} {record.name}: {message}",
            level=record.levelname,
            logger=record.name,
            exception=record.exc_info is not None,
        )


def _bounded(value: Any) -> Any:
    """Make *value* safe to keep: JSON-serialisable, and no larger than the limit.

    Args:
        value: What a caller or a source handed over.

    Returns:
        The plain value, or its clipped JSON rendering with the original length
        when that rendering exceeds :data:`STATEMENT_LIMIT`.
    """
    plain = _plain(value)
    rendered = json.dumps(plain, ensure_ascii=False, default=repr)
    if len(rendered) <= STATEMENT_LIMIT:
        return plain
    return f"{rendered[:STATEMENT_LIMIT]}… [{len(rendered)} chars]"


def _plain(value: Any) -> Any:
    """Reduce *value* to something ``json.dumps`` accepts.

    Args:
        value: Anything a driver or an event might carry.

    Returns:
        The value if it is already plain, a list or dict of plain values, or
        its ``repr`` otherwise.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _plain(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_plain(inner) for inner in value]
    return repr(value)


# -- binding --------------------------------------------------------------

_binding: Binding[Inspector] = Binding(
    "inspector",
    "enter keel.observability.inspector_lifespan(...) during startup",
)


def set_inspector(inspector: Inspector | None) -> None:
    """Install the process-wide inspector.

    Args:
        inspector: The inspector, or ``None`` to unbind.
    """
    _binding.set(inspector)


def current_inspector() -> Inspector:
    """Return the inspector in effect.

    Returns:
        The bound inspector.

    Raises:
        ConfigurationError: If none is bound.
    """
    return _binding.current()


def bound_inspector() -> Inspector | None:
    """Return the inspector in effect, or ``None`` when nothing is bound.

    Returns:
        The inspector, or ``None``.
    """
    try:
        return _binding.current()
    except ConfigurationError:
        return None


@contextmanager
def trace(name: str) -> Iterator[Trace | None]:
    """Record the block as one trace on the bound inspector, if there is one.

    The facade a middleware calls without caring whether an inspector was
    configured: no inspector, or a disabled one, yields ``None`` and records
    nothing.

    Args:
        name: What the work is.

    Yields:
        The open trace, or ``None``.
    """
    inspector = bound_inspector()
    if inspector is None:
        yield None
        return
    with inspector.trace(name) as opened:
        yield opened


@contextmanager
def use_inspector(inspector: Inspector) -> Iterator[Inspector]:
    """Override the bound inspector for the duration of a block.

    Args:
        inspector: The inspector to use.

    Yields:
        The same inspector.
    """
    with _binding.use(inspector) as bound:
        yield bound


@asynccontextmanager
async def inspector_lifespan(
    config: InspectorConfig,
    *,
    database: Database | None = None,
    events: EventDispatcher | None = None,
    logs: bool = True,
) -> AsyncIterator[Inspector]:
    """Bind an inspector for the life of the process, watching the sources given.

    Entered after the subsystems it watches are bound, since it needs the
    database's engine and the dispatcher they announce on. Restores whatever was
    bound before, like every other lifespan.

    Args:
        config: What to record. When disabled, nothing is subscribed and
            :func:`trace` yields ``None``.
        database: The database whose statements to record, if any.
        events: The dispatcher the cache and queue were given, if any.
        logs: Whether to record log lines.

    Yields:
        The bound inspector.
    """
    previous = _binding.peek()
    inspector = Inspector(config)
    inspector.watch(database=database, events=events, logs=logs)
    set_inspector(inspector)
    try:
        yield inspector
    finally:
        inspector.detach()
        set_inspector(previous)


__all__ = [
    "ECHO_LOGGERS",
    "STATEMENT_LIMIT",
    "SUMMARY_LIMIT",
    "Entry",
    "Inspector",
    "InspectorConfig",
    "Trace",
    "bound_inspector",
    "current_inspector",
    "current_trace",
    "inspector_lifespan",
    "set_inspector",
    "trace",
    "use_inspector",
]
