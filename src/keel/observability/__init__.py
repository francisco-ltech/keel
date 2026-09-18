"""The observability subsystem.

Three imports cover the whole of it. One at start-up::

    from keel.observability import LoggingConfig, configure_logging

    configure_logging(LoggingConfig.from_env())

and one wherever a value worth carrying becomes known::

    from keel.observability import correlate

    with correlate(request_id=request_id):
        ...

Everything downstream of that second block — this process's log lines, the jobs
it dispatches, and the worker that eventually runs them — carries the field,
without a single call site being told about it.

**There is no manager, no driver seam and no fake**, and each absence is a
decision. There is no backend to swap: a formatter and a context variable are
the whole implementation, and a ``Manager[T]`` over two formatters would be the
pattern-itis ADR 0000's counter-rule exists to refuse. There is no fake because
a test asserts on the real thing — bind a field, read it back; install the
handler on a ``StringIO``, read the line. A recording double would be a second
implementation of a formatter, which is the point at which a contract suite
would start earning its place and not before.

And one in a readiness endpoint, which is handed the dependencies to ask rather
than discovering them — :mod:`keel.observability.health` says why::

    report = await probe({"database": check_database, "cache": check_cache})

And, in development, one around each request, so the queries, cache calls,
dispatches and log lines it produced can be read back as one timeline —
:mod:`keel.observability.inspector`::

    with trace(f"{method} {path}") as recorded:
        ...

:func:`correlate` and :func:`correlation` live in
:mod:`keel.support.correlation`, because the queue seals those fields onto every
envelope and a tenant is not a logging concept. They are re-exported here the
way ``keel.cache`` re-exports the sentinels: an application doing the common
thing should need one import.
"""

from __future__ import annotations

from keel.observability.config import FORMATTERS, LEVELS, LoggingConfig
from keel.observability.health import (
    DEFAULT_TIMEOUT,
    PROBE_KEY,
    Check,
    CheckResult,
    HealthReport,
    check_cache,
    check_database,
    check_queue,
    check_tokens,
    probe,
)
from keel.observability.inspector import (
    Entry,
    Inspector,
    InspectorConfig,
    Trace,
    bound_inspector,
    current_inspector,
    current_trace,
    inspector_lifespan,
    set_inspector,
    trace,
    use_inspector,
)
from keel.observability.logs import (
    ADOPTED_LOGGERS,
    CORRELATION_KEY,
    HANDLER_NAME,
    JsonFormatter,
    TextFormatter,
    configure_logging,
)
from keel.support.correlation import (
    RESERVED_FIELDS,
    SECRET_MARKERS,
    correlate,
    correlation,
    correlation_fields,
)

__all__ = [
    "ADOPTED_LOGGERS",
    "CORRELATION_KEY",
    "DEFAULT_TIMEOUT",
    "FORMATTERS",
    "HANDLER_NAME",
    "LEVELS",
    "PROBE_KEY",
    "RESERVED_FIELDS",
    "SECRET_MARKERS",
    "Check",
    "CheckResult",
    "Entry",
    "HealthReport",
    "Inspector",
    "InspectorConfig",
    "JsonFormatter",
    "LoggingConfig",
    "TextFormatter",
    "Trace",
    "bound_inspector",
    "check_cache",
    "check_database",
    "check_queue",
    "check_tokens",
    "configure_logging",
    "correlate",
    "correlation",
    "correlation_fields",
    "current_inspector",
    "current_trace",
    "inspector_lifespan",
    "probe",
    "set_inspector",
    "trace",
    "use_inspector",
]
