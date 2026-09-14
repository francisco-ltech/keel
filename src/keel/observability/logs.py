"""Structured logging, and the one hook that makes it carry context.

Four modules in Keel already call ``logging.getLogger(__name__)`` and none of
them knows this package exists. That is the constraint the design is built
around: **a log line has to carry the current correlation fields whoever emitted
it**, including a third-party library, without a single call site changing.

**Pattern: Strategy, selected by name.** :class:`JsonFormatter` and
:class:`TextFormatter` are two implementations of ``logging.Formatter`` and
:func:`configure_logging` picks one from configuration. Two implementors with
genuinely different audiences — a log aggregator and a developer's terminal — so
the seam is substance rather than a factory over one thing.

**No new dependency.** ``structlog`` is the obvious alternative and was
declined: Keel has two runtime dependencies, the job here is a formatter plus a
context hook, and taking a logging framework would push its vocabulary into
every application built on Keel and into every library that already logs through
the standard module. The standard module also happens to have the exact hook
this needs.

**The hook is** :func:`logging.setLogRecordFactory` **, not a**
``logging.Filter``. Both can attach fields, and the difference is *when*. A
factory runs inside ``Logger.makeRecord``, in the task that made the call, so
the fields are the ones in effect at the log statement. A filter runs on the
handler's side — which under ``QueueHandler``/``QueueListener``, the standard
way to keep logging off the event loop, is a different **thread** with an empty
context. The filter version would work in every test and lose the request id in
the one deployment that most needs it. A filter attached to the root *logger*
has a second, quieter failure: filters do not run for records that merely
propagate from a child, so ``keel.queue.failed``'s lines would carry nothing.

The factory chains over whatever was installed before it and is reinstalled
idempotently, so calling :func:`configure_logging` twice — a test, a reload —
does not stack wrappers.

**The one thing this reaches outside the root logger for is a server that
refuses to propagate.** ``uvicorn`` and ``gunicorn`` install their own plain
handler and set ``propagate = False``, so their lines — including the traceback
under "Exception in ASGI application" — would be the single unstructured class
in an otherwise JSON stream. :data:`ADOPTED_LOGGERS` says which, and why that is
worse than having no structure at all.

**What goes on a record, and what deliberately does not.** The correlation
fields, and the current principal's **id** as ``user_id``. Not its roles, which
are an authorization input with no diagnostic value per line, and emphatically
not its ``claims``: that mapping is untyped and populated by the application,
ADR 0007 offers "a tenant, a scope, a token id" as examples of what belongs in
it, and copying it onto every record would make the log aggregator a mirror of
whatever an author decided to stash on the principal. The id alone is the join
that answers "what happened to this user", it is already written into audit
columns and URLs, and it is a UUID — ADR 0008 removed the email claim from
``Identity`` for exactly the reason that keeps this to the id.
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final

from keel.auth.identity import current_identity
from keel.observability.config import LoggingConfig
from keel.support.correlation import EMPTY, RESERVED_FIELDS, correlation

if TYPE_CHECKING:
    from typing import TextIO

HANDLER_NAME: Final = "keel.observability"
"""The name given to the handler this module installs.

Named rather than remembered in a module global, so :func:`configure_logging`
can replace its own handler on a second call while leaving handlers somebody
else installed exactly where they are.
"""

CORRELATION_KEY: Final = "keel_correlation"
"""The record attribute the fields are attached under.

Namespaced, and that is not decoration. ``Logger.makeRecord`` raises ``KeyError``
when an ``extra=`` key collides with an attribute the record already has, so an
unqualified ``correlation`` would make ``logging.info(..., extra={"correlation":
...})`` — a perfectly ordinary line for an application with its own notion of the
word — start raising the moment Keel's factory is installed.
"""

TEXT_FIELD: Final = "keel_correlation_text"
"""Where :class:`TextFormatter` stashes its rendered fields, for the same reason."""

TEXT_FORMAT: Final = f"%(asctime)s %(levelname)-8s %(name)s %(message)s%({TEXT_FIELD})s"
"""The developer-facing line. Fields trail the message so it stays readable."""

ADOPTED_LOGGERS: Final[tuple[str, ...]] = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "gunicorn.error",
    "gunicorn.access",
)
"""Loggers that ship with their own handler and refuse to propagate.

Both servers install a plain handler on these and set ``propagate = False``, so
their output bypasses everything below — including the traceback under
``"Exception in ASGI application"``, which is the single line most worth joining
to a request id. One unstructured class of line in an otherwise JSON stream is
worse than none at all, because the shipper parses neither reliably: a stream is
one shape or it is not. :func:`configure_logging` takes their handlers off and
lets them propagate to the root, which is where this module's formatter is.

``uvicorn.access`` is in the list and is usually *not* adopted, which is the
point: :func:`_adopt` only takes over a logger that has handlers, and
``--no-access-log`` leaves that one with none. Listing it means an access log
somebody deliberately left on comes out as JSON like everything else, without
the flag quietly stopping working.

Naming them costs nothing when the servers are absent — ``getLogger`` on a name
nothing has used creates an inert placeholder and imports no web framework.
"""

_base_factory: Callable[..., logging.LogRecord] | None = None
"""Whatever made log records before this module wrapped it."""

_NOT_EXTRA: Final[frozenset[str]] = (
    frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__)
    | {"asctime", "message", "taskName"}
    | {CORRELATION_KEY, TEXT_FIELD, "color_message"}
    | RESERVED_FIELDS
)
"""Record attributes that did not come from ``extra=``, and never become a field.

Derived from a probe record rather than listed, so a new member in a future
``LogRecord`` does not silently start appearing in every line — the same reason
the repo loops over ``__slots__`` instead of remembering two places. ``asctime``
and ``message`` are added later by ``Formatter`` and are not on a fresh record;
``taskName`` is named because it is absent before Python 3.12.
:data:`~keel.support.correlation.RESERVED_FIELDS` joins them because a formatter
writes those itself, which is the same reason ``correlate`` refuses the names.

``color_message`` is the one name here that belongs to somebody else. Uvicorn
passes an ANSI-escaped copy of its own message through ``extra=`` on every line
it writes, and since :data:`ADOPTED_LOGGERS` deliberately routes those lines
here, taking the duplicate with them is this module's consequence to own.
"""


def _ambient() -> Mapping[str, str]:
    """Return the fields a record created right now should carry.

    Returns:
        The correlation fields, plus ``user_id`` when a principal is bound and
        nothing has already bound that name — an explicit
        ``correlate(user_id=...)`` is a deliberate statement about who the work
        is for and outranks the ambient identity.
    """
    fields = correlation()
    identity = current_identity()
    if identity is None or "user_id" in fields:
        return fields
    return {**fields, "user_id": str(identity.id)}


def _correlating_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
    """Make a log record and attach the current correlation fields to it.

    Args:
        *args: Whatever ``Logger.makeRecord`` passes through.
        **kwargs: The same.

    Returns:
        The record, carrying its fields under :data:`CORRELATION_KEY`.
    """
    record = (_base_factory or logging.LogRecord)(*args, **kwargs)
    # Through __dict__ because that is how logging's own `extra=` works, and
    # because LogRecord declares no such attribute for a type checker to accept.
    record.__dict__[CORRELATION_KEY] = _ambient()
    return record


def _reaches_us(factory: Callable[..., logging.LogRecord]) -> bool:
    """Whether *factory* already runs :func:`_correlating_factory` inside itself.

    Asked by making one throwaway record and looking for
    :data:`CORRELATION_KEY` on it, rather than by inspecting the callable: a
    chaining factory closes over its predecessor, and there is nothing to read.

    The question has to be asked, and asking it cheaply is why. A vendor agent —
    sentry, ddtrace — installs itself by capturing whatever is installed and
    wrapping it, so after ``configure_logging`` the chain is *vendor over ours*.
    Re-capturing the vendor as the base on a second call would then make the two
    wrap each other, and the next log call would recurse until the stack ran out.

    Args:
        factory: The installed factory to probe.

    Returns:
        ``True`` if this module is already somewhere in the chain.
    """
    probe = factory("keel.observability.probe", logging.DEBUG, __file__, 0, "", None, None)
    return CORRELATION_KEY in probe.__dict__


def _install_record_factory() -> None:
    """Make every new log record carry the correlation fields.

    The first guard is not belt-and-braces: without it a second call would set
    :data:`_base_factory` to :func:`_correlating_factory` itself, and the next
    log call would recurse until the stack ran out. It also means a test that
    restores the original factory gets a working installation back on the next
    call rather than a silently skipped one.

    The second is :func:`_reaches_us`, which is what stops a second call from
    unlinking a third-party factory installed after the first. Between them the
    three cases are covered: nothing installed since — do nothing; a vendor
    wrapped us — leave it alone, both sets of fields already arrive; a vendor
    replaced us outright — chain over it, so neither is lost.
    """
    global _base_factory
    installed = logging.getLogRecordFactory()
    if installed is _correlating_factory or _reaches_us(installed):
        return
    _base_factory = installed
    logging.setLogRecordFactory(_correlating_factory)


def _extras(record: logging.LogRecord) -> Iterator[tuple[str, Any]]:
    """Yield the attributes ``extra=`` put on *record*, and nothing else.

    ``RESERVED_FIELDS`` exists because a field that silently vanished from every
    log line is invisible, and dropping ``extra=`` is the same mistake with the
    same consequence: ``logging.info("x", extra={"shipment": "S1"})`` is the most
    ordinary structured-logging call there is, and a formatter that ignored it
    would make the two halves of this subsystem apply opposite rules to it.

    Args:
        record: The record to read.

    Yields:
        Name/value pairs, in the order they were attached.
    """
    for name, value in record.__dict__.items():
        if name not in _NOT_EXTRA:
            yield name, value


class JsonFormatter(logging.Formatter):
    """One JSON object per line, for a log aggregator to index.

    The structural members are the formatter's own and nothing can displace
    them: a name in :data:`keel.support.correlation.RESERVED_FIELDS` is skipped
    outright, which covers ``exception`` — written last, and so the one member
    ``setdefault`` alone would not have protected — as well as the four written
    first. Values that will not serialise are rendered with ``str`` rather than
    raising: a logging call that throws inside a failure handler is how one lost
    line becomes an outage.

    That promise is why ``ensure_ascii`` is left at its default. A lone surrogate
    — which is what ``surrogateescape`` produces from any non-UTF-8 byte in a
    path or an environment value — serialises happily and then raises
    ``UnicodeEncodeError`` in ``stream.write``, losing the whole line at the one
    moment it mattered. Escaping also puts ``U+2028``/``U+2029`` beyond the reach
    of a shipper that splits on Unicode line boundaries and would otherwise read
    one record as several.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as a single JSON object.

        Correlation fields are written before ``extra=`` attributes, so an
        ambient field wins a name collision: it is the one the reader is joining
        on across processes.

        Args:
            record: The record to render.

        Returns:
            One line of JSON. ``json.dumps`` escapes newlines, so a multi-line
            message stays one line and a value cannot forge a second record.
        """
        payload: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, tz=UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for name, value in record.__dict__.get(CORRELATION_KEY, EMPTY).items():
            if name not in RESERVED_FIELDS:
                payload.setdefault(name, value)
        for name, value in _extras(record):
            payload.setdefault(name, value)
        if record.exc_info is not None:
            payload["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exception"] = record.exc_text
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    """A readable line with the fields appended, for a terminal.

    Not a development-only convenience: a single-process deployment whose logs a
    human tails is a real deployment, and JSON read by eye is how people stop
    reading their logs.

    Its timestamp is ``logging``'s own — **local time**, where
    :class:`JsonFormatter` writes UTC. Deliberate rather than an oversight: a
    machine correlating across hosts needs one zone, and a person reading a
    terminal needs the clock on their wall.
    """

    def __init__(self) -> None:
        super().__init__(TEXT_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as a line, with its fields after the message.

        ``extra=`` attributes trail the correlation fields, for the reason
        :func:`_extras` gives: a developer reading a terminal and a query reading
        the aggregator must not be shown different facts about the same call.

        Args:
            record: The record to render.

        Returns:
            The formatted line, exception traceback included as usual.
        """
        fields = dict(record.__dict__.get(CORRELATION_KEY, EMPTY))
        for name, value in _extras(record):
            fields.setdefault(name, value)
        rendered = " ".join(f"{name}={value}" for name, value in fields.items())
        record.__dict__[TEXT_FIELD] = f" {rendered}" if rendered else ""
        return super().format(record)


_STRATEGIES: Final[dict[str, Callable[[], logging.Formatter]]] = {
    "json": JsonFormatter,
    "text": TextFormatter,
}
"""The Strategy table :class:`~keel.observability.config.LoggingConfig` selects from."""


def _adopt(names: tuple[str, ...]) -> None:
    """Route *names* through the root handler instead of their own.

    **Only a logger that has handlers of its own is touched**, and that
    restriction is load-bearing rather than tidiness. ``uvicorn`` decides
    whether to write an access line at all by asking
    ``getLogger("uvicorn.access").hasHandlers()``, and ``--no-access-log`` says
    no by emptying that logger and setting ``propagate = False``. Turning
    propagation back on there would answer yes again and resurrect every line
    the flag exists to suppress — in the *same* shape as the application's own,
    which is worse than the second shape this function is here to remove. So:
    take over output that exists, never create output that does not.

    Args:
        names: The loggers to take over. See :data:`ADOPTED_LOGGERS` for why
            these in particular, and why naming an absent one is free.
    """
    for name in names:
        adopted = logging.getLogger(name)
        if not adopted.handlers:
            continue
        for installed in list(adopted.handlers):
            adopted.removeHandler(installed)
        adopted.propagate = True


def configure_logging(
    config: LoggingConfig | None = None,
    *,
    stream: TextIO | None = None,
) -> logging.Handler:
    """Install Keel's log handler and make every record carry its context.

    Call this once, as early as a process can — in an application factory, or in
    a worker's ``main`` — and before anything that might log. It is a function
    rather than a lifespan on purpose: the interesting failures happen *during*
    start-up, and a lifespan configures logging too late to record them.

    Handlers installed by anything else are left alone; only a previous handler
    from this function is replaced, so calling it twice does not double every
    line. Nothing is removed from the **root** logger that this did not put
    there. :data:`ADOPTED_LOGGERS` is the deliberate exception — a server that
    refuses to propagate is the one thing that can put a second shape in the
    stream, so its handlers are taken off.

    Args:
        config: What to install. Defaults to :class:`LoggingConfig`'s defaults.
        stream: Where lines go. Defaults to ``sys.stderr``, matching
            ``logging.basicConfig`` and leaving ``stdout`` free for a process
            that writes output there. A test passes a ``StringIO``.

    Returns:
        The installed handler, so a caller can add a filter to it or take it off
        again.
    """
    settings = config or LoggingConfig()
    _install_record_factory()
    _adopt(ADOPTED_LOGGERS)

    handler: logging.Handler = logging.StreamHandler(stream or sys.stderr)
    handler.set_name(HANDLER_NAME)
    handler.setFormatter(_STRATEGIES[settings.formatter]())

    root = logging.getLogger()
    for previous in [known for known in root.handlers if known.get_name() == HANDLER_NAME]:
        root.removeHandler(previous)
    root.addHandler(handler)
    root.setLevel(settings.level.upper())
    return handler


__all__ = [
    "ADOPTED_LOGGERS",
    "CORRELATION_KEY",
    "HANDLER_NAME",
    "TEXT_FIELD",
    "TEXT_FORMAT",
    "JsonFormatter",
    "TextFormatter",
    "configure_logging",
]
