"""Correlation context and structured logging.

The claim this file exists to pin is one sentence: **a log line carries the
fields in effect when it was written, whoever wrote it, and a dispatched job
carries them to whichever process runs it.**

The second half of that is tested here only as far as the envelope; the worker
end of it is in ``test_worker.py``, against a real Redis, because a context that
survives ``dict`` and not ``json`` would pass every assertion below.
"""

from __future__ import annotations

import io
import json
import logging
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, ClassVar

import anyio
import pytest

from keel.auth import Identity, acting_as
from keel.exceptions import ConfigurationError
from keel.observability import (
    ADOPTED_LOGGERS,
    CORRELATION_KEY,
    RESERVED_FIELDS,
    SECRET_MARKERS,
    JsonFormatter,
    LoggingConfig,
    TextFormatter,
    configure_logging,
    correlate,
    correlation,
    correlation_fields,
    logs,
)
from keel.queue import Job, dispatch, dispatch_many
from keel.queue.envelope import Envelope
from keel.queue.failed import FailedJob, encode_envelope
from keel.testing import fake_queue

pytestmark = [pytest.mark.anyio]


# -- the correlation context ----------------------------------------------


def test_nothing_is_bound_by_default() -> None:
    assert correlation() == {}


def test_binding_merges_with_the_surrounding_scope() -> None:
    """The decision that separates this from `acting_as`.

    Replacing would make the worker's own `job_id` binding erase the request id
    the envelope carried — the exact field the mechanism exists for.
    """
    with correlate(request_id="req-1"):
        assert correlation() == {"request_id": "req-1"}
        with correlate(job_id="job-1"):
            assert correlation() == {"request_id": "req-1", "job_id": "job-1"}


def test_an_inner_scope_may_overwrite_and_the_outer_value_comes_back() -> None:
    with correlate(tenant="acme"):
        assert correlation()["tenant"] == "acme"
        with correlate(tenant="globex"):
            assert correlation()["tenant"] == "globex"
        assert correlation()["tenant"] == "acme"


def test_the_binding_is_restored_on_the_exception_path() -> None:
    with correlate(request_id="outer"):
        with pytest.raises(RuntimeError), correlate(request_id="inner"):
            raise RuntimeError("boom")
        assert correlation()["request_id"] == "outer"


async def test_concurrent_tasks_do_not_see_each_others_fields() -> None:
    """The property a module-level dict would fail, and a ContextVar gives free."""
    seen: dict[str, str] = {}

    async def work(name: str) -> None:
        with correlate(request_id=name):
            await anyio.sleep(0.01)
            seen[name] = correlation()["request_id"]

    async with anyio.create_task_group() as tasks:
        for name in ("a", "b", "c"):
            tasks.start_soon(work, name)

    assert seen == {"a": "a", "b": "b", "c": "c"}
    assert correlation() == {}


def test_a_non_string_value_is_coerced() -> None:
    """Coerced here, where the mistake is, rather than at the log call frames away."""
    identifier = uuid.uuid4()
    with correlate(attempt=3, tenant=identifier):
        assert correlation() == {"attempt": "3", "tenant": str(identifier)}


def test_a_none_value_is_dropped_and_leaves_an_outer_value_alone() -> None:
    """`correlate(tenant=maybe)` means "nothing to add", not "the tenant is None"."""
    with correlate(tenant="acme"):
        assert correlation() == {"tenant": "acme"}
        with correlate(tenant=None, request_id="req-1"):
            assert correlation() == {"tenant": "acme", "request_id": "req-1"}


@pytest.mark.parametrize("name", sorted(RESERVED_FIELDS))
def test_every_reserved_field_is_refused(name: str) -> None:
    """Parametrised over the constant, not over a copy of it.

    A hand-written list went stale the moment someone edited the tuple: dropping
    `time` and `message` from `RESERVED_FIELDS` was a one-line mutation the suite
    did not notice. Looping over the thing itself is the same fix as looping over
    `__slots__` rather than remembering both places.
    """
    with pytest.raises(ConfigurationError, match="structured log record"), correlate(**{name: "x"}):
        pass


@pytest.mark.parametrize("marker", SECRET_MARKERS)
def test_every_credential_marker_is_refused_as_a_substring(marker: str) -> None:
    """A check on the *name*, which catches the reflex and nothing more.

    A secret bound under an innocent name still reaches the log; what stops that
    is that nothing binds a field unless an author wrote it down. Parametrised
    over the tuple for the reason above — dropping `secret` and `credential` from
    it was two more mutations nothing caught.
    """
    with (
        pytest.raises(ConfigurationError, match="names a credential"),
        correlate(**{f"inbound_{marker}": "x"}),
    ):
        pass


def test_a_session_id_is_a_correlation_field_rather_than_a_credential() -> None:
    """Deliberately not refused, and the absence is worth a test.

    It is the join between a sequence of requests. Refusing it would not stop
    anyone recording it — it would push them to spell it `sid`, which is the
    unreadable version of the same field.
    """
    with correlate(session_id="sess-1"):
        assert correlation()["session_id"] == "sess-1"


def test_fields_arriving_as_data_are_dropped_rather_than_raised() -> None:
    """An envelope sealed by another release must not be able to kill a worker."""
    cleaned = correlation_fields({"request_id": "req-1", "token": "secret"}, refuse=False)

    assert cleaned == {"request_id": "req-1"}


# -- structured logging ----------------------------------------------------


@pytest.fixture
def logging_state() -> Iterator[None]:
    """Put the process-wide logging configuration back the way it was.

    `configure_logging` installs a handler and a record factory for the whole
    process, which is the point of it — so a test that did not restore all of it
    would leak its stream into every test that ran afterwards. That now includes
    the server loggers it adopts and the module's memory of what it chains to,
    which is what makes the order tests run in stop mattering.
    """
    root = logging.getLogger()
    handlers = list(root.handlers)
    level = root.level
    factory = logging.getLogRecordFactory()
    chained = logs._base_factory
    servers = {
        name: (list(logging.getLogger(name).handlers), logging.getLogger(name).propagate)
        for name in ADOPTED_LOGGERS
    }
    try:
        yield
    finally:
        for installed in list(root.handlers):
            root.removeHandler(installed)
        for previous in handlers:
            root.addHandler(previous)
        root.setLevel(level)
        logging.setLogRecordFactory(factory)
        logs._base_factory = chained
        for name, (server_handlers, propagate) in servers.items():
            server = logging.getLogger(name)
            server.handlers = list(server_handlers)
            server.propagate = propagate


def lines(stream: io.StringIO) -> list[str]:
    """Return the non-empty lines written to *stream*."""
    return [line for line in stream.getvalue().splitlines() if line]


def only(stream: io.StringIO) -> dict[str, Any]:
    """Return the single JSON record written to *stream*."""
    written = lines(stream)
    assert len(written) == 1, written
    parsed: dict[str, Any] = json.loads(written[0])
    return parsed


def test_a_module_that_never_heard_of_this_still_carries_the_fields(
    logging_state: None,
) -> None:
    """The requirement. `keel.queue.failed` calls `getLogger` and nothing else.

    A `logging.Filter` on the root *logger* would fail this: filters do not run
    for a record that merely propagates up from a child logger.
    """
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)

    with correlate(request_id="req-1"):
        logging.getLogger("keel.queue.failed").error("could not record a dead letter")

    record = only(stream)
    assert record["request_id"] == "req-1"
    assert record["logger"] == "keel.queue.failed"
    assert record["level"] == "ERROR"
    assert record["message"] == "could not record a dead letter"


def test_a_json_record_is_one_line_even_when_the_message_is_not(
    logging_state: None,
) -> None:
    """A value cannot forge a second record, which is what log injection is."""
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)

    with correlate(request_id="a\nb"):
        logging.getLogger("test").info("first\nsecond")

    record = only(stream)
    assert record["message"] == "first\nsecond"
    assert record["request_id"] == "a\nb"


def test_an_exception_is_rendered_into_the_record(logging_state: None) -> None:
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)

    try:
        raise ValueError("bad")
    except ValueError:
        logging.getLogger("test").exception("it failed")

    record = only(stream)
    assert "ValueError: bad" in record["exception"]


def test_configuring_twice_does_not_double_every_line(logging_state: None) -> None:
    """Idempotent: a reload, or a factory called twice in a test, must not stack."""
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)
    configure_logging(LoggingConfig(), stream=stream)

    logging.getLogger("test").info("once")

    assert len(lines(stream)) == 1


def test_a_handler_somebody_else_installed_is_left_alone(logging_state: None) -> None:
    """Only this module's own handler is replaced; nothing else is removed."""
    other = logging.StreamHandler(io.StringIO())
    root = logging.getLogger()
    root.addHandler(other)

    configure_logging(LoggingConfig(), stream=io.StringIO())

    assert other in root.handlers


def test_a_uvicorn_error_line_is_formatted_like_every_other_line(
    logging_state: None,
) -> None:
    """The line most worth joining, and the only one that was not JSON.

    `uvicorn.error` propagates to `uvicorn`, which ships with `propagate=False`
    and its own plain handler — so the traceback under "Exception in ASGI
    application" went out in a second shape. `--no-access-log` removed uvicorn's
    *structured* half and left exactly this one, which is worse than leaving
    both: a stream is one shape or it is not.
    """
    server = logging.getLogger("uvicorn")
    server.addHandler(logging.StreamHandler(io.StringIO()))
    server.propagate = False
    stream = io.StringIO()

    configure_logging(LoggingConfig(), stream=stream)

    with correlate(request_id="req-1"):
        try:
            raise RuntimeError("the handler blew up")
        except RuntimeError:
            logging.getLogger("uvicorn.error").exception("Exception in ASGI application")

    record = only(stream)
    assert record["logger"] == "uvicorn.error"
    assert record["request_id"] == "req-1"
    assert "RuntimeError: the handler blew up" in record["exception"]


@pytest.mark.parametrize("name", ADOPTED_LOGGERS)
def test_every_adopted_logger_is_handed_to_the_root(name: str, logging_state: None) -> None:
    """Parametrised over the constant, so adding a name cannot forget a test."""
    server = logging.getLogger(name)
    server.addHandler(logging.StreamHandler(io.StringIO()))
    server.propagate = False

    configure_logging(LoggingConfig(), stream=io.StringIO())

    assert server.handlers == []
    assert server.propagate is True


@pytest.mark.parametrize("name", ADOPTED_LOGGERS)
def test_a_silenced_server_logger_is_left_silenced(name: str, logging_state: None) -> None:
    """`--no-access-log` must keep working, and adopting naively broke it.

    Uvicorn asks `getLogger("uvicorn.access").hasHandlers()` to decide whether
    to write an access line at all, and the flag answers no by emptying that
    logger and setting `propagate = False`. Turning propagation back on answers
    yes again — resurrecting every line the flag suppresses, in the same shape
    as `app/observability.py`'s own, which is a worse duplicate than the plain
    one this whole mechanism exists to remove.
    """
    server = logging.getLogger(name)
    server.handlers = []
    server.propagate = False

    configure_logging(LoggingConfig(), stream=io.StringIO())

    assert server.propagate is False
    assert server.hasHandlers() is False


def test_a_factory_installed_after_ours_survives_a_second_configure(
    logging_state: None,
) -> None:
    """A vendor agent — sentry, ddtrace — installs a chaining factory of its own.

    Capturing the base factory once meant a second `configure_logging` put ours
    back over the *original*, dropping the vendor's link and its fields from
    every record, silently.

    Blindly re-capturing is not the fix, and this test is the reason: a vendor
    wraps whatever it finds, so re-capturing it makes the two wrap each other
    and the next log call is a `RecursionError`. Detecting that we are already
    in the chain, and leaving it alone, is.
    """
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)
    ours = logging.getLogRecordFactory()

    def vendor(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = ours(*args, **kwargs)
        record.__dict__["trace_id"] = "trace-9"
        return record

    logging.setLogRecordFactory(vendor)
    configure_logging(LoggingConfig(), stream=stream)

    with correlate(request_id="req-1"):
        logging.getLogger("test").info("still chained")

    record = only(stream)
    assert record["trace_id"] == "trace-9"
    assert record["request_id"] == "req-1"


def test_a_factory_that_replaced_ours_outright_is_chained_over_rather_than_lost(
    logging_state: None,
) -> None:
    """The other half: something installed a factory that does *not* call ours.

    Then there is nothing to preserve by standing aside, and the right move is
    to wrap it — so the next `configure_logging` restores the correlation fields
    without discarding whatever the replacement adds.
    """

    def replacement(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = logging.LogRecord(*args, **kwargs)
        record.__dict__["trace_id"] = "trace-9"
        return record

    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)
    logging.setLogRecordFactory(replacement)
    configure_logging(LoggingConfig(), stream=stream)

    with correlate(request_id="req-1"):
        logging.getLogger("test").info("chained over")

    record = only(stream)
    assert record["trace_id"] == "trace-9"
    assert record["request_id"] == "req-1"


def test_the_factory_is_never_chained_to_itself(logging_state: None) -> None:
    """The guard that stops `configure_logging` twice recursing until the stack goes.

    Without it the second call captures our own factory as the base, and the
    next log call is a `RecursionError` rather than a line.
    """
    configure_logging(LoggingConfig(), stream=io.StringIO())
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)

    logging.getLogger("test").info("no recursion")

    assert only(stream)["message"] == "no recursion"


@pytest.mark.parametrize("name", sorted(RESERVED_FIELDS))
def test_no_structural_member_can_be_displaced(name: str) -> None:
    """The formatter's own defence, behind `RESERVED_FIELDS`.

    `correlate(level=...)` is refused, so this reaches the formatter the only
    way it can — a record built by hand, as an older release's envelope might.
    `exception` is the member that makes the parametrisation worth having: it is
    written *last*, so `setdefault` alone would have let a forged one through on
    any line that carried no traceback.
    """
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "hello", None, None)
    record.__dict__[CORRELATION_KEY] = {name: "FORGED", "request_id": "req-1"}

    parsed = json.loads(JsonFormatter().format(record))

    assert parsed.get(name) != "FORGED"
    assert parsed["level"] == "INFO"
    assert parsed["request_id"] == "req-1"


def test_a_value_carrying_a_lone_surrogate_still_produces_a_line(logging_state: None) -> None:
    """`JsonFormatter` promises a log call cannot throw. This is where it did.

    A lone surrogate is what `surrogateescape` makes of any non-UTF-8 byte — in
    a path, in an environment value — and `ensure_ascii=False` emitted it
    happily, leaving `stream.write` to raise `UnicodeEncodeError` and `logging`
    to swallow the whole line. A `StringIO` would not catch this: the failure is
    in the encode, so the stream under test has to be a real one.
    """
    raw = io.BytesIO()
    stream = io.TextIOWrapper(raw, encoding="utf-8", write_through=True)
    configure_logging(LoggingConfig(), stream=stream)

    with correlate(path="/srv/data/caf\udce9"):
        logging.getLogger("test").info("read a path")

    stream.flush()
    parsed = json.loads(raw.getvalue().decode("utf-8"))
    assert parsed["path"] == "/srv/data/caf\udce9"
    assert parsed["message"] == "read a path"


def test_a_unicode_line_separator_cannot_forge_a_record(logging_state: None) -> None:
    """U+2028 is a line boundary to anything that splits the way Unicode says.

    `json.dumps` does not treat it as one, so a shipper that does would read a
    single record as two — the same injection `\\n` was already escaped to
    prevent, in the spelling that survived escaping.
    """
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)

    logging.getLogger("test").info("before\u2028after")

    written = lines(stream)
    assert len(written) == 1
    assert "\u2028" not in written[0]
    assert json.loads(written[0])["message"] == "before\u2028after"


def test_extra_fields_reach_the_line(logging_state: None) -> None:
    """The most ordinary structured-logging call there is, and it was dropped.

    `RESERVED_FIELDS` exists because a field that silently vanished from every
    log line is invisible. A formatter that read only the correlation key made
    exactly that mistake about `extra=`, so the two halves of this subsystem
    were applying opposite rules to the same error.
    """
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)

    with correlate(request_id="req-1"):
        logging.getLogger("test").info("indexed", extra={"shipment": "S1", "items": 3})

    record = only(stream)
    assert record["shipment"] == "S1"
    assert record["items"] == 3
    assert record["request_id"] == "req-1"


def test_an_extra_field_cannot_displace_a_structural_member(logging_state: None) -> None:
    """Merging `extra=` must not weaken the lock the correlation half has."""
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)

    logging.getLogger("test").warning("careful", extra={"level": "FORGED", "time": "never"})

    record = only(stream)
    assert record["level"] == "WARNING"
    assert record["time"] != "never"


def test_a_correlation_field_outranks_an_extra_of_the_same_name(logging_state: None) -> None:
    """The ambient field is the one a reader joins on across processes."""
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)

    with correlate(tenant="acme"):
        logging.getLogger("test").info("served", extra={"tenant": "globex"})

    assert only(stream)["tenant"] == "acme"


def test_extra_named_correlation_does_not_break_the_log_call(logging_state: None) -> None:
    """`makeRecord` raises `KeyError` when `extra=` names an existing attribute.

    An unqualified `correlation` attribute therefore turned a working log call
    into an exception the moment Keel's factory was installed — a logging call
    that can raise where it could not before, which is the failure mode this
    whole module is written to avoid.
    """
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)

    logging.getLogger("test").info("shipped", extra={"correlation": "an app's own word"})

    assert only(stream)["correlation"] == "an app's own word"


def test_the_text_formatter_shows_extra_fields_too(logging_state: None) -> None:
    """A terminal and an aggregator must not be told different things."""
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "hello", None, None)
    record.__dict__[CORRELATION_KEY] = {"request_id": "req-1"}
    record.__dict__["shipment"] = "S1"

    line = TextFormatter().format(record)

    assert line.endswith("hello request_id=req-1 shipment=S1")


def test_the_text_formatter_puts_the_fields_after_the_message() -> None:
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "hello", None, None)
    record.__dict__[CORRELATION_KEY] = {"request_id": "req-1"}

    line = TextFormatter().format(record)

    assert line.endswith("hello request_id=req-1")


def test_the_bound_principals_id_reaches_the_record(logging_state: None) -> None:
    """The id joins the logs to a user. Roles and claims deliberately do not.

    `claims` is untyped and application-populated — ADR 0007 offers "a token id"
    as an example of what lives there — so copying it onto every line would make
    the aggregator a mirror of whatever an author stashed on the principal.
    """
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)
    who = Identity(
        id=uuid.uuid4(),
        roles=frozenset({"admin"}),
        claims={"token_id": "tok-should-not-be-logged"},
    )

    with acting_as(who):
        logging.getLogger("test").info("acted")

    written = lines(stream)[0]
    assert json.loads(written)["user_id"] == str(who.id)
    assert "admin" not in written
    assert "tok-should-not-be-logged" not in written


def test_an_explicit_user_id_outranks_the_bound_principal(logging_state: None) -> None:
    stream = io.StringIO()
    configure_logging(LoggingConfig(), stream=stream)

    with acting_as(Identity(id=uuid.uuid4())), correlate(user_id="impersonated"):
        logging.getLogger("test").info("acted")

    assert only(stream)["user_id"] == "impersonated"


def test_an_unconfigured_process_still_formats(logging_state: None) -> None:
    """A record made before the factory was installed has no fields attached."""
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "hello", None, None)

    parsed = json.loads(JsonFormatter().format(record))

    assert parsed["message"] == "hello"


@pytest.mark.parametrize("value", ["shouting", "JSONL"])
def test_configuration_is_refused_at_load(value: str) -> None:
    with pytest.raises(ConfigurationError):
        LoggingConfig(level=value, formatter=value)


def test_configuration_reads_the_environment() -> None:
    config = LoggingConfig.from_env({"LOG_LEVEL": "debug", "LOG_FORMAT": "TEXT"})

    assert config == LoggingConfig(level="DEBUG", formatter="text")


# -- the envelope loop -----------------------------------------------------


@dataclass(frozen=True, slots=True)
class Correlated(Job):
    """A job that exists to be dispatched and inspected."""

    max_attempts: ClassVar[int] = 1

    marker: str

    async def handle(self) -> None:
        """Nothing; this job is never run."""


async def test_a_dispatch_seals_the_ambient_correlation_onto_the_envelope() -> None:
    """The promise `Envelope.context`'s docstring has been making since Phase 3."""
    with fake_queue() as queued, correlate(request_id="req-1", tenant="acme"):
        await dispatch(Correlated(marker="one"))

    assert queued.assert_pushed(Correlated).context == {
        "request_id": "req-1",
        "tenant": "acme",
    }


async def test_a_job_dispatched_from_a_handler_does_not_inherit_the_parents_job_id() -> None:
    """A child's envelope must not claim to be its parent.

    `Worker._process` binds `job` and `job_id` around an attempt, so a dispatch
    from inside a handler sealed the *parent's* id onto the child. Log lines hid
    it — the child worker overwrites both before the handler writes anything —
    but `keel_failed_jobs.context` kept it, so a dead letter named a `job_id`
    that was not the row's. Renaming makes it the field an operator wanted.
    """
    parent = correlate(job="parent.job", job_id="job-parent")
    with fake_queue() as queued, correlate(request_id="req-1"), parent:
        await dispatch(Correlated(marker="child"))

    context = queued.assert_pushed(Correlated).context
    assert context == {
        "request_id": "req-1",
        "parent_job": "parent.job",
        "parent_job_id": "job-parent",
    }


async def test_a_grandchild_names_its_own_parent_rather_than_the_first_one() -> None:
    """The rename has to overwrite an inherited `parent_job_id`, not sit beside it."""
    parent = correlate(job_id="job-parent")
    with fake_queue() as queued, correlate(parent_job_id="job-grandparent"), parent:
        await dispatch(Correlated(marker="grandchild"))

    assert queued.assert_pushed(Correlated).context == {"parent_job_id": "job-parent"}


async def test_a_dispatch_outside_any_scope_carries_nothing() -> None:
    with fake_queue() as queued:
        await dispatch(Correlated(marker="two"))

    assert queued.assert_pushed(Correlated).context == {}


async def test_an_explicit_override_replaces_the_ambient_context() -> None:
    """An override parameter, like `on=` and `connection=`, not a merge.

    It is also the only spelling that can say "carry nothing", which a merge
    could not express — and it is named `context_override` rather than `context`
    so that the discarding is visible at the call site instead of in a docstring.
    """
    with fake_queue() as queued, correlate(request_id="req-1"):
        await dispatch(Correlated(marker="three"), context_override={"reason": "backfill"})

    assert queued.assert_pushed(Correlated).context == {"reason": "backfill"}


async def test_an_explicit_override_may_be_empty() -> None:
    with fake_queue() as queued, correlate(request_id="req-1"):
        await dispatch(Correlated(marker="four"), context_override={})

    assert queued.assert_pushed(Correlated).context == {}


async def test_a_credential_cannot_be_dispatched_as_context() -> None:
    with fake_queue(), pytest.raises(ConfigurationError, match="names a credential"):
        await dispatch(Correlated(marker="five"), context_override={"access_token": "hunter2"})


async def test_every_envelope_in_a_bulk_dispatch_carries_the_context() -> None:
    with fake_queue() as queued, correlate(request_id="req-1"):
        await dispatch_many([Correlated(marker="a"), Correlated(marker="b")])

    assert [envelope.context for envelope in queued.pushed] == [
        {"request_id": "req-1"},
        {"request_id": "req-1"},
    ]


def test_a_retried_dead_letter_does_not_acquire_todays_context() -> None:
    """ADR 0006 decision 8: the replay is verbatim, and that has to include this.

    `FailedJobs.retry` bypasses `dispatch()`, so nothing reseals the envelope —
    but the property is worth a test rather than an argument, because the
    obvious "tidy-up" is to route the retry through `dispatch()`.
    """
    original = Envelope.seal(Correlated(marker="old"), context={"request_id": "req-original"})
    row = FailedJob(envelope=encode_envelope(original))

    with correlate(request_id="req-today"):
        replay = row.envelope_for_retry()

    assert replay.context["request_id"] == "req-original"
    assert replay.context["retried_from"] == original.id
