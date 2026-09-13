"""The dead-letter table.

Three assertions here carry more than their own weight.

"Recorded with enough information to re-run it" is checked by actually re-running
it: the stored envelope is decoded, pushed, taken back off the fake queue and
opened into a job, and *that* job is compared with the original. A test that
compared column values would pass on a row that cannot be reconstituted, which
is the only failure mode this table has.

"Recording survives an unavailable database" is checked against a real, refused
connection rather than a mock that raises. The failure being defended against is
a TCP-level one that surfaces from inside SQLAlchemy's pool, and a mock raising
`RuntimeError` from the top of the call would not exercise the path that
actually runs at 3am.

`retry()`'s ordering is checked from both sides. It is easy to write a retry that
deletes the row and pushes afterwards; that passes any test which only checks
the happy path, and loses the job the first time the queue is down.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, ClassVar, cast

import pytest

from keel.database import Database, DatabaseConfig, Model, set_database, uow, utcnow
from keel.exceptions import RecordNotFoundError
from keel.queue import Envelope, Job, QueueConfig, QueueManager, use_queue
from keel.queue.failed import (
    MESSAGE_LIMIT,
    TRACEBACK_LIMIT,
    DatabaseFailureSink,
    FailedJob,
    FailedJobs,
    decode_envelope,
    encode_envelope,
)
from keel.queue.fake import FakeQueue
from keel.testing import fake_queue

if TYPE_CHECKING:
    from sqlalchemy import Table

pytestmark = [pytest.mark.anyio]


@dataclass(frozen=True, slots=True)
class ExportLedger(Job):
    """A job with a payload worth reconstructing exactly."""

    name: ClassVar[str] = "keel_test_export_ledger"
    queue: ClassVar[str] = "reports"
    max_attempts: ClassVar[int] = 5

    ledger_id: str
    fmt: str = "csv"

    async def handle(self) -> None:
        """Never runs; these tests care about the envelope, not the work."""


@dataclass(frozen=True, slots=True)
class SweepCaches(Job):
    """A second job, so filtering by name has something to filter against."""

    name: ClassVar[str] = "keel_test_sweep_caches"

    region: str

    async def handle(self) -> None:
        """Never runs."""


class ExplodingQueue:
    """A queue that refuses every push, standing in for "the broker is down"."""

    def __init__(self, name: str = "exploding") -> None:
        self._name = name
        self.attempts = 0

    @property
    def name(self) -> str:
        return self._name

    async def push(self, envelope: Envelope) -> str:
        self.attempts += 1
        raise ConnectionError("broker unreachable")

    async def push_many(self, envelopes: Sequence[Envelope]) -> list[str]:
        raise ConnectionError("broker unreachable")

    async def size(self, queue: str | None = None) -> int:
        return 0

    async def clear(self, queue: str | None = None) -> int:
        return 0

    async def close(self) -> None:
        return None


def bind_queue(driver: str, connection: object) -> Any:
    """Bind an arbitrary queue object as the default connection."""
    manager = QueueManager(QueueConfig(driver=driver))
    manager.extend(driver, lambda _name: cast("Any", connection))
    return use_queue(manager)


def dead_letter(job: Job, *, attempts: int = 3, context: dict[str, Any] | None = None) -> Envelope:
    """Seal a job and age it to the point a worker would give up on it."""
    envelope = Envelope.seal(job, context=context or {"correlation_id": "abc-123"})
    for _ in range(attempts):
        envelope = envelope.attempted()
    return envelope


def boom(message: str = "downstream said no") -> Exception:
    """Return an exception carrying a real traceback."""
    try:
        raise RuntimeError(message)
    except RuntimeError as exc:
        return exc


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    """A live database with the dead-letter table created and dropped around it."""
    tables = [cast("Table", FailedJob.__table__)]
    instance = Database(DatabaseConfig(url=database_url))
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.create_all, tables=tables)
    set_database(instance)
    yield instance
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.drop_all, tables=tables)
    set_database(None)
    await instance.close()


# -- encoding -------------------------------------------------------------


def test_an_envelope_survives_a_round_trip_through_storage() -> None:
    original = dead_letter(ExportLedger("led-1", fmt="parquet"))
    assert decode_envelope(encode_envelope(original)) == original


def test_decoding_drops_fields_this_version_does_not_know() -> None:
    stored = encode_envelope(dead_letter(ExportLedger("led-1")))
    stored["shipped_by_a_newer_release"] = {"anything": True}
    assert decode_envelope(stored).job == ExportLedger.name


def test_a_row_reconstitutes_the_envelope_it_was_built_from() -> None:
    envelope = dead_letter(ExportLedger("led-1"))
    record = FailedJob(
        job=envelope.job,
        envelope_id=envelope.id,
        queue=envelope.queue,
        attempts=envelope.attempts,
        exception="RuntimeError",
        message="downstream said no",
        traceback="",
        failed_at=utcnow(),
        context={},
        envelope=encode_envelope(envelope),
    )
    assert record.as_envelope() == envelope


# -- recording ------------------------------------------------------------


@pytest.mark.postgres
async def test_a_dead_lettered_job_is_recorded_with_enough_to_rerun_it(
    database: Database,
) -> None:
    job = ExportLedger("led-42", fmt="parquet")
    envelope = dead_letter(job, attempts=5, context={"correlation_id": "cid-9", "tenant": "acme"})
    error = boom()

    async with uow() as session:
        await FailedJobs(session).record(envelope, error)

    async with uow() as session:
        rows = await FailedJobs(session).list()
        assert len(rows) == 1
        row = rows[0]

        assert row.job == ExportLedger.name
        assert row.envelope_id == envelope.id
        assert row.queue == "reports"
        assert row.attempts == 5
        assert row.exception == "RuntimeError"
        assert row.message == "downstream said no"
        assert "raise RuntimeError" in row.traceback
        assert row.context == {"correlation_id": "cid-9", "tenant": "acme"}

        # The point of the row: it reconstitutes into the job that failed.
        assert row.as_envelope() == envelope
        assert row.as_envelope().open() == job


@pytest.mark.postgres
async def test_a_long_message_and_traceback_are_bounded(database: Database) -> None:
    envelope = dead_letter(ExportLedger("led-1"))
    async with uow() as session:
        row = await FailedJobs(session).record(envelope, boom("y" * (MESSAGE_LIMIT + 1000)))
        assert len(row.message) <= MESSAGE_LIMIT + 64
        assert "truncated" in row.message
        assert len(row.traceback) <= TRACEBACK_LIMIT + 64


# -- the sink -------------------------------------------------------------


@pytest.mark.postgres
async def test_the_sink_writes_a_row(database: Database) -> None:
    envelope = dead_letter(ExportLedger("led-7"))
    await DatabaseFailureSink().record(envelope, boom())

    async with uow() as session:
        assert await FailedJobs(session).count() == 1


async def test_recording_survives_an_unavailable_database() -> None:
    """A refused connection, not a mocked raise: the real failure comes out of the pool."""
    unreachable = Database(
        DatabaseConfig(
            url="postgresql+asyncpg://keel:keel@127.0.0.1:1/keel",
            connect_timeout=1.0,
            pool_pre_ping=False,
        )
    )
    seen: list[tuple[BaseException, Envelope, BaseException]] = []
    sink = DatabaseFailureSink(
        database=unreachable,
        on_error=lambda failure, envelope, error: seen.append((failure, envelope, error)),
    )

    envelope = dead_letter(ExportLedger("led-9"))
    original = boom()

    # The whole contract: this returns, it does not raise.
    await sink.record(envelope, original)

    assert len(seen) == 1
    failure, reported, cause = seen[0]
    assert isinstance(failure, Exception)
    assert reported == envelope
    assert cause is original
    await unreachable.close()


async def test_an_unrecordable_dead_letter_reaches_the_log_in_full(
    caplog: pytest.LogCaptureFixture,
) -> None:
    unreachable = Database(
        DatabaseConfig(
            url="postgresql+asyncpg://keel:keel@127.0.0.1:1/keel",
            connect_timeout=1.0,
            pool_pre_ping=False,
        )
    )
    envelope = dead_letter(ExportLedger("led-11"))

    with caplog.at_level(logging.ERROR, logger="keel.queue.failed"):
        await DatabaseFailureSink(database=unreachable).record(envelope, boom())

    assert caplog.records, "the last-resort log line is the only record left"
    logged = caplog.records[0].getMessage()
    # Enough to re-dispatch by hand is the bar.
    assert ExportLedger.name in logged
    assert "led-11" in logged
    await unreachable.close()


# -- retrying -------------------------------------------------------------


@pytest.mark.postgres
async def test_retrying_re_dispatches_an_equal_job_and_removes_the_row(
    database: Database,
) -> None:
    job = ExportLedger("led-77", fmt="parquet")
    envelope = dead_letter(job, attempts=5, context={"correlation_id": "cid-1"})

    async with uow() as session:
        row = await FailedJobs(session).record(envelope, boom())
        identifier = row.id

    with fake_queue() as queued:
        async with uow() as session:
            await FailedJobs(session).retry(identifier)

    assert len(queued.pushed) == 1
    pushed = queued.pushed[0]

    assert pushed.open() == job
    assert pushed.queue == "reports"
    assert pushed.max_attempts == 5
    # A fresh budget, or the retry would arrive already exhausted.
    assert pushed.attempts == 0
    # A fresh identity, still joinable to the one that died.
    assert pushed.id != envelope.id
    assert pushed.context["correlation_id"] == "cid-1"
    assert pushed.context["retried_from"] == envelope.id

    async with uow() as session:
        assert await FailedJobs(session).count() == 0


@pytest.mark.postgres
async def test_retry_removes_the_row_only_if_the_dispatch_succeeded(
    database: Database,
) -> None:
    envelope = dead_letter(ExportLedger("led-88"))
    async with uow() as session:
        identifier = (await FailedJobs(session).record(envelope, boom())).id

    broken = ExplodingQueue()
    with bind_queue("exploding", broken), pytest.raises(ConnectionError):
        async with uow() as session:
            await FailedJobs(session).retry(identifier)

    assert broken.attempts == 1
    async with uow() as session:
        # Still there. Losing it here is the failure the table exists to prevent.
        assert await FailedJobs(session).get(identifier) is not None

    with fake_queue() as queued:
        async with uow() as session:
            await FailedJobs(session).retry(identifier)

    assert len(queued.pushed) == 1
    async with uow() as session:
        assert await FailedJobs(session).get(identifier) is None


@pytest.mark.postgres
async def test_retrying_something_that_is_gone_says_so(database: Database) -> None:
    with fake_queue():
        async with uow() as session:
            with pytest.raises(RecordNotFoundError):
                await FailedJobs(session).retry(uuid.uuid4())


@pytest.mark.postgres
async def test_retry_all_can_be_narrowed_to_one_job(database: Database) -> None:
    async with uow() as session:
        failed = FailedJobs(session)
        await failed.record(dead_letter(ExportLedger("a")), boom())
        await failed.record(dead_letter(ExportLedger("b")), boom())
        await failed.record(dead_letter(SweepCaches("eu-west")), boom())

    with fake_queue() as queued:
        async with uow() as session:
            accepted = await FailedJobs(session).retry_all(ExportLedger.name)

    assert len(accepted) == 2
    assert {envelope.job for envelope in queued.pushed} == {ExportLedger.name}

    async with uow() as session:
        remaining = await FailedJobs(session).list()
        assert [row.job for row in remaining] == [SweepCaches.name]


@pytest.mark.postgres
async def test_retry_all_respects_its_limit(database: Database) -> None:
    async with uow() as session:
        failed = FailedJobs(session)
        for index in range(4):
            await failed.record(dead_letter(ExportLedger(str(index))), boom())

    with fake_queue() as queued:
        async with uow() as session:
            await FailedJobs(session).retry_all(limit=2)

    assert len(queued.pushed) == 2
    async with uow() as session:
        assert await FailedJobs(session).count() == 2


# -- forgetting and pruning ----------------------------------------------


@pytest.mark.postgres
async def test_forget_removes_a_row_and_reports_whether_it_did(database: Database) -> None:
    async with uow() as session:
        identifier = (await FailedJobs(session).record(dead_letter(ExportLedger("x")), boom())).id

    async with uow() as session:
        failed = FailedJobs(session)
        assert await failed.forget(identifier) is True
        assert await failed.forget(identifier) is False
        assert await failed.count() == 0


@pytest.mark.postgres
async def test_prune_respects_its_cutoff(database: Database) -> None:
    now = utcnow()
    ages = {
        "ancient": now - timedelta(days=90),
        "old": now - timedelta(days=31),
        "recent": now - timedelta(days=2),
        "fresh": now,
    }
    async with uow() as session:
        failed = FailedJobs(session)
        for label, moment in ages.items():
            await failed.record(dead_letter(ExportLedger(label)), boom(), failed_at=moment)

    async with uow() as session:
        removed = await FailedJobs(session).prune(timedelta(days=30))
    assert removed == 2

    async with uow() as session:
        survivors = {
            row.envelope["payload"]["ledger_id"] for row in await FailedJobs(session).list()
        }
    assert survivors == {"recent", "fresh"}

    # An absolute cutoff is the other spelling, and it is exclusive.
    async with uow() as session:
        assert await FailedJobs(session).prune(now - timedelta(days=1)) == 1

    async with uow() as session:
        assert await FailedJobs(session).count() == 1


@pytest.mark.postgres
async def test_prune_refuses_a_naive_cutoff(database: Database) -> None:
    from datetime import datetime

    async with uow() as session:
        with pytest.raises(ValueError, match="aware datetime"):
            await FailedJobs(session).prune(datetime(2020, 1, 1))


def test_the_sink_satisfies_the_protocol_the_worker_declares() -> None:
    """Structural conformance, checked by the type checkers as much as at runtime.

    The two modules do not import each other; this is the only thing keeping the
    shapes in step, so it is asserted rather than assumed.
    """
    from keel.queue.worker import FailureSink

    sink: FailureSink = DatabaseFailureSink()
    assert callable(sink.record)


def test_the_sink_says_where_it_writes() -> None:
    assert "bound database" in repr(DatabaseFailureSink())


def test_the_fake_queue_is_what_records_the_retry() -> None:
    # Guards the helper above: if `bind_queue` stopped binding, several tests
    # would silently assert against the wrong connection.
    recorder = FakeQueue("probe")
    with bind_queue("probe", recorder):
        from keel.queue import queue as current

        assert current() is recorder
