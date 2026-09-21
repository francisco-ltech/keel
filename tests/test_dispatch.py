"""Dispatching, and the guarantee that makes it safe.

The promise being pinned here is one sentence: **a job dispatched inside a unit
of work reaches the queue only if that unit of work commits.**

Everything else in this file is detail around it. A suite that only checked
"dispatch enqueues the job" would pass against an implementation that pushes
immediately — which is the implementation that emails a customer about an
invoice whose transaction rolled back, and whose worker then dead-letters the
job because the row it names does not exist.

The second theme is the fake. It records rather than runs, and the tests here
assume that distinction: asserting *what was dispatched* is a different question
from asserting *what the job does*, and conflating them means neither failure
says where to look.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import ClassVar, cast

import pytest
from sqlalchemy import String, Table
from sqlalchemy.orm import Mapped, mapped_column

from keel.database import (
    Database,
    DatabaseConfig,
    Model,
    Repository,
    TimestampMixin,
    UUIDPrimaryKey,
    set_database,
    uow,
)
from keel.database.hooks import after_commit, current_session, in_transaction
from keel.exceptions import ConfigurationError
from keel.queue import (
    FakeQueue,
    Job,
    NullQueue,
    QueueAssertionError,
    QueueConfig,
    QueueManager,
    SyncQueue,
    dispatch,
    dispatch_many,
    dispatch_now,
    queue_lifespan,
    set_queue_manager,
)
from keel.queue.dispatch import current_queue_manager
from keel.testing import fake_queue

pytestmark = [pytest.mark.anyio]

executed: list[str] = []


@dataclass(frozen=True, slots=True)
class SendInvoice(Job):
    """A job that records having run, so inline execution is observable."""

    invoice_id: str

    async def handle(self) -> None:
        executed.append(self.invoice_id)


@dataclass(frozen=True, slots=True)
class Reindex(Job):
    """A job on its own queue, to prove routing."""

    document_id: str
    queue: ClassVar[str] = "search"

    async def handle(self) -> None:
        executed.append(f"reindex:{self.document_id}")


@dataclass(frozen=True, slots=True)
class Digest(Job):
    """A unique job, to prove deduplication."""

    user_id: str
    unique_for: ClassVar[float | None] = 60.0

    async def handle(self) -> None:
        executed.append(f"digest:{self.user_id}")


class Invoice(Model, UUIDPrimaryKey, TimestampMixin):
    """A row to write inside the transaction under test."""

    __tablename__ = "keel_test_dispatch_invoices"

    reference: Mapped[str] = mapped_column(String(50))


class Invoices(Repository[Invoice]):
    model = Invoice


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    """Bindings and the execution log are process-global; leaking either poisons later tests."""
    executed.clear()
    yield
    executed.clear()
    set_queue_manager(None)


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    """A live database with the dispatch test table created and dropped."""
    tables = [cast("Table", Invoice.__table__)]
    instance = Database(DatabaseConfig(url=database_url))
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.create_all, tables=tables)
    set_database(instance)
    yield instance
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.drop_all, tables=tables)
    set_database(None)
    await instance.close()


# -- binding --------------------------------------------------------------


async def test_dispatching_with_nothing_bound_says_what_to_do() -> None:
    set_queue_manager(None)
    with pytest.raises(ConfigurationError) as error:
        await dispatch(SendInvoice("1"))
    message = str(error.value)
    assert "set_queue_manager" in message
    assert "fake_queue" in message


async def test_the_lifespan_binds_and_then_restores() -> None:
    set_queue_manager(None)
    async with queue_lifespan(QueueConfig(driver="null")) as bound:
        assert current_queue_manager() is bound
    with pytest.raises(ConfigurationError):
        current_queue_manager()


async def test_a_nested_lifespan_restores_the_outer_one() -> None:
    async with queue_lifespan(QueueConfig(driver="null")) as outer:
        async with queue_lifespan(QueueConfig(driver="null")) as inner:
            assert current_queue_manager() is inner
        assert current_queue_manager() is outer


# -- dispatching, outside any transaction ---------------------------------


async def test_dispatch_records_the_job() -> None:
    with fake_queue() as queued:
        await dispatch(SendInvoice("inv-1"))
        queued.assert_pushed(SendInvoice, invoice_id="inv-1")


async def test_the_fake_does_not_run_the_job() -> None:
    """Recording and running are different questions, and this is the seam."""
    with fake_queue():
        await dispatch(SendInvoice("inv-1"))
    assert executed == []


async def test_a_job_goes_to_its_declared_queue() -> None:
    with fake_queue() as queued:
        await dispatch(Reindex("doc-1"))
        queued.assert_pushed_on(Reindex, "search")


async def test_the_queue_can_be_overridden_at_dispatch() -> None:
    with fake_queue() as queued:
        await dispatch(Reindex("doc-1"), on="backfill")
        queued.assert_pushed_on(Reindex, "backfill")


async def test_a_delay_is_carried_on_the_envelope() -> None:
    with fake_queue() as queued:
        await dispatch(SendInvoice("inv-1"), delay=timedelta(minutes=5))
        queued.assert_delayed(SendInvoice, 300.0)


async def test_dispatch_many_records_each_job() -> None:
    with fake_queue() as queued:
        ids = await dispatch_many([SendInvoice("a"), SendInvoice("b")])
        assert len(ids) == 2
        queued.assert_pushed_times(SendInvoice, 2)


async def test_a_unique_job_is_only_enqueued_once() -> None:
    with fake_queue() as queued:
        first = await dispatch(Digest("user-1"))
        second = await dispatch(Digest("user-1"))
        queued.assert_pushed_times(Digest, 1)
        assert second == first, "the duplicate should report the id already in flight"


async def test_uniqueness_is_per_payload() -> None:
    with fake_queue() as queued:
        await dispatch(Digest("user-1"))
        await dispatch(Digest("user-2"))
        queued.assert_pushed_times(Digest, 2)


# -- the guarantee --------------------------------------------------------


@pytest.mark.postgres
async def test_a_job_dispatched_in_a_transaction_waits_for_the_commit(
    database: Database,
) -> None:
    with fake_queue() as queued:
        async with uow() as session:
            await Invoices(session).create(reference="INV-1")
            await dispatch(SendInvoice("inv-1"))
            queued.assert_nothing_pushed()  # still inside the transaction

        queued.assert_pushed(SendInvoice, invoice_id="inv-1")


@pytest.mark.postgres
async def test_a_rolled_back_transaction_dispatches_nothing(database: Database) -> None:
    """The defect this design exists to prevent.

    Push immediately and the worker is emailing a customer about an invoice
    that never existed — and losing the race, so the job dead-letters looking
    like corrupted data.
    """
    with fake_queue() as queued, pytest.raises(RuntimeError):
        async with uow() as session:
            await Invoices(session).create(reference="INV-DOOMED")
            await dispatch(SendInvoice("inv-doomed"))
            raise RuntimeError("the write failed")

    queued.assert_nothing_pushed()


@pytest.mark.postgres
async def test_the_row_is_committed_before_the_job_is_dispatched(
    database: Database,
) -> None:
    """Ordering, not just presence: the worker must be able to read the row."""
    seen: list[int] = []

    with fake_queue() as queued:
        async with uow() as session:
            await Invoices(session).create(reference="INV-2")
            await dispatch(SendInvoice("inv-2"))

        # A separate transaction: what a worker would see.
        async with uow() as session:
            seen.append(await Invoices(session).count())

        queued.assert_pushed(SendInvoice, invoice_id="inv-2")

    assert seen == [1]


@pytest.mark.postgres
async def test_a_failing_after_commit_callback_is_logged_by_default(
    database: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """The commit stands and the caller has returned, so a log line is all that is left."""

    async def refused() -> None:
        raise RuntimeError("the mail server is down")

    with caplog.at_level(logging.ERROR, logger="keel.database"):
        async with uow():
            after_commit(refused)

    assert "after-commit callback" in caplog.text
    assert "the mail server is down" in caplog.text


@pytest.mark.postgres
async def test_after_commit_can_be_opted_out_of(database: Database) -> None:
    """Occasionally correct, and it has to look deliberate."""
    with fake_queue() as queued:
        async with uow() as session:
            await Invoices(session).create(reference="INV-3")
            await dispatch(SendInvoice("inv-3"), after_commit=False)
            queued.assert_pushed(SendInvoice, invoice_id="inv-3")


@pytest.mark.postgres
async def test_opting_out_dispatches_even_when_the_transaction_fails(
    database: Database,
) -> None:
    with fake_queue() as queued, pytest.raises(RuntimeError):
        async with uow() as session:
            await Invoices(session).create(reference="INV-4")
            await dispatch(SendInvoice("inv-4"), after_commit=False)
            raise RuntimeError("the write failed")

    queued.assert_pushed(SendInvoice, invoice_id="inv-4")


@pytest.mark.postgres
async def test_dispatch_many_also_waits_for_the_commit(database: Database) -> None:
    with fake_queue() as queued:
        async with uow() as session:
            await Invoices(session).create(reference="INV-5")
            await dispatch_many([SendInvoice("a"), SendInvoice("b")])
            queued.assert_nothing_pushed()
        queued.assert_pushed_times(SendInvoice, 2)


@pytest.mark.postgres
async def test_the_session_is_discoverable_only_inside_a_transaction(
    database: Database,
) -> None:
    """The mechanism dispatch relies on to know it should wait."""
    assert in_transaction() is False
    assert current_session() is None

    async with uow() as session:
        assert in_transaction() is True
        assert current_session() is session

    assert in_transaction() is False


@pytest.mark.postgres
async def test_a_nested_unit_of_work_restores_the_outer_session(
    database: Database,
) -> None:
    async with uow() as outer:
        async with uow() as inner:
            assert current_session() is inner
        assert current_session() is outer


# -- the other drivers ----------------------------------------------------


async def test_the_sync_driver_runs_the_job_immediately() -> None:
    """Not a test double: this is how a one-process dev environment works."""
    manager = QueueManager(QueueConfig(driver="sync"))
    set_queue_manager(manager)
    await dispatch(SendInvoice("inv-sync"))
    assert executed == ["inv-sync"]


async def test_the_sync_driver_lets_a_failing_job_raise() -> None:
    @dataclass(frozen=True, slots=True)
    class Explodes(Job):
        async def handle(self) -> None:
            raise RuntimeError("job failed")

    set_queue_manager(QueueManager(QueueConfig(driver="sync")))
    with pytest.raises(RuntimeError, match="job failed"):
        await dispatch(Explodes())


async def test_the_null_driver_discards_the_job() -> None:
    set_queue_manager(QueueManager(QueueConfig(driver="null")))
    await dispatch(SendInvoice("inv-null"))
    assert executed == []


async def test_dispatch_now_bypasses_the_queue_entirely() -> None:
    with fake_queue() as queued:
        await dispatch_now(SendInvoice("inv-inline"))
        queued.assert_nothing_pushed()
    assert executed == ["inv-inline"]


def test_sync_and_null_are_real_drivers_not_doubles() -> None:
    """Documented in the config, and worth pinning so it is not "simplified"."""
    assert isinstance(QueueManager(QueueConfig(driver="sync")).connection(), SyncQueue)
    assert isinstance(QueueManager(QueueConfig(driver="null")).connection(), NullQueue)


def test_an_unknown_driver_points_at_register_driver() -> None:
    manager = QueueManager(QueueConfig(driver="rabbit"))
    with pytest.raises(ConfigurationError) as error:
        manager.connection()
    assert "register_driver" in str(error.value)


def test_a_custom_driver_can_be_registered() -> None:
    """Open/closed: adding a backend must not mean editing the manager."""
    manager = QueueManager(QueueConfig(driver="custom"))
    manager.register_driver("custom", lambda name, _config: FakeQueue(name))
    assert isinstance(manager.connection(), FakeQueue)


# -- the fake's assertions ------------------------------------------------


async def test_assert_pushed_failure_shows_what_was_dispatched() -> None:
    """Assertion quality is a feature: a failure must say what did happen."""
    with fake_queue() as queued:
        await dispatch(SendInvoice("inv-1"))
        with pytest.raises(QueueAssertionError) as error:
            queued.assert_pushed(Reindex, document_id="doc-9")

    message = str(error.value)
    assert "Reindex" in message
    assert "Dispatched jobs:" in message
    assert "inv-1" in message, "the timeline should show what was dispatched instead"


async def test_assert_nothing_pushed_reports_the_count() -> None:
    with fake_queue() as queued:
        await dispatch(SendInvoice("inv-1"))
        with pytest.raises(QueueAssertionError, match="1 were recorded"):
            queued.assert_nothing_pushed()


async def test_assert_pushed_times_catches_a_dispatch_in_a_loop() -> None:
    """One welcome email becoming four hundred is the bug this catches."""
    with fake_queue() as queued:
        for index in range(4):
            await dispatch(SendInvoice(f"inv-{index}"))
        with pytest.raises(QueueAssertionError, match="1x, got 4"):
            queued.assert_pushed_times(SendInvoice, 1)


async def test_assert_not_pushed_passes_when_nothing_matched() -> None:
    with fake_queue() as queued:
        await dispatch(SendInvoice("inv-1"))
        queued.assert_not_pushed(Reindex)


async def test_the_fake_matches_on_a_subset_of_the_payload() -> None:
    """Restating the whole payload makes a test break when a field is added."""
    with fake_queue() as queued:
        await dispatch(SendInvoice("inv-1"))
        queued.assert_pushed(SendInvoice)
