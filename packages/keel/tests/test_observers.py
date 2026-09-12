"""Model observers.

The promise being pinned here is narrow and important: **an observer never sees
a change that did not commit.** Everything else in this file is detail around
that. A test suite for observers that only checks "the hook fired" would pass
against an implementation that fires inside the flush — which is the
implementation that sends a welcome email for a user whose transaction rolled
back.

The soft-delete classification tests matter for a similar reason. A soft delete
is an UPDATE in SQL, and an observer that has to work that out from a column
diff will get it wrong eventually.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from sqlalchemy import String, Table
from sqlalchemy.orm import Mapped, mapped_column

from keel.database import (
    Database,
    DatabaseConfig,
    Model,
    Repository,
    SoftDeleteMixin,
    TimestampMixin,
    UUIDPrimaryKey,
    set_database,
    uow,
)
from keel.database.observers import (
    ModelEvent,
    Observer,
    clear_observers,
    observe,
    observers_for,
    take_pending,
)

pytestmark = [pytest.mark.anyio]


class Account(Model, UUIDPrimaryKey, TimestampMixin, SoftDeleteMixin):
    """A soft-deletable model, so all four lifecycles are reachable."""

    __tablename__ = "keel_test_accounts"

    name: Mapped[str] = mapped_column(String(100))
    plan: Mapped[str] = mapped_column(String(50), default="free")


class Accounts(Repository[Account]):
    model = Account


@dataclass
class Recorder(Observer[Account]):
    """An observer that writes down what it was told."""

    calls: list[tuple[str, str]] = field(default_factory=list)
    changes: list[Mapping[str, tuple[Any, Any]]] = field(default_factory=list)

    async def created(self, instance: Account) -> None:
        self.calls.append(("created", instance.name))

    async def updated(self, instance: Account, changes: Mapping[str, tuple[Any, Any]]) -> None:
        self.calls.append(("updated", instance.name))
        self.changes.append(changes)

    async def deleted(self, instance: Account) -> None:
        self.calls.append(("deleted", instance.name))

    async def restored(self, instance: Account) -> None:
        self.calls.append(("restored", instance.name))

    @property
    def lifecycles(self) -> list[str]:
        return [lifecycle for lifecycle, _ in self.calls]


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    """A live database with the observer test table created and dropped."""
    tables = [cast("Table", Account.__table__)]
    instance = Database(DatabaseConfig(url=database_url))
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.create_all, tables=tables)
    set_database(instance)
    yield instance
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.drop_all, tables=tables)
    set_database(None)
    await instance.close()


@pytest.fixture(autouse=True)
def _no_leaked_observers() -> Iterator[None]:
    """Registrations are process-global; leaking one poisons later tests."""
    clear_observers()
    yield
    clear_observers()


@pytest.fixture
def recorder() -> Recorder:
    """A registered observer watching :class:`Account`."""
    instance = Recorder()
    observe(Account, instance)
    return instance


# -- registration ---------------------------------------------------------


def test_an_observer_is_found_for_the_model_it_watches() -> None:
    recorder = Recorder()
    observe(Account, recorder)
    assert observers_for(Account) == [recorder]


def test_unsubscribing_removes_the_registration() -> None:
    recorder = Recorder()
    unsubscribe = observe(Account, recorder)
    unsubscribe()
    assert observers_for(Account) == []


def test_unsubscribing_twice_is_harmless() -> None:
    unsubscribe = observe(Account, Recorder())
    unsubscribe()
    unsubscribe()


def test_a_model_with_no_observer_has_none() -> None:
    assert observers_for(Account) == []


# -- the central promise --------------------------------------------------


@pytest.mark.postgres
async def test_created_fires_after_the_commit(database: Database, recorder: Recorder) -> None:
    async with uow() as session:
        await Accounts(session).create(name="ada")
        assert recorder.calls == [], "the observer must not run inside the transaction"

    assert recorder.calls == [("created", "ada")]


@pytest.mark.postgres
async def test_a_rolled_back_change_reaches_no_observer(
    database: Database, recorder: Recorder
) -> None:
    """The bug this design exists to prevent.

    An observer firing inside the flush sends the welcome email, and then the
    transaction fails and the user does not exist.
    """
    with pytest.raises(RuntimeError):
        async with uow() as session:
            await Accounts(session).create(name="doomed")
            raise RuntimeError("the write failed")

    assert recorder.calls == []


@pytest.mark.postgres
async def test_the_row_is_readable_when_the_observer_runs(
    database: Database, recorder: Recorder
) -> None:
    """Dispatch happens after commit but before the session closes."""
    seen: list[uuid.UUID] = []

    class IdReader(Observer[Account]):
        async def created(self, instance: Account) -> None:
            seen.append(instance.id)

    observe(Account, IdReader())

    async with uow() as session:
        created = await Accounts(session).create(name="readable")

    assert seen == [created.id]


# -- lifecycles -----------------------------------------------------------


@pytest.mark.postgres
async def test_updated_reports_what_changed(database: Database, recorder: Recorder) -> None:
    async with uow() as session:
        account = await Accounts(session).create(name="ada")

    async with uow() as session:
        loaded = await Accounts(session).get_or_fail(account.id)
        await Accounts(session).update(loaded, plan="pro")

    assert recorder.lifecycles == ["created", "updated"]
    assert recorder.changes[0]["plan"] == ("free", "pro")


@pytest.mark.postgres
async def test_a_soft_delete_is_reported_as_deleted_not_updated(
    database: Database, recorder: Recorder
) -> None:
    """It is an UPDATE in SQL, and an observer should not have to know that."""
    async with uow() as session:
        account = await Accounts(session).create(name="ada")

    async with uow() as session:
        loaded = await Accounts(session).get_or_fail(account.id)
        await Accounts(session).delete(loaded)

    assert recorder.lifecycles == ["created", "deleted"]


@pytest.mark.postgres
async def test_clearing_deleted_at_is_reported_as_restored(
    database: Database, recorder: Recorder
) -> None:
    async with uow() as session:
        account = await Accounts(session).create(name="ada")

    async with uow() as session:
        repository = Accounts(session)
        await repository.delete(await repository.get_or_fail(account.id))

    async with uow() as session:
        repository = Accounts(session)
        trashed = await repository.find_trashed(account.id)
        assert trashed is not None
        await repository.restore(trashed)

    assert recorder.lifecycles == ["created", "deleted", "restored"]


@pytest.mark.postgres
async def test_a_hard_delete_is_reported_as_deleted(database: Database, recorder: Recorder) -> None:
    async with uow() as session:
        account = await Accounts(session).create(name="ada")

    async with uow() as session:
        repository = Accounts(session)
        await repository.force_delete(await repository.get_or_fail(account.id))

    assert recorder.lifecycles == ["created", "deleted"]


@pytest.mark.postgres
async def test_events_arrive_in_the_order_they_happened(
    database: Database, recorder: Recorder
) -> None:
    async with uow() as session:
        repository = Accounts(session)
        await repository.create(name="first")
        await repository.create(name="second")

    assert recorder.calls == [("created", "first"), ("created", "second")]


# -- isolation and failure ------------------------------------------------


@pytest.mark.postgres
async def test_a_failing_observer_does_not_stop_the_others(database: Database) -> None:
    """The write is already durable; aborting here would run some and not others."""
    failures: list[ModelEvent] = []
    survivor = Recorder()

    class Exploding(Observer[Account]):
        async def created(self, instance: Account) -> None:
            raise RuntimeError("observer is broken")

    instance = Database(database.config, on_observer_error=lambda _exc, ev: failures.append(ev))
    set_database(instance)
    observe(Account, Exploding())
    observe(Account, survivor)

    try:
        async with uow() as session:
            await Accounts(session).create(name="resilient")
    finally:
        await instance.close()
        set_database(database)

    assert survivor.calls == [("created", "resilient")]
    assert len(failures) == 1
    assert failures[0].lifecycle == "created"


@pytest.mark.postgres
async def test_a_failing_observer_does_not_undo_the_write(database: Database) -> None:
    class Exploding(Observer[Account]):
        async def created(self, instance: Account) -> None:
            raise RuntimeError("observer is broken")

    instance = Database(database.config, on_observer_error=lambda _exc, _ev: None)
    set_database(instance)
    observe(Account, Exploding())

    async with uow() as session:
        account = await Accounts(session).create(name="committed")

    await instance.close()
    set_database(database)

    async with uow() as session:
        assert await Accounts(session).get(account.id) is not None


@pytest.mark.postgres
async def test_no_events_are_buffered_when_nothing_observes(database: Database) -> None:
    """Buffering every change for nobody would make every write pay for the feature."""
    async with uow() as session:
        await Accounts(session).create(name="unwatched")
        assert take_pending(session) == []


@pytest.mark.postgres
async def test_the_buffer_is_drained_between_transactions(
    database: Database, recorder: Recorder
) -> None:
    async with uow() as session:
        await Accounts(session).create(name="one")
    async with uow() as session:
        await Accounts(session).create(name="two")

    assert recorder.calls == [("created", "one"), ("created", "two")]
