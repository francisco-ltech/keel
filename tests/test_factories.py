"""Model factories: what they generate, and — more importantly — what they don't.

Most of this file is about absence. A factory that invents a value for a column
something else owns produces a row that no code path in production could ever
write, and a fixture like that does not fail: it passes, and quietly makes the
assertion around it meaningless. So the load-bearing tests here are the negative
ones.

`id` and the timestamps are the clearest case. `id` is assigned by the mapper
`init` event and the timestamps by Postgres, so a built instance must show a key
and *no* timestamps — the exact opposite of what polyfactory would do left alone.

Foreign keys are the case with teeth. A generated UUID in a foreign key column
does not fail in the factory; it fails at flush, in whichever test happened to
persist the row, with a Postgres constraint error that names a column the test
never mentioned. The test below pins the column to `None` so that the failure,
if the configuration ever regresses, lands here instead.

The seed test asserts reproducibility of the *columns* and explicitly not of the
`id`, because that distinction is the one someone will otherwise waste an hour
on: seeding the factory cannot make a UUIDv7 deterministic, since the key does
not come from the factory at all.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, cast

import pytest
from sqlalchemy import ForeignKey, Integer, String, Table, select
from sqlalchemy.orm import Mapped, mapped_column, relationship

from keel.database import (
    Database,
    DatabaseConfig,
    Model,
    SoftDeleteMixin,
    TimestampMixin,
    UUIDPrimaryKey,
)
from keel.database.factories import GENERATED_COLUMNS, ModelFactory

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = [pytest.mark.anyio, pytest.mark.postgres]


class Owner(Model, UUIDPrimaryKey, TimestampMixin):
    """A parent, so a foreign key has something real to point at."""

    __tablename__ = "keel_test_factory_owners"

    handle: Mapped[str] = mapped_column(String(40))


class Widget(Model, UUIDPrimaryKey, TimestampMixin):
    """The ordinary case: a few generated columns and nothing else."""

    __tablename__ = "keel_test_factory_widgets"

    name: Mapped[str] = mapped_column(String(50))
    quantity: Mapped[int] = mapped_column(Integer)


class Note(Model, UUIDPrimaryKey, TimestampMixin):
    """Carries a foreign key and a relationship, both of which must be left alone."""

    __tablename__ = "keel_test_factory_notes"

    body: Mapped[str] = mapped_column(String(200))
    owner_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("keel_test_factory_owners.id"))
    owner: Mapped[Owner] = relationship(lazy="raise")


class Trashable(Model, UUIDPrimaryKey, TimestampMixin, SoftDeleteMixin):
    """A soft-deletable model, whose `deleted_at` must never be invented."""

    __tablename__ = "keel_test_factory_trashables"

    label: Mapped[str] = mapped_column(String(30))


class OwnerFactory(ModelFactory[Owner]):
    __model__ = Owner


class WidgetFactory(ModelFactory[Widget]):
    __model__ = Widget


class NoteFactory(ModelFactory[Note]):
    __model__ = Note


class TrashableFactory(ModelFactory[Trashable]):
    __model__ = Trashable


MODELS = (Owner, Widget, Note, Trashable)


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    """A live database with this module's tables created and dropped around it."""
    tables = [cast("Table", model.__table__) for model in MODELS]
    instance = Database(DatabaseConfig(url=database_url))
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.create_all, tables=tables)
    yield instance
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.drop_all, tables=tables)
    await instance.close()


@pytest.fixture
async def session(database: Database) -> AsyncIterator[AsyncSession]:
    """A transaction that is rolled back, so persistence tests leave nothing behind."""
    async with database.session() as opened:
        transaction = await opened.begin()
        yield opened
        await transaction.rollback()


def unset(instance: object, column: str) -> bool:
    """Whether a mapped column has no value on this instance yet.

    Read through `getattr` rather than as an attribute because the annotations
    describe a *persisted* row: `created_at` is `datetime`, not `datetime | None`,
    and that is right — every row in the table has one. A transient instance is
    the gap between the two, and `x.created_at is None` would be flagged by mypy
    as a comparison that can never be true.
    """
    return getattr(instance, column) is None


# -- building, without a database -----------------------------------------


def test_build_needs_no_database() -> None:
    """The whole point of the build/create split: this test is not even async."""
    widget = WidgetFactory.build()

    assert isinstance(widget, Widget)
    assert isinstance(widget.name, str)
    assert isinstance(widget.quantity, int)


def test_a_built_instance_already_has_a_uuid7_key() -> None:
    widget = WidgetFactory.build()

    assert isinstance(widget.id, uuid.UUID)
    assert widget.id.version == 7


def test_a_built_instance_has_no_timestamps() -> None:
    """They belong to the database's clock; a factory value would be a lie."""
    widget = WidgetFactory.build()

    assert unset(widget, "created_at")
    assert unset(widget, "updated_at")


def test_overrides_win() -> None:
    widget = WidgetFactory.build(name="known", quantity=7)

    assert widget.name == "known"
    assert widget.quantity == 7


def test_two_builds_differ() -> None:
    first, second = WidgetFactory.build(), WidgetFactory.build()

    assert first.id != second.id
    assert (first.name, first.quantity) != (second.name, second.quantity)


def test_a_soft_deletable_model_builds_undeleted() -> None:
    """`deleted_at` is nullable, so polyfactory would otherwise set it half the time."""
    trashable = TrashableFactory.build()

    assert trashable.deleted_at is None
    assert trashable.is_deleted is False


def test_the_excluded_set_is_the_one_the_module_documents() -> None:
    assert set(GENERATED_COLUMNS) == {"id", "created_at", "updated_at", "deleted_at"}
    assert WidgetFactory.__excluded_fields__ == GENERATED_COLUMNS


# -- foreign keys and relationships ---------------------------------------


def test_foreign_keys_are_not_generated() -> None:
    """A random UUID here is a constraint violation deferred to some other test."""
    note = NoteFactory.build()

    assert unset(note, "owner_id")
    assert isinstance(note.body, str)


def test_a_foreign_key_can_be_supplied() -> None:
    owner = OwnerFactory.build()
    note = NoteFactory.build(owner_id=owner.id)

    assert note.owner_id == owner.id


async def test_a_supplied_foreign_key_persists(session: AsyncSession) -> None:
    owner = await OwnerFactory.create(session)
    note = await NoteFactory.create(session, owner_id=owner.id)

    assert note.owner_id == owner.id


# -- determinism ----------------------------------------------------------


def test_the_same_seed_reproduces_the_same_columns() -> None:
    WidgetFactory.seed(20260912)
    first = WidgetFactory.build()

    WidgetFactory.seed(20260912)
    second = WidgetFactory.build()

    assert (first.name, first.quantity) == (second.name, second.quantity)


def test_seeding_does_not_make_the_key_deterministic() -> None:
    """Keys come from the mapper event and the clock, not from the factory."""
    WidgetFactory.seed(20260912)
    first = WidgetFactory.build()

    WidgetFactory.seed(20260912)
    second = WidgetFactory.build()

    assert first.id != second.id


def test_different_seeds_produce_different_data() -> None:
    WidgetFactory.seed(1)
    first = WidgetFactory.build()

    WidgetFactory.seed(2)
    second = WidgetFactory.build()

    assert (first.name, first.quantity) != (second.name, second.quantity)


# -- persisting -----------------------------------------------------------


async def test_create_inserts_and_flushes(session: AsyncSession) -> None:
    widget = await WidgetFactory.create(session, name="persisted")

    found = (await session.execute(select(Widget).where(Widget.name == "persisted"))).scalar_one()
    assert found is widget
    assert found.id == widget.id


async def test_create_lets_the_database_stamp_the_timestamps(session: AsyncSession) -> None:
    """Not set on build, set after the insert — which is the whole reason to exclude them."""
    widget = await WidgetFactory.create(session)
    await session.refresh(widget)

    assert widget.created_at is not None
    assert widget.updated_at is not None


async def test_create_does_not_commit(session: AsyncSession) -> None:
    """Transaction scope belongs to the caller; a factory that committed would steal it."""
    await WidgetFactory.create(session)

    assert session.in_transaction()


async def test_create_many_produces_distinct_rows(session: AsyncSession) -> None:
    widgets = await WidgetFactory.create_many(session, 5)

    assert len(widgets) == 5
    assert len({widget.id for widget in widgets}) == 5

    count = (await session.execute(select(Widget))).scalars().all()
    assert len(count) == 5


async def test_create_many_applies_overrides_to_every_row(session: AsyncSession) -> None:
    widgets = await WidgetFactory.create_many(session, 3, quantity=42)

    assert [widget.quantity for widget in widgets] == [42, 42, 42]


async def test_create_many_of_zero_is_a_no_op(session: AsyncSession) -> None:
    widgets = await WidgetFactory.create_many(session, 0)

    assert widgets == []
