"""Seeding: order, the all-or-nothing transaction, and the idempotency guard.

The central test here is `test_a_failing_seeder_leaves_nothing_behind`, and it is
worth saying why it is written the way it is. Asserting on the state of the
*session* that just rolled back proves nothing — a rolled-back session would show
an empty table even if the rows had been committed by a seeder that opened its
own transaction. So the assertion is made from a **new** transaction, after the
run has finished and failed. That is the only way to distinguish "undone" from
"invisible from here", and the distinction is the entire promise of the module.

The order test is likewise deliberately not just a list of names. A second
seeder counts the rows the first inserted, so it fails if the run were reordered
*or* if the shared session were not flushed between seeders — sessions here have
autoflush off, and "runs after" would otherwise not imply "can see".

The emptiness guard is checked against soft-deleted rows too. A table whose rows
are all soft deleted is not empty as far as its unique constraints are
concerned, and a guard that consulted the default query scope would cheerfully
re-seed straight into a duplicate key error naming a row no query can see.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, cast

import pytest
from sqlalchemy import String, Table, func, select
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
    with_deleted,
)
from keel.database.factories import ModelFactory
from keel.database.seeding import (
    FactorySeeder,
    Seeder,
    SeederRegistry,
    SeedIfEmpty,
    SeedResult,
    run_seeders,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = [pytest.mark.anyio, pytest.mark.postgres]


class Category(Model, UUIDPrimaryKey, TimestampMixin, SoftDeleteMixin):
    """Soft deletable, so the emptiness guard can be checked against hidden rows."""

    __tablename__ = "keel_test_seed_categories"

    title: Mapped[str] = mapped_column(String(60))


class Item(Model, UUIDPrimaryKey, TimestampMixin):
    """An ordinary table, seeded second so it can observe the first."""

    __tablename__ = "keel_test_seed_items"

    label: Mapped[str] = mapped_column(String(60))


class CategoryFactory(ModelFactory[Category]):
    __model__ = Category


class ItemFactory(ModelFactory[Item]):
    __model__ = Item


class Categories(Repository[Category]):
    model = Category


MODELS = (Category, Item)


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    """A live database with this module's tables created and dropped around it.

    Created per test rather than per module: these tests commit, so a shared
    table would let one test's rows decide another test's emptiness guard.
    """
    tables = [cast("Table", model.__table__) for model in MODELS]
    instance = Database(DatabaseConfig(url=database_url))
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.create_all, tables=tables)
    yield instance
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.drop_all, tables=tables)
    await instance.close()


async def count_of(database: Database, model: type[Model]) -> int:
    """Count rows from a fresh transaction, soft-deleted ones included."""
    async with database.transaction() as session:
        statement = with_deleted(select(func.count()).select_from(model))
        return int((await session.execute(statement)).scalar_one())


class SeedCategories(FactorySeeder):
    """Three categories, built by the same factory a test would use."""

    factory = CategoryFactory


class SeedItems(SeedIfEmpty):
    """One item per existing category, so it fails unless categories ran first."""

    model = Item

    async def run(self, session: AsyncSession) -> None:
        categories = (await session.execute(select(Category))).scalars().all()
        if not categories:
            raise AssertionError("SeedItems ran before SeedCategories")
        for category in categories:
            await ItemFactory.create(session, label=category.title)


class Boom(Seeder):
    """Fails after writing, which is the only interesting way to fail."""

    async def run(self, session: AsyncSession) -> None:
        await ItemFactory.create(session, label="doomed")
        raise RuntimeError("seeder exploded")


# -- names ----------------------------------------------------------------


def test_a_seeder_is_named_after_its_class_by_default() -> None:
    assert SeedCategories.name == "SeedCategories"
    assert Boom().name == "Boom"


def test_a_seeder_can_name_itself() -> None:
    class Custom(Seeder):
        name = "reference data"

        async def run(self, session: AsyncSession) -> None: ...

    assert Custom.name == "reference data"


def test_a_factory_seeder_takes_its_model_from_its_factory() -> None:
    assert SeedCategories.model is Category


# -- order ----------------------------------------------------------------


async def test_seeders_run_in_declared_order(database: Database) -> None:
    results = await run_seeders([SeedCategories(3), SeedItems()], database)

    assert [result.name for result in results] == ["SeedCategories", "SeedItems"]
    assert all(result.ran for result in results)
    assert await count_of(database, Category) == 3
    assert await count_of(database, Item) == 3


async def test_a_seeder_out_of_order_fails_loudly(database: Database) -> None:
    """The dependency is the ordering; getting it wrong must not silently no-op."""
    with pytest.raises(AssertionError, match="before SeedCategories"):
        await run_seeders([SeedItems(), SeedCategories(3)], database)


# -- all or nothing -------------------------------------------------------


async def test_a_failing_seeder_leaves_nothing_behind(database: Database) -> None:
    """The main promise: asserted from a new transaction, not the failed one."""
    with pytest.raises(RuntimeError, match="seeder exploded"):
        await run_seeders([SeedCategories(3), Boom()], database)

    assert await count_of(database, Category) == 0
    assert await count_of(database, Item) == 0


async def test_the_original_exception_propagates_unwrapped(database: Database) -> None:
    """A caller needs the real failure, not a SeedingError hiding it."""
    with pytest.raises(RuntimeError) as raised:
        await run_seeders([Boom()], database)

    assert str(raised.value) == "seeder exploded"


# -- idempotency ----------------------------------------------------------


async def test_the_empty_guard_prevents_a_second_run_duplicating(database: Database) -> None:
    first = await run_seeders([SeedCategories(3)], database)
    second = await run_seeders([SeedCategories(3)], database)

    assert first == (SeedResult(name="SeedCategories", ran=True),)
    assert second == (SeedResult(name="SeedCategories", ran=False),)
    assert await count_of(database, Category) == 3


async def test_the_empty_guard_counts_soft_deleted_rows(database: Database) -> None:
    """A hidden row still owns its unique constraints, so the table is not empty."""
    await run_seeders([SeedCategories(2)], database)

    async with database.transaction() as session:
        for category in await Categories(session).list():
            category.soft_delete()

    results = await run_seeders([SeedCategories(2)], database)

    assert results == (SeedResult(name="SeedCategories", ran=False),)
    assert await count_of(database, Category) == 2


async def test_a_plain_seeder_runs_every_time(database: Database) -> None:
    """The default is "run me"; a seeder that is not safe twice has to say so."""

    class Always(Seeder):
        async def run(self, session: AsyncSession) -> None:
            await ItemFactory.create(session, label="again")

    await run_seeders([Always()], database)
    await run_seeders([Always()], database)

    assert await count_of(database, Item) == 2


# -- what the rows look like afterwards -----------------------------------


async def test_seeded_rows_are_readable_through_a_repository(database: Database) -> None:
    await run_seeders([SeedCategories(4)], database)

    async with database.transaction() as session:
        categories = await Categories(session).list()

    assert len(categories) == 4
    assert all(category.created_at is not None for category in categories)
    assert all(category.deleted_at is None for category in categories)
    assert len({category.id for category in categories}) == 4


async def test_overrides_reach_every_seeded_row(database: Database) -> None:
    await run_seeders([SeedCategories(3, title="pinned")], database)

    async with database.transaction() as session:
        categories = await Categories(session).list()

    assert [category.title for category in categories] == ["pinned"] * 3


# -- the registry and the bound database ----------------------------------


async def test_a_registry_runs_its_seeders_in_registration_order(database: Database) -> None:
    registry = SeederRegistry(SeedCategories(2))
    registry.add(SeedItems())

    results = await registry.run(database)

    assert [result.name for result in results] == ["SeedCategories", "SeedItems"]
    assert len(registry) == 2
    assert "SeedCategories, SeedItems" in repr(registry)


async def test_add_returns_the_registry_for_chaining() -> None:
    registry = SeederRegistry()

    assert registry.add(SeedCategories(1)).add(SeedItems()) is registry
    assert [seeder.name for seeder in registry.seeders] == ["SeedCategories", "SeedItems"]


async def test_seeding_falls_back_to_the_bound_database(database: Database) -> None:
    """So a script inside `database_lifespan` can call `run_seeders` with one argument."""
    set_database(database)
    try:
        await run_seeders([SeedCategories(2)])
    finally:
        set_database(None)

    assert await count_of(database, Category) == 2
