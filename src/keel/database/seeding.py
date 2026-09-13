"""Seeding a database with development and demo data.

Seeding is where hand-written fixtures go to rot. The usual shape is a script
that inserts rows with literal keyword arguments, which starts out as a copy of
the test fixtures and diverges from them on the first schema change — with the
result that the demo data and the test data disagree about what a valid row is,
and only one of them is checked by CI. So seeders here build rows through the
same :mod:`~keel.database.factories` the tests use. A column added to a model is
picked up by both, or by neither.

Three properties are what this module actually provides.

**All or nothing.** Every seeder in a run shares one transaction. A seeder that
raises halfway leaves the database exactly as it found it, rather than
half-populated — which is the state that produces the worst kind of bug report,
because the operator's next move is to run the seeder again and the second run
now sees data that should not exist.

**Order is declared, not discovered.** Seeders run in the order given. A seeder
that needs another's rows depends on it by being listed after it, which is a
weaker mechanism than a dependency graph and a much easier one to read. Each
seeder is flushed before the next begins — sessions here have ``autoflush``
off — so "listed after" genuinely means "can see".

**Idempotency has one implementation.** Re-running a seeder against a populated
database must not duplicate rows, and left to each seeder that becomes a
hand-rolled count check per class, written slightly differently every time and
forgotten once. :class:`SeedIfEmpty` expresses the guard once::

    class Roles(SeedIfEmpty):
        model = Role

        async def run(self, session: AsyncSession) -> None:
            await RoleFactory.create_many(session, 3)

There is deliberately no CLI here. A library that grew a command-line entry
point would have to own argument parsing, logging configuration and an event
loop policy on behalf of an application that already has all three.
:func:`run_seeders` is an ``await``-able an application calls from its own
entry point.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Final

from sqlalchemy import literal, select

from keel.database.engine import Database
from keel.database.factories import ModelFactory
from keel.database.model import Model
from keel.database.soft_delete import with_deleted

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger: Final = logging.getLogger("keel.database.seeding")
"""Named for the module rather than ``__name__`` so an application can raise or
silence seeding output by name without importing anything."""


class Seeder(ABC):
    """One unit of seed data.

    Subclasses implement :meth:`run` and, if they are not safe to run twice,
    :meth:`should_run` — or inherit :class:`SeedIfEmpty`, which implements the
    common answer.

    Attributes:
        name: Used in log output. Defaults to the class name, because a seeder
            called ``Roles`` needs no better label and requiring one would just
            produce ``name = "Roles"`` on every subclass.
    """

    name: ClassVar[str] = "Seeder"

    __slots__ = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Default :attr:`name` to the subclass's own name.

        Args:
            **kwargs: Passed through to the base implementation.
        """
        super().__init_subclass__(**kwargs)
        if "name" not in cls.__dict__:
            cls.name = cls.__name__

    @abstractmethod
    async def run(self, session: AsyncSession) -> None:
        """Insert this seeder's rows.

        Never commits. The transaction spans the whole run so that a later
        failure can undo this seeder too; committing here would break that.

        Args:
            session: The shared session for this run.
        """

    async def should_run(self, session: AsyncSession) -> bool:
        """Whether this seeder has work to do.

        Defaults to ``True``: a seeder is assumed to be safe to run, and one
        that is not says so. The alternative default — skip unless proven
        necessary — would silently do nothing for the seeder that forgot to
        override it, which is a much harder failure to notice than a duplicate.

        Args:
            session: The shared session for this run.

        Returns:
            ``True`` to run, ``False`` to skip.
        """
        _ = session
        return True


class SeedIfEmpty(Seeder, ABC):
    """A seeder that runs only while its table is empty.

    The idempotency guard, written once::

        class Roles(SeedIfEmpty):
            model = Role

            async def run(self, session: AsyncSession) -> None:
                await RoleFactory.create_many(session, 3)

    Emptiness is checked including soft-deleted rows. A table whose every row
    has been soft deleted still holds them, and their unique constraints —
    re-seeding into it is how you discover that the hard way, at 2am, from a
    duplicate key error naming a row that no query can see.

    Attributes:
        model: The mapped class whose emptiness gates this seeder.
    """

    model: ClassVar[type[Model]]

    __slots__ = ()

    async def should_run(self, session: AsyncSession) -> bool:
        """Whether :attr:`model`'s table has no rows at all.

        Args:
            session: The shared session for this run.

        Returns:
            ``True`` if the table is empty.
        """
        statement = with_deleted(select(literal(1)).select_from(self.model).limit(1))
        result = await session.execute(statement)
        return result.first() is None


class FactorySeeder(SeedIfEmpty, ABC):
    """Seeds rows for one model through its factory.

    The shortest path to the property this module exists for — seed data and
    test data built by the same code::

        class Users(FactorySeeder):
            factory = UserFactory

        await run_seeders([Users(25, is_active=True)], database)

    The gated model is taken from the factory, since a factory already knows
    which model it builds and repeating it is one more thing to get wrong.

    Args:
        count: How many rows to insert.
        **overrides: Column values pinned on every row — a tenant, a flag.
            Anything that should differ per row is left to the factory.

    Attributes:
        factory: The factory to build rows with. Its ``__model__`` becomes
            :attr:`~SeedIfEmpty.model`.
    """

    # `ClassVar` cannot hold a type variable, so the model parameter is erased here.
    # Nothing is lost: the concrete subclass still names a parameterised factory.
    factory: ClassVar[type[ModelFactory[Any]]]

    __slots__ = ("_count", "_overrides")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Derive :attr:`~SeedIfEmpty.model` from the declared factory.

        Args:
            **kwargs: Passed through to the base implementation.
        """
        super().__init_subclass__(**kwargs)
        factory = cls.__dict__.get("factory")
        if factory is not None:
            cls.model = factory.__model__

    def __init__(self, count: int = 1, **overrides: Any) -> None:
        self._count = count
        self._overrides = overrides

    async def run(self, session: AsyncSession) -> None:
        """Insert :attr:`count` rows built by :attr:`factory`.

        Args:
            session: The shared session for this run.
        """
        await self.factory.create_many(session, self._count, **self._overrides)


@dataclass(frozen=True, slots=True)
class SeedResult:
    """What one seeder did.

    Returned rather than only logged so a caller — a deployment script, a test
    — can assert on the outcome instead of scraping log output.

    Attributes:
        name: The seeder's name.
        ran: ``False`` when :meth:`Seeder.should_run` declined, which is the
            normal outcome of seeding an already-populated database.
    """

    name: str
    ran: bool


class SeederRegistry:
    """An ordered collection of seeders to run together.

    Worth having over a bare list because the order *is* the dependency
    declaration, and a mutable list passed around gains entries in unpredictable
    places. Registration is explicit and one-directional::

        registry = SeederRegistry(Roles(), Users(25))
        registry.add(Posts(100))
        await registry.run(database)

    Args:
        *seeders: The seeders, in the order they must run.
    """

    __slots__ = ("_seeders",)

    def __init__(self, *seeders: Seeder) -> None:
        self._seeders: list[Seeder] = list(seeders)

    def add(self, *seeders: Seeder) -> SeederRegistry:
        """Append seeders to the end of the run.

        Args:
            *seeders: The seeders to add.

        Returns:
            This registry, so registration can be chained.
        """
        self._seeders.extend(seeders)
        return self

    @property
    def seeders(self) -> tuple[Seeder, ...]:
        """The registered seeders, in run order."""
        return tuple(self._seeders)

    async def run(self, database: Database | None = None) -> tuple[SeedResult, ...]:
        """Run every registered seeder in one transaction.

        Args:
            database: The database to seed. Defaults to the bound one.

        Returns:
            One result per seeder, in run order.
        """
        return await run_seeders(self._seeders, database)

    def __len__(self) -> int:
        """The number of registered seeders."""
        return len(self._seeders)

    def __repr__(self) -> str:
        """List the seeder names, which is what a failing run needs."""
        names = ", ".join(seeder.name for seeder in self._seeders)
        return f"<SeederRegistry [{names}]>"


async def run_seeders(
    seeders: Sequence[Seeder],
    database: Database | None = None,
) -> tuple[SeedResult, ...]:
    """Run *seeders* in order, inside a single transaction.

    The transaction is the contract. If any seeder raises, nothing any of them
    did is committed and the exception propagates unchanged — a caller should
    see the real failure, not a wrapper, and the log line immediately above it
    names the seeder that produced it.

    Example:
        >>> await run_seeders([Roles(), Users(25)], database)  # doctest: +SKIP

    Args:
        seeders: The seeders, in dependency order.
        database: The database to seed. Defaults to the bound one, so a script
            inside ``database_lifespan`` need not pass anything.

    Returns:
        One result per seeder, in run order, saying which actually ran.
    """
    target = database if database is not None else _bound_database()
    results: list[SeedResult] = []

    async with target.transaction() as session:
        for seeder in seeders:
            if not await seeder.should_run(session):
                logger.info("skipping seeder %s: nothing to do", seeder.name)
                results.append(SeedResult(name=seeder.name, ran=False))
                continue

            logger.info("running seeder %s", seeder.name)
            await seeder.run(session)
            # Autoflush is off, so staged rows are invisible until flushed. This is
            # what lets the next seeder's `should_run` — and its foreign keys — see them.
            await session.flush()
            results.append(SeedResult(name=seeder.name, ran=True))

    return tuple(results)


def _bound_database() -> Database:
    """Return the process-wide database.

    Imported inside the function on purpose: :mod:`keel.database` may come to
    re-export this module, and a module-level import would then be a cycle. The
    engine itself is imported normally, since it cannot import this one.

    Returns:
        The bound database.

    Raises:
        ConfigurationError: If none is bound.
    """
    from keel.database import current_database

    return current_database()


__all__ = [
    "FactorySeeder",
    "SeedIfEmpty",
    "SeedResult",
    "Seeder",
    "SeederRegistry",
    "run_seeders",
]
