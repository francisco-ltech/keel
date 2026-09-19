"""The database subsystem.

A model, a repository, and a unit of work::

    from keel.database import (
        Model, Repository, SoftDeleteMixin, TimestampMixin,
    UUIDPrimaryKey, uow,
    )

    class Post(Model, UUIDPrimaryKey, TimestampMixin, SoftDeleteMixin):
        __tablename__ = "posts"
        title: Mapped[str]

    class Posts(Repository[Post]):
        model = Post

    async def publish(post_id: UUID) -> PostRead:
        async with uow() as session:
            post = await Posts(session).get_or_fail(post_id)
            post.published_at = utcnow()
            return PostRead.model_validate(post)

and wiring is one context manager, as it is for the cache::

    async with database_lifespan(DatabaseConfig.from_env()):
        ...

There is deliberately no ``get_session`` dependency to import. See
:mod:`keel.database.engine` for why holding a session across a request is the
one thing this subsystem will not help you do.

A note on fakes, since every other Keel subsystem has one: the database has
none, and that is not an omission. A cache, a queue and a mailer are all
worth faking because their real implementations are slow, remote or
irreversible. A database's test double is a *real database inside a transaction
that gets rolled back* — anything else stops testing the queries, which are the
part most likely to be wrong. :func:`keel.testing.rollback_session` provides it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, AbstractContextManager, asynccontextmanager
from typing import TYPE_CHECKING

from keel.database.config import DatabaseConfig
from keel.database.engine import Database
from keel.database.ids import timestamp_of, uuid7
from keel.database.model import (
    NAMING_CONVENTION,
    Model,
    PublicId,
    TimestampMixin,
    UUIDPrimaryKey,
    utcnow,
)
from keel.database.observers import ModelEvent, Observer, observe
from keel.database.pagination import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, Page
from keel.database.repository import Repository
from keel.database.soft_delete import SoftDeleteMixin, only_deleted_option, with_deleted
from keel.support.binding import Binding

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

_binding: Binding[Database] = Binding(
    "database",
    "call keel.database.set_database(...) during startup, "
    "or use keel.database.use_database(...) in a test",
)


def set_database(database: Database | None) -> None:
    """Install the process-wide database.

    Args:
        database: The database to install, or ``None`` to unbind.
    """
    _binding.set(database)


def current_database() -> Database:
    """Return the database currently in effect.

    Returns:
        The context-local override if one is active, otherwise the process-wide
        database.

    Raises:
        ConfigurationError: If none is bound.
    """
    return _binding.current()


def use_database(database: Database) -> AbstractContextManager[Database]:
    """Override the bound database for the duration of a block.

    Args:
        database: The database to use.

    Returns:
        A context manager yielding the same database.
    """
    return _binding.use(database)


def uow() -> AbstractAsyncContextManager[AsyncSession]:
    """Open a transaction on the bound database.

    The unit-of-work idiom, and the only sanctioned way to reach the database
    from application code::

        async with uow() as session:
            ...

    Commits on a clean exit, rolls back on an exception, and closes the session
    either way — which is what returns the connection to the pool. Keep the
    block tight: it should span the work that must be atomic, not the request.

    Returns:
        An async context manager yielding a session inside a transaction.
    """
    return current_database().transaction()


@asynccontextmanager
async def database_lifespan(config: DatabaseConfig) -> AsyncIterator[Database]:
    """Bind a database for the life of the application.

    Framework-agnostic, like the cache's: it drops into a FastAPI ``lifespan``,
    a worker process or a script unchanged.

    Restores whatever was bound before rather than unbinding, so a test lifespan
    nested inside an application lifespan does not silently kill the outer one.

    Args:
        config: How to reach and pool the database.

    Yields:
        The bound database.
    """
    previous = _binding.peek()
    database = Database(config)
    set_database(database)
    try:
        yield database
    finally:
        await database.close()
        set_database(previous)


__all__ = [
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "NAMING_CONVENTION",
    "Database",
    "DatabaseConfig",
    "Model",
    "ModelEvent",
    "Observer",
    "Page",
    "PublicId",
    "Repository",
    "SoftDeleteMixin",
    "TimestampMixin",
    "UUIDPrimaryKey",
    "current_database",
    "database_lifespan",
    "observe",
    "only_deleted_option",
    "set_database",
    "timestamp_of",
    "uow",
    "use_database",
    "utcnow",
    "uuid7",
    "with_deleted",
]
