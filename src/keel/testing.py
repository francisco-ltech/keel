"""Test helpers.

Shipped as part of the library rather than kept in the test suite, because the
fakes are for *applications built on Keel*, not only for Keel's own tests. A
battery whose test double lives in the framework's private test directory is a
battery nobody else can test against.

The two helpers here are deliberately different in kind, and the difference is
the point:

* :func:`fake_cache` swaps the cache for a recording double. Faking is right for
  a cache because the real thing is remote and slow, and because what a test
  wants to know is *what the code did to it*.
* :func:`rolled_back_database` does the opposite — it gives you the **real**
  database and throws away the changes. Faking a database means not testing the
  queries, which are the part most likely to be wrong.

Knowing which subsystems deserve a fake and which deserve a real instance with
an undo is most of what makes a test suite trustworthy.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import async_sessionmaker

from keel.cache.config import CacheConfig, StoreConfig
from keel.cache.fake import FakeStore
from keel.cache.manager import CacheManager
from keel.cache.proxy import use_cache
from keel.cache.repository import Repository
from keel.contracts.cache import Store
from keel.database import use_database
from keel.database.engine import Database
from keel.queue.config import QueueConfig
from keel.queue.dispatch import use_queue
from keel.queue.fake import FakeQueue
from keel.queue.manager import QueueManager

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection


@contextmanager
def fake_cache(
    *,
    default_ttl: float | None = 300.0,
    inner: Store | None = None,
) -> Iterator[FakeStore]:
    """Replace the bound cache with a recording fake for the duration of a block.

    The override is context-local, so tests running concurrently do not see each
    other's cache and no teardown is needed beyond leaving the block.

    Example:
        >>> with fake_cache() as cached:  # doctest: +SKIP
        ...     await refresh_profile(42)
        ...     cached.assert_put("profile:42")

    Args:
        default_ttl: The lifetime applied when the code under test does not
            specify one. Match your production configuration if the test asserts
            on TTLs.
        inner: The store the fake should delegate to. Defaults to a fresh
            in-memory store; pass a real one to record against a live backend.

    Yields:
        The fake, for assertions.
    """
    store = FakeStore(inner)
    config = CacheConfig(
        default="default",
        stores={"default": StoreConfig(driver="array", ttl=default_ttl)},
    )
    manager = CacheManager(config)
    # Replace the configured store wholesale rather than adding a "fake" driver: the
    # code under test asks for the default by name and must get *this* instance.
    manager.extend("default", lambda name: Repository(store, default_ttl, name))
    with use_cache(manager):
        yield store


class _SavepointDatabase(Database):
    """A database whose sessions join an already-open transaction.

    Constructed by :func:`rolled_back_database`. It reuses the original
    database's engine — building a second one would open a second pool, and the
    whole trick depends on every session sharing one connection.

    ``__init__`` copies *every* declared slot rather than the two or three it
    happens to need. Naming them individually has already broken twice in this
    codebase — once here and once on ``CacheProxy`` — because adding a field to the
    parent silently leaves it unset on the subclass, and the ``AttributeError``
    surfaces somewhere unrelated. A loop over ``__slots__`` cannot go stale.
    """

    __slots__ = ()

    def __init__(self, origin: Database, connection: AsyncConnection) -> None:
        # Bypasses Database.__init__ on purpose: that builds an engine, and this
        # object must borrow rather than create one. Copies every slot; see above.
        for slot in Database.__slots__:
            setattr(self, slot, getattr(origin, slot))
        self._sessions = async_sessionmaker(
            bind=connection,
            expire_on_commit=False,
            autoflush=False,
            # Makes session.commit() release a SAVEPOINT instead of committing the
            # outer transaction, so code under test can commit and still be undone.
            join_transaction_mode="create_savepoint",
        )


@asynccontextmanager
async def rolled_back_database(database: Database) -> AsyncIterator[Database]:
    """Run a block against the real database and undo everything afterwards.

    This is the ``RefreshDatabase`` equivalent, and it is what makes a database
    test suite fast enough to run on every save: no truncation, no re-seeding,
    no migrations between tests — one outer transaction per test, rolled back at
    the end.

    Code under test can call ``commit()`` normally. Sessions are bound to a
    single connection with ``join_transaction_mode="create_savepoint"``, so a
    commit releases a savepoint and the outer transaction still owns the undo.

    The bound database is overridden for the duration, so application code
    reaching the database through :func:`keel.database.uow` participates without
    knowing anything about the test.

    Example:
        >>> async with rolled_back_database(app_database):  # doctest: +SKIP
        ...     await create_user(payload)

    Args:
        database: The real database to borrow an engine and connection from.

    Yields:
        The overridden database, should a test need it directly.
    """
    async with database.engine.connect() as connection:
        transaction = await connection.begin()
        bound = _SavepointDatabase(database, connection)
        try:
            with use_database(bound):
                yield bound
        finally:
            await transaction.rollback()


@contextmanager
def fake_queue(*, driver: str = "fake") -> Iterator[FakeQueue]:
    """Replace the bound queue with a recording fake for the duration of a block.

    Records dispatches without running them — see :mod:`keel.queue.fake` for why
    that differs from how the cache fake works. To run jobs instead, bind a
    manager configured with the ``sync`` driver.

    Example:
        >>> with fake_queue() as queued:  # doctest: +SKIP
        ...     await invoices.approve(invoice_id)
        ...     queued.assert_pushed(SendInvoiceEmail, invoice_id=str(invoice_id))

    Args:
        driver: The driver name to bind under. Rarely worth changing.

    Yields:
        The fake, for assertions.
    """
    manager = QueueManager(QueueConfig(driver=driver))
    recorder = FakeQueue(driver)
    manager.extend(driver, lambda _name: recorder)
    with use_queue(manager):
        yield recorder


__all__ = ["fake_cache", "fake_queue", "rolled_back_database"]
