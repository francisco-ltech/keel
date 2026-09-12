"""Shared fixtures.

The important one is ``store``: a parametrised fixture that yields every driver
claiming to satisfy the cache contract. Any test using it runs once per driver,
which is what turns the contract from documentation into something enforced.

``NullStore`` is deliberately absent from that parametrisation. It implements the
*interface* but not the *behaviour* — a store that never retains anything cannot
satisfy "put then get returns the value" — so it is covered by its own tests
instead. Null Object implementations are the standard exception to Liskov
substitutability, and pretending otherwise would mean weakening the contract for
every other driver.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest

from keel.cache.fake import FakeStore
from keel.cache.stores.array import ArrayStore
from keel.cache.stores.eventful import EventfulStore
from keel.contracts.cache import Store
from keel.support.events import EventDispatcher
from keel.support.keys import KeyNamespace

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380/0")


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Run async tests on asyncio only; Keel does not support trio."""
    return "asyncio"


class ManualClock:
    """A monotonic clock the test advances by hand.

    TTL behaviour is a core part of the contract and asserting on it with real
    sleeps would make the suite slow and flaky. Injecting the clock keeps those
    assertions exact and instant.
    """

    __slots__ = ("_now",)

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start

    def __call__(self) -> float:
        """Return the current instant."""
        return self._now

    def advance(self, seconds: float) -> None:
        """Move time forward."""
        self._now += seconds


@pytest.fixture
def clock() -> ManualClock:
    """A hand-advanced clock for TTL assertions."""
    return ManualClock()


@pytest.fixture
def namespace() -> KeyNamespace:
    """A namespace unique to the test run, so a shared Redis stays safe."""
    return KeyNamespace(f"keel-test:{os.getpid()}")


@pytest.fixture(scope="session")
def redis_url() -> str:
    """The Redis URL under test, skipping the session if it is unreachable."""
    pytest.importorskip("redis", reason="redis client not installed")
    import anyio
    from redis.asyncio import Redis

    async def ping() -> bool:
        client: Redis = Redis.from_url(REDIS_URL)
        try:
            await client.ping()
            return True
        except Exception:  # noqa: BLE001 — any failure means "not available"
            return False
        finally:
            await client.aclose()

    if not anyio.run(ping):
        pytest.skip(f"no Redis reachable at {REDIS_URL}")
    return REDIS_URL


@pytest.fixture
async def array_store(namespace: KeyNamespace) -> AsyncIterator[Store]:
    """An in-memory store."""
    store = ArrayStore(namespace)
    yield store
    await store.close()


@pytest.fixture
async def redis_store(redis_url: str, namespace: KeyNamespace) -> AsyncIterator[Store]:
    """A Redis-backed store, flushed before and after so runs cannot bleed."""
    from keel.cache.stores.redis import RedisStore

    store = RedisStore.from_url(redis_url, namespace)
    await store.flush()
    yield store
    await store.flush()
    await store.close()


@pytest.fixture
async def fake_store(namespace: KeyNamespace) -> AsyncIterator[Store]:
    """The recording fake over an in-memory store.

    Included in the contract parametrisation on purpose: a test double that does
    not behave like the thing it doubles is worse than no double at all.
    """
    store = FakeStore(ArrayStore(namespace))
    yield store
    await store.close()


@pytest.fixture
async def eventful_store(namespace: KeyNamespace) -> AsyncIterator[Store]:
    """An instrumented store, to prove the decorator is behaviour-preserving."""
    store = EventfulStore(ArrayStore(namespace), EventDispatcher(), "test")
    yield store
    await store.close()


@pytest.fixture(
    params=[
        pytest.param("array_store", id="array"),
        pytest.param("fake_store", id="fake"),
        pytest.param("eventful_store", id="eventful"),
        pytest.param("redis_store", id="redis", marks=pytest.mark.redis),
    ]
)
def store(request: pytest.FixtureRequest) -> Store:
    """Every store that claims to satisfy the cache contract.

    Args:
        request: Supplies the name of the concrete fixture to resolve.

    Returns:
        One store per parametrisation.
    """
    resolved: Store = request.getfixturevalue(request.param)
    return resolved


@pytest.fixture(autouse=True)
def _unbind_cache() -> Iterator[None]:
    """Leave no cache bound between tests.

    Without this a test that installs a manager and fails before tearing it down
    would silently change the next test's behaviour.
    """
    from keel.cache.proxy import set_cache_manager

    yield
    set_cache_manager(None)


DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+asyncpg://keel:keel@localhost:5433/keel")


@pytest.fixture(scope="session")
def database_url(worker_id: str) -> Iterator[str]:
    """The Postgres URL under test, skipping the session if it is unreachable.

    Under xdist each worker gets **its own database**, created here and dropped
    at the end of the session. Test modules create and drop their own tables in
    a shared database, which is fine serially and a race in parallel: one
    module's ``drop_all`` runs while another is mid-``create_all``. Namespacing
    the tables instead would mean touching every fixture and would still leave
    advisory locks and sequences shared. One database per worker removes the
    whole class of interference in one place.

    Serially there is no worker, and the configured database is used directly.
    """
    pytest.importorskip("asyncpg", reason="asyncpg not installed")
    import anyio
    from sqlalchemy import text

    from keel.database import Database, DatabaseConfig

    async def reachable(url: str) -> bool:
        database = Database(DatabaseConfig(url=url, statement_timeout=None))
        try:
            async with database.connect() as connection:
                await connection.execute(text("SELECT 1"))
            return True
        except Exception:  # noqa: BLE001 — any failure means "not available"
            return False
        finally:
            await database.close()

    if not anyio.run(reachable, DATABASE_URL):
        pytest.skip(f"no Postgres reachable at {DATABASE_URL}")

    if worker_id == "master":
        yield DATABASE_URL
        return

    import asyncpg

    base, _, _ = DATABASE_URL.rpartition("/")
    name = f"keel_test_{worker_id}"
    admin = DATABASE_URL.replace("+asyncpg", "")

    async def administer(statement: str) -> None:
        connection = await asyncpg.connect(admin)
        try:
            await connection.execute(statement)
        finally:
            await connection.close()

    anyio.run(administer, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    anyio.run(administer, f'CREATE DATABASE "{name}"')
    try:
        yield f"{base}/{name}"
    finally:
        anyio.run(administer, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture(scope="session")
def worker_id() -> str:
    """The xdist worker name, or ``master`` when running serially.

    Declared here rather than relying on xdist's own fixture so the suite still
    collects when xdist is not installed.
    """
    return os.environ.get("PYTEST_XDIST_WORKER", "master")


@pytest.fixture
def namespace_suffix() -> str:
    """A per-test suffix, so parallel or repeated runs cannot collide on Redis."""
    return f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
