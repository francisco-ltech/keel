"""Regression tests for defects found in the Phase 1 review.

Each test here corresponds to something that was actually wrong and was actually
verified against a live Redis before being fixed. They are grouped in one file
on purpose: a reviewer coming to this code later can read it as the list of
mistakes already made, which is more useful than the same assertions scattered
across ten modules.
"""

from __future__ import annotations

import anyio
import pytest
from redis.asyncio import Redis

from keel.cache.config import DEFAULT_PREFIX, CacheConfig, StoreConfig
from keel.cache.counters import COUNTER_MAX, COUNTER_MIN, guard_overflow
from keel.cache.manager import CacheManager
from keel.cache.repository import Repository
from keel.cache.stores.array import ArrayStore
from keel.cache.stores.redis import DEFAULT_NAMESPACE, RedisStore
from keel.exceptions import CacheValueError, ConfigurationError
from keel.support.keys import KeyNamespace
from keel.support.manager import Manager
from keel.support.serialization import PickleSerializer

pytestmark = [pytest.mark.anyio]


# -- CRITICAL: flush() was FLUSHDB whenever the prefix was empty ----------


def test_a_redis_store_without_a_namespace_gets_a_safe_default() -> None:
    """It used to get the empty namespace, whose pattern was ``*``."""
    store = RedisStore(Redis(), None)
    assert store.namespace == KeyNamespace(DEFAULT_NAMESPACE)
    assert not store.namespace.is_empty


def test_a_redis_store_refuses_an_explicitly_empty_namespace() -> None:
    """Passing one deliberately is a decision, and it is the wrong one."""
    with pytest.raises(ConfigurationError) as error:
        RedisStore(Redis(), KeyNamespace(""))
    assert "flush()" in str(error.value)


def test_from_env_defaults_to_a_real_prefix() -> None:
    """The documented happy path used to produce an unnamespaced store."""
    assert CacheConfig.from_env({}).store().prefix == DEFAULT_PREFIX


@pytest.mark.redis
async def test_flush_cannot_reach_keys_outside_the_store(redis_url: str) -> None:
    """The headline defect, as a black-box test.

    A store with no namespace scanned ``*`` and unlinked every key on the
    server, including another application's session and a queue's jobs.
    """
    raw: Redis = Redis.from_url(redis_url, decode_responses=False)
    try:
        await raw.set("someone-elses-session", b"important")
        await raw.set("queue:jobs", b"important")

        store = RedisStore.from_url(redis_url)  # no namespace given
        await store.flush()
        await store.close()

        assert await raw.exists("someone-elses-session") == 1
        assert await raw.exists("queue:jobs") == 1
    finally:
        await raw.delete("someone-elses-session", "queue:jobs")
        await raw.aclose()


# -- MAJOR: a glob metacharacter in a prefix selected other namespaces ----


@pytest.mark.redis
async def test_a_prefix_containing_a_glob_class_does_not_flush_its_neighbour(
    redis_url: str,
) -> None:
    """``KeyNamespace("app[1]")`` used to match — and delete — ``app1:*``."""
    raw: Redis = Redis.from_url(redis_url, decode_responses=False)
    try:
        await raw.set("app1:victim", b"other namespace")

        store = RedisStore.from_url(redis_url, KeyNamespace("app[1]"))
        await store.flush()
        await store.close()

        assert await raw.exists("app1:victim") == 1
    finally:
        await raw.delete("app1:victim")
        await raw.aclose()


# -- MAJOR: the emulated increment raised instead of serialising ----------


@pytest.mark.redis
async def test_concurrent_emulated_increments_all_land(redis_url: str) -> None:
    """It used to lose 19 of 20 increments to ``LockTimeoutError``.

    ``async with lock`` fails on contention rather than waiting for it, so the
    fallback path turned concurrency into a pile of errors under exactly the
    load a counter exists to handle.
    """
    store = RedisStore.from_url(redis_url, KeyNamespace("keel-test:emulated"), PickleSerializer())
    await store.flush()
    try:
        errors: list[str] = []

        async def bump() -> None:
            try:
                await store.increment("hits")
            except Exception as exc:  # noqa: BLE001 — the point is that none occur
                errors.append(type(exc).__name__)

        async with anyio.create_task_group() as tasks:
            for _ in range(20):
                tasks.start_soon(bump)

        assert errors == []
        assert await store.get("hits") == 20
    finally:
        await store.flush()
        await store.close()


# -- MAJOR: drivers disagreed at the counter boundary --------------------


def test_guard_overflow_passes_representable_values() -> None:
    assert guard_overflow("k", COUNTER_MAX) == COUNTER_MAX
    assert guard_overflow("k", COUNTER_MIN) == COUNTER_MIN


@pytest.mark.parametrize(
    "value",
    [pytest.param(COUNTER_MAX + 1, id="above"), pytest.param(COUNTER_MIN - 1, id="below")],
)
def test_guard_overflow_rejects_values_redis_cannot_hold(value: int) -> None:
    with pytest.raises(CacheValueError) as error:
        guard_overflow("counter", value)
    assert "overflow" in str(error.value)


async def test_the_in_memory_store_overflows_where_redis_would() -> None:
    """It used to return a number the Redis store cannot produce.

    Python integers are unbounded; Redis counters are 64-bit. Letting the
    in-memory store exceed the range would make a test pass and the same code
    fail in production — so the most constrained backend sets the contract.
    """
    store = ArrayStore()
    await store.put("counter", COUNTER_MAX)
    with pytest.raises(CacheValueError):
        await store.increment("counter")


# -- MAJOR: the error message named the wrong extension point ------------


def test_an_unknown_driver_points_at_register_driver() -> None:
    """It used to say ``extend()``, which takes a different kind of factory.

    Following the old message produced a manager that bypassed instrumentation
    and the configured TTL, and failed silently rather than loudly.
    """
    manager = CacheManager(
        CacheConfig(default="default", stores={"default": StoreConfig(driver="memcached")})
    )
    with pytest.raises(ConfigurationError) as error:
        manager.store()

    message = str(error.value)
    assert "register_driver" in message
    assert "extend() replaces a single configured store" in message


# -- MINOR: dropping a memoised instance leaked its resources ------------


class _Closeable:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _CloseableManager(Manager[_Closeable]):
    def __init__(self) -> None:
        super().__init__("default")
        self.built: list[_Closeable] = []

    def _make(self, name: str) -> _Closeable:
        instance = _Closeable()
        self.built.append(instance)
        return instance

    async def _close_instance(self, instance: _Closeable) -> None:
        await instance.close()


async def test_discard_closes_the_instance_it_drops() -> None:
    manager = _CloseableManager()
    instance = manager.driver()

    await manager.discard()

    assert instance.closed is True
    assert manager.driver() is not instance


async def test_discarding_an_unbuilt_name_is_a_no_op() -> None:
    await _CloseableManager().discard("never-built")


async def test_close_all_closes_every_instance_even_if_one_raises() -> None:
    """A loop that abandoned on the first error leaked the rest of the pools."""

    class Exploding(_CloseableManager):
        async def _close_instance(self, instance: _Closeable) -> None:
            await instance.close()
            if instance is self.built[0]:
                raise RuntimeError("first one fails")

    manager = Exploding()
    manager.driver("a")
    manager.driver("b")

    with pytest.raises(RuntimeError, match="first one fails"):
        await manager.close_all()

    assert all(instance.closed for instance in manager.built)
    assert list(manager.resolved_names) == []


# -- MINOR: the proxy invariant, stated as a test ------------------------


def test_every_repository_property_is_overridden_on_the_proxy() -> None:
    """The invariant the proxy design rests on, checked mechanically.

    `CacheProxy` does not call `super().__init__`, so any `Repository` state not
    reachable through an overridden property raises `AttributeError` the first
    time a call arrives through the facade. This already happened once; a test
    is cheaper than remembering.
    """
    from keel.cache.proxy import CacheProxy

    properties = {
        name
        for name, attribute in vars(Repository).items()
        if isinstance(attribute, property) and not name.startswith("_")
    }
    overridden = {
        name for name, attribute in vars(CacheProxy).items() if isinstance(attribute, property)
    }

    missing = properties - overridden
    assert not missing, (
        f"CacheProxy must override every Repository property or it breaks when "
        f"called through the facade; missing: {sorted(missing)}"
    )


@pytest.mark.redis
async def test_redis_reports_an_overflow_as_an_overflow(redis_url: str) -> None:
    """It used to be reported as "a non-numeric value", which it is not.

    ``INCRBY`` past 2^63-1 raises a ``ResponseError``; the driver caught every
    one of those and blamed the value's type, sending anyone debugging it to
    look for a string where the real problem was the range.
    """
    store = RedisStore.from_url(redis_url, KeyNamespace("keel-test:overflow"))
    try:
        await store.put("counter", COUNTER_MAX)
        with pytest.raises(CacheValueError) as error:
            await store.increment("counter")
        assert "overflow" in str(error.value)
    finally:
        await store.flush()
        await store.close()
