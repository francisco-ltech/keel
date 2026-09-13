"""Redis-specific behaviour the shared contract suite cannot reach.

The contract suite runs `RedisStore` with the default `JsonSerializer`, so the
whole emulated-increment path never executes there — and that fallback is the
most intricate code in the driver. It is exercised here, along with the two
safety properties that only mean anything against a real server: that `flush`
cannot reach outside its own namespace, and that a store only closes a client it
created.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable

import pytest
from redis.asyncio import Redis

from keel.cache.stores.redis import SCAN_BATCH, RedisStore
from keel.exceptions import CacheValueError
from keel.support.keys import KeyNamespace
from keel.support.sentinels import MISSING
from keel.support.serialization import PickleSerializer

pytestmark = [pytest.mark.anyio, pytest.mark.redis]


@pytest.fixture
async def pickle_store(redis_url: str, namespace: KeyNamespace) -> AsyncIterator[RedisStore]:
    """A store whose encoding Redis cannot increment natively."""
    store = RedisStore.from_url(redis_url, namespace.child("pickle"), PickleSerializer())
    await store.flush()
    yield store
    await store.flush()
    await store.close()


# -- the emulated increment path ------------------------------------------


async def test_pickle_encoding_reports_that_it_cannot_increment_natively(
    pickle_store: RedisStore,
) -> None:
    assert pickle_store.supports_atomic_increment is False


async def test_increment_still_works_under_an_encoding_redis_cannot_read(
    pickle_store: RedisStore,
) -> None:
    """The guarantee holds either way; only the cost changes."""
    assert await pickle_store.increment("counter") == 1
    assert await pickle_store.increment("counter") == 2
    assert await pickle_store.increment("counter", 10) == 12
    assert await pickle_store.increment("counter", -2) == 10


async def test_emulated_increment_rejects_a_non_numeric_value(
    pickle_store: RedisStore,
) -> None:
    await pickle_store.put("word", "not a number")
    with pytest.raises(CacheValueError) as error:
        await pickle_store.increment("word")
    assert "not an integer" in str(error.value)


async def test_emulated_increment_rejects_a_bool(pickle_store: RedisStore) -> None:
    """``bool`` is an ``int`` subclass; incrementing one is a caller error."""
    await pickle_store.put("flag", True)
    with pytest.raises(CacheValueError):
        await pickle_store.increment("flag")


async def test_emulated_increment_preserves_the_existing_expiry(
    pickle_store: RedisStore,
) -> None:
    """Counting an event must not extend the window it is counted in.

    This is the subtle half of the fallback: ``INCRBY`` preserves the TTL for
    free, so the emulation has to read the remaining time and put it back.
    """
    await pickle_store.put("counter", 5, 30)
    await pickle_store.increment("counter")

    remaining = await pickle_store.client.pttl(pickle_store.namespace.apply("counter"))
    assert 0 < remaining <= 30_000, f"expiry was not preserved: pttl={remaining}"


async def test_emulated_increment_on_a_new_key_leaves_it_without_an_expiry(
    pickle_store: RedisStore,
) -> None:
    await pickle_store.increment("fresh")
    remaining = await pickle_store.client.pttl(pickle_store.namespace.apply("fresh"))
    assert remaining == -1, "a counter created by increment should not expire"


async def test_emulated_increment_round_trips_through_the_pickle_encoding(
    pickle_store: RedisStore,
) -> None:
    await pickle_store.increment("counter", 7)
    assert await pickle_store.get("counter") == 7


# -- namespace safety ------------------------------------------------------


async def test_flush_leaves_keys_outside_the_namespace_alone(
    redis_url: str, namespace: KeyNamespace
) -> None:
    """The reason ``flush`` scans instead of calling ``FLUSHDB``.

    Caches share servers. A cache clear must never be capable of wiping a queue,
    a session table, or another application.
    """
    mine = RedisStore.from_url(redis_url, namespace.child("mine"))
    theirs = RedisStore.from_url(redis_url, namespace.child("theirs"))
    try:
        await mine.put("key", "mine")
        await theirs.put("key", "theirs")

        await mine.flush()

        assert await mine.get("key") is MISSING
        assert await theirs.get("key") == "theirs"
    finally:
        await theirs.flush()
        await mine.close()
        await theirs.close()


async def test_flush_pages_through_more_keys_than_one_scan_returns(
    redis_url: str, namespace: KeyNamespace
) -> None:
    """SCAN is cursor-based; a single call does not see the whole keyspace."""
    store = RedisStore.from_url(redis_url, namespace.child("many"))
    try:
        count = SCAN_BATCH * 2 + 50
        await store.put_many({f"key:{i}": i for i in range(count)}, None)

        assert await store.flush() is True

        remaining = await store.client.scan(0, match=store.namespace.pattern(), count=SCAN_BATCH)
        assert remaining[1] == [], "flush left keys behind"
    finally:
        await store.close()


# -- bulk writes -----------------------------------------------------------


async def test_put_many_with_a_non_positive_ttl_evicts_instead_of_writing(
    redis_store: RedisStore,
) -> None:
    await redis_store.put_many({"a": 1, "b": 2})
    assert await redis_store.put_many({"a": 10, "b": 20}, 0) is False
    assert await redis_store.get("a") is MISSING
    assert await redis_store.get("b") is MISSING


async def test_put_many_applies_one_ttl_to_every_key(redis_store: RedisStore) -> None:
    await redis_store.put_many({"a": 1, "b": 2}, 30)
    for key in ("a", "b"):
        remaining = await redis_store.client.pttl(redis_store.namespace.apply(key))
        assert 0 < remaining <= 30_000


# -- client ownership ------------------------------------------------------


def _spy_on_close(store: RedisStore) -> Callable[[], bool]:
    """Record whether the store closes its client, without closing it for real.

    Asserting on the client's behaviour *after* a close is a test of redis-py
    (which quietly reconnects), not of Keel. What matters here is the ownership
    decision, so that is what gets observed.
    """
    closed = False
    original = store.client.aclose

    async def spy(close_connection_pool: bool | None = None) -> None:
        nonlocal closed
        closed = True
        await original(close_connection_pool)

    store.client.aclose = spy  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    return lambda: closed


async def test_a_store_given_a_client_does_not_close_it(redis_url: str) -> None:
    """Ownership matters: closing a pool the application still uses is a bug."""
    client: Redis = Redis.from_url(redis_url, decode_responses=False)
    try:
        store = RedisStore(client, KeyNamespace("keel-test:borrowed"))
        was_closed = _spy_on_close(store)
        await store.put("key", "value")
        await store.close()

        assert was_closed() is False, "close() shut down a client it did not own"
        assert await client.ping() is True
    finally:
        await client.aclose()


async def test_a_store_that_built_its_own_client_closes_it(redis_url: str) -> None:
    store = RedisStore.from_url(redis_url, KeyNamespace("keel-test:owned"))
    was_closed = _spy_on_close(store)
    await store.put("key", "value")
    await store.close()

    assert was_closed() is True


async def test_the_client_property_exposes_the_underlying_connection(
    redis_store: RedisStore,
) -> None:
    """Documented escape hatch for operations outside the cache contract."""
    assert await redis_store.client.ping() is True


# -- encoding tolerance ----------------------------------------------------


async def test_a_decoding_client_still_round_trips(redis_url: str) -> None:
    """``_as_bytes`` exists for exactly this: a client configured the other way."""
    client: Redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        store = RedisStore(client, KeyNamespace("keel-test:decoded"))
        await store.put("key", {"nested": [1, 2]})
        assert await store.get("key") == {"nested": [1, 2]}
        assert (await store.many(["key"]))["key"] == {"nested": [1, 2]}
        await store.flush()
    finally:
        await client.aclose()
