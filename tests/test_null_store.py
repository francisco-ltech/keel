"""The Null Object store, and the lock that comes with it.

``NullStore`` is excluded from the driver contract suite on purpose: it
implements the interface but not the retention behaviour. Its documented
behaviour is asserted here instead, including the deliberate absence of mutual
exclusion in ``NullLock``.
"""

from __future__ import annotations

import pytest

from keel.cache.lock import NullLock
from keel.cache.stores.null import NullStore
from keel.support.keys import KeyNamespace
from keel.support.sentinels import MISSING

pytestmark = [pytest.mark.anyio]


@pytest.fixture
def store() -> NullStore:
    return NullStore()


# -- reads ----------------------------------------------------------------


async def test_reads_always_miss(store: NullStore) -> None:
    assert await store.get("key") is MISSING


async def test_a_read_still_misses_after_a_put(store: NullStore) -> None:
    """The whole point: writes are accepted and discarded."""
    assert await store.put("key", "value") is True
    assert await store.get("key") is MISSING


async def test_many_reports_every_key_as_missing(store: NullStore) -> None:
    result = await store.many(["a", "b"])
    assert result == {"a": MISSING, "b": MISSING}


async def test_many_of_no_keys_is_empty(store: NullStore) -> None:
    assert await store.many([]) == {}


# -- writes ---------------------------------------------------------------


async def test_put_reports_success(store: NullStore) -> None:
    assert await store.put("key", "value") is True
    assert await store.put("key", "value", 60) is True
    assert await store.put("key", "value", None) is True


async def test_put_many_reports_success(store: NullStore) -> None:
    assert await store.put_many({"a": 1, "b": 2}) is True
    assert await store.put_many({}) is True


async def test_add_always_reports_that_it_created_the_entry(store: NullStore) -> None:
    """Nothing is ever present, so every ``add`` is a creation."""
    assert await store.add("key", "first") is True
    assert await store.add("key", "second") is True


async def test_increment_returns_the_delta(store: NullStore) -> None:
    assert await store.increment("counter") == 1
    assert await store.increment("counter") == 1
    assert await store.increment("counter", 7) == 7
    assert await store.increment("counter", -3) == -3


# -- removal --------------------------------------------------------------


async def test_forget_reports_that_nothing_was_removed(store: NullStore) -> None:
    await store.put("key", "value")
    assert await store.forget("key") is False


async def test_forget_if_reports_that_nothing_matched(store: NullStore) -> None:
    assert await store.forget_if("key", "value") is False


async def test_flush_succeeds(store: NullStore) -> None:
    assert await store.flush() is True


# -- interface parity -----------------------------------------------------


def test_the_namespace_is_accepted_and_reported() -> None:
    namespace = KeyNamespace("nominal")
    assert NullStore(namespace).namespace is namespace


def test_the_default_namespace_is_empty(store: NullStore) -> None:
    assert store.namespace == KeyNamespace()


def test_increments_are_reported_as_atomic(store: NullStore) -> None:
    assert store.supports_atomic_increment is True


async def test_close_is_harmless(store: NullStore) -> None:
    await store.close()


# -- locks ----------------------------------------------------------------


async def test_lock_returns_a_null_lock(store: NullStore) -> None:
    lock = store.lock("resource")
    assert isinstance(lock, NullLock)
    assert lock.name == "resource"
    assert await lock.acquire() is True


def test_an_explicit_owner_token_is_kept(store: NullStore) -> None:
    assert store.lock("resource", owner="me").owner == "me"


def test_owner_tokens_are_generated_and_distinct(store: NullStore) -> None:
    first = store.lock("resource")
    second = store.lock("resource")
    assert first.owner
    assert first.owner != second.owner


async def test_two_null_locks_on_one_name_can_both_acquire(store: NullStore) -> None:
    """Documents intent: a disabled cache grants every lock.

    This is *not* mutual exclusion, and it is deliberate. Turning the cache off
    must not deadlock an application that coordinates through a lock; code that
    needs real exclusion must not be taking it from a store that retains
    nothing.
    """
    first = store.lock("resource", 30)
    second = store.lock("resource", 30)

    assert await first.acquire() is True
    assert await second.acquire() is True


async def test_release_always_reports_success(store: NullStore) -> None:
    lock = store.lock("resource")
    assert await lock.release() is True
    assert await lock.release() is True


async def test_block_returns_immediately_even_when_another_holder_exists() -> None:
    holder = NullLock("resource")
    await holder.acquire()

    waiter = NullLock("resource")
    acquired = await waiter.block(timeout=0.0, poll=0.0)

    assert acquired is waiter


async def test_the_context_manager_grants_and_exits_cleanly() -> None:
    lock = NullLock("resource")
    async with lock as held:
        assert held is lock
    assert await lock.release() is True


async def test_the_context_manager_does_not_swallow_an_error() -> None:
    with pytest.raises(RuntimeError):
        async with NullLock("resource"):
            raise RuntimeError("boom")


async def test_the_context_manager_never_blocks_on_a_held_lock() -> None:
    """``StoreLock`` would raise ``LockTimeoutError`` here. ``NullLock`` grants."""
    await NullLock("resource").acquire()
    async with NullLock("resource") as second:
        assert await second.get_owner() == second.owner


async def test_get_owner_reports_this_instance_once_held() -> None:
    lock = NullLock("resource", owner="me")
    await lock.acquire()
    assert await lock.get_owner() == "me"


async def test_get_owner_reports_nothing_before_acquiring() -> None:
    """Matches the contract: ``None`` while the lock is free.

    A Null Object may decline to provide the *guarantee* — this lock grants to
    everyone and provides no mutual exclusion — but it should not lie about its
    own observable state, or it stops being substitutable for reasons that have
    nothing to do with the guarantee it is opting out of.
    """
    lock = NullLock("resource")
    assert await lock.get_owner() is None


async def test_force_release_marks_the_lock_free() -> None:
    lock = NullLock("resource", owner="me")
    await lock.acquire()
    await lock.force_release()
    assert await lock.get_owner() is None
    assert await lock.acquire() is True
