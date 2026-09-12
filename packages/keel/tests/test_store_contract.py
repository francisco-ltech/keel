"""The cache contract, enforced against every driver.

Each test here runs once per store in the ``store`` parametrisation. That is the
point: a driver is not "a cache" because it has the right method names, it is a
cache because it behaves identically to the others under the same assertions.

When these pass for a new driver, it is safe to configure. When one fails, the
driver is wrong — not the test — unless the contract itself is being changed, in
which case change it here first and let the failures tell you which drivers need
updating.
"""

from __future__ import annotations

import anyio
import pytest

from keel.contracts.cache import Store
from keel.exceptions import CacheValueError, LockTimeoutError
from keel.support.sentinels import MISSING, is_missing

pytestmark = [pytest.mark.anyio, pytest.mark.contract]


# -- presence and absence ------------------------------------------------


async def test_missing_key_reports_missing(store: Store) -> None:
    assert await store.get("absent") is MISSING


async def test_stored_value_round_trips(store: Store) -> None:
    await store.put("greeting", "hello")
    assert await store.get("greeting") == "hello"


async def test_none_is_a_storable_value(store: Store) -> None:
    """The reason misses use a sentinel rather than ``None``.

    A cache that cannot tell "I stored None" from "I have nothing" will
    recompute a legitimately-null result on every call, forever.
    """
    await store.put("nothing", None)
    stored = await store.get("nothing")
    assert stored is None
    assert not is_missing(stored)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(42, id="int"),
        pytest.param(3.5, id="float"),
        pytest.param(True, id="bool"),
        pytest.param("text", id="str"),
        pytest.param([1, 2, 3], id="list"),
        pytest.param({"nested": {"deep": [1, None, "x"]}}, id="dict"),
        pytest.param([], id="empty-list"),
        pytest.param("", id="empty-string"),
    ],
)
async def test_values_survive_the_round_trip(store: Store, value: object) -> None:
    await store.put("value", value)
    assert await store.get("value") == value


async def test_falsey_values_are_not_treated_as_misses(store: Store) -> None:
    """A classic cache bug: ``if not cached: recompute()``."""
    for key, value in (("zero", 0), ("empty", ""), ("false", False), ("none", None)):
        await store.put(key, value)
        assert not is_missing(await store.get(key)), f"{key} was reported missing"


# -- overwriting and removal ---------------------------------------------


async def test_put_overwrites(store: Store) -> None:
    await store.put("key", "first")
    await store.put("key", "second")
    assert await store.get("key") == "second"


async def test_forget_removes_and_reports_whether_it_existed(store: Store) -> None:
    await store.put("key", "value")
    assert await store.forget("key") is True
    assert await store.get("key") is MISSING
    assert await store.forget("key") is False


async def test_flush_clears_the_namespace(store: Store) -> None:
    await store.put_many({"a": 1, "b": 2, "c": 3})
    assert await store.flush() is True
    assert await store.get("a") is MISSING
    assert await store.get("b") is MISSING


# -- atomicity -----------------------------------------------------------


async def test_add_creates_only_when_absent(store: Store) -> None:
    assert await store.add("key", "first") is True
    assert await store.add("key", "second") is False
    assert await store.get("key") == "first"


async def test_add_succeeds_again_after_removal(store: Store) -> None:
    await store.add("key", "first")
    await store.forget("key")
    assert await store.add("key", "second") is True


async def test_concurrent_add_has_exactly_one_winner(store: Store) -> None:
    """The primitive locks are built on. If this is wrong, locks are wrong."""
    results: list[bool] = []

    async def contend(index: int) -> None:
        results.append(await store.add("contended", index, 30))

    async with anyio.create_task_group() as tasks:
        for index in range(25):
            tasks.start_soon(contend, index)

    assert sum(results) == 1, f"expected exactly one winner, got {sum(results)}"


async def test_forget_if_removes_only_on_match(store: Store) -> None:
    await store.put("key", "expected")
    assert await store.forget_if("key", "something-else") is False
    assert await store.get("key") == "expected"
    assert await store.forget_if("key", "expected") is True
    assert await store.get("key") is MISSING


async def test_forget_if_on_absent_key_is_false(store: Store) -> None:
    assert await store.forget_if("absent", "anything") is False


# -- counters ------------------------------------------------------------


async def test_increment_creates_at_zero(store: Store) -> None:
    assert await store.increment("counter") == 1
    assert await store.increment("counter") == 2
    assert await store.increment("counter", 10) == 12


async def test_increment_accepts_negative_amounts(store: Store) -> None:
    await store.increment("counter", 5)
    assert await store.increment("counter", -2) == 3


async def test_increment_rejects_non_numeric_values(store: Store) -> None:
    await store.put("word", "not a number")
    with pytest.raises(CacheValueError):
        await store.increment("word")


async def test_concurrent_increments_do_not_lose_updates(store: Store) -> None:
    async def bump() -> None:
        await store.increment("hits")

    async with anyio.create_task_group() as tasks:
        for _ in range(50):
            tasks.start_soon(bump)

    assert await store.get("hits") == 50


# -- lifetimes -----------------------------------------------------------


async def test_ttl_of_none_stores_indefinitely(store: Store) -> None:
    assert await store.put("permanent", "value", None) is True
    assert await store.get("permanent") == "value"


@pytest.mark.parametrize("ttl", [0, -1, -0.5])
async def test_non_positive_ttl_stores_nothing(store: Store, ttl: float) -> None:
    assert await store.put("ephemeral", "value", ttl) is False
    assert await store.get("ephemeral") is MISSING


async def test_non_positive_ttl_evicts_an_existing_entry(store: Store) -> None:
    await store.put("key", "value")
    await store.put("key", "replacement", 0)
    assert await store.get("key") is MISSING


async def test_add_with_non_positive_ttl_stores_nothing(store: Store) -> None:
    assert await store.add("key", "value", 0) is False
    assert await store.get("key") is MISSING


async def test_entries_expire(store: Store) -> None:
    """Uses a real short sleep: the only assertion that needs the wall clock.

    The array store's expiry is tested precisely with an injected clock in
    ``test_array_store.py``; this one exists so Redis is held to the same
    promise.
    """
    await store.put("brief", "value", 0.15)
    assert await store.get("brief") == "value"
    await anyio.sleep(0.3)
    assert await store.get("brief") is MISSING


# -- bulk operations -----------------------------------------------------


async def test_many_returns_every_key_requested(store: Store) -> None:
    await store.put("present", "value")
    result = await store.many(["present", "absent"])
    assert result["present"] == "value"
    assert result["absent"] is MISSING
    assert set(result) == {"present", "absent"}


async def test_many_of_no_keys_is_empty(store: Store) -> None:
    assert await store.many([]) == {}


async def test_put_many_stores_everything(store: Store) -> None:
    assert await store.put_many({"a": 1, "b": [2], "c": None}) is True
    assert await store.get("a") == 1
    assert await store.get("b") == [2]
    assert await store.get("c") is None


async def test_put_many_of_nothing_succeeds(store: Store) -> None:
    assert await store.put_many({}) is True


# -- locks ---------------------------------------------------------------


async def test_lock_is_exclusive(store: Store) -> None:
    first = store.lock("resource", 30)
    second = store.lock("resource", 30)
    assert await first.acquire() is True
    assert await second.acquire() is False


async def test_lock_is_available_again_after_release(store: Store) -> None:
    first = store.lock("resource", 30)
    second = store.lock("resource", 30)
    await first.acquire()
    assert await first.release() is True
    assert await second.acquire() is True


async def test_releasing_a_lock_you_lost_does_nothing(store: Store) -> None:
    """The defect the owner token exists to prevent.

    A holder whose lock expired must not be able to release the lock its
    successor now holds.
    """
    stale = store.lock("resource", 30, owner="stale-owner")
    await stale.acquire()
    await stale.force_release()

    successor = store.lock("resource", 30, owner="new-owner")
    assert await successor.acquire() is True

    assert await stale.release() is False
    assert await successor.get_owner() == "new-owner"


async def test_lock_context_manager_releases_on_error(store: Store) -> None:
    lock = store.lock("resource", 30)
    with pytest.raises(RuntimeError):
        async with lock:
            raise RuntimeError("boom")
    assert await store.lock("resource", 30).acquire() is True


async def test_entering_a_held_lock_raises(store: Store) -> None:
    holder = store.lock("resource", 30)
    await holder.acquire()
    with pytest.raises(LockTimeoutError):
        async with store.lock("resource", 30):
            pass  # pragma: no cover — the body must not run


async def test_block_times_out_on_a_held_lock(store: Store) -> None:
    holder = store.lock("resource", 30)
    await holder.acquire()
    with pytest.raises(LockTimeoutError):
        await store.lock("resource", 30).block(0.2, poll=0.05)


async def test_block_acquires_once_released(store: Store) -> None:
    holder = store.lock("resource", 30)
    await holder.acquire()

    async def release_shortly() -> None:
        await anyio.sleep(0.1)
        await holder.release()

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(release_shortly)
        acquired = await store.lock("resource", 30).block(2.0, poll=0.05)

    assert await acquired.get_owner() == acquired.owner


async def test_lock_expires_on_its_own(store: Store) -> None:
    """A holder that dies must not hold the lock forever."""
    await store.lock("resource", 0.15).acquire()
    await anyio.sleep(0.3)
    assert await store.lock("resource", 30).acquire() is True


async def test_lock_reports_no_owner_when_free(store: Store) -> None:
    assert await store.lock("resource", 30).get_owner() is None


# -- isolation -----------------------------------------------------------


async def test_keys_are_namespaced(store: Store) -> None:
    """Whatever a store does to keys, callers only ever see their own."""
    await store.put("scoped", "value")
    assert await store.get("scoped") == "value"
    assert store.namespace.apply("scoped").endswith("scoped")
