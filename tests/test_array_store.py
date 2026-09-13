"""Clock-dependent behaviour of the in-memory store.

The driver contract suite proves entries expire *eventually*, using a real
sleep. This file uses the injected clock to assert the exact moment they do, and
to pin down the two behaviours a wall-clock test cannot see: that an increment
does not extend the window it counts in, and that an expired entry is evicted
rather than merely hidden.
"""

from __future__ import annotations

import anyio
import pytest

from conftest import ManualClock
from keel.cache.stores.array import ArrayStore
from keel.support.keys import KeyNamespace
from keel.support.sentinels import MISSING
from keel.support.serialization import PickleSerializer

pytestmark = [pytest.mark.anyio]


@pytest.fixture
def store(clock: ManualClock) -> ArrayStore:
    return ArrayStore(KeyNamespace("keel:test"), clock=clock)


def entry_count(store: ArrayStore) -> int:
    """How many entries the dictionary physically holds, expired or not."""
    return len(store)


# -- the moment of expiry -------------------------------------------------


async def test_a_value_is_present_just_before_its_ttl(
    store: ArrayStore, clock: ManualClock
) -> None:
    await store.put("key", "value", 10)
    clock.advance(9.999)
    assert await store.get("key") == "value"


async def test_a_value_is_missing_just_after_its_ttl(store: ArrayStore, clock: ManualClock) -> None:
    await store.put("key", "value", 10)
    clock.advance(10.001)
    assert await store.get("key") is MISSING


async def test_a_value_is_missing_exactly_at_its_deadline(
    store: ArrayStore, clock: ManualClock
) -> None:
    """The boundary is inclusive: ``expires_at <= now`` means expired."""
    await store.put("key", "value", 10)
    clock.advance(10.0)
    assert await store.get("key") is MISSING


async def test_a_value_stored_forever_outlives_any_amount_of_time(
    store: ArrayStore, clock: ManualClock
) -> None:
    await store.put("key", "value", None)
    clock.advance(10_000_000.0)
    assert await store.get("key") == "value"


async def test_overwriting_restarts_the_lifetime(store: ArrayStore, clock: ManualClock) -> None:
    await store.put("key", "first", 10)
    clock.advance(9.0)
    await store.put("key", "second", 10)
    clock.advance(9.0)
    assert await store.get("key") == "second"


async def test_many_reports_expiry_per_key(store: ArrayStore, clock: ManualClock) -> None:
    await store.put("brief", 1, 5)
    await store.put("long", 2, 50)
    clock.advance(6.0)

    assert await store.many(["brief", "long"]) == {"brief": MISSING, "long": 2}


async def test_put_many_applies_one_deadline_to_every_key(
    store: ArrayStore, clock: ManualClock
) -> None:
    await store.put_many({"a": 1, "b": 2}, 10)
    clock.advance(9.0)
    assert await store.many(["a", "b"]) == {"a": 1, "b": 2}
    clock.advance(2.0)
    assert await store.many(["a", "b"]) == {"a": MISSING, "b": MISSING}


# -- increment and the window it counts in --------------------------------


async def test_increment_preserves_the_original_expiry(
    store: ArrayStore, clock: ManualClock
) -> None:
    """Counting an event must not extend the window it is counted in.

    A rate limiter whose counter renewed its TTL on every request would never
    reset, so this is the difference between a working limiter and a lockout.
    """
    await store.put("hits", 1, 10)
    clock.advance(9.0)

    assert await store.increment("hits") == 2

    clock.advance(1.5)
    assert await store.get("hits") is MISSING


async def test_increment_of_an_absent_key_creates_it_without_an_expiry(
    store: ArrayStore, clock: ManualClock
) -> None:
    assert await store.increment("hits") == 1
    clock.advance(10_000.0)
    assert await store.get("hits") == 1


async def test_increment_after_expiry_starts_from_zero_again(
    store: ArrayStore, clock: ManualClock
) -> None:
    await store.put("hits", 41, 10)
    clock.advance(11.0)
    assert await store.increment("hits") == 1


async def test_repeated_increments_share_one_deadline(
    store: ArrayStore, clock: ManualClock
) -> None:
    await store.put("hits", 0, 10)
    for _ in range(5):
        clock.advance(1.0)
        await store.increment("hits")

    assert await store.get("hits") == 5
    clock.advance(6.0)
    assert await store.get("hits") is MISSING


# -- eviction rather than concealment -------------------------------------


async def test_an_expired_entry_is_evicted_when_it_is_read(
    store: ArrayStore, clock: ManualClock
) -> None:
    """Hiding an expired entry would leak memory for every key ever written."""
    await store.put("key", "value", 10)
    clock.advance(11.0)
    assert entry_count(store) == 1

    assert await store.get("key") is MISSING

    assert entry_count(store) == 0


async def test_an_expired_entry_is_evicted_when_it_is_forgotten(
    store: ArrayStore, clock: ManualClock
) -> None:
    await store.put("key", "value", 10)
    clock.advance(11.0)

    assert await store.forget("key") is False
    assert entry_count(store) == 0


async def test_an_expired_entry_is_evicted_when_it_is_overwritten_by_add(
    store: ArrayStore, clock: ManualClock
) -> None:
    await store.put("key", "old", 10)
    clock.advance(11.0)

    assert await store.add("key", "new", 10) is True
    assert entry_count(store) == 1
    assert await store.get("key") == "new"


async def test_a_non_positive_ttl_removes_the_entry_outright(store: ArrayStore) -> None:
    await store.put("key", "value")
    assert await store.put("key", "replacement", 0) is False
    assert entry_count(store) == 0


async def test_forget_if_evicts_an_expired_entry_without_matching(
    store: ArrayStore, clock: ManualClock
) -> None:
    await store.put("key", "owner", 10)
    clock.advance(11.0)

    assert await store.forget_if("key", "owner") is False
    assert entry_count(store) == 0


# -- add and the expired slot ---------------------------------------------


async def test_add_fails_while_the_previous_entry_is_alive(
    store: ArrayStore, clock: ManualClock
) -> None:
    assert await store.add("key", "first", 10) is True
    clock.advance(9.0)
    assert await store.add("key", "second", 10) is False
    assert await store.get("key") == "first"


async def test_add_succeeds_again_once_the_previous_entry_has_expired(
    store: ArrayStore, clock: ManualClock
) -> None:
    """This is how a lock becomes available again after its holder dies."""
    assert await store.add("key", "first", 10) is True
    clock.advance(11.0)
    assert await store.add("key", "second", 10) is True
    assert await store.get("key") == "second"


async def test_a_lock_expires_at_its_ttl_to_the_instant(
    store: ArrayStore, clock: ManualClock
) -> None:
    holder = store.lock("resource", 30)
    assert await holder.acquire() is True

    clock.advance(29.0)
    assert await store.lock("resource", 30).acquire() is False

    clock.advance(2.0)
    successor = store.lock("resource", 30)
    assert await successor.acquire() is True
    assert await successor.get_owner() == successor.owner


# -- construction ---------------------------------------------------------


async def test_the_default_clock_is_the_real_one(anyio_backend: str) -> None:
    """Production never passes a clock, so the default has to work too."""
    store = ArrayStore()
    await store.put("key", "value", 0.05)
    assert await store.get("key") == "value"
    await anyio.sleep(0.1)
    assert await store.get("key") is MISSING


async def test_the_store_defaults_to_json_and_an_empty_namespace() -> None:
    store = ArrayStore()
    await store.put("key", {"a": 1})
    assert await store.get("key") == {"a": 1}
    assert store.supports_atomic_increment is True


async def test_a_serializer_can_be_injected() -> None:
    store = ArrayStore(serializer=PickleSerializer())
    await store.put("key", {1, 2, 3})
    assert await store.get("key") == {1, 2, 3}


async def test_flush_and_close_both_empty_the_dictionary(store: ArrayStore) -> None:
    await store.put_many({"a": 1, "b": 2})
    assert await store.flush() is True
    assert entry_count(store) == 0

    await store.put("c", 3)
    await store.close()
    assert entry_count(store) == 0
