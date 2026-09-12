"""Event dispatch and the store decorator that uses it.

Two halves. ``EventDispatcher`` is the generic Observer: sync and async
listeners, subclass routing, unsubscription, and the guarantee that a broken
listener cannot break the operation that emitted the event. ``EventfulStore`` is
its first consumer: the right event, for the right operation, carrying the
caller's own unqualified key.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import anyio
import pytest

from keel.cache.events import (
    CacheEvent,
    CacheFlushed,
    CacheHit,
    CacheMissed,
    CounterIncremented,
    KeyForgotten,
    KeyWritten,
)
from keel.cache.stores.array import ArrayStore
from keel.cache.stores.eventful import EventfulStore
from keel.support.events import EventDispatcher
from keel.support.keys import KeyNamespace
from keel.support.sentinels import MISSING

pytestmark = [pytest.mark.anyio]


@dataclass
class Ping:
    """A plain event."""

    label: str = "ping"


@dataclass
class LoudPing(Ping):
    """A subclass, to check that listeners on the base type still fire."""


@dataclass
class Unrelated:
    """An event nobody in these tests subscribes to."""


class Recorder:
    """Collects the events it is given."""

    def __init__(self, label: str = "recorder") -> None:
        self.label = label
        self.seen: list[Any] = []

    def __call__(self, event: Any) -> None:
        self.seen.append(event)


# -- EventDispatcher ------------------------------------------------------


async def test_a_sync_listener_fires() -> None:
    dispatcher = EventDispatcher()
    recorder = Recorder()
    dispatcher.listen(Ping, recorder)

    event = Ping()
    await dispatcher.dispatch(event)

    assert recorder.seen == [event]


async def test_an_async_listener_fires() -> None:
    dispatcher = EventDispatcher()
    seen: list[Ping] = []

    async def listener(event: Ping) -> None:
        await anyio.sleep(0)
        seen.append(event)

    dispatcher.listen(Ping, listener)
    await dispatcher.dispatch(Ping())

    assert len(seen) == 1


async def test_sync_and_async_listeners_coexist() -> None:
    dispatcher = EventDispatcher()
    order: list[str] = []

    async def asynchronous(event: Any) -> None:
        order.append("async")

    dispatcher.listen(Ping, lambda event: order.append("sync"))
    dispatcher.listen(Ping, asynchronous)
    await dispatcher.dispatch(Ping())

    assert order == ["sync", "async"]


async def test_listeners_run_in_registration_order() -> None:
    """An observer recording a timeline needs this to be predictable."""
    dispatcher = EventDispatcher()
    order: list[int] = []

    def numbered(index: int) -> Callable[[Any], None]:
        def listener(event: Any) -> None:
            order.append(index)

        return listener

    for index in range(5):
        dispatcher.listen(Ping, numbered(index))

    await dispatcher.dispatch(Ping())

    assert order == [0, 1, 2, 3, 4]


async def test_a_listener_on_a_base_type_receives_subclass_events() -> None:
    dispatcher = EventDispatcher()
    recorder = Recorder()
    dispatcher.listen(Ping, recorder)

    await dispatcher.dispatch(LoudPing())

    assert len(recorder.seen) == 1
    assert isinstance(recorder.seen[0], LoudPing)


async def test_a_listener_on_a_subclass_does_not_receive_base_events() -> None:
    dispatcher = EventDispatcher()
    recorder = Recorder()
    dispatcher.listen(LoudPing, recorder)

    await dispatcher.dispatch(Ping())

    assert recorder.seen == []


async def test_an_event_reaches_listeners_on_every_matching_type() -> None:
    dispatcher = EventDispatcher()
    on_base, on_subclass = Recorder("base"), Recorder("subclass")
    dispatcher.listen(Ping, on_base)
    dispatcher.listen(LoudPing, on_subclass)

    await dispatcher.dispatch(LoudPing())

    assert len(on_base.seen) == 1
    assert len(on_subclass.seen) == 1


async def test_an_unrelated_event_reaches_nobody() -> None:
    dispatcher = EventDispatcher()
    recorder = Recorder()
    dispatcher.listen(Ping, recorder)

    await dispatcher.dispatch(Unrelated())

    assert recorder.seen == []


async def test_dispatching_with_no_listeners_is_harmless() -> None:
    await EventDispatcher().dispatch(Ping())


# -- has_listeners --------------------------------------------------------


def test_has_listeners_is_false_on_a_fresh_dispatcher() -> None:
    assert EventDispatcher().has_listeners() is False
    assert EventDispatcher().has_listeners(Ping) is False


def test_has_listeners_without_a_type_asks_whether_anything_is_listening() -> None:
    dispatcher = EventDispatcher()
    dispatcher.listen(Ping, Recorder())
    assert dispatcher.has_listeners() is True


def test_has_listeners_narrows_to_one_type() -> None:
    dispatcher = EventDispatcher()
    dispatcher.listen(Ping, Recorder())
    assert dispatcher.has_listeners(Ping) is True
    assert dispatcher.has_listeners(Unrelated) is False


def test_has_listeners_accounts_for_subclass_routing() -> None:
    dispatcher = EventDispatcher()
    dispatcher.listen(Ping, Recorder())
    assert dispatcher.has_listeners(LoudPing) is True


def test_has_listeners_is_false_again_once_the_last_one_unsubscribes() -> None:
    dispatcher = EventDispatcher()
    recorder = Recorder()
    unsubscribe = dispatcher.listen(Ping, recorder)
    unsubscribe()
    assert dispatcher.has_listeners() is False
    assert dispatcher.has_listeners(Ping) is False


# -- unsubscription -------------------------------------------------------


async def test_the_returned_unsubscribe_removes_that_listener() -> None:
    dispatcher = EventDispatcher()
    staying, leaving = Recorder("staying"), Recorder("leaving")
    dispatcher.listen(Ping, staying)
    unsubscribe = dispatcher.listen(Ping, leaving)

    unsubscribe()
    await dispatcher.dispatch(Ping())

    assert len(staying.seen) == 1
    assert leaving.seen == []


async def test_unsubscribing_twice_is_harmless() -> None:
    dispatcher = EventDispatcher()
    unsubscribe = dispatcher.listen(Ping, Recorder())
    unsubscribe()
    unsubscribe()
    await dispatcher.dispatch(Ping())


def test_unsubscribing_does_not_disturb_an_identical_registration() -> None:
    """Returning the unsubscribe is what stops one caller removing another's."""
    dispatcher = EventDispatcher()
    recorder = Recorder()
    first = dispatcher.listen(Ping, recorder)
    dispatcher.listen(Ping, recorder)

    first()

    assert dispatcher.has_listeners(Ping) is True


# -- failure isolation ----------------------------------------------------


async def test_a_raising_listener_does_not_propagate() -> None:
    """A broken metrics hook must not fail a cache write."""
    dispatcher = EventDispatcher()
    dispatcher.listen(Ping, _explode)

    await dispatcher.dispatch(Ping())


async def test_a_raising_listener_is_reported_to_on_error() -> None:
    failures: list[tuple[BaseException, Any]] = []
    dispatcher = EventDispatcher(on_error=lambda exc, event: failures.append((exc, event)))
    dispatcher.listen(Ping, _explode)

    event = Ping()
    await dispatcher.dispatch(event)

    assert len(failures) == 1
    exc, reported = failures[0]
    assert isinstance(exc, RuntimeError)
    assert str(exc) == "listener failed"
    assert reported is event


async def test_a_raising_listener_does_not_stop_the_others() -> None:
    failures: list[BaseException] = []
    dispatcher = EventDispatcher(on_error=lambda exc, event: failures.append(exc))
    after = Recorder("after")
    dispatcher.listen(Ping, _explode)
    dispatcher.listen(Ping, after)

    await dispatcher.dispatch(Ping())

    assert len(after.seen) == 1
    assert len(failures) == 1


async def test_a_raising_async_listener_is_also_caught() -> None:
    failures: list[BaseException] = []
    dispatcher = EventDispatcher(on_error=lambda exc, event: failures.append(exc))

    async def explode(event: Any) -> None:
        await anyio.sleep(0)
        raise ValueError("async listener failed")

    dispatcher.listen(Ping, explode)
    await dispatcher.dispatch(Ping())

    assert [type(exc) for exc in failures] == [ValueError]


def _explode(event: Any) -> None:
    raise RuntimeError("listener failed")


# -- clear ----------------------------------------------------------------


async def test_clear_removes_every_listener() -> None:
    dispatcher = EventDispatcher()
    recorder = Recorder()
    dispatcher.listen(Ping, recorder)
    dispatcher.listen(Unrelated, recorder)

    dispatcher.clear()
    await dispatcher.dispatch(Ping())

    assert dispatcher.has_listeners() is False
    assert recorder.seen == []


def test_clearing_an_empty_dispatcher_is_harmless() -> None:
    EventDispatcher().clear()


# -- EventfulStore --------------------------------------------------------


@pytest.fixture
def events() -> EventDispatcher:
    return EventDispatcher()


@pytest.fixture
def seen(events: EventDispatcher) -> list[CacheEvent]:
    recorded: list[CacheEvent] = []
    events.listen(CacheEvent, recorded.append)
    return recorded


@pytest.fixture
def store(events: EventDispatcher) -> EventfulStore:
    """An instrumented store whose inner store namespaces its keys."""
    return EventfulStore(ArrayStore(KeyNamespace("keel:test")), events, "sessions")


async def test_a_hit_emits_cache_hit(store: EventfulStore, seen: list[CacheEvent]) -> None:
    await store.put("user:42", "Ada")
    seen.clear()

    await store.get("user:42")

    assert seen == [CacheHit("sessions", "user:42", "Ada")]


async def test_a_miss_emits_cache_missed(store: EventfulStore, seen: list[CacheEvent]) -> None:
    await store.get("absent")
    assert seen == [CacheMissed("sessions", "absent")]


async def test_events_carry_the_unqualified_key(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    """``user:42`` is useful to an observer; ``keel:test:user:42`` is noise."""
    await store.get("user:42")

    missed = seen[0]
    assert isinstance(missed, CacheMissed)
    assert missed.key == "user:42"
    assert store.namespace.apply("user:42") == "keel:test:user:42"


async def test_events_carry_the_configured_store_name(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    await store.get("key")
    assert seen[0].store == "sessions"


async def test_the_default_store_name_is_default(events: EventDispatcher) -> None:
    seen: list[CacheEvent] = []
    events.listen(CacheEvent, seen.append)
    await EventfulStore(ArrayStore(), events).get("key")
    assert seen[0].store == "default"


async def test_many_emits_one_event_per_key(store: EventfulStore, seen: list[CacheEvent]) -> None:
    await store.put("present", 1)
    seen.clear()

    await store.many(["present", "absent"])

    assert seen == [CacheHit("sessions", "present", 1), CacheMissed("sessions", "absent")]


async def test_put_emits_key_written_with_the_ttl(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    await store.put("key", "value", 60)
    assert seen == [KeyWritten("sessions", "key", "value", 60)]


async def test_a_rejected_put_emits_a_forget(store: EventfulStore, seen: list[CacheEvent]) -> None:
    """Regression: it used to emit nothing at all.

    A non-positive TTL does not store — it *evicts*. Staying silent left an
    observer's timeline showing a key that was no longer there.
    """
    assert await store.put("key", "value", 0) is False
    assert seen == [KeyForgotten("sessions", "key", existed=True)]


async def test_put_many_emits_one_write_per_key(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    await store.put_many({"a": 1, "b": 2}, 30)
    assert seen == [
        KeyWritten("sessions", "a", 1, 30),
        KeyWritten("sessions", "b", 2, 30),
    ]


async def test_a_rejected_put_many_emits_nothing(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    assert await store.put_many({"a": 1, "b": 2}, 0) is False
    assert seen == []


async def test_add_emits_only_when_it_creates(store: EventfulStore, seen: list[CacheEvent]) -> None:
    assert await store.add("key", "first") is True
    assert seen == [KeyWritten("sessions", "key", "first", None)]

    seen.clear()
    assert await store.add("key", "second") is False
    assert seen == []


async def test_increment_emits_the_resulting_value(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    await store.increment("hits", 5)
    assert seen == [CounterIncremented("sessions", "hits", 5, 5)]


async def test_increment_does_not_claim_the_entry_has_no_expiry(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    """Regression: it used to emit ``KeyWritten(ttl=None)``.

    An increment inherits whatever lifetime the entry already had, and this
    layer cannot see it — so reporting a write with no TTL told observers a key
    expiring in a minute would live forever.
    """
    await store.put("hits", 1, 60)
    seen.clear()

    await store.increment("hits")

    assert not any(isinstance(event, KeyWritten) for event in seen)


async def test_forget_emits_even_when_nothing_existed(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    """Knowing that code evicted a key that was not there is often the point."""
    assert await store.forget("absent") is False
    assert seen == [KeyForgotten("sessions", "absent", False)]


async def test_forget_records_that_an_entry_existed(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    await store.put("key", "value")
    seen.clear()

    await store.forget("key")

    assert seen == [KeyForgotten("sessions", "key", True)]


async def test_forget_if_emits_only_on_a_match(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    await store.put("key", "owner")
    seen.clear()

    assert await store.forget_if("key", "someone-else") is False
    assert seen == []

    assert await store.forget_if("key", "owner") is True
    assert seen == [KeyForgotten("sessions", "key", True)]


async def test_flush_emits_cache_flushed(store: EventfulStore, seen: list[CacheEvent]) -> None:
    await store.flush()
    assert seen == [CacheFlushed("sessions")]


async def test_a_lock_is_delegated_without_emitting_cache_events(
    store: EventfulStore, seen: list[CacheEvent]
) -> None:
    """A lock's reads and writes are coordination, not caching."""
    lock = store.lock("resource", 30)

    assert await lock.acquire() is True
    assert await lock.get_owner() == lock.owner
    assert await lock.release() is True

    assert seen == []


async def test_lock_construction_is_delegated_to_the_inner_store(store: EventfulStore) -> None:
    from keel.cache.lock import StoreLock

    assert isinstance(store.lock("resource"), StoreLock)


def test_the_wrapper_delegates_its_capability_report(store: EventfulStore) -> None:
    assert store.supports_atomic_increment is store.inner.supports_atomic_increment
    assert store.namespace is store.inner.namespace


async def test_close_is_delegated(store: EventfulStore, seen: list[CacheEvent]) -> None:
    await store.put("key", "value")
    seen.clear()

    await store.close()

    assert seen == []
    assert await store.inner.get("key") is MISSING


async def test_an_operation_still_succeeds_when_a_listener_raises(
    events: EventDispatcher,
) -> None:
    """The isolation guarantee, exercised where it actually matters."""
    events.listen(CacheEvent, _explode)
    store = EventfulStore(ArrayStore(), events, "sessions")

    assert await store.put("key", "value") is True
    assert await store.get("key") == "value"
