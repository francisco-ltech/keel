"""Observability decorator.

A Decorator in the strict sense: it implements
:class:`~keel.contracts.cache.Store`, wraps another store, adds behaviour, and
can itself be wrapped. Because it satisfies the same contract as what it wraps,
nothing above it knows it is there — the Repository, the facade and application
code are all unchanged whether events are being emitted or not.

Making this a decorator rather than a flag inside each driver is what keeps the
drivers honest. ``RedisStore`` knows about Redis; it should not also know about
observability, and adding a third concern to it would start the slide towards a
class that knows about everything.

The manager applies this wrapper whenever it was given a dispatcher, not when
a listener happens to exist — so instrumentation is a deployment decision made
once at startup rather than something that switches on and off as listeners come
and go. Pass ``events=None`` to the manager to remove the wrapper entirely; with
it present but unsubscribed, each operation pays one extra call and an empty
dispatch loop.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from keel.cache.events import (
    CacheFlushed,
    CacheHit,
    CacheMissed,
    CounterIncremented,
    KeyForgotten,
    KeyWritten,
)
from keel.contracts.cache import Lock, Store
from keel.support.events import EventDispatcher
from keel.support.keys import KeyNamespace
from keel.support.sentinels import Maybe, is_missing


class EventfulStore:
    """Wraps a store and announces what passes through it.

    Args:
        inner: The store to wrap.
        events: The dispatcher to publish to.
        name: The store's configured name, carried on every event so an
            observer can tell ``default`` from ``sessions``.
    """

    __slots__ = ("_events", "_inner", "_name")

    def __init__(self, inner: Store, events: EventDispatcher, name: str = "default") -> None:
        self._inner = inner
        self._events = events
        self._name = name

    @property
    def inner(self) -> Store:
        """The wrapped store, for tests that need to bypass instrumentation."""
        return self._inner

    @property
    def namespace(self) -> KeyNamespace:
        """Delegated to the wrapped store."""
        return self._inner.namespace

    @property
    def supports_atomic_increment(self) -> bool:
        """Delegated to the wrapped store."""
        return self._inner.supports_atomic_increment

    async def get(self, key: str) -> Maybe[Any]:
        """Retrieve a value, emitting a hit or a miss.

        Args:
            key: The unqualified key.

        Returns:
            The stored value, or ``MISSING``.
        """
        value = await self._inner.get(key)
        if is_missing(value):
            await self._events.dispatch(CacheMissed(self._name, key))
        else:
            await self._events.dispatch(CacheHit(self._name, key, value))
        return value

    async def many(self, keys: Sequence[str]) -> dict[str, Maybe[Any]]:
        """Retrieve several values, emitting one event per key.

        Args:
            keys: The unqualified keys.

        Returns:
            Every requested key mapped to its value or ``MISSING``.
        """
        results = await self._inner.many(keys)
        for key, value in results.items():
            if is_missing(value):
                await self._events.dispatch(CacheMissed(self._name, key))
            else:
                await self._events.dispatch(CacheHit(self._name, key, value))
        return results

    async def put(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Store a value, emitting a write.

        Args:
            key: The unqualified key.
            value: The value to store.
            ttl: Lifetime in seconds.

        Returns:
            ``True`` if stored.
        """
        stored = await self._inner.put(key, value, ttl)
        if stored:
            await self._events.dispatch(KeyWritten(self._name, key, value, ttl))
        else:
            # A non-positive TTL evicts rather than writes. Reporting nothing
            # would leave an observer showing a key that is no longer there.
            await self._events.dispatch(KeyForgotten(self._name, key, existed=True))
        return stored

    async def put_many(self, values: Mapping[str, Any], ttl: float | None = None) -> bool:
        """Store several values, emitting one write per key.

        Args:
            values: Unqualified keys mapped to values.
            ttl: Lifetime applied to all of them.

        Returns:
            ``True`` if every value was stored.
        """
        stored = await self._inner.put_many(values, ttl)
        if stored:
            for key, value in values.items():
                await self._events.dispatch(KeyWritten(self._name, key, value, ttl))
        return stored

    async def add(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Store a value only if absent, emitting a write when it lands.

        Args:
            key: The unqualified key.
            value: The value to store.
            ttl: Lifetime in seconds.

        Returns:
            ``True`` if this call created the entry.
        """
        created = await self._inner.add(key, value, ttl)
        if created:
            await self._events.dispatch(KeyWritten(self._name, key, value, ttl))
        return created

    async def increment(self, key: str, by: int = 1) -> int:
        """Add to a numeric entry, emitting a counter change.

        Not a :class:`~keel.cache.events.KeyWritten`: an increment inherits the
        entry's existing lifetime, and this layer cannot see what that is.

        Args:
            key: The unqualified key.
            by: The amount to add.

        Returns:
            The value after the operation.
        """
        updated = await self._inner.increment(key, by)
        await self._events.dispatch(CounterIncremented(self._name, key, updated, by))
        return updated

    async def forget(self, key: str) -> bool:
        """Remove an entry, emitting a forget whether or not one existed.

        The event fires even on a no-op: knowing that code tried to evict a key
        that was not there is frequently the interesting part.

        Args:
            key: The unqualified key.

        Returns:
            ``True`` if an entry was removed.
        """
        existed = await self._inner.forget(key)
        await self._events.dispatch(KeyForgotten(self._name, key, existed))
        return existed

    async def forget_if(self, key: str, expected: Any) -> bool:
        """Conditionally remove an entry, emitting a forget when it matched.

        Args:
            key: The unqualified key.
            expected: The value the entry must hold.

        Returns:
            ``True`` if the entry matched and was removed.
        """
        removed = await self._inner.forget_if(key, expected)
        if removed:
            await self._events.dispatch(KeyForgotten(self._name, key, True))
        return removed

    async def flush(self) -> bool:
        """Clear the namespace, emitting a flush.

        Returns:
            ``True`` once cleared.
        """
        flushed = await self._inner.flush()
        await self._events.dispatch(CacheFlushed(self._name))
        return flushed

    def lock(self, name: str, ttl: float = 60.0, *, owner: str | None = None) -> Lock:
        """Build a lock on the wrapped store.

        Locks are delegated undecorated: a lock's reads and writes are
        coordination, not caching, and surfacing them as cache hits would make
        an observer's timeline misleading.

        Args:
            name: The lock's name.
            ttl: Seconds after which the lock self-releases.
            owner: An explicit owner token.

        Returns:
            An unacquired lock.
        """
        return self._inner.lock(name, ttl, owner=owner)

    async def close(self) -> None:
        """Close the wrapped store."""
        await self._inner.close()
