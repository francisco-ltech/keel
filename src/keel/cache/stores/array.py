"""In-memory store.

The default store for tests and for single-process development. It is not a
toy: it serialises values through the same :class:`~keel.support.serialization.Serializer`
the Redis store uses, so a value that cannot survive the cache in production
cannot survive it in a test either. Catching that divergence locally is worth
the encode/decode cost in a store that never leaves the process.

Compound operations hold an :class:`asyncio.Lock`. Be precise about what that
buys: no method here awaits anything that can yield, so each one is already
indivisible within a single event loop, and the mutex is insurance against a
future edit introducing a suspension point inside a read-modify-write rather
than protection the code needs today. It is *not* thread-safety —
:class:`asyncio.Lock` does not synchronise across threads — so this store
belongs to one event loop. Use the Redis store when several processes or
threads must share a cache.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from keel.cache.counters import guard_overflow
from keel.contracts.cache import Lock
from keel.exceptions import CacheValueError
from keel.support.keys import KeyNamespace
from keel.support.sentinels import MISSING, Maybe
from keel.support.serialization import JsonSerializer, Serializer


@dataclass(slots=True)
class _Entry:
    """A stored value and the monotonic instant it expires, if ever."""

    payload: bytes
    expires_at: float | None

    def has_expired(self, now: float) -> bool:
        """Whether this entry is no longer valid at *now*."""
        return self.expires_at is not None and self.expires_at <= now


class ArrayStore:
    """A cache held in a dictionary in this process.

    Args:
        namespace: The key namespace. Cosmetic here — the dictionary is private
            — but kept so keys read identically across drivers.
        serializer: Encoding strategy, matching the Redis store's default.
        clock: Monotonic time source. Injectable so TTL behaviour can be tested
            without sleeping; production never passes this.
    """

    __slots__ = ("_clock", "_entries", "_mutex", "_namespace", "_serializer")

    def __init__(
        self,
        namespace: KeyNamespace | None = None,
        serializer: Serializer | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._namespace = namespace or KeyNamespace()
        self._serializer = serializer or JsonSerializer()
        self._clock = clock
        self._entries: dict[str, _Entry] = {}
        self._mutex = asyncio.Lock()

    def __len__(self) -> int:
        """Return how many entries are retained, expired ones included.

        Exposed so the eviction guarantee is observable from outside: an entry
        that has expired must be *removed*, not merely hidden behind a read that
        reports a miss, or a long-running process leaks memory in proportion to
        the keys it has ever seen.
        """
        return len(self._entries)

    @property
    def namespace(self) -> KeyNamespace:
        """The slice of the keyspace this store owns."""
        return self._namespace

    @property
    def supports_atomic_increment(self) -> bool:
        """Increments are serialised by an in-process mutex, so they are atomic."""
        return True

    def _expiry(self, ttl: float | None) -> float | None:
        """Translate a relative TTL into an absolute monotonic deadline."""
        return None if ttl is None else self._clock() + ttl

    def _live_entry(self, key: str) -> _Entry | None:
        """Return the unexpired entry for *key*, evicting it if it has expired."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.has_expired(self._clock()):
            del self._entries[key]
            return None
        return entry

    async def get(self, key: str) -> Maybe[Any]:
        """Retrieve a value.

        Args:
            key: The unqualified key.

        Returns:
            The stored value, or ``MISSING``.
        """
        qualified = self._namespace.apply(key)
        entry = self._live_entry(qualified)
        if entry is None:
            return MISSING
        return self._serializer.loads(entry.payload)

    async def many(self, keys: Sequence[str]) -> dict[str, Maybe[Any]]:
        """Retrieve several values.

        Args:
            keys: The unqualified keys.

        Returns:
            Every requested key mapped to its value or ``MISSING``.
        """
        return {key: await self.get(key) for key in keys}

    async def put(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Store a value, overwriting any existing entry.

        Args:
            key: The unqualified key.
            value: The value to store.
            ttl: Lifetime in seconds, or ``None`` for indefinite.

        Returns:
            ``True`` if stored; ``False`` when a non-positive *ttl* made the
            write a no-op.
        """
        qualified = self._namespace.apply(key)
        if ttl is not None and ttl <= 0:
            self._entries.pop(qualified, None)
            return False
        payload = self._serializer.dumps(value)
        self._entries[qualified] = _Entry(payload, self._expiry(ttl))
        return True

    async def put_many(self, values: Mapping[str, Any], ttl: float | None = None) -> bool:
        """Store several values.

        Args:
            values: Unqualified keys mapped to values.
            ttl: Lifetime applied to all of them.

        Returns:
            ``True`` if every value was stored.
        """
        results = [await self.put(key, value, ttl) for key, value in values.items()]
        return all(results)

    async def add(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Store a value only if the key is absent.

        Args:
            key: The unqualified key.
            value: The value to store.
            ttl: Lifetime in seconds.

        Returns:
            ``True`` if this call created the entry.
        """
        if ttl is not None and ttl <= 0:
            return False
        qualified = self._namespace.apply(key)
        async with self._mutex:
            if self._live_entry(qualified) is not None:
                return False
            self._entries[qualified] = _Entry(self._serializer.dumps(value), self._expiry(ttl))
            return True

    async def increment(self, key: str, by: int = 1) -> int:
        """Add to a numeric entry, creating it at zero if absent.

        The entry's existing expiry is preserved, matching Redis ``INCRBY``:
        counting an event must not extend the window it is counted in.

        Args:
            key: The unqualified key.
            by: The amount to add.

        Returns:
            The value after the operation.

        Raises:
            CacheValueError: If the entry exists but is not an integer.
        """
        qualified = self._namespace.apply(key)
        async with self._mutex:
            entry = self._live_entry(qualified)
            if entry is None:
                current = 0
                expires_at = None
            else:
                decoded = self._serializer.loads(entry.payload)
                if not isinstance(decoded, int) or isinstance(decoded, bool):
                    raise CacheValueError(
                        f"cannot increment key {key!r}: it holds "
                        f"{type(decoded).__name__}, not an integer"
                    )
                current = decoded
                expires_at = entry.expires_at
            updated = guard_overflow(key, current + by)
            self._entries[qualified] = _Entry(self._serializer.dumps(updated), expires_at)
            return updated

    async def forget(self, key: str) -> bool:
        """Remove an entry.

        Args:
            key: The unqualified key.

        Returns:
            ``True`` if an entry was removed.
        """
        qualified = self._namespace.apply(key)
        existed = self._live_entry(qualified) is not None
        self._entries.pop(qualified, None)
        return existed

    async def forget_if(self, key: str, expected: Any) -> bool:
        """Remove an entry only if it holds *expected*.

        Args:
            key: The unqualified key.
            expected: The value the entry must hold.

        Returns:
            ``True`` if the entry matched and was removed.
        """
        qualified = self._namespace.apply(key)
        async with self._mutex:
            entry = self._live_entry(qualified)
            if entry is None or self._serializer.loads(entry.payload) != expected:
                return False
            del self._entries[qualified]
            return True

    async def flush(self) -> bool:
        """Remove every entry in this store.

        Returns:
            ``True`` always.
        """
        self._entries.clear()
        return True

    def lock(self, name: str, ttl: float = 60.0, *, owner: str | None = None) -> Lock:
        """Construct a lock backed by this store.

        Args:
            name: The lock's name.
            ttl: Seconds after which the lock self-releases.
            owner: An explicit owner token; generated when omitted.

        Returns:
            An unacquired lock.
        """
        from keel.cache.lock import StoreLock

        return StoreLock(self, name, ttl, owner=owner)

    async def close(self) -> None:
        """Drop every entry. In-memory stores hold no external resources."""
        self._entries.clear()
