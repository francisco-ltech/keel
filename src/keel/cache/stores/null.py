"""Null store.

The Null Object pattern. Every write succeeds, every read misses, nothing is
retained. Its purpose is to let an application turn caching off through
configuration rather than through conditionals scattered at every call site:
``if settings.cache_enabled:`` disappears, and the code path under test is the
same one that runs in production.

Note that a null cache changes performance, not correctness — unless something
depends on the cache *for* correctness, such as a lock. That is why
:meth:`lock` returns a :class:`~keel.cache.lock.NullLock` which always grants:
disabling the cache should not deadlock the application, and code that needs
real mutual exclusion should not be getting it from a disabled cache.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from keel.cache.lock import NullLock
from keel.contracts.cache import Lock
from keel.support.keys import KeyNamespace
from keel.support.sentinels import MISSING, Maybe


class NullStore:
    """A store that retains nothing.

    Args:
        namespace: Accepted for interface parity; it has no effect.
    """

    __slots__ = ("_namespace",)

    def __init__(self, namespace: KeyNamespace | None = None) -> None:
        self._namespace = namespace or KeyNamespace()

    @property
    def namespace(self) -> KeyNamespace:
        """The nominal namespace. Nothing is stored under it."""
        return self._namespace

    @property
    def supports_atomic_increment(self) -> bool:
        """Nothing is stored, so increments are trivially consistent."""
        return True

    async def get(self, key: str) -> Maybe[Any]:
        """Always miss.

        Args:
            key: Ignored.

        Returns:
            ``MISSING``.
        """
        return MISSING

    async def many(self, keys: Sequence[str]) -> dict[str, Maybe[Any]]:
        """Always miss, for every key.

        Args:
            keys: The requested keys.

        Returns:
            Every key mapped to ``MISSING``.
        """
        return dict.fromkeys(keys, MISSING)

    async def put(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Discard the value and report success.

        Args:
            key: Ignored.
            value: Ignored.
            ttl: Ignored.

        Returns:
            ``True`` — the caller's contract is satisfied; nothing promised the
            value would be readable later.
        """
        return True

    async def put_many(self, values: Mapping[str, Any], ttl: float | None = None) -> bool:
        """Discard the values and report success.

        Args:
            values: Ignored.
            ttl: Ignored.

        Returns:
            ``True``.
        """
        return True

    async def add(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Report that the entry was created, since nothing is ever present.

        Args:
            key: Ignored.
            value: Ignored.
            ttl: Ignored.

        Returns:
            ``True``.
        """
        return True

    async def increment(self, key: str, by: int = 1) -> int:
        """Report the value an increment from zero would produce.

        Args:
            key: Ignored.
            by: The amount that would have been added.

        Returns:
            *by*, since the counter is always absent.
        """
        return by

    async def forget(self, key: str) -> bool:
        """Report that nothing was removed.

        Args:
            key: Ignored.

        Returns:
            ``False`` — no entry existed to remove.
        """
        return False

    async def forget_if(self, key: str, expected: Any) -> bool:
        """Report that nothing matched.

        Args:
            key: Ignored.
            expected: Ignored.

        Returns:
            ``False``.
        """
        return False

    async def flush(self) -> bool:
        """Succeed trivially.

        Returns:
            ``True``.
        """
        return True

    def lock(self, name: str, ttl: float = 60.0, *, owner: str | None = None) -> Lock:
        """Return a lock that always grants.

        Args:
            name: The lock's name.
            ttl: Ignored.
            owner: An explicit owner token.

        Returns:
            A :class:`~keel.cache.lock.NullLock`.
        """
        return NullLock(name, owner=owner)

    async def close(self) -> None:
        """Do nothing. There is nothing to release."""
