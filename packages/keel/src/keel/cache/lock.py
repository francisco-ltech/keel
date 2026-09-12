"""Cache-backed locks.

One implementation serves every driver, because the contract already exposes the
two primitives a correct lock needs: :meth:`~keel.contracts.cache.Store.add` for
atomic acquisition and :meth:`~keel.contracts.cache.Store.forget_if` for
ownership-checked release. Drivers that can do better — Redis, with a single
round trip — override the relevant method; the rest inherit a working lock for
free. That is Template Method doing real work rather than decoration.

The owner token is what makes release safe. Consider the sequence this prevents:

1. Process A acquires ``import`` with a 10s TTL and stalls.
2. The TTL elapses; the lock is released by the backend.
3. Process B acquires ``import``.
4. Process A wakes and calls ``release()``.

Without an ownership check, step 4 releases *B's* lock and two importers run
concurrently. With one, step 4 is a no-op and A learns it lost the lock.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from types import TracebackType
from typing import TYPE_CHECKING, Self

from keel.exceptions import LockTimeoutError

if TYPE_CHECKING:
    from keel.contracts.cache import Store

LOCK_PREFIX = "lock"


class StoreLock:
    """A lock held as a cache entry whose value is the holder's owner token.

    Args:
        store: The backend this lock lives in.
        name: The lock's name, namespaced under ``lock:`` to keep it clear of
            ordinary cache keys.
        ttl: Seconds after which the backend releases the lock on its own. This
            bounds the damage from a holder that dies without releasing, and is
            why a TTL is required rather than optional.
        owner: An explicit owner token; a random one is generated when omitted.
            Supplying one is for tests and administrative recovery. Two live
            locks sharing a token defeat the ownership check entirely — either
            can release the other — so a caller that supplies tokens is
            responsible for their uniqueness.
    """

    __slots__ = ("_name", "_owner", "_store", "_ttl")

    def __init__(
        self,
        store: Store,
        name: str,
        ttl: float = 60.0,
        *,
        owner: str | None = None,
    ) -> None:
        self._store = store
        self._name = name
        self._ttl = ttl
        self._owner = owner or secrets.token_hex(16)

    @property
    def name(self) -> str:
        """The lock's name, unqualified."""
        return self._name

    @property
    def owner(self) -> str:
        """The opaque token identifying this holder."""
        return self._owner

    @property
    def key(self) -> str:
        """The cache key backing this lock."""
        return f"{LOCK_PREFIX}:{self._name}"

    async def acquire(self) -> bool:
        """Attempt to take the lock without waiting.

        Returns:
            ``True`` if the lock was taken.
        """
        return await self._store.add(self.key, self._owner, self._ttl)

    async def release(self) -> bool:
        """Release the lock if this instance still owns it.

        Returns:
            ``True`` if this call released the lock.
        """
        return await self._store.forget_if(self.key, self._owner)

    async def force_release(self) -> None:
        """Release the lock regardless of owner.

        Only for administrative recovery — using this in normal flow reintroduces
        exactly the race the owner token exists to prevent.
        """
        await self._store.forget(self.key)

    async def get_owner(self) -> str | None:
        """Return the owner token currently recorded for this lock.

        Returns:
            The owner token, or ``None`` if the lock is free.
        """
        from keel.support.sentinels import is_missing

        value = await self._store.get(self.key)
        if is_missing(value):
            return None
        return str(value)

    async def block(self, timeout: float, *, poll: float = 0.05) -> Self:
        """Wait for the lock, then take it.

        Args:
            timeout: Seconds to wait before giving up.
            poll: Seconds between attempts.

        Returns:
            The lock, now held.

        Raises:
            LockTimeoutError: If the lock could not be taken within *timeout*.
        """
        deadline = time.monotonic() + timeout
        while True:
            if await self.acquire():
                return self
            if time.monotonic() + poll >= deadline:
                raise LockTimeoutError(self._name, timeout)
            await asyncio.sleep(poll)

    async def __aenter__(self) -> Self:
        """Acquire the lock.

        Returns:
            The held lock.

        Raises:
            LockTimeoutError: If the lock is already held elsewhere.
        """
        if not await self.acquire():
            raise LockTimeoutError(self._name, 0.0)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Release the lock, including when the body raised."""
        await self.release()


class NullLock:
    """A lock that is always available and never contended.

    Paired with :class:`~keel.cache.stores.null.NullStore`: when caching is
    disabled, code that coordinates through a lock should still run rather than
    deadlock. It does *not* provide mutual exclusion, which is the correct
    behaviour for a disabled cache and the wrong behaviour to rely on.
    """

    __slots__ = ("_held", "_name", "_owner")

    def __init__(self, name: str, *, owner: str | None = None) -> None:
        self._name = name
        self._owner = owner or secrets.token_hex(16)
        self._held = False

    @property
    def name(self) -> str:
        """The lock's name."""
        return self._name

    @property
    def owner(self) -> str:
        """The opaque owner token."""
        return self._owner

    async def acquire(self) -> bool:
        """Always succeed.

        Returns:
            ``True``.
        """
        self._held = True
        return True

    async def release(self) -> bool:
        """Always succeed.

        Returns:
            ``True``.
        """
        self._held = False
        return True

    async def force_release(self) -> None:
        """Mark the lock free."""
        self._held = False

    async def get_owner(self) -> str | None:
        """Report the owner, or ``None`` while unheld.

        Tracks acquisition purely so this matches the contract's "``None`` if
        the lock is free". A Null Object may decline to provide the *guarantee*
        — mutual exclusion — but it should not lie about its observable state.

        Returns:
            This lock's owner token while held, otherwise ``None``.
        """
        return self._owner if self._held else None

    async def block(self, timeout: float, *, poll: float = 0.05) -> Self:
        """Return immediately, since the lock is never contended.

        Args:
            timeout: Ignored.
            poll: Ignored.

        Returns:
            The lock.
        """
        self._held = True
        return self

    async def __aenter__(self) -> Self:
        """Enter without contention.

        Returns:
            The lock.
        """
        self._held = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Mark the lock free."""
        self._held = False
