"""Cache contracts.

Two interfaces, deliberately split.

:class:`Store` is the *implementor* — the narrow set of operations a backend has
to provide, every one of which needs the backend's own atomicity guarantees.
:class:`Repository` (in :mod:`keel.cache.repository`) is the *abstraction* — the
ergonomic API application code calls, built entirely on top of these primitives.

That split is the Bridge pattern, and it is the reason adding ``remember()``
costs nothing per driver while adding a driver costs nothing per convenience
method.

Anything that can be derived from these primitives belongs on the Repository,
not here. Every method below earns its place by needing something only the
backend can do atomically.

Neither protocol is ``runtime_checkable``, deliberately. ``isinstance`` against
a protocol checks only that the attribute *names* exist — a class with eleven
synchronous methods and entirely wrong signatures passes — so it would offer a
third-party driver author false assurance. The real conformance check is the
parametrised suite in ``tests/test_store_contract.py``: add the driver to its
fixture and run it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any, Protocol, Self

from keel.support.keys import KeyNamespace
from keel.support.sentinels import Maybe


class Lock(Protocol):
    """A mutual exclusion primitive backed by the cache.

    Implementations must be *ownership-aware*: releasing a lock you no longer
    hold — because it expired and another process took it — must be a no-op
    rather than releasing someone else's lock. This is the single most common
    distributed-lock defect, so it is stated in the contract rather than left
    to each driver.
    """

    @property
    def name(self) -> str:
        """The lock's name, unqualified by any namespace."""
        ...

    @property
    def owner(self) -> str:
        """An opaque token identifying this holder, used to make release safe."""
        ...

    async def acquire(self) -> bool:
        """Attempt to take the lock without waiting.

        Returns:
            ``True`` if the lock was taken, ``False`` if it is already held.
        """
        ...

    async def release(self) -> bool:
        """Release the lock if and only if this instance still owns it.

        Returns:
            ``True`` if the lock was released by this call.
        """
        ...

    async def force_release(self) -> None:
        """Release the lock regardless of who holds it.

        Intended for administrative recovery, never for normal flow.
        """
        ...

    async def get_owner(self) -> str | None:
        """Return the owner token currently recorded for this lock.

        Returns:
            The owner token, or ``None`` if the lock is free.
        """
        ...

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
        ...

    async def __aenter__(self) -> Self:
        """Acquire the lock, raising if it is unavailable.

        Returns:
            The held lock.

        Raises:
            LockTimeoutError: If the lock is already held.
        """
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Release the lock, including when the body raised."""
        ...


class Store(Protocol):
    """A cache backend.

    Implementations are interchangeable (Strategy). The shared test suite in
    ``tests/test_store_contract.py`` runs against every one of them, so the
    guarantees below are enforced rather than merely documented:

    * ``None`` is a storable value and must be distinguishable from a miss.
    * ``put`` with a ``ttl`` of ``None`` stores indefinitely; a ``ttl`` at or
      below zero stores nothing and evicts any existing entry.
    * ``add`` is atomic put-if-absent. It is the primitive locks are built on,
      so an implementation that fakes it with get-then-put is incorrect.
    * ``flush`` clears only what this store's namespace owns.
    """

    @property
    def namespace(self) -> KeyNamespace:
        """The slice of the backend keyspace this store owns."""
        ...

    @property
    def supports_atomic_increment(self) -> bool:
        """Whether the backend can increment the stored encoding natively.

        This is a capability report, not a behavioural switch: :meth:`increment`
        is atomic either way. A driver reporting ``False`` is telling you it has
        to fall back to a lock-guarded read-modify-write, which is correct but
        costs three round trips instead of one.
        """
        ...

    async def get(self, key: str) -> Maybe[Any]:
        """Retrieve a value.

        Args:
            key: The unqualified key.

        Returns:
            The stored value, or :data:`~keel.support.sentinels.MISSING`.
        """
        ...

    async def many(self, keys: Sequence[str]) -> dict[str, Maybe[Any]]:
        """Retrieve several values in one round trip.

        Args:
            keys: The unqualified keys.

        Returns:
            A mapping containing every requested key, with
            :data:`~keel.support.sentinels.MISSING` for those absent.
        """
        ...

    async def put(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Store a value, overwriting any existing entry.

        Args:
            key: The unqualified key.
            value: The value to store.
            ttl: Lifetime in seconds, or ``None`` to store indefinitely.

        Returns:
            ``True`` if the value was stored.
        """
        ...

    async def put_many(self, values: Mapping[str, Any], ttl: float | None = None) -> bool:
        """Store several values.

        Args:
            values: Unqualified keys mapped to values.
            ttl: Lifetime in seconds applied to all of them.

        Returns:
            ``True`` if every value was stored.
        """
        ...

    async def add(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Store a value only if the key is absent, atomically.

        Args:
            key: The unqualified key.
            value: The value to store.
            ttl: Lifetime in seconds.

        Returns:
            ``True`` if this call created the entry, ``False`` if it existed.
        """
        ...

    async def increment(self, key: str, by: int = 1) -> int:
        """Add to a numeric entry, creating it at zero if absent.

        Args:
            key: The unqualified key.
            by: The amount to add; negative decrements.

        Returns:
            The value after the operation.

        Raises:
            CacheValueError: If the entry exists but is not numeric.
        """
        ...

    async def forget(self, key: str) -> bool:
        """Remove an entry.

        Args:
            key: The unqualified key.

        Returns:
            ``True`` if an entry was removed, ``False`` if none existed.
        """
        ...

    async def forget_if(self, key: str, expected: Any) -> bool:
        """Remove an entry only if it currently holds *expected*, atomically.

        This exists for one reason: releasing a lock safely. A holder whose lock
        has already expired and been taken by someone else must not delete the
        new holder's entry, and a get-then-forget pair cannot guarantee that —
        there is always a gap. Only the backend can close it.

        Args:
            key: The unqualified key.
            expected: The value the entry must hold for the removal to happen.

        Returns:
            ``True`` if the entry matched and was removed.
        """
        ...

    async def flush(self) -> bool:
        """Remove every entry within this store's namespace.

        Returns:
            ``True`` if the operation completed.
        """
        ...

    def lock(self, name: str, ttl: float = 60.0, *, owner: str | None = None) -> Lock:
        """Construct a lock backed by this store.

        This is deliberately synchronous: building a lock touches no I/O, and
        making it a coroutine would force ``await`` on a pure construction.

        Args:
            name: The lock's name.
            ttl: Seconds after which the lock self-releases, bounding the damage
                from a holder that dies.
            owner: An explicit owner token; generated when omitted.

        Returns:
            An unacquired lock.
        """
        ...

    async def close(self) -> None:
        """Release any backend resources held by this store."""
        ...
