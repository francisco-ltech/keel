"""The cache test double.

A Test Spy, built as a Decorator over a real store. That construction matters:
the fake does not reimplement caching, it records what passes through and
delegates to something that actually works. So a test using the fake exercises
real TTL handling, real serialisation and real atomicity, and the assertions are
about what the code under test *did*, not about a simplified imitation of a
cache.

Wrapping any store rather than only the in-memory one also means the same
assertions work against Redis in an integration test, which is where a
hand-written mock would have to be thrown away.

This class is the reason the seam exists. A subsystem that cannot be asserted
against is a dependency; one that can is a battery.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from keel.cache.stores.array import ArrayStore
from keel.contracts.cache import Lock, Store
from keel.support.keys import KeyNamespace
from keel.support.sentinels import MISSING, UNSET, Maybe, Sentinel, is_missing

type OperationKind = Literal[
    "get", "many", "put", "put_many", "add", "increment", "forget", "forget_if", "flush"
]


@dataclass(frozen=True, slots=True)
class Operation:
    """One recorded cache operation.

    Attributes:
        kind: Which operation ran.
        key: The key involved, or ``None`` for whole-store operations.
        value: The value written, or the value read on a hit.
        ttl: The lifetime requested, in seconds.
        hit: For reads, whether the key was present.
        result: Whether the operation reported success.
    """

    kind: OperationKind
    key: str | None = None
    value: Any = None
    ttl: float | None = None
    hit: bool | None = None
    result: bool | int | None = field(default=None, compare=False)

    def describe(self) -> str:
        """Render this operation as one readable line for assertion output."""
        parts: list[str] = [self.kind]
        if self.key is not None:
            parts.append(repr(self.key))
        if self.kind in {"put", "put_many", "add", "increment"}:
            parts.append(f"value={self.value!r}")
            parts.append(f"ttl={self.ttl!r}")
        if self.hit is not None:
            parts.append("HIT" if self.hit else "MISS")
        return " ".join(parts)


class CacheAssertionError(AssertionError):
    """Raised when a cache assertion fails.

    Subclasses :class:`AssertionError` so pytest presents it as a failed
    assertion rather than an error.
    """


class FakeStore:
    """Records every operation, then delegates it to a real store.

    Args:
        inner: The store to delegate to. Defaults to a fresh in-memory store,
            which is what almost every test wants.
    """

    __slots__ = ("_inner", "_operations")

    def __init__(self, inner: Store | None = None) -> None:
        self._inner: Store = inner if inner is not None else ArrayStore()
        self._operations: list[Operation] = []

    # -- inspection ------------------------------------------------------

    @property
    def operations(self) -> tuple[Operation, ...]:
        """Every operation recorded, in the order it happened."""
        return tuple(self._operations)

    @property
    def inner(self) -> Store:
        """The store being delegated to."""
        return self._inner

    def reset(self) -> None:
        """Discard the recorded history, leaving stored data intact."""
        self._operations.clear()

    def _record(self, operation: Operation) -> None:
        """Append *operation* to the history."""
        self._operations.append(operation)

    def _of_kind(self, *kinds: OperationKind) -> list[Operation]:
        """Return recorded operations matching any of *kinds*."""
        return [op for op in self._operations if op.kind in kinds]

    def _timeline(self) -> str:
        """Render the whole history, for inclusion in a failure message."""
        if not self._operations:
            return "  (no cache operations were recorded)"
        return "\n".join(f"  {i + 1}. {op.describe()}" for i, op in enumerate(self._operations))

    def _fail(self, message: str) -> CacheAssertionError:
        """Build an assertion error with the recorded timeline attached."""
        return CacheAssertionError(f"{message}\n\nRecorded cache operations:\n{self._timeline()}")

    # -- assertions ------------------------------------------------------

    def assert_hit(self, key: str) -> None:
        """Assert that *key* was read and found.

        Args:
            key: The key expected to have been a hit.

        Raises:
            CacheAssertionError: If the key was never read, or was only missed.
        """
        reads = [op for op in self._of_kind("get", "many") if op.key == key]
        if not reads:
            raise self._fail(f"expected a cache read of {key!r}, but it was never read")
        if not any(op.hit for op in reads):
            raise self._fail(f"expected {key!r} to be a cache hit, but every read missed")

    def assert_missed(self, key: str) -> None:
        """Assert that *key* was read and found absent.

        Args:
            key: The key expected to have been a miss.

        Raises:
            CacheAssertionError: If the key was never read, or always hit.
        """
        reads = [op for op in self._of_kind("get", "many") if op.key == key]
        if not reads:
            raise self._fail(f"expected a cache read of {key!r}, but it was never read")
        if not any(op.hit is False for op in reads):
            raise self._fail(f"expected {key!r} to miss, but every read was a hit")

    def assert_put(
        self,
        key: str,
        value: Any = UNSET,
        ttl: float | Sentinel | None = UNSET,
    ) -> None:
        """Assert that *key* was written.

        Args:
            key: The key expected to have been written.
            value: When given, the value it must have been written with.
            ttl: When given, the lifetime it must have been written with.

        Raises:
            CacheAssertionError: If no matching write was recorded.
        """
        writes = [op for op in self._of_kind("put", "put_many", "add") if op.key == key]
        if not writes:
            raise self._fail(f"expected {key!r} to be written to the cache, but it never was")
        if value is not UNSET and not any(op.value == value for op in writes):
            actual = ", ".join(repr(op.value) for op in writes)
            raise self._fail(f"expected {key!r} to be written with {value!r}, but got: {actual}")
        if ttl is not UNSET and not any(op.ttl == ttl for op in writes):
            actual = ", ".join(repr(op.ttl) for op in writes)
            raise self._fail(f"expected {key!r} to be written with ttl={ttl!r}, but got: {actual}")

    def assert_not_put(self, key: str) -> None:
        """Assert that *key* was never written.

        Args:
            key: The key expected to be untouched by writes.

        Raises:
            CacheAssertionError: If any write to the key was recorded.
        """
        if any(op.key == key for op in self._of_kind("put", "put_many", "add")):
            raise self._fail(f"expected {key!r} never to be written, but it was")

    def assert_forgotten(self, key: str) -> None:
        """Assert that *key* was removed.

        Args:
            key: The key expected to have been forgotten.

        Raises:
            CacheAssertionError: If no removal was recorded.
        """
        if not any(op.key == key for op in self._of_kind("forget", "forget_if")):
            raise self._fail(f"expected {key!r} to be forgotten, but it never was")

    def assert_flushed(self) -> None:
        """Assert that the store was flushed.

        Raises:
            CacheAssertionError: If no flush was recorded.
        """
        if not self._of_kind("flush"):
            raise self._fail("expected the cache to be flushed, but it never was")

    def assert_nothing_written(self) -> None:
        """Assert that no write of any kind happened.

        Raises:
            CacheAssertionError: If any write was recorded.
        """
        writes = self._of_kind("put", "put_many", "add", "increment")
        if writes:
            raise self._fail(f"expected no cache writes, but {len(writes)} were recorded")

    def assert_operation_count(self, expected: int) -> None:
        """Assert the total number of recorded operations.

        Useful for pinning down that a loop hit the cache once rather than N
        times.

        Args:
            expected: The number of operations expected.

        Raises:
            CacheAssertionError: If the count differs.
        """
        actual = len(self._operations)
        if actual != expected:
            raise self._fail(f"expected {expected} cache operations, recorded {actual}")

    # -- Store contract --------------------------------------------------

    @property
    def namespace(self) -> KeyNamespace:
        """Delegated to the wrapped store."""
        return self._inner.namespace

    @property
    def supports_atomic_increment(self) -> bool:
        """Delegated to the wrapped store."""
        return self._inner.supports_atomic_increment

    async def get(self, key: str) -> Maybe[Any]:
        """Read a value, recording whether it hit.

        Args:
            key: The key to read.

        Returns:
            The stored value, or ``MISSING``.
        """
        value = await self._inner.get(key)
        hit = not is_missing(value)
        self._record(Operation("get", key, None if not hit else value, hit=hit))
        return value

    async def many(self, keys: Sequence[str]) -> dict[str, Maybe[Any]]:
        """Read several values, recording one entry per key.

        Args:
            keys: The keys to read.

        Returns:
            Every requested key mapped to its value or ``MISSING``.
        """
        results = await self._inner.many(keys)
        for key, value in results.items():
            hit = not is_missing(value)
            self._record(Operation("many", key, None if not hit else value, hit=hit))
        return results

    async def put(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Write a value, recording the write.

        Args:
            key: The key to write.
            value: The value to store.
            ttl: Lifetime in seconds.

        Returns:
            ``True`` if stored.
        """
        result = await self._inner.put(key, value, ttl)
        self._record(Operation("put", key, value, ttl, result=result))
        return result

    async def put_many(self, values: Mapping[str, Any], ttl: float | None = None) -> bool:
        """Write several values, recording one entry per key.

        Args:
            values: Keys mapped to values.
            ttl: Lifetime applied to all of them.

        Returns:
            ``True`` if every value was stored.
        """
        result = await self._inner.put_many(values, ttl)
        for key, value in values.items():
            self._record(Operation("put_many", key, value, ttl, result=result))
        return result

    async def add(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Write a value if absent, recording the attempt.

        Args:
            key: The key to write.
            value: The value to store.
            ttl: Lifetime in seconds.

        Returns:
            ``True`` if this call created the entry.
        """
        result = await self._inner.add(key, value, ttl)
        self._record(Operation("add", key, value, ttl, result=result))
        return result

    async def increment(self, key: str, by: int = 1) -> int:
        """Increment a counter, recording the resulting value.

        Args:
            key: The counter's key.
            by: The amount to add.

        Returns:
            The value after the operation.
        """
        result = await self._inner.increment(key, by)
        self._record(Operation("increment", key, result, result=result))
        return result

    async def forget(self, key: str) -> bool:
        """Remove an entry, recording the removal.

        Args:
            key: The key to remove.

        Returns:
            ``True`` if an entry was removed.
        """
        result = await self._inner.forget(key)
        self._record(Operation("forget", key, result=result))
        return result

    async def forget_if(self, key: str, expected: Any) -> bool:
        """Conditionally remove an entry, recording the attempt.

        Args:
            key: The key to remove.
            expected: The value the entry must hold.

        Returns:
            ``True`` if the entry matched and was removed.
        """
        result = await self._inner.forget_if(key, expected)
        self._record(Operation("forget_if", key, expected, result=result))
        return result

    async def flush(self) -> bool:
        """Clear the store, recording the flush.

        Returns:
            ``True`` once cleared.
        """
        result = await self._inner.flush()
        self._record(Operation("flush", result=result))
        return result

    def lock(self, name: str, ttl: float = 60.0, *, owner: str | None = None) -> Lock:
        """Build a lock on the wrapped store.

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


__all__ = ["MISSING", "CacheAssertionError", "FakeStore", "Operation", "OperationKind"]
