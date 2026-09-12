"""The cache API application code calls.

This is the abstraction half of the Bridge. It holds no backend knowledge at
all: every method here is expressed in terms of the
:class:`~keel.contracts.cache.Store` primitives, which is what lets a
convenience like :meth:`remember` be written once and work on every driver,
including drivers that do not exist yet.

The inverse also holds and is the more valuable half. Because drivers implement
only the primitives, writing one is a contained job — there is no risk of a new
driver getting ``remember`` subtly wrong, because a new driver does not
implement ``remember``.

Subclassing note, and it is load-bearing: every method reads state through a
*property* rather than the attribute behind it. That is what allows
:class:`~keel.cache.proxy.CacheProxy` to turn this whole class into a
lazily-resolved proxy by overriding a handful of properties instead of
forwarding twenty methods.

The rule this imposes: **adding state to this class means adding a property for
it, and overriding that property on the proxy.** Reading `self._something`
directly inside a method compiles fine and then fails at runtime with an
`AttributeError` the moment the call arrives through the facade — which has
already happened once during development, caught by the FastAPI integration
test rather than by any unit test.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import timedelta
from inspect import isawaitable
from typing import Any, cast, overload

from keel.contracts.cache import Lock, Store
from keel.support.sentinels import MISSING, UNSET, Sentinel, is_missing

SINGLE_FLIGHT_LOCK_TTL = 30.0
"""How long a single-flight lock survives if its holder dies mid-computation.

Bounds the damage: a callback that hangs blocks other callers for this long, not
forever. Longer than any callback worth serialising should take.
"""

SINGLE_FLIGHT_LOCK_TIMEOUT = 30.0
"""How long a waiting caller blocks before giving up and raising.

Deliberately equal to the TTL: waiting longer than the lock can possibly be held
means waiting on something that has already been released.
"""

SINGLE_FLIGHT_LOCK_POLL = 0.05
"""How often a waiting caller retries. Trades a little Redis traffic for latency."""

type TTLInput = float | int | timedelta | Sentinel | None
"""A lifetime: seconds, a :class:`~datetime.timedelta`, ``None`` for forever,
or omitted to take the store's configured default."""


def normalise_ttl(ttl: TTLInput, default: float | None) -> float | None:
    """Reduce the several ways of expressing a lifetime to seconds or ``None``.

    Args:
        ttl: The caller's lifetime, which may be omitted.
        default: The store's configured default, used when *ttl* is omitted.

    Returns:
        A lifetime in seconds, or ``None`` to store indefinitely.
    """
    if ttl is UNSET:
        return default
    if ttl is None:
        return None
    if isinstance(ttl, timedelta):
        return ttl.total_seconds()
    if isinstance(ttl, Sentinel):
        # Any sentinel other than UNSET reaching here is a caller error; treating
        # it as "unspecified" is safer than storing the sentinel itself.
        return default
    return float(ttl)


class Repository:
    """Ergonomic cache operations over a :class:`~keel.contracts.cache.Store`.

    Args:
        store: The backend to operate on.
        default_ttl: The lifetime applied when a caller does not specify one.
            ``None`` means entries live indefinitely by default, which is rarely
            what you want for a cache and is therefore not the default here.
        name: The store's configured name, used in error messages.
        single_flight_ttl: How long a single-flight lock survives if its holder
            dies mid-computation.
        single_flight_timeout: How long a waiting caller blocks before raising.
        single_flight_poll: How often a waiting caller retries.
    """

    __slots__ = ("_default_ttl", "_lock_poll", "_lock_timeout", "_lock_ttl", "_name", "_store")

    def __init__(
        self,
        store: Store,
        default_ttl: float | None = 300.0,
        name: str = "default",
        *,
        single_flight_ttl: float = SINGLE_FLIGHT_LOCK_TTL,
        single_flight_timeout: float = SINGLE_FLIGHT_LOCK_TIMEOUT,
        single_flight_poll: float = SINGLE_FLIGHT_LOCK_POLL,
    ) -> None:
        self._store = store
        self._default_ttl = default_ttl
        self._name = name
        self._lock_ttl = single_flight_ttl
        self._lock_timeout = single_flight_timeout
        self._lock_poll = single_flight_poll

    @property
    def store(self) -> Store:
        """The backend this repository operates on."""
        return self._store

    @property
    def default_ttl(self) -> float | None:
        """The lifetime applied when a caller does not specify one."""
        return self._default_ttl

    @property
    def name(self) -> str:
        """The store's configured name."""
        return self._name

    @property
    def single_flight_ttl(self) -> float:
        """How long a single-flight lock survives if its holder dies."""
        return self._lock_ttl

    @property
    def single_flight_timeout(self) -> float:
        """How long a waiting caller blocks before raising."""
        return self._lock_timeout

    @property
    def single_flight_poll(self) -> float:
        """How often a waiting caller retries."""
        return self._lock_poll

    # -- reading ---------------------------------------------------------

    async def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value, falling back to *default*.

        Args:
            key: The key to read.
            default: Returned when the key is absent.

        Returns:
            The stored value, or *default*.
        """
        value = await self.store.get(key)
        return default if is_missing(value) else value

    async def many(self, keys: Sequence[str], default: Any = None) -> dict[str, Any]:
        """Retrieve several values in one round trip.

        Args:
            keys: The keys to read.
            default: Substituted for keys that are absent.

        Returns:
            Every requested key mapped to its value or *default*.
        """
        found = await self.store.many(keys)
        return {key: (default if is_missing(value) else value) for key, value in found.items()}

    async def has(self, key: str) -> bool:
        """Whether a key is present.

        Args:
            key: The key to test.

        Returns:
            ``True`` if an entry exists, including one holding ``None``.
        """
        return not is_missing(await self.store.get(key))

    async def missing(self, key: str) -> bool:
        """Whether a key is absent.

        Args:
            key: The key to test.

        Returns:
            ``True`` if no entry exists.
        """
        return not await self.has(key)

    async def pull(self, key: str, default: Any = None) -> Any:
        """Read a value and remove it.

        Not atomic: another caller can read the same value between the get and
        the forget. For single-consumer handoff use a queue, not a cache.

        Args:
            key: The key to read and remove.
            default: Returned when the key is absent.

        Returns:
            The stored value, or *default*.
        """
        value = await self.store.get(key)
        if is_missing(value):
            return default
        await self.store.forget(key)
        return value

    # -- writing ---------------------------------------------------------

    async def put(self, key: str, value: Any, ttl: TTLInput = UNSET) -> bool:
        """Store a value, overwriting any existing entry.

        Args:
            key: The key to write.
            value: The value to store.
            ttl: Lifetime. Omit for the configured default, ``None`` for forever.

        Returns:
            ``True`` if the value was stored.
        """
        return await self.store.put(key, value, normalise_ttl(ttl, self.default_ttl))

    async def put_many(self, values: Mapping[str, Any], ttl: TTLInput = UNSET) -> bool:
        """Store several values.

        Args:
            values: Keys mapped to values.
            ttl: Lifetime applied to all of them.

        Returns:
            ``True`` if every value was stored.
        """
        return await self.store.put_many(values, normalise_ttl(ttl, self.default_ttl))

    async def add(self, key: str, value: Any, ttl: TTLInput = UNSET) -> bool:
        """Store a value only if the key is absent, atomically.

        Args:
            key: The key to write.
            value: The value to store.
            ttl: Lifetime.

        Returns:
            ``True`` if this call created the entry.
        """
        return await self.store.add(key, value, normalise_ttl(ttl, self.default_ttl))

    async def forever(self, key: str, value: Any) -> bool:
        """Store a value with no expiry.

        Args:
            key: The key to write.
            value: The value to store.

        Returns:
            ``True`` if the value was stored.
        """
        return await self.store.put(key, value, None)

    async def increment(self, key: str, by: int = 1) -> int:
        """Add to a numeric entry, creating it at zero if absent.

        Args:
            key: The counter's key.
            by: The amount to add.

        Returns:
            The value after the operation.
        """
        return await self.store.increment(key, by)

    async def decrement(self, key: str, by: int = 1) -> int:
        """Subtract from a numeric entry.

        Args:
            key: The counter's key.
            by: The amount to subtract.

        Returns:
            The value after the operation.
        """
        return await self.store.increment(key, -by)

    async def forget(self, key: str) -> bool:
        """Remove an entry.

        Args:
            key: The key to remove.

        Returns:
            ``True`` if an entry was removed.
        """
        return await self.store.forget(key)

    async def flush(self) -> bool:
        """Remove every entry in this store's namespace.

        Returns:
            ``True`` once cleared.
        """
        return await self.store.flush()

    # -- the interesting one ---------------------------------------------

    # Two overloads, not one union-typed parameter: given an `async def` callback a
    # bare `Callable[[], T | Awaitable[T]]` infers `T = Never` and rejects the call.
    @overload
    async def remember[T](
        self,
        key: str,
        callback: Callable[[], Awaitable[T]],
        ttl: TTLInput = UNSET,
        *,
        single_flight: bool = False,
    ) -> T: ...

    @overload
    async def remember[T](
        self,
        key: str,
        callback: Callable[[], T],
        ttl: TTLInput = UNSET,
        *,
        single_flight: bool = False,
    ) -> T: ...

    async def remember[T](
        self,
        key: str,
        callback: Callable[[], T | Awaitable[T]],
        ttl: TTLInput = UNSET,
        *,
        single_flight: bool = False,
    ) -> T:
        """Return the cached value for *key*, computing and storing it if absent.

        This is the Template Method the whole Repository exists for: the shape
        of the algorithm — look, compute on miss, store, return — is fixed here,
        while the primitive steps vary by driver and the computation varies by
        caller.

        Args:
            key: The key to read and populate.
            callback: Produces the value on a miss. May be sync or async.
            ttl: Lifetime for the computed value.
            single_flight: Serialise concurrent misses behind a lock so the
                callback runs once rather than once per waiting caller. Worth
                it when the callback is expensive — a slow query, an upstream
                API — and wasteful when it is not, which is why it is off by
                default.

        Returns:
            The cached or freshly computed value.
        """
        return cast("T", await self._remember(key, callback, ttl, single_flight=single_flight))

    async def _remember(
        self,
        key: str,
        callback: Callable[[], Any],
        ttl: TTLInput,
        *,
        single_flight: bool,
    ) -> Any:
        """The shared implementation behind the overloaded public methods.

        Exists because the public signatures are overloaded: one overloaded
        method cannot call another with a union-typed argument, since the union
        matches neither branch. Both entry points funnel through here.

        Deliberately **not generic**, and untyped in its return. Threading
        ``Callable[[], T | Awaitable[T]]`` through a second generic function lets
        a type solver widen the variable to ``T | Awaitable[T]`` — which is a
        legitimate solution, so the declared ``-> T`` silently becomes untrue.
        mypy happens to pick the narrow solution and ty picks the wide one; both
        are correct, which is the tell that the signature was ambiguous rather
        than that one checker is wrong.

        The type information lives in the public overloads, which are the only
        place a caller sees. Here it is ``Any`` on purpose, and the ``cast`` at
        each call site is where that claim is made explicit rather than
        accidental.
        """
        cached = await self.store.get(key)
        if not is_missing(cached):
            # The store cannot know what it holds — the callback's return type is
            # the only declaration of the value's type.
            return cached
        if single_flight:
            return await self._remember_single_flight(key, callback, ttl)
        return await self._compute_and_store(key, callback, ttl)

    @overload
    async def remember_forever[T](
        self,
        key: str,
        callback: Callable[[], Awaitable[T]],
        *,
        single_flight: bool = False,
    ) -> T: ...

    @overload
    async def remember_forever[T](
        self,
        key: str,
        callback: Callable[[], T],
        *,
        single_flight: bool = False,
    ) -> T: ...

    async def remember_forever[T](
        self,
        key: str,
        callback: Callable[[], T | Awaitable[T]],
        *,
        single_flight: bool = False,
    ) -> T:
        """Like :meth:`remember`, with no expiry on the stored value.

        Args:
            key: The key to read and populate.
            callback: Produces the value on a miss.
            single_flight: Serialise concurrent misses behind a lock.

        Returns:
            The cached or freshly computed value.
        """
        return cast("T", await self._remember(key, callback, None, single_flight=single_flight))

    async def _compute_and_store(
        self,
        key: str,
        callback: Callable[[], Any],
        ttl: TTLInput,
    ) -> Any:
        """Run *callback*, store the result under *key*, and return it.

        Untyped for the same reason as :meth:`_remember`: the sync/async union
        cannot be resolved by the type system, only at runtime by
        :func:`inspect.isawaitable`.
        """
        produced = callback()
        value = await produced if isawaitable(produced) else produced
        await self.put(key, value, ttl)
        return value

    async def _remember_single_flight(
        self,
        key: str,
        callback: Callable[[], Any],
        ttl: TTLInput,
    ) -> Any:
        """Compute under a lock, re-checking the cache once the lock is held.

        The second read is the point. By the time a waiting caller acquires the
        lock, the caller that held it has usually already stored the value, so
        the expensive work happens once no matter how many callers missed
        simultaneously.
        """
        lock = self.lock(f"remember:{key}", ttl=self.single_flight_ttl)
        # Not `async with await lock.block(...)`: block() already acquires, and
        # __aenter__ would then re-acquire and fail. Acquire once, release in `finally`.
        await lock.block(self.single_flight_timeout, poll=self.single_flight_poll)
        try:
            cached = await self.store.get(key)
            if not is_missing(cached):
                return cached
            return await self._compute_and_store(key, callback, ttl)
        finally:
            await lock.release()

    # -- coordination ----------------------------------------------------

    def lock(self, name: str, ttl: float = 60.0, *, owner: str | None = None) -> Lock:
        """Build a lock backed by this store.

        Args:
            name: The lock's name.
            ttl: Seconds after which the lock self-releases.
            owner: An explicit owner token; generated when omitted.

        Returns:
            An unacquired lock.
        """
        return self.store.lock(name, ttl, owner=owner)

    async def close(self) -> None:
        """Release the underlying store's resources."""
        await self.store.close()

    def __repr__(self) -> str:
        """Identify the store by name and driver, which is what you want in a traceback."""
        return f"<Repository {self.name!r} store={type(self.store).__name__}>"


__all__ = ["MISSING", "Repository", "TTLInput", "normalise_ttl"]
