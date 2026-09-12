"""Redis store.

An Adapter: it translates between Keel's :class:`~keel.contracts.cache.Store`
contract and ``redis.asyncio.Redis``, which speaks a different vocabulary —
bytes rather than objects, ``None`` for both "absent" and "stored null",
millisecond expiries, and integer reply codes where the contract wants booleans.

Two details are worth knowing before changing anything here.

``forget_if`` is a Lua script rather than a get-then-delete pair. Redis runs
scripts atomically, which is what makes lock release safe; doing it in two round
trips would reopen the race the owner token exists to close.

``flush`` scans and unlinks this store's namespace rather than calling
``FLUSHDB``. Caches share servers, and a cache clear should never be capable of
wiping a queue, a session table, or another application.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from keel.cache.counters import guard_overflow as _guard_overflow
from keel.cache.lock import StoreLock
from keel.contracts.cache import Lock
from keel.exceptions import CacheValueError, ConfigurationError
from keel.support.keys import KeyNamespace
from keel.support.sentinels import MISSING, Maybe
from keel.support.serialization import JsonSerializer, Serializer

if TYPE_CHECKING:
    from redis.commands.core import AsyncScript

_COMPARE_AND_DELETE: Final = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

DEFAULT_NAMESPACE: Final = "keel"
"""Applied when a caller does not name one.

A network-backed store shares its server with other applications, other Keel
subsystems, and often other environments. An unnamespaced store would build the
``*`` pattern and flush all of it, so the safe choice has to be the default
rather than something a careful operator remembers to configure.
"""

EMULATED_INCREMENT_LOCK_TTL: Final = 5.0
"""Lifetime of the lock guarding an emulated increment."""

EMULATED_INCREMENT_TIMEOUT: Final = 5.0
"""How long a caller waits for that lock before giving up."""

EMULATED_INCREMENT_POLL: Final = 0.01
"""Retry interval while waiting. Short: an increment is a few milliseconds."""

SCAN_BATCH: Final = 500
"""Keys per SCAN iteration. Large enough to be cheap, small enough not to block Redis."""


class RedisStore:
    """A cache backed by Redis.

    Args:
        client: A configured async client. Passing one in means the application
            owns its lifecycle and connection pool; the store will not close it.
        namespace: The slice of the Redis keyspace this store owns. Strongly
            recommended — it is what makes :meth:`flush` safe on a shared server.
        serializer: Encoding strategy. The default keeps integers as bare digits
            so ``INCRBY`` works natively.
    """

    __slots__ = ("_cad", "_client", "_namespace", "_owns_client", "_serializer")

    def __init__(
        self,
        client: Redis,
        namespace: KeyNamespace | None = None,
        serializer: Serializer | None = None,
        *,
        _owns_client: bool = False,
    ) -> None:
        self._client = client
        self._namespace = self._require_namespace(namespace)
        self._serializer = serializer or JsonSerializer()
        self._owns_client = _owns_client
        self._cad: AsyncScript = client.register_script(_COMPARE_AND_DELETE)

    @staticmethod
    def _require_namespace(namespace: KeyNamespace | None) -> KeyNamespace:
        """Return a usable namespace, refusing an explicitly empty one.

        Args:
            namespace: The caller's namespace, or ``None`` to take the default.

        Returns:
            A non-empty namespace.

        Raises:
            ConfigurationError: If a namespace was given and it is empty. That
                is a caller who has thought about it and got it wrong, which
                deserves an error rather than a silent default.
        """
        if namespace is None:
            return KeyNamespace(DEFAULT_NAMESPACE)
        if namespace.is_empty:
            raise ConfigurationError(
                "a Redis store needs a non-empty key namespace: without one, "
                "flush() would clear every key on the server, including other "
                "applications' data"
            )
        return namespace

    @classmethod
    def from_url(
        cls,
        url: str,
        namespace: KeyNamespace | None = None,
        serializer: Serializer | None = None,
    ) -> RedisStore:
        """Build a store that owns its own client.

        Args:
            url: A ``redis://`` connection URL.
            namespace: The key namespace.
            serializer: Encoding strategy.

        Returns:
            A store which will close its client on :meth:`close`.
        """
        client: Redis = Redis.from_url(url, decode_responses=False)
        return cls(client, namespace, serializer, _owns_client=True)

    @property
    def namespace(self) -> KeyNamespace:
        """The slice of the keyspace this store owns."""
        return self._namespace

    @property
    def client(self) -> Redis:
        """The underlying client, for operations outside the cache contract."""
        return self._client

    @property
    def supports_atomic_increment(self) -> bool:
        """Whether ``INCRBY`` can act on the configured encoding."""
        return self._serializer.supports_atomic_increment

    @staticmethod
    def _as_bytes(raw: bytes | str) -> bytes:
        """Normalise a Redis reply to bytes.

        The client is built with ``decode_responses=False`` so replies are
        already bytes, but the library types them as ``bytes | str`` and a
        caller may pass in a client configured the other way. Encoding here is
        cheaper than discovering the difference through a serialisation error.
        """
        return raw.encode() if isinstance(raw, str) else raw

    @staticmethod
    def _milliseconds(ttl: float) -> int:
        """Convert a TTL in seconds to whole milliseconds, never rounding to zero."""
        return max(1, int(ttl * 1000))

    async def get(self, key: str) -> Maybe[Any]:
        """Retrieve a value.

        Args:
            key: The unqualified key.

        Returns:
            The stored value, or ``MISSING``.
        """
        raw = await self._client.get(self._namespace.apply(key))
        if raw is None:
            return MISSING
        return self._serializer.loads(self._as_bytes(raw))

    async def many(self, keys: Sequence[str]) -> dict[str, Maybe[Any]]:
        """Retrieve several values in one round trip.

        Args:
            keys: The unqualified keys.

        Returns:
            Every requested key mapped to its value or ``MISSING``.
        """
        if not keys:
            return {}
        raws = await self._client.mget([self._namespace.apply(key) for key in keys])
        return {
            key: MISSING if raw is None else self._serializer.loads(self._as_bytes(raw))
            for key, raw in zip(keys, raws, strict=True)
        }

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
            await self._client.delete(qualified)
            return False
        payload = self._serializer.dumps(value)
        if ttl is None:
            return bool(await self._client.set(qualified, payload))
        return bool(await self._client.set(qualified, payload, px=self._milliseconds(ttl)))

    async def put_many(self, values: Mapping[str, Any], ttl: float | None = None) -> bool:
        """Store several values in one pipeline.

        Args:
            values: Unqualified keys mapped to values.
            ttl: Lifetime applied to all of them.

        Returns:
            ``True`` if every value was stored.
        """
        if not values:
            return True
        if ttl is not None and ttl <= 0:
            await self._client.delete(*(self._namespace.apply(key) for key in values))
            return False
        px = None if ttl is None else self._milliseconds(ttl)
        async with self._client.pipeline(transaction=False) as pipe:
            for key, value in values.items():
                pipe.set(self._namespace.apply(key), self._serializer.dumps(value), px=px)
            results = await pipe.execute()
        return all(bool(result) for result in results)

    async def add(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Store a value only if the key is absent, via ``SET NX``.

        Args:
            key: The unqualified key.
            value: The value to store.
            ttl: Lifetime in seconds.

        Returns:
            ``True`` if this call created the entry.
        """
        if ttl is not None and ttl <= 0:
            return False
        px = None if ttl is None else self._milliseconds(ttl)
        created = await self._client.set(
            self._namespace.apply(key),
            self._serializer.dumps(value),
            px=px,
            nx=True,
        )
        return bool(created)

    async def increment(self, key: str, by: int = 1) -> int:
        """Add to a numeric entry, preserving any existing expiry.

        Uses ``INCRBY`` when the serializer stores integers as bare digits. When
        it does not, the read-modify-write is serialised behind a lock on this
        same store, so the guarantee holds either way — it just costs more.

        Args:
            key: The unqualified key.
            by: The amount to add.

        Returns:
            The value after the operation.

        Raises:
            CacheValueError: If the entry exists but is not numeric, or if the
                result would overflow the backend's 64-bit counter.
            LockTimeoutError: Only on the emulated path, and only if the lock
                cannot be taken within :data:`EMULATED_INCREMENT_TIMEOUT` —
                which means sustained contention on a single counter.
        """
        if not self.supports_atomic_increment:
            return await self._increment_under_lock(key, by)
        try:
            return int(await self._client.incrby(self._namespace.apply(key), by))
        except ResponseError as exc:
            if "overflow" in str(exc).lower():
                raise CacheValueError(
                    f"cannot increment key {key!r}: the result would overflow a 64-bit counter"
                ) from exc
            raise CacheValueError(
                f"cannot increment key {key!r}: Redis reports a non-numeric value"
            ) from exc

    async def _increment_under_lock(self, key: str, by: int) -> int:
        """Emulate an atomic increment for encodings Redis cannot read."""
        from keel.support.sentinels import is_missing

        lock = self.lock(f"increment:{key}", ttl=EMULATED_INCREMENT_LOCK_TTL)
        # `async with lock` would *fail* on contention rather than wait for it,
        # which turns concurrent increments into a pile of LockTimeoutErrors and
        # silently loses every one of them. Waiting is the entire point here.
        await lock.block(EMULATED_INCREMENT_TIMEOUT, poll=EMULATED_INCREMENT_POLL)
        try:
            current = await self.get(key)
            if is_missing(current):
                base = 0
            elif isinstance(current, int) and not isinstance(current, bool):
                base = current
            else:
                raise CacheValueError(
                    f"cannot increment key {key!r}: it holds "
                    f"{type(current).__name__}, not an integer"
                )
            updated = _guard_overflow(key, base + by)
            ttl_ms = await self._client.pttl(self._namespace.apply(key))
            remaining = ttl_ms / 1000 if ttl_ms and ttl_ms > 0 else None
            await self.put(key, updated, remaining)
            return updated
        finally:
            await lock.release()

    async def forget(self, key: str) -> bool:
        """Remove an entry.

        Args:
            key: The unqualified key.

        Returns:
            ``True`` if an entry was removed.
        """
        return bool(await self._client.delete(self._namespace.apply(key)))

    async def forget_if(self, key: str, expected: Any) -> bool:
        """Remove an entry only if it holds *expected*, atomically.

        Args:
            key: The unqualified key.
            expected: The value the entry must hold.

        Returns:
            ``True`` if the entry matched and was removed.
        """
        deleted = await self._cad(
            keys=[self._namespace.apply(key)],
            args=[self._serializer.dumps(expected)],
        )
        return bool(deleted)

    async def flush(self) -> bool:
        """Remove the keys in this store's namespace, leaving the rest alone.

        Note the weaker promise: ``SCAN`` is cursor-based and gives no snapshot
        guarantee, so a key written by another process *during* the scan may
        survive. Every key present when the scan started and still present when
        its page is reached is removed.

        Returns:
            ``True`` once the namespace has been cleared.
        """
        pattern = self._namespace.pattern()
        cursor = 0
        while True:
            cursor, keys = await self._client.scan(cursor, match=pattern, count=SCAN_BATCH)
            if keys:
                await self._client.unlink(*keys)
            if cursor == 0:
                return True

    def lock(self, name: str, ttl: float = 60.0, *, owner: str | None = None) -> Lock:
        """Construct a lock backed by this store.

        The generic :class:`~keel.cache.lock.StoreLock` is correct here without
        specialisation, because :meth:`forget_if` is already atomic.

        Args:
            name: The lock's name.
            ttl: Seconds after which the lock self-releases.
            owner: An explicit owner token; generated when omitted.

        Returns:
            An unacquired lock.
        """
        return StoreLock(self, name, ttl, owner=owner)

    async def close(self) -> None:
        """Close the client, but only if this store created it."""
        if self._owns_client:
            await self._client.aclose()
