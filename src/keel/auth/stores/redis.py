"""Redis token store.

An Adapter between the :class:`~keel.contracts.auth.TokenStore` contract and
``redis.asyncio.Redis``.

Two keys per token, and the second one is the interesting one. The record lives
under its digest, which is what every authenticated request looks up. A set per
subject holds that subject's digests, which is what "sign out everywhere" needs
— and which a scan of the whole namespace would also give, at the cost of doing
it on the one operation people run while panicking about a compromised account.

**The index is allowed to hold stale members.** Redis expires the record and
cannot reach into the set to say so, and chasing that with keyspace
notifications would make correctness depend on a server setting. So reads skip
members whose record is gone and remove them on the way past, and
:meth:`purge_expired` sweeps the rest. The index is a hint; the record is the
truth.

**Nothing here ever deletes the index key.** Every removal is an ``SREM`` of
digests this call actually read, and Redis drops a set once its last member
goes. Deleting the key instead would destroy any digest added between the read
and the write, leaving a live token that no longer appears in its subject's
index — invisible to ``issued_for`` and immune to ``revoke_subject`` for the
rest of its life. A review caught that; the regression tests name it.

**The index carries no TTL**, and an earlier attempt to give it one was worse
than nothing: ``EXPIRE ... GT`` refuses on a key that has no expiry, and
``SADD`` never creates one, so the call failed silently on every issue. Pruning
is what bounds the set, which makes :meth:`purge_expired` a real operation to
schedule rather than an optimisation.

``purge_expired`` scans this store's namespace rather than the keyspace, for the
reason the cache's ``flush`` does: token stores share servers, and an
administrative operation must not be able to reach a queue or another
application.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Final
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from keel.auth.identity import Identity
from keel.auth.tokens import (
    IssuedToken,
    TokenRecord,
    build_record,
    decode_record,
    digest_token,
    encode_record,
    generate_token,
)
from keel.exceptions import ConfigurationError
from keel.support.clock import utcnow
from keel.support.keys import KeyNamespace

if TYPE_CHECKING:
    from keel.auth.config import TokenConfig

RECORDS: Final = "record"
"""Namespace segment for the digest-keyed records."""

SUBJECTS: Final = "subject"
"""Namespace segment for the per-subject digest sets."""

SCAN_BATCH: Final = 500
"""Keys per SCAN round trip. Large enough to be few round trips, small enough
not to block the server on one."""


def _require_namespace(namespace: KeyNamespace) -> KeyNamespace:
    """Return *namespace*, refusing one that scopes nothing.

    Args:
        namespace: The configured namespace.

    Returns:
        The namespace, unchanged.

    Raises:
        ConfigurationError: If it is empty. ``child()`` would still produce a
            usable-looking prefix, so the guard in ``KeyNamespace.pattern()``
            never fires and ``purge_expired`` sweeps every ``subject:*`` key on
            the server instead.
    """
    if namespace.is_empty:
        raise ConfigurationError(
            "a redis token store needs a non-empty namespace: purge_expired "
            "would otherwise scan keys belonging to anything else sharing the "
            "server"
        )
    return namespace


class RedisTokenStore:
    """Stores tokens in Redis.

    Args:
        client: A connected async Redis client.
        name: The driver name this store was built as.
        namespace: Prefix applied to every key. Must not be empty.
        ttl: Default lifetime in seconds, or ``None`` for no expiry.
    """

    __slots__ = ("_client", "_name", "_owns_client", "_records", "_subjects", "_ttl")

    def __init__(
        self,
        client: Redis,
        *,
        name: str = "redis",
        namespace: KeyNamespace,
        ttl: float | None = None,
        owns_client: bool = False,
    ) -> None:
        namespace = _require_namespace(namespace)
        self._client = client
        self._name = name
        self._records = namespace.child(RECORDS)
        self._subjects = namespace.child(SUBJECTS)
        self._ttl = ttl
        self._owns_client = owns_client

    @classmethod
    def from_url(cls, url: str, config: TokenConfig, *, name: str = "redis") -> RedisTokenStore:
        """Build a store from a connection URL.

        Args:
            url: The Redis URL.
            config: The namespace and default lifetime to use.
            name: The driver name this store was resolved under. Passed
                explicitly rather than read from ``config.driver``, which names
                the *default* driver and is wrong for any other.

        Returns:
            A store owning its own client.
        """
        return cls(
            Redis.from_url(url, decode_responses=False),
            name=name,
            namespace=KeyNamespace(config.prefix),
            ttl=config.ttl,
            owns_client=True,
        )

    @property
    def name(self) -> str:
        """The driver name this store was built as."""
        return self._name

    async def issue(
        self,
        identity: Identity,
        *,
        ttl: float | None = None,
        label: str | None = None,
    ) -> IssuedToken:
        """Mint a token for *identity*.

        Args:
            identity: The principal the token authenticates.
            ttl: Lifetime in seconds; the configured default when omitted.
            label: Free text for a device listing.

        Returns:
            The token, whose plaintext is readable only here.
        """
        plaintext = generate_token()
        record = build_record(
            identity,
            plaintext=plaintext,
            ttl=self._ttl if ttl is None else ttl,
            label=label,
        )
        lifetime = record.ttl_remaining()

        pipe = self._client.pipeline(transaction=True)
        pipe.set(
            self._records.apply(record.digest),
            encode_record(record),
            ex=None if lifetime is None else int(lifetime) + 1,
        )
        pipe.sadd(self._subjects.apply(str(record.subject)), record.digest)
        await pipe.execute()
        return IssuedToken(plaintext=plaintext, record=record)

    async def resolve(self, plaintext: str) -> Identity | None:
        """Return the principal a token authenticates.

        Args:
            plaintext: The token as presented.

        Returns:
            The identity, or ``None`` if unknown, expired or revoked.
        """
        record = await self._read(digest_token(plaintext))
        return None if record is None else record.identity

    async def revoke(self, plaintext: str) -> bool:
        """Invalidate one token.

        Args:
            plaintext: The token as presented.

        Returns:
            Whether a live token was removed.
        """
        digest = digest_token(plaintext)
        record = await self._read(digest)
        if record is None:
            await self._client.unlink(self._records.apply(digest))
            return False
        pipe = self._client.pipeline(transaction=True)
        pipe.unlink(self._records.apply(digest))
        pipe.srem(self._subjects.apply(str(record.subject)), digest)
        await pipe.execute()
        return True

    async def revoke_subject(self, subject: UUID) -> int:
        """Invalidate every token belonging to one principal.

        Args:
            subject: The principal's id.

        Returns:
            How many live tokens were removed.
        """
        index = self._subjects.apply(str(subject))
        members: set[bytes | str] = await self._client.smembers(index)
        if not members:
            return 0

        digests = [self._as_text(member) for member in members]
        keys = [self._records.apply(digest) for digest in digests]
        raws = await self._client.mget(keys)
        now = utcnow()
        live = sum(1 for raw in raws if raw is not None and not decode_record(raw).is_expired(now))

        pipe = self._client.pipeline(transaction=True)
        pipe.unlink(*keys)
        # SREM only the digests this call read. Deleting the index key would take
        # a token issued since the SMEMBERS with it, and nothing could revoke it.
        pipe.srem(index, *digests)
        await pipe.execute()
        return live

    async def issued_for(self, subject: UUID) -> Sequence[TokenRecord]:
        """List a principal's live tokens.

        Args:
            subject: The principal's id.

        Returns:
            The records, newest first.
        """
        index = self._subjects.apply(str(subject))
        members: set[bytes | str] = await self._client.smembers(index)
        if not members:
            return []

        digests = [self._as_text(member) for member in members]
        raws = await self._client.mget([self._records.apply(digest) for digest in digests])
        now = utcnow()
        live: list[TokenRecord] = []
        dead: list[str] = []
        for digest, raw in zip(digests, raws, strict=True):
            record = None if raw is None else decode_record(raw)
            if record is not None and not record.is_expired(now):
                live.append(record)
            else:
                dead.append(digest)
        if dead:
            # Self-healing: a listing already paid for the reads, so it may as
            # well leave the index smaller than it found it.
            await self._client.srem(index, *dead)
        return sorted(live, key=lambda record: record.issued_at, reverse=True)

    async def purge_expired(self) -> int:
        """Reclaim expired records and the index members pointing at them.

        Redis drops most records itself, so the work here is mostly the sets,
        which nothing else bounds. Since the index carries no TTL, this is a
        real operation to schedule rather than an optimisation.

        Returns:
            How many entries were reclaimed.
        """
        reclaimed = 0
        async for index in self._client.scan_iter(match=self._subjects.pattern(), count=SCAN_BATCH):
            reclaimed += await self._purge_index(index)
        return reclaimed

    async def _purge_index(self, index: bytes | str) -> int:
        """Reclaim one subject's dead entries.

        Args:
            index: The subject set's key.

        Returns:
            How many entries were reclaimed. Zero if the key is not a set —
            something else's key inside this namespace must not wedge
            housekeeping for every subject after it in the scan.
        """
        try:
            members: set[bytes | str] = await self._client.smembers(index)
        except ResponseError:
            return 0
        if not members:
            return 0

        digests = [self._as_text(member) for member in members]
        raws = await self._client.mget([self._records.apply(digest) for digest in digests])
        now = utcnow()
        dead = [
            digest
            for digest, raw in zip(digests, raws, strict=True)
            if raw is None or decode_record(raw).is_expired(now)
        ]
        if not dead:
            return 0

        pipe = self._client.pipeline(transaction=True)
        pipe.unlink(*[self._records.apply(digest) for digest in dead])
        # SREM, never a delete of the index key: a digest added since the
        # SMEMBERS above would go with it and become unrevocable.
        pipe.srem(index, *dead)
        await pipe.execute()
        return len(dead)

    async def close(self) -> None:
        """Close the client, but only if this store created it.

        An application sharing one client between the cache and the token store
        would otherwise lose the pool when either shuts down.
        """
        if self._owns_client:
            await self._client.aclose()

    @staticmethod
    def _as_text(raw: bytes | str) -> str:
        """Normalise a Redis reply to str.

        Replies are bytes with the default client, but the library types them
        as ``bytes | str`` and a caller may hand in a client configured the
        other way. ADR-free trivia; the cache store carries the same helper.

        Args:
            raw: The reply.

        Returns:
            The decoded string.
        """
        return raw.decode() if isinstance(raw, bytes) else raw

    async def _read(self, digest: str) -> TokenRecord | None:
        """Load a record, treating an expired one as absent.

        Redis usually removes it first, but a record with a fractional TTL can
        outlive its own ``expires_at`` by under a second. Checking here means
        the two drivers cannot disagree at that boundary.

        Args:
            digest: The token's digest.

        Returns:
            The record, or ``None``.
        """
        raw = await self._client.get(self._records.apply(digest))
        if raw is None:
            return None
        record = decode_record(raw)
        return None if record.is_expired() else record


__all__ = ["RECORDS", "SCAN_BATCH", "SUBJECTS", "RedisTokenStore"]
