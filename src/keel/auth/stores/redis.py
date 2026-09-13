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
members whose record is gone, and :meth:`purge_expired` prunes them. The index
is a hint; the record is the truth.

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


class RedisTokenStore:
    """Stores tokens in Redis.

    Args:
        client: A connected async Redis client.
        name: The driver name this store was built as.
        namespace: Prefix applied to every key. Must not be empty.
        ttl: Default lifetime in seconds, or ``None`` for no expiry.
    """

    __slots__ = ("_client", "_name", "_records", "_subjects", "_ttl")

    def __init__(
        self,
        client: Redis,
        *,
        name: str = "redis",
        namespace: KeyNamespace,
        ttl: float | None = None,
    ) -> None:
        self._client = client
        self._name = name
        self._records = namespace.child(RECORDS)
        self._subjects = namespace.child(SUBJECTS)
        self._ttl = ttl

    @classmethod
    def from_url(cls, url: str, config: TokenConfig) -> RedisTokenStore:
        """Build a store from a connection URL.

        Args:
            url: The Redis URL.
            config: The namespace and default lifetime to use.

        Returns:
            A store owning its own client.
        """
        return cls(
            Redis.from_url(url),
            namespace=KeyNamespace(config.prefix),
            ttl=config.ttl,
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
        key = self._records.apply(record.digest)
        index = self._subjects.apply(str(record.subject))
        pipe.set(key, encode_record(record), ex=None if lifetime is None else int(lifetime) + 1)
        pipe.sadd(index, record.digest)
        if lifetime is None:
            pipe.persist(index)
        else:
            # The index must outlive its longest-lived member, never the reverse:
            # a set that expired first would strand tokens nothing can revoke.
            pipe.expire(index, int(lifetime) + 1, gt=True)
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
        digests: set[bytes | str] = await self._client.smembers(index)
        if not digests:
            return 0

        keys = [self._records.apply(self._as_text(digest)) for digest in digests]
        live = sum(1 for raw in await self._client.mget(keys) if raw is not None)
        pipe = self._client.pipeline(transaction=True)
        pipe.unlink(*keys)
        pipe.unlink(index)
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
        digests: set[bytes | str] = await self._client.smembers(index)
        if not digests:
            return []

        ordered = [self._as_text(digest) for digest in digests]
        raws = await self._client.mget([self._records.apply(digest) for digest in ordered])
        now = utcnow()
        live = [
            record
            for raw in raws
            if raw is not None and not (record := decode_record(raw)).is_expired(now)
        ]
        return sorted(live, key=lambda record: record.issued_at, reverse=True)

    async def purge_expired(self) -> int:
        """Prune index members whose record Redis has already dropped.

        The records need no purging — Redis expired them. What accumulates is
        the sets pointing at them, so this is the operation that keeps a
        long-lived subject's index from growing without bound.

        Returns:
            How many stale members were removed.
        """
        removed = 0
        async for index in self._client.scan_iter(match=self._subjects.pattern(), count=SCAN_BATCH):
            digests: set[bytes | str] = await self._client.smembers(index)
            if not digests:
                await self._client.unlink(index)
                continue
            ordered = [self._as_text(digest) for digest in digests]
            raws = await self._client.mget([self._records.apply(digest) for digest in ordered])
            stale = [digest for digest, raw in zip(ordered, raws, strict=True) if raw is None]
            if stale:
                await self._client.srem(index, *stale)
                removed += len(stale)
            if len(stale) == len(ordered):
                await self._client.unlink(index)
        return removed

    async def close(self) -> None:
        """Close the Redis connection. Tokens outlive the process."""
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
