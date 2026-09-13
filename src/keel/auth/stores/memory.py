"""In-process token store.

A real deployment choice, not a test double: a single-process service or a
development run needs nowhere else to put tokens, and "everyone is signed out
when the process restarts" is an honest property rather than a limitation to
work around.

Two dictionaries rather than one. Tokens are looked up by digest on every
authenticated request, and by subject only when someone signs out everywhere —
so the index exists to keep the common path a single hash lookup while
``revoke_subject`` stays better than a full scan.
"""

from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from keel.auth.identity import Identity
from keel.auth.tokens import IssuedToken, TokenRecord, build_record, digest_token, generate_token
from keel.support.clock import utcnow


class MemoryTokenStore:
    """Holds tokens for the life of the process.

    Args:
        name: The driver name this store was built as.
        ttl: Default lifetime in seconds, or ``None`` for no expiry.
    """

    __slots__ = ("_by_digest", "_by_subject", "_name", "_ttl")

    def __init__(self, name: str = "memory", ttl: float | None = None) -> None:
        self._name = name
        self._ttl = ttl
        self._by_digest: dict[str, TokenRecord] = {}
        self._by_subject: dict[UUID, set[str]] = {}

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
        self._by_digest[record.digest] = record
        self._by_subject.setdefault(record.subject, set()).add(record.digest)
        return IssuedToken(plaintext=plaintext, record=record)

    async def resolve(self, plaintext: str) -> Identity | None:
        """Return the principal a token authenticates.

        Args:
            plaintext: The token as presented.

        Returns:
            The identity, or ``None`` if unknown, expired or revoked.
        """
        record = self._by_digest.get(digest_token(plaintext))
        if record is None:
            return None
        if record.is_expired():
            self._forget(record)
            return None
        return record.identity

    async def revoke(self, plaintext: str) -> bool:
        """Invalidate one token.

        Args:
            plaintext: The token as presented.

        Returns:
            Whether a live token was removed.
        """
        record = self._by_digest.get(digest_token(plaintext))
        if record is None:
            return False
        expired = record.is_expired()
        self._forget(record)
        return not expired

    async def revoke_subject(self, subject: UUID) -> int:
        """Invalidate every token belonging to one principal.

        Args:
            subject: The principal's id.

        Returns:
            How many live tokens were removed.
        """
        removed = 0
        for digest in tuple(self._by_subject.get(subject, ())):
            record = self._by_digest.get(digest)
            if record is None:
                continue
            if not record.is_expired():
                removed += 1
            self._forget(record)
        return removed

    async def issued_for(self, subject: UUID) -> Sequence[TokenRecord]:
        """List a principal's live tokens.

        Args:
            subject: The principal's id.

        Returns:
            The records, newest first.
        """
        now = utcnow()
        live = [
            record
            for digest in self._by_subject.get(subject, ())
            if (record := self._by_digest.get(digest)) is not None and not record.is_expired(now)
        ]
        return sorted(live, key=lambda record: record.issued_at, reverse=True)

    async def purge_expired(self) -> int:
        """Drop expired records.

        Returns:
            How many were removed.
        """
        now = utcnow()
        stale = [record for record in self._by_digest.values() if record.is_expired(now)]
        for record in stale:
            self._forget(record)
        return len(stale)

    async def close(self) -> None:
        """Discard every token. There is nothing else holding them."""
        self._by_digest.clear()
        self._by_subject.clear()

    def _forget(self, record: TokenRecord) -> None:
        """Remove a record from both indexes, leaving no empty subject entry.

        Args:
            record: The record to drop.
        """
        self._by_digest.pop(record.digest, None)
        digests = self._by_subject.get(record.subject)
        if digests is None:
            return
        digests.discard(record.digest)
        if not digests:
            del self._by_subject[record.subject]


__all__ = ["MemoryTokenStore"]
