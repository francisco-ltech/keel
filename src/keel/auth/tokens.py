"""Bearer tokens: what one is, and how it is stored.

Three decisions live here, and each is the kind that is invisible until it is a
breach.

**Tokens are hashed at rest.** A store holds SHA-256 of the token, never the
token. Anything that can hand back a working credential is a credential dump
with extra steps.

**SHA-256, not Argon2.** The opposite of the password rule, for the opposite
reason: a token is 256 bits of ``secrets`` output, so there is no dictionary to
run and nothing for a slow hash to buy. Argon2 here would cost ~44 ms of the
request budget on every authenticated call to defend against an attack that
cannot happen.

**Comparison is by digest lookup**, so there is no partial-match oracle to time
and no need for :func:`hmac.compare_digest`. A digest is either found or it is
not.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final
from uuid import UUID

from keel.auth.identity import Identity
from keel.exceptions import SerializationError
from keel.support.clock import utcnow

TOKEN_BYTES: Final = 32
"""256 bits. Enough that guessing is not a threat model, which is what lets the
digest be a fast hash."""


def generate_token() -> str:
    """Mint a new token secret.

    Returns:
        A URL-safe string, usable unencoded in a header or a query string.
    """
    return secrets.token_urlsafe(TOKEN_BYTES)


def digest_token(plaintext: str) -> str:
    """Reduce a token to what a store may keep.

    Args:
        plaintext: The token.

    Returns:
        Its hex SHA-256 digest.
    """
    return hashlib.sha256(plaintext.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class TokenRecord:
    """What a store keeps about a token.

    Frozen and carrying no secret, so it is safe to return from an API that
    lists a principal's sessions.

    Attributes:
        digest: The token's SHA-256, and the store's key.
        identity: The principal, snapshotted at issue time.
        issued_at: When it was minted.
        expires_at: When it stops resolving, or ``None`` for a token that never
            does. Reachable only by configuring the default lifetime away.
        label: Free text for a "signed-in devices" listing.
    """

    digest: str
    identity: Identity
    issued_at: datetime
    expires_at: datetime | None = None
    label: str | None = None

    @property
    def subject(self) -> UUID:
        """The principal's id, which is what tokens are indexed by."""
        return self.identity.id

    def is_expired(self, now: datetime | None = None) -> bool:
        """Whether this record has passed its expiry.

        Args:
            now: The instant to judge against; defaults to the current time.

        Returns:
            ``True`` once expired. A record with no expiry is never expired.
        """
        if self.expires_at is None:
            return False
        return (now or utcnow()) >= self.expires_at

    def ttl_remaining(self, now: datetime | None = None) -> float | None:
        """Seconds until expiry.

        Args:
            now: The instant to measure from; defaults to the current time.

        Returns:
            The remaining lifetime, never negative, or ``None`` for a record
            that does not expire.
        """
        if self.expires_at is None:
            return None
        return max(0.0, (self.expires_at - (now or utcnow())).total_seconds())


@dataclass(frozen=True, slots=True)
class IssuedToken:
    """A freshly minted token, and the only place its plaintext exists.

    Returned by :meth:`~keel.contracts.auth.TokenStore.issue` and not stored.
    Hand :attr:`plaintext` to the caller once; afterwards only the digest
    remains, and nothing can reverse it.

    Attributes:
        plaintext: The secret to give the caller.
        record: What the store kept.
    """

    plaintext: str = field(repr=False)
    record: TokenRecord

    @property
    def identity(self) -> Identity:
        """The principal this token authenticates."""
        return self.record.identity

    @property
    def expires_at(self) -> datetime | None:
        """When the token stops resolving."""
        return self.record.expires_at


def build_record(
    identity: Identity,
    *,
    plaintext: str,
    ttl: float | None,
    label: str | None,
    now: datetime | None = None,
) -> TokenRecord:
    """Assemble the record for a token about to be stored.

    Shared by every driver so they cannot disagree about how a TTL becomes an
    expiry — the kind of drift a contract suite catches late and a shared
    function prevents.

    Args:
        identity: The principal.
        plaintext: The token secret, hashed here and not retained.
        ttl: Lifetime in seconds, or ``None`` for a token that never expires.
        label: Free text for a device listing.
        now: Issue time; defaults to the current time.

    Returns:
        The record to store.
    """
    issued_at = now or utcnow()
    return TokenRecord(
        digest=digest_token(plaintext),
        identity=identity,
        issued_at=issued_at,
        expires_at=None if ttl is None else issued_at + timedelta(seconds=ttl),
        label=label,
    )


def encode_record(record: TokenRecord) -> str:
    """Serialise a record for a backend that stores strings.

    Shared by every driver that needs it, so two of them cannot disagree about
    the format and leave tokens written by one unreadable by the other.

    Args:
        record: The record to encode.

    Returns:
        Compact JSON.
    """
    return json.dumps(
        {
            "digest": record.digest,
            "id": str(record.identity.id),
            "roles": sorted(record.identity.roles),
            "claims": dict(record.identity.claims),
            "issued_at": record.issued_at.isoformat(),
            "expires_at": None if record.expires_at is None else record.expires_at.isoformat(),
            "label": record.label,
        },
        separators=(",", ":"),
    )


def decode_record(raw: str | bytes) -> TokenRecord:
    """Rebuild a record encoded by :func:`encode_record`.

    Args:
        raw: The stored JSON.

    Returns:
        The record.

    Raises:
        SerializationError: If the payload is not a record this version wrote —
            a truncated value, or one from a future format. Loud rather than
            silent, because the alternative is authenticating against a
            half-parsed identity.
    """
    try:
        data: dict[str, Any] = json.loads(raw)
        expires_at = data["expires_at"]
        return TokenRecord(
            digest=data["digest"],
            identity=Identity(
                id=UUID(data["id"]),
                roles=frozenset(data["roles"]),
                claims=data["claims"],
            ),
            issued_at=datetime.fromisoformat(data["issued_at"]),
            expires_at=None if expires_at is None else datetime.fromisoformat(expires_at),
            label=data["label"],
        )
    except (TypeError, ValueError, KeyError) as exc:
        raise SerializationError(f"stored token record is unreadable: {exc}") from exc


__all__ = [
    "TOKEN_BYTES",
    "IssuedToken",
    "TokenRecord",
    "build_record",
    "decode_record",
    "digest_token",
    "encode_record",
    "generate_token",
]
