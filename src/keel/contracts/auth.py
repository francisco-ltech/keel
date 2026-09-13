"""Auth contracts.

One protocol: :class:`TokenStore`. What varies between implementations is where
the tokens live — Redis, a table, a dict — and not what issuing or revoking
means, which is exactly the variation a driver seam is for.

**There is no Bridge here.** The cache earned one because ``remember`` is a
substantial abstraction built from primitives; a token store's surface *is* the
primitives, and a second layer would be ceremony. ADR 0001 and 0006 both landed
the same way.

**There is no guard protocol, and no user provider.** Both were considered and
declined for now; the reasoning is in ADR 0007, because "we looked and decided
against it" is the part a future reader cannot reconstruct.

Not ``runtime_checkable``, for the reason :mod:`keel.contracts.cache` gives:
``isinstance`` against a protocol checks attribute *names* only, so it would
offer a third-party driver author false assurance. The real conformance check is
``tests/test_token_store_contract.py`` — add the driver to its fixture and run
it.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol
from uuid import UUID

if TYPE_CHECKING:
    from keel.auth.identity import Identity
    from keel.auth.tokens import IssuedToken, TokenRecord


class TokenStore(Protocol):
    """Issues, resolves and revokes bearer tokens.

    Every implementation stores the **digest** of a token, never the token. A
    store that can hand back a working credential is a credential dump with
    extra steps, and the difference is invisible until it leaks.

    Expiry is checked on read as well as enforced by the backend. Redis drops an
    expired key on its own and a table does not, so a contract that relied on
    the backend would mean two different behaviours under one name.
    """

    @property
    def name(self) -> str:
        """The driver name this store was built as."""
        ...

    async def issue(
        self,
        identity: Identity,
        *,
        ttl: float | None = None,
        label: str | None = None,
    ) -> IssuedToken:
        """Mint a token for *identity*.

        Args:
            identity: The principal the token authenticates. Stored as a
                snapshot: a token issued before a demotion still carries the old
                roles until it expires or is revoked.
            ttl: Lifetime in seconds, or ``None`` for the store's configured
                default. A token that never expires is possible only by
                configuring that default away, so it cannot happen by omission.
            label: What a "signed-in devices" listing shows. Free text.

        Returns:
            The token. Its plaintext is readable exactly once, here — nothing
            can recover it afterwards.
        """
        ...

    async def resolve(self, plaintext: str) -> Identity | None:
        """Return the principal a token authenticates.

        Args:
            plaintext: The token as the caller presented it.

        Returns:
            The identity, or ``None`` if the token is unknown, expired or
            revoked. One answer for all three on purpose: distinguishing them
            tells an attacker which of their guesses was once real.
        """
        ...

    async def revoke(self, plaintext: str) -> bool:
        """Invalidate one token.

        Args:
            plaintext: The token as the caller presented it.

        Returns:
            Whether a live token was removed. ``False`` for one already gone.
        """
        ...

    async def revoke_subject(self, subject: UUID) -> int:
        """Invalidate every token belonging to one principal.

        The "sign out everywhere" operation, and what a password reset must call.

        Args:
            subject: The principal's id.

        Returns:
            How many live tokens were removed.
        """
        ...

    async def issued_for(self, subject: UUID) -> Sequence[TokenRecord]:
        """List a principal's live tokens.

        Args:
            subject: The principal's id.

        Returns:
            The records, newest first, holding no recoverable secret. Expired
            ones are excluded, so this is what a device listing renders.
        """
        ...

    async def purge_expired(self) -> int:
        """Reclaim whatever an expired token left behind.

        Housekeeping only. An expired token must already fail to resolve without
        this ever running, so calling it can never change an authentication
        outcome — it reclaims space, and a live token must survive it untouched.

        Returns:
            How many entries were reclaimed. **Not comparable between drivers**:
            one backend expires records itself and leaves only index entries to
            sweep, another holds the records until told. The contract suite
            asserts the guarantee above rather than the number, because pinning
            the number would mean pinning one backend's bookkeeping onto all of
            them.
        """
        ...

    async def close(self) -> None:
        """Release resources held by this store."""
        ...


__all__ = ["TokenStore"]
