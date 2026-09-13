"""The token store test double.

A Test Spy, and — unlike :class:`~keel.queue.fake.FakeQueue` — built the way
:class:`~keel.cache.fake.FakeStore` is: a **Decorator over a real store** that
records what passes through and delegates the actual work.

The queue's reasoning does not apply here, and the difference is which side of
the seam the behaviour belongs to. Running a job means running *the application's*
handler, so a fake that ran one would test the handler while claiming to test the
dispatcher. A token store's behaviour is entirely its own — digesting, expiry,
per-subject indexing — it is cheap to have for real, and code under test almost
always has to sign in and then *use* the credential it was given. A recorder that
answered ``None`` to every :meth:`resolve` could not support that, and would not
pass the contract suite that every other driver passes.

    with fake_tokens() as tokens:
        await sessions.sign_in(email, password)
        tokens.assert_issued(user.id, label="web")
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal
from uuid import UUID

from keel.auth.identity import Identity
from keel.auth.stores.memory import MemoryTokenStore
from keel.auth.tokens import IssuedToken, TokenRecord, digest_token
from keel.contracts.auth import TokenStore

type OperationKind = Literal[
    "issue", "resolve", "revoke", "revoke_subject", "issued_for", "purge_expired"
]

DIGEST_PREVIEW = 12
"""Characters of a digest shown in a failure timeline. Enough to tell two tokens
apart, short enough that the timeline stays one line per operation."""


@dataclass(frozen=True, slots=True)
class TokenOperation:
    """One recorded token operation.

    Attributes:
        kind: Which operation ran.
        subject: The principal involved, or ``None`` when the operation was
            addressed by token rather than by principal.
        digest: The token's digest, for operations that named a token. Never the
            token — a recording double that kept plaintexts would undo the one
            property this subsystem exists to have.
        label: The label an issue was asked for.
        ttl: The lifetime an issue was asked for.
        hit: For resolves, whether the token was accepted.
        result: What the operation reported.
    """

    kind: OperationKind
    subject: UUID | None = None
    digest: str | None = None
    label: str | None = None
    ttl: float | None = None
    hit: bool | None = None
    result: bool | int | None = field(default=None, compare=False)

    def describe(self) -> str:
        """Render this operation as one readable line for assertion output."""
        parts: list[str] = [self.kind]
        if self.subject is not None:
            parts.append(str(self.subject))
        if self.digest is not None:
            parts.append(f"token={self.digest[:DIGEST_PREVIEW]}…")
        if self.kind == "issue":
            parts.append(f"label={self.label!r}")
            parts.append(f"ttl={self.ttl!r}")
        if self.hit is not None:
            parts.append("ACCEPTED" if self.hit else "REFUSED")
        if self.result is not None:
            parts.append(f"-> {self.result!r}")
        return " ".join(parts)


class TokenAssertionError(AssertionError):
    """Raised when a token assertion fails.

    Subclasses :class:`AssertionError` so pytest renders it as a failed
    assertion rather than an error.
    """


class FakeTokenStore:
    """Records every token operation, then delegates it to a real store.

    Args:
        inner: The store to delegate to. Defaults to a fresh in-memory store,
            which is what almost every test wants; pass a real one to record
            against a live backend.
        name: The driver name this store was built as.
        ttl: Default lifetime for the store built when *inner* is omitted.
            Ignored when *inner* is given, because that store already has one.
    """

    __slots__ = ("_inner", "_name", "_operations")

    def __init__(
        self,
        inner: TokenStore | None = None,
        *,
        name: str = "fake",
        ttl: float | None = None,
    ) -> None:
        self._name = name
        self._inner: TokenStore = inner if inner is not None else MemoryTokenStore(name, ttl)
        self._operations: list[TokenOperation] = []

    # -- inspection -------------------------------------------------------

    @property
    def name(self) -> str:
        """The driver name this store was built as."""
        return self._name

    @property
    def inner(self) -> TokenStore:
        """The store being delegated to."""
        return self._inner

    @property
    def operations(self) -> tuple[TokenOperation, ...]:
        """Every operation recorded, in the order it happened."""
        return tuple(self._operations)

    def reset(self) -> None:
        """Discard the recorded history, leaving the stored tokens intact."""
        self._operations.clear()

    def _record(self, operation: TokenOperation) -> None:
        """Append *operation* to the history."""
        self._operations.append(operation)

    def _of_kind(self, *kinds: OperationKind) -> list[TokenOperation]:
        """Return recorded operations matching any of *kinds*."""
        return [op for op in self._operations if op.kind in kinds]

    def _issues(self, subject: UUID, label: str | None) -> list[TokenOperation]:
        """Return recorded issues for *subject*, narrowed by *label* when given."""
        return [
            op
            for op in self._of_kind("issue")
            if op.subject == subject and (label is None or op.label == label)
        ]

    def _timeline(self) -> str:
        """Render the whole history, for inclusion in a failure message."""
        if not self._operations:
            return "  (no token operations were recorded)"
        return "\n".join(f"  {i + 1}. {op.describe()}" for i, op in enumerate(self._operations))

    def _fail(self, message: str) -> TokenAssertionError:
        """Build an assertion error with the recorded timeline attached."""
        return TokenAssertionError(f"{message}\n\nRecorded token operations:\n{self._timeline()}")

    # -- assertions -------------------------------------------------------

    def assert_issued(self, subject: UUID, *, label: str | None = None) -> TokenOperation:
        """Assert that a token was issued for *subject*.

        Args:
            subject: The principal expected to have been given a token.
            label: When given, the label it must have been issued with.

        Returns:
            The first matching operation, so a caller can assert on its ttl.

        Raises:
            TokenAssertionError: If no matching issue was recorded.
        """
        matches = self._issues(subject, label)
        if not matches:
            detail = f" labelled {label!r}" if label is not None else ""
            raise self._fail(f"expected a token issued for {subject}{detail}, but none was")
        return matches[0]

    def assert_not_issued(self, subject: UUID, *, label: str | None = None) -> None:
        """Assert that no token was issued for *subject*.

        The assertion a failed-login test wants: the interesting failure is a
        credential handed out on the *unhappy* path.

        Args:
            subject: The principal.
            label: Narrow the assertion to issues carrying this label.

        Raises:
            TokenAssertionError: If a matching issue was recorded.
        """
        if self._issues(subject, label):
            raise self._fail(f"expected no token issued for {subject}, but one was")

    def assert_issued_times(self, subject: UUID, times: int, *, label: str | None = None) -> None:
        """Assert *subject* was issued exactly *times* tokens.

        The assertion that catches an issue inside a loop, which is how one sign
        -in becomes four hundred live credentials.

        Args:
            subject: The principal.
            times: The expected count.
            label: Narrow to issues carrying this label.

        Raises:
            TokenAssertionError: If the count differs.
        """
        actual = len(self._issues(subject, label))
        if actual != times:
            raise self._fail(f"expected {times} tokens issued for {subject}, got {actual}")

    def assert_nothing_issued(self) -> None:
        """Assert that no token of any kind was issued.

        Raises:
            TokenAssertionError: If anything was issued.
        """
        issues = self._of_kind("issue")
        if issues:
            raise self._fail(f"expected no tokens to be issued, but {len(issues)} were")

    def assert_resolved(self, plaintext: str) -> TokenOperation:
        """Assert that a token was presented and accepted.

        Args:
            plaintext: The token the code under test should have authenticated.

        Returns:
            The first accepted resolve of that token.

        Raises:
            TokenAssertionError: If it was never presented, or always refused.
        """
        digest = digest_token(plaintext)
        presented = [op for op in self._of_kind("resolve") if op.digest == digest]
        if not presented:
            raise self._fail("expected that token to be resolved, but it never was")
        accepted = [op for op in presented if op.hit]
        if not accepted:
            raise self._fail("expected that token to be accepted, but every resolve refused it")
        return accepted[0]

    def assert_rejected(self, plaintext: str) -> TokenOperation:
        """Assert that a token was presented and refused.

        Args:
            plaintext: The token expected not to authenticate.

        Returns:
            The first refused resolve of that token.

        Raises:
            TokenAssertionError: If it was never presented, or was accepted.
        """
        digest = digest_token(plaintext)
        presented = [op for op in self._of_kind("resolve") if op.digest == digest]
        if not presented:
            raise self._fail("expected that token to be resolved, but it never was")
        refused = [op for op in presented if op.hit is False]
        if not refused:
            raise self._fail("expected that token to be refused, but every resolve accepted it")
        return refused[0]

    def assert_revoked(self, plaintext: str) -> TokenOperation:
        """Assert that a token was revoked.

        Args:
            plaintext: The token expected to have been invalidated.

        Returns:
            The matching operation.

        Raises:
            TokenAssertionError: If no revocation of it was recorded.
        """
        digest = digest_token(plaintext)
        for op in self._of_kind("revoke"):
            if op.digest == digest:
                return op
        raise self._fail("expected that token to be revoked, but it never was")

    def assert_not_revoked(self, plaintext: str) -> None:
        """Assert that a token was left alone.

        Args:
            plaintext: The token expected to survive.

        Raises:
            TokenAssertionError: If it was revoked.
        """
        digest = digest_token(plaintext)
        if any(op.digest == digest for op in self._of_kind("revoke")):
            raise self._fail("expected that token not to be revoked, but it was")

    def assert_subject_revoked(self, subject: UUID) -> TokenOperation:
        """Assert that every one of *subject*'s tokens was revoked.

        What a password-reset test asserts, and the one people forget to write
        until an account is compromised and the old sessions still work.

        Args:
            subject: The principal expected to have been signed out everywhere.

        Returns:
            The matching operation, whose ``result`` is how many were removed.

        Raises:
            TokenAssertionError: If no such revocation was recorded.
        """
        for op in self._of_kind("revoke_subject"):
            if op.subject == subject:
                return op
        raise self._fail(f"expected every token for {subject} to be revoked, but none was")

    # -- TokenStore contract ----------------------------------------------

    async def issue(
        self,
        identity: Identity,
        *,
        ttl: float | None = None,
        label: str | None = None,
    ) -> IssuedToken:
        """Mint a token through the wrapped store, recording the issue.

        Args:
            identity: The principal the token authenticates.
            ttl: Lifetime in seconds; the wrapped store's default when omitted.
            label: Free text for a device listing.

        Returns:
            The token, whose plaintext is readable only here.
        """
        issued = await self._inner.issue(identity, ttl=ttl, label=label)
        self._record(
            TokenOperation(
                "issue",
                subject=identity.id,
                digest=issued.record.digest,
                label=label,
                ttl=ttl,
            )
        )
        return issued

    async def resolve(self, plaintext: str) -> Identity | None:
        """Resolve a token through the wrapped store, recording the outcome.

        Args:
            plaintext: The token as presented.

        Returns:
            The identity, or ``None`` if unknown, expired or revoked.
        """
        identity = await self._inner.resolve(plaintext)
        self._record(
            TokenOperation(
                "resolve",
                subject=None if identity is None else identity.id,
                digest=digest_token(plaintext),
                hit=identity is not None,
            )
        )
        return identity

    async def revoke(self, plaintext: str) -> bool:
        """Revoke a token through the wrapped store, recording the attempt.

        Args:
            plaintext: The token as presented.

        Returns:
            Whether a live token was removed.
        """
        result = await self._inner.revoke(plaintext)
        self._record(TokenOperation("revoke", digest=digest_token(plaintext), result=result))
        return result

    async def revoke_subject(self, subject: UUID) -> int:
        """Revoke every token for *subject*, recording the sweep.

        Args:
            subject: The principal's id.

        Returns:
            How many live tokens were removed.
        """
        removed = await self._inner.revoke_subject(subject)
        self._record(TokenOperation("revoke_subject", subject=subject, result=removed))
        return removed

    async def issued_for(self, subject: UUID) -> Sequence[TokenRecord]:
        """List a principal's live tokens, recording the read.

        Args:
            subject: The principal's id.

        Returns:
            The records, newest first.
        """
        records = await self._inner.issued_for(subject)
        self._record(TokenOperation("issued_for", subject=subject, result=len(records)))
        return records

    async def purge_expired(self) -> int:
        """Purge through the wrapped store, recording the housekeeping.

        Returns:
            How many records the wrapped store removed.
        """
        removed = await self._inner.purge_expired()
        self._record(TokenOperation("purge_expired", result=removed))
        return removed

    async def close(self) -> None:
        """Close the wrapped store. The history survives, for assertions after teardown."""
        await self._inner.close()


__all__ = [
    "DIGEST_PREVIEW",
    "FakeTokenStore",
    "OperationKind",
    "TokenAssertionError",
    "TokenOperation",
]
