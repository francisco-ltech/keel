"""Exception hierarchy.

Every error Keel raises descends from :class:`KeelError`, so an application can
catch the whole framework with one clause without also catching ``ValueError``
from its own code. Subsystem errors descend from a subsystem base for the same
reason one level down.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID


class KeelError(Exception):
    """Base class for every error raised by Keel."""


class ConfigurationError(KeelError):
    """Raised when the application is wired incorrectly.

    These are programmer errors — an unknown driver name, a missing binding —
    and should surface at startup rather than on the first request that needs
    the misconfigured subsystem.
    """


class SerializationError(KeelError):
    """Raised when a value cannot be encoded for, or decoded from, a backend.

    Lives here rather than beside the serializers so that it descends from
    :class:`KeelError` like everything else. A caller wrapping a cache call in
    ``except KeelError`` must catch this — an unserialisable value is one of the
    likelier cache failures in practice.
    """


class DatabaseError(KeelError):
    """Base class for database failures."""


class RecordNotFoundError(DatabaseError):
    """Raised when a row that was required is absent.

    Carries the model name and the lookup that failed so the message is useful
    without a debugger, and so an HTTP layer can turn it into a 404 without
    re-deriving what was being looked for.
    """

    def __init__(self, model: str, criteria: object) -> None:
        self.model = model
        self.criteria = criteria
        super().__init__(f"no {model} matching {criteria!r}")


class InvalidCursorError(DatabaseError):
    """Raised when a pagination cursor cannot be decoded.

    Almost always a client sending back something it did not receive, so the
    message deliberately avoids echoing the value into logs.
    """

    def __init__(self, reason: str = "cursor is not valid") -> None:
        super().__init__(reason)


class AuthenticationRequiredError(KeelError):
    """Raised when work that needs a principal is attempted without one.

    A programming error rather than a failed login: the edge either did not
    authenticate the caller or did not bind the result.

    **An edge must not map this to 401.** A caller reaching it has usually sent
    a perfectly good credential that nothing read, so a 401 tells them to retry
    with the thing that just worked, and hides the wiring bug among ordinary
    auth failures where no alarm looks. It belongs with
    :class:`ConfigurationError` as a 500 — it should have been impossible.
    Rejecting a *missing or bad* credential is the edge's own error to raise,
    at the edge, before any service runs.
    """


class AuthorizationDeniedError(KeelError):
    """Raised when a known caller is refused an action on a resource.

    The counterpart to :class:`AuthenticationRequiredError` and deliberately a
    different class, because the two mean opposite things to an edge. This one
    is an **ordinary answer**: the request was understood, the caller was
    identified — or identified as nobody — and the rule said no. An edge maps it
    to **403**. The other one means the edge itself is broken and maps to 500.
    Collapsing them loses the distinction between "you may not" and "this
    service never asked who you are", which is the difference between a metric
    and an alarm.

    Carries the resource's *type name* rather than the resource. An exception
    outlives the block that raised it, and holding an ORM object in one holds
    its session open with it.

    Args:
        action: The ability that was refused, as the call site named it.
        resource: What it was refused on. Only its type name is retained.
        subject: The refused principal's id, or ``None`` when nobody was
            authenticated. Present so an audit line needs no second lookup.
    """

    def __init__(self, action: str, resource: object, subject: UUID | None = None) -> None:
        self.action = action
        self.resource_type = type(resource).__name__
        self.subject = subject
        who = "an unauthenticated caller" if subject is None else f"identity {subject}"
        super().__init__(f"{who} may not {action} {self.resource_type}")


class UnsupportedHashError(KeelError):
    """Raised when a stored password hash uses an unconfigured algorithm.

    Almost always a half-finished migration — bcrypt hashes in the table, only
    Argon2 configured. Loud rather than silent, because returning "wrong
    password" would lock every affected account out indefinitely and look like
    a user error.
    """


class CacheError(KeelError):
    """Base class for cache failures."""


class CacheValueError(CacheError):
    """Raised when a cached value is unusable for the requested operation.

    Most commonly: incrementing a key that holds something non-numeric.
    """


class LockTimeoutError(CacheError):
    """Raised when a lock could not be acquired within the allotted time."""

    def __init__(self, name: str, timeout: float) -> None:
        self.name = name
        self.timeout = timeout
        super().__init__(f"could not acquire lock {name!r} within {timeout}s")
