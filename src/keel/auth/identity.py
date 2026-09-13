"""Who the current work is being done for.

Audit columns, authorization and any job that acts on someone's behalf all need
the same answer, and threading it through every signature between the edge and
the code that asks is not workable. So the identity is published on a context
variable, exactly as ``Database.transaction()`` publishes the active session,
and for the same reasons: concurrent requests each get their own, and asyncio
tasks inherit context correctly.

**The identity is a value, never the application's user row.** That is the load
-bearing decision here. An ORM instance placed on a context variable outlives
the session that loaded it, and the next attribute access raises
``DetachedInstanceError`` — usually somewhere far away from the code that put it
there. It is also unserialisable, and a job carrying "who asked for this" has to
survive a trip through Redis. Converting at the edge is the same rule ADR 0002
already applies to services returning schemas rather than models.

**There is no guest Identity.** ``current_identity()`` returns ``None`` when
nobody is authenticated. A Null Object is the obvious pattern and is declined:
a guest object with an ``id`` is indistinguishable from a real caller at every
site that forgets to check, which is precisely how an authorization bug reaches
production. ``None`` cannot be mistaken for a principal, and
:func:`require_identity` is there for callers that would rather raise.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Final
from uuid import UUID

from keel.exceptions import AuthenticationRequiredError


@dataclass(frozen=True, slots=True)
class Identity:
    """A principal, reduced to what any subsystem might need about one.

    Frozen because it is shared across a request and may be serialised into a
    job; a mutable one would let a handler change who it is acting as.

    Attributes:
        id: The principal's primary key. A ``UUID`` because Keel's models use
            UUIDv7 keys (ADR 0003), so an audit column referencing this needs no
            conversion. A service whose users are keyed otherwise builds the
            ``Identity`` at the edge with a UUID it owns.
        roles: Role names captured when the principal was authenticated. Held
            here rather than loaded on demand so an authorization check costs no
            query, at the cost of being as stale as the session that issued it.
        claims: Anything else the edge knew and a policy might want — a tenant,
            a scope, a token id. Deliberately untyped and deliberately small.
    """

    id: UUID
    roles: frozenset[str] = frozenset()
    claims: Mapping[str, Any] = field(default_factory=dict)

    def has_role(self, *names: str) -> bool:
        """Whether this principal holds any of *names*.

        Args:
            *names: Role names, tested as a union rather than an intersection —
                "editor or admin" is the question that gets asked.

        Returns:
            ``True`` if at least one matches. No arguments means ``False``: an
            empty question is not a satisfied one.
        """
        return any(name in self.roles for name in names)


_current: Final[ContextVar[Identity | None]] = ContextVar("keel_current_identity", default=None)
"""The principal in effect, or ``None``. Set at the edge, read anywhere."""


def current_identity() -> Identity | None:
    """Return the principal this work is being done for.

    Returns:
        The bound identity, or ``None`` when nobody is authenticated. Unauthenticated
        is an ordinary state, so this does not raise; see :func:`require_identity`.
    """
    return _current.get()


def require_identity() -> Identity:
    """Return the principal, refusing to continue without one.

    For code whose correctness depends on there being a caller — writing an
    audit column, resolving a policy — where ``None`` would silently produce a
    wrong answer rather than an error.

    Returns:
        The bound identity.

    Raises:
        AuthenticationRequiredError: If nothing is bound.
    """
    identity = _current.get()
    if identity is None:
        raise AuthenticationRequiredError(
            "no identity is bound; authenticate at the edge, or wrap the call "
            "in `with acting_as(identity):`"
        )
    return identity


@contextmanager
def acting_as(identity: Identity | None) -> Iterator[Identity | None]:
    """Bind *identity* for the duration of the block.

    The only supported way to set one. A bare ``ContextVar.set`` leaks into
    whatever the task does next, and the token needed to undo it is easy to drop
    on an exception path.

    Args:
        identity: The principal to bind, or ``None`` to run a block explicitly
            unauthenticated — which is how a job that must not inherit its
            dispatcher's caller says so.

    Yields:
        The identity that was bound.
    """
    token = _current.set(identity)
    try:
        yield identity
    finally:
        _current.reset(token)


__all__ = ["Identity", "acting_as", "current_identity", "require_identity"]
