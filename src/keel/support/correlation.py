"""Ambient fields that travel with a unit of work.

Every subsystem eventually wants the same thing the identity context already
provides: something the edge knows, that code five frames deeper has to be able
to read, and that nobody wants threaded through every signature in between. A
request id is the canonical one — it is what makes a worker's log lines joinable
to the API call that caused the work, which is the promise
:class:`keel.queue.envelope.Envelope`'s ``context`` field has been making since
Phase 3 with nothing populating it.

The mechanism is the one :func:`keel.auth.identity.acting_as` and
:meth:`keel.database.engine.Database.transaction` already use, for the same
reasons: a :class:`~contextvars.ContextVar` rather than a global, so concurrent
requests each get their own and asyncio tasks inherit correctly; bound through a
context manager, so the token that undoes it cannot be dropped on an exception
path.

**No pattern, deliberately.** This is a context variable and a context manager.
A manager-and-driver seam over it would be ceremony — there is no backend, there
is nothing to swap, and there is nothing to fake, because a test binds real
fields and reads them back. ADR 0000's counter-rule is the whole justification
for the size of this module.

It lives in ``support`` rather than in ``keel.observability`` because logging is
not the only consumer: the queue seals these onto every envelope, and a tenant
or a feature-flag cohort is the same kind of value with nothing to do with logs.
``keel.observability`` re-exports it for the applications that only care about
the logging half.

Three decisions were each close enough to be worth stating.

**Binding merges with the surrounding scope.** ``acting_as`` replaces, because
there is exactly one principal and an inner scope naming a different one means
it. Fields are a *set*, and the question an inner scope asks is "also record the
tenant", never "forget the request id" — which is precisely what the worker
would be doing to the field this mechanism exists to carry.

**An inner scope may overwrite a field an outer one set**, and the outer value
comes back when the block exits. Refusing would leave a worker unable to bind
``job_id`` inside a scope that already had one; accepting the call and silently
keeping the outer value would lie to the code that asked.

**Values are coerced to ``str``, and ``None`` is dropped.** A correlation field
lands in two places that cannot hold an arbitrary object: a JSON log line, and
``Envelope.context``, which is serialised onto the queue and stored in
``keel_failed_jobs.context``. Both fail *late* — at the dispatch or at the log
call, frames away from the ``correlate()`` responsible — so the invariant is
established here, where the mistake is. ``None`` is dropped rather than rendered
as ``"None"``, so ``correlate(tenant=maybe_tenant)`` means "nothing to add"
rather than asserting that the tenant is the string None, and an outer scope's
value survives it.

**Two names are refused**, and the refusal is loud for the same reason ADR 0008
makes an unregistered policy raise: a field that silently vanished from every
log line is invisible, and a field that silently *replaced* ``level`` is worse.
:data:`RESERVED_FIELDS` are the members a structured record writes itself.
:data:`SECRET_MARKERS` catch the reflex — ``correlate(token=bearer)`` in a piece
of middleware — and nothing more than that. **This is a check on a name, not on
a value.** A credential bound under an innocent name reaches the log, and the
only real defence is that nothing binds a field unless an author wrote it down.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType
from typing import Final

from keel.exceptions import ConfigurationError

_NOTHING: Final[dict[str, str]] = {}

EMPTY: Final[Mapping[str, str]] = MappingProxyType(_NOTHING)
"""The fields in effect when nothing has been bound. Shared, and immutable."""

RESERVED_FIELDS: Final[frozenset[str]] = frozenset(
    {"time", "level", "logger", "message", "exception"}
)
"""Members a structured log record writes for itself.

Binding one would either be dropped by the formatter or displace the record's
own value, and both are silent. The formatter defends itself as well — it writes
these last — so this is the half that says which spelling was the mistake.
"""

SECRET_MARKERS: Final[tuple[str, ...]] = (
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "authorization",
    "bearer",
    "cookie",
    "api_key",
    "apikey",
    "private_key",
)
"""Substrings that make a field name look like a credential.

A correlation field is copied onto every log record and onto every envelope this
context dispatches, so a credential bound here is a credential in the log
aggregator and in ``keel_failed_jobs``. ``password`` also covers the stored hash,
which is the other thing that must never be written down: ``password_hash`` and
``hashed_password`` are the names anyone actually uses for one. ``bearer`` and
``cookie`` are the two other spellings a piece of middleware reaches for, because
they are what the headers are called.

``session_id`` is deliberately **not** here. It is a legitimate correlation field
— the join between a sequence of requests — and refusing it would not stop anyone
recording it, it would push them to spell it ``sid`` or ``sess``, which is worse
than the thing the list was trying to prevent.
"""

_fields: Final[ContextVar[Mapping[str, str]]] = ContextVar("keel_correlation", default=EMPTY)
"""The fields in effect. Bound at the edge, read anywhere."""


def _refusal(name: str) -> str | None:
    """Return why *name* may not be a correlation field, or ``None`` if it may.

    Args:
        name: The field name.

    Returns:
        A message naming the problem, or ``None`` when the name is acceptable.
    """
    lowered = name.lower()
    if lowered in RESERVED_FIELDS:
        return (
            f"{name!r} is a member a structured log record writes for itself; "
            f"correlation fields may not be named {sorted(RESERVED_FIELDS)}"
        )
    if any(marker in lowered for marker in SECRET_MARKERS):
        return (
            f"{name!r} names a credential, and every correlation field is written "
            f"to every log line and sealed onto every envelope dispatched from "
            f"this context. Carry a reference to the secret, never the secret."
        )
    return None


def correlation() -> Mapping[str, str]:
    """Return the correlation fields in effect.

    Returns:
        An immutable mapping, empty when nothing has been bound. Reading is the
        common case — once per log record — so this hands back the bound mapping
        rather than copying it.
    """
    return _fields.get()


def correlation_fields(fields: Mapping[str, object], *, refuse: bool = True) -> dict[str, str]:
    """Normalise an arbitrary mapping into correlation fields.

    Args:
        fields: The candidate fields. Values are coerced with ``str`` and
            ``None`` values are dropped; see the module docstring.
        refuse: What to do about a reserved or credential-shaped name.
            ``True`` raises, which is right when a programmer wrote the name:
            the mistake is at the call site and should be fixed there. ``False``
            drops it, which is right when the mapping is *data* — a queue
            envelope, an inbound header — because a name chosen elsewhere must
            not be able to stop the process that reads it.

    Returns:
        A plain dict of string fields, safe to serialise and safe to log.

    Raises:
        ConfigurationError: If *refuse* is set and a name is reserved or looks
            like a credential.
        Exception: Whatever a value's ``__str__`` raises. Coercion happens here
            rather than at the log call precisely so the traceback names the
            binding, and wrapping it would hide which object misbehaved.
    """
    cleaned: dict[str, str] = {}
    for name, value in fields.items():
        reason = _refusal(name)
        if reason is not None:
            if refuse:
                raise ConfigurationError(reason)
            continue
        if value is None:
            continue
        cleaned[name] = value if isinstance(value, str) else str(value)
    return cleaned


@contextmanager
def correlate(**fields: object) -> Iterator[Mapping[str, str]]:
    """Bind *fields* alongside the surrounding scope's, for the duration of a block.

    The only supported way to set one. A bare ``ContextVar.set`` leaks into
    whatever the task does next, and the token needed to undo it is easy to drop
    on an exception path — the same argument
    :func:`keel.auth.identity.acting_as` makes.

    Example:
        >>> with correlate(request_id="abc123"):  # doctest: +SKIP
        ...     await handle(request)

    Args:
        **fields: Fields to add. They merge with whatever is already bound and
            take precedence over it for the names they share; everything else
            survives, and the previous mapping is restored on the way out.

    Yields:
        The merged mapping now in effect.

    Raises:
        ConfigurationError: If a name is reserved or looks like a credential.
        Exception: Whatever a value's ``__str__`` raises; see
            :func:`correlation_fields`. Nothing is bound when it does.
    """
    merged = MappingProxyType({**_fields.get(), **correlation_fields(fields)})
    token = _fields.set(merged)
    try:
        yield merged
    finally:
        _fields.reset(token)


__all__ = [
    "EMPTY",
    "RESERVED_FIELDS",
    "SECRET_MARKERS",
    "correlate",
    "correlation",
    "correlation_fields",
]
