"""Exception hierarchy.

Every error Keel raises descends from :class:`KeelError`, so an application can
catch the whole framework with one clause without also catching ``ValueError``
from its own code. Subsystem errors descend from a subsystem base for the same
reason one level down.
"""

from __future__ import annotations


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
