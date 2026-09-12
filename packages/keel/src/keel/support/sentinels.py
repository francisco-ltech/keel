"""Sentinel values used across Keel.

A cache must be able to store ``None`` as a legitimate value. That rules out the
common ``return None on miss`` shortcut, so misses are signalled with a distinct
singleton instead.

The enum-with-one-member idiom is used rather than a bare ``object()`` because
type checkers can narrow ``Literal[Sentinel.MISSING]`` in a union, which a plain
sentinel instance does not support.
"""

from __future__ import annotations

import enum
from typing import Final, TypeIs


class Sentinel(enum.Enum):
    """Singleton sentinels. One member, one meaning."""

    MISSING = enum.auto()
    """A key was absent, as distinct from a key holding ``None``."""

    UNSET = enum.auto()
    """An argument was not supplied, as distinct from being supplied as ``None``.

    The cache needs both: ``ttl=None`` means *store forever*, while omitting
    ``ttl`` means *use the store's configured default*. Collapsing the two would
    make the default unreachable without repeating it at every call site.
    """

    def __repr__(self) -> str:
        """Render as a bare name so assertion output stays readable."""
        return f"<{self.name}>"

    def __bool__(self) -> bool:
        """A missing value is falsey, matching the intuition of ``if value:``."""
        return False


MISSING: Final = Sentinel.MISSING
"""Returned by stores when a key is absent, distinguishing it from a stored ``None``."""

UNSET: Final = Sentinel.UNSET
"""Default for optional arguments whose ``None`` value is already meaningful."""

type Maybe[T] = T | Sentinel
"""A value that may be absent. Narrow it with :func:`is_missing` or :func:`is_present`."""


def is_missing[T](value: Maybe[T]) -> TypeIs[Sentinel]:
    """Return ``True`` when *value* is the :data:`MISSING` sentinel.

    Args:
        value: The possibly-absent value to test.

    Returns:
        ``True`` if the value is absent. Narrows in both directions, so the
        ``else`` branch sees a plain ``T`` rather than ``T | Sentinel``.
    """
    return value is MISSING


def is_present[T](value: Maybe[T]) -> TypeIs[T]:
    """Return ``True`` when *value* is a real value rather than :data:`MISSING`.

    Args:
        value: The possibly-absent value to test.

    Returns:
        ``True`` if the value is present. Narrows in both directions.
    """
    return value is not MISSING
