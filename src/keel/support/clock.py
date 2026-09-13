"""The current time, in one place.

Trivial, and here rather than in a subsystem for two reasons: every subsystem
that stores an expiry needs it, and a token store on Redis must not import the
database layer to find out what time it is.

Aware, always. A naive datetime is a bug waiting for a deployment in another
timezone, and it compares unequal to an aware one rather than failing loudly.
"""

from __future__ import annotations

from datetime import UTC, datetime


def utcnow() -> datetime:
    """Return an aware UTC timestamp.

    Returns:
        The current time in UTC.
    """
    return datetime.now(UTC)


__all__ = ["utcnow"]
