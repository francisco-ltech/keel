"""Counter bounds.

Redis counters are 64-bit signed integers and ``INCRBY`` refuses to exceed that
range. Python integers have no such limit, so without this the in-memory store
would happily return a number the Redis store cannot produce — and the shared
contract suite's claim that the drivers "behave identically under the same
assertions" would be false precisely at the boundary where it matters.

The rule Keel applies: the most constrained backend sets the contract. It is
better for an in-memory test to fail the way production will than to pass and
defer the failure.
"""

from __future__ import annotations

from typing import Final

from keel.exceptions import CacheValueError

COUNTER_MIN: Final = -(2**63)
"""Lowest value a counter may hold, matching Redis."""

COUNTER_MAX: Final = 2**63 - 1
"""Highest value a counter may hold, matching Redis."""


def guard_overflow(key: str, value: int) -> int:
    """Return *value*, or raise if it is outside the portable counter range.

    Args:
        key: The counter's key, for the error message.
        value: The prospective new value.

    Returns:
        *value*, unchanged, when it is representable.

    Raises:
        CacheValueError: If the value would overflow a 64-bit signed counter.
    """
    if not COUNTER_MIN <= value <= COUNTER_MAX:
        raise CacheValueError(
            f"cannot increment key {key!r}: the result would overflow a 64-bit counter"
        )
    return value
