"""Time-ordered primary keys.

UUID4 is the obvious default and the wrong one for a primary key. Random values
scatter inserts across the whole B-tree, so every write dirties a different
page: the index stops fitting in cache, write amplification climbs, and rows
created together end up nowhere near each other on disk. On a table that grows,
that is a real and permanent cost paid for nothing — the randomness buys
unguessability, which a *primary key* rarely needs.

UUIDv7 (RFC 9562) keeps the unguessability of the random tail and prepends a
48-bit millisecond timestamp, so values sort by creation time. Inserts land at
the right-hand edge of the index, and ``ORDER BY id`` is a usable approximation
of ``ORDER BY created_at`` without a second index.

Python 3.14 added :func:`uuid.uuid7`. Keel supports 3.13, so this module uses
the standard library's implementation when it exists and falls back to its own
otherwise — same output format either way.
"""

from __future__ import annotations

import secrets
import threading
import time
import uuid
from typing import Final

_UNIX_TS_MS_BITS: Final = 48
_RAND_A_BITS: Final = 12
_RAND_A_MAX: Final = (1 << _RAND_A_BITS) - 1
_SEQUENCE_HEADROOM_BITS: Final = 10
"""Bits of randomness seeded into the counter at the start of each millisecond.

Leaves roughly 3,000 increments of room before the counter overflows into the
next millisecond, while still making the low bits unpredictable.
"""

_VERSION: Final = 0x7
_VARIANT_MASK: Final = 0x3F
_VARIANT_BITS: Final = 0x80

_lock = threading.Lock()
_last_timestamp_ms = -1
_sequence = 0


def _fallback_uuid7() -> uuid.UUID:
    """Generate a UUIDv7 without :func:`uuid.uuid7`.

    Monotonic within a millisecond: the 12-bit ``rand_a`` field is used as a
    counter, seeded randomly at the start of each millisecond and incremented
    for every value generated inside it. Without that, two ids created in the
    same millisecond have no defined order, and "time-ordered" would be a claim
    that fails exactly when a burst of rows is inserted together — which is
    precisely when ordering is being relied upon.

    Returns:
        A version 7 UUID.
    """
    global _last_timestamp_ms, _sequence

    with _lock:
        timestamp_ms = time.time_ns() // 1_000_000

        if timestamp_ms > _last_timestamp_ms:
            _last_timestamp_ms = timestamp_ms
            _sequence = secrets.randbits(_SEQUENCE_HEADROOM_BITS)
        else:
            # Same millisecond, or a clock that went backwards. Holding the last
            # timestamp and advancing the counter keeps ordering from regressing.
            _sequence += 1
            if _sequence > _RAND_A_MAX:
                # Counter exhausted: borrow the next millisecond rather than emit a
                # duplicate. Above ~4,000 ids/ms this drifts ahead of the wall clock.
                _last_timestamp_ms += 1
                _sequence = 0
            timestamp_ms = _last_timestamp_ms

        sequence = _sequence

    value = bytearray(16)
    value[0:6] = timestamp_ms.to_bytes(6, "big")
    value[6] = (_VERSION << 4) | (sequence >> 8)
    value[7] = sequence & 0xFF
    value[8:16] = secrets.token_bytes(8)
    value[8] = (value[8] & _VARIANT_MASK) | _VARIANT_BITS

    return uuid.UUID(bytes=bytes(value))


uuid7 = getattr(uuid, "uuid7", _fallback_uuid7)
"""Generate a time-ordered UUID.

Uses :func:`uuid.uuid7` on Python 3.14+, and an equivalent implementation
below that.
"""


def timestamp_of(value: uuid.UUID) -> float:
    """Return the Unix timestamp encoded in a UUIDv7, in seconds.

    Useful for debugging and for backfills: given an id, you can tell when the
    row was created without reading the row.

    Args:
        value: A version 7 UUID.

    Returns:
        Seconds since the Unix epoch, with millisecond resolution.

    Raises:
        ValueError: If *value* is not a version 7 UUID, since the timestamp
            field is meaningless for any other version.
    """
    if value.version != 7:
        raise ValueError(f"expected a UUIDv7, got version {value.version}")
    return int.from_bytes(value.bytes[0:6], "big") / 1000


__all__ = ["timestamp_of", "uuid7"]
