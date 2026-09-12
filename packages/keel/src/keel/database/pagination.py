"""Keyset pagination.

``LIMIT n OFFSET m`` is the obvious approach and it degrades in two ways that
both bite exactly when a table has grown enough to matter.

It gets slower the deeper you go: the database must walk and discard every one
of the `m` skipped rows, so page 500 costs five hundred times page one. And it
is *incorrect* under concurrent writes — a row inserted before your cursor
shifts everything down, so the reader sees a duplicate on the next page, or a
row deleted shifts everything up and they silently miss one.

Keyset pagination asks "give me the rows after this one" instead of "skip this
many". The cost is constant regardless of depth, and inserts elsewhere in the
table cannot shift the window. The trade-off is real and worth stating: there is
no random access, so you cannot jump to page 500 — which is the right constraint
for an API, and the wrong one for a paginated admin table with numbered pages.

The ordering key is the primary key. That is only sound because Keel's keys are
UUIDv7 and therefore time-ordered (see :mod:`keel.database.ids`); with random
UUID4 keys, ordering by id would be stable but meaningless to a reader.
"""

from __future__ import annotations

import base64
import binascii
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Final

from keel.exceptions import InvalidCursorError

DEFAULT_PAGE_SIZE: Final = 50
"""Rows per page when the caller does not say."""

MAX_PAGE_SIZE: Final = 200
"""Hard ceiling on rows per page.

Unbounded page sizes are how one client turns a paginated endpoint back into an
unpaginated one. The cap is enforced rather than documented.
"""


def encode_cursor(value: uuid.UUID) -> str:
    """Encode a position into an opaque cursor.

    Opaque on purpose: a cursor that looks like an id invites clients to
    construct their own, which then breaks the moment the ordering key changes.
    Base64 of the raw bytes is short and signals "do not read this".

    Args:
        value: The identifier of the last row on the page.

    Returns:
        A URL-safe cursor string.
    """
    return base64.urlsafe_b64encode(value.bytes).rstrip(b"=").decode()


def decode_cursor(cursor: str) -> uuid.UUID:
    """Decode a cursor produced by :func:`encode_cursor`.

    Args:
        cursor: The cursor string.

    Returns:
        The identifier it encodes.

    Raises:
        InvalidCursorError: If the cursor is malformed. Clients send back
            corrupted cursors often enough that this needs to be a clean 400
            rather than a 500.
    """
    padding = "=" * (-len(cursor) % 4)
    try:
        raw = base64.urlsafe_b64decode(cursor + padding)
        return uuid.UUID(bytes=raw)
    except (binascii.Error, ValueError, TypeError) as exc:
        raise InvalidCursorError from exc


def clamp_limit(limit: int | None) -> int:
    """Bring a caller-supplied page size within bounds.

    Args:
        limit: The requested page size, or ``None`` for the default.

    Returns:
        A page size between 1 and :data:`MAX_PAGE_SIZE`.
    """
    if limit is None:
        return DEFAULT_PAGE_SIZE
    return max(1, min(limit, MAX_PAGE_SIZE))


@dataclass(frozen=True, slots=True)
class Page[ItemT]:
    """One page of results, plus how to ask for the next.

    Deliberately carries no total count. Counting the whole table to render
    "page 1 of 412" costs a full scan on every request, and keyset pagination
    cannot use the answer anyway. If a total is genuinely needed, it should be
    a separate, cached, explicitly-requested call.

    Attributes:
        items: The rows, in order.
        next_cursor: Pass this back to get the following page, or ``None`` when
            this is the last one.
        limit: The page size that produced this page.
    """

    items: Sequence[ItemT]
    next_cursor: str | None
    limit: int

    @property
    def has_more(self) -> bool:
        """Whether another page exists."""
        return self.next_cursor is not None

    @property
    def is_empty(self) -> bool:
        """Whether this page contains no rows."""
        return not self.items

    def __len__(self) -> int:
        """Return the number of rows on this page."""
        return len(self.items)

    def __iter__(self) -> Iterator[ItemT]:
        """Iterate the rows, so a page can be used where a sequence is expected."""
        return iter(self.items)


__all__ = [
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "Page",
    "clamp_limit",
    "decode_cursor",
    "encode_cursor",
]
