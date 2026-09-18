"""Events emitted by the cache.

These are the vocabulary an observer needs to reconstruct what a request did to
the cache: what it looked for, what it found, what it wrote. They are plain
frozen dataclasses with no behaviour — an event is a fact that has already
happened, so there is nothing to do to it.

Keys carried here are *unqualified*, matching what the caller passed rather than
what reached the backend. An observer showing ``user:42`` is useful; one showing
``keel:cache:prod:user:42`` is noise.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final


@dataclass(frozen=True, slots=True)
class CacheEvent:
    """Base class for every cache event.

    Attributes:
        store: The name of the store the operation ran against.
    """

    store: str


@dataclass(frozen=True, slots=True)
class CacheHit(CacheEvent):
    """A key was found.

    Attributes:
        key: The unqualified key.
        value: The value that was returned.
    """

    key: str
    value: Any = None


@dataclass(frozen=True, slots=True)
class CacheMissed(CacheEvent):
    """A key was absent.

    Attributes:
        key: The unqualified key.
    """

    key: str


@dataclass(frozen=True, slots=True)
class KeyWritten(CacheEvent):
    """A value was stored.

    Attributes:
        key: The unqualified key.
        value: The value that was stored.
        ttl: Its lifetime in seconds, or ``None`` for indefinite.
    """

    key: str
    value: Any = None
    ttl: float | None = None


@dataclass(frozen=True, slots=True)
class KeyForgotten(CacheEvent):
    """An entry was removed.

    Attributes:
        key: The unqualified key.
        existed: Whether there was an entry to remove.
    """

    key: str
    existed: bool = False


@dataclass(frozen=True, slots=True)
class CounterIncremented(CacheEvent):
    """A counter was changed.

    Distinct from :class:`KeyWritten` because an increment does not set a
    lifetime — it inherits whatever the entry already had. Reporting it as a
    write would force this event to name a TTL it does not know, and an observer
    that trusted it would show "forever" for a key expiring in a minute.

    Attributes:
        key: The unqualified key.
        value: The counter's value after the operation.
        by: The amount added; negative for a decrement.
    """

    key: str
    value: int = 0
    by: int = 1


@dataclass(frozen=True, slots=True)
class CacheFlushed(CacheEvent):
    """A store's namespace was cleared."""


@dataclass(frozen=True, slots=True)
class LockAcquired(CacheEvent):
    """A lock was taken.

    Attributes:
        name: The lock's name.
        owner: The token identifying the holder.
        waited: Seconds spent in ``block()`` before it was taken; zero for an
            immediate ``acquire()``. The number a request inspector shows when
            a single-flight ``remember`` spent its time waiting rather than
            computing.
    """

    name: str = ""
    owner: str = field(default="", repr=False)
    waited: float = 0.0


@dataclass(frozen=True, slots=True)
class LockReleased(CacheEvent):
    """A lock was let go, by its holder or by force.

    Attributes:
        name: The lock's name.
    """

    name: str = ""


EVENT_VERBS: Final[Mapping[type[CacheEvent], str]] = MappingProxyType(
    {
        CacheHit: "hit",
        CacheMissed: "miss",
        KeyWritten: "write",
        KeyForgotten: "forget",
        CounterIncremented: "increment",
        CacheFlushed: "flush",
        LockAcquired: "lock",
        LockReleased: "unlock",
    }
)
"""How each event reads as one word: a timeline's verb, a counter's label."""


def verb(event: CacheEvent) -> str:
    """Return the one-word reading of *event*.

    Args:
        event: Any cache event, including one a driver added.

    Returns:
        The verb from :data:`EVENT_VERBS`, or the class name lowercased.
    """
    return EVENT_VERBS.get(type(event), type(event).__name__.lower())
