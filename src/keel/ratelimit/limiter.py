"""A fixed-window rate limiter over the cache's store.

One implementation serves every driver, as the lock does: the contract's
``add`` gives an atomic "create with a lifetime if absent", ``increment`` an
atomic count that keeps the lifetime, and nothing else is needed.

**Windows are aligned to the clock**, not opened by the first hit. A window is
the span ``[n * window, (n + 1) * window)`` and its counter is keyed by ``n``,
so a new window is a new key and the old one is never touched again. That is
what makes the algorithm race-free: the first draft opened a window at the
first hit and had to repair a counter whose lifetime was lost between the
``add`` and the ``increment``, and the repair overwrote concurrent
increments, so a burst got twice the allowance. A counter keyed by its window
needs no repair. Its lifetime is set once by the ``add`` and outlives the
window by a whole extra window, so a replica whose clock lags cannot find it
already gone.

The known weakness of a fixed window is a burst that straddles its edge, which
can reach twice the limit across two windows. For a sign-in or a reset
request that is the difference between five guesses and ten, and a sliding
window's extra bookkeeping buys nothing against that.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from keel.cache import is_missing
from keel.exceptions import ConfigurationError, TooManyAttemptsError

if TYPE_CHECKING:
    from keel.contracts.cache import Store

PREFIX: Final = "ratelimit"
"""Every limiter key sits under this, inside the store's own namespace."""


@dataclass(frozen=True, slots=True)
class Limit:
    """How many attempts a window allows.

    Attributes:
        attempts: Hits allowed inside one window. At least one.
        window: The window's length in seconds. Positive.
    """

    attempts: int
    window: float

    def __post_init__(self) -> None:
        """Refuse a limit that allows nothing or a window that never closes."""
        if self.attempts < 1:
            raise ConfigurationError(f"a limit needs at least one attempt, got {self.attempts}")
        if self.window <= 0:
            raise ConfigurationError(f"a limit's window must be positive, got {self.window}")

    @classmethod
    def per_minute(cls, attempts: int) -> Limit:
        """A limit over a sixty-second window."""
        return cls(attempts, 60.0)

    @classmethod
    def per_hour(cls, attempts: int) -> Limit:
        """A limit over an hour."""
        return cls(attempts, 3600.0)

    @classmethod
    def per_day(cls, attempts: int) -> Limit:
        """A limit over a day."""
        return cls(attempts, 86400.0)


@dataclass(frozen=True, slots=True)
class Attempt:
    """What one hit against a limit found.

    Attributes:
        allowed: Whether this hit was inside the limit.
        remaining: Hits left in the window after this one. Zero when refused.
        retry_after: Seconds until the window closes, when refused; zero when
            allowed. Ceil it for a ``Retry-After`` header.
    """

    allowed: bool
    remaining: int
    retry_after: float


class RateLimiter:
    """Count hits per key inside fixed windows on a cache store.

    Args:
        store: Any store satisfying the cache contract. Keys are namespaced
            under :data:`PREFIX` inside the store's own prefix, so a flush of
            the cache clears every window too, which is the right answer.
    """

    __slots__ = ("_store",)

    def __init__(self, store: Store) -> None:
        self._store = store

    async def hit(self, key: str, limit: Limit) -> int:
        """Count one attempt and return the window's total so far.

        Args:
            key: What is being limited: an address, a client, a pair.
            limit: The window to count inside.

        Returns:
            Hits in the current window, this one included.
        """
        counter = _counter(key, limit)
        # The lifetime outlives the window by one more, so a replica whose
        # clock lags cannot increment a counter the store already dropped.
        await self._store.add(counter, 0, ttl=limit.window * 2)
        return await self._store.increment(counter)

    async def attempt(self, key: str, limit: Limit) -> Attempt:
        """Count one attempt and say whether it was inside the limit.

        Args:
            key: What is being limited.
            limit: The window and the allowance.

        Returns:
            The verdict, with what is left or how long to wait.
        """
        hits = await self.hit(key, limit)
        if hits > limit.attempts:
            return Attempt(allowed=False, remaining=0, retry_after=retry_after(limit))
        return Attempt(allowed=True, remaining=limit.attempts - hits, retry_after=0.0)

    async def guard(self, key: str, limit: Limit) -> Attempt:
        """Count one attempt and refuse it if the limit is spent.

        Args:
            key: What is being limited.
            limit: The window and the allowance.

        Returns:
            The verdict, when allowed.

        Raises:
            TooManyAttemptsError: When refused, carrying how long to wait.
        """
        verdict = await self.attempt(key, limit)
        if not verdict.allowed:
            raise TooManyAttemptsError(key, retry_after=verdict.retry_after)
        return verdict

    async def remaining(self, key: str, limit: Limit) -> int:
        """How many attempts the current window still allows, without counting one.

        Args:
            key: What is being limited.
            limit: The allowance to measure against.

        Returns:
            Attempts left, never negative.
        """
        hits = await self._store.get(_counter(key, limit))
        used = 0 if is_missing(hits) else int(hits)
        return max(0, limit.attempts - used)

    async def clear(self, key: str, limit: Limit) -> None:
        """Forget the current window, so the next hit starts it afresh.

        The call a successful sign-in makes: a correct password after four
        wrong ones should not leave the owner one mistake from a lockout.

        Args:
            key: What is being limited.
            limit: The window the key was counted in.
        """
        await self._store.forget(_counter(key, limit))


def retry_after(limit: Limit) -> float:
    """Seconds until the current window closes, from this process's clock.

    Args:
        limit: The window.

    Returns:
        Seconds, never more than the window.
    """
    return limit.window - (time.time() % limit.window)


def _counter(key: str, limit: Limit) -> str:
    """The key of the current window's counter."""
    return f"{PREFIX}:{key}:{int(time.time() // limit.window)}"


def retry_after_header(seconds: float) -> str:
    """Render a wait as ``Retry-After`` wants it: whole seconds, at least one.

    Args:
        seconds: The wait, fractional.

    Returns:
        The header value.
    """
    return str(max(1, math.ceil(seconds)))


__all__ = ["PREFIX", "Attempt", "Limit", "RateLimiter", "retry_after", "retry_after_header"]
