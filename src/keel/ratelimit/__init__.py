"""Rate limiting.

Two imports cover it. One wherever an attempt should be counted::

    from keel.ratelimit import Limit, guard

    await guard(f"sign-in:{email}|{client}", Limit.per_minute(5))

which raises :class:`~keel.exceptions.TooManyAttemptsError` with how long to
wait, and :func:`attempt` for a caller that wants the verdict without the
exception. Both count against the cache's default store, so there is nothing
to wire: whatever ``cache_lifespan`` bound is what the windows live in, and
``fake_cache()`` in a test gives every test its own.

**No driver seam, no config and no fake of its own**, each a decision. The
store is the seam, as it is for the lock: every cache driver already offers
the two atomic operations a fixed window needs. A ``RATELIMIT_*`` knob would
name nothing, since the store is the cache's and the limits are the
application's. And a fake would record what the real thing already makes
observable, which is the 429. ADR 0017.
"""

from __future__ import annotations

from keel.cache import cache
from keel.exceptions import TooManyAttemptsError
from keel.ratelimit.limiter import PREFIX, Attempt, Limit, RateLimiter, retry_after_header


def rate_limiter() -> RateLimiter:
    """A limiter over the bound default cache store, resolved now.

    Returns:
        The limiter. Cheap to build; nothing is held between calls. A limiter
        over another store is ``RateLimiter(cache.of(name).store)``.
    """
    return RateLimiter(cache.store)


async def attempt(key: str, limit: Limit) -> Attempt:
    """Count one attempt against the default store and return the verdict.

    Args:
        key: What is being limited.
        limit: The window and the allowance.

    Returns:
        The verdict.
    """
    return await rate_limiter().attempt(key, limit)


async def guard(key: str, limit: Limit) -> Attempt:
    """Count one attempt against the default store and refuse it if spent.

    Args:
        key: What is being limited.
        limit: The window and the allowance.

    Returns:
        The verdict, when allowed.

    Raises:
        TooManyAttemptsError: When refused.
    """
    return await rate_limiter().guard(key, limit)


async def clear(key: str, limit: Limit) -> None:
    """Forget a key's current window on the default store.

    Args:
        key: What is being limited.
        limit: The window it was counted in.
    """
    await rate_limiter().clear(key, limit)


__all__ = [
    "PREFIX",
    "Attempt",
    "Limit",
    "RateLimiter",
    "TooManyAttemptsError",
    "attempt",
    "clear",
    "guard",
    "rate_limiter",
    "retry_after_header",
]
