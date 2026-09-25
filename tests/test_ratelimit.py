"""The limiter over every store that satisfies the cache contract, and the facade."""

from __future__ import annotations

import time

import anyio
import pytest

from keel.cache import Store
from keel.exceptions import ConfigurationError, TooManyAttemptsError
from keel.ratelimit import Limit, RateLimiter, attempt, clear, guard, retry_after_header
from keel.testing import fake_cache

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(("attempts", "window"), [(0, 60.0), (3, 0.0), (3, -1.0)])
def test_a_limit_that_cannot_work_is_refused(attempts: int, window: float) -> None:
    with pytest.raises(ConfigurationError):
        Limit(attempts, window)


def test_the_constructors_name_their_windows() -> None:
    assert Limit.per_minute(5) == Limit(5, 60.0)
    assert Limit.per_hour(5).window == 3600.0
    assert Limit.per_day(5).window == 86400.0


@pytest.mark.contract
async def test_attempts_inside_the_window_are_allowed_and_counted_down(store: Store) -> None:
    limiter = RateLimiter(store)
    limit = Limit(3, 3600.0)

    verdicts = [await limiter.attempt("ada", limit) for _ in range(4)]

    assert [v.allowed for v in verdicts] == [True, True, True, False]
    assert [v.remaining for v in verdicts] == [2, 1, 0, 0]
    assert 0 < verdicts[-1].retry_after <= 3600.0
    assert await limiter.remaining("ada", limit) == 0


@pytest.mark.contract
async def test_keys_do_not_share_a_window(store: Store) -> None:
    limiter = RateLimiter(store)
    limit = Limit(1, 3600.0)

    assert (await limiter.attempt("ada", limit)).allowed
    assert (await limiter.attempt("grace", limit)).allowed
    assert not (await limiter.attempt("ada", limit)).allowed


@pytest.mark.contract
async def test_a_key_ending_in_a_number_gets_its_own_window(store: Store) -> None:
    """The window's index is appended to the key; a key that looks like one must not collide."""
    limiter = RateLimiter(store)
    limit = Limit(1, 3600.0)
    index = int(time.time() // limit.window)

    assert (await limiter.attempt("ada", limit)).allowed
    assert (await limiter.attempt(f"ada:{index}", limit)).allowed


@pytest.mark.contract
async def test_a_window_closes_on_its_own(store: Store) -> None:
    """A new window is a new key; the old counter is never read again."""
    limiter = RateLimiter(store)
    limit = Limit(1, 0.2)
    # Start just after a window edge, so the two hits below share one window.
    time.sleep(limit.window - (time.time() % limit.window))

    assert (await limiter.attempt("ada", limit)).allowed
    assert not (await limiter.attempt("ada", limit)).allowed
    time.sleep(limit.window)

    assert (await limiter.attempt("ada", limit)).allowed


@pytest.mark.contract
async def test_clear_opens_a_fresh_window(store: Store) -> None:
    limiter = RateLimiter(store)
    limit = Limit(1, 3600.0)
    await limiter.attempt("ada", limit)
    assert not (await limiter.attempt("ada", limit)).allowed

    await limiter.clear("ada", limit)

    assert (await limiter.attempt("ada", limit)).allowed


@pytest.mark.contract
async def test_guard_raises_with_the_wait(store: Store) -> None:
    limiter = RateLimiter(store)
    limit = Limit(1, 3600.0)
    await limiter.guard("ada", limit)

    with pytest.raises(TooManyAttemptsError) as caught:
        await limiter.guard("ada", limit)

    assert caught.value.key == "ada"
    assert 0 < caught.value.retry_after <= 3600.0


@pytest.mark.contract
async def test_a_concurrent_burst_is_counted_in_full(store: Store) -> None:
    """The first draft lost hits under concurrency on Redis: a burst got twice the allowance."""
    limiter = RateLimiter(store)
    limit = Limit(5, 3600.0)
    allowed: list[bool] = []

    async def one() -> None:
        allowed.append((await limiter.attempt("ada", limit)).allowed)

    async with anyio.create_task_group() as group:
        for _ in range(16):
            group.start_soon(one)

    assert allowed.count(True) == 5
    assert await limiter.remaining("ada", limit) == 0
    assert not (await limiter.attempt("ada", limit)).allowed


@pytest.mark.contract
async def test_a_counter_outlives_its_window(store: Store) -> None:
    """The lifetime is a whole extra window, for a replica whose clock lags."""
    limiter = RateLimiter(store)
    limit = Limit(1, 0.2)
    time.sleep(limit.window - (time.time() % limit.window))
    index = int(time.time() // limit.window)
    await limiter.hit("ada", limit)
    time.sleep(limit.window * 1.5)

    assert await store.get(f"ratelimit:ada:{index}") == 1


async def test_the_facade_counts_against_the_bound_cache() -> None:
    limit = Limit(2, 3600.0)
    with fake_cache():
        assert (await attempt("ada", limit)).allowed
        await guard("ada", limit)
        with pytest.raises(TooManyAttemptsError):
            await guard("ada", limit)
        await clear("ada", limit)
        assert (await attempt("ada", limit)).remaining == 1
    with fake_cache():
        # A fresh fake is a fresh set of windows: tests do not leak into each other.
        assert (await attempt("ada", limit)).remaining == 1


def test_retry_after_rounds_up_to_whole_seconds() -> None:
    assert retry_after_header(0.2) == "1"
    assert retry_after_header(12.01) == "13"
    assert retry_after_header(0.0) == "1"
