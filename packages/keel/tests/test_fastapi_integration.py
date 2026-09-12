"""The seam, proved end to end through a real FastAPI application.

Everything else in this suite tests a piece. This tests the claim: that an
application can call a module-level facade, that wiring is one context manager,
and that a test can replace the whole subsystem and assert on what the code did
to it.

The last one is the point. If `fake_cache()` cannot be wrapped around an HTTP
request and asked what was cached, the seam has not bought anything.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient

from examples.fastapi_app import QUERY_COUNT, build_app, get_profile
from keel.cache import CacheConfig, StoreConfig
from keel.testing import fake_cache

pytestmark = [pytest.mark.anyio]


@pytest.fixture
def config() -> CacheConfig:
    return CacheConfig(
        default="default",
        stores={"default": StoreConfig(driver="array", prefix="example", ttl=60.0)},
    )


@pytest.fixture(autouse=True)
def _reset_query_count() -> None:
    QUERY_COUNT["calls"] = 0


@pytest.fixture
async def client(config: CacheConfig) -> AsyncIterator[AsyncClient]:
    """An HTTP client driving the app through its real lifespan."""
    app = build_app(config)
    transport = ASGITransport(app=app)
    async with (
        AsyncClient(transport=transport, base_url="http://test") as http,
        app.router.lifespan_context(app),
    ):
        yield http


# -- the application works -------------------------------------------------


async def test_a_cold_request_computes_the_profile(client: AsyncClient) -> None:
    response = await client.get("/users/42")
    assert response.status_code == 200
    assert response.json() == {"id": 42, "name": "User 42", "plan": "pro"}
    assert QUERY_COUNT["calls"] == 1


async def test_a_second_request_is_served_from_cache(client: AsyncClient) -> None:
    await client.get("/users/42")
    await client.get("/users/42")
    assert QUERY_COUNT["calls"] == 1, "the second request should not have queried"


async def test_different_users_are_cached_separately(client: AsyncClient) -> None:
    await client.get("/users/1")
    await client.get("/users/2")
    assert QUERY_COUNT["calls"] == 2


async def test_eviction_forces_a_recompute(client: AsyncClient) -> None:
    await client.get("/users/42")
    assert (await client.delete("/users/42/cache")).status_code == 204
    await client.get("/users/42")
    assert QUERY_COUNT["calls"] == 2


async def test_cache_operations_are_observable(client: AsyncClient) -> None:
    """The hook the Phase 5 request inspector will hang off."""
    await client.get("/users/42")
    await client.get("/users/42")

    events = (await client.get("/_cache/events")).json()
    assert "CacheMissed profile:42" in events
    assert "KeyWritten profile:42" in events
    assert "CacheHit profile:42" in events


# -- the payoff ------------------------------------------------------------


async def test_a_test_can_replace_the_cache_and_assert_on_it() -> None:
    """What the whole seam exists to make possible.

    No dependency override, no monkeypatching, no mock of a Redis client — the
    application code is unchanged and the test asks the cache what happened.
    """
    with fake_cache() as cached:
        profile = await get_profile(7)

        assert profile["id"] == 7
        cached.assert_missed("profile:7")
        cached.assert_put("profile:7", ttl=60)


async def test_the_fake_proves_a_second_read_does_not_recompute() -> None:
    with fake_cache() as cached:
        await get_profile(7)
        cached.reset()

        await get_profile(7)

        cached.assert_hit("profile:7")
        cached.assert_nothing_written()
        assert QUERY_COUNT["calls"] == 1


async def test_a_failing_cache_assertion_explains_itself() -> None:
    """Assertion quality is a feature: a failure has to say what did happen."""
    with fake_cache() as cached:
        await get_profile(7)

        with pytest.raises(AssertionError) as error:
            cached.assert_put("profile:999")

    message = str(error.value)
    assert "profile:999" in message
    assert "Recorded cache operations" in message
    assert "profile:7" in message, "the timeline should show what was cached instead"
