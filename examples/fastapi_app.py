"""A FastAPI application using the cache seam.

Read this before the library source — it is the whole point of the machinery,
and it is deliberately short.

Three things to notice:

1. **The route does not receive a cache.** No `Depends(get_cache)`, no parameter
   threaded down from the app. `cache` is imported at module scope and resolves
   per call, so the handler's signature says what the *endpoint* takes, not what
   its dependencies happen to be.

2. **The service function has no idea it is in a web app.** It would work
   unchanged in a worker or a CLI command, which is why Keel's core takes no
   dependency on FastAPI.

3. **Wiring is one context manager.** `cache_lifespan` binds on startup and
   closes the connection pool on shutdown, which is the part everyone forgets
   until a few hundred restarts have leaked it.
"""

from __future__ import annotations

import os
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from keel.cache import CacheConfig, EventDispatcher, cache, cache_lifespan
from keel.cache.events import CacheEvent

# -- the domain ------------------------------------------------------------

RECENT_EVENTS = 200
"""How many cache events the example keeps for inspection."""

QUERY_COUNT = {"calls": 0}
"""Stands in for a database. Counting calls is how the example shows the cache
is doing something rather than merely being present."""


async def load_profile(user_id: int) -> dict[str, object]:
    """Pretend to run an expensive query."""
    QUERY_COUNT["calls"] += 1
    return {"id": user_id, "name": f"User {user_id}", "plan": "pro"}


async def get_profile(user_id: int) -> dict[str, object]:
    """Return a user's profile, computing it at most once per TTL.

    `single_flight` means that if a hundred requests for a cold profile arrive
    together, the query runs once and the other ninety-nine wait for it, rather
    than a hundred identical queries hitting the database at the same moment.
    """
    return await cache.remember(
        f"profile:{user_id}",
        lambda: load_profile(user_id),
        ttl=60,
        single_flight=True,
    )


async def forget_profile(user_id: int) -> None:
    """Evict a profile, so the next read recomputes it."""
    await cache.forget(f"profile:{user_id}")


# -- the application -------------------------------------------------------


def build_app(config: CacheConfig | None = None) -> FastAPI:
    """Construct the application.

    Args:
        config: Cache configuration. Defaults to reading the environment, which
            is what a real deployment does; tests pass one explicitly.

    Returns:
        A configured application.
    """
    settings = config or CacheConfig.from_env()
    events = EventDispatcher()
    # Bounded: an unbounded list here would grow for the life of the process,
    # which is a memory leak in the very thing this endpoint demonstrates.
    seen: deque[CacheEvent] = deque(maxlen=RECENT_EVENTS)
    events.listen(CacheEvent, seen.append)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with cache_lifespan(settings, events):
            yield

    app = FastAPI(title="Keel cache example", lifespan=lifespan)

    @app.get("/users/{user_id}")
    async def read_user(user_id: int) -> dict[str, object]:
        return await get_profile(user_id)

    @app.delete("/users/{user_id}/cache", status_code=204)
    async def evict_user(user_id: int) -> None:
        await forget_profile(user_id)

    @app.get("/_cache/events")
    async def read_cache_events() -> list[str]:
        """What the Phase 5 request inspector will be built on.

        Every cache operation in the process is already observable, because the
        manager wraps its stores in the instrumentation decorator whenever a
        dispatcher is supplied.
        """
        return [f"{type(event).__name__} {getattr(event, 'key', '')}".strip() for event in seen]

    return app


app = build_app() if os.environ.get("CACHE_STORE") else None
"""Module-level app for `uvicorn examples.fastapi_app:app`, built only when the
environment is configured, so importing this module in a test is side-effect
free."""
