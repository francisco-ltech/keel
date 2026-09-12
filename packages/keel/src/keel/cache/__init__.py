"""The cache subsystem.

The public surface is small on purpose. Most application code needs exactly one
import::

    from keel.cache import cache

    await cache.put("user:42", payload, ttl=60)
    profile = await cache.remember("profile:42", load_profile, ttl=300)

Wiring happens once, at startup::

    from keel.cache import CacheConfig, cache_lifespan

    async with cache_lifespan(CacheConfig.from_env()):
        ...

and tests replace the whole subsystem with one line::

    from keel.testing import fake_cache

    with fake_cache() as cached:
        await handler()
        cached.assert_put("profile:42")

Everything else in this package is machinery those three usages rest on.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from keel.cache.config import CacheConfig, StoreConfig
from keel.cache.events import (
    CacheEvent,
    CacheFlushed,
    CacheHit,
    CacheMissed,
    CounterIncremented,
    KeyForgotten,
    KeyWritten,
)
from keel.cache.fake import CacheAssertionError, FakeStore, Operation
from keel.cache.lock import NullLock, StoreLock
from keel.cache.manager import CacheManager
from keel.cache.proxy import (
    CacheProxy,
    _bound_cache_manager,
    cache,
    current_cache_manager,
    set_cache_manager,
    use_cache,
)
from keel.cache.repository import Repository, TTLInput
from keel.cache.stores.array import ArrayStore
from keel.cache.stores.eventful import EventfulStore
from keel.cache.stores.null import NullStore
from keel.contracts.cache import Lock, Store
from keel.support.events import EventDispatcher
from keel.support.sentinels import MISSING, UNSET, is_missing, is_present


@asynccontextmanager
async def cache_lifespan(
    config: CacheConfig,
    events: EventDispatcher | None = None,
) -> AsyncIterator[CacheManager]:
    """Bind a cache manager for the life of the application.

    Framework-agnostic by design — it is an async context manager, so it drops
    into a FastAPI ``lifespan``, an ASGI server, a worker process or a script
    without Keel needing to know which.

    Closing matters: stores that own a connection pool leak it otherwise, and a
    leaked pool is the kind of failure that only shows up after a few hundred
    restarts.

    Args:
        config: The cache configuration.
        events: A dispatcher to instrument stores with, or ``None`` for no
            instrumentation.

    Yields:
        The bound manager, for code that needs named stores directly.
    """
    previous = _bound_cache_manager()
    manager = CacheManager(config, events)
    set_cache_manager(manager)
    try:
        yield manager
    finally:
        await manager.close()
        # Restore rather than unbind: these lifespans nest, and unbinding would
        # leave the outer manager silently dead for the rest of the process.
        set_cache_manager(previous)


__all__ = [
    "MISSING",
    "UNSET",
    "ArrayStore",
    "CacheAssertionError",
    "CacheConfig",
    "CacheEvent",
    "CacheFlushed",
    "CacheHit",
    "CacheManager",
    "CacheMissed",
    "CacheProxy",
    "CounterIncremented",
    "EventDispatcher",
    "EventfulStore",
    "FakeStore",
    "KeyForgotten",
    "KeyWritten",
    "Lock",
    "NullLock",
    "NullStore",
    "Operation",
    "Repository",
    "Store",
    "StoreConfig",
    "StoreLock",
    "TTLInput",
    "cache",
    "cache_lifespan",
    "current_cache_manager",
    "is_missing",
    "is_present",
    "set_cache_manager",
    "use_cache",
]
