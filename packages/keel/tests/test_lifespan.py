"""``cache_lifespan`` — the one wiring call an application makes at startup.

Three promises: the facade works inside the block, the manager is closed on the
way out, and the binding is removed even when the body raises. The last one
matters because a leaked binding turns a failed startup into a process that
silently caches into a manager nobody owns.
"""

from __future__ import annotations

import pytest

from keel.cache import cache_lifespan
from keel.cache.config import CacheConfig, StoreConfig
from keel.cache.manager import CacheManager
from keel.cache.proxy import cache, current_cache_manager
from keel.cache.stores.array import ArrayStore
from keel.exceptions import ConfigurationError
from keel.support.events import EventDispatcher

pytestmark = [pytest.mark.anyio]


class ClosingStore(ArrayStore):
    """An array store that remembers whether it was closed."""

    def __init__(self) -> None:
        super().__init__()
        self.closes = 0

    async def close(self) -> None:
        self.closes += 1
        await super().close()


@pytest.fixture
def config() -> CacheConfig:
    return CacheConfig(
        default="default",
        stores={
            "default": StoreConfig(driver="array", ttl=60.0),
            "sessions": StoreConfig(driver="array", prefix="sessions"),
        },
    )


async def test_the_facade_works_inside_the_block(config: CacheConfig) -> None:
    async with cache_lifespan(config):
        assert await cache.put("key", "value") is True
        assert await cache.get("key") == "value"


async def test_the_manager_is_yielded_for_named_stores(config: CacheConfig) -> None:
    async with cache_lifespan(config) as manager:
        assert isinstance(manager, CacheManager)
        assert manager.config is config
        assert current_cache_manager() is manager

        sessions = manager.store("sessions")
        await sessions.put("key", "in-sessions")
        assert await cache.get("key") is None


async def test_the_dispatcher_is_passed_through(config: CacheConfig) -> None:
    events = EventDispatcher()
    async with cache_lifespan(config, events) as manager:
        assert manager.events is events


async def test_no_dispatcher_means_no_instrumentation(config: CacheConfig) -> None:
    async with cache_lifespan(config) as manager:
        assert manager.events is None


async def test_the_binding_is_removed_when_the_block_ends(config: CacheConfig) -> None:
    async with cache_lifespan(config):
        await cache.put("key", "value")

    with pytest.raises(ConfigurationError) as error:
        await cache.get("key")
    assert "no cache manager is bound" in str(error.value)


async def test_the_manager_is_closed_on_the_way_out() -> None:
    """Stores that own a connection pool leak it otherwise."""
    stores: list[ClosingStore] = []

    def factory(name: str, store_config: StoreConfig) -> ClosingStore:
        store = ClosingStore()
        stores.append(store)
        return store

    config = CacheConfig(stores={"default": StoreConfig(driver="custom")})
    async with cache_lifespan(config) as manager:
        manager.register_driver("custom", factory)
        await cache.put("key", "value")
        assert list(manager.resolved_names) == ["default"]

    assert [store.closes for store in stores] == [1]
    assert list(manager.resolved_names) == []


async def test_the_binding_is_removed_even_when_the_body_raises(config: CacheConfig) -> None:
    with pytest.raises(RuntimeError):
        async with cache_lifespan(config):
            await cache.put("key", "value")
            raise RuntimeError("startup failed")

    with pytest.raises(ConfigurationError):
        await cache.get("key")


async def test_the_manager_is_closed_even_when_the_body_raises() -> None:
    stores: list[ClosingStore] = []

    def factory(name: str, store_config: StoreConfig) -> ClosingStore:
        store = ClosingStore()
        stores.append(store)
        return store

    config = CacheConfig(stores={"default": StoreConfig(driver="custom")})
    with pytest.raises(RuntimeError):
        async with cache_lifespan(config) as manager:
            manager.register_driver("custom", factory)
            await cache.put("key", "value")
            raise RuntimeError("boom")

    assert [store.closes for store in stores] == [1]


async def test_the_error_escaping_the_block_is_the_original_one(config: CacheConfig) -> None:
    with pytest.raises(RuntimeError, match="startup failed"):
        async with cache_lifespan(config):
            raise RuntimeError("startup failed")


async def test_consecutive_lifespans_do_not_share_data(config: CacheConfig) -> None:
    async with cache_lifespan(config):
        await cache.put("key", "first")

    async with cache_lifespan(config):
        assert await cache.get("key") is None


async def test_a_nested_lifespan_restores_the_outer_binding(config: CacheConfig) -> None:
    """Nesting a test lifespan inside an application lifespan must be safe.

    An earlier version unbound on exit, which left the outer lifespan silently
    dead for the rest of the process — the kind of defect that only shows up as
    "the cache is unbound" somewhere unrelated.
    """
    async with cache_lifespan(config) as outer:
        await cache.put("key", "outer")

        async with cache_lifespan(config) as inner:
            assert current_cache_manager() is inner
            assert await cache.get("key") is None

        assert outer is not inner
        assert current_cache_manager() is outer
        assert await cache.get("key") == "outer"

    with pytest.raises(ConfigurationError):
        current_cache_manager()
