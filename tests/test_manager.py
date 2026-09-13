"""Driver resolution — ``CacheManager`` and the generic ``Manager`` beneath it.

Covers memoisation, the two extension points (``extend`` for a configured store,
``register_driver`` for a driver type), the error messages a misconfiguration
produces, and the lifecycle methods a test suite or shutdown hook relies on.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from keel.cache.config import CacheConfig, StoreConfig
from keel.cache.manager import CacheManager
from keel.cache.repository import Repository
from keel.cache.stores.array import ArrayStore
from keel.cache.stores.eventful import EventfulStore
from keel.cache.stores.null import NullStore
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
            "sessions": StoreConfig(driver="array", prefix="sessions", ttl=None),
            "disabled": StoreConfig(driver="null"),
        },
    )


@pytest.fixture
def manager(config: CacheConfig) -> CacheManager:
    return CacheManager(config)


# -- resolution -----------------------------------------------------------


def test_store_returns_a_repository(manager: CacheManager) -> None:
    assert isinstance(manager.store("default"), Repository)


def test_store_carries_the_configured_name_and_ttl(manager: CacheManager) -> None:
    sessions = manager.store("sessions")
    assert sessions.name == "sessions"
    assert sessions.default_ttl is None
    assert manager.store("default").default_ttl == 60.0


def test_the_same_name_returns_the_same_instance(manager: CacheManager) -> None:
    """Memoisation is the point: a second call must not open a second pool."""
    assert manager.store("default") is manager.store("default")


def test_different_names_return_different_instances(manager: CacheManager) -> None:
    assert manager.store("default") is not manager.store("sessions")


def test_store_without_a_name_resolves_the_configured_default(manager: CacheManager) -> None:
    assert manager.store() is manager.store("default")
    assert manager.default_name == "default"


def test_a_non_default_default_is_honoured() -> None:
    config = CacheConfig(
        default="sessions",
        stores={"default": StoreConfig(), "sessions": StoreConfig(prefix="sessions")},
    )
    manager = CacheManager(config)
    assert manager.store().name == "sessions"


def test_the_driver_named_in_configuration_is_the_one_built(manager: CacheManager) -> None:
    assert isinstance(manager.store("default").store, ArrayStore)
    assert isinstance(manager.store("disabled").store, NullStore)


def test_the_configured_prefix_becomes_the_store_namespace(manager: CacheManager) -> None:
    assert manager.store("sessions").store.namespace.prefix == "sessions"


async def test_the_pickle_serializer_is_selected_by_configuration() -> None:
    """The only externally visible difference: what the store can hold."""
    config = CacheConfig(stores={"default": StoreConfig(driver="array", serializer="pickle")})
    repository = CacheManager(config).store()
    moment = datetime(2026, 1, 1, tzinfo=UTC)
    await repository.put("when", moment)
    assert await repository.get("when") == moment


def test_config_and_events_are_exposed(config: CacheConfig, manager: CacheManager) -> None:
    assert manager.config is config
    assert manager.events is None


# -- misconfiguration -----------------------------------------------------


def test_an_unknown_store_name_raises_and_lists_the_known_ones(manager: CacheManager) -> None:
    with pytest.raises(ConfigurationError) as error:
        manager.store("nope")
    message = str(error.value)
    assert "'nope'" in message
    assert "default" in message
    assert "sessions" in message


def test_an_unknown_driver_raises_and_names_the_built_in_drivers() -> None:
    config = CacheConfig(stores={"default": StoreConfig(driver="memcached")})
    with pytest.raises(ConfigurationError) as error:
        CacheManager(config).store()
    message = str(error.value)
    assert "memcached" in message
    assert "array" in message
    assert "null" in message
    assert "redis" in message


def test_a_failed_resolution_is_not_memoised(manager: CacheManager) -> None:
    with pytest.raises(ConfigurationError):
        manager.store("nope")
    assert list(manager.resolved_names) == []


# -- extension ------------------------------------------------------------


def test_register_driver_makes_a_custom_driver_configurable() -> None:
    config = CacheConfig(stores={"default": StoreConfig(driver="custom", ttl=15.0)})
    manager = CacheManager(config)
    built: list[str] = []

    def factory(name: str, store_config: StoreConfig) -> ArrayStore:
        built.append(name)
        return ArrayStore()

    manager.register_driver("custom", factory)
    repository = manager.store()

    assert built == ["default"]
    assert isinstance(repository.store, ArrayStore)
    assert repository.default_ttl == 15.0


def test_a_registered_driver_receives_its_store_configuration() -> None:
    store_config = StoreConfig(driver="custom", prefix="wherever")
    manager = CacheManager(CacheConfig(stores={"default": store_config}))
    seen: list[StoreConfig] = []

    def factory(name: str, received: StoreConfig) -> ArrayStore:
        seen.append(received)
        return ArrayStore()

    manager.register_driver("custom", factory)
    manager.store()
    assert seen == [store_config]


def test_a_registered_driver_wins_over_a_built_in_name() -> None:
    """Registration is how an application overrides a shipped driver."""
    manager = CacheManager(CacheConfig(stores={"default": StoreConfig(driver="array")}))
    manager.register_driver("array", lambda name, config: NullStore())
    assert isinstance(manager.store().store, NullStore)


def test_extend_replaces_a_named_store_wholesale(manager: CacheManager) -> None:
    replacement = Repository(NullStore(), 1.0, "default")
    manager.extend("default", lambda name: replacement)
    assert manager.store("default") is replacement
    assert manager.store("sessions") is not replacement


def test_extend_passes_the_resolved_name_to_the_factory(manager: CacheManager) -> None:
    names: list[str] = []

    def factory(name: str) -> Repository:
        names.append(name)
        return Repository(NullStore(), 1.0, name)

    manager.extend("sessions", factory)
    manager.store("sessions")
    assert names == ["sessions"]


def test_extending_an_already_resolved_name_raises(manager: CacheManager) -> None:
    """Callers may be holding the old instance, so a silent swap would lie."""
    manager.store("default")
    with pytest.raises(ConfigurationError) as error:
        manager.extend("default", lambda name: Repository(NullStore(), 1.0, name))
    assert "already been resolved" in str(error.value)


def test_extend_is_allowed_again_after_forgetting_the_name(manager: CacheManager) -> None:
    manager.store("default")
    manager.forget("default")
    replacement = Repository(NullStore(), 1.0, "default")
    manager.extend("default", lambda name: replacement)
    assert manager.store("default") is replacement


# -- lifecycle ------------------------------------------------------------


def test_resolved_names_reflects_only_what_has_been_built(manager: CacheManager) -> None:
    assert list(manager.resolved_names) == []
    manager.store("sessions")
    assert list(manager.resolved_names) == ["sessions"]
    manager.store("default")
    assert sorted(manager.resolved_names) == ["default", "sessions"]


def test_resolved_names_is_a_snapshot_not_a_live_view(manager: CacheManager) -> None:
    names = manager.resolved_names
    manager.store("default")
    assert list(names) == []


def test_forget_causes_the_next_call_to_rebuild(manager: CacheManager) -> None:
    first = manager.store("default")
    manager.forget("default")
    assert list(manager.resolved_names) == []
    assert manager.store("default") is not first


def test_forget_without_a_name_forgets_the_default(manager: CacheManager) -> None:
    first = manager.store()
    manager.forget()
    assert manager.store() is not first


def test_forgetting_an_unresolved_name_is_a_no_op(manager: CacheManager) -> None:
    manager.forget("sessions")
    manager.forget("never-configured")
    assert list(manager.resolved_names) == []


def test_reset_rebuilds_every_store(manager: CacheManager) -> None:
    first = manager.store("default")
    second = manager.store("sessions")
    manager.reset()
    assert list(manager.resolved_names) == []
    assert manager.store("default") is not first
    assert manager.store("sessions") is not second


async def test_close_closes_every_built_store_and_forgets_them() -> None:
    config = CacheConfig(stores={"default": StoreConfig(driver="custom")})
    manager = CacheManager(config)
    stores: list[ClosingStore] = []

    def factory(name: str, store_config: StoreConfig) -> ClosingStore:
        store = ClosingStore()
        stores.append(store)
        return store

    manager.register_driver("custom", factory)
    manager.store()

    await manager.close()

    assert [store.closes for store in stores] == [1]
    assert list(manager.resolved_names) == []


async def test_close_does_not_build_stores_that_were_never_used() -> None:
    config = CacheConfig(
        stores={"default": StoreConfig(driver="custom"), "other": StoreConfig(driver="custom")}
    )
    manager = CacheManager(config)
    built: list[str] = []

    def factory(name: str, store_config: StoreConfig) -> ArrayStore:
        built.append(name)
        return ArrayStore()

    manager.register_driver("custom", factory)
    manager.store("default")
    await manager.close()

    assert built == ["default"]


async def test_close_on_an_untouched_manager_is_harmless(manager: CacheManager) -> None:
    await manager.close()
    assert list(manager.resolved_names) == []


# -- instrumentation ------------------------------------------------------


def test_an_event_dispatcher_wraps_every_store(config: CacheConfig) -> None:
    manager = CacheManager(config, EventDispatcher())
    assert isinstance(manager.store("default").store, EventfulStore)
    assert isinstance(manager.store("disabled").store, EventfulStore)


def test_the_wrapper_keeps_the_configured_store_underneath(config: CacheConfig) -> None:
    manager = CacheManager(config, EventDispatcher())
    wrapper = manager.store("disabled").store
    assert isinstance(wrapper, EventfulStore)
    assert isinstance(wrapper.inner, NullStore)


def test_without_a_dispatcher_stores_are_left_bare(manager: CacheManager) -> None:
    assert not isinstance(manager.store("default").store, EventfulStore)


def test_a_registered_driver_is_wrapped_too(config: CacheConfig) -> None:
    events = EventDispatcher()
    wrapped_config = config.with_store("custom", StoreConfig(driver="custom"))
    manager = CacheManager(wrapped_config, events)
    manager.register_driver("custom", lambda name, store_config: ArrayStore())
    assert isinstance(manager.store("custom").store, EventfulStore)
    assert manager.events is events


# -- the redis branch -----------------------------------------------------


@pytest.mark.redis
async def test_the_redis_driver_is_assembled_from_a_url(redis_url: str) -> None:
    from keel.cache.stores.redis import RedisStore

    config = CacheConfig(
        stores={"default": StoreConfig(driver="redis", url=redis_url, prefix="keel-manager-test")}
    )
    manager = CacheManager(config)
    repository = manager.store()
    assert isinstance(repository.store, RedisStore)
    assert repository.store.namespace.prefix == "keel-manager-test"
    await manager.close()
