"""The module-level ``cache`` facade and the binding behind it.

Covers the unbound error, the two binding layers (process-wide default and
context-local override), delegation of the Repository properties, and the
property that makes the facade safe to import at module scope: every call
re-resolves, so a rebind takes effect on the very next operation.
"""

from __future__ import annotations

import pytest

from keel.cache.config import CacheConfig, StoreConfig
from keel.cache.manager import CacheManager
from keel.cache.proxy import (
    CacheProxy,
    cache,
    current_cache_manager,
    set_cache_manager,
    use_cache,
)
from keel.cache.repository import Repository
from keel.cache.stores.array import ArrayStore
from keel.cache.stores.null import NullStore
from keel.exceptions import ConfigurationError

pytestmark = [pytest.mark.anyio]


def build_manager(default_ttl: float | None = 60.0) -> CacheManager:
    return CacheManager(
        CacheConfig(
            default="default",
            stores={
                "default": StoreConfig(driver="array", ttl=default_ttl),
                "sessions": StoreConfig(driver="array", prefix="sessions", ttl=None),
            },
        )
    )


# -- unbound --------------------------------------------------------------


async def test_using_the_facade_with_nothing_bound_raises() -> None:
    with pytest.raises(ConfigurationError) as error:
        await cache.get("key")
    message = str(error.value)
    assert "no cache manager is bound" in message
    assert "set_cache_manager" in message
    assert "use_cache" in message


def test_current_cache_manager_raises_when_nothing_is_bound() -> None:
    with pytest.raises(ConfigurationError):
        current_cache_manager()


def test_repr_says_so_when_unbound() -> None:
    assert repr(cache) == "<CacheProxy unbound>"


# -- the process-wide binding --------------------------------------------


async def test_set_cache_manager_makes_the_facade_work_end_to_end() -> None:
    set_cache_manager(build_manager())

    assert await cache.put("user:42", {"name": "Ada"}) is True
    assert await cache.get("user:42") == {"name": "Ada"}
    assert await cache.has("user:42") is True
    assert await cache.forget("user:42") is True
    assert await cache.get("user:42") is None


async def test_the_facade_supports_the_whole_repository_api() -> None:
    """Nothing is forwarded by hand, so this is really a test of the Bridge."""
    set_cache_manager(build_manager())

    await cache.put_many({"a": 1, "b": 2})
    assert await cache.many(["a", "b"]) == {"a": 1, "b": 2}
    assert await cache.increment("hits", 3) == 3
    assert await cache.decrement("hits") == 2
    assert await cache.pull("a") == 1
    assert await cache.missing("a") is True
    assert await cache.remember("lazy", lambda: "computed") == "computed"
    assert await cache.add("lazy", "other") is False
    assert await cache.forever("permanent", 1) is True
    assert await cache.flush() is True
    assert await cache.has("permanent") is False


def test_set_cache_manager_none_unbinds() -> None:
    set_cache_manager(build_manager())
    assert current_cache_manager() is not None
    set_cache_manager(None)
    with pytest.raises(ConfigurationError):
        current_cache_manager()


def test_repr_shows_the_repository_it_resolves_to() -> None:
    set_cache_manager(build_manager())
    assert repr(cache) == "<CacheProxy -> <Repository 'default' store=ArrayStore>>"


# -- the context-local override ------------------------------------------


async def test_use_cache_overrides_for_the_block_and_restores_afterwards() -> None:
    default = build_manager()
    set_cache_manager(default)
    await cache.put("key", "from-default")

    override = build_manager()
    with use_cache(override) as yielded:
        assert yielded is override
        assert current_cache_manager() is override
        assert await cache.get("key") is None
        await cache.put("key", "from-override")

    assert current_cache_manager() is default
    assert await cache.get("key") == "from-default"


async def test_the_override_wins_over_the_process_wide_default() -> None:
    set_cache_manager(build_manager())
    override = build_manager()
    with use_cache(override):
        await cache.put("only-here", "value")
        assert current_cache_manager() is override

    assert await cache.get("only-here") is None
    assert await override.store().get("only-here") == "value"


def test_use_cache_works_with_nothing_bound_underneath() -> None:
    override = build_manager()
    with use_cache(override):
        assert current_cache_manager() is override
    with pytest.raises(ConfigurationError):
        current_cache_manager()


def test_use_cache_restores_the_previous_override_when_nested() -> None:
    outer = build_manager()
    inner = build_manager()
    with use_cache(outer):
        with use_cache(inner):
            assert current_cache_manager() is inner
        assert current_cache_manager() is outer


def test_use_cache_restores_even_when_the_block_raises() -> None:
    default = build_manager()
    set_cache_manager(default)
    with pytest.raises(RuntimeError), use_cache(build_manager()):
        raise RuntimeError("boom")
    assert current_cache_manager() is default


# -- delegation -----------------------------------------------------------


def test_the_proxy_delegates_store_ttl_and_name_to_the_bound_repository() -> None:
    manager = build_manager(default_ttl=90.0)
    set_cache_manager(manager)
    subject = manager.store()

    assert cache.subject is subject
    assert cache.store is subject.store
    assert cache.default_ttl == 90.0
    assert cache.name == "default"


def test_the_delegated_properties_follow_a_rebind() -> None:
    set_cache_manager(build_manager(default_ttl=10.0))
    assert cache.default_ttl == 10.0
    set_cache_manager(build_manager(default_ttl=20.0))
    assert cache.default_ttl == 20.0


async def test_of_returns_a_specific_named_store() -> None:
    set_cache_manager(build_manager())

    sessions = cache.of("sessions")
    assert isinstance(sessions, Repository)
    assert sessions.name == "sessions"
    assert sessions.default_ttl is None

    await sessions.put("key", "in-sessions")
    assert await cache.get("key") is None
    assert await sessions.get("key") == "in-sessions"


def test_of_raises_for_an_unknown_store() -> None:
    set_cache_manager(build_manager())
    with pytest.raises(ConfigurationError):
        cache.of("nope")


async def test_a_proxy_can_be_built_over_any_resolver() -> None:
    """The proxy is not tied to the global binding; it takes a resolver."""
    target = Repository(ArrayStore(), 5.0, "explicit")
    proxy = CacheProxy(lambda: target)

    assert proxy.name == "explicit"
    assert proxy.default_ttl == 5.0
    await proxy.put("key", "value")
    assert await target.get("key") == "value"


# -- the point of the whole design ---------------------------------------


async def test_a_rebind_takes_effect_on_the_next_call_through_the_same_object() -> None:
    """The imported ``cache`` object is never re-imported, so this must hold.

    If the proxy resolved once and cached the result, a test that swapped the
    manager would silently keep writing to the previous one.
    """
    first = build_manager()
    set_cache_manager(first)
    await cache.put("key", "written-to-first")
    assert await cache.get("key") == "written-to-first"

    second = build_manager()
    set_cache_manager(second)

    assert await cache.get("key") is None
    assert await first.store().get("key") == "written-to-first"


async def test_a_rebind_reaches_a_store_captured_before_it() -> None:
    """A repository handed out earlier keeps pointing at its own manager."""
    first = build_manager()
    set_cache_manager(first)
    captured = cache.of("default")

    set_cache_manager(build_manager())
    await captured.put("key", "old-manager")

    assert await cache.get("key") is None
    assert await first.store().get("key") == "old-manager"


async def test_the_facade_sees_a_store_replaced_by_extend() -> None:
    manager = build_manager()
    set_cache_manager(manager)
    manager.extend("default", lambda name: Repository(NullStore(), None, name))

    assert await cache.put("key", "value") is True
    assert await cache.get("key") is None
