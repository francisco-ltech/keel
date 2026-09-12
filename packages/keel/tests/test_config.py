"""Cache configuration — defaults, validation, and reading the environment.

The validation is the interesting part: a misconfiguration should surface when
the configuration is built, not on the first cache miss in production.
"""

from __future__ import annotations

import pytest

from keel.cache.config import (
    DEFAULT_PREFIX,
    KNOWN_DRIVERS,
    SETTING_VARS,
    URL_VAR,
    CacheConfig,
    StoreConfig,
)
from keel.exceptions import ConfigurationError

# -- defaults -------------------------------------------------------------


def test_a_store_defaults_to_an_in_memory_json_cache() -> None:
    config = StoreConfig()
    assert config.driver == "array"
    assert config.prefix == ""
    assert config.ttl == 300.0
    assert config.url is None
    assert config.serializer == "json"


def test_an_unconfigured_cache_has_one_default_store() -> None:
    config = CacheConfig()
    assert config.default == "default"
    assert list(config.stores) == ["default"]
    assert config.store() == StoreConfig()


def test_each_cache_config_gets_its_own_stores_mapping() -> None:
    """A shared mutable default would leak configuration between applications."""
    assert CacheConfig().stores is not CacheConfig().stores


def test_the_shipped_driver_names() -> None:
    assert sorted(KNOWN_DRIVERS) == ["array", "null", "redis"]


def test_the_environment_names_are_not_vendor_prefixed() -> None:
    """The names belong to the deployment, not to Keel.

    ``REDIS_URL`` is what every hosting provider injects, and ``CACHE_*`` is
    what Laravel uses. A ``KEEL_`` prefix would mean re-mapping both in every
    deployment and would leave the variables misnamed if Keel were ever swapped
    out. Namespacing is still available — ``from_env(prefix=...)`` — it is just
    not imposed.
    """
    assert URL_VAR == "REDIS_URL"
    assert SETTING_VARS == "CACHE_"
    assert not URL_VAR.startswith("KEEL")
    assert not SETTING_VARS.startswith("KEEL")


# -- StoreConfig validation ----------------------------------------------


def test_an_unknown_serializer_is_rejected() -> None:
    with pytest.raises(ConfigurationError) as error:
        StoreConfig(serializer="yaml")
    message = str(error.value)
    assert "yaml" in message
    assert "json" in message
    assert "pickle" in message


@pytest.mark.parametrize("serializer", ["json", "pickle"])
def test_the_supported_serializers_are_accepted(serializer: str) -> None:
    assert StoreConfig(serializer=serializer).serializer == serializer


def test_a_redis_store_without_a_url_is_rejected() -> None:
    with pytest.raises(ConfigurationError) as error:
        StoreConfig(driver="redis")
    assert "redis" in str(error.value)
    assert "url" in str(error.value)


def test_a_redis_store_with_an_empty_url_is_rejected() -> None:
    with pytest.raises(ConfigurationError):
        StoreConfig(driver="redis", url="")


def test_a_redis_store_with_a_url_is_accepted() -> None:
    config = StoreConfig(driver="redis", url="redis://localhost:6379/0")
    assert config.url == "redis://localhost:6379/0"


def test_a_non_redis_store_needs_no_url() -> None:
    assert StoreConfig(driver="array").url is None


def test_an_unknown_driver_is_not_rejected_here() -> None:
    """Custom drivers are legitimate; the manager reports one it cannot build."""
    assert StoreConfig(driver="memcached").driver == "memcached"


def test_a_store_config_is_frozen() -> None:
    config = StoreConfig()
    with pytest.raises(AttributeError):
        config.driver = "null"  # type: ignore[misc]  # ty: ignore[invalid-assignment]


# -- CacheConfig validation ----------------------------------------------


def test_a_default_naming_a_missing_store_is_rejected() -> None:
    with pytest.raises(ConfigurationError) as error:
        CacheConfig(default="sessions", stores={"default": StoreConfig()})
    message = str(error.value)
    assert "'sessions'" in message
    assert "known stores: default" in message


def test_a_config_with_no_stores_at_all_is_rejected() -> None:
    with pytest.raises(ConfigurationError) as error:
        CacheConfig(stores={})
    assert "known stores: none" in str(error.value)


# -- store lookup ---------------------------------------------------------


def test_store_returns_the_named_configuration() -> None:
    sessions = StoreConfig(driver="null", prefix="sessions")
    config = CacheConfig(stores={"default": StoreConfig(), "sessions": sessions})
    assert config.store("sessions") is sessions


def test_store_without_a_name_returns_the_default() -> None:
    sessions = StoreConfig(prefix="sessions")
    config = CacheConfig(default="sessions", stores={"sessions": sessions})
    assert config.store() is sessions


def test_store_lists_the_known_names_when_one_is_missing() -> None:
    config = CacheConfig(
        stores={
            "default": StoreConfig(),
            "sessions": StoreConfig(),
            "ratelimit": StoreConfig(),
        }
    )
    with pytest.raises(ConfigurationError) as error:
        config.store("nope")
    message = str(error.value)
    assert "cache store 'nope' is not configured" in message
    assert "known stores: default, ratelimit, sessions" in message


# -- with_store -----------------------------------------------------------


def test_with_store_returns_a_new_config() -> None:
    original = CacheConfig()
    extended = original.with_store("sessions", StoreConfig(prefix="sessions"))

    assert extended is not original
    assert extended.store("sessions").prefix == "sessions"


def test_with_store_leaves_the_original_untouched() -> None:
    original = CacheConfig()
    original.with_store("sessions", StoreConfig())

    assert list(original.stores) == ["default"]
    with pytest.raises(ConfigurationError):
        original.store("sessions")


def test_with_store_replaces_an_existing_name() -> None:
    original = CacheConfig()
    replaced = original.with_store("default", StoreConfig(driver="null"))

    assert replaced.store().driver == "null"
    assert original.store().driver == "array"


def test_with_store_keeps_the_default_name() -> None:
    config = CacheConfig(default="sessions", stores={"sessions": StoreConfig()}).with_store(
        "other", StoreConfig()
    )
    assert config.default == "sessions"


# -- from_env -------------------------------------------------------------


def test_from_env_reads_every_variable() -> None:
    config = CacheConfig.from_env(
        {
            f"{SETTING_VARS}STORE": "redis",
            f"{SETTING_VARS}PREFIX": "keel:prod",
            URL_VAR: "redis://cache:6379/1",
            f"{SETTING_VARS}TTL": "900",
            f"{SETTING_VARS}SERIALIZER": "pickle",
        }
    )

    store = config.store()
    assert config.default == "default"
    assert store.driver == "redis"
    assert store.prefix == "keel:prod"
    assert store.url == "redis://cache:6379/1"
    assert store.ttl == 900.0
    assert store.serializer == "pickle"


def test_from_env_with_an_empty_mapping_gives_the_documented_defaults() -> None:
    config = CacheConfig.from_env({})
    store = config.store()

    assert list(config.stores) == ["default"]
    assert store.driver == "array"
    assert store.prefix == DEFAULT_PREFIX, "an unprefixed redis store can flush a whole server"
    assert store.url is None
    assert store.ttl == 300.0
    assert store.serializer == "json"


@pytest.mark.parametrize("spelling", ["none", "NONE", "None", "null", "NULL", ""])
def test_from_env_understands_the_forever_spellings(spelling: str) -> None:
    config = CacheConfig.from_env({f"{SETTING_VARS}TTL": spelling})
    assert config.store().ttl is None


@pytest.mark.parametrize("raw", ["0.5", "60", "-1"])
def test_from_env_parses_a_numeric_ttl(raw: str) -> None:
    assert CacheConfig.from_env({f"{SETTING_VARS}TTL": raw}).store().ttl == float(raw)


def test_from_env_rejects_a_non_numeric_ttl() -> None:
    with pytest.raises(ConfigurationError) as error:
        CacheConfig.from_env({f"{SETTING_VARS}TTL": "five minutes"})
    message = str(error.value)
    assert f"{SETTING_VARS}TTL" in message
    assert "'five minutes'" in message


def test_from_env_still_validates_the_store_it_builds() -> None:
    """A redis store from the environment needs a url like any other."""
    with pytest.raises(ConfigurationError) as error:
        CacheConfig.from_env({f"{SETTING_VARS}STORE": "redis"})
    assert "url" in str(error.value)


def test_from_env_reads_os_environ_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"{SETTING_VARS}STORE", "null")
    monkeypatch.setenv(f"{SETTING_VARS}TTL", "none")

    config = CacheConfig.from_env()

    assert config.store().driver == "null"
    assert config.store().ttl is None
