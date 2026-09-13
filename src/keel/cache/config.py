"""Cache configuration.

Plain dataclasses, deliberately. Keel is a library, so it declares the shape of
the configuration it needs and lets the application decide where those values
come from — environment, Pydantic settings, a YAML file, a test fixture.

Taking a hard dependency on ``pydantic-settings`` here would force that choice
on every consumer to save the application about six lines. :meth:`CacheConfig.from_env`
exists so the six lines are not needed either, without the dependency.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Final

from keel.exceptions import ConfigurationError

URL_VAR: Final = "REDIS_URL"
"""Where the Redis connection string is read from.

The name every hosting provider and client library already uses. Renaming it to
something Keel-specific would mean re-mapping it in every deployment.
"""

SETTING_VARS: Final = "CACHE_"
"""Prefix for the remaining knobs: ``CACHE_STORE``, ``CACHE_PREFIX``, ``CACHE_TTL``.

Named after the subsystem rather than the framework, matching Laravel's
``CACHE_STORE`` / ``CACHE_PREFIX`` and, more importantly, keeping the variables
meaningful if Keel is ever replaced.
"""

DEFAULT_PREFIX: Final = "keel"
"""Namespace applied when the environment does not specify one.

Not the empty string: an unnamespaced network store owns the server's entire
keyspace, which makes ``flush()`` a destructive operation against anything else
sharing it."""

KNOWN_DRIVERS: Final = frozenset({"redis", "array", "null"})
"""Drivers this package ships.

Others are valid once registered via
:meth:`~keel.cache.manager.CacheManager.register_driver`, so this set is used
for error messages rather than validation."""


@dataclass(frozen=True, slots=True)
class StoreConfig:
    """How to build one named store.

    Attributes:
        driver: Which backend to use. ``redis``, ``array`` or ``null`` are built
            in; any other name must have been registered with
            :meth:`~keel.cache.manager.CacheManager.register_driver`.
        prefix: The key namespace this store owns. Strongly recommended for
            shared backends — it bounds what ``flush()`` can destroy.
        ttl: The lifetime applied when a caller does not specify one. ``None``
            means entries live indefinitely by default.
        url: Connection URL, for drivers that need one.
        serializer: ``json`` (safe, cross-language, supports atomic increment)
            or ``pickle`` (arbitrary objects, trusted writers only).
    """

    driver: str = "array"
    prefix: str = ""
    ttl: float | None = 300.0
    url: str | None = None
    serializer: str = "json"

    def __post_init__(self) -> None:
        """Reject configuration that cannot possibly work, at load time.

        Raises:
            ConfigurationError: If the serializer is unknown or a Redis store
                has no URL. Catching this at startup beats catching it on the
                first cache miss in production.
        """
        if self.serializer not in {"json", "pickle"}:
            raise ConfigurationError(
                f"unknown serializer {self.serializer!r}; expected 'json' or 'pickle'"
            )
        if self.driver == "redis" and not self.url:
            raise ConfigurationError("the redis cache driver requires a 'url'")


def _default_stores() -> dict[str, StoreConfig]:
    """The single in-memory store an unconfigured application gets."""
    return {"default": StoreConfig()}


@dataclass(frozen=True, slots=True)
class CacheConfig:
    """The cache subsystem's configuration.

    Attributes:
        default: The store name used when a caller does not specify one.
        stores: Every configured store, by name. Multiple stores are normal —
            a short-lived ``default`` alongside a long-lived ``sessions``, say —
            and each gets its own namespace and TTL.
    """

    default: str = "default"
    stores: Mapping[str, StoreConfig] = field(default_factory=_default_stores)

    def __post_init__(self) -> None:
        """Verify the default store exists.

        Raises:
            ConfigurationError: If ``default`` names a store that is not
                configured.
        """
        if self.default not in self.stores:
            known = ", ".join(sorted(self.stores)) or "none"
            raise ConfigurationError(
                f"default cache store {self.default!r} is not configured; known stores: {known}"
            )

    def store(self, name: str | None = None) -> StoreConfig:
        """Return one store's configuration.

        Args:
            name: The store name, or ``None`` for the default.

        Returns:
            The store's configuration.

        Raises:
            ConfigurationError: If the store is not configured.
        """
        resolved = name or self.default
        try:
            return self.stores[resolved]
        except KeyError as exc:
            known = ", ".join(sorted(self.stores)) or "none"
            raise ConfigurationError(
                f"cache store {resolved!r} is not configured; known stores: {known}"
            ) from exc

    def with_store(self, name: str, config: StoreConfig) -> CacheConfig:
        """Return a copy with one store added or replaced.

        Args:
            name: The store name.
            config: Its configuration.

        Returns:
            A new config; the receiver is unchanged.
        """
        return replace(self, stores={**self.stores, name: config})

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        prefix: str = "",
    ) -> CacheConfig:
        """Build a single-store configuration from environment variables.

        Reads ``CACHE_STORE``, ``CACHE_PREFIX``, ``CACHE_TTL``,
        ``CACHE_SERIALIZER`` and ``REDIS_URL``.

        Args:
            env: The mapping to read; defaults to :data:`os.environ`.
            prefix: Prepended to every variable name, for an application that
                wants its own namespace. Empty by default — these names belong
                to the deployment, not to Keel.

        Returns:
            A configuration with one store named ``default``.

        Raises:
            ConfigurationError: If the TTL is not a number.
        """
        source = os.environ if env is None else env
        raw_ttl = source.get(f"{prefix}{SETTING_VARS}TTL", "300")
        try:
            ttl = None if raw_ttl.lower() in {"", "none", "null"} else float(raw_ttl)
        except ValueError as exc:
            raise ConfigurationError(
                f"{prefix}{SETTING_VARS}TTL must be a number of seconds or 'none', got {raw_ttl!r}"
            ) from exc

        return cls(
            default="default",
            stores={
                "default": StoreConfig(
                    driver=source.get(f"{prefix}{SETTING_VARS}STORE", "array"),
                    prefix=source.get(f"{prefix}{SETTING_VARS}PREFIX", DEFAULT_PREFIX),
                    ttl=ttl,
                    url=source.get(f"{prefix}{URL_VAR}"),
                    serializer=source.get(f"{prefix}{SETTING_VARS}SERIALIZER", "json"),
                )
            },
        )
