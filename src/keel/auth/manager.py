"""Builds token stores from configuration.

The Abstract Factory for the token subsystem, and the third reuse of
:class:`~keel.support.manager.Manager` unchanged — "resolve a named driver from
configuration and memoise it" is the one piece that has now transferred from the
cache to the queue to here without being re-derived.

What it buys: adding a Postgres-backed token store means writing an Adapter and
registering it, not editing this class. That is the Open/Closed principle with a
concrete cost attached, because the alternative is a service vendoring its own
copy of the facade to add one driver.

The Redis driver is imported inside :meth:`TokenManager._make` rather than at the
top. ``redis`` is a hard dependency of Keel, so this buys less than the queue's
lazy SAQ import does — but a memory-backed deployment still should not build a
connection pool class it never instantiates, and a manager whose imports vary by
driver is the shape every subsystem here uses.
"""

from __future__ import annotations

from collections.abc import Callable

from keel.auth.config import KNOWN_TOKEN_DRIVERS, TokenConfig
from keel.auth.fake import FakeTokenStore
from keel.auth.stores.memory import MemoryTokenStore
from keel.contracts.auth import TokenStore
from keel.exceptions import ConfigurationError
from keel.support.manager import Manager

type TokenStoreFactory = Callable[[str, TokenConfig], TokenStore]
"""Builds a token store from its configured name and configuration."""


class TokenManager(Manager[TokenStore]):
    """Resolves named token stores.

    Args:
        config: Where tokens live and how long they last.
    """

    __slots__ = ("_config", "_drivers")

    def __init__(self, config: TokenConfig) -> None:
        super().__init__(config.driver)
        self._config = config
        self._drivers: dict[str, TokenStoreFactory] = {}

    @property
    def config(self) -> TokenConfig:
        """The configuration this manager builds from."""
        return self._config

    def register_driver(self, driver: str, factory: TokenStoreFactory) -> None:
        """Teach this manager a driver it does not ship with.

        Args:
            driver: The value that will appear as ``TokenConfig.driver``.
            factory: Receives the name and configuration, returns a store.
        """
        self._drivers[driver] = factory

    def store(self, name: str | None = None) -> TokenStore:
        """Return the named token store.

        Args:
            name: The driver name, or ``None`` for the configured default.

        Returns:
            The memoised store.

        Raises:
            ConfigurationError: If the driver is unknown.
        """
        return self.driver(name)

    def _make(self, name: str) -> TokenStore:
        """Build the store for *name*.

        Args:
            name: The driver name.

        Returns:
            A token store.

        Raises:
            ConfigurationError: If the driver is not one this package ships and
                was not registered with :meth:`register_driver`.
        """
        registered = self._drivers.get(name)
        if registered is not None:
            return registered(name, self._config)

        match name:
            case "memory":
                return MemoryTokenStore(name, self._config.ttl)
            case "fake":
                return FakeTokenStore(name=name, ttl=self._config.ttl)
            case "redis":
                # Imported lazily so a memory-backed deployment does not pay for a
                # connection-pool implementation it never instantiates.
                from keel.auth.stores.redis import RedisTokenStore

                if not self._config.url:
                    raise ConfigurationError("the redis token store needs a url")
                return RedisTokenStore.from_url(self._config.url, self._config, name=name)
            case unknown:
                known = ", ".join(sorted(KNOWN_TOKEN_DRIVERS))
                raise ConfigurationError(
                    f"unknown token driver {unknown!r}; built-in drivers are {known}. "
                    f"Add your own with TokenManager.register_driver({unknown!r}, factory) "
                    f"before first use."
                )

    async def _close_instance(self, instance: TokenStore) -> None:
        """Release a store's resources.

        Args:
            instance: The store being discarded.
        """
        await instance.close()

    async def close(self) -> None:
        """Close every store that has been built."""
        await self.close_all()


__all__ = ["TokenManager", "TokenStoreFactory"]
