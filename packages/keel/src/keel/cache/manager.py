"""Builds cache repositories from configuration.

The Abstract Factory for the cache subsystem. It owns the mapping from a driver
name in configuration to an assembled object graph — store, optional
instrumentation wrapper, repository — and memoises the result so that asking for
the same store twice returns the same connection pool rather than a second one.

Assembly lives here and nowhere else. Application code asks for ``"sessions"``
and gets something satisfying the contract; it never learns whether that is
Redis, whether it is instrumented, or how its keys are namespaced.
"""

from __future__ import annotations

from collections.abc import Callable

from keel.cache.config import KNOWN_DRIVERS, CacheConfig, StoreConfig
from keel.cache.repository import Repository
from keel.cache.stores.array import ArrayStore
from keel.cache.stores.eventful import EventfulStore
from keel.cache.stores.null import NullStore
from keel.contracts.cache import Store
from keel.exceptions import ConfigurationError
from keel.support.events import EventDispatcher
from keel.support.keys import KeyNamespace
from keel.support.manager import Manager
from keel.support.serialization import JsonSerializer, PickleSerializer, Serializer

type DriverFactory = Callable[[str, StoreConfig], Store]
"""Builds a store from its configured name and configuration."""


class CacheManager(Manager[Repository]):
    """Resolves named cache repositories.

    Args:
        config: The cache configuration.
        events: When provided, every store is wrapped in
            :class:`~keel.cache.stores.eventful.EventfulStore` so operations are
            observable. Pass ``None`` to disable instrumentation outright —
            wrapping is decided once, at build time, so listeners registered
            later still work but instrumentation cannot be switched on later
            without :meth:`~keel.support.manager.Manager.reset`.
    """

    __slots__ = ("_config", "_drivers", "_events")

    def __init__(self, config: CacheConfig, events: EventDispatcher | None = None) -> None:
        super().__init__(config.default)
        self._config = config
        self._events = events
        self._drivers: dict[str, DriverFactory] = {}

    def register_driver(self, driver: str, factory: DriverFactory) -> None:
        """Teach this manager how to build a driver it does not ship with.

        Two extension points exist and they answer different questions.
        :meth:`~keel.support.manager.Manager.extend` replaces one *configured
        store* wholesale — that is how the test fake is installed. This method
        adds a new *driver type*, so any number of stores can then be configured
        with ``driver="memcached"``.

        Args:
            driver: The value that will appear as ``StoreConfig.driver``.
            factory: Receives the store's name and configuration and returns a
                store.
        """
        self._drivers[driver] = factory

    @property
    def config(self) -> CacheConfig:
        """The configuration this manager builds from."""
        return self._config

    @property
    def events(self) -> EventDispatcher | None:
        """The dispatcher stores are instrumented with, if any."""
        return self._events

    def store(self, name: str | None = None) -> Repository:
        """Return the named cache repository.

        An alias for :meth:`~keel.support.manager.Manager.driver` that reads
        naturally at call sites: ``cache.store("sessions")``.

        Args:
            name: The store name, or ``None`` for the default.

        Returns:
            The memoised repository.

        Raises:
            ConfigurationError: If the store is not configured.
        """
        return self.driver(name)

    def _make(self, name: str) -> Repository:
        """Assemble the repository for *name*.

        Args:
            name: The store name.

        Returns:
            A repository wrapping a freshly built store.

        Raises:
            ConfigurationError: If the store or its driver is unknown.
        """
        config = self._config.store(name)
        store = self._make_store(name, config)
        if self._events is not None:
            store = EventfulStore(store, self._events, name)
        return Repository(store, config.ttl, name)

    def _make_store(self, name: str, config: StoreConfig) -> Store:
        """Build the backend for one configured store.

        Args:
            name: The store name, used in error messages.
            config: Its configuration.

        Returns:
            An unwrapped store.

        Raises:
            ConfigurationError: If the driver is not one this package ships and
                was not registered with :meth:`register_driver`.
        """
        namespace = KeyNamespace(config.prefix)

        registered = self._drivers.get(config.driver)
        if registered is not None:
            return registered(name, config)

        match config.driver:
            case "array":
                return ArrayStore(namespace, self._serializer(config))
            case "null":
                return NullStore(namespace)
            case "redis":
                # Imported lazily so that an application using only the array
                # store never pays for importing the Redis client.
                from keel.cache.stores.redis import RedisStore

                if not config.url:
                    # Unreachable: StoreConfig rejects a urlless redis store at
                    # construction. Kept because the type is `str | None` and a
                    # future driver factory could bypass that validation.
                    raise ConfigurationError(  # pragma: no cover
                        f"cache store {name!r} needs a redis url"
                    )
                return RedisStore.from_url(config.url, namespace, self._serializer(config))
            case unknown:
                known = ", ".join(sorted(KNOWN_DRIVERS))
                raise ConfigurationError(
                    f"cache store {name!r} uses unknown driver {unknown!r}; "
                    f"built-in drivers are {known}. Add your own with "
                    f"CacheManager.register_driver({unknown!r}, factory) before "
                    f"first use — note that extend() replaces a single configured "
                    f"store, which is a different thing."
                )

    @staticmethod
    def _serializer(config: StoreConfig) -> Serializer:
        """Return the encoding strategy named by *config*."""
        return PickleSerializer() if config.serializer == "pickle" else JsonSerializer()

    async def _close_instance(self, instance: Repository) -> None:
        """Release a repository's underlying store.

        Args:
            instance: The repository being discarded.
        """
        await instance.close()

    async def close(self) -> None:
        """Close every store that has been built, then forget them all.

        Call this from the application's shutdown hook. Stores that own a
        connection pool — Redis does — leak it otherwise.
        """
        await self.close_all()
