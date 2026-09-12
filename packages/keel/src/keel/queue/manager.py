"""Builds queue connections from configuration.

The Abstract Factory for the queue subsystem, and a direct reuse of
:class:`~keel.support.manager.Manager` — one of the few pieces ADR 0001 predicted
would transfer from the cache, and it did, unchanged.

What did *not* transfer is everything below it: no Bridge, no Store/Repository
split, no symmetrical consume contract. The factory generalises because
"resolve a named driver from configuration and memoise it" is genuinely the same
problem for any subsystem. The layers underneath do not, because a queue is not
shaped like a cache.
"""

from __future__ import annotations

from collections.abc import Callable

from keel.contracts.queue import Queue
from keel.exceptions import ConfigurationError
from keel.queue.config import KNOWN_DRIVERS, QueueConfig
from keel.queue.drivers import NullQueue, SyncQueue
from keel.queue.fake import FakeQueue
from keel.support.manager import Manager

type QueueFactory = Callable[[str, QueueConfig], Queue]
"""Builds a queue from its configured name and configuration."""


class QueueManager(Manager[Queue]):
    """Resolves named queue connections.

    Args:
        config: How to reach the queue.
    """

    __slots__ = ("_config", "_drivers")

    def __init__(self, config: QueueConfig) -> None:
        super().__init__(config.driver)
        self._config = config
        self._drivers: dict[str, QueueFactory] = {}

    @property
    def config(self) -> QueueConfig:
        """The configuration this manager builds from."""
        return self._config

    def register_driver(self, driver: str, factory: QueueFactory) -> None:
        """Teach this manager a driver it does not ship with.

        Args:
            driver: The value that will appear as ``QueueConfig.driver``.
            factory: Receives the name and configuration, returns a queue.
        """
        self._drivers[driver] = factory

    def connection(self, name: str | None = None) -> Queue:
        """Return the named queue connection.

        Args:
            name: The driver name, or ``None`` for the configured default.

        Returns:
            The memoised connection.

        Raises:
            ConfigurationError: If the driver is unknown.
        """
        return self.driver(name)

    def _make(self, name: str) -> Queue:
        """Build the connection for *name*.

        Args:
            name: The driver name.

        Returns:
            A queue.

        Raises:
            ConfigurationError: If the driver is not one this package ships and
                was not registered with :meth:`register_driver`.
        """
        registered = self._drivers.get(name)
        if registered is not None:
            return registered(name, self._config)

        match name:
            case "sync":
                return SyncQueue(name)
            case "null":
                return NullQueue(name)
            case "fake":
                return FakeQueue(name)
            case "saq":
                # Imported lazily so a service using the sync driver — or one
                # that only dispatches and never runs a worker — does not pay
                # for importing a worker runtime.
                from keel.queue.saq_driver import SaqQueue

                if not self._config.url:  # pragma: no cover — QueueConfig validates this
                    raise ConfigurationError("the saq queue driver needs a url")
                return SaqQueue.from_url(self._config.url, self._config)
            case unknown:
                known = ", ".join(sorted(KNOWN_DRIVERS))
                raise ConfigurationError(
                    f"unknown queue driver {unknown!r}; built-in drivers are {known}. "
                    f"Add your own with QueueManager.register_driver({unknown!r}, factory) "
                    f"before first use."
                )

    async def _close_instance(self, instance: Queue) -> None:
        """Release a connection's resources.

        Args:
            instance: The queue being discarded.
        """
        await instance.close()

    async def close(self) -> None:
        """Close every connection that has been built."""
        await self.close_all()


__all__ = ["QueueFactory", "QueueManager"]
