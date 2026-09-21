"""Builds mailers from configuration.

The Abstract Factory for the mail subsystem, and another reuse of
:class:`~keel.support.manager.Manager` unchanged. What it buys is the same as
everywhere else: a provider's API client is an Adapter registered under a
name, not an edit to this class.

Unlike the cache's and the queue's, the networked driver is imported at the
top: ``smtplib`` is the standard library, so there is no optional dependency
to keep out of a process that never sends, and :mod:`keel.mail` exports
:class:`SmtpMailer` anyway.
"""

from __future__ import annotations

from collections.abc import Callable

from keel.contracts.mail import Mailer
from keel.exceptions import ConfigurationError
from keel.mail.config import KNOWN_DRIVERS, MailConfig
from keel.mail.drivers import LogMailer, NullMailer
from keel.mail.fake import FakeMailer
from keel.mail.smtp_driver import SmtpMailer
from keel.support.events import EventDispatcher
from keel.support.manager import Manager

type MailerFactory = Callable[[str, MailConfig], Mailer]
"""Builds a mailer from its configured name and configuration."""


class MailManager(Manager[Mailer]):
    """Resolves named mailers.

    Args:
        config: How mail leaves the process.
        events: Where :func:`~keel.mail.binding.send` announces each delivery,
            or ``None`` for no announcement. The same seam the cache and the
            queue have, and what the request inspector and metrics read.
    """

    __slots__ = ("_config", "_drivers", "_events")

    def __init__(self, config: MailConfig, events: EventDispatcher | None = None) -> None:
        super().__init__(config.driver)
        self._config = config
        self._events = events
        self._drivers: dict[str, MailerFactory] = {}

    @property
    def config(self) -> MailConfig:
        """The configuration this manager builds from."""
        return self._config

    @property
    def events(self) -> EventDispatcher | None:
        """The dispatcher deliveries are announced on, if any."""
        return self._events

    def register_driver(self, driver: str, factory: MailerFactory) -> None:
        """Teach this manager a driver it does not ship with.

        Args:
            driver: The value that will appear as ``MailConfig.driver``.
            factory: Receives the name and configuration, returns a mailer.
        """
        self._drivers[driver] = factory

    def mailer(self, name: str | None = None) -> Mailer:
        """Return the named mailer.

        Args:
            name: The driver name, or ``None`` for the configured default.

        Returns:
            The memoised mailer.

        Raises:
            ConfigurationError: If the driver is unknown.
        """
        return self.driver(name)

    def _make(self, name: str) -> Mailer:
        """Build the mailer for *name*.

        Args:
            name: The driver name.

        Returns:
            A mailer.

        Raises:
            ConfigurationError: If the driver is not one this package ships and
                was not registered with :meth:`register_driver`.
        """
        registered = self._drivers.get(name)
        if registered is not None:
            return registered(name, self._config)

        match name:
            case "log":
                return LogMailer(name)
            case "null":
                return NullMailer(name)
            case "fake":
                return FakeMailer(name)
            case "smtp":
                return SmtpMailer(self._config, name)
            case unknown:
                known = ", ".join(sorted(KNOWN_DRIVERS))
                raise ConfigurationError(
                    f"unknown mail driver {unknown!r}; built-in drivers are {known}. "
                    f"Add your own with MailManager.register_driver({unknown!r}, factory) "
                    f"before first use."
                )

    async def _close_instance(self, instance: Mailer) -> None:
        """Release a mailer's resources.

        Args:
            instance: The mailer being discarded.
        """
        await instance.close()

    async def close(self) -> None:
        """Close every mailer that has been built."""
        await self.close_all()


__all__ = ["MailManager", "MailerFactory"]
