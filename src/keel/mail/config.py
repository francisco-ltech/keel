"""Mail configuration.

A plain frozen dataclass with ``from_env``, matching every other subsystem:
Keel declares the shape, the application decides where the values come from.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from keel.exceptions import ConfigurationError

SETTING_VARS: Final = "MAIL_"
"""Prefix for this subsystem's knobs. ``MAIL_DRIVER``, ``MAIL_HOST``, …"""

KNOWN_DRIVERS: Final[frozenset[str]] = frozenset({"smtp", "log", "null", "fake"})
"""Drivers this package ships.

``log`` is the default and is not a test double: it is what a development run
or a staging environment with no mail server wants, and it writes who was
mailed about what without a body, so a log is never a mailbox. ``null``
discards. ``fake`` records, for tests.
"""

SECURITY: Final[frozenset[str]] = frozenset({"none", "starttls", "tls"})
"""How the SMTP connection is protected.

``none`` for a local Mailpit, ``starttls`` for the usual submission port 587,
``tls`` for an implicit-TLS port 465. Refused if unknown, at load time.
"""

DEFAULT_SENDER: Final = "noreply@localhost"
"""The ``From`` when neither the configuration nor the message names one.

A placeholder a mail server will reject, on purpose: a deployment that sends
mail must say who it is, and the default is the reminder.
"""


@dataclass(frozen=True, slots=True)
class MailConfig:
    """How this process sends mail.

    Attributes:
        driver: ``smtp``, ``log``, ``null`` or ``fake``, or any driver
            registered with :meth:`~keel.mail.manager.MailManager.register_driver`.
        sender: The ``From`` address used when a message names none.
        host: The SMTP server, for the ``smtp`` driver.
        port: Its port. 1025 is Mailpit's; 587 is submission.
        username: SMTP credentials, or ``None`` to send unauthenticated.
        password: With ``username``.
        security: One of :data:`SECURITY`.
        timeout: Seconds an SMTP conversation may take before it is abandoned.
    """

    driver: str = "log"
    sender: str = DEFAULT_SENDER
    host: str = "localhost"
    port: int = 1025
    username: str | None = None
    password: str | None = None
    security: str = "none"
    timeout: float = 10.0

    def __post_init__(self) -> None:
        """Reject configuration that cannot work, at load time.

        Raises:
            ConfigurationError: If the security mode is unknown, the port or
                timeout is not positive, the sender is empty, or credentials
                are set with ``security="none"``, which would send them in
                clear.
        """
        if self.security not in SECURITY:
            raise ConfigurationError(
                f"{self.security!r} is not a mail security mode; use one of {sorted(SECURITY)}"
            )
        if (self.username or self.password) and self.security == "none":
            raise ConfigurationError(
                "MAIL_USERNAME is set with MAIL_SECURITY=none: the credentials would cross "
                "the network in clear; use starttls or tls"
            )
        if not 0 < self.port < 65536:
            raise ConfigurationError(f"MAIL_PORT must be a port number, got {self.port}")
        if self.timeout <= 0:
            raise ConfigurationError(f"MAIL_TIMEOUT must be positive, got {self.timeout}")
        if not self.sender.strip():
            raise ConfigurationError("MAIL_FROM must name an address")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, prefix: str = "") -> MailConfig:
        """Build a configuration from environment variables.

        Reads ``MAIL_DRIVER``, ``MAIL_FROM``, ``MAIL_HOST``, ``MAIL_PORT``,
        ``MAIL_USERNAME``, ``MAIL_PASSWORD``, ``MAIL_SECURITY`` and
        ``MAIL_TIMEOUT``.

        Args:
            env: The mapping to read; defaults to :data:`os.environ`.
            prefix: Prepended to every variable name.

        Returns:
            The configuration.

        Raises:
            ConfigurationError: If a number is not one, or a value is not one
                this package accepts.
        """
        source = os.environ if env is None else env
        defaults = cls()

        def read(name: str, fallback: str | None) -> str | None:
            return source.get(f"{prefix}{SETTING_VARS}{name}", fallback)

        def number(name: str, fallback: float) -> float:
            raw = read(name, None)
            if raw is None or not raw.strip():
                return fallback
            try:
                return float(raw)
            except ValueError as exc:
                raise ConfigurationError(
                    f"{prefix}{SETTING_VARS}{name} must be a number, got {raw!r}"
                ) from exc

        return cls(
            driver=(read("DRIVER", defaults.driver) or defaults.driver).lower(),
            sender=read("FROM", defaults.sender) or defaults.sender,
            host=read("HOST", defaults.host) or defaults.host,
            port=int(number("PORT", defaults.port)),
            username=read("USERNAME", None) or None,
            password=read("PASSWORD", None) or None,
            security=(read("SECURITY", defaults.security) or defaults.security).lower(),
            timeout=number("TIMEOUT", defaults.timeout),
        )


__all__ = ["DEFAULT_SENDER", "KNOWN_DRIVERS", "SECURITY", "SETTING_VARS", "MailConfig"]
