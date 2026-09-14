"""Logging configuration.

A plain frozen dataclass with ``from_env``, matching the cache, the database and
the queue: Keel declares the shape of the configuration it needs and the
application decides where the values come from.

Two knobs, and no more. Handler layout, per-logger levels and sampling are all
things a deployment can do to the ``logging`` module directly after
:func:`keel.observability.configure_logging` has run — re-declaring them here
would be a second, worse spelling of ``logging.config.dictConfig``.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from keel.exceptions import ConfigurationError

SETTING_VARS: Final = "LOG_"
"""Prefix for this subsystem's knobs. ``LOG_LEVEL``, ``LOG_FORMAT``."""

LEVELS: Final[frozenset[str]] = frozenset(
    {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
)
"""The level names accepted, checked at load rather than at the first log call."""

FORMATTERS: Final[frozenset[str]] = frozenset({"json", "text"})
"""The formatters shipped. See :mod:`keel.observability.logs` for what each is for."""


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    """How this process writes its logs.

    Attributes:
        level: The root logger's level, as a name. A string rather than an
            ``int`` because that is what an environment variable carries, and
            converting once here beats every caller doing it.
        formatter: ``json`` or ``text``. ``json`` is the default because a
            service's logs are read by a machine first; ``text`` exists so a
            developer running the process in a terminal is not reading JSON by
            eye, and is what ``LOG_FORMAT=text`` selects.
    """

    level: str = "INFO"
    formatter: str = "json"

    def __post_init__(self) -> None:
        """Reject configuration that cannot work, at load time.

        Raises:
            ConfigurationError: If the level or the formatter is not one this
                package ships. Caught here rather than at the first log call,
                which in a worker may be hours later and in a request handler is
                the one path nobody exercised.
        """
        if self.level.upper() not in LEVELS:
            raise ConfigurationError(
                f"{self.level!r} is not a log level; use one of {sorted(LEVELS)}"
            )
        if self.formatter not in FORMATTERS:
            raise ConfigurationError(
                f"{self.formatter!r} is not a log formatter; use one of {sorted(FORMATTERS)}"
            )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, prefix: str = "") -> LoggingConfig:
        """Build a configuration from environment variables.

        Reads ``LOG_LEVEL`` and ``LOG_FORMAT``.

        Args:
            env: The mapping to read; defaults to :data:`os.environ`.
            prefix: Prepended to every variable name, for an application that
                wants its own namespace.

        Returns:
            The configuration.

        Raises:
            ConfigurationError: If either value is not one this package ships.
        """
        source = os.environ if env is None else env
        return cls(
            level=source.get(f"{prefix}{SETTING_VARS}LEVEL", "INFO").upper(),
            formatter=source.get(f"{prefix}{SETTING_VARS}FORMAT", "json").lower(),
        )


__all__ = ["FORMATTERS", "LEVELS", "SETTING_VARS", "LoggingConfig"]
