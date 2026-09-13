"""Database configuration.

A plain frozen dataclass, for the same reason the cache's is: Keel declares the
shape it needs and the application decides where the values come from.

Several defaults here are deliberately opinionated rather than neutral, because
the neutral value is the one that produces an outage at 3am:

* ``statement_timeout`` is set. Without it a single pathological query holds a
  connection until someone notices, and the pool starves behind it.
* ``pool_recycle`` is below the interval at which most managed Postgres
  providers drop idle connections, so the pool discards them before the server
  does and the application never sees a dead socket.
* ``pool_pre_ping`` is on. It costs a round trip per checkout and removes the
  entire class of "first request after an idle period fails".
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from keel.exceptions import ConfigurationError

URL_VAR: Final = "DATABASE_URL"
"""Where the connection string is read from.

``DATABASE_URL`` rather than something Keel-specific because it is what every
hosting platform already injects — Fly, Railway, Render, Neon, Heroku — and what
Alembic and psql tooling look for. A framework that renames it makes every
deployment map it back by hand, forever.
"""

SETTING_VARS: Final = "DB_"
"""Prefix for the remaining knobs: ``DB_POOL_SIZE``, ``DB_ECHO`` and friends.

Named after the subsystem, not the framework. Laravel does not call it
``LARAVEL_DB_HOST``, and neither should this: the variables describe the
application's infrastructure, and they should keep their names if Keel is ever
swapped out from underneath them.
"""

ASYNC_DRIVERS: Final = frozenset({"postgresql+asyncpg", "postgresql+psycopg", "sqlite+aiosqlite"})
"""URL schemes known to be async. Used for a clearer error than the one
SQLAlchemy raises when handed a sync driver to an async engine."""


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    """How to reach and pool the database.

    Attributes:
        url: An async SQLAlchemy URL, e.g.
            ``postgresql+asyncpg://user:pass@host:5432/db``.
        echo: Log every statement. Useful in a test run, ruinous in production.
        pool_size: Connections kept open per process.
        max_overflow: Extra connections allowed under burst, discarded after.
        pool_timeout: Seconds to wait for a free connection before raising.
            A caller that waits indefinitely turns pool exhaustion into a hang
            rather than an error, which is much harder to diagnose.
        pool_recycle: Seconds after which an idle connection is replaced.
        pool_pre_ping: Check a connection is alive on checkout.
        statement_timeout: Server-side cap on a single statement, in seconds,
            or ``None`` to leave the server default. Applied per connection.
        connect_timeout: Seconds to wait when opening a new connection.
    """

    url: str
    echo: bool = False
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: float = 30.0
    pool_recycle: int = 1800
    pool_pre_ping: bool = True
    statement_timeout: float | None = 30.0
    connect_timeout: float = 10.0

    def __post_init__(self) -> None:
        """Reject configuration that cannot work, at load time.

        Raises:
            ConfigurationError: If the URL is missing, or names a synchronous
                driver. The latter is worth catching here because SQLAlchemy's
                own error arrives later and reads as an internal problem.
        """
        if not self.url:
            raise ConfigurationError("a database url is required")
        scheme = self.url.split("://", 1)[0]
        if "+" not in scheme:
            raise ConfigurationError(
                f"database url {scheme!r} names no driver; Keel is async-only, so use "
                f"an async driver such as 'postgresql+asyncpg://'"
            )
        if scheme not in ASYNC_DRIVERS:
            known = ", ".join(sorted(ASYNC_DRIVERS))
            raise ConfigurationError(
                f"database driver {scheme!r} is not a known async driver ({known}); "
                f"a synchronous driver will block the event loop"
            )

    @property
    def is_sqlite(self) -> bool:
        """Whether this points at SQLite, which does not support pooling options."""
        return self.url.startswith("sqlite")

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        prefix: str = "",
    ) -> DatabaseConfig:
        """Build a configuration from environment variables.

        Reads ``DATABASE_URL`` plus every knob as ``DB_*``: ``DB_ECHO``,
        ``DB_POOL_SIZE``, ``DB_MAX_OVERFLOW``, ``DB_POOL_TIMEOUT``,
        ``DB_POOL_RECYCLE``, ``DB_POOL_PRE_PING``, ``DB_STATEMENT_TIMEOUT`` and
        ``DB_CONNECT_TIMEOUT``.

        A partial reader is worse than none: it silently ignores a setting the
        operator believed they had changed, so every field is covered.

        Args:
            env: The mapping to read; defaults to :data:`os.environ`.
            prefix: Prepended to every variable name, for an application that
                wants its own namespace (``ACME_DATABASE_URL``). Empty by
                default — the library should not impose one, because the names
                belong to the deployment, not to Keel.

        Returns:
            The configuration.

        Raises:
            ConfigurationError: If a numeric variable is not a number, or the
                url is absent.
        """
        source = os.environ if env is None else env

        def variable(name: str) -> str:
            return f"{prefix}{SETTING_VARS}{name}"

        def number(name: str, default: str) -> float:
            raw = source.get(variable(name), default)
            try:
                return float(raw)
            except ValueError as exc:
                raise ConfigurationError(f"{variable(name)} must be a number, got {raw!r}") from exc

        raw_timeout = source.get(variable("STATEMENT_TIMEOUT"), "30")
        statement_timeout = (
            None
            if raw_timeout.lower() in {"", "none", "null"}
            else number("STATEMENT_TIMEOUT", "30")
        )

        def flag(name: str, default: bool) -> bool:
            raw = source.get(variable(name))
            if raw is None:
                return default
            return raw.lower() in {"1", "true", "yes", "on"}

        return cls(
            url=source.get(f"{prefix}{URL_VAR}", ""),
            echo=flag("ECHO", False),
            pool_size=int(number("POOL_SIZE", "5")),
            max_overflow=int(number("MAX_OVERFLOW", "10")),
            pool_timeout=number("POOL_TIMEOUT", "30"),
            pool_recycle=int(number("POOL_RECYCLE", "1800")),
            pool_pre_ping=flag("POOL_PRE_PING", True),
            statement_timeout=statement_timeout,
            connect_timeout=number("CONNECT_TIMEOUT", "10"),
        )
