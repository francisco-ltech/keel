"""Queue configuration.

Plain frozen dataclasses, matching the cache and the database: Keel declares the
shape it needs, the application decides where the values come from.

Environment names follow the same rule as the rest — subsystem, not framework.
``QUEUE_DRIVER`` and ``QUEUE_NAME``; the Redis URL is the same ``REDIS_URL`` the
cache reads, because a service running both against one Redis should not have to
say so twice.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from keel.exceptions import ConfigurationError

URL_VAR: Final = "REDIS_URL"
"""Shared with the cache. One Redis, one variable."""

SETTING_VARS: Final = "QUEUE_"
"""Prefix for the queue's own knobs."""

KNOWN_DRIVERS: Final = frozenset({"saq", "sync", "null", "fake"})
"""Drivers this package ships.

``sync`` and ``null`` are not test doubles — they are real choices for a real
deployment. ``sync`` runs jobs inline, which is how a single-process
development environment avoids needing a worker at all; ``null`` discards them,
which is what a smoke-test environment wants.
"""

DEFAULT_PREFIX: Final = "keel:queue"
"""Namespace for queue keys.

Non-empty for the reason the cache's is: a shared Redis holds other things, and
an unnamespaced queue makes administrative operations dangerous. A *sibling* of
the cache's prefix rather than sharing it, so a cache flush cannot drain the
queue.
"""


@dataclass(frozen=True, slots=True)
class QueueConfig:
    """How to reach the queue.

    Attributes:
        driver: ``saq``, ``sync``, ``null`` or ``fake``, or any driver
            registered with :meth:`~keel.queue.manager.QueueManager.register_driver`.
        url: Connection URL, for drivers that need one.
        prefix: Key namespace.
        default_queue: The queue a job goes to when it names none.
        concurrency: How many jobs one worker process runs at once. A worker is
            usually I/O-bound, so this is higher than a CPU count would suggest;
            it is capped by the database pool, since most jobs open a
            transaction and a worker that runs more jobs than it has connections
            spends its time waiting for one.
    """

    driver: str = "sync"
    url: str | None = None
    prefix: str = DEFAULT_PREFIX
    default_queue: str = "default"
    concurrency: int = 10

    def __post_init__(self) -> None:
        """Reject configuration that cannot work, at load time.

        Raises:
            ConfigurationError: If the driver needs a URL and has none, or the
                concurrency is not positive.
        """
        if self.driver == "saq" and not self.url:
            raise ConfigurationError("the saq queue driver requires a 'url'")
        if self.concurrency < 1:
            raise ConfigurationError(
                f"queue concurrency must be at least 1, got {self.concurrency}"
            )
        if not self.prefix:
            raise ConfigurationError(
                "a queue needs a non-empty prefix: administrative operations "
                "would otherwise reach keys belonging to anything else sharing "
                "the backend"
            )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, prefix: str = "") -> QueueConfig:
        """Build a configuration from environment variables.

        Reads ``QUEUE_DRIVER``, ``QUEUE_PREFIX``, ``QUEUE_DEFAULT``,
        ``QUEUE_CONCURRENCY`` and ``REDIS_URL``.

        Args:
            env: The mapping to read; defaults to :data:`os.environ`.
            prefix: Prepended to every variable name, for an application that
                wants its own namespace.

        Returns:
            The configuration.

        Raises:
            ConfigurationError: If the concurrency is not a number.
        """
        source = os.environ if env is None else env
        raw_concurrency = source.get(f"{prefix}{SETTING_VARS}CONCURRENCY", "10")
        try:
            concurrency = int(raw_concurrency)
        except ValueError as exc:
            raise ConfigurationError(
                f"{prefix}{SETTING_VARS}CONCURRENCY must be a whole number, got {raw_concurrency!r}"
            ) from exc

        return cls(
            driver=source.get(f"{prefix}{SETTING_VARS}DRIVER", "sync"),
            url=source.get(f"{prefix}{URL_VAR}"),
            prefix=source.get(f"{prefix}{SETTING_VARS}PREFIX", DEFAULT_PREFIX),
            default_queue=source.get(f"{prefix}{SETTING_VARS}DEFAULT", "default"),
            concurrency=concurrency,
        )


__all__ = ["DEFAULT_PREFIX", "KNOWN_DRIVERS", "SETTING_VARS", "URL_VAR", "QueueConfig"]
