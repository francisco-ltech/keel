"""Auth configuration.

A plain frozen dataclass, matching every other subsystem: Keel declares the
shape, the application decides where the values come from.

The knobs exist for one concrete reason. Argon2 at pwdlib's recommended
parameters costs ~44 ms per hash on a developer laptop, by design — that cost is
the defence. A suite that registers users in its fixtures pays it per test, so
``HASHING_*`` lets a test environment turn it down without the production
default moving.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from keel.exceptions import ConfigurationError

SETTING_VARS: Final = "HASHING_"
"""Prefix for the hashing knobs. Subsystem, not framework."""

TOKEN_VARS: Final = "TOKEN_"
"""Prefix for the token store's knobs."""

URL_VAR: Final = "REDIS_URL"
"""Shared with the cache and the queue. One Redis, one variable."""

KNOWN_TOKEN_DRIVERS: Final = frozenset({"redis", "memory", "fake"})
"""Drivers this package ships.

``memory`` is not a test double — it is what a single-process deployment or a
development run wants, and it holds tokens for exactly as long as the process
does. ``fake`` is the recording double.
"""

DEFAULT_TOKEN_PREFIX: Final = "keel:tokens"
"""Namespace for token keys.

Non-empty for the reason the cache's and the queue's are: a shared Redis holds
other things, and an unnamespaced administrative operation reaches them. That
shipped as a real bug once.
"""

DEFAULT_TOKEN_TTL: Final = 1_209_600.0
"""Fourteen days.

A default rather than a policy — long enough that a person is not signed out
mid-week, short enough that a token stolen and forgotten stops working. A
service that wants sessions to outlive that says so; one that wants an hour
says that.
"""

MINIMUM_MEMORY_COST: Final = 8192
"""8 MiB, in KiB. Below this Argon2 stops being meaningfully memory-hard, which
is the whole reason to prefer it, so a lower value is refused rather than
quietly accepted."""


@dataclass(frozen=True, slots=True)
class HashingConfig:
    """Argon2id cost parameters.

    Defaults are pwdlib's recommended set, restated here rather than deferred to
    so that a pwdlib upgrade cannot change a deployment's cost silently.

    Attributes:
        time_cost: Iterations.
        memory_cost: Memory per hash, in KiB.
        parallelism: Lanes.
    """

    time_cost: int = 3
    memory_cost: int = 65536
    parallelism: int = 4

    def __post_init__(self) -> None:
        """Reject parameters that cannot work, at load time.

        Raises:
            ConfigurationError: If any parameter is below its floor.
        """
        if self.time_cost < 1:
            raise ConfigurationError(f"hashing time_cost must be at least 1, got {self.time_cost}")
        if self.memory_cost < MINIMUM_MEMORY_COST:
            raise ConfigurationError(
                f"hashing memory_cost must be at least {MINIMUM_MEMORY_COST} KiB, "
                f"got {self.memory_cost}; below that Argon2 is no longer memory-hard"
            )
        if self.parallelism < 1:
            raise ConfigurationError(
                f"hashing parallelism must be at least 1, got {self.parallelism}"
            )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, prefix: str = "") -> HashingConfig:
        """Build a configuration from environment variables.

        Reads ``HASHING_TIME_COST``, ``HASHING_MEMORY_COST`` and
        ``HASHING_PARALLELISM``.

        Args:
            env: The mapping to read; defaults to :data:`os.environ`.
            prefix: Prepended to every variable name.

        Returns:
            The configuration.

        Raises:
            ConfigurationError: If a value is not a whole number, or is below
                its floor.
        """
        source = os.environ if env is None else env
        defaults = cls()

        def whole(name: str, fallback: int) -> int:
            raw = source.get(f"{prefix}{SETTING_VARS}{name}")
            if raw is None:
                return fallback
            try:
                return int(raw)
            except ValueError as exc:
                raise ConfigurationError(
                    f"{prefix}{SETTING_VARS}{name} must be a whole number, got {raw!r}"
                ) from exc

        return cls(
            time_cost=whole("TIME_COST", defaults.time_cost),
            memory_cost=whole("MEMORY_COST", defaults.memory_cost),
            parallelism=whole("PARALLELISM", defaults.parallelism),
        )


@dataclass(frozen=True, slots=True)
class TokenConfig:
    """Where bearer tokens live and how long they last.

    Attributes:
        driver: ``redis``, ``memory`` or ``fake``, or any driver registered with
            :meth:`~keel.auth.manager.TokenManager.register_driver`.
        url: Connection URL, for drivers that need one.
        prefix: Key namespace.
        ttl: Default lifetime in seconds, or ``None`` for tokens that never
            expire. ``None`` has to be asked for: a token with no expiry is a
            permanent credential, which is a decision rather than an omission.
    """

    driver: str = "memory"
    url: str | None = None
    prefix: str = DEFAULT_TOKEN_PREFIX
    ttl: float | None = DEFAULT_TOKEN_TTL

    def __post_init__(self) -> None:
        """Reject configuration that cannot work, at load time.

        Raises:
            ConfigurationError: If the driver needs a URL and has none, the
                prefix is empty, or the lifetime is not positive.
        """
        if self.driver == "redis" and not self.url:
            raise ConfigurationError("the redis token store requires a 'url'")
        if not self.prefix:
            raise ConfigurationError(
                "a token store needs a non-empty prefix: administrative "
                "operations would otherwise reach keys belonging to anything "
                "else sharing the backend"
            )
        if self.ttl is not None and self.ttl <= 0:
            raise ConfigurationError(
                f"token ttl must be positive, or None for no expiry; got {self.ttl}"
            )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, prefix: str = "") -> TokenConfig:
        """Build a configuration from environment variables.

        Reads ``TOKEN_DRIVER``, ``TOKEN_PREFIX``, ``TOKEN_TTL`` and ``REDIS_URL``.
        ``TOKEN_TTL=never`` is how a deployment asks for tokens that do not
        expire — spelled out, because an empty value meaning "forever" is the
        kind of default nobody intends.

        Args:
            env: The mapping to read; defaults to :data:`os.environ`.
            prefix: Prepended to every variable name.

        Returns:
            The configuration.

        Raises:
            ConfigurationError: If the lifetime is not a number.
        """
        source = os.environ if env is None else env
        raw_ttl = source.get(f"{prefix}{TOKEN_VARS}TTL")
        if raw_ttl is None:
            ttl: float | None = DEFAULT_TOKEN_TTL
        elif raw_ttl.strip().lower() == "never":
            ttl = None
        else:
            try:
                ttl = float(raw_ttl)
            except ValueError as exc:
                raise ConfigurationError(
                    f"{prefix}{TOKEN_VARS}TTL must be a number of seconds or "
                    f"'never', got {raw_ttl!r}"
                ) from exc

        return cls(
            driver=source.get(f"{prefix}{TOKEN_VARS}DRIVER", "memory"),
            url=source.get(f"{prefix}{URL_VAR}"),
            prefix=source.get(f"{prefix}{TOKEN_VARS}PREFIX", DEFAULT_TOKEN_PREFIX),
            ttl=ttl,
        )


__all__ = [
    "DEFAULT_TOKEN_PREFIX",
    "DEFAULT_TOKEN_TTL",
    "KNOWN_TOKEN_DRIVERS",
    "MINIMUM_MEMORY_COST",
    "SETTING_VARS",
    "TOKEN_VARS",
    "URL_VAR",
    "HashingConfig",
    "TokenConfig",
]
