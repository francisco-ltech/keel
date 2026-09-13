"""Password hashing configuration.

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
"""Prefix for this subsystem's knobs. Subsystem, not framework."""

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


__all__ = ["MINIMUM_MEMORY_COST", "SETTING_VARS", "HashingConfig"]
