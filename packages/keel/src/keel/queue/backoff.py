"""How long to wait before retrying a failed job.

A Strategy: the worker asks "how long before attempt N?" and the policy answers.
Interchangeable because the right answer depends entirely on why the job failed,
and only the person writing the job knows that — a rate-limited API wants long
exponential waits, a transient database blip wants a short fixed one, and a job
that is idempotent and cheap may want no wait at all.

**Jitter is on by default, and that is the important part.** Without it, a
hundred jobs failing at the same instant — which is what happens when a
dependency goes down — retry at the same instant too, and keep doing so in
lockstep. The retry storm then knocks the dependency over again just as it
recovers. Spreading retries across a window is the difference between a
thundering herd and a recovery.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

MAX_DELAY: Final = 3600.0
"""Ceiling on any computed delay, in seconds.

Exponential growth reaches absurd numbers quickly — attempt 20 of a doubling
policy is eight days. A job delayed past the point anyone is still looking at
the incident is indistinguishable from a lost one.
"""


@runtime_checkable
class Backoff(Protocol):
    """Answers how long to wait before a given retry attempt.

    Unlike the store contracts, this one *is* ``runtime_checkable``: it has a
    single method with a trivial signature, so an ``isinstance`` check is
    actually meaningful rather than a rubber stamp on attribute names.
    """

    def delay_for(self, attempt: int) -> float:
        """Return the delay before *attempt*, in seconds.

        Args:
            attempt: The attempt about to be made, counting from 1. The first
                retry is attempt 2, because attempt 1 already happened.

        Returns:
            Seconds to wait. Never negative, never above :data:`MAX_DELAY`.
        """
        ...


@dataclass(frozen=True, slots=True)
class NoBackoff:
    """Retry immediately.

    For jobs whose failure carries no information about when to try again — a
    lost lock, a lease that expired. Wrong for anything that failed because a
    dependency was unhappy, since retrying instantly is how you keep it unhappy.
    """

    def delay_for(self, attempt: int) -> float:
        """Return zero.

        Args:
            attempt: Ignored.

        Returns:
            ``0.0``.
        """
        return 0.0


@dataclass(frozen=True, slots=True)
class FixedBackoff:
    """Wait the same amount before every retry.

    Attributes:
        seconds: The delay.
        jitter: Fraction of the delay to randomise by, so simultaneous failures
            do not retry simultaneously. ``0.2`` spreads retries over ±20%.
    """

    seconds: float = 5.0
    jitter: float = 0.2

    def delay_for(self, attempt: int) -> float:
        """Return the fixed delay, jittered.

        Args:
            attempt: Ignored — that is what makes this policy fixed.

        Returns:
            Seconds to wait.
        """
        return _apply_jitter(self.seconds, self.jitter)


@dataclass(frozen=True, slots=True)
class ExponentialBackoff:
    """Double the wait after each failure, up to a ceiling.

    The default for Keel's jobs. A failing dependency usually needs time rather
    than another request, and the time it needs is unknown — so the policy
    escalates until either the job succeeds or it runs out of attempts.

    Attributes:
        base: The delay before the first retry.
        factor: Multiplier applied per attempt.
        maximum: Ceiling, so growth stops being theoretical.
        jitter: Fraction to randomise by. See the module docstring on why this
            defaults to a real value rather than zero.
    """

    base: float = 1.0
    factor: float = 2.0
    maximum: float = 300.0
    jitter: float = 0.2

    def delay_for(self, attempt: int) -> float:
        """Return the delay before *attempt*, jittered.

        Args:
            attempt: The attempt about to be made, counting from 1.

        Returns:
            Seconds to wait.
        """
        exponent = max(0, attempt - 2)
        raw = self.base * (self.factor**exponent)
        return _apply_jitter(min(raw, self.maximum), self.jitter)


def _apply_jitter(delay: float, jitter: float) -> float:
    """Spread *delay* randomly by ±*jitter* and clamp it to sane bounds.

    Args:
        delay: The computed delay.
        jitter: Fraction to vary by; ``0`` disables.

    Returns:
        A delay between ``0`` and :data:`MAX_DELAY`.
    """
    if jitter > 0:
        spread = delay * jitter
        delay = delay + random.uniform(-spread, spread)
    return max(0.0, min(delay, MAX_DELAY))


DEFAULT_BACKOFF: Final[Backoff] = ExponentialBackoff()
"""Applied to any job that does not choose otherwise."""


__all__ = [
    "DEFAULT_BACKOFF",
    "MAX_DELAY",
    "Backoff",
    "ExponentialBackoff",
    "FixedBackoff",
    "NoBackoff",
]
