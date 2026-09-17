"""Readiness: whether this process can do its job right now, and if not, why.

A readiness probe is a claim made to an orchestrator, which acts on it by
routing traffic away. Three things make that claim trustworthy, and a hand-rolled
``/ready`` usually has one of them:

* **Every dependency the process needs is asked.** An API that checks Postgres
  and not Redis reports ready while every authenticated request fails, because
  the token store is in Redis.
* **The wait is bounded.** A database check that waits out a 30-second pool
  timeout answers long after the orchestrator gave up, and each probe that
  arrives meanwhile adds one more coroutine queued on the same pool.
* **The answer says which dependency failed.** "503" starts an investigation;
  "503, cache: ConnectionError" ends one.

**Patterns.** None beyond a function, deliberately. A check is ``async () -> None``
that raises on failure, and :func:`probe` runs a mapping of them concurrently.
Declined, with what would change each:

* **A ``HealthCheck`` protocol or base class.** One method and no state is a
  function. It becomes a class the day a check needs configuration a closure or
  ``functools.partial`` cannot carry.
* **A registry the subsystems add themselves to.** Which dependencies gate
  readiness is the *application's* decision — a worker-shaped process that binds
  a cache it barely uses may not want it to — so the application passes the
  mapping. Self-registration would make that list invisible at the call site.
* **Critical versus non-critical checks.** Every check gates. A dependency whose
  failure should not take the process out of rotation is not a readiness check,
  and belongs in metrics.
* **Caching the result.** A probe every few seconds per replica is a trivial
  query per replica, and a cached "ready" is exactly the stale claim a probe
  exists to avoid.

**Liveness is not here.** Liveness must not touch dependencies — restarting a
process does not fix its database, and it does lose the in-flight work — so it
needs nothing from Keel: an endpoint that returns 200, or for a worker, the
heartbeat file ``Worker.healthy`` refreshes.

The built-in checks import their subsystems on call rather than at module
scope, so ``configure_logging`` — the first thing a process runs — does not pay
for a database driver, and a dispatch-only process does not import a token store.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

from keel.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

type Check = Callable[[], Awaitable[object]]
"""A readiness check: returns if the dependency answers, raises if it does not.

The return value is ignored, so a bound method that happens to return something
— ``Queue.size`` — can be passed without a wrapper.
"""

DEFAULT_TIMEOUT: Final = 0.8
"""Seconds each check may take before it counts as failed.

Below Kubernetes' default probe timeout of one second, with room for the
response itself, so the 503 and the name of the failed check arrive while the
orchestrator is still listening. At the full second they arrive after it.
"""

PROBE_KEY: Final = "keel:health:probe"
"""The cache key :func:`check_cache` reads. Never written, so always a miss."""


@dataclass(frozen=True, slots=True)
class CheckResult:
    """What one check reported.

    Attributes:
        name: The name it was registered under.
        ok: Whether it returned within the timeout without raising.
        duration: Seconds it took, which is about the timeout if it ran out.
        error: The exception's class name when it failed. The class and not the
            message, because this goes into a response body and a driver's
            message can carry a hostname or a query.
    """

    name: str
    ok: bool
    duration: float
    error: str | None = None


@dataclass(frozen=True, slots=True)
class HealthReport:
    """Every check's result, in the order the checks were given.

    Attributes:
        checks: One result per check.
    """

    checks: tuple[CheckResult, ...]

    @property
    def ok(self) -> bool:
        """Whether every check passed."""
        return all(result.ok for result in self.checks)

    @property
    def status(self) -> Literal["ready", "unready"]:
        """The one-word answer a response body leads with."""
        return "ready" if self.ok else "unready"

    def as_dict(self) -> dict[str, Any]:
        """Render the report for a JSON response body.

        Returns:
            ``{"status": ..., "checks": {name: {"ok", "duration_ms"[, "error"]}}}``.
        """
        checks: dict[str, dict[str, Any]] = {}
        for result in self.checks:
            entry: dict[str, Any] = {
                "ok": result.ok,
                "duration_ms": round(result.duration * 1000, 1),
            }
            if result.error is not None:
                entry["error"] = result.error
            checks[result.name] = entry
        return {"status": self.status, "checks": checks}


async def probe(checks: Mapping[str, Check], *, timeout: float = DEFAULT_TIMEOUT) -> HealthReport:
    """Run every check concurrently, each under its own time limit.

    Concurrently because the probe's latency should be the slowest dependency's,
    not the sum of them. A failure is logged here with its message, at
    ``WARNING``, since the report only carries the exception's class.

    Cancelling the probe cancels the checks and propagates: a request that went
    away is not a dependency that failed.

    Args:
        checks: Check functions by the name the report should use.
        timeout: Seconds each check may take.

    Returns:
        The report. It never raises for a failing check.

    Raises:
        ConfigurationError: If *checks* is empty or *timeout* is not positive. A
            probe that checks nothing always answers ready, which is worse than
            having no probe.
    """
    if not checks:
        raise ConfigurationError("probe() needs at least one check; an empty probe is always ready")
    if timeout <= 0:
        raise ConfigurationError(f"probe() timeout must be positive, got {timeout!r}")

    async with asyncio.TaskGroup() as group:
        tasks = [group.create_task(_run(name, check, timeout)) for name, check in checks.items()]
    return HealthReport(tuple(task.result() for task in tasks))


async def _run(name: str, check: Check, timeout: float) -> CheckResult:
    """Run one check, turning any ``Exception`` into a failed result.

    Args:
        name: The check's name.
        check: The check.
        timeout: Seconds it may take.

    Returns:
        Its result.
    """
    started = time.perf_counter()
    try:
        async with asyncio.timeout(timeout) as deadline:
            await check()
        if deadline.expired():
            # The check caught the cancel and returned late; it still overran.
            raise TimeoutError
    except Exception as error:  # noqa: BLE001 — every failure is a result, not a crash
        # `asyncio.timeout` raises TimeoutError only for its own deadline, so an
        # outer cancellation is a BaseException and still propagates.
        duration = time.perf_counter() - started
        logger.warning(
            "readiness check %s failed after %.3fs: %s: %s",
            name,
            duration,
            type(error).__name__,
            error,
        )
        return CheckResult(name, ok=False, duration=duration, error=type(error).__name__)
    return CheckResult(name, ok=True, duration=time.perf_counter() - started)


async def check_database() -> None:
    """Run ``SELECT 1`` through the bound database's pool.

    Raises:
        ConfigurationError: If no database is bound.
    """
    from keel.database import current_database

    await current_database().ping()


async def check_cache(name: str | None = None) -> None:
    """Read a key that is never written from a cache store.

    A read rather than a write, so a probe cannot fill a store or evict anything.
    It goes to the store *under* the event decorator when there is one, because
    a probe every few seconds per replica would otherwise be a stream of cache
    misses that no application code caused.

    Args:
        name: The store to read, or ``None`` for the default. Pass another with
            ``functools.partial(check_cache, "sessions")``.

    Raises:
        ConfigurationError: If no cache is bound, or *name* is not configured.
    """
    from keel.cache import current_cache_manager
    from keel.cache.stores.eventful import EventfulStore

    store = current_cache_manager().store(name).store
    if isinstance(store, EventfulStore):
        store = store.inner
    await store.get(PROBE_KEY)


async def check_tokens(name: str | None = None) -> None:
    """Resolve a freshly generated token, which no store can know.

    A resolve is exactly the read every authenticated request makes, so this
    fails for the reason those requests would, and writes nothing.

    Args:
        name: The token store, or ``None`` for the default.

    Raises:
        ConfigurationError: If no token manager is bound.
    """
    from keel.auth import token_store
    from keel.auth.tokens import generate_token

    await token_store(name).resolve(generate_token())


async def check_queue(name: str | None = None) -> None:
    """Count the default lane of a queue connection.

    ``Queue.size`` is already in the contract "for health checks", and it is
    the same connection :func:`keel.queue.dispatch` pushes on.

    Args:
        name: The connection, or ``None`` for the default.

    Raises:
        ConfigurationError: If no queue manager is bound.
    """
    from keel.queue.dispatch import queue

    await queue(name).size()


__all__ = [
    "DEFAULT_TIMEOUT",
    "PROBE_KEY",
    "Check",
    "CheckResult",
    "HealthReport",
    "check_cache",
    "check_database",
    "check_queue",
    "check_tokens",
    "probe",
]
