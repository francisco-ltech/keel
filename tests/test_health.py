"""Readiness checks.

The claims pinned here: **a failing dependency is reported by name and never
raised; a slow one costs the timeout and not more; checks run concurrently; and
cancelling a probe is not a dependency failing.** Then each built-in check is run
once against the real backend and once against one that is not there, because a
check that cannot fail is the defect this module exists to prevent.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from functools import partial

import pytest
from sqlalchemy import text

from keel.auth import TokenConfig, TokenManager, use_token_manager
from keel.cache import CacheConfig, CacheManager, CacheMissed, EventDispatcher, use_cache
from keel.cache.config import StoreConfig
from keel.database import Database, DatabaseConfig, use_database
from keel.database.engine import PROBE_APPLICATION_NAME
from keel.exceptions import ConfigurationError
from keel.observability import (
    PROBE_KEY,
    Check,
    CheckResult,
    HealthReport,
    check_cache,
    check_database,
    check_queue,
    check_tokens,
    probe,
)
from keel.queue import QueueConfig, QueueManager, use_queue

pytestmark = [pytest.mark.anyio]

UNREACHABLE_REDIS = "redis://127.0.0.1:1/0"
"""Port 1 refuses at once, so a failing check fails fast rather than timing out."""

UNREACHABLE_POSTGRES = "postgresql+asyncpg://nobody:nobody@127.0.0.1:1/nowhere"


async def passes() -> None:
    """A dependency that answers."""


async def refuses() -> None:
    """A dependency that is down, with a message a response body must not carry."""
    raise ConnectionRefusedError("connect to db-primary.internal:5432 refused")


async def hangs() -> None:
    """A dependency that never answers."""
    await asyncio.sleep(3600)


# -- probe ----------------------------------------------------------------


async def test_every_check_passing_is_ready() -> None:
    report = await probe({"database": passes, "cache": passes})

    assert report.ok
    assert report.status == "ready"
    assert [result.name for result in report.checks] == ["database", "cache"]


async def test_one_failure_is_unready_and_names_only_the_failed_check() -> None:
    report = await probe({"database": passes, "cache": refuses})

    assert not report.ok
    assert report.as_dict()["status"] == "unready"
    database, cache = report.checks
    assert database.ok and database.error is None
    assert not cache.ok and cache.error == "ConnectionRefusedError"


async def test_the_body_carries_the_class_but_never_the_message() -> None:
    """A driver's message names hosts and ports, and ``/ready`` is often public."""
    body = (await probe({"database": refuses})).as_dict()

    assert body == {
        "status": "unready",
        "checks": {
            "database": {
                "ok": False,
                "duration_ms": body["checks"]["database"]["duration_ms"],
                "error": "ConnectionRefusedError",
            }
        },
    }
    assert "internal" not in repr(body)


async def test_the_message_goes_to_the_log_instead(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="keel.observability.health"):
        await probe({"database": refuses})

    assert "db-primary.internal:5432" in caplog.text


async def test_a_hanging_check_fails_at_the_timeout_rather_than_waiting() -> None:
    started = time.perf_counter()
    report = await probe({"database": hangs}, timeout=0.05)
    elapsed = time.perf_counter() - started

    assert not report.ok
    assert report.checks[0].error == "TimeoutError"
    assert elapsed < 0.5


async def test_checks_run_concurrently() -> None:
    """Three 0.2s checks in well under 0.6s: latency is the slowest, not the sum."""

    async def slow() -> None:
        await asyncio.sleep(0.2)

    started = time.perf_counter()
    report = await probe({"a": slow, "b": slow, "c": slow}, timeout=2)

    assert report.ok
    assert time.perf_counter() - started < 0.5


async def test_a_hanging_check_does_not_delay_the_others_results() -> None:
    report = await probe({"database": passes, "cache": hangs}, timeout=0.05)

    assert [result.ok for result in report.checks] == [True, False]


async def test_cancelling_the_probe_propagates_rather_than_reporting_a_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A client that hung up is not a dependency that is down.

    The log assertion is the sharp half: the task group re-raises the outer
    cancel either way, so only the warning shows a check swallowed its own.
    """
    task = asyncio.create_task(probe({"database": hangs}, timeout=60))
    await asyncio.sleep(0.01)

    with caplog.at_level(logging.WARNING, logger="keel.observability.health"):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert caplog.records == []


async def test_a_check_that_swallows_its_cancellation_is_still_a_timeout() -> None:
    """``asyncio.timeout`` raises only if the cancel it sent propagates.

    A check that catches it and returns would otherwise be ``ok`` at any age.
    """

    async def stubborn() -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await asyncio.sleep(0.1)

    report = await probe({"stubborn": stubborn}, timeout=0.05)

    assert report.checks[0].error == "TimeoutError"


async def test_a_return_value_is_ignored() -> None:
    async def counts() -> int:
        return 7

    assert (await probe({"queue": counts})).ok


async def test_an_empty_probe_is_refused() -> None:
    """It would always answer ready, which is worse than having no probe."""
    with pytest.raises(ConfigurationError, match="at least one check"):
        await probe({})


@pytest.mark.parametrize("timeout", [0, -1.0])
async def test_a_non_positive_timeout_is_refused(timeout: float) -> None:
    with pytest.raises(ConfigurationError, match="positive"):
        await probe({"database": passes}, timeout=timeout)


def test_a_report_with_a_failure_is_not_ok() -> None:
    passed = CheckResult("a", ok=True, duration=0.0)
    failed = CheckResult("b", ok=False, duration=0.0)

    assert HealthReport((passed,)).ok
    report = HealthReport((passed, failed))

    assert not report.ok


# -- built-in checks: nothing bound ----------------------------------------


@pytest.mark.parametrize(
    "check",
    [check_database, check_cache, check_tokens, check_queue],
    ids=["database", "cache", "tokens", "queue"],
)
async def test_an_unbound_subsystem_is_reported_not_raised(check: Check) -> None:
    """A lifespan left out of the wiring shows up as unready, with the reason."""
    report = await probe({"dependency": check})

    assert report.checks[0].error == "ConfigurationError"


# -- built-in checks: in-process drivers -----------------------------------


def array_cache(events: EventDispatcher | None = None) -> CacheManager:
    """A cache manager whose only store is in-memory."""
    config = CacheConfig(default="array", stores={"array": StoreConfig(driver="array")})
    return CacheManager(config, events)


async def test_check_cache_passes_on_an_in_memory_store() -> None:
    manager = array_cache()
    with use_cache(manager):
        assert (await probe({"cache": check_cache})).ok
    await manager.close()


async def test_check_cache_emits_no_cache_events() -> None:
    """Otherwise every replica's probe is a stream of misses nobody caused."""
    events = EventDispatcher()
    missed: list[CacheMissed] = []
    events.listen(CacheMissed, missed.append)
    manager = array_cache(events)
    with use_cache(manager):
        assert (await probe({"cache": check_cache})).ok
    await manager.close()

    assert missed == []


async def test_check_cache_reads_a_named_store() -> None:
    manager = array_cache()
    with use_cache(manager):
        report = await probe({"sessions": partial(check_cache, "sessions")})
    await manager.close()

    assert report.checks[0].error == "ConfigurationError"


async def test_check_cache_writes_nothing() -> None:
    manager = array_cache()
    with use_cache(manager):
        await probe({"cache": check_cache})
        assert await manager.store().has(PROBE_KEY) is False
    await manager.close()


async def test_check_tokens_and_check_queue_pass_on_in_process_drivers() -> None:
    tokens = TokenManager(TokenConfig(driver="memory"))
    queues = QueueManager(QueueConfig(driver="sync"))
    with use_token_manager(tokens), use_queue(queues):
        report = await probe({"tokens": check_tokens, "queue": check_queue})
    await tokens.close()
    await queues.close()

    assert report.ok, report.as_dict()


# -- built-in checks: real backends ----------------------------------------


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    """A bound database, closed afterwards."""
    instance = Database(DatabaseConfig(url=database_url))
    with use_database(instance):
        yield instance
    await instance.close()


@pytest.mark.postgres
async def test_check_database_passes_against_postgres(database: Database) -> None:
    assert (await probe({"database": check_database})).ok


@pytest.mark.postgres
async def test_check_database_fails_against_nothing() -> None:
    broken = Database(DatabaseConfig(url=UNREACHABLE_POSTGRES, pool_pre_ping=False))
    try:
        with use_database(broken):
            report = await probe({"database": check_database})
    finally:
        await broken.close()

    assert not report.ok


@pytest.mark.postgres
async def test_a_saturated_request_pool_is_still_ready(database_url: str) -> None:
    """Busy is not down.

    A request arriving now waits for a connection and is served. A probe that
    queued behind it would call every replica unready at once, under load, and
    leave the load balancer with nowhere to send anything.
    """
    small = Database(DatabaseConfig(url=database_url, pool_size=1, max_overflow=0))
    try:
        with use_database(small):
            async with small.connect():
                report = await probe({"database": check_database}, timeout=0.5)
    finally:
        await small.close()

    assert report.ok, report.as_dict()


async def probe_connections(database: Database) -> int:
    """Count probe connections to this database, as Postgres sees them.

    By application name rather than every backend: serially the database is
    shared, and a dev server or ``psql`` on it would otherwise move the count.
    """
    async with database.connect() as connection:
        result = await connection.execute(
            text(
                "SELECT count(*) FROM pg_stat_activity"
                " WHERE datname = current_database() AND application_name = :name"
            ),
            {"name": PROBE_APPLICATION_NAME},
        )
        return int(result.scalar_one())


@pytest.mark.postgres
async def test_the_probe_holds_one_connection_and_close_releases_it(
    database: Database, database_url: str
) -> None:
    observed = Database(DatabaseConfig(url=database_url))
    try:
        with use_database(observed):
            for _ in range(5):
                assert (await probe({"database": check_database})).ok
        assert await probe_connections(database) == 1
    finally:
        await observed.close()

    assert await probe_connections(database) == 0


def redis_cache(url: str, prefix: str) -> CacheManager:
    """A cache manager whose default store is Redis at *url*."""
    store = StoreConfig(driver="redis", url=url, prefix=prefix)
    return CacheManager(CacheConfig(default="redis", stores={"redis": store}))


@pytest.mark.redis
async def test_check_cache_passes_against_redis(redis_url: str, namespace_suffix: str) -> None:
    manager = redis_cache(redis_url, f"keel-test:{namespace_suffix}")
    with use_cache(manager):
        report = await probe({"cache": check_cache})
    await manager.close()

    assert report.ok, report.as_dict()


@pytest.mark.redis
async def test_check_cache_fails_against_nothing(namespace_suffix: str) -> None:
    manager = redis_cache(UNREACHABLE_REDIS, f"keel-test:{namespace_suffix}")
    with use_cache(manager):
        report = await probe({"cache": check_cache})
    await manager.close()

    assert not report.ok


@pytest.mark.redis
async def test_check_tokens_and_check_queue_pass_against_redis(
    redis_url: str, namespace_suffix: str
) -> None:
    prefix = f"keel-test:{namespace_suffix}"
    tokens = TokenManager(TokenConfig(driver="redis", url=redis_url, prefix=f"{prefix}:t"))
    queues = QueueManager(QueueConfig(driver="saq", url=redis_url, prefix=f"{prefix}:q"))
    with use_token_manager(tokens), use_queue(queues):
        report = await probe({"tokens": check_tokens, "queue": check_queue})
    await tokens.close()
    await queues.close()

    assert report.ok, report.as_dict()


@pytest.mark.redis
async def test_check_tokens_and_check_queue_fail_against_nothing(namespace_suffix: str) -> None:
    prefix = f"keel-test:{namespace_suffix}"
    tokens = TokenManager(TokenConfig(driver="redis", url=UNREACHABLE_REDIS, prefix=f"{prefix}:t"))
    queues = QueueManager(QueueConfig(driver="saq", url=UNREACHABLE_REDIS, prefix=f"{prefix}:q"))
    with use_token_manager(tokens), use_queue(queues):
        report = await probe({"tokens": check_tokens, "queue": check_queue})
    await tokens.close()
    await queues.close()

    assert [result.ok for result in report.checks] == [False, False]
