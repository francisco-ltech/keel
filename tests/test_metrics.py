"""Metrics, read back from the real exposition.

There is no fake registry and no assertion on an instrument's internals: every
test here does what a scraper does — renders the registry and reads the text —
because the exposition format is the contract, and a counter that increments
but renders under the wrong name or label is a dashboard that stays flat.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import ClassVar

import pytest
from sqlalchemy import text

from keel.cache import CacheConfig, CacheManager, StoreConfig
from keel.cache.events import CacheHit, CacheMissed
from keel.database import Database, DatabaseConfig, use_database
from keel.exceptions import ConfigurationError
from keel.observability import (
    UNMATCHED_ROUTE,
    CheckResult,
    HealthReport,
    Metrics,
    MetricsConfig,
    bound_metrics,
    current_metrics,
    metrics_lifespan,
    use_metrics,
)
from keel.observability.metrics import _operation
from keel.queue import Envelope, Job, QueueConfig, dispatch, queue_lifespan
from keel.queue.worker import (
    JobDeadLettered,
    JobRecovered,
    JobStarted,
    JobSucceeded,
    JobUnroutable,
    Worker,
    WorkerFaulted,
)
from keel.support.events import EventDispatcher

pytestmark = [pytest.mark.anyio]

ON: MetricsConfig = MetricsConfig(enabled=True)


@dataclass(frozen=True, slots=True)
class Announce(Job):
    """A job with nothing to do; only its dispatch is of interest."""

    target: str

    max_attempts: ClassVar[int] = 1

    async def handle(self) -> None:
        """Do nothing."""


@pytest.fixture
def metrics() -> Metrics:
    return Metrics(ON)


@pytest.fixture
def events() -> EventDispatcher:
    return EventDispatcher()


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    instance = Database(DatabaseConfig(url=database_url))
    with use_database(instance):
        yield instance
    await instance.close()


def exposition(metrics: Metrics) -> str:
    """Render the registry the way a scraper reads it."""
    body, content_type = metrics.render()
    assert content_type.startswith("text/plain")
    return body.decode()


def sample(rendered: str, name: str, **labels: str) -> float | None:
    """Return one sample's value from the exposition, or ``None`` if absent."""
    # The exposition sorts labels by name, whatever order the instrument declared.
    wanted = ",".join(f'{key}="{value}"' for key, value in sorted(labels.items()))
    labelled = re.escape("{" + wanted + "}") if labels else ""
    found = re.search(rf"^{re.escape(name)}{labelled} (\S+)$", rendered, re.M)
    return float(found.group(1)) if found else None


# -- what the application hands in ------------------------------------------


def test_a_request_is_counted_and_timed_by_route_template(metrics: Metrics) -> None:
    metrics.request("GET", "/users/{id}", 200, 0.02)
    metrics.request("GET", "/users/{id}", 200, 0.03)
    metrics.request("GET", UNMATCHED_ROUTE, 404, 0.001)

    rendered = exposition(metrics)
    assert (
        sample(
            rendered, "keel_http_requests_total", method="GET", route="/users/{id}", status="200"
        )
        == 2
    )
    assert (
        sample(rendered, "keel_http_requests_total", method="GET", route="unmatched", status="404")
        == 1
    )
    assert (
        sample(
            rendered, "keel_http_request_duration_seconds_count", method="GET", route="/users/{id}"
        )
        == 2
    )
    assert (
        sample(
            rendered,
            "keel_http_request_duration_seconds_bucket",
            method="GET",
            route="/users/{id}",
            le="0.025",
        )
        == 1
    )


def test_a_readiness_report_is_counted_per_check(metrics: Metrics) -> None:
    report = HealthReport(
        (
            CheckResult("database", True, 0.004),
            CheckResult("cache", False, 1.0, error="ConnectionError"),
        )
    )
    metrics.readiness(report)

    rendered = exposition(metrics)
    assert sample(rendered, "keel_readiness_checks_total", check="database", result="ok") == 1
    assert sample(rendered, "keel_readiness_checks_total", check="cache", result="failed") == 1
    assert sample(rendered, "keel_readiness_check_duration_seconds_count", check="cache") == 1


def test_the_process_collectors_are_in_the_registry(metrics: Metrics) -> None:
    rendered = exposition(metrics)
    assert "python_info" in rendered
    assert "python_gc_objects_collected_total" in rendered
    if os.path.exists("/proc"):  # the process collector reads procfs, so nothing on macOS
        assert "process_resident_memory_bytes" in rendered


def test_two_instances_do_not_share_a_registry() -> None:
    first, second = Metrics(ON), Metrics(ON)
    first.request("GET", "/", 200, 0.1)

    assert (
        sample(
            exposition(second), "keel_http_requests_total", method="GET", route="/", status="200"
        )
        is None
    )


# -- sources -----------------------------------------------------------------


@pytest.mark.postgres
async def test_statements_are_counted_and_timed_by_operation(
    metrics: Metrics, database: Database
) -> None:
    detach = metrics.watch(database=database)
    try:
        async with database.transaction() as session:
            await session.execute(text("SELECT 1"))
            await session.execute(text("select 2"))
            await session.execute(text("CREATE TEMP TABLE keel_metrics_probe (v int)"))
            await session.execute(text("EXPLAIN SELECT 3"))
    finally:
        detach()

    rendered = exposition(metrics)
    assert sample(rendered, "keel_db_queries_total", operation="SELECT") == 2
    assert sample(rendered, "keel_db_queries_total", operation="CREATE") == 1
    assert sample(rendered, "keel_db_queries_total", operation="OTHER") == 1
    assert sample(rendered, "keel_db_query_duration_seconds_count", operation="SELECT") == 2
    assert "SELECT 1" not in rendered, "a statement is never a label"


@pytest.mark.postgres
async def test_detaching_stops_counting(metrics: Metrics, database: Database) -> None:
    metrics.watch(database=database)()
    async with database.transaction() as session:
        await session.execute(text("SELECT 4"))

    assert sample(exposition(metrics), "keel_db_queries_total", operation="SELECT") is None


@pytest.mark.parametrize(
    ("statement", "operation"),
    [
        ("SELECT 1", "SELECT"),
        ("  \n insert into t values (1)", "INSERT"),
        ("WITH x AS (SELECT 1) SELECT * FROM x", "WITH"),
        ("EXPLAIN ANALYZE SELECT 1", "OTHER"),
        ("", "OTHER"),
    ],
)
def test_the_operation_label_is_bounded(statement: str, operation: str) -> None:
    assert _operation(statement) == operation


async def test_cache_round_trips_are_counted_by_store_and_outcome(
    metrics: Metrics, events: EventDispatcher
) -> None:
    manager = CacheManager(
        CacheConfig(stores={"default": StoreConfig(driver="array", prefix="t")}), events
    )
    detach = metrics.watch(events=events)
    try:
        store = manager.store()
        await store.get("k")
        await store.put("k", 1)
        await store.get("k")
        assert await store.remember("slow", lambda: 42, single_flight=True) == 42
    finally:
        detach()
        await manager.close()

    rendered = exposition(metrics)
    assert sample(rendered, "keel_cache_operations_total", store="default", operation="miss") == 3
    assert sample(rendered, "keel_cache_operations_total", store="default", operation="hit") == 1
    assert sample(rendered, "keel_cache_operations_total", store="default", operation="write") == 2
    assert sample(rendered, "keel_cache_operations_total", store="default", operation="lock") == 1
    assert sample(rendered, "keel_cache_operations_total", store="default", operation="unlock") == 1
    assert "k" not in re.findall(r'key="([^"]*)"', rendered), "a key is never a label"


async def test_dispatches_are_counted_by_queue_and_deferral(
    metrics: Metrics, events: EventDispatcher
) -> None:
    detach = metrics.watch(events=events)
    try:
        async with queue_lifespan(QueueConfig(driver="null"), events):
            await dispatch(Announce("a"))
            await dispatch(Announce("b"), on="mail")
    finally:
        detach()

    rendered = exposition(metrics)
    assert sample(rendered, "keel_jobs_dispatched_total", queue="default", deferred="false") == 1
    assert sample(rendered, "keel_jobs_dispatched_total", queue="mail", deferred="false") == 1


async def test_worker_events_are_counted_by_job_and_outcome(
    metrics: Metrics, events: EventDispatcher
) -> None:
    envelope = Envelope.seal(Announce("x"))
    detach = metrics.watch(events=events)
    try:
        await events.dispatch(JobStarted(worker="w", envelope=envelope))
        await events.dispatch(JobSucceeded(worker="w", envelope=envelope, duration=0.3))
        await events.dispatch(WorkerFaulted(worker="w", activity="reserve", error=OSError()))
        await events.dispatch(JobRecovered(worker="w", lane="default", job_id="j1"))
    finally:
        detach()

    rendered = exposition(metrics)
    assert sample(rendered, "keel_jobs_started_total", job="Announce") == 1
    assert sample(rendered, "keel_jobs_total", job="Announce", outcome="succeeded") == 1
    assert sample(rendered, "keel_jobs_total", job="Announce", outcome="started") is None
    assert sample(rendered, "keel_job_duration_seconds_bucket", job="Announce", le="0.5") == 1
    assert sample(rendered, "keel_worker_faults_total", activity="reserve") == 1
    assert sample(rendered, "keel_jobs_recovered_total", lane="default") == 1
    assert envelope.id not in rendered, "a job id is never a label"


async def test_a_watched_worker_exposes_its_liveness_as_gauges(metrics: Metrics) -> None:
    """Read off the worker at scrape time, not tracked by event arithmetic."""
    worker = Worker(QueueConfig(driver="saq", url="redis://127.0.0.1:1/0"), handle_signals=False)
    detach = metrics.watch(worker=worker)

    rendered = exposition(metrics)
    assert sample(rendered, "keel_worker_jobs_in_flight") == 0
    since = sample(rendered, "keel_worker_seconds_since_queue_answered")
    assert since is not None and 0 <= since < 5

    detach()
    assert "keel_worker_jobs_in_flight" not in exposition(metrics), (
        "an API process has no worker line"
    )


# -- lifecycle ---------------------------------------------------------------


async def test_a_disabled_instance_subscribes_to_nothing_and_needs_no_extra(
    events: EventDispatcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Off means off: a process nothing scrapes need not even install the extra."""
    monkeypatch.setitem(sys.modules, "prometheus_client", None)
    async with metrics_lifespan(MetricsConfig(enabled=False), events=events) as off:
        off.watch(events=events)
        off.request("GET", "/", 200, 0.1)
        await events.dispatch(CacheHit("default", "k"))

        assert not events.has_listeners()
        with pytest.raises(ConfigurationError, match="disabled"):
            off.registry  # noqa: B018 — the property is the check
        with pytest.raises(ConfigurationError, match="disabled"):
            off.render()


@pytest.mark.postgres
async def test_the_lifespan_watches_everything_and_undoes_it(
    database: Database, events: EventDispatcher
) -> None:
    async with metrics_lifespan(ON, database=database, events=events) as bound:
        assert bound_metrics() is bound and current_metrics() is bound
        async with database.transaction() as session:
            await session.execute(text("SELECT 5"))
        await events.dispatch(CacheMissed("default", "k"))

    assert bound_metrics() is None
    assert not events.has_listeners()
    async with database.transaction() as session:
        await session.execute(text("SELECT 6"))
    rendered = exposition(bound)
    assert sample(rendered, "keel_db_queries_total", operation="SELECT") == 1
    assert sample(rendered, "keel_cache_operations_total", store="default", operation="miss") == 1


async def test_the_override_binds_for_the_block(metrics: Metrics) -> None:
    with pytest.raises(ConfigurationError, match="metrics_lifespan"):
        current_metrics()
    with use_metrics(metrics):
        assert current_metrics() is metrics
    assert bound_metrics() is None


@pytest.mark.postgres
async def test_watching_twice_counts_once_and_detaching_twice_is_harmless(
    metrics: Metrics, database: Database
) -> None:
    first = metrics.watch(database=database)
    second = metrics.watch(database=database)
    async with database.transaction() as session:
        await session.execute(text("SELECT 7"))
    assert sample(exposition(metrics), "keel_db_queries_total", operation="SELECT") == 1

    metrics.detach()
    first()
    second()
    metrics.detach()


def test_a_missing_extra_says_what_to_install(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "prometheus_client", None)
    with pytest.raises(ConfigurationError, match=r"keel\[metrics\]"):
        Metrics(ON)


def test_from_env() -> None:
    assert MetricsConfig.from_env({}) == MetricsConfig(enabled=True)
    assert MetricsConfig.from_env({"METRICS_ENABLED": "false"}) == MetricsConfig(enabled=False)
    assert MetricsConfig.from_env({"METRICS_ENABLED": "Off"}) == MetricsConfig(enabled=False)
    assert MetricsConfig.from_env({"METRICS_ENABLED": "1"}) == MetricsConfig(enabled=True)


def test_the_sample_helper_reads_what_it_should() -> None:
    """The helper above is load-bearing for every other test; pin it."""
    rendered = 'keel_x_total{a="1",b="2"} 3.0\nkeel_y 4.0\n'
    assert sample(rendered, "keel_x_total", a="1", b="2") == 3.0
    assert sample(rendered, "keel_x_total", a="1") is None
    assert sample(rendered, "keel_y") == 4.0


def test_worker_gauges_read_live_values(metrics: Metrics) -> None:
    worker = Worker(QueueConfig(driver="saq", url="redis://127.0.0.1:1/0"), handle_signals=False)
    metrics.watch(worker=worker)
    before = sample(exposition(metrics), "keel_worker_seconds_since_queue_answered")
    time.sleep(0.05)
    after = sample(exposition(metrics), "keel_worker_seconds_since_queue_answered")
    assert before is not None and after is not None and after > before


# -- what the review caught --------------------------------------------------


def test_an_unknown_method_is_one_series(metrics: Metrics) -> None:
    """A parser that accepts PROPFIND accepts a series per word a client invents."""
    for method in ("PROPFIND", "MKCALENDAR", "purge", "get"):
        metrics.request(method, "/health", 405, 0.001)

    rendered = exposition(metrics)
    assert (
        sample(rendered, "keel_http_requests_total", method="OTHER", route="/health", status="405")
        == 3
    )
    assert (
        sample(rendered, "keel_http_requests_total", method="GET", route="/health", status="405")
        == 1
    )
    assert "PROPFIND" not in rendered


@pytest.mark.postgres
async def test_a_failed_statement_is_still_counted(metrics: Metrics, database: Database) -> None:
    """Throughput that drops during an incident is a dashboard lying."""
    detach = metrics.watch(database=database)
    try:
        async with database.transaction() as session:
            with pytest.raises(Exception, match="does_not_exist"):
                await session.execute(text("SELECT does_not_exist_fn()"))
        async with database.transaction() as session:
            await session.execute(text("SELECT 1"))
    finally:
        detach()

    rendered = exposition(metrics)
    assert sample(rendered, "keel_db_queries_total", operation="SELECT") == 2
    assert sample(rendered, "keel_db_errors_total", operation="SELECT") == 1
    assert sample(rendered, "keel_db_query_duration_seconds_count", operation="SELECT") == 1


async def test_one_delivery_is_one_terminal_outcome(
    metrics: Metrics, events: EventDispatcher
) -> None:
    """An unroutable job that expires is announced twice; it is counted once."""
    envelope = Envelope.seal(Announce("x"))
    detach = metrics.watch(events=events)
    try:
        await events.dispatch(JobUnroutable(worker="w", envelope=envelope, retry_in=None))
        await events.dispatch(JobDeadLettered(worker="w", envelope=envelope))
        await events.dispatch(JobUnroutable(worker="w", envelope=envelope, retry_in=30.0))
    finally:
        detach()

    rendered = exposition(metrics)
    assert sample(rendered, "keel_jobs_total", job="Announce", outcome="dead_lettered") == 1
    assert sample(rendered, "keel_jobs_total", job="Announce", outcome="unroutable") == 1


@pytest.mark.postgres
async def test_one_holder_letting_go_does_not_stop_another(
    metrics: Metrics, database: Database
) -> None:
    first = metrics.watch(database=database)
    metrics.watch(database=database)
    first()
    async with database.transaction() as session:
        await session.execute(text("SELECT 8"))

    assert sample(exposition(metrics), "keel_db_queries_total", operation="SELECT") == 1


def test_a_dispatch_only_process_does_not_import_the_worker_runtime() -> None:
    """The lazy-export rule keel.queue keeps, checked in a fresh interpreter."""
    probe = "\n".join(
        [
            "import asyncio, sys",
            "from keel.observability import MetricsConfig, metrics_lifespan",
            "from keel.support.events import EventDispatcher",
            "async def main():",
            "    async with metrics_lifespan(MetricsConfig(), events=EventDispatcher()):",
            "        print('saq' in sys.modules, 'keel.queue.worker' in sys.modules)",
            "asyncio.run(main())",
        ]
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["False", "False"], result.stdout
