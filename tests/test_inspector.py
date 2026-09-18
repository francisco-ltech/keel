"""The request inspector, against the real sources it records from.

The claim to pin is that **one trace holds everything one unit of work did, and
nothing another unit of work did** — queries from a real engine, cache events
from a real dispatcher, dispatches through the real ``dispatch()``, and log
records through the real ``logging`` module. The SQL half runs against Postgres
because the engine hook fires inside SQLAlchemy's greenlet, and whether a
context variable is visible there is exactly the kind of thing a fake would
get right by accident.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, ClassVar

import anyio
import pytest
from sqlalchemy import text

from keel.cache import CacheConfig, CacheManager, StoreConfig
from keel.cache.events import CacheHit, CacheMissed, KeyWritten
from keel.database import Database, DatabaseConfig, use_database
from keel.exceptions import ConfigurationError
from keel.observability import (
    Inspector,
    InspectorConfig,
    bound_inspector,
    correlate,
    current_inspector,
    current_trace,
    inspector_lifespan,
    trace,
    use_inspector,
)
from keel.observability.inspector import SUMMARY_LIMIT, _TraceHandler
from keel.queue import Job, QueueConfig, dispatch, dispatch_many, queue_lifespan
from keel.support.events import EventDispatcher

pytestmark = [pytest.mark.anyio]

ON: InspectorConfig = InspectorConfig(enabled=True)


@dataclass(frozen=True, slots=True)
class Notify(Job):
    """A job with nothing to do; only its dispatch is of interest."""

    target: str

    max_attempts: ClassVar[int] = 1

    async def handle(self) -> None:
        """Do nothing."""


# -- fixtures --------------------------------------------------------------


@pytest.fixture
def inspector() -> Inspector:
    return Inspector(ON)


@pytest.fixture
def events() -> EventDispatcher:
    return EventDispatcher()


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    """A bound database, closed afterwards."""
    instance = Database(DatabaseConfig(url=database_url))
    with use_database(instance):
        yield instance
    await instance.close()


def kinds(inspector: Inspector, kind: str) -> list[Any]:
    """Return the entries of one kind from the most recent trace."""
    return [entry for entry in inspector.recent[-1].entries if entry.kind == kind]


# -- the trace -------------------------------------------------------------


async def test_a_trace_carries_the_correlation_fields_and_its_tags(inspector: Inspector) -> None:
    with correlate(request_id="r-1"), inspector.trace("GET /users") as recorded:
        assert recorded is not None
        assert current_trace() is recorded
        recorded.tag(status=200, route=None)

    assert current_trace() is None
    finished = inspector.recent[-1]
    assert finished is recorded
    assert finished.fields == {"request_id": "r-1", "status": "200"}
    assert finished.duration is not None and finished.duration >= 0


async def test_a_trace_is_json_serialisable(inspector: Inspector) -> None:
    with inspector.trace("work") as recorded:
        assert recorded is not None
        recorded.record("custom", "did a thing", duration=0.01, count=3, nested={"a": [1]})

    listing = json.dumps(inspector.recent[-1].as_dict(entries=False))
    full = json.loads(json.dumps(inspector.recent[-1].as_dict()))
    assert "entries" not in listing
    assert full["counts"] == {"custom": 1}
    assert full["entries"][0]["detail"] == {"count": 3, "nested": {"a": [1]}}


async def test_a_full_trace_counts_what_it_cannot_keep() -> None:
    small = Inspector(InspectorConfig(enabled=True, max_entries=2))
    with small.trace("loop") as recorded:
        assert recorded is not None
        for index in range(5):
            recorded.record("custom", f"entry {index}")

    assert [entry.summary for entry in small.recent[-1].entries] == ["entry 0", "entry 1"]
    assert small.recent[-1].dropped == 3


async def test_a_summary_is_bounded(inspector: Inspector) -> None:
    with inspector.trace("long") as recorded:
        assert recorded is not None
        recorded.record("custom", "x" * (SUMMARY_LIMIT * 2))

    assert len(inspector.recent[-1].entries[0].summary) == SUMMARY_LIMIT


async def test_only_the_most_recent_traces_are_retained() -> None:
    two = Inspector(InspectorConfig(enabled=True, retain=2))
    ids = []
    for name in ("first", "second", "third"):
        with two.trace(name) as recorded:
            assert recorded is not None
            ids.append(recorded.id)

    assert [kept.name for kept in two.recent] == ["second", "third"]
    assert two.find(ids[0]) is None
    found = two.find(ids[2])
    assert found is not None and found.name == "third"
    two.clear()
    assert not two.recent


async def test_concurrent_traces_do_not_see_each_other(inspector: Inspector) -> None:
    """The reason it is a context variable and not an attribute on the inspector."""
    seen: dict[str, list[str]] = {}

    async def work(name: str) -> None:
        with inspector.trace(name) as recorded:
            assert recorded is not None
            for _ in range(3):
                recorded.record("custom", name)
                await anyio.sleep(0.001)
            seen[name] = [entry.summary for entry in recorded.entries]

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(work, "a")
        tasks.start_soon(work, "b")

    assert seen == {"a": ["a", "a", "a"], "b": ["b", "b", "b"]}
    assert len(inspector.recent) == 2


async def test_a_nested_trace_is_its_own(inspector: Inspector) -> None:
    with inspector.trace("outer") as outer:
        assert outer is not None
        with inspector.trace("inner") as inner:
            assert inner is not None
            inner.record("custom", "inside")
        assert current_trace() is outer
        outer.record("custom", "outside")

    assert [kept.name for kept in inspector.recent] == ["inner", "outer"]
    assert [entry.summary for entry in inspector.recent[-1].entries] == ["outside"]


# -- disabled, and unbound -------------------------------------------------


async def test_a_disabled_inspector_records_and_subscribes_to_nothing(
    events: EventDispatcher,
) -> None:
    off = Inspector(InspectorConfig(enabled=False))
    handlers_before = list(logging.getLogger().handlers)

    detach = off.watch(events=events, logs=True)
    with off.trace("nothing") as recorded:
        assert recorded is None
        assert current_trace() is None
        await events.dispatch(CacheHit("default", "k"))
    detach()

    assert not off.recent
    assert not events.has_listeners()
    assert logging.getLogger().handlers == handlers_before


async def test_the_facade_is_a_no_op_when_nothing_is_bound() -> None:
    assert bound_inspector() is None
    with pytest.raises(ConfigurationError, match="inspector_lifespan"):
        current_inspector()
    with trace("anything") as recorded:
        assert recorded is None


async def test_the_facade_records_on_the_bound_inspector(inspector: Inspector) -> None:
    with use_inspector(inspector), trace("bound") as recorded:
        assert recorded is not None
        assert current_inspector() is inspector

    assert inspector.recent[-1].name == "bound"


# -- configuration ---------------------------------------------------------


def test_from_env_reads_every_knob() -> None:
    config = InspectorConfig.from_env(
        {
            "INSPECTOR_ENABLED": "Yes",
            "INSPECTOR_RETAIN": "7",
            "INSPECTOR_MAX_ENTRIES": "9",
            "INSPECTOR_PARAMETERS": "1",
        }
    )
    assert config == InspectorConfig(enabled=True, retain=7, max_entries=9, parameters=True)
    assert InspectorConfig.from_env({}) == InspectorConfig()


@pytest.mark.parametrize(
    "env",
    [{"INSPECTOR_RETAIN": "0"}, {"INSPECTOR_MAX_ENTRIES": "many"}, {"INSPECTOR_RETAIN": "-1"}],
)
def test_a_count_that_keeps_nothing_is_refused(env: dict[str, str]) -> None:
    with pytest.raises(ConfigurationError, match="INSPECTOR_"):
        InspectorConfig.from_env(env)


# -- sources ---------------------------------------------------------------


@pytest.mark.postgres
async def test_queries_are_recorded_with_their_duration(
    inspector: Inspector, database: Database
) -> None:
    """The engine hook runs in SQLAlchemy's greenlet; the trace must be visible there."""
    detach = inspector.watch(database=database)
    try:
        with inspector.trace("query") as recorded:
            assert recorded is not None
            async with database.transaction() as session:
                await session.execute(text("SELECT 1 AS probe"))
    finally:
        detach()

    queries = kinds(inspector, "query")
    probe = [entry for entry in queries if entry.summary == "SELECT 1 AS probe"]
    assert len(probe) == 1, [entry.summary for entry in queries]
    assert probe[0].duration is not None and probe[0].duration > 0
    assert probe[0].detail["statement"] == "SELECT 1 AS probe"
    assert "parameters" not in probe[0].detail, "bind parameters are off by default"


@pytest.mark.postgres
async def test_parameters_are_recorded_only_when_asked(database: Database) -> None:
    curious = Inspector(InspectorConfig(enabled=True, parameters=True))
    detach = curious.watch(database=database)
    try:
        with curious.trace("query") as recorded:
            assert recorded is not None
            async with database.transaction() as session:
                await session.execute(text("SELECT :secret AS value"), {"secret": "hunter2"})
    finally:
        detach()

    query = next(entry for entry in kinds(curious, "query") if "value" in entry.summary)
    assert "hunter2" in json.dumps(query.detail["parameters"])


@pytest.mark.postgres
async def test_a_query_outside_a_trace_is_not_recorded(
    inspector: Inspector, database: Database
) -> None:
    detach = inspector.watch(database=database)
    try:
        async with database.transaction() as session:
            await session.execute(text("SELECT 2"))
    finally:
        detach()

    assert not inspector.recent


@pytest.mark.postgres
async def test_detaching_stops_the_engine_hook(inspector: Inspector, database: Database) -> None:
    inspector.watch(database=database)()
    with inspector.trace("after") as recorded:
        assert recorded is not None
        async with database.transaction() as session:
            await session.execute(text("SELECT 3"))

    assert kinds(inspector, "query") == []


async def test_cache_operations_are_recorded_without_their_values(
    inspector: Inspector, events: EventDispatcher
) -> None:
    """Through a real manager, so the decorator's events are the ones observed."""
    manager = CacheManager(
        CacheConfig(stores={"default": StoreConfig(driver="array", prefix="t")}), events
    )
    detach = inspector.watch(events=events)
    try:
        with inspector.trace("cache") as recorded:
            assert recorded is not None
            store = manager.store()
            assert await store.get("profile:42") is None
            await store.put("profile:42", {"password": "hunter2"}, ttl=60)
            assert await store.get("profile:42") == {"password": "hunter2"}
    finally:
        detach()
        await manager.close()

    cached = kinds(inspector, "cache")
    assert [entry.summary for entry in cached] == [
        "miss profile:42",
        "write profile:42",
        "hit profile:42",
    ]
    assert cached[1].detail == {"store": "default", "key": "profile:42", "ttl": 60.0}
    assert "hunter2" not in json.dumps([entry.as_dict() for entry in cached])


async def test_every_cache_event_reads_as_a_verb(
    inspector: Inspector, events: EventDispatcher
) -> None:
    detach = inspector.watch(events=events)
    try:
        with inspector.trace("verbs"):
            await events.dispatch(CacheMissed("default", "a"))
            await events.dispatch(KeyWritten("default", "a", value=1, ttl=None))
            await events.dispatch(CacheHit("default", "a", value=1))
    finally:
        detach()

    assert [entry.summary for entry in kinds(inspector, "cache")] == ["miss a", "write a", "hit a"]


async def test_a_dispatch_is_recorded_with_whether_it_waited(
    inspector: Inspector, events: EventDispatcher
) -> None:
    detach = inspector.watch(events=events)
    try:
        async with queue_lifespan(QueueConfig(driver="null"), events):
            with inspector.trace("dispatch") as recorded:
                assert recorded is not None
                await dispatch(Notify("now"))
                await dispatch(Notify("later"), delay=30)
                await dispatch_many([Notify("a"), Notify("b")])
    finally:
        detach()

    jobs = kinds(inspector, "job")
    assert [entry.summary for entry in jobs] == [
        "dispatch Notify to default",
        "dispatch Notify to default in 30s",
        "dispatch Notify to default",
        "dispatch Notify to default",
    ]
    assert all(entry.detail["deferred"] is False for entry in jobs)
    assert jobs[1].detail["delay"] == 30.0
    assert jobs[0].detail["connection"] == "null"


@pytest.mark.postgres
async def test_a_dispatch_inside_a_unit_of_work_is_recorded_as_deferred(
    inspector: Inspector, events: EventDispatcher, database: Database
) -> None:
    detach = inspector.watch(events=events)
    try:
        async with queue_lifespan(QueueConfig(driver="null"), events):
            with inspector.trace("uow") as recorded:
                assert recorded is not None
                async with database.transaction():
                    await dispatch(Notify("deferred"))
                    await dispatch(Notify("immediate"), after_commit=False)
    finally:
        detach()

    jobs = kinds(inspector, "job")
    # The immediate push comes first: the deferred one is announced at the commit.
    assert [entry.detail["deferred"] for entry in jobs] == [False, True]
    assert jobs[1].summary.endswith("after commit")


async def test_log_records_are_recorded_on_the_trace(inspector: Inspector) -> None:
    detach = inspector.watch(logs=True)
    logger = logging.getLogger("tests.inspector.logs")
    try:
        logger.warning("outside")
        with inspector.trace("logs"):
            logger.warning("inside %s", "the trace")
            try:
                raise ValueError("boom")
            except ValueError:
                logger.exception("with a traceback")
    finally:
        detach()

    lines = kinds(inspector, "log")
    assert [entry.summary for entry in lines] == [
        "WARNING tests.inspector.logs: inside the trace",
        "ERROR tests.inspector.logs: with a traceback",
    ]
    assert lines[1].detail == {
        "level": "ERROR",
        "logger": "tests.inspector.logs",
        "exception": True,
    }
    assert not any(isinstance(h, _TraceHandler) for h in logging.getLogger().handlers)


async def test_a_record_that_cannot_format_still_lands(inspector: Inspector) -> None:
    """Called directly: pytest's own capture handler re-raises a bad format string."""
    record = logging.LogRecord(
        "tests.inspector.bad", logging.WARNING, __file__, 1, "%d items", ("no",), None
    )
    with inspector.trace("bad format"):
        _TraceHandler().emit(record)

    assert kinds(inspector, "log")[0].summary == "WARNING tests.inspector.bad: %d items"


# -- the lifespan ----------------------------------------------------------


@pytest.mark.postgres
async def test_the_lifespan_watches_everything_and_undoes_it(
    database: Database, events: EventDispatcher
) -> None:
    logger = logging.getLogger("tests.inspector.lifespan")
    async with inspector_lifespan(ON, database=database, events=events) as bound:
        assert bound_inspector() is bound
        with trace("everything") as recorded:
            assert recorded is not None
            async with database.transaction() as session:
                await session.execute(text("SELECT 4"))
            await events.dispatch(CacheHit("default", "k"))
            logger.warning("hello")
        assert set(recorded.counts()) == {"query", "cache", "log"}

    assert bound_inspector() is None
    assert not events.has_listeners()
    assert not any(isinstance(h, _TraceHandler) for h in logging.getLogger().handlers)
    with bound.trace("after") as later:
        assert later is not None
        async with database.transaction() as session:
            await session.execute(text("SELECT 5"))
        logger.warning("after")
    assert later.entries == [], "the lifespan's exit must detach every source"


async def test_a_disabled_lifespan_costs_nothing(events: EventDispatcher) -> None:
    async with inspector_lifespan(InspectorConfig(), events=events) as bound:
        assert not bound.enabled
        assert not events.has_listeners()
        with trace("off") as recorded:
            assert recorded is None


# -- what the review caught -------------------------------------------------


@pytest.mark.postgres
async def test_echoed_statements_do_not_smuggle_parameters_onto_the_trace(
    database_url: str,
) -> None:
    """`DB_ECHO=true` logs every bind parameter; `parameters=False` must still hold."""
    echoing = Database(DatabaseConfig(url=database_url, echo=True))
    inspector = Inspector(InspectorConfig(enabled=True, parameters=False))
    detach = inspector.watch(database=echoing, logs=True)
    try:
        with inspector.trace("echo"):
            async with echoing.transaction() as session:
                await session.execute(text("SELECT :pw AS p"), {"pw": "hunter2-the-password"})
    finally:
        detach()
        await echoing.close()

    assert "hunter2" not in json.dumps(inspector.recent[-1].as_dict())
    assert kinds(inspector, "query"), "the statement itself is still recorded"


@pytest.mark.postgres
async def test_a_bulk_statement_cannot_blow_the_trace_budget(database: Database) -> None:
    from keel.observability.inspector import STATEMENT_LIMIT

    curious = Inspector(InspectorConfig(enabled=True, parameters=True))
    detach = curious.watch(database=database)
    try:
        with curious.trace("bulk"):
            async with database.transaction() as session:
                await session.execute(text("CREATE TEMP TABLE keel_inspector_probe (v text)"))
                await session.execute(
                    text("INSERT INTO keel_inspector_probe (v) VALUES (:v)"),
                    [{"v": "x" * 200}] * 2000,
                )
    finally:
        detach()

    insert = next(entry for entry in kinds(curious, "query") if "INSERT" in entry.summary)
    assert insert.detail["executemany"] is True
    assert len(json.dumps(insert.detail)) < 3 * STATEMENT_LIMIT
    assert "chars]" in insert.detail["parameters"], "the clip says how much there was"


@pytest.mark.postgres
async def test_a_dispatch_on_a_rolled_back_unit_of_work_is_not_shown_as_dispatched(
    inspector: Inspector, events: EventDispatcher, database: Database
) -> None:
    """A timeline that shows a job the queue never received is worse than none."""
    detach = inspector.watch(events=events)
    try:
        async with queue_lifespan(QueueConfig(driver="null"), events):
            with inspector.trace("rollback"), pytest.raises(RuntimeError, match="abandon"):
                async with database.transaction():
                    await dispatch(Notify("never"))
                    raise RuntimeError("abandon")
            with inspector.trace("commit") as committed:
                assert committed is not None
                async with database.transaction():
                    await dispatch(Notify("eventually"))
                    assert committed.entries == [], "announced at the commit, not before"
    finally:
        detach()

    rolled_back, committed_trace = inspector.recent[-2], inspector.recent[-1]
    assert [entry.kind for entry in rolled_back.entries] == []
    jobs = [entry for entry in committed_trace.entries if entry.kind == "job"]
    assert len(jobs) == 1 and jobs[0].detail["deferred"] is True


async def test_single_flight_shows_the_lock_and_its_wait(
    inspector: Inspector, events: EventDispatcher
) -> None:
    """Two identical misses with five unexplained seconds between them explain nothing."""
    manager = CacheManager(
        CacheConfig(stores={"default": StoreConfig(driver="array", prefix="t")}), events
    )
    detach = inspector.watch(events=events)
    try:
        store = manager.store()
        with inspector.trace("single flight"):
            assert await store.remember("slow", lambda: 42, single_flight=True) == 42
    finally:
        detach()
        await manager.close()

    summaries = [entry.summary for entry in kinds(inspector, "cache")]
    assert summaries == [
        "miss slow",
        "lock remember:slow",
        "miss slow",
        "write slow",
        "unlock remember:slow",
    ]
    lock = kinds(inspector, "cache")[1]
    assert "owner" not in lock.detail and "waited" in lock.detail


async def test_a_waited_lock_says_how_long(inspector: Inspector, events: EventDispatcher) -> None:
    manager = CacheManager(
        CacheConfig(stores={"default": StoreConfig(driver="array", prefix="t")}), events
    )
    detach = inspector.watch(events=events)
    try:
        holder = manager.store().lock("busy", ttl=5)
        assert await holder.acquire()

        async def let_go() -> None:
            await anyio.sleep(0.15)
            await holder.release()

        with inspector.trace("waiting"):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(let_go)
                waiter = manager.store().lock("busy", ttl=5)
                await waiter.block(2.0, poll=0.01)
                await waiter.release()
    finally:
        detach()
        await manager.close()

    waited = [entry for entry in kinds(inspector, "cache") if "after waiting" in entry.summary]
    assert len(waited) == 1
    assert waited[0].detail["waited"] >= 0.1


async def test_a_task_that_outlives_its_trace_cannot_write_into_it(inspector: Inspector) -> None:
    released = anyio.Event()

    async def straggler() -> None:
        await released.wait()
        recorded = current_trace()
        assert recorded is not None, "the task inherited the trace"
        assert recorded.record("custom", "too late") is None

    async with anyio.create_task_group() as tasks:
        with inspector.trace("request") as recorded:
            assert recorded is not None
            tasks.start_soon(straggler)
            recorded.record("custom", "in time")
        released.set()

    assert [entry.summary for entry in inspector.recent[-1].entries] == ["in time"]
    assert inspector.recent[-1].dropped == 0


async def test_a_trace_is_retained_even_when_its_reset_fails(inspector: Inspector) -> None:
    """Exited in another task: the reset raises, and the trace must still be kept."""
    opened = inspector.trace("split")
    recorded = opened.__enter__()
    assert recorded is not None

    async def close_elsewhere() -> None:
        with pytest.raises(ValueError, match="different Context"):
            opened.__exit__(None, None, None)

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(close_elsewhere)

    assert inspector.recent[-1] is recorded
    assert recorded.duration is not None
    assert recorded.record("custom", "after") is None


async def test_an_application_recorded_detail_is_coerced(inspector: Inspector) -> None:
    """Decision 5 invites `record(..., response=whatever)`; the endpoint must not 500."""
    with inspector.trace("http") as recorded:
        assert recorded is not None
        recorded.record("http", "GET https://api", response=object(), when={1: {2, 3}})

    rendered = json.loads(json.dumps(inspector.recent[-1].as_dict()))
    assert rendered["entries"][0]["detail"]["response"].startswith("<object object")
    assert rendered["entries"][0]["detail"]["when"] == {"1": [2, 3]}


@pytest.mark.postgres
async def test_detaching_twice_is_harmless_and_watching_twice_records_once(
    inspector: Inspector, database: Database
) -> None:
    first = inspector.watch(database=database)
    second = inspector.watch(database=database)
    with inspector.trace("twice") as recorded:
        assert recorded is not None
        async with database.transaction() as session:
            await session.execute(text("SELECT 99"))
    assert [entry.summary for entry in kinds(inspector, "query") if "99" in entry.summary] == [
        "SELECT 99"
    ]

    inspector.detach()
    first()
    second()
    inspector.detach()


async def test_a_synchronously_run_job_is_announced_before_its_own_entries(
    inspector: Inspector, events: EventDispatcher
) -> None:
    @dataclass(frozen=True, slots=True)
    class Work(Job):
        token: str

        max_attempts: ClassVar[int] = 1

        async def handle(self) -> None:
            recorded = current_trace()
            assert recorded is not None
            recorded.record("custom", f"handler ran for {self.token}")

    detach = inspector.watch(events=events)
    try:
        async with queue_lifespan(QueueConfig(driver="sync"), events):
            with inspector.trace("sync"):
                await dispatch(Work("1"))
    finally:
        detach()

    assert [entry.kind for entry in inspector.recent[-1].entries] == ["job", "custom"]


async def test_the_trace_name_is_bounded(inspector: Inspector) -> None:
    with inspector.trace("GET /" + "a" * 9000):
        pass

    assert len(inspector.recent[-1].name) == SUMMARY_LIMIT
