"""The cron scheduler.

The test that matters is ``test_schedulers_racing_on_one_due_entry_run_it_once``.
Everything else in this file supports it.

It is built to be hard to pass by accident. Five schedulers, each with its **own**
``Database`` — a separate engine and a separate connection pool, which is what a
replica actually is; sharing one pool would let two "replicas" get the same
backend and make the advisory lock trivially re-entrant. They tick concurrently
in an anyio task group against real Postgres, on ten distinct due instants, and
the assertion is on the exact push count per instant, not merely "at least one".

Each round then ticks a **sixth** time, sequentially, after every lock has been
released. That half is what separates the two mechanisms: a design relying on the
advisory lock alone passes the concurrent half and fails here, because the lock
is long gone by then and only the committed claim row can stop a second run.

The lock-release tests probe from a different engine, so they are asking a
different Postgres backend whether the lock is free — the only phrasing of the
question that means anything, since advisory locks are re-entrant within a
session.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, cast

import anyio
import pytest
from sqlalchemy import Table, text

from keel.database import Database, DatabaseConfig, Model, set_database, uow
from keel.queue import Envelope, Job, QueueConfig, QueueManager, use_queue
from keel.queue.fake import FakeQueue
from keel.queue.scheduler import (
    DEFAULT_CATCH_UP,
    LOCK_NAMESPACE,
    CronError,
    CronTrigger,
    DuplicateEntryError,
    IntervalTrigger,
    Schedule,
    ScheduleEntry,
    ScheduleError,
    Scheduler,
    ScheduleRun,
    ScheduleRuns,
    _CronFields,
    entry_lock_key,
)
from keel.testing import fake_queue

pytestmark = [pytest.mark.anyio]

BASE = datetime(2026, 9, 12, 3, 0, tzinfo=UTC)
"""A fixed instant, so nothing in this file depends on when it is run."""


@dataclass(frozen=True, slots=True)
class PruneExports(Job):
    """The canonical recurring job."""

    name: ClassVar[str] = "keel_test_prune_exports"

    async def handle(self) -> None:
        """Never runs; the scheduler's job is to dispatch, not to execute."""


@dataclass(frozen=True, slots=True)
class RefreshIndex(Job):
    """A second recurring job, for schedules with more than one entry."""

    name: ClassVar[str] = "keel_test_refresh_index"

    shard: str = "default"

    async def handle(self) -> None:
        """Never runs."""


class ExplodingQueue:
    """A queue that refuses every push, standing in for "the broker is down"."""

    def __init__(self, name: str = "exploding") -> None:
        self._name = name
        self.attempts = 0

    @property
    def name(self) -> str:
        return self._name

    async def push(self, envelope: Envelope) -> str:
        self.attempts += 1
        raise ConnectionError("broker unreachable")

    async def push_many(self, envelopes: Sequence[Envelope]) -> list[str]:
        raise ConnectionError("broker unreachable")

    async def size(self, queue: str | None = None) -> int:
        return 0

    async def clear(self, queue: str | None = None) -> int:
        return 0

    async def close(self) -> None:
        return None


def bind_queue(driver: str, connection: object) -> Any:
    """Bind an arbitrary queue object as the default connection."""
    manager = QueueManager(QueueConfig(driver=driver))
    manager.extend(driver, lambda _name: cast("Any", connection))
    return use_queue(manager)


async def lock_is_free(probe: Database, name: str) -> bool:
    """Ask a *different* backend whether an entry's advisory lock is unheld.

    Asked by taking the lock rather than by reading ``pg_locks``: the encoding of
    a bigint key across ``classid``/``objid`` is an implementation detail, and
    taking the lock is the same question the scheduler asks.

    Args:
        probe: A database whose pool is not the scheduler's, so the answer is
            not "yes, because advisory locks are re-entrant in one session".
        name: The schedule entry's name.

    Returns:
        ``True`` if the lock was free, having been taken and released again.
    """
    key = entry_lock_key(name)
    async with probe.engine.connect() as connection:
        session = await connection.execution_options(isolation_level="AUTOCOMMIT")
        acquired = bool(
            (
                await session.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": key})
            ).scalar_one()
        )
        if acquired:
            await session.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
        return acquired


# -- triggers, parsing and the schedule: no services needed ---------------


def test_an_interval_trigger_lands_on_a_grid_every_replica_agrees_on() -> None:
    trigger = IntervalTrigger(timedelta(minutes=5))
    moment = datetime(2026, 9, 12, 3, 7, 30, tzinfo=UTC)
    assert trigger.previous_due(moment) == datetime(2026, 9, 12, 3, 5, tzinfo=UTC)
    assert trigger.next_due(moment) == datetime(2026, 9, 12, 3, 10, tzinfo=UTC)
    # Anchored at the epoch, so two schedulers booted at different times agree.
    assert IntervalTrigger(timedelta(minutes=5)).previous_due(moment) == trigger.previous_due(
        moment
    )


def test_an_interval_trigger_refuses_a_non_positive_interval() -> None:
    with pytest.raises(ScheduleError, match="must be positive"):
        IntervalTrigger(timedelta(0))


def test_an_interval_trigger_has_not_fired_before_its_anchor() -> None:
    trigger = IntervalTrigger(timedelta(hours=1), anchor=BASE)
    assert trigger.previous_due(BASE - timedelta(minutes=1)) is None
    assert trigger.next_due(BASE - timedelta(minutes=1)) == BASE


def test_a_cron_trigger_includes_the_instant_it_is_asked_about() -> None:
    trigger = CronTrigger("0 3 * * *")
    # previous_due is inclusive; next_due is strictly after. The distinction is
    # what lets a tick landing exactly on 03:00 run 03:00 rather than yesterday's.
    assert trigger.previous_due(BASE) == BASE
    assert trigger.next_due(BASE) == BASE + timedelta(days=1)
    assert trigger.previous_due(BASE - timedelta(seconds=1)) == BASE - timedelta(days=1)


def test_a_bad_cron_expression_fails_when_it_is_declared() -> None:
    with pytest.raises(CronError):
        CronTrigger("not a cron expression")
    with pytest.raises(CronError):
        Schedule().cron("0 3 * *", PruneExports())


@pytest.mark.parametrize(
    "expression",
    [
        "0 3 * * *",
        "*/5 * * * *",
        "0 0 * * 1",
        "30 2 1 * *",
        "0 9-17/2 * * mon-fri",
        "0 0 13 * 5",
        "15,45 * * * *",
        "0 0 29 2 *",
        "0 12 * jan-mar *",
        "0 0 * * 7",
    ],
)
@pytest.mark.parametrize(
    "moment",
    [
        datetime(2026, 9, 12, 3, 0, tzinfo=UTC),
        datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 2, 28, 23, 59, tzinfo=UTC),
        datetime(2027, 7, 4, 14, 37, tzinfo=UTC),
    ],
)
def test_the_builtin_parser_agrees_with_croniter(expression: str, moment: datetime) -> None:
    """The fallback is only trustworthy if it answers what croniter answers.

    Exercised directly rather than through ``CronTrigger``, because croniter is
    installed in this environment and would otherwise shadow the branch that
    runs when it is not.
    """
    croniter = pytest.importorskip("croniter").croniter
    fields = _CronFields.parse(expression)
    assert fields.next_due(moment) == croniter(expression, moment).get_next(datetime)
    assert fields.previous_due(moment) == croniter(
        expression, moment + timedelta(seconds=60)
    ).get_prev(datetime)


def test_the_builtin_parser_applies_crons_day_of_month_or_day_of_week_rule() -> None:
    # Both fields restricted means OR, not AND — the most-missed rule in cron.
    fields = _CronFields.parse("0 0 13 * 5")
    assert fields.next_due(datetime(2026, 1, 1, tzinfo=UTC)) == datetime(2026, 1, 2, tzinfo=UTC)
    # Only one restricted means it alone decides.
    assert _CronFields.parse("0 0 13 * *").next_due(datetime(2026, 1, 1, tzinfo=UTC)) == datetime(
        2026, 1, 13, tzinfo=UTC
    )


def test_the_builtin_parser_gives_up_on_an_impossible_expression() -> None:
    assert _CronFields.parse("0 0 30 2 *").next_due(datetime(2026, 1, 1, tzinfo=UTC)) is None


@pytest.mark.parametrize(
    "expression",
    ["0 3 * *", "0 3 * * * *", "99 3 * * *", "0 3 * * xyz", "5-1 3 * * *", "0 3 * * 1/0"],
)
def test_the_builtin_parser_rejects_nonsense(expression: str) -> None:
    with pytest.raises(CronError):
        _CronFields.parse(expression)


def test_a_schedule_is_inert_declarative_data() -> None:
    schedule = (
        Schedule()
        .cron("0 3 * * *", PruneExports())
        .every(timedelta(minutes=5), RefreshIndex("primary"), name="refresh-primary")
        .every(300.0, RefreshIndex("secondary"), name="refresh-secondary")
    )
    assert len(schedule) == 3
    assert PruneExports.name in schedule
    assert [entry.name for entry in schedule] == [
        PruneExports.name,
        "refresh-primary",
        "refresh-secondary",
    ]
    assert schedule.entries[0].catch_up == DEFAULT_CATCH_UP
    assert "0 3 * * *" in str(schedule)


def test_two_entries_may_not_share_a_name() -> None:
    schedule = Schedule().cron("0 3 * * *", PruneExports())
    with pytest.raises(DuplicateEntryError, match="advisory"):
        schedule.cron("0 4 * * *", PruneExports())


def test_a_lock_key_is_stable_and_positive() -> None:
    key = entry_lock_key("nightly-prune")
    assert key == entry_lock_key("nightly-prune")
    assert 0 < key < 2**63
    assert entry_lock_key("nightly-prune") != entry_lock_key("nightly-prune ")
    assert LOCK_NAMESPACE == "keel.queue.scheduler"


# -- against real Postgres ------------------------------------------------


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    """A live database with the claim table created and dropped around it."""
    tables = [cast("Table", ScheduleRun.__table__)]
    instance = Database(DatabaseConfig(url=database_url))
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.create_all, tables=tables)
    set_database(instance)
    yield instance
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.drop_all, tables=tables)
    set_database(None)
    await instance.close()


@pytest.fixture
async def replicas(database: Database, database_url: str) -> AsyncIterator[list[Database]]:
    """Five databases with independent pools, standing in for five replicas.

    Independent on purpose: two schedulers sharing a pool could be handed the
    same backend, and a Postgres advisory lock is re-entrant within one session
    — so the race would be won by construction rather than by the lock.
    """
    instances = [Database(DatabaseConfig(url=database_url)) for _ in range(5)]
    yield instances
    for instance in instances:
        await instance.close()


def entry_for(
    due: datetime, *, name: str, catch_up: timedelta | None = DEFAULT_CATCH_UP
) -> Schedule:
    """A one-entry schedule whose latest due instant is exactly *due*."""
    return Schedule().add(
        ScheduleEntry(
            name=name,
            trigger=IntervalTrigger(timedelta(days=365), anchor=due),
            job=PruneExports(),
            catch_up=catch_up,
        )
    )


@pytest.mark.postgres
async def test_schedulers_racing_on_one_due_entry_run_it_once(
    database: Database,
    replicas: list[Database],
) -> None:
    rounds = 10
    schedule = Schedule().add(
        ScheduleEntry(
            name="racing-prune",
            trigger=IntervalTrigger(timedelta(minutes=5), anchor=BASE),
            job=PruneExports(),
        )
    )
    failures: list[BaseException] = []
    schedulers = [
        Scheduler(schedule, database=replica, on_error=lambda exc, _entry: failures.append(exc))
        for replica in replicas
    ]

    with fake_queue() as queued:
        for index in range(rounds):
            due = BASE + timedelta(minutes=5 * index)
            moment = due + timedelta(seconds=1)

            before = len(queued.pushed)
            async with anyio.create_task_group() as tasks:
                for scheduler in schedulers:
                    tasks.start_soon(scheduler.tick, moment)

            assert not failures, f"a scheduler errored: {failures}"
            assert len(queued.pushed) - before == 1, (
                f"round {index}: five concurrent schedulers dispatched "
                f"{len(queued.pushed) - before} jobs for {due.isoformat()}"
            )

            # The lock is released by now, so anything that still refuses to
            # run this instant is doing so because of the committed claim.
            assert await schedulers[0].tick(moment) == []
            assert len(queued.pushed) - before == 1

        assert len(queued.pushed) == rounds
        assert {envelope.job for envelope in queued.pushed} == {PruneExports.name}

    async with uow() as session:
        assert await ScheduleRuns(session).count() == rounds


@pytest.mark.postgres
async def test_a_tick_dispatches_a_due_entry_and_records_the_claim(
    database: Database,
) -> None:
    schedule = entry_for(BASE, name="claim-recorded")
    scheduler = Scheduler(schedule, database=database)

    with fake_queue() as queued:
        assert await scheduler.tick(BASE) == ["claim-recorded"]

    assert len(queued.pushed) == 1
    async with uow() as session:
        assert await ScheduleRuns(session).last_run("claim-recorded") == BASE


@pytest.mark.postgres
async def test_the_advisory_lock_is_released_after_a_run(
    database: Database,
    replicas: list[Database],
) -> None:
    scheduler = Scheduler(entry_for(BASE, name="released-after-run"), database=database)
    with fake_queue() as queued:
        assert await scheduler.tick(BASE) == ["released-after-run"]
    assert len(queued.pushed) == 1
    assert await lock_is_free(replicas[0], "released-after-run")


@pytest.mark.postgres
async def test_the_advisory_lock_is_released_after_a_failing_run(
    database: Database,
    replicas: list[Database],
) -> None:
    failures: list[BaseException] = []
    scheduler = Scheduler(
        entry_for(BASE, name="released-after-failure"),
        database=database,
        on_error=lambda exc, _entry: failures.append(exc),
    )

    broken = ExplodingQueue()
    with bind_queue("exploding", broken):
        assert await scheduler.tick(BASE) == []

    assert broken.attempts == 1
    assert [type(error) for error in failures] == [ConnectionError]
    assert await lock_is_free(replicas[0], "released-after-failure")

    # And the claim was compensated, so the instant is reconsidered rather than
    # recorded as run.
    async with uow() as session:
        assert await ScheduleRuns(session).last_run("released-after-failure") is None

    with fake_queue() as queued:
        assert await scheduler.tick(BASE) == ["released-after-failure"]
    assert len(queued.pushed) == 1


@pytest.mark.postgres
async def test_one_failing_entry_does_not_end_the_tick(database: Database) -> None:
    schedule = Schedule().add(
        ScheduleEntry(
            name="broken-trigger",
            trigger=cast("Any", _RaisingTrigger()),
            job=PruneExports(),
        )
    )
    schedule.add(
        ScheduleEntry(
            name="healthy",
            trigger=IntervalTrigger(timedelta(days=365), anchor=BASE),
            job=RefreshIndex("primary"),
        )
    )
    failures: list[BaseException] = []
    scheduler = Scheduler(
        schedule, database=database, on_error=lambda exc, _entry: failures.append(exc)
    )

    with fake_queue() as queued:
        assert await scheduler.tick(BASE) == ["healthy"]

    assert len(queued.pushed) == 1
    assert [type(error) for error in failures] == [ZeroDivisionError]


class _RaisingTrigger:
    """A trigger that fails, to prove one entry cannot take down a tick."""

    def previous_due(self, moment: datetime) -> datetime | None:
        raise ZeroDivisionError("this trigger is broken")

    def next_due(self, moment: datetime) -> datetime:
        raise ZeroDivisionError("this trigger is broken")


# -- missed ticks ---------------------------------------------------------


@pytest.mark.postgres
async def test_a_missed_tick_runs_late_inside_the_catch_up_window(
    database: Database,
) -> None:
    """The documented default: a deploy that spanned 03:00 does not skip 03:00."""
    scheduler = Scheduler(entry_for(BASE, name="late-but-run"), database=database)
    with fake_queue() as queued:
        assert await scheduler.tick(BASE + timedelta(minutes=30)) == ["late-but-run"]
    assert len(queued.pushed) == 1


@pytest.mark.postgres
async def test_a_missed_tick_is_abandoned_beyond_the_catch_up_window(
    database: Database,
) -> None:
    """And a scheduler booting eleven hours later does not run last night's job."""
    scheduler = Scheduler(entry_for(BASE, name="too-late"), database=database)
    with fake_queue() as queued:
        assert await scheduler.tick(BASE + timedelta(hours=11)) == []
    assert queued.pushed == ()
    async with uow() as session:
        assert await ScheduleRuns(session).count() == 0


@pytest.mark.postgres
async def test_catch_up_none_refuses_to_run_late(database: Database) -> None:
    schedule = entry_for(BASE, name="wall-clock-only", catch_up=None)
    scheduler = Scheduler(schedule, database=database, tick_interval=5.0)

    with fake_queue() as queued:
        # A minute late is late; the digest is no longer worth sending.
        assert await scheduler.tick(BASE + timedelta(minutes=1)) == []
        assert queued.pushed == ()
        # Within one tick interval is not "late", it is just the loop's period.
        assert await scheduler.tick(BASE + timedelta(seconds=3)) == ["wall-clock-only"]
        assert len(queued.pushed) == 1


@pytest.mark.postgres
async def test_only_the_most_recent_missed_instant_is_caught_up(
    database: Database,
) -> None:
    """A minutely job down for half an hour fires once on recovery, not thirty times."""
    schedule = Schedule().add(
        ScheduleEntry(
            name="minutely",
            trigger=IntervalTrigger(timedelta(minutes=1), anchor=BASE),
            job=PruneExports(),
        )
    )
    scheduler = Scheduler(schedule, database=database)

    with fake_queue() as queued:
        recovered = BASE + timedelta(minutes=30)
        assert await scheduler.tick(recovered) == ["minutely"]
        assert len(queued.pushed) == 1

    async with uow() as session:
        assert await ScheduleRuns(session).last_run("minutely") == recovered


@pytest.mark.postgres
async def test_an_entry_that_has_never_been_due_does_nothing(database: Database) -> None:
    schedule = Schedule().add(
        ScheduleEntry(
            name="not-yet",
            trigger=IntervalTrigger(timedelta(hours=1), anchor=BASE),
            job=PruneExports(),
        )
    )
    scheduler = Scheduler(schedule, database=database)
    with fake_queue() as queued:
        assert await scheduler.tick(BASE - timedelta(minutes=1)) == []
    assert queued.pushed == ()


# -- claim housekeeping ---------------------------------------------------


@pytest.mark.postgres
async def test_a_claim_can_only_be_taken_once(database: Database) -> None:
    async with uow() as session:
        runs = ScheduleRuns(session)
        assert await runs.claim("once", BASE) is True
        # The savepoint keeps the losing insert from poisoning this transaction:
        # the caller must still be able to work afterwards.
        assert await runs.claim("once", BASE) is False
        assert await runs.count() == 1
        assert await runs.last_run("once") == BASE


@pytest.mark.postgres
async def test_claims_are_pruned_by_cutoff(database: Database) -> None:
    async with uow() as session:
        runs = ScheduleRuns(session)
        for days in (30, 10, 1):
            await runs.claim("prunable", BASE - timedelta(days=days))

    async with uow() as session:
        assert await ScheduleRuns(session).prune(BASE - timedelta(days=7)) == 2

    async with uow() as session:
        assert await ScheduleRuns(session).count() == 1


@pytest.mark.postgres
async def test_claim_pruning_refuses_a_naive_cutoff(database: Database) -> None:
    async with uow() as session:
        with pytest.raises(ValueError, match="aware datetime"):
            await ScheduleRuns(session).prune(datetime(2020, 1, 1))


def test_a_scheduler_describes_itself() -> None:
    scheduler = Scheduler(Schedule().cron("0 3 * * *", PruneExports()), tick_interval=5.0)
    assert repr(scheduler) == "<Scheduler 1 entries every 5.0s>"
    assert len(scheduler.schedule) == 1


def test_an_empty_schedule_says_so() -> None:
    assert str(Schedule()) == "(no scheduled jobs)"
    assert repr(Schedule()) == "<Schedule 0 entries>"
    assert FakeQueue("probe").pushed == ()
