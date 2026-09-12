"""Recurring jobs, declared in code and run exactly once across N replicas.

    schedule = (
        Schedule()
        .cron("0 3 * * *", PruneExports())
        .every(timedelta(minutes=5), RefreshSearchIndex())
    )

    async with queue_lifespan(...), database_lifespan(...):
        await Scheduler(schedule).run()

Three separations carry the design, and each one is a named pattern doing work
rather than decorating a loop.

**Strategy — :class:`Trigger`.** "When is this next due?" has two answers people
actually want (a cron expression and a fixed interval) and a third they
sometimes need (their own). The scheduler asks; the trigger answers. This is the
same shape as :mod:`keel.queue.backoff`, for the same reason: the algorithm
varies independently of the thing that calls it.

**Command — the job.** A schedule entry holds a
:class:`~keel.queue.job.Job` instance, already parameterised, which is what lets
a schedule be inert data. Nothing is executed by declaring it.

**Schedule is data; :class:`Scheduler` is behaviour.** A :class:`Schedule` is an
immutable-ish collection of :class:`ScheduleEntry` values with no clock, no
database and no queue. It can be built at import time, unit-tested with no
services, printed by a CLI, or diffed between releases. The runner is a separate
object precisely so that "what runs when" is reviewable without reading a loop.

## Exactly-once across replicas

N replicas hold the same schedule. A job due at 03:00 must run once. Two
mechanisms, doing two different jobs — conflating them is the usual bug:

1. **A Postgres advisory lock excludes concurrent replicas.** It is *only*
   mutual exclusion. On its own it does not give exactly-once: replica A takes
   the lock, dispatches, releases; replica B takes it a millisecond later and,
   knowing nothing, dispatches the same due instant again.
2. **A row in ``keel_schedule_runs``, unique on ``(entry, due_at)``, is the
   durable record that a due instant has been claimed.** *That* is what makes it
   exactly-once, and it survives the lock being released, the process dying, and
   the replica set being rescheduled onto other nodes.

The lock is not therefore decorative. Without it, every replica would race to
insert the same row, N-1 of them would take a unique-violation on every tick,
and the losers would still have paid for a transaction and a savepoint rollback
each time. It converts a contended write into a single ``pg_try_advisory_lock``
that returns false.

**Locking granularity: one lock per schedule entry, not one per tick and not one
per due instant.**

* *Per tick* (a single "I am the scheduler leader" lock) is simpler and wrong in
  a specific way: one replica then serialises every entry, so a schedule with a
  slow dispatch delays unrelated entries behind it, and the benefit of running
  N replicas evaporates. It also makes the lock a leader election, which needs a
  lease and a fencing token to be correct, and this needs neither.
* *Per (entry, due instant)* would mint a fresh key every minute for every
  entry. Advisory locks are cheap, but the keys are what an operator greps for
  in ``pg_locks`` during an incident, and a key space that changes every tick is
  not greppable. The due instant is already in the claim row, which is where a
  value that varies belongs.

Per entry is the granularity at which contention actually exists.

The lock is **session-level on a dedicated AUTOCOMMIT connection**, exactly as
:func:`keel.database.migrations.upgrade` does it, and released in a ``finally``
that invalidates the connection if the release itself fails — a pooled
connection handed back still holding a lock would deadlock every later tick
against a lock nobody is deliberately holding. That helper is not imported from
:mod:`keel.database.migrations` because importing that module pulls in Alembic,
an optional extra that a worker process deliberately does not carry; the
approach, the key derivation and the recovery are copied verbatim.

Session-level rather than ``pg_try_advisory_xact_lock``, despite the transaction
variant releasing itself: the lock has to be held *across* the claim's commit
and the subsequent push, and a transaction-scoped lock is gone at commit —
precisely when the push has not happened yet.

## Missed ticks

**Default: run late, up to :data:`DEFAULT_CATCH_UP` (one hour).**

The overwhelmingly common reason a scheduler was down over a due time is a
deploy, a restart or a node eviction, all of which last seconds to minutes. A
nightly prune that did not run because 03:00 landed inside a rolling update
should still prune; skipping it means the work silently does not happen and
nobody finds out until the disk fills. So the scheduler looks at the most recent
due instant *at or before now* and runs it if nobody has claimed it.

Two bounds keep that from being reckless:

* Only the **most recent** missed instant is considered, never the backlog. A
  five-minute job that was down for a day would otherwise fire 288 times in one
  tick — a thundering herd built out of a recovery, which is the same failure
  the jitter in :mod:`keel.queue.backoff` exists to prevent.
* Lateness beyond ``catch_up`` is abandoned rather than run. The window is also
  what stops a *first* boot from immediately running everything: a scheduler
  starting at 14:00 sees 03:00 as eleven hours late and leaves it alone.

``catch_up=None`` is the other behaviour, for work whose value is tied to the
wall clock — a 07:00 "good morning" digest is worse than useless at 09:00. It
means "only run if the due instant is within one tick", i.e. do not run late at
all.

Declined, deliberately:

* **A transactional outbox.** It would close the last window (the process dying
  between the claim commit and the push) at the cost of a relay process and a
  second table. A cron scheduler is not a payments ledger; the window is one
  process death wide, and a missed run is visible as a claim row whose job never
  appeared.
* **Singleton / leader election.** ADR 0000 already rules Singleton out, and a
  leader election is a per-tick lock with extra machinery — see above.
* **Observer for tick notifications.** ``on_error`` is a callback because there
  is one event and one listener. An :class:`~keel.support.events.EventDispatcher`
  here would be ceremony.
* **Depending on SAQ's own cron support.** It exists, but it ties the schedule to
  one driver and to that driver's idea of exactly-once, and Keel's queue
  contract is deliberately driver-agnostic.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Protocol, Self

from sqlalchemy import DateTime, String, UniqueConstraint, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, mapped_column

from keel.database import current_database
from keel.database.model import Model, TimestampMixin, UUIDPrimaryKey, utcnow
from keel.database.repository import Repository
from keel.exceptions import KeelError
from keel.queue.dispatch import dispatch
from keel.queue.job import Job

_croniter: Any | None
"""``croniter``'s entry point, or ``None`` when it is not installed.

Optional rather than a dependency: it reaches Keel only as a transitive
requirement of SAQ, so an application on another driver — or none — would
otherwise get an ``ImportError`` out of a scheduler that has nothing to do with
SAQ. :class:`_CronFields` covers the standard five-field syntax when it is
absent.
"""

try:  # pragma: no cover — one branch per environment, both are exercised in CI
    from croniter import croniter as _croniter_impl  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    _croniter = None
else:
    _croniter = _croniter_impl

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable, Iterator

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

    from keel.database.engine import Database

logger = logging.getLogger(__name__)

LOCK_NAMESPACE: Final = "keel.queue.scheduler"
"""The string every scheduler advisory-lock key is derived from.

A namespaced string rather than hand-picked integers, for the reason
:data:`keel.database.migrations.LOCK_NAMESPACE` gives: the key is then
self-documenting and a new one can be minted without anyone tracking which
numbers are taken.
"""

DEFAULT_TICK_INTERVAL: Final = 5.0
"""Seconds between ticks.

Cron's resolution is a minute, so this could be far larger. Five seconds instead
because the interval is also the tolerance for ``catch_up=None`` entries, and
because it bounds how late a run is when a replica happens to be mid-tick when
another one dies. The cost is one cheap query per entry per tick.
"""

DEFAULT_CATCH_UP: Final = timedelta(hours=1)
"""How late a missed run may still be started. See the module docstring."""

DEFAULT_RUN_RETENTION: Final = timedelta(days=7)
"""How long claim rows are worth keeping.

They are the audit trail for "did the nightly job actually run", which is a
question asked about last night and last week, not last quarter. Keeping them
forever turns a coordination table into a growing one.
"""

_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
"""The anchor every :class:`IntervalTrigger` counts from.

Anchoring at a fixed instant rather than at process start is what makes N
replicas agree on *which* instants are due. Anchored at start-up, two replicas
booted forty seconds apart would compute two different five-minute grids, both
would be "due" at times the other was not, and the claim row would be the only
thing preventing a doubled run — turning the lock into decoration and the
schedule into something that shifts on every deploy.
"""


class ScheduleError(KeelError):
    """Base class for schedule-definition problems."""


class CronError(ScheduleError):
    """Raised when a cron expression cannot be parsed.

    Carries the expression and the field that failed, because a five-field
    string with one bad character gives no clue which field the parser objected
    to.
    """

    def __init__(self, expression: str, reason: str) -> None:
        self.expression = expression
        super().__init__(f"cannot parse cron expression {expression!r}: {reason}")


class DuplicateEntryError(ScheduleError):
    """Raised when two schedule entries claim the same name.

    Not tolerable rather than merely untidy: the name is both the advisory-lock
    key and half of the claim row's unique key, so two entries sharing one would
    make the first to run suppress the second, silently and forever.
    """

    def __init__(self, name: str) -> None:
        super().__init__(
            f"two schedule entries are named {name!r}; the name keys the advisory "
            f"lock and the run record, so pass `name=` to distinguish them"
        )


# -- triggers --------------------------------------------------------------


class Trigger(Protocol):
    """Answers when a schedule entry fires.

    Not ``runtime_checkable``, unlike :class:`keel.queue.backoff.Backoff`. That
    one has a single method with a trivial signature, so an ``isinstance`` check
    means something; this has two with a shared temporal contract that attribute
    names cannot express, and a check that only proves the names exist is the
    false assurance ADR 0000 records removing from the store contracts.
    """

    def previous_due(self, moment: datetime) -> datetime | None:
        """Return the latest firing instant at or before *moment*.

        This, not ``next_due``, is what the scheduler runs on: asking "what
        should have happened by now" is the only formulation that survives the
        scheduler being asleep, restarted or replaced between two instants.

        Args:
            moment: An aware datetime.

        Returns:
            The instant, or ``None`` if this trigger has never fired.
        """
        ...

    def next_due(self, moment: datetime) -> datetime:
        """Return the first firing instant strictly after *moment*.

        For diagnostics — "when does this next run" is what an operator asks of
        a schedule listing — and for tests.

        Args:
            moment: An aware datetime.

        Returns:
            The next instant.
        """
        ...


@dataclass(frozen=True, slots=True)
class IntervalTrigger:
    """Fires on a fixed grid, anchored at the Unix epoch.

    Attributes:
        interval: The spacing. Must be positive.
        anchor: The instant the grid is counted from. Defaults to the epoch; see
            :data:`_EPOCH` for why a fixed anchor rather than start-up.
    """

    interval: timedelta
    anchor: datetime = _EPOCH

    def __post_init__(self) -> None:
        """Reject an interval that would make the grid meaningless.

        Raises:
            ScheduleError: If the interval is not positive.
        """
        if self.interval <= timedelta(0):
            raise ScheduleError(f"interval must be positive, got {self.interval}")

    def previous_due(self, moment: datetime) -> datetime | None:
        """Return the grid instant at or before *moment*.

        Args:
            moment: An aware datetime.

        Returns:
            The instant, or ``None`` if *moment* precedes the anchor.
        """
        elapsed = moment - self.anchor
        if elapsed < timedelta(0):
            return None
        return self.anchor + self.interval * (elapsed // self.interval)

    def next_due(self, moment: datetime) -> datetime:
        """Return the first grid instant strictly after *moment*.

        Args:
            moment: An aware datetime.

        Returns:
            The next instant.
        """
        previous = self.previous_due(moment)
        if previous is None:
            return self.anchor
        return previous + self.interval

    def __str__(self) -> str:
        """Render for a schedule listing."""
        return f"every {self.interval}"


@dataclass(frozen=True, slots=True)
class CronTrigger:
    """Fires according to a five-field cron expression.

    Uses ``croniter`` when it is installed and a small built-in parser when it
    is not. The built-in is not a reimplementation for its own sake: ``croniter``
    reaches Keel only as a transitive dependency of SAQ, so an application using
    a different queue driver — or none — would otherwise get an ``ImportError``
    from a scheduler that has nothing to do with SAQ. It covers the standard
    five fields with ``*``, ``,``, ``-``, ``/`` and three-letter month and
    weekday names, which is every expression anyone writes by hand; the
    extensions (``@hourly``, ``L``, ``#``, seconds, a sixth field) are left to
    ``croniter``, and asking for one without it raises rather than guesses.

    Attributes:
        expression: The five-field expression, e.g. ``"0 3 * * *"``.
    """

    expression: str
    _fields: _CronFields | None = field(init=False, repr=False, compare=False, default=None)
    """The built-in parse, or ``None`` when ``croniter`` is doing the work.

    Not simply "parse it both ways": the built-in accepts a strict subset, so
    running it alongside ``croniter`` would reject expressions ``croniter``
    handles perfectly well and make the presence of an optional dependency
    change which schedules are legal.
    """

    def __post_init__(self) -> None:
        """Validate the expression eagerly, with whichever engine is in use.

        Eagerly, because a schedule is declared at import time and a typo in a
        cron string should fail the process that is starting rather than the
        tick that happens at 03:00 three weeks later.

        Raises:
            CronError: If the expression cannot be parsed.
        """
        if _croniter is None:
            object.__setattr__(self, "_fields", _CronFields.parse(self.expression))
        elif not _croniter.is_valid(self.expression):
            raise CronError(self.expression, "croniter does not recognise it")

    def previous_due(self, moment: datetime) -> datetime | None:
        """Return the latest firing instant at or before *moment*.

        Args:
            moment: An aware datetime.

        Returns:
            The instant, or ``None`` if the expression matches nothing within
            the search horizon (a genuinely impossible expression such as
            ``"0 0 30 2 *"``).
        """
        fields = self._fields
        if fields is None:
            # ``get_prev`` is strictly-before, so asking it about a moment that
            # is itself a firing instant would skip to the one before. Cron's
            # resolution is a minute, so truncating to the minute and stepping
            # one second into it makes the boundary inclusive without ever
            # reaching forward past *moment*.
            inside = moment.replace(second=1, microsecond=0)
            return _from_croniter(self.expression, inside, backwards=True)
        return fields.previous_due(moment)

    def next_due(self, moment: datetime) -> datetime:
        """Return the first firing instant strictly after *moment*.

        Args:
            moment: An aware datetime.

        Returns:
            The next instant.

        Raises:
            CronError: If the expression matches nothing within the search
                horizon.
        """
        fields = self._fields
        found = (
            _from_croniter(self.expression, moment, backwards=False)
            if fields is None
            else fields.next_due(moment)
        )
        if found is None:
            raise CronError(self.expression, "it matches no instant in the next four years")
        return found

    def __str__(self) -> str:
        """Render for a schedule listing."""
        return f"cron({self.expression})"


# -- the schedule ----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ScheduleEntry:
    """One recurring job: a trigger, a command, and how late it may run.

    Attributes:
        name: Identifies this entry in the advisory lock and the claim row.
            Defaults to the job's name. Change it only with the same care as
            renaming a job — the claim history is keyed on it, so a renamed
            entry looks brand new and may run once immediately.
        trigger: When it fires.
        job: What to dispatch. A parameterised instance, not a class, so the
            schedule can carry the arguments a recurring job needs.
        catch_up: How late a missed run may still be started, or ``None`` to
            never run late. See the module docstring.
        on: Override the job's declared queue.
        connection: Which queue connection to dispatch on.
    """

    name: str
    trigger: Trigger
    job: Job
    catch_up: timedelta | None = DEFAULT_CATCH_UP
    on: str | None = None
    connection: str | None = None

    def __str__(self) -> str:
        """Render as one line of a schedule listing."""
        return f"{self.name}: {self.trigger} -> {self.job}"


class Schedule:
    """The declared set of recurring jobs. Inert data.

    Holds no clock, no database and no queue, which is what lets it be built at
    import time, asserted on without services, and printed by a CLI. The fluent
    methods return ``self`` so a whole schedule is one expression::

        schedule = (
            Schedule()
            .cron("0 3 * * *", PruneExports())
            .every(timedelta(minutes=5), RefreshSearchIndex(), catch_up=None)
        )

    Args:
        entries: Entries to start from, for composing a schedule out of several
            modules' worth.
    """

    __slots__ = ("_entries",)

    def __init__(self, entries: Iterable[ScheduleEntry] = ()) -> None:
        self._entries: dict[str, ScheduleEntry] = {}
        for entry in entries:
            self.add(entry)

    def add(self, entry: ScheduleEntry) -> Self:
        """Add a fully-built entry.

        Args:
            entry: The entry.

        Returns:
            This schedule, for chaining.

        Raises:
            DuplicateEntryError: If an entry of the same name is already
                present.
        """
        if entry.name in self._entries:
            raise DuplicateEntryError(entry.name)
        self._entries[entry.name] = entry
        return self

    def cron(
        self,
        expression: str,
        job: Job,
        *,
        name: str | None = None,
        catch_up: timedelta | None = DEFAULT_CATCH_UP,
        on: str | None = None,
        connection: str | None = None,
    ) -> Self:
        """Declare a job on a cron expression.

        Args:
            expression: A five-field cron expression, e.g. ``"0 3 * * *"``.
            job: The job to dispatch, already parameterised.
            name: Entry name; defaults to the job's name.
            catch_up: How late a missed run may still start.
            on: Override the job's declared queue.
            connection: Which queue connection to dispatch on.

        Returns:
            This schedule, for chaining.
        """
        return self.add(
            ScheduleEntry(
                name=name or job.name,
                trigger=CronTrigger(expression),
                job=job,
                catch_up=catch_up,
                on=on,
                connection=connection,
            )
        )

    def every(
        self,
        interval: timedelta | float,
        job: Job,
        *,
        name: str | None = None,
        catch_up: timedelta | None = DEFAULT_CATCH_UP,
        on: str | None = None,
        connection: str | None = None,
    ) -> Self:
        """Declare a job on a fixed interval.

        Args:
            interval: A ``timedelta``, or seconds as a number.
            job: The job to dispatch, already parameterised.
            name: Entry name; defaults to the job's name.
            catch_up: How late a missed run may still start.
            on: Override the job's declared queue.
            connection: Which queue connection to dispatch on.

        Returns:
            This schedule, for chaining.
        """
        spacing = interval if isinstance(interval, timedelta) else timedelta(seconds=interval)
        return self.add(
            ScheduleEntry(
                name=name or job.name,
                trigger=IntervalTrigger(spacing),
                job=job,
                catch_up=catch_up,
                on=on,
                connection=connection,
            )
        )

    @property
    def entries(self) -> tuple[ScheduleEntry, ...]:
        """Every entry, in declaration order."""
        return tuple(self._entries.values())

    def __iter__(self) -> Iterator[ScheduleEntry]:
        """Iterate the entries in declaration order."""
        return iter(self._entries.values())

    def __len__(self) -> int:
        """How many entries are declared."""
        return len(self._entries)

    def __contains__(self, name: object) -> bool:
        """Whether an entry of this name is declared."""
        return name in self._entries

    def __str__(self) -> str:
        """Render the whole schedule, one entry per line — what a CLI prints."""
        if not self._entries:
            return "(no scheduled jobs)"
        return "\n".join(str(entry) for entry in self._entries.values())

    def __repr__(self) -> str:
        """Summarise without dumping every entry into a traceback."""
        return f"<Schedule {len(self._entries)} entries>"


# -- the durable record ----------------------------------------------------


class ScheduleRun(Model, UUIDPrimaryKey, TimestampMixin):
    """A claim on one entry's one due instant. The exactly-once guarantee.

    The unique constraint on ``(entry, due_at)`` is not a safety net behind the
    advisory lock — it is the guarantee, and the lock is the optimisation. Locks
    are held by processes and processes end; a committed row does not.

    No separate index on ``entry``: the unique constraint's B-tree already leads
    with it, so the "has this entry claimed this instant" lookup and the
    "everything this entry has run" listing both use it. ``due_at`` is indexed
    on its own for :meth:`ScheduleRuns.prune`, which deletes by cutoff across
    every entry.

    Attributes:
        entry: The schedule entry's name.
        due_at: The firing instant claimed, not the instant the claim was made —
            the two differ by however late the run is, which is exactly the
            distinction that makes a missed-tick policy auditable. ``created_at``
            records the latter.
    """

    __tablename__ = "keel_schedule_runs"
    __table_args__ = (UniqueConstraint("entry", "due_at"),)

    entry: Mapped[str] = mapped_column(String(255))
    due_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class ScheduleRuns(Repository[ScheduleRun]):
    """Claims and their housekeeping."""

    model = ScheduleRun

    async def claim(self, entry: str, due_at: datetime) -> bool:
        """Try to claim one entry's due instant.

        Written as insert-and-catch rather than check-then-insert. The check
        would be a lie: between the ``SELECT`` and the ``INSERT`` another
        replica can commit, and the whole point of this row is to be correct
        when that happens. The unique constraint is the only test that cannot be
        raced.

        The insert runs inside a **savepoint**. In Postgres a failed statement
        aborts the whole transaction, so without one a lost race would poison
        the caller's unit of work and turn a normal, expected outcome into an
        error the caller has to recover from.

        Args:
            entry: The schedule entry's name.
            due_at: The firing instant being claimed.

        Returns:
            ``True`` if this caller claimed it, ``False`` if someone already had.
        """
        savepoint = await self.session.begin_nested()
        try:
            self.session.add(ScheduleRun(entry=entry, due_at=due_at))
            await self.session.flush()
        except IntegrityError:
            await savepoint.rollback()
            return False
        await savepoint.commit()
        return True

    async def release(self, entry: str, due_at: datetime) -> bool:
        """Give a claim back, so the instant can be attempted again.

        The compensating action for a dispatch that failed after the claim
        committed. Without it the entry would be recorded as run when it was
        not, and no later tick would reconsider it.

        Args:
            entry: The schedule entry's name.
            due_at: The firing instant to un-claim.

        Returns:
            ``True`` if a claim was removed.
        """
        removed = await self.purge(ScheduleRun.entry == entry, ScheduleRun.due_at == due_at)
        return removed > 0

    async def last_run(self, entry: str) -> datetime | None:
        """Return the latest due instant claimed for *entry*.

        For a status command: "when did the nightly prune last run" is the
        question this table exists to answer for humans.

        Args:
            entry: The schedule entry's name.

        Returns:
            The instant, or ``None`` if it has never run.
        """
        statement = (
            self.query()
            .where(ScheduleRun.entry == entry)
            .order_by(ScheduleRun.due_at.desc())
            .limit(1)
        )
        found = await self.first_from(statement)
        return None if found is None else found.due_at

    async def prune(self, older_than: datetime | timedelta = DEFAULT_RUN_RETENTION) -> int:
        """Delete claims older than a cutoff.

        Args:
            older_than: An absolute cutoff, or an age subtracted from now.

        Returns:
            How many rows were removed.

        Raises:
            ValueError: If given a naive datetime — comparing one with a
                ``timestamptz`` column assumes a timezone, wrongly, on exactly
                the deployment that is not in UTC.
        """
        cutoff = utcnow() - older_than if isinstance(older_than, timedelta) else older_than
        if cutoff.tzinfo is None:
            raise ValueError("prune() needs an aware datetime; use keel.database.utcnow()")
        return await self.purge(ScheduleRun.due_at < cutoff)


def entry_lock_key(name: str) -> int:
    """Derive the advisory-lock key for a schedule entry.

    Derived the same way :data:`keel.database.migrations.MIGRATION_LOCK_KEY` is
    — ``sha256`` of a namespaced string, first eight bytes, top bit masked off —
    so the number is stable across every replica and positive, which is what
    keeps it readable in ``pg_locks`` rather than appearing as a large negative.

    Args:
        name: The schedule entry's name.

    Returns:
        A 63-bit key.
    """
    digest = hashlib.sha256(f"{LOCK_NAMESPACE}:{name}".encode()).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


class Scheduler:
    """Runs a :class:`Schedule`. The behaviour half of the split.

    Args:
        schedule: What to run.
        database: Where the claims live and where the advisory lock is taken.
            Defaults to the bound database, resolved per call so a scheduler can
            be constructed before the lifespan binds one.
        tick_interval: Seconds between ticks, and the tolerance for entries
            declared ``catch_up=None``.
        clock: Returns the current aware instant. Injected so the missed-tick
            behaviour can be asserted exactly rather than by sleeping.
        on_error: Called with any exception an entry raised and the entry it came
            from. One entry failing must not stop the rest of the tick, for the
            same reason one observer failing does not stop the others — so the
            failure is reported rather than propagated. Defaults to logging.
    """

    __slots__ = ("_clock", "_database", "_on_error", "_schedule", "_tick_interval")

    def __init__(
        self,
        schedule: Schedule,
        *,
        database: Database | None = None,
        tick_interval: float = DEFAULT_TICK_INTERVAL,
        clock: Callable[[], datetime] = utcnow,
        on_error: Callable[[BaseException, ScheduleEntry], None] | None = None,
    ) -> None:
        self._schedule = schedule
        self._database = database
        self._tick_interval = tick_interval
        self._clock = clock
        self._on_error = on_error or _log_entry_failure

    @property
    def schedule(self) -> Schedule:
        """The schedule this scheduler runs."""
        return self._schedule

    async def run(self) -> None:
        """Tick forever, until the surrounding task is cancelled.

        The loop is deliberately trivial: everything interesting is in
        :meth:`tick`, which is what makes the interesting part testable without
        a clock, a sleep or a cancellation.

        Cancellation is allowed to propagate — a scheduler that swallowed it
        would keep a shutting-down worker alive.
        """
        logger.info("scheduler started with %d entries", len(self._schedule))
        while True:
            await self.tick()
            await asyncio.sleep(self._tick_interval)

    async def tick(self, moment: datetime | None = None) -> list[str]:
        """Run every entry that is due and unclaimed.

        Args:
            moment: The instant to evaluate the schedule at. Defaults to the
                injected clock.

        Returns:
            The names of the entries dispatched, in schedule order. Empty is the
            normal case — most ticks find nothing due.
        """
        now = moment or self._clock()
        dispatched: list[str] = []
        for entry in self._schedule:
            try:
                if await self._run_if_due(entry, now):
                    dispatched.append(entry.name)
            except Exception as exc:  # noqa: BLE001 — one entry must not end the tick
                self._on_error(exc, entry)
        return dispatched

    async def _run_if_due(self, entry: ScheduleEntry, now: datetime) -> bool:
        """Claim and dispatch *entry* if its latest due instant is unhandled.

        The order of the four steps is the whole design:

        1. Work out the due instant and whether it is still within the entry's
           catch-up window. Both are pure arithmetic, so an entry that is not
           due costs no database round trip at all — which is what makes a
           five-second tick over a large schedule affordable.
        2. Take the advisory lock. Failing to get it is not an error: another
           replica has this entry, and the correct response is to move on.
        3. Claim the instant in its own committed transaction. The claim must be
           durable **before** the push, so that a crash between the two loses a
           run rather than duplicating one — for a scheduler that is the right
           way round, because a duplicate is the failure the feature promises
           not to produce and a miss is visible in the claim table.
        4. Push, with ``after_commit=False``. There is no transaction left to
           defer to, and the scheduler needs the push's outcome in hand: if it
           raises, the claim is released so the next tick reconsiders the
           instant. Deferring would hand the failure to a callback that cannot
           un-claim anything.

        Args:
            entry: The entry to consider.
            now: The instant to evaluate at.

        Returns:
            ``True`` if the job was dispatched.

        Raises:
            Exception: Whatever the dispatch raised, after the claim has been
                released.
        """
        due = entry.trigger.previous_due(now)
        if due is None or not self._within_catch_up(entry, due, now):
            return False

        async with self._entry_lock(entry.name) as acquired:
            if not acquired:
                logger.debug("entry %s is held by another replica", entry.name)
                return False

            async with self._transaction() as session:
                if not await ScheduleRuns(session).claim(entry.name, due):
                    return False

            try:
                await dispatch(
                    entry.job,
                    on=entry.on,
                    connection=entry.connection,
                    after_commit=False,
                )
            except Exception:
                await self._release_claim(entry.name, due)
                raise
            logger.info("dispatched %s for %s", entry.name, due.isoformat())
            return True

    def _within_catch_up(self, entry: ScheduleEntry, due: datetime, now: datetime) -> bool:
        """Whether a due instant is recent enough to still be worth running.

        Args:
            entry: The entry, carrying its catch-up window.
            due: The firing instant.
            now: The instant being evaluated at.

        Returns:
            ``True`` if the run should proceed.
        """
        window = entry.catch_up
        if window is None:
            # "Never run late" still has to tolerate the tick interval: the
            # scheduler only wakes every few seconds, so a strict `due == now`
            # would mean an entry fires only when a tick lands on its instant to
            # the microsecond, i.e. essentially never.
            window = timedelta(seconds=self._tick_interval)
        return timedelta(0) <= now - due <= window

    async def _release_claim(self, name: str, due: datetime) -> None:
        """Undo a claim whose dispatch failed, reporting rather than raising.

        Runs while the failing path is already unwinding, so an exception here
        would replace the dispatch failure — the one the operator needs — with a
        database error about the compensation.

        Args:
            name: The schedule entry's name.
            due: The firing instant to un-claim.
        """
        try:
            async with self._transaction() as session:
                await ScheduleRuns(session).release(name, due)
        except Exception:
            logger.exception(
                "dispatch of %s for %s failed and its claim could not be released; "
                "the entry will not be retried for that instant",
                name,
                due.isoformat(),
            )

    def _bound_database(self) -> Database:
        """Return the database to use.

        Returns:
            The injected database, or the bound one. Resolved per call rather
            than at construction, so a scheduler can be assembled before the
            lifespan binds anything — and so a test's context-local override
            (``keel.testing.rolled_back_database``) is honoured.
        """
        return self._database or current_database()

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        """Open a unit of work on the configured or bound database.

        Yields:
            A session inside a transaction.
        """
        async with self._bound_database().transaction() as session:
            yield session

    @asynccontextmanager
    async def _entry_lock(self, name: str) -> AsyncIterator[bool]:
        """Hold the advisory lock for one entry, or report that someone else has it.

        ``pg_try_advisory_lock`` rather than the polling wait
        :func:`keel.database.migrations.upgrade` uses, because the two waits mean
        opposite things. A replica waiting for the migration lock still has to
        migrate afterwards; a replica that cannot get *this* lock has nothing
        left to do — the entry is being handled — so waiting would only make the
        tick take longer before reaching the same conclusion.

        Args:
            name: The schedule entry's name.

        Yields:
            ``True`` if the lock was taken and is held for the block, ``False``
            if another session holds it.
        """
        key = entry_lock_key(name)
        async with self._bound_database().engine.connect() as connection:
            # AUTOCOMMIT so the lock is not parked inside an idle transaction:
            # one held for the length of a dispatch would hold back vacuum on
            # every table in the database.
            session = await connection.execution_options(isolation_level="AUTOCOMMIT")
            acquired = bool(
                (
                    await session.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": key})
                ).scalar_one()
            )
            if not acquired:
                yield False
                return
            try:
                yield True
            finally:
                await _release_lock(session, key)

    def __repr__(self) -> str:
        """Identify the scheduler by the size of its schedule."""
        return f"<Scheduler {len(self._schedule)} entries every {self._tick_interval}s>"


async def _release_lock(connection: AsyncConnection, key: int) -> None:
    """Release an advisory lock, discarding the connection if that fails.

    Copied from :func:`keel.database.migrations._release` rather than imported,
    because importing that module pulls in Alembic — an optional extra a worker
    process has no reason to carry. The reasoning is identical: a session-level
    lock lives as long as its backend, so returning a pooled connection that
    still holds one hands the lock to whoever checks it out next, and every
    later tick blocks on a lock nobody meant to hold. Invalidating closes the
    backend, which is the one release path that cannot itself fail.

    Logged rather than raised: this runs in a ``finally``, and an exception here
    would replace whatever the tick was already failing with.

    Args:
        connection: The connection holding the lock.
        key: The advisory-lock key.
    """
    try:
        await connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
    except Exception:
        logger.exception(
            "could not release the scheduler advisory lock (%s); discarding the "
            "connection so the lock dies with its backend",
            key,
        )
        await connection.invalidate()


def _log_entry_failure(error: BaseException, entry: ScheduleEntry) -> None:
    """Report a schedule entry that could not be dispatched.

    Args:
        error: What went wrong.
        entry: The entry it went wrong for.
    """
    logger.error(
        "schedule entry %s failed to dispatch: %s: %s",
        entry.name,
        type(error).__name__,
        error,
        exc_info=error,
    )


# -- cron parsing ----------------------------------------------------------

SEARCH_HORIZON_DAYS: Final = 4 * 366
"""How far the built-in parser will look for a match before giving up.

Four years covers the leap-day case — ``"0 0 29 2 *"`` is legal and fires once
every four years — and bounds the search so an impossible expression such as
``"0 0 30 2 *"`` returns rather than looping forever.
"""

_MONTHS: Final[dict[str, int]] = {
    name: index
    for index, name in enumerate(
        ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"),
        start=1,
    )
}
_WEEKDAYS: Final[dict[str, int]] = {
    name: index for index, name in enumerate(("sun", "mon", "tue", "wed", "thu", "fri", "sat"))
}

_FIELD = re.compile(r"^(?:\*|(?P<start>\w+)(?:-(?P<end>\w+))?)(?:/(?P<step>\d+))?$")


def _from_croniter(expression: str, moment: datetime, *, backwards: bool) -> datetime | None:
    """Ask ``croniter`` for the adjacent firing instant.

    Args:
        expression: The cron expression.
        moment: Where to search from.
        backwards: ``True`` for the previous instant, ``False`` for the next.

    Returns:
        The instant, or ``None`` if croniter reports there is none.

    Raises:
        CronError: If croniter rejects the expression.
    """
    factory = _croniter
    if factory is None:  # pragma: no cover — callers check first
        raise CronError(expression, "croniter is not installed")
    try:
        iterator = factory(expression, moment)
        found = iterator.get_prev(datetime) if backwards else iterator.get_next(datetime)
    except Exception as exc:
        raise CronError(expression, str(exc)) from exc
    result: datetime = found
    return result


def _parse_field(
    expression: str,
    raw: str,
    low: int,
    high: int,
    names: dict[str, int] | None = None,
) -> frozenset[int]:
    """Expand one cron field into the set of values it matches.

    Args:
        expression: The whole expression, for the error message.
        raw: The field text.
        low: Lowest legal value.
        high: Highest legal value.
        names: Three-letter aliases, for months and weekdays.

    Returns:
        Every value the field matches.

    Raises:
        CronError: If the field is malformed or out of range.
    """
    values: set[int] = set()
    for part in raw.split(","):
        match = _FIELD.match(part.strip())
        if match is None:
            raise CronError(expression, f"{part!r} is not a range, list, step or '*'")
        step = int(match["step"] or 1)
        if step < 1:
            raise CronError(expression, f"step in {part!r} must be at least 1")
        if match["start"] is None:
            start, end = low, high
        else:
            start = _parse_value(expression, match["start"], low, high, names)
            end = (
                high
                if match["end"] is None and match["step"] is not None
                else start
                if match["end"] is None
                else _parse_value(expression, match["end"], low, high, names)
            )
        if end < start:
            raise CronError(expression, f"range {part!r} ends before it starts")
        values.update(range(start, end + 1, step))
    return frozenset(values)


def _parse_value(
    expression: str,
    token: str,
    low: int,
    high: int,
    names: dict[str, int] | None,
) -> int:
    """Turn one cron token into a number.

    Args:
        expression: The whole expression, for the error message.
        token: A number or a three-letter name.
        low: Lowest legal value.
        high: Highest legal value.
        names: Three-letter aliases, if this field has any.

    Returns:
        The value.

    Raises:
        CronError: If the token is not recognised or is out of range.
    """
    if names is not None and token.lower() in names:
        value = names[token.lower()]
    elif token.isdigit():
        value = int(token)
    else:
        raise CronError(expression, f"{token!r} is not a number this field accepts")
    if not low <= value <= high:
        raise CronError(expression, f"{token!r} is outside {low}-{high}")
    return value


@dataclass(frozen=True, slots=True)
class _CronFields:
    """A parsed five-field cron expression, and the search over it.

    The fallback for environments without ``croniter``. Kept as expanded value
    sets rather than a matcher per field so that both directions of the search
    can step over sorted candidates instead of testing every minute — a naive
    minute-by-minute scan over the four-year horizon is two million iterations
    per call, which is fine once and not fine on every tick.

    Attributes:
        expression: The original text, for error messages.
        minutes: Matching minutes.
        hours: Matching hours.
        days: Matching days of the month.
        months: Matching months.
        weekdays: Matching weekdays, Sunday as 0.
        restricted_day: Whether both the day-of-month and day-of-week fields are
            restricted. Cron's most-missed rule: when they are, a day matches if
            *either* does, not both.
    """

    expression: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    restricted_day: bool

    @classmethod
    def parse(cls, expression: str) -> Self:
        """Parse a five-field cron expression.

        Args:
            expression: The expression.

        Returns:
            The parsed fields.

        Raises:
            CronError: If it does not have five fields, or one of them is
                malformed.
        """
        parts = expression.split()
        if len(parts) != 5:
            raise CronError(
                expression,
                f"expected 5 fields (minute hour day month weekday), got {len(parts)}"
                + ("; install croniter for @shortcuts and extended syntax" if parts else ""),
            )
        minute, hour, day, month, weekday = parts
        weekdays = _parse_field(expression, weekday, 0, 7, _WEEKDAYS)
        return cls(
            expression=expression,
            minutes=_parse_field(expression, minute, 0, 59),
            hours=_parse_field(expression, hour, 0, 23),
            days=_parse_field(expression, day, 1, 31),
            months=_parse_field(expression, month, 1, 12, _MONTHS),
            # 7 and 0 are both Sunday in every cron implementation.
            weekdays=frozenset(0 if value == 7 else value for value in weekdays),
            restricted_day=day.strip() != "*" and weekday.strip() != "*",
        )

    def _matches_day(self, moment: datetime) -> bool:
        """Whether a date satisfies the month, day and weekday fields.

        Args:
            moment: Any instant on the day in question.

        Returns:
            ``True`` if the day matches.
        """
        if moment.month not in self.months:
            return False
        # isoweekday() is Monday=1..Sunday=7; cron counts Sunday as 0.
        weekday = moment.isoweekday() % 7
        by_day = moment.day in self.days
        by_weekday = weekday in self.weekdays
        return (by_day or by_weekday) if self.restricted_day else (by_day and by_weekday)

    def next_due(self, moment: datetime) -> datetime | None:
        """Return the first firing instant strictly after *moment*.

        Args:
            moment: An aware datetime.

        Returns:
            The instant, or ``None`` if none falls inside the search horizon.
        """
        return self._search(moment + timedelta(minutes=1), forwards=True)

    def previous_due(self, moment: datetime) -> datetime | None:
        """Return the latest firing instant at or before *moment*.

        Args:
            moment: An aware datetime.

        Returns:
            The instant, or ``None`` if none falls inside the search horizon.
        """
        return self._search(moment, forwards=False)

    def _search(self, start: datetime, *, forwards: bool) -> datetime | None:
        """Walk days from *start* until one carries a matching time.

        Args:
            start: The instant to search from, inclusive, truncated to the
                minute — cron has no finer resolution and a fractional second
                would make the boundary ambiguous.
            forwards: Direction of travel.

        Returns:
            The matching instant, or ``None`` if the horizon was exhausted.
        """
        cursor = start.replace(second=0, microsecond=0)
        hours = sorted(self.hours, reverse=not forwards)
        minutes = sorted(self.minutes, reverse=not forwards)
        step = timedelta(days=1) if forwards else timedelta(days=-1)

        for offset in range(SEARCH_HORIZON_DAYS):
            day = cursor + step * offset
            if not self._matches_day(day):
                continue
            for hour in hours:
                for minute in minutes:
                    candidate = day.replace(hour=hour, minute=minute)
                    # Only the starting day is bounded by the starting time;
                    # every later (or earlier) day is searched whole, which is
                    # why the hour and minute lists are sorted in the direction
                    # of travel — the first candidate found is the nearest one.
                    if offset == 0 and (candidate < cursor if forwards else candidate > cursor):
                        continue
                    return candidate
        return None


__all__ = [
    "DEFAULT_CATCH_UP",
    "DEFAULT_RUN_RETENTION",
    "DEFAULT_TICK_INTERVAL",
    "LOCK_NAMESPACE",
    "SEARCH_HORIZON_DAYS",
    "CronError",
    "CronTrigger",
    "DuplicateEntryError",
    "IntervalTrigger",
    "Schedule",
    "ScheduleEntry",
    "ScheduleError",
    "ScheduleRun",
    "ScheduleRuns",
    "Scheduler",
    "Trigger",
    "entry_lock_key",
]
