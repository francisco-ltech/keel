"""The worker process.

A **Template Method**, and the only consume-side implementation there is. The
loop's shape is fixed and split across two methods — :meth:`Worker._consume`
reserves, :meth:`Worker._execute` runs and then acknowledges, retries or
dead-letters — while every step that could reasonably differ is delegated to
something that already owns the decision:

* *how long to wait before retrying* is asked of the job's
  :class:`~keel.queue.backoff.Backoff` Strategy, never decided here;
* *how many attempts remain* is read off the envelope, which froze the policy at
  dispatch so a deploy cannot retroactively change it;
* *what happens to an exhausted job* is handed to a :class:`FailureSink`;
* *who is told about any of it* is an :class:`~keel.support.events.EventDispatcher`.

ADR 0000 declines a consume-side protocol: one implementation behind an
interface is indirection pretending to be design. What it does not decline is
seams, and the four above are where a real deployment needs them.

**Graceful shutdown is the reason this is a class.** A loop cannot hold the
state a correct shutdown needs: whether new work is still being accepted,
which jobs are still in flight, when the deadline expires, and whether the
operator has asked twice. On the first SIGTERM or SIGINT the worker stops
reserving, lets in-flight jobs finish, and returns from :meth:`run`. A second
signal cancels them where they stand. Either way the process is expected to be
gone before an orchestrator's grace period runs out, which is what
:data:`DEFAULT_SHUTDOWN_GRACE` is sized against.

**Killing a worker mid-job loses nothing**, and that is a claim about two
mechanisms rather than one. A worker asked to stop drains, which covers SIGTERM.
A worker that is *killed* drains nothing, so its jobs are left on the driver's
active list and recovered by :meth:`Worker._recover_orphans`, which sweeps on
its own timer and re-queues anything whose holder is gone. The attempt that died
is still charged — :meth:`~keel.queue.saq_driver.SaqQueue.reserve` charges it
before the handler runs — so a job that kills its worker every time runs out of
budget and is dead-lettered rather than eating the fleet one replica at a time.

Signals are taken with :func:`anyio.open_signal_receiver` rather than
``loop.add_signal_handler``. Two reasons: it delivers signals as an async
iterable, so the handler is ordinary task code that can await the queue instead
of a callback that must not block; and it restores the previous disposition when
its scope exits, so a worker embedded in a larger process does not permanently
steal SIGINT from it. The cost is that it must run on the main thread of an
anyio event loop — where a worker process lives anyway, and where it does not,
:attr:`Worker.handles_signals` reports that it declined rather than pretending.
"""

from __future__ import annotations

import os
import signal
import socket
import time
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, Protocol

import anyio

from keel.exceptions import ConfigurationError
from keel.queue.config import QueueConfig
from keel.queue.envelope import Envelope
from keel.queue.job import Job, JobError, PermanentFailureError, UnknownJobError
from keel.queue.saq_driver import DEFAULT_RESERVE_TIMEOUT, Reservation, SaqQueue
from keel.support.events import EventDispatcher

if TYPE_CHECKING:
    from anyio.abc import TaskGroup

STOP_SIGNALS: Final = (signal.SIGTERM, signal.SIGINT)
"""What an orchestrator sends, and what a terminal sends."""

DEFAULT_SHUTDOWN_GRACE: Final = 25.0
"""Seconds in-flight jobs get after a stop request, before they are cancelled.

Sized under Kubernetes' 30-second default ``terminationGracePeriodSeconds``: the
worker must be *finished* when the grace period ends, not just starting to think
about it, or SIGKILL arrives mid-transaction.
"""

DEFAULT_MAINTENANCE_INTERVAL: Final = 1.0
"""Seconds between promoting delayed jobs and refreshing the heartbeat."""

DEFAULT_SWEEP_INTERVAL: Final = 60.0
"""Seconds between sweeps for jobs whose worker died holding them.

Sixty, matching SAQ's own worker, and the reasoning is the same on both sides.
A sweep is an ``LRANGE 0 -1`` over the active list plus a ``GET`` per entry, so
it costs in proportion to in-flight work and buys nothing in the normal case. It
is also pointless to poll faster than the thing being detected: a job is only
recognisable as abandoned once it has outrun its own timeout, which defaults to
five minutes, so a one-minute sweep already finds every orphan on its first or
second pass.

The value is passed to the driver as the *lock* it holds, so it doubles as the
cluster-wide sweep period: shortening it here shortens both, and the two cannot
drift apart.
"""

DEFAULT_HEARTBEAT_TIMEOUT: Final = 60.0
"""How stale the last activity may get before :attr:`Worker.healthy` turns false.

Comfortably more than :data:`DEFAULT_MAINTENANCE_INTERVAL` — the maintenance
tick is what keeps the heartbeat fresh on an idle worker, so anything close to
it would report a healthy but quiet worker as dead.
"""

DEFAULT_UNROUTABLE_DELAY: Final = 30.0
"""Seconds an unroutable job waits before being offered again. See :class:`Worker`."""

DEFAULT_UNROUTABLE_AFTER: Final = 3600.0
"""Seconds after dispatch at which an unroutable job stops being a rollout artefact."""


# -- events ---------------------------------------------------------------
#
# Frozen dataclasses with no behaviour, like keel.cache.events: an event is a
# fact that has already happened, so there is nothing to do to it.


@dataclass(frozen=True, slots=True)
class WorkerEvent:
    """Base class for every event a worker emits.

    Attributes:
        worker: The worker's identity, so a fleet's events stay attributable.
    """

    worker: str


@dataclass(frozen=True, slots=True)
class WorkerStarted(WorkerEvent):
    """A worker began serving.

    Attributes:
        queues: The queue names it will reserve from.
    """

    queues: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class WorkerStopped(WorkerEvent):
    """A worker's run loop returned.

    Attributes:
        forced: Whether in-flight jobs were cancelled rather than allowed to
            finish, which is the difference between a clean rollout and one an
            operator should go and look at.
    """

    forced: bool = False


@dataclass(frozen=True, slots=True)
class JobRecovered(WorkerEvent):
    """A job left behind by a dead worker was put back on the queue.

    Not a :class:`JobEvent`: a sweep works on the driver's own job ids and never
    opens the envelope, so there is none to attach. It is also the event worth
    alerting on — one is a restart, a stream of them is a worker that keeps
    dying.

    Attributes:
        lane: The queue name that was swept.
        job_id: The driver's id for the job that was recovered.
    """

    lane: str = ""
    job_id: str = ""


@dataclass(frozen=True, slots=True)
class JobEvent(WorkerEvent):
    """Base class for events about one dispatched job.

    Attributes:
        envelope: The envelope as it stood when the event happened, so an
            observer can read the attempt count without a second lookup.
    """

    envelope: Envelope


@dataclass(frozen=True, slots=True)
class JobStarted(JobEvent):
    """A job was reserved and its handler is about to run."""


@dataclass(frozen=True, slots=True)
class JobSucceeded(JobEvent):
    """A job's handler returned.

    Attributes:
        duration: Seconds the handler ran for.
    """

    duration: float = 0.0


@dataclass(frozen=True, slots=True)
class JobRetrying(JobEvent):
    """A job failed and has attempts left.

    Attributes:
        error: What the handler raised.
        retry_in: Seconds until the next attempt becomes visible, as the job's
            own backoff policy computed it.
    """

    error: BaseException | None = None
    retry_in: float = 0.0


@dataclass(frozen=True, slots=True)
class JobDeadLettered(JobEvent):
    """A job used up its attempts, or failed in a way retrying cannot fix.

    Attributes:
        error: The failure that ended it.
    """

    error: BaseException | None = None


@dataclass(frozen=True, slots=True)
class JobUnroutable(JobEvent):
    """A job arrived at a worker that has no class registered under its name.

    Attributes:
        error: The :class:`~keel.queue.job.UnknownJobError` raised.
        retry_in: Seconds until it is offered again, or ``None`` when the worker
            gave up on it and dead-lettered it instead.
    """

    error: BaseException | None = None
    retry_in: float | None = None


@dataclass(frozen=True, slots=True)
class DeadLetterDiscarded:
    """A dead-lettered envelope was not durably recorded anywhere.

    Emitted by :class:`EventFailureSink`, which is the default, and also by the
    worker when a configured sink raises. Both mean the same operational thing —
    the payload is gone and nobody can replay it — which is why they share an
    event rather than splitting hairs over whose fault it was.

    Not a :class:`WorkerEvent`: a sink is given an envelope and an error, not a
    worker identity, and inventing one here would be a lie.

    Attributes:
        envelope: The envelope that was lost.
        error: The failure that dead-lettered it, or the sink's own failure.
    """

    envelope: Envelope
    error: BaseException


# -- the failure sink -----------------------------------------------------


class FailureSink(Protocol):
    """Somewhere a dead-lettered envelope can be put so a human can find it.

    Narrow on purpose. A dead letter needs exactly two things recorded — what
    was going to run, and why it stopped — and every extra parameter is one an
    implementor of a database-backed sink would have to invent a column for.
    Everything else an implementation wants is already inside the envelope.

    Not ``runtime_checkable``, matching the store contracts: ``isinstance`` on a
    protocol checks attribute names only, which is false assurance.
    """

    async def record(self, envelope: Envelope, error: BaseException) -> None:
        """Record a job that will not be attempted again.

        Args:
            envelope: The envelope as it stood on its final attempt.
            error: What ended it.
        """
        ...


class EventFailureSink:
    """The default sink: announces the loss and stores nothing.

    Not a Null Object, and the distinction matters. A Null Object would be
    silent, and a queue that silently drops exhausted jobs is the single most
    expensive default a job system can ship. This one is loud — an observer
    sees :class:`DeadLetterDiscarded` and an operator learns they have no failed
    job table — while still requiring no schema to get a worker running.

    Args:
        events: Where to announce the loss.
    """

    __slots__ = ("_events",)

    def __init__(self, events: EventDispatcher) -> None:
        self._events = events

    async def record(self, envelope: Envelope, error: BaseException) -> None:
        """Announce that the envelope was dropped.

        Args:
            envelope: The envelope as it stood on its final attempt.
            error: What ended it.
        """
        await self._events.dispatch(DeadLetterDiscarded(envelope=envelope, error=error))


# -- the worker -----------------------------------------------------------


class Worker:
    """Runs jobs off one or more queues until asked to stop.

    Args:
        config: How to reach the queue. Must name the ``saq`` driver unless
            *queue* is supplied, since the consume side is SAQ-specific.
        queues: Which queue names to serve, in the order they are polled.
            Defaults to the configuration's single default queue.
        queue: An already-built driver, shared with the dispatching side or
            supplied by a test. When omitted the worker builds — and owns, and
            closes — its own.
        failure_sink: Where exhausted jobs go. Defaults to
            :class:`EventFailureSink`.
        events: Lifecycle observer. A private dispatcher is created when
            omitted, which is only useful if the caller reads
            :attr:`Worker.events`.
        name: This worker's identity in its events. Defaults to host and pid,
            which is what a log aggregator can correlate with a container.
        shutdown_grace: Seconds in-flight jobs get after a stop request.
        maintenance_interval: Seconds between promoting delayed jobs.
        sweep_interval: Seconds between sweeps for orphaned jobs, and the
            lock the driver holds while sweeping.
        reserve_timeout: Seconds a reserve blocks; also the worst-case latency
            of noticing a stop request.
        heartbeat_timeout: How stale :attr:`last_activity` may get before
            :attr:`healthy` turns false.
        unroutable_delay: Seconds an unknown job waits before being offered
            again.
        unroutable_after: Seconds after dispatch at which an unknown job is
            dead-lettered instead of being offered again.
        handle_signals: Whether to install signal handlers. Turn it off to embed
            a worker in a process that manages its own.

    Raises:
        ConfigurationError: If no *queue* is given and the configuration cannot
            build one.
    """

    __slots__ = (
        "_config",
        "_events",
        "_forced",
        "_heartbeat",
        "_heartbeat_timeout",
        "_in_flight",
        "_maintenance_interval",
        "_name",
        "_owns_queue",
        "_queue",
        "_queues",
        "_reserve_timeout",
        "_running",
        "_shutdown_grace",
        "_signals_installed",
        "_sink",
        "_stop_requested",
        "_stopping",
        "_sweep_interval",
        "_unroutable_after",
        "_unroutable_delay",
        "_work_scope",
        "handle_signals",
    )

    def __init__(
        self,
        config: QueueConfig,
        queues: Sequence[str] | None = None,
        *,
        queue: SaqQueue | None = None,
        failure_sink: FailureSink | None = None,
        events: EventDispatcher | None = None,
        name: str | None = None,
        shutdown_grace: float = DEFAULT_SHUTDOWN_GRACE,
        maintenance_interval: float = DEFAULT_MAINTENANCE_INTERVAL,
        sweep_interval: float = DEFAULT_SWEEP_INTERVAL,
        reserve_timeout: float = DEFAULT_RESERVE_TIMEOUT,
        heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT,
        unroutable_delay: float = DEFAULT_UNROUTABLE_DELAY,
        unroutable_after: float = DEFAULT_UNROUTABLE_AFTER,
        handle_signals: bool = True,
    ) -> None:
        self._config = config
        self._queues = tuple(queues) if queues else (config.default_queue,)
        self._queue = queue
        self._owns_queue = queue is None
        self._events = events or EventDispatcher()
        self._sink: FailureSink = failure_sink or EventFailureSink(self._events)
        self._name = name or f"{socket.gethostname()}:{os.getpid()}"
        self._shutdown_grace = shutdown_grace
        self._maintenance_interval = maintenance_interval
        self._sweep_interval = sweep_interval
        self._reserve_timeout = reserve_timeout
        self._heartbeat_timeout = heartbeat_timeout
        self._unroutable_delay = unroutable_delay
        self._unroutable_after = unroutable_after
        self.handle_signals = handle_signals

        self._running = False
        self._forced = False
        self._signals_installed = False
        self._stop_requested = False
        self._in_flight = 0
        self._heartbeat = time.time()
        # Both are event-loop bound, so they cannot exist before run() does.
        self._stopping: anyio.Event | None = None
        self._work_scope: anyio.CancelScope | None = None

    # -- liveness ---------------------------------------------------------

    @property
    def name(self) -> str:
        """This worker's identity in its events."""
        return self._name

    @property
    def events(self) -> EventDispatcher:
        """The lifecycle observer, for registering listeners before :meth:`run`."""
        return self._events

    @property
    def queues(self) -> tuple[str, ...]:
        """The queue names this worker serves."""
        return self._queues

    @property
    def running(self) -> bool:
        """Whether :meth:`run` is currently executing."""
        return self._running

    @property
    def in_flight(self) -> int:
        """How many jobs are executing right now."""
        return self._in_flight

    @property
    def last_activity(self) -> float:
        """Unix timestamp of the last reserve, maintenance tick or job event.

        The value a liveness endpoint should expose alongside :attr:`healthy`,
        because "unhealthy since 40 seconds ago" is actionable and "unhealthy"
        is not.
        """
        return self._heartbeat

    @property
    def healthy(self) -> bool:
        """Whether the loop is alive and has done something recently.

        The **liveness** question, so it stays true while draining: a worker
        finishing its last jobs is working exactly as intended, and a probe that
        called it dead would have the orchestrator kill it mid-job — the precise
        outcome graceful shutdown exists to avoid.
        """
        return self._running and (time.time() - self._heartbeat) < self._heartbeat_timeout

    @property
    def accepting(self) -> bool:
        """Whether new jobs are still being reserved.

        The **readiness** question. False from the moment a stop is requested,
        which is what lets a rollout take a worker out of rotation before it
        goes quiet.
        """
        return self._running and not self._stop_requested

    @property
    def handles_signals(self) -> bool:
        """Whether signal handlers were actually installed.

        False when :attr:`handle_signals` was off, before :meth:`run` starts, or
        when the event loop declined — on a non-main thread, say. Exposed rather
        than assumed, because a worker that silently failed to install them
        looks identical to one that is ignoring SIGTERM.
        """
        return self._signals_installed

    # -- lifecycle --------------------------------------------------------

    async def run(self) -> None:
        """Serve the configured queues until stopped.

        Returns when every consumer has stopped reserving and every in-flight
        job has finished — or been cancelled, if the grace period ran out.

        Raises:
            RuntimeError: If this worker is already running. Two loops sharing
                one worker's shutdown state would each think the other's signal
                was theirs.
            ConfigurationError: If no queue was supplied and the configuration
                cannot build one.
        """
        if self._running:
            raise RuntimeError(f"worker {self._name} is already running")
        queue = self._resolve_queue()
        self._running = True
        self._forced = False
        self._touch()
        stopping = anyio.Event()
        self._stopping = stopping
        if self._stop_requested:
            stopping.set()

        await self._events.dispatch(WorkerStarted(worker=self._name, queues=self._queues))
        try:
            async with anyio.create_task_group() as supervisor:
                supervisor.start_soon(self._watch_signals)
                supervisor.start_soon(self._enforce_deadline)
                with anyio.CancelScope() as scope:
                    self._work_scope = scope
                    slots = anyio.Semaphore(self._config.concurrency)
                    async with anyio.create_task_group() as workers:
                        workers.start_soon(self._maintain, queue)
                        workers.start_soon(self._recover_orphans, queue)
                        for lane in self._queues:
                            workers.start_soon(self._consume, queue, lane, workers, slots)
                self._forced = scope.cancel_called
                # Nothing is left to supervise; the signal watcher and the
                # deadline timer are both parked on awaits that will never fire.
                supervisor.cancel_scope.cancel()
        finally:
            self._running = False
            self._signals_installed = False
            self._stopping = None
            self._work_scope = None
            if self._owns_queue:
                await queue.close()
                self._queue = None
        await self._events.dispatch(WorkerStopped(worker=self._name, forced=self._forced))

    async def stop(self) -> None:
        """Ask the worker to finish what it is doing and return from :meth:`run`.

        Returns as soon as the request is recorded rather than waiting for the
        loop to drain, because a job is allowed to stop its own worker — a
        migration job that has done its work, a self-updating replica — and
        waiting would mean waiting for itself. Await :meth:`run` for completion.

        Called before :meth:`run`, it makes that run a no-op, which is what a
        cancelled startup should look like.
        """
        self._stop_requested = True
        if self._stopping is not None:
            self._stopping.set()

    def kill(self) -> None:
        """Cancel in-flight jobs immediately, without waiting for them.

        What a second SIGTERM does. Synchronous because a signal handler and a
        deadline timer both need it and neither can afford to await anything.
        """
        self._stop_requested = True
        if self._stopping is not None:
            self._stopping.set()
        if self._work_scope is not None:
            self._work_scope.cancel()

    # -- the loop ---------------------------------------------------------

    async def _consume(
        self,
        queue: SaqQueue,
        lane: str,
        tasks: TaskGroup,
        slots: anyio.Semaphore,
    ) -> None:
        """Reserve jobs from one queue and start them, until asked to stop.

        A slot is taken *before* reserving rather than after, so the worker
        never holds a job it has no capacity to run: a reserved job is invisible
        to every other worker, and reserving beyond capacity is how a queue ends
        up with work parked on a saturated replica while an idle one polls an
        empty list.

        Args:
            queue: The driver to reserve from.
            lane: The queue name to serve.
            tasks: Where to start each job. The same group the caller is waiting
                on, which is what makes "let in-flight jobs finish" free.
            slots: The worker-wide concurrency budget. Shared across lanes on
                purpose — ``concurrency`` is capped by the database pool, and a
                per-lane budget would multiply it by the number of queues.
        """
        while not self._should_stop():
            await slots.acquire()
            held = True
            try:
                if self._should_stop():
                    return
                reservation = await queue.reserve(lane, timeout=self._reserve_timeout)
                self._touch()
                if reservation is not None:
                    # The slot now belongs to the job task, which releases it.
                    held = False
                    tasks.start_soon(self._process, queue, reservation, slots)
            finally:
                if held:
                    slots.release()

    async def _maintain(self, queue: SaqQueue) -> None:
        """Promote delayed jobs and keep the heartbeat fresh.

        Args:
            queue: The driver to maintain.
        """
        stopping = self._stopping
        while not self._should_stop():
            for lane in self._queues:
                await queue.promote_due(lane)
            self._touch()
            if stopping is None:  # pragma: no cover — run() always sets it
                return
            with anyio.move_on_after(self._maintenance_interval):
                await stopping.wait()

    async def _recover_orphans(self, queue: SaqQueue) -> None:
        """Re-queue jobs whose worker died while holding them.

        Its own task, not a branch of :meth:`_maintain`, for two reasons. A
        sweep runs on a minute-scale timer while promotion runs on a
        second-scale one, and a sweep that finds orphans blocks for an abort
        handshake per job — inlining it would stall every delayed job in the
        process behind a recovery that has nothing to do with them.

        The sweep itself is guarded across replicas by SAQ, atomically; see
        :meth:`~keel.queue.saq_driver.SaqQueue.sweep`. Nothing here needs to
        coordinate with the rest of the fleet.

        Args:
            queue: The driver to sweep.
        """
        stopping = self._stopping
        while not self._should_stop():
            for lane in self._queues:
                recovered = await queue.sweep(lane, lock=self._sweep_interval)
                for job_id in recovered:
                    await self._events.dispatch(
                        JobRecovered(worker=self._name, lane=lane, job_id=job_id)
                    )
            self._touch()
            if stopping is None:  # pragma: no cover — run() always sets it
                return
            with anyio.move_on_after(self._sweep_interval):
                await stopping.wait()

    async def _process(
        self,
        queue: SaqQueue,
        reservation: Reservation,
        slots: anyio.Semaphore,
    ) -> None:
        """Run one reserved job and decide what happens to it.

        The template's fixed part. Every branch below ends the SAQ attempt
        before deciding anything else, because an attempt that is still open
        holds the job's key and a re-dispatch under the same id would be
        rejected as a duplicate of itself.

        Args:
            queue: The driver the job came from.
            reservation: What was reserved.
            slots: The concurrency semaphore to release when done.
        """
        self._in_flight += 1
        try:
            await self._execute(queue, reservation)
        finally:
            self._in_flight -= 1
            self._touch()
            slots.release()

    async def _execute(self, queue: SaqQueue, reservation: Reservation) -> None:
        """Open the envelope, run it, and route the outcome.

        The reservation's envelope already has this attempt counted — the driver
        charges it at reserve time so that a crash costs a life. Everything here
        therefore reads ``attempt.attempts`` as "this delivery's number".

        Args:
            queue: The driver the job came from.
            reservation: What was reserved.
        """
        attempt = reservation.envelope
        if attempt.attempts > attempt.max_attempts:
            # Only reachable through recovery: a job that kills its worker is
            # swept back and delivered again, and without this it would keep
            # being delivered for ever. `exhausted` is not the test — that is
            # true *on* the last legitimate attempt, which must still run.
            await queue.fail(reservation, "attempt budget exceeded after recovery")
            await self._dead_letter(attempt, JobError("job exceeded its attempts after recovery"))
            return
        try:
            job = attempt.open()
        except UnknownJobError as exc:
            await self._offer_again(queue, reservation, exc)
            return
        except JobError as exc:
            # The payload no longer fits the job's fields. Deterministic: every
            # remaining attempt would fail identically, so spending them only
            # delays the moment a human sees the envelope.
            await queue.fail(reservation, self._describe(exc))
            await self._dead_letter(attempt, exc)
            return

        await self._events.dispatch(JobStarted(worker=self._name, envelope=attempt))
        started = time.monotonic()
        try:
            await self._handle(job, attempt.timeout)
        except PermanentFailureError as exc:
            # The handler has said retrying cannot help. Spending the remaining
            # attempts would only delay the dead-letter a human needs to see,
            # and would make a broken job look like a flaky one.
            await queue.fail(reservation, self._describe(exc))
            await self._dead_letter(attempt, exc)
        except Exception as exc:  # noqa: BLE001 — a handler may raise anything
            await self._retry_or_bury(queue, reservation, job, exc)
        else:
            await queue.ack(reservation)
            await self._events.dispatch(
                JobSucceeded(
                    worker=self._name,
                    envelope=attempt,
                    duration=time.monotonic() - started,
                )
            )

    @staticmethod
    async def _handle(job: Job, timeout: float | None) -> None:
        """Run the handler, under the envelope's timeout if it has one.

        Args:
            job: The rebuilt command.
            timeout: Seconds one attempt may run, or ``None`` for unbounded.

        Raises:
            TimeoutError: If the handler outran its timeout. Treated as an
                ordinary failure by the caller, because from the queue's side it
                is one — the work did not happen and may happen next time.
        """
        if timeout is None:
            await job.handle()
            return
        with anyio.fail_after(timeout):
            await job.handle()

    async def _retry_or_bury(
        self,
        queue: SaqQueue,
        reservation: Reservation,
        job: Job,
        error: BaseException,
    ) -> None:
        """Re-dispatch the failed job, or hand it to the failure sink.

        The delay is the *job's* answer, not the worker's: a rate-limited API
        and a flaky socket want completely different waits, and only the person
        who wrote the job knows which this is.

        The re-dispatch goes through :meth:`SaqQueue.retry` rather than a
        ``fail()`` and a ``push()``, so ending this attempt and scheduling the
        next one are one Redis transaction. There is then no instant at which
        the job is on no list at all, which is the instant a crash would
        otherwise lose it — and it is the one crash window sweeping could not
        have covered, because a failed job is no longer on the active list for a
        sweep to find.

        Args:
            queue: The driver to re-dispatch through.
            reservation: What was reserved, for the transactional re-dispatch.
            job: The job that failed, for its backoff policy.
            error: What the handler raised.
        """
        attempt = reservation.envelope
        if attempt.exhausted:
            await queue.fail(reservation, self._describe(error))
            await self._dead_letter(attempt, error)
            return
        delay = job.backoff.delay_for(attempt.attempts + 1)
        await queue.retry(
            reservation,
            replace(attempt, delay=delay),
            delay=delay,
            error=self._describe(error),
        )
        await self._events.dispatch(
            JobRetrying(worker=self._name, envelope=attempt, error=error, retry_in=delay)
        )

    async def _offer_again(
        self,
        queue: SaqQueue,
        reservation: Reservation,
        error: UnknownJobError,
    ) -> None:
        """Put an unroutable job back without spending one of its attempts.

        ``UnknownJobError`` says the *worker* is wrong, not the job. It is
        overwhelmingly a rollout in progress: an old replica has picked up a job
        only the new code defines. Treating that as a failure would be actively
        harmful — it burns a retry budget on a job that is perfectly valid, and
        three old replicas polling the same queue would dead-letter it before
        the new ones are even accepting traffic.

        So the envelope goes back after a delay long enough that a cluster of
        ignorant workers cannot spin on it, and the attempt the driver charged
        at reserve time is **refunded** — no attempt was made, and charging for
        one would let three old replicas exhaust a perfectly good job's budget
        between them. The refund is explicit rather than achieved by not
        charging, because the charge is what makes crash recovery safe and it
        must not be conditional on what the worker discovers afterwards.

        That cannot be unbounded, or a job whose class was genuinely deleted
        circulates forever with nobody looking at it. A rollout is minutes; past
        :attr:`unroutable_after` the honest conclusion is that no worker will
        ever route it, and it belongs in the failure sink where a human is.

        Args:
            queue: The driver to re-dispatch through.
            reservation: What was reserved.
            error: The routing failure.
        """
        refunded = replace(reservation.envelope, attempts=max(0, reservation.envelope.attempts - 1))
        if time.time() - refunded.dispatched_at >= self._unroutable_after:
            await queue.fail(reservation, self._describe(error))
            await self._events.dispatch(
                JobUnroutable(worker=self._name, envelope=refunded, error=error, retry_in=None)
            )
            await self._dead_letter(refunded, error)
            return
        await queue.retry(
            reservation,
            replace(refunded, delay=self._unroutable_delay),
            delay=self._unroutable_delay,
            error=self._describe(error),
        )
        await self._events.dispatch(
            JobUnroutable(
                worker=self._name,
                envelope=refunded,
                error=error,
                retry_in=self._unroutable_delay,
            )
        )

    async def _dead_letter(self, envelope: Envelope, error: BaseException) -> None:
        """Announce a job's death and hand it to the sink.

        The event goes first so an observer records the fact even if the sink is
        the thing that is broken, and a sink that raises is contained rather
        than allowed to take the worker down with it — losing one envelope is
        bad, losing the process that was about to retry a hundred others is
        worse.

        Args:
            envelope: The envelope as it stood on its final attempt.
            error: What ended it.
        """
        await self._events.dispatch(
            JobDeadLettered(worker=self._name, envelope=envelope, error=error)
        )
        try:
            await self._sink.record(envelope, error)
        except Exception as exc:  # noqa: BLE001 — a broken sink must not stop the worker
            await self._events.dispatch(DeadLetterDiscarded(envelope=envelope, error=exc))

    # -- shutdown ---------------------------------------------------------

    async def _watch_signals(self) -> None:
        """Turn stop signals into a graceful shutdown, and a second one into a hard stop."""
        if not self.handle_signals:
            return
        with ExitStack() as stack:
            try:
                signals = stack.enter_context(anyio.open_signal_receiver(*STOP_SIGNALS))
            except (NotImplementedError, ValueError, RuntimeError):
                # No signal support here: a non-main thread, or a platform
                # without it. Reported through `handles_signals` rather than
                # raised, so an embedded worker still runs.
                return
            self._signals_installed = True
            async for _ in signals:
                if self._stop_requested:
                    self.kill()
                    return
                await self.stop()

    async def _enforce_deadline(self) -> None:
        """Cancel in-flight jobs if they outlast the shutdown grace period.

        Parked on the stop event for the worker's whole life, then on a timer.
        Both awaits are abandoned when the work group finishes early, because
        the supervising group is cancelled the moment it does — which is how a
        clean drain returns immediately instead of waiting out the grace period.
        """
        stopping = self._stopping
        if stopping is None:  # pragma: no cover — run() always sets it
            return
        await stopping.wait()
        await anyio.sleep(self._shutdown_grace)
        self.kill()

    # -- internals --------------------------------------------------------

    def _resolve_queue(self) -> SaqQueue:
        """Return the driver to serve from, building one if none was supplied.

        Returns:
            The queue driver.

        Raises:
            ConfigurationError: If the configuration does not describe a SAQ
                queue. The consume side is SAQ-specific by design: ``sync`` runs
                jobs at dispatch and ``null`` discards them, so neither has
                anything for a worker to reserve.
        """
        if self._queue is not None:
            return self._queue
        if self._config.driver != "saq" or not self._config.url:
            raise ConfigurationError(
                f"a worker needs the 'saq' queue driver and a url, but the "
                f"configuration names {self._config.driver!r}. The sync and null "
                f"drivers have no consume side: sync runs jobs at dispatch and "
                f"null discards them, so there is nothing for a worker to do."
            )
        queue = SaqQueue.from_url(self._config.url, self._config)
        self._queue = queue
        return queue

    def _should_stop(self) -> bool:
        """Whether the loops should wind down.

        A method rather than a bare attribute read so the loops re-read it on
        every pass: the flag is set from a signal handler, and a type checker
        that narrowed it at the top of a loop would be reasoning about a value
        that changes underneath it.

        Returns:
            ``True`` once a stop has been requested.
        """
        return self._stop_requested

    def _touch(self) -> None:
        """Record that the loop is alive."""
        self._heartbeat = time.time()

    @staticmethod
    def _describe(error: BaseException) -> str:
        """Render an exception for the driver's error field.

        Args:
            error: The failure.

        Returns:
            A one-line description. No traceback: it goes into a Redis value
            read by dashboards, and the full trace belongs in the log the
            observer writes.
        """
        return f"{type(error).__name__}: {error}"

    def __repr__(self) -> str:
        """Identify the worker, its queues and whether it is still accepting."""
        state = "running" if self._running else "stopped"
        if self._running and not self.accepting:
            state = "draining"
        return f"<Worker {self._name} queues={list(self._queues)} {state}>"


__all__ = [
    "DEFAULT_HEARTBEAT_TIMEOUT",
    "DEFAULT_MAINTENANCE_INTERVAL",
    "DEFAULT_SHUTDOWN_GRACE",
    "DEFAULT_SWEEP_INTERVAL",
    "DEFAULT_UNROUTABLE_AFTER",
    "DEFAULT_UNROUTABLE_DELAY",
    "STOP_SIGNALS",
    "DeadLetterDiscarded",
    "EventFailureSink",
    "FailureSink",
    "JobDeadLettered",
    "JobEvent",
    "JobRecovered",
    "JobRetrying",
    "JobStarted",
    "JobSucceeded",
    "JobUnroutable",
    "Worker",
    "WorkerEvent",
    "WorkerStarted",
    "WorkerStopped",
]
