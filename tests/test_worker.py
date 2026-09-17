"""The worker, end to end against a real Redis.

The retry ladder and the shutdown are the two things this file exists for, and
neither can be faked convincingly. A retry is only a retry if the job really
went back onto the queue and really came off it again; a graceful shutdown is
only graceful if a real SIGTERM arrives while a real job is half done and the
job still finishes. So the shutdown tests send actual signals to the pytest
process, which is safe precisely because the worker installs the handlers —
`handles_signals` is polled before every kill, and a test that skipped that
check would take the suite down with it.

State that a job handler has to reach lives in module-level records, reset
between tests: a `Job` is reconstructed from its payload inside the worker, so
there is no instance for a test to hold on to.
"""

from __future__ import annotations

import os
import signal
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar

import anyio
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from keel.exceptions import ConfigurationError
from keel.queue import Envelope, ExponentialBackoff, FixedBackoff, Job, QueueConfig
from keel.queue.backoff import Backoff
from keel.queue.saq_driver import SaqQueue
from keel.queue.worker import (
    DeadLetterDiscarded,
    EventFailureSink,
    JobDeadLettered,
    JobRecovered,
    JobRetrying,
    JobStarted,
    JobSucceeded,
    JobUnroutable,
    JobUnsettled,
    Worker,
    WorkerEvent,
    WorkerFaulted,
    WorkerStopped,
)
from keel.support.correlation import correlation
from keel.support.events import EventDispatcher

pytestmark = [pytest.mark.anyio, pytest.mark.redis]

IMMEDIATE: Backoff = FixedBackoff(seconds=0.0, jitter=0.0)
"""No wait between retries, so the ladder runs in milliseconds rather than minutes."""


# -- what the handlers record ---------------------------------------------


@dataclass
class Record:
    """Everything the module-level job handlers write to."""

    ran: list[str] = field(default_factory=list)
    finished: list[str] = field(default_factory=list)
    context: list[dict[str, str]] = field(default_factory=list)
    hold: float = 0.4
    started: anyio.Event | None = None


record = Record()


@pytest.fixture(autouse=True)
def _fresh_record() -> None:
    """No test may see another's handler output."""
    # The handlers reach this by module attribute, so rebinding it is the reset.
    global record
    record = Record()


# -- jobs ------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WorkerEcho(Job):
    """Succeeds, and says so."""

    message: str

    max_attempts: ClassVar[int] = 1

    async def handle(self) -> None:
        """Record having run."""
        record.ran.append(self.message)


@dataclass(frozen=True, slots=True)
class WorkerCorrelated(Job):
    """Records the correlation fields its handler can see."""

    message: str

    max_attempts: ClassVar[int] = 1

    async def handle(self) -> None:
        """Record what the worker bound around this attempt."""
        record.ran.append(self.message)
        record.context.append(dict(correlation()))


@dataclass(frozen=True, slots=True)
class WorkerAlwaysFails(Job):
    """Fails every time, cheaply, so the whole retry ladder fits in a test."""

    token: str

    max_attempts: ClassVar[int] = 3
    backoff: ClassVar[Backoff] = IMMEDIATE

    async def handle(self) -> None:
        """Record the attempt and blow up."""
        record.ran.append(self.token)
        raise RuntimeError("dependency is down")


@dataclass(frozen=True, slots=True)
class WorkerFailsOnce(Job):
    """Fails its first attempt and succeeds on the retry, after a real wait."""

    token: str

    max_attempts: ClassVar[int] = 3
    backoff: ClassVar[Backoff] = FixedBackoff(seconds=0.25, jitter=0.0)

    async def handle(self) -> None:
        """Fail the first time this token is seen."""
        record.ran.append(self.token)
        if len(record.ran) == 1:
            raise RuntimeError("first attempt always fails")
        record.finished.append(self.token)


@dataclass(frozen=True, slots=True)
class WorkerSlow(Job):
    """Announces that it started, holds for a while, then finishes."""

    token: str

    max_attempts: ClassVar[int] = 1
    timeout: ClassVar[float | None] = None

    async def handle(self) -> None:
        """Signal, sleep, then record completion."""
        record.ran.append(self.token)
        if record.started is not None:
            record.started.set()
        await anyio.sleep(record.hold)
        record.finished.append(self.token)


@dataclass(frozen=True, slots=True)
class WorkerSlowButTimed(Job):
    """Runs long enough to overlap several sweeps, and declares a timeout."""

    token: str

    max_attempts: ClassVar[int] = 1
    timeout: ClassVar[float | None] = 30.0

    async def handle(self) -> None:
        """Signal, hold, then record completion."""
        record.ran.append(self.token)
        if record.started is not None:
            record.started.set()
        await anyio.sleep(record.hold)
        record.finished.append(self.token)


@dataclass(frozen=True, slots=True)
class WorkerSurvivesACrash(Job):
    """Hangs on its first delivery, so its worker can be killed holding it."""

    token: str

    max_attempts: ClassVar[int] = 3
    timeout: ClassVar[float | None] = 1.0
    backoff: ClassVar[Backoff] = IMMEDIATE

    async def handle(self) -> None:
        """Never return the first time; succeed once recovered."""
        record.ran.append(self.token)
        if len(record.ran) == 1:
            if record.started is not None:
                record.started.set()
            await anyio.sleep(600)
        record.finished.append(self.token)


# -- doubles ---------------------------------------------------------------


class RecordingSink:
    """A failure sink that keeps what it was given."""

    def __init__(self) -> None:
        self.records: list[tuple[Envelope, BaseException]] = []

    async def record(self, envelope: Envelope, error: BaseException) -> None:
        self.records.append((envelope, error))


class BrokenSink:
    """A failure sink that is itself the failure."""

    async def record(self, envelope: Envelope, error: BaseException) -> None:
        raise OSError("the failed_jobs table is gone")


class FaultingQueue(SaqQueue):
    """The real driver, with a named method made to raise a set number of times.

    What a Redis failover looks like from the worker: a call that raises
    `ConnectionError` a few times and then works again. Only the named method
    faults, so the test can say which loop was hit and check the others kept
    going.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.faults: dict[str, int] = {}
        self.dropped_replies: dict[str, int] = {}

    def _fault(self, method: str) -> None:
        remaining = self.faults.get(method, 0)
        if remaining > 0:
            self.faults[method] = remaining - 1
            raise RedisConnectionError(f"{method}: connection lost")

    def _drop_reply(self, method: str) -> None:
        """The other shape of a failover: the write landed and the reply did not."""
        remaining = self.dropped_replies.get(method, 0)
        if remaining > 0:
            self.dropped_replies[method] = remaining - 1
            raise RedisConnectionError(f"{method}: reply lost")

    async def reserve(self, queue: str | None = None, *, timeout: float = 5.0) -> Any:
        self._fault("reserve")
        return await super().reserve(queue, timeout=timeout)

    async def promote_due(self, queue: str | None = None) -> int:
        self._fault("promote_due")
        return await super().promote_due(queue)

    async def sweep(self, queue: str | None = None, *, lock: float) -> list[str]:
        self._fault("sweep")
        return await super().sweep(queue, lock=lock)

    async def ack(self, reservation: Any) -> None:
        self._fault("ack")
        await super().ack(reservation)

    async def fail(self, reservation: Any, error: str) -> None:
        await super().fail(reservation, error)
        self._drop_reply("fail")


# -- fixtures --------------------------------------------------------------


@pytest.fixture
def prefix() -> str:
    """A key namespace no other test or process is using."""
    return f"keel-worker-test:{os.getpid()}:{uuid.uuid4().hex[:10]}"


@pytest.fixture
def config(redis_url: str, prefix: str) -> QueueConfig:
    return QueueConfig(driver="saq", url=redis_url, prefix=prefix, concurrency=4)


@pytest.fixture
async def queue(redis_url: str, config: QueueConfig) -> AsyncIterator[SaqQueue]:
    driver = SaqQueue.from_url(redis_url, config)
    yield driver
    await driver.clear()
    await driver.close()


@pytest.fixture
async def faulting(redis_url: str, config: QueueConfig) -> AsyncIterator[FaultingQueue]:
    driver = FaultingQueue.from_url(redis_url, config)
    yield driver
    await driver.clear()
    await driver.close()


@pytest.fixture
def events() -> EventDispatcher:
    return EventDispatcher()


@pytest.fixture
def seen(events: EventDispatcher) -> list[Any]:
    """Every lifecycle event, in order."""
    collected: list[Any] = []
    events.listen(WorkerEvent, collected.append)
    events.listen(DeadLetterDiscarded, collected.append)
    return collected


@pytest.fixture
def sink() -> RecordingSink:
    return RecordingSink()


@pytest.fixture
def make_worker(
    config: QueueConfig, queue: SaqQueue, events: EventDispatcher, sink: RecordingSink
) -> Callable[..., Worker]:
    """A worker wired to the test's queue, tuned for speed rather than production."""

    def build(**overrides: Any) -> Worker:
        settings: dict[str, Any] = {
            "queue": queue,
            "events": events,
            "failure_sink": sink,
            "name": "test-worker",
            "maintenance_interval": 0.05,
            "reserve_timeout": 0.2,
            "shutdown_grace": 10.0,
            "handle_signals": False,
        }
        settings.update(overrides)
        return Worker(config, **settings)

    return build


def of_type(seen: list[Any], event_type: type) -> list[Any]:
    """Return the collected events of one type."""
    return [event for event in seen if isinstance(event, event_type)]


async def drive(worker: Worker, until: Callable[[], bool], timeout: float = 20.0) -> None:
    """Run *worker* until *until* holds, then stop it and wait for the loop to return."""
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        try:
            with anyio.fail_after(timeout):
                while not until():
                    await anyio.sleep(0.02)
        finally:
            await worker.stop()


# -- executing work --------------------------------------------------------


async def test_a_worker_executes_a_pushed_job(
    queue: SaqQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    await queue.push(Envelope.seal(WorkerEcho("hello")))
    worker = make_worker()

    await drive(worker, lambda: bool(record.ran))

    assert record.ran == ["hello"]
    assert len(of_type(seen, JobStarted)) == 1
    assert len(of_type(seen, JobSucceeded)) == 1
    assert of_type(seen, JobSucceeded)[0].envelope.attempts == 1
    assert await queue.size() == 0


async def test_a_job_runs_under_the_context_it_was_dispatched_with(
    queue: SaqQueue, make_worker: Callable[..., Worker], events: EventDispatcher
) -> None:
    """The whole point of the slice, through a real Redis round trip.

    A context that survived `dict` but not `json` would pass every in-process
    assertion and lose the request id in production, which is why this is here
    rather than in `test_observability.py`.
    """
    listened: list[dict[str, str]] = []
    events.listen(JobSucceeded, lambda _event: listened.append(dict(correlation())))
    await queue.push(Envelope.seal(WorkerCorrelated("hello"), context={"request_id": "req-42"}))
    worker = make_worker()

    await drive(worker, lambda: bool(record.ran))

    assert record.context[0]["request_id"] == "req-42"
    assert record.context[0]["job"] == WorkerCorrelated.name
    assert record.context[0]["job_id"]
    # The lifecycle listeners are inside the binding too, so a log line written
    # from one carries the request id rather than only the handler's doing.
    assert listened and listened[0]["request_id"] == "req-42"


async def test_the_context_does_not_outlive_the_job(
    queue: SaqQueue, make_worker: Callable[..., Worker]
) -> None:
    """Two jobs, two request ids, and no leakage in either direction."""
    await queue.push(Envelope.seal(WorkerCorrelated("one"), context={"request_id": "req-1"}))
    await queue.push(Envelope.seal(WorkerCorrelated("two"), context={"request_id": "req-2"}))
    worker = make_worker()

    await drive(worker, lambda: len(record.ran) == 2)

    seen_ids = {fields["request_id"] for fields in record.context}
    assert seen_ids == {"req-1", "req-2"}
    assert correlation() == {}


async def test_a_refused_field_on_the_wire_does_not_kill_the_worker(
    queue: SaqQueue, make_worker: Callable[..., Worker]
) -> None:
    """An envelope sealed elsewhere is data, so a bad name is dropped, not raised.

    Raising here would take the worker's task group down over a field name
    chosen by an older release — one poisoned envelope for the whole replica.
    """
    await queue.push(
        Envelope.seal(
            WorkerCorrelated("odd"),
            context={"request_id": "req-9", "api_key": "hunter2", "level": "FORGED"},
        )
    )
    worker = make_worker()

    await drive(worker, lambda: bool(record.ran))

    assert record.context[0]["request_id"] == "req-9"
    assert "api_key" not in record.context[0]
    assert "level" not in record.context[0]


async def test_a_worker_serves_every_queue_it_was_given(
    queue: SaqQueue, config: QueueConfig, make_worker: Callable[..., Worker]
) -> None:
    await queue.push(Envelope.seal(WorkerEcho("fast"), queue="default"))
    await queue.push(Envelope.seal(WorkerEcho("slow-lane"), queue="slow"))
    worker = make_worker(queues=["default", "slow"])

    await drive(worker, lambda: len(record.ran) == 2)

    assert sorted(record.ran) == ["fast", "slow-lane"]
    await queue.clear("slow")


async def test_a_delayed_job_runs_once_its_delay_elapses(
    queue: SaqQueue, make_worker: Callable[..., Worker]
) -> None:
    """Nothing else promotes SAQ's scheduled set; the worker's maintenance tick does."""
    await queue.push(Envelope.seal(WorkerEcho("patient"), delay=0.5))
    worker = make_worker()

    await drive(worker, lambda: bool(record.ran))

    assert record.ran == ["patient"]


# -- the retry ladder ------------------------------------------------------


async def test_a_failing_job_is_retried_then_dead_lettered_exactly_once(
    queue: SaqQueue,
    make_worker: Callable[..., Worker],
    sink: RecordingSink,
    seen: list[Any],
) -> None:
    """Three attempts, two retries, one dead letter — and the sink is not a bin."""
    await queue.push(Envelope.seal(WorkerAlwaysFails("doomed")))
    worker = make_worker()

    await drive(worker, lambda: bool(sink.records))
    # A dead letter must not also come back as a fourth attempt.
    await anyio.sleep(0.3)

    assert record.ran == ["doomed", "doomed", "doomed"]
    assert len(sink.records) == 1

    envelope, error = sink.records[0]
    assert envelope.attempts == 3
    assert envelope.exhausted is True
    assert isinstance(error, RuntimeError)

    retries = of_type(seen, JobRetrying)
    assert [event.envelope.attempts for event in retries] == [1, 2]
    assert len(of_type(seen, JobDeadLettered)) == 1
    assert await queue.size() == 0


async def test_the_retry_delay_comes_from_the_jobs_own_backoff(
    queue: SaqQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """The worker asks the policy; it does not have one of its own."""
    await queue.push(Envelope.seal(WorkerFailsOnce("flaky")))
    worker = make_worker()

    started = time.monotonic()
    await drive(worker, lambda: bool(record.finished))
    elapsed = time.monotonic() - started

    assert record.finished == ["flaky"]
    retries = of_type(seen, JobRetrying)
    assert len(retries) == 1
    assert retries[0].retry_in == pytest.approx(0.25)
    assert elapsed >= 0.25, "the retry was made visible before its backoff elapsed"
    assert len(of_type(seen, JobSucceeded)) == 1


async def test_a_payload_that_no_longer_fits_is_not_retried(
    queue: SaqQueue, make_worker: Callable[..., Worker], sink: RecordingSink
) -> None:
    """Deterministic failure: spending the budget only delays a human seeing it."""
    await queue.push(
        Envelope(
            job=WorkerEcho.name,
            payload={"renamed_field": "boom"},
            queue="default",
            max_attempts=3,
        )
    )
    worker = make_worker()

    await drive(worker, lambda: bool(sink.records))

    assert record.ran == []
    assert len(sink.records) == 1
    assert sink.records[0][0].attempts == 1


async def test_a_broken_failure_sink_does_not_stop_the_worker(
    queue: SaqQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    await queue.push(Envelope.seal(WorkerAlwaysFails("doomed")))
    worker = make_worker(failure_sink=BrokenSink())

    await drive(worker, lambda: bool(of_type(seen, DeadLetterDiscarded)))

    discarded = of_type(seen, DeadLetterDiscarded)[0]
    assert isinstance(discarded.error, OSError)
    assert worker.running is False


async def test_the_default_sink_announces_that_nothing_was_recorded(
    queue: SaqQueue, config: QueueConfig, events: EventDispatcher, seen: list[Any]
) -> None:
    """Silence is the expensive default; this one is loud instead."""
    await queue.push(Envelope.seal(WorkerAlwaysFails("doomed")))
    worker = Worker(
        config,
        queue=queue,
        events=events,
        name="test-worker",
        maintenance_interval=0.05,
        reserve_timeout=0.2,
        handle_signals=False,
    )

    await drive(worker, lambda: bool(of_type(seen, DeadLetterDiscarded)))

    assert isinstance(of_type(seen, DeadLetterDiscarded)[0].error, RuntimeError)


async def test_the_default_sink_is_the_one_the_worker_picks(
    config: QueueConfig, queue: SaqQueue, events: EventDispatcher
) -> None:
    worker = Worker(config, queue=queue, events=events)
    # Nothing public exposes the sink, so prove the type is constructible and
    # behaves, which is what the worker relies on.
    envelope = Envelope.seal(WorkerEcho("x"))
    collected: list[Any] = []
    events.listen(DeadLetterDiscarded, collected.append)

    await EventFailureSink(events).record(envelope, RuntimeError("boom"))

    assert collected[0].envelope == envelope
    assert worker.name.endswith(str(os.getpid()))


# -- unknown jobs ----------------------------------------------------------


async def test_an_unknown_job_is_offered_again_without_spending_an_attempt(
    queue: SaqQueue,
    make_worker: Callable[..., Worker],
    sink: RecordingSink,
    seen: list[Any],
) -> None:
    """A rollout artefact, not a failure: the worker is wrong, the job is fine."""
    envelope = Envelope(
        job="AJobOnlyTheNewCodeDefines", payload={}, queue="default", max_attempts=3
    )
    await queue.push(envelope)
    worker = make_worker(unroutable_delay=30.0)

    await drive(worker, lambda: bool(of_type(seen, JobUnroutable)))

    event = of_type(seen, JobUnroutable)[0]
    assert event.retry_in == 30.0
    assert event.envelope.attempts == 0, "no attempt was made, so none was spent"
    assert sink.records == []
    assert of_type(seen, JobDeadLettered) == []
    # Back on the queue, invisible until the delay elapses.
    assert await queue.size() == 0
    assert await queue.lane("default").count("incomplete") == 1


async def test_an_unknown_job_is_eventually_dead_lettered(
    queue: SaqQueue,
    make_worker: Callable[..., Worker],
    sink: RecordingSink,
    seen: list[Any],
) -> None:
    """A rollout takes minutes; past that, nothing is ever going to route it."""
    envelope = Envelope(
        job="AJobNobodyDefinesAnyMore",
        payload={},
        queue="default",
        max_attempts=3,
        dispatched_at=time.time() - 7200,
    )
    await queue.push(envelope)
    worker = make_worker(unroutable_after=3600.0)

    await drive(worker, lambda: bool(sink.records))

    assert of_type(seen, JobUnroutable)[0].retry_in is None
    assert len(sink.records) == 1
    assert sink.records[0][0].attempts == 0
    assert await queue.lane("default").count("incomplete") == 0


# -- liveness --------------------------------------------------------------


async def test_health_tracks_the_loop(queue: SaqQueue, make_worker: Callable[..., Worker]) -> None:
    worker = make_worker()
    assert worker.healthy is False
    assert worker.accepting is False

    observed: list[tuple[bool, bool, float]] = []

    async def look() -> None:
        observed.append((worker.healthy, worker.accepting, worker.last_activity))

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        with anyio.fail_after(10):
            while not worker.running:
                await anyio.sleep(0.01)
            await anyio.sleep(0.2)
            await look()
        await worker.stop()

    assert observed == [(True, True, observed[0][2])]
    assert observed[0][2] > 0
    assert worker.healthy is False


async def test_a_worker_refuses_to_run_twice(
    queue: SaqQueue, make_worker: Callable[..., Worker]
) -> None:
    worker = make_worker()

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        with anyio.fail_after(10):
            while not worker.running:
                await anyio.sleep(0.01)
        with pytest.raises(RuntimeError, match="already running"):
            await worker.run()
        await worker.stop()


async def test_a_worker_without_a_consume_side_says_so(redis_url: str) -> None:
    worker = Worker(QueueConfig(driver="sync"))
    with pytest.raises(ConfigurationError, match="no consume side"):
        await worker.run()


async def test_stopping_before_running_makes_the_run_a_no_op(
    queue: SaqQueue, make_worker: Callable[..., Worker]
) -> None:
    await queue.push(Envelope.seal(WorkerEcho("never")))
    worker = make_worker()

    await worker.stop()
    with anyio.fail_after(10):
        await worker.run()

    assert record.ran == []
    assert await queue.size() == 1


# -- graceful shutdown -----------------------------------------------------


async def _await_signal_handlers(worker: Worker) -> None:
    """Never send a signal to the test process before the worker can catch it."""
    with anyio.fail_after(10):
        while not worker.handles_signals:
            await anyio.sleep(0.01)


async def test_sigterm_lets_an_in_flight_job_finish_and_returns(
    queue: SaqQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """The reason the worker is a class and not a loop.

    A job is half done when the orchestrator asks the process to go away. It
    must finish, the loop must stop reserving, and `run()` must return on its
    own — no cancellation, no lost work, no waiting out the grace period.
    """
    record.hold = 0.8
    record.started = anyio.Event()
    await queue.push(Envelope.seal(WorkerSlow("in-flight")))
    worker = make_worker(handle_signals=True, shutdown_grace=30.0)

    started_at = time.monotonic()
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        await _await_signal_handlers(worker)
        with anyio.fail_after(10):
            assert record.started is not None
            await record.started.wait()

        assert worker.in_flight == 1
        os.kill(os.getpid(), signal.SIGTERM)

        with anyio.fail_after(10):
            while worker.accepting:
                await anyio.sleep(0.01)
        assert worker.healthy is True, "a draining worker is still alive"
    elapsed = time.monotonic() - started_at

    assert record.finished == ["in-flight"], "the in-flight job was cut short"
    assert worker.running is False
    assert elapsed < 25.0, "run() waited out the grace period instead of returning"

    stopped = of_type(seen, WorkerStopped)
    assert len(stopped) == 1
    assert stopped[0].forced is False


async def test_a_second_signal_cancels_what_the_first_was_waiting_for(
    queue: SaqQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """An operator who asks twice has decided the job is not worth waiting for."""
    record.hold = 30.0
    record.started = anyio.Event()
    await queue.push(Envelope.seal(WorkerSlow("doomed")))
    worker = make_worker(handle_signals=True, shutdown_grace=300.0)

    started_at = time.monotonic()
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        await _await_signal_handlers(worker)
        with anyio.fail_after(10):
            assert record.started is not None
            await record.started.wait()

        os.kill(os.getpid(), signal.SIGTERM)
        with anyio.fail_after(10):
            while worker.accepting:
                await anyio.sleep(0.01)
        # A different signal, because two of the same can coalesce before the
        # interpreter runs its handler.
        os.kill(os.getpid(), signal.SIGINT)
    elapsed = time.monotonic() - started_at

    assert elapsed < 20.0
    assert record.finished == [], "the second signal should not have waited"
    assert of_type(seen, WorkerStopped)[0].forced is True


async def test_the_grace_period_bounds_a_job_that_will_not_finish(
    queue: SaqQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """A wedged job must not hold the process past the orchestrator's patience."""
    record.hold = 30.0
    record.started = anyio.Event()
    await queue.push(Envelope.seal(WorkerSlow("wedged")))
    worker = make_worker(shutdown_grace=0.3)

    started_at = time.monotonic()
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        with anyio.fail_after(10):
            assert record.started is not None
            await record.started.wait()
        await worker.stop()
    elapsed = time.monotonic() - started_at

    assert elapsed < 10.0
    assert record.finished == []
    assert of_type(seen, WorkerStopped)[0].forced is True


async def test_signal_handlers_are_not_installed_when_declined(
    queue: SaqQueue, make_worker: Callable[..., Worker]
) -> None:
    worker = make_worker(handle_signals=False)

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        with anyio.fail_after(10):
            while not worker.running:
                await anyio.sleep(0.01)
        assert worker.handles_signals is False
        await worker.stop()


# -- crash recovery --------------------------------------------------------


async def test_killing_a_worker_mid_job_loses_nothing(
    queue: SaqQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """The phase's headline claim, made against a real Redis.

    A worker is killed the way a SIGKILL or an OOM kill takes one: no drain, no
    acknowledgement, no failure recorded — the job simply stops existing from
    the process's point of view while still sitting on the driver's active list.
    A second worker then has to notice, put it back, and run it to completion,
    on the budget the dead attempt already spent.
    """
    record.started = anyio.Event()
    await queue.push(Envelope.seal(WorkerSurvivesACrash("orphan")))

    victim = make_worker(sweep_interval=1.0)
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(victim.run)
        with anyio.fail_after(10):
            assert record.started is not None
            await record.started.wait()
        victim.kill()

    assert record.finished == [], "the victim was supposed to die holding the job"
    assert await queue.size() == 0, "an orphan is invisible, which is why it needs sweeping"

    # Nothing can call it abandoned until it has outrun its own timeout.
    await anyio.sleep(1.3)

    survivor = make_worker(sweep_interval=1.0)
    await drive(survivor, lambda: bool(record.finished))

    assert record.ran == ["orphan", "orphan"]
    assert record.finished == ["orphan"]
    assert of_type(seen, JobRecovered), "the recovery should be observable, not silent"

    succeeded = of_type(seen, JobSucceeded)
    assert len(succeeded) == 1
    assert succeeded[0].envelope.attempts == 2, "recovery must not refund the dead attempt"
    assert await queue.size() == 0


async def test_a_job_that_keeps_killing_its_worker_is_not_immortal(
    queue: SaqQueue, make_worker: Callable[..., Worker], sink: RecordingSink
) -> None:
    """Recovery without a budget would hand a poison job the whole fleet, forever."""
    await queue.push(
        Envelope(
            job=WorkerEcho.name,
            payload={"message": "immortal"},
            queue="default",
            max_attempts=3,
            attempts=3,
        )
    )
    worker = make_worker()

    await drive(worker, lambda: bool(sink.records))

    assert record.ran == [], "an over-budget job must not be handed to a handler again"
    assert len(sink.records) == 1
    assert sink.records[0][0].attempts == 4
    assert await queue.size() == 0


async def test_a_healthy_workers_own_jobs_are_never_swept(
    queue: SaqQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """A sweep landing on live work would run it twice; the timers make that likely."""
    record.hold = 1.5
    record.started = anyio.Event()
    await queue.push(Envelope.seal(WorkerSlowButTimed("live")))
    worker = make_worker(sweep_interval=1.0)

    await drive(worker, lambda: bool(record.finished))

    assert record.ran == ["live"], "the job was reserved more than once"
    assert of_type(seen, JobRecovered) == []


# -- surviving the driver ---------------------------------------------------

INSTANT: Backoff = FixedBackoff(seconds=0.02, jitter=0.0)
"""Fault backoff for tests: long enough to be a wait, short enough not to notice."""


async def test_a_reserve_error_does_not_cancel_the_jobs_in_flight(
    faulting: FaultingQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """A transient Redis error while polling must not end the run or the jobs in it.

    Before the fix, `reserve` raising propagated out of the worker's task group,
    which cancelled its siblings — the running jobs included — and skipped the
    grace period entirely. One failover then cost every job on the replica.
    """
    record.hold = 0.6
    record.started = anyio.Event()
    await faulting.push(Envelope.seal(WorkerSlow("in-flight")))
    worker = make_worker(queue=faulting, fault_backoff=INSTANT)

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        with anyio.fail_after(10):
            assert record.started is not None
            await record.started.wait()
        faulting.faults["reserve"] = 1
        await faulting.push(Envelope.seal(WorkerEcho("after")))
        with anyio.fail_after(10):
            while "after" not in record.ran:
                await anyio.sleep(0.02)
        await worker.stop()

    assert record.finished == ["in-flight"], "the in-flight job was cancelled"
    faulted = of_type(seen, WorkerFaulted)
    assert [event.activity for event in faulted] == ["reserve"]
    assert faulted[0].lane == "default"
    assert isinstance(faulted[0].error, RedisConnectionError)
    stopped = of_type(seen, WorkerStopped)
    assert len(stopped) == 1 and stopped[0].forced is False


@pytest.mark.parametrize(("method", "activity"), [("promote_due", "promote"), ("sweep", "sweep")])
async def test_a_maintenance_error_does_not_stop_the_worker(
    faulting: FaultingQueue,
    make_worker: Callable[..., Worker],
    seen: list[Any],
    method: str,
    activity: str,
) -> None:
    """The maintenance and sweep loops fault the same way the reserve loop does."""
    faulting.faults[method] = 2
    await faulting.push(Envelope.seal(WorkerEcho("still served")))
    worker = make_worker(queue=faulting, fault_backoff=INSTANT, sweep_interval=0.05)

    await drive(worker, lambda: bool(record.ran) and len(of_type(seen, WorkerFaulted)) == 2)

    assert record.ran == ["still served"]
    faulted = of_type(seen, WorkerFaulted)
    assert [event.activity for event in faulted] == [activity, activity]
    assert [event.failures for event in faulted] == [1, 2]
    assert faulting.faults[method] == 0, "the loop stopped retrying before the fault cleared"
    assert of_type(seen, WorkerStopped)[0].forced is False


async def test_consecutive_faults_back_off_and_the_worker_stays_alive(
    faulting: FaultingQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """The wait grows per consecutive failure, and a faulting loop is a live loop.

    A restart does not fix an unreachable Redis, so liveness must not flap
    during an outage: the heartbeat is refreshed by the retry itself.
    """
    faulting.faults["reserve"] = 3
    await faulting.push(Envelope.seal(WorkerEcho("eventually")))
    worker = make_worker(
        queue=faulting,
        fault_backoff=ExponentialBackoff(base=0.02, factor=2.0, maximum=1.0, jitter=0.0),
        heartbeat_timeout=0.5,
    )

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        with anyio.fail_after(10):
            while len(of_type(seen, WorkerFaulted)) < 3:
                await anyio.sleep(0.01)
        assert worker.healthy is True, "an outage is not a hung loop"
        with anyio.fail_after(10):
            while not record.ran:
                await anyio.sleep(0.02)
        await worker.stop()

    faulted = of_type(seen, WorkerFaulted)
    assert [event.failures for event in faulted] == [1, 2, 3]
    assert [event.retry_in for event in faulted] == [0.02, 0.04, 0.08]
    assert record.ran == ["eventually"]


async def test_a_stop_request_cuts_a_fault_wait_short(
    faulting: FaultingQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """Backing off for a minute must not make a SIGTERM take a minute."""
    faulting.faults["reserve"] = 100
    worker = make_worker(queue=faulting, fault_backoff=FixedBackoff(seconds=60.0, jitter=0.0))

    started_at = time.monotonic()
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        with anyio.fail_after(10):
            while not of_type(seen, WorkerFaulted):
                await anyio.sleep(0.02)
        await anyio.sleep(0.1)
        await worker.stop()

    assert time.monotonic() - started_at < 5.0, "stop() waited out the fault backoff"
    assert worker.running is False


async def test_an_outcome_the_queue_cannot_record_is_reported_and_recovered(
    faulting: FaultingQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """A job ran, but Redis would not take the ack. The worker must go on, and
    the job must come back, because the reservation is still on the active list.
    """
    faulting.faults["ack"] = 1
    await faulting.push(
        Envelope(
            job=WorkerEcho.name,
            payload={"message": "twice"},
            queue="default",
            max_attempts=3,
            timeout=1.0,
        )
    )
    worker = make_worker(queue=faulting, fault_backoff=INSTANT, sweep_interval=0.5)

    await drive(worker, lambda: len(of_type(seen, JobSucceeded)) == 1)

    assert record.ran == ["twice", "twice"]
    unsettled = of_type(seen, JobUnsettled)
    assert len(unsettled) == 1
    assert isinstance(unsettled[0].error, RedisConnectionError)
    assert unsettled[0].envelope.attempts == 1
    assert of_type(seen, JobRecovered), "the redelivery should be the sweep's, and visible"
    assert of_type(seen, JobSucceeded)[0].envelope.attempts == 2
    assert worker.in_flight == 0
    assert await faulting.size() == 0
    assert of_type(seen, WorkerStopped)[0].forced is False


async def test_a_dead_letter_survives_a_driver_that_drops_the_reply(
    faulting: FaultingQueue,
    make_worker: Callable[..., Worker],
    seen: list[Any],
    sink: RecordingSink,
) -> None:
    """`fail()` landed on Redis and raised in the client. The sink must still be written.

    The review found the terminal branches called the driver first and the sink
    second, so a lost reply skipped the sink: the job was finished on the server,
    absent from every list, and recorded nowhere — the one outcome a dead-letter
    store exists to rule out.
    """
    faulting.dropped_replies["fail"] = 1
    await faulting.push(
        Envelope(
            job=WorkerAlwaysFails.name, payload={"token": "pay me"}, queue="default", max_attempts=1
        )
    )
    worker = make_worker(queue=faulting, fault_backoff=INSTANT)

    await drive(worker, lambda: bool(sink.records))

    assert record.ran == ["pay me"]
    assert len(sink.records) == 1
    assert len(of_type(seen, JobDeadLettered)) == 1
    assert len(of_type(seen, JobUnsettled)) == 1, "the driver's error is still reported"
    assert await faulting.size() == 0


async def test_a_fault_between_the_dequeue_and_the_start_does_not_strand_the_job(
    faulting: FaultingQueue, make_worker: Callable[..., Worker], seen: list[Any]
) -> None:
    """The one gap the sweep refuses to cover, so `reserve` has to cover it itself.

    SAQ's dequeue has moved the job to the active list; Keel's write of the
    start fails. Without a re-queue the job sits there with no start for ever,
    invisible to `size()` and to the sweep, and the fault event names no job.
    """
    lane = faulting.lane("default")
    real_update = lane.update
    calls = 0

    async def update_once_broken(*args: Any, **kwargs: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RedisConnectionError("update: connection lost")
        await real_update(*args, **kwargs)

    lane.update = update_once_broken  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    await faulting.push(Envelope.seal(WorkerEcho("returned")))
    worker = make_worker(queue=faulting, fault_backoff=INSTANT)

    await drive(worker, lambda: bool(record.ran))

    assert record.ran == ["returned"]
    assert [event.activity for event in of_type(seen, WorkerFaulted)] == ["reserve"]
    succeeded = of_type(seen, JobSucceeded)
    assert succeeded[0].envelope.attempts == 1, (
        "a reservation that was never made is not an attempt"
    )
    assert await faulting.size() == 0


async def test_a_worker_that_cannot_reach_its_queue_stops_reporting_ready(
    faulting: FaultingQueue, make_worker: Callable[..., Worker]
) -> None:
    """Liveness holds through an outage; readiness must not, or a bad rollout goes green."""
    for method in ("reserve", "promote_due", "sweep"):
        faulting.faults[method] = 10_000
    worker = make_worker(
        queue=faulting, fault_backoff=INSTANT, heartbeat_timeout=0.3, sweep_interval=0.05
    )

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(worker.run)
        with anyio.fail_after(5):
            while not worker.running:
                await anyio.sleep(0.02)
            ready_at_first = worker.accepting
            assert ready_at_first, "ready until the queue has gone quiet for a while"
            while worker.accepting:
                await anyio.sleep(0.02)
        assert worker.healthy is True, "an unanswered queue is not a hung loop"
        assert time.time() - worker.last_success >= 0.3
        assert time.time() - worker.last_activity < 0.3
        await worker.stop()
