"""The SAQ driver, against a real Redis.

Almost nothing here can be proved against a fake. The three properties that
matter — that a duplicate loses an atomic race, that a delayed job is genuinely
invisible, and that `clear()` cannot reach a neighbour's keys — are all
statements about what Redis does, and a double that agreed with them would only
be agreeing with this file's author.

Every test namespaces itself with a fresh prefix, because the development Redis is
shared with the cache suite and with whatever else is running on the machine.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, replace
from typing import ClassVar

import anyio
import pytest
from redis.asyncio import Redis
from saq.job import Status
from saq.queue.redis import RedisQueue

from keel.exceptions import ConfigurationError, SerializationError
from keel.queue import Envelope, Job, QueueConfig, QueueManager
from keel.queue.saq_driver import (
    ENVELOPE_KWARG,
    SWEEP_LOCK_KEY,
    SaqQueue,
    decode,
    encode,
)

pytestmark = [pytest.mark.anyio, pytest.mark.redis]


# -- jobs ------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SaqEcho(Job):
    """A job whose only job is to survive a round trip."""

    message: str

    async def handle(self) -> None:
        """Do nothing; this driver suite never runs handlers."""


@dataclass(frozen=True, slots=True)
class SaqBrief(Job):
    """A job whose short timeout makes it recognisable as orphaned in a second."""

    message: str

    timeout: ClassVar[float | None] = 1.0

    async def handle(self) -> None:
        """Do nothing."""


@dataclass(frozen=True, slots=True)
class SaqUntimed(Job):
    """A job with no timeout, which is therefore unrecoverable by design."""

    message: str

    timeout: ClassVar[float | None] = None

    async def handle(self) -> None:
        """Do nothing."""


@dataclass(frozen=True, slots=True)
class SaqUniqueEcho(Job):
    """A job type where a second identical dispatch should be dropped."""

    message: str

    unique_for: ClassVar[float | None] = 60.0

    async def handle(self) -> None:
        """Do nothing."""


# -- fixtures --------------------------------------------------------------


@pytest.fixture
def prefix() -> str:
    """A key namespace no other test or process is using."""
    return f"keel-saq-test:{os.getpid()}:{uuid.uuid4().hex[:10]}"


@pytest.fixture
def config(redis_url: str, prefix: str) -> QueueConfig:
    """A saq configuration pointed at this test's own namespace."""
    return QueueConfig(driver="saq", url=redis_url, prefix=prefix, concurrency=2)


@pytest.fixture
async def queue(redis_url: str, config: QueueConfig) -> AsyncIterator[SaqQueue]:
    """A queue that cleans up both lanes the suite uses."""
    driver = SaqQueue.from_url(redis_url, config)
    yield driver
    for lane in ("default", "slow"):
        await driver.clear(lane)
    await driver.close()


# -- the round trip --------------------------------------------------------


async def test_an_envelope_survives_the_round_trip(queue: SaqQueue) -> None:
    """The whole point of the adapter: what comes back is what went in."""
    envelope = Envelope.seal(SaqEcho("hello"), context={"request_id": "r-1"})

    assert await queue.push(envelope) == envelope.id

    reservation = await queue.reserve(timeout=2)
    assert reservation is not None
    assert reservation.lane == "default"
    # Reserving charges the attempt, so that is the one field expected to move.
    assert reservation.envelope == replace(envelope, attempts=1)
    assert reservation.envelope.open() == SaqEcho("hello")
    assert reservation.envelope.context == {"request_id": "r-1"}


async def test_encode_and_decode_are_inverses() -> None:
    envelope = Envelope.seal(SaqEcho("x"), delay=3.5, context={"tenant": "acme"})
    assert decode(encode(envelope)) == envelope


async def test_decode_refuses_a_job_that_keel_did_not_dispatch() -> None:
    """A shared lane name is a real deployment mistake; it should say so."""
    with pytest.raises(SerializationError) as error:
        decode({"a": 1, "b": 2})
    assert ENVELOPE_KWARG in str(error.value)


async def test_decode_drops_fields_this_code_does_not_know() -> None:
    """Forward compatibility: last week's worker reads this week's envelope."""
    envelope = Envelope.seal(SaqEcho("x"))
    payload = encode(envelope)
    payload[ENVELOPE_KWARG]["invented_next_week"] = "surprise"

    assert decode(payload) == envelope


async def test_push_does_not_run_the_job(queue: SaqQueue) -> None:
    await queue.push(Envelope.seal(SaqEcho("unrun")))
    assert await queue.size() == 1


async def test_push_many_keeps_dispatch_order(queue: SaqQueue) -> None:
    envelopes = [Envelope.seal(SaqEcho(f"m-{index}")) for index in range(5)]

    assert await queue.push_many(envelopes) == [item.id for item in envelopes]
    assert await queue.size() == 5


async def test_the_connection_reports_its_configured_name(queue: SaqQueue) -> None:
    assert queue.name == "saq"


async def test_the_manager_builds_this_driver_for_the_saq_config(config: QueueConfig) -> None:
    """`manager.py` calls `SaqQueue.from_url(url, config)`; prove the signature holds."""
    manager = QueueManager(config)
    try:
        assert isinstance(manager.connection(), SaqQueue)
    finally:
        await manager.close()


# -- uniqueness ------------------------------------------------------------


async def test_a_duplicate_unique_dispatch_is_dropped(queue: SaqQueue) -> None:
    first = Envelope.seal(SaqUniqueEcho("same"))
    second = Envelope.seal(SaqUniqueEcho("same"))
    assert first.unique_key == second.unique_key
    assert first.id != second.id

    assert await queue.push(first) == first.id
    assert await queue.push(second) == first.id, "the caller must be told which id won"
    assert await queue.size() == 1


async def test_uniqueness_does_not_leak_between_payloads(queue: SaqQueue) -> None:
    await queue.push(Envelope.seal(SaqUniqueEcho("a")))
    await queue.push(Envelope.seal(SaqUniqueEcho("b")))
    assert await queue.size() == 2


async def test_a_unique_job_can_be_dispatched_again_once_it_is_done(queue: SaqQueue) -> None:
    """Uniqueness is "already in flight", not "ever dispatched"."""
    first = Envelope.seal(SaqUniqueEcho("recurring"))
    await queue.push(first)
    reservation = await queue.reserve(timeout=2)
    assert reservation is not None
    await queue.ack(reservation)

    second = Envelope.seal(SaqUniqueEcho("recurring"))
    assert await queue.push(second) == second.id


# -- delay -----------------------------------------------------------------


async def test_a_delayed_job_is_not_immediately_available(queue: SaqQueue) -> None:
    await queue.push(Envelope.seal(SaqEcho("later"), delay=60))

    assert await queue.size() == 0
    assert await queue.reserve(timeout=0.2) is None


async def test_a_delayed_job_becomes_available_once_promoted(queue: SaqQueue) -> None:
    """SAQ promotes on demand, so a worker that never asks looks like a lost job."""
    await queue.push(Envelope.seal(SaqEcho("soon"), delay=0.5))
    assert await queue.size() == 0

    await anyio.sleep(1.0)

    assert await queue.promote_due() == 1
    assert await queue.size() == 1


async def test_size_counts_only_what_a_worker_could_take(queue: SaqQueue) -> None:
    await queue.push(Envelope.seal(SaqEcho("now")))
    await queue.push(Envelope.seal(SaqEcho("later"), delay=300))

    assert await queue.size() == 1


# -- namespace safety ------------------------------------------------------


async def test_clear_does_not_touch_another_namespace(redis_url: str, prefix: str) -> None:
    """The property `RedisStore.flush` has, restated for jobs."""
    mine = SaqQueue.from_url(
        redis_url, QueueConfig(driver="saq", url=redis_url, prefix=f"{prefix}:mine")
    )
    theirs = SaqQueue.from_url(
        redis_url, QueueConfig(driver="saq", url=redis_url, prefix=f"{prefix}:theirs")
    )
    try:
        await mine.push(Envelope.seal(SaqEcho("mine")))
        await theirs.push(Envelope.seal(SaqEcho("theirs")))

        assert await mine.clear() == 1

        assert await mine.size() == 0
        assert await theirs.size() == 1, "a clear reached outside its own prefix"
    finally:
        await mine.clear()
        await theirs.clear()
        await mine.close()
        await theirs.close()


async def test_clear_does_not_touch_another_lane_in_the_same_namespace(
    queue: SaqQueue,
) -> None:
    await queue.push(Envelope.seal(SaqEcho("fast")))
    await queue.push(Envelope.seal(SaqEcho("slow"), queue="slow"))

    assert await queue.clear("default") == 1

    assert await queue.size("default") == 0
    assert await queue.size("slow") == 1


async def test_a_glob_metacharacter_in_the_prefix_is_escaped(redis_url: str, prefix: str) -> None:
    """`app[1]` must not select `app1`'s keys — the bug `escape_glob` exists for."""
    bracketed = SaqQueue.from_url(
        redis_url, QueueConfig(driver="saq", url=redis_url, prefix=f"{prefix}:app[1]")
    )
    plain = SaqQueue.from_url(
        redis_url, QueueConfig(driver="saq", url=redis_url, prefix=f"{prefix}:app1")
    )
    try:
        await bracketed.push(Envelope.seal(SaqEcho("bracketed")))
        await plain.push(Envelope.seal(SaqEcho("plain")))

        await bracketed.clear()

        assert await plain.size() == 1
    finally:
        await bracketed.clear()
        await plain.clear()
        await bracketed.close()
        await plain.close()


async def test_clear_reports_the_unfinished_jobs_it_discarded(queue: SaqQueue) -> None:
    await queue.push(Envelope.seal(SaqEcho("waiting")))
    await queue.push(Envelope.seal(SaqEcho("scheduled"), delay=300))

    assert await queue.clear() == 2


async def test_an_empty_prefix_is_refused(redis_url: str) -> None:
    """Silently meaning "everything" is how an incident becomes an outage."""
    client: Redis = Redis.from_url(redis_url, decode_responses=False)
    try:
        # ":" gets past QueueConfig's own emptiness check and still namespaces
        # nothing, which is exactly the case the driver has to catch itself.
        config = QueueConfig(driver="saq", url=redis_url, prefix=":")
        lane = RedisQueue(client, name=":default")
        with pytest.raises(ConfigurationError) as error:
            SaqQueue(lane, config)
        assert "non-empty prefix" in str(error.value)
    finally:
        await client.aclose()


async def test_a_lane_named_outside_the_configured_namespace_is_refused(
    redis_url: str, config: QueueConfig
) -> None:
    client: Redis = Redis.from_url(redis_url, decode_responses=False)
    try:
        lane = RedisQueue(client, name="somebody-elses-queue")
        with pytest.raises(ConfigurationError) as error:
            SaqQueue(lane, config)
        assert SaqQueue.lane_name(config.prefix, config.default_queue) in str(error.value)
    finally:
        await client.aclose()


# -- client ownership ------------------------------------------------------


def _spy_on_close(client: Redis) -> Callable[[], bool]:
    """Record whether the client is closed, without closing it for real.

    Copied from the cache's Redis suite for the same reason it exists there:
    asserting on a client's behaviour after a close tests redis-py's quiet
    reconnect, not Keel's ownership decision.
    """
    closed = False
    original = client.aclose

    async def spy(close_connection_pool: bool | None = None) -> None:
        nonlocal closed
        closed = True
        await original(close_connection_pool)

    client.aclose = spy  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    return lambda: closed


async def test_a_queue_given_a_lane_does_not_close_its_client(
    redis_url: str, config: QueueConfig
) -> None:
    """Closing a pool the application still dispatches through is a bug."""
    client: Redis = Redis.from_url(redis_url, decode_responses=False)
    lane = RedisQueue(client, name=SaqQueue.lane_name(config.prefix, config.default_queue))
    queue = SaqQueue(lane, config)
    was_closed = _spy_on_close(client)

    await queue.close()

    assert was_closed() is False
    await client.aclose()


async def test_a_queue_that_built_its_client_closes_it(redis_url: str, config: QueueConfig) -> None:
    queue = SaqQueue.from_url(redis_url, config)
    was_closed = _spy_on_close(queue.client)

    await queue.close()

    assert was_closed() is True


# -- lanes -----------------------------------------------------------------


async def test_lanes_are_memoised_and_share_one_client(queue: SaqQueue) -> None:
    assert queue.lane("slow") is queue.lane("slow")
    assert queue.lane("slow").redis is queue.lane("default").redis


async def test_a_lane_is_named_with_the_prefix(queue: SaqQueue, prefix: str) -> None:
    assert queue.lane("slow").name == f"{prefix}:slow"


# -- crash recovery --------------------------------------------------------


async def test_reserving_writes_the_charged_attempt_back(queue: SaqQueue) -> None:
    """The charge has to reach Redis, or a sweep would hand back a fresh budget."""
    envelope = Envelope.seal(SaqEcho("charged"))
    await queue.push(envelope)

    assert await queue.reserve(timeout=2) is not None

    stored = await queue.lane("default").job(envelope.id)
    assert stored is not None
    assert decode(stored.kwargs).attempts == 1
    assert stored.status is Status.ACTIVE
    assert stored.started > 0
    assert stored.scheduled == 0, "a reserved job must not look due to the scheduler"


async def test_sweep_leaves_a_job_that_only_just_started_alone(queue: SaqQueue) -> None:
    """The failure mode this rule exists for: stealing work from a live worker."""
    await queue.push(Envelope.seal(SaqBrief("running")))
    assert await queue.reserve(timeout=2) is not None

    assert await queue.sweep(lock=60) == []


async def test_sweep_requeues_an_orphan_with_the_budget_it_has_left(queue: SaqQueue) -> None:
    await queue.push(Envelope.seal(SaqBrief("orphan")))
    assert await queue.reserve(timeout=2) is not None

    await anyio.sleep(1.2)

    assert len(await queue.sweep(lock=60)) == 1
    assert await queue.size() == 1

    again = await queue.reserve(timeout=2)
    assert again is not None
    assert again.envelope.attempts == 2, "the attempt that died must still be charged"


async def test_a_job_with_no_timeout_cannot_be_recovered(queue: SaqQueue) -> None:
    """Documented consequence: nothing tells it apart from a job still running."""
    await queue.push(Envelope.seal(SaqUntimed("forever")))
    assert await queue.reserve(timeout=2) is not None

    await anyio.sleep(1.2)

    assert await queue.sweep(lock=60) == []


async def test_sweep_discards_an_active_id_with_no_record(queue: SaqQueue) -> None:
    envelope = Envelope.seal(SaqBrief("ghost"))
    await queue.push(envelope)
    assert await queue.reserve(timeout=2) is not None
    lane = queue.lane("default")
    await queue.client.delete(lane.job_id(envelope.id))

    assert len(await queue.sweep(lock=60)) == 1
    assert await queue.client.llen(lane.namespace("active")) == 0


async def test_only_one_sweeper_runs_per_lock_window(
    redis_url: str, config: QueueConfig, queue: SaqQueue
) -> None:
    """Two replicas sweeping one lane must not both scan it."""
    other = SaqQueue.from_url(redis_url, config)
    lane = queue.lane("default")
    try:
        assert await other.sweep(lock=60) == [], "nothing to sweep, but the lock is taken"

        await queue.push(Envelope.seal(SaqBrief("orphan")))
        assert await queue.reserve(timeout=2) is not None
        await anyio.sleep(1.2)

        assert await queue.sweep(lock=60) == [], "the lock did not hold the second sweeper off"
        assert await queue.client.llen(lane.namespace("active")) == 1

        await queue.client.delete(lane.namespace(SWEEP_LOCK_KEY))

        assert len(await queue.sweep(lock=60)) == 1
        assert await queue.client.llen(lane.namespace("active")) == 0
    finally:
        await other.close()


# -- transactional retry ---------------------------------------------------


async def test_retry_puts_a_reserved_job_straight_back(queue: SaqQueue) -> None:
    await queue.push(Envelope.seal(SaqEcho("again")))
    reservation = await queue.reserve(timeout=2)
    assert reservation is not None

    await queue.retry(reservation, reservation.envelope, delay=0.0, error="boom")

    assert await queue.size() == 1
    again = await queue.reserve(timeout=2)
    assert again is not None
    assert again.envelope.attempts == 2


async def test_a_delayed_retry_is_not_immediately_available(queue: SaqQueue) -> None:
    await queue.push(Envelope.seal(SaqEcho("later")))
    reservation = await queue.reserve(timeout=2)
    assert reservation is not None

    await queue.retry(reservation, reservation.envelope, delay=60.0, error="boom")

    assert await queue.size() == 0
    assert await queue.lane("default").count("incomplete") == 1


async def test_clear_with_no_argument_clears_only_the_default_queue(queue: SaqQueue) -> None:
    """The resolved reading of the contract: `None` is the default lane, never all."""
    await queue.push(Envelope.seal(SaqEcho("fast")))
    await queue.push(Envelope.seal(SaqEcho("slow"), queue="slow"))

    assert await queue.clear() == 1

    assert await queue.size("default") == 0
    assert await queue.size("slow") == 1
