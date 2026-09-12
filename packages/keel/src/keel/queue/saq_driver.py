"""The SAQ queue driver.

An **Adapter**, in the strict sense: SAQ already implements everything Keel needs
from a Redis-backed queue — a Lua-atomic enqueue, a scheduled set, a blocking
reserve, a sweeper — and speaks about it in a vocabulary that is not Keel's. It
has its own ``Job`` dataclass with its own ``attempts``, its own ``timeout`` and
its own retry policy. This module translates, and translates only.

Four decisions are worth knowing before changing anything here.

**The envelope travels as one opaque kwarg, not as SAQ job fields.** SAQ's
``Job`` has ``attempts``, ``timeout`` and ``status`` fields that SAQ itself
mutates during a job's life. Spreading Keel's envelope across them would mean
SAQ's bookkeeping silently rewriting Keel's wire format — an envelope is a
*record of a dispatch* and must come back exactly as it went in. So
:data:`ENVELOPE_KWARG` carries the whole dataclass as a plain dict, and the SAQ
fields we do set (``key``, ``scheduled``, ``timeout``, ``retries``) are hints to
SAQ's scheduler rather than the source of truth.

**One Keel queue owns a family of SAQ queues.** SAQ's ``Queue`` is one named
lane; Keel's :class:`~keel.contracts.queue.Queue` routes by ``envelope.queue``.
So this adapter holds a lane per queue name, all sharing one Redis client, built
on demand. Anything else would mean a connection pool per lane.

**The namespace lives in the lane's name.** SAQ derives every key it writes from
``self.name`` — ``saq:{name}:*`` and ``saq:job:{name}:*`` — so putting Keel's
prefix *into* the name is what makes :meth:`SaqQueue.clear` provably unable to
touch anything else, in exactly the way ``RedisStore.flush`` cannot. The same
reasoning applies: a shared Redis holds other applications, and an
administrative operation that can reach them is a loaded gun.

**An attempt is charged when a job is reserved, not when it fails.** A delivery
that kills the worker holding it is still a delivery, so :meth:`SaqQueue.reserve`
writes the incremented envelope back before the handler runs. That is what makes
:meth:`SaqQueue.sweep` safe to wire up: a recovered job resumes on the budget it
has left instead of a fresh one, and a job that crashes its worker every time
dies after ``max_attempts`` rather than eating the fleet.

Declined:

* **SAQ's own retry and backoff** (``retries``/``retry_delay``/``retry_backoff``).
  Keel's backoff is a Strategy that lives on the job class, and SAQ cannot see
  it. Two retry mechanisms racing over the same job is worse than one.
* **SAQ's ``Worker``.** See :mod:`keel.queue.worker` — the loop is Keel's, so
  that lifecycle events, the failure sink and the shutdown contract are Keel's
  too.
* **A generic adapter over ``saq.Queue``.** SAQ also has Postgres and HTTP
  backends, but the namespace guarantee above is Redis key-space reasoning and
  does not transfer. An adapter that claimed to work for all three would be
  claiming a safety property it could not deliver for two of them.
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

from redis.asyncio import Redis
from saq.job import Job as SaqJob
from saq.job import Status
from saq.queue.redis import RedisQueue
from saq.utils import now as saq_now
from saq.utils import seconds as saq_seconds

from keel.exceptions import ConfigurationError, SerializationError
from keel.queue.config import QueueConfig
from keel.queue.envelope import Envelope
from keel.support.keys import SEPARATOR, escape_glob

ENVELOPE_KWARG: Final = "keel_envelope"
"""The single SAQ kwarg the whole envelope travels in.

Named rather than positional so a SAQ dashboard shows one readable blob, and
distinctive enough that a job enqueued into this lane by something other than
Keel is detected rather than half-decoded.
"""

SAQ_KEY_ROOT: Final = "saq"
"""SAQ's own top-level key segment. Hard-coded in ``RedisQueue.namespace``."""

SAQ_JOB_KEY_ROOT: Final = "saq:job"
"""Prefix of SAQ's per-job records. Hard-coded in ``RedisQueue.job_id``."""

SCAN_BATCH: Final = 500
"""Keys per SCAN iteration while clearing. Matches the cache's Redis store."""

DEFAULT_RESERVE_TIMEOUT: Final = 1.0
"""Seconds :meth:`SaqQueue.reserve` blocks before reporting an empty queue.

Short on purpose: it is also the worst-case latency of a shutdown request, since
a consumer parked in a blocking ``BLMOVE`` cannot notice one.
"""

SAQ_TIMEOUT_DISABLED: Final = 0
"""What SAQ's ``Job.timeout`` means by "no timeout"."""

SWEEP_LOCK_KEY: Final = "keel-sweep"
"""Lane-local key holding the sweep lock.

Distinct from SAQ's own ``sweep`` key so that Keel's sweeper and SAQ's — should
anything ever run both against one lane — do not silently take each other's
turn. It lives inside the lane's namespace, so :meth:`SaqQueue.clear` removes it
with everything else.
"""

SWEEP_ERROR: Final = "recovered: the worker holding this job did not finish it"
"""Recorded on a job that a sweep re-queued, for dashboards and post-mortems."""


def _require_prefix(prefix: str) -> str:
    """Return *prefix*, refusing one that would namespace nothing.

    Args:
        prefix: The configured key namespace.

    Returns:
        The prefix, unchanged.

    Raises:
        ConfigurationError: If it is empty. An empty prefix makes the lane name
            ``:default``, whose clear pattern is a hair away from ``saq:*`` — and
            a queue that can flush a neighbour's jobs during an incident is the
            failure this check exists to prevent. Mirrors
            ``RedisStore._require_namespace``.
    """
    if not prefix.strip(SEPARATOR):
        raise ConfigurationError(
            "a SAQ queue needs a non-empty prefix: the prefix is what scopes "
            "clear() to this application's keys, and without one it would "
            "discard jobs belonging to anything else sharing the Redis server"
        )
    return prefix


def _envelope_fields() -> frozenset[str]:
    """Return the envelope's field names, for tolerant decoding.

    Returns:
        Every field :class:`~keel.queue.envelope.Envelope` currently declares.
    """
    return frozenset(field.name for field in dataclasses.fields(Envelope))


ENVELOPE_FIELDS: Final = _envelope_fields()
"""Computed once: the decoder consults it per job."""


def encode(envelope: Envelope) -> dict[str, Any]:
    """Render *envelope* as SAQ kwargs.

    Args:
        envelope: The sealed job.

    Returns:
        A mapping SAQ will JSON-encode verbatim.
    """
    return {ENVELOPE_KWARG: dataclasses.asdict(envelope)}


def decode(kwargs: Mapping[str, Any] | None) -> Envelope:
    """Rebuild an envelope from SAQ kwargs.

    Fields the running code does not know are dropped rather than raising, which
    is the forward-compatibility promise :mod:`keel.queue.envelope` makes: a
    worker on last week's code must be able to read this week's envelope at
    least well enough to fail intelligibly. ``version`` is passed through
    untouched so the reader can see what it was actually handed.

    Args:
        kwargs: The SAQ job's kwargs.

    Returns:
        The envelope.

    Raises:
        SerializationError: If the job carries no Keel envelope at all, which
            means something other than Keel enqueued into this lane.
    """
    raw = (kwargs or {}).get(ENVELOPE_KWARG)
    if not isinstance(raw, Mapping):
        raise SerializationError(
            f"a job in this lane carries no {ENVELOPE_KWARG!r} kwarg, so it was "
            f"not dispatched by Keel; the lane name is probably shared with "
            f"another producer"
        )
    known = {key: value for key, value in raw.items() if key in ENVELOPE_FIELDS}
    return Envelope(**known)


def _is_orphaned(saq_job: SaqJob) -> bool:
    """Whether a job on the active list has outlived the worker that took it.

    Deliberately conservative in one direction: a job that has not recorded a
    start is never orphaned, because :meth:`SaqQueue.reserve` records the start
    a round trip after the dequeue and a sweep in that gap would steal live
    work. Erring the other way costs a delayed recovery; erring this way runs a
    job twice while the first copy is still running.

    Args:
        saq_job: The job read back off the active list.

    Returns:
        ``True`` if it has been running longer than it was ever allowed to.
    """
    if not saq_job.started or saq_job.timeout <= SAQ_TIMEOUT_DISABLED:
        return False
    return saq_seconds(saq_now() - saq_job.started) > saq_job.timeout


@dataclass(frozen=True, slots=True)
class Reservation:
    """One job taken off the queue and not yet accounted for.

    A value object, deliberately not a protocol: it pairs Keel's envelope with
    the SAQ job that has to be finished, because acknowledging work requires the
    driver's own handle and the worker has no business reconstructing it. ADR
    0000 declines a consume-side interface; this is data, not an interface.

    Attributes:
        envelope: What the worker is being asked to run.
        lane: The Keel queue name it came from.
        saq_job: SAQ's handle on the same job, needed to finish it.
    """

    envelope: Envelope
    lane: str
    saq_job: SaqJob


class SaqQueue:
    """A queue backed by SAQ on Redis.

    Args:
        queue: The SAQ lane for the configuration's default queue. Passing one
            in means the application owns the Redis client and its pool; this
            adapter will not close it. Its name must be the one
            :meth:`lane_name` computes, because that name is the whole of the
            namespace guarantee.
        config: How to reach the queue, and under what prefix.

    Raises:
        ConfigurationError: If the prefix is empty, or the lane's name is not
            the one the configuration implies.
    """

    __slots__ = ("_config", "_lanes", "_owns_client", "_prefix", "_redis")

    def __init__(
        self,
        queue: RedisQueue,
        config: QueueConfig,
        *,
        _owns_client: bool = False,
    ) -> None:
        self._prefix = _require_prefix(config.prefix)
        expected = self.lane_name(self._prefix, config.default_queue)
        if queue.name != expected:
            raise ConfigurationError(
                f"the SAQ lane passed in is named {queue.name!r}, but this "
                f"configuration namespaces its default queue as {expected!r}. "
                f"Keel scopes clear() to that name, so a mismatch would leave "
                f"jobs unreachable; build the lane with "
                f"SaqQueue.lane_name(prefix, queue) or use SaqQueue.from_url()."
            )
        self._config = config
        self._redis: Redis = queue.redis
        self._lanes: dict[str, RedisQueue] = {config.default_queue: queue}
        self._owns_client = _owns_client

    # -- construction -----------------------------------------------------

    @staticmethod
    def lane_name(prefix: str, queue: str) -> str:
        """Return the SAQ queue name for a Keel queue name.

        Args:
            prefix: The configured key namespace.
            queue: The Keel queue name.

        Returns:
            The name SAQ should use, which is also the key namespace it derives
            every one of its keys from.
        """
        return f"{prefix}{SEPARATOR}{queue}"

    @classmethod
    def from_url(cls, url: str, config: QueueConfig | None = None) -> SaqQueue:
        """Build a queue that owns its own Redis client.

        Args:
            url: A ``redis://`` connection URL.
            config: How to reach the queue; defaults are taken from
                :class:`~keel.queue.config.QueueConfig` when omitted.

        Returns:
            A queue which will close its client on :meth:`close`.
        """
        settings = config or QueueConfig(driver="saq", url=url)
        client: Redis = Redis.from_url(url, decode_responses=False)
        lane = RedisQueue(client, name=cls.lane_name(settings.prefix, settings.default_queue))
        return cls(lane, settings, _owns_client=True)

    # -- inspection -------------------------------------------------------

    @property
    def name(self) -> str:
        """The configured name of this queue connection."""
        return self._config.driver

    @property
    def config(self) -> QueueConfig:
        """The configuration this queue was built from."""
        return self._config

    @property
    def client(self) -> Redis:
        """The underlying Redis client, for operations outside the contract."""
        return self._redis

    def lane(self, queue: str | None = None) -> RedisQueue:
        """Return the SAQ lane serving a Keel queue name, building it if new.

        Args:
            queue: The Keel queue name, or ``None`` for the configured default.

        Returns:
            The memoised lane. Every lane shares this adapter's Redis client, so
            adding one costs a dictionary entry rather than a connection pool.
        """
        resolved = queue or self._config.default_queue
        existing = self._lanes.get(resolved)
        if existing is not None:
            return existing
        lane = RedisQueue(self._redis, name=self.lane_name(self._prefix, resolved))
        self._lanes[resolved] = lane
        return lane

    # -- Queue contract ---------------------------------------------------

    async def push(self, envelope: Envelope) -> str:
        """Enqueue one envelope.

        Uniqueness is delegated to SAQ rather than reimplemented: SAQ's enqueue
        is a Lua script that refuses a job whose key is already in the
        ``incomplete`` set, atomically. So a unique envelope is keyed on its
        ``unique_key`` and a duplicate simply loses the race, which is the only
        way to get this right against concurrent dispatchers.

        Args:
            envelope: The sealed job.

        Returns:
            The id under which it was accepted — the *existing* dispatch's id
            when this one was dropped as a duplicate, so a caller can tell the
            two apart by comparing it with ``envelope.id``.
        """
        lane = self.lane(envelope.queue)
        accepted = await lane.enqueue(self._as_saq_job(envelope))
        if accepted is not None:
            return envelope.id
        existing = await lane.job(self._job_key(envelope))
        if existing is None:
            # The in-flight duplicate finished and expired between the enqueue
            # and this read. Reporting the caller's own id is honest: nothing is
            # queued under it, but nothing was lost either.
            return envelope.id
        return decode(existing.kwargs).id

    async def push_many(self, envelopes: Sequence[Envelope]) -> list[str]:
        """Enqueue several envelopes, in order.

        Sequential, unlike the contract's suggestion, because SAQ exposes no
        batch entry point — each enqueue is its own Lua script call. Issuing
        them concurrently would save round trips but make a partial failure
        indeterminate: some enqueued, some not, and no way to report which.

        Args:
            envelopes: The sealed jobs, in dispatch order.

        Returns:
            The accepted ids, in the same order.
        """
        return [await self.push(envelope) for envelope in envelopes]

    async def size(self, queue: str | None = None) -> int:
        """Return how many jobs are available to a worker right now.

        Scheduled-for-later jobs are deliberately excluded. The question this
        answers is "is the queue backing up", and counting work that is not
        supposed to have run yet answers it wrongly — a nightly batch of delayed
        jobs would read as a permanent backlog.

        Args:
            queue: Which named queue to count, or ``None`` for the default.

        Returns:
            The number of jobs waiting.
        """
        return await self.lane(queue).count("queued")

    async def clear(self, queue: str | None = None) -> int:
        """Discard every job in one queue, and nothing else.

        Scans and unlinks the two key families SAQ derives from the lane's name,
        rather than reaching for ``FLUSHDB``. The reasoning is
        ``RedisStore.flush``'s, unchanged: queues share servers, and a queue
        clear should never be capable of wiping a cache, a session table or
        another application. The lane name carries the configured prefix, which
        :func:`_require_prefix` guarantees is non-empty, so the pattern can
        never widen to everything.

        The same weaker promise applies as there: ``SCAN`` is cursor-based and
        offers no snapshot, so a job enqueued *during* the sweep may survive.

        Args:
            queue: Which named queue to clear, or ``None`` for the default.

        Returns:
            How many unfinished jobs were discarded. Counted before the sweep,
            from SAQ's ``incomplete`` set, so it excludes the finished-job
            records that are also removed — those were not jobs any more.
        """
        lane = self.lane(queue)
        discarded = await lane.count("incomplete")
        safe = escape_glob(lane.name)
        for pattern in (
            f"{SAQ_KEY_ROOT}{SEPARATOR}{safe}{SEPARATOR}*",
            f"{SAQ_JOB_KEY_ROOT}{SEPARATOR}{safe}{SEPARATOR}*",
        ):
            await self._unlink_matching(pattern)
        return discarded

    async def close(self) -> None:
        """Close the Redis client, but only if this queue created it.

        The lanes themselves hold nothing else worth releasing: SAQ's per-lane
        pub/sub multiplexer starts lazily on ``listen()``, and Keel never calls
        it — a job's outcome is observed by the worker that ran it, and a sweep
        judges staleness from the record rather than waiting on a handshake.
        """
        if not self._owns_client:
            return
        await self._redis.aclose()
        await self._redis.connection_pool.disconnect()

    # -- consume side -----------------------------------------------------
    #
    # Not part of the Queue protocol, and deliberately not promoted into one:
    # there is exactly one consumer, keel.queue.worker.Worker, and ADR 0000
    # declines interfaces with a single implementor. These are ordinary methods
    # on the concrete driver, which is what the worker holds.

    async def reserve(
        self,
        queue: str | None = None,
        *,
        timeout: float = DEFAULT_RESERVE_TIMEOUT,
    ) -> Reservation | None:
        """Take the next available job off a queue and count the attempt.

        Two writes happen here that look like bookkeeping and are not.

        **The attempt is counted at reservation, not at failure.** An attempt is
        a *delivery*, and a delivery that kills the worker holding it is still a
        delivery. Counting on failure would mean a job that segfaults its worker
        comes back with an untouched budget every time :meth:`sweep` recovers
        it, and a crash-looping job would be immortal — it would take down
        replica after replica for ever, which is the single worst failure mode a
        job runner can have.

        **The job is marked ``ACTIVE`` with a start time.** That is what tells
        :meth:`sweep` the difference between a job somebody is running and a job
        whose worker is gone: SAQ's staleness test is "active, and older than
        its timeout". Without this write every reserved job looks abandoned the
        instant it is picked up, and a sweep would yank work out from under a
        healthy worker.

        The two writes are not atomic with the dequeue. A worker that dies in
        between leaves a job whose stored attempt count is one behind — it gets
        one delivery more than its budget, never fewer, which is the direction
        an at-least-once queue is allowed to err in.

        Args:
            queue: Which named queue to take from, or ``None`` for the default.
            timeout: Seconds to block waiting for one.

        Returns:
            The reservation, whose envelope already has this attempt counted, or
            ``None`` if nothing became available in time.
        """
        resolved = queue or self._config.default_queue
        lane = self.lane(resolved)
        saq_job = await lane.dequeue(timeout=timeout)
        if saq_job is None:
            return None
        envelope = decode(saq_job.kwargs).attempted()
        await lane.update(
            saq_job,
            status=Status.ACTIVE,
            started=saq_now(),
            kwargs=encode(envelope),
            # The original dispatch's `scheduled` is a past epoch once a delayed
            # job has run, and SAQ's scheduler treats any non-zero score as
            # "due" — so leaving it would let a swept job be promoted a second
            # time and delivered twice. Zeroing it here costs nothing: a job
            # being reserved is, by definition, no longer scheduled.
            scheduled=0,
        )
        return Reservation(envelope=envelope, lane=resolved, saq_job=saq_job)

    async def ack(self, reservation: Reservation) -> None:
        """Record a reserved job as finished successfully.

        Args:
            reservation: What :meth:`reserve` returned.
        """
        await self.lane(reservation.lane).finish(reservation.saq_job, Status.COMPLETE)

    async def fail(self, reservation: Reservation, error: str) -> None:
        """Record a reserved job as finished, unsuccessfully and for good.

        Terminal: use it when nothing further will be attempted. A failure with
        another attempt to come goes through :meth:`retry` instead, which is one
        Redis transaction rather than this plus a push.

        Args:
            reservation: What :meth:`reserve` returned.
            error: A short description, stored on the SAQ job for dashboards.
        """
        await self.lane(reservation.lane).finish(reservation.saq_job, Status.FAILED, error=error)

    async def retry(
        self,
        reservation: Reservation,
        envelope: Envelope,
        *,
        delay: float,
        error: str,
    ) -> None:
        """Put a reserved job back on the queue, with a new envelope and a delay.

        A single ``MULTI``/``EXEC``, which is the whole reason this exists
        instead of ``fail()`` followed by ``push()``. That pair has a window
        between the two calls in which the job is on no list at all, and a
        worker that dies inside it leaves nothing for :meth:`sweep` to find,
        because sweeping looks at the *active* list and ``fail()`` has already
        removed it from there. Doing both halves in one transaction closes the
        window rather than documenting it.

        SAQ's own delay machinery is borrowed here and only here: setting
        ``retry_delay`` makes ``_retry`` schedule rather than queue. The number
        it schedules with still comes from Keel's backoff Strategy — SAQ is
        being used as the transport for a decision it did not make.

        Args:
            reservation: What :meth:`reserve` returned.
            envelope: The envelope the next attempt should carry.
            delay: Seconds before it becomes visible again.
            error: A short description of what ended this attempt.
        """
        saq_job = reservation.saq_job
        saq_job.kwargs = encode(envelope)
        saq_job.retry_delay = delay
        saq_job.retry_backoff = False
        saq_job.scheduled = 0
        await self.lane(reservation.lane).retry(saq_job, error)

    async def sweep(self, queue: str | None = None, *, lock: float) -> list[str]:
        """Recover jobs whose worker died while holding them.

        A reserved job lives on SAQ's *active* list until the worker that took
        it says otherwise. A worker that is killed says nothing, so without this
        the job sits there for ever — the one hole in "killing a worker loses
        nothing". Sweeping re-queues it, and because :meth:`reserve` already
        charged the attempt, it comes back with the budget it has left rather
        than a fresh one.

        **This does not call SAQ's own ``Queue.sweep``, and that is a decision
        rather than an oversight.** SAQ's rule is "re-queue an active job that
        is not marked ``ACTIVE``, or that has outrun its timeout", and the first
        half of that is fatal here: SAQ's worker marks a job active inside its
        own dequeue, while Keel's :meth:`reserve` needs a second round trip to
        do it. Every job Keel reserves therefore looks abandoned for the
        microseconds in between, and a sweep landing in that gap yanks live work
        away from a healthy worker and runs it twice. Observed on the first run,
        not theorised. So the rule here is only the honest half: **started, and
        overdue by its own timeout.** A job that has not started yet is left
        alone, whatever its status says.

        The consequence is worth stating plainly: **a job that declares no
        timeout can never be recovered**, because nothing distinguishes it from
        one that is still running. That is the strongest argument for
        :data:`~keel.queue.job.DEFAULT_TIMEOUT` being a number rather than
        ``None``, and recovery latency is that number — a job that wants to come
        back sooner should declare a shorter one.

        **Concurrent sweepers.** ``SET NX EX`` is atomic, so exactly one replica
        per *lock* seconds does the scan and every other returns immediately
        with nothing. That is the whole guard, and it is enough: the re-queue
        itself is a single SAQ transaction, so even a lock that expired
        mid-sweep could not produce a torn job.

        Args:
            queue: Which named queue to sweep, or ``None`` for the default.
            lock: Seconds the cluster-wide sweep lock is held. Also the shortest
                interval at which any sweeping actually happens on this lane,
                whatever the callers' own timers say.

        Returns:
            The SAQ job ids that were re-queued or discarded.
        """
        lane = self.lane(queue)
        guard = lane.namespace(SWEEP_LOCK_KEY)
        if not await self._redis.set(guard, b"1", nx=True, ex=max(1, int(lock))):
            return []
        active = lane.namespace("active")
        swept: list[str] = []
        for raw in await self._redis.lrange(active, 0, -1):
            job_id = raw.decode() if isinstance(raw, bytes) else str(raw)
            record = await self._redis.get(job_id)
            saq_job = lane.deserialize(record) if record else None
            if saq_job is None:
                # On the active list with no record behind it. Nothing to
                # recover; leaving it there would make the list grow for ever.
                async with self._redis.pipeline(transaction=True) as pipe:
                    pipe.lrem(active, 0, job_id)
                    pipe.zrem(lane.namespace("incomplete"), job_id)
                    await pipe.execute()
                swept.append(job_id)
            elif _is_orphaned(saq_job):
                await lane.retry(saq_job, SWEEP_ERROR)
                swept.append(job_id)
        return swept

    async def promote_due(self, queue: str | None = None) -> int:
        """Move jobs whose delay has elapsed onto the available list.

        SAQ keeps delayed jobs in a sorted set and promotes them only when
        something asks it to; SAQ's own worker does this on a timer, and since
        Keel runs its own loop, Keel has to. A queue whose delayed jobs are
        never promoted looks exactly like a queue that lost them.

        Args:
            queue: Which named queue to promote in, or ``None`` for the default.

        Returns:
            How many jobs became available.
        """
        return len(await self.lane(queue).schedule())

    # -- internals --------------------------------------------------------

    def _job_key(self, envelope: Envelope) -> str:
        """Return the SAQ job key an envelope is stored under.

        The unique key when the job type is unique, so SAQ's atomic enqueue does
        the deduplication; otherwise the envelope's own id, which is unique by
        construction and stable across retries — a retry must reuse the key so
        the dashboard shows one job with several attempts rather than several
        jobs.
        """
        return envelope.unique_key or envelope.id

    def _as_saq_job(self, envelope: Envelope) -> SaqJob:
        """Translate an envelope into the SAQ job that carries it.

        ``function`` is the Keel job name rather than a fixed dispatcher name:
        nothing in this process resolves it — Keel's worker reads the envelope —
        but SAQ's web UI and log lines print it, and a queue dump that says
        ``SendInvoiceEmail`` is worth more than one that says ``keel.run``.
        """
        scheduled = int(time.time() + envelope.delay) if envelope.delay > 0 else 0
        timeout = (
            SAQ_TIMEOUT_DISABLED if envelope.timeout is None else max(1, int(envelope.timeout))
        )
        return SaqJob(
            function=envelope.job,
            kwargs=encode(envelope),
            key=self._job_key(envelope),
            timeout=timeout,
            scheduled=scheduled,
            # Keel's worker decides every retry, so SAQ must never schedule one
            # of its own. The remaining budget is still declared, because it is
            # what SAQ's sweeper would use to re-queue a job orphaned by a
            # worker that died mid-attempt, rather than aborting it outright.
            retries=max(1, envelope.max_attempts - envelope.attempts),
        )

    async def _unlink_matching(self, pattern: str) -> None:
        """Remove every key matching *pattern*, a page at a time.

        Args:
            pattern: An already-escaped ``SCAN MATCH`` glob.
        """
        cursor = 0
        while True:
            cursor, keys = await self._redis.scan(cursor, match=pattern, count=SCAN_BATCH)
            if keys:
                await self._redis.unlink(*keys)
            if cursor == 0:
                return

    def __repr__(self) -> str:
        """Identify the prefix and the lanes built so far."""
        lanes = ",".join(sorted(self._lanes))
        return f"<SaqQueue prefix={self._prefix!r} lanes=[{lanes}]>"


__all__ = [
    "DEFAULT_RESERVE_TIMEOUT",
    "ENVELOPE_KWARG",
    "SCAN_BATCH",
    "SWEEP_ERROR",
    "SWEEP_LOCK_KEY",
    "Reservation",
    "SaqQueue",
    "decode",
    "encode",
]
