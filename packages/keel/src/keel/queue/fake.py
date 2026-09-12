"""The queue test double.

A Test Spy — and pointedly **not** built the way :class:`~keel.cache.fake.FakeStore`
is. That one is a Decorator over a real store, so a test exercises real TTLs and
real serialisation. The same trick does not work here, and understanding why is
the difference between a useful fake and a misleading one.

A cache's behaviour is cheap to have for real. A queue's is not: running the job
means the test now exercises the handler, its database writes, its outbound
calls and its failure modes — all while claiming to test the code that
*dispatched* it. When `create_invoice` is under test, the question is "did it
enqueue the email?", not "does the email job work?". Those are two tests, and
conflating them means neither failure tells you where to look.

So this records and does not run. Running is what :class:`~keel.queue.drivers.SyncQueue`
is for, and a test that wants both can wrap one in the other.

    with fake_queue() as queued:
        await invoices.approve(invoice_id)
        queued.assert_pushed(SendInvoiceEmail, invoice_id=str(invoice_id))
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from keel.queue.envelope import Envelope
from keel.queue.job import DEFAULT_QUEUE, Job


class QueueAssertionError(AssertionError):
    """Raised when a queue assertion fails.

    Subclasses :class:`AssertionError` so pytest renders it as a failed
    assertion rather than an error.
    """


class FakeQueue:
    """Records every dispatch and executes none of them.

    Args:
        name: The configured name of this connection.
    """

    __slots__ = ("_cleared", "_name", "_pushed")

    def __init__(self, name: str = "fake") -> None:
        self._name = name
        self._pushed: list[Envelope] = []
        self._cleared = 0

    # -- inspection -------------------------------------------------------

    @property
    def name(self) -> str:
        """The configured name of this queue connection."""
        return self._name

    @property
    def pushed(self) -> tuple[Envelope, ...]:
        """Every envelope dispatched, in order."""
        return tuple(self._pushed)

    def reset(self) -> None:
        """Discard the recorded history."""
        self._pushed.clear()
        self._cleared = 0

    def _matching(self, job: type[Job], **payload: Any) -> list[Envelope]:
        """Return recorded envelopes for *job* whose payload contains *payload*."""
        return [
            envelope
            for envelope in self._pushed
            if envelope.job == job.name
            and all(envelope.payload.get(key) == value for key, value in payload.items())
        ]

    def _timeline(self) -> str:
        """Render the history for a failure message."""
        if not self._pushed:
            return "  (nothing was dispatched)"
        return "\n".join(
            f"  {index + 1}. {envelope.job} payload={dict(envelope.payload)!r} "
            f"queue={envelope.queue!r} delay={envelope.delay}"
            for index, envelope in enumerate(self._pushed)
        )

    def _fail(self, message: str) -> QueueAssertionError:
        """Build an assertion error with the recorded timeline attached."""
        return QueueAssertionError(f"{message}\n\nDispatched jobs:\n{self._timeline()}")

    # -- assertions -------------------------------------------------------

    def assert_pushed(self, job: type[Job], **payload: Any) -> Envelope:
        """Assert that *job* was dispatched, optionally with specific fields.

        Args:
            job: The job class expected.
            **payload: Field values it must have been dispatched with. A subset
                is enough — asserting on the one field a test cares about beats
                restating the whole payload and breaking when an unrelated field
                is added.

        Returns:
            The first matching envelope, so a caller can make further
            assertions about its queue or delay.

        Raises:
            QueueAssertionError: If no matching dispatch was recorded.
        """
        matches = self._matching(job, **payload)
        if not matches:
            detail = f" with {payload!r}" if payload else ""
            raise self._fail(f"expected {job.name} to be dispatched{detail}, but it was not")
        return matches[0]

    def assert_not_pushed(self, job: type[Job], **payload: Any) -> None:
        """Assert that *job* was never dispatched.

        Args:
            job: The job class.
            **payload: Narrow the assertion to dispatches with these fields.

        Raises:
            QueueAssertionError: If a matching dispatch was recorded.
        """
        if self._matching(job, **payload):
            raise self._fail(f"expected {job.name} not to be dispatched, but it was")

    def assert_pushed_times(self, job: type[Job], times: int, **payload: Any) -> None:
        """Assert *job* was dispatched exactly *times*.

        The assertion that catches a dispatch inside a loop, which is how one
        welcome email becomes four hundred.

        Args:
            job: The job class.
            times: The expected count.
            **payload: Narrow to dispatches with these fields.

        Raises:
            QueueAssertionError: If the count differs.
        """
        actual = len(self._matching(job, **payload))
        if actual != times:
            raise self._fail(f"expected {job.name} to be dispatched {times}x, got {actual}")

    def assert_nothing_pushed(self) -> None:
        """Assert that no job of any kind was dispatched.

        Raises:
            QueueAssertionError: If anything was dispatched.
        """
        if self._pushed:
            raise self._fail(f"expected no dispatches, but {len(self._pushed)} were recorded")

    def assert_pushed_on(self, job: type[Job], queue: str) -> Envelope:
        """Assert *job* went to a particular queue.

        Args:
            job: The job class.
            queue: The queue name expected.

        Returns:
            The matching envelope.

        Raises:
            QueueAssertionError: If it went somewhere else, or nowhere.
        """
        for envelope in self._matching(job):
            if envelope.queue == queue:
                return envelope
        raise self._fail(f"expected {job.name} on queue {queue!r}")

    def assert_delayed(self, job: type[Job], seconds: float) -> Envelope:
        """Assert *job* was dispatched with a delay.

        Args:
            job: The job class.
            seconds: The delay expected.

        Returns:
            The matching envelope.

        Raises:
            QueueAssertionError: If no dispatch carried that delay.
        """
        for envelope in self._matching(job):
            if envelope.delay == seconds:
                return envelope
        raise self._fail(f"expected {job.name} to be delayed by {seconds}s")

    # -- Queue contract ---------------------------------------------------

    async def push(self, envelope: Envelope) -> str:
        """Record the dispatch without running the job.

        Honours uniqueness, because a test asserting that a unique job is only
        enqueued once should be testing that and not the driver.

        Args:
            envelope: The sealed job.

        Returns:
            The accepted id — the earlier dispatch's id when this one was
            dropped as a duplicate.
        """
        if envelope.unique_key is not None:
            for existing in self._pushed:
                if existing.unique_key == envelope.unique_key:
                    return existing.id
        self._pushed.append(envelope)
        return envelope.id

    async def push_many(self, envelopes: Sequence[Envelope]) -> list[str]:
        """Record each dispatch.

        Args:
            envelopes: The sealed jobs.

        Returns:
            The accepted ids.
        """
        return [await self.push(envelope) for envelope in envelopes]

    async def size(self, queue: str | None = None) -> int:
        """Return how many dispatches were recorded for one queue.

        Args:
            queue: The queue to count, or ``None`` for the default one.

        Returns:
            The number recorded on that queue.
        """
        lane = queue or DEFAULT_QUEUE
        return sum(1 for envelope in self._pushed if envelope.queue == lane)

    async def clear(self, queue: str | None = None) -> int:
        """Discard recorded dispatches for one queue.

        ``None`` means the *default* queue, not every queue. An administrative
        operation that destroys the widest possible thing when an argument is
        omitted is the same defect ``Repository.purge()`` refuses to have.

        Args:
            queue: The queue to clear, or ``None`` for the default one.

        Returns:
            How many were discarded.
        """
        lane = queue or DEFAULT_QUEUE
        before = len(self._pushed)
        self._pushed = [item for item in self._pushed if item.queue != lane]
        removed = before - len(self._pushed)
        self._cleared += removed
        return removed

    async def total(self) -> int:
        """Return how many dispatches were recorded across every queue.

        Exists because :meth:`size` deliberately answers only about one lane,
        and a test asserting "nothing anywhere" needs the other question.

        Returns:
            The total recorded.
        """
        return len(self._pushed)

    async def close(self) -> None:
        """Do nothing. Nothing is held."""


__all__ = ["FakeQueue", "QueueAssertionError"]
