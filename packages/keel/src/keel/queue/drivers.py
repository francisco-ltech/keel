"""Queue drivers that need no external service.

Two of them, and neither is a test double despite the resemblance.

:class:`SyncQueue` executes jobs inline at dispatch. That is how a
single-process development environment works without running a worker at all,
and it is Laravel's `QUEUE_CONNECTION=sync`. It is also the sharpest possible
demonstration of why dispatch-after-commit matters: run a job inline inside an
open transaction and it sees a database state nobody else can see yet.

:class:`NullQueue` discards. For a smoke-test environment, or for turning a
subsystem off without threading a flag through the code that dispatches — the
Null Object pattern, exactly as :class:`~keel.cache.stores.null.NullStore` is.

The recording double lives in :mod:`keel.queue.fake`, because a test wants to
assert on dispatches rather than run them, and those are different objects.
"""

from __future__ import annotations

from collections.abc import Sequence

from keel.queue.envelope import Envelope


class SyncQueue:
    """Runs each job immediately, in the dispatching process.

    Note:
        Retries, backoff and timeouts do not apply. A failing job raises into
        the caller, which is the opposite of what a queue is for — and is the
        right behaviour here, because the alternative is swallowing an error in
        development and discovering it in production.

    Args:
        name: The configured name of this connection.
    """

    __slots__ = ("_name",)

    def __init__(self, name: str = "sync") -> None:
        self._name = name

    @property
    def name(self) -> str:
        """The configured name of this queue connection."""
        return self._name

    async def push(self, envelope: Envelope) -> str:
        """Execute the job now.

        Args:
            envelope: The sealed job.

        Returns:
            The envelope's id, once the job has finished.

        Raises:
            Exception: Whatever the job raised. Deliberately not caught.
        """
        await envelope.open().handle()
        return envelope.id

    async def push_many(self, envelopes: Sequence[Envelope]) -> list[str]:
        """Execute each job in order.

        Args:
            envelopes: The sealed jobs.

        Returns:
            Their ids.
        """
        return [await self.push(envelope) for envelope in envelopes]

    async def size(self, queue: str | None = None) -> int:
        """Return zero: nothing is ever waiting.

        Args:
            queue: Ignored.

        Returns:
            ``0``.
        """
        return 0

    async def clear(self, queue: str | None = None) -> int:
        """Return zero: there is nothing to clear.

        Args:
            queue: Ignored.

        Returns:
            ``0``.
        """
        return 0

    async def close(self) -> None:
        """Do nothing. Nothing is held."""


class NullQueue:
    """Accepts every job and runs none of them.

    The Null Object pattern: lets background work be switched off through
    configuration rather than through a conditional at every dispatch site.

    Args:
        name: The configured name of this connection.
    """

    __slots__ = ("_name",)

    def __init__(self, name: str = "null") -> None:
        self._name = name

    @property
    def name(self) -> str:
        """The configured name of this queue connection."""
        return self._name

    async def push(self, envelope: Envelope) -> str:
        """Discard the job and report success.

        Args:
            envelope: The sealed job.

        Returns:
            The envelope's id. Nothing promised the job would run.
        """
        return envelope.id

    async def push_many(self, envelopes: Sequence[Envelope]) -> list[str]:
        """Discard them all.

        Args:
            envelopes: The sealed jobs.

        Returns:
            Their ids.
        """
        return [envelope.id for envelope in envelopes]

    async def size(self, queue: str | None = None) -> int:
        """Return zero.

        Args:
            queue: Ignored.

        Returns:
            ``0``.
        """
        return 0

    async def clear(self, queue: str | None = None) -> int:
        """Return zero.

        Args:
            queue: Ignored.

        Returns:
            ``0``.
        """
        return 0

    async def close(self) -> None:
        """Do nothing."""


__all__ = ["NullQueue", "SyncQueue"]
