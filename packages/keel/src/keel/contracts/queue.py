"""The queue contract.

One protocol, covering the **dispatch** side only.

That asymmetry is deliberate and is the main structural difference from the
cache. The cache has one contract because both halves of it — reading and
writing — are used by the same code in the same process. A queue has two
distinct halves used by two distinct populations: application code *pushes*, and
exactly one component, the worker, *consumes*.

So the push side gets a protocol, because application code must be able to swap
a real queue for a recording fake. The consume side does not, because there is
one worker implementation and an interface with a single implementor is
indirection pretending to be design. See ADR 0000 on declining patterns that do
not earn their place.

The methods here are what a caller genuinely needs. Everything ergonomic —
``later()``, ``chain()``, dispatch-after-commit — is built on top in
:mod:`keel.queue.dispatch`, for the same reason the cache's conveniences live on
the Repository: a new driver should not have to reimplement them, and cannot get
them subtly wrong if it never implements them at all.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    # Deferred to break a genuine cycle: `keel.queue.__init__` imports this
    # module, so importing `keel.queue.envelope` eagerly here fails whenever the
    # contract happens to be imported first. `Envelope` appears only in
    # annotations, which `from __future__ import annotations` already defers.
    from keel.queue.envelope import Envelope


class Queue(Protocol):
    """Somewhere envelopes can be put so a worker can take them out.

    Implementations are interchangeable from the caller's side. The shared test
    suite in ``tests/test_queue_contract.py`` runs against each one, so the
    guarantees below are enforced rather than described:

    * ``push`` returns the envelope's id and does not execute the job.
    * An envelope with a ``unique_key`` already in flight is dropped rather than
      duplicated, and the call reports which happened.
    * ``push`` with a delay makes the job invisible until the delay elapses.
    """

    @property
    def name(self) -> str:
        """The configured name of this queue connection."""
        ...

    async def push(self, envelope: Envelope) -> str:
        """Enqueue one envelope.

        Args:
            envelope: The sealed job.

        Returns:
            The id under which it was accepted. When a unique job was dropped as
            a duplicate, this is the id of the dispatch already in flight, so a
            caller can tell the two apart by comparing it with
            ``envelope.id``.
        """
        ...

    async def push_many(self, envelopes: Sequence[Envelope]) -> list[str]:
        """Enqueue several envelopes.

        Not a loop over :meth:`push` in every driver: a backend that can accept
        a batch in one round trip should, because the common case for this is
        fanning out hundreds of jobs at once.

        Args:
            envelopes: The sealed jobs, in dispatch order.

        Returns:
            The accepted ids, in the same order.
        """
        ...

    async def size(self, queue: str | None = None) -> int:
        """Return how many jobs are waiting.

        For health checks and dashboards. Approximate by nature — jobs are being
        taken while it counts — so do not build correctness on it.

        Args:
            queue: Which named queue to count. ``None`` means the *default*
                queue, not the sum across every queue.

        Returns:
            The number of jobs waiting on that queue.
        """
        ...

    async def clear(self, queue: str | None = None) -> int:
        """Discard every waiting job.

        Administrative and destructive. Exists because the alternative during an
        incident is people running redis-cli.

        Args:
            queue: Which named queue to clear. ``None`` means the *default*
                queue — never every queue. An administrative operation that
                destroys the widest possible thing when an argument is omitted
                is the defect :meth:`keel.database.repository.Repository.purge`
                refuses to have, and two implementations of this protocol once
                disagreed about it.

        Returns:
            How many jobs were discarded.
        """
        ...

    async def close(self) -> None:
        """Release any connection this queue holds."""
        ...


__all__ = ["Queue"]
