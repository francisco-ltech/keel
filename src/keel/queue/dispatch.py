"""Dispatching jobs.

The interface application code uses, and the one place that decides *when* a job
actually reaches the queue.

    await dispatch(SendInvoiceEmail(invoice_id))
    await dispatch(SendReminder(invoice_id), delay=timedelta(days=3))

**Inside a unit of work, dispatch is deferred until the commit succeeds.** That
is the default, not an option, and it is the most important behaviour in this
module. The alternative — pushing immediately — produces this:

    async with uow() as session:
        invoice = await Invoices(session).create(...)
        await dispatch(SendInvoiceEmail(invoice.id))
        await charge(invoice)          # raises; the transaction rolls back

...a worker emailing a customer about an invoice that never existed, and usually
failing to find it, so the job dead-letters looking like corrupted data. Making
the safe ordering the default rather than a thing to remember is the same
reasoning as ADR 0002's refusal to offer a request-scoped session: the unsafe
version is what people write when they are not thinking about it.

Outside a transaction, dispatch is immediate — there is nothing to wait for.

Two escape hatches, both explicit:

* ``dispatch(job, after_commit=False)`` pushes now even inside a transaction.
  Correct when the job must run whatever happens to the write, which is rare and
  should look deliberate.
* ``dispatch_now(job)`` bypasses the queue entirely and runs the handler inline.
  For a CLI command or a test that wants the work done, not queued.

**Every dispatch seals the ambient correlation fields onto the envelope.** That
is what makes a worker's log lines joinable to the request that caused the work,
and it is what ``Envelope.context``'s docstring has promised since Phase 3. It
happens here rather than in ``Envelope.seal`` because sealing is packaging and
this is the layer that decides what a dispatch means — which is also why
``FailedJobs.retry`` bypassing this module (ADR 0006, decision 8) keeps its
replay verbatim: a job retried next week carries the context of the request that
originally caused it, not of the operator who pressed the button.

Two names are rewritten on the way out — see :data:`INHERITED_FIELDS` — and one
parameter is deliberately not called ``context``: ``context_override=`` replaces
what travels rather than adding to it, which is a surprise worth spelling out in
the keyword rather than in a docstring nobody opens.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import timedelta
from types import MappingProxyType
from typing import Any, Final

from keel.contracts.queue import Queue
from keel.database.hooks import after_commit as defer_until_commit
from keel.queue.envelope import Envelope
from keel.queue.job import Job
from keel.queue.manager import QueueManager
from keel.support.binding import Binding
from keel.support.correlation import correlation, correlation_fields

_binding: Binding[QueueManager] = Binding(
    "queue manager",
    "call keel.queue.set_queue_manager(...) during startup, "
    "or use keel.testing.fake_queue() in a test",
)


def set_queue_manager(manager: QueueManager | None) -> None:
    """Install the process-wide queue manager.

    Args:
        manager: The manager to install, or ``None`` to unbind.
    """
    _binding.set(manager)


def current_queue_manager() -> QueueManager:
    """Return the manager currently in effect.

    Returns:
        The context-local override if one is active, otherwise the process-wide
        manager.

    Raises:
        ConfigurationError: If nothing is bound.
    """
    return _binding.current()


def bound_queue_manager() -> QueueManager | None:
    """Return the process-wide manager without raising when none is bound.

    Returns:
        The bound manager, or ``None``. Used by the lifespan to restore a
        previous binding rather than unbinding.
    """
    return _binding.peek()


def use_queue(manager: QueueManager) -> AbstractContextManager[QueueManager]:
    """Override the bound queue manager for the duration of a block.

    Args:
        manager: The manager to use.

    Returns:
        A context manager yielding the same manager.
    """
    return _binding.use(manager)


def queue(name: str | None = None) -> Queue:
    """Return a queue connection.

    Args:
        name: The connection name, or ``None`` for the configured default.

    Returns:
        The connection.
    """
    return current_queue_manager().connection(name)


def _seconds(delay: float | timedelta) -> float:
    """Normalise a delay to seconds."""
    return delay.total_seconds() if isinstance(delay, timedelta) else float(delay)


INHERITED_FIELDS: Final[Mapping[str, str]] = MappingProxyType(
    {"job": "parent_job", "job_id": "parent_job_id"}
)
"""Fields renamed on the way out, because they describe the dispatcher.

:meth:`keel.queue.worker.Worker._process` binds ``job`` and ``job_id`` around an
attempt, so a job dispatched *from a handler* would otherwise seal its parent's
id onto its own envelope — and a dead letter's ``keel_failed_jobs.context`` would
then name a ``job_id`` that is not the row's. Log lines hid it, because the child
worker overwrites both names before the handler writes anything. Renaming turns a
value that was wrong into the one an operator actually wants: what enqueued this.
"""


def _travelling(context: Mapping[str, Any] | None) -> Mapping[str, str]:
    """Return the ambient data an envelope should carry.

    A *context_override* **replaces** the ambient fields rather than merging with
    them, which is the opposite of how
    :func:`keel.support.correlation.correlate` nests — and the parameter is
    spelled to say so. Nesting a scope adds knowledge; this is an override like
    ``on=`` and ``connection=``, and it is the only spelling that can say "carry
    nothing", which merging cannot express. A caller wanting both writes
    ``context_override={**correlation(), "reason": "backfill"}``.

    Args:
        context: What the caller asked to carry, or ``None`` for the ambient
            correlation fields with :data:`INHERITED_FIELDS` renamed.

    Returns:
        The fields to seal onto the envelope, normalised so they will survive
        the trip through the queue and into ``keel_failed_jobs.context``.

    Raises:
        ConfigurationError: If an explicit context names a reserved field or one
            that looks like a credential.
    """
    if context is not None:
        return correlation_fields(context)
    ambient = correlation()
    if not any(name in ambient for name in INHERITED_FIELDS):
        return ambient
    carried = dict(ambient)
    for name, parent in INHERITED_FIELDS.items():
        if name in carried:
            carried[parent] = carried.pop(name)
    return carried


async def dispatch(
    job: Job,
    *,
    delay: float | timedelta = 0.0,
    on: str | None = None,
    connection: str | None = None,
    after_commit: bool = True,
    context_override: Mapping[str, Any] | None = None,
) -> str:
    """Send a job to the queue.

    Args:
        job: The job to run.
        delay: How long before a worker may pick it up.
        on: Override the job's declared queue.
        connection: Which queue connection to use.
        after_commit: When inside a unit of work, wait for the commit. Setting
            this to ``False`` pushes immediately, which means the job may run
            against a transaction that later rolls back — occasionally correct,
            never accidental.
        context_override: What the envelope should carry instead of the
            correlation fields in effect. Leave it alone and the ambient fields
            travel, which is what joins the worker's log lines to the request
            that caused the work; passing a mapping **replaces** them rather
            than adding to them. See :func:`_travelling`.

    Returns:
        The envelope id. When the dispatch was deferred this is still the id the
        job will carry, because ids are generated at seal time rather than
        assigned by the backend.
    """
    envelope = Envelope.seal(
        job, delay=_seconds(delay), queue=on, context=_travelling(context_override)
    )
    target = queue(connection)

    if after_commit:

        async def push() -> None:
            await target.push(envelope)

        if defer_until_commit(push):
            return envelope.id

    return await target.push(envelope)


async def dispatch_many(
    jobs: Sequence[Job],
    *,
    delay: float | timedelta = 0.0,
    on: str | None = None,
    connection: str | None = None,
    after_commit: bool = True,
    context_override: Mapping[str, Any] | None = None,
) -> list[str]:
    """Send several jobs in one round trip.

    Args:
        jobs: The jobs to run, in order.
        delay: Applied to all of them.
        on: Override their declared queue.
        connection: Which queue connection to use.
        after_commit: Defer until the surrounding transaction commits.
        context_override: What every envelope should carry instead of the
            correlation fields in effect. Replaces rather than merges.

    Returns:
        The envelope ids, in order.
    """
    travelling = _travelling(context_override)
    envelopes = [
        Envelope.seal(job, delay=_seconds(delay), queue=on, context=travelling) for job in jobs
    ]
    target = queue(connection)

    if after_commit:

        async def push() -> None:
            await target.push_many(envelopes)

        if defer_until_commit(push):
            return [envelope.id for envelope in envelopes]

    return await target.push_many(envelopes)


async def dispatch_now(job: Job) -> None:
    """Run a job inline, bypassing the queue entirely.

    Not the same as the ``sync`` driver: that is a deployment choice affecting
    every dispatch, this is one call deciding it does not want to wait. Useful
    in a CLI command, a migration backfill, or a test that wants the work done.

    Nothing is sealed and nothing is bound: the handler runs in the caller's own
    context, so it already sees the correlation fields a queued copy would have
    had to carry.

    Args:
        job: The job to run.

    Raises:
        Exception: Whatever the handler raises. No retries apply — there is no
            worker involved to perform them.
    """
    await job.handle()


__all__ = [
    "INHERITED_FIELDS",
    "bound_queue_manager",
    "current_queue_manager",
    "dispatch",
    "dispatch_many",
    "dispatch_now",
    "queue",
    "set_queue_manager",
    "use_queue",
]
