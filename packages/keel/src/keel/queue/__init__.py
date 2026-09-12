"""The queue subsystem.

Application code needs one import:

    from keel.queue import Job, dispatch

    @dataclass(frozen=True, slots=True)
    class SendInvoiceEmail(Job):
        invoice_id: str
        async def handle(self) -> None:
            await invoices.email(self.invoice_id)

    await dispatch(SendInvoiceEmail(str(invoice.id)))

Wiring is one context manager, as it is for the cache and the database:

    async with queue_lifespan(QueueConfig.from_env()):
        ...

The design deliberately does not mirror the cache. See ADR 0001 for why the
Store/Repository split does not transfer, and :mod:`keel.contracts.queue` for
why only the dispatch side has a protocol.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib import import_module
from typing import TYPE_CHECKING, Final

from keel.contracts.queue import Queue
from keel.queue.backoff import (
    Backoff,
    ExponentialBackoff,
    FixedBackoff,
    NoBackoff,
)
from keel.queue.config import QueueConfig
from keel.queue.dispatch import (
    bound_queue_manager,
    current_queue_manager,
    dispatch,
    dispatch_many,
    dispatch_now,
    queue,
    set_queue_manager,
    use_queue,
)
from keel.queue.drivers import NullQueue, SyncQueue
from keel.queue.envelope import Envelope
from keel.queue.fake import FakeQueue, QueueAssertionError
from keel.queue.job import (
    Job,
    JobError,
    PermanentFailureError,
    UnknownJobError,
    registered_jobs,
    resolve_job,
)
from keel.queue.manager import QueueManager


@asynccontextmanager
async def queue_lifespan(config: QueueConfig) -> AsyncIterator[QueueManager]:
    """Bind a queue manager for the life of the process.

    Framework-agnostic, like the cache's and the database's: it drops into a
    FastAPI ``lifespan``, a worker's main coroutine, or a script unchanged. That
    matters more here than elsewhere — the same wiring runs in an API replica
    that only dispatches and in a worker replica that only consumes.

    Restores whatever was bound before rather than unbinding, so a test lifespan
    nested inside an application lifespan does not silently kill the outer one.

    Args:
        config: How to reach the queue.

    Yields:
        The bound manager.
    """
    previous = bound_queue_manager()
    manager = QueueManager(config)
    set_queue_manager(manager)
    try:
        yield manager
    finally:
        await manager.close()
        set_queue_manager(previous)


# -- lazily exported ------------------------------------------------------
#
# These live behind ``__getattr__`` (PEP 562) rather than being imported here,
# and the reason is a property worth protecting: a service that only *dispatches*
# — an API replica — should not pay to import a worker runtime it will never
# run. `saq_driver` and `worker` pull in SAQ, which is an optional extra, so an
# eager import would also make `import keel.queue` fail for anyone who did not
# install it. `failed` and `scheduler` pull in the whole database layer, which a
# queue-only process has no use for.
#
# The names are still declared under TYPE_CHECKING, so editors and both type
# checkers resolve them exactly as if they were imported normally.

if TYPE_CHECKING:
    from keel.queue.failed import DatabaseFailureSink, FailedJob, FailedJobs
    from keel.queue.saq_driver import Reservation, SaqQueue
    from keel.queue.scheduler import (
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
        Trigger,
    )
    from keel.queue.worker import (
        DeadLetterDiscarded,
        EventFailureSink,
        FailureSink,
        JobDeadLettered,
        JobEvent,
        JobRecovered,
        JobRetrying,
        JobStarted,
        JobSucceeded,
        JobUnroutable,
        Worker,
        WorkerEvent,
        WorkerStarted,
        WorkerStopped,
    )

_LAZY: Final[dict[str, str]] = {
    "SaqQueue": "keel.queue.saq_driver",
    "Reservation": "keel.queue.saq_driver",
    "DEFAULT_SWEEP_INTERVAL": "keel.queue.saq_driver",
    "Worker": "keel.queue.worker",
    "FailureSink": "keel.queue.worker",
    "EventFailureSink": "keel.queue.worker",
    "WorkerEvent": "keel.queue.worker",
    "WorkerStarted": "keel.queue.worker",
    "WorkerStopped": "keel.queue.worker",
    "JobEvent": "keel.queue.worker",
    "JobStarted": "keel.queue.worker",
    "JobSucceeded": "keel.queue.worker",
    "JobRetrying": "keel.queue.worker",
    "JobDeadLettered": "keel.queue.worker",
    "JobUnroutable": "keel.queue.worker",
    "JobRecovered": "keel.queue.worker",
    "DeadLetterDiscarded": "keel.queue.worker",
    "FailedJob": "keel.queue.failed",
    "FailedJobs": "keel.queue.failed",
    "DatabaseFailureSink": "keel.queue.failed",
    "Schedule": "keel.queue.scheduler",
    "Scheduler": "keel.queue.scheduler",
    "ScheduleEntry": "keel.queue.scheduler",
    "ScheduleRun": "keel.queue.scheduler",
    "ScheduleRuns": "keel.queue.scheduler",
    "Trigger": "keel.queue.scheduler",
    "CronTrigger": "keel.queue.scheduler",
    "IntervalTrigger": "keel.queue.scheduler",
    "ScheduleError": "keel.queue.scheduler",
    "CronError": "keel.queue.scheduler",
    "DuplicateEntryError": "keel.queue.scheduler",
}


def __getattr__(name: str) -> object:
    """Import a lazily-exported name on first use.

    Args:
        name: The attribute being looked up.

    Returns:
        The object from its defining module.

    Raises:
        AttributeError: If the name is not part of this package's surface.
    """
    module_name = _LAZY.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value  # cached, so the indirection is paid once
    return value


def __dir__() -> list[str]:
    """Include the lazy names, so tab completion and `dir()` show the real surface."""
    return sorted(set(globals()) | set(_LAZY))


__all__ = [
    "Backoff",
    "CronError",
    "CronTrigger",
    "DatabaseFailureSink",
    "DeadLetterDiscarded",
    "DuplicateEntryError",
    "Envelope",
    "EventFailureSink",
    "ExponentialBackoff",
    "FailedJob",
    "FailedJobs",
    "FailureSink",
    "FakeQueue",
    "FixedBackoff",
    "IntervalTrigger",
    "Job",
    "JobDeadLettered",
    "JobError",
    "JobEvent",
    "JobRecovered",
    "JobRetrying",
    "JobStarted",
    "JobSucceeded",
    "JobUnroutable",
    "NoBackoff",
    "NullQueue",
    "PermanentFailureError",
    "Queue",
    "QueueAssertionError",
    "QueueConfig",
    "QueueManager",
    "Reservation",
    "SaqQueue",
    "Schedule",
    "ScheduleEntry",
    "ScheduleError",
    "ScheduleRun",
    "ScheduleRuns",
    "Scheduler",
    "SyncQueue",
    "Trigger",
    "UnknownJobError",
    "Worker",
    "WorkerEvent",
    "WorkerStarted",
    "WorkerStopped",
    "bound_queue_manager",
    "current_queue_manager",
    "dispatch",
    "dispatch_many",
    "dispatch_now",
    "queue",
    "queue_lifespan",
    "registered_jobs",
    "resolve_job",
    "set_queue_manager",
    "use_queue",
]
