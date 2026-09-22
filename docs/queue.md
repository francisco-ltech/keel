# Queue

Background work as jobs: an operation packaged with its parameters, dispatched from
a request and run later by a worker. The request returns before the slow part
happens, and the slow part is retried when it fails.

## Wiring

One context manager, the same shape as the cache and the database. It works in a
FastAPI `lifespan`, a worker's main coroutine, or a script.

```python
from keel.queue import QueueConfig, queue_lifespan

async with queue_lifespan(QueueConfig.from_env()):
    ...
```

The SAQ driver needs the `queue` extra: `pip install "keel[queue]"`. The other
drivers need nothing.

`QueueConfig.from_env()` reads these variables. There is no `KEEL_` prefix.

| Variable | Default | What it is |
|---|---|---|
| `QUEUE_DRIVER` | `sync` | `saq`, `sync`, `null` or `fake` |
| `REDIS_URL` | unset | Shared with the cache. Required by `saq` |
| `QUEUE_PREFIX` | `keel:queue` | Key namespace. A sibling of the cache's, so a cache flush cannot drain the queue |
| `QUEUE_DEFAULT` | `default` | The queue a job goes to when it names none |
| `QUEUE_CONCURRENCY` | `10` | Jobs one worker process runs at once |

`saq` is the real queue, on Redis. `sync` runs each job inline at dispatch, so a
single-process development setup needs no worker. `null` discards every job, for a
smoke-test environment. `fake` records without running; `fake_queue()` binds it.

## Jobs

A job is a frozen dataclass with a `handle()` method. The fields are the payload
and travel over the wire, so declare them as wire types: `str`, not `UUID`.

```python
from dataclasses import dataclass
from typing import ClassVar

from keel.queue import Backoff, ExponentialBackoff, Job, PermanentFailureError


@dataclass(frozen=True, slots=True)
class SendInvoiceEmail(Job):
    invoice_id: str

    max_attempts: ClassVar[int] = 5
    timeout: ClassVar[float | None] = 30.0
    backoff: ClassVar[Backoff] = ExponentialBackoff(base=2.0, maximum=60.0)

    async def handle(self) -> None:
        async with uow() as session:
            invoice = await Invoices(session).get_by_pid(UUID(self.invoice_id))
            if invoice is None:
                raise PermanentFailureError(f"invoice {self.invoice_id} is gone")
            summary = InvoiceRead.model_validate(invoice)
        await send(invoice_mail(summary))
```

`Invoices`, `InvoiceRead` and `invoice_mail` are the application's own; the
template's `SendWelcome` in `app/modules/users/jobs.py` is this shape for real.

Policy lives in `ClassVar`s: `max_attempts` (default 3, total attempts including
the first), `timeout` (default 300 seconds per attempt), `backoff` (default
`ExponentialBackoff()` with jitter), `queue` and `unique_for`. Class-level, so a
caller cannot pass them and an old envelope cannot override them. `FixedBackoff`
and `NoBackoff` are the other two policies.

Raise `PermanentFailureError` when a retry cannot help. The job is dead-lettered
at once instead of spending its attempt budget on a row that was deleted on purpose.

Write `handle()` to be idempotent. Delivery is at least once, and a worker that
dies after the work but before the acknowledgement hands the job to another.

A job registers itself under its class name when the class is defined. A worker
that never imports the module treats every envelope for it as unroutable. The
template's `JOBS` tuple in `app/modules/__init__.py` makes that import explicit.

## Dispatch waits for the commit

Inside `uow()`, `dispatch()` holds the push until the transaction commits. A
rollback pushes nothing. That is the default, not an option; the reasoning is in
[ADR 0006](adr/0006-the-queue.md), decision 3.

```python
from keel.database import uow
from keel.queue import dispatch

async with uow() as session:
    invoice = await Invoices(session).create(...)
    await dispatch(SendInvoiceEmail(invoice_id=str(invoice.pid)))
```

Outside a transaction the push is immediate. `dispatch()` also takes `delay`, `on`
and `connection`. `dispatch_many()` sends a list in one round trip.

Two opt-outs, both meant to look deliberate. `dispatch(job, after_commit=False)`
pushes now even inside a transaction. `dispatch_now(job)` runs the handler inline
with no queue, no retries and no envelope, for a CLI command or a backfill.

## The worker

`Worker(config, failure_sink=..., events=...)` is the consume side, and there is
only one. It needs the `saq` driver. `await worker.run()` returns when asked to stop.

It is supervised: reserve, promote and sweep run as separate tasks and one
raising does not cancel the others. On the first SIGTERM it stops reserving,
lets in-flight jobs finish and returns; a second signal cancels them. It sweeps
for orphans on a timer, so a job whose worker was killed is re-queued once it
has outrun its own timeout. When Redis raises, the loop reports `WorkerFaulted`
and waits under `fault_backoff` rather than exiting.

`worker.healthy` is the liveness question and `worker.accepting` is readiness. The
template's `app/worker.py` touches a heartbeat file while `healthy` is true, and
`app/healthcheck.py` is the exec probe that checks the file's age.

## Failed jobs and the schedule

A job that runs out of attempts goes to `keel_failed_jobs` through
`DatabaseFailureSink`, with the whole envelope and the traceback. `FailedJobs` is
the repository an operator uses: `record`, `retry`, `retry_all`, `forget`,
`prune`. It takes a session and never commits, like every other repository.

Recurring work is a `Schedule`, inert data built at import time.

```python
from datetime import timedelta

from keel.queue import Schedule, Scheduler

SCHEDULE = (
    Schedule()
    .cron("0 3 * * *", PruneDeadLetters())
    .every(timedelta(minutes=5), RefreshSearchIndex())
)
await Scheduler(SCHEDULE).run()
```

`.cron()` builds a `CronTrigger` and `.every()` an `IntervalTrigger`. Every worker
replica can run the scheduler. A per-entry Postgres advisory lock stops replicas
racing, and a committed `(entry, due_at)` row is what makes each run exactly once.
A missed run starts late, up to an hour, and only the most recent one.

## In tests

`fake_queue()` binds a recording fake for the block. It runs nothing.

```python
from keel.testing import fake_queue

with fake_queue() as queued:
    await invoices.approve(invoice_id)
queued.assert_pushed(SendInvoiceEmail, invoice_id=str(invoice_id))
```

"Did it enqueue" and "does the job work" are two tests. The first uses the fake;
the second awaits `handle()` directly and asserts on the effect. `assert_not_pushed`,
`assert_pushed_times`, `assert_nothing_pushed`, `assert_pushed_on` and
`assert_delayed` cover the other questions.

## In the template

`app/modules/items/jobs.py` defines `IndexItem`, dispatched from `create_item`
inside its unit of work. `app/modules/users/jobs.py` defines `SendWelcome`,
dispatched on registration. `app/worker.py` wires the lifespans, the
`DatabaseFailureSink`, the log listeners, the heartbeat, and a nightly
`PruneDeadLetters` at 03:00. `tests/test_jobs.py` shows both kinds of test.

## Limits

- No consume-side protocol. One implementation behind an interface is
  indirection. [ADR 0006](adr/0006-the-queue.md), decision 2.
- No job middleware chain, such as `WithoutOverlapping` or `RateLimited`, until a
  second middleware exists. [ADR 0006](adr/0006-the-queue.md).
- No batches, chains or priorities within a queue. Separate queues plus dedicated
  workers cover the need. [ADR 0006](adr/0006-the-queue.md).
- A job with `timeout = None` can never be recovered from a dead worker.
  [ADR 0006](adr/0006-the-queue.md), decision 5.

## Further reading

- [ADR 0006 — the queue](adr/0006-the-queue.md)
