# ADR 0006 — The queue

**Status:** accepted · **Date:** 2026-09-12 · **Phase:** 3

## Context

ADR 0001 ended with a prediction that turned out to matter: **the cache seam does
not transfer to the queue.** A cache is caller-push, key-addressed and
value-returning; a queue is worker-pull, payload-carrying and effect-producing.
`Store`'s entire vocabulary is `(key, value, ttl)`, while a job is
`(payload, queue, delay, attempts, backoff, reserved_until)` with no key and
nothing to return.

So this subsystem was designed from the queue's problem, reaching back for the
pieces that generalise — `Manager[T]`, `Binding`, `EventDispatcher`, the
parametrised contract technique — rather than copying a shape that fit something
else. What follows is what that produced, and where the prediction held.

## Decisions

### 1. A job is the Command pattern, and the payload is fused to the handler

`Job` packages an operation with its parameters as an object, so it can be
serialised, queued, logged, retried and executed by a process that does not know
what it does. This is the one place in Keel where a pattern *is* the design
rather than a supporting player, and it is the pattern the cache never needed.

Data and behaviour live in one class. Splitting them means two files to change
per field and a mapping between them that can silently drift; Laravel fuses them
for the same reason.

**Policy lives in `ClassVar`s, so it never reaches the wire.** How many times to
retry is a property of the job *type*, decided by its author — not a value a
caller passes, and not something an old envelope can override after a deploy
changes the policy.

### 2. Only the dispatch side has a protocol

`Queue` covers `push`, `push_many`, `size`, `clear`, `close`. There is no
consume-side protocol.

The asymmetry is the point. Application code pushes and must be able to swap a
real queue for a recording fake, so the push side gets an interface. Exactly one
component consumes, and an interface with a single implementor is indirection
pretending to be design (ADR 0000).

**No Bridge either.** It earned its place in the cache because `remember` is a
substantial abstraction derived from primitives. A queue's dispatch side is
`push`/`later`/`bulk`; a second layer would be ceremony. This was the review's
call in Phase 1 and it held.

### 3. Dispatch waits for the commit, by default

The behaviour the phase exists for:

```python
async with uow() as session:
    invoice = await Invoices(session).create(...)
    await dispatch(SendInvoiceEmail(invoice.id))
    await charge(invoice)  # raises
```

Push immediately and a worker is emailing a customer about an invoice that never
existed — and usually losing the race, so the job dead-letters looking like
corrupted data. Making the safe ordering the *default* rather than something to
remember is the same reasoning as ADR 0002's refusal to offer a request-scoped
session: the unsafe version is what people write when they are not thinking
about it.

Two pieces make it work, both in `keel.database.hooks`: a context variable
publishing the active session, so code deep in the call stack can discover a
transaction without it being threaded through every signature; and a per-session
callback buffer drained after commit and discarded otherwise. This generalises
the Phase 2 observer buffer, which was built for the same guarantee.

Outside a transaction, dispatch is immediate. `after_commit=False` and
`dispatch_now()` are the escape hatches, and both have to be written down.

### 4. An attempt is charged at reservation, not at failure

**A delivery that kills its worker is still a delivery.** Charging on failure
means a job that reliably crashes its worker is never seen to fail, so it is
never exhausted, so it eats the fleet one worker at a time.

`reserve()` writes the incremented envelope back with the job. A recovered job
resumes on what it has left, and a poison job dies after `max_attempts` like any
other. The unroutable path explicitly *refunds* the charge, because that case is
the worker being wrong rather than the job.

### 5. Orphan recovery is Keel's own, because SAQ's would have eaten live work

The acceptance criterion for this phase was *"killing the worker mid-job loses
nothing"*. Meeting it required a sweeper, and the obvious move — call SAQ's
`Queue.sweep()` — is wrong here.

SAQ's staleness rule is "re-queue an active job that is not marked `ACTIVE`, or
that has outrun its timeout". SAQ's own worker marks a job active inside its
dequeue; Keel's `reserve()` needs a second round trip. Every job Keel reserves
therefore looks abandoned for the gap in between, and a sweep landing in that
window yanks live work off a healthy worker and runs it twice. **This was
observed, not theorised.**

Keel's sweeper applies only the honest half of the rule — started, and overdue by
its own timeout — under a `SET NX EX` lock so one replica scans per window. It
runs on its own task at 60s, not inside the maintenance loop, so a minute-scale
scan cannot stall second-scale promotion of delayed jobs.

**The honest consequence:** a job that declares no timeout can never be
recovered, because nothing distinguishes it from one still running. That is the
strongest argument for `DEFAULT_TIMEOUT` being a number rather than `None`, and
it is pinned by a test.

### 6. `UnknownJobError` costs the job nothing

A worker running yesterday's code receiving today's job is the *worker* being
wrong. The envelope is re-offered with a delay and no attempt spent, bounded at
an hour so it cannot loop forever. Burning attempts there would let three stale
replicas dead-letter a perfectly valid job before a rollout finished.

A `JobError` — the payload no longer fits the fields — is dead-lettered
immediately. It is deterministic, so retrying only delays a human seeing it.

### 7. The fake records; it does not run

`FakeStore` is a Decorator over a real store, so cache tests exercise real TTLs
and real serialisation. That trick does not transfer, and the reason is worth
stating: running the job means the test now exercises the handler, its writes,
its outbound calls and its failure modes — while claiming to test the code that
*dispatched* it. "Did this enqueue the email?" and "does the email job work?" are
two tests, and conflating them means neither failure says where to look.

Running is what `SyncQueue` is for, and it is a real deployment choice rather
than a double — how a single-process development environment works without a
worker at all.

### 8. Failure recording downgrades; retry inverts the dispatch ordering

A database outage while dead-lettering must not propagate: the sink logs the
whole encoded envelope plus the original traceback, so the job is re-dispatchable
by hand, and deliberately does not retry — a retry loop would hold the worker
slot for the length of the outage while everything behind it dead-lettered too.

`retry()` pushes *inside* the transaction and deletes the row only if the push
returned — the inverse of `dispatch()`. Deferring would delete, commit, then
discover the queue is down, losing the job. The residual risk is the mirror
(push succeeds, commit fails ⇒ at-least-once), which handlers must already
tolerate. It also bypasses `dispatch()` so the replay is verbatim: re-sealing
from a live `Job` would silently apply *today's* policy and drop the original
context.

### 9. Cron: the lock is not what makes it exactly-once

Scheduling is guarded by a per-entry Postgres advisory lock, but the guarantee
comes from a committed `(entry, due_at)` row. The lock is mutual exclusion only.
Per-tick locking was rejected: it makes one replica serialise every job and turns
into a leader election needing a lease.

Missed ticks run late by default, up to an hour, and only the most recent missed
instant — never the backlog.

## Consequences

**`keel.queue` exports lazily.** `Worker` and `SaqQueue` pull in SAQ, an optional
extra; `FailedJobs` and `Scheduler` pull in the database layer. A dispatch-only
API replica should pay for none of it, so they resolve through `__getattr__` with
`TYPE_CHECKING` declarations so editors and both type checkers still see them.

**Jobs must be dataclasses**, checked when the payload is built rather than at
class creation, because `@dataclass` has not run when `__init_subclass__` fires.
Relatedly, `slots=True` *rebuilds* the class and fires `__init_subclass__` twice,
so registration compares module and name rather than identity.

**`DEFAULT_TIMEOUT` is load-bearing**, per decision 5.

**Delays are approximate below a second.** SAQ's delayed jobs sit in a sorted set
until something promotes them, and promotion is rate-limited by SAQ's internal
one-second lock.

## What the contract suite caught

`FakeQueue` documented `clear(None)` as "every queue" while the protocol said
"the default queue", and the two shipped disagreeing — found by a human reading
both docstrings, which is not a dependable process. Resolved in favour of the
narrow reading, for the same reason `Repository.purge()` refuses to run without
criteria: an administrative, destructive operation must not do the widest
possible thing when an argument is omitted.

Writing the suite then immediately found a second defect: `keel.contracts.queue`
and `keel.queue` imported each other, which only failed when the contract was
imported first.

Both are the Phase 1 lesson repeating. A shared, parametrised suite is what makes
a small implementation trustworthy, and it is the technique that transfers even
when the shape does not.

`SyncQueue` and `NullQueue` are excluded from it — they satisfy the interface and
deliberately not the behaviour, the same exception `NullStore` gets.

## What was deliberately not done

* **Job batches and chains.** Real features with real cost; nothing needs them.
* **A job middleware chain.** `WithoutOverlapping` and `RateLimited` are the
  obvious Chain of Responsibility, and the pattern is right — but building the
  chain before a second middleware exists would be scaffolding for one case.
* **A queue admin UI.** SAQ ships one; mounting it is template work.
* **Priorities within a queue.** Separate queues plus dedicated workers cover the
  actual need, without the starvation failure mode.

## Verification

- `test_a_rolled_back_transaction_dispatches_nothing` — the guarantee the phase
  exists for.
- `test_the_row_is_committed_before_the_job_is_dispatched` — ordering, not just
  presence.
- `test_killing_a_worker_mid_job_loses_nothing` — the acceptance criterion,
  mutation-checked: deleting the one line that starts the recovery task makes it
  fail and nothing else does.
- `test_a_healthy_workers_own_jobs_are_never_swept` — the regression guard for
  the SAQ rule that was rejected.
- `test_a_job_that_keeps_killing_its_worker_is_not_immortal` — decision 4.
- `test_a_job_with_no_timeout_cannot_be_recovered` — the honest consequence.
- `test_only_one_sweeper_runs_per_lock_window` — proves the *lock* gates it
  rather than emptiness.
- `test_clear_with_no_argument_clears_only_the_default_queue` — across every
  implementation.
- Two schedulers racing one due entry produce exactly one run.
