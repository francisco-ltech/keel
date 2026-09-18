# Roadmap

Keel is built in phases, each one a subsystem taken to completion rather than a
layer taken across everything. A phase is done when it has an ADR, a
parametrised contract suite over its drivers, a fake, and template support.

The numbers are referenced from the ADRs ("that needs a current-user context,
which is Phase 4"), so they are stable once assigned.

## Done

| Phase | Subsystem | ADR |
|---|---|---|
| 1 | **The cache seam.** Facade over a swappable driver with a fake, `remember` with single-flight, atomic locks. Redis, in-memory and null drivers. | [0001](adr/0001-the-cache-seam.md) |
| 0 | **The skeleton.** The copier template, settings, the async engine, and `uow()` — session lifetime decided before anything was built on it. | [0002](adr/0002-the-unit-of-work.md) |
| 2 | **The data layer.** Generic repository, UUIDv7 keys, soft deletes as a global scope, keyset pagination, observers after commit, factories and seeders, advisory-locked migrations. | [0003](adr/0003-the-data-layer.md) |
| 3 | **The queue.** Jobs as Commands, dispatch that waits for the commit, the SAQ driver, a supervised worker with orphan recovery, durable failed jobs, and cron. | [0006](adr/0006-the-queue.md) |
| 4 | **Auth and authz.** The current-identity context, Argon2 hashing, bearer tokens, and authorization policies. Detail below. | [0007](adr/0007-identity-and-tokens.md), [0008](adr/0008-authorization-policies.md) |
| 5 | **Observability.** Correlation and structured logging, readiness checks, the worker's fault handling, a development request inspector, and Prometheus metrics over the same sources. Detail below. | [0009](adr/0009-correlation-and-logging.md), [0010](adr/0010-readiness-checks.md), [0011](adr/0011-the-request-inspector.md), [0012](adr/0012-metrics.md) |

Phase 1 ran before Phase 0 on purpose. The cache is the smallest subsystem that
still needs every part of the shape — facade, driver, fake, contract suite — so
it was the cheapest place to find out whether the shape was right before three
more subsystems were built on it. ADR 0001 records what transferred and what
did not.

### Phase 4 in detail

The largest remaining gap against Laravel, and the one three other decisions
were waiting on.

* The **current-identity context** — a frozen `Identity` value on a ContextVar,
  never the application's user row, and no guest object.
* **Password hashing** — Argon2id with rehash-on-login and a constant-cost miss.
* **Bearer tokens** — hashed at rest, expiry checked on read, per-subject
  revocation. Redis and in-memory drivers, a recording fake, one contract suite.
* **Authorization policies** — a function per resource type, registered by exact
  type, checked in the service rather than the router.

**Declined:** a guard protocol and a user provider (Laravel's two seams here),
and a Chain of Responsibility for policies. The ADRs carry the reasoning and the
condition that would change each.

**Known limits, recorded rather than fixed:** the equal-cost login holds only
while stored hashes share the current cost parameters; revocation cannot reach a
sign-in already in flight; nothing is rate-limited. ADR 0007, "What this does
not do".

**What it unblocked:** audit columns (`created_by`, `updated_by`), deferred by
ADR 0003 because they needed a current-user context, and multi-tenancy, whose
mechanism is the global query scope the soft-delete listener demonstrates.
Neither is built.

### Phase 5 in detail

Four slices, each an ADR, plus one fix the second slice's review found:

* **Correlation and logging** ([0009](adr/0009-correlation-and-logging.md)) —
  a context of fields, a record factory that puts them on every log line, and
  `dispatch()` sealing them onto the envelope so a worker's lines name the
  request that caused the work.
* **Readiness** ([0010](adr/0010-readiness-checks.md)) — `probe()` over named
  checks, concurrent and each under a deadline, on a database connection of
  its own.
* **The worker survives a driver fault** ([0006, decision
  10](adr/0006-the-queue.md)) — a loop that hits a Redis error pauses under a
  backoff rather than cancelling every running job.
* **The request inspector** ([0011](adr/0011-the-request-inspector.md)) —
  under `DEBUG`, every request as one timeline. It closed ADR 0001's open
  question: cache events stay on the `Store`.
* **Metrics** ([0012](adr/0012-metrics.md)) — Prometheus counters and
  histograms over the same sources, every label bounded by construction, and
  the worker's liveness numbers read off the worker at scrape time.

**Not built:** tracing. Exported spans with sampling and retention have no
caller, and the inspector answers the development half of that question.

## Next

Nothing is scheduled. Phase 5 closed with metrics; see below for what would
get a number next and why nothing has one yet.

## Beyond, unscheduled

Mail, object storage, notifications, rate limiting, and a job middleware chain.
Each is a real feature with a real cost, and none has a caller yet. They get a
phase number when something needs them — assigning one earlier is how a roadmap
starts describing work that never happens.
