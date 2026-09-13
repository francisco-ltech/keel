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

Phase 1 ran before Phase 0 on purpose. The cache is the smallest subsystem that
still needs every part of the shape — facade, driver, fake, contract suite — so
it was the cheapest place to find out whether the shape was right before three
more subsystems were built on it. ADR 0001 records what transferred and what
did not.

## Next

### Phase 4 — auth and authz

The largest remaining gap against Laravel, and the one three other decisions are
waiting on.

Scope: password hashing and verification (Argon2 is already a dependency),
sessions or tokens, a current-user context, and authorization as policies
against a resource. Both halves of the shape apply — a driver seam for *how* a
caller is identified, and a fake for tests.

It unblocks:

* **Audit columns** (`created_by`, `updated_by`), deferred by ADR 0003 because a
  column that is always `NULL` is worse than no column.
* **Multi-tenancy**, whose mechanism is the global query scope the soft-delete
  listener already demonstrates.
* The generated template's own README, which tells users authentication arrives
  as a new module rather than as a migration of every row.

Open questions to settle in its ADR: where the current user lives (a ContextVar,
the way `keel.database.hooks` publishes the active session, is the obvious
candidate and the pattern already exists); and whether a policy is a Strategy
per resource or a Chain of Responsibility — the latter being the pattern ADR
0006 earmarked for job middleware and declined for having only one case.

### Phase 5 — observability

The request inspector, plus structured logging, health and metrics.

ADR 0001 leaves one question deliberately open for this phase: whether cache
events are emitted from the `Store` or the `Repository`. The inspector is the
first consumer with a real opinion, and guessing before it exists risks building
the wrong answer twice.

## Beyond, unscheduled

Mail, object storage, notifications, rate limiting, and a job middleware chain.
Each is a real feature with a real cost, and none has a caller yet. They get a
phase number when something needs them — assigning one earlier is how a roadmap
starts describing work that never happens.
