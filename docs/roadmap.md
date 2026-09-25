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

## Distribution

Not a phase, and done alongside the last one ([ADR
0013](adr/0013-the-installer.md)): `uv tool install` from the repository puts a
`keel` command on the path, `keel new` generates a service pinned to the commit
its scaffold came from, and `keel update` brings it forward. Releases are tags,
`v0.1.0` the first, recorded in the changelog; `keel new` generates from the
latest one. PyPI is the next step, needing a name check and a publish workflow.
The repository carries an MIT license, like Laravel's.

## Phase 6 — the batteries with a caller

Started 2026-09-21. The "beyond" list below had a rule: a subsystem gets a
number when something needs it. Mail was first, because the template's
registration had nobody to tell and the password reset it declines needs the
same seam.

* **Mail** ([0015](adr/0015-mail.md)) — `send()` over `smtp`, `log`, `null`
  and a recording fake, a `Message` that refuses header injection once for
  every driver, SMTP from the standard library on a worker thread, and a
  welcome on registration in the template that is sent only once the
  transaction commits. Mailpit beside Postgres and Redis under `just up`.
* **Password reset, and messages from templates** ([0016](adr/0016-password-reset.md))
  — the template's change path for somebody who cannot sign in: a single-use
  code hashed at rest, redeemed in one statement, silent about whether the
  address exists. Writing its message met ADR 0015's condition for a template
  engine, so `keel.mail.templates` renders both messages from a shared layout.
* **Rate limiting** ([0017](adr/0017-rate-limiting.md)) — a fixed window on
  the cache's store, no driver family of its own; `guard` raises with the
  wait and `attempt` answers. Sign-in is five wrong passwords a minute per
  address and client, reset requests ten a minute per client, both before
  anything is read or hashed.

## Next

Nothing else in Phase 6 is scheduled. Object storage, notifications and a
job middleware chain still wait for something in the template to need them.

## Beyond, unscheduled

Object storage, notifications, rate limiting, and a job middleware chain.
Each is a real feature with a real cost, and none has a caller yet. They get a
phase number when something needs them — assigning one earlier is how a roadmap
starts describing work that never happens.
