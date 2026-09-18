# Keel

[![CI](https://github.com/francisco-ltech/keel/actions/workflows/ci.yml/badge.svg)](https://github.com/francisco-ltech/keel/actions/workflows/ci.yml)

*A keel is the backbone of a hull: the member everything else is fastened to,
and what stops the boat being pushed sideways.*

Batteries for backend services. A core library you install, and a starter
template you generate from.

A service is one bounded domain — `invoices`, say — in one codebase, owning one
database schema. It deploys as an API, as a queue worker, or as both from the
same image, with the entrypoint chosen per deployment. Purely backend: no
frontend, no templating, no asset pipeline.

Keel takes its design principles from Laravel: convention over configuration,
one uniform way to reach each subsystem, and a test double for everything. It
borrows the ideas, not the framework. The core library depends on no web
framework, so the parts a worker uses carry no HTTP baggage.

Each subsystem is reached the same way: a **facade** the application calls, a
**swappable driver** behind it, and a **fake** that records what happened so
tests can assert on it. That uniformity is the whole idea, it is what makes a
set of features feel like one framework.

Nothing is published to an index yet. `keel new` installs the library from this
repository, pinned to the commit the scaffold came from; a contributor's
project links a checkout by path instead.

## Layout

```
src/keel/          the core library
template/          copier template for new applications
examples/          a small FastAPI app used as an end-to-end test
docs/adr/          why things are shaped the way they are
```

## What works today

| Subsystem | State |
|---|---|
| `keel.cache` | Cache, atomic locks, `remember` with single-flight. Drivers: Redis, in-memory, null. Recording fake with assertions. |
| `keel.database` | Async engine, pooling with a statement timeout, the `uow()` transaction idiom, declarative base with a migration-safe naming convention. |
| `keel.testing` | `fake_cache()` and `rolled_back_database()`. |
| `keel.database` (Phase 2) | Generic repository, UUIDv7 keys, soft deletes as a global query scope, keyset pagination, model observers that fire after commit, factories and seeders, advisory-locked migrations with drift detection. |
| `keel.queue` (Phase 3) | Jobs as Commands, dispatch that waits for the transaction to commit, SAQ driver, a supervised worker with graceful shutdown and orphan recovery, durable failed jobs, and cron guarded by an advisory lock. |
| `keel.auth` (Phase 4) | The current-identity context, Argon2 password hashing with rehash-on-login, hashed-at-rest bearer tokens, and authorization policies registered per resource type with `authorize()` / `allows()`. Drivers: Redis, in-memory. Recording fake. No guard protocol. |
| `keel.observability` (Phase 5) | A correlation context, structured JSON logging that carries it onto every record including third-party ones, jobs that inherit the request id that dispatched them, readiness checks, a development request inspector that records each request's queries, cache calls, dispatches and log lines as one timeline, and Prometheus metrics over the same sources. |
| `template/` | Generates a service in three shapes — API, worker, or both — with domain modules, Alembic, tests and Docker. |

Mail, storage and rate limiting are not in the box yet.
[The roadmap](docs/roadmap.md) says what is coming and in what order.

## Getting started

Install the command once, then generate a service:

```bash
uv tool install "keel[cli] @ git+https://github.com/francisco-ltech/keel"
keel new invoices     # asks for a name, a description and the shape
cd invoices && just up && just migrate && just dev
```

`keel new` copies the template from this repository, pins the project's Keel
dependency to the commit the scaffold came from, runs `uv sync`, and makes the
first commit. `keel update invoices` brings a project forward when the template
moves. Nothing is on PyPI yet; the git install is the installer ([ADR
0013](docs/adr/0013-the-installer.md)).

Working on Keel itself is `just`, not make; `just` alone lists every recipe.

```bash
just install          # uv sync, every extra included
just hooks            # the pre-commit and pre-push hooks
just up               # postgres + redis + mailpit
just check            # lint, both type checkers, tests
just new ../my-api    # a project linked to this checkout, editable
```

Postgres is on **5433** and Redis on **6380**, to avoid clashing with anything
already running on the default ports. Override `DATABASE_URL` or `REDIS_URL` to
point elsewhere. `just doctor` says whether they are actually answering.

## The one rule worth knowing before you write code

A database session is never held for the lifetime of a request. Routes receive
services; services open `async with uow() as session:` around the work that must
be atomic and return schemas, not ORM objects. There is deliberately no
`get_session` dependency to import.

This is not style. FastAPI PR #12066 a deadlock when dependencies are closed
during response-model validation has been open since August 2024 with
production outages attached, and the failure is invisible at p99 while the pool
starves. See [ADR 0002](docs/adr/0002-the-unit-of-work.md).

## Design notes

- [The roadmap](docs/roadmap.md): the phases, what each one delivered, and what
  Phase 4 unblocks.

- [ADR 0000 — design patterns are the bar](docs/adr/0000-design-patterns-are-the-bar.md):
  the standing rule this codebase is held to, including the patterns
  deliberately declined.
- [ADR 0001 — the cache seam](docs/adr/0001-the-cache-seam.md): the
  facade/driver/fake pattern, what it bought, and where it does *not* transfer.
- [ADR 0002 — the unit of work](docs/adr/0002-the-unit-of-work.md): session
  lifetime, and why the database is the one subsystem with no fake.
- [ADR 0003 — the data layer](docs/adr/0003-the-data-layer.md): the repository,
  soft deletes, keyset pagination, observers after commit, and why Keel owns
  this rather than depending on Advanced-Alchemy.
- [ADR 0004 — two type checkers](docs/adr/0004-two-type-checkers.md): ty for the
  inner loop, mypy as the gate, and the criteria for dropping one.
- [ADR 0013 — the installer](docs/adr/0013-the-installer.md): `keel new` and
  `keel update`, why the template's config moved to the repository root, and why
  a project pins Keel to the commit its scaffold came from.
- [ADR 0012 — metrics](docs/adr/0012-metrics.md): a second Observer over the
  inspector's sources, why `prometheus_client` was taken where `structlog` was
  not, every label bounded by construction, and the worker's liveness read at
  scrape time rather than tracked by event arithmetic.
- [ADR 0011 — the request inspector](docs/adr/0011-the-request-inspector.md):
  a trace on a context variable, why cache events stay on the store, what a
  development tool must keep off a trace, and the nine defects its review found.
- [ADR 0010 — readiness checks](docs/adr/0010-readiness-checks.md): a check as
  a function, every check under its own deadline, and why the database is probed
  on a connection of its own.
- [ADR 0009 — correlation and logging](docs/adr/0009-correlation-and-logging.md):
  why the record factory rather than a filter, what reaches a log line and what
  never does, and the guarantee the secret-name denylist does not give.
- [ADR 0008 — authorization policies](docs/adr/0008-authorization-policies.md):
  a policy as a function registered per resource type, why the check moved into
  the service, and the chain that was declined for the second time.
- [ADR 0007 — identity and tokens](docs/adr/0007-identity-and-tokens.md): the
  current-user context, why there is no guest, hashed-at-rest bearer tokens, and
  the guard protocol and user provider that were declined.
- [ADR 0006 — the queue](docs/adr/0006-the-queue.md): jobs as Commands, dispatch
  after commit, why the cache's shape did not transfer, and why Keel sweeps for
  orphans itself instead of using SAQ's.
