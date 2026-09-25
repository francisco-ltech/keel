# Keel

[![CI](https://github.com/francisco-ltech/keel/actions/workflows/ci.yml/badge.svg)](https://github.com/francisco-ltech/keel/actions/workflows/ci.yml)

*A keel is the backbone of a hull: the member everything else is fastened to,
and what stops the boat being pushed sideways.*

Backend services, batteries included. A core library you install, and a starter
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

## Layout

```
src/keel/          the core library
template/          copier template for new applications
docs/              one guide per feature, and the ADRs behind them
examples/          a small FastAPI app used as an end-to-end test
```

## What is in the box

Each feature has a short guide: how to wire it, the calls, the fake, and
what the template does with it. The reasoning lives in the ADRs each guide
links.

| Feature | Guide | In one line |
|---|---|---|
| Cache | [docs/cache.md](docs/cache.md) | `cache.remember` with single flight, atomic locks and counters; Redis, in-memory or null behind it. |
| Database | [docs/database.md](docs/database.md) | The `uow()` transaction idiom, repositories, UUIDv7 keys with public identifiers, soft deletes, observers after commit, locked migrations. |
| Queue | [docs/queue.md](docs/queue.md) | Jobs as commands, dispatch that waits for the commit, a supervised worker, durable failed jobs, a cron schedule. |
| Authentication | [docs/authentication.md](docs/authentication.md) | The current identity, Argon2 passwords, hashed bearer tokens, and a policy per resource type. |
| Rate limiting | [docs/ratelimit.md](docs/ratelimit.md) | `guard(key, Limit.per_minute(5))` on the cache's store, raising with how long to wait; sign-in and reset requests throttled in the template. |
| Mail | [docs/mail.md](docs/mail.md) | `send(Message(...))` over SMTP, a logging driver or null, messages rendered from templates, header injection refused before a driver sees it. |
| Observability | [docs/observability.md](docs/observability.md) | A request id on every log line into the worker, readiness checks, a request inspector for development, Prometheus metrics. |
| Template | [docs/getting-started.md](docs/getting-started.md) | A service in three shapes, API, worker or both, with domain modules, Alembic, tests and Docker. |

Every subsystem is reached the same way. Tests replace any of them with one
line from `keel.testing`: `fake_cache()`, `fake_queue()`, `fake_tokens()`,
`fake_mail()`, and `rolled_back_database()` for the one that has no fake.

## Getting started

```bash
uv tool install "keel[cli] @ git+https://github.com/francisco-ltech/keel"
keel new invoices     # asks for a name, a description and the shape
cd invoices
just up               # Postgres, Redis and Mailpit, via Docker Compose
just dev              # the app in containers next to them: http://localhost:8000/docs
```

[docs/getting-started.md](docs/getting-started.md) continues from here: what
the service has on day one, a walk through it with `curl`, `keel update`, and
working on Keel itself.

## The one rule worth knowing before you write code

A database session is never held for the lifetime of a request. Routes receive
services; services open `async with uow() as session:` around the work that
must be atomic and return schemas, not ORM objects. There is deliberately no
`get_session` dependency to import. This is not style: the failure is a pool
that starves while p99 still looks healthy. See
[ADR 0002](docs/adr/0002-the-unit-of-work.md).

## Design notes

- [The roadmap](docs/roadmap.md): the phases, what each one delivered, and
  what is not scheduled.
- [ADR 0000 — design patterns are the bar](docs/adr/0000-design-patterns-are-the-bar.md):
  the standing rule this codebase is held to, including the patterns
  deliberately declined.
- [ADR 0004 — two type checkers](docs/adr/0004-two-type-checkers.md): ty for
  the inner loop, mypy as the gate.
- [ADR 0013 — the installer](docs/adr/0013-the-installer.md): `keel new` and
  `keel update`, and why a project pins Keel to the commit its scaffold came
  from.
- The [changelog](CHANGELOG.md): releases are tags.

## License

MIT, see [LICENSE](LICENSE).
