# Changelog

Releases are tags. `keel new` generates from the latest one, and `keel update`
moves a project between them. Entries say what changed for someone building on
Keel; the ADRs say why.

## v0.1.0 — 2026-09-19

The first release: five phases and an installer, built from 2026-09-12.

- **Phase 0, the skeleton.** The copier template, plain-dataclass configs with
  `from_env`, the async engine, and `uow()` — a session that is never held
  across a request. ADR 0002.
- **Phase 1, the cache.** A facade over Redis, in-memory and null drivers,
  `remember` with single flight, atomic locks, and a recording fake. ADR 0001.
- **Phase 2, the data layer.** A generic repository, UUIDv7 keys, soft deletes
  as a global scope, keyset pagination, observers after commit, factories and
  seeders, advisory-locked migrations. ADR 0003.
- **Phase 3, the queue.** Jobs as Commands, dispatch that waits for the commit,
  the SAQ driver, a supervised worker with graceful shutdown, orphan recovery
  and fault handling, durable failed jobs, and cron. ADR 0006.
- **Phase 4, auth and authz.** The current-identity context, Argon2 hashing
  with rehash on login, hashed-at-rest bearer tokens, and authorization
  policies per resource type. ADRs 0007 and 0008.
- **Phase 5, observability.** Correlation and structured logging, readiness
  checks, a development request inspector, and Prometheus metrics over the
  same sources. ADRs 0009 to 0012.
- **The installer.** `uv tool install` from the repository, `keel new` and
  `keel update`, a generated project pinned to the commit its scaffold came
  from. ADR 0013.
- **The template.** A service in three shapes — API, worker, or both from one
  image — with users, sessions and items modules, Alembic, rollback-per-test
  against a real Postgres, Docker Compose, and its own `CLAUDE.md`.

Two type checkers gate every commit (ADR 0004), and every subsystem is held to
a named design pattern or a recorded reason for declining one (ADR 0000).
