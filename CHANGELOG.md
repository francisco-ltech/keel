# Changelog

Releases are tags. `keel new` generates from the latest one, and `keel update`
moves a project between them. Entries say what changed for someone building on
Keel; the ADRs say why.

## Unreleased

- **`/items` is the caller's own.** The session names the owner, so a client
  never sends its user id over the wire to work with its items.
  `/users/{owner_id}/items` stays for an administrator acting for somebody,
  and both go through the same service and policy.
- **`GET /sessions/current` returns a profile, not a user record.** No id:
  the session names the caller.
- **A primary key never crosses the wire.** `keel.database.PublicId` adds a
  random `pid` beside the time-ordered primary key, with `get_by_pid` on the
  repository; every route that names a user or an item names its `pid`, and
  every response carries the `pid` and nothing else. Migration
  `0003_public_ids` backfills existing rows. ADR 0014.
- **Administrative routes need an administrator.** Listing users is `list`
  on a `Directory` resource, granted to the admin role; reading another
  account is the owner's or an admin's, not any signed-in caller's.

## v0.1.3 — 2026-09-19

- **The app containers migrate the database on start.** `just dev` ran the
  API against whatever schema the host's `just migrate` had reached, and a
  database it had not reached answered every request with a 500. Both
  containers now run `python -m app.migrate` before their process, and so
  does `just migrate`.
- **Migrations run under Keel's advisory lock.** `app.migrate` calls Keel's
  locked `upgrade`, and the generated `env.py` now honours the connection it
  hands in; before, nothing in a generated project ever took the lock, so two
  replicas starting together could both run the DDL.

## v0.1.2 — 2026-09-19

- **The app containers can fetch Keel.** A project generated from the
  repository depends on Keel as a git source, and the slim image `just dev`
  ran had no git, so `uv sync` in the container failed with "Git executable
  not found". Such a project now uses the full uv image; a checkout-linked
  project keeps the slim one.

## v0.1.1 — 2026-09-19

The first release's last step, `just dev`, failed on a fresh Mac and could
not have run on Windows. A project generated from v0.1.0 gets all of this
with `keel update`.

- **`just dev` runs the application in Docker, on every OS.** It was a bash
  script supervising two host processes, and it needed a bash macOS does not
  ship, so the README's last step failed on a fresh Mac and could not have run
  on Windows. It is `docker compose --profile app up` now, for every shape: the
  app next to Postgres and Redis, from the mounted source, with the API
  reloading on change and each container's virtualenv kept across restarts.
  `just serve` runs the API on the host for a debugger; `just down` stops all
  of it.
- **The generated justfile runs under PowerShell.** Every recipe is a plain
  command, and `windows-shell` is set, so `just` needs no `sh` on Windows.
- **The worker's heartbeat file defaults to the OS temp directory** rather
  than `/tmp`.
- **A project generated from a checkout on Windows installs.** The checkout's
  path is written to `pyproject.toml` as a TOML literal string, with forward
  slashes, so a drive letter and backslashes no longer make it unparseable.

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
