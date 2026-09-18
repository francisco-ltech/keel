# Keel — working notes for agents

Batteries for backend services. A core library (`src/keel`) plus a copier
starter template (`template/`). A service is one bounded domain in one codebase,
owning one schema, deployed as an API, a worker, or both from the same image.

## Skills

`.claude/skills/` holds three, kept in the repo so a change and the skill
describing it move in the same commit:

- `keel-subsystem` — the shape a subsystem takes, and what transfers between them
- `keel-review` — the adversarial review brief, and what it has caught before
- `keel-template` — changing `template/` safely, and the traps already hit

`.claude/agents/keel-reviewer.md` is the QA half of the loop. Non-trivial work
gets built by one agent and reviewed by that one — read-only, adversarial, and
never the agent that wrote the code. On the token store the author reported all
gates green and the reviewer found six defects, two of them critical.

## Read before designing anything

`docs/adr/0000-design-patterns-are-the-bar.md` is the standing rule and is not
optional. Name the pattern you use, justify what it buys in one sentence, and
record what you declined. "This should just be a function" is a valid finding.

`docs/roadmap.md` is what the phase numbers in the ADRs refer to.

The other ADRs carry the reasoning per subsystem. Link to them; never restate
them, or the copy drifts and starts lying.

## Commands

`just` — not make. `just` alone lists everything.

| | |
|---|---|
| `just quick` | lint + both type checkers, ~1s. What pre-commit runs. |
| `just test` | suite in parallel, ~16s |
| `just test-serial` | one process, for tracebacks and debuggers |
| `just check` | quick + test. What pre-push runs. |
| `just test-generator` | generates the template in 3 shapes, ~70s |
| `just doctor` | whether Postgres and Redis actually answer |

Postgres is on **5433**, Redis on **6380**, to avoid clashing with anything on
the default ports. `just up` starts them.

## Rules that bite if ignored

**Sessions are never held across a request.** There is no `get_session`
dependency and there must not be one — FastAPI PR #12066. Routes get services;
services open `async with uow() as session:` around the atomic work and return
schemas, not ORM objects. ADR 0002.

**Dispatch waits for the commit.** `dispatch()` inside a unit of work defers
until it commits. Do not "fix" this by pushing immediately. ADR 0006.

**Every subsystem is a facade over a swappable driver with a fake.** Drivers are
held to one parametrised contract suite. A Null-Object driver is the documented
exception and is excluded from it rather than allowed to weaken it.

**Two type checkers.** ty for speed, mypy as the gate, both must pass. ty does
not honour mypy's error codes, so a suppression needs both comments:
`# type: ignore[arg-type]  # ty: ignore[invalid-argument-type]`. ADR 0004.

**Warnings are errors.** One narrow exception for a third-party deprecation.

**Environment names carry no `KEEL_` prefix.** `DATABASE_URL`, `REDIS_URL`,
`DB_*`, `CACHE_*`, `QUEUE_*`. They name your infrastructure, not the framework.

## Writing style

**Inline comments: two lines maximum.** Clear and brief. If the point needs more
room it belongs in the docstring, an ADR, or the README — not in a longer
comment. A file-header banner in a config file (`.env.example`, `alembic.ini`)
is that file's docstring and is exempt; comments in the body are not.

**Docstrings explain why, not what.** "Manages the cache" is a failure. Say what
the thing buys and what breaks without it. Google convention, enforced by ruff.

**Commit messages carry no attribution trailers.**

## Testing

Tests run in parallel; each xdist worker gets its own database via the
`database_url` fixture. A test module that creates tables must create and drop
its own, and must leave the shared database empty.

Service-backed tests **skip** when Postgres or Redis is unreachable rather than
failing. That keeps the suite runnable without Docker, and it means a green run
with the services down proves much less than it looks like. `just doctor`.

## Layout

```
src/keel/
  cache/      facade, drivers, locks, fake
  database/   engine, uow, repository, soft delete, observers, migrations
  queue/      jobs, dispatch, drivers, worker, failed jobs, scheduler
  observability/  logging, correlation re-exports, readiness checks, the request inspector, metrics
  contracts/  the protocols drivers implement
  support/    binding, manager, events, keys, serialization — subsystem-agnostic
template/     copier template; three service shapes
docs/adr/     why things are the way they are
```

`keel.queue` exports the worker, SAQ driver, failed jobs and scheduler
**lazily**, so a dispatch-only process does not import a worker runtime. Keep
that: adding an eager import at the top of `keel/queue/__init__.py` undoes it.
