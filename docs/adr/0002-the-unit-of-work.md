# ADR 0002 — Sessions are never held across a request

**Status:** accepted · **Date:** 2026-09-11 · **Phase:** 0

## Context

The conventional FastAPI database idiom is a dependency that yields a session:

```python
async def get_session() -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.get("/items/{item_id}")
async def read_item(item_id: UUID, session: SessionDep) -> ItemRead: ...
```

The upstream `full-stack-fastapi-template` ships exactly this. It is in most
tutorials. It is also the shape with an open, unfixed deadlock in FastAPI
itself.

**[FastAPI PR #12066](https://github.com/fastapi/fastapi/pull/12066)** — *"Fix
deadlock that can occur when closing dependencies during response model
validation"* — was opened on **2024-08-24** and was still open when this was
written, last touched 2026-07-23, with 26 comments. Those comments include a
production outage and a report that removing `response_model` makes the problem
go away. The mechanism: a dependency with cleanup is held open *through*
response-model validation, so the connection is not returned until after
serialisation.

Two things make it worse than it sounds.

The failure is invisible in the obvious metrics. Requests are still "served"
quickly, so p99 latency looks healthy while the pool starves and throughput
collapses. You find it by running out of connections, not by watching a
dashboard.

And it compounds with Starlette's threadpool. Sync `def` handlers and sync
dependencies share a fixed pool of 40 threads. An application that holds a
connection per in-flight request and runs handlers in that threadpool has two
independent exhaustion limits, and neither announces itself.

## Decision

**A session is scoped to the work that needs a transaction, never to a request.**

Keel ships no session dependency. There is nothing named `get_session` to
import, and that absence is the design: the safe path has to be the only path
on offer, because the unsafe one is what every tutorial shows.

The idiom is a unit of work opened inside the service:

```python
# app/modules/posts/service.py
async def publish(post_id: UUID) -> PostRead:
    async with uow() as session:
        post = await Posts(session).find_or_fail(post_id)
        post.published_at = utcnow()
        result = PostRead.model_validate(post)
    return result  # connection already back in the pool
```

Three rules fall out of it, and they are what the starter template enforces by
example:

1. **Routes receive services, not sessions.** A handler's signature describes
   what the *endpoint* takes, not what its dependencies happen to need.
2. **Services return schemas, not ORM objects.** Returning a detached instance
   after the session closes is the trap this layout exists to avoid — it either
   raises on lazy load or silently triggers a query outside a transaction.
3. **The block spans the atomic work, not the handler.** Two writes that must
   land together share one `uow()`. Anything that does not need the database —
   serialisation, an HTTP call, rendering — happens outside it.

Alongside it, `statement_timeout` is set per connection by default (30s). A
connection held by a pathological query is a connection the pool has lost, so
the timeout is part of the same concern rather than a separate tuning knob.

## Consequences

**You cannot lazy-load after the block.** This is the cost, and it is real:
relationships must be eager-loaded inside the unit of work or accessed inside
it. It is also the point — implicit lazy loads are how an endpoint quietly
becomes N+1 queries, and in async SQLAlchemy they raise rather than working
slowly, so the design surfaces the problem instead of deferring it.

**Two transactions per request is normal, not a smell.** A handler that reads,
calls an upstream API, then writes should use two units of work with the network
call between them, rather than holding a connection across it.

**There is no database fake, deliberately.** Every other Keel subsystem ships
one. A cache, a queue and a mailer are worth faking because the real thing is
remote, slow or irreversible, and because what the test wants to know is what
the code *did* to it. A database's test double is a real database inside a
transaction that gets rolled back: `keel.testing.rolled_back_database` binds a
session factory with `join_transaction_mode="create_savepoint"`, so code under
test can call `commit()` normally and the outer transaction still owns the undo.
Faking the database means not testing the queries, which are the part most
likely to be wrong.

Knowing which subsystems deserve a fake and which deserve a real instance with
an undo is most of what makes a suite trustworthy, and it is the first place
the "every battery gets a fake" rule from ADR 0001 correctly does not apply.

**Migrations get deterministic names.** `Model.metadata` carries a naming
convention. Without one, Alembic autogenerates constraint names chosen by the
backend, so the same schema produces different scripts on different databases
and a downgrade cannot find what it is dropping. It must be set before the first
table exists; retrofitting means a migration that renames every constraint in
the schema.

## What this does not solve

**Dispatch-after-commit.** A job enqueued inside a transaction that later rolls
back is a job that runs against data which never existed. The `uow()` boundary
is the right place to hook `after_commit`, and Phase 3 will do it. It is not
built now because a hook with no queue behind it is speculation.

**Read replicas, and per-request read/write splitting.** Nothing here prevents
them; nothing here provides them.

**The N+1 query itself.** The design makes lazy loading fail loudly instead of
silently. Eager-loading ergonomics are Phase 2's repository work.

## Verification

The rule is only as good as its evidence, so the behaviour is pinned against a
real Postgres rather than asserted:

- `test_a_failing_block_is_rolled_back` — the transaction boundary.
- `test_a_statement_timeout_is_applied_to_every_connection` — the pool guard.
- `test_code_under_test_can_commit_normally` — the savepoint trick, which is
  what lets application code stay ignorant of the test harness.
- `test_ping_raises_on_an_unreachable_database` — a readiness probe that cannot
  fail is worse than none, because the orchestrator keeps routing to a process
  that cannot serve.
