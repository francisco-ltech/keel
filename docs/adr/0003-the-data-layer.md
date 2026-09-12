# ADR 0003 — The data layer

**Status:** accepted · **Date:** 2026-09-12 · **Phase:** 2

## Context

ADR 0002 settled *when* a session is open. This one settles what you do with it.

The problem is arithmetic. The upstream FastAPI template hand-writes `get`,
`get_by_x`, `list`, `create`, `update` and `delete` for every model — roughly
150 lines per table that are ninety percent identical and drift apart the moment
someone fixes a bug in one copy. Ten models is fifteen hundred lines nobody reads
and everybody maintains. Alongside that, the template has no soft deletes, no
audit trail, no factories, no seeding, and pagination that degrades as the table
grows.

Phase 2 is the layer that makes the boilerplate unnecessary rather than merely
shorter.

## Decisions

### 1. Keel owns its repository rather than depending on Advanced-Alchemy

The Phase 1 research recommended Advanced-Alchemy: it ships a repository,
filters, audit mixins and a first-class FastAPI extension, and "compose, don't
build" was the stated posture.

It was evaluated and declined. The reasons, in order of weight:

* **It would become the core of the data layer, not an accessory.** The
  bus-factor argument that is tolerable for SAQ behind a facade is not tolerable
  for the thing every model inherits from.
* **The need is modest.** What we actually use is about 350 lines. Taking a
  dependency to avoid 350 lines of code we fully understand is a poor trade when
  the dependency defines our public API.
* **The parametrised-contract technique transfers instead.** Phase 1's real
  lesson was that a shared test suite makes a small implementation trustworthy.
  That is cheaper to apply than a dependency is to absorb.

This is a reversal of the roadmap's recommendation and is recorded as such.
Advanced-Alchemy remains the right answer for a team that wants filters,
uniqueness helpers and eight-database support out of the box.

### 2. `Repository[ModelT]`, with two boundaries that do not move

Subclasses declare their model and get fifteen methods:

```python
class Users(Repository[User]):
    model = User

    async def by_email(self, email: str) -> User | None:
        return await self.first(User.email == email)
```

**A repository takes a session; it never opens one.** Transaction scope is the
service's decision (ADR 0002). A repository that opened its own transaction
would make it impossible for two repositories to participate in one.

**A repository returns ORM objects, not schemas.** Converting to a wire format
is the service's job. A repository that returned schemas could not be composed —
the caller could no longer modify what it fetched.

`query()` is public so a subclass can build a statement the base class never
anticipated. A repository that forces every query through its own vocabulary
becomes the thing people work around.

Two methods are deliberately blunt about danger. `update()` warns that it
assigns whatever it is given, because a request body passed straight in is a
mass-assignment vulnerability. `purge()` **refuses to run without criteria** — a
forgotten argument should not be able to truncate a table.

### 3. Primary keys are UUIDv7, assigned at construction

Random UUID4 keys scatter B-tree inserts across the index: every write dirties a
different page, the index stops fitting in cache, and rows created together end
up nowhere near each other on disk. That is a permanent cost paid for
unguessability a primary key rarely needs. UUIDv7 keeps the random tail and
prepends a millisecond timestamp, so inserts land at the right-hand edge and
`ORDER BY id` approximates `ORDER BY created_at` without a second index.

Keel ships its own generator with an in-millisecond monotonic counter, falling
back to `uuid.uuid7` on Python 3.14+. The counter is not decoration: without it,
ids created in the same millisecond have no defined order, so "time-ordered"
would fail exactly when a burst of rows is inserted together — which is when
ordering is being relied on.

**Assignment happens in a mapper `init` event, not via a column default.** A
Python-side `default` runs when the INSERT is built, so `Post(title="x").id`
would be `None` until flush, and a service building a graph of rows would have
to flush after each parent just to learn the foreign key for its children. An
event rather than an `__init__` override because a mixin's `__init__` only wins
if it precedes the declarative base in the MRO — which would force every model
in every application to declare its bases in a particular order and fail
confusingly when someone forgot.

Phase 0 documented the opposite behaviour and a Phase 0 test asserted it. Both
were wrong, and are now inverted.

### 4. Soft deletes are a global scope, not a helper you remember

Marking a row deleted is easy. Making every query filter it out is not, and one
forgotten `WHERE deleted_at IS NULL` shows deleted data to a user — the failure
that makes teams abandon soft deletes entirely.

So the filter is applied *for* callers: a `do_orm_execute` listener rewrites
every ORM SELECT touching a `SoftDeleteMixin` model, using `with_loader_criteria`
so it reaches relationship and eager loads too. Filtering `select(Author)` is the
easy half; a deleted book appearing *inside* its author is exactly as wrong, and
is the half hand-written filters always miss.

Three cases are skipped, each for a reason: non-SELECT statements (an explicit
UPDATE should do what it says), column loads (refreshing an object you already
hold should not fail), and statements carrying the opt-out.

`delete()` is soft or hard depending on the model, so call sites get one verb
whose meaning is a property of the model rather than something every caller must
know. `force_delete()` and `find_trashed()` exist for when they must.

**The known sharp edge, documented on the mixin:** a unique constraint plus soft
deletes means deleting a user leaves the row, so re-registering that email
violates the constraint. Make the index partial, or include `deleted_at` in it.

The listener registers on `Session` at import time, which is a process-wide
effect. It is scoped to models inheriting the mixin, and that trade — global
reach for guaranteed application — is the point.

### 5. Pagination is keyset, and carries no total

`LIMIT/OFFSET` degrades two ways, both when the table has grown enough to
matter: it gets slower with depth (the database walks and discards every skipped
row), and it is *incorrect* under concurrent writes — an insert before the
cursor shifts everything down, so the reader sees a duplicate or misses a row.

Keyset asks "give me rows after this one". Constant cost, and inserts elsewhere
cannot shift the window. The trade-off is real: no random access, so you cannot
jump to page 500. That is the right constraint for an API and the wrong one for
an admin table with numbered pages.

`Page` carries no total count. Counting the whole table to render "page 1 of
412" costs a scan on every request, and keyset cannot use the answer anyway.

Cursors are opaque base64 — a cursor that looks like an id invites clients to
construct their own, which breaks the moment the ordering key changes. Decoding
failures raise `InvalidCursorError` and **do not echo the value**, which is
attacker-controlled and ends up in logs.

### 6. Model observers fire after commit, not during flush

Laravel fires `created`/`updated`/`deleted` synchronously inside the write.
Python cannot: SQLAlchemy's mapper events are synchronous, so an observer that
wants to send mail, enqueue a job or call an API has nothing it can do there.

Events are therefore **buffered during flush and dispatched after commit**. Two
consequences, both improvements on the synchronous version: observers can be
async and do real work, and an observer never sees a change that gets rolled
back. Firing "user created" inside a transaction that later fails is how a
welcome email arrives for an account that does not exist.

Soft deletes are *classified*, not reported literally. A soft delete is an
UPDATE in SQL; an observer wants to hear `deleted`, and clearing `deleted_at`
means `restored`. The buffer inspects attribute history to tell the three apart,
so observers do not each reconstruct it from a column diff and get it wrong.

A failing observer does not stop the others — the write is already durable, so
aborting would run some and not others with no way to retry the rest. Failures
go to the `on_observer_error` hook.

**This is the seam Phase 3 needs.** "Dispatch this job after the transaction
commits" is the same mechanism with a different listener.

### 7. Factories do not generate foreign keys, and seeders share them

A generated UUID in an FK column does not fail in the factory — it fails at
flush, in whatever test happened to persist the row, with a Postgres error
naming a column that test never mentioned. Polyfactory's alternative, generating
the *related object*, is worse: it silently inserts a parent nobody asked for, so
unrelated `count() == 1` assertions break. Callers pass `owner_id=owner.id`.

Factories also never generate `id`, `created_at`, `updated_at` or `deleted_at` —
a factory that fills those in produces rows that could never exist.

Seeders run in one transaction, in order, so a partial seed cannot leave a
half-populated database, and they build rows through the same factories the
tests use — a seeder that constructs rows by hand drifts from what tests
exercise. Two non-obvious findings are encoded in the implementation:

* **`run_seeders` flushes between seeders.** Keel's sessions use
  `autoflush=False`, so "B runs after A" does not otherwise imply "B can see A's
  rows".
* **"Seed if empty" counts soft-deleted rows.** Otherwise a fully soft-deleted
  table reads as empty and re-seeding collides with unique constraints on
  invisible rows.

### 8. Migrations take an advisory lock, and drift is a test

N replicas booting together all run `upgrade head`, race, and produce duplicate
objects or a corrupted `alembic_version`. `upgrade()` holds a Postgres
session-level advisory lock, so one migrates and the rest wait and find nothing
to do. The lock is released even when the migration raises.

`check_for_drift()` compares the models against the migration history so "a
model changed and nobody generated a migration" fails the build rather than
surprising someone at deploy.

## Consequences

**Lazy loading raises rather than working slowly.** Under async SQLAlchemy an
unloaded relationship outside a session is an error, not a hidden query. That is
a feature — the N+1 becomes a correctness failure instead of a performance one —
but it means `Repository.eager()` has to be pleasant, and it is why the models in
the test suite use `lazy="raise"`.

**The soft-delete listener is process-wide.** Any session in the process gets
the filter, including ones Keel did not create.

**Observers cannot veto a change.** They run after commit; work that must be
atomic with the write belongs in the service, inside the same unit of work.

**`Any` appears at two boundaries on purpose.** `Repository.model` is
`ClassVar[type[Any]]` because the type parameter is bound to `Model`, which
knows nothing about primary keys; the `id` requirement is checked once in
`__init__` instead. The cache's `remember` helpers are `Any`-typed for a
different reason recorded in ADR 0004.

## A recurring defect worth naming

Twice now, adding a field to a class has broken a subclass that deliberately
bypasses `__init__`: `CacheProxy` in Phase 1, `_SavepointDatabase` in Phase 2.
Both compiled, both type-checked, and both failed at runtime somewhere
unrelated. `_SavepointDatabase` now copies every declared slot in a loop rather
than naming the two or three it happens to need, and `CacheProxy` has a test
asserting it overrides every property `Repository` exposes.

The pattern, not the instance, was the problem. Any class that skips its
parent's constructor needs a mechanical guarantee, not a careful author.

## What was deliberately not done

* **Audit columns** (`created_by`, `updated_by`). They need a current-user
  context, which is Phase 4. A column that is always `NULL` is worse than none.
* **Tagged cache invalidation on model events**, a query DSL, filter objects, and
  a `Repository` that generates endpoints. Each is a real feature with a real
  cost; none has a caller yet.
* **Multi-tenancy.** The soft-delete listener demonstrates that a global scope is
  achievable, which is the mechanism `tenant_id` would use. Building it before a
  tenant exists would be guessing.
* **A `migrated_database` test helper.** The generated template still uses
  `create_all` alongside Alembic — two sources of truth, currently covered by the
  drift check. Worth closing when something depends on it.

## Verification

Behaviour is pinned against a real Postgres, not asserted:

- `test_the_soft_delete_filter_reaches_relationship_loads` — the half that
  hand-written filters miss.
- `test_paginating_walks_every_row_exactly_once` — 25 rows over 4 pages, union
  equals the input, no duplicates and no holes.
- `test_a_rolled_back_change_reaches_no_observer` — the promise observers exist
  for.
- `test_a_soft_delete_is_reported_as_deleted_not_updated` — the classification.
- `test_purge_without_criteria_refuses` — a missing argument cannot truncate.
- `test_concurrent_emulated_increments_all_land` and the migration lock's
  concurrency test — the two places a lock is load-bearing.
- `test_the_primary_key_is_available_before_the_flush` — the Phase 0 correction.
