# Database

Keel's database subsystem is async SQLAlchemy with the decisions already made. It gives
you a declarative base with a naming convention, time-ordered primary keys, a generic
repository, soft deletes as a global scope, keyset pagination, after-commit hooks and a
locked `alembic upgrade`. Without it, every service rewrites those pieces and gets one of
them subtly wrong.

## The one rule

A session is never held across a request. There is no `get_session` dependency, and there
must not be one. A route receives a service; the service opens a unit of work around the
atomic work and returns a schema, not an ORM object.

```python
from keel.database import uow


async def publish(post_id: UUID) -> PostRead:
    async with uow() as session:
        post = await Posts(session).get_or_fail(post_id)
        post.published_at = utcnow()
        return PostRead.model_validate(post)
```

The block commits on a clean exit, rolls back on an exception and closes the session
either way. Keep it around the work that must be atomic, not the whole request. The
reason, including the FastAPI deadlock it avoids, is in
[ADR 0002](adr/0002-the-unit-of-work.md).

## Wiring

One context manager binds the database for the life of the process, in a FastAPI
lifespan, a worker or a script alike.

```python
from keel.database import DatabaseConfig, database_lifespan

async with database_lifespan(DatabaseConfig.from_env()):
    ...
```

`from_env` reads `DATABASE_URL` and every knob under `DB_*`. The URL must name an async
driver, such as `postgresql+asyncpg://`.

| Variable | Default | What it does |
|---|---|---|
| `DATABASE_URL` | required | Async SQLAlchemy URL |
| `DB_ECHO` | `false` | Log every statement |
| `DB_POOL_SIZE` | `5` | Connections kept open per process |
| `DB_MAX_OVERFLOW` | `10` | Extra connections allowed under burst |
| `DB_POOL_TIMEOUT` | `30` | Seconds to wait for a free connection |
| `DB_POOL_RECYCLE` | `1800` | Seconds before an idle connection is replaced |
| `DB_POOL_PRE_PING` | `true` | Check a connection is alive on checkout |
| `DB_STATEMENT_TIMEOUT` | `30` | Server-side cap per statement; `none` to lift it |
| `DB_CONNECT_TIMEOUT` | `10` | Seconds to wait when opening a connection |

## Models

Every table inherits `Model`, which carries `NAMING_CONVENTION` so Alembic produces the
same constraint names on every backend. Mix in what the table needs.

```python
from keel.database import Model, PublicId, SoftDeleteMixin, TimestampMixin, UUIDPrimaryKey


class Post(Model, UUIDPrimaryKey, PublicId, TimestampMixin, SoftDeleteMixin):
    __tablename__ = "posts"
    title: Mapped[str]
```

`UUIDPrimaryKey` assigns a UUIDv7 `id` at construction, so `Post(title="x").id` is set
before any flush. `PublicId` adds a random `pid`, the only identifier a URL or response
carries ([ADR 0014](adr/0014-public-identifiers.md)). `TimestampMixin` adds `created_at`
and `updated_at`. `SoftDeleteMixin` adds `deleted_at` and hides deleted rows from every
query.

## Repositories and the unit of work

A repository takes a session and never opens or commits one. Subclass `Repository[T]`
and declare the model; the common methods come for free.

```python
from keel.database import Repository


class Posts(Repository[Post]):
    model = Post
```

`get(id)` returns the row or `None`; `get_or_fail(id)` raises `RecordNotFoundError`
instead. `get_by_pid(pid)` is the lookup a route makes. `create(**values)` builds, adds
and flushes, so a constraint failure points at the call that caused it. `update`,
`delete`, `restore`, `count` and `exists` follow the same shape.

`list(*criteria)` is unbounded and ordered by primary key. Anything reaching an HTTP
response should use `paginate(*criteria, cursor=None, limit=None)`, which is keyset and
carries no total count. It returns a `Page` with `items`, `next_cursor` and `has_more`,
and clamps the size to `MAX_PAGE_SIZE`.

Soft-deleted rows are filtered out of every `SELECT` automatically, including relationship
loads. Opting out is explicit: wrap a statement in `with_deleted(...)`, or use the
repository's `with_trashed` and `find_trashed`. `only_deleted_option()` narrows a query to
the recycle bin; combine it with `with_deleted`.

## After the commit

`after_commit(callback)` defers a coroutine function until the surrounding unit of work
commits. It returns `False` outside a transaction, so the caller can do the work at once.
The queue's `dispatch()` uses this to hold a job until the row it names exists.

```python
from keel.database.hooks import after_commit

async with uow() as session:
    invoice = await Invoices(session).create(total=total)
    after_commit(lambda: notify(invoice.id))
```

An `Observer[T]` reacts to `created`, `updated`, `deleted` and `restored` on one model.
Register it with `observe(Post, PostObserver())`, which returns an unsubscribe function. A
soft delete reports as `deleted`, and clearing `deleted_at` reports as `restored`.

A failing observer or callback cannot roll anything back and does not stop the others. It
is logged with its traceback on `keel.database`. Pass `on_observer_error` or
`on_deferred_error` to `Database(...)` to route it elsewhere.

## Migrations

`keel.database.migrations.upgrade(database, config)` runs `alembic upgrade head` under a
session-level Postgres advisory lock. Any number of replicas can call it on start; one
migrates, the rest wait and find nothing to do. It lifts `statement_timeout` for the
migration, so an index build is not killed by the request-sized cap. `config` is an
`AlembicConfig(script_location=..., ini_path=...)`.

`assert_no_drift(database, config)` fails a test when a model changed and nobody generated
a migration.

## In tests

There is no fake database. A cache or a queue is worth faking because the real one is
remote or irreversible; a database's queries are the part most likely to be wrong, so a
fake would stop testing them ([ADR 0003](adr/0003-the-data-layer.md)).

`keel.testing.rolled_back_database(database)` binds a database whose sessions join one
outer transaction and rolls it back at the end. Code under test calls `uow()` and commits
normally; the commit releases a savepoint. Every row written in one test shares a
`created_at`, because `now()` is the transaction timestamp.

`keel.database.factories.ModelFactory` builds valid rows and leaves `id`, the timestamps,
`deleted_at` and foreign keys alone. `keel.database.seeding.run_seeders` runs seeders in
order inside one transaction, and `SeedIfEmpty` makes a re-run a no-op.

## In the template

`app/main.py` and `app/worker.py` open `database_lifespan(settings.database_config())`.
`app/modules/items/models.py` is the `Item` table; `repository.py` takes a session and
returns rows; `service.py` opens `uow()` and returns schemas. `python -m app.migrate`
runs `upgrade` and is what `docker-compose.yml` runs before the API and the worker.
`tests/conftest.py` wraps every test in `rolled_back_database`.

## Limits

- No fake database, and no SQLite substitute in tests. [ADR 0003](adr/0003-the-data-layer.md)
- No audit columns, no multi-tenancy, no query DSL or filter objects. [ADR 0003](adr/0003-the-data-layer.md)
- No read replicas or read/write splitting. [ADR 0002](adr/0002-the-unit-of-work.md)
- No slugs or hashids as public identifiers; a `pid` is a UUID4. [ADR 0014](adr/0014-public-identifiers.md)

## Further reading

- [ADR 0002 — the unit of work](adr/0002-the-unit-of-work.md)
- [ADR 0003 — the data layer](adr/0003-the-data-layer.md)
- [ADR 0014 — public identifiers](adr/0014-public-identifiers.md)
