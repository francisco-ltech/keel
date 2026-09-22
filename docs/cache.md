# Cache

A key-value cache behind one import, `from keel.cache import cache`. The same
calls run against Redis, an in-process store, or nothing at all, and a test can
swap in a recording fake without touching the code under test. Without it,
every service that caches grows its own Redis client, its own key format, and
its own untestable global.

## Wiring

Bind the cache once, for the life of the process, inside whatever lifespan your
framework offers.

```python
from keel.cache import CacheConfig, cache_lifespan

async with cache_lifespan(CacheConfig.from_env()):
    ...
```

`CacheConfig.from_env()` builds one store named `default` from these variables.

| Variable | Default | Meaning |
|---|---|---|
| `CACHE_STORE` | `array` | Driver: `redis`, `array` or `null`. |
| `CACHE_PREFIX` | `keel:cache` | Key namespace. `flush()` deletes everything under it, and nothing outside it. |
| `CACHE_TTL` | `300` | Seconds a value lives when a call names no TTL. `none` means forever. |
| `CACHE_SERIALIZER` | `json` | `json`, or `pickle` for arbitrary objects from trusted writers only. |
| `REDIS_URL` | unset | Connection string. Required by the `redis` driver, ignored by the others. |

`redis` is for anything deployed: several processes share it, and it survives a
restart. `array` is for one process on one event loop, so tests and local
development. `null` retains nothing, which turns caching off through
configuration instead of an `if` at every call site.

Keep `CACHE_PREFIX` a sibling of the queue's and the token store's prefixes,
never a parent. They share one Redis, and a flush scans the whole prefix.

## Using it

Read, write and remove. A missing key returns the default you pass, or `None`.

```python
from keel.cache import cache

await cache.put("user:42", payload, ttl=60)
user = await cache.get("user:42")
await cache.forget("user:42")
```

`remember` reads the key and, on a miss, runs the callback and stores what it
returns. The callback may be sync or async.

```python
profile = await cache.remember("profile:42", lambda: load_profile(42), ttl=300)
```

With `single_flight=True`, concurrent misses queue behind a lock and the
callback runs once. Use it when the callback is a slow query or an upstream
call. Leave it off when the callback is cheap, because the lock costs a round
trip.

```python
report = await cache.remember("report:daily", build_report, single_flight=True)
```

Counters are atomic on every driver, and a key that does not exist starts at
zero.

```python
hits = await cache.increment("hits:/items")
await cache.decrement("seats:event:7", by=2)
```

Locks are cache entries with an owner token, so a holder that dies releases
after `ttl`, and a holder that lost the lock cannot release someone else's.
`async with` fails at once if the lock is taken. `block()` waits for it. Both
raise `keel.exceptions.LockTimeoutError` when they give up.

```python
async with cache.lock("import", ttl=30):
    await run_import()

lock = cache.lock("import", ttl=30)
await lock.block(timeout=5)
try:
    await run_import()
finally:
    await lock.release()
```

A second store is `cache.of("sessions")`, once it is configured. Omitting `ttl`
takes the store's default. Passing `ttl=None` stores forever.

## In tests

`fake_cache()` replaces the bound cache with a recording fake for one block. The
fake delegates to a real in-memory store, so TTLs and serialisation are real.
The override is context-local, so parallel tests do not see each other's cache.

```python
from keel.testing import fake_cache


async def test_profile_is_cached() -> None:
    with fake_cache() as cached:
        await refresh_profile(42)
        cached.assert_missed("profile:42")
        cached.assert_put("profile:42", ttl=300.0)
```

The assertions are `assert_hit`, `assert_missed`, `assert_put`,
`assert_not_put`, `assert_forgotten`, `assert_flushed`,
`assert_nothing_written` and `assert_operation_count`. A failure prints the
whole recorded timeline. `cached.operations` is the raw list.

## In the template

`app/settings.py` reads `CACHE_*` and `REDIS_URL` into `CacheSettings` and
`Settings.cache_config()` turns that into a `CacheConfig`. `app/main.py` and
`app/worker.py` each call `cache_lifespan` with it, sharing the event dispatcher
the inspector and metrics read from. `app/modules/items/jobs.py` is the worked
example: the `IndexItem` job renders an item's card and writes it with
`cache.put` under a key built by `card_key()`. `tests/test_jobs.py` reads it
back through the same facade.

## Limits

- No tagged invalidation, and no `flexible()` stale-while-revalidate. Declined
  in [ADR 0001](adr/0001-the-cache-seam.md); the contract can carry them later.
- No stampede protection beyond `single_flight`. Same ADR.
- `pull()` is not atomic. Two readers can both see the value. Single-consumer
  handoff is a queue's job, [ADR 0006](adr/0006-the-queue.md).
- `null` gives locks that always grant. A disabled cache must not deadlock the
  application, so it provides no mutual exclusion either. It is the Null Object
  exception in [ADR 0001](adr/0001-the-cache-seam.md).
- Events come from the store, not the repository, so an observer sees
  primitives and not a `remember` miss. Settled in [ADR 0011](adr/0011-the-request-inspector.md).

## Further reading

- [ADR 0001 — the cache seam](adr/0001-the-cache-seam.md): the facade, driver
  and fake shape, what a review changed, and what did not transfer.
- [ADR 0011 — the request inspector](adr/0011-the-request-inspector.md): why
  cache events stay on the store.
- [ADR 0012 — metrics](adr/0012-metrics.md): the `keel_cache_operations_total`
  counter over the same events.
