# ADR 0001 — The cache seam

**Status:** accepted · **Date:** 2026-09-11 · **Phase:** 1 (spike)

## Context

Keel exists to give FastAPI applications the batteries Laravel ships with. The
assessment that preceded this work concluded that the gap between the two is not
a feature list — it is one structural idea. Every Laravel subsystem is reached
through the same seam: a facade the application calls, a swappable driver behind
it, and a fake that records what happened so tests can assert on it. Ten
subsystems, one shape. That is why forty features feel like one framework rather
than forty libraries.

So the first thing to build is not "a cache". It is the seam, proved on exactly
one subsystem. If the seam is right, the queue, mailer, storage and notification
subsystems are each a driver plus a fake. If it is wrong, better to find out in
a day than in month two.

The cache is the right subsystem to prove it on because it is small enough to
finish and awkward enough to be honest: it has real atomicity requirements, a
genuine null-value problem, expiry semantics, and a distributed-locking primitive
that is easy to get subtly wrong.

## Decision

Four collaborating pieces, in dependency order.

```
        cache  ──────────────────►  CacheProxy      (virtual proxy)
                                         │
        CacheManager ────────────►  Repository      (bridge: abstraction)
             │                           │
        StoreConfig                      ▼
                                       Store        (bridge: implementor)
                                         ▲
                    ┌────────────────────┼────────────────────┐
                ArrayStore          RedisStore            NullStore
                    ▲                    ▲
                EventfulStore        FakeStore           (decorators)
```

### 1. `Store` and `Repository` are split (Bridge)

`Store` holds only operations that need the backend's own guarantees: `get`,
`put`, `add`, `increment`, `forget`, `forget_if`, `flush`, `many`, `put_many`,
`lock`. Everything else — `remember`, `pull`, `has`, `forever`, `decrement`,
TTL normalisation — lives on `Repository` and is written in terms of those
primitives.

The payoff runs in both directions, and the second one matters more:

- Adding a convenience method costs nothing per driver.
- **Adding a driver costs nothing per convenience method.** A new driver cannot
  get `remember` subtly wrong, because a new driver does not implement
  `remember`.

The rule for the contract is therefore: *if it can be derived from the
primitives, it does not belong in the contract.* Every method on `Store` earns
its place by needing something only a backend can do atomically.

### 2. Drivers are interchangeable and a shared suite proves it (Strategy + LSP)

`tests/test_store_contract.py` runs every assertion against `ArrayStore`,
`RedisStore`, `FakeStore` and `EventfulStore`. A driver is not "a cache" because
it has the right method names; it is a cache because it behaves identically
under the same assertions. 45 contract tests × 4 drivers is where most of the
suite's value sits.

`ArrayStore` serialises through the same `Serializer` as `RedisStore` rather than
holding live objects. It costs an encode/decode in a store that never leaves the
process, and it buys dev/prod parity: a value that cannot survive the cache in
production cannot survive it in a test either.

**`NullStore` is deliberately excluded from that parametrisation.** It satisfies
the interface but not the behaviour — a store that retains nothing cannot satisfy
"put then get returns the value". Null Object implementations are the standard
exception to Liskov substitutability. Weakening the shared contract to admit it
would have cost every other driver a real guarantee, so it gets its own tests
instead.

### 3. Instrumentation and test-recording are decorators, not flags

`EventfulStore` and `FakeStore` both implement `Store` and wrap another `Store`.
Nothing above them knows they are there.

This is what keeps the drivers honest. `RedisStore` knows about Redis. It should
not also know about observability, and adding a third concern to it starts the
slide toward a class that knows about everything.

`FakeStore` being a decorator over a *real* store — rather than a hand-written
imitation — means a test exercises real TTL handling, real serialisation and real
atomicity. It also means the same assertions work against live Redis in an
integration test, where a mock would have to be thrown away.

### 4. The facade is a virtual proxy over `Repository`, not `__getattr__` magic

`CacheProxy` subclasses `Repository` and overrides exactly two properties:
`store` and `default_ttl`. Because every inherited method reads its state through
those properties rather than the attributes behind them, all twenty delegate
correctly to whatever is currently bound — with no forwarding code and no loss of
type information.

The alternatives were both worse. Explicit delegation means duplicating twenty
signatures and re-duplicating them on every change. `__getattr__` forwarding
means the whole public API types as `Any`, which in a library whose selling point
is strictness is close to self-defeating.

**The invariant this creates, and it has already been broken once.** Adding
state to `Repository` requires adding a property for it *and* overriding that
property on the proxy. Reading `self._something` directly inside a method type-
checks fine and then raises `AttributeError` the first time the call arrives
through the facade. That happened during this phase: three single-flight lock
parameters were added to `Repository.__init__`, and `remember(single_flight=True)`
broke for every caller going through `cache`. Nothing in the unit tests caught
it — they construct `Repository` directly. The FastAPI integration test did,
which is the argument for having one.

The cost is real and worth naming: this design trades a compile-time duplication
problem for a runtime invariant a reviewer has to hold in their head. It is the
right trade at twenty methods and three properties. It would not be at a hundred.

Binding is two-layer, and the layers exist for different reasons:

- **Process-wide default** — a plain module global, set at startup. Not a
  `ContextVar`, because Starlette runs an application's lifespan in a different
  task from its request handlers, and a context variable set in one is not
  visible in the other. This is a real bug that looks like "the cache is
  unbound in production but fine in tests".
- **Context-local override** — a `ContextVar`, for tests and scoped swaps, so
  concurrent tests cannot see each other's cache and teardown is automatic.

### 5. Two extension points, because there are two questions

`Manager.extend(name, factory)` replaces one *configured store* wholesale. That
is how `fake_cache()` installs the recording fake: the code under test asks for
the default store and must get that exact instance.

`CacheManager.register_driver(driver, factory)` adds a new *driver type*, so any
number of stores can then be configured with `driver="memcached"`.

Conflating them would have meant either "you can't install a fake" or "adding a
driver requires editing `CacheManager`". `_make_store` consults the registry
before its own match statement, so it is genuinely closed for modification.

## Consequences worth stating plainly

**`forget_if` is in the contract because of locks.** Safe lock release needs
atomic compare-and-delete; a get-then-forget pair always has a gap, and the gap
is exactly the window in which an expired holder deletes its successor's lock.
Only a backend can close it — Redis with a Lua script, `ArrayStore` under its
mutex. Putting it in the contract meant one `StoreLock` works correctly on every
driver with no per-driver lock subclass.

**The owner token is not decoration.** `test_releasing_a_lock_you_lost_does_nothing`
encodes the defect it prevents, and that test runs against real Redis.

**Locks are delegated undecorated.** A lock's reads and writes are coordination,
not caching. Surfacing them as cache hits would make an observer's timeline
misleading.

**`MISSING` is an enum member, not `object()`.** Type checkers can narrow an enum
member in a union. `is_missing` uses `TypeIs` rather than `TypeGuard` so the
`else` branch narrows too — with `TypeGuard` the negative branch still saw
`T | Sentinel` and strict mypy rejected every caller.

**`UNSET` is a second sentinel, and the distinction is load-bearing.**
`ttl=None` means *store forever*; omitting `ttl` means *use the configured
default*. Collapsing them makes the default unreachable without repeating it at
every call site.

**Keel takes no configuration-library dependency.** `CacheConfig` is a plain
frozen dataclass; the application decides whether those values come from
pydantic-settings, YAML or a fixture. `from_env()` exists so the convenience is
there without the coupling.

## What was deliberately not done

- **No Singleton.** Lifetimes belong to the manager and the lifespan context, not
  to classes deciding to be unique. `CacheManager` memoises instances; that is
  caching, not Singleton, and it can be reset.
- **No service locator in application code.** The facade is a convenience at the
  edges. Services should take what they need as arguments; `cache` exists so a
  route or a job does not have to thread a manager through four constructors.
- **No general DI container.** That is Phase 0/1-proper work. Building half of
  one here would have produced something to throw away.
- **No tags, no `flexible()`, no stampede protection beyond `single_flight`.**
  Tagged invalidation is a real feature with a real cost; it can be added behind
  the existing contract when something needs it.
- **No abstract base classes for stores.** `Store` is a `Protocol`. Drivers do
  not inherit from Keel, which means a third-party store needs no dependency on
  us to satisfy the contract.

## Added after review

An adversarial review found five defects that changed the design, not just the
code. Recording them because the reasoning generalises to the next subsystem.

**A shared backend is never owned wholesale.** `RedisStore` defaulted to an
empty namespace, whose `SCAN MATCH` pattern is `*` — so `flush()` on a
default-configured store unlinked every key on the server, other applications
included, while the module docstring promised the opposite. Two rules now:
the safe default is a real namespace (`keel`), and an *explicitly* empty one is
refused rather than silently corrected. Any future driver that shares a backend
inherits this rule.

**A namespace is a literal; a pattern is syntax.** The prefix was interpolated
into the glob unescaped, so `KeyNamespace("app[1]")` matched — and flushed —
`app1:*`. Escaping happens at the boundary between the two.

**`async with lock` fails on contention; it does not wait for it.** The emulated
increment used it, so twenty concurrent increments produced one success and
nineteen `LockTimeoutError`s — and the docstring claimed they were serialised.
Any read-modify-write under a lock must `block()`, and any docstring claiming a
guarantee needs a concurrent test proving it. Both now exist.

**The most constrained backend sets the contract.** Redis counters are 64-bit;
Python integers are not. The in-memory store happily returned values Redis
cannot produce, which meant a test could pass and the same code fail in
production. `guard_overflow` now applies Redis's range to every driver. The
principle matters more than the case: a contract shared across drivers is only
as strong as its narrowest implementation, and pretending otherwise moves
failures from the test suite to production.

**`runtime_checkable` was removed from `Store` and `Lock`.** `isinstance`
against a protocol checks attribute *names* only — a class with eleven
synchronous methods and entirely wrong signatures passes — so it offered a
third-party driver author false assurance. The conformance mechanism is the
parametrised contract suite, and that should eventually ship as something
external drivers can run.

One correction the review made to this document's own claims: `ArrayStore`'s
`asyncio.Lock` does not give it "the same atomicity guarantees the contract
requires of Redis". No method in that store awaits anything that suspends, so
each is already indivisible within one event loop, and an `asyncio.Lock` gives
no protection across threads at all. The mutex is insurance against a future
edit introducing a suspension point inside a read-modify-write. The store
belongs to one event loop, and that is now what it says.

## Status of the bet

The seam holds for one subsystem, which is not the same as holding.

The original claim here was that Phase 3 would be "a driver plus a fake". Review
pressure-tested that and it does not survive. The cache seam is *caller-push,
key-addressed, value-returning*; a queue is *worker-pull, payload-carrying,
effect-producing*. Concretely:

- `Store`'s vocabulary is `(key, value, ttl)`. A job is `(payload, queue, delay,
  attempts, max_attempts, backoff, reserved_until)`. There is no key, and
  nothing to return.
- The Bridge earns its place here because `remember` is a substantial
  convenience derived from primitives. A queue's abstraction half is nearly
  empty — `push`, `later`, `bulk` — so the same split would be ceremony.
- The worker loop (reserve → run → ack/retry/dead-letter) has no analogue. It
  needs a lease with a visibility timeout, which is not what `StoreLock` is.
- `FakeStore` works as a Decorator over a real store because a cache is cheap to
  have for real. A fake queue that *records* dispatches and one that *runs* them
  are different objects, and both are wanted.
- `Manager[T]` generalises as a memoising factory, but `T = Repository` bakes in
  "one instance per name, built once, closed at shutdown". A queue needs a
  connection per worker. The lifecycle hook added during review (`_close_instance`,
  `discard`, `close_all`) is the beginning of fixing that, not the end.

**The honest claim is narrower and still worth having.** What transfers to every
subsequent subsystem: `KeyNamespace`, `Serializer`, `Sentinel`, `EventDispatcher`,
the facade/`ContextVar` binding, `cache_lifespan`'s shape, `Manager[T]`'s
memoisation and lifecycle, and — most valuable — the parametrised contract suite
*as a technique*. What does not transfer is the `Store`/`Repository` split
itself, which is right for a cache and should be re-derived rather than copied.

Phase 3 should therefore design the queue's contract from the queue's problem,
and reach back for these pieces, rather than starting from this file.

## Open question, deliberately not settled

*Settled by [ADR 0011](0011-the-request-inspector.md), decision 2: they stay on
the `Store`. The paragraphs below are kept as the question was asked.*

**Should events be emitted from the `Store` or the `Repository`?** They are
emitted from the `Store` today, via `EventfulStore`. The argument for moving
them up: the decorator sees only primitives, so it cannot report that
`remember("profile:42")` was a miss that triggered a single-flight recompute —
which is the thing an inspector most wants to show. It also cannot know the
normalised TTL, and it emits N events for one `put_many` round trip. Laravel
emits from its repository, not its store.

The argument for leaving it: a store-level decorator observes *all* access,
including code that bypasses the repository, and composes with any driver.

This is not settled because Phase 5 — the request inspector — is the thing that
will actually have an opinion, and guessing now risks building the wrong one
twice. The defects this layering caused (an increment reporting a TTL it could
not know; an evicting `put` reporting nothing) have been fixed in place.

## References

- Ashish Pratap Singh, *Every Important Design Pattern Explained in 18 Minutes* —
  the pattern vocabulary used here.
- Laravel's `Illuminate\Cache` — the manager/repository/store split this follows.
- FastAPI PR #12066 (open since 2024-08-24) — why Keel will not hold resources
  across a request via `Depends`. Not load-bearing for the cache, but it is the
  reason the data layer in Phase 2 will look the way it does.
