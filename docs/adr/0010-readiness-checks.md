# ADR 0010 — Readiness checks

**Status:** accepted · **Date:** 2026-09-17 · **Phase:** 5 (slice two)

## Context

The template already answered two probes. `/health` touched nothing, which is
right. `/ready` called `Database.healthy()`, and was wrong in three ways:

- **It asked one dependency of four.** Tokens, the cache and the queue are all in
  Redis. With Redis down the API reported ready while every authenticated request
  failed resolving its bearer token.
- **Its wait was unbounded.** The check queued on the request pool behind a
  30-second `pool_timeout`, so it answered long after the orchestrator stopped
  listening, and every probe arriving meanwhile queued one more coroutine on the
  same pool.
- **It said nothing about which dependency failed.**

The worker's heartbeat file was called a readiness probe in four places. It
answers the liveness question — `Worker.healthy` says so — and is unchanged here
apart from the label.

## Decisions

### 1. A check is a function, and the application passes the list

`probe(checks: Mapping[str, Check], *, timeout)` where a `Check` is
`async () -> object` that raises on failure. Built-ins: `check_database`,
`check_cache`, `check_tokens`, `check_queue`. The template's `READINESS` mapping
is the list, and a generated test fails if it stops covering every subsystem the
API binds.

No protocol and no registry. Which dependencies gate readiness is the
application's decision, and a subsystem registering itself would make that list
invisible at the one place someone looks for it.

### 2. Concurrent, and each check under its own deadline

The probe's latency is the slowest dependency's, not the sum. `DEFAULT_TIMEOUT`
is 0.8 seconds — under Kubernetes' default one-second probe timeout with room for
the response, so the 503 and the name of the failed check arrive while the
orchestrator is still listening.

A check that catches its own cancellation and returns late is still a
`TimeoutError`: `asyncio.timeout` raises only if the cancel it sent propagates,
so the deadline's `expired()` is checked as well. Cancelling the probe itself
propagates — a client that hung up is not a dependency that is down.

### 3. The body carries the exception class; the log carries the message

`/ready` is often reachable from outside, and a driver's message names hosts and
ports. The class — `ConnectionRefusedError`, `TimeoutError`,
`ConfigurationError` for a lifespan left out of the wiring — is enough to know
where to look, and the full message is logged at `WARNING` with the request id
already on the line.

### 4. The database is probed on a connection of its own

This is the decision the review changed. `Database.ping()` first went through the
request pool, on the argument that a process with no free connection cannot
serve. That is false: a request arriving at a saturated pool waits and is
served. And because every replica shares the database, every replica saturates
together — so under a traffic burst the probe took the **whole fleet** out of
rotation at once, the load balancer answered with its own 502s, and the pools
drained only to be refilled by the herd returning.

`Database` now builds a second engine with a one-connection pool, pre-ping, and
no `statement_timeout` listener. It is built in `__init__` and connects on the
first ping, so a process that never probes opens nothing. It is built *eagerly*
because `_SavepointDatabase` copies every slot: a lazily created engine on the
copy would never be disposed.

The trade is stated rather than hidden. A database that is down, a wrong
`DATABASE_URL`, or `SELECT 1` slower than the deadline still makes every replica
unready together. During a rollout that is the point — a replica with a wrong URL
never takes traffic. During a brownout it means the load balancer, not the
application, answers. A replica that stayed in rotation would answer with a
problem document carrying a request id, which is better, but only if it can
tell a brownout from a misconfiguration, and nothing here can. Set
`failureThreshold` to ride out a blip.

### 5. The other checks read, and bypass instrumentation

- `check_cache` reads a key that is never written, on the store *under*
  `EventfulStore`, so a probe every few seconds per replica is not a stream of
  cache misses nobody caused.
- `check_tokens` resolves a freshly generated token — the read every
  authenticated request makes, and nothing is written.
- `check_queue` calls `Queue.size()`, which the contract already documents as
  being for health checks, on the connection `dispatch()` pushes on.

Null and in-memory drivers answer without I/O, which is correct: they have no
dependency to be down.

## What was declined

| Declined | What would change it |
|---|---|
| A `HealthCheck` protocol or base class | A check needing configuration a closure or `functools.partial` cannot carry. |
| Subsystems registering their own checks | Nothing. The list is the application's decision and belongs at its call site. |
| Critical and non-critical checks | Nothing. A dependency whose failure should not take the process out of rotation is not a readiness check; it belongs in metrics. |
| Caching the result | Probe traffic that costs something. Today it is `SELECT 1` per replica every few seconds, and a cached "ready" is the stale claim a probe exists to avoid. |
| Dependencies checked only until the first success | Replicas that fail independently of each other. It keeps the rollout gate and never ejects the fleet, but a replica that loses its own network later stays in rotation. |
| A worker readiness probe | Readiness decides where traffic goes, and a worker takes none. |
| `Database.healthy()` | Removed. It was an unbounded twin of `ping()` with no caller left. |
| A `READY_TIMEOUT` setting | An orchestrator whose probe timeout is not one second. It is one argument to `probe()`. |

## Consequences

**Each `Database` holds one more connection once probed**, opened on the first
probe. A pool sized to a managed database's connection limit needs one per
replica of headroom.

**A fake records the probe.** `check_cache` and `check_tokens` under
`fake_cache()` or `fake_tokens()` add a recorded `get` or `resolve`, so a test
that calls `/ready` and then asserts an exact operation count will see them.

**A probe that times out while a connection is being set up can leave that
connection open on the server** until Python's cyclic garbage collector runs.
The window is in SQLAlchemy's asyncpg adapter — after asyncpg has connected,
before the pool has registered the connection — and plain asyncpg does not
have it. The pool's accounting stays correct, so `pool_size` is exceeded
without anything noticing, and `close()` cannot release what the pool never
tracked.

The dedicated probe connection makes this rarer only while the database is
healthy. A probe cancelled mid-query invalidates its connection, so during a
slow-database incident — every probe timing out — every probe reconnects
inside the window, as it did on the shared pool. Measured with gc disabled
over 400 probes: 32 stray connections on the shared pool, 27 forcing a
reconnect per probe, and 4 to 34 on the persistent probe engine depending on how
many probes timed out. All were reclaimed by gc. Recorded rather than
mitigated: the cost is extra connections on a database that is already
struggling, bounded by probe frequency, and the fix belongs upstream.

**The probe cannot see "no new connections".** A rotated password, a full
`max_connections` or a `pg_hba` change fails every new request connection while
the warm probe connection keeps answering, until `pool_recycle` replaces it. The
shared-pool probe was as blind, since it checked out a connection that was
already open. Its connection is named `keel-readiness-probe` in
`pg_stat_activity`, so it can be told apart from the ones serving requests.

## What the review caught

**The probe ejected the fleet under load**, as decision 4 describes. The test
that should have caught it, `test_check_database_is_bounded_when_the_pool_is_exhausted`,
asserted the defect as a feature: it proved the probe failed fast on a
saturated pool, which is exactly what it must not do. It is now
`test_a_saturated_request_pool_is_still_ready`, and fails if the probe shares the
request pool again.

**A check that swallowed its cancellation reported `ok` at any age**, against a
docstring promising "returned within the timeout".

**`DEFAULT_TIMEOUT` argued against its own value.** It said a limit longer than
Kubernetes' one second answers too late; at exactly one second a hanging
dependency answered at 1.006.

**A one-off Redis error kills the worker and cancels jobs mid-run** — outside
this slice, confirmed while checking what the worker's liveness means during an
outage. `reserve` raising propagates out of the worker's task group, which
cancels its siblings, including running jobs, skipping `shutdown_grace`. Fixed
as ADR 0006, decision 10.

**The connection-count test counted every backend on the database**, so run
serially against the shared database it failed three times in six while any
other client was connecting. It counts the probe's `application_name` now.

What it confirmed rather than found: 3,000 randomly cancelled Redis probes
delivered no reply to the wrong caller (redis-py disconnects on cancellation
mid-read), 1,500 randomly cancelled Postgres probes left the pool's accounting
correct and the next query working, and each built-in check reaches the network
on every driver that has one.

## Verification

- A saturated request pool reads as ready; a refused database, Redis, token
  store and queue each read as unready, by name.
- A probe opens exactly one server connection however often it runs, and
  `close()` releases it.
- A hanging check fails at the deadline and does not delay the others' results;
  three 0.2s checks finish in under 0.5s.
- The response body never contains the exception's message; the log does.
- An unbound subsystem is a `ConfigurationError` result, not a raised exception.
- Cancelling the probe propagates and logs nothing.
- In the generated project, `/ready` with the database overridden to an
  unreachable URL is a 503 naming `database`, with every other check still `ok`.
