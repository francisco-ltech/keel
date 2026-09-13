# ADR 0007 — Identity and bearer tokens

**Status:** accepted · **Date:** 2026-09-13 · **Phase:** 4

## Context

Phase 4 is the largest gap against Laravel and the one three other decisions are
waiting on: audit columns, multi-tenancy, and the template's own README. This
ADR covers the first slice — *who the caller is*, *how a password is stored*, and
*how a bearer credential is issued and revoked*. Guards, policies and a user
provider are deliberately absent, and section 7 says why.

The prediction ADR 0001 made and ADR 0006 confirmed held a third time: the
pieces that transfer are `Manager[T]`, `Binding[T]`, the lifespan shape and the
parametrised contract technique. Everything below them was derived from the
problem again.

## Decisions

### 1. The identity is a value on a ContextVar, and there is no guest

Audit columns, authorization and any job acting on someone's behalf all need the
same answer, and threading it between the edge and the code that asks is not
workable. So it is published exactly as `Database.transaction()` publishes the
active session.

**Never the application's user row.** An ORM instance on a context variable
outlives the session that loaded it and raises `DetachedInstanceError` somewhere
far from the code that put it there; it is also unserialisable, and a job
carrying "who asked for this" has to survive a trip through Redis.

**Null Object declined.** A guest `Identity` with an `id` is indistinguishable
from a real caller at every site that forgets to check, which is precisely how an
authorization bug reaches production. `current_identity()` returns `None`, and
`require_identity()` is there for callers that would rather raise.

### 2. SHA-256 for tokens, Argon2 for passwords — the opposite rules, on purpose

A password is low-entropy and guessable, so the defence is a slow hash. A token
is 256 bits of `secrets` output, so there is no dictionary to run and Argon2
would buy nothing while costing ~44 ms of the request budget on *every*
authenticated call.

Comparison is by digest lookup, so there is no partial-match oracle to time and
no need for `hmac.compare_digest`.

**Only the digest is stored, in every driver.** A store that can hand back a
working credential is a credential dump with extra steps, and the difference is
invisible until it leaks. This is the property the contract suite asserts rather
than trusting each driver to remember.

### 3. Expiry is checked on read as well as enforced by the backend

Redis drops an expired key on its own and a dict does not. A contract that
relied on the backend would mean two behaviours under one name, so
`TokenRecord.is_expired()` is checked on the read path in both drivers — which
also closes the sub-second window where a record with a fractional TTL outlives
its own `expires_at`.

`build_record`, `encode_record` and `decode_record` are shared rather than
per-driver for the same reason: two drivers disagreeing about how a TTL becomes
an expiry, or about the stored JSON shape, is drift a contract suite catches late
and a shared function prevents.

### 4. No Bridge, and no guard protocol

`TokenStore` is one protocol covering issue/resolve/revoke/list/purge. The cache
earned a Bridge because `remember` is a substantial abstraction built from
primitives; a token store's surface *is* the primitives, so a
`Store`/`Repository` split here would be ceremony. Third subsystem, third time
this landed the same way.

Not `runtime_checkable`, for the reason `keel.contracts.cache` gives:
`isinstance` against a protocol checks attribute *names* only, so it would offer
a third-party driver author false assurance. `tests/test_token_store_contract.py`
is the real conformance check.

### 5. The fake decorates a real store, where the queue's records

`FakeQueue` records and does not run, because running a job means running the
*application's* handler — the test would exercise the handler while claiming to
test the dispatcher (ADR 0006, decision 7).

That reasoning does not reach here, and the distinction is which side of the seam
the behaviour lives on. A token store's behaviour is entirely its own: digesting,
expiry, per-subject indexing. It is cheap to have for real, and code under test
almost always signs in and then *uses* the credential it was handed. A recorder
answering `None` to every `resolve` could not support that, and could not pass
the contract suite the real drivers pass.

So `FakeTokenStore` is built like `FakeStore`: a Decorator over a working store,
recording what passes through. It is **in** the contract parametrisation, which
is the whole justification for the construction.

### 6. `memory` is a deployment, not a double

`MemoryTokenStore` is what a single-process service or a development run wants.
"Everyone is signed out when the process restarts" is an honest property rather
than a limitation, and it means the `fake` driver is free to be a test double
without anything depending on it in production.

### 7. What was looked at and left out

* **A guard protocol** (`Guard.attempt/user/logout`, Laravel's shape). It would
  have exactly one implementation until sessions exist alongside tokens, and an
  interface with a single implementor is indirection pretending to be design.
* **A user provider.** Loading the principal's row is the application's job and
  its schema is the application's. Keel's `Identity` is built at the edge from
  whatever the service already knows, which is also what keeps the core free of
  a model dependency.
* **Policies**, at the time this was written — the Strategy-versus-chain question
  needed a second resource before it could be answered honestly. It has one now,
  and [ADR 0008](0008-authorization-policies.md) answers it.
* **Token rotation / refresh pairs.** Real, and a decision about session policy
  rather than storage. `revoke` plus `issue` composes it today.

### 8. `AuthenticationRequiredError` is a 500, and edges must not translate it

`require_identity()` raises when nothing is bound. That means the edge did not
authenticate the caller, or authenticated and did not bind the result — a
wiring bug, not a rejected credential.

The tempting mapping is 401, and it is wrong. A caller who reaches it has
usually sent a working credential that nothing read, so 401 tells them to retry
with the thing that just succeeded and they loop; worse, the failure lands in
the 4xx bucket, where nobody is paged, so a route that silently authenticates
nobody looks like ordinary login noise. It belongs beside `ConfigurationError`
as a 500: it should have been impossible.

Rejecting a missing or unusable credential is a different error, raised at the
edge before any service runs, by whatever dependency reads the header. Keeping
the two apart is what makes "some users can never sign in" show up as an alarm
rather than a metric.

## Consequences

**`keel.auth` exports tokens lazily.** Hashing pulls in pwdlib, an optional
extra, so an eager import would break `import keel.auth` for a token-only
service. Tokens are lazy for the weaker but still real reason that a service
using only `acting_as` should not import a store, a factory and a Redis adapter
to get it. `token_lifespan` therefore lives in `keel.auth.binding` rather than in
the package `__init__`, where `queue_lifespan` sits — defining it in `__init__`
would mean importing `TokenManager` eagerly and undoing the laziness.

**The binding is separate from the manager**, following `keel.queue.dispatch`. A
factory that also owns a process-wide mutable slot is two responsibilities in one
import, and a worker holding one manager per tenant should be able to import the
factory without the global.

**A token with no expiry is reachable only by configuring the default away.**
`TOKEN_TTL=never`, spelled out, because an empty value meaning "forever" is the
kind of default nobody intends.

## What the contract suite caught

Every behavioural assertion passed on the first run. What writing the suite
*surfaced* is two places where the drivers answered the same question
differently, and only one of them turned out to be a contract the drivers could
both keep.

**`revoke_subject` counted differently, and Redis's answer was wrong.** The
protocol says "how many **live** tokens were removed". `MemoryTokenStore` checks
`is_expired()`; `RedisTokenStore` counted every record `MGET` still returned.
Because a record key gets `int(ttl) + 1` seconds of Redis TTL, a token that had
already expired — invisible to both `resolve` and `issued_for` — was still
counted for up to a second:

```
memory  resolve -> None   issued_for -> 0   revoke_subject -> 0
redis   resolve -> None   issued_for -> 0   revoke_subject -> 1   # before
```

A "signed out 3 devices" message for two devices is a small lie on the one screen
people read while panicking. Redis could satisfy the contract — it only had to
apply the `is_expired()` filter its other read paths already apply — and now
does. `test_revoke_subject_does_not_count_a_token_that_had_already_expired` pins
it across every driver.

**`purge_expired`'s return value is not comparable across drivers**, and that
one is the contract's problem rather than a driver's. Memory removes expired
records and counts them; Redis has none to remove — the server already did it —
and counts the stale index members it pruned instead. Both are defensible, so
the protocol now says the number is not comparable, and the suite asserts the
invariant that matters instead: *nothing live is touched, and expired tokens
stay gone*. A number nobody can compare is worse than no number, and the honest
fix was to say so in the contract rather than to pick one backend's bookkeeping
and impose it on the others.

## What the adversarial review caught

The suite was written against a Redis driver a concurrent review was taking
apart. It found six defects, and none of them were behavioural assertions the
contract suite could have made — which is the argument for running both.

**Two tokens could be orphaned beyond recovery.** `revoke_subject` and
`purge_expired` read the subject index, made a second round trip, then deleted
the whole index key instead of removing the digests they had read. A token
issued in that window survived with no index entry: still resolving, absent from
`issued_for`, and immune to `revoke_subject` for the rest of its life. On the
password-reset path. Both now `SREM` only what they read, and Redis drops a set
with its last member — so the index key is never deleted by anything.

**The index TTL was inert.** `EXPIRE ... GT` refuses on a key with no expiry and
`SADD` never creates one, so the call returned false on every issue while a
comment explained the invariant it was maintaining. It maintained nothing.
Pruning bounds the set instead, which is what makes `purge_expired` a real
operation to schedule rather than an optimisation. **A comment asserting a
guarantee the code does not implement is worse than no comment**, and this is the
second time on this project that a plausible-looking Redis call has done nothing.

**The default prefixes nested, and had done since Phase 3.** The cache and the
queue both defaulted to `keel`, and all three subsystems read one `REDIS_URL` —
so `cache.flush()`, scanning `keel:*`, already deleted queue keys. Tokens at
`keel:tokens` would have been the third victim. They are now siblings:
`keel:cache`, `keel:queue`, `keel:tokens`. ADR 0001's namespace fix made a
prefix safe against *other applications*; it never considered Keel's own
subsystems nesting inside each other.

Also: one foreign key inside the namespace wedged `purge_expired` permanently
via `WRONGTYPE`, aborting the scan before every index after it; an empty
namespace was documented as forbidden and not refused, which bypasses
`KeyNamespace.pattern()`'s guard entirely; and `close()` closed a client it may
have been handed rather than created.

Each has a regression test named for it in `test_redis_token_store.py`, and the
orphaning one is mutation-checked: restoring the index delete makes it fail and
nothing else does.

## What this does not do

Four limits, all found by review and none of them fixed. They are recorded
because a subsystem that overstates its guarantees is worse than one that has
fewer of them.

**The equal-cost login holds only while the stored hashes are homogeneous.** The
dummy is hashed at the *currently configured* cost; a row written before a cost
raise verifies faster than it. Measured at 10ms against 36ms after raising the
parameters — one request classifies an address, and a wrong password never
triggers a rehash, so the oracle does not heal under attack. Raising cost
parameters should be paired with an offline rehash, not left to rehash-on-login.

**Revocation cannot reach a sign-in already in flight.** `revoke_subject` kills
the tokens that exist when it runs. A sign-in that read the row before a
password change commits issues its token afterwards and survives. Closing it
needs a revocation epoch per subject, checked at resolve — which costs a read on
every authenticated request, and is why it is not built.

**A deleted account can leave a resolvable token** by the same interleaving. It
passes an ownership check and is stopped only by each service's own row lookup.

**Nothing is rate-limited.** Not sign-in, not registration, not the password
change — which makes that endpoint unlimited online recovery of a plaintext
password for anyone holding a stolen token. Rate limiting is unscheduled on the
roadmap, and a service putting this in front of real users needs a limiter it
supplies itself.

## Verification

- `test_the_plaintext_is_never_recoverable_from_the_store` — the property the
  subsystem exists for, asserted against every driver rather than trusted.
- `test_roles_and_claims_survive_a_round_trip` — the serialisation test; a driver
  that drops claims authorises the wrong thing.
- `test_an_expired_token_resolves_to_none` — decision 3, on the read path.
- `test_revoke_subject_leaves_another_subjects_tokens_alone` — the blast radius
  of a password reset.
- `test_purge_expired_leaves_live_tokens_working` — housekeeping is safe to run
  on a schedule against live traffic.
- `test_issued_for_returns_newest_first` — the order a device listing renders in.
- The whole suite runs against `MemoryTokenStore`, `RedisTokenStore` and
  `FakeTokenStore`; the Redis parametrisation skips when no server answers, which
  is why a green run with the services down proves much less.
