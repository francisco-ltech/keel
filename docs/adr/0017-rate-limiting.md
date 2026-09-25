# ADR 0017 — Rate limiting

**Status:** accepted · **Date:** 2026-09-25 · **Phase:** 6 (third slice)

## Context

ADR 0007 recorded "nothing is rate-limited" as a known limit of the auth
phase, and ADR 0016 left reset requests for unregistered addresses
uncounted, naming a rate-limiting subsystem as the thing that would count
them. Those are two open, unauthenticated routes that an attacker can hammer
without limit: Argon2 makes a password guess slow, and slow is not finite.
The roadmap's rule was that a subsystem gets built when the template needs
it. Two routes need it.

## Decisions

### 1. A fixed window on the cache's store, and no driver seam of its own

`keel.ratelimit.RateLimiter` counts hits per key inside windows aligned to
the clock: the window is `[n * window, (n + 1) * window)` and its counter is
keyed by `n`, so a new window is a new key and the old one is never touched
again. It needs two atomic operations, "create with a lifetime if absent"
and "increment", and every cache driver already offers both through the
store contract. So the store is the seam, exactly as
it is for the lock (ADR 0001), and the limiter is one implementation over
every driver rather than a driver family with a manager, a config and a fake.
`RATELIMIT_*` would name nothing: the store is the cache's and the limits are
the application's.

A fixed window's known weakness is a burst across its edge, which can reach
twice the allowance over two windows. For a sign-in that is ten guesses
instead of five, and a sliding window's second counter or a token bucket's
timestamp arithmetic buys nothing against it. Laravel's limiter is a fixed
window for the same reason.

The first draft opened a window at the first hit, as Laravel's limiter
does, and repaired a counter whose lifetime was lost between the "add if
absent" and the increment by re-putting it. The review ran sixteen
concurrent attempts against Redis and the repair overwrote the others'
increments: a burst got nine attempts from an allowance of five, inside one
window, on demand. The array store never showed it, because nothing in it
suspends between the two calls, which is why the contract suite now has a
concurrent case. A counter keyed by its window needs no repair, and its
lifetime, set once by the add, is two windows, so a replica whose clock lags
cannot find it already dropped. The wait a refusal reports is computed from
the clock and never exceeds the window.

### 2. `guard` raises, `attempt` answers

Two calls, on the precedent of `authorize` and `allows` (ADR 0008). `guard`
raises `TooManyAttemptsError` carrying the wait, which the template's edge
turns into a 429 with `Retry-After`. `attempt` returns the verdict for a
caller that wants to decide. Both count the attempt; a caller that only wants
to look has `remaining`. `clear` forgets the window, which is what a
successful sign-in does: four typos and a success should not leave the owner
one mistake from a lockout.

### 3. The key is the address and the client together

Sign-in counts `sign-in:<email>|<client>` at five a minute. Keyed on the
address alone, an attacker locks the owner out by guessing from anywhere;
keyed on the client alone, one place guesses at every address five times
each. Together, each address gets its own five from each place. The count
happens before the row is read and before Argon2 runs, so a refused attempt
costs nothing. Reset requests count `reset-request:<client>` at ten a minute,
whatever the address: the per-account cooldown (ADR 0016) already covers a
registered address, and this is what stops one place walking a list.

What the pair does not cap is the total across many places: an attacker
with many addresses gets five guesses from each. That is the price of the
lockout resistance, and it is recorded rather than closed; an aggregate
per-account cap would hand the lockout back.

The client is the peer address as ASGI reports it. Behind a reverse proxy
that is the proxy for everyone, and the limit becomes one shared window.
The fix is uvicorn's `--proxy-headers` with `--forwarded-allow-ips` naming
the proxy, which rewrites the peer from `X-Forwarded-For` before the
application sees it. The template never reads that header itself: unrewritten
it is whatever the client typed, and a limit keyed on it is keyed on nothing.

### 4. No fake

`fake_cache()` already gives every test its own store, so every test has
its own windows, and what the limiter does is observable where it matters:
the 429 and its header. A recording fake would be a second implementation of
"count", which is the point at which a contract suite would start earning
its place and not before. The limiter is held to the cache's contract suite
instead, over the array, fake, eventful and Redis stores.

## What was declined

| Declined | What would change it |
|---|---|
| A driver family with a manager, config and fake | Decision 1. A backend the cache does not have. |
| A sliding window or a token bucket | Decision 1. A route where a burst of twice the allowance is a real cost. |
| A middleware or a route decorator in Keel | The core imports no web framework (ADR 0001). The template's `client_of` and the 429 handler are eight lines of application code. |
| Reading `X-Forwarded-For` | Decision 3. Nothing: the server does it correctly when told which proxy to trust. |
| A limit on registration | The 409 for a taken address is the enumeration answer, and a limit on `POST /users` does not change it. A deployment that wants one has `guard`. |
| An event when a limit refuses | Metrics already count the 429 by route. |
| An aggregate per-account cap on sign-in | Decision 3. It would let one attacker lock every owner out. |

## Consequences

**A limit lives in the cache, so `CACHE_STORE=null` disables every limit.** A
null store never has a counter, so every attempt is the first. The null
store is for turning caching off through configuration; a deployment that
does that has also turned off the throttle, and should know it.

**A limit lives in the cache, so a flush clears every window.** That is the
right answer: the keys are the cache's, and a flush that spared them would be
a flush that lied about what it cleared.

**A management command that signs in shares one window** under the client
`unknown`. Five wrong passwords from scripts in a minute lock that window;
the routes are unaffected.

**`CACHE_STORE=array` gives each process its own windows**, so an API with
N workers allows N times the limit. The template defaults to `redis`; `array`
is one variable away and the change is silent.

**`CACHE_SERIALIZER=pickle` turns every hit into a locked read-modify-write**,
since the Redis store can only `INCRBY` bare digits. It counts correctly and
costs a lock per attempt, and sustained contention on one key surfaces as
`LockTimeoutError`, which the template's edge reports as a 500.

## Verification

- `test_attempts_inside_the_window_are_allowed_and_counted_down`,
  `test_keys_do_not_share_a_window`, `test_a_window_closes_on_its_own` and
  `test_clear_opens_a_fresh_window` — over every store, decision 1.
- `test_a_concurrent_burst_is_counted_in_full` — sixteen at once over every
  store, Redis included, decision 1.
- `test_a_counter_outlives_its_window` — the lifetime, decision 1.
- `test_the_facade_counts_against_the_bound_cache` — a fresh fake is a fresh
  set of windows, decision 4.
- Generated: `test_five_wrong_passwords_close_the_door_for_a_minute`,
  `test_a_correct_password_clears_the_window`, `test_the_window_is_per_address`,
  `test_the_window_is_per_client_too` and
  `test_a_client_gets_ten_requests_a_minute_whatever_the_address` — decisions
  2 and 3.
