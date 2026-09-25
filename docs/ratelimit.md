# Rate limiting

A count of attempts per key inside a window, kept in the cache. Without it,
an unauthenticated route can be hammered without limit: Argon2 makes a
password guess slow, and slow is not finite.

## Wiring

None. The limiter counts against the cache's default store, so whatever
`cache_lifespan` bound is where the windows live. There is no `RATELIMIT_*`
variable: the store is the cache's and the limits are the application's.
See [cache](cache.md) for the store.

## Using it

```python
from keel.ratelimit import Limit, clear, guard

SIGN_IN_LIMIT = Limit.per_minute(5)

await guard(f"sign-in:{email.lower()}|{client}", SIGN_IN_LIMIT)
...  # verify the password
await clear(f"sign-in:{email.lower()}|{client}")  # on success
```

`guard` counts one attempt and raises `TooManyAttemptsError` when the window
is spent. The exception carries `retry_after` in seconds; `retry_after_header`
renders it for a `Retry-After` header. `attempt` counts and returns the
verdict instead, an `Attempt` with `allowed`, `remaining` and `retry_after`.
`clear` forgets the window, so a correct password after four wrong ones does
not leave the owner one mistake from a lockout.

A `Limit` is an allowance and a window: `Limit(5, 60.0)`, or
`Limit.per_minute(5)`, `per_hour`, `per_day`. Windows are fixed and aligned
to the clock: a minute window is each clock minute. A burst across the edge
can reach twice the allowance over two windows, which is the known cost of a
fixed window and a small one for a sign-in.

`rate_limiter()` returns a `RateLimiter` over the default store, with the
same calls plus `hit` and `remaining`; `RateLimiter(cache.of(name).store)`
counts on another store.

## Choosing a key

The key is what gets its own window. For sign-in the template uses the
address and the client together, so an attacker cannot lock the owner out by
guessing from elsewhere, and cannot spread guesses across addresses from one
place. For reset requests it uses the client alone, since the account already
has a cooldown of its own.

The client is the peer address as the server saw it. Behind a reverse proxy
that is the proxy's address for everyone, and a limit keyed on it becomes one
shared window. Run uvicorn with `--proxy-headers` and `--forwarded-allow-ips`
naming the proxy, and it rewrites the peer from `X-Forwarded-For` before the
application reads it. Never read that header yourself: unrewritten, it is
whatever the client typed.

## In tests

`fake_cache()` gives every test its own store, so every test has its own
windows and nothing leaks between them. Assert on what the limit does: the
429 and its `Retry-After`. There is no fake limiter, because a recording
double would be a second implementation of "count".

## In the template

`app/modules/sessions/service.py` guards sign-in at five a minute per address
and client, before the row is read or anything is hashed, and clears the
window on success. `app/modules/password_resets/service.py` guards requests
at ten a minute per client. `app/security.py` has `client_of`, and
`app/errors.py` turns the exception into a 429 with `Retry-After`.

## Limits

- `CACHE_STORE=null` disables every limit: a null store never has a counter,
  so every attempt is the first. `CACHE_STORE=array` gives each process its
  own windows, so N workers allow N times the limit. ADR 0017.
- No cap on the total across many clients. An attacker with many addresses
  gets the sign-in allowance from each; a per-account cap would let one of
  them lock the owner out. ADR 0017.
- No sliding window or token bucket. A route where a burst of twice the
  allowance is a real cost is what would change that. ADR 0017.
- No middleware or decorator in Keel. The core imports no web framework; the
  template's edge is a few lines. ADR 0017.

## Further reading

- [ADR 0017 — rate limiting](adr/0017-rate-limiting.md)
- [ADR 0001 — the cache seam](adr/0001-the-cache-seam.md): the store the
  limiter counts on.
