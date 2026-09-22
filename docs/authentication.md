# Authentication and authorization

`keel.auth` answers four questions: who the caller is, how a password is stored,
how a bearer token is issued and revoked, and whether the caller may do this to
this thing. Each part stands alone, so a worker that never sees a password still
knows who a job is acting for. There is no guard protocol and no user provider;
[ADR 0007](adr/0007-identity-and-tokens.md) says why both were declined.

## Identity

An `Identity` is a frozen value: an `id`, a set of `roles`, and a small mapping
of `claims`. It is never the user row, because a row outlives its session and
cannot travel through a queue. The edge builds one from whatever it knows.

```python
from keel.auth import Identity, acting_as, current_identity, require_identity

with acting_as(Identity(id=user_id, roles=frozenset({"admin"}))):
    current_identity()  # the Identity above, or None outside the block
    require_identity()  # the same, or AuthenticationRequiredError
```

`acting_as` is the only supported way to bind one, and the binding ends with the
block. There is no guest object: `current_identity()` returns `None` when
nobody is signed in, and a policy has to say what `None` gets.
`require_identity()` raises when nothing is bound. That is a wiring bug, not a
failed login, so an edge maps it to 500 and never to 401.

A worker job acting for someone uses the same call. It rebuilds the `Identity`
from the job payload and wraps its work in `acting_as`, so audit columns and
policies inside the job find the same caller a route would.

## Passwords

Hashing is Argon2id through pwdlib. Install the extra: `keel[auth]`. Without it,
`import keel.auth` still works, because hashing is exported lazily.

```python
from keel.auth import HashingConfig, hash_password, hashing_lifespan, verify_password

async with hashing_lifespan(HashingConfig.from_env()):
    stored = hash_password("correct horse")
    verify_password("correct horse", stored)  # True
    verify_password("anything", None)  # False, at the cost of a real miss
```

Pass `None` as the stored hash when the address is unknown. The hasher verifies
against a dummy so the miss takes as long as a hit, and a login cannot leak
which addresses exist. Rehash on login is
`password_hasher().verify_and_upgrade(plaintext, hashed)`. It returns
`(matched, replacement)`; persist `replacement` when it is not `None`, because
the stored hash was made at an older cost. Run either call on a thread and
outside a unit of work, or tens of milliseconds of CPU stall the event loop.

| Variable | Default | Meaning |
|---|---|---|
| `HASHING_TIME_COST` | `3` | Iterations, at least 1 |
| `HASHING_MEMORY_COST` | `65536` | KiB per hash, at least `MINIMUM_MEMORY_COST` (8192) |
| `HASHING_PARALLELISM` | `4` | Lanes, at least 1 |

Below 8 MiB Argon2 stops being memory-hard, so a lower value is refused at
load. A test suite turns the cost down; the production default does not move.

## Bearer tokens

A token is 256 bits from `secrets`. Every store keeps only its SHA-256 digest,
so a leaked store hands out nothing that works. The plaintext exists once, in
the `IssuedToken` returned at issue time.

```python
from keel.auth import TokenConfig, issue_token, resolve_token, revoke_token, token_lifespan

async with token_lifespan(TokenConfig.from_env()):
    issued = await issue_token(identity, label="web")  # ttl= to override the default
    identity = await resolve_token(issued.plaintext)  # Identity, or None
    await revoke_token(issued.plaintext)  # True if a live token was removed
```

`resolve_token` answers `None` for an unknown, expired or revoked token. The
identity comes back as it was at issue time, roles and claims included, so a
check costs no query. To sign a person out of every device, reach the store
directly: `await token_store().revoke_subject(user_id)`.

| Variable | Default | Meaning |
|---|---|---|
| `TOKEN_DRIVER` | `memory` | `redis`, `memory` or `fake` |
| `TOKEN_PREFIX` | `keel:tokens` | Key namespace, must be non-empty |
| `TOKEN_TTL` | `1209600` | Seconds, fourteen days; `never` for no expiry |
| `REDIS_URL` | | Required by `redis`; shared with the cache and the queue |

`memory` is a real deployment for a single process: everyone is signed out when
it restarts. `redis` is shared across replicas. `fake` records every operation
and still works, for tests.

## Policies

A policy is one function per resource type, `(identity, action, resource) ->
bool`. The registry looks the resource's exact type up and asks that function.
`authorize` raises `AuthorizationDeniedError` on a refusal; `allows` returns the
answer. A type with no policy raises `ConfigurationError` rather than denying,
so a forgotten registration is loud.

```python
from keel.auth import Identity, authorize, register_policy


def account_policy(identity: Identity | None, action: str, account: Account) -> bool:
    if identity is None:
        return False
    mine = identity.id == account.user_id
    match action:
        case "view" | "update" | "delete":
            return mine or identity.has_role("admin")
        case "change-password":
            return mine
        case _:
            return False


register_policy(Account, account_policy)  # once, at import

authorize("update", Account(user.id))  # raises AuthorizationDeniedError, or returns
```

`Account` is a frozen dataclass holding a `user_id` and nothing else. Policies
are synchronous, and `register_policy` refuses an `async def`, because a
coroutine is truthy and would permit everything.

Call `authorize` in the service, not the router. The service already holds the
identifiers it is about to act on, and nothing in the check reads the request.
A route and a job then share one answer. An edge maps the error to 403.

## In tests

`fake_tokens()` from `keel.testing` swaps in a recording store that still issues
working credentials, so a test can sign in and then use what it was given.

```python
from keel.auth import Identity, acting_as
from keel.testing import fake_tokens

with fake_tokens() as tokens:
    await sessions.sign_in(payload)
    tokens.assert_issued(user.id, label="web")

with acting_as(Identity(id=owner.id)):
    await users.update_user(owner.pid, changes)  # runs as the owner
```

`use_policies()` gives a test an empty registry to register its own rules into.
There is no permissive policy double, because one that escaped would be a hole.

## In the template

`app/security.py` translates the `Authorization` header into an `Identity`.
`Caller` refuses the request with 401 when no valid token is presented, and
`MaybeCaller` hands back `None` for routes that behave differently when signed
in. The `sessions` module is the login: `POST /sessions` verifies a password
and issues a token, `GET /sessions/current` returns the caller's profile, and
`DELETE /sessions/current` revokes the presented token.

`app/policies.py` holds three rules. `Account` is owner-or-admin for view,
update and delete, and owner alone for `change-password`. `Directory` is every
account at once, and only an admin may `list` it. `ItemsOf` is owner-or-admin
for one owner's items. Nothing in the scaffold issues the admin role yet.

Routes name users by public identifier, and a service resolves it to the row
before calling `authorize` with the primary key. A primary key never crosses
the wire; see [ADR 0014](adr/0014-public-identifiers.md).

## Limits

Each of these is absent on purpose, and the ADR records what would change it.

- A guard protocol: one implementation is indirection. [ADR 0007](adr/0007-identity-and-tokens.md)
- A user provider: the row and its schema are the application's. [ADR 0007](adr/0007-identity-and-tokens.md)
- A policy chain: nothing yet has to run for every resource. [ADR 0008](adr/0008-authorization-policies.md)
- Refresh tokens: `revoke_token` plus `issue_token` composes rotation. [ADR 0007](adr/0007-identity-and-tokens.md)
- Password reset, and rate limiting on sign-in: both are the application's. [ADR 0007](adr/0007-identity-and-tokens.md)
- A source for roles: the template's admin arms wait for one. [ADR 0008](adr/0008-authorization-policies.md)

## Further reading

- [ADR 0007 — identity and tokens](adr/0007-identity-and-tokens.md)
- [ADR 0008 — authorization policies](adr/0008-authorization-policies.md)
- [ADR 0014 — public identifiers](adr/0014-public-identifiers.md)
