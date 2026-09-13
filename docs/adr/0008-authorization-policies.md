# ADR 0008 — Authorization policies

**Status:** accepted · **Date:** 2026-09-13 · **Phase:** 4

## Context

ADR 0007 left one question open: **is a policy a Strategy per resource, or a
Chain of Responsibility?** It was left open deliberately, because answering it
with one resource in hand would have been scaffolding for one case.

The evidence arrived from the template. Phase 4's first pass shipped an `Owner`
FastAPI dependency that compared a path's `user_id` against the caller. It could
not be mounted on the items routes, which live at `/users/{owner_id}/items`, so
those routes stayed anonymous and the generated README had to say so. A review
also found that `Owner`'s parameter was not pinned to the path, which meant
FastAPI would source it from the **query string** on any route that spelled the
parameter differently — a total bypass one rename away.

Both failures share a cause. Ownership was being decided from the URL.

## Decisions

### 1. Authorization happens in the service, not in the router

The fix for `owner_id` versus `user_id` is not a smarter dependency. It is to
stop the path being part of the answer.

A service already has the identifiers it is about to act on, as arguments. It
builds the resource reference from those and calls `authorize()` before opening
its transaction. Routers keep exactly one job — "is somebody there", answered
401 by the bearer dependency — and policies answer "may they", answered 403.

`Owner` and `require_owner` are deleted rather than kept alongside. Two
mechanisms for one question is how they drift.

The property that fell out: query-string smuggling is not defended against, it
is unrepresentable. Nothing in the check reads the request.

### 2. A policy is a function, and the registry is keyed by exact type

`Callable[[Identity | None, str, Any], bool]`. No state, one operation — a class
would be a function wearing a constructor. `register` is generic, so the one
place the resource type is known statically is the place both type checkers
verify the policy accepts it.

**Strategy, selected by `type(resource)`.** Lookup is by exact type with no MRO
walk: a subclass inheriting its parent's rule is a permission nobody granted.

**Actions are strings, one callable per resource.** Laravel's method-per-ability
is `getattr(policy, action)` in Python — the same string, resolved across an
object's whole attribute surface, needing a whitelist to be safe. It is no more
typed at the call site either, since `authorize("update", item)` is a string
either way. One callable with a `match` puts every ability in one block whose
`case _` arm *is* the deny-by-default. What would change this: abilities needing
different signatures, where one callable degrades to `**kwargs`.

### 3. An unregistered resource type raises; it does not deny

Both refuse the request. Only one of them is visible.

A forgotten registration answering 403 is indistinguishable from a real refusal
and lands in the bucket nobody is paged for — the identical argument ADR 0007 §8
makes for `AuthenticationRequiredError`. So it raises `ConfigurationError`,
naming the type and how to register it. Nothing is permitted by omission, and
the omission is loud.

### 4. `None` identity reaches the policy

Denying centrally would be safer by one reading and would make "anyone may read
a published post" inexpressible. So the registry passes it through, and the
`Identity | None` annotation is what stops an author forgetting the case: both
checkers refuse `identity.id` until it is handled.

That is ADR 0007's "there is no guest object" enforced by the type system
rather than by a special case.

### 5. Policies are synchronous

`Identity` carries `roles` and `claims` precisely so a check costs no query
(ADR 0007 decision 1). An async policy is an invitation to issue one, on a path
with no transaction open. A rule that genuinely needs a row the caller cannot
already hold is what would change this.

## What was declined

| Declined | What would change it |
|---|---|
| **Chain of Responsibility** | The first rule that must run for *every* resource before or after its own policy — a super-admin bypass, a tenant check, a suspended-account gate. There is none today, and one link is the ceremony ADR 0006 refused for job middleware. When it arrives, the honest shape is a chain whose middle link is this registry. |
| **A manager and driver seam** | A backend to swap. There is none: this is a dict and some functions. |
| **A parametrised contract suite** | A second implementation. Writing a fake to parametrise over would be manufacturing one. |
| **A test double in `keel.testing`** | Nothing. A permissive policy double is an authorization hole that can escape a suite. `use_policies()` gives a fresh empty registry in one line. |
| **A Null Object default policy** | Nothing. Permissive is a hole; denying hides the wiring bug, which is decision 3. |
| **`Policy` in `keel.contracts`** | `contracts/` is where driver seams live. A policy has no backend. |
| **An `identity=` argument** | Nothing. `with acting_as(them):` already asks the question about someone else, and avoids a `None` that would collide with "explicitly nobody". |

## Consequences

**Exports are eager.** Laziness in `keel.auth` exists for pwdlib and for keeping
a store and a Redis client out of a dispatch-only process. `policies.py` imports
`identity`, `exceptions` and `support.binding` — the same category as `identity`,
which is already eager.

**The binding is pre-installed**, since there is no lifespan for a dict.
`use_policies()` is a ContextVar override and is therefore invisible from a
Starlette lifespan, which is the hazard `Binding`'s own docstring documents.

**A job carries an identity with no roles.** `acting_as(Identity(id=owner_id))`
is rebuilt from the job payload, so a handler can never do more than the person
it acts for.

**The generated scaffold's admin rules are dormant.** Nothing in it issues an
`admin` role — there is no role column — so the admin arms are exercised by
tests that mint a token with roles directly, which is what an application with a
role source would do.

**ADR 0007's limits are unchanged by this.** A token issued to an account
deleted moments later still satisfies the policy for its own id, and is stopped
only by the service's row lookup. Policies did not close that.

## What the review found

The shipped code was correct — no live vulnerability, no wrong status, no
ordering error. What it did not have was a suite that would notice if that
stopped being true. Four one-line edits each left all 79 generated tests green,
and two were directly exploitable:

* **Deleting `change_password`'s `authorize`.** Every non-owner test on that
  route sent a deliberately wrong `current_password`, and *that* path answers
  403 too — so `assert status == 403` passed whether the policy ran or not. A
  stranger who knew the password could take the account. This is the coverage
  genuinely lost when `require_owner` was deleted: the assertion had been
  produced by the dependency, the mechanism moved, and the assertion stayed.
* **Moving `create_item`'s `authorize` after the owner lookup.** A stranger then
  gets 404 for an unregistered id and 403 for a real one — a free oracle for
  which user ids exist. Three docstrings asserted the ordering; nothing tested
  it, because the test that came closest used an admin and took the permitted
  branch.
* Weakening `change-password`'s action string to `update`, and giving the job's
  `acting_as` an `admin` role.

Tests now cover all four, each mutation-checked.

**An `async def` policy failed open.** `bool()` of a coroutine is `True`, so an
async policy permitted every caller for every action on that type, with only an
"never awaited" warning. The fail-safe on a falsey return does not cover the
truthy half, and `async def` is the reflex — every other extension point in Keel
is async. `register()` now refuses a coroutine function outright, so it is a
start-up error like every other wiring mistake here.

**`test_ownership_cannot_be_smuggled_through_the_query_string` documents rather
than guards.** Its failures are a strict subset of the plain stranger tests', and
the channel is closed by construction: the service takes an ordinary Python
argument, so there is no injected scalar for a query value to bind to. Kept for
naming the hazard, not counted as coverage.

## Verification

- A stranger is refused write, read, update and delete on another owner's items,
  through a real ASGI client, at 403 with a problem document.
- `?owner_id=<attacker>` changes nothing, because nothing in the check reads the
  request.
- An admin may write anybody's items and may update or delete an account, but is
  **refused** `change-password` — that route's precondition is the password in
  force, and a role is not knowledge of it.
- Mutation-checked twice: removing a registration makes the routes 500 rather
  than opening them, and removing an `authorize()` call fails the suite.
