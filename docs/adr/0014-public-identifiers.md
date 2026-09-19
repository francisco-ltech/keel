# ADR 0014 — Public identifiers

**Status:** accepted · **Date:** 2026-09-19 · **Applies to:** the data layer, and every route that names a row

## Context

ADR 0003 made primary keys UUIDv7, for the index: inserts cluster, and
`ORDER BY id` approximates creation order. The template then put that key in
every URL and every response, and let any signed-in caller list every account.
Two things are wrong with that, and they are different things.

A UUIDv7 carries its creation time in its top bits, and rows created together
have neighbouring keys, so an identifier on the wire tells a stranger when a
row was made and roughly what was made beside it. And a key that is also the
join column is coupled to the schema: renumbering, sharding or merging tables
would change what every client holds.

Loco's answer, which this follows, is a `pid`: a second identifier, random,
that is the only one clients ever see.

## Decisions

### 1. `PublicId` is a mixin, `pid` is a random UUID4, and the primary key stays

`keel.database.PublicId` adds a `pid` column: unique, indexed, assigned
eagerly like the primary key so a row has one before it is flushed, with a
server default so a backfill or a `psql` insert cannot leave it empty. The
repository gains `get_by_pid`, and a model without the mixin gets a `TypeError` naming it —
checked on the class, since an attribute that merely happens to be called
`pid` would let the query match nothing in silence. There is no
`get_by_pid_or_fail`: the first draft had one, and nothing called it, because
its `RecordNotFoundError` is a 500 to the template's error handlers rather
than a 404.

Two columns rather than a random primary key, because the two reasons pull
apart: the index wants order, the wire wants unguessability, and one column
cannot be both. Declined: exposing the UUIDv7 and calling it fine because it
is unguessable in practice. It leaks creation time, and a public identifier
should say nothing.

### 2. Routes name a `pid`, responses carry a `pid`, and the caller's own things need neither

The template's `UserRead` and `ItemRead` carry `pid` and no `id`; `ItemRead`
drops `owner_id` too, because the owner is whoever the route was addressed
to. Every route that names a row takes its `pid`. And the routes for the
caller's own things — `/items`, `GET /sessions/current` — carry no identifier
at all, because the session names the caller (ADR 0007's identity context).

### 3. A service resolves the `pid` first, then authorizes against the primary key

Policies keep taking primary keys, and `Identity.id` stays the primary key,
because that is what the token store holds and what a job's `acting_as`
rebuilds. So a service that acts on a named row reads it by `pid` first and
then calls `authorize` with the row's key. The first draft's rule was the
reverse — authorize on the path parameter before any read, so a stranger
learns nothing about which ids exist — and it no longer buys anything: a
`pid` is random, so a stranger holding one was given it, and a 403 on it
tells them nothing they did not have. The owner-addressed item routes go
through one `resolve_owner`, so the translation happens in one place.

### 4. Administrative routes need an administrator

Listing users is `list` on a `Directory` resource with no fields, granted to
the admin role and nobody else: a listing of every address is the
enumeration the sign-in path refuses to give. Reading another account is the
owner's or an admin's; the first draft let any signed-in caller read any
account on the grounds that it was a directory, which is the thing this ADR
says it is not.

## What was declined

| Declined | What would change it |
|---|---|
| Random primary keys | Decision 1: the index wants order. |
| `pid` on the `Identity`, and policies comparing `pid`s | Every policy and every job would change for a comparison the service can make once after one read. |
| Opaque, non-UUID public identifiers (a short slug, a hashid) | A URL that has to be pretty. A UUID4 is unguessable and needs no secret; a hashid needs one and is reversible without it. |
| A source for roles | Still the application's, as ADR 0008 left it. The admin routes are gated on the role; what issues it is not Keel's to decide. |

## Consequences

**Migration `0003_public_ids`** adds and backfills the columns; a project on
an earlier release picks it up with `keel update`, and its containers migrate
on start.

**A cached card holds no primary key**: `card_key` and the card carry the
`pid`. The index job's payload does carry the owner's primary key, because it
rebuilds `Identity(id=...)` to act with the owner's authority, and a queue
payload is internal: serialised to Redis and, on exhaustion, to
`keel_failed_jobs`, neither of which a client reads.

**No error message names a primary key.** A `NotFoundError`'s detail is
copied into the problem document, and the first draft's items messages named
the owner's key on every 404, which a `timestamp_of` turns back into the
account's creation time. They say `pid` or nothing now.

**The profile carries the `pid`.** The first draft returned no identifier from
`GET /sessions/current`, and a client that had stored only its credentials
could then never reach `PUT /users/{pid}/password`, the one route this ADR
calls the owner's alone. A `pid` is public by design; the caller's is the one
identifier a client needs.

**The generated README's try-it sequence names no identifier at all**: register,
sign in, and work with `/items`.

## What the review caught

- **A primary key crossed the wire on every items 404**, in the message
  copied into the problem document; `timestamp_of` on it gives the account's
  creation time. Consequences.
- **A returning client could not learn its own `pid`**, so it could never
  change its own password. The profile carries it now.
- **Five docstrings still described authorize-before-lookup**, the rule this
  ADR reverses.
- **`get_by_pid`'s guard looked for an attribute called `pid`**, which a
  property satisfies while the query matches nothing; it checks the mixin now.
- **`get_by_pid_or_fail` had no caller and could not have one.** Removed.
- **The owner-addressed item routes resolve the owner in one unit of work and
  act in another.** Kept, and the window's only symptom, a 404, names no key;
  folding the resolution into every service function would give each two
  owner parameters for one race that already answers correctly.

## Verification

- `test_a_pid_is_assigned_eagerly_and_is_not_time_ordered`,
  `test_get_by_pid_finds_the_row_and_only_that_row`,
  `test_a_model_without_a_pid_says_so`,
  `test_a_row_inserted_without_a_pid_gets_one_from_the_server` — decision 1,
  against Postgres.
- In the generated project: `test_no_response_carries_a_primary_key`,
  `test_listing_users_needs_an_administrator`,
  `test_another_users_account_is_not_readable`, and every users, items and
  jobs test rewritten to address rows by `pid`.
