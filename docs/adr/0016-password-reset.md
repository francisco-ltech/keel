# ADR 0016 — Password reset, and messages from templates

**Status:** accepted · **Date:** 2026-09-23 · **Phase:** 6 (second slice)

## Context

The template's README listed a password reset under "deliberately not here"
for five releases: "it needs a single-use token delivered out of band before
it is safe". ADR 0015 delivered the out-of-band half. This slice is the rest,
and it is template code rather than a Keel subsystem: a table, two routes, a
message, and the rules that keep an unauthenticated endpoint from becoming an
oracle or a takeover.

Writing the message exposed the second decision. ADR 0015 declined a template
engine "until a second message whose layout the first should share". The
reset was that message, and the review of this slice began with the author
having written it as a second f-string.

## Decisions

### 1. A reset is its own module

`app/modules/password_resets/` with the usual files: a `PasswordReset` table,
a repository, a service, a message, a router, and a job where there is a
worker. Not a corner of `users`, because it owns a table, a lifetime and a
message of its own, and the two rules below are easier to see in a small
module. Not a corner of `sessions`, which has no table on purpose.

### 2. The code is hashed at rest and never stored

32 bytes from `secrets`, URL-safe, and the table keeps its SHA-256, exactly as
the bearer token store does (ADR 0007). The plaintext exists in the message
and, with a worker, in the job's payload for the seconds between the push and
the send. A leaked table hands out nothing that works.

It is not a bearer token and it does not go in the token store. A store entry
resolves to an `Identity`, so a reset code in it would sign the holder in.
The temptation is real, since the store already hashes, expires and revokes;
the reason it is wrong is what the store's `resolve` means.

### 3. Redeeming is one statement

`UPDATE password_resets SET used_at = now() WHERE code_digest = ? AND used_at
IS NULL AND expires_at > now() RETURNING user_id`. A read followed by a write
would let two requests presenting the same code both succeed in the gap; a
compare-and-set cannot. The same shape as `replace_hash_if_unchanged` in the
users repository, for the same reason.

A successful redemption also retires every other outstanding code for the
account and revokes every session. So does `change_password`, and so does
disabling the account: somebody who changes a password on suspecting a leak
must not be undone an hour later by a code mailed before the change. The
first draft retired codes on redemption only, and the review reset an
account with a code issued before its owner had changed the password.

Every path that touches an account and its codes takes the account's row
lock first, then the reset rows. Two codes for one account redeemed at once
deadlocked in the first draft, one caller getting a 500; the lock order is
what removes the cycle. The redemption also looks the code up before it
hashes the new password, so a code that matches nothing costs a query rather
than 40 ms of Argon2 and 64 MiB on a route with no caller to charge it to.

`expires_at` is written from the application's clock and compared with the
database's `now()`. They are not the same clock, and a one-hour lifetime
absorbs any skew worth having.

### 4. The request answers 202 and says nothing in the reply

An unknown address, a disabled account and a request inside the cooldown all
answer as a successful request does: 202, no body. What differs is whether a
message goes out, and how long the request takes. The review measured it:
2.7 ms for an unknown address, 6.7 ms for one that gets a code, bands that do
not overlap. That timing oracle is accepted rather than closed, because the
route next to it, `POST /users`, answers a registered address with a 409 and
so tells an attacker outright what this route's clock would tell them
slowly. Closing one without the other would be theatre; closing both is a
registration flow that answers by mail, which is application policy. The
first draft's docstring claimed the message was the only difference, and the
ADR said the same; both now say what is true.

The cooldown is one message per account per minute, and it holds under
concurrency because the request takes the account's row lock before it
looks: `SELECT ... FOR UPDATE` on the user, then the window check, then the
insert. The first draft checked and then inserted with no lock between, and
twenty-five concurrent requests sent fifteen messages. The earlier code
stays valid, so a flood cannot lock the owner out; it can only stop the
flood from reaching the mailbox. The alternative, superseding the earlier
code on each request, is what Laravel does, and it hands an attacker a way
to keep the owner's link dead. Unregistered addresses are not throttled:
they cost a query and nothing else, and counting them is the rate-limiting
subsystem's job when it arrives.

### 5. The code travels in the body

`POST /password-resets/redeem` with `{code, password}`, the one verb in an
application of nouns. `PUT /password-resets/{code}` would put a live
credential in the path, and the access log and the request inspector record
paths. A body is the one place a secret can travel without being written
down.

### 6. Messages come from templates

`keel.mail.templates.MailTemplates` is the smallest thing that answers ADR
0015's condition: a directory, a text template per message that must exist,
an HTML one that may, both rendered into a `Message`. Jinja, as the
`templates` extra, imported in that module and nowhere else in the package.
Two rules are load-bearing. HTML templates autoescape and text templates do
not, decided by the file name, so `{{ user.full_name }}` is safe in one and
readable in the other. An undefined variable is an error rather than blank
text, because a reset message with an empty code is worse than no message.

The generated project has one renderer in `app/mail.py`, over
`app/templates/mail`, and a `layout.html.j2` every HTML message extends. A
module's `mail.py` names the template pair, the recipients and the subject,
and nothing else. The welcome moved onto it in the same change.

## What was declined

| Declined | What would change it |
|---|---|
| Reusing the token store for reset codes | Decision 2. Nothing: `resolve` would sign the holder in. |
| Superseding the earlier code on each request | Decision 4. |
| The code in the URL | Decision 5. |
| Throttling requests for unregistered addresses | Decision 4. The rate-limiting subsystem, whose first caller this is. |
| A `Mailable` class with a `build()` step, or subjects in templates | Decision 6. A message is a function naming a template pair; a subject is a string at the call site, where the whole message reads in one place. |
| Templates in Keel's package for the welcome and the reset | The messages are the application's. Keel renders; what is said is the project's to change. |

## Consequences

**The api-only shape sends the code on the request, after the commit.** As
with the welcome: a slow server slows the request, and a refused send is a
log line on `keel.database`. The `both` shape hands it to a worker.

**With a worker, the plaintext code rides through Redis** for the seconds
between push and send, lands in `keel_failed_jobs` only if every attempt
fails, and reaches the log only if that write fails too, since the failed-job
store logs the envelope it could not keep. A code is single-use and expires
in an hour, and an operator who can read any of those can also write the
users table. Accepted, and said in the job's docstring, rather than worked
around with a second table. The job's `repr` redacts the code, so a log line
that merely names the job does not carry it.

**`keel[templates]` is in every generated project's dependencies.** A service
that builds its messages by hand can drop the extra and `app/mail.py`.

## What the review caught

Eight findings, the concurrency ones reproduced against Postgres through the
generated application:

- **The cooldown was a read-then-write race**: twenty-five concurrent
  requests, fifteen messages. Decision 4.
- **The request was a timing oracle** while the docstring claimed the
  message was the only difference. Decision 4, and the claim is gone.
- **A code issued before a password change still opened the account** after
  it. Decision 3.
- **Two codes for one account redeemed at once deadlocked**, one caller
  getting a 500. Decision 3.
- **`MailTemplates` swallowed a broken `extends`** as "no HTML variant", so a
  renamed layout would turn every message text-only under a green suite.
  Decision 6: the lookup is separated from the render.
- **Redemption hashed the new password before looking at the code**, 40 ms
  and 64 MiB per unauthenticated request. Decision 3.
- **The expiry test leaned on a one-second margin** against the transaction
  clock. It backdates by minutes now.
- **A third resting place for the plaintext code**, the failed-job store's
  log line, went unnamed. Consequences.

## Verification

Generated, in `tests/test_password_resets.py`:

- `test_an_unknown_address_answers_the_same_and_sends_nothing` and
  `test_a_disabled_account_gets_no_code` — decision 4.
- `test_a_second_request_inside_the_cooldown_sends_nothing` — decision 4,
  and the first code still redeems.
- `test_a_code_works_once` and `test_a_successful_reset_retires_every_earlier_code`
  — decision 3.
- `test_redeeming_sets_the_password_and_signs_every_device_out` — decision 3.
- `test_a_wrong_code_is_refused_without_saying_why` and
  `test_an_expired_code_is_refused` — one message for all three.
- `test_the_code_is_stored_only_as_a_digest` — decision 2.
- `test_the_cooldown_holds_against_concurrent_requests` and
  `test_two_codes_for_one_account_redeemed_at_once_do_not_deadlock` — decisions
  3 and 4, against committed rows through the application's own pool.
- `test_changing_the_password_retires_outstanding_codes` and
  `test_an_invalid_code_is_refused_without_hashing` — decision 3.

In Keel, `tests/test_mail_templates.py`: `test_html_escapes_and_text_does_not`,
`test_an_undefined_variable_is_an_error_not_blank_text` and
`test_a_broken_extends_is_an_error_not_a_missing_html_body` — decision 6.
