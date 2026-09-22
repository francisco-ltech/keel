# Mail

One call sends an email, a driver behind it decides how, and a fake records
what would have gone out. Without it, every service grows its own `smtplib`
wrapper, and none of them refuse header injection.

## Wiring

```python
from keel.mail import MailConfig, mail_lifespan

async with mail_lifespan(MailConfig.from_env()):
    ...
```

| Variable | Default | Meaning |
|---|---|---|
| `MAIL_DRIVER` | `log` | `smtp`, `log`, `null` or `fake` |
| `MAIL_FROM` | `noreply@localhost` | The `From` address for a message that names none |
| `MAIL_HOST` | `localhost` | The SMTP server |
| `MAIL_PORT` | `1025` | Its port |
| `MAIL_USERNAME` | unset | SMTP credentials, if the server wants them |
| `MAIL_PASSWORD` | unset | With the username |
| `MAIL_SECURITY` | `none` | `none`, `starttls` or `tls` |
| `MAIL_TIMEOUT` | `10` | Seconds one SMTP conversation may take |

`smtp` is the standard library's `smtplib` on a worker thread, one connection
per message. `log` writes who was mailed about what and sends nothing, for an
environment with no server. `null` discards everything, for a process that
must never send. Credentials with `MAIL_SECURITY=none` are refused at start-up,
since the password would cross the network in clear.

## Using it

```python
from keel.mail import Message, send

delivery = await send(
    Message(
        to=user.email,
        subject="Welcome",
        text=f"Hello {user.full_name}, your account is ready.",
        html=f"<p>Hello {escape(user.full_name)}, your account is ready.</p>",
    )
)
delivery.message_id  # what the mail carries, for a log line or a ticket
```

A `Message` is validated when it is built, once for every driver. It needs a
recipient. A line break in any address, the subject or a custom header is
header injection and raises `InvalidMessageError`. `headers=` may carry a list
id or a tracking key, but not a header the message writes itself, such as
`To`, `Bcc` or `Subject`. Text is mandatory and HTML is an alternative to it.
`cc`, `bcc` and `reply_to` are keyword arguments; blind copies reach the
envelope and never a header.

`send()` fills in `MAIL_FROM` when the message names no sender, then announces
a `MailSent` event on the manager's dispatcher with the driver, recipient
count, subject and message id. Never a body or an address.

A server that cannot be reached, or that refuses every recipient, raises
`MailDeliveryError` naming the host and port. Its `permanent` flag is set when
the answer was a 5xx, which no retry will change. A server that refuses some
recipients after accepting the rest has sent the mail, so that is reported on
the `Delivery` instead: `recipients` is who got it and `refused` is who did
not, with the server's reason.

## Sending later

There is no queue inside the mail subsystem. A job that calls `send()` is the
whole feature:

```python
@dataclass(frozen=True, slots=True)
class SendWelcome(Job):
    user_pid: str
    max_attempts: ClassVar[int] = 5

    async def handle(self) -> None:
        async with uow() as session:
            user = await UserRepository(session).get_by_pid(UUID(self.user_pid))
            if user is None:
                raise PermanentFailureError(f"user {self.user_pid} no longer exists")
            account = UserRead.model_validate(user)
        try:
            await send(welcome(account))
        except MailDeliveryError as exc:
            if exc.permanent:
                raise PermanentFailureError(str(exc)) from exc
            raise
```

Dispatched inside the unit of work, the job is pushed only once the
transaction commits, so a row that rolls back welcomes nobody. See
[queue](queue.md). A service with no worker registers the send with
`after_commit` from `keel.database.hooks` instead; a send that fails there is
logged on `keel.database` with its traceback, since the response has already
gone out.

## In tests

```python
from keel.testing import fake_mail

with fake_mail() as outbox:
    await register(payload)

sent = outbox.assert_sent(to="ada@example.com", subject_contains="Welcome")
assert "Ada" in sent.text
outbox.assert_sent_times(1)
```

`assert_nothing_sent()` is the other half: register a duplicate address and
prove the outbox stayed empty. The fake renders every message the way the SMTP
driver would, so a message production would refuse fails the test too. A
failed assertion prints the timeline of what was sent.

## In the template

`app/modules/users/mail.py` builds the welcome, one function per message and
no template engine. With a worker, `create_user` dispatches `SendWelcome` from
`app/modules/users/jobs.py`. Without one, the send runs after the commit on
the request. Mailpit runs beside Postgres and Redis under `just up`, the app
containers point at it, and every message a development run sends is at
http://localhost:8025. The generated suite runs on the `log` driver and needs
no Mailpit; the tests that ask what was sent enter `fake_mail()`.

## Limits

- No template engine or `Mailable` class. A message is a function returning a
  `Message`; a second message that should share a layout is what would change
  that. ADR 0015.
- No attachments, and no provider driver for SES, Postmark or Resend. Each of
  them speaks SMTP; a driver arrives with the deployment that needs one.
- No address validation beyond the line-break check. What is deliverable is
  the server's decision, reported per recipient.
- No readiness check on the mail server. No request needs it to answer, and a
  probe on it would pull the API out of rotation for a dead relay.

## Further reading

- [ADR 0015 — mail](adr/0015-mail.md): the subsystem shape applied a third
  time, a message validated once for every driver, SMTP from the standard
  library, and what the review caught.
