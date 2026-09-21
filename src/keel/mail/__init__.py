"""The mail subsystem.

Two imports cover the common case. One at start-up::

    from keel.mail import MailConfig, mail_lifespan

    async with mail_lifespan(MailConfig.from_env()):
        ...

and one wherever something is worth an email::

    from keel.mail import Message, send

    await send(Message(to=user.email, subject="Welcome", text=body))

**The shape is the one every subsystem takes** (ADR 0001, ADR 0007): a
:class:`~keel.contracts.mail.Mailer` protocol, drivers behind it — ``smtp``
for real, ``log`` for a run with no server, ``null`` for a process that must
never send — a :class:`MailManager` that builds them from configuration, a
facade over the bound one, and a recording :class:`FakeMailer` with
assertions, reached in tests through :func:`keel.testing.fake_mail`.

**What is not here** is a template engine, a queue of its own, or a
``Mailable``. A message's body is a string the application rendered however it
likes; sending it later is :func:`keel.queue.dispatch` with a job that calls
:func:`send`, which is what the starter template does for its welcome mail.
ADR 0015 records the reasoning and what was declined.
"""

from __future__ import annotations

from keel.contracts.mail import Mailer
from keel.mail.binding import (
    MailSent,
    bound_mail_manager,
    mail_lifespan,
    mail_manager,
    mailer,
    send,
    set_mail_manager,
    use_mail_manager,
)
from keel.mail.config import DEFAULT_SENDER, KNOWN_DRIVERS, SECURITY, MailConfig
from keel.mail.drivers import LogMailer, NullMailer
from keel.mail.fake import FakeMailer, MailAssertionError
from keel.mail.manager import MailerFactory, MailManager
from keel.mail.message import (
    FORBIDDEN_IN_HEADERS,
    RESERVED_HEADERS,
    Delivery,
    Message,
    new_message_id,
)
from keel.mail.smtp_driver import SmtpMailer

__all__ = [
    "DEFAULT_SENDER",
    "FORBIDDEN_IN_HEADERS",
    "KNOWN_DRIVERS",
    "RESERVED_HEADERS",
    "SECURITY",
    "Delivery",
    "FakeMailer",
    "LogMailer",
    "MailAssertionError",
    "MailConfig",
    "MailManager",
    "MailSent",
    "Mailer",
    "MailerFactory",
    "Message",
    "NullMailer",
    "SmtpMailer",
    "bound_mail_manager",
    "mail_lifespan",
    "mail_manager",
    "mailer",
    "new_message_id",
    "send",
    "set_mail_manager",
    "use_mail_manager",
]
