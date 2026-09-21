"""The contract every mailer that claims to deliver is held to.

Parametrised over the log driver, the fake and, when Mailpit answers, SMTP.
``NullMailer`` is excluded on purpose: it is the documented Null Object, and
holding it to "delivers" would only mean weakening the suite until it passed.
"""

from __future__ import annotations

import pytest

from keel.exceptions import InvalidMessageError
from keel.mail import FakeMailer, LogMailer, MailConfig, Mailer, Message, SmtpMailer

pytestmark = [pytest.mark.anyio, pytest.mark.contract]


def note(**overrides: object) -> Message:
    fields: dict[str, object] = {
        "to": "ada@example.com",
        "subject": "Welcome",
        "text": "Hello Ada.",
        "sender": "keel@example.com",
    }
    fields.update(overrides)
    return Message(**fields)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


@pytest.fixture
def log_backend() -> Mailer:
    return LogMailer()


@pytest.fixture
def fake_backend() -> Mailer:
    return FakeMailer()


@pytest.fixture
def smtp_backend(mailpit: tuple[str, int, str]) -> Mailer:
    host, port, _ = mailpit
    return SmtpMailer(MailConfig(driver="smtp", host=host, port=port))


@pytest.fixture(
    params=["log_backend", "fake_backend", pytest.param("smtp_backend", marks=pytest.mark.mailpit)]
)
def backend(request: pytest.FixtureRequest) -> Mailer:
    """Every mailer that claims to deliver. NullMailer is excluded on purpose."""
    return request.getfixturevalue(request.param)  # type: ignore[no-any-return]


async def test_a_delivery_names_every_recipient_and_a_message_id(backend: Mailer) -> None:
    message = note(cc="grace@example.com", bcc="ops@example.com")

    delivery = await backend.send(message)

    assert delivery.recipients == message.recipients
    assert delivery.message_id.startswith("<") and delivery.message_id.endswith(">")
    assert delivery.driver == backend.name


async def test_two_deliveries_have_distinct_message_ids(backend: Mailer) -> None:
    first = await backend.send(note())
    second = await backend.send(note())
    assert first.message_id != second.message_id


async def test_close_is_idempotent(backend: Mailer) -> None:
    await backend.close()
    await backend.close()


async def test_a_message_with_no_sender_is_refused_by_every_driver(backend: Mailer) -> None:
    """The facade fills the sender; a driver reached around it must not quietly deliver."""
    with pytest.raises(InvalidMessageError, match="sender"):
        await backend.send(note(sender=None))
