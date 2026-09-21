"""The mail subsystem: the message, the drivers, the manager and the facade.

The SMTP half runs against a real Mailpit and reads the delivered mail back
through its API, because whether a ``Bcc`` stays out of the headers, whether
the HTML arrived as an alternative, and whether the ``Message-ID`` on the wire
is the one the delivery record names are exactly the things a fake would get
right by construction.
"""

from __future__ import annotations

import logging
import re
import socketserver
import threading
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from keel.exceptions import ConfigurationError, InvalidMessageError, MailDeliveryError
from keel.mail import (
    DEFAULT_SENDER,
    Delivery,
    FakeMailer,
    LogMailer,
    MailAssertionError,
    MailConfig,
    Mailer,
    MailManager,
    MailSent,
    Message,
    NullMailer,
    SmtpMailer,
    bound_mail_manager,
    mail_lifespan,
    mail_manager,
    mailer,
    send,
    set_mail_manager,
    use_mail_manager,
)
from keel.support.events import EventDispatcher
from keel.testing import fake_mail

pytestmark = [pytest.mark.anyio]


def note(**overrides: Any) -> Message:
    """A valid message, with whatever a test wants changed."""
    fields: dict[str, Any] = {
        "to": "ada@example.com",
        "subject": "Welcome",
        "text": "Hello Ada.",
        "sender": "keel@example.com",
    }
    fields.update(overrides)
    return Message(**fields)


# -- the message ------------------------------------------------------------


def test_a_message_normalises_its_addresses() -> None:
    message = Message(
        " ada@example.com ",
        "  Welcome ",
        "Hi",
        cc=["grace@example.com"],
        bcc="ops@example.com",
        sender="keel@example.com",
    )

    assert message.to == ("ada@example.com",)
    assert message.subject == "Welcome"
    assert message.recipients == ("ada@example.com", "grace@example.com", "ops@example.com")


@pytest.mark.parametrize(
    "overrides",
    [
        {"to": []},
        {"to": ""},
        {"to": "   "},
        {"subject": ""},
        {"subject": "Hi\nBcc: everyone@example.com"},
        {"to": "ada@example.com\r\nBcc: everyone@example.com"},
        {"cc": ["grace@example.com", "x@example.com\n"]},
        {"sender": "keel@example.com\nX-Injected: yes"},
        {"headers": {"X-Tag": "a\nb"}},
        {"headers": {"X-Tag\n": "a"}},
        {"headers": {"Bcc": "everyone@example.com"}},
        {"headers": {"to": "attacker@example.com"}},
        {"headers": {" Subject ": "Replaced"}},
    ],
)
def test_a_message_that_cannot_be_sent_is_refused_on_construction(
    overrides: dict[str, Any],
) -> None:
    with pytest.raises(InvalidMessageError):
        note(**overrides)


def test_with_sender_fills_only_a_missing_sender() -> None:
    unsigned = note(sender=None)
    signed = note()

    assert unsigned.with_sender("noreply@example.com").sender == "noreply@example.com"
    assert signed.with_sender("noreply@example.com") is signed


def test_as_mime_renders_alternatives_headers_and_no_bcc() -> None:
    message = note(
        html="<p>Hello <b>Ada</b>.</p>",
        cc="grace@example.com",
        bcc="ops@example.com",
        reply_to="support@example.com",
        headers={"X-Tag": "welcome"},
    )

    mime = message.as_mime("<abc@example.com>")

    assert mime["From"] == "keel@example.com"
    assert mime["To"] == "ada@example.com"
    assert mime["Cc"] == "grace@example.com"
    assert mime["Reply-To"] == "support@example.com"
    assert mime["Message-ID"] == "<abc@example.com>"
    assert mime["X-Tag"] == "welcome"
    assert mime["Bcc"] is None, "a blind copy is never written into a header"
    assert mime.get_content_type() == "multipart/alternative"
    plain, html = mime.get_body(("plain",)), mime.get_body(("html",))
    assert plain is not None and html is not None
    assert plain.get_content().strip() == "Hello Ada."
    assert "<b>Ada</b>" in html.get_content()


def test_as_mime_refuses_a_message_with_no_sender() -> None:
    with pytest.raises(InvalidMessageError, match="sender"):
        note(sender=None).as_mime("<abc@example.com>")


# -- configuration ------------------------------------------------------------


def test_from_env_reads_every_knob() -> None:
    config = MailConfig.from_env(
        {
            "MAIL_DRIVER": "SMTP",
            "MAIL_FROM": "hello@example.com",
            "MAIL_HOST": "mail.example.com",
            "MAIL_PORT": "587",
            "MAIL_USERNAME": "user",
            "MAIL_PASSWORD": "secret",
            "MAIL_SECURITY": "STARTTLS",
            "MAIL_TIMEOUT": "2.5",
        }
    )

    assert config == MailConfig(
        driver="smtp",
        sender="hello@example.com",
        host="mail.example.com",
        port=587,
        username="user",
        password="secret",
        security="starttls",
        timeout=2.5,
    )
    assert MailConfig.from_env({}) == MailConfig()
    assert MailConfig().driver == "log" and MailConfig().sender == DEFAULT_SENDER


@pytest.mark.parametrize(
    "env",
    [
        {"MAIL_SECURITY": "ssl"},
        {"MAIL_PORT": "0"},
        {"MAIL_PORT": "many"},
        {"MAIL_TIMEOUT": "-1"},
        {"MAIL_FROM": "   "},
        {"MAIL_USERNAME": "relay", "MAIL_PASSWORD": "hunter2", "MAIL_SECURITY": "none"},
    ],
)
def test_a_configuration_that_cannot_work_is_refused(env: dict[str, str]) -> None:
    with pytest.raises(ConfigurationError):
        MailConfig.from_env(env)


# -- the manager --------------------------------------------------------------


async def test_the_manager_builds_every_shipped_driver() -> None:
    manager = MailManager(MailConfig(driver="log"))
    try:
        assert isinstance(manager.mailer(), LogMailer)
        assert isinstance(manager.mailer("null"), NullMailer)
        assert isinstance(manager.mailer("fake"), FakeMailer)
        assert isinstance(manager.mailer("smtp"), SmtpMailer)
        assert manager.mailer() is manager.mailer("log"), "memoised"
    finally:
        await manager.close()


async def test_an_unknown_driver_points_at_register_driver() -> None:
    manager = MailManager(MailConfig(driver="pigeon"))
    with pytest.raises(ConfigurationError, match="register_driver"):
        manager.mailer()


async def test_a_registered_driver_is_built_with_the_configuration() -> None:
    seen: list[tuple[str, MailConfig]] = []

    def build(name: str, config: MailConfig) -> Mailer:
        seen.append((name, config))
        return FakeMailer(name)

    manager = MailManager(MailConfig(driver="pigeon"))
    manager.register_driver("pigeon", build)
    assert manager.mailer().name == "pigeon"
    assert seen == [("pigeon", manager.config)]


# -- the drivers that need no server ----------------------------------------------


async def test_the_log_mailer_writes_the_envelope_and_never_the_body(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="keel.mail.drivers")
    message = note(text="reset link: https://example.com/reset/SECRET", bcc="ops@example.com")

    delivery = await LogMailer().send(message)

    line = caplog.records[-1].getMessage()
    assert "ada@example.com" in line and "ops@example.com" in line and "Welcome" in line
    assert "SECRET" not in line
    assert delivery.message_id in line
    assert delivery == Delivery(delivery.message_id, message.recipients, "log")


async def test_the_null_mailer_discards_but_still_answers() -> None:
    delivery = await NullMailer().send(note())
    assert delivery.driver == "null" and delivery.message_id.startswith("<")


# -- the fake -----------------------------------------------------------------------


async def test_the_fake_records_and_asserts() -> None:
    outbox = FakeMailer()
    await outbox.send(note())
    await outbox.send(note(to="grace@example.com", subject="Invoice #1"))

    assert outbox.assert_sent(to="ada@example.com").subject == "Welcome"
    assert outbox.assert_sent(subject_contains="Invoice").to == ("grace@example.com",)
    outbox.assert_sent_times(2)
    outbox.assert_sent_times(1, to="ada@example.com")
    with pytest.raises(MailAssertionError, match=re.escape("no message to nobody@example.com")):
        outbox.assert_sent(to="nobody@example.com")
    with pytest.raises(MailAssertionError, match="expected nothing"):
        outbox.assert_nothing_sent()
    outbox.reset()
    outbox.assert_nothing_sent()


def test_a_failed_assertion_is_an_assertion_error() -> None:
    """So a test framework reports it as a failed test rather than an errored one."""
    assert issubclass(MailAssertionError, AssertionError)


# -- the facade and the lifespan ----------------------------------------------------


async def test_send_fills_in_the_configured_sender_and_announces() -> None:
    events = EventDispatcher()
    seen: list[MailSent] = []
    events.listen(MailSent, seen.append)

    async with mail_lifespan(MailConfig(driver="fake", sender="noreply@example.com"), events):
        delivery = await send(note(sender=None, bcc="ops@example.com"))
        outbox = mailer()
        assert isinstance(outbox, FakeMailer)
        assert outbox.sent[0].sender == "noreply@example.com"

    assert delivery.recipients == ("ada@example.com", "ops@example.com")
    assert seen == [MailSent("fake", 2, "Welcome", delivery.message_id)]
    assert bound_mail_manager() is None


async def test_the_lifespan_restores_what_was_bound_before() -> None:
    outer = MailManager(MailConfig(driver="null"))
    set_mail_manager(outer)
    try:
        async with mail_lifespan(MailConfig(driver="log")):
            assert mail_manager().config.driver == "log"
        assert mail_manager() is outer
    finally:
        set_mail_manager(None)


async def test_the_override_wins_over_the_process_wide_binding() -> None:
    """The context-local layer is for tests, and it takes precedence on purpose."""
    override = MailManager(MailConfig(driver="null"))
    async with mail_lifespan(MailConfig(driver="log")):
        with use_mail_manager(override):
            assert mail_manager() is override
        assert mail_manager().config.driver == "log"


async def test_nothing_bound_says_what_to_do() -> None:
    with pytest.raises(ConfigurationError, match="mail_lifespan"):
        await send(note())


async def test_fake_mail_binds_a_recording_outbox() -> None:
    with fake_mail() as outbox:
        await send(note(sender=None))
        outbox.assert_sent(to="ada@example.com", subject_contains="Welcome")
        assert outbox.sent[0].sender == "tests@keel.invalid"
    assert bound_mail_manager() is None


# -- smtp, against Mailpit ------------------------------------------------------------


@pytest.fixture
async def smtp(mailpit: tuple[str, int, str]) -> AsyncIterator[tuple[SmtpMailer, str]]:
    host, port, api = mailpit
    config = MailConfig(driver="smtp", host=host, port=port, sender="keel@example.com")
    yield SmtpMailer(config), api


async def delivered(api: str, message_id: str) -> dict[str, Any]:
    """Find one delivered message by its Message-ID and return Mailpit's full record."""
    async with httpx.AsyncClient(base_url=api, timeout=5) as client:
        listing = (await client.get("/api/v1/messages", params={"limit": 200})).json()
        # Exact: a message id with a stray ">" on it must not match by accident.
        summary = next(m for m in listing["messages"] if f"<{m['MessageID']}>" == message_id)
        detail: dict[str, Any] = (await client.get(f"/api/v1/message/{summary['ID']}")).json()
        return detail


@pytest.mark.mailpit
async def test_smtp_delivers_the_message_mailpit_shows(smtp: tuple[SmtpMailer, str]) -> None:
    driver, api = smtp
    message = note(
        html="<p>Hello <b>Ada</b>.</p>",
        cc="grace@example.com",
        bcc="ops@example.com",
        reply_to="support@example.com",
    )

    delivery = await driver.send(message)
    record = await delivered(api, delivery.message_id)

    assert delivery.driver == "smtp"
    assert record["From"]["Address"] == "keel@example.com"
    assert [r["Address"] for r in record["To"]] == ["ada@example.com"]
    assert [r["Address"] for r in record["Cc"]] == ["grace@example.com"]
    assert [r["Address"] for r in record["ReplyTo"]] == ["support@example.com"]
    assert record["Subject"] == "Welcome"
    assert "Hello Ada." in record["Text"]
    assert "<b>Ada</b>" in record["HTML"]
    # Mailpit records the envelope recipient the header did not name.
    assert [r["Address"] for r in record["Bcc"]] == ["ops@example.com"]


@pytest.mark.mailpit
async def test_the_facade_over_smtp_fills_the_sender(mailpit: tuple[str, int, str]) -> None:
    host, port, api = mailpit
    async with mail_lifespan(
        MailConfig(driver="smtp", host=host, port=port, sender="noreply@example.com")
    ):
        delivery = await send(Message("ada@example.com", "Plain", "Just text."))
    record = await delivered(api, delivery.message_id)
    assert record["From"]["Address"] == "noreply@example.com"
    assert record["HTML"] == ""


async def test_an_unreachable_server_is_a_delivery_error() -> None:
    driver = SmtpMailer(MailConfig(driver="smtp", host="127.0.0.1", port=1, timeout=2))
    with pytest.raises(MailDeliveryError, match=re.escape("127.0.0.1:1")) as caught:
        await driver.send(note())
    assert caught.value.permanent is False


@pytest.mark.mailpit
async def test_a_display_name_sender_still_mints_a_matching_message_id(
    smtp: tuple[SmtpMailer, str],
) -> None:
    """The id is minted from the address inside the angle brackets, not the display form."""
    driver, api = smtp

    delivery = await driver.send(note(sender="Ada Lovelace <ada@example.com>"))
    record = await delivered(api, delivery.message_id)

    assert delivery.message_id.endswith("@example.com>")
    assert f"<{record['MessageID']}>" == delivery.message_id


# -- smtp, against a server that refuses -----------------------------------------------


class StubSmtp:
    """The least SMTP that lets a test choose the answer per recipient.

    Runs on a thread of its own, since the driver talks to it from one.
    """

    def __init__(self, answers: dict[str, str]) -> None:
        self.answers = answers
        self.envelopes: list[tuple[str, ...]] = []
        self._server = socketserver.TCPServer(("127.0.0.1", 0), self._handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    def __enter__(self) -> StubSmtp:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._server.shutdown()
        self._server.server_close()

    def _handler(self) -> type[socketserver.StreamRequestHandler]:
        stub = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                self.wfile.write(b"220 stub\r\n")
                accepted: list[str] = []
                while line := self.rfile.readline():
                    verb, _, rest = line.decode().rstrip("\r\n").partition(" ")
                    match verb.upper():
                        case "EHLO" | "HELO":
                            self.wfile.write(b"250 stub\r\n")
                        case "MAIL":
                            self.wfile.write(b"250 ok\r\n")
                        case "RCPT":
                            address = rest.partition("<")[2].rstrip(">")
                            answer = stub.answers.get(address, "250 ok")
                            if answer.startswith("250"):
                                accepted.append(address)
                            self.wfile.write(f"{answer}\r\n".encode())
                        case "DATA":
                            self.wfile.write(b"354 go\r\n")
                            while self.rfile.readline() != b".\r\n":
                                pass
                            stub.envelopes.append(tuple(accepted))
                            self.wfile.write(b"250 queued\r\n")
                        case "QUIT":
                            self.wfile.write(b"221 bye\r\n")
                            return
                        case _:
                            self.wfile.write(b"250 ok\r\n")

        return Handler


async def test_a_partial_refusal_is_delivered_to_the_rest_and_reported() -> None:
    """Raising here would make a retry send the accepted recipient a second copy."""
    with StubSmtp({"bad@example.com": "550 no such user"}) as server:
        driver = SmtpMailer(MailConfig(driver="smtp", host="127.0.0.1", port=server.port))

        delivery = await driver.send(note(to=["ada@example.com", "bad@example.com"]))

    assert delivery.recipients == ("ada@example.com",)
    assert delivery.refused == {"bad@example.com": "550 no such user"}
    assert server.envelopes == [("ada@example.com",)]


@pytest.mark.parametrize(
    ("answer", "permanent"), [("550 no such user", True), ("451 try later", False)]
)
async def test_a_refusal_of_every_recipient_says_whether_a_retry_can_help(
    answer: str, permanent: bool
) -> None:
    with StubSmtp({"ada@example.com": answer}) as server:
        driver = SmtpMailer(MailConfig(driver="smtp", host="127.0.0.1", port=server.port))
        with pytest.raises(MailDeliveryError, match=re.escape("ada@example.com")) as caught:
            await driver.send(note())

    assert caught.value.permanent is permanent
    assert server.envelopes == []
