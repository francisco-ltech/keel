"""The SMTP mailer.

The standard library's :mod:`smtplib`, run on a worker thread, rather than an
async SMTP client. Sending mail is one short conversation per message, on a
process that is not a mail relay; the event loop is unblocked by
``anyio.to_thread.run_sync`` exactly as it is for Argon2, and the alternative
is a dependency whose whole value is a feature this does not need — a pooled,
pipelined connection. The day a service sends thousands of messages a minute
from one process, that dependency has a caller; today it would be ceremony.

One connection per message, closed afterwards. A connection held open across
messages has to survive the server's idle timeout, and the code to notice
that it did not is larger than the connect it saves.
"""

from __future__ import annotations

import smtplib
import ssl
from email.utils import parseaddr
from types import MappingProxyType
from typing import Final

import anyio

from keel.exceptions import MailDeliveryError
from keel.mail.config import MailConfig
from keel.mail.message import Delivery, Message, new_message_id

_REFUSED: Final = (smtplib.SMTPException, OSError)
"""What the transport raises: a refusal, a timeout, an unreachable host."""


class SmtpMailer:
    """Sends through an SMTP server.

    Args:
        config: Host, port, credentials and security mode.
        name: The driver name this mailer was built as.
    """

    __slots__ = ("_config", "_name")

    def __init__(self, config: MailConfig, name: str = "smtp") -> None:
        self._config = config
        self._name = name

    @property
    def name(self) -> str:
        """The driver name."""
        return self._name

    @property
    def config(self) -> MailConfig:
        """The configuration this mailer connects with."""
        return self._config

    async def send(self, message: Message) -> Delivery:
        """Deliver *message* over one SMTP conversation.

        Args:
            message: What to send, sender already resolved.

        Returns:
            What was sent and under which ``Message-ID``. A recipient the
            server refused while accepting the others is in ``refused``, not
            raised: the mail went out, and a retry would send it twice.

        Raises:
            MailDeliveryError: If the server refused every recipient, the
                sender or the data, the connection failed, or the conversation
                timed out. Names the server and the refused recipients, never
                a body. ``permanent`` is set when the server's answer was a
                5xx, which a retry will not change.
        """
        message_id = new_message_id(self._sender_domain(message))
        mime = message.as_mime(message_id)
        refused = await anyio.to_thread.run_sync(self._deliver, mime.as_bytes(), message)
        accepted = tuple(r for r in message.recipients if r not in refused)
        return Delivery(
            message_id=message_id,
            recipients=accepted,
            driver=self._name,
            refused=MappingProxyType(refused),
        )

    async def close(self) -> None:
        """Nothing is held between messages."""

    def _deliver(self, payload: bytes, message: Message) -> dict[str, str]:
        config = self._config
        where = f"{config.host}:{config.port}"
        try:
            with self._connect() as client:
                if config.username:
                    client.login(config.username, config.password or "")
                refused = client.sendmail(
                    message.sender or config.sender, list(message.recipients), payload
                )
        except smtplib.SMTPRecipientsRefused as exc:
            names = ", ".join(sorted(exc.recipients))
            raise MailDeliveryError(
                f"smtp {where} refused every recipient: {names}",
                permanent=all(_is_permanent(code) for code, _ in exc.recipients.values()),
            ) from exc
        except smtplib.SMTPResponseException as exc:
            raise MailDeliveryError(
                f"smtp {where} refused the message: {exc}", permanent=_is_permanent(exc.smtp_code)
            ) from exc
        except _REFUSED as exc:
            raise MailDeliveryError(f"smtp {where} refused the message: {exc}") from exc
        return {recipient: f"{code} {_text(reply)}" for recipient, (code, reply) in refused.items()}

    def _connect(self) -> smtplib.SMTP:
        config = self._config
        if config.security == "tls":
            return smtplib.SMTP_SSL(
                config.host,
                config.port,
                timeout=config.timeout,
                context=ssl.create_default_context(),
            )
        client = smtplib.SMTP(config.host, config.port, timeout=config.timeout)
        if config.security == "starttls":
            try:
                client.starttls(context=ssl.create_default_context())
            except BaseException:
                client.close()
                raise
        return client

    @staticmethod
    def _sender_domain(message: Message) -> str | None:
        # parseaddr, so "Ada <ada@example.com>" yields example.com and not "example.com>".
        _, address = parseaddr(message.sender or "")
        return address.rsplit("@", 1)[1] if "@" in address else None


def _is_permanent(code: int) -> bool:
    """A 5xx is the server's final word; a 4xx asks for a retry."""
    return 500 <= code < 600


def _text(reply: bytes | str) -> str:
    return reply.decode(errors="replace") if isinstance(reply, bytes) else reply


__all__ = ["SmtpMailer"]
