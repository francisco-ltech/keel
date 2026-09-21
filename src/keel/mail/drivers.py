"""The mailers that need no server.

Two of them, and they are not the same kind of thing. :class:`LogMailer` is a
real deployment choice — the development run with no Mailpit, the staging
environment that must not email customers — that records who was mailed about
what. :class:`NullMailer` is a Null Object: it satisfies the interface and
deliberately not the behaviour, and is excluded from the contract suite for
the reason ``NullStore`` and ``NullQueue`` are.
"""

from __future__ import annotations

import logging

from keel.mail.message import Delivery, Message, new_message_id

logger = logging.getLogger(__name__)


class LogMailer:
    """Writes a line per message and delivers nothing.

    The line names the recipients, the subject and the ``Message-ID`` — never
    a body. A log is read by more people than a mailbox is, and a password
    reset link belongs in exactly one of them.

    Args:
        name: The driver name this mailer was built as.
    """

    __slots__ = ("_name",)

    def __init__(self, name: str = "log") -> None:
        self._name = name

    @property
    def name(self) -> str:
        """The driver name."""
        return self._name

    async def send(self, message: Message) -> Delivery:
        """Log the message's envelope.

        Args:
            message: What would have been sent.

        Returns:
            A delivery record, as if it had been.
        """
        message_id = new_message_id()
        # Rendered and dropped: a message SMTP could not send fails here too.
        message.as_mime(message_id)
        logger.info(
            "mail to %s from %s: %r (%s)",
            ", ".join(message.recipients),
            message.sender,
            message.subject,
            message_id,
        )
        return Delivery(message_id=message_id, recipients=message.recipients, driver=self._name)

    async def close(self) -> None:
        """Nothing to release."""


class NullMailer:
    """Discards every message.

    For a process that must never send — a data migration replaying events, a
    load test — and only for that. It still mints a ``Message-ID`` so a caller
    that logs the delivery has something to log.

    Args:
        name: The driver name this mailer was built as.
    """

    __slots__ = ("_name",)

    def __init__(self, name: str = "null") -> None:
        self._name = name

    @property
    def name(self) -> str:
        """The driver name."""
        return self._name

    async def send(self, message: Message) -> Delivery:
        """Discard the message.

        Args:
            message: Ignored.

        Returns:
            A delivery record for a delivery that did not happen.
        """
        message_id = new_message_id()
        message.as_mime(message_id)
        return Delivery(message_id=message_id, recipients=message.recipients, driver=self._name)

    async def close(self) -> None:
        """Nothing to release."""


__all__ = ["LogMailer", "NullMailer"]
