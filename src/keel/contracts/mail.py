"""Mail contracts.

One protocol: :class:`Mailer`. What varies between implementations is *how* a
message leaves the process — an SMTP conversation, a log line, nothing — and
not what a message is, which is exactly the variation a driver seam is for.

**There is no Bridge here**, for the reason the queue and the token store
declined one: a mailer's surface *is* the primitive, ``send``, and a second
layer over one method would be ceremony. What a Laravel ``Mailable`` adds —
building the message from a template — is the application's, and a
:class:`~keel.mail.message.Message` is what it builds.

Not ``runtime_checkable``, for the reason :mod:`keel.contracts.cache` gives:
``isinstance`` against a protocol checks attribute *names* only. The real
conformance check is ``tests/test_mail_contract.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from keel.mail.message import Delivery, Message


class Mailer(Protocol):
    """Sends one message, and says what it did with it.

    ``send`` either delivers the whole message to the transport or raises
    :class:`~keel.exceptions.MailDeliveryError`; it never returns having
    delivered to some recipients and not others without saying so in the
    error. The message it is given is already valid — recipients present,
    no header injection — because :class:`~keel.mail.message.Message` refuses
    to exist otherwise. The one thing a message may still lack is a sender,
    which the facade fills in; every driver renders the message, so one
    handed a senderless message directly refuses it, whichever driver it is.
    """

    @property
    def name(self) -> str:
        """The driver name this mailer was built as."""
        ...

    async def send(self, message: Message) -> Delivery:
        """Hand *message* to the transport.

        Args:
            message: What to send. ``sender`` is already resolved: the facade
                fills in the configured address when the message named none.

        Returns:
            What was sent and under which ``Message-ID``.

        Raises:
            MailDeliveryError: If the transport refused or could not be reached.
        """
        ...

    async def close(self) -> None:
        """Release anything the transport holds. Idempotent."""
        ...


__all__ = ["Mailer"]
