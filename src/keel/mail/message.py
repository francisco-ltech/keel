"""What a message is, and what it renders to.

A :class:`Message` is a **value object**: frozen, validated on construction,
and the only thing a :class:`~keel.contracts.mail.Mailer` accepts. Two things
are checked here rather than left to a driver, so that every driver — the
real one, the logging one, the recording fake — refuses the same input:

* **At least one recipient.** A message to nobody is a bug at the call site,
  not something a transport should quietly accept.
* **No line break in any address or the subject.** A header with a newline in
  it is header injection: an address followed by a newline and ``Bcc:`` is
  one more recipient, courtesy of whoever typed the form field. Refused with
  :class:`~keel.exceptions.InvalidMessageError` before anything is built.

The MIME rendering is the standard library's :class:`email.message.EmailMessage`,
which gets encoding, multipart alternatives and folding right in ways a
hand-built string does not. Text is mandatory and HTML is an alternative to
it, never a replacement: a client that cannot show HTML still reads the mail.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from email.message import EmailMessage
from email.utils import make_msgid
from types import MappingProxyType
from typing import Final

from keel.exceptions import InvalidMessageError

FORBIDDEN_IN_HEADERS: Final = ("\r", "\n")
"""What no header value may contain. Either one ends a header and starts another."""

RESERVED_HEADERS: Final[frozenset[str]] = frozenset(
    {
        "from",
        "to",
        "cc",
        "bcc",
        "subject",
        "reply-to",
        "message-id",
        "date",
        "mime-version",
        "content-type",
        "content-transfer-encoding",
        "content-disposition",
    }
)
"""Headers the rendering writes itself, so ``headers=`` may not.

A ``Bcc`` smuggled in here would go on the wire; a second ``To`` would make the
renderer raise on one driver and pass on the rest.
"""

_EMPTY: Final[Mapping[str, str]] = MappingProxyType({})


def _addresses(value: str | Sequence[str] | None, field_name: str) -> tuple[str, ...]:
    """Normalise one address or several to a tuple, refusing header injection.

    Args:
        value: A single address, a sequence of them, or ``None``.
        field_name: Which field, for the error.

    Returns:
        The addresses, stripped.

    Raises:
        InvalidMessageError: If an address is empty or contains a line break.
    """
    if value is None:
        return ()
    items = (value,) if isinstance(value, str) else tuple(value)
    cleaned: list[str] = []
    for item in items:
        # Checked before stripping: a trailing newline is still an injection attempt.
        if any(char in item for char in FORBIDDEN_IN_HEADERS):
            raise InvalidMessageError(f"{field_name} contains a line break: header injection")
        address = item.strip()
        if not address:
            raise InvalidMessageError(f"{field_name} contains an empty address")
        cleaned.append(address)
    return tuple(cleaned)


@dataclass(frozen=True, slots=True)
class Message:
    """One email, ready to send.

    Attributes:
        to: The recipients. One address or several; at least one.
        subject: The subject line, single-line.
        text: The plain-text body. Mandatory, see the module docstring.
        html: An HTML alternative to ``text``, or ``None``.
        sender: The ``From`` address, or ``None`` to take the configured one.
        reply_to: Where replies go, if not to the sender.
        cc: Carbon copies.
        bcc: Blind copies: sent to, never written into a header.
        headers: Extra headers, for a list id or a tracking key. Values are
            checked for line breaks like every other header, and a name the
            message writes itself — ``To``, ``Bcc``, ``Subject`` and the rest
            of :data:`RESERVED_HEADERS` — is refused.
    """

    to: tuple[str, ...]
    subject: str
    text: str
    html: str | None = None
    sender: str | None = None
    reply_to: str | None = None
    cc: tuple[str, ...] = ()
    bcc: tuple[str, ...] = ()
    headers: Mapping[str, str] = field(default_factory=lambda: _EMPTY)

    def __init__(
        self,
        to: str | Sequence[str],
        subject: str,
        text: str,
        *,
        html: str | None = None,
        sender: str | None = None,
        reply_to: str | None = None,
        cc: str | Sequence[str] | None = None,
        bcc: str | Sequence[str] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Build and validate a message.

        Raises:
            InvalidMessageError: If there is no recipient, the subject is empty
                or multi-line, an address is malformed, a header contains a
                line break, or a header names one the message writes itself.
        """
        recipients = _addresses(to, "to")
        if not recipients:
            raise InvalidMessageError("a message needs at least one recipient")
        if not subject.strip():
            raise InvalidMessageError("a message needs a subject")
        if any(char in subject for char in FORBIDDEN_IN_HEADERS):
            raise InvalidMessageError("subject contains a line break: header injection")
        extra = dict(headers or {})
        for name, value in extra.items():
            if any(char in name + value for char in FORBIDDEN_IN_HEADERS):
                raise InvalidMessageError(f"header {name!r} contains a line break")
            if name.strip().lower() in RESERVED_HEADERS:
                raise InvalidMessageError(
                    f"header {name!r} is written by the message itself; use its field"
                )
        object.__setattr__(self, "to", recipients)
        object.__setattr__(self, "subject", subject.strip())
        object.__setattr__(self, "text", text)
        object.__setattr__(self, "html", html)
        object.__setattr__(self, "sender", _addresses(sender, "sender")[0] if sender else None)
        object.__setattr__(
            self, "reply_to", _addresses(reply_to, "reply_to")[0] if reply_to else None
        )
        object.__setattr__(self, "cc", _addresses(cc, "cc"))
        object.__setattr__(self, "bcc", _addresses(bcc, "bcc"))
        object.__setattr__(self, "headers", MappingProxyType(extra))

    @property
    def recipients(self) -> tuple[str, ...]:
        """Everyone the transport delivers to: ``to``, ``cc`` and ``bcc``, in that order."""
        return self.to + self.cc + self.bcc

    def with_sender(self, sender: str) -> Message:
        """Return this message with *sender* filled in, if it named none.

        Args:
            sender: The configured ``From`` address.

        Returns:
            This message if it already had a sender, otherwise a copy with one.
        """
        if self.sender is not None:
            return self
        return Message(
            self.to,
            self.subject,
            self.text,
            html=self.html,
            sender=sender,
            reply_to=self.reply_to,
            cc=self.cc,
            bcc=self.bcc,
            headers=self.headers,
        )

    def as_mime(self, message_id: str) -> EmailMessage:
        """Render to a MIME message the transport can write out.

        Args:
            message_id: The ``Message-ID`` to stamp, minted by the sender so a
                delivery record and the mail agree.

        Returns:
            The rendered message. ``Bcc`` is deliberately absent from it.

        Raises:
            InvalidMessageError: If no sender is set; the facade fills one in,
                so reaching here without one means a driver was called directly
                with a message that never went through it.
        """
        if self.sender is None:
            raise InvalidMessageError("a message needs a sender before it can be rendered")
        mime = EmailMessage()
        mime["From"] = self.sender
        mime["To"] = ", ".join(self.to)
        if self.cc:
            mime["Cc"] = ", ".join(self.cc)
        if self.reply_to:
            mime["Reply-To"] = self.reply_to
        mime["Subject"] = self.subject
        mime["Message-ID"] = message_id
        for name, value in self.headers.items():
            mime[name] = value
        mime.set_content(self.text)
        if self.html is not None:
            mime.add_alternative(self.html, subtype="html")
        return mime


@dataclass(frozen=True, slots=True)
class Delivery:
    """What a mailer did with a message.

    Attributes:
        message_id: The ``Message-ID`` the mail carries, for a log line or a
            support ticket to quote.
        recipients: Everyone it was handed to, blind copies included.
        driver: Which mailer sent it.
        refused: Recipients the server turned away after accepting the rest,
            with its reason for each. The message *was* delivered to
            ``recipients``, so this is reported rather than raised: a retry
            would send the accepted ones a second copy.
    """

    message_id: str
    recipients: tuple[str, ...]
    driver: str
    refused: Mapping[str, str] = field(default_factory=lambda: _EMPTY)


def new_message_id(domain: str | None = None) -> str:
    """Mint a ``Message-ID``.

    Args:
        domain: The domain part, or ``None`` for this host's name.

    Returns:
        A globally unique id in angle brackets, as the header wants it.
    """
    return make_msgid(domain=domain)


__all__ = ["FORBIDDEN_IN_HEADERS", "RESERVED_HEADERS", "Delivery", "Message", "new_message_id"]
