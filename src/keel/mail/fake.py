"""The mail test double.

A Test Spy that **records and does not deliver**, the way
:class:`~keel.queue.fake.FakeQueue` records and does not run. The cache's
trick — a Decorator over a real backend — works when the real behaviour is
cheap to have; delivering mail needs a server, and a test that asserts
"the welcome mail was sent" wants the message, not a Mailpit round trip.

    with fake_mail() as outbox:
        await users.create_user(payload)
    outbox.assert_sent(to="ada@example.com", subject_contains="Welcome")
"""

from __future__ import annotations

from keel.exceptions import KeelError
from keel.mail.message import Delivery, Message, new_message_id


class MailAssertionError(KeelError, AssertionError):
    """An expectation about sent mail did not hold.

    Both a Keel error and an ``AssertionError``, so a test framework reports
    it as a failed assertion rather than an error in the test.
    """


class FakeMailer:
    """Records every message it is asked to send.

    Args:
        name: The driver name this mailer was built as.
    """

    __slots__ = ("_name", "_sent")

    def __init__(self, name: str = "fake") -> None:
        self._name = name
        self._sent: list[Message] = []

    @property
    def name(self) -> str:
        """The driver name."""
        return self._name

    @property
    def sent(self) -> tuple[Message, ...]:
        """Every message sent so far, in order."""
        return tuple(self._sent)

    def reset(self) -> None:
        """Forget everything sent so far."""
        self._sent.clear()

    async def send(self, message: Message) -> Delivery:
        """Record the message.

        Args:
            message: What would have been sent.

        Returns:
            A delivery record, as if it had been.
        """
        message_id = new_message_id()
        # Rendered and dropped, so a test fails on what production would refuse.
        message.as_mime(message_id)
        self._sent.append(message)
        return Delivery(message_id=message_id, recipients=message.recipients, driver=self._name)

    async def close(self) -> None:
        """Nothing to release."""

    def _matching(self, to: str | None, subject_contains: str | None) -> list[Message]:
        return [
            message
            for message in self._sent
            if (to is None or to in message.recipients)
            and (subject_contains is None or subject_contains in message.subject)
        ]

    def _timeline(self) -> str:
        if not self._sent:
            return "nothing was sent"
        lines = [f"  {', '.join(m.recipients)}: {m.subject!r}" for m in self._sent]
        return "sent so far:\n" + "\n".join(lines)

    def assert_sent(self, *, to: str | None = None, subject_contains: str | None = None) -> Message:
        """Assert that a matching message was sent, and return the first one.

        Args:
            to: An address that must be among the recipients.
            subject_contains: Text the subject must contain.

        Returns:
            The first matching message, for further assertions on its body.

        Raises:
            MailAssertionError: If nothing matches.
        """
        found = self._matching(to, subject_contains)
        if not found:
            wanted = ", ".join(
                part
                for part in (
                    f"to {to}" if to else "",
                    f"subject containing {subject_contains!r}" if subject_contains else "",
                )
                if part
            )
            raise MailAssertionError(
                f"no message {wanted or 'at all'} was sent; {self._timeline()}"
            )
        return found[0]

    def assert_sent_times(
        self, times: int, *, to: str | None = None, subject_contains: str | None = None
    ) -> None:
        """Assert exactly *times* matching messages were sent.

        Args:
            times: The expected count.
            to: An address that must be among the recipients.
            subject_contains: Text the subject must contain.

        Raises:
            MailAssertionError: If the count differs.
        """
        found = self._matching(to, subject_contains)
        if len(found) != times:
            raise MailAssertionError(
                f"expected {times} matching message(s), found {len(found)}; {self._timeline()}"
            )

    def assert_nothing_sent(self) -> None:
        """Assert no message was sent at all.

        Raises:
            MailAssertionError: If anything was.
        """
        if self._sent:
            raise MailAssertionError(f"expected nothing to be sent; {self._timeline()}")


__all__ = ["FakeMailer", "MailAssertionError"]
