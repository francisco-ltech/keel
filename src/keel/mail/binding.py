"""Where the process finds its mailer, and the one call application code makes.

Two layers from :class:`~keel.support.binding.Binding`, for the reasons the
token store gives: a plain attribute for the process-wide default, because a
lifespan runs in a different task from the handlers; a ``ContextVar`` override
so concurrent tests cannot see each other's outbox.

:func:`send` is a **Virtual Proxy** over the default mailer, the role
:func:`keel.queue.dispatch.dispatch` plays for the queue. It resolves the
binding per call, fills in the configured sender when the message named none,
and announces the delivery on the manager's dispatcher — so an observer can
count mail without any driver knowing observers exist.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass

from keel.contracts.mail import Mailer
from keel.mail.config import MailConfig
from keel.mail.manager import MailManager
from keel.mail.message import Delivery, Message
from keel.support.binding import Binding
from keel.support.events import EventDispatcher

_binding: Binding[MailManager] = Binding(
    "mail manager",
    "wrap the work in `async with mail_lifespan(config)`, call "
    "keel.mail.set_mail_manager(MailManager(MailConfig.from_env())) during startup, "
    "or use keel.testing.fake_mail() in a test",
)


@dataclass(frozen=True, slots=True)
class MailSent:
    """A message left the process, or was recorded as if it had.

    A fact, like the cache's and the worker's events: no bodies, no addresses
    beyond a count, because an event reaches log lines, traces and counters,
    none of which should hold a mailbox.

    Attributes:
        driver: Which mailer sent it.
        recipients: How many addresses it went to.
        subject: The subject line.
        message_id: The ``Message-ID`` it carries.
    """

    driver: str
    recipients: int
    subject: str
    message_id: str


def mail_manager() -> MailManager:
    """Return the manager in effect.

    Returns:
        The context-local override if one is active, otherwise the process-wide
        manager.

    Raises:
        ConfigurationError: If nothing is bound.
    """
    return _binding.current()


def bound_mail_manager() -> MailManager | None:
    """Return the process-wide manager without raising.

    Returns:
        The bound manager, or ``None``.
    """
    return _binding.peek()


def set_mail_manager(manager: MailManager | None) -> None:
    """Install the process-wide manager.

    Args:
        manager: The manager to install, or ``None`` to unbind.
    """
    _binding.set(manager)


@contextmanager
def use_mail_manager(manager: MailManager) -> Iterator[MailManager]:
    """Override the bound manager for the duration of a block.

    Args:
        manager: The manager to use.

    Yields:
        The manager now in effect.
    """
    with _binding.use(manager) as bound:
        yield bound


def mailer(name: str | None = None) -> Mailer:
    """Return a mailer from the bound manager.

    Args:
        name: The driver name, or ``None`` for the configured default.

    Returns:
        The mailer.

    Raises:
        ConfigurationError: If no manager is bound, or the driver is unknown.
    """
    return mail_manager().mailer(name)


async def send(message: Message, *, connection: str | None = None) -> Delivery:
    """Send a message through the bound mailer.

    The sender is filled in from the configuration when the message named
    none, so application code builds a message once and the deployment
    decides who it is from.

    Args:
        message: What to send.
        connection: Which mailer to use, or ``None`` for the default.

    Returns:
        What was sent and under which ``Message-ID``.

    Raises:
        ConfigurationError: If no manager is bound.
        MailDeliveryError: If the transport refused it.
    """
    manager = mail_manager()
    resolved = message.with_sender(manager.config.sender)
    delivery = await manager.mailer(connection).send(resolved)
    events = manager.events
    if events is not None:
        await events.dispatch(
            MailSent(
                driver=delivery.driver,
                recipients=len(delivery.recipients),
                subject=resolved.subject,
                message_id=delivery.message_id,
            )
        )
    return delivery


@asynccontextmanager
async def mail_lifespan(
    config: MailConfig, events: EventDispatcher | None = None
) -> AsyncIterator[MailManager]:
    """Bind a mail manager for the life of the process.

    Framework-agnostic, like every other lifespan, and it restores whatever was
    bound before rather than unbinding.

    Args:
        config: How mail leaves the process.
        events: A dispatcher to announce deliveries on, or ``None`` for none.

    Yields:
        The bound manager.
    """
    previous = bound_mail_manager()
    manager = MailManager(config, events)
    set_mail_manager(manager)
    try:
        yield manager
    finally:
        await manager.close()
        set_mail_manager(previous)


__all__ = [
    "MailSent",
    "bound_mail_manager",
    "mail_lifespan",
    "mail_manager",
    "mailer",
    "send",
    "set_mail_manager",
    "use_mail_manager",
]
