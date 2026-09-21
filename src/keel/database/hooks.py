"""Work that must not happen until the transaction commits.

Phase 2 gave model observers this guarantee. This generalises it, because
observers are not the only thing that needs it — dispatching a job is the other,
and there will be more.

The problem it solves is worth stating concretely. This is wrong:

    async with uow() as session:
        invoice = await Invoices(session).create(...)
        await dispatch(SendInvoiceEmail(invoice.id))   # pushed immediately
        await charge(invoice)                          # raises

The transaction rolls back, the invoice never existed, and a worker is already
emailing the customer about it. Worse, the worker probably wins the race: it
reads the invoice id, finds nothing, and dead-letters a job that looks like
corrupted data.

The fix is not to remember to move the dispatch after the block — people forget,
and reviewers do not notice. It is to make "after the block" the default
behaviour of dispatching inside one.

Two pieces make that possible:

* **A current-session context variable.** ``Database.transaction()`` publishes
  the session it opened, so code deeper in the call stack can discover that a
  transaction is in progress without it being threaded through every signature.
  A ``ContextVar`` rather than a global because concurrent requests each have
  their own, and asyncio tasks inherit context correctly.
* **A per-session callback buffer**, drained after the commit succeeds and
  discarded otherwise.

Application code does not normally touch either: it calls ``dispatch()``, which
consults them. This module is the mechanism, not the interface.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

type AfterCommit = Callable[[], Awaitable[None]]
"""A coroutine function to run once the surrounding transaction has committed."""

logger = logging.getLogger("keel.database")

CALLBACKS_KEY: Final = "keel_after_commit"
"""Key under which pending callbacks live on ``Session.info``.

On the session rather than in a module-level list because two sessions may be
open at once, and one rolling back must not discard the other's work.
"""

_active_session: ContextVar[AsyncSession | None] = ContextVar("keel_active_session", default=None)


def current_session() -> AsyncSession | None:
    """Return the session of the innermost open transaction, if any.

    Returns:
        The active session, or ``None`` when called outside a unit of work.
    """
    return _active_session.get()


def in_transaction() -> bool:
    """Whether the caller is inside a unit of work.

    Returns:
        ``True`` if a transaction is open in this task's context.
    """
    return _active_session.get() is not None


def after_commit(callback: AfterCommit, session: AsyncSession | None = None) -> bool:
    """Run *callback* once the current transaction commits.

    Args:
        callback: A coroutine function taking no arguments.
        session: The session to attach to; defaults to the active one.

    Returns:
        ``True`` if the callback was deferred, ``False`` if there is no
        transaction to defer until — in which case the caller should do the work
        immediately rather than silently dropping it.
    """
    target = session or current_session()
    if target is None:
        return False
    pending: list[AfterCommit] = target.info.setdefault(CALLBACKS_KEY, [])
    pending.append(callback)
    return True


def take_after_commit(session: AsyncSession) -> list[AfterCommit]:
    """Remove and return the callbacks buffered on *session*.

    Args:
        session: The session to drain.

    Returns:
        The callbacks, in the order they were registered.
    """
    pending: list[AfterCommit] = session.info.pop(CALLBACKS_KEY, [])
    return pending


async def run_after_commit(
    session: AsyncSession,
    on_error: Callable[[BaseException], None] | None = None,
) -> int:
    """Run every callback buffered on *session*.

    Called by :meth:`keel.database.engine.Database.transaction` once the commit
    has succeeded.

    A failing callback does not stop the others, for the same reason a failing
    observer does not: the transaction is already durable, so abandoning the
    rest would leave some side effects applied and some not, with no way to
    retry the remainder.

    Args:
        session: The session whose callbacks should run.
        on_error: Called with any exception a callback raises. When omitted,
            the failure is logged with its traceback on ``keel.database``: a
            dropped dispatch or mail would be invisible otherwise.

    Returns:
        The number of callbacks run.
    """
    pending = take_after_commit(session)
    for callback in pending:
        try:
            await callback()
        except Exception as exc:  # one callback must not break the rest
            if on_error is None:
                logger.exception("after-commit callback %r failed; the commit stands", callback)
            else:
                on_error(exc)
    return len(pending)


def publish_session(session: AsyncSession) -> object:
    """Make *session* the active one for this context.

    Args:
        session: The session opened by a unit of work.

    Returns:
        A token to pass to :func:`withdraw_session`.
    """
    return _active_session.set(session)


def withdraw_session(token: object) -> None:
    """Restore whatever session was active before :func:`publish_session`.

    Restoring rather than clearing, so nested units of work — a service calling
    another service that opens its own — leave the outer one active.

    Args:
        token: The token returned by :func:`publish_session`.
    """
    _active_session.reset(token)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


__all__ = [
    "CALLBACKS_KEY",
    "AfterCommit",
    "after_commit",
    "current_session",
    "in_transaction",
    "publish_session",
    "run_after_commit",
    "take_after_commit",
    "withdraw_session",
]
