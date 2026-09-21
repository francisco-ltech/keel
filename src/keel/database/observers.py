"""Model lifecycle observers.

Laravel fires `created`, `updated` and `deleted` synchronously, inside the write.
Python cannot copy that directly: SQLAlchemy's mapper events are synchronous
functions called during flush, and there is no way to `await` inside one. An
observer that wants to send an email, enqueue a job or call an API — which is
what observers are *for* — has nothing it can do there.

So the events are **buffered during flush and dispatched after commit**. Two
consequences, both of them improvements over the synchronous version:

* Observers can be async, and do real work.
* An observer never sees a change that gets rolled back. Firing "user created"
  inside a transaction that later fails is how a welcome email arrives for an
  account that does not exist — a bug that is very hard to find and very easy to
  ship.

This is also the seam Phase 3 needs. "Dispatch this job after the transaction
commits" is the same mechanism with a different listener, so the queue will hook
in here rather than growing its own.

Soft deletes are classified, not reported literally. A soft delete is an UPDATE
at the SQL level, but an observer wants to hear `deleted`; setting `deleted_at`
back to `None` is also an UPDATE, and means `restored`. The buffer inspects
attribute history to tell the three apart.

    class Users(Observer[User]):
        async def created(self, user: User) -> None:
            await mail.send(Welcome(user))

    observe(User, Users())
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final, Literal

from sqlalchemy import event
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import instance_state

from keel.database.soft_delete import SoftDeleteMixin

logger = logging.getLogger("keel.database")

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

type Lifecycle = Literal["created", "updated", "deleted", "restored"]

PENDING_KEY: Final = "keel_pending_model_events"
"""Key under which buffered events live on ``Session.info``.

Stored on the session rather than in a module-level list because two sessions
may be flushing concurrently, and their events must not interleave.
"""


@dataclass(frozen=True, slots=True)
class ModelEvent:
    """Something that happened to a row, recorded at flush and delivered later.

    Attributes:
        lifecycle: Which of the four things happened.
        instance: The model instance. Still attached and usable, because
            dispatch happens before the session closes.
        changes: For ``updated``, the columns that changed, mapped to
            ``(old, new)``. Empty for the other lifecycles. Captured at flush
            because attribute history is gone after the commit.
    """

    lifecycle: Lifecycle
    instance: Any
    changes: Mapping[str, tuple[Any, Any]] = field(default_factory=dict)


class Observer[ModelT]:
    """Reacts to changes to one model.

    Subclass and override only the hooks you need; the rest are no-ops. Each
    receives the instance after the transaction has committed, so the change is
    durable by the time the observer runs.

    Warning:
        An observer runs *after* commit, which means it cannot veto a change and
        its own failure cannot roll one back. If work must be atomic with the
        write, it belongs in the service, inside the same unit of work — not
        here.
    """

    async def created(self, instance: ModelT) -> None:
        """Called after a new row is committed."""

    async def updated(self, instance: ModelT, changes: Mapping[str, tuple[Any, Any]]) -> None:
        """Called after an existing row is committed with changes.

        Args:
            instance: The updated row.
            changes: Columns that changed, mapped to ``(old, new)``.
        """

    async def deleted(self, instance: ModelT) -> None:
        """Called after a row is deleted, softly or otherwise."""

    async def restored(self, instance: ModelT) -> None:
        """Called after a soft-deleted row is brought back."""


_observers: dict[type, list[Observer[Any]]] = defaultdict(list)


def observe(model: type, observer: Observer[Any]) -> Callable[[], None]:
    """Register *observer* for changes to *model* and its subclasses.

    Args:
        model: The mapped class to watch.
        observer: The observer to notify.

    Returns:
        A function that removes this registration — returned rather than
        exposed as a ``forget`` method so a caller cannot accidentally remove
        someone else's observer.
    """
    _observers[model].append(observer)

    def unsubscribe() -> None:
        registered = _observers.get(model)
        if registered and observer in registered:
            registered.remove(observer)

    return unsubscribe


def observers_for(model: type) -> list[Observer[Any]]:
    """Return every observer watching *model*.

    Args:
        model: The mapped class.

    Returns:
        Observers registered against the class or any of its bases, so an
        observer on a base model sees its subclasses.
    """
    return [
        observer
        for registered, group in _observers.items()
        if issubclass(model, registered)
        for observer in group
    ]


def clear_observers() -> None:
    """Remove every registration. Intended for test teardown."""
    _observers.clear()


def _changes_of(instance: object) -> dict[str, tuple[Any, Any]]:
    """Return the columns modified on *instance*, as ``{name: (old, new)}``.

    Read during flush because SQLAlchemy discards attribute history once the
    transaction commits — by the time an observer runs, "what changed" is no
    longer answerable, so it has to be captured here.
    """
    state = instance_state(instance)
    changed: dict[str, tuple[Any, Any]] = {}
    for attribute in state.mapper.column_attrs:
        history = state.attrs[attribute.key].history
        if not history.has_changes():
            continue
        before = history.deleted[0] if history.deleted else None
        after = history.added[0] if history.added else None
        changed[attribute.key] = (before, after)
    return changed


def _classify_update(instance: object, changes: Mapping[str, tuple[Any, Any]]) -> Lifecycle:
    """Decide whether an UPDATE is really a delete, a restore, or an update.

    A soft delete is an UPDATE that sets ``deleted_at``; a restore is one that
    clears it. Reporting either as ``updated`` would make observers reconstruct
    the distinction from the diff, every time, and get it wrong occasionally.
    """
    if not isinstance(instance, SoftDeleteMixin) or "deleted_at" not in changes:
        return "updated"
    before, after = changes["deleted_at"]
    if before is None and after is not None:
        return "deleted"
    if before is not None and after is None:
        return "restored"
    return "updated"


def _buffer(session: Session, occurrence: ModelEvent) -> None:
    """Append an event to this session's pending list."""
    pending: list[ModelEvent] = session.info.setdefault(PENDING_KEY, [])
    pending.append(occurrence)


@event.listens_for(Session, "after_flush")
def _record_changes(session: Session, _context: Any) -> None:
    """Capture what the flush is about to write.

    ``after_flush`` rather than ``before_flush``: by this point SQLAlchemy has
    decided exactly which objects are being inserted, updated and deleted, but
    the attribute history is still intact. Neither is true in both directions at
    any other point in the cycle.

    Args:
        session: The flushing session.
        _context: SQLAlchemy's flush context, unused.
    """
    for instance in session.new:
        if observers_for(type(instance)):
            _buffer(session, ModelEvent("created", instance))

    for instance in session.dirty:
        if not session.is_modified(instance, include_collections=False):
            continue
        if not observers_for(type(instance)):
            continue
        changes = _changes_of(instance)
        if not changes:
            continue
        _buffer(session, ModelEvent(_classify_update(instance, changes), instance, changes))

    for instance in session.deleted:
        if observers_for(type(instance)):
            _buffer(session, ModelEvent("deleted", instance))


def take_pending(session: AsyncSession | Session) -> list[ModelEvent]:
    """Remove and return the events buffered on *session*.

    Args:
        session: The session to drain. Accepts either flavour, since the async
            session delegates ``info`` to the sync one it wraps.

    Returns:
        The buffered events, in the order they were recorded.
    """
    pending: list[ModelEvent] = session.info.pop(PENDING_KEY, [])
    return pending


async def dispatch_pending(
    session: AsyncSession | Session,
    on_error: Callable[[BaseException, ModelEvent], None] | None = None,
) -> int:
    """Deliver every buffered event to its observers.

    Called by :meth:`keel.database.engine.Database.transaction` once the commit
    has succeeded. Application code should not need to call it.

    An observer that raises must not prevent the others from running: the write
    is already committed, so aborting here would leave some observers run and
    some not, with no way to retry the rest. Failures go to *on_error*.

    Args:
        session: The session whose events should be delivered.
        on_error: Called with the exception and the event it came from. When
            omitted, the failure is logged with its traceback on
            ``keel.database``, naming the observer and the event.

    Returns:
        The number of events delivered.
    """
    pending = take_pending(session)
    for occurrence in pending:
        for observer in observers_for(type(occurrence.instance)):
            try:
                await _deliver(observer, occurrence)
            except Exception as exc:  # one observer must not break the rest
                if on_error is None:
                    logger.exception(
                        "observer %r failed on %s of %s; the commit stands",
                        observer,
                        occurrence.lifecycle,
                        type(occurrence.instance).__name__,
                    )
                else:
                    on_error(exc, occurrence)
    return len(pending)


async def _deliver(observer: Observer[Any], occurrence: ModelEvent) -> None:
    """Route one event to the matching hook on one observer."""
    match occurrence.lifecycle:
        case "created":
            await observer.created(occurrence.instance)
        case "updated":
            await observer.updated(occurrence.instance, occurrence.changes)
        case "deleted":
            await observer.deleted(occurrence.instance)
        case "restored":
            await observer.restored(occurrence.instance)


__all__ = [
    "Lifecycle",
    "ModelEvent",
    "Observer",
    "clear_observers",
    "dispatch_pending",
    "observe",
    "observers_for",
    "take_pending",
]
