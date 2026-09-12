"""Soft deletes, applied as a global query scope.

Marking a row deleted instead of removing it is easy. Making every query
remember to filter it out is not — and a single forgotten ``WHERE deleted_at IS
NULL`` shows deleted data to a user, which is the failure mode that makes teams
abandon soft deletes entirely.

So the filter is not something callers apply; it is applied for them. A
``do_orm_execute`` listener rewrites every SELECT that touches a model
inheriting :class:`SoftDeleteMixin`, adding the criterion through
``with_loader_criteria`` so it reaches relationship loads and eager loads too,
not just the top-level entity.

Opting out is explicit and reads as such::

    from keel.database.soft_delete import with_deleted

    statement = with_deleted(select(User).where(User.email == email))

The listener is registered once, on :class:`~sqlalchemy.orm.Session`, at import
time. That is a process-wide effect and worth being deliberate about: it applies
to *any* session in the process, including ones Keel did not create. It is
scoped to entities that inherit the mixin, so a model that does not opt in is
untouched.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import DateTime, event
from sqlalchemy.orm import Mapped, ORMExecuteState, Session, mapped_column, with_loader_criteria

from keel.database.model import utcnow

if TYPE_CHECKING:
    from sqlalchemy.sql import Executable

INCLUDE_DELETED: Final = "keel_include_deleted"
"""Execution-option key that disables the global filter for one statement."""


class SoftDeleteMixin:
    """Gives a model a ``deleted_at`` column and removes it from normal queries.

    Rows are hidden, not removed, and the hiding is automatic — see the module
    docstring. Indexed because every query against the table now filters on it.

    Note:
        A unique constraint and soft deletes interact badly: deleting a user
        with a given email leaves the row in place, so re-registering that email
        violates the constraint. If a column is unique *and* the table is soft
        deletable, make the constraint partial —
        ``Index(..., unique=True, postgresql_where=deleted_at.is_(None))`` — or
        include ``deleted_at`` in it. This is the single most common way soft
        deletes go wrong in production.
    """

    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
        nullable=True,
        index=True,
    )

    @property
    def is_deleted(self) -> bool:
        """Whether this row is currently soft deleted."""
        return self.deleted_at is not None

    def soft_delete(self) -> None:
        """Mark this row deleted, hiding it from subsequent queries."""
        self.deleted_at = utcnow()

    def restore(self) -> None:
        """Un-delete this row."""
        self.deleted_at = None


def with_deleted[StatementT: Executable](statement: StatementT) -> StatementT:
    """Return *statement* with the soft-delete filter disabled.

    Args:
        statement: Any executable statement.

    Returns:
        The same statement carrying the opt-out execution option.
    """
    return statement.execution_options(**{INCLUDE_DELETED: True})


def only_deleted_option() -> Any:
    """Return a loader option that shows *only* soft-deleted rows.

    For an administrative "recycle bin" view. Combine with :func:`with_deleted`,
    since this replaces the criterion rather than removing it.

    Returns:
        A loader option to pass to ``.options(...)``.
    """
    return with_loader_criteria(
        SoftDeleteMixin,
        lambda cls: cls.deleted_at.is_not(None),
        include_aliases=True,
    )


@event.listens_for(Session, "do_orm_execute")
def _exclude_soft_deleted(state: ORMExecuteState) -> None:
    """Add the soft-delete criterion to every ORM SELECT that needs one.

    Three cases are skipped, and each one matters:

    * Non-SELECT statements. An UPDATE or DELETE that a caller wrote explicitly
      should do what it says.
    * Column loads — the refresh of an already-loaded object's attributes.
      Filtering there would make refreshing a soft-deleted instance fail rather
      than return its columns, which is not what "hidden from queries" should
      mean once you already hold the object.
    * Statements carrying the opt-out option.

    Args:
        state: SQLAlchemy's description of the statement about to execute.
    """
    if not state.is_select or state.is_column_load:
        return
    if state.execution_options.get(INCLUDE_DELETED, False):
        return

    state.statement = state.statement.options(
        with_loader_criteria(
            SoftDeleteMixin,
            lambda cls: cls.deleted_at.is_(None),
            include_aliases=True,
        )
    )


__all__ = ["INCLUDE_DELETED", "SoftDeleteMixin", "only_deleted_option", "with_deleted"]
