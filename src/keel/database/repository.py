"""The generic repository.

The problem it solves is arithmetic. The upstream FastAPI template hand-writes
`get`, `get_by_x`, `list`, `create`, `update` and `delete` for every model —
roughly 150 lines that are 90% identical between tables and drift apart the
moment someone fixes a bug in one copy. Ten models is fifteen hundred lines of
code nobody reads and everybody maintains.

A repository per model still exists, because each one has real queries that are
its own. What it no longer contains is the six methods every model needs::

    class Users(Repository[User]):
        model = User

        async def by_email(self, email: str) -> User | None:
            return await self.first(User.email == email)

That subclass has `get`, `get_or_fail`, `list`, `paginate`, `create`, `delete`,
`restore`, `count` and `exists` for free, and one method of its own that is
actually about users.

Two boundaries are deliberate and worth keeping.

**A repository takes a session; it does not open one.** Transaction scope is the
service's decision — see ADR 0002 — and a repository that opened its own
transaction would make it impossible for two repositories to participate in one.

**A repository returns ORM objects, not schemas.** Converting to a wire format
is the service's job. A repository that returned schemas could not be composed:
the caller could no longer modify what it fetched.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, cast

from sqlalchemy import ColumnExpressionArgument, CursorResult, func, literal, select
from sqlalchemy import delete as sql_delete
from sqlalchemy.orm import selectinload

from keel.database.model import Model
from keel.database.pagination import Page, clamp_limit, decode_cursor, encode_cursor
from keel.database.soft_delete import SoftDeleteMixin, with_deleted
from keel.exceptions import RecordNotFoundError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.sql import Select


class Repository[ModelT: Model]:
    """Data access for one model.

    Subclasses declare the model they serve::

        class Users(Repository[User]):
            model = User

    Args:
        session: The session to operate in, supplied by the caller's unit of
            work. The repository never commits — the transaction belongs to the
            service that opened it.
    """

    model: ClassVar[type[Any]]
    """The mapped class this repository serves. Set it in the subclass body."""

    __slots__ = ("_session",)

    def __init__(self, session: AsyncSession) -> None:
        cls = type(self)
        if not hasattr(cls, "model"):
            raise TypeError(
                f"{cls.__name__} must set a `model` class attribute, "
                f"e.g. `class {cls.__name__}(Repository[User]): model = User`"
            )
        if not hasattr(cls.model, "id"):
            raise TypeError(
                f"{cls.model.__name__} has no `id` column; Repository addresses rows "
                f"and paginates by primary key, so the model must mix in UUIDPrimaryKey"
            )
        self._session = session

    @staticmethod
    def identifier_of(instance: Any) -> uuid.UUID:
        """Return an instance's primary key.

        Exists because the type parameter is bound to :class:`Model`, which
        knows nothing about primary keys — the ``id`` column comes from a mixin.
        Rather than widen the bound and force every model to inherit a combined
        base, the requirement is checked once in ``__init__`` and read through
        here.

        Args:
            instance: A row belonging to this repository's model.

        Returns:
            Its primary key.
        """
        return cast("uuid.UUID", instance.id)

    @property
    def session(self) -> AsyncSession:
        """The session this repository is operating in."""
        return self._session

    @property
    def is_soft_deletable(self) -> bool:
        """Whether this repository's model hides deleted rows instead of removing them."""
        return issubclass(self.model, SoftDeleteMixin)

    # -- building blocks --------------------------------------------------

    def query(self) -> Select[tuple[ModelT]]:
        """Return a SELECT for this model, as the starting point for a query.

        Every read below is built from this, and it is public so a subclass can
        build one that is not. Escape hatches matter: a repository that forces
        every query through its own vocabulary becomes the thing you work around.

        Returns:
            A select statement for this repository's model.
        """
        return select(self.model)

    @staticmethod
    def eager(*relationships: Any) -> tuple[Any, ...]:
        """Return loader options that fetch *relationships* up front.

        Under async SQLAlchemy a lazy load outside a session raises rather than
        quietly issuing a query, so relationships must be requested. That is a
        feature — it turns the N+1 query from a silent performance bug into an
        error — but it means the ergonomics of asking have to be good.

        Args:
            *relationships: Relationship attributes, e.g. ``User.items``.

        Returns:
            Loader options to pass to :meth:`list`, :meth:`get` or ``.options``.
        """
        return tuple(selectinload(relationship) for relationship in relationships)

    # -- reading ----------------------------------------------------------

    async def get(self, identifier: uuid.UUID, *options: Any) -> ModelT | None:
        """Fetch one row by primary key.

        Args:
            identifier: The primary key.
            *options: Loader options, e.g. from :meth:`eager`.

        Returns:
            The row, or ``None`` if it does not exist or is soft deleted.
        """
        statement = self.query().where(self.model.id == identifier)
        return await self.first_from(statement, *options)

    async def get_or_fail(self, identifier: uuid.UUID, *options: Any) -> ModelT:
        """Fetch one row by primary key, or raise.

        The common case at the edge of a service: the caller has an id from a
        URL and a missing row is a 404, not a branch to write by hand every time.

        Args:
            identifier: The primary key.
            *options: Loader options.

        Returns:
            The row.

        Raises:
            RecordNotFoundError: If no such row is visible.
        """
        found = await self.get(identifier, *options)
        if found is None:
            raise RecordNotFoundError(self.model.__name__, identifier)
        return found

    async def first(
        self,
        *criteria: ColumnExpressionArgument[bool],
        options: Sequence[Any] = (),
    ) -> ModelT | None:
        """Fetch the first row matching *criteria*.

        Args:
            *criteria: SQLAlchemy filter expressions.
            options: Loader options.

        Returns:
            The first match, or ``None``.
        """
        return await self.first_from(self.query().where(*criteria), *options)

    async def first_or_fail(
        self,
        *criteria: ColumnExpressionArgument[bool],
        options: Sequence[Any] = (),
    ) -> ModelT:
        """Fetch the first row matching *criteria*, or raise.

        Args:
            *criteria: SQLAlchemy filter expressions.
            options: Loader options.

        Returns:
            The first match.

        Raises:
            RecordNotFoundError: If nothing matches.
        """
        found = await self.first(*criteria, options=options)
        if found is None:
            raise RecordNotFoundError(self.model.__name__, criteria)
        return found

    async def first_from(self, statement: Select[tuple[ModelT]], *options: Any) -> ModelT | None:
        """Execute a statement and return its first row.

        Args:
            statement: A select built from :meth:`query` or by hand.
            *options: Loader options.

        Returns:
            The first row, or ``None``.
        """
        if options:
            statement = statement.options(*options)
        result = await self._session.execute(statement.limit(1))
        return result.scalars().first()

    async def list(
        self,
        *criteria: ColumnExpressionArgument[bool],
        order_by: Any = None,
        limit: int | None = None,
        options: Sequence[Any] = (),
    ) -> Sequence[ModelT]:
        """Fetch rows matching *criteria*.

        Note:
            Unbounded by default, which is correct for a repository — the caller
            knows whether it is fetching three rows or three million. Anything
            reaching an HTTP response should use :meth:`paginate` instead.

        Args:
            *criteria: SQLAlchemy filter expressions.
            order_by: Ordering, defaulting to primary key (creation order, since
                keys are time-ordered).
            limit: Optional cap.
            options: Loader options.

        Returns:
            The matching rows.
        """
        statement = (
            self.query()
            .where(*criteria)
            .order_by(order_by if order_by is not None else self.model.id)
        )
        if limit is not None:
            statement = statement.limit(limit)
        if options:
            statement = statement.options(*options)
        result = await self._session.execute(statement)
        return result.scalars().all()

    async def paginate(
        self,
        *criteria: ColumnExpressionArgument[bool],
        cursor: str | None = None,
        limit: int | None = None,
        descending: bool = False,
        options: Sequence[Any] = (),
    ) -> Page[ModelT]:
        """Fetch one keyset-paginated page.

        Fetches one row more than requested to decide whether a next page
        exists, then discards it — which is why there is no count query here.
        See :mod:`keel.database.pagination` for why this is not OFFSET.

        Args:
            *criteria: SQLAlchemy filter expressions.
            cursor: The cursor from the previous page, or ``None`` to start.
            limit: Page size, clamped to a sane maximum.
            descending: Newest first when ``True``.
            options: Loader options.

        Returns:
            A page of rows and the cursor for the next one.

        Raises:
            InvalidCursorError: If *cursor* is malformed.
        """
        size = clamp_limit(limit)
        key = self.model.id
        statement = self.query().where(*criteria)

        if cursor is not None:
            position = decode_cursor(cursor)
            statement = statement.where(key < position if descending else key > position)

        statement = statement.order_by(key.desc() if descending else key.asc())
        if options:
            statement = statement.options(*options)

        result = await self._session.execute(statement.limit(size + 1))
        rows = list(result.scalars().all())

        has_more = len(rows) > size
        items = rows[:size]
        next_cursor = encode_cursor(self.identifier_of(items[-1])) if has_more and items else None
        return Page(items=items, next_cursor=next_cursor, limit=size)

    async def count(self, *criteria: ColumnExpressionArgument[bool]) -> int:
        """Count rows matching *criteria*.

        Args:
            *criteria: SQLAlchemy filter expressions.

        Returns:
            The number of matching rows.
        """
        statement = select(func.count()).select_from(self.model).where(*criteria)
        result = await self._session.execute(statement)
        return int(result.scalar_one())

    async def exists(self, *criteria: ColumnExpressionArgument[bool]) -> bool:
        """Whether any row matches *criteria*.

        Cheaper than :meth:`count` — it stops at the first match rather than
        scanning every one to produce a number the caller discards.

        Args:
            *criteria: SQLAlchemy filter expressions.

        Returns:
            ``True`` if at least one row matches.
        """
        statement = select(literal(1)).select_from(self.model).where(*criteria).limit(1)
        result = await self._session.execute(statement)
        return result.first() is not None

    # -- writing ----------------------------------------------------------

    def add(self, instance: ModelT) -> ModelT:
        """Stage an instance for insertion.

        Synchronous, because it is: nothing reaches the database until the unit
        of work flushes. The instance already has its primary key.

        Args:
            instance: The row to insert.

        Returns:
            The same instance, for chaining.
        """
        self._session.add(instance)
        return instance

    async def create(self, **values: Any) -> ModelT:
        """Build, stage and flush a new row.

        Flushes so that database-side constraints fail *here*, attributable to
        this call, rather than at the end of the transaction where the traceback
        points at the commit and not at the cause.

        Args:
            **values: Column values.

        Returns:
            The persisted instance.
        """
        instance: ModelT = self.model(**values)
        self._session.add(instance)
        await self._session.flush()
        return instance

    async def update(self, instance: ModelT, **values: Any) -> ModelT:
        """Apply values to an instance and flush.

        Warning:
            This assigns whatever it is given. Passing a request body straight
            in is a mass-assignment vulnerability — the service decides which
            fields a caller may change.

        Args:
            instance: The row to modify, already loaded in this session.
            **values: Column values to set.

        Returns:
            The updated instance.
        """
        for column, value in values.items():
            setattr(instance, column, value)
        await self._session.flush()
        return instance

    async def delete(self, instance: ModelT) -> None:
        """Delete a row — softly if the model supports it.

        A model inheriting :class:`~keel.database.soft_delete.SoftDeleteMixin`
        is marked deleted and disappears from queries; anything else is removed.
        Callers get one verb whose meaning is a property of the model rather
        than something every call site has to know.

        Args:
            instance: The row to delete.
        """
        if self.is_soft_deletable:
            cast("SoftDeleteMixin", instance).soft_delete()
            await self._session.flush()
            return
        await self._session.delete(instance)
        await self._session.flush()

    async def force_delete(self, instance: ModelT) -> None:
        """Remove a row from the table, even if the model is soft deletable.

        For data that must actually go — an erasure request, a purge job.

        Args:
            instance: The row to remove.
        """
        await self._session.delete(instance)
        await self._session.flush()

    async def restore(self, instance: ModelT) -> ModelT:
        """Un-delete a soft-deleted row.

        Args:
            instance: The row to restore.

        Returns:
            The restored instance.

        Raises:
            TypeError: If the model is not soft deletable, since restoring a
                hard-deleted row is not a thing that can be done.
        """
        if not self.is_soft_deletable:
            raise TypeError(f"{self.model.__name__} is not soft deletable")
        cast("SoftDeleteMixin", instance).restore()
        await self._session.flush()
        return instance

    async def purge(self, *criteria: ColumnExpressionArgument[bool]) -> int:
        """Hard-delete matching rows in one statement.

        Bypasses the ORM's unit of work, so no instances are loaded and no
        model events fire. That is the point — this is for cleaning up a million
        expired rows, where loading them would be the expensive part.

        Args:
            *criteria: SQLAlchemy filter expressions. Required: a `purge()` with
                no criteria would truncate the table, and that should be spelled
                out rather than achieved by forgetting an argument.

        Returns:
            The number of rows removed.

        Raises:
            ValueError: If no criteria are given.
        """
        if not criteria:
            raise ValueError(
                "purge() requires criteria; pass an explicit always-true expression "
                "if you really mean to delete every row"
            )
        result = await self._session.execute(sql_delete(self.model).where(*criteria))
        await self._session.flush()
        return int(cast("CursorResult[Any]", result).rowcount)

    # -- soft-delete aware reads -----------------------------------------

    async def with_trashed(
        self,
        *criteria: ColumnExpressionArgument[bool],
        limit: int | None = None,
    ) -> Sequence[ModelT]:
        """Fetch rows including soft-deleted ones.

        Args:
            *criteria: SQLAlchemy filter expressions.
            limit: Optional cap.

        Returns:
            The matching rows, deleted ones included.
        """
        statement = with_deleted(self.query().where(*criteria).order_by(self.model.id))
        if limit is not None:
            statement = statement.limit(limit)
        result = await self._session.execute(statement)
        return result.scalars().all()

    async def find_trashed(self, identifier: uuid.UUID) -> ModelT | None:
        """Fetch a row by primary key even if it is soft deleted.

        The lookup a "restore" endpoint needs, which the normal :meth:`get`
        cannot do by design.

        Args:
            identifier: The primary key.

        Returns:
            The row, or ``None``.
        """
        statement = with_deleted(self.query().where(self.model.id == identifier))
        result = await self._session.execute(statement)
        return result.scalars().first()


__all__ = ["Page", "Repository"]
