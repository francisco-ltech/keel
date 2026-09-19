"""The declarative base and the mixins every table gets.

The naming convention on the metadata is the important part and is easy to skip.
Without it, Alembic autogenerates constraint names chosen by the database, which
means the same schema produces different migration scripts on different
backends, and a downgrade cannot find the constraint it is trying to drop. It
has to be set before the first table is defined; retrofitting it means a
migration that renames every constraint in the schema.

Phase 0 ships the base and timestamps. Soft deletes, audit columns and the
repository arrive in Phase 2 — they are listed in the ADR rather than stubbed
here, because a half-built mixin is worse than an absent one.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Final

from sqlalchemy import DateTime, MetaData, event, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from keel.database.ids import uuid7
from keel.support.clock import utcnow

NAMING_CONVENTION: Final[dict[str, str]] = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
"""Deterministic constraint names, so migrations are reproducible across
backends and a downgrade can name what it drops."""


class Model(DeclarativeBase):
    """Base class for every table.

    Attributes:
        metadata: Carries :data:`NAMING_CONVENTION`, which is what makes
            Alembic autogeneration deterministic.
    """

    metadata = MetaData(naming_convention=NAMING_CONVENTION)

    def __repr__(self) -> str:
        """Show the class and primary key, which is what a traceback needs."""
        keys = self.__mapper__.primary_key
        values = ", ".join(f"{key.name}={getattr(self, key.name, None)!r}" for key in keys)
        return f"<{type(self).__name__} {values}>"


class UUIDPrimaryKey:
    """A time-ordered UUID primary key, assigned at construction.

    Two properties, both deliberate.

    **Time-ordered** (UUIDv7): inserts land at the right-hand edge of the index
    instead of scattering across it. See :mod:`keel.database.ids`.

    **Assigned eagerly**, not at flush. A SQLAlchemy Python-side ``default``
    runs when the INSERT is built, which means ``Widget(name="x").id`` would be
    ``None`` until the unit of work flushes. That is a genuine obstacle: a
    service building a graph of related rows would have to flush after each
    parent just to learn the foreign key to hand its children. Assigning in the
    ``init`` mapper event removes the round trip *and* the flush.

    The event is registered once, on :class:`Model`, and applies to every
    subclass that mixes this in — so a new table gets the behaviour by
    inheriting, with nothing to remember.
    """

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid7)


class PublicId:
    """A second identifier, and the only one that crosses the wire.

    The primary key is for the database: time-ordered so inserts cluster,
    joined on, never meant to be seen. A ``pid`` is for everyone else — the
    URL, the response body, a log line a client quotes back — and it is a
    random UUID4, so it reveals nothing about when the row was created or what
    was created next to it, and cannot be walked. Loco calls this ``pid``
    too, and the name is kept on purpose: it should look different from ``id``
    everywhere it appears, because it is.

    Two columns rather than a random primary key, because the reasons for a
    time-ordered key (:mod:`keel.database.ids`) are about the index and the
    reasons for an unguessable one are about the wire, and one column cannot
    be both.

    Assigned eagerly, like the primary key, so a row has its ``pid`` before it
    is flushed and a service can put it in a response without a refresh. The
    server default covers rows a migration or a ``psql`` session inserts.
    """

    pid: Mapped[uuid.UUID] = mapped_column(
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        unique=True,
        index=True,
    )


class TimestampMixin:
    """``created_at`` and ``updated_at``.

    Inserts are defaulted server-side so that rows written by a migration, a
    fixture load or a psql session get them too — an application is never the
    only writer for long.

    Updates are stamped **Python-side**, and that asymmetry is deliberate.
    ``onupdate=func.now()`` is a SQL expression, so after an UPDATE SQLAlchemy
    marks the attribute expired and the next attribute access triggers a lazy
    refresh. Under asyncio that refresh raises ``MissingGreenlet`` from whatever
    innocuous line happened to touch it — typically a ``model_validate`` call
    after the transaction block — and the traceback points nowhere near the
    cause. A Python-side ``onupdate`` has the value in hand, so nothing expires
    and nothing needs an explicit ``refresh()``.

    Nothing is lost by it: ``onupdate`` only ever applies to statements
    SQLAlchemy itself emits, whether the value comes from Python or from SQL.
    Writers outside the ORM are covered by ``server_default`` on insert and are
    responsible for their own updates either way.

    Note:
        ``func.now()`` is the *transaction* timestamp in Postgres, not the
        statement timestamp. Every row inserted inside one transaction shares a
        ``created_at``, so it is not a total order — sort by
        ``(created_at, id)`` when order matters. This is easy to miss in
        production and immediately obvious under a rollback-per-test suite,
        where a whole test runs in one transaction.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=utcnow,
        nullable=False,
    )


@event.listens_for(Model, "init", propagate=True)
def _assign_identity(
    target: object,
    _args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    """Give a new instance its primary key before ``__init__`` runs.

    Observer pattern, and the reason it is an event rather than an ``__init__``
    override: a mixin's ``__init__`` only wins if it precedes the declarative
    base in the MRO, which would force every model in every application to
    declare its bases in a particular order and fail confusingly when someone
    forgot. An event has no such requirement.

    Args:
        target: The instance being constructed.
        _args: Positional arguments, unused — declarative models take keywords.
        kwargs: Keyword arguments, mutated in place to add ``id`` when the
            caller did not supply one.
    """
    if isinstance(target, UUIDPrimaryKey) and kwargs.get("id") is None:
        kwargs["id"] = uuid7()
    if isinstance(target, PublicId) and kwargs.get("pid") is None:
        kwargs["pid"] = uuid.uuid4()


__all__: list[str] = [
    "NAMING_CONVENTION",
    "Model",
    "PublicId",
    "TimestampMixin",
    "UUIDPrimaryKey",
    "utcnow",
]
