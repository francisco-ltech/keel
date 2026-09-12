"""Test-data factories for Keel models.

A factory answers a question every test asks and no test wants to answer: *what
does a valid row look like?* Written by hand, the answer is a dictionary of
fifteen keyword arguments copied between test files, where fourteen of them are
noise and the fifteenth is the thing under test. When a column is added, every
copy has to be found. Factories make the noise implicit and leave the test
saying only what it cares about::

    user = UserFactory.build(email="known@example.com")

Polyfactory already knows how to read a SQLAlchemy mapper and invent plausible
values for its columns. What it does not know is Keel's conventions, and left to
itself it gets three things actively wrong — each producing a row that could
never exist in production, which is the worst possible test fixture because it
passes.

**Generated columns.** ``id`` is assigned by the mapper ``init`` event (see
:class:`~keel.database.model.UUIDPrimaryKey`) and ``created_at`` / ``updated_at``
by the database. A factory that fills them in is not reproducing an insert, it
is overriding one — and a test asserting on ``created_at`` would then be
asserting on a value Faker chose, not on the database's clock. They are excluded
centrally in :data:`GENERATED_COLUMNS`, not per subclass, so a new factory
inherits the rule rather than remembering it.

**``deleted_at``.** It is nullable, so polyfactory would set it to a datetime
roughly half the time. A suite where one build in two is invisible to every
query is not flaky, it is haunted. It is treated as generated for the same
reason: only :meth:`~keel.database.soft_delete.SoftDeleteMixin.soft_delete`
should ever write it.

**Foreign keys and relationships.** These are off, and the decision is not a
matter of taste. A random UUID in a foreign key column violates the constraint —
the insert fails, and it fails at flush time with a Postgres error rather than
anywhere near the factory. The alternative polyfactory offers, generating the
*related object* too, is worse: it silently inserts a parent row the test never
asked for, so a test asserting ``await Users(session).count() == 1`` fails for
reasons that have nothing to do with it. Related rows are the one thing a test
must state, because the shape of the graph is usually the thing being tested::

    author = await AuthorFactory.create(session)
    book = await BookFactory.create(session, author_id=author.id)

A factory that guessed here would be guessing about the part that matters.

Persistence is split deliberately. :meth:`ModelFactory.build` never touches the
database, which is what makes a factory usable in a unit test of a pure
function; :meth:`ModelFactory.create` adds and flushes into a session the caller
owns. Nothing here opens a transaction or commits — see ADR 0002; the session's
owner decides when the work lands.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Final

from polyfactory.factories.sqlalchemy_factory import SQLAlchemyFactory

from keel.database.model import Model

if TYPE_CHECKING:
    from polyfactory.field_meta import FieldMeta
    from sqlalchemy.ext.asyncio import AsyncSession

GENERATED_COLUMNS: Final[frozenset[str]] = frozenset(
    {"id", "created_at", "updated_at", "deleted_at"}
)
"""Columns a factory must never invent, because something else owns them.

``id`` comes from the mapper ``init`` event, the timestamps from the database's
own clock, and ``deleted_at`` from an explicit soft delete. A value invented for
any of them describes a row that no code path could produce.
"""


class ModelFactory[ModelT: Model](SQLAlchemyFactory[ModelT]):
    """Builds instances of one Keel model.

    Declare one per model::

        class UserFactory(ModelFactory[User]):
            __model__ = User

    The model is inferred from the type parameter, so ``__model__`` is
    redundant — write it anyway. It is the line a reader looks for, and it makes
    the factory work under a runtime that has erased the parameter.

    Two verbs, and the split is the point::

        user = UserFactory.build()                  # no database at all
        user = await UserFactory.create(session)    # added and flushed

    A factory that always persisted would be unusable in a test of a pure
    function, and a suite that reaches for a database to check a validation rule
    is a suite nobody runs on save.

    Overrides always win, and are how a test says what it is actually about::

        UserFactory.build(email="known@example.com", is_active=False)

    Note:
        Foreign keys and relationships are not generated — pass them. See the
        module docstring for why guessing them is worse than omitting them.

    Note:
        A model with a natural (non-UUID) primary key needs
        ``__set_primary_key__ = True`` on its factory, since nothing else will
        supply the key. Keel's own convention is
        :class:`~keel.database.model.UUIDPrimaryKey`, where the opposite is true.
    """

    __is_base_factory__ = True

    __set_primary_key__: ClassVar[bool] = False
    __set_foreign_keys__: ClassVar[bool] = False
    __set_relationships__: ClassVar[bool] = False
    __set_association_proxy__: ClassVar[bool] = False

    __excluded_fields__: ClassVar[frozenset[str]] = GENERATED_COLUMNS
    """Field names this factory refuses to generate.

    Override to extend it — ``__excluded_fields__ = GENERATED_COLUMNS | {"slug"}``
    — for a column some other machinery owns, such as a database trigger.
    """

    __config_keys__ = (*SQLAlchemyFactory.__config_keys__, "__excluded_fields__")

    @classmethod
    def should_set_field_value(cls, field_meta: FieldMeta, **kwargs: Any) -> bool:
        """Decide whether to generate a value for one field.

        The single choke point for :data:`GENERATED_COLUMNS`. Doing it here
        rather than with a per-field ``Ignore()`` in each subclass means a
        factory written next year cannot forget: there is no line to omit.

        Args:
            field_meta: The field polyfactory is considering.
            **kwargs: The build-time overrides, which always take precedence.

        Returns:
            ``False`` for a column something other than the factory owns.
        """
        if field_meta.name in cls.__excluded_fields__:
            return False
        return super().should_set_field_value(field_meta, **kwargs)

    @classmethod
    def seed(cls, value: int) -> None:
        """Make subsequent builds reproducible.

        A failing test that generated its data randomly is only useful if you
        can generate the same data again. Print the seed on failure, then pin
        it::

            UserFactory.seed(20260912)
            user = UserFactory.build()   # same values, every run

        Two caveats, both worth knowing before trusting it:

        * The Faker instance is shared by every polyfactory factory in the
          process, so this reseeds all of them. Seed once, at the start of the
          test, rather than between builds.
        * It does **not** make ``id`` reproducible. Primary keys come from the
          mapper event, not from the factory, and a UUIDv7 is a function of the
          clock. Assert on the columns, not the key.

        Args:
            value: The seed. Any integer; a date is a readable choice.
        """
        cls.seed_random(value)

    @classmethod
    async def create(cls, session: AsyncSession, **overrides: Any) -> ModelT:
        """Build one instance, add it to *session*, and flush.

        Flushes rather than commits, for the reason a repository does: a
        constraint violation surfaces here, attributable to this row, instead of
        at the end of the transaction where the traceback points at the commit.
        The transaction still belongs to whoever opened it.

        Args:
            session: The session to insert into, from the caller's unit of work.
            **overrides: Column values to pin instead of generating.

        Returns:
            The persisted instance, with its primary key already set.
        """
        instance = cls.build(**overrides)
        session.add(instance)
        await session.flush()
        return instance

    @classmethod
    async def create_many(cls, session: AsyncSession, count: int, **overrides: Any) -> list[ModelT]:
        """Build *count* instances, add them all, and flush once.

        One flush for the batch, not one per row — the difference is a single
        round trip against *count* of them, which is what makes seeding a
        thousand rows for a pagination test tolerable.

        Args:
            session: The session to insert into.
            count: How many rows to build.
            **overrides: Column values applied to every row. Anything that must
                differ per row should be left to the generator.

        Returns:
            The persisted instances, in the order they were built.
        """
        instances = cls.batch(count, **overrides)
        session.add_all(instances)
        await session.flush()
        return instances


__all__ = ["GENERATED_COLUMNS", "ModelFactory"]
