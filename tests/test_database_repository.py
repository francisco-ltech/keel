"""The generic repository, soft deletes, and keyset pagination.

Three things here are load-bearing beyond their own assertions.

The soft-delete tests check that the global scope reaches *relationship* loads,
not just top-level queries. A filter that only covers `select(Parent)` is worse
than none, because it makes deleted rows invisible in the place people look and
visible in the place they do not.

The pagination tests walk a full result set across pages and assert the union is
exactly the input with no duplicates and nothing missing. Off-by-one errors in
keyset pagination produce a duplicate or a hole at every boundary, and a test
that only checks page one never sees them.

The eager-loading test asserts that a relationship survives the transaction. In
async SQLAlchemy a lazy load outside a session raises, so "did you remember to
eager load" is a correctness question, not a performance one.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
from sqlalchemy import ForeignKey, String, Table, select, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from keel.database import (
    Database,
    DatabaseConfig,
    Model,
    PublicId,
    Repository,
    SoftDeleteMixin,
    TimestampMixin,
    UUIDPrimaryKey,
    set_database,
    uow,
    with_deleted,
)
from keel.database.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    clamp_limit,
    decode_cursor,
    encode_cursor,
)
from keel.exceptions import InvalidCursorError, RecordNotFoundError

pytestmark = [pytest.mark.anyio]


class Author(Model, UUIDPrimaryKey, TimestampMixin, SoftDeleteMixin):
    """A soft-deletable parent, used to prove the global scope."""

    __tablename__ = "keel_test_authors"

    name: Mapped[str] = mapped_column(String(100))
    books: Mapped[list[Book]] = relationship(back_populates="author", lazy="raise")


class Book(Model, UUIDPrimaryKey, TimestampMixin, SoftDeleteMixin):
    """A soft-deletable child, so relationship loads can be checked too."""

    __tablename__ = "keel_test_books"

    title: Mapped[str] = mapped_column(String(200))
    author_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("keel_test_authors.id"))
    author: Mapped[Author] = relationship(back_populates="books", lazy="raise")


class Tag(Model, UUIDPrimaryKey):
    """A model that is *not* soft deletable, to prove `delete()` adapts."""

    __tablename__ = "keel_test_tags"

    label: Mapped[str] = mapped_column(String(50))


class Authors(Repository[Author]):
    """Repository under test."""

    model = Author

    async def named(self, name: str) -> Author | None:
        """A query of its own, to show what a subclass is actually for."""
        return await self.first(Author.name == name)


class Books(Repository[Book]):
    model = Book


class Sku(Model, UUIDPrimaryKey, PublicId):
    """A model with a public identifier beside its primary key."""

    __tablename__ = "keel_test_skus"

    code: Mapped[str] = mapped_column(String(50))


class Skus(Repository[Sku]):
    model = Sku


class Label(Model, UUIDPrimaryKey):
    """Not a PublicId model, but with something called pid, to fool a lazy check."""

    __tablename__ = "keel_test_labels"

    text_: Mapped[str] = mapped_column("text", String(50))

    @property
    def pid(self) -> str:
        return "not a column"


class Tags(Repository[Tag]):
    model = Tag


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    """A live database with the test tables created and dropped around it."""
    tables = [cast("Table", model.__table__) for model in (Author, Book, Tag, Sku, Label)]
    instance = Database(DatabaseConfig(url=database_url))
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.create_all, tables=tables)
    set_database(instance)
    yield instance
    async with instance.connect() as connection:
        await connection.run_sync(Model.metadata.drop_all, tables=tables)
    set_database(None)
    await instance.close()


# -- construction ---------------------------------------------------------


def test_a_repository_without_a_model_says_so() -> None:
    class Broken(Repository[Author]):
        pass

    with pytest.raises(TypeError, match="must set a `model`"):
        Broken(None)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


def test_a_repository_over_a_keyless_model_says_so() -> None:
    """Every read addresses rows by primary key, so the model must have one."""

    class Keyless(Model):
        __tablename__ = "keel_test_keyless"
        code: Mapped[str] = mapped_column(String(10), primary_key=True)

    class Broken(Repository[Any]):
        model = Keyless

    with pytest.raises(TypeError, match="no `id` column"):
        Broken(None)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


# -- reading --------------------------------------------------------------


@pytest.mark.postgres
async def test_create_then_get_round_trips(database: Database) -> None:
    async with uow() as session:
        created = await Authors(session).create(name="Le Guin")

    async with uow() as session:
        found = await Authors(session).get(created.id)
    assert found is not None
    assert found.name == "Le Guin"


@pytest.mark.postgres
async def test_get_returns_none_for_an_unknown_id(database: Database) -> None:
    async with uow() as session:
        assert await Authors(session).get(uuid.uuid4()) is None


@pytest.mark.postgres
async def test_get_or_fail_names_the_model_and_the_id(database: Database) -> None:
    missing = uuid.uuid4()
    async with uow() as session:
        with pytest.raises(RecordNotFoundError) as error:
            await Authors(session).get_or_fail(missing)

    message = str(error.value)
    assert "Author" in message
    assert str(missing) in message


@pytest.mark.postgres
async def test_a_subclass_query_uses_the_inherited_helpers(database: Database) -> None:
    """What the base class is for: the subclass writes only its own query."""
    async with uow() as session:
        await Authors(session).create(name="Butler")

    async with uow() as session:
        found = await Authors(session).named("Butler")
    assert found is not None


@pytest.mark.postgres
async def test_first_or_fail_reports_the_criteria(database: Database) -> None:
    async with uow() as session:
        with pytest.raises(RecordNotFoundError):
            await Authors(session).first_or_fail(Author.name == "nobody")


@pytest.mark.postgres
async def test_count_and_exists_agree(database: Database) -> None:
    async with uow() as session:
        repository = Authors(session)
        for name in ("a", "b", "c"):
            await repository.create(name=name)

    async with uow() as session:
        repository = Authors(session)
        assert await repository.count() == 3
        assert await repository.count(Author.name == "a") == 1
        assert await repository.exists(Author.name == "a") is True
        assert await repository.exists(Author.name == "zz") is False


@pytest.mark.postgres
async def test_list_is_ordered_by_creation(database: Database) -> None:
    """Time-ordered keys make `ORDER BY id` mean something."""
    async with uow() as session:
        repository = Authors(session)
        for name in ("first", "second", "third"):
            await repository.create(name=name)

    async with uow() as session:
        names = [author.name for author in await Authors(session).list()]
    assert names == ["first", "second", "third"]


# -- eager loading --------------------------------------------------------


@pytest.mark.postgres
async def test_a_relationship_is_usable_after_the_transaction_when_eager_loaded(
    database: Database,
) -> None:
    """The N+1 question, which async turns into a correctness question."""
    async with uow() as session:
        author = await Authors(session).create(name="Jemisin")
        await Books(session).create(title="The Fifth Season", author_id=author.id)

    async with uow() as session:
        loaded = await Authors(session).get(author.id, *Authors.eager(Author.books))

    assert loaded is not None
    assert [book.title for book in loaded.books] == ["The Fifth Season"]


@pytest.mark.postgres
async def test_a_relationship_not_eager_loaded_raises_rather_than_querying(
    database: Database,
) -> None:
    """`lazy="raise"` plus async: the failure is loud, which is the point."""
    async with uow() as session:
        author = await Authors(session).create(name="Wolfe")
        await Books(session).create(title="Severian", author_id=author.id)

    async with uow() as session:
        loaded = await Authors(session).get(author.id)

    assert loaded is not None
    with pytest.raises(Exception, match=r"lazy load|not available|greenlet"):
        _ = loaded.books


# -- soft deletes ---------------------------------------------------------


@pytest.mark.postgres
async def test_delete_hides_a_soft_deletable_row(database: Database) -> None:
    async with uow() as session:
        author = await Authors(session).create(name="hidden")

    async with uow() as session:
        repository = Authors(session)
        await repository.delete(await repository.get_or_fail(author.id))

    async with uow() as session:
        assert await Authors(session).get(author.id) is None


@pytest.mark.postgres
async def test_a_soft_deleted_row_is_still_in_the_table(database: Database) -> None:
    """Hidden, not gone — otherwise "restore" would be impossible."""
    async with uow() as session:
        author = await Authors(session).create(name="hidden")
        await Authors(session).delete(author)

    async with uow() as session:
        found = await Authors(session).find_trashed(author.id)
    assert found is not None
    assert found.is_deleted is True


@pytest.mark.postgres
async def test_restore_brings_a_row_back(database: Database) -> None:
    async with uow() as session:
        author = await Authors(session).create(name="returning")
        await Authors(session).delete(author)

    async with uow() as session:
        repository = Authors(session)
        trashed = await repository.find_trashed(author.id)
        assert trashed is not None
        await repository.restore(trashed)

    async with uow() as session:
        assert await Authors(session).get(author.id) is not None


@pytest.mark.postgres
async def test_the_soft_delete_filter_reaches_relationship_loads(database: Database) -> None:
    """The half that a hand-written filter always misses.

    Filtering `select(Author)` is easy to remember. Filtering the books loaded
    *through* an author is not, and a deleted book showing up inside its parent
    is exactly as wrong as one showing up in a list.
    """
    async with uow() as session:
        author = await Authors(session).create(name="mixed")
        kept = await Books(session).create(title="kept", author_id=author.id)
        removed = await Books(session).create(title="removed", author_id=author.id)
        await Books(session).delete(removed)

    async with uow() as session:
        loaded = await Authors(session).get(author.id, *Authors.eager(Author.books))

    assert loaded is not None
    assert [book.title for book in loaded.books] == ["kept"]
    assert kept.id != removed.id


@pytest.mark.postgres
async def test_with_deleted_opts_out_of_the_filter(database: Database) -> None:
    async with uow() as session:
        author = await Authors(session).create(name="opt-out")
        await Authors(session).delete(author)

    async with uow() as session:
        visible = (await session.execute(select(Author))).scalars().all()
        everything = (await session.execute(with_deleted(select(Author)))).scalars().all()

    assert len(visible) == 0
    assert len(everything) == 1


@pytest.mark.postgres
async def test_with_trashed_lists_deleted_rows_too(database: Database) -> None:
    async with uow() as session:
        repository = Authors(session)
        alive = await repository.create(name="alive")
        dead = await repository.create(name="dead")
        await repository.delete(dead)

    async with uow() as session:
        repository = Authors(session)
        assert {author.name for author in await repository.list()} == {"alive"}
        assert {author.name for author in await repository.with_trashed()} == {"alive", "dead"}
    assert alive.id != dead.id


@pytest.mark.postgres
async def test_delete_removes_a_model_that_is_not_soft_deletable(database: Database) -> None:
    """One verb, whose meaning is a property of the model."""
    async with uow() as session:
        tag = await Tags(session).create(label="ephemeral")
        assert Tags(session).is_soft_deletable is False
        await Tags(session).delete(tag)

    async with uow() as session:
        assert await Tags(session).count() == 0


@pytest.mark.postgres
async def test_restoring_a_hard_deletable_model_is_a_type_error(database: Database) -> None:
    async with uow() as session:
        tag = await Tags(session).create(label="x")
        with pytest.raises(TypeError, match="not soft deletable"):
            await Tags(session).restore(tag)


@pytest.mark.postgres
async def test_force_delete_removes_a_soft_deletable_row(database: Database) -> None:
    async with uow() as session:
        author = await Authors(session).create(name="erased")
        await Authors(session).force_delete(author)

    async with uow() as session:
        assert await Authors(session).find_trashed(author.id) is None


# -- bulk removal ---------------------------------------------------------


@pytest.mark.postgres
async def test_purge_deletes_matching_rows_and_reports_how_many(database: Database) -> None:
    async with uow() as session:
        repository = Authors(session)
        for name in ("keep", "drop", "drop"):
            await repository.create(name=name)

    async with uow() as session:
        removed = await Authors(session).purge(Author.name == "drop")
    assert removed == 2

    async with uow() as session:
        assert await Authors(session).count() == 1


@pytest.mark.postgres
async def test_purge_without_criteria_refuses(database: Database) -> None:
    """A missing argument should not be able to truncate a table."""
    async with uow() as session:
        with pytest.raises(ValueError, match="requires criteria"):
            await Authors(session).purge()


# -- pagination -----------------------------------------------------------


def test_a_cursor_round_trips() -> None:
    value = uuid.uuid4()
    assert decode_cursor(encode_cursor(value)) == value


def test_a_cursor_is_url_safe_and_unpadded() -> None:
    cursor = encode_cursor(uuid.uuid4())
    assert "=" not in cursor
    assert "+" not in cursor
    assert "/" not in cursor


@pytest.mark.parametrize("cursor", ["", "not-base64!!", "YWJj", "x" * 40])
def test_a_malformed_cursor_is_a_clean_error(cursor: str) -> None:
    """Clients send corrupted cursors; that must be a 400, not a 500."""
    with pytest.raises(InvalidCursorError):
        decode_cursor(cursor)


def test_a_malformed_cursor_does_not_echo_its_value() -> None:
    """The value is attacker-controlled and ends up in logs."""
    secret = encode_cursor(uuid.uuid4())[:-4] + "!!!!"
    with pytest.raises(InvalidCursorError) as error:
        decode_cursor(secret)
    assert secret not in str(error.value)


@pytest.mark.parametrize(
    ("requested", "expected"),
    [(None, DEFAULT_PAGE_SIZE), (0, 1), (-5, 1), (10, 10), (10_000, MAX_PAGE_SIZE)],
)
def test_the_page_size_is_clamped(requested: int | None, expected: int) -> None:
    assert clamp_limit(requested) == expected


@pytest.mark.postgres
async def test_paginating_walks_every_row_exactly_once(database: Database) -> None:
    """The assertion that catches off-by-one errors at page boundaries."""
    total = 25
    async with uow() as session:
        repository = Authors(session)
        for index in range(total):
            await repository.create(name=f"author-{index:02d}")

    seen: list[str] = []
    cursor: str | None = None
    pages = 0
    while True:
        async with uow() as session:
            page = await Authors(session).paginate(cursor=cursor, limit=7)
        seen.extend(author.name for author in page.items)
        pages += 1
        if not page.has_more:
            break
        cursor = page.next_cursor
        assert pages < 10, "pagination did not terminate"

    assert len(seen) == total, "a row was duplicated or skipped"
    assert len(set(seen)) == total
    assert seen == sorted(seen)
    assert pages == 4


@pytest.mark.postgres
async def test_the_last_page_reports_no_more(database: Database) -> None:
    async with uow() as session:
        await Authors(session).create(name="only")

    async with uow() as session:
        page = await Authors(session).paginate(limit=10)
    assert page.has_more is False
    assert page.next_cursor is None
    assert len(page) == 1


@pytest.mark.postgres
async def test_an_empty_table_paginates_to_an_empty_page(database: Database) -> None:
    async with uow() as session:
        page = await Authors(session).paginate()
    assert page.is_empty is True
    assert page.has_more is False


@pytest.mark.postgres
async def test_descending_pagination_reverses_the_walk(database: Database) -> None:
    async with uow() as session:
        repository = Authors(session)
        for index in range(5):
            await repository.create(name=f"author-{index}")

    async with uow() as session:
        page = await Authors(session).paginate(limit=2, descending=True)
    assert [author.name for author in page.items] == ["author-4", "author-3"]


@pytest.mark.postgres
async def test_pagination_respects_criteria(database: Database) -> None:
    async with uow() as session:
        repository = Authors(session)
        for name in ("match", "other", "match"):
            await repository.create(name=name)

    async with uow() as session:
        page = await Authors(session).paginate(Author.name == "match", limit=10)
    assert len(page) == 2


@pytest.mark.postgres
async def test_pagination_excludes_soft_deleted_rows(database: Database) -> None:
    async with uow() as session:
        repository = Authors(session)
        kept = await repository.create(name="kept")
        gone = await repository.create(name="gone")
        await repository.delete(gone)
        assert kept.id != gone.id

    async with uow() as session:
        page = await Authors(session).paginate(limit=10)
    assert [author.name for author in page.items] == ["kept"]


# -- public identifiers -----------------------------------------------------


@pytest.mark.postgres
async def test_a_pid_is_assigned_eagerly_and_is_not_time_ordered(database: Database) -> None:
    """Before the flush, like the primary key; random, unlike it."""
    sku = Sku(code="A")
    assert sku.pid is not None and sku.pid.version == 4
    assert sku.id.version == 7
    async with uow() as session:
        session.add(sku)
    assert Sku(code="B").pid != sku.pid


@pytest.mark.postgres
async def test_get_by_pid_finds_the_row_and_only_that_row(database: Database) -> None:
    async with uow() as session:
        created = await Skus(session).create(code="A")
        await Skus(session).create(code="B")
    async with uow() as session:
        found = await Skus(session).get_by_pid(created.pid)
        assert found is not None and found.id == created.id
        assert await Skus(session).get_by_pid(uuid.uuid4()) is None
        assert await Skus(session).get_by_pid(created.id) is None, "a primary key is not a pid"


@pytest.mark.postgres
async def test_a_model_without_a_pid_says_so(database: Database) -> None:
    async with uow() as session:
        with pytest.raises(TypeError, match="PublicId"):
            await Tags(session).get_by_pid(uuid.uuid4())


@pytest.mark.postgres
async def test_a_model_whose_pid_is_not_a_column_says_so(database: Database) -> None:
    """An attribute called pid is not the mixin; matching nothing would be silent."""

    class Labels(Repository[Label]):
        model = Label

    async with uow() as session:
        with pytest.raises(TypeError, match="PublicId"):
            await Labels(session).get_by_pid(uuid.uuid4())


@pytest.mark.postgres
async def test_a_row_inserted_without_a_pid_gets_one_from_the_server(database: Database) -> None:
    """A migration backfill or a psql session must not be able to insert a row with no pid."""
    async with uow() as session:
        await session.execute(
            text("INSERT INTO keel_test_skus (id, code) VALUES (:id, 'raw')"),
            {"id": uuid.uuid4()},
        )
    async with uow() as session:
        raw = await Skus(session).first(Sku.code == "raw")
        assert raw is not None and raw.pid is not None and raw.pid.version == 4
