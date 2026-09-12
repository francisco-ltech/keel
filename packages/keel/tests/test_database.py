"""The database subsystem.

Two things here are worth more than the rest.

``test_a_failing_block_is_rolled_back`` pins the transaction boundary, which is
the only reason `uow()` exists rather than callers managing sessions themselves.

The ``rolled_back_database`` tests pin the test-suite guarantee: that a test can
write, commit, and leave nothing behind. If that breaks, every database test in
every application built on Keel starts leaking state into the next one, and the
failures show up somewhere else entirely.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import String, select, text
from sqlalchemy.orm import Mapped, mapped_column

from keel.database import (
    Database,
    DatabaseConfig,
    Model,
    TimestampMixin,
    UUIDPrimaryKey,
    current_database,
    database_lifespan,
    set_database,
    uow,
    utcnow,
)
from keel.database.config import SETTING_VARS, URL_VAR
from keel.exceptions import ConfigurationError
from keel.testing import rolled_back_database

pytestmark = [pytest.mark.anyio]


class Widget(Model, UUIDPrimaryKey, TimestampMixin):
    """A table that exists only for these tests."""

    __tablename__ = "keel_test_widgets"

    name: Mapped[str] = mapped_column(String(100))


# -- configuration --------------------------------------------------------


def test_a_url_is_required() -> None:
    with pytest.raises(ConfigurationError, match="url is required"):
        DatabaseConfig(url="")


def test_a_driverless_url_is_rejected() -> None:
    """``postgresql://`` is the sync driver, and would block the event loop."""
    with pytest.raises(ConfigurationError) as error:
        DatabaseConfig(url="postgresql://user@host/db")
    assert "async driver" in str(error.value)


def test_a_synchronous_driver_is_rejected() -> None:
    with pytest.raises(ConfigurationError) as error:
        DatabaseConfig(url="postgresql+psycopg2://user@host/db")
    assert "block the event loop" in str(error.value)


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+asyncpg://user@host/db",
        "postgresql+psycopg://user@host/db",
        "sqlite+aiosqlite:///./test.db",
    ],
)
def test_known_async_drivers_are_accepted(url: str) -> None:
    assert DatabaseConfig(url=url).url == url


def test_sqlite_is_recognised_so_pool_options_can_be_skipped() -> None:
    assert DatabaseConfig(url="sqlite+aiosqlite:///:memory:").is_sqlite is True
    assert DatabaseConfig(url="postgresql+asyncpg://u@h/d").is_sqlite is False


def test_from_env_reads_every_documented_variable() -> None:
    config = DatabaseConfig.from_env(
        {
            URL_VAR: "postgresql+asyncpg://u@h/d",
            f"{SETTING_VARS}ECHO": "true",
            f"{SETTING_VARS}POOL_SIZE": "20",
            f"{SETTING_VARS}MAX_OVERFLOW": "5",
            f"{SETTING_VARS}STATEMENT_TIMEOUT": "5",
        }
    )
    assert config.url == "postgresql+asyncpg://u@h/d"
    assert config.echo is True
    assert config.pool_size == 20
    assert config.max_overflow == 5
    assert config.statement_timeout == 5.0


def test_from_env_accepts_none_for_the_statement_timeout() -> None:
    config = DatabaseConfig.from_env(
        {URL_VAR: "postgresql+asyncpg://u@h/d", f"{SETTING_VARS}STATEMENT_TIMEOUT": "none"}
    )
    assert config.statement_timeout is None


def test_from_env_rejects_a_non_numeric_pool_size() -> None:
    with pytest.raises(ConfigurationError, match="must be a number"):
        DatabaseConfig.from_env(
            {URL_VAR: "postgresql+asyncpg://u@h/d", f"{SETTING_VARS}POOL_SIZE": "lots"}
        )


# -- binding --------------------------------------------------------------


async def test_uow_without_a_bound_database_says_what_to_do() -> None:
    set_database(None)
    with pytest.raises(ConfigurationError) as error:
        async with uow():
            pass  # pragma: no cover — the block must not run
    message = str(error.value)
    assert "set_database" in message
    assert "use_database" in message


# -- the real thing -------------------------------------------------------


@pytest.fixture
async def database(database_url: str) -> AsyncIterator[Database]:
    """A live database with the test table created and dropped around it."""
    instance = Database(DatabaseConfig(url=database_url))
    async with instance.connect() as connection:
        await connection.run_sync(Widget.metadata.create_all)
    set_database(instance)
    yield instance
    async with instance.connect() as connection:
        await connection.run_sync(Widget.metadata.drop_all)
    set_database(None)
    await instance.close()


@pytest.mark.postgres
async def test_health_reports_a_reachable_database(database: Database) -> None:
    assert await database.healthy() is True


@pytest.mark.postgres
async def test_health_reports_an_unreachable_database() -> None:
    """A readiness probe that cannot fail is worse than no probe at all."""
    unreachable = Database(
        DatabaseConfig(url="postgresql+asyncpg://keel:keel@localhost:1/nope", pool_pre_ping=False)
    )
    try:
        assert await unreachable.healthy() is False
    finally:
        await unreachable.close()


@pytest.mark.postgres
async def test_a_statement_timeout_is_applied_to_every_connection(database: Database) -> None:
    """Without it, one pathological query starves the pool behind it."""
    async with uow() as session:
        assert (await session.execute(text("SHOW statement_timeout"))).scalar_one() == "30s"


@pytest.mark.postgres
async def test_a_clean_block_commits(database: Database) -> None:
    async with uow() as session:
        session.add(Widget(name="kept"))

    async with uow() as session:
        names = (await session.execute(select(Widget.name))).scalars().all()
    assert "kept" in names


@pytest.mark.postgres
async def test_a_failing_block_is_rolled_back(database: Database) -> None:
    """The reason `uow()` exists instead of callers managing sessions."""
    with pytest.raises(RuntimeError):
        async with uow() as session:
            session.add(Widget(name="discarded"))
            await session.flush()
            raise RuntimeError("something went wrong")

    async with uow() as session:
        names = (await session.execute(select(Widget.name))).scalars().all()
    assert "discarded" not in names


@pytest.mark.postgres
async def test_timestamps_are_set_by_the_database(database: Database) -> None:
    """Server-side defaults, so rows written by a migration get them too."""
    async with uow() as session:
        widget = Widget(name="stamped")
        session.add(widget)
        await session.flush()
        created = widget.created_at

    assert created is not None
    assert created.tzinfo is not None, "timestamps must be timezone-aware"
    assert abs((utcnow() - created).total_seconds()) < 60


@pytest.mark.postgres
async def test_the_primary_key_is_available_before_the_flush(database: Database) -> None:
    """Phase 2 changed this, and the change is the point.

    A SQLAlchemy Python-side ``default`` runs when the INSERT is built, so the
    id used to appear only at flush. A service building a graph of rows then had
    to flush after each parent just to learn the foreign key for its children.
    A mapper ``init`` event assigns it at construction instead.
    """
    widget = Widget(name="early")
    assert isinstance(widget.id, uuid.UUID)
    assert widget.id.version == 7, "keys are time-ordered, not random"

    async with uow() as session:
        session.add(widget)
        await session.flush()
        assert (await session.get_one(Widget, widget.id)) is widget


# -- the test-suite guarantee ---------------------------------------------


@pytest.mark.postgres
async def test_rolled_back_database_undoes_writes(database: Database) -> None:
    async with rolled_back_database(database):
        async with uow() as session:
            session.add(Widget(name="temporary"))

        async with uow() as session:
            inside = (await session.execute(select(Widget.name))).scalars().all()
        assert "temporary" in inside

    async with uow() as session:
        after = (await session.execute(select(Widget.name))).scalars().all()
    assert "temporary" not in after


@pytest.mark.postgres
async def test_code_under_test_can_commit_normally(database: Database) -> None:
    """The savepoint trick: an explicit commit must still be undoable.

    Application code should not have to know it is in a test, which means it
    must be free to call ``commit()`` — and that commit must still be rolled
    back when the test ends.
    """
    async with rolled_back_database(database):
        session = current_database().session()
        session.add(Widget(name="explicitly-committed"))
        await session.commit()
        await session.close()

    async with uow() as session:
        after = (await session.execute(select(Widget.name))).scalars().all()
    assert "explicitly-committed" not in after


@pytest.mark.postgres
async def test_rolled_back_database_restores_the_previous_binding(database: Database) -> None:
    async with rolled_back_database(database) as temporary:
        assert current_database() is temporary
    assert current_database() is database


@pytest.mark.postgres
async def test_rolled_back_database_undoes_even_when_the_block_raises(
    database: Database,
) -> None:
    with pytest.raises(RuntimeError):
        async with rolled_back_database(database):
            async with uow() as session:
                session.add(Widget(name="doomed"))
            raise RuntimeError("test failed")

    async with uow() as session:
        after = (await session.execute(select(Widget.name))).scalars().all()
    assert "doomed" not in after


# -- lifespan -------------------------------------------------------------


@pytest.mark.postgres
async def test_the_lifespan_binds_and_then_restores(database_url: str) -> None:
    set_database(None)
    async with database_lifespan(DatabaseConfig(url=database_url)) as bound:
        assert current_database() is bound
    with pytest.raises(ConfigurationError):
        current_database()


@pytest.mark.postgres
async def test_a_nested_lifespan_restores_the_outer_one(database_url: str) -> None:
    """Same footgun the cache's lifespan had: unbinding kills the outer scope."""
    async with database_lifespan(DatabaseConfig(url=database_url)) as outer:
        async with database_lifespan(DatabaseConfig(url=database_url)) as inner:
            assert current_database() is inner
        assert current_database() is outer


@pytest.mark.postgres
async def test_the_model_repr_names_the_primary_key(database: Database) -> None:
    """A traceback should say which row, not just which table."""
    async with uow() as session:
        widget = Widget(name="named")
        session.add(widget)
        await session.flush()
        rendered = repr(widget)

    assert rendered.startswith("<Widget id=")
    assert str(widget.id) in rendered


# -- feedback from the first consumer of this API -------------------------


def test_from_env_reads_every_field() -> None:
    """A partial reader silently ignores a setting the operator changed."""
    config = DatabaseConfig.from_env(
        {
            URL_VAR: "postgresql+asyncpg://u@h/d",
            f"{SETTING_VARS}POOL_TIMEOUT": "7",
            f"{SETTING_VARS}POOL_RECYCLE": "60",
            f"{SETTING_VARS}POOL_PRE_PING": "false",
            f"{SETTING_VARS}CONNECT_TIMEOUT": "3",
        }
    )
    assert config.pool_timeout == 7.0
    assert config.pool_recycle == 60
    assert config.pool_pre_ping is False
    assert config.connect_timeout == 3.0


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param("postgresql+asyncpg://u@h/d", {"timeout": 3.0}, id="asyncpg"),
        pytest.param("postgresql+psycopg://u@h/d", {"connect_timeout": 3}, id="psycopg"),
        pytest.param("sqlite+aiosqlite:///:memory:", {}, id="sqlite"),
    ],
)
def test_the_connect_timeout_reaches_the_driver(url: str, expected: dict[str, object]) -> None:
    """It was declared and then never passed anywhere — a dead knob."""
    config = DatabaseConfig(url=url, connect_timeout=3.0)
    assert Database._connect_args(config) == expected


@pytest.mark.postgres
async def test_reading_a_timestamp_after_an_update_does_not_need_a_refresh(
    database: Database,
) -> None:
    """Regression: this used to raise ``MissingGreenlet``.

    With ``onupdate=func.now()`` the value is computed server-side, so after an
    UPDATE the attribute is expired and the next access triggers a lazy refresh
    — which under asyncio raises from whatever line happened to touch it, far
    from the cause. A Python-side ``onupdate`` has the value in hand.
    """
    async with uow() as session:
        widget = Widget(name="original")
        session.add(widget)
        await session.flush()

    async with uow() as session:
        stored = await session.get_one(Widget, widget.id)
        stored.name = "changed"

    # Outside the block, with no session: the trap fired exactly here.
    assert stored.name == "changed"
    assert stored.updated_at.tzinfo is not None
