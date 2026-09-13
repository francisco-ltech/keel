"""Migration safety.

The test that matters most here is ``test_two_replicas_booting_at_once``. Every
other test in this file pins a detail; that one pins the whole reason the module
exists. It runs two ``upgrade()`` calls concurrently against one database, with
a migration slow enough that they genuinely overlap, and asserts three
independent things: only one of them applied anything, the *loser* already saw
the winner's committed revision before it started (so it waited rather than
raced), and the migration's side effect happened exactly once. Remove the
advisory lock and all three fail.

Everything runs against a scratch database created for the session and dropped
afterwards, so the shared ``keel`` database is left exactly as it was found.
That is not only tidiness: ``compare_metadata`` reports every table it can see,
so a drift check sharing a database with another test's tables could never
legitimately return "no differences".
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path

import anyio
import pytest
from sqlalchemy import Column, MetaData, PrimaryKeyConstraint, Table, Text, Uuid, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from keel.database import Database, DatabaseConfig
from keel.database.migrations import (
    MIGRATION_LOCK_KEY,
    AlembicConfig,
    MigrationEnvironmentError,
    MigrationError,
    MigrationLockTimeoutError,
    SchemaDriftError,
    UpgradeResult,
    assert_no_drift,
    check_for_drift,
    upgrade,
)
from keel.database.model import NAMING_CONVENTION

pytestmark = [pytest.mark.anyio]

SCRATCH_DATABASE = "keel_mig_test"
PREFIX = "keel_mig_test_"
LEDGER = f"{PREFIX}ledger"
VERSION_TABLE = f"{PREFIX}version"

CLASSID = MIGRATION_LOCK_KEY >> 32
OBJID = MIGRATION_LOCK_KEY & 0xFFFFFFFF


# -- a scratch migration environment --------------------------------------

ENV_PY = '''
"""A minimal env.py that honours the shared connection.

This is the connection-sharing branch every application's env.py needs in order
to be driven by keel.database.migrations.upgrade(); see test_an_env_py_that_
opens_its_own_loop_is_reported for what happens without it.
"""

from alembic import context

config = context.config
connection = config.attributes.get("connection")
if connection is None:
    raise RuntimeError("this env.py is only ever driven programmatically")

context.configure(
    connection=connection,
    target_metadata=None,
    version_table=config.attributes.get("version_table", "alembic_version"),
)
with context.begin_transaction():
    context.run_migrations()
'''

STANDALONE_ENV_PY = '''
"""An env.py in the style of Alembic's async template: its own event loop."""

import asyncio


async def _run():
    return None


_coroutine = _run()
try:
    asyncio.run(_coroutine)
finally:
    # Closed explicitly so the failure is a clean RuntimeError rather than a
    # RuntimeError plus a "coroutine was never awaited" warning.
    _coroutine.close()
'''

LEDGER_MIGRATION = f'''
"""create the ledger

Revision ID: 0001_ledger
Revises:
"""

import sqlalchemy as sa
from alembic import op

revision = "0001_ledger"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "{LEDGER}",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("note", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_{LEDGER}")),
    )


def downgrade() -> None:
    op.drop_table("{LEDGER}")
'''

SLOW_ENTRY_MIGRATION = f'''
"""record that this process migrated, slowly

Revision ID: 0002_entry
Revises: 0001_ledger
"""

from alembic import op

revision = "0002_entry"
down_revision = "0001_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Slow on purpose: without it the two concurrent upgrades might not overlap
    # and the test would pass whether or not the lock exists.
    op.execute("SELECT pg_sleep(0.75)")
    op.execute(
        "INSERT INTO {LEDGER} (id, note) VALUES (gen_random_uuid(), 'migrated')"
    )


def downgrade() -> None:
    op.execute("DELETE FROM {LEDGER} WHERE note = 'migrated'")
'''

FAILING_MIGRATION = f'''
"""a migration that dies half way

Revision ID: 0002_boom
Revises: 0001_ledger
"""

from alembic import op

revision = "0002_boom"
down_revision = "0001_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("INSERT INTO {LEDGER} (id, note) VALUES (gen_random_uuid(), 'doomed')")
    op.execute("SELECT 1 / 0")


def downgrade() -> None:
    pass
'''


def write_environment(root: Path, *, env: str = ENV_PY, revisions: list[str] | None = None) -> Path:
    """Lay out a migrations directory under ``root`` and return it."""
    location = root / "migrations"
    versions = location / "versions"
    versions.mkdir(parents=True)
    (location / "env.py").write_text(env)
    for index, source in enumerate(revisions or [], start=1):
        (versions / f"{index:04d}.py").write_text(source)
    return location


@pytest.fixture
def alembic_config(tmp_path: Path) -> AlembicConfig:
    """The two-revision environment used by most tests."""
    return AlembicConfig(
        script_location=write_environment(
            tmp_path, revisions=[LEDGER_MIGRATION, SLOW_ENTRY_MIGRATION]
        ),
        version_table=VERSION_TABLE,
    )


# -- an isolated database --------------------------------------------------

# Not tidiness: ``compare_metadata`` reports *every* table it can see, so a drift
# check sharing a database with other tests could never return "no differences".


@pytest.fixture(scope="session")
def migration_database_url(database_url: str) -> Iterator[str]:
    """Create a scratch database for this module and drop it at the end."""
    admin_url = make_url(database_url)
    scratch_url = admin_url.set(database=SCRATCH_DATABASE)

    async def administer(*statements: str) -> None:
        # AUTOCOMMIT because CREATE/DROP DATABASE cannot run inside a transaction.
        engine = create_async_engine(
            admin_url.render_as_string(hide_password=False), isolation_level="AUTOCOMMIT"
        )
        try:
            async with engine.connect() as connection:
                for statement in statements:
                    await connection.execute(text(statement))
        finally:
            await engine.dispose()

    drop = f'DROP DATABASE IF EXISTS "{SCRATCH_DATABASE}" WITH (FORCE)'
    anyio.run(administer, drop, f'CREATE DATABASE "{SCRATCH_DATABASE}"')
    # render_as_string, not str(): URL.__str__ masks the password.
    yield scratch_url.render_as_string(hide_password=False)
    anyio.run(administer, drop)


@pytest.fixture
async def clean_database(migration_database_url: str) -> AsyncIterator[str]:
    """Empty the scratch database before each test.

    Recreating the schema is both faster and more thorough than dropping named
    tables: a migration under test may have created something the test does not
    know about, and a failed one may have left it half-built.
    """
    engine = create_async_engine(migration_database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("DROP SCHEMA public CASCADE"))
            await connection.execute(text("CREATE SCHEMA public"))
    finally:
        await engine.dispose()
    yield migration_database_url


@pytest.fixture
async def database(clean_database: str) -> AsyncIterator[Database]:
    """A live database with nothing in it."""
    instance = Database(DatabaseConfig(url=clean_database))
    yield instance
    await instance.close()


@pytest.fixture
async def replica(clean_database: str) -> AsyncIterator[Callable[[], Database]]:
    """A factory for extra ``Database`` instances, standing in for extra pods.

    Separate instances, not a shared one: each replica owns its own pool, which
    is what makes the advisory lock the only thing coordinating them.
    """
    instances: list[Database] = []

    def build() -> Database:
        instance = Database(DatabaseConfig(url=clean_database))
        instances.append(instance)
        return instance

    yield build

    for instance in instances:
        await instance.close()


async def advisory_locks_held(database: Database) -> int:
    """How many backends currently hold Keel's migration advisory lock."""
    async with database.connect() as connection:
        result = await connection.execute(
            text(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                "AND classid = :classid AND objid = :objid AND objsubid = 1"
            ),
            {"classid": CLASSID, "objid": OBJID},
        )
        return int(result.scalar_one())


async def ledger_notes(database: Database) -> list[str]:
    """Every note the migrations have written."""
    async with database.connect() as connection:
        result = await connection.execute(text(f"SELECT note FROM {LEDGER}"))
        return [str(row[0]) for row in result]


async def table_exists(database: Database, name: str) -> bool:
    """Whether a table of that name exists."""
    async with database.connect() as connection:
        result = await connection.execute(text("SELECT to_regclass(:name)"), {"name": name})
        return result.scalar_one() is not None


# -- AlembicConfig ---------------------------------------------------------


def test_the_script_location_is_made_absolute(tmp_path: Path) -> None:
    location = write_environment(tmp_path)
    config = AlembicConfig(script_location=Path(location))
    assert config.script_location.is_absolute()


def test_a_missing_script_location_is_rejected_at_construction(tmp_path: Path) -> None:
    """Better here than as 'unknown revision head' at deploy time."""
    with pytest.raises(MigrationError, match="is not a directory"):
        AlembicConfig(script_location=tmp_path / "nowhere")


def test_from_ini_resolves_the_script_location_relative_to_the_ini(tmp_path: Path) -> None:
    """Alembic resolves it against the CWD, which is why it breaks under systemd."""
    write_environment(tmp_path)
    ini = tmp_path / "alembic.ini"
    ini.write_text("[alembic]\nscript_location = migrations\n")

    config = AlembicConfig.from_ini(ini)

    assert config.script_location == (tmp_path / "migrations").resolve()
    assert config.ini_path == ini.resolve()


def test_from_ini_rejects_a_missing_file(tmp_path: Path) -> None:
    with pytest.raises(MigrationError, match="no alembic configuration"):
        AlembicConfig.from_ini(tmp_path / "absent.ini")


def test_from_ini_rejects_an_ini_without_a_script_location(tmp_path: Path) -> None:
    ini = tmp_path / "alembic.ini"
    ini.write_text("[alembic]\ntimezone = UTC\n")
    with pytest.raises(MigrationError, match="declares no script_location"):
        AlembicConfig.from_ini(ini)


def test_the_version_table_reaches_env_py_through_attributes(tmp_path: Path) -> None:
    """``env.py`` is the only thing that can pass it to ``context.configure``."""
    config = AlembicConfig(script_location=write_environment(tmp_path), version_table=VERSION_TABLE)
    built = config.to_alembic_config()
    assert built.attributes["version_table"] == VERSION_TABLE
    assert built.get_main_option("script_location") == str(config.script_location)


def test_the_lock_key_is_stable_and_positive() -> None:
    """Replicas only agree on a lock if they derive the same number from it."""
    assert MIGRATION_LOCK_KEY == 2187107764658350896
    assert 0 < MIGRATION_LOCK_KEY < 2**63


# -- upgrade ---------------------------------------------------------------


@pytest.mark.postgres
async def test_upgrade_applies_every_revision_and_says_which(
    database: Database, alembic_config: AlembicConfig
) -> None:
    result = await upgrade(database, alembic_config)

    assert result.before == ()
    assert result.after == ("0002_entry",)
    assert set(result.applied) == {"0001_ledger", "0002_entry"}
    assert result.was_up_to_date is False
    assert await ledger_notes(database) == ["migrated"]


@pytest.mark.postgres
async def test_a_second_upgrade_reports_that_there_was_nothing_to_do(
    database: Database, alembic_config: AlembicConfig
) -> None:
    """The result every replica but one gets, and the one a boot log wants."""
    await upgrade(database, alembic_config)
    result = await upgrade(database, alembic_config)

    assert result.applied == ()
    assert result.was_up_to_date is True
    assert "already up to date" in str(result)


@pytest.mark.postgres
async def test_upgrade_can_stop_at_a_named_revision(
    database: Database, alembic_config: AlembicConfig
) -> None:
    result = await upgrade(database, alembic_config, "0001_ledger")

    assert result.after == ("0001_ledger",)
    assert result.applied == ("0001_ledger",)
    assert await ledger_notes(database) == []


@pytest.mark.postgres
async def test_two_replicas_booting_at_once(
    database: Database, replica: Callable[[], Database], alembic_config: AlembicConfig
) -> None:
    """The whole point of the feature.

    Two processes call ``upgrade()`` at the same moment against one database.
    Without the advisory lock both read an empty version table, both run
    ``0001_ledger``, and one of them dies on ``DuplicateTable``; if they get past
    that, both run ``0002_entry`` and the ledger ends up with two rows.

    Three assertions, each of which fails on its own without the lock:

    * exactly one call reports revisions applied;
    * the other call's *pre-migration* read already shows the winner's committed
      head — proof it was serialised behind it rather than racing it;
    * the migration's side effect happened once.
    """
    first = replica()
    second = replica()
    results: list[UpgradeResult] = []

    started = time.monotonic()

    async def boot(instance: Database) -> None:
        results.append(await upgrade(instance, alembic_config, timeout=30.0))

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(boot, first)
        tasks.start_soon(boot, second)

    elapsed = time.monotonic() - started

    winners = [result for result in results if result.applied]
    losers = [result for result in results if not result.applied]

    assert len(winners) == 1, f"both replicas migrated: {results}"
    assert len(losers) == 1
    assert set(winners[0].applied) == {"0001_ledger", "0002_entry"}
    assert losers[0].before == ("0002_entry",), (
        "the waiting replica should have seen the winner's committed head before it "
        f"started, but saw {losers[0].before}"
    )
    assert losers[0].was_up_to_date is True
    assert await ledger_notes(database) == ["migrated"]
    assert elapsed >= 0.75, "the runs did not actually overlap; the test proves nothing"


@pytest.mark.postgres
async def test_the_lock_is_released_after_a_successful_upgrade(
    database: Database, alembic_config: AlembicConfig
) -> None:
    assert await advisory_locks_held(database) == 0
    await upgrade(database, alembic_config)
    assert await advisory_locks_held(database) == 0


@pytest.mark.postgres
async def test_the_lock_is_released_after_a_failing_upgrade(
    database: Database, tmp_path: Path
) -> None:
    """The case that matters: a leaked lock turns one bad deploy into a wedged one."""
    config = AlembicConfig(
        script_location=write_environment(
            tmp_path, revisions=[LEDGER_MIGRATION, FAILING_MIGRATION]
        ),
        version_table=VERSION_TABLE,
    )

    with pytest.raises(Exception, match="division by zero"):
        await upgrade(database, config)

    assert await advisory_locks_held(database) == 0
    # And nothing was left half-applied: the whole upgrade ran in one
    # transaction, so even the revision that succeeded was rolled back.
    assert await table_exists(database, LEDGER) is False


@pytest.mark.postgres
async def test_a_contended_lock_times_out_with_a_usable_error(
    database: Database, replica: Callable[[], Database], alembic_config: AlembicConfig
) -> None:
    """Holds the lock by hand, so the timeout is tested rather than the migration."""
    holder = replica()
    async with holder.engine.connect() as connection:
        session = await connection.execution_options(isolation_level="AUTOCOMMIT")
        await session.execute(text("SELECT pg_advisory_lock(:key)"), {"key": MIGRATION_LOCK_KEY})
        try:
            started = time.monotonic()
            with pytest.raises(MigrationLockTimeoutError) as error:
                await upgrade(database, alembic_config, timeout=0.5, poll_interval=0.05)
            waited = time.monotonic() - started
        finally:
            await session.execute(
                text("SELECT pg_advisory_unlock(:key)"), {"key": MIGRATION_LOCK_KEY}
            )

    assert error.value.key == MIGRATION_LOCK_KEY
    assert error.value.timeout == 0.5
    assert "pg_locks" in str(error.value), "the error should say how to find the holder"
    assert waited >= 0.5, "it gave up before the timeout elapsed"


@pytest.mark.postgres
async def test_the_lock_is_released_even_when_the_wait_timed_out(
    database: Database, replica: Callable[[], Database], alembic_config: AlembicConfig
) -> None:
    """A caller that never got the lock must not release someone else's."""
    holder = replica()
    async with holder.engine.connect() as connection:
        session = await connection.execution_options(isolation_level="AUTOCOMMIT")
        await session.execute(text("SELECT pg_advisory_lock(:key)"), {"key": MIGRATION_LOCK_KEY})
        with pytest.raises(MigrationLockTimeoutError):
            await upgrade(database, alembic_config, timeout=0.2, poll_interval=0.05)
        assert await advisory_locks_held(holder) == 1, "the holder lost its lock"
        await session.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": MIGRATION_LOCK_KEY})

    assert await advisory_locks_held(database) == 0


@pytest.mark.postgres
async def test_an_env_py_that_opens_its_own_loop_is_reported(
    database: Database, tmp_path: Path
) -> None:
    """The nested ``asyncio.run`` otherwise raises something about event loops."""
    config = AlembicConfig(
        script_location=write_environment(
            tmp_path, env=STANDALONE_ENV_PY, revisions=[LEDGER_MIGRATION]
        ),
        version_table=VERSION_TABLE,
    )

    with pytest.raises(MigrationEnvironmentError) as error:
        await upgrade(database, config)

    assert "config.attributes['connection']" in str(error.value)
    assert await advisory_locks_held(database) == 0


# -- drift -----------------------------------------------------------------


def ledger_metadata(*, extra_column: bool = False, extra_table: bool = False) -> MetaData:
    """A metadata mirroring what ``0001_ledger`` creates, optionally drifted."""
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    ledger = Table(
        LEDGER,
        metadata,
        Column("id", Uuid(), nullable=False),
        Column("note", Text(), nullable=False),
        PrimaryKeyConstraint("id", name=f"pk_{LEDGER}"),
    )
    if extra_column:
        ledger.append_column(Column("severity", Text(), nullable=True))
    if extra_table:
        Table(
            f"{PREFIX}orphan",
            metadata,
            Column("id", Uuid(), nullable=False),
            PrimaryKeyConstraint("id", name=f"pk_{PREFIX}orphan"),
        )
    return metadata


@pytest.mark.postgres
async def test_no_drift_when_the_migrations_describe_the_models(
    database: Database, alembic_config: AlembicConfig
) -> None:
    """Includes the version table, which must not be reported as unexpected."""
    await upgrade(database, alembic_config)

    assert await check_for_drift(database, alembic_config, metadata=ledger_metadata()) == []


@pytest.mark.postgres
async def test_a_model_with_no_migration_is_reported(
    database: Database, alembic_config: AlembicConfig
) -> None:
    await upgrade(database, alembic_config)

    differences = await check_for_drift(
        database, alembic_config, metadata=ledger_metadata(extra_table=True)
    )

    assert len(differences) == 1
    assert f"{PREFIX}orphan" in differences[0]
    assert "missing from the database" in differences[0]


@pytest.mark.postgres
async def test_a_column_added_to_a_model_with_no_migration_is_reported(
    database: Database, alembic_config: AlembicConfig
) -> None:
    await upgrade(database, alembic_config)

    differences = await check_for_drift(
        database, alembic_config, metadata=ledger_metadata(extra_column=True)
    )

    assert len(differences) == 1
    assert LEDGER in differences[0]
    assert "severity" in differences[0]


@pytest.mark.postgres
async def test_a_table_the_models_no_longer_declare_is_reported(
    database: Database, alembic_config: AlembicConfig
) -> None:
    """Drift is symmetric: a table nobody owns is as much a signal as a missing one."""
    await upgrade(database, alembic_config)

    differences = await check_for_drift(
        database, alembic_config, metadata=MetaData(naming_convention=NAMING_CONVENTION)
    )

    assert any(
        LEDGER in difference and "not in the models" in difference for difference in differences
    )


@pytest.mark.postgres
async def test_assert_no_drift_is_silent_when_they_agree(
    database: Database, alembic_config: AlembicConfig
) -> None:
    await upgrade(database, alembic_config)
    await assert_no_drift(database, alembic_config, metadata=ledger_metadata())


@pytest.mark.postgres
async def test_assert_no_drift_names_the_offending_table_and_column(
    database: Database, alembic_config: AlembicConfig
) -> None:
    """A check that only says 'drift detected' sends the reader back to the CLI."""
    await upgrade(database, alembic_config)

    with pytest.raises(SchemaDriftError) as error:
        await assert_no_drift(database, alembic_config, metadata=ledger_metadata(extra_column=True))

    message = str(error.value)
    assert LEDGER in message
    assert "severity" in message
    assert "autogenerate" in message, "the message should say what to do about it"
    assert len(error.value.differences) == 1
    assert error.value.differences[0] in message


@pytest.mark.postgres
async def test_drift_is_detected_before_any_migration_has_run(
    database: Database, alembic_config: AlembicConfig
) -> None:
    """An empty database against real models is the loudest drift there is."""
    differences = await check_for_drift(database, alembic_config, metadata=ledger_metadata())

    assert len(differences) == 1
    assert LEDGER in differences[0]
    assert "missing from the database" in differences[0]
