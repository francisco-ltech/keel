"""Migration safety: a lock around ``upgrade``, and a check that models match it.

Two failure modes motivate this module, and neither of them is hypothetical.

**N replicas boot at once.** Every one of them runs ``alembic upgrade head`` on
start-up, and Alembic offers no coordination: each reads ``alembic_version``,
sees the same stale revision, and runs the same ``CREATE TABLE``. The winners
and losers are decided by Postgres, so the symptom is a random subset of pods
crash-looping with ``DuplicateTableError``, or — worse, and rarer — an
``alembic_version`` row that says a migration ran when only half of it did.
:func:`upgrade` closes that window with a **session-level Postgres advisory
lock**: whoever takes it migrates, everyone else waits and then finds nothing
left to do.

**A model changed and nobody generated a migration.** SQLAlchemy will happily
emit queries for a column the database does not have, so this surfaces as a
``ProgrammingError`` on the first request that touches it, in production,
minutes after a deploy that passed CI. :func:`check_for_drift` turns it into a
build failure by asking Alembic's own autogenerate comparison whether the live
schema and :class:`keel.database.Model`'s metadata still agree.

Both comparisons Alembic can be talkative about — ``compare_type`` and
``compare_server_default`` — are deliberately **enabled** here. They are off by
default in Alembic because they produce false positives on some dialect/type
combinations, and the temptation is to leave them off for a quiet life. That
would mean a widened ``VARCHAR`` or a changed ``server_default`` reports as "no
drift", which is precisely the change most likely to be forgotten. Keel's model
vocabulary was checked against a live Postgres — UUID primary keys, ``DateTime``
with ``timezone=True`` and a ``now()`` server default, ``Boolean`` with a
``true()`` server default, length-bounded ``String``, ``Text``, and foreign keys
with ``ondelete`` — and produces no spurious differences with both switches on.
It also matches what the starter template's ``env.py`` configures, so a drift
check and a ``--autogenerate`` run agree with each other.

A note on the shape of :func:`upgrade`. Alembic's API is synchronous, so the
migration runs through ``AsyncConnection.run_sync`` and the connection is handed
to ``env.py`` through ``config.attributes["connection"]`` — this is Alembic's
own documented recipe for programmatic use from asyncio. An ``env.py`` that
ignores that attribute and builds its own engine with ``asyncio.run`` cannot be
driven from here: the nested ``asyncio.run`` raises inside the running loop.
:func:`upgrade` detects that case and says so rather than letting the error
surface as something unrelated.
"""

from __future__ import annotations

import asyncio
import configparser
import hashlib
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, MetaData, text

from keel.database.engine import Database
from keel.database.model import Model
from keel.exceptions import KeelError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection

logger = logging.getLogger(__name__)

LOCK_NAMESPACE: Final = "keel.database.migrations"
"""The constant the advisory-lock key is derived from.

It is a *string* rather than a hand-picked number so that the key is
self-documenting and so that a second, unrelated lock can be minted the same way
without anyone having to remember which integers are already spoken for.
"""

MIGRATION_LOCK_KEY: Final[int] = (
    int.from_bytes(hashlib.sha256(LOCK_NAMESPACE.encode()).digest()[:8], "big")
    & 0x7FFF_FFFF_FFFF_FFFF
)
"""The advisory-lock key: ``sha256("keel.database.migrations")[:8]``, masked to
63 bits (value ``2187107764658350896``).

Two properties are being bought here. It is **stable** — every process in every
replica of every deployment derives the same number from the same string, which
is the entire point of the lock. And it is **positive**: ``pg_advisory_lock``
takes a signed ``bigint``, and the sign of a truncated hash is an accident, so
masking the top bit keeps the number readable in ``pg_locks`` and in a log line
instead of appearing as a large negative.

The collision risk against an application's own advisory locks is the same as
for any 63-bit key and is not worth defending against; the practical advice, if
you mint your own, is to derive them the same way.
"""

DEFAULT_LOCK_TIMEOUT: Final = 60.0
"""Seconds :func:`upgrade` will wait for the lock before giving up.

Long enough that a replica behind a slow migration waits it out, short enough
that a genuinely stuck lock fails a deployment rather than hanging it — a boot
that never completes is much harder to diagnose than one that exits with a
message.
"""

DEFAULT_POLL_INTERVAL: Final = 0.1
"""Seconds between attempts at the lock.

The wait polls ``pg_try_advisory_lock`` rather than blocking in
``pg_advisory_lock``, for two reasons. A blocking lock is a running statement,
so it collides with the ``statement_timeout`` Keel sets on every connection and
would fail with a confusing timeout from the wrong layer. And polling leaves the
event loop free, so a process waiting for the lock can still answer a liveness
probe.
"""


class MigrationError(KeelError):
    """Base class for migration failures.

    Lives here rather than in :mod:`keel.exceptions` only because migrations are
    an optional extra: importing this module pulls in Alembic, and the shared
    hierarchy must stay importable without it.
    """


class MigrationLockTimeoutError(MigrationError):
    """Raised when the migration advisory lock could not be taken in time.

    Almost always means another process is still migrating and is slower than
    the timeout allowed. The rarer and more interesting cause is a leaked lock:
    a backend that is still alive, still holds the lock, and is no longer doing
    anything with it. ``pg_locks`` joined against ``pg_stat_activity`` on the
    key below will say which.

    Attributes:
        key: The advisory-lock key that was contended.
        timeout: How long the caller waited, in seconds.
    """

    def __init__(self, key: int, timeout: float) -> None:
        self.key = key
        self.timeout = timeout
        super().__init__(
            f"could not acquire the migration advisory lock ({key}) within {timeout}s; "
            f"another process is probably still migrating. Inspect it with: "
            f"SELECT * FROM pg_locks WHERE locktype = 'advisory' "
            f"AND classid = {key >> 32} AND objid = {key & 0xFFFFFFFF}"
        )


class SchemaDriftError(MigrationError):
    """Raised by :func:`assert_no_drift` when models and migrations disagree.

    Attributes:
        differences: One human-readable line per difference, in the order
            Alembic reported them.
    """

    def __init__(self, differences: list[str]) -> None:
        self.differences = differences
        listing = "\n".join(f"  - {difference}" for difference in differences)
        super().__init__(
            f"the database schema does not match the models "
            f"({len(differences)} difference(s)):\n{listing}\n"
            f"Generate a migration with `alembic revision --autogenerate` and review it."
        )


class MigrationEnvironmentError(MigrationError):
    """Raised when the application's ``env.py`` cannot be driven from asyncio.

    :func:`upgrade` shares its connection through ``config.attributes``, which is
    Alembic's documented recipe. An ``env.py`` that ignores it and opens its own
    async engine calls ``asyncio.run`` inside an already-running loop, and the
    resulting ``RuntimeError`` says nothing about migrations at all.
    """


@dataclass(frozen=True, slots=True)
class AlembicConfig:
    """Where an application's migrations live.

    A plain frozen dataclass, matching
    :class:`~keel.database.config.DatabaseConfig`: Keel's core is deliberately
    pydantic-free, and this is a value object with no behaviour worth a base
    class. It exists so that callers stop hand-rolling
    ``alembic.config.Config`` — which is easy to get subtly wrong, because a
    relative ``script_location`` is resolved against the *current working
    directory*, so the same configuration works from a Makefile and fails from a
    systemd unit.

    Attributes:
        script_location: The migrations directory — the one holding ``env.py``
            and ``versions/``. Resolved to an absolute path on construction, so
            it no longer depends on where the process was started.
        ini_path: An ``alembic.ini`` to load first, for the settings that only
            live there (``file_template``, ``post_write_hooks``, logging).
            Optional: nothing :mod:`keel.database.migrations` does needs one.
        version_table: The table Alembic stamps. Worth naming explicitly if the
            database is shared with another application, where two services both
            writing ``alembic_version`` is a silent corruption.
        version_table_schema: The schema that table lives in, or ``None`` for
            the connection's default.

    Raises:
        MigrationError: If ``script_location`` is not a directory. This is
            checked at construction on purpose — the alternative is an
            "unknown revision 'head'" from Alembic at deploy time, which reads
            like a broken migration history rather than a wrong path.
    """

    script_location: Path
    ini_path: Path | None = None
    version_table: str = "alembic_version"
    version_table_schema: str | None = None

    def __post_init__(self) -> None:
        """Normalise both paths to absolute and reject a missing script directory."""
        object.__setattr__(self, "script_location", Path(self.script_location).resolve())
        if self.ini_path is not None:
            object.__setattr__(self, "ini_path", Path(self.ini_path).resolve())
        if not self.script_location.is_dir():
            raise MigrationError(
                f"migration script location {self.script_location} is not a directory"
            )

    @classmethod
    def from_ini(
        cls,
        ini_path: str | Path,
        *,
        version_table: str = "alembic_version",
        version_table_schema: str | None = None,
    ) -> AlembicConfig:
        """Build a configuration from an ``alembic.ini``.

        ``script_location`` is read from the file and resolved **relative to the
        ini** rather than to the current working directory. Alembic itself does
        the latter, which is the single most common reason a migration command
        works in one place and not another; an ini and its ``versions/``
        directory ship together, so anchoring to the file is the interpretation
        that is right every time.

        Args:
            ini_path: Path to the ``alembic.ini``.
            version_table: The table Alembic stamps.
            version_table_schema: The schema that table lives in.

        Returns:
            The configuration.

        Raises:
            MigrationError: If the file is missing or declares no
                ``script_location``.
        """
        path = Path(ini_path).resolve()
        if not path.is_file():
            raise MigrationError(f"no alembic configuration at {path}")

        parser = configparser.ConfigParser()
        parser.read(path)
        location = parser.get("alembic", "script_location", fallback="")
        if not location:
            raise MigrationError(f"{path} declares no script_location under [alembic]")

        return cls(
            script_location=(path.parent / location),
            ini_path=path,
            version_table=version_table,
            version_table_schema=version_table_schema,
        )

    def to_alembic_config(self) -> Config:
        """Build the ``alembic.config.Config`` the Alembic API wants.

        The version-table settings are written both as main options and into
        ``attributes``, because there is no single place Alembic reads them
        from: ``env.py`` is the only thing that can pass them to
        ``context.configure``, and it needs somewhere to find them.

        Returns:
            A configuration ready to hand to :mod:`alembic.command`.
        """
        config = Config(file_=str(self.ini_path) if self.ini_path else None)
        config.set_main_option("script_location", str(self.script_location))
        config.set_main_option("version_table", self.version_table)
        if self.version_table_schema is not None:
            config.set_main_option("version_table_schema", self.version_table_schema)
        config.attributes["version_table"] = self.version_table
        config.attributes["version_table_schema"] = self.version_table_schema
        return config

    @property
    def migration_context_options(self) -> dict[str, Any]:
        """Options shared by every ``MigrationContext`` this module configures.

        Returns:
            Keyword options for ``MigrationContext.configure(opts=...)``.
        """
        return {
            "version_table": self.version_table,
            "version_table_schema": self.version_table_schema,
        }


@dataclass(frozen=True, slots=True)
class UpgradeResult:
    """What one :func:`upgrade` call actually did.

    Returned rather than only logged, because the interesting assertion in a
    boot sequence — and in the test that proves the lock works — is "exactly one
    of these processes did the work", and that is a value, not a log line.

    Attributes:
        before: The revisions stamped in the database before the run. Empty on a
            database that has never been migrated.
        after: The revisions stamped afterwards.
        applied: The revisions this call ran, newest first. **Empty means the
            database was already up to date** — which is the expected result for
            every replica but one.
    """

    before: tuple[str, ...]
    after: tuple[str, ...]
    applied: tuple[str, ...]

    @property
    def was_up_to_date(self) -> bool:
        """Whether there was nothing to do."""
        return not self.applied and self.before == self.after

    def __str__(self) -> str:
        """Render a one-line summary suitable for a boot log."""
        if self.was_up_to_date:
            return f"database already up to date at {', '.join(self.before) or 'base'}"
        return (
            f"applied {len(self.applied)} revision(s): "
            f"{', '.join(reversed(self.applied))} "
            f"({', '.join(self.before) or 'base'} -> {', '.join(self.after) or 'base'})"
        )


async def upgrade(
    database: Database,
    config: AlembicConfig,
    revision: str = "head",
    *,
    timeout: float = DEFAULT_LOCK_TIMEOUT,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
) -> UpgradeResult:
    """Run ``alembic upgrade`` under a Postgres advisory lock.

    Safe to call unconditionally from every replica's start-up. The first
    process to take the lock migrates; the others block on
    :data:`MIGRATION_LOCK_KEY` until it finishes, then discover the work is done
    and return a result whose ``applied`` is empty. Without the lock they all
    read the same stale ``alembic_version``, run the same DDL, and a random
    subset crash-loops on ``DuplicateTableError``.

    Three details are load-bearing:

    * **The lock is session-level, on its own connection.** Session-level so it
      survives the migration's transaction boundaries; on a separate connection
      so that releasing it cannot fail merely because the migration's
      transaction aborted. A ``pg_advisory_unlock`` issued on a connection whose
      transaction is in the failed state raises, which is exactly the moment the
      release matters most.
    * **The wait polls rather than blocks.** ``pg_advisory_lock`` is a running
      statement and would be killed by the ``statement_timeout`` Keel applies to
      every connection; ``pg_try_advisory_lock`` in a loop gives an honest
      timeout and keeps the event loop responsive.
    * **``statement_timeout`` is lifted for the migration itself.** The default
      is sized for a request. Backfilling a column or building an index is not a
      request, and having a deploy die 30 seconds into an index build — leaving
      a partially applied migration — is a worse failure than a slow deploy.

    The connection is passed to ``env.py`` through
    ``config.attributes["connection"]``, Alembic's documented recipe for
    programmatic use under asyncio. Your ``env.py`` must honour it::

        connectable = config.attributes.get("connection")
        if connectable is not None:
            do_run_migrations(connectable)
        else:
            asyncio.run(run_async_migrations())

    Args:
        database: The database to migrate. Its engine is borrowed; no second
            pool is opened.
        config: Where the migrations live.
        revision: The Alembic revision to upgrade to. ``"head"`` unless you are
            deliberately stepping.
        timeout: Seconds to wait for the lock before giving up.
        poll_interval: Seconds between attempts at the lock.

    Returns:
        What happened, including whether anything was applied at all.

    Raises:
        MigrationLockTimeoutError: If the lock was not free within ``timeout``.
        MigrationEnvironmentError: If ``env.py`` ignores the shared connection
            and tries to open its own async engine.
    """
    async with (
        _advisory_lock(database, timeout=timeout, poll_interval=poll_interval),
        database.connect() as connection,
    ):
        await connection.execute(text("SET statement_timeout = 0"))
        result = await connection.run_sync(_upgrade_sync, config, revision)

    logger.info("%s", result)
    return result


async def check_for_drift(
    database: Database,
    config: AlembicConfig,
    *,
    metadata: MetaData | None = None,
) -> list[str]:
    """Report the differences between the live schema and the models.

    This is the check that makes "someone changed a model and forgot to generate
    a migration" a red build instead of a production incident. Wire it into CI
    against a database that has just had ``upgrade`` run on it: if the migration
    history really does describe the models, there is nothing to report.

    It is Alembic's own autogenerate comparison, so it sees exactly what
    ``alembic revision --autogenerate`` would write into a migration — including
    type and server-default changes, which are off by default in Alembic and are
    turned **on** here. See the module docstring for why, and for the evidence
    that Keel's model vocabulary does not trip them.

    What it does not see: anything outside SQLAlchemy's reflection. Triggers,
    functions, row-level security policies and partitioning are invisible to
    autogenerate, so a migration that only manages those will look like drift in
    neither direction. That is a property of Alembic, not of this function, and
    is worth knowing before treating an empty list as "the schema is correct".

    Args:
        database: The database whose live schema is inspected.
        config: Supplies the version-table settings, so Alembic's own bookkeeping
            table is not reported as an unexpected table.
        metadata: What to compare against. Defaults to
            :attr:`keel.database.Model.metadata`, which is where every model
            inheriting Keel's base registers itself. Override it only when a
            single process owns more than one declarative base, or in a test
            that must not be affected by every model the suite happens to have
            imported.

    Returns:
        One readable line per difference, in Alembic's order. **Empty means no
        drift.**
    """
    target = Model.metadata if metadata is None else metadata
    async with database.connect() as connection:
        return await connection.run_sync(_compare_sync, config, target)


async def assert_no_drift(
    database: Database,
    config: AlembicConfig,
    *,
    metadata: MetaData | None = None,
) -> None:
    """Raise unless the live schema matches the models.

    The one-line form of :func:`check_for_drift`, for a CI step or a start-up
    assertion in a non-production environment.

    Args:
        database: The database whose live schema is inspected.
        config: Supplies the version-table settings.
        metadata: What to compare against; defaults to
            :attr:`keel.database.Model.metadata`.

    Raises:
        SchemaDriftError: If there is any difference. The message lists every
            one of them, named by table and column, because a check that only
            says "drift detected" leaves the reader to re-run it by hand to find
            out what changed.
    """
    differences = await check_for_drift(database, config, metadata=metadata)
    if differences:
        raise SchemaDriftError(differences)


# -- the advisory lock ----------------------------------------------------


@asynccontextmanager
async def _advisory_lock(
    database: Database,
    *,
    key: int = MIGRATION_LOCK_KEY,
    timeout: float,
    poll_interval: float,
) -> AsyncIterator[None]:
    """Hold a session-level advisory lock for the duration of a block.

    Uses a dedicated connection in ``AUTOCOMMIT`` so the lock is not sitting
    inside an idle transaction — one that stayed open for the length of a long
    migration would hold back vacuum on every table in the database.

    Args:
        database: Supplies the engine to borrow a connection from.
        key: The advisory-lock key.
        timeout: Seconds to wait for the lock.
        poll_interval: Seconds between attempts.

    Yields:
        ``None``, with the lock held.

    Raises:
        MigrationLockTimeoutError: If the lock was not free within ``timeout``.
    """
    async with database.engine.connect() as connection:
        session = await connection.execution_options(isolation_level="AUTOCOMMIT")
        deadline = time.monotonic() + timeout
        attempts = 0
        while True:
            attempts += 1
            acquired = (
                await session.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": key})
            ).scalar_one()
            if acquired:
                break
            if time.monotonic() >= deadline:
                raise MigrationLockTimeoutError(key, timeout)
            if attempts == 1:
                logger.info("waiting for the migration advisory lock (%s)", key)
            await asyncio.sleep(poll_interval)

        try:
            yield
        finally:
            await _release(session, key)


async def _release(connection: AsyncConnection, key: int) -> None:
    """Release the advisory lock, discarding the connection if that fails.

    A session-level lock lives as long as its backend, so a pooled connection
    handed back while still holding one would pass the lock to whoever checks it
    out next — and every later migration in the process would block on a lock
    nobody is deliberately holding. Invalidating the connection closes the
    backend, which is the one release path that cannot itself fail.

    The failure is logged rather than raised: this runs in a ``finally``, and an
    exception here would replace whatever the migration was already failing
    with, which is the error the operator actually needs to see.

    Args:
        connection: The connection holding the lock.
        key: The advisory-lock key.
    """
    try:
        await connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
    except Exception:
        logger.exception(
            "could not release the migration advisory lock (%s); discarding "
            "the connection so the lock dies with its backend",
            key,
        )
        await connection.invalidate()


# -- the synchronous halves, run through AsyncConnection.run_sync ---------


def _current_heads(connection: Connection, config: AlembicConfig) -> tuple[str, ...]:
    """Read the revisions the database is stamped with.

    Args:
        connection: A synchronous connection.
        config: Supplies the version-table settings.

    Returns:
        The stamped revisions; empty if the version table does not exist yet.
    """
    context = MigrationContext.configure(
        connection=connection, opts=config.migration_context_options
    )
    return tuple(context.get_current_heads())


def _upgrade_sync(connection: Connection, config: AlembicConfig, revision: str) -> UpgradeResult:
    """Run ``alembic upgrade`` on an already-open synchronous connection.

    Args:
        connection: The synchronous facade handed over by ``run_sync``.
        config: Where the migrations live.
        revision: The revision to upgrade to.

    Returns:
        What was applied.

    Raises:
        MigrationEnvironmentError: If ``env.py`` ignores the shared connection.
    """
    alembic_config = config.to_alembic_config()
    alembic_config.attributes["connection"] = connection

    before = _current_heads(connection, config)
    try:
        command.upgrade(alembic_config, revision)
    except RuntimeError as error:
        if "asyncio.run() cannot be called from a running event loop" in str(error):
            raise MigrationEnvironmentError(
                f"{config.script_location / 'env.py'} opened its own event loop instead of "
                f"using the connection in config.attributes['connection']. Add the "
                f"connection-sharing branch documented on keel.database.migrations.upgrade()."
            ) from error
        raise
    after = _current_heads(connection, config)

    return UpgradeResult(before=before, after=after, applied=_applied(config, before, after))


def _applied(
    config: AlembicConfig, before: tuple[str, ...], after: tuple[str, ...]
) -> tuple[str, ...]:
    """Name the revisions between two stamps.

    Derived from the before/after stamps rather than observed as the migrations
    run, because the run happens inside ``env.py`` — application code this module
    does not control and must not require hooks in.

    Args:
        config: Supplies the script directory to walk.
        before: The stamps before the run.
        after: The stamps afterwards.

    Returns:
        The revisions applied, newest first. Empty if nothing moved.
    """
    if before == after:
        return ()
    script = ScriptDirectory.from_config(config.to_alembic_config())
    return tuple(
        entry.revision for entry in script.iterate_revisions(after or "base", before or "base")
    )


def _compare_sync(connection: Connection, config: AlembicConfig, metadata: MetaData) -> list[str]:
    """Compare the live schema against ``metadata`` on a synchronous connection.

    Args:
        connection: The synchronous facade handed over by ``run_sync``.
        config: Supplies the version-table settings.
        metadata: What to compare against.

    Returns:
        One readable line per difference.
    """
    context = MigrationContext.configure(
        connection=connection,
        opts={
            **config.migration_context_options,
            "target_metadata": metadata,
            # On, not off. See the module docstring: the default hides exactly
            # the changes people forget to migrate.
            "compare_type": True,
            "compare_server_default": True,
        },
    )
    return [_describe(difference) for difference in _flatten(compare_metadata(context, metadata))]


# -- rendering a diff a human can act on ----------------------------------


def _flatten(diffs: Any) -> list[Any]:
    """Unwrap Alembic's nested diff lists into one flat sequence.

    Column-level modifications arrive grouped in a sub-list — one list per
    column, holding a tuple per changed attribute — which is convenient for
    rendering a migration script and inconvenient for counting differences.

    Args:
        diffs: What ``compare_metadata`` returned.

    Returns:
        A flat list of diff tuples.
    """
    flat: list[Any] = []
    for diff in diffs:
        if isinstance(diff, list):
            flat.extend(_flatten(diff))
        else:
            flat.append(diff)
    return flat


def _qualified(schema: str | None, table: str) -> str:
    """Join a schema and table name for display.

    Args:
        schema: The schema, or ``None`` for the connection default.
        table: The table name.

    Returns:
        ``schema.table``, or just ``table``.
    """
    return f"{schema}.{table}" if schema else table


def _describe(diff: Any) -> str:
    """Render one Alembic diff tuple as a sentence naming what changed.

    The message is the whole product here. "3 differences found" sends the
    reader back to run autogenerate by hand; naming the table and column tells
    them whether they forgot a migration or whether the migration is wrong.

    Args:
        diff: One diff tuple from ``compare_metadata``.

    Returns:
        A single line, always beginning with the operation.
    """
    operation = str(diff[0])

    if operation in {"add_table", "remove_table"}:
        verb = "missing from the database" if operation == "add_table" else "not in the models"
        return f"table {diff[1].fullname!r} is {verb}"

    if operation in {"add_column", "remove_column"}:
        _, schema, table, column = diff
        verb = "missing from the database" if operation == "add_column" else "not in the models"
        return f"column {_qualified(schema, table)}.{column.name!r} ({column.type}) is {verb}"

    if operation.startswith("modify_"):
        _, schema, table, column, _existing, old, new = diff
        attribute = operation.removeprefix("modify_")
        return (
            f"column {_qualified(schema, table)}.{column!r} differs in {attribute}: "
            f"database has {old!r}, models say {new!r}"
        )

    if operation in {
        "add_index",
        "remove_index",
        "add_constraint",
        "remove_constraint",
        "add_fk",
        "remove_fk",
    }:
        obj = diff[1]
        kind = operation.split("_", 1)[1]
        table = getattr(getattr(obj, "table", None), "fullname", "?")
        verb = "missing from the database" if operation.startswith("add_") else "not in the models"
        return f"{kind} {getattr(obj, 'name', None)!r} on table {table!r} is {verb}"

    # Alembic grows new diff kinds (comments, identity columns) between minor
    # versions. An unrecognised one is still drift and must still be reported.
    return f"{operation}: {diff[1:]!r}"


__all__ = [
    "DEFAULT_LOCK_TIMEOUT",
    "DEFAULT_POLL_INTERVAL",
    "LOCK_NAMESPACE",
    "MIGRATION_LOCK_KEY",
    "AlembicConfig",
    "MigrationEnvironmentError",
    "MigrationError",
    "MigrationLockTimeoutError",
    "SchemaDriftError",
    "UpgradeResult",
    "assert_no_drift",
    "check_for_drift",
    "upgrade",
]
