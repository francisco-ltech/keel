"""The engine, the session factory, and the transaction idiom.

One rule shapes this whole module, and it is worth stating before the code:

    **A database session is never held for the lifetime of a request.**

FastAPI's own PR #12066 — "Fix deadlock that can occur when closing dependencies
during response model validation" — has been open since August 2024, with
production outages reported in its comments. A session yielded by ``Depends`` is
held open *through* response-model validation, so under load the pool starves
while every individual request still looks fast at p99. The compounding hazard
is Starlette's fixed 40-thread pool, which sync dependencies share.

Keel's answer is to make the safe thing the only thing on offer: there is no
``get_session`` dependency here, deliberately. A route receives services; a
service opens a transaction around the work that needs one and returns the
connection before it builds a response.

    async def publish(post_id: UUID) -> PostRead:
        async with uow() as session:
            post = await Posts(session).find_or_fail(post_id)
            post.publish()
        return PostRead.model_validate(post)   # session already returned

The second thing this module does is refuse to let a query hold a connection
forever: ``statement_timeout`` is applied per connection, so a pathological
query fails instead of starving the pool behind it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, Final

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from keel.database.config import DatabaseConfig
from keel.database.hooks import publish_session, run_after_commit, withdraw_session
from keel.database.observers import ModelEvent, dispatch_pending

PROBE_APPLICATION_NAME: Final = "keel-readiness-probe"
"""How the probe's connection names itself in ``pg_stat_activity``.

So an operator counting connections against a limit can tell the one per
process that serves no request.
"""


class Database:
    """Owns the engine and hands out sessions.

    One instance per process. It holds a connection pool, so building a second
    one silently doubles the connection count the database sees — which is why
    the application binds a single instance at startup and reaches it through
    :func:`~keel.database.current_database` rather than constructing its own.

    Args:
        config: How to reach and pool the database.
        on_observer_error: Called when a model observer raises after a commit.
            Observers run once the write is durable, so a failure there cannot
            roll anything back and must not stop the remaining observers — but
            it must not vanish either. Logged on ``keel.database`` by default;
            pass something to route it elsewhere.
        on_deferred_error: Called when an after-commit callback raises — a job
            that could not be pushed, a mail the server refused. Logged on
            ``keel.database`` by default; pass something to route it elsewhere.
    """

    __slots__ = (
        "_config",
        "_engine",
        "_on_deferred_error",
        "_on_observer_error",
        "_probe_engine",
        "_sessions",
    )

    def __init__(
        self,
        config: DatabaseConfig,
        on_observer_error: Callable[[BaseException, ModelEvent], None] | None = None,
        on_deferred_error: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._config = config
        self._on_observer_error = on_observer_error
        self._on_deferred_error = on_deferred_error
        self._engine = self._create_engine(config)
        self._probe_engine = self._create_probe_engine(config) or self._engine
        self._sessions = async_sessionmaker(
            self._engine,
            expire_on_commit=False,
            autoflush=False,
        )

    @staticmethod
    def _connect_args(config: DatabaseConfig) -> dict[str, Any]:
        """Return driver-specific connection arguments.

        The connect timeout is spelled differently by every driver — asyncpg
        calls it ``timeout``, psycopg calls it ``connect_timeout`` — so it is
        translated here rather than pushed onto the caller.

        Args:
            config: The database configuration.

        Returns:
            Arguments to pass through to the DBAPI's connect call.
        """
        scheme = config.url.split("://", 1)[0]
        match scheme:
            case "postgresql+asyncpg":
                return {"timeout": config.connect_timeout}
            case "postgresql+psycopg":
                return {"connect_timeout": int(config.connect_timeout)}
            case _:
                # SQLite and anything unrecognised: connecting is local or the
                # spelling is unknown, so pass nothing rather than guess.
                return {}

    @staticmethod
    def _create_engine(config: DatabaseConfig) -> AsyncEngine:
        """Build the engine, applying pool settings the backend supports."""
        options: dict[str, Any] = {"echo": config.echo, "pool_pre_ping": config.pool_pre_ping}
        connect_args = Database._connect_args(config)
        if connect_args:
            options["connect_args"] = connect_args
        if not config.is_sqlite:
            # SQLite's async driver uses a pool that rejects these arguments.
            options |= {
                "pool_size": config.pool_size,
                "max_overflow": config.max_overflow,
                "pool_timeout": config.pool_timeout,
                "pool_recycle": config.pool_recycle,
            }

        engine = create_async_engine(config.url, **options)
        if config.statement_timeout is not None and not config.is_sqlite:
            _apply_statement_timeout(engine, config.statement_timeout)
        return engine

    @staticmethod
    def _create_probe_engine(config: DatabaseConfig) -> AsyncEngine | None:
        """Build the one-connection engine :meth:`ping` uses, or ``None`` for SQLite.

        Its own pool, so a request pool that is merely busy does not read as a
        database that is down: requests wait for a connection and are served,
        and a probe sharing their queue would report every replica unready at
        the same moment. No connection opens until the first ping, and there is
        no ``statement_timeout`` listener — ``SELECT 1`` cannot run long, and
        the probe's own deadline bounds it.

        Args:
            config: The database configuration.

        Returns:
            The probe engine, or ``None`` where the main engine must serve.
        """
        if config.is_sqlite:
            return None
        connect_args = Database._connect_args(config)
        match config.url.split("://", 1)[0]:
            case "postgresql+asyncpg":
                connect_args["server_settings"] = {"application_name": PROBE_APPLICATION_NAME}
            case "postgresql+psycopg":
                connect_args["application_name"] = PROBE_APPLICATION_NAME
        return create_async_engine(
            config.url,
            pool_size=1,
            max_overflow=0,
            pool_timeout=config.pool_timeout,
            pool_recycle=config.pool_recycle,
            pool_pre_ping=True,
            connect_args=connect_args,
        )

    @property
    def config(self) -> DatabaseConfig:
        """The configuration this database was built from."""
        return self._config

    @property
    def engine(self) -> AsyncEngine:
        """The underlying engine, for migrations and administrative work."""
        return self._engine

    def session(self) -> AsyncSession:
        """Return a new, unopened session.

        Prefer :meth:`transaction`. Use this only when you need control over
        the transaction boundary that the context manager does not give you —
        and remember that you are then responsible for closing it.

        Returns:
            A new session.
        """
        return self._sessions()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """Open a session and a transaction, committing on success.

        The transaction is committed when the block exits normally and rolled
        back if it raises. The session is closed either way, which is the part
        that returns the connection to the pool.

        Once the commit succeeds, two kinds of deferred work run: buffered model
        events reach their observers (:mod:`keel.database.observers`), and
        buffered after-commit callbacks run (:mod:`keel.database.hooks`) — which
        is how a job dispatched inside the block reaches the queue only if the
        block succeeded.

        Both happen here rather than inside, because neither must react to a
        change that is subsequently rolled back.

        The session is also published to the context for the duration, so code
        deeper in the call stack can discover that a transaction is open without
        it being threaded through every signature.

        Yields:
            A session inside an open transaction.
        """
        # Not merged into one `async with`: the work below has to run inside the
        # session but *after* the transaction has committed.
        async with self._sessions() as session:
            token = publish_session(session)
            try:
                async with session.begin():
                    yield session
            finally:
                withdraw_session(token)
            await dispatch_pending(session, self._on_observer_error)
            await run_after_commit(session, self._on_deferred_error)

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        """Open a raw connection, for work that is not ORM-shaped.

        Yields:
            A connection inside an open transaction.
        """
        async with self._engine.begin() as connection:
            yield connection

    async def ping(self) -> None:
        """Run ``SELECT 1`` on a connection of its own, raising if it fails.

        Not through the request pool; :meth:`_create_probe_engine` says why. So
        this answers "does the database answer", not "is a connection free".

        Unbounded: it waits as long as the connect timeout allows. A readiness
        probe wants :func:`keel.observability.check_database`, which
        :func:`keel.observability.probe` runs under a deadline.
        """
        async with self._probe_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def close(self) -> None:
        """Dispose of both pools. Call this once, at shutdown."""
        await self._engine.dispose()
        if self._probe_engine is not self._engine:
            await self._probe_engine.dispose()

    def __repr__(self) -> str:
        """Identify the backend without leaking the password."""
        scheme = self._config.url.split("://", 1)[0]
        return f"<Database {scheme}>"


def _apply_statement_timeout(engine: AsyncEngine, seconds: float) -> None:
    """Apply a server-side statement timeout to every new connection.

    Done with a connection event rather than a URL parameter because the option
    is spelled differently by every driver, and because setting it per
    connection means it survives a pool recycle.

    Args:
        engine: The engine whose connections should be capped.
        seconds: The cap.
    """
    milliseconds = max(1, int(seconds * 1000))

    @event.listens_for(engine.sync_engine, "connect")
    def _set_timeout(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"SET statement_timeout = {milliseconds}")
        finally:
            cursor.close()
