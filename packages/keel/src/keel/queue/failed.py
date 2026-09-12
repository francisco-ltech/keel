"""The dead-letter table: what happens to a job that has run out of attempts.

A queue's retry budget answers "is this failure transient?". When the answer
turns out to be no, the job has to go *somewhere* — and the default in most
stacks is nowhere, which is how a payment capture disappears with nothing but a
log line to prove it existed.

**Pattern: Memento.** :class:`FailedJob` is a captured snapshot of an
:class:`~keel.queue.envelope.Envelope` — the originator's state, externalised so
it can be restored later without the originator (the worker) being involved or
even running. It is the textbook use: the row is opaque to the operator tooling
that stores and lists it, and meaningful only to the thing that reconstitutes
it. The whole envelope is stored, not a reconstruction of it, because a
reconstruction is a second serialiser to keep in step with the first and it
drifts the first time a field is added.

**Pattern: Repository.** :class:`FailedJobs` is a
:class:`~keel.database.repository.Repository` subclass carrying the five verbs
an operator actually types at 3am — ``record``, ``retry``, ``retry_all``,
``forget``, ``prune``. It takes a session and never commits, like every other
repository in Keel (ADR 0002): retrying a dead letter is frequently one step in
a larger unit of work.

**Pattern: Strategy.** :class:`DatabaseFailureSink` is one implementation of
:class:`keel.queue.worker.FailureSink` — the worker's "what do I do with a dead
letter" step. The worker holds something with ``record(envelope, error)`` and
does not know whether that writes a row, posts to Sentry, or does nothing.

Declined, deliberately:

* **Re-declaring the sink protocol here.** The worker declares the shape it
  needs and this module satisfies it structurally, with no import in either
  direction. A second declaration of the same contract would be one more thing
  to keep in step, and it would make the dead-letter table depend on the worker
  — which an API replica that only lists and retries dead letters has no reason
  to import.
* **Soft deletes on the table.** ``forget`` means forget. A dead-letter row that
  is still there but invisible is the worst of both — it keeps the payload
  (often personal data) and hides it from the operator who asked for it to go.
* **A Command object per operator action** (``RetryFailedJob`` and friends).
  There is no undo, no queueing and no logging of the actions themselves, so
  Command would buy nothing that a method does not.
* **Storing the exception object.** Pickling an exception ties the row to the
  code that raised it; a class name, a message and a formatted traceback are
  what a human reads and they survive any deploy.
"""

from __future__ import annotations

import dataclasses
import logging
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta
from traceback import format_exception
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import JSON, DateTime, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from keel.database import current_database
from keel.database.model import Model, TimestampMixin, UUIDPrimaryKey, utcnow
from keel.database.repository import Repository
from keel.queue.dispatch import queue as queue_connection
from keel.queue.envelope import Envelope

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from keel.database.engine import Database

logger = logging.getLogger(__name__)

MESSAGE_LIMIT: Final = 4_000
"""Characters of an exception message kept.

Bounded because a message is attacker- and library-influenced: an ORM error
carrying a rejected bulk INSERT, or a driver echoing a multi-megabyte payload,
would otherwise make the dead-letter table larger than the data it protects.
The truncation is marked so nobody debugs a silently clipped string.
"""

TRACEBACK_LIMIT: Final = 20_000
"""Characters of a formatted traceback kept.

Generous, because the traceback is the reason anyone opens this table, and a
deep recursion — the one failure where the traceback is genuinely enormous — is
still legible from its first and last frames. Truncation keeps the *tail*, since
the frames nearest the raise are the ones that explain it.
"""

DEFAULT_RETENTION: Final = timedelta(days=30)
"""How long a dead letter is worth keeping when nobody says otherwise.

Long enough to survive a holiday and a slow incident review, short enough that
the table does not become an unbounded copy of every payload the system has
ever refused to process — which, for anything carrying personal data, is a
retention problem rather than a disk one.
"""

_JSON: Final = JSON().with_variant(JSONB(), "postgresql")
"""``JSONB`` on Postgres, plain ``JSON`` everywhere else.

``JSONB`` so an operator can ask containment questions — "every dead letter for
tenant X" — with an index behind them, which ``JSON`` cannot answer without
re-parsing every row. The variant keeps the table creatable on SQLite, so a
project can exercise this model in a unit test without a server.
"""

_ENVELOPE_FIELDS: Final[frozenset[str]] = frozenset(
    field.name for field in dataclasses.fields(Envelope)
)
"""The envelope's field names, used to drop anything this version cannot accept.

The envelope is a versioned wire format whose contract is that an older reader
must cope with a newer writer. A dead-letter row is that same problem with an
arbitrary delay attached, so decoding filters rather than fails.
"""


def _truncate(text: str, limit: int, *, keep: str = "head") -> str:
    """Clip *text* to *limit* characters, marking that it was clipped.

    Args:
        text: The string to bound.
        limit: Maximum characters to keep.
        keep: ``"head"`` to keep the beginning, ``"tail"`` to keep the end.

    Returns:
        The original string, or a clipped one carrying an explicit marker so a
        reader is never misled into debugging a string that is not complete.
    """
    if len(text) <= limit:
        return text
    marker = f"... [truncated, {len(text)} characters]"
    if keep == "tail":
        return marker + text[-limit:]
    return text[:limit] + marker


def encode_envelope(envelope: Envelope) -> dict[str, Any]:
    """Render an envelope as a JSON-storable mapping.

    Uses ``dataclasses.asdict`` rather than an explicit field list so that a
    field added to :class:`~keel.queue.envelope.Envelope` is stored without
    anyone remembering to edit this module — the failure mode of a hand-written
    encoder is that the *new* field is the one silently dropped, and the new
    field is the one nobody has habits about yet.

    Args:
        envelope: The envelope to store.

    Returns:
        A mapping whose values must already be serialisable — the same
        constraint :meth:`keel.queue.job.Job.payload` documents, since the
        payload has already survived one trip through the queue by the time a
        job dead-letters.
    """
    return dataclasses.asdict(envelope)


def decode_envelope(raw: Mapping[str, Any]) -> Envelope:
    """Rebuild an envelope from a stored mapping.

    Unknown keys are dropped instead of raising. A row written by a newer
    release, replayed by an older one during a rollback, must degrade to "the
    fields I understand" rather than becoming permanently unretryable — which
    would defeat the point of having kept it.

    Args:
        raw: The mapping produced by :func:`encode_envelope`.

    Returns:
        The envelope.

    Raises:
        TypeError: If a field the current envelope requires is absent, which
            means the row predates a mandatory addition and cannot be honoured.
    """
    return Envelope(**{key: value for key, value in raw.items() if key in _ENVELOPE_FIELDS})


class FailedJob(Model, UUIDPrimaryKey, TimestampMixin):
    """One job that exhausted its attempts, kept so it can be understood and re-run.

    The column set answers two different questions, and both have to be
    answerable without the other:

    * *What broke?* — ``job``, ``queue``, ``attempts``, ``exception``,
      ``message``, ``traceback``, ``failed_at``, ``context``. Flat columns, not
      a JSON blob, because an operator filters and groups on these and a
      dashboard should not have to reach inside a document to do it.
    * *Can I run it again?* — ``envelope``, the whole sealed envelope. Redundant
      with the flat columns on purpose: the flat ones are for humans and are
      allowed to be lossy, the blob is for machines and is not.

    Indexes, and the reasoning for each:

    * ``job`` — the first question an operator asks is "which job is failing",
      and :meth:`FailedJobs.retry_all` filters on it. High cardinality and
      always an equality predicate.
    * ``failed_at`` — :meth:`FailedJobs.prune` deletes by cutoff and every
      listing sorts by recency. Without it, pruning a large table is a full scan
      that competes with the workers still writing to it.
    * ``envelope_id`` — the lookup that starts from a log line. Not *unique*:
      at-least-once delivery means two workers can genuinely dead-letter the
      same dispatch, and a unique constraint would turn that into a crash inside
      the failure handler, which is the worst possible place for one.

    Indexes deliberately **not** added: ``queue``, because it is low cardinality
    and is always filtered alongside ``job`` — Postgres would ignore it — and
    ``context``, because which key matters is application-specific and a GIN
    index on every dead letter is a cost paid by everyone for a query few run.

    Attributes:
        job: The registered job name, as carried on the envelope.
        envelope_id: The failed dispatch's id, for joining to worker logs.
        queue: Which queue it was taken from.
        attempts: How many attempts were made before giving up.
        exception: The exception's class name.
        message: ``str(error)``, bounded by :data:`MESSAGE_LIMIT`.
        traceback: The formatted traceback, bounded by :data:`TRACEBACK_LIMIT`.
        failed_at: When the final attempt failed.
        context: The ambient data the envelope carried — correlation id, tenant,
            request id. Duplicated out of ``envelope`` because it is the field
            that makes this row joinable to the request that caused the work,
            and it should not require decoding a blob to read.
        envelope: The complete sealed envelope, for verbatim re-dispatch.
    """

    __tablename__ = "keel_failed_jobs"

    job: Mapped[str] = mapped_column(String(255), index=True)
    envelope_id: Mapped[str] = mapped_column(String(64), index=True)
    queue: Mapped[str] = mapped_column(String(255))
    attempts: Mapped[int]
    exception: Mapped[str] = mapped_column(String(255))
    message: Mapped[str] = mapped_column(Text)
    traceback: Mapped[str] = mapped_column(Text)
    failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    context: Mapped[dict[str, Any]] = mapped_column(_JSON, default=dict)
    envelope: Mapped[dict[str, Any]] = mapped_column(_JSON)

    def as_envelope(self) -> Envelope:
        """Rebuild the envelope exactly as it was when the job died.

        Returns:
            The envelope, attempt count and original id included. This is the
            forensic view; use :meth:`envelope_for_retry` to get one that can
            actually be pushed.
        """
        return decode_envelope(self.envelope)

    def envelope_for_retry(self) -> Envelope:
        """Rebuild the envelope in a state fit to be dispatched again.

        Three fields are deliberately not preserved:

        * **``attempts`` is reset to zero.** The alternative — carrying the
          exhausted count forward — makes the retry pointless: the envelope
          arrives already satisfying ``exhausted``, so the worker either
          dead-letters it without running it or runs it exactly once with no
          retry budget at all. Neither is what an operator asking for a retry
          means. The count is not *lost*; the ``attempts`` column keeps it, and
          that is where the history belongs, because the number of attempts the
          job made in its previous life is a fact about the past, not a
          constraint on its future.
        * **A fresh ``id``.** This is a new dispatch and needs its own identity
          in the queue, in the worker's logs, and in any uniqueness check the
          driver applies. The old id survives in ``context["retried_from"]`` so
          the two are still joinable.
        * **``delay`` is cleared.** The original delay was consumed before the
          first attempt; re-applying it would make "retry now" mean "retry in
          three days", which is not what the button says.

        Everything else — payload, queue, ``max_attempts``, timeout, context —
        is carried verbatim, which is the reason the whole envelope is stored.

        Returns:
            A pushable envelope.
        """
        original = self.as_envelope()
        return replace(
            original,
            attempts=0,
            delay=0.0,
            id=str(uuid.uuid4()),
            dispatched_at=time.time(),
            context={**original.context, "retried_from": original.id},
        )


class FailedJobs(Repository[FailedJob]):
    """Operator-facing data access for the dead-letter table.

    Every method here takes its transaction from the caller, like every other
    repository (ADR 0002). That matters more than usual for :meth:`retry`, whose
    correctness depends on the order of a push and a commit — see its docstring.
    """

    model = FailedJob

    async def record(
        self,
        envelope: Envelope,
        error: BaseException,
        *,
        failed_at: datetime | None = None,
    ) -> FailedJob:
        """Write one dead letter.

        Args:
            envelope: The envelope as the worker last had it, attempt count
                included.
            error: The exception from the final attempt.
            failed_at: When it failed. Defaults to now, and is a parameter
                because the worker may be recording a failure it observed a
                moment ago — and because ``created_at`` cannot stand in for it:
                that column defaults to ``now()``, which in Postgres is the
                *transaction* timestamp, so a batch of records written together
                would all claim the same instant.

        Returns:
            The persisted row.
        """
        return await self.create(
            job=envelope.job,
            envelope_id=envelope.id,
            queue=envelope.queue,
            attempts=envelope.attempts,
            exception=type(error).__name__,
            message=_truncate(str(error), MESSAGE_LIMIT),
            traceback=_truncate(
                "".join(format_exception(type(error), error, error.__traceback__)),
                TRACEBACK_LIMIT,
                keep="tail",
            ),
            failed_at=failed_at or utcnow(),
            context=dict(envelope.context),
            envelope=encode_envelope(envelope),
        )

    async def retry(self, identifier: uuid.UUID, *, connection: str | None = None) -> str:
        """Re-dispatch one dead letter and remove its row.

        **The ordering here is the interesting part, and it is the reverse of
        the one :func:`keel.queue.dispatch.dispatch` defaults to.**

        Ordinary dispatch defers the push until after the commit, because the
        job depends on rows that must exist before it runs; pushing first risks
        a worker acting on a transaction that rolled back. Retrying a dead
        letter inverts every term of that argument. The row must stop existing
        *only once the job is safely enqueued*, so:

        * **Push first, inside the transaction.** The delete below is reached
          only if the queue accepted the envelope. A queue outage therefore
          leaves the dead letter exactly where it was.
        * **Do not defer.** Registering an after-commit callback would delete
          the row, commit, and only then discover the queue is unreachable — the
          job would be gone from both the queue and the table. That is the one
          outcome this table exists to prevent, so ``dispatch()`` is bypassed in
          favour of pushing the stored envelope straight at the connection.
          (Bypassing it is also what makes the re-dispatch *verbatim*:
          ``dispatch()`` re-seals from a live :class:`~keel.queue.job.Job` and
          would silently apply today's ``max_attempts`` and drop the context.)

        The residual risk is the mirror image: the push succeeds, the caller's
        commit then fails, and the row survives a job that is already queued —
        so it can be retried twice. That is at-least-once delivery, which every
        handler is required to tolerate anyway
        (:meth:`keel.queue.job.Job.handle`), and it is strictly the better of
        the two directions to fail in.

        Args:
            identifier: The dead letter's primary key.
            connection: Which queue connection to push to. Defaults to the
                configured default; the envelope carries the *queue* name, but
                not which backend it came from.

        Returns:
            The id the queue accepted, which is the new envelope's id unless the
            driver deduplicated it against a dispatch already in flight.

        Raises:
            RecordNotFoundError: If there is no such row.
            Exception: Whatever the queue raises, with the row left intact.
        """
        record = await self.get_or_fail(identifier)
        accepted = await queue_connection(connection).push(record.envelope_for_retry())
        # force_delete rather than delete: if this model ever gains
        # SoftDeleteMixin, `delete()` would quietly become a no-op that leaves
        # the row to be retried again on the next sweep.
        await self.force_delete(record)
        return accepted

    async def retry_all(
        self,
        job_name: str | None = None,
        *,
        connection: str | None = None,
        limit: int | None = None,
    ) -> list[str]:
        """Re-dispatch every dead letter, or every one of a single job.

        Fails fast rather than continuing past an error, deliberately: the
        realistic cause of a push failing is that the queue is down, in which
        case the remaining pushes will fail too and the only thing persistence
        would buy is a longer outage report. Rows that were already retried have
        had their deletes staged in the caller's transaction, so aborting it
        re-exposes them — and re-running ``retry_all`` after the queue is back
        is safe, because a retried row either went (and is gone) or did not (and
        is still there).

        Args:
            job_name: Restrict to one registered job name. ``None`` retries
                everything, which is the "the dependency is back up" case.
            connection: Which queue connection to push to.
            limit: Cap on how many to retry in one call. Worth setting when the
                table is large: every retry is a round trip to the queue, and an
                unbounded sweep inside one transaction holds a connection for as
                long as that takes.

        Returns:
            The ids the queue accepted, in the order they were pushed.
        """
        statement = self.query().order_by(FailedJob.failed_at)
        if job_name is not None:
            statement = statement.where(FailedJob.job == job_name)
        if limit is not None:
            statement = statement.limit(limit)
        records: Sequence[FailedJob] = (await self.session.execute(statement)).scalars().all()

        accepted: list[str] = []
        for record in records:
            accepted.append(await queue_connection(connection).push(record.envelope_for_retry()))
            await self.force_delete(record)
        return accepted

    async def forget(self, identifier: uuid.UUID) -> bool:
        """Delete a dead letter without running it.

        For the job that failed because it should never have been dispatched.

        Args:
            identifier: The dead letter's primary key.

        Returns:
            ``True`` if a row was removed, ``False`` if it was already gone —
            reported rather than raised, because "forget something that is not
            there" has already achieved what the caller wanted.
        """
        return await self.purge(FailedJob.id == identifier) > 0

    async def prune(self, older_than: datetime | timedelta = DEFAULT_RETENTION) -> int:
        """Delete dead letters that failed before a cutoff.

        Args:
            older_than: Either an absolute cutoff — everything that failed
                strictly before this instant goes — or an age, which is
                subtracted from now. Both spellings exist because both callers
                exist: a scheduled prune says "thirty days", and a one-off
                clean-up after an incident says "before Tuesday".

        Returns:
            How many rows were removed.

        Raises:
            ValueError: If given a naive datetime. Comparing one against a
                ``timestamptz`` column silently assumes a timezone, and the
                assumption is wrong on exactly the deployment that is not in UTC.
        """
        cutoff = utcnow() - older_than if isinstance(older_than, timedelta) else older_than
        if cutoff.tzinfo is None:
            raise ValueError("prune() needs an aware datetime; use keel.database.utcnow()")
        return await self.purge(FailedJob.failed_at < cutoff)


class DatabaseFailureSink:
    """Records dead letters in :class:`FailedJob`, and never lets that fail the worker.

    The Strategy the worker delegates its dead-letter step to. It satisfies
    ``async def record(self, envelope, error) -> None``.

    **What happens when the database is down at the moment a job dead-letters.**
    Nothing propagates. The worker is already handling a failure; a second one
    raised from inside the handler either kills the worker — turning one lost
    job into an outage — or is caught by whatever generic clause the worker has
    and rethrown as a job failure, which would re-queue a job that has by
    definition run out of attempts. Neither reaction makes the lost job any less
    lost.

    So the failure is *downgraded, not swallowed*: the whole encoded envelope
    and the original traceback go to the logger at ``ERROR``. Logs are the one
    durable store that is definitionally still working when the database is not,
    and a line carrying the complete envelope is enough to re-dispatch by hand.
    An operator who wants more — a Sentry event, a file on disk — passes
    ``on_error``.

    This is the same reasoning as
    :func:`keel.database.hooks.run_after_commit`'s: work that runs *after* the
    thing that could have been rolled back cannot use exceptions to undo
    anything, so its only honest options are "continue" and "report".

    No retry loop, and that is a decision rather than an omission. A sink that
    retried would hold the worker's slot for the length of a database outage,
    while every other job in flight dead-lettered behind it and queued up to do
    the same. Retrying the *records* is what :meth:`FailedJobs.record` is for,
    from the log line, once the database is back.

    Args:
        database: The database to write to. Defaults to the bound one, resolved
            per call rather than at construction so the sink can be built before
            the lifespan binds anything — a worker assembles its collaborators
            at import time and starts them afterwards.
        on_error: Called with the recording failure, the envelope that could not
            be recorded, and the original job error. Defaults to logging all
            three.
    """

    __slots__ = ("_database", "_on_error")

    def __init__(
        self,
        *,
        database: Database | None = None,
        on_error: Callable[[BaseException, Envelope, BaseException], None] | None = None,
    ) -> None:
        self._database = database
        self._on_error = on_error or _log_unrecorded

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        """Open a unit of work on the configured or bound database.

        Yields:
            A session inside a transaction.
        """
        target = self._database or current_database()
        async with target.transaction() as session:
            yield session

    async def record(self, envelope: Envelope, error: BaseException) -> None:
        """Persist a dead letter.

        Args:
            envelope: The envelope as the worker last had it.
            error: The exception from the final attempt.
        """
        try:
            async with self._transaction() as session:
                await FailedJobs(session).record(envelope, error)
        except Exception as exc:  # noqa: BLE001 — see the class docstring
            self._on_error(exc, envelope, error)

    def __repr__(self) -> str:
        """Say which database this writes to, without leaking a password."""
        target = self._database
        where = "the bound database" if target is None else repr(target)
        return f"<DatabaseFailureSink {where}>"


def _log_unrecorded(
    failure: BaseException,
    envelope: Envelope,
    error: BaseException,
) -> None:
    """Emit an unrecordable dead letter to the log, in full.

    The last-resort durable store. Everything needed to re-dispatch the job by
    hand is in the message, because by the time anyone reads it the envelope is
    not recoverable from anywhere else.

    Args:
        failure: Why the row could not be written.
        envelope: The job that has now been lost from the queue.
        error: The exception that killed the job in the first place, logged
            alongside so the line is not merely a record that something was
            lost.
    """
    logger.error(
        "could not record a dead-lettered job (%s: %s); the envelope is below and "
        "this log line is now the only record of it. job=%s envelope=%r original=%r",
        type(failure).__name__,
        failure,
        envelope.job,
        encode_envelope(envelope),
        "".join(format_exception(type(error), error, error.__traceback__)),
    )


__all__ = [
    "DEFAULT_RETENTION",
    "MESSAGE_LIMIT",
    "TRACEBACK_LIMIT",
    "DatabaseFailureSink",
    "FailedJob",
    "FailedJobs",
    "decode_envelope",
    "encode_envelope",
]
