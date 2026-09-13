"""What actually travels on the queue.

A job is an object in this process; an envelope is the bytes that reach another
one. Keeping them separate is what lets the driver stay dumb — SAQ, or anything
else, moves envelopes and never learns what a job *is*.

The split also draws the compatibility boundary in the right place. An envelope
is a **versioned wire format**: a worker running last week's code has to be able
to read one produced by this week's, at least well enough to fail intelligibly.
Anything added here needs a default.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Final, Self

from keel.queue.job import Job

FORMAT_VERSION: Final = 1
"""Bumped when the envelope's shape changes incompatibly.

Present from the start because retrofitting a version field means the first
version is the one you cannot identify.
"""


@dataclass(frozen=True, slots=True)
class Envelope:
    """One dispatched job, in transit.

    Attributes:
        id: Unique to this dispatch. Time-ordered, so a queue dump sorts into
            dispatch order without a separate timestamp index.
        job: The registered job name.
        payload: The job's fields.
        queue: Which queue this belongs to.
        max_attempts: Copied from the job at dispatch, deliberately. A policy
            change should not retroactively alter how many times an already-
            queued job is tried; the envelope carries the rules it was created
            under.
        timeout: Seconds one attempt may run.
        unique_key: Set when the job type is unique.
        delay: Seconds to wait before the job becomes visible.
        context: Ambient data travelling with the job — correlation id, request
            id, tenant. This is the field that makes a worker's logs joinable to
            the request that caused the work, and it is why the queue depends on
            a context mechanism rather than the other way round.
        attempts: How many times this has been tried. Zero at dispatch.
        dispatched_at: Unix timestamp, for measuring queue latency.
        version: The envelope format version.
    """

    job: str
    payload: Mapping[str, Any]
    queue: str
    max_attempts: int
    timeout: float | None = None
    unique_key: str | None = None
    delay: float = 0.0
    context: Mapping[str, Any] = field(default_factory=dict)
    attempts: int = 0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    dispatched_at: float = field(default_factory=time.time)
    version: int = FORMAT_VERSION

    @classmethod
    def seal(
        cls,
        job: Job,
        *,
        delay: float = 0.0,
        queue: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> Self:
        """Package a job for dispatch.

        Args:
            job: The job to send.
            delay: Seconds before it becomes visible to a worker.
            queue: Override the job's declared queue — for draining a backlog
                onto a dedicated lane, say.
            context: Ambient data to carry along.

        Returns:
            The sealed envelope.
        """
        return cls(
            job=job.name,
            payload=job.payload(),
            queue=queue or job.queue,
            max_attempts=job.max_attempts,
            timeout=job.timeout,
            unique_key=job.unique_key(),
            delay=max(0.0, delay),
            context=dict(context or {}),
        )

    def open(self) -> Job:
        """Rebuild the job this envelope carries.

        Returns:
            The job instance.

        Raises:
            UnknownJobError: If this process has no class registered under the
                envelope's job name.
            JobError: If the payload no longer matches the job's fields.
        """
        from keel.queue.job import resolve_job

        return resolve_job(self.job).from_payload(dict(self.payload))

    def attempted(self) -> Self:
        """Return a copy with the attempt count incremented.

        Returns:
            A new envelope; the receiver is unchanged, because an envelope is a
            record of a dispatch and mutating it would lose the original.
        """
        return replace(self, attempts=self.attempts + 1)

    @property
    def exhausted(self) -> bool:
        """Whether this job has used up its attempts and should be dead-lettered."""
        return self.attempts >= self.max_attempts

    def __repr__(self) -> str:
        """Identify the job and where it is in its retry budget."""
        return (
            f"<Envelope {self.job} id={self.id[:8]} queue={self.queue!r} "
            f"attempt={self.attempts}/{self.max_attempts}>"
        )


__all__ = ["FORMAT_VERSION", "Envelope"]
