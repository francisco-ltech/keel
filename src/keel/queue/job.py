"""Jobs.

A job is the **Command** pattern, and this is the one place in Keel where that
pattern is the whole design rather than a supporting player. A Command packages
an operation together with its parameters as an object, so it can be handed to
something that does not know what it does — stored, queued, logged, retried,
replayed. That is exactly what a queue needs and exactly what a cache never did,
which is why the cache's shape does not transfer here (see ADR 0001).

A job is therefore two things fused, and both halves matter:

* **Data** — a payload that must survive serialisation, a network hop, and a
  process that may be running different code by the time it arrives.
* **Behaviour** — ``handle()``, which runs in the worker.

Keeping them in one class is deliberate. Splitting the payload from its handler
means two files to change for every field, and a registry mapping one to the
other that can silently drift. Laravel fuses them for the same reason.

    @dataclass(frozen=True, slots=True)
    class SendInvoiceEmail(Job):
        invoice_id: UUID

        max_attempts: ClassVar[int] = 5
        queue: ClassVar[str] = "emails"

        async def handle(self) -> None:
            await invoices.email(self.invoice_id)

Policy lives in ``ClassVar``s, not in fields, so it is not serialised: how many
times to retry is a property of the job *type*, decided by the person who wrote
it, not a value a caller passes in and not something an old envelope should be
able to override after a deploy changes the policy.
"""

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Final, Self, cast

from keel.exceptions import KeelError
from keel.queue.backoff import DEFAULT_BACKOFF, Backoff

DEFAULT_QUEUE: Final = "default"
"""Queue a job goes to when it does not name one."""

DEFAULT_MAX_ATTEMPTS: Final = 3
"""Total attempts, not retries.

Three means "try, retry, retry" — chosen because the overwhelmingly common
failure is transient and clears within seconds, while a job that has failed
three times is usually failing for a reason that a fourth attempt will not fix.
"""

DEFAULT_TIMEOUT: Final = 300.0
"""Seconds a single attempt may run before the worker abandons it.

Bounded by default: a job with no timeout that hangs holds a worker slot
forever, and enough of them starve the queue with no error anywhere to explain
why nothing is being processed.
"""


class JobError(KeelError):
    """Base class for job-definition problems."""


class PermanentFailureError(KeelError):
    """Raised by a handler to say retrying cannot help.

    The queue's default is to retry, because most failures are transient. Some
    are not: a validation error, a row that no longer exists, a 4xx from an
    upstream. Retrying those burns the attempt budget, delays the dead-letter
    that a human needs to see, and in the meantime looks like a flaky job rather
    than a broken one.

    This is the job-side half of an error taxonomy whose HTTP half already
    exists. The same ``RecordNotFoundError`` means *404* to a router and *stop,
    this will never work* to a worker, and only the caller knows which context
    it is in — so the classification is made at the handler boundary rather than
    inferred from the exception type:

        async def handle(self) -> None:
            try:
                invoice = await invoices.get(self.invoice_id)
            except RecordNotFoundError as exc:
                raise PermanentFailureError(f"invoice {self.invoice_id} is gone") from exc
            await mail.send(invoice)

    Note:
        This is about *retry policy*, not severity. A permanent failure is still
        recorded in the failed-jobs table with its traceback, and is still
        re-dispatchable by hand once the cause is fixed.
    """


class UnknownJobError(JobError):
    """Raised when an envelope names a job this process cannot construct.

    Almost always a deploy in progress: a worker running yesterday's code has
    received a job that only today's code defines. Worth distinguishing from a
    handler failure, because the fix is "wait for the rollout", not "debug the
    job".
    """

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(
            f"no job registered as {name!r}; the module defining it may not be "
            f"imported in this process, or this worker may be running older code"
        )


class DuplicateJobError(JobError):
    """Raised when two job classes claim the same name.

    Silently allowing it would mean a payload deserialising into the wrong
    class, which is the kind of failure that looks like corrupted data.
    """

    def __init__(self, name: str, existing: type[Job], incoming: type[Job]) -> None:
        super().__init__(
            f"job name {name!r} is claimed by both {existing.__module__}.{existing.__qualname__} "
            f"and {incoming.__module__}.{incoming.__qualname__}; set a distinct "
            f"`name` ClassVar on one of them"
        )


_registry: dict[str, type[Job]] = {}


def _is_same_definition(existing: type[Job], incoming: type[Job]) -> bool:
    """Whether two classes are the same definition rather than a real clash.

    ``@dataclass(slots=True)`` cannot add slots to an existing class, so it
    builds a replacement and returns that. The replacement is a *different*
    object, and creating it fires ``__init_subclass__`` a second time — so a
    naive identity check reports the job as colliding with itself. Since
    ``slots=True`` is the recommended way to declare a job, that check would
    have made the documented example fail.

    Compared on ``__name__`` rather than ``__qualname__`` because the rebuilt
    class does not always carry the qualified name through — a job declared
    inside a function loses its ``<locals>`` prefix. ``__name__`` is also the
    right comparison on the merits: it is what the wire name defaults to, so two
    classes sharing one in a module are a genuine collision rather than a
    false positive.
    """
    return (existing.__module__, existing.__name__) == (
        incoming.__module__,
        incoming.__name__,
    )


class Job(ABC):
    """An operation with its parameters, executed later by a worker.

    Subclasses must be dataclasses — the payload is serialised by field, and a
    plain class gives nothing to serialise. This is checked when the job is
    turned into an envelope rather than at class creation, because the
    ``@dataclass`` decorator has not run yet when ``__init_subclass__`` fires.

    Attributes:
        name: How this job is identified on the wire. Defaults to the class
            name. Set it explicitly before renaming a class that already has
            jobs in flight — otherwise every queued envelope becomes
            unroutable.
        queue: Which queue to dispatch to. Separate queues are how slow work is
            stopped from starving fast work.
        max_attempts: Total attempts including the first.
        timeout: Seconds one attempt may run.
        backoff: How long to wait between attempts.
        unique_for: Seconds during which a second dispatch with the same
            uniqueness key is dropped. ``None`` disables it.
    """

    name: ClassVar[str]
    queue: ClassVar[str] = DEFAULT_QUEUE
    max_attempts: ClassVar[int] = DEFAULT_MAX_ATTEMPTS
    timeout: ClassVar[float | None] = DEFAULT_TIMEOUT
    backoff: ClassVar[Backoff] = DEFAULT_BACKOFF
    unique_for: ClassVar[float | None] = None

    def __init_subclass__(cls, *, register: bool = True, **kwargs: Any) -> None:
        """Register the subclass under its name.

        Auto-registration by convention, so defining a job is enough to make it
        dispatchable and a worker importing the module can route to it. The
        alternative — an explicit registry call — is one more thing to forget,
        and forgetting it fails at runtime in the worker rather than at import
        in the process that dispatched.

        Args:
            register: Pass ``False`` for an intermediate base class that is not
                itself dispatchable.
            **kwargs: Forwarded to ``super()``.

        Raises:
            DuplicateJobError: If another class already claims the name.
        """
        super().__init_subclass__(**kwargs)
        if not register or ABC in cls.__bases__:
            return
        if "name" not in cls.__dict__:
            cls.name = cls.__name__
        existing = _registry.get(cls.name)
        if existing is not None and not _is_same_definition(existing, cls):
            raise DuplicateJobError(cls.name, existing, cls)
        _registry[cls.name] = cls

    @abstractmethod
    async def handle(self) -> None:
        """Do the work.

        Runs in a worker process, possibly minutes after dispatch, possibly on a
        different machine, and possibly more than once.

        **Write it to be idempotent.** At-least-once delivery is the guarantee a
        queue can actually make: a worker that dies after finishing the work but
        before acknowledging it will hand the job to someone else. A handler
        that charges a card without a guard will charge twice, and no amount of
        queue configuration prevents that.
        """

    def unique_key(self) -> str | None:
        """Return the key that makes this dispatch unique, if any.

        Defaults to the job name plus its payload, so dispatching the same job
        with the same arguments twice within ``unique_for`` seconds enqueues it
        once. Override to make uniqueness coarser — keyed on a user id rather
        than the whole payload, say.

        Returns:
            A stable key, or ``None`` when this job type is not unique.
        """
        if self.unique_for is None:
            return None
        fields = sorted(self.payload().items())
        rendered = ",".join(f"{key}={value!r}" for key, value in fields)
        return f"{self.name}:{rendered}"

    def payload(self) -> dict[str, Any]:
        """Return this job's fields as a serialisable mapping.

        Returns:
            The dataclass fields, shallowly. Values still have to survive the
            configured serializer — a ``UUID`` or ``datetime`` will not survive
            JSON, so declare such fields as ``str`` and convert at the boundary.

        Raises:
            JobError: If the subclass is not a dataclass, since there is then
                nothing to serialise.
        """
        # `hasattr`, not `dataclasses.is_dataclass`: its TypeGuard narrows the success
        # path to Never here. The attribute is the same thing the stdlib checks.
        cls = type(self)
        if not hasattr(cls, "__dataclass_fields__"):
            raise JobError(
                f"{cls.__name__} must be a dataclass: a job's payload is "
                f"serialised field by field, so decorate it with @dataclass"
            )
        # `fields()` rather than __dataclass_fields__: it filters out ClassVar
        # pseudo-fields, where a job's policy lives. None of that belongs on the wire.
        return {
            field.name: getattr(self, field.name) for field in dataclasses.fields(cast("Any", cls))
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> Self:
        """Rebuild a job from a payload produced by :meth:`payload`.

        Args:
            payload: The serialised fields.

        Returns:
            The reconstructed job.

        Raises:
            JobError: If the payload does not match the job's current fields —
                which happens when a field is added or renamed while envelopes
                for the old shape are still queued.
        """
        try:
            return cls(**payload)
        except TypeError as exc:
            raise JobError(
                f"cannot rebuild {cls.name} from its payload: {exc}. A field was "
                f"probably added or renamed while jobs of the old shape were "
                f"still in flight"
            ) from exc

    def __repr__(self) -> str:
        """Render as name plus payload, which is what a log line wants."""
        try:
            fields = ", ".join(f"{key}={value!r}" for key, value in self.payload().items())
        except JobError:  # pragma: no cover — only for a misdeclared job
            fields = "<not a dataclass>"
        return f"{self.name}({fields})"


def resolve_job(name: str) -> type[Job]:
    """Return the job class registered under *name*.

    Args:
        name: The name carried on the envelope.

    Returns:
        The job class.

    Raises:
        UnknownJobError: If nothing is registered under that name.
    """
    try:
        return _registry[name]
    except KeyError as exc:
        raise UnknownJobError(name) from exc


def registered_jobs() -> dict[str, type[Job]]:
    """Return a copy of the registry, for diagnostics and worker start-up logs."""
    return dict(_registry)


def clear_registry() -> None:
    """Empty the registry. Intended for test isolation."""
    _registry.clear()


__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_QUEUE",
    "DEFAULT_TIMEOUT",
    "DuplicateJobError",
    "Job",
    "JobError",
    "PermanentFailureError",
    "UnknownJobError",
    "clear_registry",
    "registered_jobs",
    "resolve_job",
]
