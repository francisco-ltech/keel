"""The queue contract, enforced against every implementation.

This suite exists because of a bug it would have caught. `FakeQueue` documented
`clear(None)` as "every queue" while the protocol said "the default queue", and
the two shipped disagreeing — discovered by a human reading both docstrings,
which is not a dependable process.

The cache learned this in Phase 1: a driver is not "a queue" because it has the
right method names, it is a queue because it behaves identically under the same
assertions. Same technique here, and the same exclusion rule.

**`SyncQueue` and `NullQueue` are deliberately absent.** They satisfy the
interface and deliberately not the behaviour — one executes on push, the other
discards — so admitting them would mean weakening the contract for the
implementations that do promise to hold jobs. That is the `NullStore` precedent
from ADR 0001, and they are covered by their own tests in `test_dispatch.py`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import ClassVar

import pytest

from keel.contracts.queue import Queue
from keel.queue import Envelope, FakeQueue, Job, QueueConfig
from keel.queue.job import DEFAULT_QUEUE

pytestmark = [pytest.mark.anyio, pytest.mark.contract]

executed: list[str] = []


@dataclass(frozen=True, slots=True)
class ContractJob(Job):
    """A job that records execution, so "push does not run it" is observable."""

    marker: str

    async def handle(self) -> None:
        executed.append(self.marker)


@dataclass(frozen=True, slots=True)
class UniqueContractJob(Job):
    """A job with a uniqueness window, to pin deduplication."""

    marker: str
    unique_for: ClassVar[float | None] = 60.0

    async def handle(self) -> None:
        executed.append(f"unique:{self.marker}")


@pytest.fixture(autouse=True)
def _clear_execution_log() -> None:
    executed.clear()


@pytest.fixture
async def fake_backend() -> AsyncIterator[Queue]:
    """The recording double."""
    queue = FakeQueue("contract")
    yield queue
    await queue.close()


@pytest.fixture
async def saq_backend(redis_url: str, namespace_suffix: str) -> AsyncIterator[Queue]:
    """A real SAQ queue on a namespace unique to this test."""
    from keel.queue.saq_driver import SaqQueue

    config = QueueConfig(driver="saq", url=redis_url, prefix=f"keel-contract-{namespace_suffix}")
    queue = SaqQueue.from_url(redis_url, config)
    await queue.clear()
    yield queue
    await queue.clear()
    await queue.close()


@pytest.fixture(
    params=[
        pytest.param("fake_backend", id="fake"),
        pytest.param("saq_backend", id="saq", marks=pytest.mark.redis),
    ]
)
def backend(request: pytest.FixtureRequest) -> Queue:
    """Every implementation that claims to hold jobs until a worker takes them."""
    resolved: Queue = request.getfixturevalue(request.param)
    return resolved


def seal(marker: str, *, queue: str | None = None, delay: float = 0.0) -> Envelope:
    """Build an envelope for the contract job."""
    return Envelope.seal(ContractJob(marker), queue=queue, delay=delay)


# -- accepting work -------------------------------------------------------


async def test_push_returns_the_envelope_id(backend: Queue) -> None:
    envelope = seal("a")
    assert await backend.push(envelope) == envelope.id


async def test_push_does_not_execute_the_job(backend: Queue) -> None:
    """The line between a queue and a function call."""
    await backend.push(seal("a"))
    assert executed == []


async def test_push_many_returns_one_id_per_envelope_in_order(backend: Queue) -> None:
    envelopes = [seal("a"), seal("b"), seal("c")]
    assert await backend.push_many(envelopes) == [item.id for item in envelopes]


async def test_push_many_of_nothing_is_accepted(backend: Queue) -> None:
    assert await backend.push_many([]) == []


# -- uniqueness -----------------------------------------------------------


async def test_a_unique_job_is_accepted_once(backend: Queue) -> None:
    first = Envelope.seal(UniqueContractJob("u"))
    second = Envelope.seal(UniqueContractJob("u"))
    assert first.unique_key == second.unique_key

    accepted = await backend.push(first)
    duplicate = await backend.push(second)

    assert accepted == first.id
    assert duplicate == first.id, (
        "a deduplicated dispatch must report the id already in flight, so the "
        "caller can tell it was dropped"
    )
    assert await backend.size() == 1


async def test_unique_jobs_with_different_payloads_both_land(backend: Queue) -> None:
    await backend.push(Envelope.seal(UniqueContractJob("one")))
    await backend.push(Envelope.seal(UniqueContractJob("two")))
    assert await backend.size() == 2


async def test_a_job_without_a_uniqueness_window_is_never_deduplicated(
    backend: Queue,
) -> None:
    await backend.push(seal("same"))
    await backend.push(seal("same"))
    assert await backend.size() == 2


# -- counting and clearing ------------------------------------------------


async def test_size_reflects_what_was_accepted(backend: Queue) -> None:
    assert await backend.size() == 0
    await backend.push_many([seal("a"), seal("b")])
    assert await backend.size() == 2


async def test_size_with_no_argument_counts_the_default_queue(backend: Queue) -> None:
    await backend.push(seal("default"))
    await backend.push(seal("elsewhere", queue="other"))

    assert await backend.size() == 1
    assert await backend.size(DEFAULT_QUEUE) == 1
    assert await backend.size("other") == 1


async def test_clear_with_no_argument_clears_only_the_default_queue(backend: Queue) -> None:
    """The disagreement this suite was written for.

    An administrative, destructive operation must not do the widest possible
    thing when an argument is omitted — the same rule ``Repository.purge()``
    follows by refusing to run without criteria.
    """
    await backend.push(seal("default"))
    await backend.push(seal("elsewhere", queue="other"))

    removed = await backend.clear()

    assert removed == 1
    assert await backend.size() == 0
    assert await backend.size("other") == 1, "clear() reached a queue it was not given"


async def test_clear_reports_how_many_it_removed(backend: Queue) -> None:
    await backend.push_many([seal("a"), seal("b"), seal("c")])
    assert await backend.clear() == 3


async def test_clearing_an_empty_queue_is_harmless(backend: Queue) -> None:
    assert await backend.clear() == 0


async def test_a_named_queue_can_be_cleared_on_its_own(backend: Queue) -> None:
    await backend.push(seal("default"))
    await backend.push(seal("elsewhere", queue="other"))

    assert await backend.clear("other") == 1
    assert await backend.size() == 1


# -- routing --------------------------------------------------------------


async def test_a_job_lands_on_the_queue_it_names(backend: Queue) -> None:
    await backend.push(seal("routed", queue="reports"))
    assert await backend.size("reports") == 1
    assert await backend.size() == 0
    await backend.clear("reports")


# -- lifecycle ------------------------------------------------------------


async def test_the_queue_reports_its_name(backend: Queue) -> None:
    assert isinstance(backend.name, str)
    assert backend.name


async def test_close_is_idempotent(backend: Queue) -> None:
    """A shutdown path that runs twice must not be a second failure mode."""
    await backend.close()
    await backend.close()
