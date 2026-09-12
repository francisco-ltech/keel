"""The cache Repository: the half of the Bridge application code actually calls.

The driver contract suite proves the primitives behave identically everywhere.
This file proves the *abstraction* built on them is right, which is a different
question and the one most likely to break: every convenience here — ``remember``,
``pull``, ``forever``, the TTL rules — is written once against the primitives, so
a mistake in it is a mistake in every driver at once.

Three things are worth more than their line count.

``normalise_ttl`` is where three different ways of saying "how long" collapse
into one, and the interesting case is the one a boolean cannot express: omitting
a TTL must mean *use the configured default* while passing ``None`` must mean
*forever*. Collapsing those makes the default unreachable, which is why
:data:`~keel.support.sentinels.UNSET` exists and why it is tested here.

The ``MISSING`` tests exist for the same reason one layer down. A cache that
answers "I hold ``None``" and "I hold nothing" with the same value recomputes a
legitimately-null result forever, so ``get`` must prefer a stored ``None`` over
the caller's default.

The single-flight tests are the only concurrency in the Repository. They assert
the callback runs once for many simultaneous misses, that the contrast case
(single flight off) really does compute more than once, and — the one that would
otherwise fail silently in production — that a raising callback still releases
its lock.
"""

from __future__ import annotations

from datetime import timedelta

import anyio
import pytest

from conftest import ManualClock
from keel.cache.fake import FakeStore
from keel.cache.lock import StoreLock
from keel.cache.repository import (
    SINGLE_FLIGHT_LOCK_POLL,
    SINGLE_FLIGHT_LOCK_TIMEOUT,
    SINGLE_FLIGHT_LOCK_TTL,
    Repository,
    normalise_ttl,
)
from keel.cache.stores.array import ArrayStore
from keel.support.keys import KeyNamespace
from keel.support.sentinels import MISSING, UNSET, is_missing, is_present

pytestmark = [pytest.mark.anyio]

DEFAULT_TTL = 300.0
"""The default lifetime the fixtures below are configured with."""

CONCURRENT_CALLERS = 20
"""How many callers pile onto one key in the single-flight tests."""


@pytest.fixture
def store(namespace: KeyNamespace, clock: ManualClock) -> ArrayStore:
    """An in-memory store on the hand-advanced clock, so TTLs assert exactly."""
    return ArrayStore(namespace, clock=clock)


@pytest.fixture
def repository(store: ArrayStore) -> Repository:
    """The repository under test.

    The single-flight timings are shortened from the production defaults on
    purpose: a lock that is wrongly held should fail this suite in under a
    second rather than hang it for thirty.
    """
    return Repository(
        store,
        DEFAULT_TTL,
        "test",
        single_flight_timeout=5.0,
        single_flight_poll=0.01,
    )


@pytest.fixture
def recorder(namespace: KeyNamespace, clock: ManualClock) -> FakeStore:
    """A recording store, for the tests that assert what the repository *did*."""
    return FakeStore(ArrayStore(namespace, clock=clock))


@pytest.fixture
def recording_repository(recorder: FakeStore) -> Repository:
    """A repository over the recording store."""
    return Repository(recorder, DEFAULT_TTL, "test")


# -- sentinels ------------------------------------------------------------


def test_missing_and_unset_are_distinct() -> None:
    """Two absences that must never be confused.

    ``MISSING`` means the cache holds nothing; ``UNSET`` means the caller said
    nothing. Collapsing them would make ``ttl=None`` and an omitted ``ttl``
    indistinguishable, and the configured default unreachable.
    """
    assert len({MISSING, UNSET}) == 2
    assert is_missing(MISSING)
    assert not is_missing(UNSET)


def test_the_sentinels_render_as_bare_names() -> None:
    """Assertion output should read ``<MISSING>``, not ``<Sentinel.MISSING: 1>``."""
    assert repr(MISSING) == "<MISSING>"
    assert repr(UNSET) == "<UNSET>"


def test_a_missing_value_is_falsey() -> None:
    assert bool(MISSING) is False
    assert not MISSING


def test_is_present_is_the_inverse_of_is_missing() -> None:
    values: tuple[object, ...] = (MISSING, UNSET, None, 0, "", "value", [])
    for value in values:
        assert is_present(value) is not is_missing(value)


# -- normalise_ttl --------------------------------------------------------


def test_normalise_ttl_returns_the_default_when_the_argument_is_omitted() -> None:
    assert normalise_ttl(UNSET, DEFAULT_TTL) == DEFAULT_TTL


def test_normalise_ttl_returns_the_default_even_when_the_default_is_forever() -> None:
    """An omitted TTL takes the default whatever the default happens to be."""
    assert normalise_ttl(UNSET, None) is None


def test_normalise_ttl_treats_an_explicit_none_as_forever() -> None:
    """The case ``UNSET`` exists to keep reachable: ``None`` overrides the default."""
    assert normalise_ttl(None, DEFAULT_TTL) is None


def test_normalise_ttl_converts_a_timedelta_to_seconds() -> None:
    assert normalise_ttl(timedelta(minutes=2), DEFAULT_TTL) == 120.0
    assert normalise_ttl(timedelta(milliseconds=500), DEFAULT_TTL) == 0.5


@pytest.mark.parametrize(
    ("ttl", "expected"),
    [
        pytest.param(2.5, 2.5, id="float"),
        pytest.param(30, 30.0, id="int"),
        pytest.param(-1, -1.0, id="negative"),
        pytest.param(0, 0.0, id="zero"),
    ],
)
def test_normalise_ttl_coerces_numbers_to_float(ttl: float, expected: float) -> None:
    """Every driver receives seconds as a float, including the useless ones.

    Non-positive lifetimes are passed through rather than rejected: what a
    ``ttl`` of zero means is the store's decision, and the contract already
    pins it.
    """
    normalised = normalise_ttl(ttl, DEFAULT_TTL)
    assert normalised == expected
    assert isinstance(normalised, float)


def test_normalise_ttl_treats_any_other_sentinel_as_omitted() -> None:
    """A stray ``MISSING`` is a caller error; falling back beats storing it."""
    assert normalise_ttl(MISSING, DEFAULT_TTL) == DEFAULT_TTL
    assert normalise_ttl(MISSING, None) is None


# -- construction ---------------------------------------------------------


def test_properties_report_construction_arguments() -> None:
    """Every piece of state is readable through a property.

    Not cosmetic: :class:`~keel.cache.proxy.CacheProxy` overrides these
    properties to become a lazily-resolved repository, so a method that reads
    the attribute behind one works here and fails through the facade.
    """
    inner = ArrayStore()
    repository = Repository(
        inner,
        45.0,
        "sessions",
        single_flight_ttl=1.0,
        single_flight_timeout=2.0,
        single_flight_poll=0.5,
    )

    assert repository.store is inner
    assert repository.default_ttl == 45.0
    assert repository.name == "sessions"
    assert repository.single_flight_ttl == 1.0
    assert repository.single_flight_timeout == 2.0
    assert repository.single_flight_poll == 0.5

    defaults = Repository(ArrayStore())
    assert defaults.single_flight_ttl == SINGLE_FLIGHT_LOCK_TTL
    assert defaults.single_flight_timeout == SINGLE_FLIGHT_LOCK_TIMEOUT
    assert defaults.single_flight_poll == SINGLE_FLIGHT_LOCK_POLL


def test_repr_names_the_store_and_its_driver() -> None:
    assert repr(Repository(ArrayStore())) == "<Repository 'default' store=ArrayStore>"
    assert repr(Repository(FakeStore())) == "<Repository 'default' store=FakeStore>"


def test_repr_reflects_a_custom_name() -> None:
    assert repr(Repository(ArrayStore(), 60.0, "sessions")) == (
        "<Repository 'sessions' store=ArrayStore>"
    )


# -- reading --------------------------------------------------------------


async def test_get_returns_the_stored_value(repository: Repository) -> None:
    await repository.put("key", "value")
    assert await repository.get("key") == "value"


async def test_get_returns_none_by_default_for_an_absent_key(repository: Repository) -> None:
    assert await repository.get("absent") is None


async def test_get_returns_the_supplied_default_for_an_absent_key(repository: Repository) -> None:
    assert await repository.get("absent", "fallback") == "fallback"


async def test_get_prefers_a_stored_none_over_the_default(repository: Repository) -> None:
    """The whole reason ``MISSING`` exists.

    Returning the default here would make a legitimately-null result
    uncacheable: it would be recomputed on every call, forever.
    """
    await repository.put("key", None)
    assert await repository.get("key", "fallback") is None


async def test_many_defaults_absent_keys_to_none(repository: Repository) -> None:
    await repository.put("present", "value")
    assert await repository.many(["present", "absent"]) == {"present": "value", "absent": None}


async def test_many_substitutes_the_default_for_absent_keys(repository: Repository) -> None:
    await repository.put("present", "value")
    found = await repository.many(["present", "absent"], "fallback")
    assert found == {"present": "value", "absent": "fallback"}


async def test_has_is_true_for_a_stored_value(repository: Repository) -> None:
    await repository.put("key", "value")
    assert await repository.has("key") is True


async def test_has_is_false_for_an_absent_key(repository: Repository) -> None:
    assert await repository.has("absent") is False


async def test_has_is_true_for_a_key_holding_none(repository: Repository) -> None:
    """Presence is about the entry, not about the truthiness of its value."""
    await repository.put("key", None)
    assert await repository.has("key") is True


async def test_missing_is_the_inverse_of_has(repository: Repository) -> None:
    await repository.put("present", None)
    for key in ("present", "absent"):
        assert await repository.missing(key) is not await repository.has(key)


async def test_pull_returns_the_value_and_removes_it(repository: Repository) -> None:
    await repository.put("key", "value")
    assert await repository.pull("key") == "value"
    assert await repository.has("key") is False


async def test_pull_of_an_absent_key_defaults_to_none(repository: Repository) -> None:
    assert await repository.pull("absent") is None


async def test_pull_returns_the_default_when_the_key_is_absent(repository: Repository) -> None:
    assert await repository.pull("absent", "fallback") == "fallback"


# -- writing and lifetimes ------------------------------------------------


async def test_put_without_a_ttl_uses_the_repository_default(
    repository: Repository, clock: ManualClock
) -> None:
    await repository.put("key", "value")
    clock.advance(DEFAULT_TTL - 1)
    assert await repository.get("key") == "value"
    clock.advance(2)
    assert await repository.get("key") is None


async def test_put_with_an_explicit_number_overrides_the_default(
    repository: Repository, clock: ManualClock
) -> None:
    await repository.put("key", "value", 10)
    clock.advance(11)
    assert await repository.get("key") is None


async def test_put_with_an_explicit_none_ttl_stores_forever(
    repository: Repository, clock: ManualClock
) -> None:
    await repository.put("key", "value", None)
    clock.advance(DEFAULT_TTL * 100)
    assert await repository.get("key") == "value"


async def test_put_accepts_a_timedelta(repository: Repository, clock: ManualClock) -> None:
    await repository.put("key", "value", timedelta(minutes=2))
    clock.advance(119)
    assert await repository.get("key") == "value"
    clock.advance(2)
    assert await repository.get("key") is None


async def test_put_many_applies_the_repository_default_ttl(
    recording_repository: Repository, recorder: FakeStore
) -> None:
    assert await recording_repository.put_many({"a": 1, "b": 2}) is True
    recorder.assert_put("a", 1, DEFAULT_TTL)
    recorder.assert_put("b", 2, DEFAULT_TTL)


async def test_put_many_with_a_timedelta_and_a_none_ttl(
    recording_repository: Repository, recorder: FakeStore
) -> None:
    await recording_repository.put_many({"a": 1}, timedelta(seconds=30))
    await recording_repository.put_many({"b": 2}, None)
    recorder.assert_put("a", 1, 30.0)
    recorder.assert_put("b", 2, None)


async def test_add_does_not_overwrite_an_existing_entry(repository: Repository) -> None:
    assert await repository.add("key", "first") is True
    assert await repository.add("key", "second") is False
    assert await repository.get("key") == "first"


async def test_add_applies_the_repository_default_ttl(
    repository: Repository, clock: ManualClock
) -> None:
    await repository.add("key", "value")
    clock.advance(DEFAULT_TTL - 1)
    assert await repository.get("key") == "value"
    clock.advance(2)
    assert await repository.get("key") is None


async def test_add_with_a_none_ttl_stores_forever(
    repository: Repository, clock: ManualClock
) -> None:
    await repository.add("key", "value", None)
    clock.advance(DEFAULT_TTL * 100)
    assert await repository.get("key") == "value"


async def test_add_with_a_timedelta(repository: Repository, clock: ManualClock) -> None:
    await repository.add("key", "value", timedelta(seconds=10))
    clock.advance(9)
    assert await repository.get("key") == "value"
    clock.advance(2)
    assert await repository.get("key") is None


async def test_forever_ignores_the_repository_default_ttl(
    repository: Repository, clock: ManualClock
) -> None:
    await repository.forever("key", "value")
    clock.advance(DEFAULT_TTL * 100)
    assert await repository.get("key") == "value"


async def test_increment_creates_and_advances_a_counter(repository: Repository) -> None:
    assert await repository.increment("hits") == 1
    assert await repository.increment("hits") == 2
    assert await repository.increment("hits", 4) == 6


async def test_decrement_subtracts(repository: Repository) -> None:
    await repository.increment("stock", 10)
    assert await repository.decrement("stock") == 9
    assert await repository.decrement("stock", 4) == 5


async def test_decrement_can_go_negative(repository: Repository) -> None:
    """A counter that has never been written starts at zero, not at nothing."""
    assert await repository.decrement("balance") == -1
    assert await repository.decrement("balance", 9) == -10


async def test_forget_reports_whether_an_entry_existed(repository: Repository) -> None:
    await repository.put("key", "value")
    assert await repository.forget("key") is True
    assert await repository.forget("key") is False


async def test_flush_clears_everything(repository: Repository) -> None:
    await repository.put_many({"a": 1, "b": 2, "c": 3})
    assert await repository.flush() is True
    assert await repository.many(["a", "b", "c"]) == {"a": None, "b": None, "c": None}


# -- remember -------------------------------------------------------------


async def test_remember_computes_and_stores_on_a_miss(
    recording_repository: Repository, recorder: FakeStore
) -> None:
    assert await recording_repository.remember("key", lambda: "computed") == "computed"
    recorder.assert_missed("key")
    recorder.assert_put("key", "computed", DEFAULT_TTL)


async def test_remember_does_not_call_the_callback_on_a_hit(
    recording_repository: Repository, recorder: FakeStore
) -> None:
    await recording_repository.put("key", "stored")
    recorder.reset()
    calls = 0

    def load() -> str:
        nonlocal calls
        calls += 1
        return "computed"

    assert await recording_repository.remember("key", load) == "stored"
    assert calls == 0
    recorder.assert_hit("key")
    recorder.assert_not_put("key")


async def test_remember_calls_the_callback_exactly_once_across_sequential_hits(
    repository: Repository,
) -> None:
    calls = 0

    def load() -> str:
        nonlocal calls
        calls += 1
        return "computed"

    for _ in range(5):
        assert await repository.remember("key", load) == "computed"
    assert calls == 1


async def test_remember_accepts_an_async_callback(repository: Repository) -> None:
    async def load() -> dict[str, int]:
        await anyio.sleep(0)
        return {"id": 42}

    assert await repository.remember("key", load) == {"id": 42}
    assert await repository.get("key") == {"id": 42}


async def test_remember_caches_a_none_result(repository: Repository) -> None:
    """``None`` is a result, not a miss — otherwise it is recomputed forever."""
    calls = 0

    def load() -> str | None:
        nonlocal calls
        calls += 1
        return None

    assert await repository.remember("key", load) is None
    assert await repository.remember("key", load) is None
    assert calls == 1


async def test_remember_caches_a_falsey_result_rather_than_recomputing(
    repository: Repository,
) -> None:
    """The classic ``if not cached: recompute()`` bug, from the caller's side."""
    calls = 0

    def load() -> int:
        nonlocal calls
        calls += 1
        return 0

    assert await repository.remember("count", load) == 0
    assert await repository.remember("count", load) == 0
    assert calls == 1


async def test_remember_uses_the_repository_default_ttl_when_omitted(
    repository: Repository, clock: ManualClock
) -> None:
    calls = 0

    def load() -> str:
        nonlocal calls
        calls += 1
        return f"value-{calls}"

    assert await repository.remember("key", load) == "value-1"
    clock.advance(DEFAULT_TTL - 1)
    assert await repository.remember("key", load) == "value-1"
    clock.advance(2)
    assert await repository.remember("key", load) == "value-2"


async def test_remember_respects_an_explicit_ttl(
    repository: Repository, clock: ManualClock
) -> None:
    calls = 0

    def load() -> str:
        nonlocal calls
        calls += 1
        return f"value-{calls}"

    assert await repository.remember("key", load, 10) == "value-1"
    clock.advance(9)
    assert await repository.remember("key", load, 10) == "value-1"
    clock.advance(2)
    assert await repository.remember("key", load, 10) == "value-2"


async def test_remember_accepts_a_timedelta_ttl(repository: Repository, clock: ManualClock) -> None:
    calls = 0

    def load() -> str:
        nonlocal calls
        calls += 1
        return f"value-{calls}"

    assert await repository.remember("key", load, timedelta(seconds=10)) == "value-1"
    clock.advance(11)
    assert await repository.remember("key", load, timedelta(seconds=10)) == "value-2"


async def test_remember_forever_survives_the_default_ttl(
    repository: Repository, clock: ManualClock
) -> None:
    calls = 0

    def load() -> str:
        nonlocal calls
        calls += 1
        return "computed"

    assert await repository.remember_forever("key", load) == "computed"
    clock.advance(DEFAULT_TTL * 100)
    assert await repository.remember_forever("key", load) == "computed"
    assert calls == 1


async def test_remember_forever_supports_single_flight(
    repository: Repository, clock: ManualClock
) -> None:
    calls = 0

    async def load() -> str:
        nonlocal calls
        calls += 1
        await anyio.sleep(0.05)
        return "computed"

    results: list[str] = []

    async def caller() -> None:
        results.append(await repository.remember_forever("key", load, single_flight=True))

    async with anyio.create_task_group() as tasks:
        for _ in range(CONCURRENT_CALLERS):
            tasks.start_soon(caller)

    assert calls == 1
    assert results == ["computed"] * CONCURRENT_CALLERS
    clock.advance(DEFAULT_TTL * 100)
    assert await repository.get("key") == "computed"


# -- single flight --------------------------------------------------------


async def test_single_flight_runs_a_slow_callback_once_for_many_concurrent_callers(
    repository: Repository,
) -> None:
    """The point of the feature: N simultaneous misses, one expensive call.

    The waiters re-read the cache once they hold the lock, so they return the
    value the first caller stored rather than recomputing it in turn.
    """
    calls = 0

    async def load() -> str:
        nonlocal calls
        calls += 1
        await anyio.sleep(0.05)
        return "computed"

    results: list[str] = []

    async def caller() -> None:
        results.append(await repository.remember("key", load, single_flight=True))

    async with anyio.create_task_group() as tasks:
        for _ in range(CONCURRENT_CALLERS):
            tasks.start_soon(caller)

    assert calls == 1
    assert results == ["computed"] * CONCURRENT_CALLERS


async def test_without_single_flight_concurrent_misses_each_compute(
    repository: Repository,
) -> None:
    """The contrast case, and why single flight is opt-in rather than free.

    The exact count is a scheduling detail; that it is more than one is the
    behaviour being pinned.
    """
    calls = 0

    async def load() -> str:
        nonlocal calls
        calls += 1
        await anyio.sleep(0.05)
        return "computed"

    async def caller() -> None:
        assert await repository.remember("key", load) == "computed"

    async with anyio.create_task_group() as tasks:
        for _ in range(CONCURRENT_CALLERS):
            tasks.start_soon(caller)

    assert calls > 1


async def test_single_flight_returns_the_cached_value_without_computing_on_a_hit(
    recording_repository: Repository, recorder: FakeStore
) -> None:
    """A hit short-circuits before the lock is ever taken."""
    await recording_repository.put("key", "stored")
    recorder.reset()
    calls = 0

    def load() -> str:
        nonlocal calls
        calls += 1
        return "computed"

    assert await recording_repository.remember("key", load, single_flight=True) == "stored"
    assert calls == 0
    recorder.assert_hit("key")
    recorder.assert_not_put("key")


async def test_single_flight_releases_its_lock_when_the_callback_raises(
    repository: Repository,
) -> None:
    """A leaked lock would block every later caller until the TTL expired.

    The repository's timeout is shortened by the fixture, so a leak fails this
    test with a ``LockTimeoutError`` instead of stalling the suite.
    """

    def boom() -> str:
        raise RuntimeError("upstream is down")

    with pytest.raises(RuntimeError, match="upstream is down"):
        await repository.remember("key", boom, single_flight=True)

    assert await repository.remember("key", lambda: "recovered", single_flight=True) == "recovered"


# -- coordination and teardown --------------------------------------------


async def test_lock_is_built_on_the_underlying_store(
    repository: Repository, store: ArrayStore
) -> None:
    """The repository holds no locking of its own; it hands the store's out."""
    lock = repository.lock("import", ttl=5.0)
    assert isinstance(lock, StoreLock)
    assert await lock.acquire() is True
    assert await store.get(lock.key) == lock.owner


async def test_close_delegates_to_the_store(repository: Repository, store: ArrayStore) -> None:
    await repository.put("key", "value")
    await repository.close()
    assert len(store) == 0
    assert await store.get("key") is MISSING
