"""The cache test double and the ``fake_cache`` helper.

Every assertion method is exercised twice — once where it should pass and once
where it should fail — because a test double whose failure message is unhelpful
is barely better than no double at all. The failure messages are therefore
asserted on directly: each one must carry the recorded timeline.
"""

from __future__ import annotations

import pytest

from keel.cache.fake import CacheAssertionError, FakeStore, Operation
from keel.cache.proxy import cache
from keel.cache.repository import Repository
from keel.cache.stores.array import ArrayStore
from keel.exceptions import ConfigurationError
from keel.support.sentinels import MISSING
from keel.testing import fake_cache

pytestmark = [pytest.mark.anyio]

TIMELINE_HEADER = "Recorded cache operations"


@pytest.fixture
def fake() -> FakeStore:
    return FakeStore()


def failure(exc: pytest.ExceptionInfo[CacheAssertionError]) -> str:
    """Return the message of a caught assertion, checking it carries a timeline."""
    message = str(exc.value)
    assert TIMELINE_HEADER in message, f"assertion message has no timeline:\n{message}"
    return message


# -- recording ------------------------------------------------------------


async def test_operations_are_recorded_in_order(fake: FakeStore) -> None:
    await fake.put("a", 1, 60)
    await fake.get("a")
    await fake.get("absent")
    await fake.forget("a")
    await fake.flush()

    assert [op.kind for op in fake.operations] == ["put", "get", "get", "forget", "flush"]


async def test_a_read_records_the_value_and_the_hit_flag(fake: FakeStore) -> None:
    await fake.put("key", "value")
    await fake.get("key")
    await fake.get("absent")

    hit, miss = fake.operations[1], fake.operations[2]
    assert hit == Operation("get", "key", "value", hit=True)
    assert miss == Operation("get", "absent", None, hit=False)


async def test_bulk_operations_record_one_entry_per_key(fake: FakeStore) -> None:
    await fake.put_many({"a": 1, "b": 2}, 30)
    await fake.many(["a", "missing"])

    assert [(op.kind, op.key) for op in fake.operations] == [
        ("put_many", "a"),
        ("put_many", "b"),
        ("many", "a"),
        ("many", "missing"),
    ]


async def test_increment_records_the_resulting_value(fake: FakeStore) -> None:
    await fake.increment("hits", 5)
    recorded = fake.operations[-1]
    assert recorded.kind == "increment"
    assert recorded.value == 5


async def test_forget_if_records_the_expected_value(fake: FakeStore) -> None:
    await fake.put("key", "owner")
    await fake.forget_if("key", "owner")
    recorded = fake.operations[-1]
    assert recorded.kind == "forget_if"
    assert recorded.value == "owner"


async def test_operations_is_a_snapshot(fake: FakeStore) -> None:
    await fake.put("a", 1)
    snapshot = fake.operations
    await fake.put("b", 2)
    assert len(snapshot) == 1


async def test_reset_clears_the_history_but_not_the_data(fake: FakeStore) -> None:
    await fake.put("key", "value")
    fake.reset()

    assert fake.operations == ()
    assert await fake.get("key") == "value"


def test_the_fake_delegates_to_a_real_store_by_default(fake: FakeStore) -> None:
    assert isinstance(fake.inner, ArrayStore)


def test_the_fake_can_wrap_any_store() -> None:
    inner = ArrayStore()
    wrapper = FakeStore(inner)
    assert wrapper.inner is inner
    assert wrapper.namespace is inner.namespace
    assert wrapper.supports_atomic_increment is inner.supports_atomic_increment


async def test_a_lock_is_delegated_to_the_inner_store_unrecorded(fake: FakeStore) -> None:
    lock = fake.lock("resource", 30)
    assert await lock.acquire() is True
    assert await fake.lock("resource", 30).acquire() is False
    assert [op.kind for op in fake.operations] == []


# -- assert_hit -----------------------------------------------------------


async def test_assert_hit_passes_when_the_key_was_read_and_found(fake: FakeStore) -> None:
    await fake.put("key", "value")
    await fake.get("key")
    fake.assert_hit("key")


async def test_assert_hit_accepts_a_hit_from_a_bulk_read(fake: FakeStore) -> None:
    await fake.put("key", "value")
    await fake.many(["key"])
    fake.assert_hit("key")


async def test_assert_hit_fails_when_the_key_was_never_read(fake: FakeStore) -> None:
    await fake.put("key", "value")
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_hit("key")
    assert "never read" in failure(error)


async def test_assert_hit_fails_when_every_read_missed(fake: FakeStore) -> None:
    await fake.get("key")
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_hit("key")
    assert "every read missed" in failure(error)


# -- assert_missed --------------------------------------------------------


async def test_assert_missed_passes_when_the_key_was_absent(fake: FakeStore) -> None:
    await fake.get("key")
    fake.assert_missed("key")


async def test_assert_missed_passes_for_the_first_of_several_reads(fake: FakeStore) -> None:
    """The miss-then-populate-then-hit sequence a ``remember`` produces."""
    await fake.get("key")
    await fake.put("key", "value")
    await fake.get("key")
    fake.assert_missed("key")
    fake.assert_hit("key")


async def test_assert_missed_fails_when_the_key_was_never_read(fake: FakeStore) -> None:
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_missed("key")
    assert "never read" in failure(error)


async def test_assert_missed_fails_when_every_read_was_a_hit(fake: FakeStore) -> None:
    await fake.put("key", "value")
    await fake.get("key")
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_missed("key")
    assert "every read was a hit" in failure(error)


# -- assert_put -----------------------------------------------------------


async def test_assert_put_passes_for_a_bare_write(fake: FakeStore) -> None:
    await fake.put("key", "value", 60)
    fake.assert_put("key")


async def test_assert_put_passes_for_put_many_and_add(fake: FakeStore) -> None:
    await fake.put_many({"bulk": 1})
    await fake.add("added", 2)
    fake.assert_put("bulk")
    fake.assert_put("added")


async def test_assert_put_passes_when_the_value_matches(fake: FakeStore) -> None:
    await fake.put("key", {"name": "Ada"}, 60)
    fake.assert_put("key", {"name": "Ada"})


async def test_assert_put_passes_when_the_ttl_matches(fake: FakeStore) -> None:
    await fake.put("key", "value", 60)
    fake.assert_put("key", ttl=60)
    fake.assert_put("key", "value", 60)


async def test_assert_put_passes_for_a_ttl_of_none(fake: FakeStore) -> None:
    await fake.put("key", "value", None)
    fake.assert_put("key", ttl=None)


async def test_assert_put_matches_any_one_of_several_writes(fake: FakeStore) -> None:
    await fake.put("key", "first", 10)
    await fake.put("key", "second", 20)
    fake.assert_put("key", "first")
    fake.assert_put("key", "second")
    fake.assert_put("key", ttl=20)


async def test_assert_put_fails_when_the_key_was_never_written(fake: FakeStore) -> None:
    await fake.get("key")
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_put("key")
    assert "never was" in failure(error)


async def test_assert_put_fails_when_the_value_differs(fake: FakeStore) -> None:
    await fake.put("key", "actual", 60)
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_put("key", "expected")
    message = failure(error)
    assert "'expected'" in message
    assert "'actual'" in message


async def test_assert_put_fails_when_the_ttl_differs(fake: FakeStore) -> None:
    await fake.put("key", "value", 60)
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_put("key", ttl=30)
    message = failure(error)
    assert "ttl=30" in message
    assert "60" in message


# -- assert_not_put -------------------------------------------------------


async def test_assert_not_put_passes_when_only_reads_happened(fake: FakeStore) -> None:
    await fake.get("key")
    fake.assert_not_put("key")


async def test_assert_not_put_ignores_writes_to_other_keys(fake: FakeStore) -> None:
    await fake.put("other", "value")
    fake.assert_not_put("key")


async def test_assert_not_put_fails_when_the_key_was_written(fake: FakeStore) -> None:
    await fake.put("key", "value")
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_not_put("key")
    assert "never to be written" in failure(error)


async def test_assert_not_put_fails_on_an_add_that_did_not_create(fake: FakeStore) -> None:
    """``add`` records the attempt, so a failed put-if-absent still counts."""
    await fake.put("key", "first")
    fake.reset()
    assert await fake.add("key", "second") is False
    with pytest.raises(CacheAssertionError):
        fake.assert_not_put("key")


# -- assert_forgotten -----------------------------------------------------


async def test_assert_forgotten_passes_after_a_forget(fake: FakeStore) -> None:
    await fake.put("key", "value")
    await fake.forget("key")
    fake.assert_forgotten("key")


async def test_assert_forgotten_passes_after_a_conditional_forget(fake: FakeStore) -> None:
    await fake.put("key", "value")
    await fake.forget_if("key", "value")
    fake.assert_forgotten("key")


async def test_assert_forgotten_passes_even_when_nothing_existed(fake: FakeStore) -> None:
    """The attempt is what is recorded, not the outcome."""
    assert await fake.forget("key") is False
    fake.assert_forgotten("key")


async def test_assert_forgotten_fails_when_the_key_was_never_removed(fake: FakeStore) -> None:
    await fake.put("key", "value")
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_forgotten("key")
    assert "never was" in failure(error)


# -- assert_flushed -------------------------------------------------------


async def test_assert_flushed_passes_after_a_flush(fake: FakeStore) -> None:
    await fake.flush()
    fake.assert_flushed()


async def test_assert_flushed_fails_when_nothing_was_flushed(fake: FakeStore) -> None:
    await fake.forget("key")
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_flushed()
    assert "never was" in failure(error)


# -- assert_nothing_written -----------------------------------------------


async def test_assert_nothing_written_passes_for_a_read_only_run(fake: FakeStore) -> None:
    await fake.get("key")
    await fake.many(["a", "b"])
    await fake.forget("key")
    fake.assert_nothing_written()


def test_assert_nothing_written_passes_when_nothing_happened_at_all(fake: FakeStore) -> None:
    fake.assert_nothing_written()


@pytest.mark.parametrize("kind", ["put", "put_many", "add", "increment"])
async def test_assert_nothing_written_fails_for_every_kind_of_write(
    fake: FakeStore, kind: str
) -> None:
    if kind == "put":
        await fake.put("key", 1)
    elif kind == "put_many":
        await fake.put_many({"key": 1})
    elif kind == "add":
        await fake.add("key", 1)
    else:
        await fake.increment("key")

    with pytest.raises(CacheAssertionError) as error:
        fake.assert_nothing_written()
    assert "1 were recorded" in failure(error)


# -- assert_operation_count -----------------------------------------------


async def test_assert_operation_count_passes_for_the_exact_number(fake: FakeStore) -> None:
    fake.assert_operation_count(0)
    await fake.get("key")
    await fake.put("key", "value")
    fake.assert_operation_count(2)


async def test_assert_operation_count_fails_when_too_many_ran(fake: FakeStore) -> None:
    await fake.get("a")
    await fake.get("b")
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_operation_count(1)
    assert "expected 1 cache operations, recorded 2" in failure(error)


async def test_assert_operation_count_fails_when_too_few_ran(fake: FakeStore) -> None:
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_operation_count(3)
    assert "recorded 0" in failure(error)


# -- the failure messages themselves --------------------------------------


def test_an_empty_timeline_says_so(fake: FakeStore) -> None:
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_flushed()
    assert "(no cache operations were recorded)" in failure(error)


async def test_the_timeline_is_numbered_and_shows_hits_and_misses(fake: FakeStore) -> None:
    await fake.get("profile:42")
    await fake.put("profile:42", {"name": "Ada"}, 300)
    await fake.get("profile:42")

    with pytest.raises(CacheAssertionError) as error:
        fake.assert_flushed()
    message = failure(error)

    assert "1. get 'profile:42' MISS" in message
    assert "2. put 'profile:42' value={'name': 'Ada'} ttl=300" in message
    assert "3. get 'profile:42' HIT" in message


async def test_a_flush_appears_in_the_timeline_without_a_key(fake: FakeStore) -> None:
    await fake.flush()
    with pytest.raises(CacheAssertionError) as error:
        fake.assert_hit("key")
    assert "1. flush" in failure(error)


def test_a_cache_assertion_error_is_an_assertion_error() -> None:
    """So pytest reports it as a failure rather than an error."""
    assert issubclass(CacheAssertionError, AssertionError)


# -- fake_cache end to end ------------------------------------------------


async def refresh_profile(user_id: int) -> dict[str, object]:
    """Production-shaped code under test: it only knows the module-level facade."""
    return await cache.remember(f"profile:{user_id}", lambda: {"id": user_id, "name": "Ada"})


async def test_fake_cache_records_what_the_code_under_test_did() -> None:
    with fake_cache() as cached:
        assert await refresh_profile(42) == {"id": 42, "name": "Ada"}

        cached.assert_missed("profile:42")
        cached.assert_put("profile:42", {"id": 42, "name": "Ada"}, 300.0)
        cached.assert_operation_count(2)


async def test_fake_cache_lets_a_second_call_hit() -> None:
    with fake_cache() as cached:
        await refresh_profile(42)
        cached.reset()
        await refresh_profile(42)

        cached.assert_hit("profile:42")
        cached.assert_nothing_written()
        cached.assert_operation_count(1)


async def test_fake_cache_applies_the_requested_default_ttl() -> None:
    with fake_cache(default_ttl=45.0) as cached:
        await cache.put("key", "value")
        cached.assert_put("key", "value", 45.0)


async def test_fake_cache_can_delegate_to_a_supplied_store() -> None:
    inner = ArrayStore()
    with fake_cache(inner=inner) as cached:
        await cache.put("key", "value")
        assert cached.inner is inner
    assert await inner.get("key") == "value"


async def test_fake_cache_unbinds_when_the_block_ends() -> None:
    with fake_cache():
        await cache.put("key", "value")

    with pytest.raises(ConfigurationError):
        await cache.get("key")


async def test_fake_cache_is_a_real_cache_not_a_stub() -> None:
    """The fake delegates, so behaviour under test is the real behaviour."""
    with fake_cache() as cached:
        await cache.put("key", None)
        assert await cache.has("key") is True
        assert await cached.get("key") is None
        assert await cached.get("absent") is MISSING


async def test_fake_cache_is_reachable_through_a_named_store_too() -> None:
    with fake_cache() as cached:
        repository = cache.of("default")
        assert isinstance(repository, Repository)
        await repository.put("key", "value")
        cached.assert_put("key")
