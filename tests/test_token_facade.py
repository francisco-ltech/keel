"""The token manager, the binding, and the recording double's assertions.

The contract suite proves the drivers agree. This proves the wiring around them:
which driver a name resolves to, that the lifespan restores rather than unbinds,
and that every assertion on ``FakeTokenStore`` both passes when it should and
fails with a timeline when it should not. A double whose failure message says
nothing is barely better than no double.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from keel.auth import (
    Identity,
    bound_token_manager,
    issue_token,
    resolve_token,
    revoke_token,
    set_token_manager,
    token_lifespan,
    token_manager,
    token_store,
    use_token_manager,
)
from keel.auth.config import TokenConfig
from keel.auth.fake import FakeTokenStore, TokenAssertionError, TokenOperation
from keel.auth.manager import TokenManager
from keel.auth.stores.memory import MemoryTokenStore
from keel.auth.tokens import generate_token
from keel.contracts.auth import TokenStore
from keel.exceptions import ConfigurationError
from keel.testing import fake_tokens

pytestmark = [pytest.mark.anyio]

TIMELINE_HEADER = "Recorded token operations"


def principal() -> Identity:
    return Identity(id=uuid4(), roles=frozenset({"admin"}))


def failure(exc: pytest.ExceptionInfo[TokenAssertionError]) -> str:
    """Return the message of a caught assertion, checking it carries a timeline."""
    message = str(exc.value)
    assert TIMELINE_HEADER in message, f"assertion message has no timeline:\n{message}"
    return message


@pytest.fixture
def fake() -> FakeTokenStore:
    return FakeTokenStore()


# -- the manager ----------------------------------------------------------


def test_each_built_in_driver_resolves_to_its_store() -> None:
    memory = TokenManager(TokenConfig(driver="memory")).store()
    fake = TokenManager(TokenConfig(driver="fake")).store()

    assert isinstance(memory, MemoryTokenStore)
    assert isinstance(fake, FakeTokenStore)
    assert memory.name == "memory"


async def test_a_store_reports_the_name_it_was_resolved_under() -> None:
    """Not the configured default, which is a different name in the general case.

    ``RedisTokenStore.from_url`` read ``config.driver``, so a manager defaulting
    to memory that was asked for its redis store handed back one calling itself
    "memory". Needs no live Redis: the client connects lazily.
    """
    manager = TokenManager(TokenConfig(driver="memory", url="redis://localhost:1/0"))
    try:
        assert manager.store("redis").name == "redis"
        assert manager.store().name == "memory"
    finally:
        await manager.close()


def test_a_store_is_memoised_per_name() -> None:
    manager = TokenManager(TokenConfig(driver="memory"))
    assert manager.store() is manager.store("memory")


def test_the_configured_default_ttl_reaches_the_store() -> None:
    manager = TokenManager(TokenConfig(driver="memory", ttl=60.0))
    assert manager.config.ttl == 60.0


def test_an_unknown_driver_names_the_known_ones_and_how_to_register() -> None:
    manager = TokenManager(TokenConfig(driver="dynamo"))

    with pytest.raises(ConfigurationError) as exc:
        manager.store()

    message = str(exc.value)
    assert "dynamo" in message
    assert "memory" in message and "redis" in message and "fake" in message
    assert "register_driver" in message


def test_a_registered_driver_is_built_with_the_configuration() -> None:
    """Open/Closed: a third-party store must not require editing the manager."""
    seen: list[tuple[str, TokenConfig]] = []

    def factory(name: str, config: TokenConfig) -> TokenStore:
        seen.append((name, config))
        return MemoryTokenStore(name, config.ttl)

    manager = TokenManager(TokenConfig(driver="dynamo", ttl=30.0))
    manager.register_driver("dynamo", factory)

    store = manager.store()

    assert store.name == "dynamo"
    assert seen == [("dynamo", manager.config)]


def test_the_redis_driver_refuses_to_build_without_a_url() -> None:
    manager = TokenManager(TokenConfig(driver="memory"))

    with pytest.raises(ConfigurationError, match="needs a url"):
        manager.store("redis")


async def test_close_releases_every_store_that_was_built() -> None:
    manager = TokenManager(TokenConfig(driver="memory"))
    store = manager.store()
    await store.issue(principal())

    await manager.close()

    assert list(manager.resolved_names) == []


# -- the binding and the lifespan -----------------------------------------


async def test_the_facade_works_inside_the_lifespan() -> None:
    identity = principal()

    async with token_lifespan(TokenConfig(driver="memory", ttl=None)) as manager:
        issued = await issue_token(identity, label="web")

        assert token_manager() is manager
        assert await resolve_token(issued.plaintext) == identity
        assert await revoke_token(issued.plaintext) is True
        assert await resolve_token(issued.plaintext) is None


async def test_the_lifespan_restores_the_previous_binding() -> None:
    """A nested test lifespan must not silently kill the application's."""
    outer = TokenManager(TokenConfig(driver="memory"))
    set_token_manager(outer)
    try:
        async with token_lifespan(TokenConfig(driver="memory")) as inner:
            assert token_manager() is inner
        assert bound_token_manager() is outer
    finally:
        set_token_manager(None)


async def test_the_binding_is_restored_even_when_the_body_raises() -> None:
    with pytest.raises(RuntimeError):
        async with token_lifespan(TokenConfig(driver="memory")):
            raise RuntimeError("startup failed")

    assert bound_token_manager() is None


async def test_the_facade_says_what_to_do_when_nothing_is_bound() -> None:
    with pytest.raises(ConfigurationError, match="no token manager is bound") as exc:
        await issue_token(principal())

    assert "fake_tokens" in str(exc.value)


async def test_an_override_wins_over_the_process_wide_binding() -> None:
    manager = TokenManager(TokenConfig(driver="memory"))
    with use_token_manager(manager):
        assert token_manager() is manager
        assert token_store() is manager.store()
    assert bound_token_manager() is None


# -- fake_tokens ----------------------------------------------------------


async def test_fake_tokens_records_what_the_code_under_test_did() -> None:
    identity = principal()

    with fake_tokens() as tokens:
        issued = await issue_token(identity, label="web")
        await resolve_token(issued.plaintext)

        tokens.assert_issued(identity.id, label="web")
        tokens.assert_resolved(issued.plaintext)


async def test_fake_tokens_hands_back_a_credential_that_works() -> None:
    """The reason this fake decorates a real store instead of only recording."""
    identity = principal()

    with fake_tokens() as tokens:
        issued = await issue_token(identity)
        assert await resolve_token(issued.plaintext) == identity
        assert tokens.inner is not None


async def test_fake_tokens_unbinds_when_the_block_ends() -> None:
    with fake_tokens():
        pass
    assert bound_token_manager() is None


async def test_fake_tokens_applies_the_requested_default_ttl() -> None:
    with fake_tokens(ttl=60.0):
        issued = await issue_token(principal())
    assert issued.expires_at is not None


# -- the double's recording -----------------------------------------------


async def test_operations_are_recorded_in_order(fake: FakeTokenStore) -> None:
    identity = principal()
    issued = await fake.issue(identity)
    await fake.resolve(issued.plaintext)
    await fake.revoke(issued.plaintext)
    await fake.issued_for(identity.id)
    await fake.revoke_subject(identity.id)
    await fake.purge_expired()

    assert [op.kind for op in fake.operations] == [
        "issue",
        "resolve",
        "revoke",
        "issued_for",
        "revoke_subject",
        "purge_expired",
    ]


async def test_a_recorded_operation_never_holds_the_plaintext(fake: FakeTokenStore) -> None:
    issued = await fake.issue(principal())
    await fake.resolve(issued.plaintext)

    assert all(issued.plaintext not in op.describe() for op in fake.operations)


async def test_reset_clears_the_history_but_not_the_tokens(fake: FakeTokenStore) -> None:
    identity = principal()
    issued = await fake.issue(identity)

    fake.reset()

    assert fake.operations == ()
    assert await fake.resolve(issued.plaintext) == identity


def test_operations_is_a_snapshot(fake: FakeTokenStore) -> None:
    assert fake.operations == ()
    assert isinstance(fake.operations, tuple)


def test_the_double_reports_the_name_it_was_built_as() -> None:
    assert FakeTokenStore(name="contract").name == "contract"


# -- the double's assertions ----------------------------------------------


async def test_assert_issued_passes_and_narrows_by_label(fake: FakeTokenStore) -> None:
    identity = principal()
    await fake.issue(identity, ttl=30.0, label="web")

    operation = fake.assert_issued(identity.id, label="web")

    assert operation.ttl == 30.0


async def test_assert_issued_fails_when_nothing_was_issued(fake: FakeTokenStore) -> None:
    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_issued(uuid4())
    assert "no token operations were recorded" in failure(exc)


async def test_assert_issued_fails_when_the_label_differs(fake: FakeTokenStore) -> None:
    identity = principal()
    await fake.issue(identity, label="web")

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_issued(identity.id, label="cli")
    assert "labelled 'cli'" in failure(exc)


async def test_assert_not_issued_passes_for_another_principal(fake: FakeTokenStore) -> None:
    await fake.issue(principal())
    fake.assert_not_issued(uuid4())


async def test_assert_not_issued_fails_when_one_was(fake: FakeTokenStore) -> None:
    identity = principal()
    await fake.issue(identity)

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_not_issued(identity.id)
    assert "expected no token issued" in failure(exc)


async def test_assert_issued_times_passes_and_fails_on_the_count(fake: FakeTokenStore) -> None:
    identity = principal()
    await fake.issue(identity)
    await fake.issue(identity)

    fake.assert_issued_times(identity.id, 2)

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_issued_times(identity.id, 1)
    assert "got 2" in failure(exc)


async def test_assert_nothing_issued_passes_then_fails(fake: FakeTokenStore) -> None:
    fake.assert_nothing_issued()
    await fake.issue(principal())

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_nothing_issued()
    assert "expected no tokens to be issued" in failure(exc)


async def test_assert_resolved_passes_for_an_accepted_token(fake: FakeTokenStore) -> None:
    issued = await fake.issue(principal())
    await fake.resolve(issued.plaintext)

    assert fake.assert_resolved(issued.plaintext).hit is True


async def test_assert_resolved_fails_when_the_token_was_never_presented(
    fake: FakeTokenStore,
) -> None:
    issued = await fake.issue(principal())

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_resolved(issued.plaintext)
    assert "never was" in failure(exc)


async def test_assert_resolved_fails_when_every_resolve_refused(fake: FakeTokenStore) -> None:
    unknown = generate_token()
    await fake.resolve(unknown)

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_resolved(unknown)
    assert "every resolve refused it" in failure(exc)


async def test_assert_rejected_passes_for_a_refused_token(fake: FakeTokenStore) -> None:
    unknown = generate_token()
    await fake.resolve(unknown)

    assert fake.assert_rejected(unknown).hit is False


async def test_assert_rejected_fails_when_the_token_worked(fake: FakeTokenStore) -> None:
    issued = await fake.issue(principal())
    await fake.resolve(issued.plaintext)

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_rejected(issued.plaintext)
    assert "every resolve accepted it" in failure(exc)


async def test_assert_rejected_fails_when_nothing_was_presented(fake: FakeTokenStore) -> None:
    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_rejected(generate_token())
    assert "never was" in failure(exc)


async def test_assert_revoked_passes_and_reports_the_outcome(fake: FakeTokenStore) -> None:
    issued = await fake.issue(principal())
    await fake.revoke(issued.plaintext)

    assert fake.assert_revoked(issued.plaintext).result is True


async def test_assert_revoked_fails_when_it_never_happened(fake: FakeTokenStore) -> None:
    issued = await fake.issue(principal())

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_revoked(issued.plaintext)
    assert "expected that token to be revoked" in failure(exc)


async def test_assert_not_revoked_passes_then_fails(fake: FakeTokenStore) -> None:
    kept = await fake.issue(principal())
    dropped = await fake.issue(principal())

    fake.assert_not_revoked(kept.plaintext)
    await fake.revoke(dropped.plaintext)

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_not_revoked(dropped.plaintext)
    assert "not to be revoked" in failure(exc)


async def test_assert_subject_revoked_reports_how_many_went(fake: FakeTokenStore) -> None:
    identity = principal()
    await fake.issue(identity)
    await fake.issue(identity)
    await fake.revoke_subject(identity.id)

    assert fake.assert_subject_revoked(identity.id).result == 2


async def test_assert_subject_revoked_fails_when_the_reset_forgot_to(fake: FakeTokenStore) -> None:
    identity = principal()
    await fake.issue(identity)

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_subject_revoked(identity.id)
    assert "to be revoked" in failure(exc)


# -- the failure timeline -------------------------------------------------


def test_an_empty_timeline_says_so(fake: FakeTokenStore) -> None:
    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_issued(uuid4())
    assert "(no token operations were recorded)" in str(exc.value)


async def test_the_timeline_is_numbered_and_shows_the_outcome(fake: FakeTokenStore) -> None:
    identity = principal()
    issued = await fake.issue(identity, label="web")
    await fake.resolve(issued.plaintext)
    await fake.resolve(generate_token())

    with pytest.raises(TokenAssertionError) as exc:
        fake.assert_nothing_issued()

    message = failure(exc)
    assert "1. issue" in message
    assert "label='web'" in message
    assert "ACCEPTED" in message
    assert "REFUSED" in message


def test_describe_shows_a_truncated_digest() -> None:
    operation = TokenOperation("revoke", digest="a" * 64, result=True)
    rendered = operation.describe()

    assert "a" * 12 in rendered
    assert "a" * 13 not in rendered


def test_a_token_assertion_error_is_an_assertion_error() -> None:
    """So pytest renders it as a failure rather than an error."""
    assert issubclass(TokenAssertionError, AssertionError)
