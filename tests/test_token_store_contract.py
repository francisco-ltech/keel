"""The token store contract, enforced against every implementation.

Each test here runs once per store in the ``backend`` parametrisation. A driver
is not "a token store" because it has the right method names — it is one because
it behaves identically to the others under the same assertions. That is the
Phase 1 lesson, repeated for the third subsystem, and it is the only thing that
makes "swap the driver" a safe sentence.

The security properties are contract tests rather than driver tests on purpose.
"the plaintext is never recoverable" and "an expired token stops resolving" are
promises :class:`~keel.contracts.auth.TokenStore` makes on behalf of every
implementation, so a new driver that quietly fails either one must fail here
rather than in production.

``FakeTokenStore`` is **in** the parametrisation, unlike ``FakeQueue``'s absent
counterparts elsewhere: it decorates a real store, so it is expected to satisfy
the behaviour and not only the interface. A test double that does not behave
like the thing it doubles is worse than no double at all.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import anyio
import pytest

from keel.auth.config import TokenConfig
from keel.auth.fake import FakeTokenStore
from keel.auth.identity import Identity
from keel.auth.stores.memory import MemoryTokenStore
from keel.auth.tokens import digest_token, encode_record, generate_token
from keel.contracts.auth import TokenStore

pytestmark = [pytest.mark.anyio, pytest.mark.contract]

BRIEF_TTL = 0.05
"""Long enough that nothing expires mid-issue, short enough to wait out."""

PAST_TTL = 0.06
"""How long to wait for :data:`BRIEF_TTL` to have passed."""


def principal(*roles: str, **claims: Any) -> Identity:
    """Build a distinct principal, so tests cannot collide on a subject id."""
    return Identity(id=uuid4(), roles=frozenset(roles), claims=claims)


# -- the implementations --------------------------------------------------


@pytest.fixture
async def memory_backend() -> AsyncIterator[TokenStore]:
    """The in-process store, with no default expiry so tests own the clock."""
    store = MemoryTokenStore("contract", None)
    yield store
    await store.close()


@pytest.fixture
async def fake_backend() -> AsyncIterator[TokenStore]:
    """The recording double, over its own in-process store."""
    store = FakeTokenStore(name="contract")
    yield store
    await store.close()


@pytest.fixture
async def redis_backend(redis_url: str, namespace_suffix: str) -> AsyncIterator[TokenStore]:
    """A real Redis store on a namespace unique to this test.

    The suffix carries the pid and a uuid, so xdist workers and repeated runs
    cannot see each other's tokens. The namespace is swept afterwards rather
    than flushed: a token store shares its server, and this suite must not be
    able to delete a cache.
    """
    from redis.asyncio import Redis

    from keel.auth.stores.redis import RedisTokenStore

    prefix = f"keel-contract-tokens-{namespace_suffix}"
    config = TokenConfig(driver="redis", url=redis_url, prefix=prefix, ttl=None)
    store = RedisTokenStore.from_url(redis_url, config)
    try:
        yield store
    finally:
        await store.close()
        client: Redis = Redis.from_url(redis_url)
        try:
            async for key in client.scan_iter(match=f"{prefix}:*", count=500):
                await client.unlink(key)
        finally:
            await client.aclose()


@pytest.fixture(
    params=[
        pytest.param("memory_backend", id="memory"),
        pytest.param("fake_backend", id="fake"),
        pytest.param("redis_backend", id="redis", marks=pytest.mark.redis),
    ]
)
def backend(request: pytest.FixtureRequest) -> TokenStore:
    """Every implementation that claims to satisfy the token store contract."""
    resolved: TokenStore = request.getfixturevalue(request.param)
    return resolved


# -- issuing and resolving ------------------------------------------------


async def test_a_token_resolves_to_the_identity_it_was_issued_for(backend: TokenStore) -> None:
    identity = principal("admin")
    issued = await backend.issue(identity)

    assert await backend.resolve(issued.plaintext) == identity


async def test_issue_reports_the_identity_and_expiry_it_recorded(backend: TokenStore) -> None:
    identity = principal()
    issued = await backend.issue(identity, ttl=60.0, label="laptop")

    assert issued.identity == identity
    assert issued.record.label == "laptop"
    assert issued.expires_at is not None
    assert issued.record.ttl_remaining() == pytest.approx(60.0, abs=1.0)


async def test_two_tokens_for_the_same_identity_are_distinct(backend: TokenStore) -> None:
    """Two devices are two credentials. Revoking one must not revoke the other."""
    identity = principal()
    first = await backend.issue(identity, label="laptop")
    second = await backend.issue(identity, label="phone")

    assert first.plaintext != second.plaintext
    assert first.record.digest != second.record.digest
    assert await backend.resolve(first.plaintext) == identity
    assert await backend.resolve(second.plaintext) == identity


async def test_roles_and_claims_survive_a_round_trip(backend: TokenStore) -> None:
    """The serialisation test. A driver that drops claims authorises the wrong thing."""
    identity = principal("admin", "editor", tenant="acme", scopes=["read", "write"], seats=3)
    issued = await backend.issue(identity)

    resolved = await backend.resolve(issued.plaintext)

    assert resolved is not None
    assert resolved.id == identity.id
    assert resolved.roles == frozenset({"admin", "editor"})
    assert resolved.claims == {"tenant": "acme", "scopes": ["read", "write"], "seats": 3}
    assert resolved.has_role("editor")


async def test_an_unknown_token_resolves_to_none(backend: TokenStore) -> None:
    assert await backend.resolve(generate_token()) is None


async def test_a_malformed_token_resolves_to_none(backend: TokenStore) -> None:
    """Whatever arrives in an Authorization header is a string, not a token."""
    assert await backend.resolve("") is None
    assert await backend.resolve("not-a-token") is None


# -- the property the subsystem exists for --------------------------------


async def test_the_plaintext_is_never_recoverable_from_the_store(backend: TokenStore) -> None:
    """A store that can hand back a working credential is a credential dump.

    Asserted against everything the store will give back: the record it keeps
    holds the digest, and the plaintext appears nowhere in its serialised form.
    """
    identity = principal()
    issued = await backend.issue(identity, label="laptop")

    records = await backend.issued_for(identity.id)

    assert [record.digest for record in records] == [digest_token(issued.plaintext)]
    assert records[0].digest != issued.plaintext
    encoded = encode_record(records[0])
    assert issued.plaintext not in encoded
    assert issued.plaintext not in repr(records[0])


# -- expiry ---------------------------------------------------------------


async def test_an_expired_token_resolves_to_none(backend: TokenStore) -> None:
    """Checked on read, not left to the backend.

    Redis drops the key itself and a dict does not, so a contract that trusted
    the backend would mean two behaviours under one name.
    """
    issued = await backend.issue(principal(), ttl=BRIEF_TTL)
    await anyio.sleep(PAST_TTL)

    assert await backend.resolve(issued.plaintext) is None


async def test_an_expired_token_is_excluded_from_the_listing(backend: TokenStore) -> None:
    identity = principal()
    live = await backend.issue(identity, label="live")
    await backend.issue(identity, ttl=BRIEF_TTL, label="stale")
    await anyio.sleep(PAST_TTL)

    records = await backend.issued_for(identity.id)

    assert [record.label for record in records] == ["live"]
    assert records[0].digest == digest_token(live.plaintext)


async def test_purge_expired_leaves_live_tokens_working(backend: TokenStore) -> None:
    """Housekeeping must be safe to run on a schedule against live traffic.

    The *return value* is deliberately not asserted across drivers: Redis
    expires the records itself and prunes only the index that pointed at them,
    so "how many were removed" counts different things in each. What every
    driver must agree on is that nothing live is touched.
    """
    identity = principal()
    live = await backend.issue(identity, label="live")
    stale = await backend.issue(identity, ttl=BRIEF_TTL, label="stale")
    await anyio.sleep(PAST_TTL)

    removed = await backend.purge_expired()

    assert removed >= 0
    assert await backend.resolve(live.plaintext) == identity
    assert await backend.resolve(stale.plaintext) is None
    assert [record.label for record in await backend.issued_for(identity.id)] == ["live"]


async def test_purging_when_nothing_has_expired_is_harmless(backend: TokenStore) -> None:
    identity = principal()
    issued = await backend.issue(identity)

    await backend.purge_expired()

    assert await backend.resolve(issued.plaintext) == identity


# -- revocation -----------------------------------------------------------


async def test_revoke_stops_a_token_resolving(backend: TokenStore) -> None:
    issued = await backend.issue(principal())

    assert await backend.revoke(issued.plaintext) is True
    assert await backend.resolve(issued.plaintext) is None


async def test_revoking_twice_reports_false_the_second_time(backend: TokenStore) -> None:
    """The signal a "sign out" endpoint needs to tell a real revocation from a replay."""
    issued = await backend.issue(principal())

    assert await backend.revoke(issued.plaintext) is True
    assert await backend.revoke(issued.plaintext) is False


async def test_revoking_an_unknown_token_reports_false(backend: TokenStore) -> None:
    assert await backend.revoke(generate_token()) is False


async def test_a_revoked_token_disappears_from_the_listing(backend: TokenStore) -> None:
    identity = principal()
    kept = await backend.issue(identity, label="laptop")
    dropped = await backend.issue(identity, label="phone")

    await backend.revoke(dropped.plaintext)

    records = await backend.issued_for(identity.id)
    assert [record.digest for record in records] == [digest_token(kept.plaintext)]


async def test_revoking_one_token_leaves_the_others_alone(backend: TokenStore) -> None:
    identity = principal()
    revoked = await backend.issue(identity, label="phone")
    kept = await backend.issue(identity, label="laptop")

    await backend.revoke(revoked.plaintext)

    assert await backend.resolve(kept.plaintext) == identity


async def test_revoke_subject_removes_every_token_and_reports_the_count(
    backend: TokenStore,
) -> None:
    identity = principal()
    first = await backend.issue(identity, label="laptop")
    second = await backend.issue(identity, label="phone")

    assert await backend.revoke_subject(identity.id) == 2
    assert await backend.resolve(first.plaintext) is None
    assert await backend.resolve(second.plaintext) is None
    assert await backend.issued_for(identity.id) == []


async def test_revoke_subject_leaves_another_subjects_tokens_alone(backend: TokenStore) -> None:
    """The blast radius of a password reset, pinned."""
    compromised = principal()
    bystander = principal()
    await backend.issue(compromised, label="laptop")
    survivor = await backend.issue(bystander, label="laptop")

    assert await backend.revoke_subject(compromised.id) == 1
    assert await backend.resolve(survivor.plaintext) == bystander
    assert len(await backend.issued_for(bystander.id)) == 1


async def test_revoke_subject_does_not_count_a_token_that_had_already_expired(
    backend: TokenStore,
) -> None:
    """ "How many *live* tokens were removed" — one that had expired was not live.

    Redis holds a record a little past its own expiry, because a key's TTL has
    whole-second granularity. A driver that counts rows instead of applying the
    same expiry check its read path applies therefore reports a sign-out larger
    than the one that happened, while ``resolve`` and ``issued_for`` disagree.
    """
    identity = principal()
    live = await backend.issue(identity, label="live")
    await backend.issue(identity, ttl=BRIEF_TTL, label="stale")
    await anyio.sleep(PAST_TTL)

    assert await backend.revoke_subject(identity.id) == 1
    assert await backend.resolve(live.plaintext) is None


async def test_revoke_subject_reports_nothing_for_a_principal_with_no_tokens(
    backend: TokenStore,
) -> None:
    assert await backend.revoke_subject(uuid4()) == 0


# -- listing --------------------------------------------------------------


async def test_issued_for_returns_newest_first(backend: TokenStore) -> None:
    """What a "signed-in devices" screen renders, in the order it renders it."""
    identity = principal()
    await backend.issue(identity, label="first")
    await anyio.sleep(0.01)
    await backend.issue(identity, label="second")
    await anyio.sleep(0.01)
    await backend.issue(identity, label="third")

    records = await backend.issued_for(identity.id)

    assert [record.label for record in records] == ["third", "second", "first"]


async def test_issued_for_carries_no_secret(backend: TokenStore) -> None:
    identity = principal("admin")
    issued = await backend.issue(identity, label="laptop")

    record = (await backend.issued_for(identity.id))[0]

    assert record.subject == identity.id
    assert record.identity == identity
    assert record.label == "laptop"
    assert record.digest == digest_token(issued.plaintext)
    assert not hasattr(record, "plaintext")


async def test_issued_for_is_empty_for_a_principal_with_no_tokens(backend: TokenStore) -> None:
    assert await backend.issued_for(uuid4()) == []


async def test_issued_for_does_not_leak_between_principals(backend: TokenStore) -> None:
    mine = principal()
    yours = principal()
    await backend.issue(mine, label="mine")
    await backend.issue(yours, label="yours")

    assert [record.label for record in await backend.issued_for(mine.id)] == ["mine"]
    assert [record.label for record in await backend.issued_for(yours.id)] == ["yours"]


async def test_the_subject_of_a_record_is_the_identity_that_was_issued_it(
    backend: TokenStore,
) -> None:
    identity = principal()
    await backend.issue(identity)

    record = (await backend.issued_for(identity.id))[0]

    assert isinstance(record.subject, UUID)
    assert record.subject == identity.id


# -- lifecycle ------------------------------------------------------------


async def test_the_store_reports_its_name(backend: TokenStore) -> None:
    assert isinstance(backend.name, str)
    assert backend.name


async def test_close_is_idempotent(backend: TokenStore) -> None:
    """A shutdown path that runs twice must not be a second failure mode."""
    await backend.close()
    await backend.close()
