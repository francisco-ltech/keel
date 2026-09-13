"""Regression tests for the Redis token store.

Every test here is named for a defect an adversarial review confirmed against a
live Redis, and each one fails against the code as it was written. They live
apart from the contract suite on purpose: these pin Redis-specific mechanics —
what the index does, what a scan survives — which the contract has no business
knowing about.

The one that matters most is the orphaning pair. Both `revoke_subject` and
`purge_expired` deleted the whole subject index rather than removing the members
they had read, so a token issued during the round trip in between survived with
no index entry: still resolving, invisible to `issued_for`, and immune to "sign
out everywhere" for the rest of its life. On the password-reset path.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable
from typing import Any
from uuid import uuid4

import anyio
import pytest
from redis.asyncio import Redis

from keel.auth.identity import Identity
from keel.auth.stores.redis import RedisTokenStore
from keel.exceptions import ConfigurationError
from keel.support.keys import KeyNamespace

pytestmark = [pytest.mark.anyio, pytest.mark.redis]

EXPIRES_FAST = 0.05
"""A lifetime short enough to have passed by the next assertion."""

SETTLE = 0.1
"""Long enough to outlast it. Redis rounds a record's own TTL up to the next
whole second, so these tests exercise the expired-but-still-present window that
``_read`` exists for — which is the point, not an accident."""


@pytest.fixture
async def store(redis_url: str, namespace_suffix: str) -> AsyncIterator[RedisTokenStore]:
    """A store in its own namespace, torn down completely afterwards."""
    namespace = KeyNamespace(f"test-tokens-{namespace_suffix}")
    client = Redis.from_url(redis_url, decode_responses=False)
    store = RedisTokenStore(client, namespace=namespace, ttl=None, owns_client=True)
    yield store
    keys = [key async for key in client.scan_iter(match=namespace.pattern(), count=500)]
    if keys:
        await client.unlink(*keys)
    await store.close()


async def _issue_during_the_index_read(
    store: RedisTokenStore, identity: Identity, operation: Awaitable[object]
) -> str:
    """Run *operation*, slipping a fresh token in between its read and its write.

    Both orphaning bugs needed exactly this interleaving: an SADD landing after
    the SMEMBERS that decided what to remove. Hooking MGET puts it there
    deterministically instead of racing for it.

    Args:
        store: The store under test.
        identity: Who the extra token belongs to.
        operation: The coroutine to run in the middle of.

    Returns:
        The plaintext of the token issued mid-operation.
    """
    issued: list[str] = []
    original = store._client.mget

    async def hooked(keys: list[str]) -> Any:
        result = await original(keys)
        if not issued:
            issued.append((await store.issue(identity)).plaintext)
        return result

    store._client.mget = hooked  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
    try:
        await operation
    finally:
        store._client.mget = original  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
    return issued[0]


async def test_revoke_subject_does_not_orphan_a_concurrently_issued_token(
    store: RedisTokenStore,
) -> None:
    """The password-reset path must not leave a token nothing can ever revoke.

    A token issued between the index read and the write used to survive the
    index deletion, resolving forever with no way to reach it.
    """
    identity = Identity(id=uuid4())
    old = await store.issue(identity)

    survivor = await _issue_during_the_index_read(
        store, identity, store.revoke_subject(identity.id)
    )

    assert await store.resolve(old.plaintext) is None
    assert await store.resolve(survivor) is not None, "the new token was revoked"
    assert len(await store.issued_for(identity.id)) == 1, "the new token left the index"
    assert await store.revoke_subject(identity.id) == 1, "the new token became unrevocable"
    assert await store.resolve(survivor) is None


async def test_purge_expired_does_not_orphan_a_concurrently_issued_token(
    store: RedisTokenStore,
) -> None:
    """Housekeeping runs on ordinary traffic, so this fired far more often."""
    identity = Identity(id=uuid4())
    await store.issue(identity, ttl=EXPIRES_FAST)
    await anyio.sleep(SETTLE)

    survivor = await _issue_during_the_index_read(store, identity, store.purge_expired())

    assert await store.resolve(survivor) is not None
    assert len(await store.issued_for(identity.id)) == 1
    assert await store.revoke_subject(identity.id) == 1


async def test_purge_expired_reclaims_the_index_it_leaves_behind(
    store: RedisTokenStore,
) -> None:
    """The index has no TTL, so nothing else bounds it.

    An earlier attempt to give it one was inert: EXPIRE ... GT refuses on a key
    with no expiry, and SADD never creates one, so it failed on every issue.
    """
    identity = Identity(id=uuid4())
    for _ in range(5):
        await store.issue(identity, ttl=EXPIRES_FAST)
    live = await store.issue(identity)
    await anyio.sleep(SETTLE)

    index = store._subjects.apply(str(identity.id))
    assert await store._client.scard(index) == 6

    assert await store.purge_expired() == 5
    assert await store._client.scard(index) == 1
    assert await store.resolve(live.plaintext) is not None


async def test_purge_expired_survives_a_foreign_key_in_its_namespace(
    store: RedisTokenStore,
) -> None:
    """One key of the wrong type used to wedge housekeeping permanently.

    SMEMBERS raised WRONGTYPE, the scan aborted, and every index after it in
    cursor order was never swept — on every retry, forever.
    """
    identity = Identity(id=uuid4())
    await store.issue(identity, ttl=EXPIRES_FAST)
    await anyio.sleep(SETTLE)
    await store._client.set(store._subjects.apply("not-a-set"), b"intruder")

    assert await store.purge_expired() == 1
    assert await store._client.scard(store._subjects.apply(str(identity.id))) == 0


async def test_revoke_subject_does_not_count_an_expired_token(
    store: RedisTokenStore,
) -> None:
    """The record's Redis TTL is rounded up, so it briefly outlives its own expiry.

    Counting key presence rather than liveness made the two drivers disagree
    about how many tokens a sign-out actually ended.
    """
    identity = Identity(id=uuid4())
    await store.issue(identity, ttl=EXPIRES_FAST)
    await store.issue(identity)
    await anyio.sleep(SETTLE)

    assert await store.revoke_subject(identity.id) == 1


async def test_an_empty_namespace_is_refused(redis_url: str) -> None:
    """``child()`` would produce a plausible-looking prefix and bypass the guard.

    ``purge_expired`` would then sweep every ``subject:*`` key on the server.
    """
    client = Redis.from_url(redis_url)
    try:
        with pytest.raises(ConfigurationError, match="non-empty namespace"):
            RedisTokenStore(client, namespace=KeyNamespace(""))
    finally:
        await client.aclose()


async def test_closing_does_not_close_a_client_it_was_handed(
    redis_url: str, namespace_suffix: str
) -> None:
    """A client shared with the cache must survive the token store shutting down."""
    client = Redis.from_url(redis_url)
    store = RedisTokenStore(client, namespace=KeyNamespace(f"shared-{namespace_suffix}"))
    try:
        await store.close()
        assert await client.ping()
    finally:
        await client.aclose()
