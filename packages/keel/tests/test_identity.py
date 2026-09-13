"""The current-identity context.

The claims worth pinning are the ones that are cheap to break silently: that
nobody is authenticated by default, that leaving a block restores what was there
before even on an exception path, and that two concurrent tasks cannot see each
other's caller. The last is the whole reason this is a ContextVar rather than a
module global, and it is the one a passing single-threaded test would hide.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from keel.auth import Identity, acting_as, current_identity, require_identity
from keel.exceptions import AuthenticationRequiredError

pytestmark = pytest.mark.anyio


def test_nobody_is_authenticated_by_default() -> None:
    """Unauthenticated is an ordinary state, not an error."""
    assert current_identity() is None


def test_require_identity_refuses_to_guess() -> None:
    """Code whose correctness needs a caller gets an error, not ``None``."""
    with pytest.raises(AuthenticationRequiredError, match="no identity is bound"):
        require_identity()


def test_the_error_says_what_to_do_about_it() -> None:
    """Half an error message is the half that names the fix."""
    with pytest.raises(AuthenticationRequiredError, match="acting_as"):
        require_identity()


def test_acting_as_binds_for_the_block_only() -> None:
    """The binding is scoped, so nothing leaks into what the task does next."""
    alice = Identity(id=uuid4())
    with acting_as(alice):
        assert current_identity() is alice
        assert require_identity() is alice
    assert current_identity() is None


def test_leaving_the_block_restores_the_outer_identity() -> None:
    """Nesting is real: a job acting as one principal inside a request for another."""
    outer, inner = Identity(id=uuid4()), Identity(id=uuid4())
    with acting_as(outer):
        with acting_as(inner):
            assert current_identity() is inner
        assert current_identity() is outer


def test_an_exception_still_restores_the_previous_identity() -> None:
    """The path people forget. A dropped token would leave a stale caller bound."""
    outer = Identity(id=uuid4())
    with acting_as(outer):
        with pytest.raises(RuntimeError), acting_as(Identity(id=uuid4())):
            raise RuntimeError("boom")
        assert current_identity() is outer


def test_acting_as_none_drops_the_caller() -> None:
    """How a job says it must not inherit whoever dispatched it."""
    with acting_as(Identity(id=uuid4())), acting_as(None):
        assert current_identity() is None


async def test_concurrent_tasks_cannot_see_each_others_caller() -> None:
    """The reason this is a ContextVar. A global would make this flap under load."""
    seen: dict[str, Identity | None] = {}

    async def act(name: str, identity: Identity) -> None:
        with acting_as(identity):
            await asyncio.sleep(0)  # yield, so the tasks interleave
            seen[name] = current_identity()

    alice, bob = Identity(id=uuid4()), Identity(id=uuid4())
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(act("alice", alice))
        tasks.create_task(act("bob", bob))

    assert seen == {"alice": alice, "bob": bob}
    assert current_identity() is None


def test_an_identity_cannot_be_edited_in_place() -> None:
    """Frozen, so a handler cannot change who it is acting as mid-request."""
    with pytest.raises(AttributeError):
        Identity(id=uuid4()).id = uuid4()  # type: ignore[misc]  # ty: ignore[invalid-assignment]


def test_has_role_asks_a_union() -> None:
    """ "editor or admin" is the question that actually gets asked."""
    identity = Identity(id=uuid4(), roles=frozenset({"editor"}))
    assert identity.has_role("editor")
    assert identity.has_role("admin", "editor")
    assert not identity.has_role("admin")


def test_an_empty_question_is_not_a_satisfied_one() -> None:
    """``has_role()`` must not be a way to accidentally authorize everything."""
    assert not Identity(id=uuid4(), roles=frozenset({"admin"})).has_role()


def test_an_identity_carries_no_roles_unless_given_some() -> None:
    """The safe default: authenticated is not authorized."""
    assert Identity(id=uuid4()).roles == frozenset()
