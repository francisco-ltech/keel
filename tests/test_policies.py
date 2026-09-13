"""Authorization policies.

**There is no parametrised contract suite here, and that is not an omission.**
Every other subsystem has one because it has several drivers answering the same
protocol, and Liskov is treated as testable rather than aspirational. There is
exactly one :class:`~keel.auth.PolicyRegistry` and there is no second one to
write: it has no backend, no configuration and nothing to swap. Manufacturing a
second implementation to parametrise over would be testing a fixture.

What is worth pinning instead is the failure direction. Every claim below is
some version of *nothing is permitted by accident*: not by a missing
registration, not by a missing caller, not by a subclass inheriting a rule, and
not by one test's registry leaking into another's.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest

from keel.auth import (
    Identity,
    PolicyRegistry,
    acting_as,
    allows,
    authorize,
    policy_registry,
    register_policy,
    use_policies,
)
from keel.exceptions import (
    AuthenticationRequiredError,
    AuthorizationDeniedError,
    ConfigurationError,
)

pytestmark = pytest.mark.anyio


@dataclass(frozen=True, slots=True)
class Account:
    """A resource identified by whose it is, with no row behind it.

    The shape the template's routers use: FastAPI hands a handler a path
    parameter, not an ORM object, because ADR 0002 forbids holding a session
    across a request. So the thing a policy judges at the edge is a reference.
    """

    owner_id: UUID


@dataclass(frozen=True, slots=True)
class Document:
    """A second resource type, so "selected by type" is actually exercised."""

    owner_id: UUID


class PrivateAccount(Account):
    """A subclass, used only to prove that a rule is not inherited."""


def account_policy(identity: Identity | None, action: str, resource: Account) -> bool:
    """Own it, or be an admin. The rule the template's two domains both use."""
    if identity is None:
        return False
    if identity.has_role("admin"):
        return True
    return action in {"view", "update", "delete"} and identity.id == resource.owner_id


def document_policy(identity: Identity | None, action: str, resource: Document) -> bool:
    """Anyone may read; only the owner may write. Needs ``None`` to reach it."""
    if action == "view":
        return True
    return identity is not None and identity.id == resource.owner_id


@pytest.fixture
def registry() -> Iterator[PolicyRegistry]:
    """An isolated registry with both rules, replacing the process-wide one."""
    with use_policies() as fresh:
        fresh.register(Account, account_policy)
        fresh.register(Document, document_policy)
        yield fresh


def test_an_unregistered_resource_is_never_permitted() -> None:
    """The claim the whole module exists to make.

    A bool would have been the obvious deny-by-default and is weaker: a
    forgotten registration answering 403 is indistinguishable from a real
    refusal, in the status bucket nobody pages on.
    """
    with use_policies():
        with pytest.raises(ConfigurationError, match="no policy is registered"):
            allows("view", Account(owner_id=uuid4()))
        with pytest.raises(ConfigurationError, match="no policy is registered"):
            authorize("view", Account(owner_id=uuid4()))


def test_the_missing_registration_error_says_what_to_register() -> None:
    """Half an error message is the half that names the fix."""
    with use_policies(), pytest.raises(ConfigurationError, match=r"register_policy\(Account"):
        allows("view", Account(owner_id=uuid4()))


def test_the_owner_may_act_and_a_stranger_may_not(registry: PolicyRegistry) -> None:
    owner, stranger = Identity(id=uuid4()), Identity(id=uuid4())
    account = Account(owner_id=owner.id)

    with acting_as(owner):
        assert allows("update", account)
    with acting_as(stranger):
        assert not allows("update", account)


def test_a_role_can_override_ownership(registry: PolicyRegistry) -> None:
    """The rule `Owner` could not express: an admin may act on anybody's."""
    admin = Identity(id=uuid4(), roles=frozenset({"admin"}))

    with acting_as(admin):
        assert allows("delete", Account(owner_id=uuid4()))


def test_an_unknown_action_is_refused_even_for_the_owner(registry: PolicyRegistry) -> None:
    """The `case _` arm. A policy answers about the abilities it names, only."""
    owner = Identity(id=uuid4())

    with acting_as(owner):
        assert not allows("transfer", Account(owner_id=owner.id))


def test_nobody_authenticated_reaches_the_policy_as_none(registry: PolicyRegistry) -> None:
    """`None` is passed through rather than short-circuited in the registry.

    Denying centrally would be safe and would also make "anyone may read a
    published document" unexpressible. The annotation ``Identity | None`` is
    what stops an author forgetting the case — both type checkers refuse
    ``identity.id`` until it is handled.
    """
    assert allows("view", Document(owner_id=uuid4()))
    assert not allows("update", Document(owner_id=uuid4()))


def test_the_rule_is_selected_by_the_resources_type(registry: PolicyRegistry) -> None:
    """Strategy. Two resources, the same action, two different answers."""
    caller = Identity(id=uuid4())
    someone_else = uuid4()

    with acting_as(caller):
        assert not allows("view", Account(owner_id=someone_else))
        assert allows("view", Document(owner_id=someone_else))


def test_a_subclass_does_not_inherit_its_parents_rule(registry: PolicyRegistry) -> None:
    """Lookup is by exact type: a permission is granted, never inherited."""
    owner = Identity(id=uuid4())

    with acting_as(owner), pytest.raises(ConfigurationError, match="PrivateAccount"):
        allows("view", PrivateAccount(owner_id=owner.id))


def test_registering_twice_for_one_type_is_refused(registry: PolicyRegistry) -> None:
    """Two rules for one resource means one of them is dead and unfindable."""
    with pytest.raises(ConfigurationError, match="already registered"):
        register_policy(Account, account_policy)


def test_authorize_says_nothing_when_it_permits(registry: PolicyRegistry) -> None:
    """Only the refused path raises; the permitted one returns and says nothing."""
    owner = Identity(id=uuid4())

    with acting_as(owner):
        authorize("update", Account(owner_id=owner.id))


def test_a_denial_carries_what_an_audit_line_needs(registry: PolicyRegistry) -> None:
    """The action, the kind of thing, and who was refused — and not the object.

    Holding the resource would keep an ORM instance, and its session, alive for
    as long as the exception is referenced.
    """
    stranger = Identity(id=uuid4())

    with acting_as(stranger), pytest.raises(AuthorizationDeniedError) as raised:
        authorize("delete", Account(owner_id=uuid4()))

    assert raised.value.action == "delete"
    assert raised.value.resource_type == "Account"
    assert raised.value.subject == stranger.id
    assert not hasattr(raised.value, "resource")


def test_a_denial_for_nobody_names_nobody(registry: PolicyRegistry) -> None:
    with pytest.raises(AuthorizationDeniedError, match="unauthenticated") as raised:
        authorize("update", Document(owner_id=uuid4()))

    assert raised.value.subject is None


def test_a_denial_is_not_a_missing_binding() -> None:
    """The distinction an edge maps to 403 and 500 respectively.

    Both descend from ``KeelError`` so one ``except`` still catches the
    framework, and neither is an instance of the other — a handler registered
    for one must never catch the other. ADR 0007, section 8.
    """
    denied = AuthorizationDeniedError("update", Account(owner_id=uuid4()))

    assert not isinstance(denied, AuthenticationRequiredError)
    assert not issubclass(AuthenticationRequiredError, AuthorizationDeniedError)


def test_a_policy_that_forgets_to_return_denies() -> None:
    """The likeliest bug in a hand-written rule, and it has to fail closed."""

    def forgetful(identity: Identity | None, action: str, resource: Account) -> bool:
        return None  # type: ignore[return-value]  # ty: ignore[invalid-return-type]

    with use_policies() as fresh:
        fresh.register(Account, forgetful)
        with acting_as(Identity(id=uuid4())):
            assert not allows("view", Account(owner_id=uuid4()))


def test_leaving_the_block_restores_the_previous_registry() -> None:
    """Nesting is real: an application's registry with a test's inside it."""
    outer = PolicyRegistry()
    with use_policies(outer):
        with use_policies() as inner:
            assert policy_registry() is inner
        assert policy_registry() is outer


def test_the_process_wide_registry_is_installed_without_a_lifespan() -> None:
    """Unlike every other binding. A dict has nothing to open or close."""
    assert isinstance(policy_registry(), PolicyRegistry)


async def test_concurrent_tasks_cannot_see_each_others_policies() -> None:
    """The reason the override is a ContextVar and not a module global.

    Two tests registering different rules for the same resource type would
    otherwise collide under xdist, and the one that lost would be a permission
    granted by another test's fixture.
    """
    account = Account(owner_id=uuid4())

    async def answer(permitted: bool) -> bool:
        with use_policies() as fresh:
            fresh.register(Account, lambda identity, action, resource: permitted)
            await asyncio.sleep(0)
            return allows("view", account)

    assert list(await asyncio.gather(answer(True), answer(False))) == [True, False]
