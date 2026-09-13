"""Whether the caller may do this to this thing.

Authentication answers *who is calling*; this answers *may they*. The two are
kept apart because they fail differently — a missing credential is a 401 the
edge raises before any service runs, a refused action is a 403 raised wherever
the rule lives, and a missing *binding* is a 500. :mod:`keel.exceptions` keeps
three classes for the three, and this module raises exactly one of them.

**Pattern: Strategy, selected by the resource's type.** "Who may do what" is the
algorithm that varies, and what it varies with is the kind of thing being acted
on: an account has one rule, a document has another. :class:`PolicyRegistry` is
the Strategy's context and nothing more — a dict keyed by type, with the lookup
failing closed.

**Pattern: Virtual Proxy.** :func:`authorize` and :func:`allows` resolve the
bound registry per call rather than capturing one, the same role
:func:`keel.queue.dispatch.dispatch` plays, so a policy registered after a
module was imported still reaches the check.

**A policy is a function, not an object.** It holds no state and has one
operation, so a class would be a function wearing a constructor — the counter
-rule in ADR 0000. An application that wants a class writes ``__call__``; the
registry cannot tell the difference and does not care.

**Actions are strings, and the resource is typed.** Laravel puts a method per
ability on a policy class. In Python that is ``getattr(policy, action)`` — the
string is still a string, it is now dispatched dynamically over an object's
whole attribute surface, and every ability has to be whitelisted to stop that
reaching somewhere it should not. A single callable with a ``match`` puts every
ability of a resource in one block whose ``case _`` arm is deny-by-default, and
:meth:`PolicyRegistry.register` still checks statically that the policy accepts
the resource type it is registered for. The typing that was worth having is
kept; the dynamic dispatch is not.

**Synchronous, deliberately.** :class:`~keel.auth.Identity` carries ``roles``
and ``claims`` precisely so an authorization check costs no query (ADR 0007,
decision 1). An ``async`` policy would invite one, on a path that frequently has
no transaction open — and a rule that genuinely needs a row belongs where the
row already is, with the loaded object passed in as the resource.

Declined, deliberately:

* **Chain of Responsibility.** The open question ADR 0007 left. A chain earns
  its place when a rule must run for *every* resource before or after its own
  policy — a super-admin bypass, a tenant check, a suspended-account gate. There
  is no such rule, and a chain of one link is the ceremony ADR 0006 already
  refused for job middleware. When the first one arrives, the honest shape is a
  chain whose middle link is this registry.
* **A Manager and a driver seam.** Every other subsystem has one because it has
  a backend to swap. A policy registry is a dict; there is nothing to configure,
  nothing to close, no second implementation and therefore no contract suite.
* **A Null Object default policy.** A permissive one is an authorization hole
  with a pattern name, and a denying one turns a forgotten registration into a
  403 nobody pages on. An unregistered type raises instead; see
  :meth:`PolicyRegistry.policy_for`.
* **Putting :data:`Policy` in :mod:`keel.contracts`.** That package is the
  driver seam. A policy has no backend and is written by the application, so it
  belongs beside the registry that calls it.
* **An ``identity=`` argument on :func:`authorize`.** Asking "what could someone
  else do" is ``with acting_as(them): allows(...)``, which already exists and
  reads better than a parameter whose ``None`` would collide with *explicitly
  nobody*.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from keel.auth.identity import Identity, current_identity
from keel.exceptions import AuthorizationDeniedError, ConfigurationError
from keel.support.binding import Binding

type Policy = Callable[[Identity | None, str, Any], bool]
"""A rule for one kind of resource: ``(identity, action, resource) -> bool``.

The identity is ``Identity | None`` rather than ``Identity`` because there is no
guest object (ADR 0007, decision 1) and because the annotation is what makes
both type checkers refuse ``identity.id`` until the author has said what an
unauthenticated caller gets. Passing ``None`` through to the policy rather than
denying in the registry is what keeps "anyone may read a published post"
expressible; the type system, not a special case, is what stops it being
forgotten.

The resource is ``Any`` and not ``object`` on purpose: the registry has already
matched on its type, so a policy annotated ``resource: Document`` is stating a
fact rather than making a claim, and ``object`` would force every one of them to
re-narrow what the lookup just proved.
"""


class PolicyRegistry:
    """The policies in effect, keyed by the type of thing they judge.

    A class rather than a module-level dict so a test can hold an isolated one
    and a process serving several tenants can hold one each — the same reason
    every other subsystem's state is an object.

    Lookup is by **exact type**, not by walking the MRO. A subclass that
    inherited its parent's rule would acquire a permission nobody granted it,
    and the failure would be silent; refusing to guess makes it loud.
    """

    __slots__ = ("_policies",)

    def __init__(self) -> None:
        self._policies: dict[type[Any], Policy] = {}

    def register[T](
        self,
        resource_type: type[T],
        policy: Callable[[Identity | None, str, T], bool],
    ) -> None:
        """Install the rule for one kind of resource.

        Generic so that registration is the one place the resource type is known
        statically and can be checked: handing ``register(Document, ...)`` a
        policy that expects an ``Account`` is a type error here rather than an
        ``AttributeError`` inside a request.

        Args:
            resource_type: The exact class :func:`authorize` will match on.
            policy: The rule. Returning a falsey value denies, which is what
                makes a policy that forgets to ``return`` fail safe.

        Raises:
            ConfigurationError: If a policy is already registered for the type.
                Silently replacing one means two registrations disagreeing and
                the loser being a permission nobody can find.
        """
        if resource_type in self._policies:
            raise ConfigurationError(
                f"a policy is already registered for {resource_type.__qualname__}; "
                f"replacing one silently is how two rules end up disagreeing"
            )
        self._policies[resource_type] = policy

    def policy_for(self, resource_type: type[Any]) -> Policy:
        """Return the rule for *resource_type*, refusing to invent one.

        Args:
            resource_type: The class to look up.

        Returns:
            The registered policy.

        Raises:
            ConfigurationError: If nothing is registered. Deny-by-default with a
                *bool* was the obvious alternative and is weaker: a forgotten
                registration would answer 403, land in the bucket nobody pages
                on, and read as ordinary permission noise. This refuses the
                request too — nothing is ever permitted by omission — but it
                refuses it as the wiring bug it is. ADR 0007, section 8, applied
                to the other half of auth.
        """
        policy = self._policies.get(resource_type)
        if policy is None:
            raise ConfigurationError(
                f"no policy is registered for {resource_type.__qualname__}; "
                f"call keel.auth.register_policy({resource_type.__name__}, ...) at "
                f"start-up. A resource with no policy is never permitted by omission"
            )
        return policy

    def allows(self, identity: Identity | None, action: str, resource: object) -> bool:
        """Ask this registry's rule whether *identity* may *action* *resource*.

        Args:
            identity: The principal, or ``None`` for an unauthenticated caller.
            action: The ability being asked about.
            resource: What it would be done to.

        Returns:
            Whether the rule permits it. Coerced with ``bool`` so an untyped
            policy returning a truthy set of matching roles still means what it
            reads as.

        Raises:
            ConfigurationError: If no policy is registered for the resource.
        """
        return bool(self.policy_for(type(resource))(identity, action, resource))


_binding: Binding[PolicyRegistry] = Binding(
    "policy registry",
    "importing keel.auth installs one, so reaching this means it was unbound "
    "deliberately; call keel.auth.use_policies() around the work instead",
)
_binding.set(PolicyRegistry())
"""Pre-installed, unlike every other subsystem's binding.

The others hold something with a configuration and a connection pool, so
refusing to work until a lifespan has run is the right failure. A registry is an
empty dict — there is nothing to configure and nothing to close, and demanding a
lifespan for one would be a wiring step whose only outcome is forgetting it.
Registering a policy at import is the whole of the setup, and the override layer
is still a ``ContextVar`` so concurrent tests cannot see each other's rules.
"""


def policy_registry() -> PolicyRegistry:
    """Return the registry currently in effect.

    Returns:
        The context-local override if one is active, otherwise the process-wide
        registry.
    """
    return _binding.current()


def register_policy[T](
    resource_type: type[T],
    policy: Callable[[Identity | None, str, T], bool],
) -> None:
    """Install a rule on the registry currently in effect.

    Args:
        resource_type: The exact class :func:`authorize` will match on.
        policy: The rule.

    Raises:
        ConfigurationError: If a policy is already registered for the type.
    """
    policy_registry().register(resource_type, policy)


@contextmanager
def use_policies(registry: PolicyRegistry | None = None) -> Iterator[PolicyRegistry]:
    """Swap the registry for the duration of a block.

    The test seam, and the reason no fake is needed: an isolated registry with
    the two rules a test cares about is more honest than a double that answers
    yes, which is a permission left switched on if it ever escapes the suite.

    **Not for a Starlette lifespan.** The override is a ``ContextVar``, and a
    lifespan runs in a different task from the request handlers, so one entered
    there is invisible where it matters — the bug
    :class:`~keel.support.binding.Binding` exists to document. An application
    installs its rules by calling :func:`register_policy` at import.

    Args:
        registry: The registry to use. Defaults to a fresh empty one, which is
            what a test wanting to register its own wants.

    Yields:
        The registry now in effect, so a test can register into it.
    """
    with _binding.use(registry if registry is not None else PolicyRegistry()) as bound:
        yield bound


def allows(action: str, resource: object) -> bool:
    """Whether the current caller may *action* *resource*.

    The identity comes from :func:`~keel.auth.current_identity`, never from an
    argument: that context variable exists so the same check works unchanged in
    a request, in a job running under ``acting_as``, and in a management
    command, and threading a principal through every signature between the edge
    and the rule is the thing ADR 0007 declined to do.

    Args:
        action: The ability being asked about.
        resource: What it would be done to.

    Returns:
        Whether it is permitted.

    Raises:
        ConfigurationError: If no policy is registered for the resource's type.
    """
    return policy_registry().allows(current_identity(), action, resource)


def authorize(action: str, resource: object) -> None:
    """Refuse the current caller unless they may *action* *resource*.

    The form to reach for. ``if not allows(...): raise`` written by hand is one
    forgotten ``not`` away from inverting a permission, and the two spellings
    read almost identically in review.

    Args:
        action: The ability being asked about.
        resource: What it would be done to.

    Raises:
        AuthorizationDeniedError: If the rule refuses. An edge maps this to 403.
        ConfigurationError: If no policy is registered for the resource's type.
    """
    identity = current_identity()
    if not policy_registry().allows(identity, action, resource):
        raise AuthorizationDeniedError(
            action, resource, subject=None if identity is None else identity.id
        )


__all__ = [
    "Policy",
    "PolicyRegistry",
    "allows",
    "authorize",
    "policy_registry",
    "register_policy",
    "use_policies",
]
