"""The auth subsystem.

Phase 4: who the caller is, how a password is stored, how a bearer token is
issued and revoked, and whether the caller may do this to this thing. Guards are
declined; see ``docs/adr/0007-identity-and-tokens.md``.

    from keel.auth import Identity, acting_as, current_identity

    with acting_as(Identity(id=user_id, roles=frozenset({"admin"}))):
        ...  # audit columns and policies can now find the caller

Authorization is a rule per resource type, asked through one function:

    register_policy(Account, account_policy)      # at start-up
    authorize("update", Account(user_id))         # raises AuthorizationDeniedError

Tokens are wired like every other subsystem, with one context manager:

    async with token_lifespan(TokenConfig.from_env()):
        issued = await issue_token(identity, label="web")
        identity = await resolve_token(issued.plaintext)

Four parts that deliberately do not depend on each other.
:mod:`keel.auth.identity` and :mod:`keel.auth.policies` are pure Python and
always available, because audit columns and authorization need them whether or
not the service has passwords at all — and because a worker authorizes too: a
job acting for someone asks the same question a route does.

Hashing needs pwdlib, which is the ``auth`` extra, so it is exported lazily
through ``__getattr__`` (PEP 562) — an eager import would make ``import
keel.auth`` fail for a service that authenticates by token and stores no
password. Tokens are lazy for the weaker but still real reason that a service
using only ``acting_as`` should not import a store, a factory and a Redis
adapter to get it. The names stay declared under ``TYPE_CHECKING`` so editors
and both type checkers resolve them normally.

``token_lifespan`` therefore lives in :mod:`keel.auth.binding` rather than here,
where ``queue_lifespan`` sits in its package's ``__init__``: defining it here
would mean importing :class:`~keel.auth.manager.TokenManager` eagerly and undoing
the laziness above.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Final

from keel.auth.identity import Identity, acting_as, current_identity, require_identity
from keel.auth.policies import (
    Policy,
    PolicyRegistry,
    allows,
    authorize,
    policy_registry,
    register_policy,
    use_policies,
)

# -- lazily exported (rationale in the module docstring) ------------------

if TYPE_CHECKING:
    from keel.auth.binding import (
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
    from keel.auth.config import (
        DEFAULT_TOKEN_PREFIX,
        DEFAULT_TOKEN_TTL,
        MINIMUM_MEMORY_COST,
        HashingConfig,
        TokenConfig,
    )
    from keel.auth.fake import FakeTokenStore, TokenAssertionError
    from keel.auth.hashing import (
        PasswordHasher,
        bound_password_hasher,
        hash_password,
        hashing_lifespan,
        password_hasher,
        set_password_hasher,
        use_password_hasher,
        verify_password,
    )
    from keel.auth.manager import TokenManager
    from keel.auth.tokens import IssuedToken, TokenRecord
    from keel.contracts.auth import TokenStore

_LAZY: Final[dict[str, str]] = {
    "FakeTokenStore": "keel.auth.fake",
    "DEFAULT_TOKEN_PREFIX": "keel.auth.config",
    "DEFAULT_TOKEN_TTL": "keel.auth.config",
    "MINIMUM_MEMORY_COST": "keel.auth.config",
    "HashingConfig": "keel.auth.config",
    "IssuedToken": "keel.auth.tokens",
    "PasswordHasher": "keel.auth.hashing",
    "TokenAssertionError": "keel.auth.fake",
    "TokenConfig": "keel.auth.config",
    "TokenManager": "keel.auth.manager",
    "TokenRecord": "keel.auth.tokens",
    "TokenStore": "keel.contracts.auth",
    "bound_password_hasher": "keel.auth.hashing",
    "bound_token_manager": "keel.auth.binding",
    "hash_password": "keel.auth.hashing",
    "hashing_lifespan": "keel.auth.hashing",
    "issue_token": "keel.auth.binding",
    "password_hasher": "keel.auth.hashing",
    "resolve_token": "keel.auth.binding",
    "revoke_token": "keel.auth.binding",
    "set_password_hasher": "keel.auth.hashing",
    "set_token_manager": "keel.auth.binding",
    "token_lifespan": "keel.auth.binding",
    "token_manager": "keel.auth.binding",
    "token_store": "keel.auth.binding",
    "use_password_hasher": "keel.auth.hashing",
    "use_token_manager": "keel.auth.binding",
    "verify_password": "keel.auth.hashing",
}


def __getattr__(name: str) -> Any:
    """Resolve a lazily exported name on first access.

    Args:
        name: The attribute being looked up.

    Returns:
        The object from its defining module.

    Raises:
        AttributeError: If the name is not exported by this package.
    """
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(import_module(module), name)


def __dir__() -> list[str]:
    """List eager and lazy names together, so tab completion sees both.

    Returns:
        Every exported name.
    """
    return sorted(__all__)


__all__ = [
    "DEFAULT_TOKEN_PREFIX",
    "DEFAULT_TOKEN_TTL",
    "MINIMUM_MEMORY_COST",
    "FakeTokenStore",
    "HashingConfig",
    "Identity",
    "IssuedToken",
    "PasswordHasher",
    "Policy",
    "PolicyRegistry",
    "TokenAssertionError",
    "TokenConfig",
    "TokenManager",
    "TokenRecord",
    "TokenStore",
    "acting_as",
    "allows",
    "authorize",
    "bound_password_hasher",
    "bound_token_manager",
    "current_identity",
    "hash_password",
    "hashing_lifespan",
    "issue_token",
    "password_hasher",
    "policy_registry",
    "register_policy",
    "require_identity",
    "resolve_token",
    "revoke_token",
    "set_password_hasher",
    "set_token_manager",
    "token_lifespan",
    "token_manager",
    "token_store",
    "use_password_hasher",
    "use_policies",
    "use_token_manager",
    "verify_password",
]
