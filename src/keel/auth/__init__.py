"""The auth subsystem.

Phase 4, first slice: who the caller is, and how a password is stored. Guards,
tokens and policies come next; see ``docs/roadmap.md``.

    from keel.auth import Identity, acting_as, current_identity

    with acting_as(Identity(id=user_id, roles=frozenset({"admin"}))):
        ...  # audit columns and policies can now find the caller

Two halves that deliberately do not depend on each other. :mod:`keel.auth.identity`
is pure Python and always available, because audit columns and authorization need
it whether or not the service has passwords at all. Hashing needs pwdlib, which is
the ``auth`` extra, so it is exported lazily through ``__getattr__`` (PEP 562) —
an eager import would make ``import keel.auth`` fail for a service that
authenticates by token and stores no password. The names stay declared under
``TYPE_CHECKING`` so editors and both type checkers resolve them normally.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Final

from keel.auth.identity import Identity, acting_as, current_identity, require_identity

# -- lazily exported (rationale in the module docstring) ------------------

if TYPE_CHECKING:
    from keel.auth.config import HashingConfig
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

_LAZY: Final[dict[str, str]] = {
    "HashingConfig": "keel.auth.config",
    "PasswordHasher": "keel.auth.hashing",
    "bound_password_hasher": "keel.auth.hashing",
    "hash_password": "keel.auth.hashing",
    "hashing_lifespan": "keel.auth.hashing",
    "password_hasher": "keel.auth.hashing",
    "set_password_hasher": "keel.auth.hashing",
    "use_password_hasher": "keel.auth.hashing",
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
    "HashingConfig",
    "Identity",
    "PasswordHasher",
    "acting_as",
    "bound_password_hasher",
    "current_identity",
    "hash_password",
    "hashing_lifespan",
    "password_hasher",
    "require_identity",
    "set_password_hasher",
    "use_password_hasher",
    "verify_password",
]
