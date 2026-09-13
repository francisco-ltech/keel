"""Where the process finds its token store.

The binding lives here rather than in :mod:`keel.auth.manager` for the reason the
queue puts its own in :mod:`keel.queue.dispatch`: the manager is a factory, and a
factory that also owns a process-wide mutable slot is two responsibilities in one
import. Keeping them apart means a service that wires managers explicitly — a
worker holding one per tenant, say — can import the factory without also
importing the global.

Two layers, both from :class:`~keel.support.binding.Binding`: a plain attribute
for the process-wide default, because Starlette runs a lifespan in a different
task from its handlers and a ``ContextVar`` set there is invisible to them; and a
``ContextVar`` override so concurrent tests cannot see each other's store.

The three convenience functions at the bottom are a **Virtual Proxy** over the
default store — the same role :func:`keel.queue.dispatch.dispatch` plays. They
resolve the binding per call rather than capturing it, so a token issued from a
module imported at start-up still reaches the store a test swapped in. They
deliberately reach only the *default* store; :func:`token_store` is how a service
with more than one names the other.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager

from keel.auth.config import TokenConfig
from keel.auth.identity import Identity
from keel.auth.manager import TokenManager
from keel.auth.tokens import IssuedToken
from keel.contracts.auth import TokenStore
from keel.support.binding import Binding

_binding: Binding[TokenManager] = Binding(
    "token manager",
    "call keel.auth.set_token_manager(TokenManager(TokenConfig.from_env())) during "
    "startup, wrap the work in `async with token_lifespan(config)`, or use "
    "keel.testing.fake_tokens() in a test",
)


def token_manager() -> TokenManager:
    """Return the manager currently in effect.

    Returns:
        The context-local override if one is active, otherwise the process-wide
        manager.

    Raises:
        ConfigurationError: If nothing is bound.
    """
    return _binding.current()


def bound_token_manager() -> TokenManager | None:
    """Return the process-wide manager without raising when none is bound.

    Returns:
        The bound manager, or ``None``. Used by the lifespan to restore a
        previous binding rather than unbinding.
    """
    return _binding.peek()


def set_token_manager(manager: TokenManager | None) -> None:
    """Install the process-wide token manager.

    Args:
        manager: The manager to install, or ``None`` to unbind.
    """
    _binding.set(manager)


@contextmanager
def use_token_manager(manager: TokenManager) -> Iterator[TokenManager]:
    """Override the bound token manager for the duration of a block.

    Args:
        manager: The manager to use.

    Yields:
        The manager now in effect.
    """
    with _binding.use(manager) as bound:
        yield bound


def token_store(name: str | None = None) -> TokenStore:
    """Return a token store from the bound manager.

    Args:
        name: The driver name, or ``None`` for the configured default.

    Returns:
        The store.

    Raises:
        ConfigurationError: If no manager is bound, or the driver is unknown.
    """
    return token_manager().store(name)


@asynccontextmanager
async def token_lifespan(config: TokenConfig) -> AsyncIterator[TokenManager]:
    """Bind a token manager for the life of the process.

    Framework-agnostic, like the cache's, the queue's and the database's: it
    drops into a FastAPI ``lifespan``, a worker's main coroutine or a script
    unchanged.

    Restores whatever was bound before rather than unbinding, so a test lifespan
    nested inside an application lifespan does not silently kill the outer one.

    Args:
        config: Where tokens live and how long they last.

    Yields:
        The bound manager.
    """
    previous = bound_token_manager()
    manager = TokenManager(config)
    set_token_manager(manager)
    try:
        yield manager
    finally:
        await manager.close()
        set_token_manager(previous)


async def issue_token(
    identity: Identity,
    *,
    ttl: float | None = None,
    label: str | None = None,
) -> IssuedToken:
    """Mint a token on the default store.

    Args:
        identity: The principal the token authenticates.
        ttl: Lifetime in seconds; the store's configured default when omitted.
        label: What a "signed-in devices" listing shows.

    Returns:
        The token. Its plaintext is readable exactly once, here.

    Raises:
        ConfigurationError: If no manager is bound.
    """
    return await token_store().issue(identity, ttl=ttl, label=label)


async def resolve_token(plaintext: str) -> Identity | None:
    """Return the principal a token authenticates, using the default store.

    Args:
        plaintext: The token as the caller presented it.

    Returns:
        The identity, or ``None`` if the token is unknown, expired or revoked.

    Raises:
        ConfigurationError: If no manager is bound.
    """
    return await token_store().resolve(plaintext)


async def revoke_token(plaintext: str) -> bool:
    """Invalidate one token on the default store.

    Args:
        plaintext: The token as the caller presented it.

    Returns:
        Whether a live token was removed.

    Raises:
        ConfigurationError: If no manager is bound.
    """
    return await token_store().revoke(plaintext)


__all__ = [
    "bound_token_manager",
    "issue_token",
    "resolve_token",
    "revoke_token",
    "set_token_manager",
    "token_lifespan",
    "token_manager",
    "token_store",
    "use_token_manager",
]
