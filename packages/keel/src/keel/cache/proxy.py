"""The module-level cache facade and the binding behind it.

``from keel.cache import cache`` gives application code a cache it can call
without threading a manager through every constructor. That convenience is
usually paid for with a global and untestable code; here it is paid for with a
Proxy.

:class:`CacheProxy` subclasses :class:`~keel.cache.repository.Repository` and
overrides only its *properties* — every one of them, which is the invariant this
rests on. Because each inherited method reads its state through a property
rather than the attribute behind it, all twenty methods delegate correctly to
whatever repository is currently bound, with no forwarding code and no loss of
type information.

Adding a *method* to ``Repository`` therefore requires no change here. Adding
*state* does: it needs a property on ``Repository`` and an override on this
class, or the first call through the facade raises ``AttributeError``.

Binding is delegated to :class:`~keel.support.binding.Binding`, which every
subsystem shares — see that module for why the process-wide layer is a plain
attribute and the override is a ``ContextVar``.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager

from keel.cache.manager import CacheManager
from keel.cache.repository import Repository
from keel.contracts.cache import Store
from keel.exceptions import ConfigurationError
from keel.support.binding import Binding

_binding: Binding[CacheManager] = Binding(
    "cache manager",
    "call keel.cache.set_cache_manager(...) during startup, "
    "or use keel.cache.use_cache(...) in a test",
)


def set_cache_manager(manager: CacheManager | None) -> None:
    """Install the process-wide cache manager.

    Call this once during application startup.

    Args:
        manager: The manager to install, or ``None`` to unbind.
    """
    _binding.set(manager)


def _bound_cache_manager() -> CacheManager | None:
    """Return the process-wide manager without raising when none is bound.

    Internal: used by :func:`~keel.cache.cache_lifespan` so it can restore a
    previous binding on exit instead of unbinding.

    Returns:
        The bound manager, or ``None``.
    """
    return _binding.peek()


def current_cache_manager() -> CacheManager:
    """Return the manager currently in effect.

    Returns:
        The context-local override if one is active, otherwise the process-wide
        manager.

    Raises:
        ConfigurationError: If no manager has been bound.
    """
    return _binding.current()


def use_cache(manager: CacheManager) -> AbstractContextManager[CacheManager]:
    """Override the bound cache manager for the duration of the block.

    Args:
        manager: The manager to use.

    Returns:
        A context manager yielding the same manager.
    """
    return _binding.use(manager)


class CacheProxy(Repository):
    """A repository that resolves its backing store on every call.

    Args:
        resolver: Returns the repository to delegate to. Called per operation,
            which is what makes a rebind take effect immediately rather than at
            the next restart.
    """

    __slots__ = ("_resolver",)

    def __init__(self, resolver: Callable[[], Repository]) -> None:
        # Deliberately does not call super().__init__: this class holds no
        # state of its own. That only works while EVERY piece of Repository's
        # state is reachable through a property this class overrides — adding a
        # plain attribute to Repository and reading it as `self._x` inside a
        # method breaks the proxy with an AttributeError at runtime. Adding
        # state to Repository means adding a property here.
        self._resolver = resolver

    @property
    def subject(self) -> Repository:
        """The repository this proxy currently stands in for."""
        return self._resolver()

    @property
    def store(self) -> Store:
        """The currently bound store."""
        return self.subject.store

    @property
    def default_ttl(self) -> float | None:
        """The currently bound store's default lifetime."""
        return self.subject.default_ttl

    @property
    def name(self) -> str:
        """The currently bound store's configured name."""
        return self.subject.name

    @property
    def single_flight_ttl(self) -> float:
        """Delegated, like every other piece of state this class stands in for."""
        return self.subject.single_flight_ttl

    @property
    def single_flight_timeout(self) -> float:
        """Delegated to the bound repository."""
        return self.subject.single_flight_timeout

    @property
    def single_flight_poll(self) -> float:
        """Delegated to the bound repository."""
        return self.subject.single_flight_poll

    def of(self, name: str) -> Repository:
        """Return a specific named store rather than the default.

        Args:
            name: The store name.

        Returns:
            That store's repository, resolved now.
        """
        return current_cache_manager().store(name)

    def __repr__(self) -> str:
        """Show what the proxy resolves to, or that it is unbound."""
        try:
            return f"<CacheProxy -> {self.subject!r}>"
        except ConfigurationError:
            return "<CacheProxy unbound>"


cache = CacheProxy(lambda: current_cache_manager().store())
"""The default cache store, resolved fresh on every call.

Safe to import at module scope before anything is configured: nothing is
resolved until an operation actually runs.
"""
