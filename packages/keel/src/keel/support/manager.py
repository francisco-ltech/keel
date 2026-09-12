"""Driver resolution.

Laravel's Manager pattern, which is an Abstract Factory with two additions that
matter in practice: instances are memoised per name, and third parties can
register drivers the framework has never heard of.

This base class is generic and subsystem-agnostic on purpose. The cache is the
first subsystem to use it; the queue, mailer and filesystem will each subclass
it with their own ``_make``. Getting the shape right once is most of why adding
the fourth subsystem should be cheaper than adding the first.

The ``extend`` hook is the Open/Closed principle made concrete: adding a
Memcached store must not require editing :class:`~keel.cache.manager.CacheManager`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator

from keel.exceptions import ConfigurationError


class Manager[T](ABC):
    """Creates and memoises named driver instances.

    Args:
        default: The name resolved when a caller does not ask for one.
    """

    __slots__ = ("_custom", "_default", "_resolved")

    def __init__(self, default: str) -> None:
        self._default = default
        self._resolved: dict[str, T] = {}
        self._custom: dict[str, Callable[[str], T]] = {}

    @property
    def default_name(self) -> str:
        """The name used when a caller does not specify one."""
        return self._default

    @property
    def resolved_names(self) -> Iterator[str]:
        """The names that have actually been instantiated so far."""
        return iter(tuple(self._resolved))

    def extend(self, name: str, factory: Callable[[str], T]) -> None:
        """Register a driver this manager does not know how to build.

        Args:
            name: The configuration name that should map to *factory*.
            factory: Receives the name and returns the driver instance.

        Raises:
            ConfigurationError: If *name* has already been resolved, since
                callers may be holding the old instance and would not see the
                replacement. Register custom drivers during startup.
        """
        if name in self._resolved:
            raise ConfigurationError(
                f"driver {name!r} has already been resolved and cannot be replaced; "
                f"register custom drivers before first use"
            )
        self._custom[name] = factory

    def driver(self, name: str | None = None) -> T:
        """Return the named driver, building it on first use.

        Args:
            name: The driver name, or ``None`` for the default.

        Returns:
            The memoised driver instance.

        Raises:
            ConfigurationError: If the name is not configured.
        """
        resolved_name = name or self._default
        if resolved_name not in self._resolved:
            self._resolved[resolved_name] = self._resolve(resolved_name)
        return self._resolved[resolved_name]

    def _resolve(self, name: str) -> T:
        """Build a driver, preferring a registered custom factory."""
        custom = self._custom.get(name)
        if custom is not None:
            return custom(name)
        return self._make(name)

    @abstractmethod
    def _make(self, name: str) -> T:
        """Build the driver *name* from configuration.

        Args:
            name: The driver name.

        Returns:
            A new driver instance.

        Raises:
            ConfigurationError: If the name is not configured.
        """

    def forget(self, name: str | None = None) -> None:
        """Drop a memoised instance so the next call rebuilds it.

        Warning:
            This does not release the instance's resources — it cannot, because
            it is synchronous and closing is an ``await``. A driver holding a
            connection pool leaks it. Use :meth:`discard` unless you know the
            driver is inert.

        Args:
            name: The driver to forget, or ``None`` for the default.
        """
        self._resolved.pop(name or self._default, None)

    async def discard(self, name: str | None = None) -> None:
        """Close a memoised instance and drop it.

        Args:
            name: The driver to discard, or ``None`` for the default.
        """
        instance = self._resolved.pop(name or self._default, None)
        if instance is not None:
            await self._close_instance(instance)

    async def _close_instance(self, instance: T) -> None:  # noqa: B027
        """Release one instance's resources.

        The base implementation does nothing, and is deliberately not
        abstract: a manager over inert drivers should not be forced to write an
        empty override. Subsystems whose drivers hold connections override it.
        Declaring the hook here rather than leaving each manager to invent its
        own is the difference between a lifecycle and four inconsistent ones.

        Args:
            instance: The driver being discarded.
        """

    def reset(self) -> None:
        """Drop every memoised instance without closing them.

        Intended for test teardown where the drivers are in-memory. Prefer
        ``await close_all()`` anywhere a driver may hold a connection.
        """
        self._resolved.clear()

    async def close_all(self) -> None:
        """Close every instance that has been built, then drop them all.

        Every instance is closed even if one raises; the first failure is
        re-raised afterwards. A manager that abandoned the loop on the first
        error would leak precisely the pools it was asked to release.

        Raises:
            BaseException: The first error raised while closing, after every
                other instance has been given the chance to close.
        """
        failure: BaseException | None = None
        for instance in tuple(self._resolved.values()):
            try:
                await self._close_instance(instance)
            except BaseException as exc:  # noqa: BLE001 — re-raised below
                failure = failure or exc
        self._resolved.clear()
        if failure is not None:
            raise failure
