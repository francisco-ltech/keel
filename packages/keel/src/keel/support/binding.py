"""Process-wide bindings with a context-local override.

Every subsystem needs the same thing: one instance installed at startup that
application code can reach without threading it through every constructor, and
a way for a test to swap it for the duration of a block. Writing that twice
would guarantee the two copies drift, so it is written once here.

The two layers are not interchangeable, and the reason is a real bug rather
than a preference:

* The **process-wide default** is a plain attribute, not a
  :class:`~contextvars.ContextVar`. Starlette runs an application's lifespan in
  a different task from its request handlers, and a context variable set in the
  lifespan is not visible in the handlers. Binding there and reading here would
  work in tests and fail in production, which is the worst available outcome.
* The **override** is a ``ContextVar``, so concurrent tests cannot see each
  other's instance and leaving the block is the only teardown needed.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from keel.exceptions import ConfigurationError


class Binding[T]:
    """Holds the instance of one subsystem that is currently in effect.

    Args:
        label: The subsystem's name, used in the error raised when nothing is
            bound.
        setup_hint: What the developer should call to bind one. Included in that
            error, because "no cache manager is bound" is only half an error
            message — the useful half is what to do about it.
    """

    __slots__ = ("_default", "_label", "_override", "_setup_hint")

    def __init__(self, label: str, setup_hint: str) -> None:
        self._label = label
        self._setup_hint = setup_hint
        self._default: T | None = None
        self._override: ContextVar[T | None] = ContextVar(f"keel_{label}_override", default=None)

    def set(self, value: T | None) -> None:
        """Install the process-wide instance.

        Args:
            value: The instance to bind, or ``None`` to unbind.
        """
        self._default = value

    def peek(self) -> T | None:
        """Return the process-wide instance without raising.

        Returns:
            The bound instance, or ``None``. Used by lifespans that restore a
            previous binding rather than unbinding.
        """
        return self._default

    def current(self) -> T:
        """Return the instance in effect.

        Returns:
            The context-local override if one is active, otherwise the
            process-wide instance.

        Raises:
            ConfigurationError: If nothing is bound. This is a wiring error and
                saying so beats an ``AttributeError`` on ``None`` from
                somewhere deep inside a request.
        """
        value = self._override.get() or self._default
        if value is None:
            raise ConfigurationError(f"no {self._label} is bound; {self._setup_hint}")
        return value

    @contextmanager
    def use(self, value: T) -> Iterator[T]:
        """Override the binding for the duration of the block.

        Args:
            value: The instance to use.

        Yields:
            The same instance, for convenience.
        """
        token = self._override.set(value)
        try:
            yield value
        finally:
            self._override.reset(token)
