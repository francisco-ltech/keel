"""A minimal event dispatcher.

The Observer pattern, kept deliberately small. Subsystems announce what happened
without knowing who cares; observability, metrics and the development request
inspector subscribe without any subsystem importing them.

Listeners may be sync or async. Both are supported because forcing ``async def``
on a listener that only appends to a list is friction with no benefit, and
because the alternative — two registration methods — doubles the API to save one
``inspect.isawaitable`` check.

A listener that raises must not break the operation that emitted the event: a
broken metrics hook should not fail a cache write. Failures are therefore routed
to *on_error* rather than propagated.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from typing import Any

type Listener = Callable[[Any], Awaitable[None] | None]
"""Receives an event. May be a coroutine function or a plain one."""

type ErrorHandler = Callable[[BaseException, Any], None]
"""Receives an exception raised by a listener, plus the event being dispatched."""


def _ignore(exc: BaseException, event: Any) -> None:
    """Swallow listener failures. Replaced in production by a logging handler."""


class EventDispatcher:
    """Routes events to listeners registered by event type.

    Args:
        on_error: Called when a listener raises. The default discards the error;
            an application should pass something that logs.
    """

    __slots__ = ("_listeners", "_on_error")

    def __init__(self, on_error: ErrorHandler | None = None) -> None:
        self._listeners: dict[type, list[Listener]] = defaultdict(list)
        self._on_error: ErrorHandler = on_error or _ignore

    def listen(self, event_type: type, listener: Listener) -> Callable[[], None]:
        """Register *listener* for events of *event_type* and its subclasses.

        Args:
            event_type: The type to subscribe to.
            listener: The callable to invoke.

        Returns:
            A function that removes this subscription. Returning the
            unsubscribe rather than exposing a ``forget`` method means callers
            cannot accidentally remove someone else's listener.
        """
        self._listeners[event_type].append(listener)

        def unsubscribe() -> None:
            registered = self._listeners.get(event_type)
            if registered and listener in registered:
                registered.remove(listener)

        return unsubscribe

    def has_listeners(self, event_type: type | None = None) -> bool:
        """Whether anything is listening.

        Args:
            event_type: Narrow the question to one event type.

        Returns:
            ``True`` if at least one listener would receive such an event.
        """
        if event_type is None:
            return any(self._listeners.values())
        return any(
            listeners
            for registered, listeners in self._listeners.items()
            if issubclass(event_type, registered)
        )

    async def dispatch(self, event: Any) -> None:
        """Deliver *event* to every listener registered for its type.

        Listeners run in registration order, sequentially. Concurrency here
        would make ordering unpredictable for observers that record a timeline.

        Args:
            event: The event instance.
        """
        for registered, listeners in list(self._listeners.items()):
            if not isinstance(event, registered):
                continue
            for listener in list(listeners):
                try:
                    result = listener(event)
                    if isawaitable(result):
                        await result
                except Exception as exc:  # noqa: BLE001 — listeners must not break emitters
                    self._on_error(exc, event)

    def clear(self) -> None:
        """Remove every listener. Intended for test teardown."""
        self._listeners.clear()
