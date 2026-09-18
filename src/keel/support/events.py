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


class Subscriptions:
    """What an observer holds so it can let go of every source exactly once.

    The request inspector and metrics each attach to several sources — an
    engine's events, a dispatcher, the logging root — and each attachment
    returns a remover. Three rules fall out of that, and this is where they
    live so neither observer re-derives them: attaching to the same source
    twice attaches once, or every statement is counted twice; a source stays
    attached until every holder that asked for it has let go, or the first
    holder's exit silently stops the second one's counting; and removing more
    times than adding is harmless, because a lifespan's exit and a handle a
    test holds may both try.
    """

    __slots__ = ("_holders", "_removers")

    def __init__(self) -> None:
        self._removers: dict[object, Callable[[], None]] = {}
        self._holders: dict[object, int] = {}

    def add(self, key: object, attach: Callable[[], Callable[[], None]]) -> object:
        """Attach to a source, or count one more holder of it.

        Args:
            key: What identifies the source, such as ``("engine", id(engine))``.
            attach: Performs the attachment and returns what undoes it. Called
                only for the first holder.

        Returns:
            The key, for a caller that will hand it back to :meth:`remove`.
        """
        if key not in self._removers:
            self._removers[key] = attach()
        self._holders[key] = self._holders.get(key, 0) + 1
        return key

    def remove(self, key: object) -> None:
        """Let go of one source; it is detached once nobody else holds it.

        Args:
            key: The key given to :meth:`add`.
        """
        remaining = self._holders.get(key)
        if remaining is None:
            return
        if remaining > 1:
            self._holders[key] = remaining - 1
            return
        del self._holders[key]
        self._removers.pop(key)()

    def clear(self) -> None:
        """Detach from every source, whoever else holds it."""
        self._holders.clear()
        for remover in list(self._removers.values()):
            remover()
        self._removers.clear()
