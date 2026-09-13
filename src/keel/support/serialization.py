"""Value serialisation strategies.

Failures raise :class:`~keel.exceptions.SerializationError`, which wraps the
underlying library error: a cache should fail in terms the caller can act on,
not leak ``pickle.UnpicklingError`` from three layers down.

Network-backed stores speak bytes, so a value has to be encoded on the way out
and decoded on the way back. Which encoding is a genuine trade-off — safety
versus reach — so it is a *Strategy*: the store depends on the protocol, the
application picks the implementation.

One non-obvious constraint drives the default choice. Redis ``INCRBY`` operates
on the stored bytes, so counters only work if integers are encoded as plain
decimal digits. JSON does exactly that (``dumps(5) == "5"``), which is why
:class:`JsonSerializer` is the default and why :class:`PickleSerializer` cannot
support atomic increments.
"""

from __future__ import annotations

import json
import pickle
from typing import Any, Protocol, runtime_checkable

from keel.exceptions import SerializationError


@runtime_checkable
class Serializer(Protocol):
    """Encodes values for a byte-oriented backend."""

    @property
    def supports_atomic_increment(self) -> bool:
        """Whether integers are encoded such that the backend can increment them."""
        ...

    def dumps(self, value: Any) -> bytes:
        """Encode *value* for storage.

        Args:
            value: The value to encode.

        Returns:
            The encoded bytes.

        Raises:
            SerializationError: If the value cannot be encoded.
        """
        ...

    def loads(self, raw: bytes) -> Any:
        """Decode bytes previously produced by :meth:`dumps`.

        Args:
            raw: The encoded bytes.

        Returns:
            The decoded value.

        Raises:
            SerializationError: If the bytes cannot be decoded.
        """
        ...


class JsonSerializer:
    """JSON encoding. Safe across processes and languages, limited in reach.

    Handles the JSON type set only. Values outside it (``datetime``, ``set``,
    arbitrary objects) raise :class:`SerializationError` rather than silently
    degrading, so the failure surfaces in development instead of production.
    """

    __slots__ = ()

    @property
    def supports_atomic_increment(self) -> bool:
        """JSON encodes integers as bare digits, so backend increments work."""
        return True

    def dumps(self, value: Any) -> bytes:
        """Encode *value* as compact UTF-8 JSON.

        Args:
            value: The value to encode.

        Returns:
            The encoded bytes.

        Raises:
            SerializationError: If the value is not JSON-encodable.
        """
        try:
            return json.dumps(value, separators=(",", ":")).encode()
        except (TypeError, ValueError) as exc:
            raise SerializationError(
                f"{type(value).__name__} is not JSON-serialisable; "
                f"use PickleSerializer if you need to cache arbitrary objects"
            ) from exc

    def loads(self, raw: bytes) -> Any:
        """Decode UTF-8 JSON bytes.

        Args:
            raw: The encoded bytes.

        Returns:
            The decoded value.

        Raises:
            SerializationError: If the bytes are not valid JSON.
        """
        try:
            return json.loads(raw)
        except (ValueError, UnicodeDecodeError) as exc:
            raise SerializationError("cached value is not valid JSON") from exc


class PickleSerializer:
    """Pickle encoding. Arbitrary Python objects, at a cost.

    Warning:
        Unpickling executes code. Only use this when every writer to the cache
        is trusted — never with a cache that untrusted input can reach.
    """

    __slots__ = ("_protocol",)

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        self._protocol = protocol

    @property
    def supports_atomic_increment(self) -> bool:
        """Pickle frames are opaque to the backend, so increments must be emulated."""
        return False

    def dumps(self, value: Any) -> bytes:
        """Encode *value* with :mod:`pickle`.

        Args:
            value: The value to encode.

        Returns:
            The encoded bytes.

        Raises:
            SerializationError: If the value cannot be pickled.
        """
        try:
            return pickle.dumps(value, protocol=self._protocol)
        except (pickle.PicklingError, TypeError, AttributeError) as exc:
            raise SerializationError(f"{type(value).__name__} is not picklable") from exc

    def loads(self, raw: bytes) -> Any:
        """Decode pickled bytes.

        Args:
            raw: The encoded bytes.

        Returns:
            The decoded value.

        Raises:
            SerializationError: If the bytes cannot be unpickled.
        """
        try:
            return pickle.loads(raw)
        except Exception as exc:  # pragma: no cover — pickle raises many types
            raise SerializationError("cached value could not be unpickled") from exc


__all__ = [
    "JsonSerializer",
    "PickleSerializer",
    "SerializationError",
    "Serializer",
]
