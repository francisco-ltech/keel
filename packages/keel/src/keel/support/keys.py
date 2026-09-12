"""Key namespacing.

Every store instance owns a slice of its backend's keyspace. ``KeyNamespace`` is
the value object that defines that slice, so ``flush()`` can mean "everything I
own" rather than "everything in this Redis database" — a distinction that is the
difference between clearing a cache and taking down a shared server.
"""

from __future__ import annotations

from dataclasses import dataclass

from keel.exceptions import ConfigurationError

SEPARATOR = ":"

GLOB_METACHARACTERS = "\\*?[]"
"""Characters Redis ``SCAN MATCH`` treats as pattern syntax.

They have to be escaped when a prefix is interpolated into a glob, or a store
namespaced ``app[1]`` matches — and therefore flushes — the keys of ``app1``.
The backslash is first so escaping it does not double-escape the others.
"""


def escape_glob(value: str) -> str:
    """Return *value* with every glob metacharacter escaped.

    Args:
        value: A literal string destined for a glob pattern.

    Returns:
        The string, safe to interpolate into ``SCAN MATCH``.
    """
    for char in GLOB_METACHARACTERS:
        value = value.replace(char, f"\\{char}")
    return value


@dataclass(frozen=True, slots=True)
class KeyNamespace:
    """An immutable prefix applied to every key a store reads or writes.

    Attributes:
        prefix: The namespace segment. Empty means the store owns the whole
            keyspace of its backend, which is only appropriate for stores that
            are not shared (``array``, ``null``).
    """

    prefix: str = ""

    def __post_init__(self) -> None:
        """Normalise away a trailing separator so ``"keel"`` and ``"keel:"`` agree."""
        object.__setattr__(self, "prefix", self.prefix.rstrip(SEPARATOR))

    @property
    def is_empty(self) -> bool:
        """Whether this namespace applies no prefix at all."""
        return not self.prefix

    def apply(self, key: str) -> str:
        """Return *key* qualified with this namespace.

        Args:
            key: The caller-facing key.

        Returns:
            The backend-facing key.
        """
        if self.is_empty:
            return key
        return f"{self.prefix}{SEPARATOR}{key}"

    def unqualify(self, key: str) -> str:
        """Return *key* with this namespace removed.

        Args:
            key: The backend-facing key.

        Returns:
            The caller-facing key. Keys outside this namespace are returned
            unchanged rather than raising, because backends may legitimately
            return keys written by other processes.
        """
        if self.is_empty:
            return key
        head = f"{self.prefix}{SEPARATOR}"
        return key.removeprefix(head)

    def pattern(self) -> str:
        """Return a glob matching every key this namespace owns, and no others.

        The prefix is escaped before interpolation: it is a literal, while the
        surrounding pattern is syntax. Without that, a namespace containing a
        metacharacter silently changes which keys the pattern selects — in
        either direction, so it can both flush a neighbour's keys and fail to
        flush its own.

        Returns:
            A pattern suitable for Redis ``SCAN MATCH``.

        Raises:
            ConfigurationError: If the namespace is empty. An empty namespace
                would produce ``*``, which matches the entire keyspace of a
                shared server — see :meth:`is_empty`.
        """
        if self.is_empty:
            raise ConfigurationError(
                "refusing to build a match-everything pattern from an empty "
                "namespace: this would select every key on the server, not just "
                "this store's. Give the store a prefix."
            )
        return f"{escape_glob(self.prefix)}{SEPARATOR}*"

    def child(self, segment: str) -> KeyNamespace:
        """Return a nested namespace below this one.

        Args:
            segment: The additional namespace segment.

        Returns:
            A new namespace; the receiver is unchanged.
        """
        if self.is_empty:
            return KeyNamespace(segment)
        return KeyNamespace(f"{self.prefix}{SEPARATOR}{segment}")
