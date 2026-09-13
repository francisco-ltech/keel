"""Password hashing.

Argon2id through pwdlib, with the two things a hand-rolled ``hash``/``verify``
pair reliably leaves out.

**Rehash on login.** Cost parameters get raised, and every password hashed
before that stays at the old cost forever unless something notices.
:meth:`PasswordHasher.verify_and_upgrade` returns a replacement hash when the
stored one is stale, so the upgrade happens on the one occasion the plaintext is
available.

**A constant-cost miss.** Verifying nothing is instant and verifying a real hash
is not, so a login that returns early for an unknown address answers "is this
address registered?" to anyone with a stopwatch. Passing ``None`` for the stored
hash verifies against a dummy and returns ``False``, which is the same shape of
defence as comparing digests rather than strings.

The dummy is hashed **in the constructor**, not on first use. Deferring it looks
like a saving and is a timing oracle: the first miss in a process would pay a
hash *and* a verify while the first hit paid only a verify — 81ms against 38ms
at production parameters. One probe per process is one per worker, per rolling
deploy, per scale-out, and every ``hashing_lifespan`` resets the counter. What
deferring bought was one hash at boot.

**A hash nobody can read is an error, not a rejection.** pwdlib raises when no
configured hasher recognises a stored digest — bcrypt rows against an Argon2-only
configuration. Translating that to ``False`` would lock those accounts out
permanently while looking like a wrong password, so it is re-raised as
:class:`~keel.exceptions.UnsupportedHashError` instead.

No Abstract Factory over algorithms, and no ``Hasher`` protocol. pwdlib already
owns that seam — it verifies bcrypt while hashing Argon2, which is the whole
migration story — so a second one here would be a driver seam over a driver
seam. The variation Keel actually has is *parameters*, and those are
configuration. ADR 0000's counter-rule.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager

from pwdlib import PasswordHash
from pwdlib.exceptions import UnknownHashError
from pwdlib.hashers.argon2 import Argon2Hasher

from keel.auth.config import HashingConfig
from keel.exceptions import UnsupportedHashError
from keel.support.binding import Binding

DUMMY_PASSWORD = "keel-dummy-password-for-constant-time-misses"
"""Hashed at construction so a miss costs what a hit costs from the first
request. Never a valid credential: it is compared against, never stored."""


class PasswordHasher:
    """Hashes and verifies passwords at a configured cost.

    Args:
        config: Argon2id parameters. Defaults to pwdlib's recommended set.
    """

    __slots__ = ("_backend", "_config", "_dummy")

    def __init__(self, config: HashingConfig | None = None) -> None:
        self._config = config or HashingConfig()
        self._backend = PasswordHash(
            (
                Argon2Hasher(
                    time_cost=self._config.time_cost,
                    memory_cost=self._config.memory_cost,
                    parallelism=self._config.parallelism,
                ),
            )
        )
        self._dummy = self._backend.hash(DUMMY_PASSWORD)

    @property
    def config(self) -> HashingConfig:
        """The parameters this hasher was built with."""
        return self._config

    def hash(self, plaintext: str) -> str:
        """Hash a password for storage.

        Args:
            plaintext: The password as the user typed it.

        Returns:
            A self-describing hash — the algorithm and its parameters travel
            with the digest, so verifying it later needs no configuration.
        """
        return self._backend.hash(plaintext)

    def verify(self, plaintext: str, hashed: str | None) -> bool:
        """Check a password against a stored hash.

        Args:
            plaintext: The password as the user typed it.
            hashed: The stored hash, or ``None`` when no such principal exists.
                Pass ``None`` rather than returning early, so the miss costs
                what a hit costs.

        Returns:
            Whether they match. ``False`` whenever *hashed* is ``None``.
        """
        if hashed is None:
            self._backend.verify(plaintext, self._dummy)
            return False
        try:
            return self._backend.verify(plaintext, hashed)
        except UnknownHashError as exc:
            raise self._unsupported(hashed) from exc

    def verify_and_upgrade(self, plaintext: str, hashed: str | None) -> tuple[bool, str | None]:
        """Check a password, and rehash it if the stored cost is stale.

        Args:
            plaintext: The password as the user typed it.
            hashed: The stored hash, or ``None`` for an absent principal.

        Returns:
            ``(matched, replacement)``. *replacement* is a new hash to persist
            when the stored one was made with different parameters, and ``None``
            when it is already current or did not match. Persisting it is the
            caller's job, because only the caller has the row.
        """
        if hashed is None:
            self._backend.verify(plaintext, self._dummy)
            return False, None
        try:
            return self._backend.verify_and_update(plaintext, hashed)
        except UnknownHashError as exc:
            raise self._unsupported(hashed) from exc

    @staticmethod
    def _unsupported(hashed: str) -> UnsupportedHashError:
        """Describe an unreadable hash without putting the digest in a log.

        Args:
            hashed: The stored hash.

        Returns:
            The error to raise, naming the scheme prefix only.
        """
        scheme = hashed.split("$", 2)[1] if hashed.startswith("$") else "unrecognised"
        return UnsupportedHashError(
            f"no configured hasher recognises a {scheme!r} hash; configure the "
            f"algorithm it was written with so those principals can log in and "
            f"be upgraded on the way through"
        )


_binding: Binding[PasswordHasher] = Binding(
    "password hasher",
    "call keel.auth.set_password_hasher(PasswordHasher(config)), or wrap the "
    "work in `async with hashing_lifespan(HashingConfig.from_env())`",
)


def password_hasher() -> PasswordHasher:
    """Return the hasher in effect.

    Returns:
        The bound hasher.

    Raises:
        ConfigurationError: If none is bound.
    """
    return _binding.current()


def bound_password_hasher() -> PasswordHasher | None:
    """Return the bound hasher without raising.

    Returns:
        The bound hasher, or ``None``.
    """
    return _binding.peek()


def set_password_hasher(hasher: PasswordHasher | None) -> None:
    """Install the process-wide hasher.

    Args:
        hasher: The hasher to bind, or ``None`` to unbind.
    """
    _binding.set(hasher)


@contextmanager
def use_password_hasher(hasher: PasswordHasher) -> Iterator[PasswordHasher]:
    """Swap the hasher for the duration of a block.

    Args:
        hasher: The hasher to use.

    Yields:
        The hasher now in effect.
    """
    with _binding.use(hasher) as bound:
        yield bound


@asynccontextmanager
async def hashing_lifespan(config: HashingConfig | None = None) -> AsyncIterator[PasswordHasher]:
    """Bind a hasher for the life of the process.

    Restores whatever was bound before rather than unbinding, so a nested test
    lifespan does not kill the outer one.

    Args:
        config: Argon2id parameters. Defaults to the recommended set.

    Yields:
        The bound hasher.
    """
    previous = _binding.peek()
    hasher = PasswordHasher(config)
    _binding.set(hasher)
    try:
        yield hasher
    finally:
        _binding.set(previous)


def hash_password(plaintext: str) -> str:
    """Hash a password with the bound hasher.

    Args:
        plaintext: The password as the user typed it.

    Returns:
        The hash.

    Raises:
        ConfigurationError: If no hasher is bound.
    """
    return password_hasher().hash(plaintext)


def verify_password(plaintext: str, hashed: str | None) -> bool:
    """Verify a password with the bound hasher.

    Args:
        plaintext: The password as the user typed it.
        hashed: The stored hash, or ``None`` for an absent principal.

    Returns:
        Whether they match.

    Raises:
        ConfigurationError: If no hasher is bound.
    """
    return password_hasher().verify(plaintext, hashed)


__all__ = [
    "DUMMY_PASSWORD",
    "PasswordHasher",
    "bound_password_hasher",
    "hash_password",
    "hashing_lifespan",
    "password_hasher",
    "set_password_hasher",
    "use_password_hasher",
    "verify_password",
]
