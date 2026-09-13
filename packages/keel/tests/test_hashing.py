"""Password hashing.

Two of these are the reason the module exists rather than a two-line wrapper.

The constant-cost miss is asserted by **counting verifications**, not by timing
one. A timing assertion in a suite that runs under xdist on a shared laptop is a
flake generator; what the design actually promises is that a miss performs the
same work as a hit, and that is exactly countable.

The rehash-on-login test raises the cost between hashing and verifying, which is
the real event it defends against.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pwdlib.hashers.argon2 import Argon2Hasher

from keel.auth import (
    HashingConfig,
    PasswordHasher,
    bound_password_hasher,
    hash_password,
    hashing_lifespan,
    password_hasher,
    set_password_hasher,
    use_password_hasher,
    verify_password,
)
from keel.auth.config import MINIMUM_MEMORY_COST, SETTING_VARS
from keel.exceptions import ConfigurationError, UnsupportedHashError

pytestmark = pytest.mark.anyio

CHEAP = HashingConfig(time_cost=1, memory_cost=MINIMUM_MEMORY_COST, parallelism=1)
"""The floor, so the suite pays milliseconds rather than the ~44 ms a production
hash costs by design."""

PASSWORD = "correct horse battery staple"


@pytest.fixture(autouse=True)
def _isolate_the_binding() -> Iterator[None]:
    """Leave the process-wide hasher as it was found.

    These tests bind and unbind it, and xdist runs whole files in a worker that
    then goes on to other files. A leaked binding would surface as an unrelated
    test failing depending on collection order.
    """
    previous = bound_password_hasher()
    set_password_hasher(None)
    try:
        yield
    finally:
        set_password_hasher(previous)


@pytest.fixture
def hasher() -> PasswordHasher:
    """A hasher at the cheapest parameters the config will accept."""
    return PasswordHasher(CHEAP)


def test_a_hash_is_not_the_password(hasher: PasswordHasher) -> None:
    """The one thing that must never regress."""
    assert PASSWORD not in hasher.hash(PASSWORD)


def test_the_same_password_hashes_differently_every_time(hasher: PasswordHasher) -> None:
    """Salted. Two users with one password must not share a digest."""
    assert hasher.hash(PASSWORD) != hasher.hash(PASSWORD)


def test_a_hash_carries_its_own_parameters(hasher: PasswordHasher) -> None:
    """Self-describing, so a hash made at one cost verifies at another."""
    stored = hasher.hash(PASSWORD)
    assert PasswordHasher(HashingConfig(time_cost=2, memory_cost=16384)).verify(PASSWORD, stored)


def test_verify_accepts_the_right_password(hasher: PasswordHasher) -> None:
    """The happy path."""
    assert hasher.verify(PASSWORD, hasher.hash(PASSWORD))


def test_verify_rejects_the_wrong_password(hasher: PasswordHasher) -> None:
    """The other happy path."""
    assert not hasher.verify("hunter2", hasher.hash(PASSWORD))


def test_verify_rejects_an_absent_principal(hasher: PasswordHasher) -> None:
    """``None`` is how a caller says "no such user" without returning early."""
    assert not hasher.verify(PASSWORD, None)


def test_a_miss_does_the_same_work_as_a_hit(
    hasher: PasswordHasher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The user-enumeration defence, counted rather than timed.

    Returning early for an unknown address answers "is this registered?" to
    anyone with a stopwatch. Both paths must verify exactly once.
    """
    calls = 0
    original = Argon2Hasher.verify

    def counted(self: Argon2Hasher, password: str | bytes, digest: str | bytes) -> bool:
        nonlocal calls
        calls += 1
        return original(self, password, digest)

    stored = hasher.hash(PASSWORD)
    hasher.verify(PASSWORD, None)  # warm the dummy before counting
    monkeypatch.setattr(Argon2Hasher, "verify", counted)

    hasher.verify(PASSWORD, stored)
    hits = calls
    hasher.verify(PASSWORD, None)
    assert calls - hits == hits == 1


def test_the_dummy_is_computed_once(hasher: PasswordHasher) -> None:
    """Cached, so a service under credential stuffing does not rehash it per attempt."""
    hasher.verify(PASSWORD, None)
    first = hasher._dummy
    hasher.verify(PASSWORD, None)
    assert hasher._dummy is first


def test_verify_and_upgrade_leaves_a_current_hash_alone(hasher: PasswordHasher) -> None:
    """No replacement means nothing for the caller to persist."""
    assert hasher.verify_and_upgrade(PASSWORD, hasher.hash(PASSWORD)) == (True, None)


def test_verify_and_upgrade_rehashes_a_stale_cost() -> None:
    """The event this defends against: parameters were raised after the hash was stored."""
    stored = PasswordHasher(CHEAP).hash(PASSWORD)
    raised = PasswordHasher(HashingConfig(time_cost=2, memory_cost=16384, parallelism=1))

    matched, replacement = raised.verify_and_upgrade(PASSWORD, stored)

    assert matched
    assert replacement is not None
    assert replacement != stored
    assert raised.verify_and_upgrade(PASSWORD, replacement) == (True, None)


def test_a_wrong_password_is_never_offered_a_replacement(hasher: PasswordHasher) -> None:
    """Rehashing on a failed verification would write an attacker's password."""
    stored = PasswordHasher(HashingConfig(time_cost=2, memory_cost=16384)).hash(PASSWORD)
    assert hasher.verify_and_upgrade("hunter2", stored) == (False, None)


def test_verify_and_upgrade_handles_an_absent_principal(hasher: PasswordHasher) -> None:
    """Same constant-cost miss as ``verify``."""
    assert hasher.verify_and_upgrade(PASSWORD, None) == (False, None)


def test_a_hash_nobody_can_read_is_an_error_not_a_rejection(hasher: PasswordHasher) -> None:
    """A half-finished migration must not look like a wrong password.

    Returning ``False`` for a bcrypt row under an Argon2-only configuration
    locks those accounts out permanently, and the symptom points at the user.
    """
    bcrypt_hash = "$2b$12$" + "k" * 53
    with pytest.raises(UnsupportedHashError, match="'2b' hash"):
        hasher.verify(PASSWORD, bcrypt_hash)


def test_the_unreadable_hash_error_says_what_to_do(hasher: PasswordHasher) -> None:
    """And names the scheme rather than logging the digest."""
    bcrypt_hash = "$2b$12$" + "k" * 53
    with pytest.raises(UnsupportedHashError) as caught:
        hasher.verify_and_upgrade(PASSWORD, bcrypt_hash)
    assert "configure the algorithm" in str(caught.value)
    assert bcrypt_hash not in str(caught.value)


def test_a_hash_in_no_recognisable_format_still_names_the_problem(
    hasher: PasswordHasher,
) -> None:
    """A truncated or corrupted column value, rather than another algorithm."""
    with pytest.raises(UnsupportedHashError, match="'unrecognised' hash"):
        hasher.verify(PASSWORD, "not-a-hash-at-all")


# -- configuration --------------------------------------------------------


def test_the_default_parameters_are_the_recommended_ones() -> None:
    """Restated rather than deferred to, so a pwdlib upgrade cannot move them silently."""
    assert HashingConfig() == HashingConfig(time_cost=3, memory_cost=65536, parallelism=4)


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"time_cost": 0}, "time_cost must be at least 1"),
        ({"memory_cost": MINIMUM_MEMORY_COST - 1}, "no longer memory-hard"),
        ({"parallelism": 0}, "parallelism must be at least 1"),
    ],
)
def test_parameters_below_their_floor_are_refused(kwargs: dict[str, int], expected: str) -> None:
    """At load time, where it is a configuration error rather than a weak hash."""
    with pytest.raises(ConfigurationError, match=expected):
        HashingConfig(**kwargs)


def test_from_env_reads_the_subsystem_prefix() -> None:
    """``HASHING_*``. Subsystem, not framework."""
    config = HashingConfig.from_env(
        {
            f"{SETTING_VARS}TIME_COST": "1",
            f"{SETTING_VARS}MEMORY_COST": "8192",
            f"{SETTING_VARS}PARALLELISM": "1",
        }
    )
    assert config == CHEAP


def test_from_env_falls_back_to_the_defaults() -> None:
    """An empty environment is a valid one; production wants the recommended cost."""
    assert HashingConfig.from_env({}) == HashingConfig()


def test_from_env_rejects_a_value_that_is_not_a_number() -> None:
    """Naming the variable, because "invalid literal for int()" does not."""
    with pytest.raises(ConfigurationError, match="TIME_COST must be a whole number"):
        HashingConfig.from_env({f"{SETTING_VARS}TIME_COST": "high"})


def test_from_env_honours_an_application_prefix() -> None:
    """For a service that namespaces its own variables."""
    assert (
        HashingConfig.from_env({f"APP_{SETTING_VARS}TIME_COST": "1"}, prefix="APP_").time_cost == 1
    )


# -- binding --------------------------------------------------------------


def test_hashing_without_a_bound_hasher_says_how_to_bind_one() -> None:
    """The failure a new service hits first, so the message has to carry the fix."""
    with pytest.raises(ConfigurationError, match="hashing_lifespan"):
        hash_password(PASSWORD)


async def test_the_lifespan_binds_and_then_restores() -> None:
    """Restores rather than unbinds, so a nested test lifespan cannot kill the outer one."""
    outer = PasswordHasher(CHEAP)
    set_password_hasher(outer)

    async with hashing_lifespan(CHEAP) as inner:
        assert bound_password_hasher() is inner
        assert inner is not outer

    assert bound_password_hasher() is outer


async def test_the_module_functions_use_the_bound_hasher() -> None:
    """The facade is the only thing application code should need."""
    async with hashing_lifespan(CHEAP):
        assert verify_password(PASSWORD, hash_password(PASSWORD))
        assert password_hasher().config == CHEAP


def test_use_password_hasher_swaps_for_a_block() -> None:
    """A ContextVar override, so concurrent tests cannot see each other's hasher."""
    outer = PasswordHasher(CHEAP)
    set_password_hasher(outer)
    swapped = PasswordHasher(HashingConfig(time_cost=2, memory_cost=16384, parallelism=1))

    with use_password_hasher(swapped):
        assert password_hasher() is swapped
    assert password_hasher() is outer
