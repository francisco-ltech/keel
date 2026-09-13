"""Key namespacing — the value object that bounds what ``flush()`` can destroy.

Covers qualification and its inverse, the trailing-separator normalisation that
makes ``"keel"`` and ``"keel:"`` the same namespace, nesting, and the value
semantics (frozen, hashable, comparable) the rest of the library assumes.
"""

from __future__ import annotations

import pytest

from keel.exceptions import ConfigurationError
from keel.support.keys import SEPARATOR, KeyNamespace


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("user:42", id="colons"),
        pytest.param("plain", id="plain"),
        pytest.param("", id="empty"),
        pytest.param("with spaces", id="spaces"),
        pytest.param("trailing:", id="trailing-separator"),
    ],
)
def test_apply_and_unqualify_round_trip(key: str) -> None:
    namespace = KeyNamespace("keel:cache")
    assert namespace.unqualify(namespace.apply(key)) == key


def test_apply_prefixes_with_the_separator() -> None:
    assert KeyNamespace("keel").apply("user:42") == "keel:user:42"


def test_unqualify_removes_only_the_leading_namespace() -> None:
    assert KeyNamespace("keel").unqualify("keel:keel:user") == "keel:user"


# -- the empty namespace --------------------------------------------------


def test_an_empty_namespace_applies_nothing() -> None:
    namespace = KeyNamespace()
    assert namespace.is_empty is True
    assert namespace.apply("user:42") == "user:42"
    assert namespace.unqualify("user:42") == "user:42"


def test_an_empty_namespace_is_the_default() -> None:
    assert KeyNamespace() == KeyNamespace("")


def test_a_namespace_of_only_separators_is_empty() -> None:
    assert KeyNamespace(":::").is_empty is True


def test_a_non_empty_namespace_reports_itself_as_such() -> None:
    assert KeyNamespace("keel").is_empty is False


# -- normalisation --------------------------------------------------------


def test_a_trailing_separator_is_normalised_away() -> None:
    assert KeyNamespace("a:") == KeyNamespace("a")
    assert KeyNamespace("a:").prefix == "a"


def test_several_trailing_separators_are_normalised_away() -> None:
    assert KeyNamespace("a:::") == KeyNamespace("a")


def test_normalisation_leaves_interior_separators_alone() -> None:
    assert KeyNamespace("a:b:").prefix == "a:b"


def test_a_normalised_namespace_qualifies_keys_identically() -> None:
    assert KeyNamespace("keel:").apply("user") == KeyNamespace("keel").apply("user")


# -- patterns -------------------------------------------------------------


def test_an_empty_namespace_refuses_to_build_a_pattern() -> None:
    """Regression: it used to return ``*``.

    A store with no prefix would then flush every key on the server — other
    applications, queues and sessions included. Refusing here makes the unsafe
    case impossible to reach rather than merely discouraged.
    """
    with pytest.raises(ConfigurationError):
        KeyNamespace().pattern()


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        pytest.param("app[1]", r"app\[1\]:*", id="bracket"),
        pytest.param("a*b", r"a\*b:*", id="star"),
        pytest.param("a?b", r"a\?b:*", id="question"),
        pytest.param(r"a\b", r"a\\b:*", id="backslash"),
    ],
)
def test_glob_metacharacters_in_a_prefix_are_escaped(prefix: str, expected: str) -> None:
    """Regression: an unescaped prefix selected a neighbour's keys.

    ``KeyNamespace("app[1]")`` produced the glob ``app[1]:*``, which Redis reads
    as a character class — so flushing it deleted everything under ``app1:``.
    """
    assert KeyNamespace(prefix).pattern() == expected


def test_the_pattern_of_a_namespace_is_bounded_by_its_prefix() -> None:
    """This is what stops a cache flush wiping a queue on a shared server."""
    assert KeyNamespace("keel:cache").pattern() == "keel:cache:*"


def test_the_pattern_matches_what_apply_produces() -> None:
    namespace = KeyNamespace("keel")
    assert namespace.apply("anything").startswith(namespace.pattern().removesuffix("*"))


# -- nesting --------------------------------------------------------------


def test_child_appends_a_segment() -> None:
    assert KeyNamespace("keel").child("sessions") == KeyNamespace("keel:sessions")


def test_child_of_an_empty_namespace_is_the_segment_alone() -> None:
    assert KeyNamespace().child("sessions") == KeyNamespace("sessions")


def test_child_nests_to_any_depth() -> None:
    nested = KeyNamespace("keel").child("cache").child("prod")
    assert nested.prefix == "keel:cache:prod"
    assert nested.apply("user:42") == "keel:cache:prod:user:42"


def test_child_leaves_the_receiver_unchanged() -> None:
    parent = KeyNamespace("keel")
    parent.child("sessions")
    assert parent.prefix == "keel"


def test_child_normalises_a_trailing_separator_in_the_segment() -> None:
    assert KeyNamespace("keel").child("sessions:") == KeyNamespace("keel:sessions")


# -- value semantics ------------------------------------------------------


def test_a_namespace_is_frozen() -> None:
    namespace = KeyNamespace("keel")
    with pytest.raises(AttributeError):
        namespace.prefix = "other"  # type: ignore[misc]  # ty: ignore[invalid-assignment]


def test_a_namespace_is_hashable_and_usable_as_a_dict_key() -> None:
    counts = {KeyNamespace("a"): 1, KeyNamespace("b"): 2}
    counts[KeyNamespace("a:")] = 3
    assert counts == {KeyNamespace("a"): 3, KeyNamespace("b"): 2}


def test_namespaces_compare_by_prefix() -> None:
    assert KeyNamespace("a") == KeyNamespace("a")
    assert KeyNamespace("a") != KeyNamespace("b")


# -- foreign keys ---------------------------------------------------------


def test_unqualify_leaves_a_key_from_another_namespace_alone() -> None:
    """Backends return keys written by other processes; raising would be wrong."""
    namespace = KeyNamespace("keel")
    assert namespace.unqualify("other:user:42") == "other:user:42"


def test_unqualify_leaves_a_merely_similar_prefix_alone() -> None:
    assert KeyNamespace("keel").unqualify("keelish:user") == "keelish:user"


def test_the_separator_is_a_colon() -> None:
    assert SEPARATOR == ":"
