"""The two serialisation strategies and the trade-off between them.

``JsonSerializer`` is safe and narrow; ``PickleSerializer`` is broad and
dangerous. The load-bearing detail is ``dumps(5) == b"5"``: Redis ``INCRBY``
operates on the stored bytes, so counters only work under an encoding that
stores integers as bare digits.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from keel.cache.stores.array import ArrayStore
from keel.exceptions import KeelError
from keel.support.serialization import (
    JsonSerializer,
    PickleSerializer,
    SerializationError,
    Serializer,
)


@dataclass
class Point:
    """A module-level class, so pickle can find it again on the way back."""

    x: int
    y: int


@pytest.fixture
def json_serializer() -> JsonSerializer:
    return JsonSerializer()


@pytest.fixture
def pickle_serializer() -> PickleSerializer:
    return PickleSerializer()


JSON_VALUES = [
    pytest.param(None, id="none"),
    pytest.param(True, id="true"),
    pytest.param(False, id="false"),
    pytest.param(0, id="zero"),
    pytest.param(-17, id="negative-int"),
    pytest.param(3.5, id="float"),
    pytest.param("", id="empty-string"),
    pytest.param("héllo ☕", id="unicode"),
    pytest.param([], id="empty-list"),
    pytest.param([1, "two", None, [3]], id="mixed-list"),
    pytest.param({}, id="empty-dict"),
    pytest.param({"nested": {"deep": [1, None, "x"]}}, id="nested-dict"),
]


# -- JSON -----------------------------------------------------------------


@pytest.mark.parametrize("value", JSON_VALUES)
def test_json_round_trips_the_json_type_set(json_serializer: JsonSerializer, value: Any) -> None:
    assert json_serializer.loads(json_serializer.dumps(value)) == value


def test_json_encodes_an_integer_as_bare_digits(json_serializer: JsonSerializer) -> None:
    """Load-bearing: Redis ``INCRBY`` reads the stored bytes as a number."""
    assert json_serializer.dumps(5) == b"5"
    assert json_serializer.dumps(-5) == b"-5"
    assert json_serializer.dumps(0) == b"0"


def test_json_output_is_compact(json_serializer: JsonSerializer) -> None:
    assert json_serializer.dumps({"a": 1, "b": [2, 3]}) == b'{"a":1,"b":[2,3]}'


def test_json_reports_that_it_supports_atomic_increment(
    json_serializer: JsonSerializer,
) -> None:
    assert json_serializer.supports_atomic_increment is True


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(datetime(2026, 1, 1, tzinfo=UTC), id="datetime"),
        pytest.param({1, 2, 3}, id="set"),
        pytest.param(Point(1, 2), id="object"),
        pytest.param(object(), id="bare-object"),
        pytest.param(b"bytes", id="bytes"),
    ],
)
def test_json_refuses_values_outside_its_type_set(
    json_serializer: JsonSerializer, value: Any
) -> None:
    """Failing loudly beats degrading silently: the developer finds out now."""
    with pytest.raises(SerializationError) as error:
        json_serializer.dumps(value)
    message = str(error.value)
    assert type(value).__name__ in message
    assert "PickleSerializer" in message


def test_json_rejects_a_self_referential_value(json_serializer: JsonSerializer) -> None:
    """A ``ValueError`` rather than a ``TypeError``: the other half of the guard."""
    loop: list[Any] = []
    loop.append(loop)
    with pytest.raises(SerializationError):
        json_serializer.dumps(loop)


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"not json", id="garbage"),
        pytest.param(b'{"unterminated": ', id="truncated"),
        pytest.param(b"\xff\xfe", id="invalid-utf8"),
    ],
)
def test_json_raises_on_malformed_bytes(json_serializer: JsonSerializer, raw: bytes) -> None:
    with pytest.raises(SerializationError) as error:
        json_serializer.loads(raw)
    assert "not valid JSON" in str(error.value)


# -- pickle ---------------------------------------------------------------


@pytest.mark.parametrize("value", JSON_VALUES)
def test_pickle_round_trips_the_json_type_set_too(
    pickle_serializer: PickleSerializer, value: Any
) -> None:
    assert pickle_serializer.loads(pickle_serializer.dumps(value)) == value


def test_pickle_round_trips_a_datetime(pickle_serializer: PickleSerializer) -> None:
    moment = datetime(2026, 9, 11, 15, 30, tzinfo=UTC)
    assert pickle_serializer.loads(pickle_serializer.dumps(moment)) == moment


def test_pickle_round_trips_a_custom_object(pickle_serializer: PickleSerializer) -> None:
    restored = pickle_serializer.loads(pickle_serializer.dumps(Point(1, 2)))
    assert restored == Point(1, 2)
    assert isinstance(restored, Point)


def test_pickle_round_trips_a_set(pickle_serializer: PickleSerializer) -> None:
    assert pickle_serializer.loads(pickle_serializer.dumps({1, 2, 3})) == {1, 2, 3}


def test_pickle_does_not_support_atomic_increment(
    pickle_serializer: PickleSerializer,
) -> None:
    """Pickle frames are opaque to Redis, so ``INCRBY`` cannot read them."""
    assert pickle_serializer.supports_atomic_increment is False


def test_pickle_does_not_encode_an_integer_as_digits(
    pickle_serializer: PickleSerializer,
) -> None:
    """The concrete reason the capability flag differs."""
    assert pickle_serializer.dumps(5) != b"5"


def test_pickle_refuses_an_unpicklable_value(pickle_serializer: PickleSerializer) -> None:
    def local_function() -> None:
        """Defined in a function body, so pickle cannot name it."""

    with pytest.raises(SerializationError) as error:
        pickle_serializer.dumps(local_function)
    assert "not picklable" in str(error.value)


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"not a pickle", id="garbage"),
        pytest.param(b"\x80\x05\x95", id="truncated-frame"),
    ],
)
def test_pickle_raises_on_malformed_bytes(pickle_serializer: PickleSerializer, raw: bytes) -> None:
    """Pickle raises several unrelated exception types; all become one."""
    with pytest.raises(SerializationError) as error:
        pickle_serializer.loads(raw)
    assert "could not be unpickled" in str(error.value)


def test_pickle_accepts_an_explicit_protocol() -> None:
    serializer = PickleSerializer(protocol=2)
    assert serializer.loads(serializer.dumps(Point(3, 4))) == Point(3, 4)


# -- the strategy seam ----------------------------------------------------


@pytest.mark.parametrize(
    "serializer",
    [pytest.param(JsonSerializer(), id="json"), pytest.param(PickleSerializer(), id="pickle")],
)
def test_both_satisfy_the_serializer_protocol(serializer: Serializer) -> None:
    assert isinstance(serializer, Serializer)


def test_the_capability_flag_is_what_distinguishes_them() -> None:
    assert JsonSerializer().supports_atomic_increment is not (
        PickleSerializer().supports_atomic_increment
    )


def test_serialization_error_is_inside_the_keel_exception_hierarchy() -> None:
    """Regression: it used to subclass bare ``Exception``.

    ``keel/exceptions.py`` promises that ``except KeelError`` catches the whole
    framework. An unserialisable value is one of the likelier cache failures, so
    the promise has to hold for it.
    """
    assert issubclass(SerializationError, KeelError)


@pytest.mark.anyio
async def test_an_unserialisable_value_is_caught_by_the_framework_base_class() -> None:
    """The promise as an application would actually rely on it."""
    store = ArrayStore()
    with pytest.raises(KeelError):
        await store.put("key", {datetime(2026, 9, 11, tzinfo=UTC)})
