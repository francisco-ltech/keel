"""The lazy export table and the modules behind it must agree.

`keel.queue.__getattr__` resolves a name with `getattr` on the module, so a name
missing from that module's `__all__` still imports — and nothing notices that a
star import, or a linter reading `__all__`, cannot see it. The review found two
new events and a default in exactly that state. Mechanical, so it cannot go
stale the way "remember to add it in both places" did.
"""

from __future__ import annotations

import importlib

import pytest

import keel.queue
from keel.queue import _LAZY


@pytest.mark.parametrize("name", sorted(_LAZY))
def test_every_lazily_exported_name_is_in_its_modules_all(name: str) -> None:
    module = importlib.import_module(_LAZY[name])
    assert name in module.__all__, f"{name} is exported lazily but {_LAZY[name]}.__all__ omits it"
    assert name in keel.queue.__all__, f"{name} is in _LAZY but not keel.queue.__all__"
    assert getattr(keel.queue, name) is getattr(module, name)
