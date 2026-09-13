"""Token store drivers.

Each is an Adapter between the :class:`~keel.contracts.auth.TokenStore` contract
and one backend. They are held to ``tests/test_token_store_contract.py``, which
is the only thing that makes "swap the driver" a safe sentence.
"""

from __future__ import annotations

__all__: list[str] = []
