"""Keel — batteries for FastAPI applications.

Each subsystem is exposed through the same seam: a facade the application calls,
a swappable driver behind it, and a fake that records what happened so tests can
assert against it.

Phase 1 ships the cache, which is the seam's proof. See
``docs/adr/0001-the-cache-seam.md`` for why the pieces are shaped the way they
are.
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
