"""Subsystem-agnostic building blocks.

Nothing here knows about caching. These are the pieces the next four subsystems
will reuse: driver resolution, event dispatch, key namespacing, serialisation
and sentinels.
"""
