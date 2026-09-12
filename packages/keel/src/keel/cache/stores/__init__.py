"""Cache backends.

Every module here implements :class:`keel.contracts.cache.Store` and is
interchangeable with the others. ``eventful`` is the odd one out: it implements
the same contract but wraps another store rather than talking to a backend.
"""
