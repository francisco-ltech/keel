# keel

The core library. Each subsystem is exposed the same way: a facade the
application calls, a swappable driver behind it, and a fake that records what
happened so tests can assert on it.

Shipped so far:

- `keel.cache` — cache, atomic locks, `remember` with single-flight. Drivers:
  Redis, in-memory, null.
- `keel.database` — async engine, session factory, and the `uow()` transaction
  idiom.
- `keel.modules` — the domain-module convention the starter template is built
  from.
- `keel.testing` — fakes and fixtures for applications built on Keel.

See `docs/adr/` at the repository root for why the pieces are shaped this way.
Not published anywhere; the starter template depends on it by path.
