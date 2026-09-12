---
name: keel-subsystem
description: The fixed shape every Keel subsystem takes — contract, drivers, manager, facade, fake, parametrised contract suite, ADR — and which parts transfer between subsystems and which do not. Read this BEFORE adding a subsystem to packages/keel (mail, storage, notifications, auth), before adding a driver to an existing one, and before deciding what a new protocol should contain. Covers the ADR 0000 obligations that a review will check.
---

# Keel — adding a subsystem

Work in `/Users/francisco/Source/keel`. Read `docs/adr/0000-design-patterns-are-the-bar.md`
first: name the pattern, justify it in one sentence, record what you declined.

## Build order

Each step compiles and is testable before the next.

| # | File | What it is |
|---|---|---|
| 1 | `contracts/<sub>.py` | The protocol. Only operations needing the backend's own guarantees. |
| 2 | `<sub>/config.py` | Frozen dataclass, `from_env(env=None, prefix="")`. No pydantic. |
| 3 | `<sub>/drivers.py` | The ones needing no service — in-memory, null. |
| 4 | `<sub>/manager.py` | `class <Sub>Manager(Manager[T])`, `_make`, `register_driver`. |
| 5 | `<sub>/<real>_driver.py` | The networked one. Lazily imported by the manager. |
| 6 | `<sub>/fake.py` | Test Spy with assertions. |
| 7 | `<sub>/__init__.py` | Facade, `<sub>_lifespan`, `__all__`. Lazy exports for optional deps. |
| 8 | `tests/test_<sub>_contract.py` | Parametrised over every real implementation. |
| 9 | `testing.py` | A `fake_<sub>()` context manager. |
| 10 | `docs/adr/000N-...md` | Decisions, declined patterns, what the suite caught. |

## What transfers, and what does not

Reuse directly — these generalise and are proven twice:

- `support/manager.py` — `Manager[T]`, memoised named drivers, `register_driver`
- `support/binding.py` — `Binding[T]`; process-wide default plus ContextVar override
- `support/events.py`, `support/keys.py`, `support/serialization.py`, `support/sentinels.py`
- The parametrised contract suite **as a technique**
- `<sub>_lifespan` as an `asynccontextmanager` that restores the previous binding

Do **not** copy without re-deriving:

- The cache's `Store`/`Repository` **Bridge**. It earned its place because
  `remember` is substantial. The queue's dispatch side is three methods, so a
  second layer there is ceremony. ADR 0001 and 0006.
- A symmetrical contract for both sides. The queue has a dispatch protocol and
  no consume protocol, because there is one worker.
- `FakeStore`'s Decorator-over-a-real-backend trick. It works when the real
  behaviour is cheap to have. `FakeQueue` records instead, because running a job
  would test the handler while claiming to test the dispatcher.

## Non-negotiables

- Core imports **no web framework**. A worker-only service must not pay for one.
- An optional dependency means a **lazy** import in the manager and a
  `__getattr__` export in `__init__.py`, with `TYPE_CHECKING` declarations so
  both type checkers still resolve the names.
- A destructive operation must not do the widest possible thing when an
  argument is omitted. `clear(None)` means the default lane; `purge()` refuses
  to run without criteria.
- A shared backend needs a non-empty namespace, and an empty one is refused
  rather than silently meaning "everything". This shipped as a real bug once.
- Errors subclass `KeelError`, and the class name ends in `Error`.

## The contract suite is the point

One parametrised fixture, every implementation, the same assertions. It has
caught, on first run: two implementations disagreeing about what `clear(None)`
means, and a circular import that only failed on one import order.

Exclude a Null-Object driver from it and give it its own tests. It satisfies the
interface and deliberately not the behaviour; admitting it would mean weakening
the contract for everything else.

## Before you say it is done

```sh
just quick          # lint + ty + mypy
just test           # ~16s
```

Then the ADR. It must include the patterns declined and why, and anything the
contract suite caught — those are the parts a future reader cannot reconstruct.
