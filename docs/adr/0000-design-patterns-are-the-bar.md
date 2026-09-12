# ADR 0000 — Design patterns are the bar

**Status:** accepted, standing · **Date:** 2026-09-12 · **Applies to:** every phase

## The rule

Keel is built from **named, classical design patterns**, applied deliberately.
The reference is the Gang of Four catalogue.

Three obligations follow, and they are not optional:

1. **Name the pattern where it is used.** A docstring that says "manages
   things" is a failure. One that says "Abstract Factory: resolves and memoises
   named drivers from configuration, so adding a backend does not require
   editing this class" is the standard.
2. **Justify what it buys.** A pattern that cannot be defended in one sentence
   is decoration. The defence goes next to the code, not in a commit message.
3. **Record what was declined, and why.** The patterns *not* used carry as much
   information as the ones that are — they tell the next reader that the
   alternative was considered rather than missed.

## Why

Recognisable structure beats invented abstraction. A reader who knows Strategy,
Decorator and Template Method can read a subsystem they have never seen and
predict where things are. A reader facing a bespoke abstraction has to
reverse-engineer the author's intent first, which is the single most repeated
complaint about the FastAPI ecosystem — *"I've had to read the code many times
to literally reverse engineer features."*

Keel exists to be the substrate under other work. Substrates are read far more
often than they are written.

## The counter-rule, which matters as much

**A pattern must earn its place.** Pattern-itis — wrapping everything in a
factory, a Bridge over a single implementation, an interface with one
implementor — is worse than no pattern, because it adds indirection while
claiming rigour.

So "this should just be a function" is a legitimate and welcome finding, and
adversarial review of this codebase is explicitly asked to look for ceremony.
That review has already produced two corrections that stuck: `runtime_checkable`
was removed from the store contracts because `isinstance` on a protocol checks
attribute *names* only and offered false assurance, and the Bridge was ruled out
for the queue after it earned its place in the cache.

## How it is enforced

- Every module docstring names its pattern and says what it buys.
- Every phase ADR carries the pattern set for that subsystem, **including the
  declined ones**.
- Review passes are briefed to challenge each pattern as ceremony or substance.

## The record so far

**In use**

| Pattern | Where |
|---|---|
| Bridge | `cache.Store` (implementor) / `cache.Repository` (abstraction) |
| Strategy | Cache drivers; serializers; backoff policies |
| Template Method | `Repository.remember`; the worker loop |
| Abstract Factory | `Manager[T]`, `CacheManager`, `QueueManager` |
| Decorator | `EventfulStore`, `FakeStore` |
| Virtual Proxy | `CacheProxy`, the `dispatch()` facade |
| Null Object | `NullStore`, `NullLock` |
| Observer | `EventDispatcher`; model observers; job lifecycle events |
| Test Spy | `FakeStore`, `FakeQueue` |
| Command | `Job` — an operation with its parameters, serialised and executed later |
| Chain of Responsibility | *Planned* for job middleware; not built, because one middleware is not a chain |

**Declined, deliberately**

| Pattern | Where it was considered | Why not |
|---|---|---|
| Singleton | Managers, `Database` | Lifetimes belong to the container and the lifespan, not to classes deciding to be unique. Memoisation is not Singleton; it can be reset. |
| Service Locator | Application code | The facade is a convenience at the edges. Services take what they need as arguments. |
| Bridge | The queue | Earned its place in the cache because `remember` is substantial. A queue's dispatch side is `push`/`later`/`bulk` — a second layer would be ceremony. |
| Abstract Base Classes for drivers | `Store`, `Queue` | Protocols instead, so a third-party driver needs no dependency on Keel to satisfy the contract. |
| A general DI container | Everywhere | Solves a team-coordination problem this project does not have. |
| A symmetrical consume contract | The queue | The worker is one implementation. An interface with a single implementor is indirection pretending to be design. |
| Decorator over a real backend, for the fake | `FakeQueue` | Works for the cache, where behaviour is cheap to have for real. Running a job would make the test exercise the handler while claiming to test the dispatcher. |

## Note

The Liskov substitution principle is treated as testable rather than aspirational:
the parametrised contract suite runs every driver through the same assertions.
`NullStore` is the documented exception — a Null Object satisfies the interface
and deliberately not the behaviour, which is why it is excluded from that suite
rather than being allowed to weaken it.
