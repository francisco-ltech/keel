# ADR 0004 — Adopt ty, keep mypy until it is stable

**Status:** accepted · **Date:** 2026-09-12 · **Supersedes:** nothing · **Review at:** ty 1.0

## Context

Keel has run `mypy --strict` since Phase 1, and it has earned its place: it
caught a `TypeGuard` that only narrowed in one direction, a `Result` with no
`rowcount`, and a test asserting `widget.id is None` against a column type that
declared it non-optional. Strict typing on a library whose selling point is that
it does not lie to you is not optional.

`ty` — Astral's checker, written in Rust — is the obvious candidate to replace
it. The upstream FastAPI template already ships both. The question was whether
to migrate, and the honest way to answer it was to run it rather than to reason
about it.

## Evidence

Measured on this repository, 61 files, 2026-09-12 with ty 0.0.80:

| | mypy `--strict` | ty 0.0.80 |
|---|---|---|
| Cold run | 6.9s | **0.21s** (~33×) |
| Findings, default rules | 0 | 10 |
| Findings, `--error all` (library only) | — | 32 |
| Maturity | stable | **Beta, 0.0.80, 121 releases, weekly cadence** |

**ty found a real defect mypy accepts.** Five of the ten diagnostics were one
issue in `cache.Repository.remember`: passing a `Callable[[], T | Awaitable[T]]`
into a second generic of the same shape lets the solver pick
`T = T | Awaitable[T]`. That is a legitimate solution, so the declared `-> T`
silently becomes untrue. mypy picks the narrow solution, ty picks the wide one,
and *both are correct* — which is the tell that the signature was ambiguous
rather than that either checker is wrong. Fixed by making the private helpers
non-generic, with the type information living entirely in the public overloads.

**mypy still checks things ty does not look for.** `warn_unreachable` has caught
a genuine bug in this codebase; ty has no equivalent. mypy's
`disallow_untyped_defs` has no counterpart either, though ruff's `ANN` rules
already enforce annotations here, so that gap is narrower than it appears.

**ty has no `--strict` preset**, only per-rule severity. At `--error all` it
reports 18 `missing-override-decorator` and 8 `unsound-assignment` — dimensions
mypy strict does not cover, currently left at default severity.

## Decision

**Adopt ty as the inner loop. Keep mypy as the gate. Run both in CI.**

- `just types` → ty. At 0.25s, type checking becomes something you run on every
  save, which at 7s it is not. That speed is the actual prize.
- `just types-mypy` → mypy `--strict`.
- `just check` and CI run both, ty first so an obvious error fails the job before
  mypy spends seven seconds finding it.
- The generated template carries the same split, and the generator test asserts
  a freshly generated project is clean under both — a starter that begins red
  teaches people to ignore the tool.

The reasoning for not migrating outright is narrow and specific: a type
checker's entire value is being trustworthy about correctness. A pre-1.0 one
shipping weekly means green today and red next Tuesday for reasons that are not
your code, with no second opinion to arbitrate. That is a bad property in the
one tool whose job is to be the arbiter.

Running two costs one extra CI step of a fifth of a second. The asymmetry is
lopsided enough that the decision is not close.

## Consequences

**Suppressions need both comments.** ty does not recognise mypy's error codes,
so `# type: ignore[arg-type]` leaves the ty diagnostic live. The convention is
now:

```python
transport = ASGITransport(app=app)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
```

Six sites carry this today — five in the library's tests, one in the template's.
This is the main friction of the pair and it disappears whenever one is dropped.

**Two configurations to keep aligned.** `[tool.mypy]` and `[tool.ty]` in the
same `pyproject.toml`, plus the template's. They can drift; the generator test
catches it for the template, nothing catches it for the library.

**Both gate.** ty is not advisory — a ty failure fails CI. That is deliberate:
an advisory checker is one nobody reads. The risk is that a ty regression blocks
a legitimate change, and the escape hatch is a `# ty: ignore` with a comment
saying which release broke it.

## When to drop mypy

Concrete criteria, so this is revisited on evidence rather than mood:

1. ty reaches **1.0** with a stability commitment.
2. It gains an **unreachable-code** rule, or `warn_unreachable`'s value is shown
   to be replaceable by test coverage.
3. Either ty honours `# type: ignore[...]` with foreign codes, or the six paired
   suppressions have been removed by other means.
4. A run of `--error all` on this repository is understood — the 18
   `missing-override-decorator` and 8 `unsound-assignment` findings are each
   either fixed or deliberately disabled.

Until then the pair stays. If ty stalls or its direction changes, the reverse
move is one line in the justfile and one CI step, and nothing in the library
depends on either tool.

## Alternatives considered

**Migrate outright to ty.** Rejected on maturity, not capability — see above.
Reasonable for a project where a broken week is cheap; this one is meant to be
the substrate under other work.

**Stay on mypy alone.** Rejected because ty found a real defect on its first
run, and because 7s versus 0.25s changes when checking happens, not just how
long it takes.

**Pyrefly.** Not evaluated. Adding a third checker to a project that already has
one too many needs a better reason than curiosity.
