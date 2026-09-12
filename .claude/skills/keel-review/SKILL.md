---
name: keel-review
description: How to run an adversarial correctness review of Keel code, and what it has actually found before. Read this when asked to review, audit, check or critique code in the Keel repo, when a subsystem is finished and before its ADR is written, or when deciding whether a design is ceremony. Includes the standing brief to give a review agent and the classes of defect that have slipped past tests and both type checkers.
---

# Keel — adversarial review

Three of these have run. Each found a real defect that lint, both type checkers
and a green suite had missed. It is the highest-yield step in the loop, so run
it before an ADR is written rather than after.

## Standing brief

Give the reviewer this, adapted:

> Review X adversarially. It passes lint, ty, mypy --strict and its suite, so do
> not report those. Find what the tooling cannot.
>
> Focus, in order: concurrency and atomicity — construct an interleaving that
> breaks it; the backend's real semantics versus what the code assumes; whether
> each design pattern earns its place or is ceremony; anything where a docstring
> claims something the code does not do.
>
> Report findings most severe first, each with a concrete failure: inputs or
> interleaving, then the wrong outcome. If you tried to break something and
> could not, say so — negative results are useful. Three real findings beat
> twenty speculative ones. Do not fix anything; report.

Tell it explicitly that "this should just be a function" is a welcome finding.
Point it at `docs/adr/0000-design-patterns-are-the-bar.md`.

## What has actually been found

Worth probing for the same shapes again:

- **A destructive default.** `flush()` on an unnamespaced store scanned `*` and
  unlinked another application's keys. The test passed because it always set a
  prefix.
- **A guarantee the docstring asserted and the code did not.** An "emulated
  increment is serialised behind a lock" that used `async with lock`, which
  *fails* on contention rather than waiting — 19 of 20 concurrent increments
  lost.
- **A third-party rule that does not fit.** SAQ's sweeper treats a job not yet
  marked active as abandoned; Keel's reserve takes two round trips, so it would
  re-run live jobs. Found by running it, not by reading it.
- **A subclass that bypasses `__init__`.** Adding a field to the parent left it
  unset on the child. Happened twice, in unrelated subsystems.
- **Two implementations of one protocol disagreeing**, each documenting the
  opposite behaviour.
- **A circular import that only fails on one import order.**

## Triage

Verify before acting. Reproduce a claimed defect yourself — a probe script
against the live Postgres or Redis, not reasoning about the code.

Then fix mechanically rather than carefully where you can. The `__init__` bug
recurred because the first fix was "remember to add the field in both places";
the second was a loop over `__slots__` that cannot go stale.

Wrong or overstated findings happen. The reviewer once reported an inert
after-commit hook that had been fixed while it was reading. Check before
believing.

## After

Every real finding gets a regression test named for the defect, and the ADR
records what the review caught. Those are the parts a future reader cannot
reconstruct from the code.
