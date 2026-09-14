---
name: keel-template
description: How to change the Keel copier template safely and verify it, including the three service shapes and the traps that have broken generated projects before. Read this BEFORE editing anything under template/, before adding a copier question, and before changing anything in src/keel that a generated project imports. Covers why the generator suite is the only real check and what it costs.
---

# Keel — changing the starter template

`template/` generates a service that is an API, a worker, or both, from one
codebase and one image. Editing it is not like editing library code: nothing
type-checks until it is generated, so the generator suite is the only real
check.

## Verify

```sh
just test-generator          # all three shapes, ~70s
```

That generates each shape, installs it, migrates, runs its own suite, and runs
ruff, ty and mypy inside it. Nothing else proves a template change works.

To look at output by hand:

```sh
uv run copier copy --trust --defaults \
  --data service_shape=worker \
  --data keel_path="$PWD" \
  template /tmp/gen && cd /tmp/gen && uv sync && just check
```

Always pass `keel_path` explicitly.

## Traps that have already bitten

- **A default that is only right on one machine.** `keel_path` defaulted to an
  absolute home directory, and the generator test passed `--defaults`, so CI
  generated projects pointing at a directory that did not exist. Derive from
  `{{ _copier_conf.src_path }}` and pass it explicitly in tests.
- **A module a worker cannot import.** `errors.py` imported FastAPI at module
  scope, and the job handler catches its exceptions — so a worker-only project
  could not import its own error taxonomy. Keep exception classes
  framework-free; put the HTTP renderer behind `has_api`.
- **Alembic proposing to drop Keel's own tables.** `FailedJob` and `ScheduleRun`
  only join `Model.metadata` when imported, so `migrations/env.py` must import
  them. Autogenerate silently emits `drop_table` otherwise.
- **A cycle between `jobs.py` and `service.py`.** The service dispatches the job
  and the handler calls the service. Defer one import inside the function.
- **Workspace environment leaking into the generated project.** The justfile
  exports `DATABASE_URL`, and a real variable outranks a `.env` file in
  pydantic-settings, so a generated project silently used the wrong database.
  The generator test strips those before running child commands.

## Shape rules

`api` gets no `worker.py` and **no saq**. `worker` gets no `main.py` and **no
fastapi or uvicorn**. `both` gets everything. Conditional generation is a
templated `_exclude` in `copier.yml` driven by two computed answers,
`has_api`/`has_worker` — not `{% if %}` in filenames, which scatters one
decision across a dozen directory listings.

Only files ending `.jinja` are rendered; the rest are copied verbatim. That is
load-bearing for `app/migrations/script.py.mako`, which is Alembic's own Mako
template and must reach the generated project untouched.

The shape decides what is *scaffolded*, never what is possible. Adding the other
half later must be writing a file, not regenerating.

## Generated code holds the same standard

Ruff with docstrings and annotations, both type checkers, comments no longer
than two lines. A starter that begins red teaches people to ignore the tools.
