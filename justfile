# Keel task runner.
#
# Postgres is on 5433 and Redis on 6380, to avoid clashing with anything already
# running on the default ports. Export DATABASE_URL or REDIS_URL to override.

export DATABASE_URL := env_var_or_default("DATABASE_URL", "postgresql+asyncpg://keel:keel@localhost:5433/keel")
export REDIS_URL := env_var_or_default("REDIS_URL", "redis://localhost:6380/0")

# Show the available recipes.
default:
    @just --list --unsorted

# ---------------------------------------------------------------- setup

# Install every dependency, including optional extras.
[group('setup')]
install:
    uv sync --all-extras

# Install the git hooks in .githooks.
[group('setup')]
hooks:
    git config core.hooksPath .githooks
    @echo "hooks installed: pre-commit formats and checks types; pre-push runs the suite"
    @echo "bypass either with --no-verify"

# Remove caches and build artefacts. Leaves .venv and the database alone.
[group('setup')]
clean:
    rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage htmlcov dist build
    find . -type d -name __pycache__ -not -path './.venv/*' -prune -exec rm -rf {} +

# ---------------------------------------------------------------- quality

# Format, then fix what can be fixed automatically.
[group('quality')]
fmt:
    uv run ruff format .
    uv run ruff check --fix .

# Check formatting and lint rules without changing anything.
[group('quality')]
lint:
    uv run ruff check .
    uv run ruff format --check .

# Type-check with ty. Fast enough to run on every save.
[group('quality')]
types:
    uv run ty check packages/keel/src packages/keel/tests

# Type-check with mypy --strict. Slower, and the gate that CI enforces.
[group('quality')]
types-mypy:
    uv run mypy packages/keel/src packages/keel/tests

# Everything CI runs, minus the template generator. Run this before you stop.
[group('quality')]
check: lint types types-mypy test

# ---------------------------------------------------------------- tests

# Extra arguments go straight to pytest, so `just test -k soft_delete -x` works.

# Run the suite, excluding the slow template generator.
#
# Parallel by default: the suite is almost entirely waiting on Postgres and
# Redis, so it drops from ~44s to ~16s. Each worker gets its own database — see
# the `database_url` fixture. Use `test-serial` when a traceback or a debugger
# matters, since xdist makes both awkward.
[group('test')]
test *ARGS:
    uv run pytest -m "not generator" -n auto --dist loadfile {{ ARGS }}

# Run the suite in one process, for debugging.
[group('test')]
test-serial *ARGS:
    uv run pytest -m "not generator" {{ ARGS }}

# Run only what needs no running service — the loop to use while editing.
[group('test')]
test-fast *ARGS:
    uv run pytest -m "not redis and not postgres and not generator" {{ ARGS }}

# Everything the pre-commit hook runs: fast, deterministic, no services needed.
[group('quality')]
quick: lint types types-mypy

# Run only the tests that talk to Postgres or Redis.
[group('test')]
test-services *ARGS:
    uv run pytest -m "redis or postgres" {{ ARGS }}

# Generate the starter template and run its suite, lint and types. Slow.
[group('test')]
test-generator *ARGS:
    uv run pytest -m generator {{ ARGS }}

# Run the suite with a coverage report.
[group('test')]
cov *ARGS:
    uv run pytest -m "not generator" -n auto --dist loadfile --cov --cov-report=term-missing {{ ARGS }}

# Re-run only what failed last time.
[group('test')]
retest *ARGS:
    uv run pytest --last-failed {{ ARGS }}

# ---------------------------------------------------------------- services

# Start Postgres, Redis and Mailpit.
[group('services')]
up:
    docker compose up -d

# Stop them, keeping their data.
[group('services')]
down:
    docker compose down

# Stop them and delete their volumes. Destroys the local database.
[group('services')]
[confirm('This deletes the local database. Continue?')]
nuke:
    docker compose down --volumes

# Follow the service logs.
[group('services')]
logs *ARGS:
    docker compose logs -f {{ ARGS }}

# Open a psql shell on the development database.
[group('services')]
psql:
    docker compose exec postgres psql -U keel -d keel

# Open a redis-cli shell.
[group('services')]
redis:
    docker compose exec redis redis-cli

# Report whether the services are actually answering, not just running.
[group('services')]
[no-exit-message]
doctor:
    #!/usr/bin/env bash
    set -uo pipefail
    echo "DATABASE_URL = $DATABASE_URL"
    echo "REDIS_URL    = $REDIS_URL"
    uv run python - <<'PY'
    import asyncio, os
    async def main() -> None:
        try:
            import asyncpg
            url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
            connection = await asyncio.wait_for(asyncpg.connect(url), timeout=5)
            print("postgres: ok —", await connection.fetchval("select version()"))
            await connection.close()
        except Exception as exc:
            print(f"postgres: FAILED — {type(exc).__name__}: {exc}")
        try:
            from redis.asyncio import Redis
            client = Redis.from_url(os.environ["REDIS_URL"])
            await asyncio.wait_for(client.ping(), timeout=5)
            print("redis:    ok")
            await client.aclose()
        except Exception as exc:
            print(f"redis:    FAILED — {type(exc).__name__}: {exc}")
    asyncio.run(main())
    PY

# ---------------------------------------------------------------- scaffolding

# Generate a new application from the starter template.
[group('scaffold')]
new DEST:
    uv run copier copy --trust template {{ DEST }}

# Re-apply the template to a project generated earlier, bringing it up to date.
[group('scaffold')]
update DEST:
    cd {{ DEST }} && uv run copier update --trust
