"""The starter template, generated and actually run.

A template is documentation that executes, and documentation rots. This test
generates a project into a temporary directory, installs it, and runs its own
suite, lint and type checks — so a change to the core library that breaks the
template fails here rather than the next time someone starts a project.

**Everything here runs three times, once per ``service_shape``.** A template
that can scaffold an API, a worker, or both has three products, and two of them
are the ones nobody generates by hand before shipping a change. The shapes are
not variations on a theme either: ``worker`` has no FastAPI installed at all, so
a stray framework import in a shared module fails there and only there.

The shape-specific assertions are collected in :data:`INVARIANTS` rather than
spread through the tests, because what makes a shape *that shape* is a short
list and it should read as one.

It is slow — three full dependency resolutions and installs — so it is marked
``generator`` and excluded from ``just test``. Run it before finishing any
change to ``keel.database``, ``keel.cache`` or ``keel.queue``'s public surface.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = REPO_ROOT
"""The template's root is the repository: copier reads ``copier.yml`` there."""

if not (TEMPLATE / "copier.yml").is_file():  # pragma: no cover - a layout change, not a run
    raise RuntimeError(
        f"{REPO_ROOT} is not the repository root, so this suite would hand copier "
        f"a path that is not a template. Fix the `parents[...]` above. Moving the "
        f"package once already turned this into 'Local template must be a directory'."
    )

pytestmark = [pytest.mark.generator, pytest.mark.postgres]


def run(
    command: list[str],
    cwd: Path,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run *command*, returning the completed process without raising.

    Args:
        command: Argv to execute.
        cwd: Working directory.
        env: Extra environment variables for the child.

    Returns:
        The completed process, so a failing assertion can print its output.
    """
    return subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=900,
        env=_child_environment(env),
    )


LEAKY_VARIABLES = ("DATABASE_URL", "REDIS_URL", "DB_URL", "CACHE_REDIS_URL")
"""Workspace configuration that must not reach the generated project.

The justfile exports ``DATABASE_URL`` so Keel's own suite can find Postgres, and
a real environment variable outranks a ``.env`` file in pydantic-settings. Left
in place, the generated project would quietly use the workspace's database
instead of its own — which is how this test spent a run writing its tables into
the shared schema and reporting phantom drift.
"""


def _child_environment(extra: dict[str, str] | None) -> dict[str, str]:
    """Build the environment for a command run inside the generated project."""
    environment = {key: value for key, value in os.environ.items() if key not in LEAKY_VARIABLES}
    environment["VIRTUAL_ENV"] = ""
    environment.update(extra or {})
    return environment


def explain(label: str, result: subprocess.CompletedProcess[str]) -> str:
    """Render a failed command's output for an assertion message."""
    return (
        f"{label} failed ({result.returncode})\n"
        f"--- stdout ---\n{result.stdout}\n"
        f"--- stderr ---\n{result.stderr}"
    )


TEMPLATE_DATABASE = "keel_template_check"


@dataclass(frozen=True, slots=True)
class Shape:
    """One value of ``service_shape``, and what makes a project that shape.

    A record rather than four parametrised lists, because the interesting claim
    is per shape and not per assertion: "a worker has no FastAPI, no ``main.py``
    and no routers" is one sentence about one product, and splitting it across
    the tests that check each half is how a shape acquires a missing invariant
    nobody notices.

    Attributes:
        name: The ``service_shape`` answer.
        present: Files this shape must scaffold, on top of :data:`ALWAYS`.
        absent: Files this shape must *not* scaffold. The half of the contract
            that catches a conditional that quietly stopped conditioning.
        importable: Distributions that must be installed in its virtualenv.
        uninstallable: Distributions that must not be — an API replica carrying
            a worker runtime is the cost this question exists to avoid.
        variables: Extra ``.env.example`` entries, with probe values.
        processes: The services ``docker-compose.yml`` must define beyond the
            two backing stores, one per entrypoint this shape scaffolds.
        undocumented: Strings the shipped prose must not contain, because they
            name a subsystem this shape does not have.
    """

    name: str
    present: tuple[str, ...] = ()
    absent: tuple[str, ...] = ()
    importable: tuple[str, ...] = ()
    uninstallable: tuple[str, ...] = ()
    variables: dict[str, str] = field(default_factory=dict)
    processes: tuple[str, ...] = ()
    undocumented: tuple[str, ...] = ()


ALWAYS = (
    "pyproject.toml",
    "alembic.ini",
    "app/settings.py",
    "app/errors.py",
    # Authorization is domain logic, not an HTTP concern: the rules are asked by
    # the services, so a worker-only project is held to them too.
    "app/policies.py",
    "tests/test_policies.py",
    "app/modules/__init__.py",
    "app/modules/users/service.py",
    "app/modules/users/repository.py",
    "app/modules/users/models.py",
    "app/modules/users/schemas.py",
    "app/migrations/env.py",
    "app/migrations/versions/0001_initial.py",
    "tests/conftest.py",
)
"""The layout every shape shares. One codebase and one schema is the claim; this
tuple is what it means concretely — the domain, the settings and the migrations
do not vary with the entrypoint."""

API_FILES = (
    "app/main.py",
    # The request-id middleware is ASGI vocabulary, so it is the API half's for
    # the same reason `security.py` is.
    "app/observability.py",
    # The bearer dependency and the login surface are HTTP, so they are the API
    # half — a worker-only project importing either would import FastAPI.
    "app/security.py",
    "app/modules/sessions/service.py",
    "app/modules/sessions/router.py",
    "app/modules/users/router.py",
    "app/modules/items/router.py",
    "tests/test_sessions.py",
    "tests/test_items.py",
)
WORKER_FILES = (
    "app/worker.py",
    "app/healthcheck.py",
    "app/modules/items/jobs.py",
    "app/migrations/versions/0002_queue_tables.py",
    "tests/test_jobs.py",
)

QUEUE_VARIABLES = {
    "QUEUE_DRIVER": "null",
    "QUEUE_PREFIX": "probe-queue",
    "QUEUE_CONCURRENCY": "23",
    "WORKER_HEALTH_FILE": "/tmp/probe-worker.health",
    "WORKER_HEALTH_MAX_AGE": "29",
    "WORKER_METRICS_PORT": "9464",
}

TOKEN_VARIABLES = {
    "TOKEN_DRIVER": "memory",
    "TOKEN_PREFIX": "probe-tokens",
    # A number rather than `never`: the settings object holds `None` for that
    # spelling, so it would not survive the round trip this test asserts on.
    # `tests/test_sessions.py` in the generated project covers `never`.
    "TOKEN_TTL": "3607",
}
"""Bearer tokens are the API half's: a worker issues and resolves none."""

INSPECTOR_VARIABLES = {
    "INSPECTOR_RETAIN": "23",
    "INSPECTOR_PARAMETERS": "true",
}
"""The request inspector is the API half's too: it records requests, and a worker serves none."""

TOKEN_PROSE = ("TOKEN_", "fake_tokens", "issue_token", "INSPECTOR_", "/_inspector")
"""Names only an HTTP edge has. A worker-only project issues no bearer token and
serves no inspector, so prose mentioning either sends its reader looking for a
subsystem that is not there."""

QUEUE_PROSE = ("fake_queue", "dispatch", "QUEUE_")
"""The mirror image, for a project with no worker."""

SHAPES = (
    Shape(
        name="api",
        present=API_FILES,
        absent=WORKER_FILES,
        # `pwdlib` arrives through the `keel[auth]` extra rather than directly,
        # and every shape hashes: the users service does it on create.
        importable=("fastapi", "uvicorn", "pwdlib", "prometheus_client"),
        # The point of the question. `saq` is a worker runtime, and a replica
        # that only serves HTTP should not ship one.
        uninstallable=("saq",),
        variables=TOKEN_VARIABLES | INSPECTOR_VARIABLES,
        processes=("api",),
        undocumented=QUEUE_PROSE,
    ),
    Shape(
        name="worker",
        present=WORKER_FILES,
        absent=API_FILES,
        importable=("saq", "pwdlib", "prometheus_client"),
        # Not merely unused: nothing in a worker-only project may import
        # FastAPI, so a shared module that quietly does fails here.
        uninstallable=("fastapi", "uvicorn"),
        variables=QUEUE_VARIABLES,
        processes=("worker",),
        undocumented=TOKEN_PROSE,
    ),
    Shape(
        name="both",
        present=API_FILES + WORKER_FILES,
        importable=("fastapi", "uvicorn", "saq", "pwdlib", "prometheus_client"),
        variables=QUEUE_VARIABLES | TOKEN_VARIABLES | INSPECTOR_VARIABLES,
        processes=("api", "worker"),
    ),
)


@pytest.fixture(scope="module", params=SHAPES, ids=[shape.name for shape in SHAPES])
def shape(request: pytest.FixtureRequest) -> Shape:
    """The service shape under test.

    Module-scoped and parametrised, so each shape is generated and installed
    once and every test below runs against all three.
    """
    chosen: Shape = request.param
    return chosen


@pytest.fixture(scope="module")
def isolated_database_url(shape: Shape, database_url: str) -> Iterator[str]:
    """Create a database used only by the generated project, and drop it after.

    The drift check compares the models against everything in the schema, so a
    table belonging to some other test reads as "a table the migrations do not
    know about" and fails the assertion. Sharing a database with the rest of the
    suite made this test report drift that did not exist — which is worse than
    no check, because the next person learns to ignore it.

    One database *per shape*, for the same reason: the shapes do not agree about
    which tables should exist, so a worker's ``keel_failed_jobs`` left in a
    shared database would read as drift to the API shape.
    """
    import anyio
    import asyncpg

    admin_url = database_url.replace("+asyncpg", "")
    name = f"{TEMPLATE_DATABASE}_{shape.name}"

    async def administer(statement: str) -> None:
        connection = await asyncpg.connect(admin_url)
        try:
            await connection.execute(statement)
        finally:
            await connection.close()

    anyio.run(administer, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    anyio.run(administer, f'CREATE DATABASE "{name}"')
    yield (
        admin_url.rsplit("/", 1)[0].replace("postgresql://", "postgresql+asyncpg://") + f"/{name}"
    )
    anyio.run(administer, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture(scope="module")
def generated(
    tmp_path_factory: pytest.TempPathFactory, shape: Shape, isolated_database_url: str
) -> Iterator[Path]:
    """A freshly generated project, installed and ready to run.

    Args:
        tmp_path_factory: Pytest's temporary directory factory.
        shape: Which ``service_shape`` to generate.
        isolated_database_url: A database used only by this shape.

    Yields:
        The generated project's root.
    """
    if shutil.which("uv") is None:  # pragma: no cover — uv is how this runs
        pytest.skip("uv is not on PATH")

    destination = tmp_path_factory.mktemp(f"generated-{shape.name}") / "app"
    result = run(
        [
            "uv",
            "run",
            "copier",
            "copy",
            "--trust",
            "--defaults",
            # HEAD rather than the latest tag, and dirty: the suite must generate
            # from this working tree, which is what it is checking.
            "--vcs-ref",
            "HEAD",
            "--data",
            f"service_shape={shape.name}",
            # By path, not by git: the project must link this checkout, whatever
            # the repository holds and wherever the checkout lives (as in CI).
            "--data",
            "keel_source=path",
            "--data",
            f"keel_path={REPO_ROOT}",
            str(TEMPLATE),
            str(destination),
        ],
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, explain("copier copy", result)

    # Its own database, via `.env` rather than per-command: the app, Alembic and pytest
    # all read settings there, and redirecting one leaves the rest on the shared one.
    (destination / ".env").write_text(f"DATABASE_URL={isolated_database_url}\n")

    installed = run(["uv", "sync"], cwd=destination)
    assert installed.returncode == 0, explain("uv sync", installed)

    yield destination
    # Nothing to clean up: the isolated_database_url fixture drops the whole
    # database, so there is no shared schema left behind.


def test_the_generated_project_has_the_expected_shape(generated: Path, shape: Shape) -> None:
    """The layout is the convention; a generator that drifts from it is a bug."""
    for relative in ALWAYS + shape.present:
        assert (generated / relative).is_file(), f"{shape.name}: missing {relative}"


def test_a_shape_scaffolds_only_its_own_half(generated: Path, shape: Shape) -> None:
    """The other half of the contract.

    A conditional that stops conditioning still passes every "is this file
    here?" assertion, and the result is an API-only project carrying a worker
    entrypoint that was never installed against.
    """
    for relative in shape.absent:
        assert not (generated / relative).exists(), (
            f"{shape.name}: {relative} belongs to the other half"
        )


def test_compose_runs_every_entrypoint_the_shape_scaffolds(generated: Path, shape: Shape) -> None:
    """A shape that scaffolds an entrypoint must be able to run it.

    The api-only shape shipped a compose file with no ``api`` service for as
    long as this went unchecked: both processes sat behind one conditional on
    the worker, so the half that needs no worker got nothing to run.
    """
    compose = yaml.safe_load((generated / "docker-compose.yml").read_text())
    assert set(compose["services"]) == {"postgres", "redis", "mailpit", *shape.processes}

    for process in shape.processes:
        assert compose["services"][process]["profiles"] == ["app"], (
            f"{shape.name}: {process} must stay behind the app profile so "
            f"`just up` starts only the backing stores"
        )


def test_a_shape_installs_only_what_its_entrypoints_need(generated: Path, shape: Shape) -> None:
    """Dependencies follow the shape, checked against the real virtualenv.

    Reading ``pyproject.toml`` would only prove the template wrote the right
    string. What matters is what ended up installed — a transitive dependency
    can put FastAPI in a worker's environment without anyone asking for it.
    """
    names = sorted({*shape.importable, *shape.uninstallable})
    probe = (
        "import importlib.util as u;"
        f"print(' '.join(n for n in {names!r} if u.find_spec(n) is not None))"
    )
    result = run(["uv", "run", "python", "-c", probe], cwd=generated)
    assert result.returncode == 0, explain("dependency probe", result)
    installed = set(result.stdout.split())

    assert set(shape.importable) <= installed, (
        f"{shape.name}: missing {sorted(set(shape.importable) - installed)}"
    )
    assert not (set(shape.uninstallable) & installed), (
        f"{shape.name}: should not ship {sorted(set(shape.uninstallable) & installed)}"
    )


def test_an_api_process_does_not_import_a_worker_runtime(generated: Path, shape: Shape) -> None:
    """`keel.queue` exports the worker lazily, and the template must not undo it.

    An API replica dispatches and never consumes, so importing ``app.main``
    must not drag in ``keel.queue.worker`` or SAQ behind it. One eager
    ``from keel.queue import Worker`` in a shared module would put a worker
    runtime in every web process, and nothing would fail — it would just cost.
    """
    if "app/main.py" not in shape.present:
        pytest.skip("no API entrypoint in this shape")

    probe = (
        "import sys, app.main;"
        "print(' '.join(n for n in ('keel.queue.worker', 'saq') if n in sys.modules))"
    )
    result = run(["uv", "run", "python", "-c", probe], cwd=generated)
    assert result.returncode == 0, explain("lazy-export probe", result)
    assert result.stdout.strip() == "", (
        f"{shape.name}: importing app.main pulled in {result.stdout.strip()}"
    )


def test_no_shape_documents_a_subsystem_it_does_not_have(generated: Path, shape: Shape) -> None:
    """The prose is conditioned on the shape too, and nothing else checks it.

    Code that mentions an absent half fails to import; prose that mentions one
    just misleads, so it rots quietly. It had rotted in four places at once: a
    worker README describing a lifespan "minus tokens" and refresh-token
    rotation via ``revoke`` plus ``issue``, and a worker ``CLAUDE.md`` carrying
    the whole verify-against-``None`` rule and a ``fake_tokens()`` rule
    justified by "a test that signs in" — in a project that cannot sign in. Two
    more instances had been fixed by eye in the same pass; the two halves of
    that outcome are the argument for checking it mechanically.
    """
    for name in ("README.md", "CLAUDE.md"):
        prose = (generated / name).read_text()
        found = [marker for marker in shape.undocumented if marker in prose]
        assert not found, f"{shape.name}: {name} mentions {found}, which it does not have"


def test_the_generated_project_keeps_models_and_schemas_apart(generated: Path) -> None:
    """The one layout rule worth enforcing mechanically.

    A schema module that imports models is how ORM objects start leaking to the
    wire, taking their lazy-loading behaviour with them.
    """
    schemas = (generated / "app/modules/users/schemas.py").read_text()
    assert "models import" not in schemas
    assert "from app.modules.users.models" not in schemas


def test_the_generated_project_holds_no_session_dependency(generated: Path) -> None:
    """ADR 0002, enforced against the thing people actually copy from.

    A `Depends`-yielded session is the shape with the open FastAPI deadlock, so
    the template must never demonstrate it.
    """
    for path in generated.rglob("app/**/*.py"):
        source = path.read_text()
        assert "yield session" not in source, f"{path.name} yields a session from a dependency"
        assert "def get_session" not in source, f"{path.name} defines a session dependency"


def test_migrations_apply_to_an_empty_database(generated: Path) -> None:
    result = run(["uv", "run", "alembic", "upgrade", "head"], cwd=generated)
    assert result.returncode == 0, explain("alembic upgrade head", result)


def test_the_generated_suite_passes(generated: Path) -> None:
    result = run(["uv", "run", "pytest", "-q"], cwd=generated)
    assert result.returncode == 0, explain("generated pytest", result)


def test_the_generated_project_is_clean_and_typed(generated: Path) -> None:
    """The template has to meet the same bar as the library it demonstrates."""
    linted = run(["uv", "run", "ruff", "check", "."], cwd=generated)
    assert linted.returncode == 0, explain("generated ruff check", linted)

    formatted = run(["uv", "run", "ruff", "format", "--check", "."], cwd=generated)
    assert formatted.returncode == 0, explain("generated ruff format", formatted)

    typed = run(["uv", "run", "mypy", "app", "tests"], cwd=generated)
    assert typed.returncode == 0, explain("generated mypy", typed)

    # The template ships both checkers, so both have to be clean in the output —
    # a generated project that starts red teaches people to ignore the tool.
    checked = run(["uv", "run", "ty", "check", "app", "tests"], cwd=generated)
    assert checked.returncode == 0, explain("generated ty", checked)


def test_autogenerating_after_the_shipped_migration_finds_no_drift(generated: Path) -> None:
    """The shipped migration must match the shipped models.

    A spurious diff here means a type, a server default or the naming convention
    is not what the migration thinks it is — and every project generated from
    the template would inherit the discrepancy on day one.
    """
    result = run(
        ["uv", "run", "alembic", "revision", "--autogenerate", "-m", "drift-check"],
        cwd=generated,
    )
    assert result.returncode == 0, explain("alembic revision --autogenerate", result)

    versions = generated / "app/migrations/versions"
    drift = [path for path in versions.glob("*.py") if "drift_check" in path.name]
    assert len(drift) == 1, "expected exactly one generated migration"

    body = drift[0].read_text()
    assert "op.create_table" not in body, f"models and shipped migration have drifted:\n{body}"
    assert "op.drop_table" not in body, f"models and shipped migration have drifted:\n{body}"
    assert "op.add_column" not in body, f"models and shipped migration have drifted:\n{body}"


def test_no_subsystem_namespace_nests_inside_another(generated: Path) -> None:
    """One Redis, several subsystems, and ``flush()`` scans a whole prefix.

    The cache, the queue and the token store all read the same ``REDIS_URL``, so
    a namespace that is a *parent* of another's — ``<slug>`` against
    ``<slug>:queue`` — means clearing the cache deletes every pending job and
    signs everybody out. Keel's own defaults shipped that way from Phase 3 until
    an adversarial review found it, and the template inherited it; this is the
    guard that keeps it fixed.
    """
    probe = "\n".join(
        [
            "import json, os",
            "from app.settings import Settings",
            "os.environ.pop('DATABASE_URL', None)",
            "groups = Settings(_env_file=None).model_dump(mode='json')",
            "print(json.dumps({name: group['prefix'] for name, group in groups.items()",
            "                  if isinstance(group, dict) and 'prefix' in group}))",
        ]
    )
    result = run(["uv", "run", "python", "-c", probe], cwd=generated)
    assert result.returncode == 0, explain("namespace probe", result)

    prefixes: dict[str, str] = json.loads(result.stdout)
    assert "cache" in prefixes, f"no namespaced subsystem found: {prefixes}"
    for name, value in prefixes.items():
        others = [other for key, other in prefixes.items() if key != name]
        nested = [other for other in others if value == other or value.startswith(f"{other}:")]
        assert not nested, f"{name}'s namespace {value!r} sits inside {nested}: {prefixes}"


def test_every_documented_environment_variable_is_actually_read(
    generated: Path, shape: Shape
) -> None:
    """`.env.example` must not document names the settings ignore.

    Regression: the generated settings used ``env_nested_delimiter="__"``, so
    ``DATABASE_URL`` and ``CACHE_STORE`` bound to nothing while
    ``extra="ignore"`` swallowed them silently. The app fell back to defaults
    and looked fine — the worst way for configuration to fail, because it fails
    quietly and only in the environment where the default is wrong.

    This walks every assignment in the shipped `.env.example` and asserts the
    settings object actually changes when it is set.
    """
    documented = [
        line.split("=", 1)[0].strip()
        for line in (generated / ".env.example").read_text().splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    ]
    assert documented, "no variables found in .env.example"

    probe = "\n".join(
        [
            "import json, os",
            "from app.settings import Settings",
            "os.environ.pop('DATABASE_URL', None)",
            "settings = Settings(_env_file=None)",
            "print(json.dumps(settings.model_dump(mode='json')))",
        ]
    )
    baseline = run(["uv", "run", "python", "-c", probe], cwd=generated)
    assert baseline.returncode == 0, explain("settings baseline", baseline)

    overrides = {
        "DATABASE_URL": "postgresql+asyncpg://probe@localhost/probe",
        "DB_ECHO": "true",
        "DB_POOL_SIZE": "17",
        "DB_MAX_OVERFLOW": "19",
        "DB_STATEMENT_TIMEOUT": "11",
        "REDIS_URL": "redis://localhost:6399/7",
        "CACHE_STORE": "null",
        "CACHE_PREFIX": "probe-prefix",
        "CACHE_TTL": "13",
        "HASHING_TIME_COST": "2",
        # Above Keel's memory-hardness floor: the config refuses anything lower,
        # so a probe below it would fail at load rather than prove anything.
        "HASHING_MEMORY_COST": "9216",
        "HASHING_PARALLELISM": "1",
        "LOG_LEVEL": "DEBUG",
        "LOG_FORMAT": "text",
        "MAIL_DRIVER": "log",
        "MAIL_FROM": "probe@example.com",
        "MAIL_HOST": "mail.probe",
        "MAIL_PORT": "2525",
        "MAIL_USERNAME": "probe-user",
        "MAIL_PASSWORD": "probe-pass",
        "MAIL_SECURITY": "starttls",
        "MAIL_TIMEOUT": "7.5",
        "APP_NAME": "Probe App",
        "DEBUG": "true",
        "METRICS_ENABLED": "false",
        **shape.variables,
    }
    unread = sorted(set(documented) - set(overrides))
    assert not unread, f".env.example documents variables this test does not probe: {unread}"
    unwritten = sorted(set(shape.variables) - set(documented))
    assert not unwritten, f"{shape.name} reads variables .env.example never mentions: {unwritten}"

    changed = run(["uv", "run", "python", "-c", probe], cwd=generated, env=overrides)
    assert changed.returncode == 0, explain("settings with overrides", changed)
    assert changed.stdout.strip() != baseline.stdout.strip(), (
        "setting every documented variable changed nothing — they are not being read"
    )

    rendered = changed.stdout
    expected_values = (
        "probe-prefix",
        "17",
        "6399",
        "9216",
        "mail.probe",
        "2525",
        "DEBUG",
        "Probe App",
        *shape.variables.values(),
    )
    for expected in expected_values:
        assert expected in rendered, f"{expected!r} did not reach the settings object:\n{rendered}"
