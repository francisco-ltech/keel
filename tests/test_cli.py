"""The ``keel`` command.

The generation itself is copier's, and the generator suite proves what comes
out; what this file pins is the wrapper: what it asks copier for, what it does
afterwards, and how it fails when a tool is missing.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from keel.cli import DEFAULT_REF, REPOSITORY, build_parser, main

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_new_defaults_to_the_repository_at_head() -> None:
    args = build_parser().parse_args(["new", "invoices"])

    assert args.dest == Path("invoices")
    assert args.source == REPOSITORY
    assert args.ref == DEFAULT_REF
    assert args.shape is None


def test_the_shape_is_one_of_the_templates_answers(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["new", "x", "--shape", "monolith"])
    assert "invalid choice" in capsys.readouterr().err


def test_no_command_prints_help_and_fails(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 2
    assert "commands" in capsys.readouterr().out


def test_version_prints_something(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["version"]) == 0
    assert capsys.readouterr().out.startswith("keel ")


def test_new_refuses_a_non_empty_destination(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "something").write_text("here")

    assert main(["new", str(tmp_path)]) == 1
    assert "not empty" in capsys.readouterr().err


def test_update_refuses_a_directory_keel_did_not_generate(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["update", str(tmp_path)]) == 1
    assert ".copier-answers.yml" in capsys.readouterr().err


def test_a_missing_copier_says_what_to_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setitem(sys.modules, "copier", None)

    assert main(["new", str(tmp_path / "svc")]) == 2
    assert "keel[cli]" in capsys.readouterr().err


@pytest.mark.generator
def test_new_from_a_checkout_links_it_by_path_and_commits(tmp_path: Path) -> None:
    """The contributor's path: `just new` and the generator suite both take it."""
    dest = tmp_path / "svc"

    assert (
        main(
            [
                "new",
                str(dest),
                "--source",
                str(REPO_ROOT),
                "--shape",
                "api",
                "--defaults",
                "--no-sync",
            ]
        )
        == 0
    )

    pyproject = (dest / "pyproject.toml").read_text()
    assert f'keel = {{ path = "{REPO_ROOT}", editable = true }}' in pyproject
    assert (dest / "app" / "main.py").is_file()
    assert not (dest / "app" / "worker.py").exists()
    log = subprocess.run(
        ["git", "log", "--oneline"], cwd=dest, capture_output=True, text=True, check=True
    ).stdout
    assert "Scaffold from the Keel template" in log
    assert (dest / ".copier-answers.yml").is_file()


@pytest.fixture
def committed_keel(tmp_path: Path) -> Path:
    """A clean clone of this repository at HEAD, or a skip until the template is committed.

    A git source reads the committed tree, and a checkout with uncommitted
    changes makes copier record a commit that exists nowhere, which is what
    breaks `update` afterwards. The clone is what a user's `keel new` sees.
    """
    committed = subprocess.run(
        ["git", "show", "HEAD:copier.yml"], cwd=REPO_ROOT, capture_output=True, check=False
    )
    if committed.returncode != 0:
        pytest.skip("a git source reads the committed tree; commit copier.yml at the root first")
    clone = tmp_path / "keel"
    subprocess.run(["git", "clone", "-q", str(REPO_ROOT), str(clone)], check=True)
    return clone


@pytest.mark.generator
def test_new_from_the_repository_pins_the_library_to_the_templates_commit(
    tmp_path: Path, committed_keel: Path
) -> None:
    """The user's path, against this checkout's own git history rather than GitHub."""
    dest = tmp_path / "svc"
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=committed_keel, capture_output=True, text=True, check=True
    ).stdout.strip()

    # A git+file URL is a git source to copier, not a directory, so `keel_source` stays `git`.
    source = f"git+file://{committed_keel}"
    assert main(["new", str(dest), "--source", source, "--defaults", "--no-sync", "--no-git"]) == 0

    pyproject = (dest / "pyproject.toml").read_text()
    assert f'rev = "{head}"' in pyproject
    assert REPOSITORY in pyproject
    assert "editable" not in pyproject
    assert not (dest / ".git").exists()


@pytest.mark.generator
def test_update_adds_the_worker_half_to_an_api_project(
    tmp_path: Path, committed_keel: Path
) -> None:
    """The README's promise: adding the other half later is `keel update`, not a new project."""
    dest = tmp_path / "svc"
    common = ["--source", str(committed_keel), "--defaults", "--no-sync"]
    assert main(["new", str(dest), "--shape", "api", *common]) == 0
    assert not (dest / "app" / "worker.py").exists()

    assert main(["update", str(dest), "--data", "service_shape=both"]) == 0

    assert (dest / "app" / "worker.py").is_file()
    assert (dest / "app" / "modules" / "items" / "jobs.py").is_file()
    assert (dest / "app" / "main.py").is_file(), "the API half must survive the update"
    assert "service_shape: both" in (dest / ".copier-answers.yml").read_text()
