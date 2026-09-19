"""The ``keel`` command.

The generation itself is copier's, and the generator suite proves what comes
out; what this file pins is the wrapper: what it asks copier for, what it does
afterwards, and how it fails when a tool is missing.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from keel.cli import REPOSITORY, build_parser, main

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_new_defaults_to_the_repository_and_copiers_own_revision() -> None:
    """No `--ref` means copier's default: the latest tag, or HEAD without one."""
    args = build_parser().parse_args(["new", "invoices"])

    assert args.dest == Path("invoices")
    assert args.source == REPOSITORY
    assert args.ref is None
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
    (tmp_path / "svc").mkdir()
    (tmp_path / "svc" / ".copier-answers.yml").write_text("")

    assert main(["new", str(tmp_path / "new")]) == 2
    assert "keel new needs copier" in capsys.readouterr().err
    assert main(["update", str(tmp_path / "svc")]) == 2
    assert "keel update needs copier" in capsys.readouterr().err


@pytest.mark.generator
def test_new_fails_when_a_follow_up_step_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A success banner followed by a silent zero after `uv sync` failed is a lie."""
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_uv = fake_bin / "uv"
    fake_uv.write_text("#!/bin/sh\necho 'uv: refusing' >&2\nexit 1\n")
    fake_uv.chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ['PATH']}")
    dest = tmp_path / "svc"

    assert main(["new", str(dest), "--source", str(REPO_ROOT), "--defaults", "--no-git"]) == 1
    err = capsys.readouterr().err
    assert "`uv sync` failed" in err
    assert err.strip().endswith("dependencies are not installed")


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


GIT_IDENTITY = ["-c", "user.name=keel tests", "-c", "user.email=tests@keel.invalid"]
"""So a commit in a scratch clone works where no identity is configured, as in CI."""


def git(*args: str, cwd: Path) -> str:
    """Run git in *cwd* with a fixed identity and return its output."""
    return subprocess.run(
        ["git", *GIT_IDENTITY, *args], cwd=cwd, capture_output=True, text=True, check=True
    ).stdout


@pytest.fixture
def committed_keel(tmp_path: Path) -> Path:
    """A clone of this repository with the working tree's template committed on top.

    A git source reads committed history, and a checkout with uncommitted
    changes makes copier record a commit that exists nowhere, which is what
    breaks `update` afterwards. Cloning and committing the working tree's
    `copier.yml` and `template/` gives a git source that is still the code
    under test rather than the last commit.
    """
    clone = tmp_path / "keel"
    subprocess.run(["git", "clone", "-q", str(REPO_ROOT), str(clone)], check=True)
    shutil.rmtree(clone / "template")
    shutil.copytree(REPO_ROOT / "template", clone / "template")
    shutil.copy2(REPO_ROOT / "copier.yml", clone / "copier.yml")
    git("add", "-A", cwd=clone)
    git("commit", "-q", "--allow-empty", "-m", "the working tree's template", cwd=clone)
    return clone


@pytest.mark.generator
def test_new_from_the_repository_pins_the_library_to_the_templates_commit(
    tmp_path: Path, committed_keel: Path
) -> None:
    """The user's path, against this checkout's own git history rather than GitHub."""
    dest = tmp_path / "svc"
    head = git("rev-parse", "HEAD", cwd=committed_keel).strip()

    # A git+file URL is a git source to copier, not a directory, so `keel_source` stays `git`.
    source = f"git+file://{committed_keel}"
    assert main(["new", str(dest), "--source", source, "--defaults", "--no-sync", "--no-git"]) == 0

    pyproject = (dest / "pyproject.toml").read_text()
    assert f'rev = "{head}"' in pyproject
    assert REPOSITORY in pyproject
    assert "editable" not in pyproject
    assert not (dest / ".git").exists()

    # Copier's clone is gone the moment the command returns; nothing may name it.
    # The answers file records the source URL on purpose: `keel update` reads it.
    for rendered in dest.rglob("*"):
        if rendered.is_file() and rendered.name != ".copier-answers.yml":
            text = rendered.read_text(errors="replace")
            assert "copier._vcs" not in text, f"{rendered.name} names copier's temp clone"
            assert str(committed_keel) not in text, f"{rendered.name} names the source path"
    assert "keel_path" not in (dest / "docker-compose.yml").read_text()


@pytest.mark.generator
def test_update_adds_the_worker_half_to_an_api_project(
    tmp_path: Path, committed_keel: Path
) -> None:
    """The README's promise: adding the other half later is `keel update`, not a new project."""
    dest = tmp_path / "svc"
    common = ["--source", str(committed_keel), "--defaults", "--no-sync"]
    assert main(["new", str(dest), "--shape", "api", *common]) == 0
    assert not (dest / "app" / "worker.py").exists()

    assert main(["update", str(dest), "--data", "service_shape=both", "--defaults"]) == 0

    assert (dest / "app" / "worker.py").is_file()
    assert (dest / "app" / "modules" / "items" / "jobs.py").is_file()
    assert (dest / "app" / "main.py").is_file(), "the API half must survive the update"
    assert "service_shape: both" in (dest / ".copier-answers.yml").read_text()


@pytest.mark.generator
def test_update_of_an_untouched_git_project_leaves_no_conflicts(
    tmp_path: Path, committed_keel: Path
) -> None:
    """The template moves by one commit; a project nobody edited must merge cleanly."""
    dest = tmp_path / "svc"
    source = f"git+file://{committed_keel}"
    assert main(["new", str(dest), "--source", source, "--defaults", "--no-sync"]) == 0
    before = git("rev-parse", "HEAD", cwd=committed_keel).strip()

    errors = committed_keel / "template" / "project" / "app" / "errors.py.jinja"
    errors.write_text(errors.read_text() + "\n# moved by the template\n")
    git("commit", "-q", "-am", "move the template", cwd=committed_keel)
    after = git("rev-parse", "HEAD", cwd=committed_keel).strip()

    assert main(["update", str(dest), "--ref", "HEAD", "--defaults"]) == 0

    assert git("diff", "--name-only", "--diff-filter=U", cwd=dest) == ""
    assert "# moved by the template" in (dest / "app" / "errors.py").read_text()
    pyproject = (dest / "pyproject.toml").read_text()
    assert f'rev = "{after}"' in pyproject and before not in pyproject


@pytest.mark.generator
def test_update_reports_a_conflict_and_does_not_exit_zero(
    tmp_path: Path, committed_keel: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    dest = tmp_path / "svc"
    source = f"git+file://{committed_keel}"
    assert main(["new", str(dest), "--source", source, "--defaults", "--no-sync"]) == 0

    errors = committed_keel / "template" / "project" / "app" / "errors.py.jinja"
    errors.write_text(errors.read_text() + "\n# upstream version\n")
    git("commit", "-q", "-am", "upstream", cwd=committed_keel)
    local = dest / "app" / "errors.py"
    local.write_text(local.read_text() + "\n# my local version\n")
    git("commit", "-q", "-am", "mine", cwd=dest)

    assert main(["update", str(dest), "--ref", "HEAD", "--defaults"]) == 1

    err = capsys.readouterr().err
    assert "conflicts in: app/errors.py" in err
    assert "<<<<<<< before updating" in local.read_text()
    assert "# my local version" in local.read_text(), "a local edit is never silently dropped"


@pytest.mark.generator
def test_update_of_a_project_from_a_dirty_checkout_says_what_to_do(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The limit ADR 0013 records, stated by the command rather than dumped by git."""
    dest = tmp_path / "svc"
    assert main(["new", str(dest), "--source", str(REPO_ROOT), "--defaults", "--no-sync"]) == 0
    marker = REPO_ROOT / "template" / "project" / ".keel-dirty-probe"
    marker.write_text("dirty\n")
    try:
        code = main(["update", str(dest), "--defaults"])
    finally:
        marker.unlink()

    err = capsys.readouterr().err
    assert code == 1
    assert "Traceback" not in err
    assert "checkout with uncommitted changes" in err
