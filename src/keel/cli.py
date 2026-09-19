"""The ``keel`` command: a new service in one line, and an old one brought forward.

Laravel's installer is what this copies. ``composer global require
laravel/installer`` puts a ``laravel`` binary on the path, and ``laravel new
invoices`` asks a few questions, copies the skeleton, installs its dependencies
and initialises git. The Python spelling is ``uv tool install`` and then::

    keel new invoices
    keel update invoices     # later, when the template has moved

**A thin wrapper over copier, and deliberately nothing more.** Copier already
does the hard parts — questions, rendering, and the update that replays a
project's answers against a newer template and merges the difference — so the
command adds only what a person would otherwise type by hand after it: ``uv
sync``, ``git init`` and a first commit, and the next three commands to run.
Each of those steps is skipped, and says so, when its tool is not on the path.

**The template is the repository.** ``copier.yml`` sits at the repository root
so copier can read a git URL as a template, and the generated project pins the
Keel library to the same commit the scaffold came from, which is the only way
the two cannot disagree. ``--source`` accepts a local checkout instead, which is
how the repository's own ``just new`` and its generator suite work, and how a
contributor links a project to the code they are changing.

**Pattern: none.** Two subcommands over one library are a function each.
Declined: ``click`` or ``typer``, because ``argparse`` covers three flags and a
dependency is a thing every service would carry; and a plugin or hook seam,
because nothing has asked to run after ``new`` that a task in ``copier.yml``
could not.
"""

from __future__ import annotations

import argparse
import shlex
import shutil
import subprocess
import sys
import warnings
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Final

REPOSITORY: Final = "https://github.com/francisco-ltech/keel"
"""Where ``keel new`` copies the template from, and where a project installs Keel from."""

LOCAL_REF: Final = "HEAD"
"""The revision used for a local checkout given as ``--source``.

Copier's own default is the template's latest tag, or its committed ``HEAD``
without one, and that is what a git source gets: the first tag becomes the
default the day it exists, with no change here. A checkout is different — a
contributor generating from it wants the code they are changing — and copier
includes uncommitted changes only when the revision is ``HEAD`` by name.
"""

CONFLICT_HELP: Final = (
    "resolve the markers, then `git add` the files; the template's version is "
    "marked 'after updating'"
)

INSTALL_HINT: Final = "install the cli extra: keel[cli]"

SHAPES: Final = ("api", "worker", "both")
"""What ``--shape`` accepts; the same answers ``copier.yml`` offers."""


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command.

    Args:
        argv: The arguments, or ``None`` for the process's own.

    Returns:
        The exit status.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        handler = args.handler
    except AttributeError:
        parser.print_help()
        return 2
    try:
        return int(handler(args))
    except Exception as exc:
        message = _explain(exc)
        if message is None:
            raise
        print(message, file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    """Return the argument parser.

    Returns:
        The parser, with a ``handler`` default on each subcommand.
    """
    parser = argparse.ArgumentParser(prog="keel", description="Batteries for backend services.")
    parser.add_argument("--version", action="version", version=f"keel {installed_version()}")
    commands = parser.add_subparsers(title="commands")

    new = commands.add_parser("new", help="generate a new service from the template")
    new.add_argument("dest", type=Path, help="where to create it; must not exist")
    new.add_argument(
        "--source",
        default=REPOSITORY,
        help=f"the template: a git URL or a local checkout (default: {REPOSITORY})",
    )
    new.add_argument(
        "--ref",
        help="template revision (default: the latest tag, or HEAD; always HEAD for a checkout)",
    )
    new.add_argument("--shape", choices=SHAPES, help="answer the shape question up front")
    new.add_argument("--defaults", action="store_true", help="take every default; ask nothing")
    new.add_argument("--no-sync", action="store_true", help="do not run `uv sync` afterwards")
    new.add_argument("--no-git", action="store_true", help="do not initialise a repository")
    new.set_defaults(handler=command_new)

    update = commands.add_parser("update", help="bring a generated service up to the template")
    update.add_argument(
        "dest", type=Path, nargs="?", default=Path(), help="the service (default: .)"
    )
    update.add_argument(
        "--ref", help="template revision (default: the latest tag, or HEAD without one)"
    )
    update.add_argument(
        "--data", action="append", default=[], metavar="NAME=VALUE", help="change an answer"
    )
    update.add_argument(
        "--defaults", action="store_true", help="take the default for any new question; ask nothing"
    )
    update.set_defaults(handler=command_update)

    commands.add_parser("version", help="print the installed version").set_defaults(
        handler=lambda _args: print(f"keel {installed_version()}") or 0
    )
    return parser


def installed_version() -> str:
    """Return the installed distribution's version, or ``unknown`` from a checkout.

    Returns:
        The version string.
    """
    try:
        return version("keel")
    except PackageNotFoundError:
        return "unknown"


def command_new(args: argparse.Namespace) -> int:
    """Generate a service, install it, and put it under version control.

    Args:
        args: The parsed arguments.

    Returns:
        The exit status.
    """
    dest: Path = args.dest
    if dest.exists() and any(dest.iterdir()):
        print(f"{dest} already exists and is not empty", file=sys.stderr)
        return 1
    copier = _copier("new")
    if copier is None:
        return 2

    data: dict[str, object] = {}
    if args.shape is not None:
        data["service_shape"] = args.shape
    source = args.source
    ref = args.ref
    local = Path(source)
    if local.is_dir():
        # A checkout: link it by path, editable, so the project follows its code,
        # and at HEAD by name, so its uncommitted changes are what is generated.
        source = str(local.resolve())
        data["keel_source"] = "path"
        data["keel_path"] = source
        ref = ref or LOCAL_REF

    with _quiet_about_dirty_checkouts():
        copier.run_copy(
            source,
            dest,
            data=data,
            vcs_ref=ref,
            defaults=args.defaults,
            unsafe=True,
        )

    problems: list[str] = []
    if not args.no_sync and not _step(["uv", "sync"], cwd=dest, what="install its dependencies"):
        problems.append("dependencies are not installed")
    wants_git = not args.no_git and not (dest / ".git").exists()
    if wants_git:
        committed = (
            _step(["git", "init", "-q"], cwd=dest, what="initialise a repository")
            and _step(["git", "add", "-A"], cwd=dest, what="stage the scaffold")
            and _step(
                ["git", "commit", "-q", "-m", "Scaffold from the Keel template"],
                cwd=dest,
                what="commit the scaffold",
            )
        )
        if not committed:
            problems.append("the scaffold is not committed, which `keel update` needs")
    if problems:
        # Last, so the failure is the last thing read rather than copier's banner.
        print(f"{dest} was generated, but: {'; '.join(problems)}", file=sys.stderr)
        return 1
    return 0


def command_update(args: argparse.Namespace) -> int:
    """Replay a service's answers against the current template and merge the difference.

    The recorded answers stand: only a question the template has gained since,
    or one changed with ``--data``, is asked. Re-asking every answer on every
    update is copier's default and the wrong one for a command run months
    after the answers were given.

    Args:
        args: The parsed arguments.

    Returns:
        The exit status.
    """
    dest: Path = args.dest
    if not (dest / ".copier-answers.yml").is_file():
        print(
            f"{dest} was not generated by `keel new`: no .copier-answers.yml there",
            file=sys.stderr,
        )
        return 1
    copier = _copier("update")
    if copier is None:
        return 2
    data: dict[str, object] = {}
    for item in args.data:
        name, separator, value = item.partition("=")
        if not separator:
            print(f"--data expects NAME=VALUE, got {item!r}", file=sys.stderr)
            return 1
        data[name] = value
    with _quiet_about_dirty_checkouts():
        copier.run_update(
            dest,
            data=data,
            vcs_ref=args.ref,
            defaults=args.defaults,
            skip_answered=True,
            unsafe=True,
            overwrite=True,
        )
    conflicted = _conflicts(dest)
    if conflicted:
        # Copier writes the markers and says nothing; a zero exit would say "done".
        print(f"updated with conflicts in: {', '.join(conflicted)}", file=sys.stderr)
        print(CONFLICT_HELP, file=sys.stderr)
        return 1
    return 0


def _conflicts(dest: Path) -> list[str]:
    """Return the files copier left with conflict markers.

    Args:
        dest: The project.

    Returns:
        The unmerged paths, as git reports them.
    """
    if shutil.which("git") is None:
        return []
    listed = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=U"],
        cwd=dest,
        capture_output=True,
        text=True,
        check=False,
    )
    return listed.stdout.split() if listed.returncode == 0 else []


def _explain(exc: Exception) -> str | None:
    """Return the one-line message an error deserves, or ``None`` for a real traceback.

    Copier raises ``UserMessageError`` for what it means the user to read, and a
    project generated from a checkout with uncommitted changes fails one level
    down, in git, with a commit that exists nowhere — the limit ADR 0013 records
    and the command should state rather than dump.

    Args:
        exc: What the command raised.

    Returns:
        The message, or ``None``.
    """
    name = type(exc).__name__
    text = str(exc)
    if name == "UserMessageError":
        return text
    if name == "ProcessExecutionError" and "did not match any file(s) known to git" in text:
        return (
            "this project records a template commit that does not exist: it was generated "
            "from a checkout with uncommitted changes. Regenerate it from a clean checkout, "
            "or from the repository, and try again"
        )
    return None


def _copier(command: str) -> Any:
    """Import copier, or say what to install.

    Args:
        command: Which subcommand needs it, for the message.

    Returns:
        The module, or ``None`` after printing the hint. ``Any`` because a
        module's attributes are not something a return type can name.
    """
    try:
        import copier
    except ImportError:
        print(f"keel {command} needs copier; {INSTALL_HINT}", file=sys.stderr)
        return None
    return copier


@contextmanager
def _quiet_about_dirty_checkouts() -> Iterator[None]:
    """Silence copier's warning that a local checkout's uncommitted changes were used.

    That is the point of ``--source .``: a contributor generates from the code
    they are changing. Warned about, it is noise; under a suite where warnings
    are errors, it is a failure.

    Yields:
        Nothing; run copier inside the block.
    """
    from copier.errors import DirtyLocalWarning

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DirtyLocalWarning)
        yield


def _step(command: list[str], *, cwd: Path, what: str) -> bool:
    """Run a follow-up command, or say why it was skipped.

    Args:
        command: The command line.
        cwd: Where to run it.
        what: What it does, for the message when it cannot.

    Returns:
        Whether it ran and succeeded.
    """
    tool = command[0]
    if shutil.which(tool) is None:
        print(f"skipped: {tool} is not on PATH, so {what} yourself", file=sys.stderr)
        return False
    completed = subprocess.run(command, cwd=cwd, check=False)
    if completed.returncode != 0:
        print(f"`{shlex.join(command)}` failed; {what} yourself", file=sys.stderr)
        return False
    return True


__all__ = ["LOCAL_REF", "REPOSITORY", "SHAPES", "build_parser", "main"]
