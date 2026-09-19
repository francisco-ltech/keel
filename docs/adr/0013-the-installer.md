# ADR 0013 — The installer

**Status:** accepted · **Date:** 2026-09-18 · **Applies to:** distribution, not a phase

## Context

Five phases built the library and the template, and using either still began
with a checkout. The generated project depended on Keel by absolute path, so it
worked on the machine that generated it and nowhere else; the template was
copied by running copier from inside the repository; and there was no `keel`
command at all. Laravel's answer to the same problem is an installer — a
globally installed package that puts a `laravel` binary on the path — and
`laravel new invoices` asks a few questions, copies the skeleton, installs its
dependencies and initialises git. This ADR is the Python spelling of that:

    uv tool install "keel[cli] @ git+https://github.com/francisco-ltech/keel"
    keel new invoices

Git-based, not PyPI, for now: the repository is the only place Keel exists.

## Decisions

### 1. The template is the repository, and its config sits at the root

Copier reads a git template's configuration from the root of the repository it
clones, so `copier.yml` moved from `template/` to the root and names
`template/project` as its `_subdirectory`. That one move is what lets
`keel new` copy straight from a git URL, and what lets `keel update` replay a
project's recorded answers against a newer commit and merge the difference —
copier tracks a project's template revision in `.copier-answers.yml`, and only
a VCS-backed template has revisions.

### 2. A generated project pins the library to the commit its scaffold came from

Copier exposes the template's commit hash to the template, and the generated
`pyproject.toml` writes it into `[tool.uv.sources]` as a git source with that
`rev`. A scaffold and the library it was cut from therefore cannot disagree,
and `keel update` moves both together. The alternative, tracking `main`, is
what turns "the project stopped working" into an archaeology exercise.

`keel_source` is the template's one new question. `git`, the default, does the
above; `path` links a local checkout, editable, which is what a contributor
wants and what the repository's own `just new` and generator suite use. The
`keel_path` question stays, asked only for `path`, and no longer has a default
derived from a temporary clone.

### 3. A thin wrapper, and `argparse`

`keel new` calls copier's Python API with the answers the flags supply, then
runs `uv sync`, `git init` and a first commit, skipping and saying so when a
tool is not on the path. `keel update` calls copier's update. Both are a
function. Declined: `click` or `typer`, because two subcommands over one library
are three flags each and a dependency is a thing every service would carry;
and a hook seam after `new`, because nothing has asked for one that a task in
`copier.yml` could not do.

Copier is an optional extra, `keel[cli]`, imported inside the command. A
service's runtime image carries no scaffolder, and a missing extra says what
to install.

### 4. Copier's own revision for a git source, `HEAD` for a checkout

Copier's default template revision is the latest tag, or the committed `HEAD`
without one, and a git source gets exactly that: the first tag becomes the
default the day it exists, with no code change. The first draft hardcoded
`HEAD` and claimed the tag would take over — the review pointed out that the
hardcoded value was precisely what would have stopped it. A local checkout
given as `--source` is the one case pinned to `HEAD` by name, because that is
the only revision at which copier includes uncommitted changes, and a
contributor generating from a checkout wants the code they are changing.

### 5. The command's exit status is honest

`keel new` returns non-zero when `uv sync` or the first commit failed, and
says so last, after copier's own banner; a zero exit over an uninstalled,
uncommitted scaffold would send the user straight into a `keel update` that
cannot work. `keel update` checks git for unmerged paths afterwards and returns
non-zero naming them: copier writes conflict markers into files and says
nothing, which is the right merge and the wrong report. Copier's own
user-facing errors, and the git failure a dirty-checkout project produces,
reach the user as one line rather than a traceback.

## What was declined

| Declined | What would change it |
|---|---|
| Publishing to PyPI | Nothing technical; a name check, a release workflow and a version discipline. The git install works today and costs nothing. |
| Bundling the template inside the wheel | It would let `keel new` run offline and would break `keel update`, which needs the template's git history. Laravel's installer needs the network too. |
| A shell-script installer | `uv tool install` is the installer. A script would reimplement it, worse. |
| `keel new` asking about the database and Redis | The generated `.env.example` and `just up` cover it. The four questions the template asks are the ones a person cannot default. |

## Consequences

**The generator suite generates from the working tree by passing `--vcs-ref
HEAD` and `keel_source=path`.** Copier includes a local checkout's uncommitted
changes only at `HEAD`, and a suite that generated from the last commit would
be checking the wrong thing.

**`just new` and `just update` are the CLI**, with `--source .` so a contributor
gets an editable link to the checkout they are in.

**The path a project points at is now only the contributor's problem.** A
project generated with the default answers installs Keel from git at a pinned
commit and clones on any machine.

**`.copier-answers.yml` is part of a generated project**, and must be committed
with it: it is what `keel update` reads. The template had never shipped one, so
the `just update` recipe that predates this ADR could not have worked; it does
now.

**A project generated from a dirty checkout cannot be updated.** Copier records
the template commit in the answers file, and for a checkout with uncommitted
changes that commit is a temporary one that exists nowhere afterwards. A user's
`keel new` reads committed history and is unaffected; a contributor who wants
`keel update` to work on a project generates it from a clean checkout, or
regenerates, and the command says so when it happens. The suite's git-source
tests clone the repository and commit the working tree's template on top, so
they exercise the code under review through a real git source.

**Every template file that names the Keel path is gated on `keel_source`.**
The first draft converted `pyproject.toml` and nothing else, so a project
generated the default way shipped a README and a compose bind mount naming
copier's temporary clone, deleted before the command returned — and because
that path is different on every run, every `keel update` conflicted on lines
nobody had edited. The git-source test now asserts no rendered file names the
clone or the source.

## What the review caught

- **The template was half-converted**: the README, the compose file and a
  comment still assumed a path dependency, and rendered copier's deleted temp
  clone for the default answer. Consequences.
- **Every `keel update` conflicted on untouched files**, for the same reason.
- **`keel update` exited zero over conflict markers.** Decision 5.
- **Copier's errors reached the user as tracebacks**, including the
  dirty-checkout case this ADR itself documents. Decision 5.
- **`keel new` exited zero after a failed `uv sync` or a failed commit**, under
  a success banner. Decision 5.
- **`HEAD` was hardcoded** while the docstring and this ADR claimed the first
  tag would take over. Decision 4.
- **`keel update` without the extra blamed `keel new`.**
- **Two comments in `copier.yml` asserted things the answers file disproved**,
  and one told the user to run `copier update`.
- **A dead branch** in the generated `pyproject.toml` for a case the CLI makes
  unreachable.

Tried and held: no root-level file leaks into a project, the first commit
excludes `.env` and `.venv`, `--shape` wins over `--defaults`, `keel update`
advances both the template and the pinned `rev`, and a local edit is never
silently overwritten.

## Verification

- `test_new_from_a_checkout_links_it_by_path_and_commits` — the contributor's
  path, with the editable link, the shape, and the first commit.
- `test_new_from_the_repository_pins_the_library_to_the_templates_commit` —
  the user's path, against this checkout's git history rather than GitHub, and
  the pinned `rev` is the commit the template came from.
- `test_a_missing_copier_says_what_to_install`, and the refusals: a non-empty
  destination, an `update` on a directory Keel did not generate.
- `test_update_of_an_untouched_git_project_leaves_no_conflicts` — the template
  moves one commit, the project merges cleanly, and the `rev` follows.
- `test_update_reports_a_conflict_and_does_not_exit_zero`,
  `test_update_of_a_project_from_a_dirty_checkout_says_what_to_do`,
  `test_new_fails_when_a_follow_up_step_fails` — decision 5.
- The generator suite, unchanged in what it checks, now copies from the
  repository root.
