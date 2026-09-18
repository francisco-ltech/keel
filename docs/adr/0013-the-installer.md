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

### 4. `HEAD` until there is a release

Copier's default template revision is the latest tag. There are none, and the
command says so by defaulting to `HEAD` rather than relying on the fallback.
The first tag changes that default to the tag, and is the point at which
`keel update` starts moving projects between releases rather than commits.

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
`keel new` reads a committed tree and is unaffected; a contributor who wants
`keel update` to work on a project generates it from a clean checkout, or
regenerates. The suite's update test clones the repository for that reason.

## Verification

- `test_new_from_a_checkout_links_it_by_path_and_commits` — the contributor's
  path, with the editable link, the shape, and the first commit.
- `test_new_from_the_repository_pins_the_library_to_the_templates_commit` —
  the user's path, against this checkout's git history rather than GitHub, and
  the pinned `rev` is the commit the template came from.
- `test_a_missing_copier_says_what_to_install`, and the refusals: a non-empty
  destination, an `update` on a directory Keel did not generate.
- The generator suite, unchanged in what it checks, now copies from the
  repository root.
