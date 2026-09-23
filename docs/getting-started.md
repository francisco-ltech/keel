# Getting started

You need [uv](https://docs.astral.sh/uv/), [just](https://just.systems/) and
Docker, on macOS, Linux or Windows.

## Generate a service

```bash
uv tool install "keel[cli] @ git+https://github.com/francisco-ltech/keel"
keel new invoices     # asks for a name, a description and the shape
cd invoices
just up               # Postgres on 5433, Redis on 6380 and Mailpit, via Docker Compose
just dev              # the app in containers next to them: http://localhost:8000/docs
```

The shape is `api`, `worker` or `both`. `keel new invoices --defaults` asks
nothing. The app containers migrate the database on start, so `just dev` is
enough. `just migrate` applies migrations from the host, and `just test` runs
the project's suite against the same Postgres.

`keel new` copies the template from this repository at its latest release,
pins the project's Keel dependency to the commit the scaffold came from, runs
`uv sync`, and makes the first commit. `keel update invoices` brings a project
forward to a newer release. Releases are tags, listed in the
[changelog](../CHANGELOG.md); how the installer works is
[ADR 0013](adr/0013-the-installer.md).

That install line takes the `keel` command from the tip of `main`. The
template it generates from, and the Keel version a project pins, come from the
latest release regardless. To hold the command itself at a release, name the
tag: `uv tool install "keel[cli] @ git+https://github.com/francisco-ltech/keel@v0.2.0"`.

`just up` also creates the `keel` database the defaults point at. If Postgres,
Redis and Mailpit already run on those ports, skip `just up`, create a
database, and point `DATABASE_URL`, `REDIS_URL` and `MAIL_HOST` in `.env` at
them.

## What the service has on day one

- Registration at `POST /users`, sign-in at `POST /sessions` that issues a
  bearer token, `GET /sessions/current` for who you are, and an `items` module
  showing the shape every domain follows: models, schemas, repository, service
  and router, plus one line in the registry. `/items` is the caller's own, with
  the owner taken from the session; `/users/{pid}/items` is how an admin acts
  for somebody. A primary key never crosses the wire. All of it in the OpenAPI
  document at `/docs`. See [authentication](authentication.md).
- A welcome mail on registration, sent only once the transaction commits and
  caught by Mailpit at http://localhost:8025. See [mail](mail.md).
- Every write inside a unit of work, and no session ever held across a
  request. Authorization policies checked in the service, so a job and a route
  share one answer to "may this caller do that". See [database](database.md).
- With the `both` shape, the API and the worker side by side from one
  codebase: creating an item dispatches a job that runs only after the commit,
  and a nightly schedule prunes dead letters. See [queue](queue.md).
- Structured JSON logs carrying a request id from the API call into the worker
  that runs its job, `/health` and `/ready`, Prometheus metrics at `/metrics`,
  and a request inspector at `/_inspector` under `DEBUG=true`. See
  [observability](observability.md).
- Alembic migrations under an advisory lock, a test suite against the real
  Postgres inside a transaction rolled back per test, and Docker Compose for
  the services and the app.

## Try it

With `just dev` running, register a user and sign in:

```bash
curl -s -X POST localhost:8000/users -H 'Content-Type: application/json' \
  -d '{"email":"ada@example.com","full_name":"Ada Lovelace","password":"correct-horse-battery-staple"}'

curl -s -X POST localhost:8000/sessions -H 'Content-Type: application/json' \
  -d '{"email":"ada@example.com","password":"correct-horse-battery-staple"}'
```

The first answers with the user, and its welcome mail is at
http://localhost:8025. The second answers with `access_token`, readable there
and nowhere else: tokens are stored hashed. From here the session names the
caller, so nothing below sends an id over the wire:

```bash
TOKEN=...      # access_token from the sign-in

curl -s localhost:8000/sessions/current -H "Authorization: Bearer $TOKEN"

curl -s -X POST localhost:8000/items \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'X-Request-ID: demo-1' -d '{"name":"First item"}'

curl -s localhost:8000/items -H "Authorization: Bearer $TOKEN"
```

Creating the item dispatches a job, pushed only after the transaction commits.
With the `both` shape, `docker compose logs worker | grep demo-1` shows the
worker running it a moment later, with the request id the request carried.
Then the operator's endpoints, no token needed: `/ready` asks every dependency,
`/metrics` is what Prometheus scrapes, and with `DEBUG=true` in `.env`,
`/_inspector/requests` lists the requests above as timelines of their queries,
cache calls, dispatches and log lines. Without a token, `/sessions/current` and
the items routes answer 401.

## Working on Keel itself

```bash
just install          # uv sync, every extra included
just hooks            # the pre-commit and pre-push hooks
just up               # postgres + redis + mailpit
just check            # lint, both type checkers, tests
just new ../my-api    # a project linked to this checkout, editable
```

Postgres is on **5433**, Redis on **6380** and Mailpit on **1025**/**8025**,
to avoid clashing with anything already running on the default ports.
Override `DATABASE_URL` or `REDIS_URL` to point elsewhere. `just doctor` says
whether they are actually answering.
