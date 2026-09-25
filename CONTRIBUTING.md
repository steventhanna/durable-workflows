# Contributing

## Running the tests

Most tests need a real database server: MySQL 8.0.16+ or Postgres 14+, matching
the backend feature the tests are built with. Each test creates its own database
(`dwt_*`), so the suite runs fully in parallel and never touches your other
data. Databases left behind by earlier runs are dropped after an hour.

The quickest way to get a server:

```sh
docker run -d --name durable-workflows-mysql \
    -e MYSQL_ROOT_PASSWORD=durable -p 33306:3306 mysql:8.4

export DURABLE_WORKFLOWS_TEST_DATABASE_URL=mysql://root:durable@127.0.0.1:33306/mysql
cargo test --workspace --no-default-features \
    --features durable-workflows/mysql,durable-workflows/fake-clock
```

For Postgres (the suite opens more than the default 100 connections):

```sh
docker run -d --name durable-workflows-pg17 \
    -e POSTGRES_PASSWORD=durable -p 55432:5432 postgres:17 -c max_connections=300

export DURABLE_WORKFLOWS_TEST_DATABASE_URL=postgres://postgres:durable@127.0.0.1:55432/postgres
cargo test --workspace --no-default-features \
    --features durable-workflows/postgres,durable-workflows/fake-clock
```

The `mysql` and `postgres` features are mutually exclusive and `postgres` is
the default, so MySQL runs need `--no-default-features`. The backend is the
feature, never the URL: the tests panic if the URL scheme does not match it.
The tests that freeze the database clock need `fake-clock`. CI runs the
suite on MySQL 8.0 and 8.4 and on Postgres 14 and 17; all four must pass.

The URL must name an existing database (the tests connect there first) and
a user allowed to `CREATE DATABASE` and `DROP DATABASE`.

If `DURABLE_WORKFLOWS_TEST_DATABASE_URL` is not set, the database tests fail
on purpose. To run only the tests that need no database:

```sh
DURABLE_WORKFLOWS_SKIP_DB_TESTS=1 cargo test --workspace --no-default-features \
    --features durable-workflows/mysql,durable-workflows/fake-clock
```

## Before opening a pull request

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --no-default-features \
    --features durable-workflows/mysql,durable-workflows/fake-clock -- -D warnings
cargo clippy --workspace --all-targets --no-default-features \
    --features durable-workflows/postgres,durable-workflows/fake-clock -- -D warnings
cargo test --workspace --no-default-features \
    --features durable-workflows/mysql,durable-workflows/fake-clock
cargo test --workspace --no-default-features \
    --features durable-workflows/postgres,durable-workflows/fake-clock
```

The two `cargo test` runs need `DURABLE_WORKFLOWS_TEST_DATABASE_URL` set to a
server of the matching backend.

## Invariants and the Quint model

Changes to the claim, lease, retry or scheduling logic should say which
invariant in [`docs/INVARIANTS.md`](docs/INVARIANTS.md) they rely on or
change, and update that document when they change behavior. Where the
behavior is modeled, also update the Quint model in [`spec/`](spec) and
re-run its checks (Node.js for Quint; Java 17+ only for `verify.sh`):

```sh
cd spec
npm install
./check.sh          # typecheck, directed tests, random simulation
./verify.sh         # optional: bounded model checking with Apalache
```

[`spec/README.md`](spec/README.md) lists what the model covers and what it
abstracts.

Trace checking replays the recorded behavior of the test suites through the
model; CI runs it in the `trace-check` job. Run it locally with
`scripts/trace-pipeline.sh mysql` (or `postgres`), and refresh
`spec/traces/expected.json` when a change moves traces on purpose. See
[`docs/TRACE_CHECKING.md`](docs/TRACE_CHECKING.md). A change that fixes a known gap removes the `#[ignore]` from its
test in `durable-workflows/tests/gaps.rs`.
