# contextdb 2

This folder is contextdb 2: the library (`contextdb/`), the `contextdb` program
(`contextdb-cli/`) and the storage library both are built on (`redb/`, which is redb 4.1.0 with
a few changes listed in its own README). It is a Cargo workspace of its own, with its own lock
file. The packages under `crates/` are contextdb 1; nothing there depends on anything here, and
nothing here depends on anything there. What the library does is in
[`contextdb/README.md`](contextdb/README.md).

Every command below is run from the repository root. Pull requests go to the `dev` branch, where
`.github/workflows/check.yml` runs the same commands (the test run with `--no-tests=pass`
added): the three test commands on Linux and macOS, and the format check, the lint and the
oldest-Rust check on Linux only.

## The whole check

Tests run under [cargo-nextest](https://nexte.st), which stops a test that hangs and names it;
install it once with `cargo install cargo-nextest --version 0.9.138 --locked`.

```sh
cargo fmt --manifest-path v2/Cargo.toml --all --check
cargo clippy --manifest-path v2/Cargo.toml --workspace --all-targets --locked -- -D warnings
cargo nextest run --manifest-path v2/Cargo.toml --workspace --locked
cargo test --manifest-path v2/Cargo.toml --workspace --locked --doc
cargo test --manifest-path v2/redb/Cargo.toml --locked --lib --tests
cargo +1.89 check --manifest-path v2/Cargo.toml --workspace --locked
```

The last line checks the oldest Rust the packages declare (`rustup toolchain install 1.89`
first). The workflow also states the size of the library and the program, rounded up to 500
lines, and the number of tests, rounded up to 10, and fails when either is passed or is no
longer within that distance: a change that grows or shrinks the code past a boundary edits
the number in the workflow file, in the same change. It also refuses a shipped file longer
than 1,500 lines, a test-only name in shipped code, an ignored test and an `allow` attribute
beyond the four `AGENTS.md` names. `AGENTS.md` says how a change is made so that these hold.

## One test

```sh
cargo nextest run --manifest-path v2/Cargo.toml -p contextdb a_fresh_store_is_one_folder
```

The Rust tests of the library are in `contextdb/tests/`, the ones that kill a process in
`contextdb/tests/crash/`, and the program's in `contextdb-cli/tests/`. A test uses only what a
user of the library or the program has.

## One SQL script

A script in `contextdb/tests/sql/` is statements with the output expected of each; the format is
described at the top of `contextdb/tests/sql.rs`. One test runs them all. To run one file:

```sh
SQL_SCRIPT=keys.sql cargo test --manifest-path v2/Cargo.toml -p contextdb --test sql
```

To rewrite the expected output of the scripts from what the engine now answers, then read the
diff before committing it:

```sh
UPDATE_EXPECTED=1 cargo test --manifest-path v2/Cargo.toml -p contextdb --test sql
```

## Measurements

`v2/numbers.sh` prints the numbers in the library's README for the commit it is run at. It only
measures: it never passes or fails, and the check does not run it.
