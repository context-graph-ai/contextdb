# contextdb 2

This folder is contextdb 2: the library (`contextdb/`), the `contextdb` program
(`contextdb-cli/`) and the storage library both are built on (`redb/`, which is redb 4.1.0 with
a few changes listed in its own README). It is a Cargo workspace of its own, with its own lock
file. The packages under `crates/` are contextdb 1; nothing there depends on anything here, and
nothing here depends on anything there. What the library does is in
[`contextdb/README.md`](contextdb/README.md).

Every command below is run from the repository root. Pull requests go to the `dev` branch, where
`.github/workflows/check.yml` owns the complete hosted check. The commands below are
the local core; hosted jobs also check the storage library's formatting/lints, rustdoc,
code/test counts and instruction cost. Tests run on Linux x86-64, Linux ARM64 and macOS;
format, lint and the oldest-Rust check run on Linux.

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
first). The workflow also states three sizes: the library and the program in lines rounded up to 500
(`SHIPPED_LINES`), the tests in number rounded up to 10 (`TESTS`) and the lines of tests rounded
up to 500 (`TEST_LINES`). It fails when one is passed or is no longer within that distance, and
`AGENTS.md` says when a number may be moved. It also refuses a shipped file longer
than 1,500 lines, a test-only name in shipped code, an ignored test and an `allow` attribute
beyond the six `AGENTS.md` names. `AGENTS.md` says how a change is made so that these hold.

## Hosted checks

The lint job also runs:

```sh
cargo fmt --manifest-path v2/redb/Cargo.toml --all --check
cargo clippy --manifest-path v2/redb/Cargo.toml --all-targets --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --manifest-path v2/Cargo.toml --workspace --locked --no-deps
```

The workflow checks shipped lines, test cases, test lines, file lengths, test-only names,
ignored tests, allow attributes and named documentation guards. Exact ceilings are its
`SHIPPED_LINES`, `TESTS` and `TEST_LINES` environment entries. Shipped lines and the
1,500-line file limit cover the two workspace members' source trees, excluding `tests.rs`;
redb and examples are excluded. The test-case count adds all nextest cases (including
those outside the default filter) and one per SQL script, excluding doc and redb tests.
Test lines include all `v2/*/tests` files and every `tests.rs`, including redb tests.

The default nextest run skips four scale cases and the three consumer performance bars.
On each pull request a release-profile job runs the 100,000-sighting recall/work/length test,
million-row 64 MiB load test and all three tests in `performance_bars.rs`.
The standalone `DELETE` membership comparison and the test that verifies `PURGE` leaves no copy
in either commit slot also run in that job and are excluded from the default profile.
Scheduled and manually dispatched runs add the nightly million-row load/append profile,
a release million-sighting job and 16 mutation shards. The default command alone does
not exercise these jobs. Read `v2/.config/nextest.toml` for their exact filters.

The pull-request release job also compares instructions per consumer statement between
the merge base and head, using each revision's own `measure` example. Removing a workload
fails. A rise above 2% fails unless a commit subject or body contains a line beginning
`cost: <workload> +<change>% <reason>`, where `<change>` is the measured percentage to
one decimal place and the space after `%` is required. Use the measured workload name
and explain the cause; the line records an accepted cost, not a fabricated allowance.
The complete base/head measurement procedure is the `release` job in
[the workflow](../.github/workflows/check.yml); run it when changing statement cost.
Each job has its own timeout; the workflow does not promise a total duration.

## One test

```sh
cargo nextest run --manifest-path v2/Cargo.toml -p contextdb a_fresh_store_is_one_folder_and_works_with_no_configuration
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

The lines about vector search are taken by the programs in `contextdb/examples/`, the generated
vectors always and the groups of lines about real search passages only on data given in the
environment variables named at the head of the script (without them each of those lines prints
`unavailable`). Every time of a search is taken by one thread, in a read session under the memory
limit that a line of the output states; `NUMBERS_LARGE=1` adds five million generated vectors and
takes the better part of an hour. Run it on a machine that is doing nothing else.
