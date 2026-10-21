# Contributing to contextdb

## Building

```bash
git clone https://github.com/context-graph-ai/contextdb.git
cd contextdb
cargo build --workspace
```

## Running Tests

```bash
cargo test --workspace
cargo test --manifest-path crates/contextdb-redb/Cargo.toml --locked
```

## Before Submitting a PR

The local CI routing patch is a policy proposal pending owner acceptance. Its
normal route uses bounded functional schema checks; original full schema, oversized
transport and corpus qualification remains executable with `CI_GATE_MODE=full`.
See [the CI guide](.github/CI.md) for exact routing and commands. Normal success
cannot certify full qualification or the unmeasured thirty-minute end-to-end budget.
The nine-command precommit requirement below remains in force.

All nine must pass before any commit, release, or "done" claim. The last installs isolated
release binaries and drives the production ticketed-Iroh durability smoke; its
feature-gated verifier is not part of the ordinary product CLI.
The smoke uses Bash 3.2-compatible syntax and requires GNU `timeout`; macOS
contributors can install it as `gtimeout` with `brew install coreutils`.

**Run `cargo fmt --all` before you consider yourself done — formatting is the gate's first
command, and an otherwise-correct change fails immediately on an unformatted line.** Run it first
when you start and again as the last thing you do.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --manifest-path crates/contextdb-redb/Cargo.toml --check
cargo clippy --manifest-path crates/contextdb-redb/Cargo.toml --all-targets -- -D warnings
cargo test --workspace
cargo test --manifest-path crates/contextdb-redb/Cargo.toml --locked
cargo build --release
bash scripts/verify-packaged-engine.sh
install_root="$(mktemp -d)"
cargo install --locked --path crates/contextdb-cli --root "$install_root"
cargo install --locked --path crates/contextdb-server --root "$install_root" \
  --features production-smoke-driver --bins
CONTEXTDB_CLI="$install_root/bin/contextdb" \
CONTEXTDB_SMOKE_DRIVER="$install_root/bin/contextdb-smoke-driver" \
CONTEXTDB_SERVER="$install_root/bin/contextdb-server" \
  scripts/installed-release-durable-sync-smoke.sh
```

`bash scripts/verify pr` runs the same local stages the PR checks run — lint, every test shard,
release build, feature shapes and the installed smoke — sequentially in one process (local cross
builds only when `CI_TARGET` is set). It complements the nine commands above; it does not replace
them. [`.github/CI.md`](.github/CI.md) maps each PR check to its local command.
Performance measurement is a separate, manual journey on a controlled native host; see
[`docs/performance/README.md`](docs/performance/README.md).

## Crate Layout

All source lives under `crates/` — see [Architecture](docs/architecture.md) for the full crate map and dependency graph.

## Pull Requests

- Fork the repo, create a branch, submit a PR against `main`
- Keep changes focused — one feature or fix per PR
- Include tests for new functionality
- Ensure all CI checks pass before requesting review

## License

By contributing, you agree that your contributions will be licensed under Apache-2.0.
