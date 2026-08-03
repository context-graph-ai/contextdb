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
```

## Before Submitting a PR

All five checks must pass. The fifth installs the release binaries into an
isolated root and drives the production ticketed-Iroh durability smoke; its
feature-gated verifier is not part of the ordinary product CLI.
The smoke uses Bash 3.2-compatible syntax and requires GNU `timeout`; macOS
contributors can install it as `gtimeout` with `brew install coreutils`.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
install_root="$(mktemp -d)"
cargo install --locked --path crates/contextdb-cli --root "$install_root"
cargo install --locked --path crates/contextdb-server --root "$install_root" \
  --features production-smoke-driver --bins
CONTEXTDB_CLI="$install_root/bin/contextdb" \
CONTEXTDB_SMOKE_DRIVER="$install_root/bin/contextdb-smoke-driver" \
CONTEXTDB_SERVER="$install_root/bin/contextdb-server" \
  scripts/installed-release-durable-sync-smoke.sh
```

## Crate Layout

All source lives under `crates/` — see [Architecture](docs/architecture.md) for the full crate map and dependency graph.

## Pull Requests

- Fork the repo, create a branch, submit a PR against `main`
- Keep changes focused — one feature or fix per PR
- Include tests for new functionality
- Ensure all CI checks pass before requesting review

## License

By contributing, you agree that your contributions will be licensed under Apache-2.0.
