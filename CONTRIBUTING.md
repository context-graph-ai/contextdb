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

All four checks must pass:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
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
