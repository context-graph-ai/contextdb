# contextdb — Agent Rules

## Verification Gate

All four must pass before any commit, release, or "done" claim:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
```

## Cargo Commands

Never run multiple cargo commands in parallel. They share an exclusive lock on `target/` and will deadlock. Run sequentially or chain with `&&`.

## Testing

- Tests use **testcontainers** for NATS. Never start Docker containers or NATS manually.
- Test suites: `cargo test -p contextdb-engine --test acceptance`, `--test integration`, `--test sql_surface_tests`
- Run the full workspace: `cargo test --workspace`

## Workspace

10 crates: contextdb-core, contextdb-tx, contextdb-relational, contextdb-graph, contextdb-vector, contextdb-parser, contextdb-planner, contextdb-engine, contextdb-server, contextdb-cli.

## Releases

Requires `cargo-release` installed locally (`cargo install cargo-release`). No `cargo login` needed — `--no-publish` skips local publishing; CI publishes via `CARGO_REGISTRY_TOKEN` org secret.

Use `cargo release {level} --execute --workspace --no-publish` where level is:
- `patch` (0.3.0 → 0.3.1) — bug fixes, no API changes
- `minor` (0.3.0 → 0.4.0) — new features, backward compatible
- `major` (0.3.0 → 1.0.0) — breaking API changes

This bumps versions across all 10 crates, commits, tags, and pushes. CI then handles crate publishing (rate limited to 5 per batch), Docker images, GitHub releases, and docs deployment to contextdb.tech.

## GitHub

Org: `context-graph-ai`, repo: `contextdb`. CI runs on push to main and PRs.
