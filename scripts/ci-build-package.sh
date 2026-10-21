#!/usr/bin/env bash
set -euo pipefail
[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]]
[[ $(git rev-parse HEAD) == "$BUILD_SHA" ]]
export CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUSTFLAGS=''
# Build the published default-feature payload FIRST. The verifier is never shipped.
cargo build --locked --release --target x86_64-unknown-linux-gnu \
  -p contextdb-cli --bin contextdb -p contextdb-server --bin contextdb-server
# Separate target tree prevents verifier feature unification from replacing payload bytes.
cargo build --locked --release --target x86_64-unknown-linux-gnu \
  --target-dir target/development-smoke -p contextdb-server \
  --features production-smoke-driver --bin contextdb-smoke-driver
export CONTEXTDB_SMOKE_DRIVER="$PWD/target/development-smoke/x86_64-unknown-linux-gnu/release/contextdb-smoke-driver"
python3 scripts/ci-build-control.py package
