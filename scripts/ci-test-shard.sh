#!/usr/bin/env bash
# Cargo runs each libtest binary in its own process; test-hook builds serialize tests within it.
set -euo pipefail
# A bundle shares setup/cache only; every invocation retains its original tuple.
if [[ -n "${CI_BUNDLE:-}" ]]; then
  bundle_plan=$(python3 -B "$(dirname -- "${BASH_SOURCE[0]}")/ci-inventory.py" --bundle "$CI_BUNDLE")
  bundle_planned=$(printf '%s\n' "$bundle_plan" | wc -l)
  bundle_executed=0
  bundle_status=0
  while IFS='|' read -r bundle_crate bundle_shard bundle_features; do
    bundle_root="${CARGO_TARGET_DIR:-$PWD/target}/ci-shard-out"
    [[ ! -L "$bundle_root" && ! -L "$bundle_root/$bundle_shard" ]] || exit 2
    # Invalidate stale receipts before launch, including a skipped child.
    rm -rf -- "${bundle_root:?}/${bundle_shard:?}"
    child_status=0
    CI_BUNDLE='' CI_CRATE="$bundle_crate" CI_SHARD="$bundle_shard" CI_FEATURES="$bundle_features" bash "${BASH_SOURCE[0]}" </dev/null || child_status=$?
    bundle_executed=$((bundle_executed + 1))
    # Attempt every constituent, but retain the first real command failure.
    if [[ "$child_status" != 0 && "$bundle_status" == 0 ]]; then bundle_status=$child_status; fi
    bundle_receipts="$bundle_root/$bundle_shard/$bundle_shard"
    for suffix in profile tests targets.json time; do
      if [[ ! -f "$bundle_receipts.$suffix" && "$bundle_status" == 0 ]]; then bundle_status=2; fi
    done
    if [[ -f "$bundle_receipts.time" ]] && ! head -n 1 "$bundle_receipts.time" | grep -Eq "^$bundle_shard [0-9]+s exit=$child_status$"; then
      if [[ "$bundle_status" == 0 ]]; then bundle_status=2; fi
    fi
  done <<< "$bundle_plan"
  if [[ "$bundle_executed" -ne "$bundle_planned" && "$bundle_status" == 0 ]]; then bundle_status=2; fi
  exit "$bundle_status"
fi
crate="${CI_CRATE:?CI_CRATE is required}"
features="${CI_FEATURES:-}"
shard="${CI_SHARD:?CI_SHARD is required}"
[[ "$crate" =~ ^contextdb-[a-z]+$ && "$shard" =~ ^contextdb-[a-z0-9-]+$ ]] || exit 2
case "${CI_GATE_MODE:-full}" in normal|full) ;; *) echo 'Invalid CI_GATE_MODE' >&2; exit 2 ;; esac
command -v rg >/dev/null 2>&1 || { echo 'Test inventory requires ripgrep (rg)' >&2; exit 2; }
target_dir="${CARGO_TARGET_DIR:-$PWD/target}"
receipt_dir="$target_dir/ci-shard-out/$shard"
# Each validated shard owns only this scratch directory, never another shard.
[[ ! -L "$target_dir/ci-shard-out" && ! -L "$receipt_dir" ]] || exit 2
rm -rf -- "$receipt_dir"
mkdir -p "$receipt_dir"
args=()
if [[ "$crate" == contextdb-redb ]]; then
  args+=(--manifest-path crates/contextdb-redb/Cargo.toml --target-dir "${CARGO_TARGET_DIR:-$PWD/target}/redb")
else
  args+=(-p "$crate")
fi
if [[ -n "$features" ]]; then
  args+=(--features "$features")
fi
# The workspace gate enables engine test seams through the server's test
# dependency. Standalone default engine integrations are a different shape:
# BUG: standalone-default text_memory_charge_measurement and
# indexed_row_memory_charge_measurement cannot compile: accountant is private
# (E0624).
# The source owner must repair this; production nightly keeps exposing it.
# All engine integrations remain delegated to the test-seams/iroh PR shard;
# the extra default shard covers lib/bins.
targets=(--lib --bins --tests)
if [[ "$crate" == contextdb-engine && -z "$features" ]]; then
  targets=(--lib --bins)
fi
# A split shard runs only its listed targets (scripts/ci-shard-targets.txt);
# examples and doctests run once, in the shard that lists `lib`.
run_extras=true
skip_case=""
manifest="$(dirname -- "${BASH_SOURCE[0]}")/ci-shard-targets.txt"
while read -r name items; do
  [[ "$name" == "$shard:" ]] || continue
  targets=()
  run_extras=false
  routing=$(python3 -B "$(dirname -- "${BASH_SOURCE[0]}")/ci-integration.py" shard-plan "$shard")
  items=${routing%%|*}
  skip_case=${routing#*|}
  for item in $items; do
    if [[ "$item" == lib ]]; then targets+=(--lib --bins); run_extras=true; else targets+=(--test "$item"); fi
  done
done < <(grep -v '^#' "$manifest")
# Reflect the actual libtest mode; the command-line seam override wins over env.
thread_mode="${RUST_TEST_THREADS:-default-parallel}"
if [[ "$features" == *test-seams* ]]; then thread_mode=1; fi
# Exact exclusion is visible in receipts and remains executable in full mode.
start=$(date +%s)
# shellcheck disable=SC2329 # Invoked by the EXIT trap.
finish() {
  status=$?
  end=$(date +%s)
  inventory_status=0
  shopt -s nullglob
  logs=("$receipt_dir"/*.log)
  : > "$receipt_dir/$shard.tests"
  if [[ ${#logs[@]} -gt 0 ]]; then
    rg '^test .* \.\.\. |^test result:' "${logs[@]}" > "$receipt_dir/$shard.tests" || inventory_status=$?
  fi
  # No matches is normal when compilation fails; an inventory tool error is not.
  if [[ "$inventory_status" -gt 1 ]]; then
    echo "Test inventory failed: rg exit=$inventory_status" >&2
    if [[ "$status" == 0 ]]; then status=$inventory_status; fi
  fi
  python3 "$(dirname -- "${BASH_SOURCE[0]}")/ci-inventory.py" --timings "$thread_mode" "${logs[@]}" > "$receipt_dir/$shard.targets.json" || {
    if [[ "$status" == 0 ]]; then status=2; fi
  }
  if [[ "$status" == 0 && "$shard" == contextdb-engine-test-seams-15 ]]; then
    python3 -B "$(dirname -- "${BASH_SOURCE[0]}")/ci-integration.py" shard-receipt "$receipt_dir/$shard.targets.json" || status=2
  fi
  printf '%s %ss exit=%s\n' "$shard" "$((end - start))" "$status" > "$receipt_dir/$shard.time"
  df -h / >> "$receipt_dir/$shard.time"
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    cat "$receipt_dir/$shard.time" >> "$GITHUB_STEP_SUMMARY"
  fi
  exit "$status"
}
trap finish EXIT
{
  source_sha=$(git rev-parse HEAD)
  printf 'source_sha=%s\n' "$source_sha"
  # Preserve exact bytes: command substitution cannot retain NUL status records.
  git status --porcelain=v1 -z > "$receipt_dir/source.status"
  git diff HEAD --binary --no-ext-diff > "$receipt_dir/source.diff"
  source_dirty=false
  if [[ -s "$receipt_dir/source.status" ]]; then source_dirty=true; fi
  printf 'source_dirty=%s\n' "$source_dirty"
  source_status_sha256=$(sha256sum "$receipt_dir/source.status" | cut -d ' ' -f 1)
  source_diff_sha256=$(sha256sum "$receipt_dir/source.diff" | cut -d ' ' -f 1)
  printf 'source_status_sha256=%s\n' "$source_status_sha256"
  printf 'source_diff_sha256=%s\n' "$source_diff_sha256"
  rm -- "$receipt_dir/source.status" "$receipt_dir/source.diff"
  rustc --version
  cargo --version
  printf 'gate_mode=%s relocated_case=%s destination=full-qualification-both-feature-tuples\n' "${CI_GATE_MODE:-full}" "$skip_case"
  printf 'crate=%s features=%s RUSTFLAGS=%s build_jobs=%s test_threads=%s\n' \
    "$crate" "$features" "${RUSTFLAGS:-}" "${CARGO_BUILD_JOBS:-default}" "$thread_mode"
  printf 'dev_debug=%s test_debug=%s incremental=%s\n' \
    "${CARGO_PROFILE_DEV_DEBUG:-default}" "${CARGO_PROFILE_TEST_DEBUG:-default}" "${CARGO_INCREMENTAL:-default}"
} > "$receipt_dir/$shard.profile"
# Integration tests must use binaries built from this same checkout.
if [[ "$crate" == contextdb-engine || "$crate" == contextdb-server ]]; then
  timeout 30m cargo build --locked -p contextdb-cli -p contextdb-server
fi
# Test-hook builds run one test at a time, as scripts/verify does; default builds run in parallel.
threads=()
if [[ "$features" == *test-seams* ]]; then threads=(--test-threads=1); fi
if [[ -n "$skip_case" ]]; then threads+=(--skip "$skip_case"); fi
set +e
# Explicit libtest targets avoid forwarding libtest flags to Criterion benches.
# Keep ignored tests ignored. Ordinary elapsed time is a receipt, not a threshold.
if [[ "$crate" == contextdb-engine || "$crate" == contextdb-server ]]; then
  env "CARGO_BIN_EXE_contextdb=$target_dir/debug/contextdb" \
      "CARGO_BIN_EXE_contextdb-server=$target_dir/debug/contextdb-server" \
    timeout 300m cargo test --locked "${args[@]}" "${targets[@]}" -- "${threads[@]}" 2>&1 | tee "$receipt_dir/$shard.log"
else
  timeout 300m cargo test --locked "${args[@]}" "${targets[@]}" -- "${threads[@]}" 2>&1 | tee "$receipt_dir/$shard.log"
fi
statuses=("${PIPESTATUS[@]}")
status=${statuses[0]}
if [[ "$status" == 0 ]]; then status=${statuses[1]}; fi
set -e
if [[ "$status" == 0 && "$run_extras" == true ]]; then
  # cargo test's default example compilation and doctests remain required.
  timeout 30m cargo test --locked "${args[@]}" --examples --no-run 2>&1 | tee "$receipt_dir/$shard.examples.log"
  timeout 30m cargo test --locked "${args[@]}" --doc -- --test-threads=1 2>&1 | tee "$receipt_dir/$shard.doc.log"
fi
exit "$status"
