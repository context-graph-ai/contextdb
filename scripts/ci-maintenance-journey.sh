#!/usr/bin/env bash
# Compile before starting the unchanged maintenance journey's process limit.
set -euo pipefail
receipt_dir=${1:?receipt directory is required}
shift
mkdir -p "$receipt_dir"
trap 'printf "%s\n" "$?" > "$receipt_dir/maintenance.exit"' EXIT
maintenance_case=paged_initial_replacement_and_reclamation_progress_under_writes::paged_initial_replacement_and_reclamation_progress_under_writes
timeout 30m cargo test --locked "$@" --test engine_seams --no-run 2>&1 | tee "$receipt_dir/maintenance-build.log"
timeout --kill-after=5s 600s cargo test --locked "$@" --test engine_seams "$maintenance_case" -- --exact --test-threads=1 2>&1 | tee "$receipt_dir/maintenance.log"
if ! rg -q '^test result: ok\. 1 passed; 0 failed; 0 ignored;' "$receipt_dir/maintenance.log"; then
  echo 'Maintenance journey did not complete exactly one unignored test' >&2
  exit 2
fi
