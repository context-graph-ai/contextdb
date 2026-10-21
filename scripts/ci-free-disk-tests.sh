#!/usr/bin/env bash
set -euo pipefail
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

expect() {
  if ! "$@"; then
    printf 'cleanup control failed: %q\n' "$*" >&2
    exit 1
  fi
}

run_case() {
  local name=$1 bytes=$2 mode=$3 threshold=$4 sudo_status=${5:-0}
  local bin="$root/$name/bin"
  case_out="$root/$name/out"
  case_calls="$root/$name/calls"
  mkdir -p "$bin"
  cat >"$bin/df" <<EOF
#!/bin/sh
printf 'Avail\n%s\n' '$bytes'
EOF
  cat >"$bin/tail" <<'EOF'
#!/bin/sh
exec /usr/bin/tail "$@"
EOF
  cat >"$bin/tr" <<'EOF'
#!/bin/sh
exec /usr/bin/tr "$@"
EOF
  cat >"$bin/sudo" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >>'$case_calls'
case "\$*" in *'rm -rf'*) exit $sudo_status;; esac
exit 0
EOF
  chmod +x "$bin"/*
  set +e
  PATH="$bin:/usr/bin:/bin" GITHUB_ACTIONS=true RUNNER_OS=Linux RUNNER_ENVIRONMENT=github-hosted \
    CI_GATE_MODE="$mode" NORMAL_CLEANUP_MIN_FREE_BYTES="$threshold" \
    bash scripts/ci-free-disk.sh >"$case_out" 2>&1
  status=$?
  set -e
}

run_case high 90000000000 normal 85899345920
expect test "$status" -eq 0
expect test ! -s "$case_calls"
expect grep -q 'Cleanup skipped' "$case_out"
run_case boundary 85899345920 normal 85899345920
expect test "$status" -eq 0
expect test ! -s "$case_calls"
expect grep -q 'Cleanup skipped' "$case_out"
run_case low 85899345919 normal 85899345920
expect test "$status" -eq 0
expect test "$(wc -l <"$case_calls")" -eq 3
expect grep -q 'below configured' "$case_out"
run_case invalid nope normal 85899345920
expect test "$status" -eq 0
expect test "$(wc -l <"$case_calls")" -eq 3
expect grep -q 'measurement is missing or invalid' "$case_out"
run_case badconfig 90000000000 normal invalid
expect test "$status" -eq 0
expect test "$(wc -l <"$case_calls")" -eq 3
expect grep -q 'threshold is missing or invalid' "$case_out"
run_case full 90000000000 full 85899345920
expect test "$status" -eq 0
expect test "$(wc -l <"$case_calls")" -eq 3
expect grep -q 'full qualification always retains cleanup' "$case_out"
run_case failure 1 normal 85899345920 19
expect test "$status" -eq 19
expect test "$(wc -l <"$case_calls")" -eq 3
# The assertion helper itself must reject a deliberately wrong helper result.
if ( expect test 999 -eq 0 ); then
  printf 'cleanup mutation unexpectedly passed\n' >&2
  exit 1
fi
printf 'cleanup controls PASS: high, boundary, low, invalid measurement/config, full, failure semantics\n'
