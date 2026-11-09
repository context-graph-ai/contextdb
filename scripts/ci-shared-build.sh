#!/usr/bin/env bash
# Share one compiled target between the build job and the test jobs of a run.
#   stamp          give every tracked source file one fixed old modification time
#   pack <file>    archive target/ (without receipts) into <file>
#   restore <dir>  unpack <dir>/build.tar.zst into target/ and stamp the sources
#   report         write how many crates the restored target still had to compile
# A target that is not fresh is only rebuilt by Cargo; nothing here decides test results.
set -euo pipefail
cd "$(dirname "$0")/.."
stamp() {
  # Cargo treats a source newer than its dep-info as changed; a fresh checkout is newer than any restored build.
  git ls-files -z | xargs -0 touch -h -d '2001-01-01T00:00:00Z'
}
case "${1:-}" in
  stamp) stamp ;;
  pack)
    stamp
    tar --exclude=target/ci-shard-out --exclude=target/ci-integration-out --exclude=target/debug/incremental \
        --exclude=target/release/incremental -I 'zstd -T0 -3' -cf "${2:?archive path}" target ;;
  restore)
    archive="${2:?artifact directory}/build.tar.zst"
    tar -I zstd -xf "$archive"
    rm -f -- "$archive"
    stamp ;;
  report)
    compiled=0
    logs=()
    shopt -s nullglob globstar
    for log in target/ci-shard-out/**/*.log target/ci-integration-out/**/*.log; do logs+=("$log"); done
    if [[ ${#logs[@]} -gt 0 ]]; then
      compiled=$(cat "${logs[@]}" | sed 's/\x1b\[[0-9;]*m//g' | grep -c '^ *Compiling ' || true)
    fi
    line="Shared build reuse: ${compiled} crate compile lines in this job's test logs (0 means the restored target was fresh)."
    echo "$line"
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then echo "$line" >> "$GITHUB_STEP_SUMMARY"; fi ;;
  *) echo 'usage: ci-shared-build.sh stamp|pack <file>|restore <dir>|report' >&2; exit 2 ;;
esac
