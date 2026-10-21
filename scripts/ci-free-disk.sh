#!/usr/bin/env bash
# Reclaim optional preinstalled tools only on disposable hosted Linux runners.
set -euo pipefail
available_bytes() {
  local value
  value=$(df -B1 --output=avail / 2>/dev/null | tail -n 1 | tr -d '[:space:]') || return 1
  [[ $value =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "$value"
}

before=$(available_bytes || true)
printf 'Free bytes before cleanup: %s\n' "${before:-invalid}"
if [[ "${GITHUB_ACTIONS:-}" == true && "${RUNNER_OS:-}" == Linux && "${RUNNER_ENVIRONMENT:-}" == github-hosted ]]; then
  threshold=${NORMAL_CLEANUP_MIN_FREE_BYTES:-}
  if [[ ${CI_GATE_MODE:-full} == normal && $threshold =~ ^[0-9]+$ && $before =~ ^[0-9]+$ && $before -ge $threshold ]]; then
    printf 'Cleanup skipped: normal gate has %s free bytes, at least configured %s.\n' "$before" "$threshold"
  else
    if [[ ${CI_GATE_MODE:-full} != normal ]]; then
      reason='full qualification always retains cleanup'
    elif [[ ! $threshold =~ ^[0-9]+$ ]]; then
      reason='normal cleanup threshold is missing or invalid'
    elif [[ ! $before =~ ^[0-9]+$ ]]; then
      reason='available root disk measurement is missing or invalid'
    else
      reason="available root disk is below configured ${threshold} bytes"
    fi
    printf 'Cleanup retained: %s.\n' "$reason"
    sudo docker image prune -a -f || true
    sudo docker system prune -a -f --volumes || true
    sudo rm -rf /opt/hostedtoolcache/CodeQL /opt/hostedtoolcache/go /opt/az /opt/ghc \
      /usr/local/lib/android /usr/share/dotnet /usr/local/share/boost
  fi
else
  printf 'Cleanup not applicable: runner is not disposable hosted Linux.\n'
fi
after=$(available_bytes || true)
printf 'Free bytes after cleanup: %s\n' "${after:-invalid}"
