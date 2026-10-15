#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

for cache_root in "$repo_root/target" "$repo_root/target/redb"; do
  if [[ -L "$cache_root" || ( -e "$cache_root" && ! -d "$cache_root" ) ]]; then
    echo "Refusing unsafe cache target root: $cache_root" >&2
    exit 1
  fi

  scratch="$cache_root/tests"
  if [[ -L "$scratch" || ( -e "$scratch" && ! -d "$scratch" ) ]]; then
    echo "Refusing unsafe cache scratch directory: $scratch" >&2
    exit 1
  fi
  if [[ -d "$scratch" ]]; then
    rm -rf -- "$scratch"
  fi
done
