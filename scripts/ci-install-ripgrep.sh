#!/usr/bin/env bash
# Put a pinned ripgrep release on PATH for the CI jobs that need `rg`. The release binary is fetched
# from GitHub and checked against its published SHA-256, so a slow or failing Ubuntu package mirror
# cannot stall a job (an `apt-get update` once used up a test job's whole time limit).
set -euo pipefail
version=15.2.0
sha256=33e15bcf1624b25cdd2a55813a47a2f95dbe126268203e76aa6a585d1e7b149c
name="ripgrep-${version}-x86_64-unknown-linux-musl"
dir="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/ripgrep"
mkdir -p "$dir/bin"
curl -fsSL --retry 5 --retry-all-errors --connect-timeout 10 --max-time 120 \
  -o "$dir/$name.tar.gz" "https://github.com/BurntSushi/ripgrep/releases/download/${version}/${name}.tar.gz"
echo "${sha256}  $dir/$name.tar.gz" | sha256sum -c --quiet -
tar -xzf "$dir/$name.tar.gz" -C "$dir" "$name/rg"
install -m 0755 "$dir/$name/rg" "$dir/bin/rg"
echo "$dir/bin" >> "${GITHUB_PATH:?GITHUB_PATH is set by the runner}"
"$dir/bin/rg" --version | head -1
