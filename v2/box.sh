#!/usr/bin/env bash
# box.sh ordinary|home <example> <arguments ...>
#
# Runs one example of the contextdb crate in one of the two machines the engine's bars are
# stated for, and prints that machine after every line the example prints:
#   ordinary  4 cores at 4.0 GHz with 16 GB        home  4 cores at 2.3 GHz with 4 GB
# Memory is the scope's MemoryMax, with no swap. The four cores are pinned with taskset, since
# the user's systemd does not delegate cpuset (ordinary 4-7, home 8-11; BOX_CPUS replaces them).
# The clock is matched by a CPU quota: 400 percent scaled by the box's clock over the host's
# highest clock on the box's first core (cpuinfo_max_freq: one busy thread runs at boost, so the
# highest clock is the honest scale). The run sees that clock as BOX_HOST_KHZ.
# A quota caps the processor time of the box as a whole, so work spread over its four cores sees
# the box's speed; it does not slow one thread, which still runs at the host's clock between
# throttles, so a single-thread time in the box is the host's time, not the box's.
# Everything is a systemd-run --user scope property or taskset; nothing needs root.
# Run from the tree's root, as numbers.sh is.
set -euo pipefail
usage() { sed -n '2,16p' "$0"; exit 2; }
[ $# -ge 2 ] || usage
case "$1" in
  ordinary) mhz=4000 memory=16G cpus=${BOX_CPUS:-4-7} ;;
  home) mhz=2300 memory=4G cpus=${BOX_CPUS:-8-11} ;;
  *) usage ;;
esac
box=$1 example=$2
shift 2
program=$(cargo build --quiet --release --locked --manifest-path v2/Cargo.toml -p contextdb \
  --example "$example" --message-format=json | grep -o "\"executable\":\"[^\"]*/$example\"" \
  | tail -n 1 | cut -d '"' -f 4)
[ -n "$program" ] || { echo "box.sh: no example $example" >&2; exit 2; }
first=${cpus%%[-,]*}
khz=$(cat "/sys/devices/system/cpu/cpu$first/cpufreq/cpuinfo_max_freq")
export BOX_HOST_KHZ=$khz
quota=$(( 400 * mhz * 1000 / khz ))
[ "$quota" -le 400 ] || quota=400
label="box=$box:cores=$cpus:memory=$memory:quota=${quota}%:host_max_khz=$khz"
systemd-run --user --scope --quiet -p MemoryMax="$memory" -p MemorySwapMax=0 \
  -p CPUQuota="${quota}%" taskset -c "$cpus" "$program" "$@" | awk -v box="$label" '{ print $0, box; fflush() }'
