#!/usr/bin/env bash
# Prints the numbers this workspace is held to, one per line as `name value unit`, for the
# commit it is run at:
#
#     v2/numbers.sh
#
# Run it on a machine that is doing nothing else. It measures and never passes or fails, and
# nothing in the check runs it. A number it could not take is printed as `unavailable`. It
# builds in release mode, works in a folder of its own under $TMPDIR and removes that folder
# when it is done. Packages cargo has downloaded before are not downloaded again, so the two
# build times leave the download out.
#
# Three groups of lines are taken on data the script is given, each in a variable of the
# environment, and are printed as `unavailable` without it:
#
#     SESSIONS=<folder>:<list file>:<count>          text files loaded one row a line: the
#                                                    first <count> names of the list, each a
#                                                    path under the folder
#     PASSAGES=<dump folder>:<answers folder>        a dump of search passages and the answers
#     PASSAGES_LARGE=<dump folder>:<answers folder>  recorded for it; a second, larger one
#
# The head of contextdb/examples/passages.rs says what a dump and its answers hold. A store's
# size is the bytes of every file in its folder after it was closed.
set -u
cd "$(dirname "$0")/.." || exit 1
scratch=$(mktemp -d) || exit 1
trap 'rm -rf -- "$scratch"' EXIT

minutes() { awk -v seconds="$1" 'BEGIN { printf "%.1f", seconds / 60 }'; }

# What the tree holds, counted the way the check counts it.
shipped=$(find v2/contextdb/src v2/contextdb-cli/src \( -name '*.rs' -o -name '*.pest' \) ! -name tests.rs -exec cat {} + | wc -l)
echo "shipped_lines $shipped lines"
if listed=$(cargo nextest list --manifest-path v2/Cargo.toml --workspace --locked -T oneline 2>/dev/null); then
  # A test is one the runner lists, or one script file.
  echo "tests_listed $(($(printf '%s\n' "$listed" | grep -c .) + $(find v2/contextdb/tests/sql -name '*.sql' | wc -l))) tests"
else
  echo "tests_listed unavailable -"
fi

# The storage library against the redb 4.1.0 that was published: cargo fetches that package
# and checks it, and every line that differs under src/ is counted, on either side.
mkdir -p "$scratch/upstream/src"
: > "$scratch/upstream/src/lib.rs"
printf '[package]\nname = "upstream"\nversion = "0.0.0"\nedition = "2021"\n\n[dependencies]\nredb = "=4.1.0"\n' > "$scratch/upstream/Cargo.toml"
published=$(cargo metadata --format-version 1 --manifest-path "$scratch/upstream/Cargo.toml" 2>/dev/null |
  grep -o '"manifest_path":"[^"]*/redb-4\.1\.0/Cargo\.toml"' | head -n 1 | cut -d '"' -f 4)
if [ -n "$published" ]; then
  echo "storage_library_changed_lines $(diff -rN "$(dirname "$published")/src" v2/redb/src | grep -c '^[<>]') lines"
else
  echo "storage_library_changed_lines unavailable -"
fi

# The measuring programs are built as any program that uses the library is: in release mode
# with nothing added. This line says what compiler flags the environment adds, so that a
# number from a build made for one processor is not read as an ordinary build's.
flags="${RUSTFLAGS:-}${CARGO_ENCODED_RUSTFLAGS:+ $CARGO_ENCODED_RUSTFLAGS}${CARGO_BUILD_RUSTFLAGS:+ $CARGO_BUILD_RUSTFLAGS}"
echo "find_build_rustflags $(printf '%s' "${flags:-none}" | tr -s ' \037' '__') -"
example() {
  cargo run --quiet --release --locked --manifest-path v2/Cargo.toml -p contextdb --example "$@"
}

# 20,000 notes of 1.6 KB, a million rows under a 64 MiB limit, and 20,000 vectors of 384
# numbers with the 10 nearest of 100 others.
for what in readme million vectors; do
  example measure -- "$what" "$scratch/$what" || echo "${what}_measurements unavailable -"
  rm -rf -- "$scratch/$what"
done

# Text files, one row a line: what was put in beside what the store takes.
if [ -n "${SESSIONS:-}" ]; then
  IFS=: read -r corpus list count <<< "$SESSIONS"
  example measure -- sessions "$corpus" "$list" "$count" "$scratch/sessions" ||
    echo "sessions_measurements unavailable -"
  rm -rf -- "$scratch/sessions"
else
  echo "sessions_measurements unavailable -"
fi

# Search passages: load, seal, search through SQL, and the answers beside the recorded ones.
given=0
for input in "${PASSAGES:-}" "${PASSAGES_LARGE:-}"; do
  [ -n "$input" ] || continue
  given=1
  example passages -- "${input%%:*}" "${input#*:}" "$scratch/passages" ||
    echo "passages_measurements unavailable -"
  rm -rf -- "$scratch/passages"
done
[ "$given" = 1 ] || echo "passages_measurements unavailable -"

# From a fresh clone to a green `cargo test`: nothing built before.
start=$SECONDS
if CARGO_TARGET_DIR="$scratch/fresh" cargo test --manifest-path v2/Cargo.toml --workspace --locked > "$scratch/fresh.log" 2>&1; then
  echo "fresh_clone_to_green_test $(minutes $((SECONDS - start))) min"
else
  echo "fresh_clone_to_green_test unavailable -"
fi

# The first build of a program that embeds the library: the README's first program.
mkdir -p "$scratch/embed/src"
awk '/^```rust/ { block++; next } /^```$/ && block == 1 { exit } block == 1' v2/contextdb/README.md > "$scratch/embed/src/main.rs"
printf '[package]\nname = "embed"\nversion = "0.0.0"\nedition = "2024"\n\n[dependencies]\ncontextdb = { path = "%s/v2/contextdb" }\n' "$PWD" > "$scratch/embed/Cargo.toml"
start=$SECONDS
if CARGO_TARGET_DIR="$scratch/embed-target" cargo build --release --manifest-path "$scratch/embed/Cargo.toml" > "$scratch/embed.log" 2>&1; then
  echo "embed_first_build $((SECONDS - start)) s"
  echo "embed_packages $(grep -c '^\[\[package\]\]' "$scratch/embed/Cargo.lock") packages"
  echo "embed_program_size $(awk -v bytes="$(wc -c < "$scratch/embed-target/release/embed")" 'BEGIN { printf "%.1f", bytes / 1048576 }') MiB"
else
  echo "embed_first_build unavailable -"
fi
