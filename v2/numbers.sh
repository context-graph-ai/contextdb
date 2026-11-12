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
# Four groups of lines are taken on data the script is given, each in a variable of the
# environment, and are printed as `unavailable` without it:
#
#     SESSIONS=<folder>:<list file>:<count>           text files loaded one row a line: the
#                                                     first <count> names of the list, each a
#                                                     path under the folder
#     PASSAGES=<dump folder>:<answers folder>         a dump of search passages and the answers
#     PASSAGES_LARGE=<dump folder>:<answers folder>   recorded for it; a second, larger one;
#     PASSAGES_LARGER=<dump folder>:<answers folder>  and a third, larger still
#
# The head of contextdb/examples/passages.rs says what a dump and its answers hold. A store's
# size is the bytes of every file in its folder after it was closed.
#
# Every time of a similarity search is taken by one thread, in a read session opened under the
# memory limit that `index_reader_limit` prints unless the line's name says another, and each
# group of such lines opens with a line ending in `_session` that says both. NUMBERS_LARGE=1
# adds five million generated sightings, which takes the better part of an hour and is run
# once for a landing.
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
# The first pass of an indexed search counts bits, with the processor's bit-count instruction
# where the processor says it has one: this line says which arm of that loop the times are of.
if grep -m 1 '^flags' /proc/cpuinfo 2>/dev/null | grep -qw popcnt; then
  echo "find_counting popcnt -"
else
  echo "find_counting plain -"
fi
echo "index_reader_limit $((1 << 30)) bytes"
echo "index_threads 1 threads"
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

# Similarity search under each mode on generated vectors: a million of the index pilot's, then
# a million sightings; and five million sightings when asked.
example measure -- index "$scratch/index" || echo "index_measurements unavailable -"
rm -rf -- "$scratch/index"
if [ "${NUMBERS_LARGE:-}" = 1 ]; then
  example measure -- sightings 5000000 "$scratch/index" ||
    echo "sightings_5000000_measurements unavailable -"
  rm -rf -- "$scratch/index"
fi

# Search passages: load, seal, build the index, search through SQL under each mode, and the
# answers beside the recorded ones. First 10,000 passages of one partition of the second
# dump; then each dump whole, the second and the third also with a tail written after the
# build, the first and the second also kept as reduced vectors.
if [ -n "${PASSAGES_LARGE:-}" ]; then
  example passages -- single "${PASSAGES_LARGE%%:*}" "$scratch/passages" ||
    echo "single_measurements unavailable -"
  rm -rf -- "$scratch/passages"
else
  echo "single_measurements unavailable -"
fi
passages() {
  if [ -n "$2" ]; then
    example passages -- "${2%%:*}" "${2#*:}" "$scratch/passages" "$3" ||
      echo "$1 unavailable -"
    rm -rf -- "$scratch/passages"
  else
    echo "$1 unavailable -"
  fi
}
passages passages_measurements "${PASSAGES:-}" sq8
passages passages_large_measurements "${PASSAGES_LARGE:-}" sq8,tail
passages passages_larger_measurements "${PASSAGES_LARGER:-}" tail

# Context Graph's declarations and statements (the head of contextdb/examples/queries.rs):
# its declarations; the second dump in its passages table; the anchors of every session file
# of the SESSIONS folder's `files`; and two generated cases.
queries() {
  example queries -- "$@" "$scratch/queries" || echo "queries_${1}_measurements unavailable -"
  rm -rf -- "$scratch/queries"
}
queries declarations
queries dag
queries grants
if [ -n "${PASSAGES_LARGE:-}" ]; then
  queries declared "${PASSAGES_LARGE%%:*}" "${PASSAGES_LARGE#*:}"
else
  echo "declared_measurements unavailable -"
fi
if [ -n "${SESSIONS:-}" ]; then
  queries anchors "${SESSIONS%%:*}/files"
else
  echo "anchors_measurements unavailable -"
fi

# The Context Graph branch, CG_BRANCH=<checkout>, checked against this tree's library: the
# compiler's errors, and the call sites still fenced for each later step.
if [ -n "${CG_BRANCH:-}" ]; then
  mkdir -p "$scratch/cg"
  tar -C "$CG_BRANCH" --exclude=./target -cf - . | tar -xf - -C "$scratch/cg"
  sed -i "s#path = \"[^\"]*/v2/contextdb\"#path = \"$PWD/v2/contextdb\"#" "$scratch/cg/Cargo.toml"
  cargo check --manifest-path "$scratch/cg/Cargo.toml" --workspace --all-targets --keep-going > "$scratch/cg.log" 2>&1
  echo "cg_branch_errors $(grep -E '^error(\[|:)' "$scratch/cg.log" | grep -cv 'could not compile') errors"
  for step in 3 5 6 7 8; do
    echo "cg_branch_fenced_step$step $(grep -r -o "cfg(lean_step$step)" "$scratch/cg/crates" | wc -l) sites"
  done
else
  echo "cg_branch_measurements unavailable -"
fi

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
