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

# The README's program at 20,000 rows of 1.6 KB, and a million rows under a 64 MiB limit.
for what in readme million; do
  cargo run --quiet --release --locked --manifest-path v2/Cargo.toml -p contextdb --example measure -- "$what" "$scratch/$what" ||
    echo "${what}_measurements unavailable -"
done

# From a fresh clone to a green `cargo test`: nothing built before.
start=$SECONDS
if CARGO_TARGET_DIR="$scratch/fresh" cargo test --manifest-path v2/Cargo.toml --workspace --locked > "$scratch/fresh.log" 2>&1; then
  echo "fresh_clone_to_green_test $(minutes $((SECONDS - start))) min"
else
  echo "fresh_clone_to_green_test unavailable -"
fi

# The first build of a program that embeds the library: the README's own program.
mkdir -p "$scratch/embed/src"
sed -n '/^```rust/,/^```$/p' v2/contextdb/README.md | sed '1d;$d' > "$scratch/embed/src/main.rs"
printf '[package]\nname = "embed"\nversion = "0.0.0"\nedition = "2024"\n\n[dependencies]\ncontextdb = { path = "%s/v2/contextdb" }\n' "$PWD" > "$scratch/embed/Cargo.toml"
start=$SECONDS
if CARGO_TARGET_DIR="$scratch/embed-target" cargo build --release --manifest-path "$scratch/embed/Cargo.toml" > "$scratch/embed.log" 2>&1; then
  echo "embed_first_build $((SECONDS - start)) s"
  echo "embed_packages $(grep -c '^\[\[package\]\]' "$scratch/embed/Cargo.lock") packages"
  echo "embed_program_size $(awk -v bytes="$(wc -c < "$scratch/embed-target/release/embed")" 'BEGIN { printf "%.1f", bytes / 1048576 }') MiB"
else
  echo "embed_first_build unavailable -"
fi
