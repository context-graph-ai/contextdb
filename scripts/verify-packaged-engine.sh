#!/usr/bin/env bash
# Build the engine only from package archives. The source paths are used only
# to stage those archives; metadata and the build resolve the unpacked closure.
set -euo pipefail

repo_root="$(pwd -P)"
# Ask Cargo for its configured/default target in ordinary CI. Preserve an
# assigned target, making relative paths stable for the unpacked workspace too.
if [[ -z "${CARGO_TARGET_DIR:-}" ]]; then
  CARGO_TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
fi
CARGO_TARGET_DIR="$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve())' "$CARGO_TARGET_DIR")"
export CARGO_TARGET_DIR
tmp_base="${TMPDIR:-/tmp}"
package_root="$(mktemp -d "$tmp_base/contextdb-packaged-engine.XXXXXX")"
printf 'Packaging temporary directory: %s\nBuild target: %s\n' "$package_root" "$CARGO_TARGET_DIR"
trap 'printf "Removing packaging temporary directory: %s\n" "$package_root"; rm -rf -- "$package_root"' EXIT

packaged_crates=(
  contextdb-core contextdb-tx contextdb-relational contextdb-graph contextdb-hnsw
  contextdb-vector contextdb-parser contextdb-planner contextdb-engine contextdb-server
  contextdb-cli
)
all_packaged_crates=(contextdb-redb "${packaged_crates[@]}")

# `cargo package --no-verify` still resolves every versioned dependency. Before
# the maintained fork and first ContextDB release exist in the registry, give
# that packaging step a complete local staging source. It is not used by the
# unpacked metadata or build below.
package_config="$package_root/package-staging.toml"
{
  printf '%s\n' '[patch.crates-io]'
  for crate in "${all_packaged_crates[@]}"; do
    printf '%s = { path = "%s/crates/%s" }\n' "$crate" "$repo_root" "$crate"
  done
} > "$package_config"

for crate in "${packaged_crates[@]}"; do
  version="$(python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["package"]["version"])' "$repo_root/crates/$crate/Cargo.toml")"
  rm -f -- "$CARGO_TARGET_DIR/package/$crate-$version.crate"
  cargo --config "$package_config" package --allow-dirty --no-verify -p "$crate"
done
# The storage fork is an independently locked package.  Package a disposable
# copy so Cargo cannot add this workspace's patch entries to its source lock.
fork_lock_before="$(sha256sum "$repo_root/crates/contextdb-redb/Cargo.lock" | awk '{print $1}')"
fork_package_input="$package_root/contextdb-redb-package-input"
cp -R "$repo_root/crates/contextdb-redb" "$fork_package_input"
# TMPDIR may itself live below the repository. Mark the disposable
# copy as its own workspace so Cargo never walks up into the source workspace.
printf '\n%s\n' '[workspace]' >> "$fork_package_input/Cargo.toml"
fork_copy_lock_before="$(sha256sum "$fork_package_input/Cargo.lock" | awk '{print $1}')"
fork_metadata="$package_root/contextdb-redb-package-input.metadata.json"
cargo metadata --locked --manifest-path "$fork_package_input/Cargo.toml" --format-version 1 > "$fork_metadata"
python3 - "$fork_metadata" "$fork_package_input" <<'PY'
import json
import pathlib
import sys
import tomllib

metadata = json.loads(pathlib.Path(sys.argv[1]).read_text())
fork_root = pathlib.Path(sys.argv[2]).resolve()
if pathlib.Path(metadata["workspace_root"]).resolve() != fork_root:
    raise SystemExit("copied storage fork inherited a parent workspace")
if len(metadata["workspace_members"]) != 1:
    raise SystemExit("copied storage fork is not a one-package standalone workspace")
root_id = metadata["resolve"]["root"]
root = next(package for package in metadata["packages"] if package["id"] == root_id)
if root["name"] != "contextdb-redb":
    raise SystemExit(f"standalone fork metadata resolved the wrong root: {root['name']}")
if pathlib.Path(root["manifest_path"]).resolve() != fork_root / "Cargo.toml":
    raise SystemExit("standalone fork metadata resolved outside its disposable copy")
lock = tomllib.loads((fork_root / "Cargo.lock").read_text())
if lock.get("patch", {}).get("unused"):
    raise SystemExit("standalone fork lock contains unrelated unused workspace patches")
print("Verified copied storage fork's standalone locked metadata")
PY
fork_version="$(python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["package"]["version"])' "$fork_package_input/Cargo.toml")"
rm -f -- "$CARGO_TARGET_DIR/package/contextdb-redb-$fork_version.crate"
cargo package --locked --allow-dirty --no-verify --manifest-path "$fork_package_input/Cargo.toml"
fork_copy_lock_after="$(sha256sum "$fork_package_input/Cargo.lock" | awk '{print $1}')"
fork_lock_after="$(sha256sum "$repo_root/crates/contextdb-redb/Cargo.lock" | awk '{print $1}')"
test "$fork_copy_lock_before" = "$fork_copy_lock_after"
test "$fork_lock_before" = "$fork_lock_after"

unpacked_root="$package_root/unpacked"
mkdir -p "$unpacked_root"
patch_entries="$package_root/patch-entries"
for crate in "${all_packaged_crates[@]}"; do
  version="$(python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["package"]["version"])' "$repo_root/crates/$crate/Cargo.toml")"
  archive="$CARGO_TARGET_DIR/package/$crate-$version.crate"
  test -f "$archive"
  tar -xzf "$archive" -C "$unpacked_root"
  package_dir="$unpacked_root/$crate-$version"
  test -d "$package_dir"
  sha256sum "$archive"
  printf '%s\t%s\n' "$crate" "${package_dir##*/}" >> "$patch_entries"
done

{
  engine_package_dir="$(awk -F '\t' '$1 == "contextdb-engine" { print $2; exit }' "$patch_entries")"
  test -n "$engine_package_dir"
  printf '%s\n' '[workspace]' "members = [\"$engine_package_dir\"]" 'resolver = "2"' '' '[patch.crates-io]'
  while IFS=$'\t' read -r crate package_dir; do
    printf '%s = { path = "%s" }\n' "$crate" "$package_dir"
  done < "$patch_entries"
} > "$unpacked_root/Cargo.toml"
# Start resolution with the same dependency pins as the source workspace.
# Cargo adjusts only the local package closure for this unpacked workspace.
cp "$repo_root/Cargo.lock" "$unpacked_root/Cargo.lock"

metadata="$package_root/metadata.json"
# The copied source lock retains dependency pins, but Cargo must rewrite the
# local package-source entries for this unpacked workspace before locked use.
cargo metadata --manifest-path "$unpacked_root/Cargo.toml" --offline --format-version 1 > /dev/null
cargo metadata --manifest-path "$unpacked_root/Cargo.toml" --locked --format-version 1 > "$metadata"
python3 - "$metadata" "$repo_root" "$unpacked_root" <<'PY'
import json
import pathlib
import sys

metadata = json.loads(pathlib.Path(sys.argv[1]).read_text())
repo_root = pathlib.Path(sys.argv[2]).resolve()
unpacked_root = pathlib.Path(sys.argv[3]).resolve()
packages = {package["name"]: package for package in metadata["packages"]}
expected = {
    "contextdb-redb", "contextdb-core", "contextdb-tx", "contextdb-relational",
    "contextdb-graph", "contextdb-hnsw", "contextdb-vector", "contextdb-parser",
    "contextdb-planner", "contextdb-engine",
}
missing = expected - packages.keys()
if missing:
    raise SystemExit(f"packaged engine closure is missing: {', '.join(sorted(missing))}")
for name in expected:
    manifest = pathlib.Path(packages[name]["manifest_path"]).resolve()
    if not manifest.is_relative_to(unpacked_root):
        raise SystemExit(f"{name} resolved outside the unpacked package closure: {manifest}")
for package in metadata["packages"]:
    manifest = pathlib.Path(package["manifest_path"]).resolve()
    if (package["source"] is None or manifest.is_relative_to(repo_root)) and not manifest.is_relative_to(unpacked_root):
        raise SystemExit(f"package resolved back to source: {manifest}")

nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
engine = packages["contextdb-engine"]
fork = packages["contextdb-redb"]
vector = packages["contextdb-vector"]
hnsw = packages["contextdb-hnsw"]
engine_deps = {dependency["name"]: dependency["pkg"] for dependency in nodes[engine["id"]]["deps"]}
if engine_deps.get("redb") != fork["id"]:
    raise SystemExit("packaged engine does not resolve its redb alias to contextdb-redb")
vector_deps = {dependency["name"]: dependency["pkg"] for dependency in nodes[vector["id"]]["deps"]}
if vector_deps.get("hnsw_rs") != hnsw["id"]:
    raise SystemExit("packaged vector dependency does not resolve to contextdb-hnsw")
print("Verified unpacked engine closure:")
print(json.dumps({name: packages[name]["manifest_path"] for name in sorted(expected)}, indent=2))
PY
cargo build --manifest-path "$unpacked_root/Cargo.toml" --locked -p contextdb-engine
