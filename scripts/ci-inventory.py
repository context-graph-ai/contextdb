#!/usr/bin/env python3
"""Fail closed when PR shards do not cover every workspace package and redb."""
from pathlib import Path
import re
import json
import sys
sys.dont_write_bytecode = True
import tomllib

ROOT = Path(__file__).resolve().parents[1]
# Nightly-only targets must be covered by the exact engine integration commands.
EXTENDED_ONLY = {"nightly-tests"}
EXTENDED_FEATURES = ["nightly-tests", "test-seams", "iroh"]


def feature_closure(package, requested, documents, paths, workspace):
    """Resolve local features without Cargo or external dependency fetching.

    Include defaults and self dev dependencies, then normal dependencies
    transitively. Other dev packages cannot stand in for declared shard features.
    Resolve local dependency aliases by path, not by their spelling. External crate features cannot enable a local
    package feature. Target-specific local edges are unsupported.
    """
    enabled = {name: set() for name in documents}
    active = {package}
    enabled[package].update(requested)
    if "default" in documents[package].get("features", {}):
        enabled[package].add("default")
    while True:
        before = (set(active), {name: set(values) for name, values in enabled.items()})
        for name in list(active):
            doc = documents[name]
            definitions = doc.get("features", {})
            tables = [doc]
            deps = {}
            kinds = ["dependencies"]
            if name == package:
                kinds.append("dev-dependencies")
            for table in tables:
                for kind in kinds:
                    for alias, spec in table.get(kind, {}).items():
                        if not isinstance(spec, dict):
                            continue
                        if spec.get("workspace"):
                            inherited = workspace.get("dependencies", {}).get(alias, {})
                            if isinstance(inherited, str):
                                inherited = {"version": inherited}
                            spec = {**inherited, **spec, "features":
                                    [*inherited.get("features", []), *spec.get("features", [])]}
                            base = workspace["manifest-root"]
                        else:
                            base = paths[name].parent
                        # A normal and dev edge can use the same alias with different
                        # features; retaining both preserves feature unification.
                        deps.setdefault(alias, []).append((spec, base, kind))
            values = set(enabled[name])
            expansions = {item for feature in values
                          for item in definitions.get(feature, [])}
            for item in expansions:
                if "/" not in item and not item.startswith("dep:"):
                    enabled[name].add(item)
            for alias, edges in deps.items():
                strong = {item.split("/", 1)[0] for item in expansions
                          if "/" in item and "?/" not in item}
                activated = alias in values or f"dep:{alias}" in expansions or alias in strong
                for spec, base, kind in edges:
                    if spec.get("optional", False) and not activated:
                        continue
                    if "path" not in spec:
                        continue
                    manifest = (base / spec["path"] / "Cargo.toml").resolve()
                    matches = [local for local, path in paths.items() if path.resolve() == manifest]
                    if len(matches) != 1:
                        raise ValueError(f"unresolved local dependency: {name}/{alias}")
                    local = matches[0]
                    if kind == "dev-dependencies" and local != package:
                        continue
                    active.add(local)
                    if spec.get("default-features", True) and "default" in documents[local].get("features", {}):
                        enabled[local].add("default")
                    enabled[local].update(spec.get("features", []))
                    for item in expansions:
                        if item.startswith(alias + "/") or item.startswith(alias + "?/"):
                            enabled[local].add(item.split("/", 1)[1])
        if before == (active, enabled):
            return enabled[package]


def target_inventory(doc, path, kind):
    targets = {target["name"]: set(target.get("required-features", []))
               for target in doc.get(kind, [])}
    if doc["package"].get("auto" + kind + "s", True):
        directory = path.parent / ("tests" if kind == "test" else "examples")
        if directory.is_dir():
            for entry in directory.iterdir():
                if entry.suffix == ".rs" or (entry.is_dir() and (entry / "main.rs").is_file()):
                    targets.setdefault(entry.stem if entry.is_file() else entry.name, set())
    return targets


def shard_target_contract(root):
    # Feature coverage is valid only while the runner executes these target
    # classes. Fail closed on an unfamiliar runner instead of guessing its scope.
    script = (root / "scripts/ci-test-shard.sh").read_text()
    expected = '''targets=(--lib --bins --tests)
if [[ "$crate" == contextdb-engine && -z "$features" ]]; then
  targets=(--lib --bins)
fi'''
    if expected not in script or script.count("targets=(") != 3 or script.count("targets=()") != 1:
        raise ValueError("unsupported shard target selection/delegation")
    if script.count('"${targets[@]}"') != 2:
        raise ValueError("shard runner lost lib/bin/integration target execution")
    for snippet in ['  [[ "$name" == "$shard:" ]] || continue\n',
                    'if [[ "$item" == lib ]]; then targets+=(--lib --bins); run_extras=true; else targets+=(--test "$item"); fi',
                    'if [[ "$status" == 0 && "$run_extras" == true ]]; then']:
        if script.count(snippet) != 1:
            raise ValueError("shard runner lost split-shard target selection")
    if script.count('--timings "$thread_mode" "${logs[@]}"') != 1:
        raise ValueError("shard runner lost per-target timing receipts")
    for command in ['--examples --no-run', '--doc -- --test-threads=1']:
        if f'cargo test --locked "${{args[@]}}" {command}' not in script:
            raise ValueError(f"shard runner lost target class: {command}")


def split_shard_contract(root, rows, documents, paths, workspace):
    """Every split shard group must run each test target of its unsplit shard exactly once.

    scripts/ci-shard-targets.txt maps split shard -> items (`lib` or a test target).
    The expected set is derived from the crate manifest: all tests whose
    required-features fit the group's feature closure (autotests are off or
    discovered by target_inventory), plus `lib`. A new target in no shard fails.
    """
    manifest = {}
    for line in (root / "scripts/ci-shard-targets.txt").read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        name, sep, items = line.partition(": ")
        if not sep or not items.split() or name in manifest:
            raise ValueError(f"bad split-shard manifest line: {line}")
        manifest[name] = items.split()
    moved = ['bounded_in_subquery_stays_inside_the_read_limits',
             'bounded_read_of_staged_graph_work_stays_within_its_limits',
             'local_transport', 'owner_read', 'reader_release', 'sql_surface',
             'staged_predecode_row', 'text_memory_charge_measurement']
    expected_shards = {
        'contextdb-engine-test-seams-4': ['acceptance'],
        'contextdb-engine-test-seams-13': ['acceptance_long_running'],
        'contextdb-engine-test-seams-14': ['acceptance_sync'],
        'contextdb-engine-test-seams-5': ['sync', *moved],
        'contextdb-engine-test-seams-11': [
            'vector_a_find_decodes_only_the_rows_it_returns', 'custody_delivery_manifest'],
        'contextdb-engine-test-seams-16': [
            'vector_a_find_over_many_contexts_spends_one_search_budget'],
        'contextdb-engine-test-seams-17': [
            'vector_one_search_covers_every_layer_on_both_routes'],
        'contextdb-engine-test-seams-15': ['sync_schema_transactions'],
    }
    for shard, targets in expected_shards.items():
        if manifest.get(shard) != targets:
            raise ValueError('reviewed shard target transfer changed: ' + shard)
    by_name = {shard: (crate, features) for shard, crate, features in rows}
    groups = {}
    for shard, crate, features in rows:
        key = (crate, tuple(sorted(features.split(","))) if features else ())
        groups.setdefault(key, []).append(shard)
    split = {key: shards for key, shards in groups.items() if len(shards) > 1}
    for shard in manifest:
        if shard not in by_name:
            raise ValueError(f"split-shard manifest names unknown shard: {shard}")
    in_split = {s for shards in split.values() for s in shards}
    if not set(manifest) <= in_split:
        raise ValueError(f"manifest lists shards outside a split group: {sorted(set(manifest) - in_split)}")
    for key, shards in split.items():
        if any(s not in manifest for s in shards):
            raise ValueError(f"shards share {key} but are not all in the split manifest: {shards}")
    for key, shards in split.items():
        crate, features = key
        closure = feature_closure(crate, list(features), documents, paths, workspace)
        expected = {"lib"}
        for target, required in target_inventory(documents[crate], paths[crate], "test").items():
            if not (required & EXTENDED_ONLY) and required <= closure:
                expected.add(target)
        listed = [item for shard in shards for item in manifest[shard]]
        duplicated = sorted({i for i in listed if listed.count(i) > 1})
        if duplicated:
            raise ValueError(f"split shards run a target twice: {duplicated}")
        if set(listed) != expected:
            raise ValueError(f"split shards {sorted(shards)} target mismatch: "
                             f"missing={sorted(expected - set(listed))}, extra={sorted(set(listed) - expected)}")
    return split


def integration_target_contract(root):
    import importlib.util
    spec = importlib.util.spec_from_file_location("integration", root / "scripts/ci-integration.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.load_manifest(root)
    module.workflow_contract(root)


def redb_target_contract(root):
    runner = (root / "scripts/ci-test-shard.sh").read_text()
    args = 'args+=(--manifest-path crates/contextdb-redb/Cargo.toml --target-dir "${CARGO_TARGET_DIR:-$PWD/target}/redb")'
    if runner.count(args) != 1:
        raise ValueError("redb shard lost exact isolated target command")
    verify = (root / "scripts/verify").read_text()
    commands = [
        'cargo test --manifest-path crates/contextdb-redb/Cargo.toml --target-dir "${CARGO_TARGET_DIR:-$PWD/target}/redb" --locked',
        'CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/redb" cargo fmt --manifest-path crates/contextdb-redb/Cargo.toml --check',
        'cargo clippy --manifest-path crates/contextdb-redb/Cargo.toml --target-dir "${CARGO_TARGET_DIR:-$PWD/target}/redb" --all-targets -- -D warnings',
    ]
    for command in commands:
        if command == commands[0]:
            continue  # integration runner checks the exact argv independently
        if verify.count(command) != 1:
            raise ValueError(f"redb runner lost exact command: {command}")
    for workflow in ["release.yml", "release-dry-run.yml"]:
        text = (root / ".github/workflows" / workflow).read_text()
        for command in commands:
            if text.count(command) != 1:
                raise ValueError(f"redb publication target mismatch: {workflow}: {command}")
    for workflow, expected in [('ci.yml', 3), ('integration.yml', 1)]:
        text = (root / '.github/workflows' / workflow).read_text()
        if text.count('crates/contextdb-redb -> ../../target/redb') != expected:
            raise ValueError(f"redb cache target mismatch: {workflow}")


def reject_target_local_dependencies(documents, workspace):
    for package, doc in documents.items():
        for target, table in doc.get("target", {}).items():
            for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
                for alias, spec in table.get(kind, {}).items():
                    if not isinstance(spec, dict):
                        continue
                    inherited = workspace.get("dependencies", {}).get(alias, {}) if spec.get("workspace") else {}
                    if "path" in spec or (isinstance(inherited, dict) and "path" in inherited):
                        raise ValueError(f"unsupported target-scoped local path dependency: {package} {target} {kind} {alias}")


def schema_replay_fixture_contract(root):
    """Freeze full resident fixtures independently of the bounded root."""
    tests = root / "crates/contextdb-engine/tests"
    for target, rows in {
            "custody_schema_replay_is_proportional_to_affected_tables": 10_000,
            "custody_schema_replay_work_limited_to_affected_tables": 10_000,
            "custody_schema_replay_identical_schema_does_no_artifact_work": 10_000,
            "custody_schema_replay_bounded_contract": 1}.items():
        text = (tests / (target + ".rs")).read_text()
        declarations = re.findall(r"^const UNRELATED_RESIDENT_ROWS: i64 = ([0-9_]+);$",
                                  text, re.MULTILINE)
        if len(declarations) != 1 or int(declarations[0].replace("_", "")) != rows:
            raise ValueError(f"schema replay frozen fixture mismatch: {target}: expected {rows}")



# Scheduling only: each tuple retains its own command and receipt identity.
BUNDLES = {
    'bundle-transactions-relational': ['contextdb-tx', 'contextdb-tx-test-seams', 'contextdb-relational'],
    'bundle-vector-index-graph': ['contextdb-vector', 'contextdb-vector-test-seams', 'contextdb-hnsw', 'contextdb-graph'],
    'bundle-parser-planner-redb': ['contextdb-parser', 'contextdb-planner', 'contextdb-redb'],
}
def pr_jobs(root):
    manifest = json.loads((root / 'scripts/ci-integration-targets.json').read_text())
    if manifest.get('schema_version') != 2 or not isinstance(manifest.get('pr_jobs'), list):
        raise ValueError('missing canonical PR job inventory')
    jobs = []
    for item in manifest['pr_jobs']:
        common = {'shard', 'name'}
        if not isinstance(item, dict) or not common <= set(item) or not item['name']:
            raise ValueError('invalid PR job')
        if set(item) in (common | {'crate', 'features'},
                         common | {'crate', 'features', 'cache_parent'}):
            rows = [(item['shard'], item['crate'], item['features'])]
        elif set(item) == common | {'constituents'} and item['shard'] in BUNDLES:
            if not isinstance(item['constituents'], list):
                raise ValueError('invalid bundle constituents')
            rows = []
            for row in item['constituents']:
                if not isinstance(row, dict) or set(row) != {'shard', 'crate', 'features'}:
                    raise ValueError('invalid bundle constituent')
                shard, crate, features = row['shard'], row['crate'], row['features']
                if crate != shard.removesuffix('-test-seams') or features != ('test-seams' if shard.endswith('-test-seams') else ''):
                    raise ValueError('bundle changed original command/features tuple')
                rows.append((shard, crate, features))
            if [row[0] for row in rows] != BUNDLES[item['shard']]:
                raise ValueError('missing, duplicate or unsupported bundle constituents')
        else:
            raise ValueError('unsupported PR job shape')
        jobs.append((item['shard'], rows))
    if len(jobs) != 26 or len({name for name, _ in jobs}) != len(jobs):
        raise ValueError('missing or duplicate PR jobs')
    parents = {item['shard']: item.get('cache_parent', item['shard'])
               for item in manifest['pr_jobs']}
    expected_parts = {
        'contextdb-engine-test-seams-4': {
            'contextdb-engine-test-seams-4', 'contextdb-engine-test-seams-13',
            'contextdb-engine-test-seams-14'},
        'contextdb-engine-test-seams-11': {
            'contextdb-engine-test-seams-11', 'contextdb-engine-test-seams-16',
            'contextdb-engine-test-seams-17'},
    }
    for parent, parts in expected_parts.items():
        if {shard for shard, bound in parents.items() if bound == parent} != parts:
            raise ValueError('acceptance/vector physical partitions changed: ' + parent)
    if any(parent not in expected_parts and parent != shard
           for shard, parent in parents.items()):
        raise ValueError('unreviewed PR cache parent binding')
    if len(set(parents.values())) != 22:
        raise ValueError('logical PR job inventory changed')
    if {name for name, _ in jobs if name.startswith('bundle-')} != set(BUNDLES):
        raise ValueError('missing PR bundle')
    return jobs


def inventory(root):
    schema_replay_fixture_contract(root)
    shard_target_contract(root)
    integration_target_contract(root)
    redb_target_contract(root)
    root_manifest = tomllib.loads((root / "Cargo.toml").read_text())
    workspace = root_manifest["workspace"]
    members = workspace["members"]
    if not members or len(members) != len(set(members)):
        raise ValueError("workspace members are empty or duplicated")
    manifests = [root / "Cargo.toml"] if "package" in root_manifest else []
    for member in members:
        matches = sorted(root.glob(member))
        if not matches:
            raise ValueError(f"workspace member has no directory: {member}")
        manifests.extend(path / "Cargo.toml" for path in matches)
    redb = root / "crates/contextdb-redb/Cargo.toml"
    if "crates/contextdb-redb" not in workspace.get("exclude", []):
        raise ValueError("standalone redb must remain explicitly excluded")
    manifests.append(redb)
    if len(manifests) != len(set(manifests)):
        raise ValueError("workspace member paths overlap or duplicate standalone redb")
    documents = {tomllib.loads(path.read_text())["package"]["name"]:
                 tomllib.loads(path.read_text()) for path in manifests}
    paths = {tomllib.loads(path.read_text())["package"]["name"]: path for path in manifests}
    packages = [tomllib.loads(path.read_text())["package"]["name"] for path in manifests]
    if len(packages) != len(set(packages)):
        raise ValueError("duplicate package names in workspace/redb inventory")
    if tomllib.loads(redb.read_text())["package"]["name"] != "contextdb-redb":
        raise ValueError("standalone redb package name changed")

    rows = [row for _, constituents in pr_jobs(root) for row in constituents]
    if not rows:
        raise ValueError("empty PR test matrix")
    if len({row[0] for row in rows}) != len(rows):
        raise ValueError("duplicate shard names in PR matrix")
    keys = [(crate, tuple(sorted(features.split(","))) if features else ())
            for _, crate, features in rows]
    if len(set(keys)) != len(keys):
        # Repeats are legal only as a split group, validated below against the manifest.
        split_keys = {key for key in keys if keys.count(key) > 1}
        for key in split_keys:
            if key[0] != "contextdb-engine" or not key[1]:
                raise ValueError("duplicate package/feature shards in PR matrix")
    expected, actual = set(packages), {crate for _, crate, _ in rows}
    if expected != actual:
        raise ValueError(f"PR package inventory mismatch: missing={sorted(expected - actual)}, "
                         f"extra={sorted(actual - expected)}")
    defaults = {crate for _, crate, features in rows if not features}
    if defaults != expected:
        raise ValueError(f"missing default package shards: {sorted(expected - defaults)}")
    reject_target_local_dependencies(documents, workspace)
    workspace = {**workspace, "manifest-root": root}
    closures = {shard: feature_closure(crate, features.split(",") if features else [],
                                      documents, paths, workspace)
                for shard, crate, features in rows}
    extended = feature_closure("contextdb-engine", EXTENDED_FEATURES, documents, paths, workspace)
    for package, doc in documents.items():
        for kind in ("test", "example"):
            for target, required in target_inventory(doc, paths[package], kind).items():
                if required & EXTENDED_ONLY:
                    if package != "contextdb-engine" or not required <= extended:
                        raise ValueError(f"uncovered extended {package} {kind} {target}: "
                                         f"required-features={sorted(required)}")
                    continue
                candidates = [(shard, features) for shard, crate, features in rows
                              if crate == package and not
                              (kind == "test" and crate == "contextdb-engine" and not features)]
                if not any(required <= closures[shard] for shard, _ in candidates):
                    raise ValueError(f"uncovered {package} {kind} {target}: "
                                     f"required-features={sorted(required)}")
    split_shard_contract(root, rows, documents, paths, workspace)
    return packages, rows


def timing_receipt(logs, thread_mode):
    """Pair Cargo's target header with its libtest summary; never invent durations."""
    records = []
    ansi = re.compile(r"\x1b\[[0-9;]*m")
    for log in logs:
        target = None
        record = None
        expected_count = None
        for raw in Path(log).read_text(encoding="utf-8", errors="replace").split("\n"):
            line = re.sub(r"^\S+Z ", "", ansi.sub("", raw))
            header = re.fullmatch(r"[ \t]+Running (\S.*?) \(([^()]+)\)\s*", line)
            if header:
                if record is not None:
                    records.append(record)
                record = None
                expected_count = None
                target = (header[1], header[2])
            elif re.fullmatch(r"[ \t]+Doc-tests \S+\s*", line):
                if record is not None:
                    records.append(record)
                record = None
                expected_count = None
                target = (line.split("Doc-tests ", 1)[1].strip(), None)
            count = re.search(r"^running (\d+) tests?$", line.strip())
            if count and expected_count is None:
                expected_count = int(count[1])
            summary = re.search(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in ([\d.]+)s", line)
            if (summary and target is not None and expected_count is not None
                    and sum(int(summary[n]) for n in range(2, 6)) == expected_count):
                record = dict(log=Path(log).name, target=target[0], executable=target[1],
                                    test_threads="1" if ".doc." in str(log) else thread_mode,
                                    result=summary[1], passed=int(summary[2]), failed=int(summary[3]),
                                    ignored=int(summary[4]), measured=int(summary[5]),
                                    filtered_out=int(summary[6]), duration_seconds=float(summary[7]))
        if record is not None:
            records.append(record)
    return {"schema_version": 1, "duration_source": "cargo-libtest-summary",
            "ordinary_runtime_is_report_only": True, "targets": records}


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "--timings":
        print(json.dumps(timing_receipt(sys.argv[3:], sys.argv[2]), indent=2))
        return 0
    try:
        packages, rows = inventory(ROOT)
        if len(sys.argv) == 3 and sys.argv[1] == '--bundle':
            matches = [items for name, items in pr_jobs(ROOT) if name == sys.argv[2] and name in BUNDLES]
            if len(matches) != 1:
                raise ValueError('unknown PR bundle')
            for shard, crate, features in matches[0]:
                print(f'{crate}|{shard}|{features}')
            return 0
    except (ValueError, KeyError, OSError, TypeError) as error:
        print(f"CI inventory FAILED: {error}", file=sys.stderr)
        return 1
    print(f"CI inventory OK: {len(packages)} packages (workspace + standalone redb), "
          f"{len(rows)} unique shards")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
