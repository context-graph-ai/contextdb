#!/usr/bin/env python3
"""Static acceptance partition and CI projection controls; never invokes Cargo."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import re
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
BASELINE_MODULE_SHA256 = "8e63a05c17941066eb4ac8a59f678ac3a24c35f55f8d9ed3247b580b50bb6149"
BASELINE_HELPERS_SHA256 = "9905e54b79db77270853f14542c5d525b5da1b33a37ca9889d0a28e3941eafbf"
BASELINE_HELPER_TESTS_SHA256 = "9883a4313ce70e8a444b3a7237e587d618fbdc163d2eac86d9acafe40a84d269"
ACCEPTANCE_TARGETS = {
    "acceptance": "../../tests/acceptance.rs",
    "acceptance_long_running": "../../tests/acceptance_long_running.rs",
    "acceptance_sync": "../../tests/acceptance_sync.rs",
}


def modules(path):
    text = path.read_text()
    return re.findall(r'#\[path = "([^"]+)"\]\s+(?:pub )?mod (\w+);', text)


def source_digest(entries):
    digest = hashlib.sha256()
    for relative, name in sorted(set(entries)):
        data = (ROOT / "tests" / relative).read_bytes()
        digest.update(name.encode() + b"\0" + relative.encode() + b"\0" + data)
    return digest.hexdigest()


def reject(call):
    try:
        call()
    except ValueError:
        return
    raise AssertionError("mutation was accepted")


roots = {name: ROOT / path.removeprefix("../../") for name, path in ACCEPTANCE_TARGETS.items()}
partitioned = {name: modules(path) for name, path in roots.items()}
assert ("acceptance/long_running.rs", "long_running") not in partitioned["acceptance"]
assert ("acceptance/sync.rs", "sync") not in partitioned["acceptance"]
assert partitioned["acceptance_long_running"] == [
    ("support/binary_path_helpers.rs", "binary_path"),
    ("acceptance/common.rs", "common"),
    ("acceptance/long_running.rs", "long_running"),
]
assert partitioned["acceptance_sync"] == [
    ("support/binary_path_helpers.rs", "binary_path"),
    ("acceptance/common.rs", "common"),
    ("acceptance/sync.rs", "sync"),
]
source_entries = {entry for entries in partitioned.values() for entry in entries
                  if entry[0] not in {"support/binary_path.rs", "support/binary_path_helpers.rs"}}
assert len(source_entries) == 48
assert source_digest(source_entries) == BASELINE_MODULE_SHA256

helpers = (ROOT / "tests/support/binary_path_helpers.rs").read_bytes()
assert hashlib.sha256(helpers + b"\n").hexdigest() == BASELINE_HELPERS_SHA256
support = (ROOT / "tests/support/binary_path.rs").read_bytes()
marker = b"#[cfg(test)]\nmod resolve_target_dir_tests"
helper_tests = support[support.index(marker):]
assert hashlib.sha256(helper_tests).hexdigest() == BASELINE_HELPER_TESTS_SHA256
assert helper_tests.count(b"#[test]") == 3
assert all(marker not in path.read_bytes() for name, path in roots.items() if name != "acceptance")

cargo = (ROOT / "crates/contextdb-engine/Cargo.toml").read_text()
for target, path in ACCEPTANCE_TARGETS.items():
    block = f'[[test]]\nname = "{target}"\npath = "{path}"'
    assert cargo.count(block) == 1

spec = importlib.util.spec_from_file_location("partition_integration", ROOT / "scripts/ci-integration.py")
integration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(integration)
rows = integration.load_manifest()
manifest_path = ROOT / "scripts/ci-integration-targets.json"
manifest = json.loads(manifest_path.read_text())
assert len(rows) == 36
assert len({row.get("logical_id", row["id"]) for row in rows}) == 30
assert len(integration.pr_inventory()) == 26
logical_pr = {
    (row.get("cache_parent", row["shard"]), row["crate"], row["features"])
    for job in integration.pr_inventory() for row in job.get("constituents", [job])
}
assert len(logical_pr) == 29

identity = {"source_sha": "1" * 40, "source_fingerprint": "2" * 64}
for proved, physical in ((True, 49), (False, 53)):
    certificate = ({"targets": {target: {} for target in integration.NORMAL_DEDUP_TARGETS},
                    "sha256": "a" * 64, "reason": "static fixture"}
                   if proved else {"targets": {}, "reason": "static fallback fixture"})
    with patch.object(integration, "source_identity", return_value=identity), \
         patch.object(integration, "normal_dedup_certificate", return_value=certificate):
        decision = integration.plan_decision("normal", False)
        combined = integration.combined_matrix(rows, True, decision)
        bundles = integration.bundle_matrix(combined, rows, True, decision)
        assert len(combined["include"]) == physical
        assert len(bundles["include"]) == 16
        assert all(sum(bundle[f"slot{slot}"] is not None for slot in range(1, 5)) <= 4
                   for bundle in bundles["include"])

read_text = Path.read_text
for mutation in ("missing", "duplicate", "cache-parent", "case-loss"):
    changed = copy.deepcopy(manifest)
    if mutation == "missing":
        changed["pr_jobs"] = [row for row in changed["pr_jobs"]
                              if row["shard"] != "contextdb-engine-test-seams-13"]
    elif mutation == "duplicate":
        changed["slices"].append(copy.deepcopy(next(
            row for row in changed["slices"] if row["id"] == "acceptance-sync-test")))
    elif mutation == "cache-parent":
        next(row for row in changed["pr_jobs"]
             if row["shard"] == "contextdb-engine-test-seams-14")["cache_parent"] = "contextdb-core"
    else:
        next(row for row in changed["slices"]
             if row["id"] == "acceptance-sync-test")["targets"] = []
    encoded = json.dumps(changed)
    with patch.object(Path, "read_text", lambda path, *args, **kwargs:
                      encoded if path == manifest_path else read_text(path, *args, **kwargs)):
        reject(integration.pr_inventory if mutation in {"missing", "cache-parent"}
               else integration.load_manifest)

print("Acceptance partition controls PASS: 48 source modules unchanged; 3 helper tests once; "
      "29 logical PR groups; 30 logical qualification slices; 49/53 physical normal/fallback rows. No Cargo or native tests.")

# Normal-only filters must preserve each of the three functional witnesses once.
import tempfile
cases = integration.LONG_RUNNING_CASES
assert len(cases) == len(set(cases)) == 3
assert sorted(case for part in integration.LONG_RUNNING_PARTITIONS.values() for case in part) == sorted(cases)
with patch.object(integration, "source_identity", return_value=identity), \
     patch.object(integration, "normal_dedup_certificate", return_value=certificate):
    decision = integration.plan_decision("normal", False)
    selected = integration.active_rows(rows, False, "normal", decision)
    combined = integration.combined_matrix(rows, True, decision)
    bundles = integration.bundle_matrix(combined, rows, True, decision)["include"]
    placed = [bundle[f"slot{slot}"] for bundle in bundles for slot in range(1, 5)
              if bundle[f"slot{slot}"] is not None]
    for row_id in ("pr-contextdb-server-test-seams", "pr-contextdb-core-test-seams", "pr-contextdb-core"):
        assert sum(slot["id"] == row_id for slot in placed) == 1
    for row_id in ("acceptance-long-running-transfer-test", "acceptance-long-running-test"):
        row = next(row for row in selected if row["id"] == row_id)
        matches = [slot for slot in placed if slot["id"] == "integration-" + row_id]
        assert len(matches) == 1
        slot = matches[0]
        assert slot["cache_key"] == "integration-acceptance-long-running-test-debug0-jobs2-test"
        command = integration.commands(row, mode="normal")[0]
        assert command[:10] == ["cargo", "test", "--locked", "-p", "contextdb-engine", "--features", integration.FEATURES, "--test", "acceptance_long_running", "--"]
        assert command[10:] == ["--test-threads=1"] + (["--skip", cases[2]] if "transfer" in row_id else [cases[2], "--exact"])
        expected = integration.LONG_RUNNING_PARTITIONS[row_id]
        entry = dict(target="../../tests/acceptance_long_running.rs", passed=len(expected), failed=0, ignored=0, measured=0, filtered_out=3-len(expected))
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "test.log"
            log.write_text("".join(f"test {case} ... ok\n" for case in expected))
            integration.long_running_counts([entry], log, row_id)
            assert entry["passed_cases"] == list(expected)
            log.write_text("".join(f"test {case} ... \x1b[32mok\x1b[0m\n" for case in expected))
            integration.long_running_counts([entry], log, row_id)
            for bad_names in (expected[:-1], expected + expected[:1], ("wrong::case",), ()):
                log.write_text("".join(f"test {case} ... ok\n" for case in bad_names))
                reject(lambda: integration.long_running_counts([entry], log, row_id))
            log.write_text("".join(f"test {case} ... ok\n" for case in expected))
            for key in ("passed", "failed", "ignored", "measured", "filtered_out", "target"):
                changed = dict(entry); changed[key] = "wrong" if key == "target" else changed[key] + 1
                reject(lambda: integration.long_running_counts([changed], log, row_id))
            reject(lambda: integration.long_running_counts([], log, row_id))
            reject(lambda: integration.long_running_counts([entry, entry], log, row_id))
        for key, value in (("targets", ["acceptance"]), ("cache_parent", "changed")):
            changed = dict(row); changed[key] = value
            reject(lambda: integration.commands(changed, mode="normal"))
# Every source dependency is fenced, including the f27 "50 applied" assertion.
read_bytes = Path.read_bytes
for relative in integration.LONG_RUNNING_FINGERPRINTS:
    with patch.object(Path, "read_bytes", lambda path, *args, **kwargs:
                      read_bytes(path, *args, **kwargs) + (b"changed" if path == ROOT / relative else b"")):
        reject(integration.long_running_contract)
print("Two-tail controls PASS: exact routing, old cache binding, 2+1 passed names, zero ignored, strict counts and source drift refusals.")
