#!/usr/bin/env python3
"""Existing native benchmark commands and truthful qualification receipts."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import shlex
import uuid
import subprocess
import time

import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

from ci_bench_inventory import declared_benches, performance_paths

ROOT = Path(__file__).resolve().parents[1]
# Existing fixtures, unmodified source thresholds and full Criterion defaults.
PROFILES = {
    "storage": [("contextdb-engine", "storage_scale_hardening", []),
                ("contextdb-engine", "engine_full_throughput", [])],
    "vector": [("contextdb-engine", "engine_throughput", ["nv_"]),
               ("contextdb-engine", "rank_policy_recall", [])],
    "workflows": [("contextdb-engine", "mixed_workflows_pr", []),
                  ("contextdb-engine", "subscriptions_pr", []),
                  ("contextdb-server", "sync_pr", [])],
    "system": [("contextdb-server", "server_throughput", []),
               ("contextdb-server", "server_sync_system", [])],
}


def capture(*command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def fresh_output(profile):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    sha = capture("git", "rev-parse", "HEAD")[:12]
    return (ROOT.parent / "contextdb-performance-receipts" /
            f"{sha}-{profile}-{stamp}-{uuid.uuid4().hex[:8]}").resolve()


def manifest(profile, native_output=None):
    native_output = fresh_output(profile) if native_output is None else native_output.resolve()
    inventory = declared_benches(ROOT)
    by_target = {(row["package"], row["name"]): row for row in inventory}
    fixtures = [(row["package"], row["name"], []) for row in inventory] if profile == "all" else PROFILES[profile]
    selected = []
    commands = []
    for crate, bench, filters in fixtures:
        row = by_target[(crate, bench)]
        if filters and row["harness"] != "criterion":
            raise ValueError(f"Criterion filter on {row['harness']} benchmark: {crate}/{bench}")
        command = ["cargo", "bench", "--locked", "-p", crate, "--bench", bench]
        if row["required_features"]:
            command += ["--features", ",".join(row["required_features"])]
        # Cargo supplies its own benchmark-mode flag to each harness. Only
        # checked Criterion targets receive the existing optional name filter.
        if filters:
            command += ["--", *filters]
        commands.append(command)
        selected.append({**row, "filters": filters, "command": command})
    selected_keys = {(row["package"], row["name"]) for row in selected}
    not_run = [row for row in inventory if (row["package"], row["name"]) not in selected_keys]
    paths = ["Cargo.lock", "crates/contextdb-redb/Cargo.lock"]
    paths += capture("git", "ls-files", "crates/*/Cargo.toml").splitlines()
    paths += sorted(performance_paths(ROOT))
    hashes = {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest()
              for p in paths if (ROOT / p).is_file()}
    if profile in {"system", "all"}:
        # Existing process fixtures resolve ROOT/target/release, not arbitrary
        # CARGO_TARGET_DIR. Build these exact checkout binaries before timing.
        commands.insert(0, ["cargo", "build", "--locked", "--release", "-p",
                            "contextdb-cli", "-p", "contextdb-server"])
    return {
        "schema": 2, "source_sha": capture("git", "rev-parse", "HEAD"),
        "source_dirty": bool(capture("git", "status", "--porcelain")),
        "profile": profile, "qualification": "pending-native-host",
        "fixture_and_dependency_sha256": hashes,
        "environment": {"CONTEXTDB_NV_BENCH_FULL": "1"},
        "commands": commands,
        "declared_benchmark_count": len(inventory),
        "selected_benchmarks": selected,
        "not_run_benchmarks": not_run,
        "filtered_benchmarks": [row for row in selected if row["filters"]],
        "global_performance_qualification": "pending-full-inventory-native-execution-and-product-acceptance",
        "operator_receipt_directory": str(native_output),
        "operator_command": shlex.join(["bash", str(ROOT / "scripts/verify"),
                                         "performance", "run", "--native-host",
                                         "--profile", profile, "--output", str(native_output)]),
        "qualification_pending": [
            "Native execution and same-machine baseline/candidate comparison are required.",
            "Use an isolated disposable checkout with exclusive ROOT/target and no competing runtime/I/O; system fixtures use that fixed target path.",
            "Existing source assertions remain gates; command exit alone is not product acceptance.",
            "Criterion observations must be reviewed for workload completeness.",
            "Product acceptance fixtures outside these benchmarks remain separate gates.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["manifest", "report", "run"])
    parser.add_argument("--profile", choices=["all", *PROFILES], default="all")
    parser.add_argument("--output", type=Path, help="fresh receipt directory (default: unique directory beside checkout); native receipts are PRIVATE and require manual redaction before public export")
    parser.add_argument("--native-host", action="store_true", help="operator confirms controlled isolated native host")
    parser.add_argument("--timeout-seconds", type=int, default=3600, help="per-command runaway budget, not a latency bar")
    args = parser.parse_args()
    if args.mode == "run":
        if not args.native_host or os.environ.get("GITHUB_ACTIONS") or os.environ.get("CI"):
            parser.error("run requires --native-host outside CI/shared GitHub runners")
        if args.timeout_seconds <= 0:
            parser.error("timeout must be positive")
        if os.environ.get("TMPDIR"):
            parser.error("unset TMPDIR: redirected benchmark storage invalidates timing")
        if any(k.startswith(("CONTEXTDB_NV_", "CONTEXTDB_SSH_")) for k in os.environ):
            parser.error("unset benchmark overrides: use the frozen full fixture defaults")
        if capture("git", "status", "--porcelain"):
            parser.error("native receipt requires a clean committed source tuple")
    output = args.output.resolve() if args.output is not None else fresh_output(args.profile)
    # A report is already written before the operator runs: its native destination
    # must remain fresh rather than reusing the report directory.
    result = manifest(args.profile, fresh_output(args.profile) if args.mode == "report" else output)
    if args.mode == "manifest":
        print(json.dumps(result, indent=2))
        return 0
    result["receipt_directory"] = str(output)
    if args.mode == "run":
        # Host identity, absolute paths and raw workload logs are private evidence.
        # Public hosted reports contain only the manifest, never native receipts.
        os.umask(0o077)
        result["visibility"] = "private"
        result["public_export"] = "requires-manual-redaction-and-owner-review; never attach raw native receipts or logs"
    if output.exists():
        parser.error(f"receipt directory already exists: {output}")
    output.mkdir(parents=True, exist_ok=False)
    if args.mode == "run":
        (output / "PRIVATE.txt").write_text(
            "PRIVATE native performance evidence: host details, owner paths and raw logs.\n"
            "Do not attach this directory to public workflows or issues.\n"
            "Manually redact host identity, absolute paths, cgroup details and logs,\n"
            "then obtain owner review before any public export.\n")
    receipt = output / "receipt.json"
    receipt.write_text(json.dumps(result, indent=2) + "\n")
    print("Operator command:", result["operator_command"])
    print("Native receipt directory:", result["operator_receipt_directory"])
    print("Benchmark inventory:", result["declared_benchmark_count"], "selected:", len(result["selected_benchmarks"]), "not run:", json.dumps(result["not_run_benchmarks"]), "filtered:", json.dumps(result["filtered_benchmarks"]))
    if args.mode == "report":
        print("Performance qualification PENDING: native commands/fixture hashes in", receipt)
        return 0
    env = os.environ.copy()
    env.update(result["environment"])
    env["CARGO_TARGET_DIR"] = str(ROOT / "target")
    result["target_directory"] = env["CARGO_TARGET_DIR"]
    result["build_environment"] = {k: env.get(k, "default") for k in
        ["CARGO_BUILD_JOBS", "CARGO_INCREMENTAL", "RUSTFLAGS", "CARGO_PROFILE_BENCH_DEBUG"]}
    result["host"] = {"platform": platform.platform(), "cpu": capture("lscpu"),
                      "filesystem": capture("df", "-T", str(output), "/tmp"),
                      "memory": Path("/proc/meminfo").read_text(),
                      "cgroup": Path("/proc/self/cgroup").read_text(),
                      "rustc": capture("rustc", "-Vv"), "cargo": capture("cargo", "-V"),
                      "TMPDIR": "unset"}
    result["resource_limits"] = {}
    for name in ["memory.max", "cpu.max", "pids.max"]:
        path = Path("/sys/fs/cgroup") / name
        result["resource_limits"][name] = path.read_text().strip() if path.exists() else "unavailable"
    result["executions"] = []
    failed = False
    run_start = time.time()
    for index, command in enumerate(result["commands"]):
        start = time.monotonic()
        with (output / f"command-{index}.log").open("w") as log:
            try:
                process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                status = process.wait(timeout=args.timeout_seconds)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                status = 124
        result["executions"].append({"command": command, "exit": status,
                                     "seconds": time.monotonic() - start,
                                     "raw_log": f"command-{index}.log"})
        failed |= status != 0
        result["execution"] = "failed" if failed else "in-progress"
        receipt.write_text(json.dumps(result, indent=2) + "\n")
        if failed:
            break
    observations = [p for p in (ROOT / "target/criterion").glob("**/new/estimates.json")
                    if p.stat().st_mtime >= run_start]
    result["criterion_observation_count"] = len(observations)
    # Copy raw Criterion observations out of mutable compiler caches.
    raw = output / "criterion"
    for path in observations:
        for source in path.parent.glob("*.json"):
            destination = raw / source.relative_to(ROOT / "target/criterion")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(source.read_bytes())
    result["completed_commands"] = sum(e["exit"] == 0 for e in result["executions"])
    result["expected_commands"] = len(result["commands"])
    result["completed_benchmark_targets"] = sum(e["exit"] == 0 and e["command"][1] == "bench" for e in result["executions"])
    result["expected_benchmark_targets"] = len(result["selected_benchmarks"])
    result["global_performance_qualification"] = ("failed-native-execution" if failed else
        "pending-full-inventory-and-product-acceptance" if result["not_run_benchmarks"] or result["filtered_benchmarks"] else
        "pending-product-acceptance-and-owner-review")
    result["execution"] = "failed" if failed else "completed"
    result["qualification"] = ("failed-native-execution" if failed else
        "pending-full-inventory-and-owner-review" if result["not_run_benchmarks"] or result["filtered_benchmarks"] else
        "pending-owner-review")
    receipt.write_text(json.dumps(result, indent=2) + "\n")
    print("Native execution:", result["execution"], "qualification:", result["qualification"], receipt)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
