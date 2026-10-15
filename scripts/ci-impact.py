#!/usr/bin/env python3
"""Map changed contracts to integration and pending controlled performance measurement."""
import json
import os
from pathlib import Path, PurePosixPath
import subprocess

import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

from ci_bench_inventory import performance_paths


# Auditable contract map: only these known performance fixtures and metadata
# omit added nightly correctness. Every original PR check still runs for them.
# New/unmapped files fail closed to full integration, even under benches/.
ROOT = Path(__file__).resolve().parents[1]
PERFORMANCE_ONLY = performance_paths(ROOT)
# Human summary labels only; GITHUB_OUTPUT keeps the raw compatibility values.
PERFORMANCE_LABELS = {
    "pending-controlled-native-qualification": "measurement pending",
    "unaffected": "not affected",
}
PERFORMANCE_METADATA = frozenset({
    '.github/workflows/performance.yml', 'scripts/ci-performance.py',
})
MAINTAINER_METADATA = frozenset({
    ".gitignore", ".dockerignore",
    "crates/contextdb-hnsw/MAINTENANCE.md", "crates/contextdb-redb/MAINTENANCE.md",
})
RUNTIME_OR_INTEGRATION = ("crates/", "tests/", "scripts/", ".github/workflows/")
BUILD_INPUTS = frozenset({"Cargo.toml", "Cargo.lock", "rust-toolchain.toml",
                          "Dockerfile", "docker-compose.yml", "book.toml"})


def path_impact(path):
    # Reject noncanonical paths before any allowlist match.
    if not path or str(PurePosixPath(path)) != path or path.startswith("/") or ".." in PurePosixPath(path).parts:
        return "unknown"
    if path in PERFORMANCE_ONLY or path in PERFORMANCE_METADATA:
        return "performance-only"
    if path in MAINTAINER_METADATA:
        return "maintainer-metadata"
    if ((path.startswith(("docs/", "skills/")) and path.endswith(".md"))
            or (len(PurePosixPath(path).parts) == 1 and
                (path.endswith(".md") or path in {"LICENSE", "NOTICE"}))):
        return "documentation-only"
    if path.startswith(RUNTIME_OR_INTEGRATION) or path in BUILD_INPUTS:
        return "runtime-or-integration"
    return "unknown"


def impact(paths):
    mapping = {path: path_impact(path) for path in paths}
    required = not paths or any(kind in {"runtime-or-integration", "unknown"}
                                for kind in mapping.values())
    performance = not paths or any(kind in {"runtime-or-integration", "performance-only", "unknown"}
                                   for kind in mapping.values())
    return {"integration_required": required,
            "reason": ("missing-or-empty-diff" if not paths else
                       ("integration-required:" if required else "integration-unaffected:") +
                       ",".join(sorted(set(mapping.values())))),
            "path_impact": mapping,
            "performance_qualification": "pending-controlled-native-qualification" if performance else "unaffected",
            "original_pr_checks": "all-required"}


def integration_required(paths):
    return impact(paths)["integration_required"]


def main():
    event = json.loads(open(os.environ["GITHUB_EVENT_PATH"]).read())
    base = (event.get("pull_request", {}).get("base", {}).get("sha")
            or event.get("before"))
    paths = []
    if base and base != "0" * 40:
        try:
            # Missing public history never becomes a silent docs-only skip.
            raw = subprocess.check_output(["git", "diff", "--no-renames", "--name-only", "-z", base, "HEAD"])
            paths = [p.decode("utf-8") for p in raw.split(b"\0") if p]
        except (subprocess.CalledProcessError, UnicodeError):
            pass
    result = impact(paths)
    required = result["integration_required"]
    print(json.dumps({**result, "paths": paths}))
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"integration_required={str(required).lower()}\n")
        output.write(f"impact_reason={result['reason']}\n")
        output.write(f"performance_qualification={result['performance_qualification']}\n")
        # JSON escapes newlines in fork-controlled paths; no output injection.
        output.write(f"path_classes={json.dumps(result['path_impact'], ensure_ascii=True)}\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as output:
            output.write(f"Integration required: **{str(required).lower()}**; reason: **{result['reason']}**.\n\n")
            label = PERFORMANCE_LABELS[result["performance_qualification"]]
            output.write(f"Performance measurement: **{label}** (not a qualification and not a required gate). All original PR checks remain required.\n\n")
            # HTML escaping prevents fork-controlled paths from altering markup.
            import html
            output.write("Path classes: <pre>" + html.escape(json.dumps(result['path_impact'], ensure_ascii=True, indent=2)) + "</pre>\n")


if __name__ == "__main__":
    main()
