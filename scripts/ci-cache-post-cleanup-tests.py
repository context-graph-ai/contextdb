#!/usr/bin/env python3
"""Offline controls for the pinned rust-cache post-job scratch repair."""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts/ci-cache-post-cleanup.sh"
PIN = "6323deb102c322ba6fcbdcafc7e3dddab59af2b6"

# Faithful executable reduction of cleanup.ts at the pinned revision: `tests`
# is a profile and its two nested cleaners are deliberately not awaited.
PINNED_FIXTURE = r"""
const fs = require('fs');
const path = require('path');
let tests = 0;
async function cleanTargetDir(targetDir) {
  const dir = await fs.promises.opendir(targetDir);
  for await (const dirent of dir) {
    if (dirent.isDirectory() && dirent.name === 'tests') {
      tests++;
      const profileDir = path.join(dir.path, dirent.name);
      try { cleanTargetDir(path.join(profileDir, 'target')); } catch {}
      try { cleanTargetDir(path.join(profileDir, 'trybuild')); } catch {}
    }
  }
}
process.on('unhandledRejection', error => {
  console.error(error.code + ' ' + error.path);
  process.exitCode = 23;
});
cleanTargetDir(process.argv[2]).then(() => setTimeout(() => {
  console.log('tests=' + tests);
}, 20));
"""


class CachePostCleanupControls(unittest.TestCase):
    def fixture(self, root):
        (root / "scripts").mkdir()
        shutil.copy2(HELPER, root / "scripts/ci-cache-post-cleanup.sh")

    def run_helper(self, root):
        env = dict(os.environ, GITHUB_ACTIONS="true", CI_GATE_MODE="normal")
        return subprocess.run(["bash", "scripts/ci-cache-post-cleanup.sh"], cwd=root,
                              env=env, capture_output=True, text=True)

    def run_pinned(self, root):
        fixture = root / "pinned-cleanup.js"
        fixture.write_text(PINNED_FIXTURE)
        return subprocess.run(["node", str(fixture), str(root / "target")],
                              cwd=root, capture_output=True, text=True)

    def test_pinned_missing_nested_target_then_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            retained = {
                root / "target/debug/compiled": b"compiled",
                root / "target/ci-shard-out/receipt": b"receipt",
                root / "target/redb/debug/compiled": b"redb",
                root / "target/redb/ci-shard-out/receipt": b"redb receipt",
            }
            for path, content in retained.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            (root / "target/tests/trybuild/contextdb-core").mkdir(parents=True)
            (root / "target/redb/tests/trybuild/contextdb-redb").mkdir(parents=True)
            before = {p: hashlib.sha256(v).hexdigest() for p, v in retained.items()}

            original = self.run_pinned(root)
            self.assertEqual(original.returncode, 23, original.stderr)
            self.assertIn("ENOENT", original.stderr)
            self.assertIn("target/tests/target", original.stderr)

            cleaned = self.run_helper(root)
            self.assertEqual(cleaned.returncode, 0, cleaned.stderr)
            self.assertFalse((root / "target/tests").exists())
            self.assertFalse((root / "target/redb/tests").exists())
            self.assertEqual(before, {p: hashlib.sha256(p.read_bytes()).hexdigest()
                                      for p in retained})
            after = self.run_pinned(root)
            self.assertEqual(after.returncode, 0, after.stderr)
            self.assertIn("tests=0", after.stdout)

    def test_missing_roots_are_noop(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            run = self.run_helper(root)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertFalse((root / "target").exists())

    def test_symlink_escape_is_rejected_and_failure_visible(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            outside = root / "outside"
            outside.mkdir()
            sentinel = outside / "sentinel"
            sentinel.write_text("retain")
            (root / "target").mkdir()
            (root / "target/tests").symlink_to(outside, target_is_directory=True)
            run = self.run_helper(root)
            self.assertNotEqual(run.returncode, 0)
            self.assertEqual(sentinel.read_text(), "retain")
            self.assertIn("Refusing unsafe cache scratch directory", run.stderr)

    def test_workflow_order_condition_and_pin(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        gate = workflow.index("- name: Require every bundled constituent to succeed")
        cleanup = workflow.index("- name: Remove disposable cache test scratch")
        build = workflow.index("\n  build:", cleanup)
        self.assertLess(gate, cleanup)
        self.assertLess(cleanup, build)
        block = workflow[cleanup:build]
        self.assertIn("if: always() && !cancelled()", block)
        self.assertIn("run: bash scripts/ci-cache-post-cleanup.sh", block)
        self.assertNotIn("continue-on-error", block)
        test_job = workflow[workflow.index("  test-bundles:"):build]
        self.assertEqual(test_job.count("Swatinem/rust-cache@" + PIN), 4)
        self.assertEqual(test_job.count(". -> target"), 4)
        self.assertEqual(test_job.count("crates/contextdb-redb -> ../../target/redb"), 4)


if __name__ == "__main__":
    unittest.main()
