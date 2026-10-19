#!/usr/bin/env python3
"""Offline omission and false-success controls; never executes Cargo."""
import hashlib
import sys
import importlib.util
import itertools
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

PATH = Path(__file__).with_name('ci-continuity.py')
SPEC = importlib.util.spec_from_file_location('continuity', PATH)
CI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CI)
PROBES = []


class Controls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name) / 'candidate'
        shutil.copytree(CI.ROOT, cls.root, ignore=shutil.ignore_patterns('.git', '__pycache__'))

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def mutate(self, relative, change):
        p = self.root / relative
        before = p.read_text()
        after = change(before)
        self.assertNotEqual(before, after, 'mutation must actually change input')
        try:
            p.write_text(after)
            with self.assertRaises((ValueError, KeyError, TypeError)):
                CI.check(self.root)
            PROBES.append({'path': relative, 'before_sha256': hashlib.sha256(before.encode()).hexdigest(), 'mutant_sha256': hashlib.sha256(after.encode()).hexdigest(), 'mutant_rejected': True})
        finally:
            p.write_text(before)

    def test_baseline(self):
        inv = CI.check(self.root)
        self.assertEqual(39, sum(t['crate'] == 'contextdb-engine' and t['kind'] == 'test' for t in inv['targets']))
        self.assertEqual(17, len(inv['shards']))

    def test_each_shard_omission(self):
        data = json.loads((self.root / 'scripts/ci-continuity-inventory.json').read_text())
        for row in data['shards']:
            with self.subTest(shard=row['shard']):
                self.mutate('.github/workflows/rc2-tests.yml', lambda s: '\n'.join(line for line in s.split('\n') if 'shard: ' + row['shard'] + ',' not in line))

    def test_feature_omissions(self):
        for feature in ('test-seams,iroh', 'contextdb-internal,test-seams'):
            self.mutate('.github/workflows/rc2-tests.yml', lambda s: s.replace('features: "' + feature + '"', 'features: ""'))
        self.mutate('.github/workflows/nightly.yml', lambda s: s.replace('test-seams,iroh,nightly-tests', 'test-seams,iroh'))

    def test_inventory_target_omission(self):
        data = json.loads((self.root / 'scripts/ci-continuity-inventory.json').read_text())
        for target in data['targets']:
            if target['crate'] == 'contextdb-engine' and target['kind'] == 'test':
                with self.subTest(target=target['name']):
                    self.mutate('scripts/ci-continuity-inventory.json', lambda s: json.dumps(dict(json.loads(s), targets=[t for t in json.loads(s)['targets'] if t != target])))

    def test_new_uncovered_required_feature(self):
        self.mutate('crates/contextdb-engine/Cargo.toml', lambda s: s.replace('required-features = ["test-seams", "iroh"]', 'required-features = ["test-seams", "mdns"]'))

    def test_removed_default_or_smoke_job(self):
        for key in ('test', 'installed-smoke', 'cross-compile'):
            self.mutate('.github/workflows/ci.yml', lambda s: s.replace('\n  ' + key + ':', '\n  omitted-' + key + ':'))

    def test_false_success_and_skips(self):
        for relative, old, new in [
            ('.github/workflows/ci.yml', 'cargo test --workspace', 'cargo test --workspace || true'),
            ('.github/workflows/rc2-tests.yml', 'exit "$status"', 'exit 0'),
            ('.github/workflows/rc2-tests.yml', 'needs: [lint, test]', 'needs: [test]'),
            ('.github/workflows/ci.yml', 'if: always()', "if: success()"),
            ('.github/workflows/ci.yml', '  test:\n', '  test:\n    continue-on-error: true\n'),
            ('.github/workflows/ci.yml', '  lint:\n', '  lint:\n    if: false\n'),
            ('.github/workflows/ci.yml', 'aggregate lint test installed-smoke cross-compile', 'aggregate lint test')]:
            with self.subTest(mutation=new):
                self.mutate(relative, lambda s: s.replace(old, new))

    def test_aggregate_real_process(self):
        # Execute the actual workflow entry point, with independently specified job outcomes.
        for names in (['lint', 'test'], ['lint', 'test', 'installed-smoke', 'cross-compile']):
            for values in itertools.product(('success', 'failure', 'cancelled', 'skipped'), repeat=len(names)):
                results = {k: {'result': v} for k, v in zip(names, values)}
                try:
                    CI.aggregate(names, results)
                    accepted = True
                except ValueError:
                    accepted = False
                self.assertEqual(all(v == 'success' for v in values), accepted, results)
            cases = [({}, False), ({k: {'result': 'success'} for k in names}, True),
                     ({k: {'result': 'success'} for k in names[:-1]}, False),
                     ({**{k: {'result': 'success'} for k in names}, 'publisher': {'result': 'success'}}, False),
                     ({k: {'result': 'neutral'} for k in names}, False)]
            for results, expected in cases:
                p = subprocess.run(['python3', str(PATH), 'aggregate', *names], env={**os.environ, 'NEEDS_JSON': json.dumps(results)}, capture_output=True)
                self.assertEqual(p.returncode == 0, expected, p.stderr)


if __name__ == '__main__':
    result = unittest.main(verbosity=2, exit=False).result
    if os.environ.get('CI_CONTINUITY_PROBES'):
        Path(os.environ['CI_CONTINUITY_PROBES']).write_text(json.dumps({'tests_successful': result.wasSuccessful(), 'baseline': 'accepted', 'mutations': PROBES, 'aggregate_outcome_combinations': 272, 'aggregate_subprocess_cases': 10}, indent=2) + '\n')
    sys.exit(0 if result.wasSuccessful() else 1)
