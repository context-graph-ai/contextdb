#!/usr/bin/env python3
"""Final offline controls for smoke enforcement, incomplete retention and fixed tags.

Loaded by ci-build-tests.py; exercise the existing publisher guard and gh double.
Mutated publisher modules live only in temporary directories, never in the tree.
"""
import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


def bind(control, publisher):
    global c, p
    c, p = control, publisher


class FinalDeliveryControls(unittest.TestCase):
    def fixture(self):
        fixture = p.SettledPublisherControls()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        return fixture

    def mutant(self, old, new):
        source = Path(c.__file__).read_text()
        self.assertEqual(source.count(old), 1, 'reviewer mutation must match exactly once')
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / 'ci-build-control.py'
        path.write_text(source.replace(old, new, 1))
        spec = importlib.util.spec_from_file_location('final_control_mutant', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_package_step_and_build_job_cannot_skip_or_mask_smoke(self):
        doc = p.SettledWorkflowControls().doc()
        p.build_execution_guard(doc)
        for mutation in ('step-if-false', 'step-continue-on-error', 'job-continue-on-error'):
            with self.subTest(mutation=mutation):
                bad = copy.deepcopy(doc)
                job = bad['jobs']['build']
                step = next(s for s in job['steps']
                            if s.get('run') == 'bash scripts/ci-build-package.sh')
                if mutation == 'step-if-false':
                    step['if'] = False
                elif mutation == 'step-continue-on-error':
                    step['continue-on-error'] = True
                else:
                    job['continue-on-error'] = True
                with self.assertRaises(ValueError):
                    p.build_execution_guard(bad)

    def check_incomplete_groups_survive(self, control):
        fixture = self.fixture()
        fixture.existing()
        preserved = {}
        # Valid, positive run identities; both groups predate all complete groups.
        # Include receipts so the unsafe completeness mutant reaches DELETE.
        for attempt, suffixes in ((1, ('.json',)), (2, ('.tar.gz', '.json'))):
            for suffix in suffixes:
                name = fixture.group(attempt) + suffix
                preserved[name] = b'old incomplete member'
                fixture.remote.assets[name] = preserved[name]
                fixture.remote.times[name] = '2026-01-01T00:00:00Z'
        for attempt in (10, 11, 12, 13):
            for suffix in p.SUFFIXES:
                name = fixture.group(attempt) + suffix
                fixture.remote.assets[name] = b'old complete member'
                fixture.remote.times[name] = f'2026-09-{attempt}T00:00:00Z'
        fixture.attempt(20)
        with patch.object(p, 'c', control):
            fixture.invoke()
        self.assertEqual({name: fixture.remote.assets.get(name) for name in preserved},
                         preserved, 'incomplete run members were deleted')
        self.assertFalse(any(e['kind'] == 'delete' and e['name'] in preserved
                             for e in fixture.remote.events))
        self.assertTrue(any(e['kind'] == 'delete' for e in fixture.remote.events),
                        'fixture must also exercise pruning of complete old groups')

    def test_incomplete_old_one_and_two_member_groups_survive(self):
        self.check_incomplete_groups_survive(c)

    def test_reviewer_incomplete_group_deletion_mutant_is_rejected(self):
        mutant = self.mutant(
            "complete = [items for items in groups.values() if len(items) == 3\n"
            "                and {pattern.fullmatch(a['name'])[2] for a in items}\n"
            "                == {'.tar.gz', '.tar.gz.sha256', '.json'}]",
            'complete = list(groups.values())')
        with self.assertRaisesRegex(AssertionError, 'incomplete run members were deleted'):
            self.check_incomplete_groups_survive(mutant)

    def test_existing_fake_rejects_target_on_release_edit(self):
        fixture = self.fixture()
        fixture.existing(draft=True)
        # The guard/fake implementer may use either existing refusal convention.
        with self.assertRaises((ValueError, AssertionError)):
            fixture.remote.command(['gh', 'release', 'edit', 'dev-latest',
                                    '--repo', c.REPO, '--draft=false', '--target', p.SHA])
        self.assertTrue(fixture.remote.releases[0]['draft'],
                        'rejected edit must not publish the hidden draft')

    def test_canonical_publisher_edits_without_target(self):
        fixture = self.fixture()
        fixture.invoke()
        edits = [e for e in fixture.remote.events if e['kind'] == 'edit']
        self.assertEqual(len(edits), 1)
        self.assertNotIn('--target', edits[0]['args'])
        self.assertEqual(fixture.remote.releases[0]['target_commitish'], p.DEFAULT)

    def test_reviewer_release_edit_target_mutant_is_rejected_by_existing_fake(self):
        mutant = self.mutant(
            "make_latest='false', name=f'Development {sha}', body=",
            "target_commitish=sha, make_latest='false', name=f'Development {sha}', body=")
        fixture = self.fixture()
        with patch.object(p, 'c', mutant):
            with self.assertRaises((ValueError, AssertionError)):
                fixture.invoke()
        self.assertTrue(fixture.remote.releases[0]['draft'])
