#!/usr/bin/env python3
"""Offline regression controls for the exact delivery exception to PR checks."""
import copy
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import sys
import unittest
from unittest.mock import patch
sys.dont_write_bytecode = True
import yaml

spec = importlib.util.spec_from_file_location('required', Path(__file__).with_name('ci-required.py'))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
REQUIRED = {'lint', 'test-bundles', 'build', 'features',
            'installed-smoke', 'cross-compile', 'impact', 'integration'}
WRITE = {'contents': 'write', 'actions': 'read'}
CALLER = {'needs': ['required'],
          'if': "github.event_name == 'push' && github.ref == 'refs/heads/dev' && github.repository == 'context-graph-ai/contextdb'",
          'uses': './.github/workflows/build.yml', 'permissions': WRITE}


def load(text):
    loader = type('Loader', (yaml.SafeLoader,), {})
    loader.yaml_implicit_resolvers = {k: [(tag, pat) for tag, pat in v if tag != 'tag:yaml.org,2002:bool']
                                       for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()}
    loader.add_implicit_resolver('tag:yaml.org,2002:bool', re.compile(r'^(?:true|false)$'), list('tf'))
    return yaml.load(text, Loader=loader)


def current():
    return load(Path('.github/workflows/ci.yml').read_text())


def candidate():
    doc = current()
    doc['jobs']['development-binaries'] = copy.deepcopy(CALLER)
    return doc


def invoke(doc, needs=None):
    workflow = yaml.safe_dump(doc, sort_keys=False, width=1000, default_flow_style=False)
    # Keep the established one-line aggregate form accepted by ci-required.py.
    workflow = re.sub(r'    needs:\n(?:    - [\w-]+\n)+',
                      lambda m: '    needs: [' + ', '.join(re.findall(r'    - ([\w-]+)', m[0])) + ']\n', workflow)
    needs = {name: {'result': 'success'} for name in REQUIRED} if needs is None else needs
    with patch.object(r.Path, 'read_text', return_value=workflow), \
         patch.dict(os.environ, {'CI_NEEDS': json.dumps(needs)}, clear=True), \
         contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return r.main()


class RequiredControls(unittest.TestCase):
    def test_original_required_inventory_and_release_compile_job_unchanged(self):
        doc = current()
        self.assertEqual(r.REQUIRED, REQUIRED)
        self.assertEqual(doc['jobs']['required']['needs'],
                         ['lint', 'test-bundles', 'build', 'features',
                          'installed-smoke', 'cross-compile', 'impact', 'integration'])
        release = load(RELEASE_JOB)['build']
        self.assertEqual(doc['jobs']['build']['steps'], release['steps'])
        self.assertEqual(doc['jobs']['build']['timeout-minutes'], release['timeout-minutes'])
        self.assertEqual(doc['permissions'], {'contents': 'read'})
        aggregate = doc['jobs']['required']
        self.assertEqual(aggregate['name'], 'PR checks')
        self.assertEqual(aggregate['if'], 'always()')
        step = aggregate['steps'][-1]
        self.assertEqual(step['env']['CI_NEEDS'], '${{ toJSON(needs) }}')
        self.assertEqual(step['run'], 'python3 scripts/ci-required.py')

    def test_actual_ci_has_exact_same_commit_delivery_caller(self):
        doc = current()
        self.assertEqual(set(doc['jobs']), REQUIRED | {'required', 'development-binaries'})
        caller = doc['jobs']['development-binaries']
        for key, value in CALLER.items():
            actual = caller[key]
            if key == 'if':
                actual = ' '.join(actual.removeprefix('${{').removesuffix('}}').split())
            self.assertEqual(actual, value, key)
        self.assertFalse({'runs-on', 'steps', 'secrets', 'with'} & set(caller))
        self.assertEqual([name for name, job in doc['jobs'].items()
                          if 'write' in job.get('permissions', {}).values()], ['development-binaries'])

    def test_required_checker_accepts_one_delivery_job_outside_aggregate(self):
        self.assertEqual(invoke(candidate()), 0)

    def assert_caller_permissions_rejected(self, permissions):
        doc = candidate()
        self.assertEqual(invoke(doc), 0)
        doc['jobs']['development-binaries']['permissions'] = permissions
        self.assertEqual(invoke(doc), 1)

    def test_caller_rejects_id_token_write(self):
        self.assert_caller_permissions_rejected(dict(WRITE, **{'id-token': 'write'}))

    def test_caller_rejects_attestations_write(self):
        self.assert_caller_permissions_rejected(dict(WRITE, attestations='write'))

    def test_caller_rejects_missing_actions_read(self):
        self.assert_caller_permissions_rejected({'contents': 'write'})

    def test_required_checker_rejects_any_other_auxiliary_or_correctness_omission(self):
        mutations = [lambda d: d['jobs'].update(extra={'runs-on': 'ubuntu-latest', 'steps': []}),
                     lambda d: d['jobs']['required']['needs'].remove('build'),
                     lambda d: d['jobs']['required']['needs'].append('development-binaries'),
                     lambda d: d['jobs']['required']['needs'].append('lint'),
                     lambda d: d['jobs'].pop('lint'),
                     lambda d: d['jobs']['lint'].update(permissions={'contents': 'write'}),
                     lambda d: d['permissions'].update(contents='write')]
        for index, mutate in enumerate(mutations):
            with self.subTest(mutation=index):
                doc = candidate()
                mutate(doc)
                self.assertEqual(invoke(doc), 1)

    def test_required_checker_validates_delivery_exception_shape(self):
        mutations = [('needs', []), ('needs', ['build']), ('needs', ['required', 'lint']),
                     ('uses', './.github/workflows/integration.yml'),
                     ('uses', 'context-graph-ai/contextdb/.github/workflows/build.yml@dev'),
                     ('if', "github.event_name == 'pull_request'"),
                     ('if', "github.event_name == 'push'"),
                     ('if', CALLER['if'].replace('refs/heads/dev', 'refs/heads/main')),
                     ('if', CALLER['if'].replace('context-graph-ai/contextdb', 'fork/contextdb')),
                     ('permissions', {'contents': 'write'}),
                     ('permissions', dict(WRITE, actions='write')),
                     ('secrets', 'inherit'), ('steps', [{'run': 'echo extra'}])]
        for key, value in mutations:
            with self.subTest(key=key, value=value):
                doc = candidate()
                doc['jobs']['development-binaries'][key] = value
                self.assertEqual(invoke(doc), 1)

    def test_same_commit_integration_caller_cannot_bypass_coverage(self):
        mutations = [lambda d: d['jobs']['integration'].update(needs=['impact']),
                     lambda d: d['jobs']['integration'].update(**{'if': 'false'}),
                     lambda d: d['jobs']['integration'].update(**{'continue-on-error': True}),
                     lambda d: d['jobs']['integration']['steps'][-1]['env'].pop('CI_COMBINED_MATRIX'),
                     lambda d: d['jobs']['integration']['steps'][-1]['env'].pop('CI_BUNDLE_MATRIX'),
                     lambda d: d['jobs']['integration']['steps'][-1].update(run='true')]
        for mutate in mutations:
            doc = candidate()
            mutate(doc)
            self.assertEqual(invoke(doc), 1)

    def test_required_results_still_fail_closed(self):
        for result in ('failure', 'cancelled', 'skipped', None):
            with self.subTest(result=result):
                needs = {name: {'result': 'success'} for name in REQUIRED}
                needs['test-bundles']['result'] = result
                self.assertEqual(invoke(candidate(), needs), 1)
        self.assertEqual(invoke(candidate(), {}), 1)

    def test_root_chain_runs_after_failure_and_smoke_is_independent(self):
        doc = current()['jobs']
        self.assertNotIn('needs', doc['lint'])
        self.assertNotIn('needs', doc['installed-smoke'])
        for name, predecessor in [('features', 'lint'), ('build', 'features'),
                                  ('cross-compile', 'build')]:
            self.assertEqual(doc[name]['needs'], predecessor)
            self.assertEqual(doc[name]['if'], 'always() && !cancelled()')
        for name, result in [('features', 'failure'), ('build', 'cancelled'),
                             ('cross-compile', 'skipped')]:
            changed = candidate()
            changed['jobs'][name]['if'] = 'success()'
            self.assertEqual(invoke(changed), 1, (name, result))


RELEASE_JOB = 'build:\n    runs-on: ubuntu-latest\n    timeout-minutes: 30\n    steps:\n      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4\n      - name: Free disk space\n        run: bash scripts/ci-free-disk.sh\n      - uses: actions-rust-lang/setup-rust-toolchain@166cdcfd11aee3cb47222f9ddb555ce30ddb9659 # v1\n        with:\n          cache: false\n          rustflags: ""\n      - uses: Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6 # v2\n        with:\n          key: release-debug0-jobs2\n      - run: bash scripts/verify pr build\n'

if __name__ == "__main__":
    unittest.main(verbosity=2)
