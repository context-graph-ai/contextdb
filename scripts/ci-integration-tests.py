#!/usr/bin/env python3
"""Portable integration contract checks; no native Rust commands are executed."""
import copy
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('integration', ROOT / 'scripts/ci-integration.py')
i = importlib.util.module_from_spec(spec)
spec.loader.exec_module(i)
spec = importlib.util.spec_from_file_location('required_controls', ROOT / 'scripts/ci-required-tests.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)


class IntegrationControls(unittest.TestCase):
    def setUp(self):
        mode = patch.dict(os.environ, {'CI_GATE_MODE': 'full', 'GITHUB_ACTIONS': 'false'})
        mode.start()
        self.addCleanup(mode.stop)
        self.rows = i.load_manifest()

    def test_receipt_uploads_are_cancel_safe_and_fail_on_missing_files(self):
        workflow_path = ROOT / '.github/workflows/ci.yml'
        workflow = workflow_path.read_text()

        def validate(source):
            upload_blocks = re.findall(
                r'(?m)^      - name: Upload constituent [123] (?:shard|integration) receipt\n'
                r'(?P<block>.*?)(?=^      - name:|\Z)', source, re.S)
            if len(upload_blocks) != 6:
                raise ValueError('expected six constituent receipt uploads')
            for block in upload_blocks:
                if not re.search(r'(?m)^        if: always\(\) && !cancelled\(\) && matrix\.slot[123]\.kind == \'(?:pr|integration)\'$\n', block):
                    raise ValueError('receipt upload condition is not cancel-safe')
                if not re.search(r'(?m)^          if-no-files-found: error$', block):
                    raise ValueError('receipt upload must fail when files are missing')

        validate(workflow)
        mutated = re.sub(r'(?m)(^      - name: Upload constituent 1 shard receipt\n        if: )always\(\) && !cancelled\(\) && ',
                         r'\1always() && ', workflow, count=1)
        with self.assertRaisesRegex(ValueError, 'not cancel-safe'):
            validate(mutated)

    def test_schema_replay_has_three_required_independent_jobs(self):
        expected = {
            'engine-family-9': ('schema replay contracts', 'custody_schema_replay_is_proportional_to_affected_tables'),
            'schema-replay-work-limited-to-affected-tables': ('schema replay / work limited to affected tables', 'custody_schema_replay_work_limited_to_affected_tables'),
            'schema-replay-identical-schema-does-no-artifact-work': ('schema replay / identical schema avoids rebuilding data', 'custody_schema_replay_identical_schema_does_no_artifact_work'),
        }
        self.assertEqual(len(i.matrix(self.rows, False)['include']), 32)
        self.assertEqual(len(i.matrix(self.rows, True)['include']), 36)
        path = ROOT / 'scripts/ci-integration-targets.json'
        original = json.loads(path.read_text()); read = Path.read_text
        for slice_id, (family, target) in expected.items():
            row = next(r for r in self.rows if r['id'] == slice_id)
            targets = [target]
            if slice_id == 'engine-family-9':
                targets.append('custody_schema_replay_bounded_contract')
            self.assertEqual(row, dict(id=slice_id, family=family, kind='engine-tests', targets=targets))
            for default in (True, False):
                needs = self.needs(default=default)
                for mutation in ('missing', 'duplicate', 'renamed'):
                    bad = copy.deepcopy(needs)
                    matrix = json.loads(bad['plan']['outputs']['matrix'])
                    item = next(r for r in matrix['include'] if r['id'] == slice_id)
                    if mutation == 'missing': matrix['include'].remove(item)
                    elif mutation == 'duplicate': matrix['include'].append(item)
                    else: item['name'] += ' changed'
                    bad['plan']['outputs']['matrix'] = json.dumps(matrix)
                    with self.subTest(slice_id=slice_id, default=default, mutation=mutation), self.assertRaises(ValueError):
                        i.required(bad, self.rows, True, default)
            # Moving a case into another job still covers the target union, but
            # must fail because it removes independent scheduling of that case.
            data = copy.deepcopy(original)
            removed = next(r for r in data['slices'] if r['id'] == slice_id)
            data['slices'].remove(removed)
            data['slices'][0]['targets'].extend(removed['targets'])
            with self.subTest(slice_id=slice_id), patch.object(
                    Path, 'read_text', lambda p, *a, **kw: json.dumps(data) if p == path else read(p, *a, **kw)):
                with self.assertRaisesRegex(ValueError, 'three separate named slices'):
                    i.load_manifest()

    def test_vector_corpora_are_four_required_jobs_with_exact_metadata(self):
        self.assertEqual(len(i.matrix(self.rows, False)['include']), 32)
        self.assertEqual(len(i.matrix(self.rows, True)['include']), 36)
        for slice_id, (family, target) in i.VECTOR_PROFILE_SLICES.items():
            row = next(r for r in self.rows if r['id'] == slice_id)
            self.assertEqual(row, dict(id=slice_id, family=family, kind='engine-tests', targets=[target]))
            self.assertEqual(i.metadata(row), dict(features='nightly-tests,test-seams,iroh',
                profile='test-profile', name=f'engine / test-profile / {family}', test_threads='1'))
            self.assertEqual(i.commands(row), [['cargo', 'test', '--locked', '-p', 'contextdb-engine',
                '--features', 'nightly-tests,test-seams,iroh', '--test', target, '--', '--test-threads=1']])
            for default in (False, True):
                for mutation in ('missing', 'duplicate', 'wrong-name', 'wrong-id'):
                    bad = self.needs(default=default)
                    matrix = json.loads(bad['plan']['outputs']['matrix'])
                    item = next(r for r in matrix['include'] if r['id'] == slice_id)
                    if mutation == 'missing': matrix['include'].remove(item)
                    elif mutation == 'duplicate': matrix['include'].append(copy.deepcopy(item))
                    elif mutation == 'wrong-name': item['name'] += ' changed'
                    else: item['id'] = 'wrong-profile'
                    bad['plan']['outputs']['matrix'] = json.dumps(matrix)
                    with self.subTest(slice_id=slice_id, default=default, mutation=mutation), self.assertRaises(ValueError):
                        i.required(bad, self.rows, True, default)

    def test_vector_manifest_rejects_missing_duplicate_combined_and_wrong_mapping(self):
        path = ROOT / 'scripts/ci-integration-targets.json'
        original = json.loads(path.read_text()); read = Path.read_text
        for slice_id in i.VECTOR_PROFILE_SLICES:
            for mutation in ('missing', 'duplicate', 'combined', 'wrong-target', 'optional', 'wrong-label'):
                data = copy.deepcopy(original)
                row = next(r for r in data['slices'] if r['id'] == slice_id)
                if mutation in ('missing', 'combined'):
                    data['slices'].remove(row)
                    if mutation == 'combined': data['slices'][0]['targets'].extend(row['targets'])
                elif mutation == 'duplicate': data['slices'].append(copy.deepcopy(row))
                elif mutation == 'wrong-target': row['targets'] = ['vector_graph_route_profile_quality']
                elif mutation == 'optional': row['kind'] = 'production'
                else: row['family'] += ' changed'
                with self.subTest(slice_id=slice_id, mutation=mutation), patch.object(
                        Path, 'read_text', lambda p, *a, **kw: json.dumps(data) if p == path else read(p, *a, **kw)):
                    with self.assertRaises(ValueError): i.load_manifest()

    def test_vector_source_mapping_registration_and_coverage_fail_closed(self):
        base = ROOT / 'crates/contextdb-engine/tests'
        helper = base / 'vector/graph_route_profile_support.rs'
        source = helper.read_text()
        mutations = [(helper, source.replace('(50_001, "anisotropic", 17)', '(5_000_usize, "anisotropic", 17_usize)')),
                     (helper, source.replace('(50_000, "isotropic", 29)', '(50_000, "isotropic", 17)')),
                     (helper, source.replace('(50_000, "isotropic", 29),', '')),
                     (helper, source.replace('Self::Several, ', '')),
                     (helper, source.replace('("narrow", "bucket = 0")', '("narrow", "bucket = 1")')),
                     (helper, source.replace('cells, 6,', 'cells, 3,')),
                     (helper, source.replace('        24,', '        12,'))]
        cargo = ROOT / 'crates/contextdb-engine/Cargo.toml'
        for index, (_, target) in enumerate(i.VECTOR_PROFILE_SLICES.values()):
            wrapper = base / f'{target}.rs'; text = wrapper.read_text()
            for a, b in [(f'run_build_quality_profile({index});', f'run_build_quality_profile({(index+1)%4});'),
                         ('#[test]', '#[test]\n    #[ignore]'),
                         ('#![cfg(feature = "nightly-tests")]', '#![cfg(feature = "test-seams")]'),
                         ('#[allow(dead_code, unused_imports)]', '#[allow(dead_code)]')]:
                mutations.append((wrapper, text.replace(a, b)))
            block = f'name = "{target}"\npath = "tests/{target}.rs"\nrequired-features = ["test-seams"]'
            mutations.append((cargo, cargo.read_text().replace(block, block.replace('["test-seams"]', '["nightly-tests"]'))))
            mutations.append((cargo, cargo.read_text() + '\n[[test]]\n' + block + '\n'))
        read = Path.read_text
        for path, text in mutations:
            with self.subTest(path=path, text=text[-100:]), patch.object(
                    Path, 'read_text', lambda p, *a, **kw: text if p == path else read(p, *a, **kw)):
                with self.assertRaises(ValueError): i.load_manifest()

    def test_exact_original_command_classes_and_thread_modes(self):
        kinds = {row['kind']: row for row in self.rows if row['kind'] != 'release-acceptance'}
        base = ['cargo', 'test', '--locked', '-p', 'contextdb-engine', '--features', 'nightly-tests,test-seams,iroh']
        release_rows = [row for row in self.rows if row['kind'] == 'release-acceptance']
        self.assertEqual(
            [i.commands(row) for row in release_rows],
            [[base + ['--release', '--test', target, '--', '--test-threads=1']]
             for target in i.ACCEPTANCE_TARGETS])
        self.assertEqual(i.commands(kinds['examples']), [base + ['--examples', '--no-run']])
        self.assertEqual(i.commands(kinds['doctests']), [base + ['--doc', '--', '--test-threads=1']])
        production = ['cargo', 'test', '--locked', '-p', 'contextdb-engine', '--features', 'nightly-tests']
        self.assertEqual(i.commands(kinds['production']), [production])
        self.assertEqual(i.commands(kinds['production-release']),
                         [production + ['--release', '--test', target]
                          for target in i.ACCEPTANCE_TARGETS])
        self.assertEqual(i.commands(kinds['workspace']), [['cargo', 'test', '--locked', '--workspace']])
        with patch.dict(os.environ, {'CARGO_TARGET_DIR': '/tmp/isolated-target'}):
            self.assertEqual(i.commands(kinds['redb']), [['cargo', 'test', '--manifest-path', 'crates/contextdb-redb/Cargo.toml', '--target-dir', '/tmp/isolated-target/redb', '--locked']])
        for row in self.rows:
            if row['kind'] == 'engine-tests':
                expected = base.copy()
                for target in row['targets']:
                    expected += ['--lib', '--bins'] if target == 'lib' else ['--test', target]
                selected = i.commands(row, mode='full')
                self.assertEqual(selected[0], expected + ['--', '--test-threads=1'])
                if set(row['targets']) & i.FULL_SCHEMA or 'sync_schema_transactions' in row['targets']:
                    self.assertEqual(selected[1][selected[1].index('--features') + 1], 'test-seams,iroh')
                else:
                    self.assertEqual(len(selected), 1)

    def test_union_omissions_duplicates_acceptance_profiles_and_optional_classes(self):
        path = ROOT / 'scripts/ci-integration-targets.json'
        original = json.loads(path.read_text())
        mutations = [lambda d: d['slices'][0]['targets'].pop(),
                     lambda d: d['slices'][0]['targets'].append('sync'),
                     lambda d: d['slices'][0]['targets'].append('new_unknown_target'),
                     lambda d: d['slices'][0]['targets'].append('acceptance'),
                     lambda d: d['slices'][-1].update(kind='production'),
                     lambda d: d['slices'].pop(),
                     lambda d: next(r for r in d['slices'] if r['kind']=='release-acceptance').update(targets=['sync']),
                     lambda d: next(r for r in d['slices'] if r['kind']=='production').update(targets=['acceptance'])]
        read = Path.read_text
        for mutation in mutations:
            data = copy.deepcopy(original)
            mutation(data)
            with self.subTest(data=data), patch.object(Path, 'read_text', lambda p, *a, **k: json.dumps(data) if p == path else read(p, *a, **k)):
                with self.assertRaises(ValueError):
                    i.load_manifest()

    def test_nightly_only_target_cannot_be_omitted(self):
        path = ROOT / 'crates/contextdb-engine/Cargo.toml'
        read = Path.read_text
        extra = '\n[[test]]\nname = "new_nightly"\npath = "tests/new_nightly.rs"\nrequired-features = ["nightly-tests"]\n'
        with patch.object(Path, 'read_text', lambda p, *a, **k: read(p,*a,**k)+extra if p == path else read(p,*a,**k)):
            with self.assertRaisesRegex(ValueError, 'new_nightly'):
                i.load_manifest()

    def needs(self, enabled=True, default=False):
        return dict(plan=dict(result='success', outputs=dict(enabled=str(enabled).lower(), **i.plan_decision('full', default), matrix=json.dumps(i.matrix(self.rows, default)))), slice=dict(result='success' if enabled else 'skipped'))

    def test_fail_closed_results_modes_and_matrix(self):
        for default in (True, False):
            for enabled in (True, False):
                needs = self.needs(enabled, default)
                i.required(needs, self.rows, enabled, default)
                for result in ['failure', 'cancelled', None, 'skipped' if enabled else 'success']:
                    bad = copy.deepcopy(needs); bad['slice']['result'] = result
                    with self.subTest(enabled=enabled, result=result), self.assertRaises(ValueError):
                        i.required(bad, self.rows, enabled, default)
                for bad in [{}, {'plan': needs['plan']}, dict(needs, unexpected={})]:
                    with self.assertRaises(ValueError):
                        i.required(bad, self.rows, enabled, default)
                for field, value in [('enabled', str(not enabled).lower()), ('run_default', str(not default).lower()), ('matrix', '{"include":[]}')]:
                    bad = copy.deepcopy(needs); bad['plan']['outputs'][field] = value
                    with self.assertRaises(ValueError):
                        i.required(bad, self.rows, enabled, default)
                bad = copy.deepcopy(needs); bad['plan']['result'] = 'failure'
                with self.assertRaises(ValueError):
                    i.required(bad, self.rows, enabled, default)
        self.assertEqual(len(i.matrix(self.rows, True)['include']) - len(i.matrix(self.rows, False)['include']), 4)

    def test_workflow_routing_and_negative_controls(self):
        i.workflow_contract()
        path = ROOT / '.github/workflows/integration.yml'
        text = path.read_text(); read = Path.read_text
        for old, new in [('fail-fast: false', 'fail-fast: true'), ('if: always()\n    runs-on', "if: success()\n    runs-on"), ('needs: [plan, slice]', 'needs: [plan]'), ('${{ fromJSON(needs.plan.outputs.matrix) }}', '{include: []}'), ("timeout-minutes: ${{ needs.plan.outputs.gate_mode == 'normal' && 30 || 330 }}", 'timeout-minutes: 330'), ('    needs: plan', '    continue-on-error: true\n    needs: plan'), ('    needs: plan', '    env: {BASH_ENV: unsafe.sh}\n    needs: plan'), ('    shell: bash', '    shell: bash {0} || true'), ('  contents: read', '  contents: write'), ('run: python3 -B scripts/ci-integration.py required', 'run: python3 -B scripts/ci-integration.py required || true')]:
            with self.subTest(old=old), patch.object(Path,'read_text',lambda p,*a,**k: text.replace(old,new) if p==path else read(p,*a,**k)):
                with self.assertRaises(ValueError):
                    i.workflow_contract()
        doc = c.current()
        jobs = c.load(text)['jobs']
        self.assertEqual(set(jobs), {'plan','slice','result'})
        self.assertEqual(jobs['slice']['strategy']['fail-fast'], False)
        test_profile = {'CARGO_PROFILE_TEST_OPT_LEVEL': '1',
                        'CARGO_PROFILE_TEST_DEBUG_ASSERTIONS': 'true',
                        'CARGO_PROFILE_TEST_OVERFLOW_CHECKS': 'true'}
        self.assertEqual(jobs['slice']['env'], test_profile)
        self.assertEqual(jobs['result']['if'], 'always()')
        validator = doc['jobs']['integration']
        self.assertEqual(validator['needs'], ['impact', 'test-bundles'])
        self.assertEqual(validator['if'], 'always()')
        bundle_job = doc['jobs']['test-bundles']
        self.assertEqual(bundle_job['needs'], 'impact')
        self.assertEqual(bundle_job['env'], test_profile)
        self.assertEqual(bundle_job['strategy']['matrix'],
                         '${{ fromJSON(needs.impact.outputs.bundle_matrix) }}')
        self.assertNotIn('max-parallel', bundle_job['strategy'])
        caches = [step for step in bundle_job['steps']
                  if step.get('uses', '').startswith('Swatinem/rust-cache@')]
        self.assertEqual(len(caches), 3)
        self.assertEqual([step['with']['key'] for step in caches],
                         ['${{ matrix.slot1.cache_key }}', '${{ matrix.slot2.cache_key }}',
                          '${{ matrix.slot3.cache_key }}'])
        self.assertEqual([step['with']['add-job-id-key'] for step in caches], [False] * 3)
        self.assertTrue(all('cache-on-failure' not in step['with'] for step in caches))
        runs = {step.get('id'): step for step in bundle_job['steps'] if step.get('id')}
        self.assertEqual(set(runs), {f'slot{slot}_{kind}' for slot in range(1, 4)
                                     for kind in ('pr', 'integration')})
        self.assertTrue(all(step['continue-on-error'] is True for step in runs.values()))
        self.assertTrue(all(step['if'].startswith('always() && !cancelled()')
                            for step in runs.values()))
        final_step = next(step for step in bundle_job['steps']
                          if step.get('name') == 'Require every bundled constituent to succeed')
        cleanup_step = next(step for step in bundle_job['steps']
                            if step.get('name') == 'Remove disposable cache test scratch')
        self.assertEqual(bundle_job['steps'].index(cleanup_step),
                         bundle_job['steps'].index(final_step) + 1)
        self.assertEqual(cleanup_step['if'], 'always() && !cancelled()')
        self.assertEqual(cleanup_step['run'], 'bash scripts/ci-cache-post-cleanup.sh')
        self.assertNotIn('continue-on-error', cleanup_step)
        final = final_step['run']
        self.assertIn('exit "$status"', final)
        outcomes = dict(os.environ, SLOT1_KIND='pr', SLOT1_PR='failure',
                        SLOT1_INTEGRATION='skipped', SLOT2_KIND='integration',
                        SLOT2_PR='skipped', SLOT2_INTEGRATION='success',
                        SLOT3_KIND='', SLOT3_PR='skipped', SLOT3_INTEGRATION='skipped')
        self.assertNotEqual(subprocess.run(['bash', '-c', final], env=outcomes).returncode, 0)
        self.assertEqual(len(i.pr_inventory()), 26)

    def test_mixed_normal_matrix_is_exact_and_fail_closed(self):
        identity = {'source_sha': '1' * 40, 'source_fingerprint': '2' * 64}
        certificate = {'targets': {t: {} for t in i.NORMAL_DEDUP_TARGETS},
                       'sha256': 'a' * 64, 'reason': 'fixture proof applies'}
        fallback = {'targets': {}, 'reason': 'fixture proof unavailable'}
        for proof, integration_count in [(certificate, 23), (fallback, 27)]:
            with patch.object(i, 'source_identity', return_value=identity), \
                 patch.object(i, 'normal_dedup_certificate', return_value=proof):
                decision = i.plan_decision('normal', False)
                value = i.combined_matrix(self.rows, True, decision)
                self.assertEqual(len(value['include']), 26 + integration_count)
                self.assertEqual(sum(row['kind'] == 'pr' for row in value['include']), 26)
                tuples = [tuple(x[k] for k in ('shard', 'crate', 'features'))
                          for row in i.pr_inventory()
                          for x in row.get('constituents', [row])]
                self.assertEqual(len(tuples), 33)
                self.assertEqual(len(set(tuples)), 33)
                logical_tuples = {(x.get('cache_parent', x['shard']), x['crate'], x['features'])
                                  for row in i.pr_inventory()
                                  for x in row.get('constituents', [row])}
                self.assertEqual(len(logical_tuples), 29)
                i.validate_combined(value, self.rows, True, decision)
                bundles = i.bundle_matrix(value, self.rows, True, decision)
                self.assertEqual(len(bundles['include']), 18)
                flattened = [bundle[f'slot{slot}'] for bundle in bundles['include']
                             for slot in range(1, 4) if bundle[f'slot{slot}'] is not None]
                self.assertEqual(len(flattened), len(value['include']))
                self.assertEqual({row['id'] for row in flattened},
                                 {row['id'] for row in value['include']})
                self.assertEqual(len(flattened), 26 + integration_count)
                i.validate_bundle_matrix(bundles, value, self.rows, True, decision)
                for mutation in ('missing', 'duplicate', 'cache', 'source', 'profile'):
                    changed_bundles = copy.deepcopy(bundles)
                    if mutation == 'missing':
                        changed_bundles['include'].pop()
                    elif mutation == 'duplicate':
                        changed_bundles['include'].append(copy.deepcopy(changed_bundles['include'][0]))
                    elif mutation == 'cache':
                        changed_bundles['include'][0]['slot1']['cache_key'] += '-changed'
                    elif mutation == 'source':
                        slot = next(slot for bundle in changed_bundles['include']
                                    for slot in (bundle['slot1'], bundle['slot2'], bundle['slot3'])
                                    if slot is not None and slot['kind'] == 'integration')
                        slot['integration']['source_sha'] = '0' * 40
                    else:
                        slot = next(slot for bundle in changed_bundles['include']
                                    for slot in (bundle['slot1'], bundle['slot2'], bundle['slot3'])
                                    if slot is not None and slot['kind'] == 'pr')
                        slot['pr']['features'] = 'mixed-profile'
                    with self.subTest(bundle_payload=mutation), self.assertRaises(ValueError):
                        i.validate_bundle_matrix(changed_bundles, value, self.rows, True, decision)
                manifest_path = ROOT / 'scripts/ci-integration-targets.json'
                original = manifest_path.read_text()
                for mutation in ('missing', 'duplicate', 'tampered'):
                    changed = json.loads(original)
                    ids = changed['normal_bundles']['bundles'][0]
                    if mutation == 'missing': ids.clear()
                    elif mutation == 'duplicate': ids.append(changed['normal_bundles']['bundles'][1][0])
                    else: ids[0] = 'pr-contextdb-cli'
                    encoded = json.dumps(changed)
                    read = Path.read_text
                    with self.subTest(bundle=mutation), \
                         patch.object(Path, 'read_text', lambda p, *a, **k: encoded if p == manifest_path else read(p, *a, **k)), \
                         self.assertRaises(ValueError):
                        i.bundle_matrix(value, self.rows, True, decision)
                needs = {name: {'result': 'success'} for name in
                         ('impact', 'test-bundles')}
                i.normal_required(needs, self.rows, True, value, bundles, decision)
                for name in needs:
                    for result in ('failure', 'cancelled', 'skipped', None):
                        bad_needs = copy.deepcopy(needs)
                        bad_needs[name]['result'] = result
                        with self.subTest(job=name, result=result), self.assertRaises(ValueError):
                            i.normal_required(bad_needs, self.rows, True, value, bundles, decision)
                for mutation in ('missing', 'duplicate', 'kind', 'payload', 'both'):
                    bad = copy.deepcopy(value)
                    if mutation == 'missing': bad['include'].pop()
                    elif mutation == 'duplicate': bad['include'].append(copy.deepcopy(bad['include'][0]))
                    elif mutation == 'kind': bad['include'][0]['kind'] = 'unknown'
                    elif mutation == 'payload': bad['include'][0]['pr']['features'] = 'changed'
                    else: bad['include'][0]['integration'] = {}
                    with self.assertRaises(ValueError):
                        i.validate_combined(bad, self.rows, True, decision)
                disabled = i.combined_matrix(self.rows, False, decision)
                self.assertEqual(len(disabled['include']), 26)
                self.assertTrue(all(row['kind'] == 'pr' for row in disabled['include']))
                disabled_bundles = i.bundle_matrix(disabled, self.rows, False, decision)
                self.assertEqual(len(disabled_bundles['include']), 15)
                self.assertEqual(sum(bundle[f'slot{slot}'] is not None
                                     for bundle in disabled_bundles['include'] for slot in range(1, 4)), 26)
                # A disabled partition is empty, not an exemption from binding
                # the published source and certificate decision.
                for field, value in [('source_sha', '0' * 40),
                                     ('source_fingerprint', '0' * 64),
                                     ('dedup_sha', 'not-a-certificate')]:
                    tampered = dict(decision, **{field: value})
                    with self.subTest(disabled_binding=field), self.assertRaises(ValueError):
                        i.validate_combined(disabled, self.rows, False, tampered)

    def test_real_entrypoint_stubbed_execution_receipts_and_failure(self):
        for kind, failure in [('engine-tests', None), ('release-acceptance', 'test'), ('examples', None), ('doctests', None), ('workspace', None), ('redb', None), ('production', None), ('production-release', None), ('engine-tests', 'build')]:
            row = next(r for r in self.rows if r['kind']==kind)
            if kind == 'engine-tests':
                row = next(r for r in self.rows if r['targets']==['acceptance'])
            with self.subTest(kind=kind, failure=failure), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp); tools = directory/'bin'; tools.mkdir()
                cargo = tools/'cargo'
                cargo.write_text('''#!/usr/bin/env python3
import json,os,sys
with open(os.environ['CALLS'],'a') as out: out.write(json.dumps(dict(argv=sys.argv[1:], flags=os.environ.get('RUSTFLAGS'), binary=os.environ.get('CARGO_BIN_EXE_contextdb'), threads=os.environ.get('RUST_TEST_THREADS')))+'\\n')
if os.environ.get('FAIL_COMMAND') == sys.argv[1]: sys.exit(37)
if '--no-run' not in sys.argv and sys.argv[1]=='test':
 print('   Doc-tests contextdb_engine' if '--doc' in sys.argv else '     Running tests/acceptance.rs (target/debug/deps/acceptance-123)')
 print('running 3 tests')
 print('test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 1901.25s')
''')
                cargo.chmod(0o755)
                rustc = tools/'rustc'; rustc.write_text('#!/bin/sh\necho synthetic-rustc\n');rustc.chmod(0o755)
                env = dict(os.environ, PATH=str(tools)+':'+os.environ['PATH'], CALLS=str(directory/'calls'), CI_INTEGRATION_OUT=str(directory/'out'), CARGO_TARGET_DIR=str(directory/'target'), CI_INTEGRATION_DEFAULT='true', RUST_TEST_THREADS='3', PYTHONDONTWRITEBYTECODE='1')
                env.pop('CI_INTEGRATION_ENABLED',None)
                env['FAIL_COMMAND']=failure or ''
                run = subprocess.run(['bash','scripts/verify','integration',row['id']],cwd=ROOT,env=env,capture_output=True,text=True,timeout=20)
                self.assertEqual(run.returncode,37 if failure else 0,run.stderr)
                calls = [json.loads(line) for line in (directory/'calls').read_text().splitlines()]
                self.assertEqual(calls[0]['argv'], ['build','--locked','-p','contextdb-cli','-p','contextdb-server'])
                if failure != 'build':
                    with patch.dict(os.environ,env):
                        self.assertEqual(calls[1]['argv'],i.commands(row)[0][1:])
                self.assertTrue(all(call['flags']=='-D warnings' for call in calls))
                receipt = json.loads(next((directory/'out').rglob('receipt.json')).read_text())
                self.assertEqual(receipt['exit_status'],run.returncode)
                self.assertIsNone(receipt['hosted_job_elapsed_seconds'])
                self.assertIsNone(receipt['hosted_queue_seconds'])
                self.assertEqual(receipt['features'],i.metadata(row)['features'])
                if not failure and kind!='examples':
                    target=receipt['targets'][0]
                    self.assertEqual((target['declared_count'],target['runnable_count'],target['ignored'],target['duration_seconds']),(3,2,1,1901.25))
                    self.assertEqual(target['test_threads'],'3' if kind in i.OPTIONAL else '1')
                else:
                    self.assertEqual(receipt['targets'],[])

    def test_local_full_run_keeps_other_slices_after_failure(self):
        selected = []
        def execute(row, out, mode=None):
            selected.append(row['id'])
            return 37 if len(selected) == 1 else 0
        for default in ('false', 'true'):
            selected.clear()
            with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {'CI_INTEGRATION_DEFAULT': default, 'CI_INTEGRATION_ENABLED': 'true', 'CI_INTEGRATION_OUT': tmp}), patch.object(sys, 'argv', ['ci-integration.py', 'run']), patch.object(i, 'run_slice', side_effect=execute):
                self.assertEqual(i.main(), 37)
                self.assertEqual(selected, [row['id'] for row in self.rows if default == 'true' or row['kind'] not in i.OPTIONAL])

    def test_receipt_parser_failure_is_not_success(self):
        row = next(r for r in self.rows if r['targets'] == ['acceptance'])
        class BrokenInventory:
            @staticmethod
            def timing_receipt(*args):
                raise ValueError('synthetic parser failure')
        with tempfile.TemporaryDirectory() as tmp, patch.object(i, 'capture', return_value=(0, 0.1)), patch.object(i, 'inventory_module', return_value=BrokenInventory):
            out = Path(tmp) / 'receipt'
            with self.assertRaisesRegex(ValueError, 'synthetic parser failure'):
                i.run_slice(dict(row, plan_decision=i.plan_decision('full', True)), out)
            self.assertNotEqual(json.loads((out / 'receipt.json').read_text())['exit_status'], 0)

    def test_plain_inventory_and_suite_leave_no_bytecode(self):
        def files():
            return {str(p.relative_to(ROOT)): p.read_bytes() for p in (ROOT / 'scripts').rglob('*') if p.is_file()}
        before = files()
        env = dict(os.environ)
        env.pop('PYTHONDONTWRITEBYTECODE', None)
        run = subprocess.run([sys.executable, 'scripts/ci-inventory.py'], cwd=ROOT, env=env, capture_output=True, text=True, timeout=20)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(files(), before)

    def test_invalid_slice_mode_and_classified_skip(self):
        for args, env in [(['run','does-not-exist'],{}),(['check'],{'CI_INTEGRATION_DEFAULT':'no'}),(['run','workspace'],{'CI_INTEGRATION_DEFAULT':'false'}),(['plan'],{'CI_INTEGRATION_ENABLED':'false','IMPACT_REASON':''})]:
            with tempfile.TemporaryDirectory() as tmp:
                run=subprocess.run([sys.executable,'-B','scripts/ci-integration.py',*args],cwd=ROOT,env=dict(os.environ,GITHUB_OUTPUT=str(Path(tmp)/'outputs'),**env),capture_output=True,text=True,timeout=10)
                self.assertNotEqual(run.returncode,0)
        with tempfile.TemporaryDirectory() as tmp:
            run=subprocess.run([sys.executable,'-B','scripts/ci-integration.py','plan'],cwd=ROOT,env=dict(os.environ,GITHUB_OUTPUT=str(Path(tmp)/'outputs'),CI_INTEGRATION_ENABLED='false',CI_INTEGRATION_DEFAULT='false',IMPACT_REASON='documentation-only',PATH_CLASSES='{"README.md":"docs"}'),capture_output=True,text=True,timeout=10)
            self.assertEqual(run.returncode,0,run.stderr)
            self.assertIn('documentation-only',run.stdout)
            self.assertIn('enabled=false',(Path(tmp)/'outputs').read_text())


if __name__=='__main__':
    unittest.main(verbosity=2)
