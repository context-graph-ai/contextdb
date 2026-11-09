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
import tomllib
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
                r'(?m)^      - name: Upload constituent [1234] (?:shard|integration) receipt\n'
                r'(?P<block>.*?)(?=^      - name:|\Z)', source, re.S)
            if len(upload_blocks) != 8:
                raise ValueError('expected eight constituent receipt uploads')
            for block in upload_blocks:
                if not re.search(r'(?m)^        if: always\(\) && !cancelled\(\) && matrix\.slot[1234]\.kind == \'(?:pr|integration)\'$\n', block):
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
        self.assertEqual(len(i.matrix(self.rows, False)['include']), 25)
        self.assertEqual(len(i.matrix(self.rows, True)['include']), 29)
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

    def test_integration_commands_never_stop_at_the_first_failing_target(self):
        several = ['cargo', 'test', '--locked', '-p', 'contextdb-engine', '--features', 'test-seams,iroh',
                   '--lib', '--bins', '--test', 'engine', '--', '--test-threads=1']
        with self.assertRaisesRegex(ValueError, 'first failing target'):
            i.no_fail_fast_contract(several)
        i.no_fail_fast_contract(several[:3] + [i.NO_FAIL_FAST] + several[3:])
        i.no_fail_fast_contract(['cargo', 'test', '--locked', '--test', 'one', '--', '--test-threads=1'])
        i.no_fail_fast_contract(several[:-2] + ['--no-run'])
        for mode in ('full',):
            for row in self.rows:
                if row['kind'] != 'engine-tests':
                    continue
                ordinary = [t for t in row['targets'] if t not in {'custody', 'vector', 'custody_schema_replay_bounded_contract'}]
                selected = i.commands(row, mode=mode)
                if ordinary:
                    self.assertEqual(selected[0][:4], ['cargo', 'test', '--locked', i.NO_FAIL_FAST], row['id'])
        # Removing the flag from the runner is refused by its own contract and by the inventory.
        source = (ROOT / 'scripts/ci-integration.py').read_text()
        self.assertEqual(source.count('base[:3] + [NO_FAIL_FAST] + base[3:]'), 1)
        stripped = source.replace('base[:3] + [NO_FAIL_FAST] + base[3:]', 'base')
        spec = importlib.util.spec_from_loader('stripped_integration', loader=None)
        stripped_module = importlib.util.module_from_spec(spec)
        stripped_module.__file__ = str(ROOT / 'scripts/ci-integration.py')
        exec(compile(stripped, stripped_module.__file__, 'exec'), stripped_module.__dict__)
        multi = next(row for row in self.rows if row['kind'] == 'engine-tests'
                     and len([t for t in row['targets'] if t not in {'custody', 'vector', 'custody_schema_replay_bounded_contract'}]) > 1)
        with self.assertRaisesRegex(ValueError, 'first failing target'):
            stripped_module.commands(multi, mode='full')

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
                         [(production[:-1] + ['nightly-tests,test-seams']
                           if target in ('acceptance', 'acceptance_sync') else production)
                          + ['--release', '--test', target]
                          for target in i.ACCEPTANCE_TARGETS])
        required = {row['name']: set(row.get('required-features', []))
                    for row in tomllib.loads((ROOT / 'crates/contextdb-engine/Cargo.toml').read_text())['test']}
        for command in i.commands(kinds['production-release']):
            target = command[command.index('--test') + 1]
            features = set(command[command.index('--features') + 1].split(','))
            self.assertLessEqual(required[target], features)
        self.assertEqual(i.commands(kinds['workspace']), [['cargo', 'test', '--locked', '--workspace']])
        with patch.dict(os.environ, {'CARGO_TARGET_DIR': '/tmp/isolated-target'}):
            default = ['cargo', 'test', '--manifest-path', 'crates/contextdb-redb/Cargo.toml',
                       '--target-dir', '/tmp/isolated-target/redb', '--locked']
            expected = [('named_root_capture_contract', 'contextdb-internal,test-seams'), ('reclaim_step_contract', 'contextdb-internal,test-seams'), ('open_table_registry_allocations', 'contextdb-internal,test-seams'), ('admission_reads_are_bounded_by_the_format', 'contextdb-internal,test-seams'), ('growth_after_compaction_contract', 'contextdb-internal'), ('cache_residency', 'contextdb-internal'), ('page_window_contract', 'contextdb-internal,test-seams'), ('startup_settle_releases_the_replaced_cache_index', 'contextdb-internal,test-seams'), ('allocator_residency_contract', 'contextdb-internal,test-seams')]
            self.assertEqual(i.commands(kinds['redb']), [default] + [
                default + ['--features', features, '--test', name, '--', '--test-threads=1']
                for name, features in expected])
        for row in self.rows:
            if row['kind'] == 'engine-tests':
                expected = base[:3] + ['--no-fail-fast'] + base[3:]
                for target in row['targets']:
                    if target in {'custody', 'vector', 'custody_schema_replay_bounded_contract'}:
                        continue
                    expected += ['--lib', '--bins'] if target == 'lib' else ['--test', target]
                selected = i.commands(row, mode='full')
                self.assertEqual(selected[0], expected + ['--', '--test-threads=1'])
                if set(row['targets']) & i.FULL_SCHEMA or 'sync_schema_transactions' in row['targets']:
                    self.assertEqual(selected[1][selected[1].index('--features') + 1], 'test-seams,iroh')
                else:
                    self.assertEqual(len(selected), 1 + len(set(row['targets']) & {'custody', 'vector', 'custody_schema_replay_bounded_contract'}))
                for target, command in zip([t for t in row['targets'] if t in {'custody', 'vector', 'custody_schema_replay_bounded_contract'}],
                                           selected[2:] if set(row['targets']) & i.FULL_SCHEMA else selected[1:]):
                    i.aggregate_module(ROOT).validate_command(ROOT, target, command)

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
        self.assertNotIn('env', jobs['slice'])
        self.assertEqual(jobs['result']['if'], 'always()')
        validator = doc['jobs']['integration']
        self.assertEqual(validator['needs'], ['impact', 'test-bundles'])
        self.assertEqual(validator['if'], 'always()')
        bundle_job = doc['jobs']['test-bundles']
        self.assertEqual(bundle_job['needs'], ['impact', 'test-builds'])
        self.assertEqual(doc['jobs']['test-builds']['strategy']['matrix'],
                         '${{ fromJSON(needs.impact.outputs.build_matrix) }}')
        self.assertNotIn('env', bundle_job)
        self.assertEqual(bundle_job['strategy']['matrix'],
                         '${{ fromJSON(needs.impact.outputs.bundle_matrix) }}')
        self.assertNotIn('max-parallel', bundle_job['strategy'])
        caches = [step for step in bundle_job['steps']
                  if step.get('uses', '').startswith('Swatinem/rust-cache@')]
        self.assertEqual(len(caches), 4)
        self.assertEqual([step['with']['key'] for step in caches],
                         ['${{ matrix.slot1.cache_key }}', '${{ matrix.slot2.cache_key }}',
                          '${{ matrix.slot3.cache_key }}', '${{ matrix.slot4.cache_key }}'])
        self.assertEqual([step['with']['add-job-id-key'] for step in caches], [False] * 4)
        self.assertTrue(all('cache-on-failure' not in step['with'] for step in caches))
        runs = {step.get('id'): step for step in bundle_job['steps'] if step.get('id')}
        self.assertEqual(set(runs), {f'slot{slot}_{kind}' for slot in range(1, 5)
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
                        SLOT3_KIND='', SLOT3_PR='skipped', SLOT3_INTEGRATION='skipped',
                        SLOT4_KIND='', SLOT4_PR='skipped', SLOT4_INTEGRATION='skipped')
        self.assertNotEqual(subprocess.run(['bash', '-c', final], env=outcomes).returncode, 0)
        self.assertEqual(len(i.pr_inventory()), 25)

    def test_mixed_normal_matrix_is_exact_and_fail_closed(self):
        identity = {'source_sha': '1' * 40, 'source_fingerprint': '2' * 64}
        certificate = {'targets': {t: {} for t in i.NORMAL_DEDUP_TARGETS},
                       'sha256': 'a' * 64, 'reason': 'fixture proof applies'}
        fallback = {'targets': {}, 'reason': 'fixture proof unavailable'}
        for proof, integration_count in [(certificate, 22), (fallback, 24)]:
            with patch.object(i, 'source_identity', return_value=identity), \
                 patch.object(i, 'normal_dedup_certificate', return_value=proof):
                decision = i.plan_decision('normal', False)
                value = i.combined_matrix(self.rows, True, decision)
                self.assertEqual(len(value['include']), 25 + integration_count)
                self.assertEqual(sum(row['kind'] == 'pr' for row in value['include']), 25)
                tuples = [tuple(x[k] for k in ('shard', 'crate', 'features'))
                          for row in i.pr_inventory()
                          for x in row.get('constituents', [row])]
                self.assertEqual(len(tuples), 31)
                self.assertEqual(len(set(tuples)), 31)
                logical_tuples = {(x.get('cache_parent', x['shard']), x['crate'], x['features'])
                                  for row in i.pr_inventory()
                                  for x in row.get('constituents', [row])}
                self.assertEqual(len(logical_tuples), 28)
                i.validate_combined(value, self.rows, True, decision)
                bundles = i.bundle_matrix(value, self.rows, True, decision)
                job_count = json.loads((ROOT / 'scripts/ci-integration-targets.json').read_text())['normal_bundles']['job_count']
                self.assertLessEqual(len(bundles['include']), job_count)
                if proof is fallback:
                    self.assertEqual(len(bundles['include']), job_count)
                flattened = [bundle[f'slot{slot}'] for bundle in bundles['include']
                             for slot in range(1, 5) if bundle[f'slot{slot}'] is not None]
                self.assertGreaterEqual(len(flattened), len(value['include']))
                self.assertEqual({row['id'] for row in flattened},
                                 {row['id'] for row in value['include']})
                self.assertEqual(len([s for s in flattened if s['fragment'] is None]
                                     + list({s['id']: s for s in flattened if s['fragment'] is not None}.values())),
                                 25 + integration_count)
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
                                    for slot in (bundle['slot1'], bundle['slot2'], bundle['slot3'], bundle['slot4'])
                                    if slot is not None and slot['kind'] == 'integration')
                        slot['integration']['source_sha'] = '0' * 40
                    else:
                        slot = next(slot for bundle in changed_bundles['include']
                                    for slot in (bundle['slot1'], bundle['slot2'], bundle['slot3'], bundle['slot4'])
                                    if slot is not None and slot['kind'] == 'pr')
                        slot['pr']['features'] = 'mixed-profile'
                    with self.subTest(bundle_payload=mutation), self.assertRaises(ValueError):
                        i.validate_bundle_matrix(changed_bundles, value, self.rows, True, decision)
                manifest_path = ROOT / 'scripts/ci-integration-targets.json'
                original = manifest_path.read_text()
                for mutation in ('missing', 'duplicate', 'tampered'):
                    changed = json.loads(original)
                    ids = changed['normal_bundles']['jobs'][0]
                    if mutation == 'missing': ids.clear()
                    elif mutation == 'duplicate': ids.append(changed['normal_bundles']['jobs'][1][0])
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
                self.assertEqual(len(disabled['include']), 25)
                self.assertTrue(all(row['kind'] == 'pr' for row in disabled['include']))
                disabled_bundles = i.bundle_matrix(disabled, self.rows, False, decision)
                layout = json.loads((ROOT / 'scripts/ci-integration-targets.json').read_text())['normal_bundles']['jobs']
                pr_jobs = [job for job in layout if any(
                    (slot if isinstance(slot, str) else slot['row']).startswith('pr-') for slot in job)]
                self.assertEqual(len(disabled_bundles['include']), len(pr_jobs))
                disabled_slots = [bundle[f'slot{slot}'] for bundle in disabled_bundles['include'] for slot in range(1, 5)
                                  if bundle[f'slot{slot}'] is not None]
                self.assertEqual({slot['id'] for slot in disabled_slots}, {row['id'] for row in disabled['include']})
                self.assertTrue(all(slot['kind'] == 'pr' for slot in disabled_slots))
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

    def test_fragments_place_every_test_exactly_once(self):
        layout = json.loads((ROOT / 'scripts/ci-integration-targets.json').read_text())['normal_bundles']['jobs']
        fragments = {}
        for job in layout:
            for slot in job:
                if isinstance(slot, dict):
                    fragments.setdefault(slot['row'], []).append(slot)
        self.assertGreaterEqual(len(fragments), 10)
        sliced = 0
        for row_id, slots in fragments.items():
            universe = sorted({t for slot in slots for t in slot['targets']})
            i.validate_fragments(row_id, universe, slots)
            groups = {}
            for slot in slots:
                for target, spec in slot.get('tests', {}).items():
                    groups.setdefault(target, []).append(spec)
            for target, specs in groups.items():
                sliced += 1
                named = sorted({n for spec in specs for n in spec.get('only', [])})
                # Every named test, and a test added later, runs in exactly one slice.
                for test in named + ['a_test_added_after_this_layout_was_reviewed']:
                    self.assertEqual(sum(i.slice_selects(spec, test) for spec in specs), 1, (row_id, target, test))
                for label in ('rest forgets a named test', 'a name in two slices',
                              'a slice loses a name the rest still skips', 'a slice is dropped'):
                    changed = copy.deepcopy(slots)
                    owners = [slot for slot in changed if target in slot.get('tests', {})]
                    changed_specs = [slot['tests'][target] for slot in owners]
                    changed_rest = next(spec for spec in changed_specs if 'except' in spec)
                    changed_only = [spec for spec in changed_specs if 'only' in spec]
                    if label == 'rest forgets a named test':
                        changed_rest['except'].pop()
                    elif label == 'a name in two slices':
                        changed_only[-1]['only'].append(changed_only[0]['only'][0])
                    elif label == 'a slice loses a name the rest still skips':
                        changed_only[0]['only'].pop()
                    else:
                        owner = next(slot for slot in owners if slot['tests'][target] is changed_only[0])
                        del owner['tests'][target]
                        owner['targets'].remove(target)
                    with self.subTest(row=row_id, target=target, mutation=label), self.assertRaises(ValueError):
                        i.validate_fragments(row_id, universe, [slot for slot in changed if slot['targets']])
        self.assertGreaterEqual(sliced, 7)
        # A whole target in no fragment, or in two, is refused.
        row_id, slots = next((k, v) for k, v in fragments.items() if len(v) > 1)
        universe = sorted({t for slot in slots for t in slot['targets']})
        lost = copy.deepcopy(slots)
        lost[0]['targets'].pop(0)
        lost[0].pop('tests', None)
        with self.assertRaises(ValueError):
            i.validate_fragments(row_id, universe, lost)

    def test_a_sliced_library_target_runs_with_lib_not_test(self):
        row = next(r for r in i.active_rows(self.rows, False, 'normal') if r['id'] == 'engine-family-1')
        fragment = dict(row='integration-engine-family-1', part=1, targets=['lib'],
                        tests={'lib': {'only': ['database::a_slow_unit_test']}})
        commands = i.commands(row, mode='normal', fragment=fragment)
        self.assertEqual(len(commands), 1)
        self.assertIn('--lib', commands[0])
        self.assertNotIn('--test', commands[0])
        self.assertEqual(commands[0][commands[0].index('--') + 1:][-2:], ['database::a_slow_unit_test', '--exact'])

    def test_shared_builds_compile_with_their_consumers_rustflags(self):
        # A different RUSTFLAGS gives Cargo different units, so the restored build would be rebuilt.
        for name, spec in i.SHARED_BUILDS.items():
            integration = 'nightly-tests' in spec['features']
            self.assertEqual(spec['rustflags'], i.INTEGRATION_RUSTFLAGS if integration else '', name)
        self.assertIn("RUSTFLAGS=INTEGRATION_RUSTFLAGS", (ROOT / 'scripts/ci-integration.py').read_text())
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        self.assertIn('RUSTFLAGS: ""', workflow)

    def test_slice_status_contract_refuses_skipped_or_hidden_failures(self):
        ok, red = dict(exit_status=0), dict(exit_status=37)
        i.slice_status_contract([ok, ok], 2, 0)
        i.slice_status_contract([red, ok, dict(exit_status=5)], 3, 37)
        for ran, planned, status in [([red], 3, 37), ([ok, red], 2, 0), ([ok, red], 2, 1), ([], 1, 0)]:
            with self.subTest(ran=ran, planned=planned, status=status), self.assertRaises(ValueError):
                i.slice_status_contract(ran, planned, status)

    def test_slice_runs_every_command_after_a_failing_one(self):
        row = next(r for r in self.rows if r['kind'] == 'engine-tests' and len(i.commands(r, mode='full')) > 1
                   and 'sync_schema_transactions' not in r['targets'] and r['id'] not in i.LONG_RUNNING_PARTITIONS)
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp); tools = directory / 'bin'; tools.mkdir()
            cargo = tools / 'cargo'
            cargo.write_text('''#!/usr/bin/env python3
import json,os,sys
with open(os.environ['CALLS'],'a') as out: out.write(json.dumps(sys.argv[1:])+'\\n')
if sys.argv[1] == 'test' and '--no-run' not in sys.argv:
    seen = os.path.join(os.environ['CALLS'] + '.first')
    first = not os.path.exists(seen)
    open(seen, 'a').close()
    print('     Running tests/x.rs (target/debug/deps/x-123)')
    print('running 1 test')
    print('test result: %s. %d passed; %d failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s' % (('FAILED', 0, 1) if first else ('ok', 1, 0)))
    sys.exit(37 if first else 0)
''')
            cargo.chmod(0o755)
            rustc = tools / 'rustc'; rustc.write_text('#!/bin/sh\necho synthetic-rustc\n'); rustc.chmod(0o755)
            env = dict(os.environ, PATH=str(tools) + ':' + os.environ['PATH'], CALLS=str(directory / 'calls'),
                       CI_INTEGRATION_OUT=str(directory / 'out'), CARGO_TARGET_DIR=str(directory / 'target'),
                       CI_INTEGRATION_DEFAULT='true', RUST_TEST_THREADS='3', PYTHONDONTWRITEBYTECODE='1')
            env.pop('CI_INTEGRATION_ENABLED', None)
            run = subprocess.run(['bash', 'scripts/verify', 'integration', row['id']], cwd=ROOT, env=env,
                                 capture_output=True, text=True, timeout=30)
            self.assertEqual(run.returncode, 37, run.stderr)
            with patch.dict(os.environ, env):
                planned = i.commands(row)
            calls = [json.loads(line) for line in (directory / 'calls').read_text().splitlines()]
            self.assertEqual(calls[1:], [command[1:] for command in planned])
            receipt = json.loads(next((directory / 'out').rglob('receipt.json')).read_text())
            self.assertEqual([c['exit_status'] for c in receipt['commands']], [37] + [0] * (len(planned) - 1))
            self.assertEqual(receipt['exit_status'], 37)

    def test_a_run_may_select_only_registered_ignores(self):
        registered = next(iter(sorted(i.registered_ignored_functions())))
        def log(passed, ignored):
            names = ''.join('test m::%s ... ignored\n' % name for name in ignored)
            return 'running 1 test\n' + names + 'test result: ok. %d passed; 0 failed; %d ignored; 0 measured; 77 filtered out' % (passed, len(ignored))
        self.assertEqual(i.run_verdict(log(1, [])), (True, ''))
        self.assertTrue(i.run_verdict(log(0, [registered]))[0])
        self.assertFalse(i.run_verdict(log(0, [registered, 'an_unregistered_one']))[0])
        self.assertFalse(i.run_verdict(log(0, ['an_unregistered_one']))[0])
        self.assertFalse(i.run_verdict(log(0, []))[0])
        self.assertFalse(i.run_verdict('')[0])

    def test_a_slice_that_ran_no_test_cannot_pass(self):
        row = next(r for r in self.rows if r['kind'] == 'engine-tests' and r['id'] not in i.LONG_RUNNING_PARTITIONS
                   and 'sync_schema_transactions' not in r['targets']
                   and any('--skip' in command or '--exact' in command for command in i.commands(r, mode='full')))
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp); tools = directory / 'bin'; tools.mkdir()
            cargo = tools / 'cargo'
            cargo.write_text('''#!/usr/bin/env python3
import sys
if sys.argv[1] == 'test' and '--no-run' not in sys.argv:
    print('     Running tests/x.rs (target/debug/deps/x-123)')
    print('running 0 tests')
    print('test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s')
''')
            cargo.chmod(0o755)
            rustc = tools / 'rustc'; rustc.write_text('#!/bin/sh\necho synthetic-rustc\n'); rustc.chmod(0o755)
            env = dict(os.environ, PATH=str(tools) + ':' + os.environ['PATH'], CI_INTEGRATION_OUT=str(directory / 'out'),
                       CARGO_TARGET_DIR=str(directory / 'target'), CI_INTEGRATION_DEFAULT='true', PYTHONDONTWRITEBYTECODE='1')
            env.pop('CI_INTEGRATION_ENABLED', None)
            run = subprocess.run(['bash', 'scripts/verify', 'integration', row['id']], cwd=ROOT, env=env,
                                 capture_output=True, text=True, timeout=30)
            self.assertNotEqual(run.returncode, 0, run.stdout)
            self.assertIn('ran no test', run.stderr)

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
            classes = {'README.md': 'documentation-only'}
            classes_file = Path(tmp) / 'path-classes.json'
            classes_file.write_text(json.dumps(classes))
            run=subprocess.run([sys.executable,'-B','scripts/ci-integration.py','plan'],cwd=ROOT,env=dict(os.environ,GITHUB_OUTPUT=str(Path(tmp)/'outputs'),CI_INTEGRATION_ENABLED='false',CI_INTEGRATION_DEFAULT='false',IMPACT_REASON='documentation-only',PATH_CLASSES_FILE=str(classes_file)),capture_output=True,text=True,timeout=10)
            self.assertEqual(run.returncode,0,run.stderr)
            self.assertEqual(run.stdout.splitlines(), [
                'Extended integration deliberately not required: documentation-only',
                'Path classes: ' + json.dumps(classes, ensure_ascii=True),
                'Performance measurement: measurement pending (not a qualification and not a required gate)',
                'All original PR checks remain required.',
            ])
            self.assertEqual(json.loads(next(line.removeprefix('Path classes: ') for line in run.stdout.splitlines() if line.startswith('Path classes: '))), classes)
            self.assertIn('enabled=false',(Path(tmp)/'outputs').read_text())


if __name__=='__main__':
    unittest.main(verbosity=2)
