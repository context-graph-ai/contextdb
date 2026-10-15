#!/usr/bin/env python3
"""Offline controls for target union and measured target receipts."""
import importlib.util
import sys
sys.dont_write_bytecode = True
from pathlib import Path
import tempfile
import subprocess
import os
import json
import shlex
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('inventory', Path(__file__).with_name('ci-inventory.py'))
i = importlib.util.module_from_spec(spec)
spec.loader.exec_module(i)


class InventoryControls(unittest.TestCase):
    def test_bundle_inventory_and_missing_duplicate_constituents(self):
        jobs = i.pr_jobs(i.ROOT)
        self.assertEqual(len(jobs), 26)
        rows = [row for _, items in jobs for row in items]
        self.assertEqual(len(rows), 33)
        self.assertEqual(len({row[0] for row in rows}), 33)
        path = i.ROOT / 'scripts/ci-integration-targets.json'
        original = json.loads(path.read_text()); read = Path.read_text
        bundle = next(row for row in original['pr_jobs'] if row['shard'] == 'bundle-transactions-relational')
        replacements = []
        for mode in ['missing', 'duplicate', 'features']:
            changed = json.loads(json.dumps(original))
            items = next(row for row in changed['pr_jobs'] if row['shard'] == bundle['shard'])['constituents']
            if mode == 'missing': items.pop(0)
            elif mode == 'duplicate': items.insert(0, dict(items[0]))
            else: items[0]['features'] = 'test-seams'
            replacements.append(json.dumps(changed))
        for replacement in replacements:
            with self.subTest(replacement=replacement[:80]), patch.object(
                    Path, 'read_text', lambda p, *a, **kw: replacement if p == path else read(p, *a, **kw)):
                with self.assertRaises(ValueError): i.inventory(i.ROOT)

    def test_bundle_entrypoint_fails_on_failed_skipped_missing_receipts(self):
        for mode, expected, count in [('success', 0, 3), ('failed', 7, 3),
                                      ('skipped', 2, 3), ('missing', 2, 3), ('bad-time', 2, 3),
                                      ('stdin-drain', 0, 3), ('count-mismatch', 2, 3)]:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                child = root / 'bash'
                child.write_text('''#!/usr/bin/env python3
import os,sys
from pathlib import Path
if os.environ['BUNDLE_TEST_MODE']=='stdin-drain':
    assert sys.stdin.read() == '', 'child inherited bundle plan stdin'
shard=os.environ['CI_SHARD']
with open(os.environ['CALLS'],'a') as out:
    out.write('|'.join(os.environ[k] for k in ['CI_CRATE','CI_SHARD','CI_FEATURES'])+'\\n')
mode=os.environ['BUNDLE_TEST_MODE'] if shard.endswith('-test-seams') else 'success'
if mode=='failed': sys.exit(7)
if mode=='skipped': sys.exit(0)
p=Path(os.environ['CARGO_TARGET_DIR'])/'ci-shard-out'/shard
p.mkdir(parents=True)
for suffix in ['profile','tests','targets.json','time']:
    if mode=='missing' and suffix=='targets.json': continue
    (p/(shard+'.'+suffix)).write_text(shard+' 1s exit='+('7' if mode=='bad-time' else '0')+'\\n')
''')
                child.chmod(0o755)
                if mode == 'count-mismatch':
                    # Force the planned-count disagreement; all three children still run.
                    counter = root / 'wc'
                    counter.write_text('#!/bin/sh\ncat >/dev/null\nprintf "4\\n"\n')
                    counter.chmod(0o755)
                if mode == 'skipped':
                    stale = root/'target/ci-shard-out/contextdb-tx-test-seams'
                    stale.mkdir(parents=True)
                    for suffix in ['profile', 'tests', 'targets.json', 'time']:
                        (stale/('contextdb-tx-test-seams.'+suffix)).write_text('contextdb-tx-test-seams 1s exit=0\n')
                env = dict(os.environ, PATH=str(root)+':'+os.environ['PATH'],
                           CARGO_TARGET_DIR=str(root/'target'), CALLS=str(root/'calls'),
                           BUNDLE_TEST_MODE=mode, CI_BUNDLE='bundle-transactions-relational')
                run = subprocess.run(['/bin/bash', 'scripts/ci-test-shard.sh'], cwd=i.ROOT,
                                     env=env, capture_output=True, text=True, timeout=15)
                self.assertEqual(run.returncode, expected, run.stderr)
                calls=(root/'calls').read_text().splitlines()
                self.assertEqual(len(calls), count)
                self.assertEqual(calls[0], 'contextdb-tx|contextdb-tx|')
                self.assertEqual(calls[1], 'contextdb-tx|contextdb-tx-test-seams|test-seams')
                if count == 3: self.assertEqual(calls[2], 'contextdb-relational|contextdb-relational|')

    def test_exact_eight_target_transfer_is_fail_closed(self):
        path = i.ROOT / 'scripts/ci-shard-targets.txt'
        original = path.read_text(); read = Path.read_text
        moved = ['bounded_in_subquery_stays_inside_the_read_limits',
                 'bounded_read_of_staged_graph_work_stays_within_its_limits',
                 'local_transport', 'owner_read', 'reader_release', 'sql_surface',
                 'staged_predecode_row', 'text_memory_charge_measurement']
        lines = {line.split(': ', 1)[0]: line.split(': ', 1)[1].split()
                 for line in original.splitlines() if line and not line.startswith('#')}
        self.assertEqual(lines['contextdb-engine-test-seams-4'], ['acceptance'])
        self.assertEqual(lines['contextdb-engine-test-seams-13'], ['acceptance_long_running'])
        self.assertEqual(lines['contextdb-engine-test-seams-14'], ['acceptance_sync'])
        self.assertEqual(lines['contextdb-engine-test-seams-5'], ['sync', *moved])
        for target in moved:
            replacement = original.replace(' ' + target, '', 1)
            with self.subTest(target=target), patch.object(
                    Path, 'read_text', lambda p, *a, **kw: replacement if p == path else read(p, *a, **kw)):
                with self.assertRaises(ValueError):
                    i.inventory(i.ROOT)

    def test_actual_inventory(self):
        i.inventory(i.ROOT)

    def test_missing_and_duplicate_target_fail_closed(self):
        path = i.ROOT / 'scripts/ci-shard-targets.txt'
        original = path.read_text()
        read = Path.read_text
        for replacement in [original.replace(' durable_public_api', '', 1),
                            original.replace(' durable_public_api', ' durable_public_api durable_public_api', 1)]:
            with self.subTest(replacement=replacement), patch.object(
                    Path, 'read_text', lambda p, *a, **kw: replacement if p == path else read(p, *a, **kw)):
                with self.assertRaisesRegex(ValueError, 'target mismatch|target twice'):
                    i.inventory(i.ROOT)

    def test_schema_replay_canonical_union_is_fail_closed(self):
        path = i.ROOT / 'scripts/ci-shard-targets.txt'
        original = path.read_text()
        shards = {name: targets.split() for name, targets in
                  (line.split(': ', 1) for line in original.splitlines() if line and not line.startswith('#'))}
        expected = {
            'contextdb-engine-test-seams-9': ['custody_schema_replay_is_proportional_to_affected_tables',
                                               'custody_schema_replay_bounded_contract',
                                               'custody_schema_replay_work_limited_to_affected_tables',
                                               'custody_schema_replay_identical_schema_does_no_artifact_work'],
        }
        self.assertEqual(len(shards), 17)
        read = Path.read_text
        _, rows = i.inventory(i.ROOT)
        self.assertEqual(sum(crate == 'contextdb-engine' and features == 'test-seams,iroh'
                             for _, crate, features in rows), 17)
        for shard, targets in expected.items():
            self.assertEqual(shards[shard], targets)
            for target in targets:
                for replacement in [original.replace(' ' + target, '', 1),
                                    original.replace(' ' + target, ' ' + target + ' ' + target, 1)]:
                    with self.subTest(shard=shard, target=target, replacement=replacement), patch.object(
                            Path, 'read_text', lambda p, *a, **kw: replacement if p == path else read(p, *a, **kw)):
                        with self.assertRaises(ValueError):
                            i.inventory(i.ROOT)
        for target in [target for targets in expected.values() for target in targets]:
            fixture = i.ROOT / 'crates/contextdb-engine/tests' / (target + '.rs')
            source = fixture.read_text()
            full = target != 'custody_schema_replay_bounded_contract'
            for rows in ([5_000, 1] if full else [5_000, 10_000]):
                replacement = source.replace('const UNRELATED_RESIDENT_ROWS: i64 = '
                                             + ('10_000' if full else '1') + ';',
                                             f'const UNRELATED_RESIDENT_ROWS: i64 = {rows};')
                with self.subTest(target=target, rows=rows), patch.object(
                        Path, 'read_text', lambda p, *a, **kw: replacement if p == fixture else read(p, *a, **kw)):
                    with self.assertRaisesRegex(ValueError, 'schema replay frozen fixture mismatch'):
                        i.inventory(i.ROOT)

    def test_vector_corpus_targets_remain_once_in_partitioned_ordinary_shards(self):
        path = i.ROOT / 'scripts/ci-shard-targets.txt'; original = path.read_text(); read = Path.read_text
        targets = ['vector_graph_recall_5000_anisotropic_per_context',
                   'vector_graph_recall_5001_topic_clusters_per_context',
                   'vector_graph_recall_50000_isotropic_per_context',
                   'vector_graph_recall_50001_anisotropic_per_context']
        _, rows = i.inventory(i.ROOT)
        self.assertEqual(sum(crate == 'contextdb-engine' and features == 'test-seams,iroh'
                             for _, crate, features in rows), 17)
        for target in targets:
            self.assertEqual(original.split().count(target), 1)
            for replacement in [original.replace(' ' + target, '', 1),
                                original.replace(' ' + target, ' ' + target + ' ' + target, 1)]:
                with self.subTest(target=target), patch.object(
                        Path, 'read_text', lambda p, *a, **kw: replacement if p == path else read(p, *a, **kw)):
                    with self.assertRaises(ValueError): i.inventory(i.ROOT)

    def test_target_durations_keep_final_parent_summary_and_thread_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'shard.log'
            path.write_text('     Running tests/family.rs (target/debug/deps/family-123)\nrunning 7 tests\n'
                            'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out; finished in 0.01s\n'
                            'test result: ok. 6 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 1900.25s\n'
                            '     Running tests/incomplete.rs (target/debug/deps/incomplete-456)\nrunning 7 tests\n'
                            'running 1 test\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out; finished in 0.01s\n')
            for threads in ['1', 'default-parallel', '3']:
                result = i.timing_receipt([path], threads)
                self.assertTrue(result['ordinary_runtime_is_report_only'])
                self.assertEqual(len(result['targets']), 1)
                row = result['targets'][0]
                self.assertEqual((row['target'], row['duration_seconds'], row['ignored'], row['test_threads']),
                                 ('tests/family.rs', 1900.25, 1, threads))

    def test_arbitrary_log_bytes_preserve_completed_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'shard.log'
            path.write_bytes(b'     Running tests/bytes.rs (target/debug/deps/bytes-123)\n'
                             b'running 1 test\noutput: \xff\xfe\x00\n'
                             b'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; '
                             b'0 filtered out; finished in 0.25s\n')
            rows = i.timing_receipt([path], '3')['targets']
            self.assertEqual(len(rows), 1)
            self.assertEqual((rows[0]['target'], rows[0]['passed'], rows[0]['duration_seconds']),
                             ('tests/bytes.rs', 1, 0.25))

    def test_only_indented_cargo_doctest_headers_start_sections(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'shard.doc.log'
            path.write_text('2026-10-16T00:00:00Z \x1b[32m   Doc-tests\x1b[0m contextdb_core\n'
                            'running 1 test\n'
                            'test example ... output mentions Doc-tests bogus_crate\n'
                            'Doc-tests unindented_test_output\n'
                            '   Doc-tests not a cargo header\n'
                            'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; '
                            '0 filtered out; finished in 0.50s\n')
            rows = i.timing_receipt([path], '3')['targets']
            self.assertEqual(len(rows), 1)
            self.assertEqual((rows[0]['target'], rows[0]['executable'], rows[0]['test_threads']),
                             ('contextdb_core', None, '1'))

    def test_embedded_headers_and_non_newline_separators_preserve_real_target(self):
        for output in ['child stderr: Running migration (step one)',
                       'Running migration (step one)',
                       'child output\x0c     Running migration (step one)',
                       'child output\u2028     Running migration (step one)']:
            with self.subTest(output=output), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / 'shard.log'
                path.write_text('     Running tests/real.rs (target/debug/deps/real-123)\n'
                                'running 1 test\n' + output + '\n'
                                'test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; '
                                '0 filtered out; finished in 0.25s\n')
                rows = i.timing_receipt([path], '3')['targets']
                self.assertEqual(len(rows), 1)
                self.assertEqual((rows[0]['target'], rows[0]['executable'], rows[0]['result']),
                                 ('tests/real.rs', 'target/debug/deps/real-123', 'FAILED'))

    def test_suite_leaves_no_source_artifacts(self):
        self.assertNotEqual(os.environ.get('CI_INVENTORY_ARTIFACT_CHILD'), '1',
                            'artifact-cleanliness child must not re-enter its parent control')
        def source_files():
            return {p.relative_to(i.ROOT): p.read_bytes()
                    for p in (i.ROOT / 'scripts').rglob('*') if p.is_file()}
        before = source_files()
        names = [name for name in unittest.defaultTestLoader.getTestCaseNames(type(self))
                 if name != 'test_suite_leaves_no_source_artifacts']
        env = dict(os.environ)
        env.pop('PYTHONDONTWRITEBYTECODE', None)
        env['CI_INVENTORY_ARTIFACT_CHILD'] = '1'
        run = subprocess.run([sys.executable, '-B', str(Path(__file__).resolve()),
                              *['InventoryControls.' + name for name in names]],
                             env=env, capture_output=True, text=True, timeout=90)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(source_files(), before)

    def test_receipt_read_errors_still_propagate(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                i.timing_receipt([Path(tmp) / 'missing.log'], '1')

    def test_real_runner_records_parallel_and_serial_modes(self):
        for features, mode, inherited, cargo_status, parser_status, expected_status in [
                ("", "default-parallel", None, 0, 0, 0), ("", "3", "3", 0, 0, 0),
                ("test-seams", "1", "3", 0, 0, 0),
                ("", "3", "3", 7, 0, 7), ("", "3", "3", 0, 9, 2),
                ("", "3", "3", 7, 9, 7)]:
            with self.subTest(features=features, cargo_status=cargo_status, parser_status=parser_status), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cargo = root / 'cargo'
                cargo.write_text("#!/bin/bash\n"
                                 "if [[ $1 == --version ]]; then echo 'cargo synthetic'; exit; fi\n"
                                 "echo \"$*\" >> \"$CALLS\"\n"
                                 "if [[ \" $* \" == *' --examples '* ]]; then exit; fi\n"
                                 "if [[ \" $* \" == *' --doc '* ]]; then echo '   Doc-tests contextdb_core'; "
                                 "else echo '     Running unittests src/lib.rs (target/debug/deps/core-123)'; fi\n"
                                 "echo 'running 1 test'\n"
                                 "echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1901.00s'\n"
                                 f"exit {cargo_status}\n")
                cargo.chmod(0o755)
                python = root / 'python3'
                python.write_text('#!/bin/bash\n'
                                  f'if [[ "$2" == --timings && {parser_status} != 0 ]]; '
                                  f'then exit {parser_status}; fi\n'
                                  f'exec {shlex.quote(sys.executable)} -B "$@"\n')
                python.chmod(0o755)
                env = dict(os.environ, PATH=str(root) + ':' + os.environ['PATH'],
                           CARGO_TARGET_DIR=str(root / 'target'), CALLS=str(root / 'calls'),
                           CI_CRATE='contextdb-core', CI_SHARD='contextdb-core', CI_FEATURES=features)
                env.pop('RUST_TEST_THREADS', None)
                if inherited is not None:
                    env['RUST_TEST_THREADS'] = inherited
                run = subprocess.run(['bash', 'scripts/ci-test-shard.sh'], cwd=i.ROOT, env=env,
                                     capture_output=True, text=True, timeout=15)
                self.assertEqual(run.returncode, expected_status, run.stderr)
                receipts = root / 'target/ci-shard-out/contextdb-core'
                profile = (receipts / 'contextdb-core.profile').read_text()
                self.assertIn('test_threads=' + mode, profile)
                self.assertIn(f'exit={expected_status}', (receipts / 'contextdb-core.time').read_text())
                if expected_status:
                    continue
                rows = json.loads((receipts / 'contextdb-core.targets.json').read_text())['targets']
                self.assertEqual(len(rows), 2)
                self.assertEqual({row['test_threads'] for row in rows}, {mode, '1'})
                calls = (root / 'calls').read_text().splitlines()
                self.assertEqual(len(calls), 3)
                self.assertEqual('--test-threads=1' in calls[0], bool(features))


if __name__ == '__main__':
    unittest.main()
