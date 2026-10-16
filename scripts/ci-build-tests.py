#!/usr/bin/env python3
"""Offline delivery controls; synthetic archives are not native build evidence."""
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
import sys
sys.dont_write_bytecode = True
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('control', Path(__file__).with_name('ci-build-control.py'))
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
SHA = 'a' * 40
OTHER = 'b' * 40


def fixtures():
    env = dict(BUILD_SHA=SHA, GITHUB_REPOSITORY=c.REPO, WORKFLOW_SHA=SHA, GITHUB_SHA=SHA,
               GITHUB_EVENT_NAME='push', GITHUB_REF='refs/heads/dev',
               GITHUB_RUN_ID='12', GITHUB_RUN_ATTEMPT='2',
               GITHUB_WORKFLOW_REF=c.REPO + '/.github/workflows/ci.yml@refs/heads/dev')
    run = dict(head_sha=SHA, head_branch='dev', head_repository={'full_name': c.REPO},
               repository={'full_name': c.REPO}, event='push', conclusion='success', status='completed',
               path='.github/workflows/ci.yml', id=12, run_attempt=2, workflow_id=4)
    job = dict(name='PR checks', conclusion='success', status='completed', head_sha=SHA,
               run_id=12, run_attempt=2)
    return env, run, job


def workflow_guard(doc):
    """Focused structural contract; actionlint is the separate schema check."""
    c.require(set(doc['on']) == {'workflow_call', 'workflow_dispatch'}, 'triggers')
    c.require(not (doc['on']['workflow_call'] or {}).get('inputs'), 'automatic call needs no source input')
    c.require(doc['permissions'] == {'contents': 'read', 'actions': 'read'}, 'global permissions')
    c.require('concurrency' not in doc, 'no workflow-wide delivery queue')
    jobs = doc['jobs']
    c.require(set(jobs) == {'qualify', 'build', 'publish'}, 'job set')
    c.require(all('concurrency' not in jobs[j] for j in ('qualify', 'build')),
              'only publisher may join delivery queue')
    c.require(jobs['publish'].get('concurrency') == {
        'group': 'contextdb-dev-binaries-publish', 'cancel-in-progress': False, 'queue': 'max'},
        'publisher serialization scope')
    c.require(type(jobs['publish']['concurrency']['cancel-in-progress']) is bool,
              'cancellation must be a boolean')
    spec = importlib.util.spec_from_file_location('queue_contract', Path(__file__).with_name('ci-build-publisher-tests.py'))
    contract = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(contract)
    contract.queue_guard(doc)
    c.require('always(' not in jobs['publish'].get('if', ''), 'publisher requires successful dependencies')
    c.require(set(jobs['build']['needs']) == {'qualify'}, 'build qualification dependency')
    c.require(set(jobs['publish']['needs']) == {'qualify', 'build'}, 'publish dependencies')
    c.require(jobs['build']['permissions'] == {'contents': 'read'}, 'build permissions')
    c.require(jobs['qualify']['permissions'] == {'contents': 'read', 'actions': 'read'}, 'guard permissions')
    c.require(jobs['publish']['permissions'] == {'contents': 'write', 'actions': 'read'}, 'publish permissions')
    for job in jobs.values():
        for step in job['steps']:
            if 'uses' in step:
                c.require(c.re.fullmatch(r'[^@]+@[0-9a-f]{40}', step['uses']), 'action commit pin')
            if step.get('uses', '').startswith('actions/checkout@'):
                c.require(step['with']['persist-credentials'] is False, 'checkout credentials')
    contract.build_execution_guard(doc)
    # Privileged helper is only checked out from the source-bound workflow SHA.
    checkout = jobs['publish']['steps'][0]
    c.require(checkout['with']['ref'] == '${{ github.workflow_sha }}', 'publish checkout')


class SourceControls(unittest.TestCase):
    def test_valid_push_and_manual(self):
        env, run, job = fixtures()
        c.source_guard(env, push_event(), SHA)
        c.aggregate_guard([job], run)
        env['GITHUB_EVENT_NAME'] = 'workflow_dispatch'
        c.source_guard(env, {'inputs': {'sha': SHA}}, SHA)

    def test_source_mutations(self):
        for key, value in [('GITHUB_REPOSITORY', 'fork/contextdb'), ('WORKFLOW_SHA', OTHER),
                           ('GITHUB_SHA', OTHER), ('BUILD_SHA', 'dev'), ('GITHUB_EVENT_NAME', 'pull_request')]:
            with self.subTest(key=key):
                env, run, _ = fixtures()
                env[key] = value
                with self.assertRaises(ValueError):
                    c.source_guard(env, push_event(), SHA)
        env, run, _ = fixtures()
        with self.assertRaises(ValueError):
            c.source_guard(env, push_event(), OTHER)

    def test_push_mutations(self):
        for key, value in [('head_sha', OTHER), ('head_branch', 'main'),
                           ('head_repository', {'full_name': 'fork/contextdb'}),
                           ('repository', {'full_name': 'fork/contextdb'}), ('event', 'pull_request'),
                           ('conclusion', 'cancelled'), ('status', 'queued'),
                           ('path', '.github/workflows/nightly.yml')]:
            with self.subTest(key=key, value=value):
                _, run, _ = fixtures()
                run[key] = value
                with self.assertRaises(ValueError):
                    c.run_guard(run, SHA)

    def test_aggregate_mutations(self):
        _, run, job = fixtures()
        for jobs in ([], [job, job]):
            with self.assertRaises(ValueError):
                c.aggregate_guard(jobs, run)
        for key, value in [('conclusion', 'failure'), ('conclusion', 'skipped'),
                           ('conclusion', 'cancelled'), ('status', 'in_progress'),
                           ('head_sha', OTHER), ('run_id', 13), ('run_attempt', 1)]:
            with self.subTest(key=key, value=value):
                bad = dict(job, **{key: value})
                with self.assertRaises(ValueError):
                    c.aggregate_guard([bad], run)

    def test_manual_rejects_wrong_ref_and_input(self):
        env, _, _ = fixtures()
        env['GITHUB_EVENT_NAME'] = 'workflow_dispatch'
        with self.assertRaises(ValueError):
            c.source_guard(env, {'inputs': {'sha': OTHER}}, SHA)
        env['GITHUB_REF'] = 'refs/heads/main'
        with self.assertRaises(ValueError):
            c.source_guard(env, {'inputs': {'sha': SHA}}, SHA)

    def test_actual_guard_manual_requires_ci_and_aggregate(self):
        env, run, job = fixtures()
        env['GITHUB_EVENT_NAME'] = 'workflow_dispatch'
        with tempfile.TemporaryDirectory() as tmp:
            event = Path(tmp, 'event.json')
            event.write_text(json.dumps({'inputs': {'sha': SHA}}))
            env['GITHUB_EVENT_PATH'] = str(event)
            def api(path):
                return {'object': {'sha': SHA}} if '/git/ref/' in path else {'id': 4}
            for runs, jobs, passes in (([run], [job], True), ([], [job], False),
                                       ([run], [], False), ([dict(run, workflow_id=5)], [job], False)):
                with self.subTest(runs=runs, jobs=jobs), patch.dict(os.environ, env), \
                     patch.object(c, 'api', side_effect=api), \
                     patch.object(c, 'pages', side_effect=lambda path, key: runs if key == 'workflow_runs' else jobs):
                    if passes:
                        c.guard()
                    else:
                        with self.assertRaises(ValueError):
                            c.guard()


def push_event():
    return {'after': SHA, 'ref': 'refs/heads/dev',
            'repository': {'full_name': c.REPO, 'default_branch': 'main'},
            'head_commit': {'id': SHA}}


class CurrentRunControls(unittest.TestCase):
    """Exercise the actual helper with strict offline Actions API responses."""
    def invoke(self, env=None, event=None, run=None, jobs=None, tip=SHA, manual=False):
        base, good_run, aggregate = fixtures()
        base.update(env or {})
        run = dict(good_run, status='in_progress', conclusion=None) if run is None else run
        jobs = [aggregate] if jobs is None else jobs
        event = push_event() if event is None else event
        if manual:
            base['GITHUB_EVENT_NAME'] = 'workflow_dispatch'
            base['GITHUB_WORKFLOW_REF'] = c.REPO + '/.github/workflows/build.yml@refs/heads/dev'
            event = {'inputs': {'sha': SHA}}
        calls = []
        def api(path, **fields):
            self.assertFalse(fields, 'admission must be read-only')
            calls.append(path)
            if path == f'repos/{c.REPO}/git/ref/heads/dev':
                return {'object': {'sha': tip}}
            if path == f'repos/{c.REPO}/actions/workflows/ci.yml':
                return {'id': 4}
            if path == f'repos/{c.REPO}/actions/runs/12':
                return run
            self.fail('unexpected API request: ' + path)
        def pages(path, key):
            calls.append(path)
            if key == 'workflow_runs' and manual:
                self.assertEqual(path, f'repos/{c.REPO}/actions/workflows/ci.yml/runs?event=push&head_sha={SHA}&per_page=100')
                return [run]
            self.assertEqual(key, 'jobs', 'automatic admission must not search completed CI runs')
            self.assertEqual(path, f'repos/{c.REPO}/actions/runs/12/attempts/2/jobs?per_page=100',
                             'only current run/latest attempt may prove aggregate success')
            return jobs
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, 'event.json')
            path.write_text(json.dumps(event))
            base['GITHUB_EVENT_PATH'] = str(path)
            with patch.dict(os.environ, base, clear=True), patch.object(c, 'api', side_effect=api), \
                 patch.object(c, 'pages', side_effect=pages), \
                 patch.object(c, 'command', side_effect=AssertionError('unmocked external command')):
                c.guard()
        return calls

    def test_current_push_in_progress_admitted_without_completed_overall_ci(self):
        calls = self.invoke()
        self.assertFalse(any('workflow_runs' in path or 'status=completed' in path or 'status=success' in path
                             for path in calls))
        self.assertTrue(any('/attempts/2/jobs' in path for path in calls))

    def test_main_default_with_distinct_tip_does_not_change_dev_admission(self):
        event = push_event()
        event['repository'].update(default_branch='main', default_branch_sha=OTHER)
        self.invoke(event=event)

    def test_automatic_rejects_other_workflow_fork_ref_and_source(self):
        for key, value in [('GITHUB_WORKFLOW_REF', c.REPO + '/.github/workflows/build.yml@refs/heads/dev'),
                           ('GITHUB_WORKFLOW_REF', c.REPO + '/.github/workflows/ci.yml@refs/heads/main'),
                           ('GITHUB_REPOSITORY', 'fork/contextdb'), ('GITHUB_REF', 'refs/heads/main'),
                           ('GITHUB_EVENT_NAME', 'workflow_call'), ('WORKFLOW_SHA', OTHER),
                           ('BUILD_SHA', OTHER), ('GITHUB_SHA', OTHER)]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                self.invoke(env={key: value})
        with self.assertRaises(ValueError):
            self.invoke(tip=OTHER)

    def test_automatic_rejects_spoofed_push_payload(self):
        for key, value in [('after', OTHER), ('ref', 'refs/heads/main'),
                           ('repository', {'full_name': 'fork/contextdb'}), ('head_commit', {'id': OTHER})]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.invoke(event=dict(push_event(), **{key: value}))

    def test_automatic_requires_current_run_attempt_and_successful_aggregate(self):
        _, run, job = fixtures()
        for key, value in [('id', 13), ('run_attempt', 1), ('workflow_id', 5), ('head_sha', OTHER),
                           ('path', '.github/workflows/nightly.yml'), ('event', 'pull_request'),
                           ('head_repository', {'full_name': 'fork/contextdb'})]:
            with self.subTest(run_field=key), self.assertRaises(ValueError):
                self.invoke(run=dict(run, status='in_progress', conclusion=None, **{key: value}))
        for jobs in ([], [job, job], [dict(job, conclusion='failure')], [dict(job, conclusion='cancelled')],
                     [dict(job, conclusion='skipped')], [dict(job, status='in_progress')],
                     [dict(job, run_id=13)], [dict(job, run_attempt=1)], [dict(job, head_sha=OTHER)]):
            with self.subTest(jobs=jobs), self.assertRaises(ValueError):
                self.invoke(jobs=jobs)

    def test_manual_exact_source_successful_push_ci_admitted(self):
        _, run, aggregate = fixtures()
        self.invoke(run=run, jobs=[aggregate], manual=True)

    def test_manual_retry_accepts_only_delivery_failure_with_successful_pr_checks(self):
        _, run, aggregate = fixtures()
        delivery = dict(aggregate, name='development-binaries / build', conclusion='failure')
        self.invoke(run=dict(run, conclusion='failure'), jobs=[aggregate, delivery], manual=True)

    def test_manual_failed_required_job_never_qualifies_even_with_aggregate_success(self):
        _, run, aggregate = fixtures()
        for result in ('failure', 'cancelled', 'timed_out'):
            with self.subTest(result=result), self.assertRaises(ValueError):
                self.invoke(run=dict(run, conclusion='failure'),
                            jobs=[aggregate, dict(aggregate, name='lint', conclusion=result)], manual=True)

    def test_manual_rejects_other_failed_jobs_and_missing_required(self):
        _, run, aggregate = fixtures()
        for jobs in ([], [dict(aggregate, conclusion='failure')],
                     [aggregate, dict(aggregate, name='lint', conclusion='failure')],
                     [aggregate, dict(aggregate, name='other-auxiliary', conclusion='failure')],
                     [aggregate, dict(aggregate, name='development-binaries-evil', conclusion='failure')]):
            with self.subTest(jobs=jobs), self.assertRaises(ValueError):
                self.invoke(run=dict(run, conclusion='failure'), jobs=jobs, manual=True)


class AssetControls(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        with tarfile.open(self.root / (c.BASE + '.tar.gz'), 'w:gz') as tar:
            for name in (*c.BINS, 'LICENSE'):
                info = tarfile.TarInfo(name)
                info.size = 4
                tar.addfile(info, io.BytesIO(b'test'))
        checksum = c.digest(self.root / (c.BASE + '.tar.gz'))
        self.receipt = dict(schema=1, source_sha=SHA, repository=c.REPO, target=c.TARGET, configuration=c.CONFIG,
                            dependency_identity={'lock_sha256': 'synthetic'}, run_id='9', run_attempt='1',
                            sha256=checksum, rustc_vV='synthetic', cargo_version='synthetic',
                            binary_versions={b: b + ' synthetic' for b in c.BINS},
                            runner={'os': 'synthetic'}, smoke='synthetic control only')
        self.write_receipt()
        (self.root / (c.BASE + '.tar.gz.sha256')).write_text(f'{checksum}  {c.BASE}.tar.gz\n')

    def write_receipt(self):
        (self.root / (c.BASE + '.json')).write_text(json.dumps(self.receipt))

    def validate(self):
        return c.validate(self.root, SHA, '9', '1', {'lock_sha256': 'synthetic'})

    def test_valid_synthetic_assets(self):
        self.validate()

    def test_receipt_mutations(self):
        original = copy.deepcopy(self.receipt)
        for key, value in [('source_sha', OTHER), ('repository', 'fork/contextdb'), ('target', 'other'),
                           ('configuration', {}), ('dependency_identity', {}), ('run_id', '10'),
                           ('run_attempt', '2'), ('sha256', '0'*64), ('rustc_vV', ''),
                           ('cargo_version', ''), ('binary_versions', {}), ('runner', {'os': ''}), ('smoke', '')]:
            with self.subTest(key=key):
                self.receipt = dict(original, **{key: value})
                self.write_receipt()
                with self.assertRaises(ValueError):
                    self.validate()

    def test_extra_missing_checksum_and_symlink(self):
        extra = self.root / 'extra'
        extra.write_text('unexpected')
        with self.assertRaises(ValueError):
            self.validate()
        extra.unlink()
        checksum = self.root / (c.BASE + '.tar.gz.sha256')
        valid = checksum.read_text()
        checksum.write_text('bad checksum')
        with self.assertRaises(ValueError):
            self.validate()
        checksum.unlink()
        with self.assertRaises(ValueError):
            self.validate()
        source = self.root.parent / (self.root.name + '-sum')
        source.write_text(valid)
        self.addCleanup(source.unlink)
        checksum.symlink_to(source)
        with self.assertRaises(ValueError):
            self.validate()

    def test_corrupt_archive(self):
        archive = self.root / (c.BASE + '.tar.gz')
        archive.write_bytes(archive.read_bytes() + b'mutant')
        with self.assertRaises(ValueError):
            self.validate()

    def test_publish_never_mutates_on_bad_assets(self):
        (self.root / 'extra').touch()
        with patch.dict(os.environ, dict(BUILD_SHA=SHA, BUILD_ASSETS=str(self.root), GITHUB_RUN_ID='9',
                                        GITHUB_RUN_ATTEMPT='1')), patch.object(c, 'guard'), \
             patch.object(c, 'identity', return_value={'lock_sha256': 'synthetic'}), \
             patch.object(c, 'command') as command:
            with self.assertRaises(ValueError):
                c.publish()
            command.assert_not_called()

    def test_publish_stale_and_metadata_failure_no_mutations(self):
        env = dict(BUILD_SHA=SHA, BUILD_ASSETS=str(self.root), GITHUB_RUN_ID='9', GITHUB_RUN_ATTEMPT='1')
        for failure in ('stale', 'metadata'):
            with self.subTest(failure=failure), patch.dict(os.environ, env), patch.object(c, 'guard'), \
                 patch.object(c, 'identity', return_value={'lock_sha256': 'synthetic'}), \
                 patch.object(c, 'api', return_value={'object': {'sha': OTHER}}), \
                 patch.object(c, 'command') as command:
                command.side_effect = subprocess.CalledProcessError(1, 'gh') if failure == 'metadata' else ['[[]]']
                with self.assertRaises((ValueError, subprocess.SubprocessError)):
                    c.publish()
                self.assertEqual(command.call_count, 1)
                self.assertEqual(command.call_args.args[0][:2], ['gh', 'api'])


    def remote(self):
        spec = importlib.util.spec_from_file_location('remote_contract', Path(__file__).with_name('ci-build-publisher-tests.py'))
        contract = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(contract)
        contract.bind(c)
        return contract.Remote(self.root, [dict(id=17, tag_name='dev-latest', prerelease=True,
                                                draft=False, immutable=False)])

    def invoke_remote(self, remote):
        env = dict(BUILD_SHA=SHA, BUILD_ASSETS=str(self.root), GITHUB_RUN_ID='9', GITHUB_RUN_ATTEMPT='1')
        with patch.dict(os.environ, env), patch.object(c, 'guard'), \
             patch.object(c, 'identity', return_value={'lock_sha256': 'synthetic'}), \
             patch.object(c, 'command', side_effect=remote.command):
            c.publish()

    def test_publish_replacements_verify_before_metadata_and_tag(self):
        remote = self.remote()
        self.invoke_remote(remote)
        events = remote.events
        order = [c.BASE + suffix for suffix in ('.tar.gz', '.tar.gz.sha256', '.json')]
        copies = [name.replace(c.BASE, c.BASE + '-' + SHA[:12] + '-r9a1', 1) for name in order]
        self.assertEqual([e['name'] for e in events if e['kind'] == 'upload'], copies + order)
        uploads = [i for i, e in enumerate(events) if e['kind'] == 'upload']
        verifies = [i for i, e in enumerate(events) if e['kind'] == 'verify']
        self.assertTrue(uploads[2] < verifies[0] < uploads[3])
        edit = next(i for i, e in enumerate(events) if e['kind'] == 'edit')
        self.assertTrue(any(uploads[-1] < i < edit for i in verifies))
        self.assertFalse(any(e['kind'] == 'upload' for e in events[edit + 1:]))
        self.assertFalse([e for e in events if e['kind'] == 'tag-write'])

    def test_upload_failure_preserves_metadata_and_tag(self):
        remote = self.remote()
        remote.fail_upload = lambda name: True
        with self.assertRaises(subprocess.SubprocessError):
            self.invoke_remote(remote)
        self.assertFalse([e for e in remote.events if e['kind'] in ('edit', 'delete', 'tag-write')])

    def test_interrupted_uploads_leave_exact_prefix_and_old_metadata(self):
        stable = [c.BASE + suffix for suffix in ('.tar.gz', '.tar.gz.sha256', '.json')]
        copies = [name.replace(c.BASE, c.BASE + '-' + SHA[:12] + '-r9a1', 1) for name in stable]
        order = copies + stable
        new = {p.name: p.read_bytes() for p in self.root.iterdir()}
        old_tar = io.BytesIO()
        with tarfile.open(fileobj=old_tar, mode='w:gz') as tar:
            for name in (*c.BINS, 'LICENSE'):
                info = tarfile.TarInfo(name)
                info.size = 4
                tar.addfile(info, io.BytesIO(b'old!'))
        old_hash = c.hashlib.sha256(old_tar.getvalue()).hexdigest()
        old = {stable[0]: old_tar.getvalue(),
               stable[1]: f'{old_hash}  {stable[0]}\n'.encode(),
               stable[2]: json.dumps(dict(self.receipt, source_sha=OTHER, sha256=old_hash)).encode()}
        recipe = Path('docs/download.md').read_text().split('```bash\n', 1)[1].split('```', 1)[0]
        checks = 'set -e\n' + recipe[recipe.index('sha256sum -c '):recipe.index('tar -tzf ')]
        # Fail before every copy/stable upload; advance dev after each upload.
        for mode, counts in (('failure', range(6)), ('tip', range(1, 7))):
            for count in counts:
                with self.subTest(mode=mode, uploaded=count):
                    remote = self.remote()
                    remote.assets = dict(old)
                    original = remote.command
                    uploads = []
                    def command(args, **kwargs):
                        if args[:2] == ['gh', 'api'] and any('/assets?name=' in a for a in args):
                            if mode == 'failure' and len(uploads) == count:
                                raise subprocess.CalledProcessError(1, args)
                            result = original(args, **kwargs)
                            uploads.append(Path(args[args.index('--input') + 1]).name)
                            if mode == 'tip' and len(uploads) >= count:
                                remote.tip = OTHER
                            return result
                        return original(args, **kwargs)
                    remote.command = command
                    with self.assertRaises((ValueError, subprocess.SubprocessError)):
                        self.invoke_remote(remote)
                    self.assertEqual(uploads, order[:count])
                    changed_stable = set(order[:count]) & set(stable)
                    missing = {order[count]} if mode == 'failure' and 3 <= count < 6 else set()
                    self.assertEqual({n: remote.assets.get(n) for n in stable},
                                     {n: None if n in missing else new[n] if n in changed_stable else old[n]
                                      for n in stable})
                    self.assertFalse([e for e in remote.events if e['kind'] in ('edit', 'delete', 'tag-write')],
                                     'metadata/tag must remain old on interruption')
                    with tempfile.TemporaryDirectory() as download:
                        for name in stable:
                            if name in remote.assets:
                                Path(download, name).write_bytes(remote.assets[name])
                        result = subprocess.run(['bash', '-c', checks], cwd=download,
                                                text=True, capture_output=True)
                    self.assertEqual(result.returncode == 0, not missing and (count <= 3 or count == 6),
                                     'interrupted mixed sets must fail the actual download checks')

    def test_download_recipe_rejects_mixed_sidecars_before_unpacking(self):
        # Execute the actual documented checks, without download, extraction or binaries.
        recipe = Path('docs/download.md').read_text().split('```bash\n', 1)[1].split('```', 1)[0]
        checks = 'set -e\n' + recipe[recipe.index('sha256sum -c '):recipe.index('tar -tzf ')]
        original = {p.name: p.read_bytes() for p in self.root.iterdir()}
        archive = self.root / (c.BASE + '.tar.gz')
        sidecar = self.root / (c.BASE + '.tar.gz.sha256')
        receipt = self.root / (c.BASE + '.json')
        for mixed in ('none', 'new-receipt-old-pair', 'new-archive-old-sidecars', 'new-pair-old-receipt'):
            with self.subTest(mixed=mixed):
                for name, content in original.items():
                    (self.root / name).write_bytes(content)
                if mixed == 'new-receipt-old-pair':
                    receipt.write_text(json.dumps(dict(self.receipt, source_sha=OTHER, sha256='0' * 64)))
                elif mixed in ('new-archive-old-sidecars', 'new-pair-old-receipt'):
                    archive.write_bytes(archive.read_bytes() + b'new archive')
                    if mixed == 'new-pair-old-receipt':
                        sidecar.write_text(f'{c.digest(archive)}  {archive.name}\n')
                result = subprocess.run(['bash', '-c', checks], cwd=self.root, text=True, capture_output=True)
                self.assertEqual(result.returncode == 0, mixed == 'none', result.stdout + result.stderr)
                if mixed in ('new-receipt-old-pair', 'new-pair-old-receipt'):
                    self.assertIn('Mixed download', result.stderr)

    def test_existing_release_preserves_unrelated_assets(self):
        # Settled retention owns only exact complete commit groups, not operator assets.
        remote = self.remote()
        remote.assets['unexpected'] = b'operator-owned'
        self.invoke_remote(remote)
        self.assertEqual(remote.assets['unexpected'], b'operator-owned')
        self.assertFalse([e for e in remote.events if e['kind'] == 'delete'])

    def test_smoke_uses_unpacked_paths_clean_environment_and_sql(self):
        calls = []
        def command(args, **kwargs):
            calls.append((args, kwargs))
            self.assertNotIn('GH_TOKEN', kwargs['env'])
            if args[0] == 'bash':
                self.assertTrue(args[1].endswith('installed-release-durable-sync-smoke.sh'))
                for key in ('CONTEXTDB_CLI', 'CONTEXTDB_SERVER'):
                    self.assertTrue(Path(kwargs['env'][key]).is_file())
                    self.assertNotIn('target/', kwargs['env'][key])
                self.assertIn('CONTEXTDB_SMOKE_DRIVER', kwargs['env'])
                return 'PASS synthetic full smoke'
            self.assertTrue(Path(args[0]).is_file())
            if args[1] == ':memory:':
                self.assertEqual(kwargs['input'], 'SELECT 1 + 1;\n')
                return '{"result":{"rows":[{"expr":2}]}}'
            return Path(args[0]).name + ' synthetic help/version'
        with patch.dict(os.environ, {'GH_TOKEN': 'must-not-inherit'}), \
             patch.object(c, 'command', side_effect=command):
            c.smoke(self.root / (c.BASE + '.tar.gz'))
        self.assertEqual(len(calls), 6)
        with patch.object(c, 'command', return_value='wrong output'):
            with self.assertRaises(ValueError):
                c.smoke(self.root / (c.BASE + '.tar.gz'))


class PackageBuildControls(unittest.TestCase):
    def test_payload_first_separate_verifier_and_every_failure_blocks_package(self):
        helper = str(Path('scripts/ci-build-package.sh').resolve())
        stub = r"""
git() { printf '%s\n' "$BUILD_SHA"; }
uname() { if [[ "$1" == -s ]]; then echo Linux; else echo x86_64; fi; }
cargo() {
  printf '%s\n' "$*" >> "$CONTROL_LOG"
  local count
  count=$(wc -l < "$CONTROL_LOG")
  [[ "$FAIL_BUILD" != "$count" ]] || return 29
  if [[ "$count" == 1 ]]; then
    mkdir -p target/x86_64-unknown-linux-gnu/release
    printf published-cli > target/x86_64-unknown-linux-gnu/release/contextdb
    printf published-server > target/x86_64-unknown-linux-gnu/release/contextdb-server
  else
    mkdir -p target/development-smoke/x86_64-unknown-linux-gnu/release
    printf verifier > target/development-smoke/x86_64-unknown-linux-gnu/release/contextdb-smoke-driver
  fi
}
python3() {
  printf '%s\n' "$*" >> "$PACKAGE_LOG"
  [[ "$*" == 'scripts/ci-build-control.py package' ]] || return 31
  [[ $(cat target/x86_64-unknown-linux-gnu/release/contextdb) == published-cli ]] || return 32
  [[ $(cat target/x86_64-unknown-linux-gnu/release/contextdb-server) == published-server ]] || return 33
  [[ $(cat "$CONTEXTDB_SMOKE_DRIVER") == verifier ]] || return 34
  [[ "$FAIL_BUILD" != package ]] || return 35
}
"""
        for failure in ('none', '1', '2', 'package'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / 'stub').write_text(stub)
                env = dict(os.environ, BUILD_SHA=SHA, BASH_ENV=str(root / 'stub'),
                           CONTROL_LOG=str(root / 'build.log'), PACKAGE_LOG=str(root / 'package.log'),
                           FAIL_BUILD=failure)
                result = subprocess.run(['bash', helper], cwd=root, env=env,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, failure == 'none', result.stderr)
                builds = (root / 'build.log').read_text().splitlines()
                self.assertEqual(len(builds), 1 if failure == '1' else 2)
                self.assertIn('-p contextdb-cli --bin contextdb -p contextdb-server --bin contextdb-server', builds[0])
                self.assertNotIn('--features', builds[0])
                self.assertNotIn('--target-dir', builds[0])
                if len(builds) == 2:
                    self.assertIn('--target-dir target/development-smoke', builds[1])
                    self.assertIn('--features production-smoke-driver --bin contextdb-smoke-driver', builds[1])
                self.assertEqual((root / 'package.log').exists(), failure not in ('1', '2'))

    def test_full_smoke_failure_propagates(self):
        fixture = AssetControls('test_valid_synthetic_assets')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        def command(args, **kwargs):
            if args[0] == 'bash':
                raise subprocess.CalledProcessError(1, args)
            if ':memory:' in args:
                return '{"result":{"rows":[{"expr":2}]}}'
            return Path(args[0]).name + ' synthetic'
        with patch.object(c, 'command', side_effect=command):
            with self.assertRaises(subprocess.SubprocessError):
                c.smoke(fixture.root / (c.BASE + '.tar.gz'))


class WorkflowControls(unittest.TestCase):
    def test_actual_workflow_uses_call_and_dispatch_only(self):
        import yaml
        doc = yaml.load(Path('.github/workflows/build.yml').read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(doc['on']), {'workflow_call', 'workflow_dispatch'})
        self.assertIn('github.sha', doc['env']['BUILD_SHA'])
        self.assertNotIn('workflow_run', doc['env']['BUILD_SHA'])
        self.assertEqual(doc['env']['WORKFLOW_SHA'], '${{ github.workflow_sha }}')

    def test_actual_workflow_publisher_alone_has_write_permissions(self):
        import yaml
        doc = yaml.load(Path('.github/workflows/build.yml').read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(doc['permissions'], {'contents': 'read', 'actions': 'read'})
        for name, job in doc['jobs'].items():
            expected = {'contents': 'read'}
            if name == 'qualify':
                expected['actions'] = 'read'
            if name == 'publish':
                expected = {'contents': 'write', 'actions': 'read'}
            self.assertEqual(job['permissions'], expected, name)

    def load_workflow(self):
        try:
            import yaml
        except ImportError:
            self.skipTest('PyYAML unavailable; actionlint schema check still required')
        # BaseLoader retains on; booleans need normal resolution in job fields.
        loader = type('Loader', (yaml.SafeLoader,), {})
        loader.yaml_implicit_resolvers = {k: [(tag, pat) for tag, pat in v if tag != 'tag:yaml.org,2002:bool']
                                           for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()}
        loader.add_implicit_resolver('tag:yaml.org,2002:bool', c.re.compile(r'^(?:true|false)$'), list('tf'))
        return yaml.load(Path('.github/workflows/build.yml').read_text(), Loader=loader)

    def assert_publisher_permissions_rejected(self, permissions):
        doc = self.load_workflow()
        workflow_guard(doc)
        doc['jobs']['publish']['permissions'] = permissions
        with self.assertRaisesRegex(ValueError, 'publish permissions'):
            workflow_guard(doc)

    def test_publisher_rejects_id_token_write(self):
        self.assert_publisher_permissions_rejected({
            'contents': 'write', 'actions': 'read', 'id-token': 'write'})

    def test_publisher_rejects_attestations_write(self):
        self.assert_publisher_permissions_rejected({
            'contents': 'write', 'actions': 'read', 'attestations': 'write'})

    def test_publisher_rejects_missing_actions_read(self):
        self.assert_publisher_permissions_rejected({'contents': 'write'})

    def test_workflow_and_independent_mutations(self):
        doc = self.load_workflow()
        workflow_guard(doc)
        mutations = [lambda d: d['on'].update(workflow_run={'workflows': ['CI'], 'types': ['completed']}),
                     lambda d: d['on'].update(push={}),
                     lambda d: d['on'].update(pull_request={}),
                     lambda d: d['on'].update(pull_request_target={}),
                     lambda d: d['on'].update(schedule=[]),
                     lambda d: d['jobs']['publish'].update(needs=['qualify']),
                     lambda d: d['jobs']['publish'].update(needs=['build']),
                     lambda d: d['jobs']['build'].update(needs=[]),
                     lambda d: d['jobs']['publish'].update(permissions={'contents': 'read'}),
                     lambda d: d['jobs']['build'].update(permissions={'contents': 'write'}),
                     lambda d: d['permissions'].update(contents='write'),
                     lambda d: d.update(concurrency={'group': 'contextdb-development-delivery', 'cancel-in-progress': False}),
                     lambda d: d['jobs']['qualify'].update(concurrency={'group': 'contextdb-development-delivery'}),
                     lambda d: d['jobs']['build'].update(concurrency={'group': 'contextdb-development-delivery'}),
                     lambda d: d['jobs']['publish'].pop('concurrency'),
                     lambda d: d['jobs']['publish']['concurrency'].update({'cancel-in-progress': True}),
                     lambda d: d['jobs']['publish']['concurrency'].update(group='other'),
                     lambda d: d['jobs']['publish'].update({'if': '${{ always() }}'}),
                     lambda d: d['jobs']['publish']['steps'][0]['with'].update(ref='dev'),
                     lambda d: d['jobs']['build']['steps'][0]['with'].update({'persist-credentials': True}),
                     lambda d: d['jobs']['build']['steps'][0].update(uses='actions/checkout@v4')]
        for index, mutate in enumerate(mutations):
            with self.subTest(mutation=index):
                bad = copy.deepcopy(doc)
                mutate(bad)
                with self.assertRaises(ValueError):
                    workflow_guard(bad)


def load_tests(loader, tests, pattern):
    """Include settled-channel RED controls in the existing required entrypoint."""
    spec = importlib.util.spec_from_file_location(
        'publisher_contracts', Path(__file__).with_name('ci-build-publisher-tests.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.bind(c)
    tests.addTests(loader.loadTestsFromModule(module))
    final_spec = importlib.util.spec_from_file_location(
        'final_delivery_contracts', Path(__file__).with_name('ci-build-final-controls-tests.py'))
    final_module = importlib.util.module_from_spec(final_spec)
    final_spec.loader.exec_module(final_module)
    final_module.bind(c, module)
    tests.addTests(loader.loadTestsFromModule(final_module))
    metadata_spec = importlib.util.spec_from_file_location(
        'publisher_metadata_controls', Path(__file__).with_name('ci-publisher-metadata-tests.py'))
    metadata_module = importlib.util.module_from_spec(metadata_spec)
    metadata_spec.loader.exec_module(metadata_module)
    tests.addTests(loader.loadTestsFromModule(metadata_module))
    execution_spec = importlib.util.spec_from_file_location(
        'execution_contract_controls', Path(__file__).with_name('ci-execution-contract-tests.py'))
    execution_module = importlib.util.module_from_spec(execution_spec)
    execution_spec.loader.exec_module(execution_module)
    execution_module.bind(sys.modules[__name__])
    tests.addTests(loader.loadTestsFromModule(execution_module))
    return tests


if __name__ == '__main__':
    unittest.main(verbosity=2)
