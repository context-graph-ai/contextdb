#!/usr/bin/env python3
"""Offline settled-channel contract tests; no network or native compilation.

Run through ci-build-tests.py. The gh double models persistent remote state,
including hidden drafts and interrupted uploads, rather than a helper's API shape.
"""
import copy
import hashlib
import fnmatch
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch


def bind(module):
    global c
    c = module


SHA = 'a' * 40
DEFAULT = 'd' * 40
SUFFIXES = ('.tar.gz', '.tar.gz.sha256', '.json')


class Remote:
    """Authenticated gh boundary: list pages, assets, refs and release operations."""
    def __init__(self, root, releases=None):
        self.root = root
        self.releases = copy.deepcopy(releases or [])
        self.assets = {}
        self.events = []
        self.fail_upload = None
        self.corrupt_upload = False
        self.list_error = False
        self.default_branch = 'main'
        self.tip = SHA
        self.counter = 100
        self.asset_ids = {}
        self.fail_at = None
        self.point = 0
        self.times = {}
        self.download_corrupt = False

    def interrupt(self):
        self.point += 1
        if self.point == self.fail_at:
            raise subprocess.CalledProcessError(1, "injected interruption")

    def record(self, kind, **fields):
        self.events.append(dict(kind=kind, **fields))

    def metadata(self):
        for name in self.assets:
            if name not in self.asset_ids:
                self.counter += 1
                self.asset_ids[name] = self.counter
        return [dict(id=self.asset_ids[name], name=name, size=len(data),
                     digest='sha256:' + c.hashlib.sha256(data).hexdigest(),
                     created_at=self.times.get(name, '2026-10-14T00:00:00Z'))
                for i, (name, data) in enumerate(self.assets.items())]

    def command(self, args, **kwargs):
        if args[:2] != ['gh', 'api'] and args[:2] != ['gh', 'release']:
            raise AssertionError('publisher executed non-control command: ' + repr(args))
        if args[:2] == ['gh', 'api']:
            path = next(a for a in args[2:] if a.startswith(('repos/', '/repos/'))).lstrip('/')
            method = args[args.index('--method') + 1] if '--method' in args else 'GET'
            fields = {}
            for i, arg in enumerate(args):
                if arg in ('-f', '-F', '--field', '--raw-field'):
                    key, value = args[i + 1].split('=', 1)
                    fields[key] = value
            if '/git/refs' in path and method != 'GET':
                self.record('tag-write', method=method, fields=fields)
                return '{}'
            if '/git/ref/' in path or '/commits/' in path:
                branch = path.rsplit('/', 1)[-1]
                return json.dumps({'object': {'sha': self.tip if branch == 'dev' else DEFAULT},
                                   'sha': self.tip if branch == 'dev' else DEFAULT})
            if path == f'repos/{c.REPO}':
                return json.dumps({'default_branch': self.default_branch})
            if re.search(r'/releases\?', path) or path.endswith('/releases'):
                if method == 'GET':
                    if self.list_error:
                        raise subprocess.CalledProcessError(1, args, stderr='HTTP 403')
                    self.record('list', paginate='--paginate' in args, authenticated=True)
                    # The channel is deliberately on the second page; drafts are included.
                    pages = [[dict(id=1, tag_name='v1', prerelease=False)], self.releases]
                    return json.dumps(pages if '--slurp' in args else pages[0])
            if '/assets' in path and method == 'GET':
                self.record('inspect')
                data = self.metadata()
                return json.dumps([data] if '--slurp' in args else data)
            if '/assets/' in path and method == 'DELETE':
                asset_id = int(path.rsplit('/', 1)[-1])
                name = next(a['name'] for a in self.metadata() if a['id'] == asset_id)
                self.record('delete', name=name)
                del self.assets[name]
                self.interrupt()
                return '{}'
            raise AssertionError('unmodeled API request: ' + repr(args))
        operation = args[2]
        if operation == 'create':
            target = args[args.index('--target') + 1]
            self.record('create', target=target, draft='--draft' in args)
            self.interrupt()
            self.releases.append(dict(id=17, tag_name='dev-latest', prerelease=True,
                                      draft='--draft' in args, target_commitish=target))
        elif operation == 'upload':
            paths = [Path(a.split('#', 1)[0]) for a in args[4:] if Path(a.split('#', 1)[0]).is_file()]
            for path in paths:
                name = path.name
                if self.fail_upload and self.fail_upload(name):
                    raise subprocess.CalledProcessError(1, args)
                if name in self.assets:
                    if '--clobber' not in args:
                        raise subprocess.CalledProcessError(1, args)
                    del self.assets[name]
                    self.interrupt()
                self.interrupt()
                self.record('upload', name=name, clobber='--clobber' in args)
                self.assets[name] = path.read_bytes() + (b'corrupt' if self.corrupt_upload else b'')
                self.interrupt()
        elif operation == 'download':
            self.record('verify', names=[args[i + 1] for i, a in enumerate(args) if a in ('-p', '--pattern')])
            dest = Path(args[args.index('--dir') + 1])
            patterns = [args[i + 1] for i, a in enumerate(args) if a in ('-p', '--pattern')]
            for name, data in self.assets.items():
                if not patterns or any(fnmatch.fnmatch(name, p) for p in patterns):
                    (dest / name).write_bytes(data + (b'corrupt-download' if self.download_corrupt else b''))
        elif operation == 'edit':
            if '--target' in args:
                raise ValueError('release edit must preserve the channel tag target')
            self.interrupt()
            self.record('edit', args=args)
            if '--draft=false' in args:
                self.releases[0]['draft'] = False
        elif operation == 'delete-asset':
            self.record('delete', name=args[4])
            self.assets.pop(args[4], None)
            self.interrupt()
        else:
            raise AssertionError('unmodeled release operation: ' + repr(args))
        return ''


class SettledPublisherControls(unittest.TestCase):
    def setUp(self):
        # Reuse the existing archive/receipt fixture, without inheriting its tests.
        spec = importlib.util.spec_from_file_location('old_delivery_fixture', Path(__file__).with_name('ci-build-tests.py'))
        old = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(old)
        self.fixture = old.AssetControls('test_valid_synthetic_assets')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.remote = Remote(self.root)

    def invoke(self, run_id='9'):
        env = dict(BUILD_SHA=SHA, BUILD_ASSETS=str(self.root), GITHUB_RUN_ID=run_id,
                   GITHUB_RUN_ATTEMPT=self.fixture.receipt['run_attempt'], GITHUB_REPOSITORY=c.REPO, GH_TOKEN='offline-token')
        with patch.dict(os.environ, env), patch.object(c, 'guard'), \
             patch.object(c, 'identity', return_value={'lock_sha256': 'synthetic'}), \
             patch.object(c, 'command', side_effect=self.remote.command), \
             patch.object(c.tarfile.TarFile, 'extract', side_effect=AssertionError('publisher unpacks artifact')), \
             patch.object(c.tarfile.TarFile, 'extractall', side_effect=AssertionError('publisher unpacks artifact')), \
             patch.object(c, 'smoke', side_effect=AssertionError('publisher executes artifact')):
            c.publish()

    def existing(self, draft=False):
        self.remote.releases = [dict(id=17, tag_name='dev-latest', prerelease=True,
                                     draft=draft, immutable=False, target_commitish=DEFAULT)]

    def test_bootstrap_uses_current_default_head_main_and_dev(self):
        for branch, expected in [('main', DEFAULT), ('dev', SHA)]:
            with self.subTest(branch=branch):
                self.remote = Remote(self.root)
                self.remote.default_branch = branch
                self.invoke()
                created = [e for e in self.remote.events if e['kind'] == 'create']
                self.assertEqual(len(created), 1)
                self.assertEqual(created[0]['target'], expected)
                self.assertTrue(created[0]['draft'])

    def test_channel_tag_is_never_moved_and_receipt_keeps_built_source(self):
        self.existing()
        self.invoke()
        self.assertFalse([e for e in self.remote.events if e['kind'] == 'tag-write'])
        receipt = json.loads(self.remote.assets[c.BASE + '.json'])
        self.assertEqual(receipt['source_sha'], SHA)
        self.assertNotEqual(receipt['source_sha'], DEFAULT)

    def test_authenticated_paginated_listing_finds_hidden_draft_without_recreate(self):
        self.existing(draft=True)
        self.invoke()
        listings = [e for e in self.remote.events if e['kind'] == 'list']
        self.assertTrue(listings and all(e['paginate'] and e['authenticated'] for e in listings))
        self.assertFalse([e for e in self.remote.events if e['kind'] == 'create'])

    def test_duplicate_channel_and_listing_auth_failure_fail_closed(self):
        for failure in ('duplicates', 'auth'):
            with self.subTest(failure=failure):
                self.remote = Remote(self.root)
                self.existing(draft=True)
                if failure == 'duplicates':
                    self.remote.releases.append(dict(self.remote.releases[0], id=18))
                else:
                    self.remote.list_error = True
                with self.assertRaises((ValueError, subprocess.SubprocessError)):
                    self.invoke()
                self.assertFalse([e for e in self.remote.events if e['kind'] in ('create', 'upload', 'edit', 'delete', 'tag-write')])

    def test_commit_group_uploaded_and_verified_before_stable_receipt_last(self):
        self.invoke()
        events = self.remote.events
        stable = [e['name'] for e in events if e['kind'] == 'upload' and e['name'] in c.ASSETS]
        self.assertEqual(stable, [c.BASE + s for s in SUFFIXES])
        start = next(i for i, e in enumerate(events) if e['kind'] == 'upload' and e['name'] in c.ASSETS)
        prefix = events[:start]
        group = [e['name'] for e in prefix if e['kind'] == 'upload']
        self.assertEqual(len(group), 3, 'commit copies must precede stable uploads')
        self.assertTrue(all(SHA[:12] + '-r9a1' in name for name in group))
        self.assertTrue(any(e['kind'] == 'verify' for e in prefix[prefix.index(next(e for e in prefix if e['kind'] == 'upload')) + 1:]),
                        'uploaded commit copies must be remotely digest-verified first')
        edit = next(i for i, e in enumerate(events) if e['kind'] == 'edit')
        self.assertGreater(edit, max(i for i, e in enumerate(events) if e['kind'] == 'upload'))
        self.assertFalse(any(e['kind'] in ('upload', 'edit', 'tag-write') for e in events[edit + 1:]))
        notes = ' '.join(events[edit]['args'])
        self.assertIn(SHA, notes)
        self.assertTrue(all(name in notes for name in group), 'notes must list commit-copy download names')

    def test_new_run_id_with_same_sha_and_attempt_preserves_old_group(self):
        self.invoke()
        before = {self.group(1) + s: self.remote.assets[self.group(1) + s] for s in SUFFIXES}
        self.fixture.receipt['run_id'] = '10'
        self.fixture.write_receipt()
        self.remote.events.clear()
        self.invoke(run_id='10')
        new_group = c.BASE + '-' + SHA[:12] + '-r10a1'
        self.assertTrue(self.consistent(new_group))
        self.assertTrue(all(self.remote.assets[n] == data for n, data in before.items()))
        self.assertFalse(any(e['kind'] == 'delete' or
                             (e['kind'] == 'upload' and e['name'] not in c.ASSETS and e['clobber'])
                             for e in self.remote.events))
        first_stable = next(i for i, e in enumerate(self.remote.events)
                            if e['kind'] == 'upload' and e['name'] in c.ASSETS)
        downloaded = {n for e in self.remote.events[:first_stable] if e['kind'] == 'verify'
                      for n in e['names']}
        self.assertTrue({new_group + s for s in SUFFIXES} <= downloaded)

    def test_downloaded_bytes_not_metadata_authorize_stable_mutation(self):
        self.existing()
        self.remote.download_corrupt = True
        with self.assertRaises(ValueError):
            self.invoke()
        self.assertTrue(any(e['kind'] == 'verify' for e in self.remote.events))
        self.assertFalse(any(e['kind'] in ('delete', 'edit') or
                             (e['kind'] == 'upload' and e['name'] in c.ASSETS)
                             for e in self.remote.events))

    def test_fresh_run_interruptions_preserve_previous_stable_or_new_fallback(self):
        self.invoke()
        previous = copy.deepcopy(self.remote)
        old_stable = {n: self.remote.assets[n] for n in c.ASSETS}
        self.attempt(2)
        self.remote = copy.deepcopy(previous)
        self.remote.point = 0
        self.invoke()
        points = self.remote.point
        self.assertGreater(points, 6)
        for point in range(1, points + 1):
            with self.subTest(point=point):
                self.remote = copy.deepcopy(previous)
                self.remote.events.clear()
                self.remote.point = 0
                self.remote.fail_at = point
                with self.assertRaises(subprocess.SubprocessError):
                    self.invoke()
                untouched = all(self.remote.assets.get(n) == b for n, b in old_stable.items())
                self.assertTrue(untouched or self.consistent(self.group(2)))
                mutations = [e for e in self.remote.events if e['kind'] in ('upload', 'delete')]
                for event in mutations:
                    if event['name'] not in c.ASSETS:
                        self.assertEqual(event['kind'], 'upload')
                        self.assertFalse(event['clobber'])

    def test_identical_complete_group_also_refuses_collision_without_mutation(self):
        self.invoke()
        before = dict(self.remote.assets)
        self.remote.events.clear()
        with self.assertRaises(ValueError):
            self.invoke()
        self.assertEqual(before, self.remote.assets)
        self.assertFalse(any(e['kind'] in ('upload', 'delete', 'edit', 'create', 'tag-write')
                             for e in self.remote.events))

    def test_corrupt_commit_upload_never_touches_stable_names(self):
        self.remote.corrupt_upload = True
        with self.assertRaises(ValueError):
            self.invoke()
        self.assertFalse([e for e in self.remote.events if e['kind'] == 'upload' and e['name'] in c.ASSETS])
        self.assertFalse([e for e in self.remote.events if e['kind'] in ('edit', 'delete')])

    def test_hidden_first_draft_survives_partial_retry(self):
        self.remote.fail_upload = lambda name: name.endswith('.json')
        with self.assertRaises(subprocess.SubprocessError):
            self.invoke()
        self.assertTrue(self.remote.releases[0]['draft'])
        self.assertFalse([e for e in self.remote.events if e['kind'] == 'edit'])
        partial = dict(self.remote.assets)
        self.assertTrue(partial)
        self.assertTrue(all(SHA[:12] in name for name in partial), 'first hidden upload must start with commit copies')
        self.remote.fail_upload = None
        self.fixture.receipt['run_attempt'] = '2'
        self.fixture.write_receipt()
        self.invoke()
        self.assertEqual(len([e for e in self.remote.events if e['kind'] == 'create']), 1)
        self.assertFalse(self.remote.releases[0]['draft'])
        self.assertTrue(all(name in self.remote.assets for name in partial), 'retry must preserve partial group identity')

    def group(self, attempt):
        return c.BASE + '-' + SHA[:12] + '-r9a' + str(attempt)

    def consistent(self, prefix):
        try:
            archive, checksum, receipt = [self.remote.assets[prefix + s] for s in SUFFIXES]
            digest = c.hashlib.sha256(archive).hexdigest()
            return (checksum == f'{digest}  {c.BASE}.tar.gz\n'.encode()
                    and json.loads(receipt)['sha256'] == digest)
        except (KeyError, ValueError):
            return False

    def attempt(self, n):
        self.fixture.receipt['run_attempt'] = str(n)
        self.fixture.write_receipt()

    def test_collision_refuses_without_any_mutation_even_partial_or_corrupt(self):
        for suffix in SUFFIXES:
            with self.subTest(suffix=suffix):
                self.remote = Remote(self.root)
                self.existing()
                self.remote.assets[self.group(1) + suffix] = b'preexisting'
                before = dict(self.remote.assets)
                with self.assertRaises(ValueError):
                    self.invoke()
                self.assertEqual(self.remote.assets, before)
                self.assertFalse([e for e in self.remote.events if e['kind'] in ('upload', 'edit', 'delete')])

    def test_same_source_receipt_only_retry_interrupted_at_every_mutation(self):
        self.invoke()
        original = copy.deepcopy(self.remote)
        archive = (self.root / (c.BASE + '.tar.gz')).read_bytes()
        self.attempt(2)
        self.assertEqual(archive, (self.root / (c.BASE + '.tar.gz')).read_bytes())
        # Reuse the audited scenario: stable is already mixed, fallback is the only good set.
        original.assets[c.BASE + '.json'] = json.dumps(dict(self.fixture.receipt, sha256='0'*64)).encode()
        self.remote = copy.deepcopy(original)
        self.remote.point = 0
        self.invoke()
        points = self.remote.point
        self.assertGreaterEqual(points, 10)
        for point in range(1, points + 1):
            with self.subTest(point=point):
                self.remote = copy.deepcopy(original)
                before = {n: d for n, d in self.remote.assets.items() if n.startswith(self.group(1))}
                self.remote.point = 0
                self.remote.fail_at = point
                with self.assertRaises(subprocess.SubprocessError):
                    self.invoke()
                self.assertTrue(self.consistent(c.BASE) or self.consistent(self.group(1)) or self.consistent(self.group(2)))
                self.assertEqual(before, {n: d for n, d in self.remote.assets.items() if n.startswith(self.group(1))})
                self.assertFalse([e for e in self.remote.events if e['kind'] == 'upload'
                                  and e['name'] not in c.ASSETS and e['clobber']])

    def test_retention_protects_current_receipt_even_when_older_than_three(self):
        self.invoke()
        old = {s: self.remote.assets[self.group(1) + s] for s in SUFFIXES}
        for n in range(10, 14):
            for suffix, data in old.items():
                name = self.group(n) + suffix
                self.remote.assets[name] = json.dumps(dict(self.fixture.receipt, run_attempt=str(n))).encode() if suffix == '.json' else data
                self.remote.times[name] = f'2026-11-{n}T00:00:00Z'
        incomplete = self.group(0) + '.tar.gz'
        self.remote.assets[incomplete] = b'incomplete'
        self.remote.assets['operator-notes.txt'] = b'operator'
        self.attempt(2)
        self.remote.events.clear()
        self.invoke()
        self.assertTrue(self.consistent(self.group(2)), 'current stable group must survive even if its creation time is old')
        for n in (11, 12, 13):
            self.assertTrue(self.consistent(self.group(n)))
        self.assertNotIn(self.group(10) + '.json', self.remote.assets)
        self.assertIn(incomplete, self.remote.assets)
        self.assertIn('operator-notes.txt', self.remote.assets)
        events = self.remote.events
        self.assertGreater(next(i for i,e in enumerate(events) if e['kind']=='delete'),
                           next(i for i,e in enumerate(events) if e['kind']=='edit'))
        # Replay every destructive/pruning point too; current stable backup always survives.
        final = copy.deepcopy(self.remote)
        self.attempt(3)
        self.remote = copy.deepcopy(final)
        self.remote.point = 0
        self.invoke()
        for point in range(1, self.remote.point + 1):
            with self.subTest(retention_point=point):
                self.remote = copy.deepcopy(final)
                self.remote.point = 0
                self.remote.fail_at = point
                with self.assertRaises(subprocess.SubprocessError):
                    self.invoke()
                self.assertTrue(self.consistent(c.BASE) or self.consistent(self.group(2)) or self.consistent(self.group(3)))


def queue_guard(doc):
    """Exact placement, value and unknown-key controls for the settled queue."""
    expected = {'group': 'contextdb-dev-binaries-publish', 'cancel-in-progress': False, 'queue': 'max'}
    if doc['jobs']['publish'].get('concurrency') != expected:
        raise ValueError('publisher needs exact max queue and cancellation disabled')
    def walk(value, path=()):
        if isinstance(value, dict):
            for key, child in value.items():
                if key == 'queue' and path != ('jobs', 'publish', 'concurrency'):
                    raise ValueError('misplaced queue')
                walk(child, path + (key,))
        elif isinstance(value, list):
            for child in value:
                walk(child, path)
    walk(doc)


class SettledWorkflowControls(unittest.TestCase):
    def doc(self):
        # The established workflow loader preserves YAML on and boolean types.
        spec = importlib.util.spec_from_file_location('delivery_workflow_fixture', Path(__file__).with_name('ci-build-tests.py'))
        old = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(old)
        return old.WorkflowControls().load_workflow()

    def test_actual_publish_queue_exact_placement(self):
        try:
            queue_guard(self.doc())
        except ValueError as error:
            self.fail(str(error))

    def test_queue_negative_mutations(self):
        doc = self.doc()
        doc['jobs']['publish']['concurrency'] = {'group': 'contextdb-dev-binaries-publish', 'cancel-in-progress': False, 'queue': 'max'}
        queue_guard(doc)
        mutations = [lambda d: d['jobs']['publish']['concurrency'].pop('queue'),
                     lambda d: d['jobs']['publish']['concurrency'].update(queue='single'),
                     lambda d: d['jobs']['publish']['concurrency'].update({'cancel-in-progress': True}),
                     lambda d: d['jobs']['publish']['concurrency'].update(typo='max'),
                     lambda d: d.update(queue='max'),
                     lambda d: d.update(concurrency={'queue': 'max'}),
                     lambda d: d['jobs']['publish'].update(queue='max'),
                     lambda d: d['jobs']['build'].update(queue='max'),
                     lambda d: d['jobs']['build'].update(concurrency={'queue': 'max'})]
        for i, mutate in enumerate(mutations):
            with self.subTest(mutation=i):
                bad = copy.deepcopy(doc)
                mutate(bad)
                with self.assertRaises(ValueError):
                    queue_guard(bad)

    def test_publisher_executes_only_trusted_source_control(self):
        doc = self.doc()
        publisher_execution_guard(doc)
        for run in ('bash "$BUILD_ASSETS/publish.sh"', 'python3 "$BUILD_ASSETS/ci-build-control.py" publish',
                    'tar -xzf "$BUILD_ASSETS/package.tar.gz"; ./contextdb --version',
                    'python3 -c "import subprocess; subprocess.run([\"$BUILD_ASSETS/contextdb\"])"',
                    "python3 <<'PYTHON'\nimport subprocess\nsubprocess.run(['artifact/contextdb'])\nPYTHON",
                    'sh -c "$BUILD_ASSETS/contextdb"', 'node "$BUILD_ASSETS/publish.js"',
                    'perl "$BUILD_ASSETS/publish.pl"', 'exec "$BUILD_ASSETS/contextdb"',
                    'source "$BUILD_ASSETS/publish.sh"', '. "$BUILD_ASSETS/publish.sh"'):
            with self.subTest(run=run):
                bad = copy.deepcopy(doc)
                bad['jobs']['publish']['steps'].append({'run': run})
                with self.assertRaises(ValueError):
                    publisher_execution_guard(bad)
        bad = copy.deepcopy(doc)
        bad['jobs']['publish']['steps'] = [s for s in bad['jobs']['publish']['steps'] if 'run' not in s]
        with self.assertRaises(ValueError):
            publisher_execution_guard(bad)

    def test_build_workflow_requires_packaging_before_artifact_upload(self):
        doc = self.doc()
        build_execution_guard(doc)
        mutations = [lambda d: d['jobs']['build'].update(steps=[s for s in d['jobs']['build']['steps'] if s.get('run') != 'bash scripts/ci-build-package.sh']),
                     lambda d: d['jobs']['build']['steps'].reverse(),
                     lambda d: next(s for s in d['jobs']['build']['steps'] if s.get('run') == 'bash scripts/ci-build-package.sh').update(run='echo skipped package smoke')]
        for i, mutate in enumerate(mutations):
            with self.subTest(mutation=i):
                bad = copy.deepcopy(doc)
                mutate(bad)
                with self.assertRaises(ValueError):
                    build_execution_guard(bad)

    def test_build_rejects_disabled_or_failure_masked_smoke(self):
        doc = self.doc()
        build_execution_guard(doc)
        for run in ('if false; then bash scripts/ci-build-package.sh; fi',
                    'bash scripts/ci-build-package.sh || true'):
            with self.subTest(run=run):
                bad = copy.deepcopy(doc)
                step = next(s for s in bad['jobs']['build']['steps']
                            if s.get('run') == 'bash scripts/ci-build-package.sh')
                step['run'] = run
                with self.assertRaises(ValueError):
                    build_execution_guard(bad)

    def test_package_must_run_smoke_and_abort_before_receipt_on_failure(self):
        # Execute the packaging path with synthetic native files, never Cargo.
        for failure in (False, True):
            with self.subTest(smoke_failure=failure), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                bins = root / 'target' / c.TARGET / 'release'
                bins.mkdir(parents=True)
                for binary in c.BINS:
                    (bins / binary).write_bytes(b'synthetic')
                (root / 'LICENSE').write_text('synthetic')
                seen = []
                def smoke(archive):
                    self.assertEqual(Path(archive), root / 'out' / (c.BASE + '.tar.gz'))
                    self.assertTrue(Path(archive).is_file())
                    seen.append(str(archive))
                    if failure:
                        raise ValueError('full smoke failed')
                    return {b: 'synthetic' for b in c.BINS}
                def command(args, **kw):
                    if args[:2] == ['git', 'rev-parse']:
                        return SHA
                    if args[:2] == ['git', 'status']:
                        return ''
                    if args[0] in ('cargo', 'rustc'):
                        return 'synthetic version only'
                    raise AssertionError(args)
                previous = Path.cwd()
                try:
                    os.chdir(root)
                    with patch.dict(os.environ, dict(BUILD_SHA=SHA, BUILD_ASSETS=str(root / 'out'), GITHUB_RUN_ID='9', GITHUB_RUN_ATTEMPT='1')), \
                         patch.object(c, 'command', side_effect=command), patch.object(c, 'smoke', side_effect=smoke), \
                         patch.object(c, 'identity', return_value={}):
                        if failure:
                            with self.assertRaises(ValueError):
                                c.package()
                        else:
                            c.package()
                    self.assertEqual(len(seen), 1)
                    self.assertEqual((root / 'out' / (c.BASE + '.json')).exists(), not failure)
                finally:
                    os.chdir(previous)

    def test_full_smoke_runs_on_exact_unpacked_payload(self):
        fixture = SettledPublisherControls()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        seen = []
        def command(args, **kw):
            if any('installed-release-durable-sync-smoke.sh' in str(a) for a in args):
                env = kw.get('env', {})
                for key in ('CONTEXTDB_CLI', 'CONTEXTDB_SERVER'):
                    self.assertTrue(Path(env[key]).is_file())
                    self.assertEqual(Path(env[key]).read_bytes(), b'test')
                    self.assertNotIn('target/', env[key])
                self.assertEqual(Path(env['CONTEXTDB_CLI']).parent, Path(env['CONTEXTDB_SERVER']).parent)
                seen.append(args)
                return 'PASS synthetic full smoke'
            if ':memory:' in args:
                return '{"result":{"rows":[{"expr":2}]}}'
            return Path(args[0]).name + ' synthetic'
        with patch.object(c, 'command', side_effect=command):
            c.smoke(fixture.root / (c.BASE + '.tar.gz'))
        self.assertEqual(len(seen), 1, 'version/help/SELECT alone are not the full packaged smoke')


def publisher_execution_guard(doc):
    # Pin the complete reviewed publisher and inherited execution metadata.
    # Any metadata change requires an explicit baseline review.
    reviewed = {'publisher': doc['jobs']['publish'],
                **{key: doc[key] for key in ('defaults', 'env') if key in doc}}
    digest = hashlib.sha256(json.dumps(
        reviewed, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    if digest != 'a5790d3f581ff23bf8d915df91074035af56ea64f6eebced4076c3e3964f2247':
        raise ValueError('publisher execution metadata differs from reviewed baseline')
    steps = doc['jobs']['publish']['steps']
    runs = [s['run'].strip() for s in steps if 'run' in s]
    if runs != ['python3 scripts/ci-build-control.py publish']:
        raise ValueError('publisher must execute source-bound control only')
    if any(s.get('uses', '').startswith(('./', 'docker://')) for s in steps):
        raise ValueError('publisher must not execute an artifact-selected action')
    checkout = [s for s in steps if s.get('uses', '').startswith('actions/checkout@')]
    if len(checkout) != 1 or checkout[0]['with']['ref'] != '${{ github.workflow_sha }}':
        raise ValueError('control must come from source-bound reviewed workflow')


def build_execution_guard(doc):
    # Pin the entire reviewed workflow, including inherited execution metadata.
    digest = hashlib.sha256(json.dumps(
        doc, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    if digest != 'af7e514fc9a0eb031e6c77970bf5927f90b408307e80f767cf6651934602c33f':
        raise ValueError('build execution metadata differs from reviewed baseline')
    job = doc['jobs']['build']
    if 'continue-on-error' in job:
        raise ValueError('build job must propagate smoke failures')
    steps = job['steps']
    packages = [i for i, s in enumerate(steps) if s.get('run') == 'bash scripts/ci-build-package.sh']
    uploads = [i for i, s in enumerate(steps) if s.get('uses', '').startswith('actions/upload-artifact@')]
    if len(packages) != 1 or len(uploads) != 1 or packages[0] >= uploads[0]:
        raise ValueError('build must package and smoke before artifact upload')
    if any(key in steps[packages[0]] for key in ('if', 'continue-on-error')):
        raise ValueError('package smoke must run and propagate failures')
