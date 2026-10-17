#!/usr/bin/env python3
"""Run one unchanged reader authentication case on two immutable revisions."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET

BASE = '6a0d4b42222f3f3063e74d16d780a3e94a87163e'
PATCH_SHA = 'd3504372be4722ebe6d386480a987aee68385351e3dc4ce5dc7cadf67756d35e'
CASE = 'persistence::macos_reader_registry_tests::macos_reader_registry_authenticates_process_and_descriptor'
FILTER = 'test(=' + CASE + ')'


def selected(data):
    cases = [(suite, name) for suite, value in data['rust-suites'].items()
             for name, case in value['testcases'].items()
             if case['filter-match']['status'] == 'matches']
    if len(cases) != 1 or cases[0][1] != CASE:
        raise RuntimeError('exactly one mapped case must be selected')


def outcome(xml, code, baseline):
    cases = ET.fromstring(xml).findall('.//testcase')
    if len(cases) != 1 or cases[0].get('name') != CASE:
        raise RuntimeError('JUnit must contain exactly the mapped case')
    case = cases[0]
    if case.find('skipped') is not None or case.find('error') is not None:
        raise RuntimeError('skipped/tooling/error result is not proof')
    failure = case.find('failure')
    if baseline:
        text = ET.tostring(case, encoding='unicode')
        diagnostic = ('ordinary self pin: ordinary owner pin admits the live '
                      'registration into a complete registry:')
        if code != 100 or failure is None or diagnostic not in text or 'authenticated reader snapshot registry is indeterminate' not in text:
            raise RuntimeError('baseline did not fail at the known admission assertion')
    elif code != 0 or failure is not None:
        raise RuntimeError('fixed case must pass all assertions')


PUBLIC_URL = 'https://github.com/context-graph-ai/contextdb.git'
BRANCH = 'refs/heads/ci-macos-reader-auth-proof'


def candidate(config, verifier):
    if set(config) != {'repository_url', 'candidate_sha'} or config['repository_url'] != PUBLIC_URL:
        raise RuntimeError('only the explicit public engine repository is accepted')
    sha = config['candidate_sha']
    if not isinstance(sha, str) or not re.fullmatch('[0-9a-f]{40}', sha):
        raise RuntimeError('reviewed public candidate SHA is required; no event-head default')
    if sha in (verifier, BASE):
        raise RuntimeError('candidate must differ from verifier and baseline')
    return sha


def main():
    root = Path(__file__).resolve().parents[1]
    out = root / 'target/macos-reader-proof'
    out.mkdir(parents=True, exist_ok=False)
    ledger = []

    def run(args, cwd=root, name=None, env=None, required=True):
        start = time.time()
        try:
            result = subprocess.run(args, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError as error:
            result = subprocess.CompletedProcess(args, 127, b'', str(error).encode())
        record = dict(command=args, cwd=str(cwd), exit=result.returncode,
                      started_unix=start, elapsed_seconds=time.time()-start)
        ledger.append(record)
        (out / 'commands.json').write_text(json.dumps(ledger, indent=2))
        if name:
            (out / (name+'.stdout')).write_bytes(result.stdout)
            (out / (name+'.stderr')).write_bytes(result.stderr)
        if required and result.returncode:
            raise RuntimeError('command failed: ' + repr(args))
        return result

    verifier = os.environ['VERIFIER_SHA']
    if not re.fullmatch('[0-9a-f]{40}', verifier):
        raise RuntimeError('verifier must be a full SHA')
    if (os.environ.get('GITHUB_EVENT_NAME') != 'push' or
            os.environ.get('GITHUB_REF') != BRANCH or
            os.environ.get('GITHUB_REPOSITORY') != 'context-graph-ai/contextdb' or
            os.environ.get('GITHUB_WORKFLOW_SHA') != verifier):
        raise RuntimeError('trusted fixed-branch push and matching workflow SHA required')
    if run(['git', 'rev-parse', 'HEAD']).stdout.decode().strip() != verifier:
        raise RuntimeError('trusted checkout differs from verifier event')
    if run(['git', 'status', '--porcelain', '--untracked-files=no']).stdout.strip():
        raise RuntimeError('verifier checkout is dirty')
    sha = candidate(json.loads((root/'scripts/macos-reader-proof.json').read_text()), verifier)
    if platform.system() != 'Darwin':
        raise RuntimeError('native macOS execution required')
    patch = root / 'scripts/fixtures/macos-reader-authentication.patch'
    if hashlib.sha256(patch.read_bytes()).hexdigest() != PATCH_SHA:
        raise RuntimeError('reviewed fixture hash mismatch')
    source_files = ['.github/workflows/macos-reader-proof.yml',
                    'scripts/ci-macos-reader-proof.py', 'scripts/macos-reader-proof.json',
                    'scripts/fixtures/macos-reader-authentication.patch', 'README.md']
    metadata = dict(verifier_sha=verifier, workflow_sha=os.environ['GITHUB_WORKFLOW_SHA'],
                    workflow_ref=os.environ.get('GITHUB_WORKFLOW_REF'),
                    event='push', ref=BRANCH, run_id=os.environ.get('GITHUB_RUN_ID'),
                    run_attempt=os.environ.get('GITHUB_RUN_ATTEMPT'),
                    baseline=BASE, fixed=sha, patch_sha256=PATCH_SHA, features='default',
                    target='contextdb-engine --lib', source_repository=PUBLIC_URL,
                    verifier_source_sha256={f: hashlib.sha256((root/f).read_bytes()).hexdigest() for f in source_files})
    for label, cmd in [('macos', ['sw_vers']), ('uname', ['uname', '-a']),
                       ('rustc', ['rustc', '-Vv']), ('cargo', ['cargo', '-V']),
                       ('nextest', ['cargo', 'nextest', '--version'])]:
        metadata[label] = run(cmd, name=label).stdout.decode()
    if not metadata['rustc'].startswith('rustc 1.94.1 ') or '0.9.140' not in metadata['nextest']:
        raise RuntimeError('unexpected toolchain')
    (out / 'identity.json').write_text(json.dumps(metadata, indent=2))
    results = {}
    # Engine objects never supply the verifier, fixture, or oracle. Only Cargo executes engine code.
    with tempfile.TemporaryDirectory(prefix='reader-proof-') as temporary:
        objects = Path(temporary)/'objects.git'
        run(['git', 'init', '--bare', str(objects)], name='source-init')
        metadata['verifier_path'] = str(root)
        metadata['engine_objects_path'] = str(objects)
        metadata['source_paths'] = {label: str(Path(temporary)/label) for label in ['baseline', 'fixed']}
        (out / 'identity.json').write_text(json.dumps(metadata, indent=2))
        for label, revision in [('baseline', BASE), ('fixed', sha)]:
            tree = Path(temporary) / label
            added = False
            try:
                run(['git', '-c', 'credential.helper=', 'fetch', '--no-tags', '--depth=1', PUBLIC_URL, revision],
                    cwd=objects, name=label+'-fetch', env=dict(os.environ, GIT_TERMINAL_PROMPT='0'))
                actual = run(['git', 'rev-parse', 'FETCH_HEAD'], cwd=objects, name=label+'-source').stdout.decode().strip()
                if actual != revision:
                    raise RuntimeError('fetched source differs from immutable revision')
                run(['git', 'rev-parse', revision+'^{tree}'], cwd=objects, name=label+'-tree')
                run(['git', '-c', 'core.hooksPath=/dev/null', 'worktree', 'add', '--detach', str(tree), revision],
                    cwd=objects, name=label+'-checkout')
                added = True
                run(['git', 'apply', '--check', str(patch)], cwd=tree, name=label+'-patch-check')
                run(['git', 'apply', str(patch)], cwd=tree, name=label+'-patch')
                run(['git', 'diff', '--stat'], cwd=tree, name=label+'-diff')
                config = Path(temporary) / (label+'.toml')
                config.write_text('[profile.reader-proof]\nretries = 0\n[profile.reader-proof.junit]\npath = "junit.xml"\nstore-success-output = true\nstore-failure-output = true\n')
                env = dict(os.environ, CARGO_TARGET_DIR=str(Path(temporary)/('target-'+label)))
                common = ['-p', 'contextdb-engine', '--lib', '--locked', '-E', FILTER]
                listing = run(['cargo', 'nextest', 'list', *common, '--message-format', 'json'],
                              cwd=tree, name=label+'-list', env=env)
                selected(json.loads(listing.stdout))
                execution = run(['cargo', 'nextest', 'run', *common, '--profile', 'reader-proof',
                                 '--config-file', str(config), '--no-fail-fast',
                                 '--success-output', 'immediate', '--failure-output', 'immediate'],
                                cwd=tree, name=label+'-run', env=env, required=False)
                junit = Path(env['CARGO_TARGET_DIR']) / 'nextest/reader-proof/junit.xml'
                xml = junit.read_text()
                (out / (label+'-junit.xml')).write_text(xml)
                outcome(xml, execution.returncode, label == 'baseline')
                results[label] = 'expected assertion failure' if label == 'baseline' else 'passed'
            except Exception as error:
                results[label] = 'FAILED: ' + str(error)
            finally:
                if added:
                    cleanup = run(['git', 'worktree', 'remove', '--force', str(tree)], cwd=objects,
                                  name=label+'-cleanup', required=False)
                    if cleanup.returncode:
                        previous = results.get(label)
                        cleanup_error = 'worktree cleanup failed (exit %s)' % cleanup.returncode
                        results[label] = 'FAILED: ' + (previous + '; ' if previous else '') + cleanup_error
                (out / 'results.json').write_text(json.dumps(results, indent=2))
    if any(value.startswith('FAILED:') for value in results.values()) or len(results) != 2:
        raise RuntimeError('both native halves must satisfy their oracle')


if __name__ == '__main__':
    status = Path(__file__).resolve().parents[1]/'target/macos-reader-proof/proof-status.json'
    try:
        main()
    except Exception as error:
        if status.parent.exists():
            status.write_text(json.dumps({'verdict': 'FAIL', 'error': str(error)}, indent=2))
        raise
    else:
        status.write_text(json.dumps({'verdict': 'PASS'}, indent=2))
