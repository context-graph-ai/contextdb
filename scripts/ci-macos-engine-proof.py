#!/usr/bin/env python3
"""Qualify the fixed public engine revision running 5 integration cases and binding 3 carried library passes."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import time

PUBLIC = 'https://github.com/context-graph-ai/contextdb.git'
BRANCH = 'refs/heads/ci-macos-reader-auth-proof'
BASE = 'bc9cce955eb85fb1488f5304426eedeb6d97e905'
PARENT_H = '897eff2849d0e800ca3a5e4a9e9837fbda6e6cbb'
PREFIX = 'local_transport::runtime::macos_locator_tests::'

# Authoritative trusted engine source files and accepted post-repair hashes (BS 415/416 acceptance).
# Any modification to compiled engine module paths or unexpected source files is rejected.
TRUSTED_ENGINE_FILES = {
    'crates/contextdb-engine/src/local_transport/runtime.rs': 'b50c07cde63c59d685c62061523345ab6f2f6357e9aff5e06246bf4f5413c9a0',
    'crates/contextdb-engine/tests/local_transport/local_transport_refusal_taxonomy.rs': '9ebdb6f9008876f6954cd341c94f16485b72eafa8efbe6f3563fd8769459b0e3',
    'crates/contextdb-engine/tests/local_transport/local_transport_runtime.rs': 'dc0bd03e0386a6518f592c0d0da70b39c128dcbd94c99d3db7fccf06d14a7bdc',
}

EXPECTED_CARRY = {
    'old_engine_candidate_sha': '897eff2849d0e800ca3a5e4a9e9837fbda6e6cbb',
    'old_verifier_sha': '8081aaba063becc0b9b960fd2a5af44453c63d4b',
    'old_native_run_id': '39087115042',
    'old_receipt_sha256': '927e061e8fe5505b19299c0edc969716456fd25db3314690cd417039e493800a',
    'old_passed_lib_binary_sha256': '8008a17a21c205d24aba6f0ce3149e9e087620721908f76bfc3385e853411cdf',
}


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def candidate(selected, verifier):
    # Engine candidate identity is explicitly bound in macos-engine-selection.json;
    # do NOT fallback to shared reader config (macos-reader-proof.json) or environment variables.
    require(isinstance(selected, dict), 'engine selection config required')
    sha = selected.get('candidate_sha')
    require(isinstance(sha, str) and re.fullmatch(r'[0-9a-f]{40}', sha),
            'approved public engine candidate H required in engine selection (null template cannot build)')
    parent = selected.get('parent')
    require(sha not in (verifier, BASE, parent, PARENT_H),
            'approved public engine H required, distinct from T, baseline, and parent')
    return sha


def selection(data):
    require(data.get('schema') == 'macos-engine-selection-v1' and
            data.get('parent') == PARENT_H,
            'unexpected selection schema/parent; expected ' + PARENT_H)
    receipt_sha = data.get('approval_receipt_sha256')
    require(isinstance(receipt_sha, str) and re.fullmatch(r'[0-9a-f]{64}', receipt_sha),
            'receipt provenance required (null template cannot build)')
    carry_receipt_sha = data.get('carry_receipt_sha256')
    require(isinstance(carry_receipt_sha, str) and re.fullmatch(r'[0-9a-f]{64}', carry_receipt_sha),
            'carry receipt provenance required (null template cannot build)')

    carry = data.get('carry_authority', {})
    require(isinstance(carry, dict), 'carry_authority object required')
    for k, v in EXPECTED_CARRY.items():
        require(carry.get(k) == v, f'carry authority mismatch for {k}')

    internal, external = data.get('internal_ids', []), data.get('external_ids', [])
    require(len(internal) == 3 and len(external) == 5 and
            len(set(internal + external)) == 8, 'exact unique 3+5 selection required')
    require(carry.get('carried_internal_ids') == internal, 'carried_internal_ids mismatch in carry_authority')
    require(all(x.startswith(PREFIX) for x in internal) and
            all(x.startswith('local_transport_runtime::') for x in external), 'full module IDs required')
    require(set(data.get('source_sha256', {})) == set(TRUSTED_ENGINE_FILES),
            'exact 3 trusted engine source paths required')
    require(data['source_sha256'] == TRUSTED_ENGINE_FILES,
            'trusted engine source mapping mismatch')
    return internal, external


def source_hashes(tree, expected):
    actual = {name: digest(tree/name) for name in expected}
    require(actual == expected, 'accepted source hash mismatch; stop for engine review')
    return actual


def package_targets(metadata, tree):
    manifest = (tree/'crates/contextdb-engine/Cargo.toml').resolve()
    packages = [p for p in metadata['packages'] if p['name'] == 'contextdb-engine' and
                Path(p['manifest_path']).resolve() == manifest]
    require(len(packages) == 1, 'exact engine package metadata required')
    package = packages[0]
    result = {}
    for label, name, kind in [('lib', 'contextdb_engine', ['lib']),
                              ('integration', 'local_transport', ['test'])]:
        targets = [t for t in package['targets'] if t['name'] == name and t['kind'] == kind]
        require(len(targets) == 1 and targets[0]['test'] is True, 'exact engine target required')
        target = targets[0]
        src = Path(target['src_path']).resolve()
        require(src.is_relative_to(tree.resolve()) and src.is_file(), 'target source outside H')
        result[label] = target
    return package['id'], result


def executable(raw, package_id, target, target_dir):
    events = [json.loads(line) for line in raw.splitlines() if line.strip()]
    finished = [e for e in events if e.get('reason') == 'build-finished']
    require(len(finished) == 1 and finished[0].get('success') is True, 'successful Cargo finish required')
    matches = [e for e in events if e.get('reason') == 'compiler-artifact' and
               e.get('package_id') == package_id and e.get('target') == target and
               e.get('profile', {}).get('test') is True and e.get('executable') is not None]
    require(len(matches) == 1, 'exact unique Cargo test artifact required')
    path = Path(matches[0]['executable'])
    require(path.is_absolute() and path.resolve().is_relative_to(target_dir.resolve()) and
            path.is_file() and os.access(path, os.X_OK), 'test executable outside owned target or not executable')
    return path.resolve()


def inventory(raw, approved, internal=False):
    names = []
    for line in raw.splitlines():
        if not line.strip() or re.fullmatch(r'\d+ tests?, \d+ benchmarks?', line):
            continue
        match = re.fullmatch(r'(.+): (test|benchmark)', line)
        require(match is not None, 'malformed native test list')
        names.append(match.group(1))
    require(len(names) == len(set(names)), 'duplicate native test list IDs')
    require(all(names.count(x) == 1 for x in approved), 'approved native test ID absent')
    if internal:
        require({x for x in names if x.startswith(PREFIX)} == set(approved),
                'internal native module changed; stop for engine review')
    return {'all_ids': names, 'selected_ids': approved,
            'listed_not_run': [x for x in names if x not in approved]}


def outcome(raw, code, full_id):
    require(code == 0, 'native case exited unsuccessfully')
    require(re.findall(r'^running (\d+) tests?$', raw, re.M) == ['1'], 'exactly one executed case required')
    require(re.findall(r'^test (.+) \.\.\. (.+)$', raw, re.M) == [(full_id, 'ok')],
            'unexpected, ignored, duplicate or missing executed ID')
    summaries = re.findall(r'^test result: (.+)$', raw, re.M)
    require(len(summaries) == 1 and re.fullmatch(
        r'ok\. 1 passed; 0 failed; 0 ignored; 0 measured; \d+ filtered out; finished in .+', summaries[0]),
        'exact 1 passed/0 failed/0 ignored summary required')


def main():
    root = Path(__file__).resolve().parents[1]
    out = root/'target/macos-engine-proof'
    out.mkdir(parents=True, exist_ok=False)
    receipt = {
        'verdict': 'FAIL',
        'native': 'UNQUALIFIED',
        'native_current_result': 'UNQUALIFIED',
        'combined_status': 'PENDING_BS_CARRY',
        'commands': [],
        'runs': [],
        'cleanup': {'created': False, 'success': False},
        'current_actual_required': 5,
        'carried_required': 3,
        'selected_total': 8,
        'carried_library_status': 'EXTERNAL_CARRY_PENDING_VALIDATION',
    }
    temporary = None
    tree = None
    expected = None

    def save():
        (out/'receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')

    def run(args, name, cwd=root, env=None, required=True):
        started = time.time()
        try:
            result = subprocess.run(args, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError as error:
            result = subprocess.CompletedProcess(args, 127, b'', str(error).encode())
        (out/(name+'.stdout')).write_bytes(result.stdout)
        (out/(name+'.stderr')).write_bytes(result.stderr)
        receipt['commands'].append({'args': args, 'cwd': str(cwd), 'exit': result.returncode,
                                    'started_unix': started, 'elapsed_seconds': time.time()-started,
                                    'stdout': name+'.stdout', 'stderr': name+'.stderr'})
        save()
        require(not required or result.returncode == 0, 'command failed: '+repr(args))
        return result

    save()
    try:
        require(platform.system() == 'Darwin', 'native Darwin required before any build')
        verifier = os.environ.get('VERIFIER_SHA', '')
        require(re.fullmatch(r'[0-9a-f]{40}', verifier), 'full verifier SHA required')
        require(os.environ.get('GITHUB_EVENT_NAME') == 'push' and
                os.environ.get('GITHUB_REF') == BRANCH and
                os.environ.get('GITHUB_REPOSITORY') == 'context-graph-ai/contextdb' and
                os.environ.get('GITHUB_WORKFLOW_SHA') == verifier and
                os.environ.get('GITHUB_SHA') == verifier, 'trusted branch/event/workflow identity required')
        require(run(['git', 'rev-parse', 'HEAD'], 'verifier-head').stdout.decode().strip() == verifier,
                'verifier checkout mismatch')
        require(not run(['git', 'status', '--porcelain', '--untracked-files=no'], 'verifier-status').stdout.strip(),
                'dirty verifier checkout')
        selected = json.loads((root/'scripts/macos-engine-selection.json').read_text())
        sha = candidate(selected, verifier)
        internal, external = selection(selected)
        expected = selected['source_sha256']
        files = ['.github/workflows/macos-engine-proof.yml', 'scripts/ci-macos-engine-proof.py',
                 'scripts/macos-engine-selection.json', 'scripts/macos-reader-proof.json']
        receipt.update(
            verifier_sha=verifier,
            candidate_sha=sha,
            repository=PUBLIC,
            workflow_sha=os.environ['GITHUB_WORKFLOW_SHA'],
            ref=BRANCH,
            event='push',
            workflow_ref=os.environ.get('GITHUB_WORKFLOW_REF'),
            run_id=os.environ.get('GITHUB_RUN_ID'),
            run_attempt=os.environ.get('GITHUB_RUN_ATTEMPT'),
            selection=selected,
            tooling_sha256={f: digest(root/f) for f in files},
            features='default',
            jobs=1,
            test_threads=1,
            platform=platform.platform(),
            carried_internal_ids=internal,
            carry_authority=selected.get('carry_authority'),
        )
        for name, command in [('macos', ['sw_vers']), ('uname', ['uname', '-a']),
                              ('rustc', ['rustc', '-Vv']), ('cargo', ['cargo', '-V'])]:
            receipt[name] = run(command, name).stdout.decode()
        require(receipt['rustc'].startswith('rustc 1.94.1 ') and
                re.search(r'^host: (aarch64|x86_64)-apple-darwin$', receipt['rustc'], re.M) and
                receipt['cargo'].startswith('cargo 1.94.1 '), 'pinned native toolchain required')
        temporary = Path(tempfile.mkdtemp(prefix='engine-proof-')).resolve()
        receipt['cleanup'] = {'created': True, 'path': str(temporary), 'success': False}
        tree = temporary/'fixed'
        objects = temporary/'objects.git'
        run(['git', 'init', '--bare', str(objects)], 'source-init')
        run(['git', '-c', 'credential.helper=', 'fetch', '--no-tags', '--depth=1', PUBLIC, sha],
            'source-fetch', cwd=objects, env=dict(os.environ, GIT_TERMINAL_PROMPT='0'))
        require(run(['git', 'rev-parse', 'FETCH_HEAD'], 'source-head', cwd=objects).stdout.decode().strip() == sha,
                'fetched H mismatch')
        commit_raw = run(['git', 'cat-file', '-p', sha], 'source-commit', cwd=objects).stdout.decode()
        parents = [l.split()[1] for l in commit_raw.splitlines() if l.startswith('parent ')]
        require(parents == [selected['parent']], 'candidate parent mismatch; expected ' + selected['parent'])
        receipt['candidate_parent'] = selected['parent']
        run(['git', '-c', 'core.hooksPath=/dev/null', 'worktree', 'add', '--detach', str(tree), sha],
            'source-checkout', cwd=objects)
        receipt['source_tree'] = run(['git', 'rev-parse', 'HEAD^{tree}'], 'source-tree', cwd=tree).stdout.decode().strip()
        receipt['source_before'] = source_hashes(tree, expected)
        receipt['cargo_inputs_sha256'] = {str(p.relative_to(tree)): digest(p) for p in tree.rglob('Cargo.toml')}
        receipt['lock_sha256'] = digest(tree/'Cargo.lock')
        target_dir = temporary/'target'
        env = dict(os.environ, CARGO_TARGET_DIR=str(target_dir), CARGO_BUILD_JOBS='1',
                   RUSTUP_TOOLCHAIN='1.94.1', CARGO_INCREMENTAL='0')
        # Leave TMPDIR, XDG_RUNTIME_DIR and explicit runtime-root variables as supplied.
        metadata = json.loads(run(['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1'],
                                  'metadata', cwd=tree, env=env).stdout)
        package_id, targets = package_targets(metadata, tree)
        receipt['package_id'] = package_id
        receipt['target_dir'] = str(target_dir)
        receipt['inventories'] = {}
        binaries = {}
        # Darwin compile + list BOTH targets with unchanged lib default command and integration --features test-seams
        for label, selector, ids in [('lib', ['--lib'], internal),
                                      ('integration', ['--features', 'test-seams', '--test', 'local_transport'], external)]:
            build = run(['cargo', 'test', '--locked', '-p', 'contextdb-engine', *selector,
                         '--no-run', '--message-format=json'], label+'-build', cwd=tree, env=env)
            binary = executable(build.stdout.decode(), package_id, targets[label], target_dir)
            binaries[label] = {'path': str(binary), 'sha256': digest(binary)}
            listed = run([str(binary), '--list', '--format', 'terse'], label+'-list', cwd=tree, env=env)
            receipt['inventories'][label] = inventory(listed.stdout.decode(), ids, label == 'lib')
        receipt['binaries'] = binaries
        save()

        # DO NOT execute library3 again. Run ONLY 5 integration FULLIDs, exactly 1 pass 0 fail 0 ignore each.
        for full_id in external:
            require(full_id not in [x['id'] for x in receipt['runs']], 'duplicate invocation')
            execution = run([binaries['integration']['path'], '--exact', full_id, '--nocapture', '--test-threads', '1'],
                            'case-'+str(len(receipt['runs'])+1), cwd=tree, env=env, required=False)
            record = {'id': full_id, 'binary': 'integration', 'exit': execution.returncode, 'passed': False}
            receipt['runs'].append(record)
            save()
            outcome(execution.stdout.decode(), execution.returncode, full_id)
            record['passed'] = True
            save()

        require(len(receipt['runs']) == 5 and all(x['passed'] for x in receipt['runs']),
                'five actual integration passes required')
        receipt['verdict'] = 'PASS'
    except Exception as error:
        receipt['error'] = str(error)
    finally:
        if tree is not None and tree.exists() and expected is not None:
            try:
                receipt['source_after'] = source_hashes(tree, expected)
                require(run(['git', 'rev-parse', 'HEAD'], 'source-head-after', cwd=tree).stdout.decode().strip() == receipt['candidate_sha'], 'H changed')
                require(not run(['git', 'status', '--porcelain', '--untracked-files=no'], 'source-status-after', cwd=tree).stdout.strip(), 'source changed')
            except Exception as error:
                receipt['verdict'] = 'FAIL'
                receipt['source_after_error'] = str(error)
        if temporary is not None:
            try:
                shutil.rmtree(temporary)
                require(not temporary.exists(), 'temporary checkout remains')
                receipt['cleanup']['success'] = True
            except Exception as error:
                receipt['verdict'] = 'FAIL'
                receipt['cleanup']['error'] = str(error)
        else:
            receipt['cleanup'] = {'created': False, 'success': True, 'reason': 'no temporary checkout created'}

        if receipt['verdict'] == 'PASS':
            receipt['native'] = 'PASS/PENDING_BS_CARRY'
            receipt['native_current_result'] = 'integration5 current PASS/PENDING_BS_CARRY'
            receipt['combined_status'] = 'PENDING_BS_CARRY'
        else:
            receipt['native'] = 'UNQUALIFIED'
            receipt['native_current_result'] = 'UNQUALIFIED'
            receipt['combined_status'] = 'UNQUALIFIED'
        save()
    require(receipt['verdict'] == 'PASS', 'engine qualification failed; see receipt and raw logs')


if __name__ == '__main__':
    main()
