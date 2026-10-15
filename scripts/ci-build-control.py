#!/usr/bin/env python3
"""Development delivery controls. No build/publish operation runs on import."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import tomllib

REPO = 'context-graph-ai/contextdb'
TARGET = 'x86_64-unknown-linux-gnu'
BASE = f'contextdb-{TARGET}'
ASSETS = {BASE + s for s in ('.tar.gz', '.tar.gz.sha256', '.json')}
BINS = ('contextdb', 'contextdb-server')
CONFIG = {'profile': 'release', 'locked': True, 'packages': ['contextdb-cli', 'contextdb-server'],
          'bins': list(BINS), 'features': 'package defaults (server iroh)',
          'CARGO_BUILD_JOBS': '2', 'CARGO_INCREMENTAL': '0', 'RUSTFLAGS': ''}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def command(args, **kw):
    return subprocess.check_output(args, text=True, timeout=kw.pop("timeout", 120), **kw).strip()


def api(path, **fields):
    args = ['gh', 'api', path]
    if fields:
        args += ['--method', 'PATCH']
        for k, v in fields.items():
            args += ['-F' if isinstance(v, bool) else '-f',
                     f'{k}={str(v).lower() if isinstance(v, bool) else v}']
    return json.loads(command(args))


def pages(path, key):
    data = json.loads(command(['gh', 'api', '--paginate', '--slurp', path]))
    return [item for page in data for item in page[key]]


def source_guard(env, event, tip):
    sha = env['BUILD_SHA']
    require(re.fullmatch('[0-9a-f]{40}', sha), 'exact SHA required')
    require(env['GITHUB_REPOSITORY'] == REPO, 'wrong origin')
    require(env['WORKFLOW_SHA'] == sha == env['GITHUB_SHA'] == tip, 'workflow/source/dev binding')
    require(env['GITHUB_EVENT_NAME'] in ('push', 'workflow_dispatch'), 'wrong event')
    require(env['GITHUB_REF'] == 'refs/heads/dev', 'delivery must use dev')
    if env['GITHUB_EVENT_NAME'] == 'workflow_dispatch':
        require(env['GITHUB_REF'] == 'refs/heads/dev', 'manual dispatch must use dev')
        require(event['inputs']['sha'] == sha, 'manual input mismatch')
    else:
        require(env['GITHUB_WORKFLOW_REF'] == REPO + '/.github/workflows/ci.yml@refs/heads/dev',
                'automatic delivery must belong to canonical dev CI')
        require(event['after'] == sha == event['head_commit']['id']
                and event['ref'] == 'refs/heads/dev'
                and event['repository']['full_name'] == REPO, 'push payload mismatch')
        require(env['GITHUB_RUN_ID'].isdigit() and int(env['GITHUB_RUN_ID']) > 0
                and env['GITHUB_RUN_ATTEMPT'].isdigit() and int(env['GITHUB_RUN_ATTEMPT']) > 0,
                'current run identity required')


def run_guard(run, sha):
    require(run['head_sha'] == sha and run['head_branch'] == 'dev'
            and run['head_repository']['full_name'] == REPO
            and run['repository']['full_name'] == REPO
            and run['event'] == 'push'
            and run['path'] == '.github/workflows/ci.yml'
            and ((run['status'] == 'in_progress' and run['conclusion'] is None)
                 or (run['status'] == 'completed' and run['conclusion'] in ('success', 'failure'))),
            'not same-repo dev push CI')


def aggregate_guard(jobs, run):
    aggregates = [j for j in jobs if j['name'] == 'PR checks']
    require(len(aggregates) == 1, 'missing/duplicate aggregate')
    job = aggregates[0]
    require(job['conclusion'] == 'success' and job['status'] == 'completed'
            and job['head_sha'] == run['head_sha']
            and job['run_id'] == run['id'] and job['run_attempt'] == run['run_attempt'],
            'aggregate not successful on exact source/attempt')


def guard():
    env = os.environ
    sha = env['BUILD_SHA']
    event = json.loads(Path(env['GITHUB_EVENT_PATH']).read_text())
    source_guard(env, event, sha)
    current_tip(sha)
    workflow_id = api(f'repos/{REPO}/actions/workflows/ci.yml')['id']
    automatic = env['GITHUB_EVENT_NAME'] == 'push'
    if automatic:
        runs = [api(f"repos/{REPO}/actions/runs/{env['GITHUB_RUN_ID']}")]
    else:
        runs = pages(f'repos/{REPO}/actions/workflows/ci.yml/runs?event=push&head_sha={sha}&per_page=100', 'workflow_runs')
    for run in runs:
        try:
            run_guard(run, sha)
            require(run['workflow_id'] == workflow_id, 'CI workflow identity mismatch')
            if automatic:
                require(str(run['id']) == env['GITHUB_RUN_ID']
                        and str(run['run_attempt']) == env['GITHUB_RUN_ATTEMPT'],
                        'not current run/attempt')
                require(run['status'] in ('in_progress', 'completed')
                        and run['conclusion'] in (None, 'success'), 'current CI failed')
            else:
                require(run['status'] == 'completed'
                        and run['conclusion'] in ('success', 'failure'), 'manual CI must complete')
            jobs = pages(f"repos/{REPO}/actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs?per_page=100", 'jobs')
            aggregate_guard(jobs, run)
            if not automatic:
                unsuccessful = [j for j in jobs if j['conclusion'] != 'success']
                delivery_failures = [j for j in unsuccessful
                                     if (j['name'] == 'development-binaries'
                                         or j['name'].startswith('development-binaries / '))
                                     and j['status'] == 'completed'
                                     and j['conclusion'] in ('failure', 'skipped')
                                     and j['head_sha'] == sha and j['run_id'] == run['id']
                                     and j['run_attempt'] == run['run_attempt']]
                require(len(delivery_failures) == len(unsuccessful), 'non-delivery CI job failed')
                require(run['conclusion'] == 'success' or any(
                    j['conclusion'] == 'failure' for j in delivery_failures),
                    'overall failure without delivery failure')
        except (KeyError, ValueError):
            continue
        print(f"Verified CI run {run['id']} attempt {run['run_attempt']} at {sha}")
        return
    raise ValueError('successful exact-source push CI and aggregate required')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def identity():
    return {'lock_sha256': digest('Cargo.lock'),
            'manifests_sha256': {p: digest(p) for p in
                                 command(['git', 'ls-files', 'Cargo.toml', '**/Cargo.toml']).splitlines()},
            'toolchain_file_sha256': digest('rust-toolchain.toml'),
            'toolchain': tomllib.loads(Path('rust-toolchain.toml').read_text())['toolchain']['channel']}


def smoke(archive):
    with tempfile.TemporaryDirectory() as tmp:
        with tarfile.open(archive) as tar:
            members = tar.getmembers()
            require({m.name for m in members} == {*BINS, 'LICENSE'} and len(members) == 3,
                    'archive inventory mismatch')
            require(all(m.isfile() and m.size < 1024**3 for m in members), 'unsafe archive member')
            for member in members:
                require(member.name in (*BINS, 'LICENSE'), 'unsafe path')
                tar.extract(member, tmp, filter='data')
        clean = {k: v for k, v in os.environ.items()
                 if k in ('PATH', 'HOME', 'TMPDIR', 'LANG', 'LC_ALL')}
        versions = {}
        for binary in BINS:
            for flag in ('--version', '--help'):
                output = command([str(Path(tmp, binary)), flag], env=clean)
                require(binary in output, f'{binary} {flag} output mismatch')
                print(output)
                if flag == '--version':
                    versions[binary] = output
        output = command([str(Path(tmp, 'contextdb')), ':memory:', '--json'],
                         input='SELECT 1 + 1;\n', env=clean)
        docs = [json.loads(line) for line in output.splitlines()]
        require(len(docs) == 1 and docs[0]['result']['rows'] == [{'expr': 2}], 'SQL smoke mismatch')
        print(output)
        clean.update(CONTEXTDB_CLI=str(Path(tmp, 'contextdb')),
                     CONTEXTDB_SERVER=str(Path(tmp, 'contextdb-server')),
                     CONTEXTDB_SMOKE_DRIVER=str(Path(os.environ.get('CONTEXTDB_SMOKE_DRIVER',
                         f'target/development-smoke/{TARGET}/release/contextdb-smoke-driver')).resolve()))
        print(command(['bash', str(Path('scripts/installed-release-durable-sync-smoke.sh').resolve())],
                      env=clean, timeout=3600))
        return versions


def package():
    sha = os.environ['BUILD_SHA']
    require(command(['git', 'rev-parse', 'HEAD']) == sha, 'checkout mismatch')
    require(not command(['git', 'status', '--porcelain', '--untracked-files=no']), 'dirty source')
    out = Path(os.environ['BUILD_ASSETS'])
    out.mkdir(parents=True, exist_ok=False)
    archive = out / (BASE + '.tar.gz')
    with tarfile.open(archive, 'w:gz') as tar:
        for binary in BINS:
            tar.add(f'target/{TARGET}/release/{binary}', arcname=binary, recursive=False)
        tar.add('LICENSE', arcname='LICENSE', recursive=False)
    versions = smoke(archive)
    receipt = {'schema': 1, 'source_sha': sha, 'repository': REPO, 'target': TARGET,
               'configuration': CONFIG, 'dependency_identity': identity(), 'sha256': digest(archive),
               'binary_versions': versions,
               'rustc_vV': command(['rustc', '-vV']), 'cargo_version': command(['cargo', '-V']),
               'runner': {k: os.environ.get(k, '') for k in ('RUNNER_OS', 'RUNNER_ARCH', 'ImageOS', 'ImageVersion')},
               'run_id': os.environ['GITHUB_RUN_ID'], 'run_attempt': os.environ['GITHUB_RUN_ATTEMPT'],
               'smoke': 'unpacked --version/--help both binaries; CLI SELECT 1 + 1 = 2; full installed-release-durable-sync-smoke.sh with separate verifier'}
    (out / (BASE + '.json')).write_text(json.dumps(receipt, sort_keys=True, indent=2) + '\n')
    (out / (BASE + '.tar.gz.sha256')).write_text(f"{receipt['sha256']}  {archive.name}\n")


def validate(root, sha, run_id, attempt, dependencies):
    root = Path(root)
    require({p.name for p in root.iterdir()} == ASSETS, 'asset set mismatch')
    require(all(p.is_file() and not p.is_symlink() for p in root.iterdir()), 'unsafe asset')
    receipt = json.loads((root / (BASE + '.json')).read_text())
    checksum = digest(root / (BASE + '.tar.gz'))
    require(receipt.get('schema') == 1, 'receipt schema mismatch')
    require(receipt['source_sha'] == sha and receipt['repository'] == REPO, 'receipt source mismatch')
    require(receipt['target'] == TARGET and receipt['configuration'] == CONFIG, 'target/config mismatch')
    require(receipt['dependency_identity'] == dependencies, 'dependency mismatch')
    require(receipt['run_id'] == run_id and receipt['run_attempt'] == attempt, 'run mismatch')
    require(receipt['sha256'] == checksum, 'receipt checksum mismatch')
    require((root / (BASE + '.tar.gz.sha256')).read_text() == f'{checksum}  {BASE}.tar.gz\n', 'checksum mismatch')
    require(set(receipt['binary_versions']) == set(BINS)
            and all(receipt['binary_versions'].values()), 'binary version evidence mismatch')
    require(receipt['rustc_vV'] and receipt['cargo_version'] and all(receipt['runner'].values())
            and receipt['smoke'], 'missing toolchain/runner/smoke evidence')
    with tarfile.open(root / (BASE + '.tar.gz')) as tar:
        members = tar.getmembers()
        require(len(members) == 3 and {m.name for m in members} == {*BINS, 'LICENSE'}
                and all(m.isfile() for m in members), 'unsafe package')
    return receipt


class StalePublisher(ValueError):
    """A superseded build exits successfully without further publication."""


def current_tip(sha):
    if api(f'repos/{REPO}/git/ref/heads/dev')['object']['sha'] != sha:
        raise StalePublisher('dev advanced; skipping superseded publisher')


def release_listing():
    data = json.loads(command(['gh', 'api', '--paginate', '--slurp',
                               f'repos/{REPO}/releases?per_page=100']))
    matches = [r for page in data for r in page if r['tag_name'] == 'dev-latest']
    require(len(matches) <= 1, 'duplicate rolling release')
    return matches


def remote_assets(release_id):
    data = json.loads(command(['gh', 'api', '--paginate', '--slurp',
                               f'repos/{REPO}/releases/{release_id}/assets?per_page=100']))
    assets = [a for page in data for a in page]
    require(len({a['name'] for a in assets}) == len(assets), 'duplicate remote asset')
    return assets


def remote_bytes(names):
    # Read actual asset bytes as data; never execute or unpack remote content.
    with tempfile.TemporaryDirectory() as tmp:
        args = ['gh', 'release', 'download', 'dev-latest', '--repo', REPO, '--dir', tmp]
        for name in names:
            args += ['--pattern', name]
        command(args)
        require({p.name for p in Path(tmp).iterdir()} == set(names), 'remote inventory mismatch')
        return {n: Path(tmp, n).read_bytes() for n in names}


def verify_remote(files):
    data = remote_bytes(files)
    require(all(hashlib.sha256(data[n]).hexdigest() == digest(p) for n, p in files.items()),
            'remote upload mismatch')


def publish():
    guard()
    env = os.environ
    sha = env['BUILD_SHA']
    root = Path(env['BUILD_ASSETS'])
    validate(root, sha, env['GITHUB_RUN_ID'], env['GITHUB_RUN_ATTEMPT'], identity())
    require(all(re.fullmatch(r'[1-9][0-9]*', env[k]) for k in
                ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT')), 'invalid run identity')
    matches = release_listing()  # Authenticated, paginated and includes hidden drafts.
    current_tip(sha)
    if not matches:
        branch = api(f'repos/{REPO}')['default_branch']
        # Resolve the CURRENT default head, never the build SHA or a cached branch name.
        from urllib.parse import quote
        bootstrap = api(f'repos/{REPO}/commits/{quote(branch, safe="")}')['sha']
        require(re.fullmatch('[0-9a-f]{40}', bootstrap), 'invalid bootstrap SHA')
        current_tip(sha)
        command(['gh', 'release', 'create', 'dev-latest', '--repo', REPO, '--draft', '--prerelease',
                 '--latest=false', '--target', bootstrap, '--title', 'Development upload pending',
                 '--notes', 'Upload pending; channel tag is stationary.'])
        matches = release_listing()
        require(len(matches) == 1, 'created release not found')
    release = matches[0]
    require(release['prerelease'] and not release.get('immutable', False),
            'rolling release must be mutable prerelease')
    release_id = release['id']
    assets = remote_assets(release_id)
    order = [BASE + suffix for suffix in ('.tar.gz', '.tar.gz.sha256', '.json')]
    # Copy sidecars unchanged: receipts and checksum lines describe the stable archive name.
    # The fallback recipe renames downloaded run copies before checking both hashes.
    with tempfile.TemporaryDirectory() as tmp:
        copies = {}
        for name in order:
            copy_name = name.replace(BASE, f"{BASE}-{sha[:12]}-r{env['GITHUB_RUN_ID']}a{env['GITHUB_RUN_ATTEMPT']}", 1)
            path = Path(tmp, copy_name)
            path.write_bytes((root / name).read_bytes())
            copies[copy_name] = path
        existing = {a['name'] for a in assets}
        require(not (set(copies) & existing), 'write-once run group already exists')
        for path in copies.values():
            current_tip(sha)
            command(['gh', 'release', 'upload', 'dev-latest', '--repo', REPO, str(path)])
        verify_remote(copies)  # Whole run group must verify before ANY stable mutation.
        for name in order:
            current_tip(sha)  # Immediately before EVERY stable-name mutation.
            command(['gh', 'release', 'upload', 'dev-latest', '--repo', REPO, str(root / name), '--clobber'])
        verify_remote({name: root / name for name in order})
        retention_assets = remote_assets(release_id)
        current_tip(sha)
        command(['gh', 'release', 'edit', 'dev-latest', '--repo', REPO, '--draft=false',
                 '--prerelease', '--latest=false', '--title', f'Development {sha}', '--notes',
                 f'Unstable development binaries. Source: {sha}. The dev-latest tag is the stationary '
                 'bootstrap point, not the binary source. Verify checksum AND receipt digest. '
                 'Run copies (rename to stable names before verification): ' + ', '.join(copies)])
    # Only successful verified metadata publication admits retention. Ignore stable names,
    # unrelated assets and incomplete groups. Group age is its last member creation time.
    groups = {}
    pattern = re.compile(re.escape(BASE) + r'-([0-9a-f]{12}-r[1-9][0-9]*a[1-9][0-9]*)(\.tar\.gz(?:\.sha256)?|\.json)')
    for asset in retention_assets:
        match = pattern.fullmatch(asset['name'])
        if match:
            groups.setdefault(match[1], []).append(asset)
    complete = [items for items in groups.values() if len(items) == 3
                and {pattern.fullmatch(a['name'])[2] for a in items}
                == {'.tar.gz', '.tar.gz.sha256', '.json'}]
    complete.sort(key=lambda items: (max(a['created_at'] for a in items), items[0]['name']), reverse=True)
    # Protect every group whose actual receipt equals the current stable receipt,
    # even when timestamps place it outside the newest three publications.
    stable_receipt = remote_bytes([BASE + '.json'])[BASE + '.json']
    require(stable_receipt == (root / (BASE + '.json')).read_bytes(), 'stable receipt changed')
    for items in complete[3:]:
        receipt_name = next(a['name'] for a in items if a['name'].endswith('.json'))
        if remote_bytes([receipt_name])[receipt_name] == stable_receipt:
            continue
        for asset in items:
            current_tip(sha)
            command(['gh', 'api', f"repos/{REPO}/releases/assets/{asset['id']}", '--method', 'DELETE'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('guard', 'package', 'publish'))
    args = parser.parse_args()
    try:
        {'guard': guard, 'package': package, 'publish': publish}[args.operation]()
    except StalePublisher as error:
        message = str(error)
        print(message)
        if os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
                summary.write(message + '\n')
    except (ValueError, KeyError, subprocess.SubprocessError) as error:
        raise SystemExit(f'development delivery refused: {error}')
