#!/usr/bin/env python3
"""Check aggregate case ownership and isolated commands without executing Cargo."""
import copy
import os
import subprocess
import tempfile
import shlex
import sys
import hashlib
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('integration', ROOT / 'scripts/ci-integration.py')
i = importlib.util.module_from_spec(spec)
spec.loader.exec_module(i)
a = i.aggregate_module()
data = a.contract(ROOT)
controls = 0


def reject(action):
    global controls
    try:
        action()
    except ValueError:
        controls += 1
    else:
        raise AssertionError('ownership mutation was accepted')


# Target and module ownership remain complete in both routing modes.
rows = i.load_manifest()
for mode in ('normal', 'full'):
    selected = i.active_rows(rows, False, mode)
    counts = {target: 0 for target in a.ROUTES}
    for row in selected:
        for command in i.commands(row, mode=mode):
            for position, token in enumerate(command[:-1]):
                if token == '--test' and command[position + 1] in counts and '--no-run' not in command:
                    counts[command[position + 1]] += 1
    assert counts == {target: int(mode == 'full' or target != 'seed_commit_memory') for target in counts}
    for target in a.AGGREGATES:
        row = next(row for row in selected if target in row['targets'])
        commands = [command for command in i.commands(row, mode=mode)
                    if '--test' in command and command[command.index('--test') + 1] == target]
        assert len(commands) == 1
        command = commands[0]
        a.validate_command(ROOT, target, command)
        entries = data['aggregates'][target]['modules']
        unique = next(entry for entry in entries if not entry['owners'])
        prefix = command.index('--skip')
        reject(lambda: a.validate_command(ROOT, target, command[:prefix] + command[prefix + 2:]))
        reject(lambda: a.validate_command(ROOT, target, command + ['--skip', unique['name'] + '::']))
        reject(lambda: a.validate_command(ROOT, target, command + command[prefix:prefix + 2]))
        split = command.index('--')
        reject(lambda: a.validate_command(ROOT, target, command[:split] + ['--test', 'engine'] + command[split:]))

# The shared main schema namespace runs once per selected feature tuple.
bounded = a.FULL_ONLY_RULE['full_skip_owner']
canonical = a.FULL_ONLY_RULE['full_owner']
prefix = a.FULL_ONLY_RULE['module'] + '::'
for mode in ('normal', 'full'):
    planned = [command for row in i.active_rows(rows, False, mode)
               for command in i.commands(row, mode=mode)]
    executions = {}
    for command in planned:
        targets = [command[n + 1] for n, value in enumerate(command[:-1]) if value == '--test']
        if not {bounded, canonical} & set(targets):
            continue
        features = command[command.index('--features') + 1]
        owners = sum(target in targets for target in (bounded, canonical))
        if '--skip' in command and prefix in command:
            owners -= int(bounded in targets)
        executions[features] = executions.get(features, 0) + owners
    assert executions == ({'nightly-tests,test-seams,iroh': 1} if mode == 'normal' else
                          {'nightly-tests,test-seams,iroh': 1, 'test-seams,iroh': 1}), executions
row = next(row for row in rows if bounded in row['targets'])
command = next(command for command in i.commands(row, mode='full')
               if command.count('--test') == 1 and command[command.index('--test') + 1] == bounded)
a.validate_command(ROOT, bounded, command)
at = command.index('--skip')
reject(lambda: a.validate_command(ROOT, bounded, command[:at] + command[at + 2:]))
reject(lambda: a.validate_command(ROOT, bounded, command + ['--skip', 'replay::']))
reject(lambda: a.validate_command(ROOT, bounded, command + ['--skip', prefix]))
reject(lambda: a.validate_command(ROOT, bounded, command[:command.index('--')] + ['--test', canonical] + command[command.index('--'):]))
normal_row = next(row for row in i.active_rows(rows, False, 'normal') if bounded in row['targets'])
normal = next(command for command in i.commands(normal_row, mode='normal') if bounded in command)
assert prefix not in normal

# Even a refreshed data checksum cannot hide missing/duplicated unique modules
# or pretend a shared module has no existing execution owner.
certificate = ROOT / 'scripts/ci-aggregate-routing.json'
read_bytes = Path.read_bytes
for target in a.AGGREGATES:
    for mutation in ('missing-unique', 'duplicate-unique', 'remove-owner', 'invent-owner'):
        changed = copy.deepcopy(data)
        entries = changed['aggregates'][target]['modules']
        unique = next(entry for entry in entries if not entry['owners'])
        shared = next(entry for entry in entries if entry['owners'])
        if mutation == 'missing-unique':
            entries.remove(unique)
        elif mutation == 'duplicate-unique':
            entries.append(copy.deepcopy(unique))
        elif mutation == 'remove-owner':
            shared['owners'] = []
            shared['proofs'] = {}
        else:
            unique['owners'] = shared['owners'][:1]
            unique['proofs'] = {unique['owners'][0]: {}}
        payload = (json.dumps(changed, indent=2) + '\n').encode()
        with patch.object(a, 'CERTIFICATE_SHA256', hashlib.sha256(payload).hexdigest()), patch.object(
                Path, 'read_bytes', lambda path: payload if path == certificate else read_bytes(path)):
            reject(lambda: a.contract(ROOT))

for mutation in ('missing-rule', 'reversed-rule'):
    changed = copy.deepcopy(data)
    if mutation == 'missing-rule':
        changed.pop('full_only_rule')
    else:
        changed['full_only_rule']['full_owner'], changed['full_only_rule']['full_skip_owner'] = (
            changed['full_only_rule']['full_skip_owner'], changed['full_only_rule']['full_owner'])
    payload = (json.dumps(changed, indent=2) + '\n').encode()
    with patch.object(a, 'CERTIFICATE_SHA256', hashlib.sha256(payload).hexdigest()), patch.object(
            Path, 'read_bytes', lambda path: payload if path == certificate else read_bytes(path)):
        reject(lambda: a.contract(ROOT))
normal_env = dict(os.environ, CI_GATE_MODE='normal')
normal_refusal = subprocess.run([sys.executable, '-B', 'scripts/ci-integration.py', 'aggregate-skips',
                                 bounded, '--aggregate-features', 'test-seams,iroh'],
                                cwd=ROOT, env=normal_env, capture_output=True, text=True)
assert normal_refusal.returncode != 0 and 'normal schema replay must retain' in normal_refusal.stderr

# Source/cfg/allocator drift must require review instead of silently rerouting.
source = ROOT / 'crates/contextdb-engine/tests/vector.rs'
with patch.object(Path, 'read_bytes', lambda path: read_bytes(path) + b'\n#[global_allocator]\n' if path == source else read_bytes(path)):
    reject(lambda: a.contract(ROOT))
# Exercise the real shell runner with a Cargo stub: filters stay isolated and
# neither a primary failure nor an aggregate failure prevents the other command.
cases = [('12', 'full', failure, expected) for failure, expected in
         (('', 0), ('primary', 7), ('custody', 9), ('vector', 11), ('plan', 2))]
cases += [('9', 'full', '', 0), ('9', 'normal', '', 0),
          ('9', 'full', 'primary', 7), ('9', 'full', 'bounded', 9), ('9', 'full', 'plan', 2)]
for number, mode, failure, expected in cases:
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        cargo = directory / 'cargo'
        cargo.write_text("#!/usr/bin/python3\n" +
                         "import sys,os,json,pathlib\n" +
                         "args=sys.argv[1:]\n" +
                         "if args==['--version']: print('cargo synthetic'); sys.exit(0)\n" +
                         "with open(os.environ['CALLS'],'a') as f: f.write(json.dumps(args)+'\\n')\n" +
                         "if args[0]=='build': sys.exit(0)\n" +
                         "target=args[args.index('--test')+1]\n" +
                         "assert os.environ['CARGO_BIN_EXE_contextdb']==os.environ['CARGO_TARGET_DIR']+'/debug/contextdb'\n" +
                         "assert os.environ['CARGO_BIN_EXE_contextdb-server']==os.environ['CARGO_TARGET_DIR']+'/debug/contextdb-server'\n" +
                         "print('     Running tests/'+target+'.rs (target/debug/deps/'+target+'-123)')\n" +
                         "print('running 1 test')\n" +
                         "print('test synthetic ... ok')\n" +
                         "print('test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s')\n" +
                         "which=os.environ['FAIL_WHICH']\n" +
                         "sys.exit(7 if which=='primary' and target not in ('custody','vector','custody_schema_replay_bounded_contract') else 9 if which=='custody' and target=='custody' else 11 if which=='vector' and target=='vector' else 9 if which=='bounded' and target=='custody_schema_replay_bounded_contract' else 0)\n")
        cargo.chmod(0o755)
        python = directory / 'python3'
        python.write_text('#!/bin/bash\n'
                          'if [[ "$FAIL_WHICH" == plan && " $* " == *" aggregate-skips $PLAN_TARGET "* ]]; then exit 9; fi\n'
                          'exec ' + shlex.quote(sys.executable) + ' -B "$@"\n')
        python.chmod(0o755)
        env = dict(os.environ, PATH=str(directory) + ':' + os.environ['PATH'],
                   CARGO_TARGET_DIR=str(directory / 'target'), CALLS=str(directory / 'calls'),
                   FAIL_WHICH=failure, PLAN_TARGET='custody' if number == '12' else bounded,
                   CI_GATE_MODE=mode, CI_CRATE='contextdb-engine',
                   CI_SHARD='contextdb-engine-test-seams-' + number, CI_FEATURES='test-seams,iroh')
        run = subprocess.run(['bash', 'scripts/ci-test-shard.sh'], cwd=ROOT, env=env,
                             capture_output=True, text=True, timeout=30)
        assert run.returncode == expected, (failure, run.returncode, run.stderr)
        calls = [json.loads(line) for line in (directory / 'calls').read_text().splitlines()]
        tests = [call for call in calls if call[0] == 'test']
        isolated = a.AGGREGATES if number == '12' else (bounded,) if mode == 'full' else ()
        assert not set(isolated) & {tests[0][n + 1] for n, value in enumerate(tests[0][:-1]) if value == '--test'}
        if number == '9' and mode == 'normal':
            assert bounded in tests[0] and canonical not in tests[0] and prefix not in tests[0]
        for target in isolated:
            commands = [call for call in tests[1:] if call[call.index('--test') + 1] == target]
            assert len(commands) == int(not (failure == 'plan' and target == env['PLAN_TARGET']))
            if commands:
                a.validate_command(ROOT, target, ['cargo'] + commands[0])
        time = directory / ('target/ci-shard-out/' + env['CI_SHARD'] + '/' + env['CI_SHARD'] + '.time')
        assert 'exit=' + str(expected) in time.read_text()

print(f'Aggregate routing PASS: fourteen exact target owners, two isolated aggregate commands, full-only seed and {controls} independent mutation refusals. Ten real shell paths use a Cargo stub; no native Rust tests run.')
