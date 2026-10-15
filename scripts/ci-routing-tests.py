#!/usr/bin/env python3
"""Focused offline controls for the proposed normal/full routing; never invoke Cargo."""
import copy
import contextlib
import io
import os
import importlib.util
import json
from pathlib import Path
import sys
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('routing', ROOT / 'scripts/ci-integration.py')
i = importlib.util.module_from_spec(spec)
spec.loader.exec_module(i)
controls = 0


def reject(call):
    global controls
    try:
        call()
    except ValueError:
        controls += 1
    else:
        raise AssertionError('negative control was admitted')


if '--plan-only' in sys.argv:
    import tempfile
    certificate = dict(targets={t: {} for t in i.NORMAL_DEDUP_TARGETS}, sha256='a' * 64,
                       reason='fixture proof applies', warning_compile_owner='engine-family-1')
    off = dict(targets={}, reason='dedup disabled: fixture proof unavailable', fallback_targets=list(i.NORMAL_DEDUP_TARGETS))
    identity = dict(source_sha='1' * 40, source_fingerprint='2' * 64)
    def proof(root=i.ROOT, check_source=True):
        return off if check_source and os.environ.get('CARGO_BUILD_TARGET') else certificate
    with patch.object(i, 'normal_dedup_certificate', side_effect=proof), patch.object(i, 'source_identity', return_value=identity):
        rows = i.load_manifest()
        decision = i.plan_decision('normal', False)
        planned = i.matrix(rows, False, 'normal', decision)
        selected = i.active_rows(rows, False, 'normal', decision)
        assert len(selected) == 23
        assert not {'engine-family-10', 'engine-family-11', 'engine-family-13',
                    'engine-family-14'} & {r['id'] for r in selected}
        owner = next(r for r in selected if r['id'] == 'engine-family-1')
        assert i.commands(owner, mode='normal')[0][-1] == '--no-run'
        needs = dict(plan=dict(result='success', outputs=dict(enabled='true', matrix=json.dumps(planned), **decision)), slice=dict(result='success'))
        i.required(needs, rows, True, False, 'normal')
        # Aggregate validates published plan, not its own current feature environment.
        with patch.dict(os.environ, {'CARGO_BUILD_TARGET': 'injected-after-plan'}):
            i.required(needs, rows, True, False, 'normal')
            reject(lambda: i.runtime_decision(owner, 'normal'))
            with tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / 'refused'
                with patch.object(i, 'capture', side_effect=AssertionError('build must not start')):
                    assert i.run_slice(owner, out, 'normal') == 1
                receipt = json.loads((out / 'receipt.json').read_text())
                assert 'no longer applicable' in receipt['refusal_reason'] and receipt['commands'] == []
        # A published no-dedup decision stays off when proof becomes applicable.
        with patch.object(i, 'normal_dedup_certificate', return_value=off):
            disabled = i.plan_decision('normal', False)
        fallback = i.active_rows(rows, False, 'normal', disabled)
        assert len(fallback) == 27
        executed = [t for row in fallback for t in row['targets']]
        assert all(executed.count(t) == 1 for t in i.NORMAL_DEDUP_TARGETS)
        for row in fallback:
            i.runtime_decision(row, 'normal')
            cmds = i.commands(row, mode='normal')
            assert not any('--no-run' in c and '--test' in c for c in cmds)
        for mutation in ['missing-sha', 'tampered-sha', 'source-sha', 'fingerprint', 'mode', 'compile-owner', 'dropped-row', 'matrix-source', 'matrix-mode', 'matrix-sha']:
            bad = copy.deepcopy(needs)
            outputs = bad['plan']['outputs']
            matrix = json.loads(outputs['matrix'])
            if mutation == 'missing-sha': del outputs['dedup_sha']
            elif mutation == 'tampered-sha': outputs['dedup_sha'] = 'b' * 64
            elif mutation == 'source-sha': outputs['source_sha'] = '3' * 40
            elif mutation == 'fingerprint': outputs['source_fingerprint'] = '4' * 64
            elif mutation == 'mode': outputs['gate_mode'] = 'full'
            elif mutation == 'compile-owner': matrix['include'] = [r for r in matrix['include'] if r['id'] != 'engine-family-1']
            elif mutation == 'dropped-row': matrix['include'].append(dict(matrix['include'][0], id='engine-family-10'))
            elif mutation == 'matrix-source': matrix['include'][0]['source_sha'] = '5' * 40
            elif mutation == 'matrix-mode': matrix['include'][0]['gate_mode'] = 'full'
            else: del matrix['include'][0]['dedup_sha']
            outputs['matrix'] = json.dumps(matrix)
            reject(lambda: i.required(bad, rows, True, False, 'normal'))
        for key in ['dedup_sha', 'source_sha', 'gate_mode']:
            broken = copy.deepcopy(owner); del broken['plan_decision'][key]
            with tempfile.TemporaryDirectory() as tmp:
                assert i.run_slice(broken, Path(tmp) / 'refused', 'normal') == 1
                assert (Path(tmp) / 'refused/receipt.json').is_file()
    i.workflow_contract()
    ci = (ROOT / '.github/workflows/ci.yml').read_text()
    assert all(int(n) <= 30 for n in i.re.findall(r'timeout-minutes: (\d+)', ci))
    print(f'Review17 plan fixtures PASS: valid→stale refuses before build with receipt; off→valid retains59/26; compile owner/dropped rows; {controls} mutation refusals; normal timeout caps.')
    sys.exit(0)

if '--dedup-only' in sys.argv:
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / 'crates/contextdb-engine/tests/probe.rs'
        source.parent.mkdir(parents=True)
        source.write_text('#[test] fn alpha() {}\n')
        manifest = root / 'scripts/ci-integration-targets.json'
        manifest.parent.mkdir()
        manifest.write_text(json.dumps({'schema_version': 1, 'slices': []}))
        cargo = root / 'crates/contextdb-engine/Cargo.toml'
        cargo.write_text('[[test]]\nname="probe"\npath="tests/probe.rs"\n')
        listing = '     Running tests/probe.rs (target/debug/probe)\nalpha: test\n1 test, 0 benchmarks\n'
        ignored = '     Running tests/probe.rs (target/debug/probe)\n0 tests, 0 benchmarks\n'
        with patch.object(i, 'NORMAL_DEDUP_TARGETS', ('probe',)):
            identity = dict(applicability=i.dedup_snapshot(root), sources={'probe': i.literal_closure(root, 'probe')})
            (root / 'identity.json').write_text(json.dumps(identity))
            for n in ['ordinary.list', 'nightly.list']: (root / (n + '.txt')).write_text(listing)
            for n in ['ordinary.ignored', 'nightly.ignored']: (root / (n + '.txt')).write_text(ignored)
            c = i.regenerate_dedup(root, root)
            assert set(i.normal_dedup_certificate(root)['targets']) == {'probe'}
            original = manifest.read_text()
            # Each failure restores execution rather than blocking lint/plan.
            for mutation in ['missing', 'unknown', 'digest', 'feature', 'ignored']:
                data = json.loads(original)
                if mutation == 'missing': del data['normal_dedup']
                elif mutation == 'unknown': data['normal_dedup']['targets']['other'] = {}
                elif mutation == 'digest': data['normal_dedup']['sha256'] = 'bad'
                elif mutation == 'feature': data['normal_dedup']['features'] = []
                else: data['normal_dedup']['targets']['probe']['cases'] = [['alpha', 'ignored']]
                manifest.write_text(json.dumps(data))
                decision = i.normal_dedup_certificate(root)
                assert not decision['targets'] and decision['fallback_targets'] == ['probe']
                controls += 1
            manifest.write_text(original)
            source.write_text('#[test] fn beta() {}\n')
            assert not i.normal_dedup_certificate(root)['targets']; controls += 1
            reject(lambda: i.regenerate_dedup(root, root))
            source.write_text('#[test] fn alpha() {}\n')
            # Unrelated production change without a feature consumer stays applicable.
            unrelated = root / 'crates/contextdb-engine/src/other.rs'
            unrelated.parent.mkdir(); unrelated.write_text('fn unrelated() {}')
            assert i.normal_dedup_certificate(root)['targets']
            unrelated.write_text('const X: bool = cfg!(feature = "nightly-tests");')
            assert not i.normal_dedup_certificate(root)['targets']; controls += 1
            unrelated.unlink()
            test_profile = {'CARGO_PROFILE_TEST_OPT_LEVEL': '1',
                            'CARGO_PROFILE_TEST_DEBUG_ASSERTIONS': 'true',
                            'CARGO_PROFILE_TEST_OVERFLOW_CHECKS': 'true'}
            with patch.dict(os.environ, test_profile):
                assert i.normal_dedup_certificate(root)['targets']
            for flags in [{'RUSTFLAGS': '--cfg other'},
                          {'CARGO_PROFILE_TEST_OPT_LEVEL': '1'},
                          dict(test_profile, CARGO_PROFILE_TEST_OPT_LEVEL='3')]:
                with patch.dict(os.environ, flags): assert not i.normal_dedup_certificate(root)['targets']
                controls += 1
            # Local-only regeneration refuses unequal case and ignored inventories.
            (root / 'nightly.list.txt').write_text(listing.replace('alpha: test', 'beta: test'))
            reject(lambda: i.regenerate_dedup(root, root))
            (root / 'nightly.list.txt').write_text(listing)
            (root / 'nightly.ignored.txt').write_text(listing)
            reject(lambda: i.regenerate_dedup(root, root))
    # Plan fallback is covered independently of the live checkout certificate.
    rows = i.load_manifest()
    with patch.object(i, 'normal_dedup_certificate', return_value=dict(targets={}, reason='dedup disabled: stale certificate', fallback_targets=list(i.NORMAL_DEDUP_TARGETS))):
        fallback = i.active_rows(rows, False, 'normal')
        assert len(fallback) == 27
        assert set(i.NORMAL_DEDUP_TARGETS) <= {t for row in fallback for t in row['targets']}
        for row in fallback: i.commands(row, mode='normal')
    print(f'Review16 fixture controls PASS: local regeneration, conservative fallback, unchanged unrelated source, all59 fallback targets/26 physical slices; {controls} negative controls.')
    sys.exit(0)

# Routing tests use the checked-in certificate as test data, never recertify a live tree.
fixture_certificate = dict(targets={t: {} for t in i.NORMAL_DEDUP_TARGETS}, warning_compile_owner='engine-family-1', sha256='a' * 64, reason='fixture proof')
i.normal_dedup_certificate = lambda *a, **k: dict(fixture_certificate, targets=fixture_certificate.get('targets', {}))

rows = i.load_manifest()
i.workflow_contract()
assert i.quarantine_contract() is None
for path, case in list(i.QUARANTINE_EXCEPTIONS)[:2]:
    source = ROOT / path
    original = source.read_text()
    read_text = Path.read_text
    with patch.object(Path, 'read_text', lambda p, *a, **kw: original.replace('CDB-160:', 'CDB-999:') if p == source else read_text(p, *a, **kw)):
        reject(lambda: i.quarantine_contract())
# CI test/lint/full checkouts are depth 1. The runtime quarantine inventory must
# inspect only checkout files, never ask Git for the frozen base object.
check_output = i.subprocess.check_output
with patch.object(i.subprocess, 'check_output', lambda argv, *a, **kw: (_ for _ in ()).throw(AssertionError('runtime guard asked for git history')) if argv[:2] == ['git', 'show'] else check_output(argv, *a, **kw)):
    assert i.quarantine_contract() is None
    assert i.load_manifest()
print(f'Quarantine guard PASS: CDB-154 baseline plus seven exact owner-approved rc2 attributes; {controls} mutation refusals.')
for mode, default, count in [('normal', False, 23), ('full', False, 32), ('full', True, 36)]:
    matrix = i.matrix(rows, default, mode)
    assert len(matrix['include']) == count
    selected = i.active_rows(rows, default, mode)
    assert len({r['id'] for r in selected}) == count
    targets = [t for row in selected if row['kind'] == 'engine-tests' for t in row['targets']]
    assert 'custody_schema_replay_bounded_contract' in targets
    assert 'vector_indexed_route_does_less_work_than_exact' in targets
    assert 'vector_graph_route_profile_quality' in targets
    assert sum(row['kind'] == 'release-acceptance' for row in selected) == 3
    assert bool(set(targets) & i.FULL_SCHEMA) == (mode == 'full')
    assert bool(set(targets) & i.FULL_CORPORA) == (mode == 'full')
    for row in selected:
        commands = i.commands(row, mode=mode)
        for cmd in commands:
            assert '--ignored' not in cmd and '--include-ignored' not in cmd
        if row['id'] == 'sync-schema-transactions':
            if mode == 'normal':
                assert commands[0][-2:] == ['--skip', i.OVERSIZED]
                assert len(commands) == 1
            else:
                assert '--skip' not in commands[0]
                assert commands[1][-2:] == [i.OVERSIZED, '--exact']
                assert commands[1][commands[1].index('--features') + 1] == 'test-seams,iroh'
        if mode == 'full' and set(row['targets']) & i.FULL_SCHEMA:
            assert len(commands) == 2
            assert commands[1][commands[1].index('--features') + 1] == 'test-seams,iroh'
            assert '--skip' not in commands[1]
    needs = dict(plan=dict(result='success', outputs=dict(enabled='true', **{k: matrix['include'][0][k] for k in i.PLAN_FIELDS}, matrix=json.dumps(matrix))),
                 slice=dict(result='success'))
    i.required(needs, rows, True, default, mode)
    for result in ['failure', 'cancelled', 'skipped', None]:
        bad = copy.deepcopy(needs); bad['slice']['result'] = result
        reject(lambda: i.required(bad, rows, True, default, mode))
    for field in ['plan', 'slice']:
        bad = copy.deepcopy(needs); del bad[field]
        reject(lambda: i.required(bad, rows, True, default, mode))
    for mutation in ['missing', 'duplicate', 'unknown']:
        bad = copy.deepcopy(needs); changed = copy.deepcopy(matrix)
        if mutation == 'missing': changed['include'].pop()
        elif mutation == 'duplicate': changed['include'].append(changed['include'][0])
        else: changed['include'][0]['id'] = 'unknown-case-family'
        bad['plan']['outputs']['matrix'] = json.dumps(changed)
        reject(lambda: i.required(bad, rows, True, default, mode))
    bad = copy.deepcopy(needs); bad['plan']['outputs']['gate_mode'] = 'invalid'
    reject(lambda: i.required(bad, rows, True, default, mode))
reject(lambda: i.matrix(rows, True, 'normal'))
reject(lambda: i.matrix(rows, False, 'typo'))
for row in rows:
    if set(row['targets']) & (i.FULL_SCHEMA | i.FULL_CORPORA):
        reject(lambda: i.commands(row, mode='normal'))
for mode, companion, passed, ignored, filtered in [('normal', False, 12, 0, 1), ('full', False, 12, 1, 0), ('full', True, 0, 1, 12)]:
    entry = dict(target='tests/sync_schema_transactions.rs', passed=passed,
                 failed=0, ignored=ignored, measured=0, filtered_out=filtered)
    i.sync_routing_counts([entry], mode, companion)
    for key in ['passed', 'filtered_out', 'ignored', 'failed', 'measured']:
        bad = dict(entry); bad[key] += 1
        reject(lambda: i.sync_routing_counts([bad], mode, companion))
    reject(lambda: i.sync_routing_counts([], mode, companion))
    reject(lambda: i.sync_routing_counts([entry, entry], mode, companion))
# Reject census changes before a plan can silently relocate the wrong test.
read = Path.read_bytes
for source in i.SYNC_ROUTING_FINGERPRINTS:
    path = ROOT / source
    with patch.object(Path, 'read_bytes', lambda p: read(p) + b' changed' if p == path else read(p)):
        reject(lambda: i.load_manifest())
# Canonical full corpus/union guards still reject lost targets.
read_text = Path.read_text
manifest = ROOT / 'scripts/ci-integration-targets.json'
for mutation in ['missing', 'duplicate']:
    data = json.loads(manifest.read_text())
    if mutation == 'missing': data['slices'][0]['targets'].pop()
    else: data['slices'][0]['targets'].append('sync')
    with patch.object(Path, 'read_text', lambda p, *a, **k: json.dumps(data) if p == manifest else read_text(p, *a, **k)):
        reject(lambda: i.load_manifest())
print(f'Routing controls PASS: 3 valid plans, exact feature companions and sync counts; {controls} negative controls rejected. No Cargo or native tests.')

# Exercise the exact shard selector used by Bash, with no test/build execution.
for mode in ['normal', 'full']:
    for shard in ['contextdb-engine-test-seams-9', 'contextdb-engine-test-seams-12', 'contextdb-engine-test-seams-15']:
        output = io.StringIO()
        with patch.dict(os.environ, {'CI_GATE_MODE': mode}), patch.object(sys, 'argv', ['ci-integration.py', 'shard-plan', shard]), contextlib.redirect_stdout(output):
            assert i.main() == 0
        targets, skip = output.getvalue().strip().split('|')
        if shard.endswith('-9'):
            assert ('custody_schema_replay_is_proportional_to_affected_tables' in targets.split()) == (mode == 'full')
            assert 'custody_schema_replay_bounded_contract' in targets.split()
        if shard.endswith('-12'):
            assert ('vector_graph_recall_50000_isotropic_per_context' in targets.split()) == True
            assert 'vector_indexed_route_does_less_work_than_exact' in targets.split()
        assert skip == (i.OVERSIZED if mode == 'normal' and shard.endswith('-15') else '')
print('Six normal/full Bash shard plans PASS; original indexed route remains normal.')
