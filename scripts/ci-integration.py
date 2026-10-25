#!/usr/bin/env python3
"""One validated integration inventory and command selection, locally and on Actions."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tomllib

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
FEATURES = 'nightly-tests,test-seams,iroh'
OPTIONAL = {'workspace', 'redb', 'production', 'production-release'}
KINDS = {'engine-tests', 'release-acceptance', 'examples', 'doctests'} | OPTIONAL
ACCEPTANCE_TARGETS = ('acceptance', 'acceptance_long_running', 'acceptance_sync')
# These complete expensive cases must remain independently scheduled.
SCHEMA_REPLAY_SLICES = {
    'engine-family-9': ('schema replay contracts', 'custody_schema_replay_is_proportional_to_affected_tables'),
    'schema-replay-work-limited-to-affected-tables': ('schema replay / work limited to affected tables', 'custody_schema_replay_work_limited_to_affected_tables'),
    'schema-replay-identical-schema-does-no-artifact-work': ('schema replay / identical schema avoids rebuilding data', 'custody_schema_replay_identical_schema_does_no_artifact_work'),
}

# Keep replication and schema transactions in separate required jobs.
SYNC_SLICES = {
    'engine-family-5': ('replication, receipts and policy', 'sync'),
    'sync-schema-transactions': ('schema replication, atomic publish and oversized transport', 'sync_schema_transactions'),
}

# Complete corpora keep one fixture and six ordered filter/scope cells per job.
VECTOR_PROFILE_SLICES = {
    'vector-graph-recall-5000-anisotropic': ('vector graph recall / 5,000 anisotropic vectors per context', 'vector_graph_recall_5000_anisotropic_per_context'),
    'vector-graph-recall-5001-topic-clusters': ('vector graph recall / 5,001 topic-clustered vectors per context', 'vector_graph_recall_5001_topic_clusters_per_context'),
    'vector-graph-recall-50000-isotropic': ('vector graph recall / 50,000 isotropic vectors per context', 'vector_graph_recall_50000_isotropic_per_context'),
    'vector-graph-recall-50001-anisotropic': ('vector graph recall / 50,001 anisotropic vectors per context', 'vector_graph_recall_50001_anisotropic_per_context'),
}


# Local proposal: normal correctness and original full qualification are distinct.
# The canonical manifest always describes the full inventory; routing is a view.
FULL_SCHEMA = {value[1] for value in SCHEMA_REPLAY_SLICES.values()}
FULL_CORPORA = {value[1] for value in VECTOR_PROFILE_SLICES.values()}
OVERSIZED = ('a_received_schema_transaction_never_publishes_a_prefix::'
             'schema_migration_chunked_transport::'
             'oversized_authenticated_schema_stays_invisible_until_one_commit_and_retry')


SYNC_ROUTING_FINGERPRINTS = {'crates/contextdb-engine/tests/sync_schema_transactions.rs': '2de1becdc822e29dc11ef5a28f15420c1a0ae8eab7984fbd2729553666d88052',
 'crates/contextdb-engine/tests/sync/a_received_schema_transaction_never_publishes_a_prefix.rs': 'ff2446bd039196506772d08b64e896160f29c750bcb30485084ecf8b5e7f9118',
 'crates/contextdb-engine/tests/sync/schema_migration_chunked_transport.rs': 'b819a0f1f9ffcda711400bf00a0d5e78fd46d914835a36b3fa46ab00665022ad'}


# Reviewed normal-only partition of the unchanged three-case native inventory.
LONG_RUNNING_CASES = ('long_running::f25_edge_offline_accumulates_ten_thousand_rows_reconnects_and_pushes', 'long_running::f26_large_pull_ten_thousand_rows_from_server_to_fresh_edge', 'long_running::f27_incremental_pull_after_initial_sync')
LONG_RUNNING_PARTITIONS = {'acceptance-long-running-transfer-test': ('long_running::f25_edge_offline_accumulates_ten_thousand_rows_reconnects_and_pushes', 'long_running::f26_large_pull_ten_thousand_rows_from_server_to_fresh_edge'), 'acceptance-long-running-test': ('long_running::f27_incremental_pull_after_initial_sync',)}
LONG_RUNNING_FINGERPRINTS = {'tests/acceptance_long_running.rs': 'c070eb6fe18b5871b42419f33ed2c78c8e938c421384e7f1bcc9f35641b6e04d', 'tests/acceptance/long_running.rs': 'a1441aa17b96b6583fcfde53406205bf223447c00da8f613d330a438a936c5cb', 'tests/acceptance/common.rs': '1bd9e6cc921e8354c500434e362995d9afb59e3c3646a459ea7b25adf7760901', 'tests/support/binary_path_helpers.rs': '05bd95280e47f5141b2a04bec3bcf5ca934c4cc642597cc46e16496d9461cb0c'}

def long_running_contract(root=ROOT):
    for path, expected in LONG_RUNNING_FINGERPRINTS.items():
        if hashlib.sha256((root / path).read_bytes()).hexdigest() != expected:
            raise ValueError('long-running case inventory changed; review exact partition: ' + path)


def long_running_counts(entries, log, row_id):
    expected = LONG_RUNNING_PARTITIONS[row_id]
    matches = [e for e in entries if e.get('target') == '../../tests/acceptance_long_running.rs']
    text = re.sub(r'\x1b\[[0-9;]*m', '', log.read_text())
    names = re.findall(r'^test (\S+) \.\.\. ok$', text, re.M)
    if (len(entries) != 1 or len(matches) != 1 or sorted(names) != sorted(expected)
            or tuple(matches[0].get(k) for k in ('passed', 'failed', 'ignored', 'measured', 'filtered_out'))
            != (len(expected), 0, 0, 0, 3 - len(expected))):
        raise ValueError('long-running receipt must prove exact selected cases and all three counts')
    matches[0]['passed_cases'] = names


# Owner-approved rc2 quarantine: these are declarations, not passing cells.
QUARANTINE_EXCEPTIONS = {
    ('crates/contextdb-engine/tests/vector_graph_recall_5000_anisotropic_per_context.rs', 'selected_f32_build_switches_pass_5000_anisotropic_per_context'): 'CDB-160: index-acceleration bar owned by the bounded-storage-rc2 campaign (S5 search-route + S10 never-built graph 1c85c7c6); re-enabled when rc2 lands on dev.',
    ('crates/contextdb-engine/tests/vector_graph_recall_5001_topic_clusters_per_context.rs', 'selected_f32_build_switches_pass_5001_topic_clusters_per_context'): 'CDB-160: index-acceleration bar owned by the bounded-storage-rc2 campaign (S5 search-route + S10 never-built graph 1c85c7c6); re-enabled when rc2 lands on dev.',
    ('crates/contextdb-engine/tests/vector_graph_recall_50000_isotropic_per_context.rs', 'selected_f32_build_switches_pass_50000_isotropic_per_context'): 'CDB-160: index-acceleration bar owned by the bounded-storage-rc2 campaign (S5 search-route + S10 never-built graph 1c85c7c6); re-enabled when rc2 lands on dev.',
    ('crates/contextdb-engine/tests/vector_graph_recall_50001_anisotropic_per_context.rs', 'selected_f32_build_switches_pass_50001_anisotropic_per_context'): 'CDB-160: index-acceleration bar owned by the bounded-storage-rc2 campaign (S5 search-route + S10 never-built graph 1c85c7c6); re-enabled when rc2 lands on dev.',
    ('crates/contextdb-engine/tests/vector/indexed_route_does_less_work_than_exact.rs', 'indexed_route_does_less_work_than_exact'): 'CDB-161: bounded-storage-rc2 S5 AUTO calibration campaign (fix SHA pending); re-enabled when rc2 lands on dev.',
    ('crates/contextdb-engine/tests/vector/indexed_route_does_less_work_than_exact.rs', 'single_context_saved_graph_auto_and_indexed_are_faster_than_exact'): 'CDB-162: bounded-storage-rc2 S5 per-candidate walk-cost campaign (fix SHA pending); re-enabled when rc2 lands on dev.',
    ('crates/contextdb-engine/tests/sync/schema_migration_chunked_transport.rs', 'oversized_authenticated_schema_stays_invisible_until_one_commit_and_retry'): 'CDB-163: bounded-storage-rc2 S3/S4 oversized-pull funding campaign (fix SHA pending); re-enabled when rc2 lands on dev.',
}
BASELINE_CDB_IGNORE = ('crates/contextdb-server/tests/durable_deletion_and_purge_tests.rs', 'CDB-154: vector-sync defect on dev makes this purge test fail nondeterministically (transport timeout, false frontier corruption, 1000 vs 1001 nodes); re-enable when the storage fix lands', 'authoritative_purge_removes_every_engine_held_copy')


def gate_mode(value=None):
    value = os.environ.get('CI_GATE_MODE', 'full') if value is None else value
    if value not in {'normal', 'full'}:
        raise ValueError('gate mode must be normal or full')
    return value


def routed_targets(targets, mode):
    gate_mode(mode)
    return [t for t in targets if mode == 'full' or t not in FULL_SCHEMA | FULL_CORPORA]


def routing_contract(root=ROOT):
    # Exact-source census fence: a renamed/added/moved case cannot silently make
    # the one-case exclusion miss, or expand what it excludes. Refresh by review.
    fingerprints = SYNC_ROUTING_FINGERPRINTS
    for path, expected in fingerprints.items():
        if hashlib.sha256((root / path).read_bytes()).hexdigest() != expected:
            raise ValueError('sync routing census changed; review exact case inventory: ' + path)
    quarantine_contract(root)
    long_running_contract(root)


def quarantine_contract(root=ROOT):
    """Allow only the exact owner-approved rc2 ignore declarations at runtime."""
    for (path, case), reason in QUARANTINE_EXCEPTIONS.items():
        text = (root / path).read_text()
        attribute = f'#[ignore = "{reason}"]'
        declaration = re.compile(re.escape(attribute) + r'\s*(?:#\[[^\]]+\]\s*)*(?:async\s+)?fn\s+' + re.escape(case) + r'\s*\(')
        if len(declaration.findall(text)) != 1:
            raise ValueError('missing, duplicate, or misbound approved quarantine: ' + path + '::' + case)
    cdb_ignores = []
    for path in (root / 'crates').glob('*/tests/**/*.rs'):
        relative = str(path.relative_to(root))
        text = path.read_text()
        for match in re.finditer(r'#\[ignore = "(CDB-[0-9]+:[^"]*)"\]\s*(?:#\[[^\]]+\]\s*)*(?:async\s+)?fn\s+([A-Za-z0-9_]+)\s*\(', text):
            cdb_ignores.append((relative, match[1], match[2]))
    expected = [(path, reason, case) for (path, case), reason in QUARANTINE_EXCEPTIONS.items()] + [BASELINE_CDB_IGNORE]
    if sorted(cdb_ignores) != sorted(expected):
        raise ValueError('CDB ignore inventory changed; only CDB-154 and the seven approved rc2 attributes are allowed')
    corpus = [(path, case) for (path, case), reason in QUARANTINE_EXCEPTIONS.items() if reason.startswith('CDB-160:')]
    if len(corpus) != 4:
        raise ValueError('four corpus quarantine declarations required')
    for path, case in corpus:
        text = (root / path).read_text()
        if text.count('#![cfg(feature = "nightly-tests")]') != 1 or text.count('#[test]') != 1:
            raise ValueError('corpus quarantine must remain nightly-only and discoverable: ' + case)


def sync_routing_counts(entries, mode, companion=False):
    gate_mode(mode)
    matches = [e for e in entries if e.get('target') == 'tests/sync_schema_transactions.rs']
    expected = ((0, 1, 12) if companion else (12, 0, 1) if mode == 'normal'
                else (12, 1, 0))
    if (len(matches) != 1 or tuple(matches[0].get(k) for k in ['passed', 'ignored', 'filtered_out']) != expected
            or any(matches[0].get(k) != 0 for k in ['failed', 'measured'])):
        raise ValueError('sync routing receipt must account for all 13 cases and exact relocation')


# Optional optimization: any inapplicable certificate restores all normal tests.
NORMAL_DEDUP_TARGETS = ('custody_custody_protocol', 'custody_custody_purge_and_discard', 'custody_server_resource_policy_contract_tests', 'custody_tenant_table_policy', 'vector_a_filtered_scope_is_charged_for_its_candidates', 'vector_a_find_answers_a_full_page_when_a_candidate_is_withdrawn', 'vector_a_find_names_only_the_pairs_it_asks_for', 'vector_a_replaced_vector_cannot_end_a_tied_frontier', 'vector_a_saved_graph_search_reports_its_distance_work', 'vector_a_tied_frontier_cannot_hide_a_smaller_row', 'vector_auto_decides_the_route_once_for_the_whole_scope', 'vector_auto_pays_what_indexed_pays', 'vector_delete_only_journal_progress_contract', 'vector_equal_distance_rows_keep_canonical_order_across_contexts', 'vector_exclusion_persistence_tests', 'vector_inspection_explain_truth_contract', 'vector_maintained_lifecycle_contract', 'vector_memory_ownership_reconciliation_contract', 'vector_named_vector_planner_tests', 'vector_operation_observations', 'vector_policy', 'vector_quantized', 'vector_rank_policy_exact_audits_the_whole_table', 'vector_received_generation_invalidation_contract', 'vector_resource_maintenance_contract', 'vector_retained_indexed_rows', 'vector_row_delete_and_guards_honor_vector_columns', 'vector_serving_merge_contract', 'vector_snapshot_frontier_completeness_contract', 'vector_tail_transaction_contract', 'vector_the_owner_doors_run_one_search_over_every_layer', 'vector_transaction_filter_snapshot_truth_contract', 'vector_writer_residency_attribution', 'vector_auto_chooses_the_cheaper_route_for_each_partition', 'vector_serving_pages', 'bounded_metadata_start_lookup_stays_inside_the_read_limits', 'custody_delivery_outcome', 'indexed_row_memory_charge_measurement', 'memory_limit_admission_holds_for_vectors', 'vector_an_exhaustive_find_streams_bodies_it_cannot_hold', 'vector_multi_partition_union_recovers_nearest_rows', 'vector_partition', 'engine', 'sql', 'vector_generation', 'history_request_memory', 'custody_delivery_manifest', 'vector_a_find_decodes_only_the_rows_it_returns', 'vector_a_find_over_many_contexts_spends_one_search_budget', 'vector_one_search_covers_every_layer_on_both_routes', 'direct_file_image_memory_lifetime', 'graph_property_memory_charge_measurement', 'received_schema_metadata_peak', 'bounded_in_subquery_stays_inside_the_read_limits', 'bounded_read_of_staged_graph_work_stays_within_its_limits', 'bounded', 'vector_lazy', 'vector_schema', 'vector_search_mode_bounded_contract')


def feature_consumers(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (root / 'crates').glob('*/src/**/*.rs')
            if re.search(r'nightly.tests|CARGO_FEATURE|RUSTFLAGS', p.read_text())}


def literal_closure(root, target):
    manifest = tomllib.loads((root / 'crates/contextdb-engine/Cargo.toml').read_text())
    records = [t for t in manifest['test'] if t['name'] == target]
    if len(records) != 1:
        raise ValueError('unknown target registration')
    pending = [root / 'crates/contextdb-engine' / records[0]['path']]
    found = {}
    while pending:
        path = pending.pop().resolve()
        name = str(path.relative_to(root.resolve()))
        if name in found:
            continue
        text = path.read_text()
        if re.search(r'nightly.tests|CARGO_FEATURE|RUSTFLAGS|include!\s*\(\s*[^"\s]', text):
            raise ValueError('feature-bearing or dynamic helper: ' + name)
        found[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        # Literal includes/path modules are closed by their containing file hash.
        explicit = list(re.finditer(r'#\[path\s*=\s*"([^"]+)"\]\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*;', text))
        for match in explicit:
            pending.append(path.parent / match[1])
        stripped = text
        for match in reversed(explicit):
            stripped = stripped[:match.start()] + stripped[match.end():]
        for child in re.findall(r'include!\s*\(\s*"([^"]+)"\s*\)', stripped):
            pending.append(path.parent / child)
        for child in re.findall(r'(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*;', stripped):
            base = path.parent if path.name in {'mod.rs', target + '.rs'} else path.parent / path.stem
            choices = [base / (child + '.rs'), base / child / 'mod.rs']
            matches = [c for c in choices if c.is_file()]
            if len(matches) != 1:
                raise ValueError('unresolved helper ' + child)
            pending.extend(matches)
    return found


def dedup_snapshot(root):
    # Dependencies/features/config can invalidate equality; unrelated Rust edits do not.
    dependencies = list((root / 'crates').glob('*/Cargo.toml'))
    dependencies += [root / x for x in ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml']]
    dependencies += [p for p in (root / '.cargo').rglob('*') if p.is_file()]
    return dict(dependencies={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in dependencies if p.is_file()},
                feature_consumers=feature_consumers(root))


def normal_dedup_certificate(root=ROOT, check_source=True):
    result = dict(targets={}, reason='dedup disabled: missing or invalid certificate', fallback_targets=list(NORMAL_DEDUP_TARGETS))
    try:
        c = json.loads((root / 'scripts/ci-integration-targets.json').read_text()).get('normal_dedup')
        if not isinstance(c, dict) or c.get('schema_version') != 2 or set(c.get('targets', {})) != set(NORMAL_DEDUP_TARGETS):
            return result
        digest = c.get('sha256')
        payload = {k: v for k, v in c.items() if k != 'sha256'}
        if digest != hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest():
            return result
        if c.get('features') != ['test-seams,iroh', FEATURES] or c.get('profile') != 'test-profile' or c.get('threads') != '1':
            return result
        for target, proof in c['targets'].items():
            if not proof.get('sources') or len(proof['cases']) != proof['case_count'] or any(status not in {'ok', 'ignored'} for _, status in proof['cases']):
                return result
        if check_source:
            if os.environ.get('RUSTFLAGS', '') not in {'', '-D warnings'}:
                raise ValueError('unqualified RUSTFLAGS')
            test_profile = {
                'CARGO_PROFILE_TEST_OPT_LEVEL': '1',
                'CARGO_PROFILE_TEST_DEBUG_ASSERTIONS': 'true',
                'CARGO_PROFILE_TEST_OVERFLOW_CHECKS': 'true',
            }
            present_test_profile = {k: os.environ[k] for k in test_profile if k in os.environ}
            if present_test_profile and present_test_profile != test_profile:
                raise ValueError('unqualified test profile tuple')
            allowed_profiles = {
                'CARGO_PROFILE_DEV_DEBUG': '0',
                'CARGO_PROFILE_TEST_DEBUG': '0',
                **test_profile,
            }
            for k, v in os.environ.items():
                if (k.startswith('CARGO_PROFILE_') and allowed_profiles.get(k) != v
                        or k.startswith('CARGO_FEATURE_') or k in {'CARGO_ENCODED_RUSTFLAGS', 'CARGO_BUILD_RUSTFLAGS', 'CARGO_BUILD_TARGET', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER'} and v):
                    raise ValueError('unqualified flags: ' + k)
            if c['applicability'].get('feature_consumers') or dedup_snapshot(root) != c['applicability']:
                raise ValueError('dependency or actual feature-consumer change')
            for target, proof in c['targets'].items():
                if literal_closure(root, target) != proof['sources']:
                    raise ValueError('stale or incomplete literal helper closure: ' + target)
                # Changed source can introduce new helpers/cases: never skip it.
                for name, expected in proof['sources'].items():
                    path = root / name
                    if path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                        raise ValueError('stale source/helper: ' + name)
        return dict(c, reason='dedup enabled: local case/ignored/source certificates apply', fallback_targets=[])
    except (ValueError, KeyError, TypeError, OSError) as error:
        result['reason'] = 'dedup disabled: ' + str(error)
        return result


def parse_local_list(path):
    targets = {}
    current = None
    finished = set()
    for line in path.read_text().splitlines():
        line = re.sub(r'\x1b\[[0-9;]*m', '', line)
        match = re.search(r'Running tests/(\w+)\.rs \(', line)
        if match:
            current = match[1]
            if current in targets:
                raise ValueError('duplicate target list: ' + current)
            targets[current] = []
        match = re.fullmatch(r'(.+): test', line)
        if match and current:
            targets[current].append(match[1])
        if re.fullmatch(r'\d+ tests?, \d+ benchmarks?', line) and current:
            finished.add(current)
    if set(targets) != set(NORMAL_DEDUP_TARGETS) or finished != set(targets):
        raise ValueError('missing/unknown/incomplete local lists; list exactly the dedup candidate targets')
    if any(len(v) != len(set(v)) for v in targets.values()):
        raise ValueError('duplicate case list')
    return targets


def regenerate_dedup(directory, root=ROOT):
    # Source identity is captured before producing local list files and checked again.
    start = json.loads((directory / 'identity.json').read_text())
    sources = {t: literal_closure(root, t) for t in NORMAL_DEDUP_TARGETS}
    identity = dict(applicability=dedup_snapshot(root), sources=sources)
    if identity['applicability']['feature_consumers']:
        raise ValueError('production feature consumers need independent equality proof; retain normal execution')
    if identity != start:
        raise ValueError('source changed since local list snapshot; capture fresh lists')
    lists = {n: parse_local_list(directory / (n + '.txt')) for n in ['ordinary.list', 'ordinary.ignored', 'nightly.list', 'nightly.ignored']}
    targets = {}
    for t in NORMAL_DEDUP_TARGETS:
        ordinary = sorted(lists['ordinary.list'][t]); nightly = sorted(lists['nightly.list'][t])
        ignored = sorted(lists['ordinary.ignored'][t])
        if ordinary != nightly or ignored != sorted(lists['nightly.ignored'][t]) or not set(ignored) <= set(ordinary):
            raise ValueError('case/ignored inventories differ: ' + t)
        targets[t] = dict(cases=[[case, 'ignored' if case in ignored else 'ok'] for case in ordinary],
                          case_count=len(ordinary), sources=sources[t])
    c = dict(schema_version=2, features=['test-seams,iroh', FEATURES], profile='test-profile', threads='1',
             warning_compile_owner='engine-family-1', targets=targets, applicability=identity['applicability'],
             local_list_sha256={n: hashlib.sha256((directory / (n + '.txt')).read_bytes()).hexdigest() for n in lists})
    c['sha256'] = hashlib.sha256(json.dumps(c, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    manifest = root / 'scripts/ci-integration-targets.json'
    data = json.loads(manifest.read_text()); data['normal_dedup'] = c
    manifest.write_text(json.dumps(data, indent=2) + '\n')
    return c


PLAN_FIELDS = {'dedup_sha', 'dedup_reason', 'source_sha', 'source_fingerprint', 'gate_mode', 'run_default'}


def source_identity(root=ROOT):
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    diff = subprocess.check_output(['git', 'diff', 'HEAD', '--binary', '--no-ext-diff'], cwd=root)
    untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z'], cwd=root)
    files = {os.fsdecode(n): hashlib.sha256((root / os.fsdecode(n)).read_bytes()).hexdigest()
             for n in untracked.split(b'\0') if n and (root / os.fsdecode(n)).is_file()}
    digest = hashlib.sha256(sha.encode() + diff + json.dumps(files, sort_keys=True).encode()).hexdigest()
    return dict(source_sha=sha, source_fingerprint=digest)


def plan_decision(mode, run_default, root=ROOT):
    certificate = normal_dedup_certificate(root) if mode == 'normal' else dict(targets={}, reason='full qualification')
    return dict(dedup_sha=certificate.get('sha256', '') if certificate['targets'] else '',
                dedup_reason=certificate['reason'], gate_mode=gate_mode(mode), run_default=str(run_default).lower(),
                **source_identity(root))


def validate_decision(decision, mode, run_default, root=ROOT):
    if (not isinstance(decision, dict) or set(decision) != PLAN_FIELDS
            or decision['gate_mode'] != gate_mode(mode) or decision['run_default'] != str(run_default).lower()
            or not isinstance(decision['dedup_sha'], str)
            or not re.fullmatch(r'(?:[0-9a-f]{64})?', decision['dedup_sha'])
            or mode == 'full' and decision['dedup_sha']
            or not isinstance(decision['dedup_reason'], str) or not decision['dedup_reason']):
        raise ValueError('missing/tampered published plan decision or mode')
    identity = source_identity(root)
    if any(decision.get(k) != v for k, v in identity.items()):
        raise ValueError('published plan source identity mismatch')
    if decision['dedup_sha']:
        c = normal_dedup_certificate(root, check_source=False)
        if not c['targets'] or c.get('sha256') != decision['dedup_sha']:
            raise ValueError('published plan certificate SHA mismatch')
    return decision


def runtime_decision(row, mode, root=ROOT):
    decision = row.get('plan_decision')
    if decision is None:
        raise ValueError('slice missing published plan decision')
    validate_decision(decision, mode, boolean(decision.get('run_default', 'invalid')), root)
    if decision['dedup_sha']:
        current = normal_dedup_certificate(root)
        if not current['targets'] or current.get('sha256') != decision['dedup_sha']:
            raise ValueError('planned dedup certificate no longer applicable: ' + current['reason'])
    # Explicit empty decision remains empty even if runtime proof now applies.
    return decision


def active_rows(rows, run_default, mode, decision=None):
    gate_mode(mode)
    if mode == 'normal' and run_default:
        raise ValueError('normal mode requires run_default=false; default suites include full qualification')
    decision = plan_decision(mode, run_default) if decision is None else validate_decision(decision, mode, run_default)
    safe = set(NORMAL_DEDUP_TARGETS) if decision['dedup_sha'] else set()
    selected = []
    for original in rows:
        if not run_default and original['kind'] in OPTIONAL:
            continue
        row = dict(original, targets=list(original['targets']))
        if mode == 'normal':
            row['dedup_planned'] = bool(safe)
        row['plan_decision'] = decision
        if row['kind'] == 'engine-tests':
            row['targets'] = [t for t in routed_targets(row['targets'], mode) if t not in safe]
            if not row['targets']:
                continue
            if row['id'] == 'engine-family-9' and mode == 'normal':
                row['family'] = 'bounded schema replay / all 14 functional oracles'
            if row['id'] == 'sync-schema-transactions' and mode == 'normal':
                row['family'] = 'schema replication and atomic publish / 12 bounded cases'
        if mode == 'normal' and row['id'] == 'acceptance-long-running-test':
            for row_id in LONG_RUNNING_PARTITIONS:
                selected.append(dict(row, id=row_id, cache_parent='acceptance-long-running-test',
                                     family=row['family'] + (' / transfer' if 'transfer' in row_id else ' / incremental')))
            continue
        selected.append(row)
    return selected


def vector_profile_contract(root, by_id):
    """Prove four required corpus jobs map bijectively to the canonical 24 cells."""
    base = root / 'crates/contextdb-engine/tests'
    source = (base / 'vector/graph_route_profile_support.rs').read_text()
    match = re.search(r'const BUILD_QUALITY_PROFILES: \[\(usize, &str, usize\); 4\] = \[(.*?)\n\];', source, re.S)
    if match is None:
        raise ValueError('missing canonical vector quality profiles')
    tuples = re.findall(r'\(\s*([\d_]+)(?:usize)?, "([a-z_]+)", ([\d_]+)(?:usize)?\),', match[1])
    profiles = [(int(n.replace('_', '')), shape, int(seed.replace('_', ''))) for n, shape, seed in tuples]
    if len(profiles) != 4 or len(set(profiles)) != 4:
        raise ValueError('vector quality requires four unique canonical profiles')
    # Freeze the original tuples, including seeds, without a second profile list.
    fingerprint = hashlib.sha256(json.dumps(profiles, separators=(',', ':')).encode()).hexdigest()
    if fingerprint != '07b228e9d90b2802530a785327e698059214e6ccdd6fa02b5b07c71bbae73f6d':
        raise ValueError('vector quality canonical corpus/seed tuples changed')
    if (re.search(r'#\[\s*(?:test|ignore)\b', source)
            or 'let filters = [("broad", "bucket < 5"), ("narrow", "bucket = 0")];' not in source
            or 'const ALL: [Self; 3] = [Self::One, Self::Several, Self::All];' not in source
            or 'BUILD_QUALITY_PROFILES.len() * filters.len() * Scope::ALL.len(),\n        24,' not in source
            or 'assert_eq!(cells, 6, "each build-quality corpus has six cells");' not in source):
        raise ValueError('vector quality must retain six cells per corpus and 24 aggregate cells')
    manifest = tomllib.loads((root / 'crates/contextdb-engine/Cargo.toml').read_text())
    for index, (slice_id, (family, target)) in enumerate(VECTOR_PROFILE_SLICES.items()):
        row = by_id.get(slice_id)
        if row != dict(id=slice_id, family=family, kind='engine-tests', targets=[target]):
            raise ValueError('vector quality requires four separate named corpus slices: ' + slice_id)
        n, shape, _ = profiles[index]
        if target != f'vector_graph_recall_{n}_{shape}_per_context':
            raise ValueError('vector quality corpus/target mapping changed: ' + target)
        registrations = [t for t in manifest.get('test', []) if t['name'] == target]
        if registrations != [dict(name=target, path=f'tests/{target}.rs', **{'required-features': ['test-seams']})]:
            raise ValueError('vector quality target registration changed: ' + target)
        wrapper = (base / f'{target}.rs').read_text()
        calls = re.findall(r'run_build_quality_profile\((.*?)\);', wrapper)
        expected_test = f'selected_f32_build_switches_pass_{n}_{shape}_per_context'
        approved = QUARANTINE_EXCEPTIONS.get((str((base / f'{target}.rs').relative_to(root)), expected_test))
        expected_ignore = f'#[ignore = "{approved}"]' if approved else None
        if (calls != [str(index)] or wrapper.count('#[test]') != 1
                or len(re.findall(r'#\[\s*ignore\b', wrapper)) != 1
                or (wrapper.count(expected_ignore) != 1 if expected_ignore else re.search(r'#\[\s*ignore\b', wrapper))
                or f'fn {expected_test}()' not in wrapper
                or wrapper.count('#![cfg(feature = "nightly-tests")]') != 1
                or wrapper.count('include!("vector/graph_route_profile_support.rs");') != 1
                or '#[allow(dead_code, unused_imports)]' not in wrapper
                or '#[path = "fixtures/maintained_search/mod.rs"]\nmod maintained_search;' not in wrapper):
            raise ValueError('vector quality wrapper/profile mapping changed: ' + target)
    calibration = by_id.get('vector-graph-recall-profile')
    if calibration != dict(id='vector-graph-recall-profile', family='vector default search-breadth calibration',
                           kind='engine-tests', targets=['vector_graph_route_profile_quality']):
        raise ValueError('vector calibration must retain its separate required slice')


def inventory_module(root):
    spec = importlib.util.spec_from_file_location('integration_inventory', root / 'scripts/ci-inventory.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_manifest(root=ROOT):
    routing_contract(root)
    manifest = json.loads((root / 'scripts/ci-integration-targets.json').read_text())
    if set(manifest) - {'normal_dedup'} != {'schema_version', 'slices', 'pr_jobs', 'normal_bundles'} or manifest['schema_version'] != 2:
        raise ValueError('unsupported integration manifest')
    rows = manifest['slices']
    if not isinstance(rows, list) or not rows:
        raise ValueError('empty integration manifest')
    ids = set()
    for row in rows:
        if (set(row) not in ({'id', 'family', 'kind', 'targets'},
                             {'id', 'family', 'kind', 'targets', 'logical_id', 'cache_parent'})
                or not re.fullmatch(r'[a-z][a-z0-9-]*', row['id']) or row['id'] in ids
                or row['kind'] not in KINDS or not isinstance(row['targets'], list)
                or not row['family'] or any(c in row['family'] for c in '\n\r|')
                or 'test-seams' in row['family']):
            raise ValueError('invalid or duplicate integration slice')
        ids.add(row['id'])
        if ('logical_id' in row) != ('cache_parent' in row):
            raise ValueError('partition metadata must bind logical identity and cache parent together')
        if any(not re.fullmatch(r'[a-zA-Z0-9_-]+', t) for t in row['targets']):
            raise ValueError('invalid target')
        if row['kind'] == 'engine-tests':
            if (not row['targets'] or any(t in ACCEPTANCE_TARGETS for t in row['targets'])
                    and row['targets'] not in ([target] for target in ACCEPTANCE_TARGETS)):
                raise ValueError('test-profile acceptance must be a separate slice')
        elif row['kind'] == 'release-acceptance':
            if row['targets'] not in ([target] for target in ACCEPTANCE_TARGETS):
                raise ValueError('release-profile must run one acceptance partition exactly')
        elif row['targets']:
            raise ValueError('Cargo default/extra classes cannot narrow their targets')
    by_id = {row['id']: row for row in rows}
    for slice_id, (family, target) in SCHEMA_REPLAY_SLICES.items():
        row = by_id.get(slice_id)
        targets = [target]
        if slice_id == 'engine-family-9':
            targets.append('custody_schema_replay_bounded_contract')
        if row != dict(id=slice_id, family=family, kind='engine-tests', targets=targets):
            raise ValueError('schema replay must retain three separate named slices: ' + slice_id)
    for slice_id, (family, target) in SYNC_SLICES.items():
        if by_id.get(slice_id) != dict(id=slice_id, family=family, kind='engine-tests', targets=[target]):
            raise ValueError('sync requires two separate named slices: ' + slice_id)
    vector_profile_contract(root, by_id)
    acceptance_parts = {
        'acceptance-test': {
            ('acceptance-test', 'acceptance'),
            ('acceptance-long-running-test', 'acceptance_long_running'),
            ('acceptance-sync-test', 'acceptance_sync')},
        'acceptance-release': {
            ('acceptance-release', 'acceptance'),
            ('acceptance-long-running-release', 'acceptance_long_running'),
            ('acceptance-sync-release', 'acceptance_sync')},
    }
    for logical_id, expected_parts in acceptance_parts.items():
        actual = {(row['id'], row['targets'][0]) for row in rows
                  if row.get('logical_id') == logical_id}
        if actual != expected_parts or any(row.get('cache_parent') != logical_id for row in rows
                                           if row.get('logical_id') == logical_id):
            raise ValueError('acceptance partition or cache-parent binding changed: ' + logical_id)
    vector_parts = {
        ('engine-family-11', ('vector_a_find_decodes_only_the_rows_it_returns',
                              'custody_delivery_manifest')),
        ('engine-family-13', ('vector_a_find_over_many_contexts_spends_one_search_budget',)),
        ('engine-family-14', ('vector_one_search_covers_every_layer_on_both_routes',)),
    }
    actual_vector_parts = {(row['id'], tuple(row['targets'])) for row in rows
                           if row.get('logical_id') == 'engine-family-11'}
    if actual_vector_parts != vector_parts or any(
            row.get('cache_parent') != 'engine-family-11' for row in rows
            if row.get('logical_id') == 'engine-family-11'):
        raise ValueError('vector physical partitions or cache-parent binding changed')
    logical_ids = [row.get('logical_id', row['id']) for row in rows]
    if len(rows) != 36 or len(set(logical_ids)) != 30:
        raise ValueError('physical/logical qualification inventory changed')
    for kind in KINDS - {'engine-tests', 'release-acceptance'}:
        if sum(row['kind'] == kind for row in rows) != 1:
            raise ValueError('missing or duplicate integration command class: ' + kind)
    module = inventory_module(root)
    workspace = tomllib.loads((root / 'Cargo.toml').read_text())['workspace']
    paths = {}
    for member in workspace['members']:
        for directory in root.glob(member):
            path = directory / 'Cargo.toml'
            paths[tomllib.loads(path.read_text())['package']['name']] = path
    redb = root / 'crates/contextdb-redb/Cargo.toml'
    paths[tomllib.loads(redb.read_text())['package']['name']] = redb
    documents = {name: tomllib.loads(path.read_text()) for name, path in paths.items()}
    workspace = dict(workspace, **{'manifest-root': root})
    closure = module.feature_closure('contextdb-engine', FEATURES.split(','), documents, paths, workspace)
    targets = module.target_inventory(documents['contextdb-engine'], paths['contextdb-engine'], 'test')
    expected = {'lib'} | {name for name, required in targets.items() if required <= closure}
    listed = [target for row in rows if row['kind'] == 'engine-tests' for target in row['targets']]
    if len(listed) != len(set(listed)) or set(listed) != expected:
        raise ValueError('integration test-profile target union mismatch (duplicate, missing or extra): '
                         f'missing={sorted(expected-set(listed))}, extra={sorted(set(listed)-expected)}')
    return rows


def metadata(row):
    kind = row['kind']
    features = '' if kind in {'workspace', 'redb'} else 'nightly-tests' if kind in OPTIONAL else FEATURES
    profile = 'release-profile' if kind in {'release-acceptance', 'production-release'} else 'test-profile'
    scope = 'engine' if kind not in {'workspace', 'redb'} else kind
    name = f'{scope} / {profile} / {row["family"]}'
    return dict(features=features, profile=profile, name=name,
                test_threads='1' if kind not in OPTIONAL and kind != 'examples'
                else 'not-run' if kind == 'examples' else os.environ.get('RUST_TEST_THREADS', 'default-parallel'))


def commands(row, root=ROOT, mode=None):
    """Keep default-target commands intact, including custom harnesses and doctests."""
    mode = gate_mode(mode)
    if mode == 'normal' and any(t in FULL_SCHEMA | FULL_CORPORA for t in row['targets']):
        raise ValueError('unrouted full-qualification target in normal mode')
    kind = row['kind']
    base = ['cargo', 'test', '--locked', '-p', 'contextdb-engine', '--features', metadata(row)['features']]
    if kind == 'engine-tests':
        for target in row['targets']:
            base += ['--lib', '--bins'] if target == 'lib' else ['--test', target]
        flags = ['--test-threads=1']
        if mode == 'normal' and 'sync_schema_transactions' in row['targets']:
            flags += ['--skip', OVERSIZED]
        if mode == 'normal' and row['id'] in LONG_RUNNING_PARTITIONS:
            long_running_contract(root)
            if row['targets'] != ['acceptance_long_running'] or row['cache_parent'] != 'acceptance-long-running-test':
                raise ValueError('long-running partition target/cache envelope changed')
            flags += (['--skip', LONG_RUNNING_CASES[2]] if 'transfer' in row['id']
                      else [LONG_RUNNING_CASES[2], '--exact'])
        result = [base + ['--'] + flags]
        if mode == 'normal':
            decision = runtime_decision(row, mode, root)
            if decision['dedup_sha'] and set(row['targets']) & set(NORMAL_DEDUP_TARGETS):
                raise ValueError('published dedup selection contains duplicate execution targets')
            if decision['dedup_sha'] and row['id'] == 'engine-family-1':
                compile_only = ['cargo', 'test', '--locked', '-p', 'contextdb-engine', '--features', FEATURES]
                for target in NORMAL_DEDUP_TARGETS:
                    compile_only += ['--test', target]
                result.insert(0, compile_only + ['--no-run'])
        # Preserve the original PR feature tuples for relocated full fixtures,
        # as well as the complete original nightly-feature commands above.
        if mode == 'full':
            qualification = [t for t in row['targets'] if t in FULL_SCHEMA]
            if qualification or 'sync_schema_transactions' in row['targets']:
                companion = ['cargo', 'test', '--locked', '-p', 'contextdb-engine',
                             '--features', 'test-seams,iroh']
                for target in qualification or ['sync_schema_transactions']:
                    companion += ['--test', target]
                companion += ['--', '--test-threads=1']
                if not qualification:
                    companion += [OVERSIZED, '--exact']
                result.append(companion)
        return result
    if kind == 'release-acceptance':
        return [base + ['--release', '--test', row['targets'][0], '--', '--test-threads=1']]
    if kind == 'examples':
        return [base + ['--examples', '--no-run']]
    if kind == 'doctests':
        return [base + ['--doc', '--', '--test-threads=1']]
    if kind == 'workspace':
        return [['cargo', 'test', '--locked', '--workspace']]
    if kind == 'redb':
        target = Path(os.environ.get('CARGO_TARGET_DIR', str(root / 'target')))
        return [['cargo', 'test', '--manifest-path', 'crates/contextdb-redb/Cargo.toml',
                 '--target-dir', str(target / 'redb'), '--locked']]
    if kind == 'production':
        return [base]
    if kind == 'production-release':
        return [base + ['--release', '--test', target] for target in ACCEPTANCE_TARGETS]
    raise ValueError('unknown command class')


def boolean(value):
    if value not in {'true', 'false'}:
        raise ValueError('integration mode must be true or false')
    return value == 'true'


def matrix(rows, run_default, mode=None, decision=None):
    mode = gate_mode(mode)
    decision = plan_decision(mode, run_default) if decision is None else validate_decision(decision, mode, run_default)
    return {'include': [dict(id=row['id'], name=('normal correctness' if mode == 'normal' else 'full qualification') + ' / ' + metadata(row)['name'], **decision)
                        for row in active_rows(rows, run_default, mode, decision)]}


ENVELOPE_FIELDS = {'kind', 'id', 'name', 'pr', 'integration'}
def pr_inventory(root=ROOT):
    manifest = json.loads((root / 'scripts/ci-integration-targets.json').read_text())
    rows = manifest.get('pr_jobs')
    if manifest.get('schema_version') != 2 or not isinstance(rows, list) or len(rows) != 26:
        raise ValueError('invalid canonical PR inventory')
    ids = [row.get('shard') for row in rows if isinstance(row, dict)]
    if len(ids) != 26 or len(set(ids)) != 26:
        raise ValueError('missing or duplicate canonical PR job')
    parents = {row['shard']: row.get('cache_parent', row['shard']) for row in rows}
    expected = {
        'contextdb-engine-test-seams-4': {
            'contextdb-engine-test-seams-4', 'contextdb-engine-test-seams-13',
            'contextdb-engine-test-seams-14'},
        'contextdb-engine-test-seams-11': {
            'contextdb-engine-test-seams-11', 'contextdb-engine-test-seams-16',
            'contextdb-engine-test-seams-17'},
    }
    for parent, parts in expected.items():
        if {shard for shard, bound in parents.items() if bound == parent} != parts:
            raise ValueError('reviewed PR partition changed: ' + parent)
    if any(parent not in expected and parent != shard for shard, parent in parents.items()):
        raise ValueError('unreviewed PR cache parent binding')
    if len(set(parents.values())) != 22:
        raise ValueError('logical PR inventory changed')
    return rows


def combined_matrix(rows, enabled, decision, root=ROOT):
    validate_decision(decision, 'normal', False, root)
    envelopes = [dict(kind='pr', id='pr-' + row['shard'],
                      name='normal correctness / ' + row['name'], pr=row, integration=None)
                 for row in pr_inventory(root)]
    if enabled:
        envelopes += [dict(kind='integration', id='integration-' + row['id'],
                           name=row['name'], pr=None, integration=row)
                      for row in matrix(rows, False, 'normal', decision)['include']]
    validate_combined({'include': envelopes}, rows, enabled, decision, root)
    return {'include': envelopes}


NORMAL_BUNDLES_SHA256 = '14e6e7c3e0958b6e0ab5c435879830fa09fac2eec1de48560304aeb3d5a7584e'
SLOT_FIELDS = ENVELOPE_FIELDS | {'cache_key'}


def _bundle_slots(value, enabled, decision, root=ROOT):
    """Expand the reviewed bundle layout without changing a canonical row payload."""
    validate_combined(value, rows=load_manifest(root), enabled=enabled, decision=decision, root=root)
    manifest = json.loads((root / 'scripts/ci-integration-targets.json').read_text())
    definition = manifest.get('normal_bundles')
    if (not isinstance(definition, dict)
            or set(definition) != {'schema_version', 'bundles', 'fallback_additions'}
            or definition['schema_version'] != 1):
        raise ValueError('invalid normal bundle manifest')
    encoded = json.dumps(definition, sort_keys=True, separators=(',', ':')).encode()
    if hashlib.sha256(encoded).hexdigest() != NORMAL_BUNDLES_SHA256:
        raise ValueError('normal bundle manifest differs from reviewed design')
    bundles = definition['bundles']
    additions = definition['fallback_additions']
    if (not isinstance(bundles, list) or len(bundles) != 16
            or any(not isinstance(bundle, list) or not 1 <= len(bundle) <= 4 for bundle in bundles)
            or any(not isinstance(row_id, str) for bundle in bundles for row_id in bundle)
            or not isinstance(additions, dict)
            or additions != {'integration-engine-family-10': 12,
                              'integration-engine-family-11': 12,
                              'integration-engine-family-13': 4,
                              'integration-engine-family-14': 4}):
        raise ValueError('invalid reviewed normal bundle layout')
    planned = [list(bundle) for bundle in bundles]
    if enabled and not decision['dedup_sha']:
        for row_id, one_based_bundle in additions.items():
            planned[one_based_bundle - 1].append(row_id)
    if any(len(bundle) > 4 for bundle in planned):
        raise ValueError('normal bundle exceeds the four reviewed constituent slots')
    flattened = [row_id for bundle in planned for row_id in bundle]
    if len(flattened) != len(set(flattened)):
        raise ValueError('normal bundles contain duplicate canonical rows')
    by_id = {row['id']: row for row in value['include']}
    selected = [[by_id[row_id] for row_id in bundle if row_id in by_id] for bundle in planned]
    selected = [bundle for bundle in selected if bundle]
    expanded = [row for bundle in selected for row in bundle]
    if len(expanded) != len(by_id) or {row['id'] for row in expanded} != set(by_id):
        raise ValueError('normal bundles are not the exact canonical row union')
    return selected


def bundle_matrix(value, rows, enabled, decision, root=ROOT):
    validate_combined(value, rows, enabled, decision, root)
    result = []
    for index, bundle in enumerate(_bundle_slots(value, enabled, decision, root), 1):
        slots = []
        for envelope in bundle:
            key = ('test-' + envelope['pr'].get('cache_parent', envelope['pr']['shard'])
                   if envelope['kind'] == 'pr'
                   else 'integration-' + envelope['integration'].get(
                       'cache_parent', envelope['integration']['id'])) + '-debug0-jobs2-test'
            if envelope['kind'] == 'integration' and envelope['integration']['id'] in LONG_RUNNING_PARTITIONS:
                key = 'integration-acceptance-long-running-test-debug0-jobs2-test'
            slots.append(dict(envelope, cache_key=key))
        name = f'bundle {index:02d} / ' + ' + '.join(
            slot['name'].removeprefix('normal correctness / ') for slot in slots)
        result.append(dict(id=f'normal-bundle-{index:02d}', name=name,
                           slot1=slots[0], slot2=slots[1] if len(slots) > 1 else None,
                           slot3=slots[2] if len(slots) > 2 else None,
                           slot4=slots[3] if len(slots) > 3 else None))
    return {'include': result}


def validate_bundle_matrix(value, combined, rows, enabled, decision, root=ROOT):
    expected = bundle_matrix(combined, rows, enabled, decision, root)
    if value != expected:
        raise ValueError('published normal bundle matrix differs from canonical expansion')


def validate_combined(value, rows, enabled, decision, root=ROOT):
    # The outer normal envelope is intentionally empty for integration rows when
    # impact classifies them off, but its published decision is never optional.
    # Validate before deriving that empty partition so a stale/tampered plan
    # cannot be accepted merely because no slice will execute.
    validate_decision(decision, 'normal', False, root)
    if not isinstance(value, dict) or set(value) != {'include'} or not isinstance(value['include'], list):
        raise ValueError('invalid combined matrix envelope')
    expected_pr = {'pr-' + row['shard']: row for row in pr_inventory(root)}
    expected_integration = {row['id']: row for row in matrix(rows, False, 'normal', decision)['include']} if enabled else {}
    expected_ids = set(expected_pr) | {'integration-' + key for key in expected_integration}
    found = set()
    for envelope in value['include']:
        if not isinstance(envelope, dict) or set(envelope) != ENVELOPE_FIELDS or envelope['id'] in found:
            raise ValueError('invalid or duplicate combined matrix envelope')
        found.add(envelope['id'])
        if envelope['kind'] == 'pr' and envelope['id'] in expected_pr:
            row = expected_pr[envelope['id']]
            if envelope != dict(kind='pr', id=envelope['id'], name='normal correctness / ' + row['name'], pr=row, integration=None):
                raise ValueError('PR matrix payload drift')
        elif envelope['kind'] == 'integration' and envelope['id'].removeprefix('integration-') in expected_integration:
            row = expected_integration[envelope['id'].removeprefix('integration-')]
            if envelope != dict(kind='integration', id=envelope['id'], name=row['name'], pr=None, integration=row):
                raise ValueError('integration matrix payload drift')
        else:
            raise ValueError('unknown combined matrix envelope')
    if found != expected_ids:
        raise ValueError('combined matrix missing or extra jobs')


def normal_required(needs, rows, enabled, published, published_bundles, decision, root=ROOT):
    expected = {'impact', 'test-bundles'}
    if not isinstance(needs, dict) or set(needs) != expected:
        raise ValueError('normal validator needs exact impact and three dispatch jobs')
    if any(needs[name].get('result') != 'success' for name in expected):
        raise ValueError('normal planning or bundle execution did not succeed')
    validate_combined(published, rows, enabled, decision, root)
    validate_bundle_matrix(published_bundles, published, rows, enabled, decision, root)


def required(needs, rows, enabled, run_default, mode=None):
    mode = gate_mode(mode)
    if not isinstance(needs, dict) or set(needs) != {'plan', 'slice'} or needs['plan'].get('result') != 'success':
        raise ValueError('missing/failed/unexpected integration jobs')
    outputs = needs['plan'].get('outputs', {})
    decision = {key: outputs[key] for key in PLAN_FIELDS if key in outputs}
    validate_decision(decision, mode, run_default)
    if (outputs.get('enabled') != str(enabled).lower()
            or json.loads(outputs.get('matrix', 'null')) != matrix(rows, run_default, mode, decision)):
        raise ValueError('integration published plan does not match source/mode/decision inventory')
    if needs['slice'].get('result') != ('success' if enabled else 'skipped'):
        raise ValueError('required integration slices did not succeed (or unexpected skip)')


def workflow_contract(root=ROOT):
    """Narrow reviewed YAML shape; offline controls independently parse and mutate it."""
    text = (root / '.github/workflows/integration.yml').read_text()
    expected_jobs = ['plan', 'slice', 'result']
    if re.findall(r'^  ([\w-]+):$', text.split('\njobs:\n', 1)[1], re.M) != expected_jobs:
        raise ValueError('integration workflow job inventory changed')
    required_snippets = [
        '    needs: plan\n    if: needs.plan.outputs.enabled == \'true\'\n',
        '      fail-fast: false\n      matrix: ${{ fromJSON(needs.plan.outputs.matrix) }}',
        '    name: ${{ matrix.name }}', "    timeout-minutes: ${{ needs.plan.outputs.gate_mode == 'normal' && 30 || 330 }}",
        '    env:\n      CARGO_PROFILE_TEST_OPT_LEVEL: "1"\n      CARGO_PROFILE_TEST_DEBUG_ASSERTIONS: "true"\n      CARGO_PROFILE_TEST_OVERFLOW_CHECKS: "true"\n',
        '          CI_INTEGRATION_SLICE: ${{ matrix.id }}',
        "  CI_INTEGRATION_ENABLED: ${{ github.event_name == 'workflow_dispatch' || inputs.enabled }}",
        '  CI_INTEGRATION_DEFAULT: ${{ inputs.run_default }}',
        '  CI_GATE_MODE: ${{ inputs.gate_mode }}',
        '      gate_mode: ${{ steps.plan.outputs.gate_mode }}',
        '      dedup_sha: ${{ steps.plan.outputs.dedup_sha }}',
        '      dedup_reason: ${{ steps.plan.outputs.dedup_reason }}',
        '      source_sha: ${{ steps.plan.outputs.source_sha }}',
        '      source_fingerprint: ${{ steps.plan.outputs.source_fingerprint }}',
        '          CI_INTEGRATION_PLAN: ${{ toJSON(matrix) }}',
        '      matrix: ${{ steps.plan.outputs.matrix }}',
        '      enabled: ${{ steps.plan.outputs.enabled }}',
        '      run_default: ${{ steps.plan.outputs.run_default }}',
        '        run: bash scripts/verify integration "${CI_INTEGRATION_SLICE}"',
        '    needs: [plan, slice]\n    if: always()\n',
        '          CI_INTEGRATION_NEEDS: ${{ toJSON(needs) }}',
        '        run: python3 -B scripts/ci-integration.py required',
        '        run: python3 -B scripts/ci-integration.py plan',
        "    if: always()\n    runs-on: ubuntu-latest",
    ]
    for snippet in required_snippets:
        if text.count(snippet) != 1:
            raise ValueError('integration workflow lost guarded routing: ' + snippet)
    if ('continue-on-error:' in text or 'max-parallel:' in text
            or len(re.findall(r'^    if:', text, re.M)) != 2
            or len(re.findall(r'^        if:', text, re.M)) != 1):
        raise ValueError('unsupported integration failure/concurrency override')
    spec = importlib.util.spec_from_file_location('integration_required', root / 'scripts/ci-required.py')
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    top_permissions = re.search(r'^permissions:\n(.*?)(?=^\S)', text, re.M | re.S)
    if top_permissions is None or guard.block_fields(top_permissions[1], 2) != {'contents': 'read\n'}:
        raise ValueError('integration must remain read-only')
    fields = {'plan': {'name', 'runs-on', 'timeout-minutes', 'outputs', 'steps'},
              'slice': {'needs', 'if', 'name', 'runs-on', 'timeout-minutes', 'env', 'strategy', 'steps'},
              'result': {'name', 'needs', 'if', 'runs-on', 'timeout-minutes', 'steps'}}
    for name in expected_jobs:
        block = re.search(r'^  ' + name + r':\n(.*?)(?=^  [\w-]+:|\Z)', text, re.M | re.S)[1]
        if set(guard.block_fields(block, 4)) != fields[name]:
            raise ValueError('unsupported integration job execution metadata: ' + name)
    runs = re.findall(r'^        run: (.*)$', text, re.M)
    if runs != ['python3 -B scripts/ci-integration.py plan', 'bash scripts/ci-free-disk.sh',
                'sudo apt-get update && sudo apt-get install -y ripgrep',
                'bash scripts/verify integration "${CI_INTEGRATION_SLICE}"',
                'python3 -B scripts/ci-integration.py required']:
        raise ValueError('integration command execution changed')
    if (re.findall(r'^\s+shell: (.*)$', text, re.M) != ['bash']
            or text.count('  run:\n    shell: bash') != 1):
        raise ValueError('integration shell override')
    verify = (root / 'scripts/verify').read_text()
    if verify.count('exec python3 -B scripts/ci-integration.py run "$@"') != 1:
        raise ValueError('integration entrypoint delegation changed')


def capture(command, env, log):
    start = time.monotonic()
    with log.open('wb') as output:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for block in iter(lambda: process.stdout.read1(65536), b''):
            output.write(block)
            sys.stdout.buffer.write(block)
            sys.stdout.buffer.flush()
        status = process.wait()
    return status, time.monotonic() - start


def run_slice(row, out, mode=None):
    mode = gate_mode(mode)
    out.mkdir(parents=True, exist_ok=False)
    start = time.monotonic()
    try:
        runtime_decision(row, mode)
    except (ValueError, KeyError, TypeError) as error:
        (out / 'receipt.json').write_text(json.dumps(dict(exit_status=1, gate_mode=mode, slice=row,
            plan_decision=row.get('plan_decision'), refusal_reason=str(error), commands=[], targets=[]), indent=2) + '\n')
        return 1
    env = dict(os.environ, RUSTFLAGS='-D warnings')
    target = Path(env.get('CARGO_TARGET_DIR', str(ROOT / 'target')))
    if not target.is_absolute():
        target = ROOT / target
    env['CARGO_BIN_EXE_contextdb'] = str(target / 'debug/contextdb')
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    dirty = subprocess.check_output(['git', 'status', '--porcelain=v1', '-z'], cwd=ROOT)
    diff = subprocess.check_output(['git', 'diff', 'HEAD', '--binary', '--no-ext-diff'], cwd=ROOT)
    toolchain = subprocess.check_output(['rustc', '-Vv'], cwd=ROOT, text=True)
    receipt = dict(schema_version=1, gate_mode=mode,
                   plan_decision=row['plan_decision'],
                   normal_dedup=dict(sha256=row['plan_decision']['dedup_sha'], reason=row['plan_decision']['dedup_reason'],
                       fallback_targets=[] if row['plan_decision']['dedup_sha'] else list(NORMAL_DEDUP_TARGETS)) if mode == 'normal' else None,
                   dedup_execution_owner='PR test-seams,iroh' if mode == 'normal' else None, source_sha=sha, source_dirty=bool(dirty),
                   relocated_cases=([dict(case=OVERSIZED, destination="full qualification; both feature tuples")]
                                    if mode == "normal" and "sync_schema_transactions" in row["targets"] else []),
                   source_status_sha256=hashlib.sha256(dirty).hexdigest(),
                   source_diff_sha256=hashlib.sha256(diff).hexdigest(), toolchain=toolchain,
                   slice=row, companion_features=('test-seams,iroh' if mode == 'full' and
                       (set(row['targets']) & FULL_SCHEMA or 'sync_schema_transactions' in row['targets']) else None),
                   **metadata(row), rustflags=env['RUSTFLAGS'],
                   hosted_job_elapsed_seconds=None, hosted_queue_seconds=None,
                   hosted_timing_status='pending-terminal-Actions-job-metadata',
                   timing_boundary='local runner entry through receipt generation; excludes hosted checkout/toolchain/cache/artifacts',
                   commands=[], targets=[], counts_status='pending', exit_status=1)
    untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z'], cwd=ROOT)
    receipt['untracked_source_sha256'] = {
        os.fsdecode(path): hashlib.sha256((ROOT / os.fsdecode(path)).read_bytes()).hexdigest()
        for path in untracked.split(b'\0') if path and (ROOT / os.fsdecode(path)).is_file()}
    receipt['fixture_sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                 for path in sorted((ROOT / 'crates').glob('*/tests/**/*')) if path.is_file()}
    receipt['integration_manifest_sha256'] = hashlib.sha256((ROOT / 'scripts/ci-integration-targets.json').read_bytes()).hexdigest()
    receipt['hosted_identity'] = {key: os.environ.get(key) for key in [
        'GITHUB_SHA', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_JOB', 'GITHUB_REPOSITORY']}
    module = inventory_module(ROOT)
    status = 1
    try:
        build = ['cargo', 'build', '--locked', '-p', 'contextdb-cli', '-p', 'contextdb-server']
        status, seconds = capture(build, env, out / 'build.log')
        receipt['binary_build_seconds'] = seconds
        if status == 0:
            for index, command in enumerate(commands(row, mode=mode)):
                log = out / (f'{index}.doc.log' if row['kind'] == 'doctests' else f'{index}.log')
                status, seconds = capture(command, env, log)
                receipt['commands'].append(dict(argv=command, exit_status=status, cargo_command_seconds=seconds))
                entries = module.timing_receipt([log], metadata(row)['test_threads'])['targets']
                actual_features = command[command.index('--features') + 1] if '--features' in command else ''
                for entry in entries:
                    entry.update(features=actual_features, command_index=index)
                receipt['targets'].extend(entries)
                if status == 0 and mode == 'normal' and row['id'] in LONG_RUNNING_PARTITIONS:
                    long_running_counts(entries, log, row['id'])
                if status == 0 and '--no-run' not in command and 'sync_schema_transactions' in row['targets']:
                    sync_routing_counts(entries, mode, companion=index > 0)
                if status:
                    break
    except BaseException:
        status = status or 1
        raise
    finally:
        receipt['exit_status'] = status
        receipt['runner_elapsed_seconds'] = time.monotonic() - start
        receipt['counts_status'] = 'completed-target-summaries-only; incomplete/custom-harness counts unknown'
        for entry in receipt['targets']:
            entry['declared_count'] = sum(entry[k] for k in ['passed', 'failed', 'ignored', 'measured', 'filtered_out'])
            entry['runnable_count'] = entry['passed'] + entry['failed']
        (out / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        lines = [f'### {metadata(row)["name"]}', '',
                 '| Target | Profile | Features | Declared | Runnable | Cargo test seconds | Hosted job / queue seconds |',
                 '|---|---|---|---:|---:|---:|---|']
        for entry in receipt['targets']:
            lines.append(f'| {entry["target"]} | {receipt["profile"]} | {entry.get("features", receipt["features"]) or "Cargo defaults"} | '
                         f'{entry["declared_count"]} | {entry["runnable_count"]} | {entry["duration_seconds"]} | pending / pending |')
        if not receipt['targets']:
            lines.append(f'| {row["targets"] or row["kind"]} | {receipt["profile"]} | {receipt["features"] or "Cargo defaults"} | unknown | unknown | not available | pending / pending |')
        lines += ['', f'Gate mode: {mode}. Relocated cases: {receipt["relocated_cases"]}.', f'Exit: {status}. Runner seconds: {receipt["runner_elapsed_seconds"]:.3f}.',
                  receipt['timing_boundary'], receipt['counts_status'], '']
        (out / 'summary.md').write_text('\n'.join(lines))
        if os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
                summary.write('\n'.join(lines))
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['plan', 'required', 'normal-required', 'check', 'run', 'shard-plan', 'shard-receipt', 'dedup-targets', 'dedup-snapshot', 'regenerate-dedup'])
    parser.add_argument('slice', nargs='?')
    args = parser.parse_args()
    if args.action not in {'run', 'shard-plan', 'shard-receipt', 'dedup-snapshot', 'regenerate-dedup'} and args.slice:
        raise ValueError('slice selection is only supported by run')
    if args.action == 'dedup-targets':
        print(' '.join('--test ' + t for t in NORMAL_DEDUP_TARGETS))
        return 0
    if args.action in {'dedup-snapshot', 'regenerate-dedup'}:
        directory = Path(args.slice)
        if args.action == 'dedup-snapshot':
            directory.mkdir(parents=True, exist_ok=True)
            identity = dict(applicability=dedup_snapshot(ROOT), sources={t: literal_closure(ROOT, t) for t in NORMAL_DEDUP_TARGETS})
            (directory / 'identity.json').write_text(json.dumps(identity, indent=2) + '\n')
        else:
            regenerate_dedup(directory)
        return 0
    mode = gate_mode()
    rows = load_manifest()
    if args.action == 'shard-receipt':
        sync_routing_counts(json.loads(Path(args.slice).read_text())['targets'], mode)
        return 0
    if args.action == 'shard-plan':
        lines = (ROOT / 'scripts/ci-shard-targets.txt').read_text().splitlines()
        matches = [line.split(': ', 1)[1].split() for line in lines
                   if line.startswith(str(args.slice) + ': ')]
        if len(matches) != 1:
            raise ValueError('unknown/duplicate split shard')
        targets = [t for t in matches[0] if mode == 'full' or t not in FULL_SCHEMA]
        if not targets:
            raise ValueError('routing must not produce an empty shard')
        skip = OVERSIZED if mode == 'normal' and 'sync_schema_transactions' in targets else ''
        print(' '.join(targets) + '|' + skip)
        return 0
    workflow_contract()
    enabled = boolean(os.environ.get('CI_INTEGRATION_ENABLED', 'true'))
    run_default = boolean(os.environ.get('CI_INTEGRATION_DEFAULT', 'true'))
    if args.action == 'check':
        print(json.dumps(matrix(rows, run_default, mode), indent=2))
    elif args.action == 'plan':
        decision = plan_decision(mode, run_default)
        integration_matrix = matrix(rows, run_default, mode, decision)
        outputs = dict(matrix=json.dumps(integration_matrix, separators=(',', ':')),
                       enabled=str(enabled).lower(), **decision)
        if mode == 'normal':
            combined = combined_matrix(rows, enabled, decision)
            bundles = bundle_matrix(combined, rows, enabled, decision)
            outputs['combined_matrix'] = json.dumps(combined, separators=(',', ':'))
            outputs['bundle_matrix'] = json.dumps(bundles, separators=(',', ':'))
        with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
            for key, value in outputs.items():
                output.write(f'{key}={value}\n')
        if not enabled:
            reason = os.environ.get('IMPACT_REASON', '')
            if not reason:
                raise ValueError('inactive integration requires a classified skip reason')
            print('Extended integration deliberately not required: ' + reason)
            print('Path classes: ' + os.environ.get('PATH_CLASSES', '{}'))
            qualification = os.environ.get('PERFORMANCE_QUALIFICATION', '')
            label = 'not affected' if qualification == 'unaffected' else 'measurement pending'
            print('Performance measurement: ' + label + ' (not a qualification and not a required gate)')
            print('All original PR checks remain required.')
            if os.environ.get('GITHUB_STEP_SUMMARY'):
                with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
                    summary.write('Extended integration deliberately not required: ' + reason + '\n')
    elif args.action == 'required':
        required(json.loads(os.environ['CI_INTEGRATION_NEEDS']), rows, enabled, run_default, mode)
        print('All required integration slices succeeded' if enabled else 'Classified integration skip; original PR checks remain required')
    elif args.action == 'normal-required':
        if mode != 'normal' or run_default:
            raise ValueError('normal validator requires normal/false')
        decision = {key: os.environ[key.upper()] for key in PLAN_FIELDS}
        normal_required(json.loads(os.environ['CI_INTEGRATION_NEEDS']), rows, enabled,
                        json.loads(os.environ['CI_COMBINED_MATRIX']),
                        json.loads(os.environ['CI_BUNDLE_MATRIX']), decision)
        print('Normal mixed matrix and published routing succeeded')
    else:
        import tempfile
        base = Path(os.environ.get('CI_INTEGRATION_OUT', str(ROOT / 'target/ci-integration-out')))
        base.mkdir(parents=True, exist_ok=True)
        invocation = Path(tempfile.mkdtemp(prefix='run-', dir=base))
        try:
            raw = os.environ.get('CI_INTEGRATION_PLAN')
            if not raw and os.environ.get('GITHUB_ACTIONS') == 'true':
                raise ValueError('hosted slice missing published matrix decision')
            published = json.loads(raw) if raw is not None else None
            decision = {key: published[key] for key in PLAN_FIELDS if key in published} if published is not None else plan_decision(mode, run_default)
            validate_decision(decision, mode, run_default)
            if published is not None:
                expected = [x for x in matrix(rows, run_default, mode, decision)['include'] if x['id'] == args.slice]
                if expected != [published]:
                    raise ValueError('slice matrix source/mode/SHA/inventory does not match published decision')
            selected = active_rows(rows, run_default, mode, decision)
        except (ValueError, KeyError, TypeError) as error:
            (invocation / 'receipt.json').write_text(json.dumps(dict(exit_status=1, refusal_reason=str(error), commands=[], targets=[]), indent=2) + '\n')
            return 1
        if args.slice:
            selected = [row for row in selected if row['id'] == args.slice]
            if not selected:
                raise ValueError('unknown or inactive integration slice')
        if not enabled:
            raise ValueError('run cannot silently skip disabled integration')
        statuses = [run_slice(row, invocation / row['id'], mode) for row in selected]
        return next((status for status in statuses if status), 0)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        print(f'::error::integration: {error}', file=sys.stderr)
        sys.exit(1)
