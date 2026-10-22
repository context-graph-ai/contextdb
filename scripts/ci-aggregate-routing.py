#!/usr/bin/env python3
"""Verify shared fixture ownership before routing aggregate test binaries."""
import hashlib
import json
from pathlib import Path
import re
import tomllib

CERTIFICATE_SHA256 = '905ca74e8df2fe59d498e063df45722a492e87d1dbf4a2b791ce0f61b4474fc5'
AGGREGATES = ('custody', 'vector')
FULL_ONLY_RULE = {'module': 'schema_replay_is_proportional_to_affected_tables', 'source': 'crates/contextdb-engine/tests/schema/schema_replay_is_proportional_to_affected_tables.rs', 'normal_owner': 'custody_schema_replay_bounded_contract', 'full_owner': 'custody_schema_replay_is_proportional_to_affected_tables', 'full_skip_owner': 'custody_schema_replay_bounded_contract'}
PROFILES = (frozenset(('test-seams', 'iroh')),
            frozenset(('nightly-tests', 'test-seams', 'iroh')))
PATH_MODULE = re.compile(r'#\[path\s*=\s*"([^"]+)"\]\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*;')


def registrations(root):
    data = tomllib.loads((root / 'crates/contextdb-engine/Cargo.toml').read_text())
    return {item['name']: item for item in data['test']}


def modules(root, path):
    text = path.read_text()
    result = []
    for match in PATH_MODULE.finditer(text):
        before = text[:match.start()].splitlines()
        attributes = []
        while before and before[-1].strip().startswith('#['):
            attributes.insert(0, before.pop().strip())
        result.append(dict(name=match[2], path=(path.parent / match[1]).resolve().relative_to(root.resolve()).as_posix(),
                           cfg=[a for a in attributes if a.startswith('#[cfg(')]))
    return result


def root_cfg(path):
    return re.findall(r'(?m)^#!\[cfg\([^\n]+', path.read_text())


def closure(root, path, target):
    pending = [path.resolve()]
    result = {}
    while pending:
        source = pending.pop()
        name = source.relative_to(root.resolve()).as_posix()
        if name in result:
            continue
        text = source.read_text()
        if re.search(r'include!\s*\(\s*[^"\s]', text):
            raise ValueError('dynamic aggregate helper needs review: ' + name)
        result[name] = text
        explicit = list(PATH_MODULE.finditer(text))
        stripped = text
        for match in explicit:
            pending.append((source.parent / match[1]).resolve())
        for match in reversed(explicit):
            stripped = stripped[:match.start()] + stripped[match.end():]
        for child in re.findall(r'include!\s*\(\s*"([^"]+)"\s*\)', stripped):
            pending.append((source.parent / child).resolve())
        for child in re.findall(r'(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*;', stripped):
            base = source.parent if source.name in ('mod.rs', target + '.rs') else source.parent / source.stem
            choices = [base / (child + '.rs'), base / child / 'mod.rs']
            matches = [p for p in choices if p.is_file()]
            if len(matches) != 1:
                raise ValueError('unresolved aggregate helper: ' + name + '::' + child)
            pending.append(matches[0].resolve())
    return result


def semantics(root, aggregate, owner, shared, records, cache):
    def cached_closure(path, target):
        key = (path.resolve(), target)
        if key not in cache:
            cache[key] = closure(root, path, target)
        return cache[key]

    base = root / 'crates/contextdb-engine'
    aggregate_path = base / records[aggregate]['path']
    owner_path = base / records[owner]['path']
    aggregate_modules = modules(root, aggregate_path)
    owner_modules = modules(root, owner_path)
    found = [m for m in owner_modules if m['path'] == shared['path']]
    if found != [shared] or root_cfg(aggregate_path) != root_cfg(owner_path):
        raise ValueError('aggregate wrapper cfg or namespace differs: ' + aggregate + '/' + owner + '/' + shared['name'])
    for profile in PROFILES:
        if any(not set(records[target].get('required-features', ())) <= profile
               for target in (aggregate, owner)):
            raise ValueError('aggregate owner has a different feature envelope: ' + owner)
    for target in (aggregate, owner):
        target_path = base / records[target]['path']
        for name, text in cached_closure(target_path, target).items():
            if re.search(r'#\s*\[\s*global_allocator\b', text):
                raise ValueError('aggregate allocator semantics need review: ' + target + '/' + name)
    shared_files = cached_closure(root / shared['path'], Path(shared['path']).stem)
    root_references = sorted({name for text in shared_files.values()
                              for name in re.findall(r'\bcrate::([A-Za-z_]\w*)', text)})
    references = {}
    for name in root_references:
        left = [m for m in aggregate_modules if m['name'] == name]
        right = [m for m in owner_modules if m['name'] == name]
        if len(left) != 1 or left != right:
            raise ValueError('aggregate crate-root helper differs: ' + owner + '/' + name)
        references[name] = left[0]
    return dict(shared=shared, owner_required_features=records[owner].get('required-features', []),
                aggregate_required_features=records[aggregate].get('required-features', []),
                wrapper_cfg=root_cfg(aggregate_path), root_helpers=references,
                allocator='no test-crate override in either complete literal closure',
                feature_profiles=[sorted(p) for p in PROFILES])


def build_certificate(root):
    """Construct review data; runtime routing never silently refreshes this data."""
    records = registrations(root)
    manifest = json.loads((root / 'scripts/ci-integration-targets.json').read_text())
    routed = {t for row in manifest['slices'] if row['kind'] == 'engine-tests' for t in row['targets']}
    wrappers = {target: root / 'crates/contextdb-engine' / item['path']
                for target, item in records.items() if target in routed or target in AGGREGATES}
    declarations = {target: modules(root, path) for target, path in wrappers.items()}
    result = dict(schema_version=1, aggregates={})
    cache = {}
    for aggregate in AGGREGATES:
        entries = []
        for shared in declarations[aggregate]:
            owners = [target for target, entries in declarations.items()
                      if target not in AGGREGATES and any(m['path'] == shared['path'] for m in entries)]
            proofs = {owner: semantics(root, aggregate, owner, shared, records, cache) for owner in owners}
            entries.append(dict(**shared, owners=owners, proofs=proofs))
        result['aggregates'][aggregate] = dict(wrapper=records[aggregate]['path'], modules=entries)
    shared = next(entry for entry in result['aggregates']['custody']['modules']
                  if entry['name'] == FULL_ONLY_RULE['module'])
    if set(shared['owners']) != {FULL_ONLY_RULE['normal_owner'], FULL_ONLY_RULE['full_owner']}:
        raise ValueError('schema replay standalone ownership changed')
    shared_sources = closure(root, root / shared['path'], Path(shared['path']).stem)
    if any('UNRELATED_RESIDENT_ROWS' in text for text in shared_sources.values()):
        raise ValueError('schema replay wrappers now change the shared workload; review selection')
    result['full_only_rule'] = FULL_ONLY_RULE
    used = set(AGGREGATES) | {owner for item in result['aggregates'].values()
                             for entry in item['modules'] for owner in entry['owners']}
    result['wrapper_sha256'] = {target: hashlib.sha256(wrappers[target].read_bytes()).hexdigest()
                                for target in sorted(used)}
    sources = {name for files in cache.values() for name in files}
    sources.update(wrappers[target].resolve().relative_to(root.resolve()).as_posix() for target in used)
    sources.add('crates/contextdb-engine/Cargo.toml')
    result['source_sha256'] = {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
                               for name in sorted(sources)}
    return result


def contract(root):
    data = (root / 'scripts/ci-aggregate-routing.json').read_bytes()
    if hashlib.sha256(data).hexdigest() != CERTIFICATE_SHA256:
        raise ValueError('aggregate ownership certificate changed; review exact unique coverage')
    expected = json.loads(data)
    for name, checksum in expected['source_sha256'].items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != checksum:
            raise ValueError('aggregate fixture source changed; review cfg, allocator and ownership: ' + name)
    if expected['schema_version'] != 1 or set(expected['aggregates']) != set(AGGREGATES):
        raise ValueError('aggregate ownership schema or target inventory changed')
    records = registrations(root)
    rows = json.loads((root / 'scripts/ci-integration-targets.json').read_text())['slices']
    routed = {t for row in rows if row['kind'] == 'engine-tests' for t in row['targets']}
    declarations = {}
    def declaration(target):
        if target not in declarations:
            declarations[target] = modules(root, root / 'crates/contextdb-engine' / records[target]['path'])
        return declarations[target]
    for target, item in expected['aggregates'].items():
        actual = declaration(target)
        recorded = [{key: entry[key] for key in ('name', 'path', 'cfg')} for entry in item['modules']]
        if recorded != actual:
            raise ValueError('aggregate unique module inventory is missing, duplicated or renamed: ' + target)
        for entry in item['modules']:
            shared = {key: entry[key] for key in ('name', 'path', 'cfg')}
            actual_owners = [owner for owner in records if owner in routed and owner not in AGGREGATES
                             and any(m['path'] == entry['path'] for m in declaration(owner))]
            if entry['owners'] != actual_owners:
                raise ValueError('aggregate unique coverage is missing or duplicates an existing owner')
            if len(entry['owners']) != len(set(entry['owners'])) or set(entry['proofs']) != set(entry['owners']):
                raise ValueError('aggregate module has duplicate or missing owner proofs')
            for owner in entry['owners']:
                if owner in AGGREGATES or owner not in expected['wrapper_sha256'] or shared not in declaration(owner):
                    raise ValueError('aggregate module was assigned to an owner that does not compile it')
        names = [entry['name'] for entry in item['modules']]
        if len(names) != len(set(names)) or not any(not entry['owners'] for entry in item['modules']):
            raise ValueError('aggregate unique coverage is missing or duplicated: ' + target)
    if expected.get('full_only_rule') != FULL_ONLY_RULE:
        raise ValueError('schema replay normal/full standalone ownership changed')
    routes_contract(root)
    return expected


ROUTES = {
    'a_row_pays_for_its_partition_key_once': ('engine-family-8', 'contextdb-engine-test-seams-8'),
    'cdb120_partition_insert_cost': ('engine-family-8', 'contextdb-engine-test-seams-8'),
    'custody': ('engine-family-12', 'contextdb-engine-test-seams-12'),
    'float_equality_follows_postgresql': ('engine-family-2', 'contextdb-engine-test-seams-2'),
    'in_transaction_count_stays_inside_its_memory': ('engine-family-1', 'contextdb-engine-test-seams-1'),
    'native_backing_allocator': ('engine-family-1', 'contextdb-engine-test-seams-1'),
    'native_terminal_allocations': ('engine-family-1', 'contextdb-engine-test-seams-1'),
    'seed_commit_memory': ('engine-family-3', None),
    'tied_index_probes_stay_inside_the_read_limits': ('engine-family-4', 'contextdb-engine-test-seams-5'),
    'vector': ('engine-family-12', 'contextdb-engine-test-seams-12'),
    'writecost_cycle_bound': ('engine-family-4', 'contextdb-engine-test-seams-5'),
    'writecost_planner': ('engine-family-2', 'contextdb-engine-test-seams-2'),
    'writecost_planner_reversed': ('engine-family-2', 'contextdb-engine-test-seams-2'),
    'writecost_reclaim': ('engine-family-4', 'contextdb-engine-test-seams-5'),
}


def routes_contract(root):
    rows = json.loads((root / 'scripts/ci-integration-targets.json').read_text())['slices']
    shards = {}
    for line in (root / 'scripts/ci-shard-targets.txt').read_text().splitlines():
        if not line.strip() or line.startswith('#'):
            continue
        name, separator, items = line.partition(': ')
        if not separator or name in shards:
            raise ValueError('duplicate or invalid aggregate shard mapping')
        shards[name] = items.split()
    for target, (slice_id, shard_id) in ROUTES.items():
        owners = [row['id'] for row in rows if row['kind'] == 'engine-tests'
                  for item in row['targets'] if item == target]
        shard_owners = [name for name, items in shards.items() for item in items if item == target]
        if owners != [slice_id] or shard_owners != ([] if shard_id is None else [shard_id]):
            raise ValueError('registered fixture lost its exact integration/shard owner: ' + target)


def skip_prefixes(root, target):
    data = contract(root)
    if target == FULL_ONLY_RULE['full_skip_owner']:
        return [FULL_ONLY_RULE['module'] + '::']
    if target not in AGGREGATES:
        raise ValueError('unknown isolated target: ' + target)
    return [entry['name'] + '::' for entry in data['aggregates'][target]['modules'] if entry['owners']]


def validate_command(root, target, command):
    prefixes = skip_prefixes(root, target)
    if (command.count('--test') != 1 or command[command.index('--test') + 1] != target
            or '--no-run' in command or '--' not in command
            or '--features' not in command
            or frozenset(command[command.index('--features') + 1].split(',')) not in PROFILES):
        raise ValueError('aggregate must run alone with its unchanged feature envelope: ' + target)
    expected = ['--test-threads=1']
    for prefix in prefixes:
        expected += ['--skip', prefix]
    if command[command.index('--') + 1:] != expected:
        raise ValueError('aggregate command lost unique coverage or duplicates an existing owner: ' + target)
