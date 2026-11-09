#!/usr/bin/env python3
"""Bounded candidate selection audit. No Cargo invocation and no runtime pass claim."""
import json
import os
from pathlib import Path
import re
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'scripts/ci-continuity-inventory.json'
REQUIRED = {'ci': ('required', ['lint', 'lint-macos', 'test-builds', 'test-bundles', 'build', 'features',
                                'installed-smoke', 'cross-compile', 'impact', 'integration']),
            'rc2-tests': ('summary', ['lint', 'test'])}


def demand(condition, message):
    if not condition:
        raise ValueError(message)


def aggregate(expected, results):
    demand(set(results) == set(expected), 'missing or unexpected correctness job')
    demand(all(isinstance(results[k], dict) and results[k].get('result') == 'success'
               for k in expected), 'every correctness job must succeed (skipped is failure)')


def attribute_end(lines, start, column):
    """Index of the line that closes the attribute opening at lines[start][column] (string-literal aware)."""
    depth, quoted, escaped = 0, False, False
    for index in range(start, len(lines)):
        for char in (lines[index][column:] if index == start else lines[index]):
            if quoted:
                if escaped:
                    escaped = False
                elif char == '\\':
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
            elif char == '[':
                depth += 1
            elif char == ']':
                depth -= 1
                if depth == 0:
                    return index
    # A mark that never closes is not an attribute (source text inside a string or a
    # comment); it stays a one-line declaration exactly as before.
    return start


def ignore_declaration(lines, index):
    """The ignore declaration opened on lines[index], or None.

    A one-line attribute is recorded as its stripped line, as before. An
    attribute that continues onto later lines (rustfmt wraps a long cfg_attr)
    is read whole, so a wrapped conditional ignore cannot drop out of the
    register.
    """
    text = lines[index]
    marks = [text.index(mark) for mark in ('#[ignore', '#[cfg_attr') if mark in text]
    if not marks:
        return None
    one_line = '#[ignore' in text or '#[cfg_attr' in text and 'ignore' in text
    end = attribute_end(lines, index, min(marks))
    if end == index:
        return text.strip() if one_line else None
    whole = ' '.join([text[min(marks):].strip()] + [line.strip() for line in lines[index + 1:end + 1]])
    whole = re.sub(r',?\s+\)', ')', re.sub(r'\(\s+', '(', whole))
    return whole if re.search(r'\bignore\b', whole) else None


def workflows(root):
    import yaml
    pending = ['ci', 'rc2-tests', 'nightly']
    result = {}
    while pending:
        name = pending.pop(0)
        if name in result:
            continue
        flow = yaml.load((root / f'.github/workflows/{name}.yml').read_text(), Loader=yaml.BaseLoader)
        result[name] = flow
        for job in flow['jobs'].values():
            reference = job.get('uses', '')
            if reference.startswith('./.github/workflows/'):
                pending.append(Path(reference).stem)
    return result


def runs(job):
    return [s['run'] for s in job.get('steps', []) if 'run' in s]


def validate_workflows(w):
    # The complete reviewed topology is pinned, including all commands, filters,
    # feature tuples, limits, conditional routes, and fork/mirror controls.
    expected = json.loads(DATA.read_text())['workflow_structure']
    demand(w == expected, 'workflow topology inventory mismatch')
    demand(set(w) == {'ci', 'rc2-tests', 'nightly', 'integration', 'build'}, 'workflow shape')
    for name, (gate, dependencies) in REQUIRED.items():
        flow = w[name]
        demand({'main', 'dev'} <= set(flow['on']['push']['branches']), name + ' push coverage')
        if name == 'ci':
            demand({'main', 'dev'} <= set(flow['on']['pull_request']['branches']), name + ' PR coverage')
        demand('workflow_dispatch' in flow['on'], name + ' dispatch missing')
        demand(flow['permissions'] == {'contents': 'read'}, name + ' permissions')
        jobs = flow['jobs']
        extra = ['development-binaries'] if name == 'ci' else []
        demand(set(jobs) == set(dependencies + [gate] + extra), name + ' correctness job set changed')
        g = jobs[gate]
        demand(g['needs'] == dependencies and g['if'] == 'always()', name + ' aggregate dependencies')
        command = ('python3 scripts/ci-required.py' if name == 'ci' else
                   'python3 scripts/ci-continuity.py aggregate ' + ' '.join(dependencies))
        demand(any(command in run.splitlines() for run in runs(g)), name + ' aggregate command absent')
    ci = w['ci']['jobs']
    demand(ci['test-bundles']['strategy']['matrix'] ==
           '${{ fromJSON(needs.impact.outputs.bundle_matrix) }}', 'bundle matrix shape')
    demand(ci['test-bundles']['strategy']['fail-fast'] == 'false', 'bundle fail-fast')
    demand(ci['cross-compile']['strategy']['matrix']['include'] == [
        {'target': 'aarch64-unknown-linux-gnu', 'os': 'ubuntu-latest'}], 'Linux cross matrix')
    nightly = w['nightly']['jobs']['full-test']
    demand(nightly['uses'] == './.github/workflows/integration.yml' and nightly['with'] ==
           {'enabled': 'true', 'run_default': 'true', 'gate_mode': 'full'}, 'nightly full selection')
    shard = w['rc2-tests']['jobs']['test']
    demand(shard['timeout-minutes'] == '330' and shard['strategy']['fail-fast'] == 'false', 'shard execution limits')
    command = next(s['run'] for s in shard['steps'] if s.get('id') == 'run')
    demand('--include-ignored' not in command, 'ignored tests must stay ignored')
    return shard, command


def expand(features, selected):
    selected = set(selected) | set(features.get('default', []))
    while True:
        old = set(selected)
        for f in list(selected):
            selected.update(x for x in features.get(f, []) if x in features)
        if old == selected:
            return selected


def feature_jobs(root, rows):
    manifests = {m['package']['name']: m for p in sorted((root / 'crates').glob('*/Cargo.toml'))
                 for m in [tomllib.loads(p.read_text())]}
    contexts = [(r['shard'], r['crate'], r['features'].split(','), True) for r in rows]
    contexts += [('nightly/full-test', 'contextdb-engine', ['test-seams', 'iroh', 'nightly-tests'], True),
                 ('ci/installed-smoke', 'contextdb-server', ['production-smoke-driver'], False)]
    result = {}
    for job, crate, enabled, dev in contexts:
        selected = {crate: set(filter(None, enabled)) | {'default'}}
        while True:
            before = {c: set(v) for c, v in selected.items()}
            for c, values in list(selected.items()):
                m = manifests[c]
                fs = m.get('features', {})
                values.update(expand(fs, values))
                entries = [x for f in values for x in fs.get(f, [])]
                deps = dict(m.get('dependencies', {}))
                if dev:
                    # Cargo unifies dependency and dev-dependency feature requests.
                    for alias, d in m.get('dev-dependencies', {}).items():
                        prior = deps.get(alias, {})
                        deps[alias] = dict(d, features=list(set(d.get('features', [])) | set(prior.get('features', [])))) if isinstance(d, dict) and isinstance(prior, dict) else d
                for alias, d in deps.items():
                    if not isinstance(d, dict) or 'path' not in d:
                        continue
                    dependency = d.get('package', alias)
                    if dependency not in manifests:
                        continue
                    forwarded = [x.split('/', 1)[1] for x in entries if x.startswith(alias + '/')]
                    if d.get('optional') and not forwarded and 'dep:' + alias not in entries and alias not in values:
                        continue
                    requested = set(d.get('features', [])) | set(forwarded)
                    if d.get('default-features', True):
                        requested.add('default')
                    selected.setdefault(dependency, set()).update(requested)
            if selected == before:
                break
        for c, values in selected.items():
            for feature in values & manifests[c].get('features', {}).keys():
                result.setdefault((c, feature), []).append(job)
    return result


def inventory(root=ROOT):
    w = workflows(root)
    shard, command = validate_workflows(w)
    rows = shard['strategy']['matrix']['include']
    demand(len(rows) == 16 and len({r['shard'] for r in rows}) == 16, 'all 16 shards required')
    targets, features = [], []
    feature_selection = feature_jobs(root, rows)
    for path in sorted((root / 'crates').glob('*/Cargo.toml')):
        m = tomllib.loads(path.read_text())
        crate = m['package']['name']
        fs = m.get('features', {})
        selections = [(r['shard'], expand(fs, filter(None, r['features'].split(',')))) for r in rows if r['crate'] == crate]
        demand(selections, 'missing crate shard: ' + crate)
        if crate == 'contextdb-engine':
            selections.append(('nightly/full-test', expand(fs, ['test-seams', 'iroh', 'nightly-tests'])))
        if crate == 'contextdb-server':
            selections.append(('ci/installed-smoke', expand(fs, ['production-smoke-driver'])))
        for f, closure in fs.items():
            jobs = feature_selection.get((crate, f), [])
            features.append({'crate': crate, 'feature': f, 'declaration': closure, 'jobs': jobs, 'tiers': sorted({'nightly' if j.startswith('nightly/') else 'normal' for j in jobs}),
                             'status': 'selected; not executed by this audit' if jobs else 'not selected by existing command families; no coverage claimed'})
        for kind, directory in [('lib', 'src'), ('bin', 'src/bin'), ('test', 'tests'), ('bench', 'benches'), ('example', 'examples')]:
            explicit = m.get(kind, [])
            if isinstance(explicit, dict):
                explicit = [dict({'name': crate.replace('-', '_'), 'path': 'src/lib.rs'}, **explicit)]
            discovered = {t['name']: dict(t) for t in explicit}
            if kind == 'lib' and (path.parent / 'src/lib.rs').exists():
                if not discovered:
                    discovered[crate.replace('-', '_')] = {'name': crate.replace('-', '_'), 'path': 'src/lib.rs'}
            if kind == 'bin' and (path.parent / 'src/main.rs').exists() and not any(t.get('path') == 'src/main.rs' for t in discovered.values()):
                discovered.setdefault(crate, {'name': crate, 'path': 'src/main.rs'})
            if kind not in ('lib',) and m['package'].get('auto' + {'bin':'bins','test':'tests','bench':'benches','example':'examples'}[kind], True):
                for p in sorted((path.parent / directory).glob('*.rs')):
                    discovered.setdefault(p.stem, {'name': p.stem, 'path': str(p.relative_to(path.parent))})
                for p in sorted((path.parent / directory).glob('*/main.rs')):
                    discovered.setdefault(p.parent.name, {'name': p.parent.name, 'path': str(p.relative_to(path.parent))})
            for name, t in sorted(discovered.items()):
                required = t.get('required-features', [])
                jobs = [j for j, selected in selections if set(required) <= selected and not (j == 'ci/installed-smoke' and kind != 'bin') and not (j == 'nightly/full-test' and kind == 'bench')]
                # Scheduled cargo test does not select custom harness benches as a suite.
                demand(jobs, f'uncovered target: {crate}/{kind}/{name} requires {required}')
                source = t.get('path', f'{directory}/{name}.rs')
                targets.append({'crate': crate, 'kind': kind, 'name': name, 'path': str((path.parent / source).relative_to(root)), 'required_features': required, 'jobs': jobs,
                                'tier': 'nightly' if 'nightly-tests' in required else 'normal',
                                'execution': 'not executed by this audit; existing ignore attributes still apply'})
    ignores = []
    nightly_cfg = []
    for p in sorted(root.rglob('*.rs')):
        if '.git' in p.parts:
            continue
        lines = p.read_text().splitlines()
        for line, text in enumerate(lines, 1):
            declaration = ignore_declaration(lines, line - 1)
            if declaration is not None:
                ignores.append({'path': str(p.relative_to(root)), 'line': line, 'declaration': declaration, 'execution': 'not selected (--include-ignored absent)'})
            if 'feature = "nightly-tests"' in text:
                nightly_cfg.append({'path': str(p.relative_to(root)), 'line': line, 'declaration': text.strip(), 'job': 'nightly/full-test', 'execution': 'not executed by this audit'})
    return {'base': 'bc0f1b0fa98424e54e54bb1b3778c245abf6f8fd',
            'workflow_structure': w, 'shards': rows, 'shard_runner': command, 'targets': targets, 'features': features,
            'nightly_cfg': nightly_cfg, 'ignore_declarations': ignores}


def check(root=ROOT):
    actual = inventory(root)
    expected = json.loads((root / 'scripts/ci-continuity-inventory.json').read_text())
    demand(actual == expected, 'manifest / target / feature / workflow inventory mismatch')
    return actual


def main():
    if sys.argv[1] == 'aggregate':
        aggregate(sys.argv[2:], json.loads(os.environ['NEEDS_JSON']))
        print('All required correctness jobs succeeded')
    elif sys.argv[1] == 'check':
        result = check()
        print(f"Selection audit: {len(result['targets'])} targets; runtime not executed")
    else:
        raise ValueError('expected check or aggregate')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as exc:
        sys.exit(str(exc))
