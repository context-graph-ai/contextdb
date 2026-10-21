"""Manifest-derived benchmark inventory shared by CI routing and qualification."""
from pathlib import Path
import re
import tomllib


def declared_benches(root):
    root = root.resolve()
    rows = []
    seen = set()
    for manifest in sorted(root.glob('crates/*/Cargo.toml')):
        doc = tomllib.loads(manifest.read_text())
        package = doc['package']['name']
        for bench in doc.get('bench', []):
            name = bench['name']
            if not re.fullmatch(r'[\w-]+', name) or (package, name) in seen:
                raise ValueError(f'invalid or duplicate benchmark: {package}/{name}')
            seen.add((package, name))
            source = (manifest.parent / bench.get('path', f'benches/{name}.rs')).resolve()
            path = source.relative_to(root).as_posix()
            text = source.read_text()
            features = bench.get('required-features', [])
            if not isinstance(features, list) or any(not isinstance(f, str) or
                    f not in doc.get('features', {}) for f in features):
                raise ValueError(f'unsupported required features: {package}/{name}')
            harness = bench.get('harness', True)
            if not isinstance(harness, bool):
                raise ValueError(f'invalid harness: {package}/{name}')
            kind = ('libtest' if harness else 'criterion' if
                    re.search(r'\bcriterion_main!\s*\(', text) else 'custom')
            rows.append({'package': package, 'name': name, 'path': path,
                         'manifest': manifest.relative_to(root).as_posix(),
                         'required_features': features, 'harness': kind})
    if not rows:
        raise ValueError('no declared benchmarks')
    return rows


def performance_paths(root):
    paths = {row['path'] for row in declared_benches(root)}
    for common in [root / 'benches/common', *root.glob('crates/*/benches/common')]:
        paths.update(p.relative_to(root).as_posix() for p in common.rglob('*') if p.is_file())
    return frozenset(paths)
