# contextdb-hnsw maintenance

This HNSW fork is based on the **hnsw_rs 0.3.4 crates.io package**, checksum
`43a5258f079b97bf2e8311ff9579e903c899dcbac0d9a138d62e9a066778bd07`
(upstream repository https://github.com/jean-pierreBoth/hnswlib-rs). The `MIT OR Apache-2.0`
license files are unchanged.

The maintained distribution is published as **`contextdb-hnsw`**. Its library remains named
`hnsw_rs`, so upstream-style callers keep their compile-time identity; `contextdb-vector` depends on
it, so it is published before `contextdb-vector`. The package identity, description, repository,
library alias, and the absence of upstream examples, tests, and benchmarks are ContextDB packaging
metadata outside the source-only upstream diff below.

## Why the fork exists

ContextDB needs four things upstream does not provide:

- **Deterministic builds.** Rebuilding a graph from the same points must produce the same topology,
  which needs a caller-provided seed for the level-assignment RNG.
- **Empty graphs.** A vector partition whose every row was deleted still loads a graph with no
  points; iterating it must yield nothing instead of panicking on the missing entry point.
- **Owned, durable graphs.** ContextDB persists each graph generation as bytes it owns and reloads
  it as a read-only graph, without the file-oriented dump format and without re-inserting vectors.
- **Bounded filtered search.** A search over an allowed-id set must charge work only for admitted
  nodes and stop at hard visit/distance limits, so a sparse filter cannot turn into a walk of the
  whole bottom layer.

[upstream-0.3.4.patch](upstream-0.3.4.patch) is the complete source-only delta against that exact
package:

| Changed source | Reason |
|---|---|
| `src/hnsw.rs` | Seeded `new_with_seed` constructors on `LayerGenerator`, `PointIndexation`, and `Hnsw`; empty-graph point iteration; `with_point_data` and read-only getters; the owned read-only `LoadedHnsw`; `search_with_bounded_control` and `search_allowed_with_bounded_control` with `HnswAllowedSearchLimits` and their typed status/result/error types; `insert_slice_with_point_id` returning the physical point identity. |
| `src/hnswio.rs` | `encode_owned_graph`, `encode_owned_graph_into`, and `decode_owned_graph` for the owned graph representation, which validates every edge and returns an error for malformed bytes; `load_hnsw_owned` and `load_hnsw_owned_with_dist`; loaded points own shared handles to their memory mapping. |
| `src/lib.rs` | Allow four upstream clippy lints so the workspace's strict lint gate passes on unchanged upstream code. |
| `src/api.rs`, `src/datamap.rs`, `src/filter.rs`, `src/flatten.rs`, `src/libext.rs` | Trailing whitespace removed from doc comments; no code change. |

To reproduce the patch, obtain and verify the exact package above, unpack it outside the repository,
and set `HNSW_UPSTREAM` to its `hnsw_rs-0.3.4` directory. From the product repository, the following
prints the same source-only unified diff:

```bash
python3 - "$HNSW_UPSTREAM" <<'PY'
import difflib
import pathlib
import sys
upstream = pathlib.Path(sys.argv[1])
fork = pathlib.Path('crates/contextdb-hnsw')
paths = sorted({p.relative_to(upstream) for p in (upstream / 'src').rglob('*') if p.is_file()}
               | {p.relative_to(fork) for p in (fork / 'src').rglob('*') if p.is_file()})
for path in paths:
    before = (upstream / path).read_text().splitlines(True) if (upstream / path).exists() else []
    after = (fork / path).read_text().splitlines(True) if (fork / path).exists() else []
    sys.stdout.writelines(difflib.unified_diff(before, after,
        fromfile=f'a/{path}' if (upstream / path).exists() else '/dev/null',
        tofile=f'b/{path}' if (fork / path).exists() else '/dev/null'))
PY
```

## Upgrading

An upgrade starts with the new upstream package identity. Rebase the retained source diff, discard
changes upstream has absorbed, regenerate the patch, then update the `contextdb-hnsw` package version
and the `contextdb-vector` dependency together. Run the workspace tests, including the HNSW rebuild
determinism, empty-partition maintenance, restart, and bounded-search tests, before changing the
pin. Do not update the upstream version without updating these records.

## Shrinking the fork

Each capability above can be proposed upstream on its own, with tests, and removed from this fork
once an upstream release carries it:

1. Seeded constructors (`new_with_seed`) with a determinism test.
2. Non-panicking iteration over a graph with no points.
3. An owned, validated graph encoding with a read-only loaded graph.
4. Allowed-set search with hard visit/distance limits and typed incomplete results.

The fork can be retired only when upstream carries all four; until then, deleting it breaks vector
search. None of these proposals has been submitted upstream.
