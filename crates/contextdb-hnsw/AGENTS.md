# contextdb-hnsw — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). This crate is ContextDB's
maintained fork of the upstream `hnsw_rs` 0.3.4 package: the HNSW graph that vector search
builds, stores and searches.

## Owns, and must not own

Owns: HNSW graph construction and search, the five capabilities upstream lacks — seeded
constructors (`new_with_seed`) for deterministic rebuilds, non-panicking iteration over a graph
with no points, the owned validated graph encoding (`encode_owned_graph`, `decode_owned_graph`,
`LoadedHnsw`) that reloads as a read-only graph, allowed-set search with hard visit/distance
limits (`search_allowed_with_bounded_control`, `HnswAllowedSearchLimits`), and read-only access
to one neighbour's identity (`neighbour_identity`) so `contextdb-vector` can walk a resident graph
and a saved graph through one traversal.

It also owns the storage-access form of seeded sequential construction and search
(`src/paged.rs`: `insert_paged`, `search_paged`, `search_paged_allowed` over a caller-supplied
`PagedPointStorage`). A provider owns node persistence and transactions; this module holds only
one insertion's finite workspace (`PagedGraphWorkspace`) and never caches the node population, so
a graph can be built and searched without being resident.

Must not own: anything that knows about ContextDB — rows, snapshots, partitions, policies,
memory charging, persistence. **The delta against upstream stays minimal: a change here is a
change to vendored upstream code.** Code that uses a graph the ContextDB way — generations,
tombstones, merges, quantized payloads, budgets — is additive code and lives in
`contextdb-vector` (`src/hnsw.rs`), never here.

[`MAINTENANCE.md`](MAINTENANCE.md) is binding: it names the exact upstream package and checksum,
lists every changed source file and why, carries the complete delta
([`upstream-0.3.4.patch`](upstream-0.3.4.patch)) with the command that regenerates it, and gives
the upgrade procedure and the upstream proposals that would let the fork shrink. A change to
`src/` updates the patch and the MAINTENANCE.md table in the same commit.

## Seams

- **Library name** stays `hnsw_rs`, so callers keep the upstream compile-time identity.
- **Above:** only `contextdb-vector` depends on it (`src/hnsw.rs`, `src/mem.rs`); it is published
  before `contextdb-vector`.
- **Below:** `anndists` distances, `rayon`, `serde`/`bincode`; nothing from ContextDB.

## Invariants and their guards

| Invariant | Guard (file → test) |
|---|---|
| The same points and seed rebuild the same topology | `crates/contextdb-engine/tests/engine/hnsw_rebuild_determinism_tests.rs` → `consecutive_hnsw_builds_within_one_open_database_are_stable`, `hnsw_build_invariant_under_parallel_pressure_does_not_drift` |
| Saved/resident mixed-generation tie ordering belongs to the vector wrapper | `crates/contextdb-vector/src/hnsw.rs` → `tests::mixed_saved_base_and_resident_tail_keep_exact_row_id_ties_at_breadth_one` |
| A graph with no points iterates to nothing instead of panicking | `src/hnsw.rs` → `tests::iterating_a_graph_with_no_points_yields_nothing`; `crates/contextdb-vector/tests/vector_tests.rs` → `a_durable_generation_with_no_live_vectors_loads_as_an_empty_graph` |
| Owned graph bytes round-trip, and malformed bytes are an error, not a graph | `src/hnswio.rs` → `tests::streamed_owned_graph_preserves_payload_bytes_and_roundtrip`; `crates/contextdb-vector/src/hnsw.rs` → `tests::durable_generation_refuses_corruption_and_incompatible_column_definition` |
| Allowed-set search finishes its greedy descent before level zero and charges only admitted nodes | `src/hnsw.rs` → `tests::allowed_search_finishes_greedy_descent_before_entering_level_zero`; `crates/contextdb-vector/src/hnsw.rs` → `tests::ordinary_allowed_search_excluded_history_adds_no_visit_or_distance_work` |
| Paged construction and search keep only one operation's workspace and give the same neighbours as the resident graph | `crates/contextdb-vector/src/hnsw.rs` → `tests::saved_graph_pages_preserve_owned_neighbor_ids_and_order_across_search_breadths`; `crates/contextdb-engine/tests/engine_seams/paged_initial_replacement_and_reclamation_progress_under_writes.rs` → `paged_initial_replacement_and_reclamation_progress_under_writes` |
| Every hunk in `upstream-0.3.4.patch` produces exactly the fork source lines it names | `crates/contextdb-core/tests/hnsw_fork_patch_matches_its_sources.rs` → `the_hnsw_fork_patch_produces_the_sources_it_names`; a fork change the patch omits entirely is unguarded — regenerate the patch with the MAINTENANCE.md command and compare |

## Where a change lives

| Change | Where it lands |
|---|---|
| Distances and ties shared by saved and resident graph traversal | Vector `src/hnsw/unfiltered.rs` and `src/hnsw.rs`; use `neighbour_identity` here instead of adding a second walk. |
| Anything vector search needs that composes existing graph calls | `contextdb-vector`, `src/hnsw.rs` — not this crate |
| A graph primitive that cannot be built from outside (a new traversal control, an encoding field) | Here, as the smallest upstream-shaped change, plus the patch and MAINTENANCE.md row |
| Paged graph construction or search primitive | `src/paged.rs`; the transactional tail, builder and durable lookup records that use it are vector's `hnsw/paged_builder.rs`. |
| Graph format read or write primitive | `src/hnsw.rs`, `src/hnswio.rs`; keep the fork delta and patch minimal. |
| An upstream upgrade | MAINTENANCE.md "Upgrading": rebase the delta, regenerate the patch, bump this package and the `contextdb-vector` dependency together |
| A capability upstream now carries | Remove it from the fork and from MAINTENANCE.md ("Shrinking the fork") |

## Fast tests

```bash
cargo test -p contextdb-hnsw
cargo test -p contextdb-engine --features test-seams --test engine
```
