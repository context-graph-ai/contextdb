# contextdb-redb — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). This crate is ContextDB's
maintained fork of the upstream redb 4.1.0 package: the single-file, crash-safe B-tree store under
every file-backed database.

## Owns, and must not own

Owns: redb's storage engine, plus the retained delta upstream lacks — bounded resumable
compaction (`compact_step` with `CompactionCursor`/`CompactionProgress`, relocating a finite page
batch that yields to a waiting writer), live-reader observations, a retained statistics snapshot
(`storage_statistics_snapshot`), and thread-scoped read admission and backend-read observation
(`read_admission.rs`: `with_read_memory_admission`, `observe_backend_reads`).

Must not own: any ContextDB policy — thresholds, batch sizes, intervals, memory limits, table
layouts. The engine decides those (`persistence.rs` calls `compact_step` with its own 64-page
batch). **The delta against upstream stays minimal**, and the five upstream integration targets
(`backward_compatibility`, `basic_tests`, `integration_tests`, `multimap_tests`,
`multithreading_tests`) stay byte-identical to upstream. What ContextDB builds on top of redb is
additive code and lives in the engine, not here.

[`MAINTENANCE.md`](MAINTENANCE.md) is binding: the exact upstream package, checksum and revision,
every changed source file and why, the complete delta ([`upstream-4.1.0.patch`](upstream-4.1.0.patch))
with the command that regenerates it, the upgrade procedure, and the upstream proposals. A change
to `src/` updates the patch and that table in the same commit.

## Seams

- **Library name** stays `redb`; the engine depends on package `contextdb-redb`, published first.
- **Outside the workspace** (`exclude` in the root `Cargo.toml`): it builds and tests from its own
  manifest and its own upstream `Cargo.lock`, so `cargo test --workspace` never covers it.
- **Above:** only `contextdb-engine` (`persistence.rs`, `read_image_memory.rs`,
  `vector_observations.rs`). The `test-seams` feature exposes `write_wait_observer` to engine tests.

## Invariants and their guards

| Invariant | Guard (file → test) |
|---|---|
| Relocation batches preserve every value across interleaved writes and a restart | `crates/contextdb-engine/tests/storage_compaction_online.rs` → `relocation_batches_preserve_values_with_interleaved_writes_and_restart` |
| A refused page allocation aborts the batch without advancing the cursor | `crates/contextdb-engine/tests/storage_compaction_online.rs` → `allocation_refusal_aborts_relocation_without_advancing_its_position` |
| A live read snapshot survives relocated and reused pages | `src/transactions.rs` → `live_snapshot_survives_relocated_and_reused_pages` |
| Relocation progresses while reads are live and yields to a waiting writer | `crates/contextdb-engine/src/database/storage_compaction_overlap_tests.rs` → `automatic_relocation_progresses_with_live_storage_reads_and_waiting_recordings` |
| A paused statistics snapshot does not block a foreground commit | `crates/contextdb-engine/tests/storage_compaction_online.rs` → `paused_storage_statistics_allow_a_foreground_commit` |
| Backend-read observation counts the bytes actually read and ends with the operation | `crates/contextdb-engine/tests/storage_compaction_online.rs` → `backend_read_observation_counts_actual_bytes_and_ends_with_the_operation` |
| Files written by older redb releases still open | `tests/backward_compatibility.rs` (upstream suite) |
| The fork's `Cargo.lock` is unchanged by packaging | no Rust test — checked by `scripts/verify-packaged-engine.sh` |
| `src/` differs from upstream 4.1.0 by exactly `upstream-4.1.0.patch`; the five upstream test targets are unmodified | unguarded — regenerate the patch with the MAINTENANCE.md command and compare |

## Where a change lives

| Change | Where it lands |
|---|---|
| When to compact, batch size, thresholds, handle recycling | Engine crate, `persistence.rs` — never here |
| A storage primitive the engine cannot build from outside | Here, as the smallest upstream-shaped change, plus the patch and MAINTENANCE.md row |
| An upstream upgrade | MAINTENANCE.md: start from the new package and its unmodified tests and lockfile, rebase the delta, bump this package and the engine dependency together |

## Fast tests

```bash
cargo test --manifest-path crates/contextdb-redb/Cargo.toml --locked
cargo test -p contextdb-engine --features test-seams --test storage_compaction_online
```
