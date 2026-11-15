# Performance result: the cost of one commit on a 16-core desktop-class machine, 2026-11-14

## Promise

- Promise, in the product's words: "Rows, relationships, vectors and the index state built from them commit together or not at all". No figure promised for its speed.
- Counted test that guards it: `a_kill_at_any_storage_call_of_a_commit_leaves_all_or_nothing` and `a_kill_inside_a_commit_of_vectors_keeps_rows_and_vectors_together` (`v2/contextdb/tests/crash/main.rs`) stop the process at every storage call of a commit and check that all of it or none of it is there.

## Measurement

- Date: 2026-11-14; revision measured: `0c708a979e9eb5cbb1dca142f514edf96656711e`, five runs, the median of the five printed
- Program and command: the `readme_*` lines of `v2/numbers.sh` (`measure readme`)
- Data: 20,000 notes of 1.6 KB, one commit per row
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a 16-core desktop-class build machine; not a user's machine); operating system: unknown; Rust toolchain: unknown; user's machine: no

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| 20,000 commits of one row | 28.0 | s | 27.9, 2026-11-14 (`090d140ec`); 11.1, 2026-11-14 (`7f150eed3`, a run in another folder) |
| the store they leave | 2.0 | MiB | 2.0, 2026-11-14 (`090d140ec`) |

## Limitations

- The time is of this machine only; no timed figure is a promise.
- The merged code (`4495faf1a`) took 27.6 to 29.9 s in the same five runs, so the engine did not slow it; the earlier 11.1 s was taken by `numbers.sh` in another folder, and the cause of the difference was not looked for.
