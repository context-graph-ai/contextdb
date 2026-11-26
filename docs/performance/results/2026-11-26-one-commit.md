# Performance result: the cost of one commit on a 16-core desktop-class machine, 2026-11-26

## Promise

- Promise, in the product's words: "Rows, relationships, vectors and the index state built from them commit together or not at all". No figure promised for its speed.
- Counted test that guards it: `a_kill_at_any_storage_call_of_a_commit_leaves_all_or_nothing` and `a_kill_inside_a_commit_of_vectors_keeps_rows_and_vectors_together` (`v2/contextdb/tests/crash/main.rs`) stop the process at every storage call of a commit and check that all of it or none of it is there.

## Measurement

- Date: 2026-11-26 (02:07 to 03:25 UTC); revision measured: `672c10078a71d8b24a81ede80490bf979b0bcc79` (the script's header records one uncommitted file, a script fix made after the run began; the run used the committed script)
- Program and command: the `readme_*` lines of `v2/numbers.sh` (`v2/numbers.sh` with `NUMBERS_LARGE=1`, `IMPORT=dump-large`, `CAMERA_SECONDS=60`)
- Data: 20,000 notes of 1.6 KB, one commit per row
- Passes and statistic: one run; as printed
- Resources: not recorded (unknown)
- Storage: ext4; temporary-store class and cache state not recorded
- Comparison: previous figures are of `0c708a979e9eb5cbb1dca142f514edf96656711e` (2026-11-14, five runs, the median printed); not a controlled comparison
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a desktop-class build machine, to itself during the run; not a user's machine); operating system: Ubuntu 26.04.1, Linux 7.0; Rust toolchain: unknown; user's machine: no
- Settings that can change the result: none recorded

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| 20,000 commits of one row (`readme_insert`) | 26.6 | s | 28.0, 2026-11-14 |
| the store they leave (`readme_store_size`) | 2.0 | MiB | 2.0, 2026-11-14 |
| open of that store (`readme_open`) | 2.087 | ms | none |
| idle anonymous memory (`readme_idle_rss_anon`) | 1.0 | MiB | none |
| a count of all rows (`readme_count_star`); rows examined | 0.105 ms; 0 | ms; rows | none |

## Limitations

- The time is of this machine only; no timed figure is a promise.
- A single run; the five-run median of 2026-11-14 is not repeated.
