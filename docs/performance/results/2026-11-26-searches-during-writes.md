# Performance result: searches while writes arrive on a 16-core desktop-class machine, 2026-11-26

## Promise

- Promise, in the product's words: "When new vectors arrive faster than maintenance keeps up with them, searches are protected and the writer is slowed. ... a search made in the middle of it stays fast."
- Counted test that guards it: `the_worker_runs_cycles_until_nothing_is_left` and `sealing_advances_only_when_asked_and_in_bounded_cycles` (`v2/contextdb/tests/vectors_maintenance.rs`) guard automatic progress and bounded sealing; `a_reader_does_the_same_counted_work_early_and_late_while_a_writer_never_stops` (`v2/contextdb/tests/readers.rs`) holds the rows read and vectors compared by a reader's search the same while a writer commits. None yet for the complete overlapping-write latency promise.

## Measurement

- Date: 2026-11-26 (02:07 to 03:25 UTC); revision measured: `672c10078a71d8b24a81ede80490bf979b0bcc79` (the script's header records one uncommitted file, a script fix made after the run began; the run used the committed script)
- Program and command: `v2/numbers.sh` with `NUMBERS_LARGE=1`, `IMPORT=dump-large`, `CAMERA_SECONDS=60`; lines `import_100008_find_*` and `sightings_1000000_contexts_{1,8,64}_find_during_backfill_*`
- Data: a 100,008-passage import with searches taken during it (and 100 taken afterwards, `idle`); and 1,000,000 sightings over 1, 8 and 64 contexts with searches taken while the index was being built (`backfill`)
- Passes and statistic: one run; p50 and p95 over the searches the lines count; threads: 1
- Resources: engine memory limit: 1,073,741,824 bytes in the find sessions (the `_session` lines), threads 1; enforced process memory: not recorded; cores available: 16 (32 threads)
- Storage: ext4; temporary-store class and cache state not recorded
- Comparison: not applicable (no earlier figure)
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a desktop-class build machine, to itself during the run; not a user's machine); operating system: Ubuntu 26.04.1, Linux 7.0; Rust toolchain: unknown; user's machine: no
- Settings that can change the result: which search mode the searches during the import and the backfill ran in is not recorded in the lines

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| searches during the import of 100,008 passages (1,414 searches), p50 and p95 | 12.034 and 25.627 | ms | none |
| searches after the import (100 searches), p50 and p95 (`find_idle`) | 122.805 and 125.748 | ms | none |
| the import itself | 16.3 s wall; 6,151 rows per second; 0 statements refused | s | none |
| searches during the build of the index over 1 context (1,343 searches; backfill 201.8 s, 818 cycles), p50 and p95 | 234.768 and 246.111 | ms | none |
| the same over 8 contexts (865 searches; backfill 138.1 s, 91 cycles) | 231.762 and 238.214 | ms | none |
| the same over 64 contexts (605 searches; backfill 120.3 s, 23 cycles) | 233.015 and 240.421 | ms | none |
| camera smoke, 60 s: `camera_500_60_writer_lag_max`, as printed | 0.000 | ms | none |

## Limitations

- The searches taken during the index build took 231.762 to 234.768 ms at the p50 and 238.214 to 246.111 ms at the p95, which is the time the exact searches over all contexts of the same stores took (p95 231.734 to 253.576 ms in [the 1,000,000-vector result](2026-11-26-find-1m.md)); once the index is built the same searches take 0.253 to 8.426 ms at the p50. **This run does not show a search staying fast while a million-vector index is built;** it shows searches answered at exact speed meanwhile.
- The searches after the 100,008-passage import took 122.805 ms at the p50, slower than the searches taken during it (12.034 ms); the cause was not looked for.
- The camera smoke ran 60 s, not a day, and its writer lag of 0.000 ms is of that run only.
- A single run; no timed figure is a promise.
