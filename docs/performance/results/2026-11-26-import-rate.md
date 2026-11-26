# Performance result: import rate on a 16-core desktop-class machine, 2026-11-26

## Promise

- Promise, in the product's words: "A large import therefore runs only as fast as maintenance keeps up, and a search made in the middle of it stays fast". No figure promised.
- Counted test that guards it: `imports_and_one_row_commits_keep_their_counted_write_bounds` (`v2/contextdb/tests/performance_bars.rs`) counts the 4 KiB units written and the syncs of imports and one-row commits against fixed bounds. None yet for the complete paced-import promise.

## Measurement

- Date: 2026-11-26 (02:07 to 03:25 UTC); revision measured: `672c10078a71d8b24a81ede80490bf979b0bcc79` (the script's header records one uncommitted file, a script fix made after the run began; the run used the committed script)
- Program and command: `v2/numbers.sh` with `NUMBERS_LARGE=1`, `IMPORT=dump-large`, `CAMERA_SECONDS=60`; the `import_*`, `append_*`, load and maintenance lines
- Data: as named in each row; the passages are 100,008 real search passages of 768 numbers each
- Passes and statistic: one run; wall time and rows per second as the lines print them; the import of 100,008 passages is printed twice in the run (the first with searches taken during it), both shown
- Resources: engine memory limit: not recorded for the import lines; the cgroup peaks are the lines' own; cores available: 16 (32 threads)
- Storage: ext4; temporary-store class and cache state not recorded
- Comparison: previous figures are of `0c708a979e9eb5cbb1dca142f514edf96656711e` (2026-11-14, five runs, the median printed); the old engine's wall time is the script's own line, taken at the old engine's revision `56dccfdc`; not a controlled comparison
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a desktop-class build machine, to itself during the run; not a user's machine); operating system: Ubuntu 26.04.1, Linux 7.0; Rust toolchain: unknown; user's machine: no
- Settings that can change the result: import mode `dump-large`; 500 rows to a commit in the earlier result, not recorded in this run's lines

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| import of 10,000 passages | 0.4 s; 24,462 rows per second; 0 statements refused | s | none |
| import of 100,008 passages, with searches taken during it | 16.3 s; 6,151 rows per second; 0 statements refused | s | none |
| import of 100,008 passages, the second printing | 16.4 s; 6,109 rows per second; 0 statements refused | s | none |
| the same import in the old engine | 404.8 | s | none |
| memory of the 100,008-passage import: memory used peak; resident peak; cgroup peak | 21.6 MiB; 213.3 MiB; 728.9 MiB (second printing: 21.9, 37.0 and 550.5) | MiB | none |
| append of another 100,008 passages after it | 19.2 s; 5,216 rows per second; 0 statements refused; memory used peak 404.6 MiB; resident peak 288.0 MiB; cgroup peak 1,214.0 MiB | s | none |
| 100,008 passages loaded (`passages_100008_load`) | 3.1 | s | 3.5, 2026-11-14 |
| 138,084 rows of 62,447 commands loaded | 22.4 | s | 12.6, 2026-11-14 |
| 1,000,000 sightings loaded | 41.0 | s | 35.3, 2026-11-14 |
| those sightings sealed, 1 cycle | 0.0 | s | 15.4 (245 cycles), 2026-11-14 |
| those sightings grouped, 1,131 cycles | 254.0 | s | 343.4 (1,123 cycles), 2026-11-14 |
| 1,000,000 generated vectors loaded | 34.6 | s | 27.0, 2026-11-14 |
| those vectors sealed, 1 cycle | 0.0 | s | 20.4 (245 cycles), 2026-11-14 |
| those vectors grouped, 1,149 cycles | 209.4 | s | 348.6 (1,150 cycles), 2026-11-14 |
| 5,000,000 sightings loaded; sealed; grouped (7,124 cycles) | 224.7; 0.0; 1,225.8 | s | none |
| 1,000,000 small rows loaded (`million_rows_load`) | 5.4 | s | 11.3, 2026-11-14 (under a 64 MiB limit) |
| 173 session files, 150,756 rows loaded | 3.4 | s | none |

## Limitations

- The import of 138,084 rows is slower than on 2026-11-14 (22.4 s against 12.6 s), and the load of 1,000,000 sightings and of 1,000,000 generated vectors are slower too; the cause was not looked for. The seal times of 0.0 s in one cycle are as printed and differ from the 245 cycles of the earlier result.
- The memory limit of the `million_rows_load` line is not recorded here; its comparison with the 64 MiB figure of 2026-11-14 is not like for like.
- No pacing figure: the lines do not say how far the writer was slowed.
- Time figures are of this machine only; no timed figure is a promise.
