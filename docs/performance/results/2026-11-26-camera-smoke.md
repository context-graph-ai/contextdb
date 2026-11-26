# Performance result: a camera-day smoke of 60 seconds on a 16-core desktop-class machine, 2026-11-26

## Promise

- Promise, in the product's words: "Once a table's retention period is full and old rows expire as fast as new ones arrive, the store stays level however long it runs without a restart: the disk it takes, the memory its process holds and the work a search does stop growing."
- Counted test that guards it: `a_season_stays_level` (`v2/contextdb/tests/performance_bars.rs`) runs eight turnovers of a fixed clock and holds the file's size at or under twice the bytes put in, its drift under one tenth, the statement peak equal, and the vectors compared by a fixed find within one tenth. It does not run for months.

## Measurement

- Date: 2026-11-26 (02:07 to 03:25 UTC); revision measured: `672c10078a71d8b24a81ede80490bf979b0bcc79` (the script's header records one uncommitted file, a script fix made after the run began; the run used the committed script)
- Program and command: `v2/numbers.sh` with `NUMBERS_LARGE=1`, `IMPORT=dump-large`, `CAMERA_SECONDS=60`; the `camera_500_60_*` lines. **The camera day ran 60 s as a smoke, not 86,400 s. The 24-hour soak is not measured.**
- Data: 500 rows to a turnover period of 7 s; nine turnovers (0 to 8) in 60.0 s of wall time
- Passes and statistic: one run; values as the lines print them at each turnover
- Resources: not recorded for this program (unknown)
- Storage: ext4; temporary-store class and cache state not recorded
- Comparison: not applicable (no earlier figure)
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a desktop-class build machine, to itself during the run; not a user's machine); operating system: Ubuntu 26.04.1, Linux 7.0; Rust toolchain: unknown; user's machine: no
- Settings that can change the result: `CAMERA_SECONDS=60`

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| turnover 0: live rows; store; times its data; memory used; resident; a find | 3,430; 20.1 MiB; 1.98; 10.8 MiB; 22.0 MiB; 0.828 ms (34 rows read, 3,454 vectors compared) | rows; MiB; ms | none |
| turnover 1 | 6,930; 32.7 MiB; 1.60; 10.8 MiB; 22.1 MiB; 3.419 ms (130 rows, 6,966 vectors) | as above | none |
| turnover 2 | 10,430; 47.1 MiB; 1.53; 10.8 MiB; 22.1 MiB; 5.205 ms (139 rows, 10,475 vectors) | as above | none |
| turnover 3 | 13,930; 63.8 MiB; 1.55; 10.8 MiB; 22.1 MiB; 6.919 ms (149 rows, 13,985 vectors) | as above | none |
| turnover 4 | 17,430; 76.5 MiB; 1.48; 10.8 MiB; 22.1 MiB; 8.479 ms (158 rows, 17,494 vectors) | as above | none |
| turnover 5 | 20,930; 91.8 MiB; 1.48; 10.8 MiB; 22.1 MiB; 10.152 ms (160 rows, 20,996 vectors) | as above | none |
| turnover 6 | 24,430; 103.6 MiB; 1.43; 10.8 MiB; 22.1 MiB; 11.861 ms (161 rows, 24,497 vectors) | as above | none |
| turnover 7 | 27,930; 124.3 MiB; 1.50; 10.8 MiB; 21.3 MiB; 13.897 ms (162 rows, 27,998 vectors) | as above | none |
| turnover 8 | 31,430; 140.3 MiB; 1.51; 10.8 MiB; 21.3 MiB; 15.340 ms (166 rows, 31,502 vectors) | as above | none |
| last turnover over first: store; memory used; vectors compared by a find | 4.29 times; 1.00 times; 4.52 times | times | none |
| statements refused; wall; longest writer lag; worker busy | 0 statements; 60.0 s; 0.000 ms; 0.002 cores | as printed | none |
| page slack ratio at turnovers 1 to 8 | 0.11041122556003725, 0.08820219373977967, 0.07549996565849892, 0.06847155501447749, 0.06393732737183865, 0.06050649633115622, 0.05788349690844468, 0.05553670395596444 (turnover 0: 0.17146964317481025) | ratio | none |
| pages compacted by a cycle, turnovers 0 to 8 | 0 at every turnover; the compaction free ratio printed NaN at turnovers 0 to 8 | pages | none |

## Limitations

- **This run does not show the store staying level.** Live rows grew at every turnover (3,430 to 31,430), the store grew 4.29 times and the vectors a find compares grew 4.52 times; live rows were still growing at the last turnover. Only memory used stayed level (10.8 MiB at every turnover, 1.00 times).
- The 86,400 s camera day was not run; the 24-hour soak is not measured, and months are not measured.
- A single run; no timed figure is a promise.
