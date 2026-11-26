# Performance result: store size against its data on a 16-core desktop-class machine, 2026-11-26

## Promise

- Promise, in the product's words: "After maintenance, a store that has been written to and expired for any length of time takes at most twice the bytes of the data it holds now. ... The engine reports how much disk each table takes and how much free space the store holds."
- Counted test that guards it: `a_store_with_twelve_indexes_stays_under_twice_its_bytes_and_under_its_recorded_size` and `two_hundred_rows_of_50_000_byte_indexed_values_make_a_store_smaller_than_twice_their_bytes` (`v2/contextdb/tests/long_keys.rs`) guard freshly loaded stores; `a_season_stays_level` (`v2/contextdb/tests/performance_bars.rs`) holds a store's file at or under twice the bytes put in after each of eight turnovers of a fixed clock.

## Measurement

- Date: 2026-11-26 (02:07 to 03:25 UTC); revision measured: `672c10078a71d8b24a81ede80490bf979b0bcc79` (the script's header records one uncommitted file, a script fix made after the run began; the run used the committed script)
- Program and command: `v2/numbers.sh` with `NUMBERS_LARGE=1`, `IMPORT=dump-large`, `CAMERA_SECONDS=60`; the `*_store_size` and `*_size_over_held` lines; `size_over_held` is the store's bytes over the bytes of the values put in
- Data: as named in each row
- Passes and statistic: one run; sizes as the lines print them
- Resources: not applicable to a size
- Storage: ext4; temporary-store class and cache state not recorded
- Comparison: previous figures are of `0c708a979e9eb5cbb1dca142f514edf96656711e` and the layout landing (2026-11-14, see [that result](2026-11-14-store-size.md)); not a controlled comparison
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a desktop-class build machine, to itself during the run; not a user's machine); operating system: Ubuntu 26.04.1, Linux 7.0; Rust toolchain: unknown; user's machine: no
- Settings that can change the result: none recorded; the point at which each line was taken (before or after maintenance) is as the program's line names it

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| 62,447 commands (138,084 rows, 70,944 of them events) | 257.3 MiB, 1.51 | MiB; times its data | 270.8 MiB, 1.59, 2026-11-14 |
| 100,008 passages, grouped | 514.8 MiB, 1.73 | MiB; times its data | 471.8 MiB, 1.58, 2026-11-14 |
| the declared-find store of 100,008 passages | 565.9 MiB, 1.84 | MiB; times its data | 527.6 MiB, 1.72, 2026-11-14 |
| 165,855 passages | 888.5 MiB, 1.80 | MiB; times its data | 766.3 MiB, 1.55, 2026-11-14 |
| 173 session files | 214.5 MiB, 0.62 | MiB; times its data | 214.5 MiB, 0.62 (not taken again), 2026-11-14 |
| 1,000,000 generated vectors, sealed and grouped | 5,087.2 MiB, 1.73 | MiB; times its data | 4,991.9 MiB, 1.70, 2026-11-14 |
| 1,000,000 sightings, sealed (4,096.0 MiB) and grouped | 5,087.2 MiB, 1.73 | MiB; times its data | 4,990.0 MiB, 1.70, 2026-11-14 |
| 5,000,000 sightings, sealed (20,167.0 MiB) and grouped | 26,112.0 MiB, 1.78 | MiB; times its data | none |
| 20,000 vectors (`vectors_20000_*`) | 43.3 MiB, 1.47 | MiB; times its data | none |
| 10,000 vectors in one partition (`single_10000_*`), grouped; 11,588 passages | 53.2 MiB, 1.79; 51.4 MiB, 1.49 | MiB; times its data | none |
| 100,008 passages imported | 537.0 MiB, 1.80 | MiB; times its data | none |
| the same store after appending another 100,008 passages (`append_100008_*`) | 926.8 MiB, about 1.55 | MiB; times its data | none |
| 1,907,000 flat rows (the 8 GiB flat-open store, `opens_8589934592_*`) | 7,884.5 MiB | MiB | none |
| 20,000 notes of 1.6 KB, one commit each | 2.0 | MiB | 2.0, 2026-11-14 |
| a flip of 10,045 rows' current flag: bytes written; the store before and after three flips | 3.6 MiB; 565.9 MiB and 565.9 MiB | MiB | none |

## Limitations

- **The store after appending 100,008 passages is about 1.55 times its data, within the promised 2 times.** The run printed `append_100008_size_over_held 3.11 x`, which divided the whole store by the appended passages' bytes alone; the measuring program now divides by everything the store holds. 1.55 is 926.8 MiB over the two passes' bytes (537.0 MiB / 1.80 = 298.3 MiB each), measured after the maintenance cycles the import runs (`v2/contextdb/examples/passages.rs`, `imported`).
- Every grouped store of this run is larger against its data than the same store on 2026-11-14 (1.73 against 1.70 at 1,000,000 sightings, 1.80 against 1.55 at 165,855 passages); the cause was not looked for.
- The camera smoke of 60 s reports stores of 1.43 to 1.98 times their data; it did not run a store through a full retention period ([camera smoke](2026-11-26-camera-smoke.md)).
- Not measured: how much disk each table takes and how much free space the store holds (the engine does not report them by a shell command); the long-run twice-live-data promise over weeks.
- No timed figure here; sizes are of this machine's filesystem only.
