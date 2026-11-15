# Performance result: import rate on a 16-core desktop-class machine, 2026-11-14

## Promise

- Promise, in the product's words: "A large import therefore runs only as fast as maintenance keeps up, and a search made in the middle of it stays fast". No figure promised.
- Counted test that guards it: none yet.

## Measurement

- Date: 2026-11-14; revision measured: `0c708a979e9eb5cbb1dca142f514edf96656711e` for the rows of the first group, which come from the 100,008-passage, 62,447-command and sightings programs of five runs (the median of the five printed); the rows marked "not taken again" keep the figures of `7f150eed3afb284c2c02220431c3cb858635f361`
- Program and command: the load lines of `v2/numbers.sh` (`million_rows_load`, `passages_100008_load`, `synthetic_1000000_*`, `anchors_62447_load`)
- Data: as named in each row; 500 rows to a commit
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a 16-core desktop-class build machine; not a user's machine); operating system: unknown; Rust toolchain: unknown; user's machine: no

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| 1,000,000 small rows loaded under a 64 MiB limit (not taken again) | 11.3 | s | 10.7, 2026-11-12 |
| 100,008 passages loaded | 3.5 | s | 3.5, 2026-11-14 (`090d140ec`) |
| 138,084 rows of 62,447 commands loaded | 12.6 | s | 12.7, 2026-11-14 (`090d140ec`) |
| 1,000,000 sightings loaded | 35.3 | s | 35.2, 2026-11-14 (`090d140ec`) |
| those sightings sealed, 245 cycles | 15.4 | s | 15.5, 2026-11-14 (`090d140ec`) |
| those sightings grouped, 1,123 cycles | 343.4 | s | 343.5, 2026-11-14 (`090d140ec`) |
| 1,000,000 generated vectors loaded (not taken again) | 27.0 | s | 26.5 to 27.1, 2026-11-12 |
| those vectors sealed, 245 cycles (not taken again) | 20.4 | s | 25.7, 2026-11-12 |
| those vectors grouped, 1,150 cycles (not taken again) | 348.6 | s | 365.8, 2026-11-12 |

## Limitations

- A search made during the import is not measured yet; no limit on the vectors a search compares one by one exists yet.
