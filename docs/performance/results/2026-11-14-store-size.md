# Performance result: store size against its data on a 16-core desktop-class machine, 2026-11-14

## Promise

- Promise, in the product's words: "after maintenance, a store that has been written to and expired for any length of time takes at most twice the bytes of the data it holds now"; "Reference stores are measured against that limit at every release".
- Counted test that guards it: `a_store_with_twelve_indexes_stays_under_twice_its_bytes_and_under_its_recorded_size` (`v2/contextdb/tests/long_keys.rs`; 4,000 rows, 12 indexes; 1.76 times its data on 2026-11-13) and `two_hundred_rows_of_50_000_byte_indexed_values_make_a_store_smaller_than_twice_their_bytes` (same file). None yet for a store written and expired over time.

## Measurement

- Date: 2026-11-14 for the figures of `numbers.sh` (taken again at the head, five runs: the sizes are the same to the printed digit); 2026-11-13 for the stores of the layout landing list
- Revision measured: `0c708a979e9eb5cbb1dca142f514edf96656711e` for the rows taken again, `7f150eed3afb284c2c02220431c3cb858635f361` for those marked "not taken again"; the layout landing list is of `5f069d73cbb90c52c6f3775ccaef53ad59c454ab`, the merged layout commit
- Program and command: the size lines of `v2/numbers.sh`; `size_over_held` is the store's bytes over the bytes of the values put in
- Data: as named in each row
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a 16-core desktop-class build machine; not a user's machine); operating system: unknown; Rust toolchain: unknown; user's machine: no

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| 62,447 commands, 15 indexes | 270.8 MiB, 1.59 | MiB; times its data | 560.6 MiB, 3.29 (2026-11-12) |
| 100,008 passages, 13 indexes | 471.8 MiB, 1.58 | MiB; times its data | 471.8 MiB, 1.58 (2026-11-12) |
| the declared-find store of 100,008 passages | 527.6 MiB, 1.72 | MiB; times its data | 632.8 MiB, 2.06 (2026-11-12) |
| 165,855 passages | 766.3 MiB, 1.55 | MiB; times its data | not recorded |
| 173 session files (not taken again) | 214.5 MiB, 0.62 | MiB; times its data | 227.9 MiB, 0.66 (2026-11-12) |
| 1,000,000 generated vectors, sealed and grouped (not taken again) | 4,991.9 MiB, 1.70 | MiB; times its data | 5,302.9 MiB, 1.81 (2026-11-12) |
| 1,000,000 sightings | 4,990.0 MiB, 1.70 | MiB; times its data | 4,990.0 MiB, 1.70 (2026-11-14 (`090d140ec`) |

## Limitations

- Not within the promise: a small store rewritten whole by a few large commits keeps free space the size of its data (24,000 rows in 4 commits: 3.3 times its data, 5.1 times before the layout fix; it stops growing); rows of about 264 bytes with 12 indexes are 2.7 times their data.
- The engine does not yet report how much disk each table takes.
