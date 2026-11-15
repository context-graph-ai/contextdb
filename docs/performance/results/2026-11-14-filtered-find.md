# Performance result: the filtered find on a 16-core desktop-class machine, 2026-11-14

## Promise

- Promise, in the product's words: "As a store grows, a vector search stays within the time the product promises". No figure promised for a search under a filter.
- Counted test that guards it: `a_filtered_search_reads_its_index_range_and_a_ranked_one_asks_every_vector` (`v2/contextdb/tests/vectors_statements.rs`) counts the vectors compared by a filtered search. The step's bars for the rows below were measured, not counted.

## Measurement

- Date: 2026-11-14; revision measured: `0c708a979e9eb5cbb1dca142f514edf96656711e` for the first row and the exact figure of the second (five runs, the median of the five printed); every other row keeps the figures of `cb9c072c687d1ab062ee90d9e4a460b2482cbab4` with measuring programs added, not taken again at the head
- Program and command: the filtered-find rows of the step's measurement (`passages`, `measure landed 5000000`)
- Data: 100,008 passages for the first two rows; a store of 5,000,000 rows for the others, with a filter passing the rows named and the key ranges named
- Passes and statistic: five passes for the passages, three for the others, each value as printed; threads: 1; memory limit: 1,073,741,824 bytes (read session); release build
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a 16-core desktop-class build machine; not a user's machine); operating system: unknown; Rust toolchain: unknown; user's machine: no
- Settings that can change the result: search mode as named in the rows

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| a filter passing 992 rows on a column with the advised index present, indexed, p95; rows read | 32.653; 2,558 rows read | ms | 32.961, 32.731, 33.000, 32.468, 32.693; 2,558 rows read (2026-11-14, `cb9c072c6`); 251 ms, 100,018 rows read before the last fixes |
| the same filter, exact, p95; rows read | 38.122; 2,561 rows read | ms | 38.3 to 39.5 (2026-11-14, `cb9c072c6`); the rest of this row (no index, 33.054, 32.892, 32.629, 32.620, 32.847) is not taken again |
| a filter passing 1,000 rows, rows compared one by one | 3.117, 3.062, 3.070 | ms | 2.98 (before the last fixes) |
| a filter passing 10,000 rows | 36.866, 36.774, 38.023 | ms | 37.4 to 39.9 |
| a filter passing 100,000 rows | 453.450, 442.631, 444.045, against the search 440.796, 442.036, 441.162 | ms | 618.6 |
| a key range of 39,000 rows of 5,000,000 (inside the one-by-one bound of 39,062), `small_set: yes` | 107.016, 112.535, 108.126; 39,010 rows read | ms | not measured |
| a key range of 39,200 rows (past the bound), `small_set: no` | 459.486, 459.392, 461.319; 1,455 rows read | ms | not measured |

## Limitations

- The step across the bound is 4.3 times in time (about 108 ms to 460 ms) at this size.
- Time figures are of this machine only; no timed figure is a promise.
