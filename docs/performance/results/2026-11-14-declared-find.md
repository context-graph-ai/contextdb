# Performance result: the consumer's declared find on a 16-core desktop-class machine, 2026-11-14

## Promise

- Promise, in the product's words: "As a store grows, a vector search stays within the time the product promises". No figure promised. The bars below were set for this step against the figures of 2026-11-12; they are not promises.
- Counted test that guards it: `a_filtered_search_reads_its_index_range_and_a_ranked_one_asks_every_vector` (`v2/contextdb/tests/vectors_statements.rs`) and `an_indexed_filter_reads_only_the_rows_it_returns` (`v2/contextdb/tests/counted_work.rs`) count the vectors compared and the rows read.

## Measurement

- Date: 2026-11-14; revision measured: `0c708a979e9eb5cbb1dca142f514edf96656711e`, five runs, the median of the five printed
- Program and command: the declared-find lines of `v2/numbers.sh` (`queries declared`): the consumer's own find statement, as it sends it, over a store built from its declarations
- Data: 100,008 passages, 13 indexes
- Passes and statistic: five passes after 5 untimed searches; p95 is the median over the five runs of the middle of a run's passes; threads: 1; memory limit: 1,073,741,824 bytes (read session)
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a 16-core desktop-class build machine; not a user's machine); operating system: unknown; Rust toolchain: unknown; user's machine: no
- Settings that can change the result: none recorded

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| one context, p95; rows read, median | 1.462; 37 | ms; rows | 1.489; 37 (2026-11-14, `090d140ec`); bar 1.50 |
| several contexts, p95; rows read, median | 3.913; 36 | ms; rows | 3.913; 36 (2026-11-14, `090d140ec`); bar 4.02 |
| all contexts, p95; rows read, median | 4.136; 36 | ms; rows | 4.167; 36 (2026-11-14, `090d140ec`); bar 4.33 |

## Limitations

- The bars were set by this step's measurement, not by the root documents, which promise no figure.
- Before the fixes of the last piece of SQL work, the same find read 12,004 rows on one context.
