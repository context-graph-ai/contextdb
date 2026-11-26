# Performance result: the consumer's declared find on a 16-core desktop-class machine, 2026-11-26

## Promise

- Promise, in the product's words: "A growing store keeps its search time". No figure promised.
- Counted test that guards it: `a_filtered_search_reads_its_index_range_and_a_ranked_one_asks_every_vector` (`v2/contextdb/tests/vectors_statements.rs`) and the indexed filters of `v2/contextdb/tests/sql/counted_work.sql` count the vectors compared and the rows read. `a_kept_count_is_the_number_of_rows_after_every_kind_of_write_and_reads_no_row` (`v2/contextdb/tests/kept_counts.rs`) counts the rows a kept count reads.

## Measurement

- Date: 2026-11-26 (02:07 to 03:25 UTC); revision measured: `672c10078a71d8b24a81ede80490bf979b0bcc79` (the script's header records one uncommitted file, a script fix made after the run began; the run used the committed script)
- Program and command: `v2/numbers.sh` with `NUMBERS_LARGE=1`, `IMPORT=dump-large`, `CAMERA_SECONDS=60`; lines `declared_*`, `kept_count_context_*`
- Data: 100,008 passages loaded through the consumer's own declarations (`declared_100008_*`); the consumer's find statement over one context, several or all; 100 questions
- Passes and statistic: one run; p50 and p95 as the lines print them; threads: 1
- Resources: engine memory limit: 1,073,741,824 bytes in the find sessions (the `_session` lines), threads 1; enforced process memory: not recorded; cores available: 16 (32 threads)
- Storage: ext4; temporary-store class and cache state not recorded
- Comparison: previous figures are of `0c708a979e9eb5cbb1dca142f514edf96656711e` (2026-11-14, five runs, the median printed); not a controlled comparison
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a desktop-class build machine, to itself during the run; not a user's machine); operating system: Ubuntu 26.04.1, Linux 7.0; Rust toolchain: unknown; user's machine: no
- Settings that can change the result: build rustflags none; popcount counting; index reader limit 1,073,741,824 bytes; index threads 1; search mode as named in each row

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| one context, p95; rows read, median and p95 | 2.580; 113 and 164 | ms; rows | 1.462; 37 (2026-11-14) |
| several contexts, p95; rows read, median and p95 | 6.435; 139 and 191 | ms; rows | 3.913; 36 (2026-11-14) |
| all contexts, p95; rows read, median and p95 | 53.885; 12,702 and 12,749 | ms; rows | 4.136; 36 (2026-11-14) |
| the ten nearest the same as exact search's, one, several, all | 100 of 100, 100 of 100, 98 of 100 | questions | not recorded |
| count of a context's rows, p95; rows examined | 0.387; 10,045 | ms; rows | none |
| count of a context's rows on a table that keeps the count, p95; rows examined | 0.006; 0 | ms; rows | none |
| the store the declarations build | 565.9 MiB; 5,933 bytes a passage; 1.84 times its data | MiB | 527.6 MiB, 1.72, 2026-11-14 |
| load | 21.9 | s | none |

## Limitations

- Not like for like with 2026-11-14: the passages table now also declares Context Graph's own index for its search filter (`event_search_passages_active_idx` on context, generation, is_current and is_searchable, as Context Graph's schema declares it), without which this commit refuses the INDEXED cells, and the engine is the one merged since (pull request 29). The slower times and larger rows-read counts against 2026-11-14 are not yet explained; a run of both commits on the same schema is what settles it.
- **The rows the find reads are higher than on 2026-11-14.** One context read 113 rows at the median, against 37; all contexts read 12,702, against 36; the all-contexts p95 is 53.885 ms against 4.136 ms. The cause was not looked for in this result.
- Over all contexts, 98 of 100 questions found the same ten as exact search.
- A single run; no timed figure is a promise.
