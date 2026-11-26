# Performance result: index rebuilds and recall as a store grows on a 16-core desktop-class machine, 2026-11-26

## Promise

- Promise, in the product's words: "The engine's own maintenance does what keeps it there: it rebuilds the index with more clusters as the data grows ... The host does nothing for it." (the paragraph "A growing store keeps its search time"). No figure promised for recall.
- Counted test that guards it: `the_worker_runs_cycles_until_nothing_is_left` and `a_partition_is_grouped_again_only_when_its_changes_reach_the_declared_share` (`v2/contextdb/tests/vectors_maintenance.rs`) guard automatic maintenance and regrouping. None yet for searches at 10 and 20 million vectors ([their result](2026-11-26-find-10m-20m.md)).

## Measurement

- Date: 2026-11-26 (02:07 to 03:25 UTC); revision measured: `672c10078a71d8b24a81ede80490bf979b0bcc79` (the script's header records one uncommitted file, a script fix made after the run began; the run used the committed script)
- Program and command: `v2/numbers.sh` with `NUMBERS_LARGE=1`, `IMPORT=dump-large`, `CAMERA_SECONDS=60`; the `*_maintenance*` lines and the recall lines of the find groups. The `rebuild_20000_*` lines are from `measure rebuild 20000` run again on the same machine on 2026-11-26 after a fix to that case: in the numbers.sh run its column kept the default indexing threshold of 100,000, so its 40,000 vectors were never grouped and every mode compared every vector. The case now declares `INDEXING_THRESHOLD 10000` and refuses to report finds over a partition without groups.
- Data: as named in each row; recall is the share of the recorded true ten nearest found, `all_ten` the questions that found all ten, over 100 questions
- Passes and statistic: one run; values as the lines print them; threads: 1; memory limit: 1,073,741,824 bytes in the find sessions
- Resources: cores available: 16 (32 threads)
- Storage: ext4; temporary-store class and cache state not recorded
- Comparison: not applicable (no earlier figure for these lines)
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a desktop-class build machine, to itself during the run; not a user's machine); operating system: Ubuntu 26.04.1, Linux 7.0; Rust toolchain: unknown; user's machine: no
- Settings that can change the result: search mode as named in each row

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| the `rebuild_20000` store: groups after the first 20,000 vectors and after 20,000 more, moved, with maintenance after each | 141, then 200 | groups | none |
| the `rebuild_20000` store: exact, indexed and automatic p95 | 7.687, 3.685 and 3.587; indexed and automatic recall 1.0000 (100 of 100); vectors compared by exact 40,092.2, by indexed and automatic 43,502.7 each on average | ms | none |
| maintenance after a load of 100,008 passages: time, cycles, partitions | 12.0 s; 26 cycles; 8 partitions | s | none |
| maintenance after a load of 165,855 passages | 18.1 s; 31 cycles; 8 partitions | s | none |
| maintenance after a load of 1,000,000 sightings | 254.0 s; 1,131 cycles; 1 partition | s | 343.4 s; 1,123 cycles, 2026-11-14 |
| maintenance after a load of 5,000,000 sightings | 1,225.8 s; 7,124 cycles; 1 partition | s | none |
| indexed recall, 11,588 passages: one, several, all partitions | 1.0000, 1.0000, 1.0000 (100 of 100 each) | recall | none |
| indexed recall, 100,008 passages: one, several, all | 1.0000, 1.0000, 0.9980 (100, 100, 98 of 100) | recall | 1.0000, 0.9990, 0.9990 (find at 100,008 vectors, 2026-11-14) |
| indexed recall, 165,855 passages: one, several, all | 0.9990, 0.9990, 1.0000 (99, 99, 100 of 100) | recall | none |
| indexed recall, 1,000,000 sightings and 5,000,000 sightings | 1.0000 (100 of 100) at both | recall | none |
| 10,000,000 sightings, loose and unbounded shapes, recall | 0.8037 | recall | none |
| 20,000,000 sightings, loose and unbounded shapes, recall | 0.8350 | recall | none |

## Limitations

- Recall is lower than 1.0000 at 100,008 and 165,855 passages in places (0.9980 over all partitions at 100,008; 0.9990 over one or several partitions at 165,855), and 0.8037 and 0.8350 for the loose and unbounded shapes at 10,000,000 and 20,000,000 sightings ([their result](2026-11-26-find-10m-20m.md), of a probe tree on another date).
- The `rebuild_20000` store is 20,000 generated vectors, then 20,000 more pointing the other way, with maintenance run after each; the second load is what makes maintenance group the partition again, from 141 groups to 200. Half the questions are turned the other way to ask about the second load.
- On that store the indexed and automatic finds compare more vectors on average than exact does (43,502.7 against 40,092.2), and not fewer on 94 of the 100 questions, though they take about half its time. The lines do not say what the extra comparisons are.
- That the engine rebuilt the index with more clusters as the data grew is shown only by the `rebuild_20000` store (141 groups to 200); the other maintenance lines show the cycles and partitions after a load.
- No timed figure is a promise; a single run.
