# Performance result: opens and the open after a kill on a 16-core desktop-class machine, 2026-11-26

## Promise

- Promise, in the product's words: "Crash recovery time after power loss or a kill does not depend on how much the store holds."
- Counted test that guards it: `a_killed_open_reads_the_log_and_last_commit_pages` (`v2/contextdb/tests/crash/main.rs`) kills a writer at 1,000 and at 20,000 rows and holds the pages the next open reads, of a writer and of a read session, to at most 4 more at 20,000 rows. `a_kill_at_any_storage_call_of_a_commit_leaves_all_or_nothing` (same file) guards atomicity.

## Measurement

- Date: 2026-11-26 (after the main run); revision measured: `672c10078a71d8b24a81ede80490bf979b0bcc79` for the 8 GiB open lines (`opens_8589934592_*`, run alone after the main run: the main run's `opens_measurements` line printed `unavailable`); 2026-11-26 (02:07 to 03:25 UTC); revision measured: `672c10078a71d8b24a81ede80490bf979b0bcc79` (the script's header records one uncommitted file, a script fix made after the run began; the run used the committed script) for the other open lines
- Program and command: the open lines of `v2/numbers.sh` (`v2/numbers.sh` with `NUMBERS_LARGE=1`, `IMPORT=dump-large`, `CAMERA_SECONDS=60`) and the separate run of the 8 GiB flat open
- Data: a flat store of 1,907,000 rows, 7,884.5 MiB, opened clean and opened after its writer was killed; the other stores as named in each row
- Passes and statistic: one run; each open time as the line prints it (one open per figure)
- Resources: not recorded for the opens (unknown)
- Storage: ext4; temporary-store class and cache state not recorded
- Comparison: not applicable (no earlier figure)
- Machine: AMD Ryzen 9 9955HX, 16 cores and 32 threads, 59 GiB of memory (a desktop-class build machine, to itself during the run; not a user's machine); operating system: Ubuntu 26.04.1, Linux 7.0; Rust toolchain: unknown; user's machine: no
- Settings that can change the result: whether the opens were of a writer or a read session is not recorded in the lines

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| clean open of 1,907,000 rows, 7,884.5 MiB (`opens_8589934592_clean_open`) | 11.083 | ms | none |
| open after the writer was killed, same store (`opens_8589934592_killed_open`) | 6.265 | ms | none |
| open of 20,000 notes (2.0 MiB) | 2.087 | ms | none |
| open of 10,000 vectors (53.2 MiB); anonymous memory | 0.113 ms; 13.9 MiB | ms; MiB | none |
| open of 11,588 passages (51.4 MiB); anonymous memory | 0.131 ms; 11.6 MiB | ms; MiB | none |
| open of 100,008 passages (514.8 MiB); anonymous memory | 0.144 ms; 123.4 MiB | ms; MiB | none |
| open of 165,855 passages (888.5 MiB); anonymous memory | 0.136 ms; 200.5 MiB | ms; MiB | none |
| open of 1,000,000 generated vectors (5,087.2 MiB); anonymous memory | 0.150 ms; 6.5 MiB | ms; MiB | none |
| open of 1,000,000 sightings (5,087.2 MiB); anonymous memory | 0.109 ms; 6.6 MiB | ms; MiB | none |
| open of 5,000,000 sightings (26,112.0 MiB); anonymous memory | 0.755 ms; 8.6 MiB | ms; MiB | none |

## Limitations

- The open of the 8 GiB store after a kill (6.265 ms) is faster than its clean open (11.083 ms); one open each, so the order is not a result.
- The stores differ in kind, so these opens are not a controlled comparison of open time against size. The anonymous memory after open of the passage stores grows with the store (11.6, 123.4 and 200.5 MiB at 11,588, 100,008 and 165,855 passages) where the vector stores stay at 6.5 to 8.6 MiB; the cause was not looked for.
- The open after a kill of the other stores was not timed; the killed open of the 8 GiB store is one open.
- Time figures are of this machine only; no timed figure is a promise.
