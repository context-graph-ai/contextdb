# Performance

What contextdb promises about speed and size, the test that guards each promise, and the last
figure measured against it. The page has two parts: the bars of the engine in `v2/` (contextdb
2), and, further down, how the old engine (contextdb 1, the packages under `crates/`) is measured.

## The bars of the engine in `v2/`

**How a promise is kept.** A promise of speed or size is guarded by a test that the check of every
pull request runs and that counts the work an operation does, such as rows read and vectors
compared; it fails when the count grows, on any machine. No test decides by a clock. A timed
figure is taken on a named machine and printed beside the promise, with the machine named and the
date. A promise is stated only where the product's own documents state it: where they give no
figure, the row says "no figure promised", and a figure in the last column is a measurement, not a
promise. Where no counted test guards a promise yet, the row says "none yet" and what is not built or not guarded yet. The table does not assert that a counted test proves a latency distribution.

**The measurements.** Landing figures are marked "measured at landing" until the controlled
comparison is recorded. Earlier figures stay in their linked result files with their own machine,
date and source commit; they are historical measurements, not results for this checkout.

**How this folder is used.** A measurement is filed as a result in `results/`, from
[`results/TEMPLATE.md`](results/TEMPLATE.md), named `<date>-<short-name>.md`. The table's row takes
its last figure from its result file and links it. A promise enters the table when it enters the
product's root documents, with the counted test that guards it or "none yet".

| Promise, in the product's words | Counted test that guards it | Last measured figure | Machine and date |
|---|---|---|---|
| **Find time at 100,000 vectors, with recall.** "As a store grows, a vector search stays within the time the product promises." No figure promised for the time or for recall. | `an_indexed_search_does_fewer_distance_evaluations_than_exact` (`v2/contextdb/tests/vectors_index.rs`) counts the vectors compared on a small store. `a_hundred_thousand_sightings_are_found_with_the_recorded_work_recall_and_length` (`v2/contextdb/tests/find_at_scale.rs`, release build, every pull request) holds the vectors compared by 100 indexed searches of 100,000 generated sightings, the questions whose ten hold all of exact search's ten, and the data file's length to the recorded numbers. | measured at landing; historical: [Result](results/2026-11-14-find-100k.md) | Machine, date and measured commit accompany the result. |
| **Find time at 1,000,000 vectors, with recall.** The same promise; no figure promised. | `a_million_sightings_are_found_with_the_recorded_work_recall_and_length` (`v2/contextdb/tests/find_at_scale.rs`, release build, nightly) holds the same three numbers at 1,000,000 generated sightings. | measured at landing; historical: [Result](results/2026-11-14-find-1m.md) | Machine, date and measured commit accompany the result. |
| **Find time at 5,000,000 vectors, with recall.** The same promise; no figure promised. | None yet. | measured at landing; historical: [Result](results/2026-11-14-find-5m.md) | Machine, date and measured commit accompany the result. |
| **The consumer's declared find** (a search over one context, several, or all). "A growing store keeps its search time." No figure promised. | `a_filtered_search_reads_its_index_range_and_a_ranked_one_asks_every_vector` (`v2/contextdb/tests/vectors_statements.rs`) and the indexed filters of `v2/contextdb/tests/sql/counted_work.sql` count the vectors compared and the rows read. | measured at landing; historical: [Result](results/2026-11-14-declared-find.md), [filtered finds](results/2026-11-14-filtered-find.md) | Machine, date and measured commit accompany the result. |
| **Searches stay fast while writes arrive.** "When new vectors arrive faster than maintenance keeps up with them, searches are protected and the writer is slowed. ... a search made in the middle of it stays fast." | `the_worker_runs_cycles_until_nothing_is_left` and `sealing_advances_only_when_asked_and_in_bounded_cycles` (`v2/contextdb/tests/vectors_maintenance.rs`) guard automatic progress and bounded sealing. None yet for the complete overlapping-write latency promise. | measured at landing | Machine, date and measured commit accompany the result. |
| **Import rate.** "A large import therefore runs only as fast as maintenance keeps up." No figure promised. | `the_worker_runs_cycles_until_nothing_is_left` and `a_cycle_reads_no_row_to_seal_or_pack` (`v2/contextdb/tests/vectors_maintenance.rs`, `cycle_work.rs`) guard progress and sealing work. None yet for the complete paced-import promise. | measured at landing; historical: [Result](results/2026-11-14-import-rate.md) | Machine, date and measured commit accompany the result. |
| **A store in steady use stays within twice its data, and reports what each table takes.** "After maintenance, a store that has been written to and expired for any length of time takes at most twice the bytes of the data it holds now. ... The engine reports how much disk each table takes and how much free space the store holds." | `a_store_with_twelve_indexes_stays_under_twice_its_bytes_and_under_its_recorded_size` and `two_hundred_rows_of_50_000_byte_indexed_values_make_a_store_smaller_than_twice_their_bytes` (`v2/contextdb/tests/long_keys.rs`) guard freshly loaded stores. `Database::verify` reports content bytes per table; physical disk/free-space reporting, bounded compaction and the long-run guard are not built yet. | measured at landing; historical: [Result](results/2026-11-14-store-size.md) | Machine, date and measured commit accompany the result. |
| **Running for months does not creep.** "Once a table's retention period is full and old rows expire as fast as new ones arrive, the store stays level however long it runs without a restart: the disk it takes, the memory its process holds and the work a search does stop growing." | None yet: the engine does not yet check this. | measured at landing | Machine, date and measured commit accompany the result. |
| **Crash recovery time does not depend on the store's size.** "Crash recovery time after power loss or a kill does not depend on how much the store holds." | None yet for size-independent recovery: this version still reads used pages after a killed writer. `a_kill_at_any_storage_call_of_a_commit_leaves_all_or_nothing` (`v2/contextdb/tests/crash/main.rs`) guards atomicity. | measured at landing | Machine, date and measured commit accompany the result. |
| **Sync runs by itself and sends only what changed.** "By default sync runs on a built-in schedule, which the host may change; nobody has to start it. ... sends only what has changed since then, so what crosses the network grows with what changed, not with the size of the store." | None yet: not built. | Not built yet. | Not applicable. |
| **A sync exchange that is cut off carries on.** "An exchange that is cut off carries on from where it stopped rather than starting again." | None yet: not built. | Not built yet. | Not applicable. |
| **A growing store keeps its search time.** "The engine's own maintenance does what keeps it there: it rebuilds the index with more clusters as the data grows ... The host does nothing for it." | `the_worker_runs_cycles_until_nothing_is_left` and `a_partition_is_grouped_again_only_when_its_changes_reach_the_declared_share` (`v2/contextdb/tests/vectors_maintenance.rs`) guard automatic maintenance and regrouping, including retraining at a doubling or turnover. None yet for searches at 10 and 20 million vectors. | measured at landing | Machine, date and measured commit accompany the result. |
| **Every promised speed and size is guarded by counting work.** "A promise of speed or size is written into the root documents only with the counted test that guards it named." | The rows above name the tests that exist (`v2/contextdb/tests/sql/counted_work.sql`, `v2/contextdb/tests/sql/query_work.sql`, `counted_work.rs`, `vectors_index.rs`, `vectors_statements.rs`, `long_keys.rs`); every row that says "none yet" still lacks its guard. | measured at landing | Machine, date and measured commit accompany the result. |
| **One commit.** "Rows, relationships, vectors and the index state built from them commit together or not at all." No figure promised for its speed. | `a_kill_at_any_storage_call_of_a_commit_leaves_all_or_nothing` and `a_kill_inside_a_commit_of_vectors_keeps_rows_and_vectors_together` (`v2/contextdb/tests/crash/main.rs`) kill the process at every storage call of a commit and check that all of it or none of it is there. | measured at landing; historical: [Result](results/2026-11-14-one-commit.md) | Machine, date and measured commit accompany the result. |

For an operator, whole-file length is `wc -c < ./notes/data` and allocated KiB is
`du -k ./notes/data`; stop the writer before comparing a stable copy. Compare those with
the application's own live-data bytes. The shell has no per-table disk/free-space report
or verification command; `Database::verify()` is an embedded API for content bytes.
Deletes free pages for later reuse, and the maintenance cycle compacts them: it moves at most 256 pages toward the start of the file in one cycle once the reclaimable pages reach the start ratio of the file (0.05 by default), and the free pages left at the end are cut off `data`.
The fresh-load guards above do not establish the long-run twice-live-data promise.

## The old engine (contextdb 1): how it is measured

Everything below this heading is about the old engine, the packages under `crates/`, and is kept
with its headings one level lower and its publishing steps adjusted to the new template; it does not describe contextdb 2.

How a contextdb change is measured, how a measurement is judged against the
product's acceptance bars, and how a reviewed result becomes public. **The
whole journey is manual.** Nothing in CI measures performance, compares a
baseline, or publishes a result.

This page is the only home for the performance run commands. Other pages link
here instead of repeating them. [`scripts/ci-performance.py`](../../scripts/ci-performance.py)
is the executable truth for profiles and commands; if this page and the script
ever disagree, the script wins and this page is wrong.

### Current state

- **No official measured result.** There is no official measured run yet, so
  there is no "latest result". [Measured results](#measured-results) lists one
  only after a reviewed summary has been merged under [`results/`](results/).
- The [`performance.yml`](../../.github/workflows/performance.yml) workflow
  (`Performance measurement (manual guide)`) is a **dormant placeholder that
  cannot run today**: its only trigger is a push filter that excludes every
  branch and every tag, and its manual-dispatch trigger is commented out. It
  never starts, never reports a status, checks out nothing, runs nothing and is
  not a required check. Enabling it is an owner-only future step (see
  [Future automation](#future-automation-owner-only-todo)).
- **The benchmarks are descriptive.** The product's acceptance bars are listed
  under [Acceptance bars](#acceptance-bars); this page and the benchmarks do not
  define, relax or replace them. Benchmark sources contain their own
  assertions (correctness, locality and internal budget checks). Those are real
  checks — a failing assertion is a failure — but an assertion in a benchmark
  is not a product acceptance bar, and a benchmark run that completes is not
  acceptance evidence.

### Acceptance bars

The rows below specify required acceptance measurements for storage growth: imports
of 10,000 and 100,000 passages, search, event append, open, memory, recall and
index build, measured as engine operations. They come from the owner-maintained
product acceptance intent with internal tracking references removed; every
engine number, statistic and scope is unchanged. The program that drives the
operations is recorded in the run's command metadata; it describes how the run
was issued and does not own or change the result.
Only the owner changes them. A result is judged against a row only by quoting that
row's number, statistic and scope; nothing else on this page or in a benchmark
source is an acceptance bar.

The index-dependent rows below are qualification requirements, not shipped capabilities or completed measurements. This release ships exact search only; index acceleration remains unqualified.
<!-- enforced by: a_search_is_exact_with_no_index_built::an_auto_find_answers_exactly_after_commit_with_no_maintenance, a_search_is_exact_with_no_index_built::indexed_is_refused_at_every_door, a_search_is_exact_with_no_index_built::no_cycle_or_commit_builds_or_writes_an_index, a_search_is_exact_with_no_index_built::declared_index_policy_is_recorded_and_has_no_effect -->

Here, p95 is the 95th-percentile latency. `EXACT` and `AUTO` compare allowed stored vectors within the active budgets in this release; `INDEXED` is refused. Recall@10 measures agreement with EXACT’s nearest ten; every measured query must reach ≥ 0.95.
<!-- enforced by: a_search_is_exact_with_no_index_built::an_auto_find_answers_exactly_after_commit_with_no_maintenance, a_search_is_exact_with_no_index_built::indexed_is_refused_at_every_door -->

| Required outcome | Acceptance evidence |
|---|---|
| Complete imports | Separate 10,000- and 100,000-passage imports finish, all passages searchable, on a 6 GiB machine with the default 1,610,612,736-byte writer budget and unchanged declarations/workload. Refusal of a required import or append fails acceptance. |
| Continued growth | After each import, append at least the same number of passages again on the same machine, declarations, workload and budgets. Before measurement, fix a working-memory ceiling within the writer budget and account for write buffers, rows/indexes, retained snapshots, caches and maintenance. Measure multiple checkpoints and comparable warm steady states. Report charged bytes by the engine component holding them, process RSS, cgroup peak and live-content size throughout. Memory must settle within the fixed working set under steady active workload as durable content grows. |
| Paging and eviction | Pair doubling measurements with source ownership and eviction proof. Durable rows, indexes and their metadata must load on demand rather than remain wholly resident. Reducing bytes per row still fails if memory keeps growing with every stored row; finite doubling alone cannot prove boundedness. |
| Grown-store operation | Measure writable open, append and overlapping maintenance with bounded-work proof. Writable open needs measured work/memory evidence, without a new numeric latency threshold. Record transient process peaks separately (see [Unpublished numeric limits](#unpublished-numeric-limits)); cache/model lifecycles mean peaks need not match initial import-end memory. |
| Read scale | Live committed row/vector content must exceed each of two smaller whole-process memory limits, each explicitly recorded as the enforced reader cgroup `memory.max` together with the matching engine limits; the dataset larger than the 6 GiB reader ceiling is no longer mandatory. At each limit, open and bounded indexed queries succeed; record live-content size, compacted file size, enforced `memory.max`, cgroup peak, RSS and charged memory. Live-content size is the engine's committed row-plus-vector bytes; compacted file size is reported separately and is not the size measure. Padding or reclaimed/sparse bytes do not count. Every other required import, growth, correctness and performance cell keeps its existing default budget. Preserve read-only open at 10k/8 ≤ 3.0 s and anonymous RSS ≤ 700 MiB. |
| First search | A fresh process searches the maintained 100k store from saved index state, with zero query graph-build work and a complete answer. Measure open, first search in that process, and warm search separately. Preserve declared read work/time/memory budgets and startup proof. |
| Index acceleration | Apply the aggregate AUTO threshold before costing selected partitions. For each fixed scope/mode query set using graphs, mean distance work must be strictly below EXACT; report equal-or-greater-work queries and their latency distribution. Mandatory 10k single-Context AUTO and INDEXED use graphs with strictly lower mean work and p95 than EXACT. Graph-using multi-Context scopes have p95 ≤ EXACT. All-exact AUTO requires equal work and paired, interleaved timing within measured noise, including planning. Strict improvement is not required at every size or for every query. Preserve per-query recall@10 ≥ 0.95 and ≤ 30 s for the 10k single-partition build. |
| Newest event | Binding the full equality prefix and ordering with `LIMIT 1` examines equal entry counts at 10 and 20,010 rows, beside neighbouring streams. Staged rows remain visible; index order avoids a separate sort inside and outside transactions. One-event append retains ≤ 25 ms warm p95. |
| Resume | Compare two runs with increasing amounts of completed work, the identical pending tail and a fixed limit. Seeking strictly after the committed position returns ordered next rows without examining completed entries or neighbouring streams. Record examined-entry counts; returned-row counts alone cannot prove this. |

These areas also remain binding for imports and vector search: exact work counts, acceleration, throughput, append, read-only open, maintenance, schema, snapshots, resources, whole-process memory, sustained writers, recall and 100k search latency. Continuous-writer reads, status reads and existing refusal budgets remain binding.

#### Unpublished numeric limits

Two memory limits are part of the acceptance scope but have no number
published here: the whole-process memory ceiling for grown-store transient
peaks, and the 100,000-passage open, search and read memory limit. Each is
defined by the run's recorded enforced limit. That is a scope definition only:
until a number is published on this page, neither is a `PASS` criterion and a
summary reports them as `NOT RUN`.

What this means for a run made with this guide:

- **No existing benchmark profile implements these rows.** The profiles below
  run the repository's descriptive benchmarks; there is no acceptance runner in
  this repository, and nothing here runs these rows today.
- A row that was not run, or was run under conditions that do not match its
  scope, is `NOT RUN` or `NOT EVIDENCE` — never `PASS`. A benchmark number that
  happens to look like an [acceptance threshold](#acceptance-bars) (for example a millisecond budget asserted inside a
  bench source) is not that row's evidence.
- The binding areas the final paragraph lists without a number have no
  acceptance threshold published in this repository. Until one is published, a
  summary cannot report `PASS` for them and reports them as `NOT RUN`.

### Why not GitHub-hosted runners

GitHub-hosted runners are shared virtual machines with variable neighbours,
disk and CPU. A timing taken there says nothing reliable about this code, so
`scripts/verify performance run` refuses to execute when `CI` or
`GITHUB_ACTIONS` is set. Nothing hosted measures or qualifies performance. A
dedicated self-hosted machine is an owner-only future step (see
[Future automation](#future-automation-owner-only-todo)) and is not configured.

### The journey

#### 1. See what the change touches (CI-only step)

This step happens only in CI: `scripts/ci-impact.py` reads the GitHub event and
output files, so it has no local mode. The `impact` job of a PR run prints one
broad answer per change in its summary: `Performance measurement:` either
**measurement pending** (any runtime, integration, benchmark fixture,
performance metadata or unrecognised path changed, or the diff could not be
read) or **not affected** (documentation or maintainer metadata only). Those are
the human labels; the machine output `performance_qualification` keeps its
compatibility values `pending-controlled-native-qualification` and
`unaffected`. Neither label means anything was qualified, and it is not a
required gate. It
does **not** say which profile to run. The per-path classes it prints are a
hint for the next step, not proof that other areas are unaffected.

#### 2. Choose a profile

Profiles come from [`scripts/ci-performance.py`](../../scripts/ci-performance.py)
(`PROFILES`); what each target measures is in the
[benchmark catalogue](../benchmarking.md).

| Profile | Benchmark targets |
|---|---|
| `storage` | `contextdb-engine`: `storage_scale_hardening`, `engine_full_throughput` |
| `vector` | `contextdb-engine`: `engine_throughput` (Criterion filter `nv_`), `rank_policy_recall` |
| `workflows` | `contextdb-engine`: `mixed_workflows_pr`, `subscriptions_pr`; `contextdb-server`: `sync_pr` |
| `system` | release build of `contextdb-cli` and `contextdb-server`, then `contextdb-server`: `server_throughput`, `server_sync_system` |
| `all` | the release build above, then every benchmark target declared in the crate manifests, unfiltered |

Every profile runs full Criterion defaults with `CONTEXTDB_NV_BENCH_FULL=1`
forced by the script; there is no reduced "smoke" sizing in this journey.

Pick the profile whose code the change touches and have the owner review the
choice. A change to shared code (storage engine internals used by everything,
dependencies, `Cargo.lock`, build settings) or one whose reach you cannot state
confidently takes the broader choice, normally `all`. Mapping a path to a
profile is a judgement made here and reviewed by the owner; nothing in CI makes
or enforces it.

A single-profile run is a **partial**, descriptive measurement of that area
only. It is never a global performance pass; the receipt says so in
`global_performance_qualification` and `not_run_benchmarks`.

Print exactly what would run for the current checkout (source SHA, dirty
state, fixture and dependency file hashes, commands, selected, filtered and
not-run benchmarks) without running or downloading anything:

```bash
bash scripts/verify performance manifest --profile <all|storage|vector|workflows|system>
```

#### 3. Prepare two clean checkouts on one controlled host

The current native runner is Linux-only. It reads Linux `/proc` and `/sys`
cgroup interfaces, calls util-linux `lscpu`, and calls GNU `df -T`; those
interfaces and commands must be available on the host. The script does not
support macOS and makes no portability claim or fallback for other systems.

You need a **baseline** commit (normally the target branch tip the change is
based on) and a **candidate** commit (the change), each in its own clean,
separate checkout, on the **same** native machine.

- Commit first. `run` refuses a dirty checkout (`git status --porcelain` not
  empty), and that includes untracked files such as a new, uncommitted doc.
- Use a disposable checkout per commit. Each run builds into that checkout's own
  `target/` (the system fixtures require that fixed path), so the run owns that
  target directory exclusively. Never share it with another build.
- Nothing else heavy runs on the machine during a measurement: no other Cargo
  build, inference, benchmark or bulk I/O. A Cargo build lock does not isolate
  runtime measurements.
- Unset `TMPDIR` and every `CONTEXTDB_NV_*` / `CONTEXTDB_SSH_*` variable; `run`
  refuses them because they redirect storage or change the frozen fixtures.
  Do not run with `CI` or `GITHUB_ACTIONS` set.
- Keep the same toolchain (from `rust-toolchain.toml`), build environment and
  resource limits for both runs.

Optionally freeze the plan for each checkout without running anything:

```bash
bash scripts/verify performance report --profile <profile> --output <new-dir-outside-checkout>
```

`report` writes `receipt.json` (the manifest plus a suggested native output
directory) into a directory that must not exist yet. It does not run
benchmarks or fetch anything.

#### 4. Run baseline, then candidate

In the baseline checkout, then in the candidate checkout:

```bash
env -u TMPDIR bash scripts/verify performance run --native-host --profile <profile> \
  --output <new-dir-outside-checkout>
```

- `--native-host` is your statement that this is a controlled, isolated native
  machine; `run` refuses without it.
- `--output` must name a directory that does not exist yet; the script refuses
  an existing one. Put it **outside the checkout** — recommended, not enforced:
  the script accepts a path inside the checkout, but the new files then make
  that checkout dirty and the next `run` there refuses. Without `--output`,
  `run` creates a unique directory beside the checkout named
  `contextdb-performance-receipts/<sha>-<profile>-<time>-<id>`.
- `--timeout-seconds` (default 3600) is a per-command runaway budget, not a
  latency figure or an acceptance bar.

Each run writes a **private** directory: `PRIVATE.txt`, `receipt.json` (source
SHA, fixture hashes, commands, host platform, CPU, memory, filesystem, cgroup
and resource limits, toolchain, per-command exit status and elapsed seconds,
completeness counts), one `command-<n>.log` per command, and a `criterion/`
copy of the Criterion estimate files written during that run. Keep both
directories privately; nothing uploads them.

`run` does not read a `report` receipt, compare against a baseline, publish
anything, or change tracked files. A run that stops on a failing command
records `failed-native-execution`; a failing benchmark assertion is a real
failure, not noise.

#### 5. Compare manually

The owner (or the contributor, for review by the owner) compares the two runs:

- Confirm both receipts name the intended baseline and candidate SHAs, the
  same profile, the same fixture hashes where the change did not touch
  fixtures, and the same host, toolchain and resource limits. If the workload
  or configuration differs, the comparison is invalid.
- Criterion targets: compare the `criterion/**/estimates.json` values between
  the two private directories. Criterion's own "change" line compares against
  whatever earlier run sat in that checkout's `target/`, which is not your
  baseline.
- Custom harness output and assertions are in `command-<n>.log`; read and
  interpret them. A zero exit means the built-in assertions held, not that the
  numbers are acceptable.
- Never use a command's elapsed seconds from `receipt.json` as a latency
  figure. It includes compilation and fixture setup.
- Report the comparison as descriptive numbers. Judge a number against an
  [acceptance bar](#acceptance-bars) only when the run actually matches that
  row's scope, and never loosen an acceptance bar to make a result pass.

### Publishing a result (manual)

Raw receipts and logs contain host identity, absolute paths and cgroup details.
They stay private. What becomes public is a short, human-curated summary,
reviewed like any other change:

1. Copy [`results/TEMPLATE.md`](results/TEMPLATE.md) to
   `docs/performance/results/<unique-id>.md`, for example
   `<date>-<profile>-<short-candidate-sha>.md`.
2. Fill in only reviewed public fields. The template is laid out for the engine in `v2/`; for the
   old engine, also state the acceptance rows you report on from
   [Acceptance bars](#acceptance-bars), quoted verbatim, each `PASS`, `FAIL`,
   `NOT RUN` or `NOT EVIDENCE` with its evidence and coverage. Never paste the
   raw receipt, logs, filesystem paths, hostnames, user names, cgroup paths or
   any secret.
3. Add a line to [Measured results](#measured-results) starting with its Kind and Status, then area, hardware label, date and the exact candidate
   source SHA that was measured. Link the run that was actually measured,
   never "the latest commit".
4. Open a normal pull request. The owner reviews and merges it. Merging a
   result does not tag a release, change branch protection or make any check
   required.

Kinds: `official` (an owner-run or owner-accepted measurement on a controlled
host), `community` (a contributor's machine-specific run, welcome as evidence
in a PR discussion), `historical` (measured before this template existed,
reconstructed from what was kept). Only the owner adds entries to the official
list.

### Measured results

#### Official

None yet. Entries take the form: `Kind: official · Status: <…>` · area
(profile) · hardware label · date · candidate SHA · link to the summary under
`results/`.

#### Community and historical

None published. Community and historical entries are listed here, never under
Official, and every one starts with its Kind and Status.

### Future automation (owner-only TODO)

Not implemented. `performance.yml` is a dormant placeholder: it cannot start
on any push, tag or dispatch, and its manual-dispatch trigger stays commented
out until the owner enables it. Changing its runner line alone would not make it
a measurement. The intended shape, recorded here and in the `performance.yml`
header: a dedicated self-hosted runner on an exclusive machine, registered by
the owner, with dispatch restricted to the owner, runs one exact reviewed
commit under one exclusive target directory and resource envelope, using the
same `run --native-host` command as above; turning that run into a public
summary stays a separate, explicit step.

None of that exists today. `run` currently refuses to execute whenever `CI` or
`GITHUB_ACTIONS` is set, which includes any GitHub Actions runner, so runner
admission in the script, resource isolation, security controls and the
execution steps for such a runner are still to be built. There is no self-hosted runner, no secret, no
fork execution and no scheduled performance run.
