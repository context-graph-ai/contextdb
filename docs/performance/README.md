# Performance measurement

How a contextdb change is measured, how a measurement is judged against the
product's acceptance bars, and how a reviewed result becomes public. **The
whole journey is manual.** Nothing in CI measures performance, compares a
baseline, or publishes a result.

This page is the only home for the performance run commands. Other pages link
here instead of repeating them. [`scripts/ci-performance.py`](../../scripts/ci-performance.py)
is the executable truth for profiles and commands; if this page and the script
ever disagree, the script wins and this page is wrong.

## Current state

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

## Acceptance bars

The rows below are the engine's acceptance evidence for bounded storage: imports
of 10,000 and 100,000 passages, search, event append, open, memory, recall and
index build, measured as engine operations. They come from the owner-maintained
product acceptance intent with internal tracking references removed; every
engine number, statistic and scope is unchanged. The program that drives the
operations is recorded in the run's command metadata; it describes how the run
was issued and does not own or change the result.
Only the owner changes them. A result is judged against a row only by quoting that
row's number, statistic and scope; nothing else on this page or in a benchmark
source is an acceptance bar.

Here, p95 is the 95th-percentile latency. `EXACT` compares every allowed stored vector; `INDEXED` requires the maintained index route; `AUTO` chooses within declared thresholds and budgets. Recall@10 measures agreement with EXACT’s nearest ten; every measured query must reach ≥ 0.95.

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

### Unpublished numeric limits

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

## Why not GitHub-hosted runners

GitHub-hosted runners are shared virtual machines with variable neighbours,
disk and CPU. A timing taken there says nothing reliable about this code, so
`scripts/verify performance run` refuses to execute when `CI` or
`GITHUB_ACTIONS` is set. Nothing hosted measures or qualifies performance. A
dedicated self-hosted machine is an owner-only future step (see
[Future automation](#future-automation-owner-only-todo)) and is not configured.

## The journey

### 1. See what the change touches (CI-only step)

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

### 2. Choose a profile

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

### 3. Prepare two clean checkouts on one controlled host

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

### 4. Run baseline, then candidate

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

### 5. Compare manually

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

## Publishing a result (manual)

Raw receipts and logs contain host identity, absolute paths and cgroup details.
They stay private. What becomes public is a short, human-curated summary,
reviewed like any other change:

1. Copy [`results/TEMPLATE.md`](results/TEMPLATE.md) to
   `docs/performance/results/<unique-id>.md`, for example
   `<date>-<profile>-<short-candidate-sha>.md`.
2. Fill in the mandatory `Kind` / `Status` first line, then only reviewed
   public fields. Descriptive numbers go in the descriptive table, which has no
   acceptance threshold and no verdict. The acceptance table lists only rows from
   [Acceptance bars](#acceptance-bars), quoted verbatim, each `PASS`, `FAIL`,
   `NOT RUN` or `NOT EVIDENCE` with its evidence and coverage. Never paste the
   raw receipt, logs, filesystem paths, hostnames, user names, cgroup paths or
   any secret.
3. Add a line to [Measured results](#measured-results) starting with the same
   Kind and Status, then area, hardware label, date and the exact candidate
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

## Measured results

### Official

None yet. Entries take the form: `Kind: official · Status: <…>` · area
(profile) · hardware label · date · candidate SHA · link to the summary under
`results/`.

### Community and historical

None published. Community and historical entries are listed here, never under
Official, and every one starts with its Kind and Status.

## Future automation (owner-only TODO)

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
