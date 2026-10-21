# contextdb CI

The normal gate publishes one source-bound inventory containing 29 unchanged logical PR
command/feature tuples in 26 physical rows and 22 certified integration rows (26 on the
fail-closed certificate fallback). It dispatches the resulting 49 or 53 physical
rows in 18 reviewed execution bundles after `impact`. Each bundle contains at most
three rows and attempts every constituent even after an earlier failure. Every
failed, cancelled, skipped or missing bundle still fails the validator and stable
`PR checks` aggregate. When integration is classified inactive, the same mapping
retains all 26 physical PR rows and omits only empty integration-only bundles.
Three jobs share setup and cache: transactions (default/internal) with relational
storage; vector retrieval (default/internal) with nearest-neighbor index and graph;
and SQL parser with query planning and redb. Each invokes the existing shard runner
sequentially and keeps its stable shard ID and separate receipt directory. Redb
retains `target/redb`; features and profiles are never combined. A failed constituent
is retained while later constituents still run; missing receipts also fail it. Receipt upload includes every
constituent directory, including evidence from a failed bundle. Single-shard local
commands are unchanged. The impact job publishes the source-bound plan and all
18 bundles once; the integration validator checks their disjoint canonical union,
exact inner payloads, and successful bundle matrix result. Each constituent
disables automatic job-ID suffixing and carries the historical `-test` suffix in
its per-row cache key, preserving each old cache namespace; feature sets and
target directories stay unchanged. The cache action uses its baseline behavior for failed jobs.
The compile roots form `lint` → `features` → `build` → `cross-compile`; each
successor runs after predecessor failure unless the workflow is cancelled, so the
aggregate records every root error. `installed-smoke` remains an independent root.
Setup savings are a forecast; hosted whole-gate timing remains unproven.

> Normal correctness blocks merge; original scale/corpus qualification
> becomes an explicit full nightly/manual result on its own source SHA. A normal
> green result does not certify that full qualification passed. This changes the
> previous requirement that every affected full case block merge. The thirty-minute
> end-to-end normal gate budget is unproven, including queue and cold setup.

`CI_GATE_MODE=normal` selects the proposed PR route. `CI_GATE_MODE=full` (the
local default) preserves original qualification. PR callers pass normal explicitly;
nightly and manual integration default to full. For local parity:

```sh
CI_GATE_MODE=normal CI_INTEGRATION_DEFAULT=false bash scripts/verify integration
CI_GATE_MODE=full CI_INTEGRATION_DEFAULT=true bash scripts/verify integration
CI_GATE_MODE=normal bash scripts/verify pr
```

Full integration retains all 30 canonical slices, including optional default
commands, release acceptance and the four corpora's 24 cells. Each original schema
root also executes with `test-seams,iroh`, and the oversized authenticated transport
case executes with that feature tuple as well as its original nightly tuple. These
companion commands use unchanged full fixtures and assertions; a failed companion
fails the same full result. Receipts record each exact command. Qualification on a
different SHA supplies no verdict on the PR SHA. Hardware latency qualification
remains the existing dormant/manual path.

Seven named engine assertions are temporarily declared but ignored while their rc2
work lands on `dev`: the four corpus profile assertions (CDB-160), the indexed-route
and saved-graph work assertions (CDB-161/CDB-162), and the oversized authenticated
schema journey (CDB-163). They remain discoverable in their original targets and the
full qualification receipt must show them as ignored, not executed. The CI guard
allows only these seven declarations plus the pre-existing CDB-154 exception; it
rejects a changed target, case, issue ID, reason, or extra quarantine. The owner
removes all seven when rc2 lands on `dev`; pending campaign fixes are not claimed as
resolved by this waiver.

Normal integration has 18 slices after the certified omissions below. It substitutes the 14-case bounded schema root
for the three original full roots and omits only the four full recall corpora.
The sync schema target excludes exactly the module-qualified oversized authenticated
journey using libtest `--skip`; all other 12 cases, including its no-op subprocess
helper, and the separate 50 sync cases remain. The exact skipped name and full
qualification destination are recorded in receipts. A source census fingerprint
fails closed on edits to the sync wrappers/modules until inventory is reviewed.
Calibration, indexed-route correctness (including its known failing assertions),
held-out native scores, declared breadth, search scope, both acceptance profiles,
and every other nightly-feature target stay normal. Only the 59 target closures certified in the manifest omit repeated normal integration
execution (778 identical cases); PR execution remains required. One compile-only
command in the existing library slice builds all 59 under the nightly feature tuple
and `RUSTFLAGS=-D warnings`. Two empty integration slices disappear; full qualification
commands remain unchanged. The optional in-repo certificate records case/ignored lists
and each target's literal source/helper hashes, dependency manifests/configuration and
actual production feature consumers. An unrelated production edit does not invalidate
it. A stale, missing, unknown or inapplicable proof disables dedup: all 59 variants run
in normal integration, with the reason and fallback identities recorded in receipts.
Normal therefore has 18 slices when applicable at planning time and 20 when planning
disables dedup. The plan publishes its exact certificate SHA (or explicit empty decision),
source SHA/fingerprint, mode and default-suite selection in outputs and every matrix row.
The slice receives that row as `CI_INTEGRATION_PLAN`. If a planned certificate becomes
inapplicable or mismatched at runtime, the slice refuses before building and writes a
failure receipt. It does not silently drop coverage or launch a long serial fallback.
A published empty decision always executes all planned targets even if proof later applies.
The normal validator checks the published source/mode/decision and exact mixed matrix, without
recomputing optimization from its own environment. Local CLI execution plans implicitly
once in-process; full remains the default. Contributors can
leave the optimization disabled; lint controls use synthetic fixtures, not live source
recertification. Unresolved/runtime-fixture/lib/bin/extras/schema wrappers, release
coverage and indexed-route failures remain required. Historical eligible repeated test
work is 4,682.51 seconds; the incomplete mapped integration work becomes 6,507.12 seconds
from 11,189.63. These old-source sums exclude compilation, setup, queue, PR work and
new bounded/schema/sync timing; they give no new end-to-end bound or acceptance. Two empty normal schema shard jobs
are removed by consolidating the canonical full targets into the existing bounded
schema shard; the full canonical target union remains checked before routing.
Normal mode rejects `run_default=true`, which would reintroduce full fixtures.


How this repository's continuous integration is organised, what each check
proves, and how to run the same thing locally. The workflow files under
[`workflows/`](workflows/) and the scripts under [`../scripts/`](../scripts/)
are authoritative; this page explains them and must change when they change.
Agents maintaining CI also read [`AGENTS.md`](AGENTS.md).

## Four check, qualification and publication paths, plus development delivery

CI has four check, qualification and publication paths, plus a separate
development-delivery path. They share one local entry point,
[`scripts/verify`](../scripts/verify)
(`bash scripts/verify --help`).

| Path | Workflow | Trigger | What a result means |
|---|---|---|---|
| PR checks | [`ci.yml`](workflows/ci.yml) (`CI`) | push to `dev` or `main`; pull request into `dev` or `main`; manual | Correctness of this exact commit. The aggregate job **`PR checks`** passes only if every PR job succeeded. |
| Extended integration | [`integration.yml`](workflows/integration.yml) (`Extended integration`), called by `ci.yml` and by [`nightly.yml`](workflows/nightly.yml) (`Nightly`, daily 03:00 UTC and manual) | Inside every PR run (gated by the impact classifier), nightly, manual | Nightly-only correctness fixtures (`nightly-tests` feature); the nightly/manual run also repeats the full default workspace and redb suites. |
| Performance measurement | [`performance.yml`](workflows/performance.yml) (`Performance measurement (manual guide)`) | Never — dormant placeholder | **MANUAL TODAY.** The workflow cannot run: its only trigger is a push filter excluding every branch and tag, and manual dispatch is commented out as an owner-only TODO. It never starts or reports a status, checks out nothing and runs no benchmark. Measurement, comparison and publication are the manual journey in [`docs/performance/README.md`](../docs/performance/README.md). |
| Stable publication | [`release.yml`](workflows/release.yml) (`Release`) on a pushed `v*` tag; [`main-image.yml`](workflows/main-image.yml) (`Main Image`) after successful `CI` on a trusted `main` push | Owner-controlled release tag or `main` push | Publishes stable release assets and the `main` container image. Required checks, integration and performance do not publish; CI can start the separate development-delivery path after checks. [`release-dry-run.yml`](workflows/release-dry-run.yml) (manual) rehearses packaging without publishing. The stable release workflow is tag-triggered; no claim that CI currently builds stable binaries. |
| Development delivery | [`build.yml`](workflows/build.yml) (`Build development binaries`), called by `ci.yml` after `PR checks` on a trusted `dev` push; manual dispatch on `dev` with the exact qualified commit | Automatic after the required aggregate succeeds, or manual with a required exact SHA that is current `dev` and has successful same-repository push CI and `PR checks` | Builds Linux x86_64 binaries with read-only build permissions; the publisher alone writes the explicitly unstable `dev-latest` prerelease. This does not satisfy or replace any required check. |

[`flake-hunt.yml`](workflows/flake-hunt.yml) (`Flake Hunt`, Mondays 03:00 UTC and
manual) reruns `cargo test --workspace` up to three times and files or updates a
flaky-test issue. It is diagnostic, not a gate.

## PR checks (`ci.yml`)

Every job uses `ubuntu-latest`. The shared
environment is all runner cores, no debug info, no incremental builds and an
empty `RUSTFLAGS`; jobs that compile with warnings-as-errors set it per command.
Superseded PR runs are cancelled; push runs are not.

| Check name (as shown on the PR) | Local command | What it proves |
|---|---|---|
| `impact` | `python3 scripts/ci-inventory.py`, then `python3 scripts/ci-impact.py` (CI only) | Two steps in this order. The inventory step runs locally too: every workspace package and the redb fork is covered by a test shard with the right feature closure (fails closed otherwise). The impact step then classifies changed paths for integration (see below); it needs the GitHub Actions event file (`GITHUB_EVENT_PATH`), the `GITHUB_OUTPUT` file and a full-history checkout, so it is not a standalone local command. |
| `lint` | `bash scripts/verify pr lint` | The job installs `ripgrep` before the offline controls. `python3 -B scripts/ci-build-tests.py`, `python3 -B scripts/ci-required-tests.py` and `python3 -B scripts/ci-inventory-tests.py` run in the lint job before `scripts/verify pr lint`: offline delivery, aggregate, target-inventory and timing-receipt regression controls, then `cargo fmt` and `clippy -D warnings` for the workspace **and** the separately-built redb fork. |
| `test / bundle <number> / <behavior families>` — 18 bundles | `CI_CRATE=… CI_SHARD=… CI_FEATURES=… bash scripts/verify pr shard` | The bundles expand to the original 29 logical PR command/feature tuples plus every active integration row. Acceptance runs as separate remaining-module, long-running, and replication targets; vector decoding and delivery, many-context search, and layer search are separate work units. Their physical rows reuse the original logical parent's cache key and preserve its profile and feature closure. The normal plan has 49 physical rows when the source-bound dedup certificate applies and 53 in the conservative fallback. Default builds use parallel libtest execution; `test-seams` builds use one test thread. The engine's `test-seams,iroh` coverage is partitioned into seventeen physical rows by [`ci-shard-targets.txt`](../scripts/ci-shard-targets.txt). The inventory guard rejects missing or repeated targets and changed parent bindings. The lib shard alone covers examples and doctests within the split group. The redb fork retains its own target directory. |
| `build` | `bash scripts/verify pr build` | Release build of the workspace, then [`verify-packaged-engine.sh`](../scripts/verify-packaged-engine.sh) builds the engine only from packaged crate archives. |
| `features` | `bash scripts/verify pr features` | Supported standalone feature shapes: engine default, engine `mdns`, server `mdns,production-smoke-driver`, plus `cargo doc` for the workspace. |
| `installed-smoke` | `bash scripts/verify pr installed` | Installs release CLI and server binaries into a temporary root and runs [`installed-release-durable-sync-smoke.sh`](../scripts/installed-release-durable-sync-smoke.sh), the installed-binary durability and sync smoke over real files and ticketed Iroh. |
| `cross-compile (<target>)` | `CI_TARGET=<target> bash scripts/verify pr cross` | Release build of CLI and server for `aarch64-unknown-linux-gnu`. The macOS targets are set aside until CDB-153 (the engine uses Linux-only `pipe2`/`SOCK_CLOEXEC`) is fixed. |
| `integration / engine / test-profile / acceptance` and named families | see Extended integration | Nightly-only fixtures when the change affects runtime or integration paths; otherwise the job still runs and records why it was not required. |
| **`PR checks`** | — | Stable aggregate. [`ci-required.py`](../scripts/ci-required.py) fails unless every job above reports `success`, and validates the exact inventory plus the sole development-delivery exception. |

The PR shard list lives only in `scripts/ci-integration-targets.json`; `ci.yml` and
`scripts/verify pr` share that canonical inventory. `bash scripts/verify pr` with no stage runs every stage and every
shard sequentially (cross only when `CI_TARGET` is set).

Normal jobs skip hosted SDK/Docker cleanup only when root free space is a valid
measurement of at least the configured 85,899,345,920 bytes. Lower or invalid
measurements and invalid configuration retain the original cleanup commands and
failure behavior. Full qualification always cleans. The helper prints before/after
free bytes and the skip or fallback reason. This is a reversible runner-resource
hint, not an engine threshold or proof of peak disk demand.

A known source defect is recorded in [`ci-test-shard.sh`](../scripts/ci-test-shard.sh):
two engine integration tests do not compile without test seams, so the default
engine shard runs lib/bin targets only and all engine integration tests run in
the `test-seams,iroh` shard. The production-layout nightly run keeps exposing it.

Each shard also uploads `<shard>.targets.json`: Cargo’s `Running` source label (for example `tests/vector_lazy.rs` or
`unittests src/lib.rs`), or the crate name for doctests, plus the executable,
passed/failed/ignored/measured/filtered counts and Cargo-reported test duration for
every completed libtest binary and doctest target. Missing summaries (for example
a compile failure or interruption) have no fabricated duration; raw logs and the
shard exit status remain authoritative. Receipt tooling errors fail the shard;
ordinary test duration does not. Child-process summaries do not replace
the enclosing binary's final summary. Example compilation has no test duration.
The thirty-minute normal correctness budget applies end to end and remains unverified. This partition is provisional pending isolated-target and full hosted
calibration; one existing schema-replay module exceeded 25 minutes in the baseline.
There is no matrix parallelism cap. Organisation runner availability and queued
job time must be checked during hosted calibration; adding jobs does not create
runner capacity.

### Impact classification

[`ci-impact.py`](../scripts/ci-impact.py) diffs the PR base (or the previous push)
against `HEAD` and classifies each path. It only decides whether the *extra*
nightly fixtures run on a PR; it never removes an original PR check.

Rules apply in this order; the first match wins.

- Known benchmark fixtures (the declared bench sources) and the performance
  metadata files `.github/workflows/performance.yml` and
  `scripts/ci-performance.py` → performance only. These match before the
  `scripts/` and `.github/workflows/` rule below.
- A short allowlist of maintainer files (`.gitignore`, `.dockerignore`, the
  `MAINTENANCE.md` notes of the vendored hnsw and redb crates) → maintainer
  metadata, which needs neither integration nor performance measurement.
- Markdown under `docs/` or `skills/`, top-level Markdown, `LICENSE`, `NOTICE`
  → documentation only.
- Any other path under `crates/`, `tests/`, `scripts/`, `.github/workflows/`,
  and `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `Dockerfile`,
  `docker-compose.yml`, `book.toml` → integration required.
- Anything else, a malformed path, or an empty or unreadable diff (for example a
  first commit with no parent) → unknown, which fails closed to integration
  required.

The job summary prints the reason, the per-path classes and a performance
label: **measurement pending** or **not affected**. Neither is a qualification
or a required gate. The step output `performance_qualification` keeps its
compatibility values `pending-controlled-native-qualification` and
`unaffected`, which `integration.yml` receives and renders with the same two
labels in its skip line. The classifier reads GitHub's event and output files,
so it runs only in CI.

## Extended integration (`integration.yml`)

Local: `bash scripts/verify integration` runs every active slice, continuing other
families after a failing slice and returning failure if any failed.
`CI_INTEGRATION_DEFAULT=false bash scripts/verify integration` matches the PR
call. Select exactly one slice with `bash scripts/verify integration <slice-id>`;
`python3 -B scripts/ci-integration.py check` prints the active ID/name matrix.
Unknown IDs, malformed modes and inactive optional slices fail before Cargo.

[`ci-integration-targets.json`](../scripts/ci-integration-targets.json) declares
the partition. Its test-profile target union is independently compared with the
engine Cargo manifest under `nightly-tests,test-seams,iroh`, including targets
that require nightly features. Omissions and duplicates fail. The same command
selector runs locally and in the reusable workflow. Every slice builds the CLI
and server from this checkout, preserves `RUSTFLAGS=-D warnings` and the original
CLI binary environment, then runs its own Cargo command.

The canonical full manifest preserves 30 logical slices as 36 physical slices, including
four optional default-suite slices; the normal routing view selects 22 physical slices
when the source-bound dedup certificate applies and 26 in its conservative fallback.
**Vector default search-breadth calibration** retains the existing
`vector_graph_route_profile_quality` target and its single calibration test.
Four full-qualification **vector graph recall** jobs each cover one complete corpus:

| Vectors per context | Corpus | Explicit target / slice ID |
|---:|---|---|
| 5,000 | anisotropic | `vector_graph_recall_5000_anisotropic_per_context` / `vector-graph-recall-5000-anisotropic` |
| 5,001 | topic-clustered | `vector_graph_recall_5001_topic_clusters_per_context` / `vector-graph-recall-5001-topic-clusters` |
| 50,000 | isotropic | `vector_graph_recall_50000_isotropic_per_context` / `vector-graph-recall-50000-isotropic` |
| 50,001 | anisotropic | `vector_graph_recall_50001_anisotropic_per_context` / `vector-graph-recall-50001-anisotropic` |

The former `selected_f32_build_switches_pass_the_affected_policy_matrix` case
maps to four `selected_f32_build_switches_pass_<rows>_<shape>_per_context` cases
in module `graph_route_profile_quality`, in the targets above. Each builds one
fixture, then keeps the two filters and three scopes together in their original
order. Every corpus retains all six cells, 100 probes per cell and the 95% recall
assertion, for 24 cells across four independently required full-qualification jobs. The canonical
profile list, explicit target registrations, wrapper indices and named job mapping
are checked before scheduling; missing, duplicate or combined jobs fail closed.
The selected build choice remains `(false, false)`. Nightly declaration count is
expected to increase by three (one matrix case becomes four); ordinary case count
is unchanged because all five profile sources require `nightly-tests`. All four corpus targets remain in PR shard 12 under `test-seams,iroh`, where
they compile their ordinary layout and run/list zero cases. Their nightly-feature
24 cells remain full qualification. No PR shard is added.

**Indexed-search work and latency** and **held-out vector recall and native-score
reference** retain their separate targets. All required engine slices preserve
`nightly-tests,test-seams,iroh`, the test profile and one test thread. Native
compilation and hosted duration calibration of the corpus partition remain
pending; this wiring does not establish a measured speedup or a 30-minute result.

Normal sync has two separate jobs in both PR and extended coverage: **replication,
receipts and policy** (`sync`, 50 existing cases) and **schema replication and atomic publish** (`sync_schema_transactions`, 12 of 13 existing Unix
cases; the exact oversized journey is accounted for in full qualification). The schema parent and nested transport module retain their test namespaces
and exact child-process filters. Both targets require `test-seams`; features,
profiles, assertions, ignored states and thread settings are unchanged. The target
union and dedicated-job guards reject missing, repeated or combined sync families.
Native compile/list verification and hosted timing calibration remain pending.

Full schema replay has three separate qualification jobs; normal PR and integration
use the single bounded target:
**schema replay contracts**, **schema replay / work limited to affected tables**, and
**schema replay / identical schema avoids rebuilding data**. The full cases retain
their assertions and canonical shared helpers. The existing **schema replay
contracts** job also requires `custody_schema_replay_bounded_contract` (14 cases: 13 canonical oracles at a one-row unrelated fixture plus one unique
receipt/contact replication twin) in both PR and extended coverage; no job is added. The proposed normal mode selects only the bounded root; full mode retains all original roots. Compile/list checks do not establish gate timing.
The hard thirty-minute end-to-end correctness budget remains unmet; full-scale qualification relocation requires owner policy approval; hosted acceptance remains pending.

The visible acceptance jobs name **acceptance remaining modules**, **acceptance
long-running sync scale**, and **acceptance replication and convergence** under
both the test and release profiles. `test-profile` means Cargo's debug-like test
profile; the three release-profile jobs partition the existing optimized acceptance rerun. Other jobs name the actual packed
behavior families (sync, reads, schema replay, vector serving and others), with
separate examples compilation and doctests. Required engine commands keep
`nightly-tests,test-seams,iroh` and one test thread. The PR jobs also use readable
family names; internal shard IDs and feature switches remain in receipts.

With `CI_INTEGRATION_DEFAULT=true` (nightly/manual), four additional visible jobs
preserve the original workspace, standalone redb, production-layout nightly and
production-layout release acceptance commands. These keep Cargo's default target
selection, feature unification, custom harnesses, doctests and inherited test
thread mode; they are not narrowed to explicit libtest targets. Redb keeps its
separate target directory. The known standalone production compile defect is
still exposed rather than skipped.

The workflow uses `fail-fast: false` without a concurrency cap. Its always-run
result job checks the validated plan/mode and requires success of the complete
active matrix. Failure, cancellation, a missing plan or an unexpected skip fails
the reusable workflow and therefore the unchanged **PR checks** aggregate.
Inactive integration emits the classified impact reason in logs and summary;
only that explicit inactive mode admits the matrix's expected skip. The original
PR checks still run. Normal PR lint/test/build/features/installed-smoke/cross jobs have 30-minute runaway
caps; helper jobs keep five minutes. Integration slices use 30 minutes in normal mode
and the existing 330 minutes in full qualification. These per-job caps do not bound the
whole normal DAG, setup or queue; they do not establish end-to-end acceptance. The hard thirty-minute normal end-to-end
correctness budget requires cold hosted calibration; per-job timings cannot certify it.

Each slice uploads `extended-integration-<slice>-<run>-<attempt>`, containing raw
build/test logs, `receipt.json` and a summary table. Local artifacts are under
`target/ci-integration-out/run-*/<slice>/` (override the output root with
`CI_INTEGRATION_OUT`). Fresh invocation directories prevent stale evidence.
Receipts identify the source/dirty diff and untracked source hashes, manifest,
fixture hashes, toolchain, features, profile, targets, thread mode and exact
commands. Completed libtest target summaries supply actual declared/runnable
counts, ignores and Cargo-reported duration through the shared shard parser.
Declared counts include filtered/ignored declarations; runnable counts are the
actually passed/failed cases. Incomplete and custom-harness counts stay unknown;
example compilation has no executed test count. No historical case total is
hardcoded or certified by the static target-union proof.

`cargo_command_seconds` includes compilation plus execution of that command;
`binary_build_seconds` records the prerequisite build. `runner_elapsed_seconds`
starts inside the local runner and excludes hosted checkout, toolchain/cache
setup and artifact upload. None is full hosted elapsed or queue time. The hosted
fields remain null/pending until terminal Actions job metadata is collected.
After the run completes, retain the paginated raw metadata with:

```sh
gh api --paginate "repos/<owner>/<repo>/actions/runs/<run-id>/attempts/<attempt>/jobs?per_page=100" > jobs.json
gh api "repos/<owner>/<repo>/actions/runs/<run-id>/attempts/<attempt>" > run.json
```

Match by exact visible job name, run ID and attempt from the receipt. Compute
whole-job elapsed from the job's `completed_at - started_at`, including setup
and upload. Keep raw timestamps and conclusions. The jobs API has no per-job
queued-at timestamp: `started_at - run_started_at` is only a run-relative start
delay (it includes dependencies), not queue duration. Leave queue time unknown
unless an actual per-job queued event is available. Never substitute a shell
step timer or run-relative delay for those missing measurements.

## Performance measurement (`performance.yml`)

**Manual today.** Every performance command, the profiles, the acceptance bars
and the publishing rules are in
[`docs/performance/README.md`](../docs/performance/README.md), the single home
for them. `performance.yml` is a dormant placeholder that cannot run today: its
only active trigger is `push` with `branches-ignore: ['**']` and
`tags-ignore: ['**']`, and `workflow_dispatch` is commented out pending
owner-controlled enablement. It never starts, never reports a status and is not
a required check. A future self-hosted measurement needs actual runner
admission, resource isolation, security controls and execution steps, not only a
`runs-on` change. Nothing hosted measures or qualifies performance.

## Publication

`bash scripts/verify publication` prints the existing mechanism and publishes
nothing. `release.yml` needs a `v*` tag matching `Cargo.toml` and uses the
repository's crates.io and website-deploy secrets; `main-image.yml` pushes
`ghcr.io/context-graph-ai/contextdb-server:main` only after a successful `CI`
push run on `main` from this repository.

Website docs refresh: [`docs-dev.yml`](workflows/docs-dev.yml) (`Docs Dev`) runs on
every push to `dev` in this repository (and manually on `dev`), holds no
`GITHUB_TOKEN` scopes and sends the website a `docs-update` repository dispatch
with ref `refs/heads/dev`; it never runs for pull requests or forks. The website
builds the freshest `dev` HEAD at execution time, not the pushed commit, and its
six-hour schedule is only the fallback. `release.yml` sends the exact release
tag instead.

## Reading results

- **Failed `PR checks`**: open the job it names in its error line. Each test
  shard's artifact `test-<shard>-<attempt>` holds the log, executed test list
  and timing even on failure.
- **Skipped or cancelled**: any non-success of a required job fails `PR checks`.
  A skip is never a pass.
- **`integration / required integration results` green with a classified skip**: the classifier judged
  the change documentation- or performance-only; every other PR check still ran.
- **Pending `PR checks`**: jobs are still running; the long test shards and
  cross builds can take hours (timeouts are 330 min for shards, 180 min for
  build-type jobs). Timeouts are runaway protection, not latency limits.
- **Performance measurement: measurement pending** in the `impact` summary: the change may affect performance and a manual measurement
  is pending; it is not a failure, not a pass and not a required gate. See
  [`docs/performance/README.md`](../docs/performance/README.md).
- **`Performance measurement (manual guide)`** never appears as a run today; the
  workflow is dormant and cannot start.

Which checks GitHub *requires* before merging is a repository setting, not
something this directory controls. `PR checks` is designed to be the single
required context; until the owner sets it, a green run is evidence, not a merge
rule.

## Prerequisites for local runs

- Rust from [`rust-toolchain.toml`](../rust-toolchain.toml) (1.94.0 with clippy
  and rustfmt); `rustup` installs it automatically.
- `python3` (3.11+, `tomllib`) and `ripgrep` (`rg`) for the inventory and shard
  receipts.
- The installed smoke needs Bash and GNU `timeout` (`gtimeout` via
  `brew install coreutils` on macOS).
- Cross builds need the target via `rustup target add` and, for aarch64 Linux,
  `gcc-aarch64-linux-gnu`.
- Disk: the full suite builds the workspace and the redb fork into separate
  target directories (`target/` and `target/redb/`); hosted Linux jobs free
  preinstalled tooling first with [`ci-free-disk.sh`](../scripts/ci-free-disk.sh).
  Run one Cargo build at a time.

[`CONTRIBUTING.md`](../CONTRIBUTING.md) lists the equivalent plain Cargo commands.

## Security boundaries

PR, integration and performance workflows run with `contents: read`, use only
public sources, and receive no repository secrets, so fork pull requests run
the same checks. No workflow uses `pull_request_target`. Third-party actions are
pinned to commit SHAs. Secrets are used only by the publication workflows and
by `Flake Hunt` (issue filing with the built-in token). Caches
(`Swatinem/rust-cache`) hold rebuildable compiler output only; a cache miss must
still pass.

## Development binary delivery (`build.yml`)

[`ci.yml`](workflows/ci.yml) calls [`build.yml`](workflows/build.yml) from the
same commit through its `development-binaries` job, only after `PR checks`
succeeds on a canonical-repository push to `dev`. The original `build` release
compile and every correctness dependency remain required; delivery is the one
strictly validated auxiliary job outside the aggregate. The guard validates the
current CI run ID, attempt, workflow identity and successful aggregate through
the Actions API while the overall CI run is still in progress. It never waits
for that run to finish. Automatic delivery works from `dev` whichever branch is
the default; it requires no adoption on `main`.

`qualify` reads Actions metadata; `build` compiles, archives and smokes Linux
x86_64 default-feature CLI/server binaries with read-only permissions. It builds
the existing smoke driver separately into its own target directory and runs the
full installed durability/sync script against the exact unpacked payload; the
verifier is never shipped. Only `publish` can replace
assets on the `dev-latest` prerelease, and only that job joins the delivery
concurrency group. The caller grants contents write and Actions read; no identity
or attestation write permission is used. The publisher uses `queue: max`,
cancellation disabled, with capacity for 100 pending runs; new arrivals beyond
that bound can be cancelled by GitHub. Job-level queue acceptance is pending a
hosted pilot. actionlint 1.7.12 rejects that job-level `queue: max` key; its
parser failure is not a lint pass. Required CI has no actionlint invocation or
pin, so no lint compatibility shim applies. Instead the exact strict offline
semantic controls (`python3 scripts/ci-build-tests.py` plus
`python3 scripts/ci-required-tests.py`) reject missing, misplaced or unknown
queue keys and values; they do not validate every GitHub expression context.

The channel tag bootstraps once at the current default-branch head and never
moves; built-source identity comes from receipts and title/notes. Authenticated,
paginated discovery includes hidden drafts and refuses duplicate channels.
Each publication uploads a new write-once group named with the target, source
SHA's first 12 characters, run ID and attempt. Any existing group member refuses
publication before mutation; run copies are never clobbered or reused. The entire
group must pass downloaded-byte verification before stable assets follow in
archive, checksum, JSON receipt order, with a fresh dev-tip check before each
mutation. Metadata follows stable-byte verification; pruning follows successful
metadata publication and keeps the newest three complete owned run groups plus
any group whose receipt equals the current stable receipt, even if older.
Incomplete groups and unrelated assets are preserved. Consumers must cross-check
both sidecars against the archive; if stable verification fails, use the newest
complete run-named set. The [download recipe](../docs/download.md) explains
renaming fallback sidecars before verification.

For a manual retry, select the `dev` ref and provide its exact current SHA.
The workflow file must also be present on the repository's default branch for
GitHub to expose manual dispatch in the UI; that discovery requirement does not
affect automatic local reuse from `dev`. Manual dispatch requires exact-source
push CI with successful `PR checks` on the latest attempt of the selected run.
An overall failed run qualifies only when delivery failed and every unsuccessful
job belongs to `development-binaries`; a failed required or unrelated job refuses
delivery. Retry with `gh workflow run build.yml --repo context-graph-ai/contextdb
--ref dev -f sha=<full-current-dev-sha>` after the dispatch workflow is available.
A manual retry cannot bypass source qualification. No hosted build or download
has been verified for this preparation yet.

[Downloads](../docs/download.md) is the canonical download recipe, coverage,
identity/checksum contract, rolling-release limitations and offline control
commands. `ci.yml` correctness, the manual performance journey and stable
`release.yml` publication remain separate. No existing required check is removed
or satisfied by a development build.

## Local certificate maintenance (optional)

No private evidence paths or artifact downloads are needed. To refresh the optional
dedup certificate, capture source identity before local ordinary/nightly libtest lists
and ignored lists. This compiles and lists tests without executing their bodies:

```bash
proof_dir="$(mktemp -d)"
python3 -B scripts/ci-integration.py dedup-snapshot "$proof_dir"
read -r -a dedup_targets <<< "$(python3 -B scripts/ci-integration.py dedup-targets)"
for mode in ordinary nightly; do
  features=test-seams,iroh
  if [[ "$mode" == nightly ]]; then features=nightly-tests,test-seams,iroh; fi
  RUSTFLAGS='-D warnings' cargo test --locked -p contextdb-engine --features "$features" \
    "${dedup_targets[@]}" -- --list --test-threads=1 > "$proof_dir/$mode.list.txt" 2>&1
  RUSTFLAGS='-D warnings' cargo test --locked -p contextdb-engine --features "$features" \
    "${dedup_targets[@]}" -- --ignored --list --test-threads=1 > "$proof_dir/$mode.ignored.txt" 2>&1
done
python3 -B scripts/ci-integration.py regenerate-dedup "$proof_dir"
```

Regeneration checks complete target/case/ignored equality, literal helper closure and
unchanged source identity. Feature-bearing/dynamic/unresolved closures stay unqualified;
keep normal execution rather than inventing equality. Review the manifest diff. The
certificate is an optimization, so inability to regenerate does not block contribution.

The separate sync census guard remains mandatory to protect the exact oversized-case
exclusion. After a controlled change, review its 13-case inventory and update only the
three `SYNC_ROUTING_FINGERPRINTS` literals from this local command:

```bash
sha256sum crates/contextdb-engine/tests/sync_schema_transactions.rs \
  crates/contextdb-engine/tests/sync/a_received_schema_transaction_never_publishes_a_prefix.rs \
  crates/contextdb-engine/tests/sync/schema_migration_chunked_transport.rs
```

Run the focused routing controls after refreshing. The 12/1 normal and 13/0 plus
1/12 full-companion runtime summary guards remain mandatory; refreshing hashes does
not authorize dropping or renaming the oversized journey.

The published integration decision is immutable for its run. Regenerating a certificate
or changing flags after planning requires a new plan; a deduplicated run with stale proof
fails with a receipt rather than turning green with missing variants. Normal timeout caps
are runaway protection; the hard thirty-minute whole-gate budget still requires hosted
measurement including cold setup and disclosed queue behavior. Policy adoption remains
pending owner approval.

Normal integration alone partitions the three unchanged long-running acceptance cases:
transfer runs exactly f25 and f26 in bundle02 slot3; incremental pull runs exactly
f27 in bundle12 slot3. Source fingerprints and receipt case names/counts fail
closed on inventory drift. Both retain the old long-running test cache binding.
Server internal checks run in bundle07 slot3; internal and default core stay
adjacent in bundle09. Full qualification, release and PR acceptance commands
remain unchanged, and all three functional oracles remain required. The 18 bundles
still use at most three slots. The observed 1,555 seconds before f27 is partial
cost evidence, not a completion bound; a complete hosted pass within 30 minutes
remains unproved.
