# CI maintenance — agent rules

Applies when you change anything under `.github/` or the CI scripts it calls
(`scripts/verify`, `scripts/ci-*.py`, `scripts/ci-*.sh`, `scripts/ci_bench_inventory.py`).
Read [`CI.md`](CI.md) first: it describes every workflow, check name and
local command. Repository-wide rules (safety boundaries, one Cargo build at a
time, the verification gate) are in the root [`AGENTS.md`](../AGENTS.md).

Scope note: an `AGENTS.md` file governs its own directory and descendants.
This one covers `.github/`; it is not loaded automatically when you start from
the repository root or edit `scripts/`. It only reaches those agents through a
link from the root instructions.

## Shared standard (context-graph-ai organisation OSS CI policy)

These rules are shared by the context-graph-ai organisation's OSS repositories —
ContextDB, context-graph and Vigil. Facts specific to this repository are in the
next section, kept separate.


- Keep four separate paths: PR checks, extended integration, controlled
  performance measurement, optional publication. Do not merge them, and do
  not let a scheduled or manual result stand in for a check on the PR's commit.
- Workflows call repository-owned entry points. Add or change commands in
  `scripts/verify` and the scripts it calls, then call them from YAML; do not
  write a second copy of a command inline in a workflow.
- One stable aggregate (`PR checks`) fails closed: any failed, cancelled,
  skipped or missing required job fails it. When you add, rename or remove a
  correctness job in `ci.yml`, update `REQUIRED` in `scripts/ci-required.py` and
  the aggregate `needs:` together. The sole excluded job is the exact same-commit
  `development-binaries` caller after `required`; the script rejects any other
  inventory or caller shape.
- Unknown impact fails toward more coverage. A path the classifier does not
  recognise, or a diff it cannot compute, must require integration, never skip
  it. Documentation-only routing may skip extra work, never an original PR check.
- Fork safety: PR, integration and performance workflows keep `contents: read`,
  use public sources only and receive no secrets. Never use `pull_request_target`
  or a `workflow_run` job to build or run untrusted code with secrets or write
  permissions. Pin every third-party action to a full commit SHA.
- Correctness is not speed. Ordinary suite and job durations are reported, not
  gates; timeouts are runaway protection. Benchmarks are descriptive; the
  product's acceptance bars are listed in the repository's performance guide,
  not in benchmark sources, and only a controlled native run measured against a
  quoted acceptance row can be acceptance evidence. Never run benchmarks on
  shared hosted runners or report a hosted result as a measurement or
  qualification.
- Report exact identity: receipts name the source SHA, dirty state, toolchain,
  feature set and fixture hashes. Missing evidence is `pending` or a failure,
  never an inferred pass.
- No authority by implementation: a CI change does not change branch-protection
  required checks, publish anything, create tags or alter secrets. Those are
  owner actions.

## contextdb-specific obligations

- The local routing patch proposes normal merge-blocking correctness and separate
  full nightly/manual qualification. This is pending owner policy acceptance;
  do not publish or infer acceptance from implementation. Keep the canonical
  full target union and exact-source full results. Normal success cannot certify
  full corpus qualification or the hard thirty-minute end-to-end feedback budget.

- Preserve every existing PR check: the redb fork (lint and its own test shard
  with its own target directory), the installed-binary durability smoke, the
  packaged-engine build, the three cross-compile targets, and the default plus
  `test-seams` shards with the engine's `test-seams,iroh` closure. Replacing one
  requires proof that the replacement runs the same tests on the same commit.
- The test shard matrix in `ci.yml` is the single inventory. `scripts/verify pr`
  parses it and `scripts/ci-inventory.py` checks it covers every workspace
  package and feature closure; keep the matrix line format those parsers expect.
- Default shards use libtest’s parallel default (or `RUST_TEST_THREADS` when set);
  `test-seams` shards override it with one test thread per binary. Shards compile
  examples and run doctests with one test thread; split groups do this once in
  the shard that lists `lib`.
  Never add blanket `--include-ignored` or pass libtest flags to Criterion
  benches.
- Engine and server tests use the CLI/server binaries built from the same
  checkout (`CARGO_BIN_EXE_*`); keep that wiring.
- The extended path uses `scripts/ci-integration-targets.json` and the validated
  `scripts/ci-integration.py` selector, locally and hosted. Keep exact target/profile
  union checks, separate acceptance profiles and optional Cargo-default commands.
  Its result job must reject missing, failed, cancelled or unexpectedly skipped
  slices. Name visible checks for behavior families and profiles; keep feature
  switches and internal shard IDs in receipts.
- The extended path runs the `nightly-tests` feature; PR calls set
  `run_default: false` because the PR shards already cover the default suite.
- Performance commands and profiles come from `scripts/ci-performance.py` and
  the declared bench targets; `run` must keep refusing CI hosts and dirty
  checkouts.
- Performance measurement is manual, and
  [`docs/performance/README.md`](../docs/performance/README.md) is the only
  home for performance run commands, the acceptance bars and the publishing
  rules; elsewhere, link to it rather than copying commands.
  `performance.yml` is a dormant placeholder with no active dispatch and no
  possible push trigger because its push filter ignores all refs. The future
  TODO is for the owner to register a dedicated self-hosted runner on an
  exclusive machine and define actual runner admission and execution controls;
  those controls do not exist yet, and implementing them is an owner decision,
  not a CI cleanup.
- Recorded source defects (for example the standalone engine integration
  compile failure noted in `scripts/ci-test-shard.sh`) are fixed in source, not
  hidden by narrowing CI.

## When you change CI

- Update [`CI.md`](CI.md) in the same change whenever a workflow name,
  trigger, job or check name, local command, prerequisite or classifier rule
  changes, and keep [`CONTRIBUTING.md`](../CONTRIBUTING.md) consistent.
- Validate statically before claiming anything: YAML parse, `actionlint`,
  `shellcheck` on changed shell, and the scripts' own failure cases (a failed,
  cancelled and missing job for `ci-required.py`; documentation-only, unknown
  and empty diffs for `ci-impact.py`).
- A hosted run proves only the commit it ran on. Do not describe CI as passing,
  fast or qualified without that run's evidence.
- Keep this directory free of private machine paths, internal plan references
  and run histories; it must make sense to an external contributor.

## Development builds

- Keep [`build.yml`](workflows/build.yml) separate from correctness, manual
  performance and stable release paths. Preserve the source/current-dev/workflow
  SHA binding and successful same-repository push CI plus exact `PR checks`
  aggregate requirement for automatic and manual delivery. Automatic reuse validates
  the current still-running CI run and attempt; never wait for overall CI to
  complete. Manual retry may admit overall failure only from delivery after
  successful required checks.
- Build/package/smoke jobs remain read-only with checkout credentials disabled.
  Only the serialized, origin-guarded publisher gets `contents: write`; never
  execute downloaded artifact contents there. Validate the complete artifact
  set, checksums and receipt identities before mutation. Never delete a release;
  upload and verify the entire write-once run group before stable assets, update
  metadata only after stable verification, and never move the stationary bootstrap
  tag. Any existing run name fails before mutation. Never clobber run copies;
  delete them only during protected pruning after successful publication. Keep the
  newest three complete owned run groups plus every group whose receipt equals
  the current stable receipt; preserve incomplete groups and unrelated assets.
- Add a download target only with native smoke of the exact unpacked package.
  Record real runs honestly. Keep the one download recipe and readiness status
  in [`docs/download.md`](../docs/download.md), linking here rather than copying.
- Run `python3 scripts/ci-build-tests.py` for the offline positive and independent
  mutation controls, plus `python3 scripts/ci-required-tests.py`, actionlint and
  shellcheck. actionlint 1.7.12 rejects the job-level `queue: max` in
  `build.yml`; report that parser failure as such, never as a lint pass, and do
  not add a blanket ignore. The Python controls are the required semantic
  check for that key but do not validate every GitHub expression context, and
  hosted queue acceptance stays pending until a real run. None of these results
  prove native builds, hosted CI or existing downloads.
