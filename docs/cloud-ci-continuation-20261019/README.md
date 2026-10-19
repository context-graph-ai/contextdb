# Cloud CI continuation

This packet identifies the exact public source tips and preserves the patches and test selectors needed to continue Linux and Darwin qualification from a cloud runner. The source branches are review-adopted, but none is a green release candidate: the Linux ContextDB tip has a reproduced strict Clippy failure, the engine Darwin tests are unrun, and the Context Graph Darwin workflow has not qualified these tips.

## Public source tips

| Repository and branch | Commit (tree; parent) | Evidence |
|---|---|---|
| [ContextDB `linux-bounded-storage-candidate-20261019`](https://github.com/context-graph-ai/contextdb/tree/linux-bounded-storage-candidate-20261019) | [`35962ff149c1e145263627f83b286fc93f398755`](https://github.com/context-graph-ai/contextdb/commit/35962ff149c1e145263627f83b286fc93f398755) (`237232c61272ae6f887864c57241c654ad97f31d`; parent `7048a52c2ad21ee8e05adb6e52acd2f7d681273b`) | Manager adoption recorded; runtime not qualified. The exact GitHub head was read back before this handoff. |
| [ContextDB `macos-reader-wait-cancellation-20261019`](https://github.com/context-graph-ai/contextdb/tree/macos-reader-wait-cancellation-20261019) | [`878170bb142790f43ad73ff644ab8ae9eec1039f`](https://github.com/context-graph-ai/contextdb/commit/878170bb142790f43ad73ff644ab8ae9eec1039f) (`12803b30d145b8067d1f3ce5abe4ae06cf2ffa9b`; parent `a883cb73b787958be20ef1e712ffbfd26093f587`) | BS-approved public source for cloud qualification only; the exact seven-field source approval is included below. Linux and Darwin runtime are UNRUN; no Mac result, SHIP approval, or merge grant is claimed. |
| [Context Graph `macos-ci-fixtures-cloud-20261019`](https://github.com/context-graph-ai/context-graph/tree/macos-ci-fixtures-cloud-20261019) | [`fc489f0aaa66bbeaea9b33d449c4add58d795d76`](https://github.com/context-graph-ai/context-graph/commit/fc489f0aaa66bbeaea9b33d449c4add58d795d76) (`da90af31490e271a9d0c743b7d2c984b9c522f26`; parent `0204d0bfbaacf7f86a4a82652c5e4853e63dd8d6`) | Published source-only. Darwin runtime and the timeout phase remain unproven. |

The preserved ContextDB baselines are `7048a52c2ad21ee8e05adb6e52acd2f7d681273b` (tree `135bbdaaecafb79c13c9bd45649d7d8e0b8cf17b`; parent `65a2ccf8066de7a4b6ec5381d5bf0e75775152d6`) and `a883cb73b787958be20ef1e712ffbfd26093f587` (tree `1ab22e2fbe53ec87fe4278960aaf1b6563668f05`; parent `50afbaba8410585bd58e7ab7a02fe70273a136d3`). The preserved Context Graph baselines are [`ci-clean-landable-corrected`](https://github.com/context-graph-ai/context-graph/tree/ci-clean-landable-corrected) at `c06667d0f96f47cc29d39692e00f6c3fd1f28e7e` (tree `4cef6ebb2c69abe2c5d1d5ab7e97e05054ead096`; parent `09b1c591c49cd145436c763384639bbeb1472c86`) and [`ci-claude-fork-session-448`](https://github.com/context-graph-ai/context-graph/tree/ci-claude-fork-session-448) at `0204d0bfbaacf7f86a4a82652c5e4853e63dd8d6` (tree `72f79968dbbcec38ea6262bf828d7374b4a8785b`; parent `c06667d0f96f47cc29d39692e00f6c3fd1f28e7e`). These refs preserve source ancestry and comparison points. Context Graph `0204d0bf` with ContextDB `a883cb73` has a full required Linux GREEN receipt in [run 39224736830](https://github.com/context-graph-ai/context-graph/actions/runs/39224736830), including the source-qualified fork. That receipt does not qualify a different dependency tuple: the whole `0204d0bf` consumer with the new bounded-storage source has 39 API errors, and a measurement-fallback tuple cannot inherit the earlier GREEN.

## Current Linux state

The two observed lint jobs on ContextDB head `35962ff` both passed continuity checks and `cargo fmt --all --check`, then failed `cargo clippy --workspace --all-targets -- -D warnings` with exit 101 and 37 diagnostics in `contextdb-vector`. One run ended failed; the second had its test jobs still pending at audit close. Thus Linux CI is red and the complete test result is pending. Run 472 is a read-only audit of those existing jobs, not a new test run. The earlier capped diagnostic inventory is also non-qualifying: 380 emitted source warning blocks deduplicated to 377 diagnostics; the unchanged strict `-D warnings` gate is still required. See [`inventory.sanitized.json`](inventory.sanitized.json).

The three source branches preserve existing history. On the bounded-storage candidate, retain the original history assertion RED as evidence; the only oracle change is the already-included owner-authorized single-file history expectation correction. The other 14 frozen files and all assertions outside that correction remain byte-for-byte unchanged. The combined source patch (467) is based on `7048a52c` and already includes the manual `OwnerKind` Clone repair (461), two transaction expectation repairs, and the single owner-authorized history expectation correction (822). Do not double-apply these components or rewrite the frozen test oracle. The original RED remains evidence; publication does not qualify the new history-test GREEN, its mutation checks, or the full 63/20 suites.

## Patch files

Each patch is a review/reference artifact with repository-relative file names. SHA-256 values below bind the exact bytes in this packet.

| Patch | Purpose | SHA-256 |
|---|---|---|
| [`patches/contextdb-467-combined.patch`](patches/contextdb-467-combined.patch) | Three-path composition based on `7048a52c`: Clone, two transaction expectations, and history correction | `e7d7923ab2fd0c866e1624e1e3e22b188fa9662c92b9225396ac7e203fe7d8a4` |
| [`patches/contextdb-822-history-one-file.patch`](patches/contextdb-822-history-one-file.patch) | Authorized one-file history-oracle correction | `88875c152f59b969049682994319a156cd65de3229d24fc772e8c60fee110cb9` |
| [`patches/contextdb-457-clone-repair.patch`](patches/contextdb-457-clone-repair.patch) | Exactly one manual `OwnerKind` Clone implementation; no transaction expectation repair | `41deb194a506f1710d3312c803d8f5820678b980ff5a7fc7a6512e3301d9d5fc` |
| [`patches/contextdb-803-combined.patch`](patches/contextdb-803-combined.patch) | Darwin wait implementation based on `a883cb73`; omits the then-untracked new test file | `d671f34bc1eeac2e7ad75bfdb1c90857c90cb7c58f0929e2baaf673bb95269e0` |
| [`patches/contextdb-801-test-only.patch`](patches/contextdb-801-test-only.patch) | Test-only baseline artifact for actual RED/GREEN comparison with published `878170bb` | `798edc4776e97ffb41a94aa1474c4c3fc6ed8b43be6117f4e6a66f586015e13b` |
| [`patches/contextdb-468-reader-wait.patch`](patches/contextdb-468-reader-wait.patch) | Complete two-path reader wait source diff based on `a883cb73` | `4184bf547ac3184be0edcb1302f945a07f3a36ba60b8ac352e8ed82627d4b819` |
| [`patches/context-graph-434-memory-probe.patch`](patches/context-graph-434-memory-probe.patch) | Darwin memory-probe repair component | `b3bb30e4bb4a2baf2b1934fc0ce804812a59265ca5b582d7c732295d383f05d2` |
| [`patches/context-graph-469-combined.patch`](patches/context-graph-469-combined.patch) | Context Graph fixture/memory-probe composition based on `0204d0bf` | `c6585a6ffa8c2be29c8f2bbad520c44dca20c327199ad97137831322ada1c354` |

Patch hashes describe these source artifacts; apply only to the matching repository and frozen parent. For actual qualification, use the exact published commit refs above rather than reconstructing a source branch from mixed components. The complete Darwin cancellation source is public `878170bb` or the full reader-wait patch (468); the combined implementation patch (803) alone does not contain both files. The test-only baseline patch (801) and the published candidate are needed for actual RED/GREEN comparison; a compiler failure or selector-list failure is not the required behavioral RED.

## Linux continuation commands

Use the public repository, the exact candidate ref, and the existing workflow names. The CI workflow's current candidate-tip run is already red, so first repair the strict lint failure with reviewed source changes and preserve the full test gates. These commands are for the next qualifying run after that repair; this handoff does not claim it has happened.

```sh
git clone https://github.com/context-graph-ai/contextdb.git
git -C contextdb fetch origin linux-bounded-storage-candidate-20261019
git -C contextdb checkout --detach 35962ff149c1e145263627f83b286fc93f398755
# After a separately reviewed strict-lint repair is published:
gh workflow run ci.yml --repo context-graph-ai/contextdb --ref linux-bounded-storage-candidate-20261019
gh workflow run rc2-tests.yml --repo context-graph-ai/contextdb --ref linux-bounded-storage-candidate-20261019
```

The strict `-D warnings` gate is required. Do not cap or allow warnings in bulk. Do not reduce targets, features, assertion coverage, budgets, or timeouts. Keep the measured runner limits at 6 GiB process memory, no swap, and one job; use the existing default-derived writer budget (1.5 GiB where applicable). No claim of full Linux success follows from the diagnostic inventory or the two failed lint runs.

## Darwin continuation commands

The three focused engine selectors and their four parity controls are in [`darwin-selectors.json`](darwin-selectors.json). The candidate source workflow commands from run 803 are recorded as proposed, not executed. On a native Darwin runner checked out at exact ContextDB `878170bb142790f43ad73ff644ab8ae9eec1039f`, the focused command is:

```sh
cargo nextest run --locked -p contextdb-engine --lib -E 'test(=persistence::reader_coordination_wait_tests::queued_cancellation_after_check_and_arm_returns_before_reader_release) | test(=persistence::reader_coordination_wait_tests::public_reader_wait_cancellation_preserves_reader_and_writer_ownership) | test(=persistence::reader_coordination_wait_tests::public_reader_wait_release_without_cancellation_cleans_up_and_reacquires)'
```

Run the compile and list commands from `darwin-selectors.json` first; then run the focused selectors and the listed parity controls on both Linux and Darwin. Preserve the decisive held-reader cancellation assertion and the original four controls. Do not infer a Darwin timeout result from source inspection or a Linux run.

The Context Graph repository already has a macOS advisory workflow at `.github/workflows/macos-advisory.yml`. Its actual `workflow_dispatch` inputs are required `cg_sha` and `contextdb_sha`, both exact commit SHAs. At published Context Graph `fc489f0`, the root Cargo manifest and all 11 ContextDB lock entries still pin `a883cb73`. `scripts/gate.sh source` rejects a requested ContextDB SHA that differs from that lock. The following command is valid only for the existing fixture/memory-probe proof against the bound baseline; it does **not** test cancellation source `878170bb`:

```sh
gh workflow run macos-advisory.yml --repo context-graph-ai/context-graph --ref macos-ci-fixtures-cloud-20261019 -f cg_sha=fc489f0aaa66bbeaea9b33d449c4add58d795d76 -f contextdb_sha=a883cb73b787958be20ef1e712ffbfd26093f587
```

This is an unexecuted command using the existing workflow interface. There is no ready dispatch pairing `fc489f0` with `878170bb` or `35962ff`. Testing new cancellation source first requires a separate reviewed Context Graph dependency-pin change: update Cargo manifests/lock entries, all source fingerprints and guard literal bindings consistently, commit and publish it, then use its actual post-commit SHA and the future workflow SHA/ref. Do not blindly rebase or invent that future SHA. No callback execution follows from these pin commands.

The reader default-feature precondition (432) remains a distinct unfixed source contract. Inspect the default-feature compile/list results and repair the source precondition with a separate reviewed change that preserves default features and original oracles. Do not hide it behind marker-only `cfg` features or remove the 60-second/600-second timeouts. The observed timeout phase is UNPROVEN; do not attribute it to the reader precondition, cancellation, or any other phase without an actual receipt.

A dispatch does not itself establish green CI or a passed Darwin runtime. Check the exact workflow run and test receipts afterward. Do not invent additional inputs or weaken the existing default timeouts (60 seconds and 600 seconds where configured).

## Source approval and remaining obligations

The exact issued [seven-field BS public source approval](bs-public-repair-approval-878170bb.json) has SHA-256 `e7cd3146360b2cedb0b6813f154003ff8758a8f98249d78b789c7754832f3243`. “Strict 7” means these seven approval fields, not seven test results. It approves public source `878170bb` and its pin **FOR CLOUD QUALIFICATION ONLY**. Linux and Darwin runtime are **UNRUN**, the observed timeout phase is **UNPROVEN**, and it grants neither SHIP nor merge authority. The current Context Graph pin remains `a883cb73`; approval does not adopt a new dependency automatically.

Consumers are entitled to funded allocation custody and unchanged assertions/public APIs. **MISSING:** the 73 `result_large_err` warnings involving private `funded_error::OriginalError` (at least 1000 bytes) and the three overlapping `large_enum_variant` warnings remain open funding/layout obligations. Blanket `expect`/`allow` edits are not a style fix, and an unfunded `Box` is not a repair. A real representation change requires proof of allocation custody, funding/accounting, and drop order while preserving assertions and public APIs. Do not blindly rewrite checked arithmetic, lifetimes, or drops.

A future private invoice-identifier rename is identifier-only: preserve field order, types, constructors, and drop behavior. The unused private three-line `ReaderRegistry::get` is the narrow removal scope, not permission for blanket dead-code cleanup. Preserve the Writer default-derived budget, 6 GiB process limit, zero swap, one job, original times/features/targets/oracles, all 17 Linux shards and current ignores. Label nightly work UNRUN until measured. Do not edit, filter, or publish corpus data. The 377 warning rows, paths, lines, messages, and existing patch bytes remain unchanged in this correction.

## Constraints and interpretation

- Keep each source candidate on its named branch; do not merge, rebase, force-push, retarget the default branch, or move bounded-storage work while its separate work remains in flight.
- Keep the public engine on Candle for embeddings and Burn for Vigil detection; `cg` understanding remains hosted (BYOK). This packet changes no runtime.
- Do not claim the three macOS consumer repairs are guaranteed green. The Context Graph fixture repair, engine cancellation behavior, and Linux strict lint/tests each need their own actual run receipts.
- The engine Darwin cancellation source is published but unrun; the seven-field source approval is supplied, while the seven runtime selectors remain unrun. The Context Graph Darwin timeout phase is unproven. Existing 6 GiB/zero-swap/one-job constraints and the existing default timeouts remain in force.
- The 377-row inventory contains only repository-relative paths, line/column, lint name, diagnostic message, occurrence count, review classification, and the narrowly needed decision facts. It omits host-local material, full logs, environment values, source archives, corpus and model data, and secret values.

## File integrity

`SHA256SUMS` covers every data file in this directory, including this README and all patch artifacts. The new files are documentation and sanitized diagnostics only; the source tree and dependency manifests are unchanged by this handoff.
