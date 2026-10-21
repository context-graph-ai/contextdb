Kind: `<official | community | historical>` · Status: `<NOT EVIDENCE | descriptive only | acceptance rows reported below>`

# Performance result: <area> on <hardware label>, <run date>

<!-- For every historical entry, keep this banner directly under the heading:
> Historical exercise of the template. Not evidence of current performance. Values labelled unknown were not recorded.
-->

Copy this file to `docs/performance/results/<unique-id>.md` and replace every
`<…>` from reviewed values only. See
[the guide](../README.md#publishing-a-result-manual).

Rules for filling it in:

- **The first line is mandatory.** Kind and Status come before anything else,
  and the same Kind and Status open this entry's line in the guide's index.
  `historical` entries also keep the banner above.
- **Public only.** Never paste raw receipts, logs, absolute paths, hostnames,
  user names, cgroup paths or secrets. Raw material stays private.
- **Never invent a value.** If a field was not recorded, write `unknown`; if it
  does not apply, write `not applicable` and say why. A guessed number is not
  an answer.
- **Descriptive is not acceptance.** Numbers go in the descriptive table, which
  has no acceptance threshold and no verdict. Only a row quoted verbatim from
  [the guide's acceptance thresholds](../README.md#acceptance-bars) goes in the
  acceptance table. A benchmark assertion, a source budget or a completed run
  is never an acceptance verdict.
- **Unknown is not a pass.** An acceptance row that was not run is `NOT RUN`;
  one whose run does not match the row's scope, or whose provenance is partly
  unknown, is `NOT EVIDENCE`.
- **Failed and partial runs are worth recording.** A historical run without a
  paired baseline, or a run that stopped early, is published with that
  limitation stated, not withheld.

## Classification

- Kind: `<official | community | historical>` — historical means measured
  before this template existed and reconstructed from what was kept.
- Measured by: `<public name or handle, or "unknown">`; reviewed by: `<owner>`
- Run date: `<YYYY-MM-DD, or "unknown">`
- Run status: `<completed | failed | incomplete | not measured>` — failed: a
  command exited non-zero; incomplete: stopped, timed out or only partly kept.

## Source

- Paired comparison: `<yes | no>`
- Baseline: `<full baseline SHA>`, or `not available: <reason>`
- Candidate / measured revision: `<full SHA>`, or `unknown`
- Profile: `<storage | vector | workflows | system | all | not applicable: <reason>>`

## Workload and configuration

Write `not applicable` for a field the benchmark does not have.

- Fixture / dataset: `<name and identity (fixture hash or file in the repo)>`; size: `<rows, vectors, bytes>`
- Input dimension: `<e.g. 384>`; concurrency: `<threads, clients>`
- Features and settings: `<enabled Cargo features and env/config that can affect the run>`
- Repetitions / samples: `<Criterion sample count, run count>`; warm or cold: `<warm | cold | both>`
- Build configuration: `<build profile, toolchain, compiler flags / RUSTFLAGS if non-empty>`
- Exact command used (verbatim, with local paths replaced by placeholders):
  `<command exactly as run, or "unknown" if not recorded>`

## Environment

- Hardware (public label): `<CPU model, cores, memory>`
- Filesystem and storage configuration (public): `<filesystem type and storage configuration; never mount paths or hostnames>`
- OS / kernel: `<…>`
- Toolchain: `<rustc version>`
- Resource limits: `<cpu/memory limits, or "none", or "unknown">`
- Isolation: `<what else was stopped during the runs, or "unknown">`

## Scope

- Benchmark targets expected: `<n>`; completed: `<n>`
- Selected: `<list>`; not run: `<list or "none">`; filters: `<e.g. engine_throughput: nv_, or "none">`
- A single-profile or filtered run is a partial, descriptive measurement of
  that area, never a global performance pass.

## Acceptance (quoted acceptance thresholds only)

One row per acceptance row this entry reports on. Quote the row from
[the guide's acceptance thresholds](../README.md#acceptance-bars) verbatim, with its
number, statistic and scope. If the entry reports on none, write "None
reported." and leave the table out.

| Acceptance row (quoted verbatim) | Verdict | Evidence and coverage |
|---|---|---|
| `<"…" — the exact row text>` | `<PASS / FAIL / NOT RUN / NOT EVIDENCE>` | `<what was measured, statistic and unit, how it matches the row's scope, what was not covered>` |

## Descriptive measurements (no acceptance threshold and no verdict)

| Target / benchmark | Statistic | Unit | Baseline | Candidate | Change |
|---|---|---|---|---|---|
| `<…>` | `<median / mean ± CI>` | `<…>` | `<value, or "not available">` | `<value, or "not measured">` | `<value, or "not comparable">` |

Notes (one line for any row that is not a straightforward paired value):

- `<target>`: `<e.g. no baseline, command failed, workload differs>`

## Missing metadata

Tick what is not known; if any box is ticked, provenance is partly unknown and
no acceptance row may be `PASS` on the strength of this entry.

- [ ] Baseline revision
- [ ] Measured revision
- [ ] Run date
- [ ] Workload / fixture identity
- [ ] Hardware, OS or toolchain
- [ ] Resource limits or isolation
- [ ] Full list of targets that ran
- [ ] Exact command

Provenance: `<complete | partly unknown: list above>`

## Limitations

- `<harness output interpreted from logs: what was read>`
- `<anything else that could affect the comparison>`

Raw receipts kept privately by: `<who, or "not kept">`
