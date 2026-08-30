# Agent readiness

This page describes how this repository measures whether an AI coding assistant can *use*
contextdb as an operator and *contribute* a change to it as a developer. Both measurements are
machine-graded — an AI model never grades another AI model's work — and both are re-run against
a specific commit, not claimed in the abstract.

## USE: can an assistant operate a database it just opened?

Three models (a frontier-tier model, a fast-tier model, and a third-party model) were each given
the same seven operator tasks, three fresh attempts per task per model, starting from nothing but
this repository and its docs:

| Task | What it covers |
|---|---|
| Create | Cold start: open a store, create a schema, insert and read back rows. |
| Policy (graph) | Build a DAG with `GRAPH_TABLE`, and get a cycle insert refused loudly rather than silently accepted. |
| Query (state machine) | Drive a state-machine table through `PROPAGATE` transitions. |
| Vector | Hybrid graph + vector queries against embedding columns. |
| Sync | Stand up a hub, enroll two edges, and reach a correct KEEP-FIRST collision outcome. |
| Diagnose | Recovery: diagnose/reset a store, and get a no-op migrate refused as expected. |
| Retention | Operator lifecycle: retention windows and durable delete that survive sync and restart. |

Every attempt is graded by executable post-conditions — process exit codes and the real
resulting database state read back with `--json` — never by asking a model whether the output
looks right. At the release-candidate commit measured here (`9d362f6`), the frontier-tier model
passed all three attempts on all seven tasks (7/7). The fast-tier model passed six of the seven
tasks fully and passed only one of three attempts on the seventh, the vector task. The
third-party model likewise passed six of the seven tasks fully and passed only one of three
attempts on the seventh, the diagnose task. A separate control run removed the repository's
agent-guidance files (`AGENTS.md` and the per-task `skills/`) and covered one operator task and
one orientation task: with the guidance gone, the operator task still passed 3/3 but took roughly
1.3-1.4x the turns and wall-clock time, and the orientation task took roughly twice as long (9
turns to 17, 43 seconds to 106 seconds). Guidance cut turns and wall-clock time on both tasks —
dramatically so for orientation — but the operator task's token count came out essentially even
with or without it; only the orientation task used dramatically fewer tokens with guidance. The
docs alone were sufficient to pass, just slower.

## CONTRIBUTE: can an assistant land a change through the real verification gate?

This measurement is narrower and says so plainly: it is a smoke test of *this repository's*
contributor surface — clear instructions, a working build, a gate that says what it means — not
a general claim about AI coding ability, and the patches produced were never merged into the
product.

Two small, real coding exercises were invented for the purpose, both graded at commit `889a372`:
adding a `LENGTH(text)` SQL scalar function, and adding a `statement_kind` field to `.explain`
output. A fresh model was given only the public repository (no hints beyond what a real
contributor would have) and graded by the repository's own five-command verification gate
(`cargo fmt --all --check`, clippy, the full test suite, a release build, and an isolated release
install plus a ticketed-Iroh durability smoke) plus a small functional spot-check per task, so a
change that passes the gate but is subtly wrong (for example counting bytes instead of
characters, or a build that quietly excludes the changed code) still fails.

Before any graded attempt counted, the grading machinery was checked against both exercises. For
`LENGTH`, a hand-written reference solution was run through the exact grader — the full
five-command gate plus the behavior spot-check — on the trial machine, at that same commit, and
passed. The `.explain` exercise had no reference solution to check that direction with, so its
grader was validated only in the rejection direction. Both negative controls — a byte-counting
`LENGTH` implementation, and a release build with the `.explain` feature excluded — were confirmed
to fail the graders' behavior checks. So the grader is proven two ways on `LENGTH` (accepts a
correct answer, rejects a wrong one) and one way on `.explain` (rejects a wrong one only).

The honest result: the strongest model tested did not clear the bar within the fixed 25-turn
budget. On the `LENGTH` exercise it repeatedly produced a semantically correct implementation
with its own tests, but ran out of turns before running `cargo fmt`, and the unformatted diff
failed the gate's first step. On the `.explain` exercise, which touches more files across the
parser, planner, and CLI, it spent its full turn budget exploring the codebase and never reached
an edit.

## What the measurement changed here

Running this measurement was not just an audit — it found and fixed real problems in the
repository it was measuring. A near-miss caused purely by unformatted code led to a direct
reminder in `AGENTS.md` telling contributors to run `cargo fmt --all` before considering a
change done. Flaky sync tests that raced real wall-clock sleeps were rewritten to run on
injected virtual time instead. The verification gate itself was fixed to build and check
correctly from a plain source export rather than assuming a git checkout was present. And the
sync guide's cross-network guidance (NAT traversal, hub-log expectations) was corrected against
what actually happens on a real two-machine run. The measurement exists to improve this surface,
and it did.
