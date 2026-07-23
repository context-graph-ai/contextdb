# contextdb-engine — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). This file carries the one
discipline that is local to this crate: how the engine reads the clock.

## Clock-seam discipline

**Every persisted timestamp goes through `contextdb_core::Wallclock::now()`.** Never
`SystemTime::now()`, never inline `duration_since(UNIX_EPOCH)` epoch math, anywhere in engine code
that writes a value a row will carry. The engine is the layer that turns "now" into durable state —
a raw clock read here is a value no test can pin and no replay can reproduce.

The seam type and its test controls live in `contextdb-core` (`crates/contextdb-core/src/types.rs`);
the discipline is enforced here because this is where the writes are.

### Mocking it in tests

Use the RAII guard, not the bare reset:

```rust
use contextdb_core::Wallclock;

let _guard = Wallclock::test_clock_guard(|| 1_700_000_000_000);
// ... drive the engine; every Wallclock::now() inside returns the mocked value
// guard restores the previous clock on drop, including on an assertion panic
```

`Wallclock::reset_test_clock()` still exists, but a trailing call to it leaks the override when an
assertion panics first — under `--test-threads=1` that override then bleeds into the next test.
Prefer the guard.

**Limitation — the override is thread-local.** An engine-internal spawned thread sees the *real*
clock, not your mock. Do not assume otherwise. Drive background work synchronously on the test
thread instead (for example `run_pruning_cycle()`), which is also what the no-sleep rule in the
root file requires.

### The audits that enforce this

Two gate-failing audits run on every `cargo test --workspace`. Both live in
`crates/contextdb-core/tests/` and scan the whole workspace, this crate included.

- **`timestamp_audit.rs`** — audits `<col> TIMESTAMP` column declarations (`created_at`,
  `valid_from`, `valid_to`) against an exact whitelist. A new timestamp-shaped column has to be
  admitted deliberately.
- **`test_estate_audit.rs`** — the **ratchet**. Per-file frozen counts of `thread::sleep(` /
  `tokio::time::sleep(` call sites and of raw clock reads across the test tree. A NEW sleep or raw
  clock read in any test file fails the gate.

**A ratchet count may be lowered, never raised.** Lower it when you delete a site. The fix for a
failing ratchet is never "bump the number" — it is `Wallclock::test_clock_guard` for
time-dependent behavior and counter/state/event waits for synchronization. Raising a count is an
explicitly approved exception, stated in the commit, not a routine edit.
