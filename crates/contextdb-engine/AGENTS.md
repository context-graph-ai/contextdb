# contextdb-engine — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). This file carries what is local to
this crate: how the engine reads the clock, and which module owns which part of the bounded read
surface.

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

## Read architecture — one session, two routes, no second path

The bounded read surface is deliberately narrow: **one** public session type, **two** routes under
it, **one** execution kernel, **one** typed refusal vocabulary. Almost every mistake in this area
is the same mistake — adding a parallel path instead of extending the owning module. Before you
add anything here, find which module below already owns the concept.

| Module | Owns |
|---|---|
| `read_session.rs` | Assembly of the one public `ReadSession` and its two frozen routes. `ReadSession::open` picks the route; `open_owner_only` / `open_owner_only_in_runtime_dir` are the owner-only doors, and they never touch the store file. |
| `direct_file_reader.rs` | The file route: the sealed hydrated snapshot of one committed image. Persistence hands it over only after proving every guard, transaction, handle and reader breadcrumb is released; the session and its cursors then hold owned memory, never a lazy persistence or blob resolver. |
| `owner_read/` | The owner route's plane — `admission` (who is let in, and the concurrency lease), `client` (the reading side), `service` (the owning side that listens, holds cursors and answers). |
| `local_transport/` | Authenticated addressing, framing, carriage and deadlines for that plane. Nothing else opens a channel. |
| `executor/bounded.rs` | The one execution kernel. Both routes run every read through it and charge the same budgets. |
| `persistence.rs` | The trusted companion record and its lock (`recorded_unserved_owner`, ~`:327`), the claim window (`observe_unsettled_claim`, ~`:775`), the reader hold (`ReaderStoreHold`, ~`:918`), and the published reader identities — the breadcrumb machinery at ~`:1122`, `:1374`, `:1435`, `:1539`, `:1759`. |
| `contextdb-core::read_contract` | The shared typed vocabulary: refusal classes and kinds a caller branches on. |

Rules that hold across all of it:

- **The file route applies only to file-backed stores.** `:memory:` is an in-process ephemeral
  writable session — no file, no companion, no owner channel, and no cross-process route of any
  kind. Do not grow a memory-backed owner or file route to make a shape look symmetric.
- **`open_owner_only` never touches the file.** It asks the channel or fails; that is the whole
  point of the door. A "fall back and read the file anyway" branch inside it is a defect.
- **Forbidden, without exception:** a shadow store beside the real one; a second transport beside
  `local_transport`; a duplicated refusal type instead of `read_contract`'s; or execution
  semantics specific to one consumer instead of `executor/bounded.rs`'s. If the shape you need
  does not fit, change the owning module — never add a parallel one.

The user-facing contract these modules implement is `docs/cli.md`; when behavior and that page
disagree, one of them is a bug, and which one is a product question, not a local decision.
