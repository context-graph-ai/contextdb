---
name: work-fabric
description: Hand a unit of work to another machine over contextdb's work ledger and move the bytes it needs over the blob plane — job lifecycle, claim arbitration, entitlement, and reclaim.
---

# The work fabric — ledger + blob plane

Two library surfaces that let a set of contextdb nodes distribute work among themselves without a
queue service: the **work ledger** (who is doing what) and the **media/blob plane** (the bytes that
work needs). They belong together because a job on the ledger can reference a `blob_ref` input, and
resolving that reference is the blob plane's job, not the ledger's.

**There is no CLI for either.** These are Rust APIs. If you are looking for a `contextdb`
command, there isn't one — embed the crates: add `contextdb-engine` and `contextdb-server` to
your `Cargo.toml` `[dependencies]` (see [`docs/getting-started.md`](../../docs/getting-started.md)
for the exact lines), or, inside a checkout of this repo, depend on the crates in `crates/` directly.

## Two modules named `work_ledger` — pick the right one

Importing the wrong one is the easy mistake here. They are different layers, owned by different
crates:

| Module | Layer | Use it for |
|---|---|---|
| `contextdb_engine::work_ledger` | Class-blind **bookkeeping**. Seven append-only tables recording lifecycle events. | Submitting jobs, reading state, recording results/failures, the pure claim/lease functions. **This is where job state actually lives.** |
| `contextdb_server::work_ledger` | **Distributed execution**, built on top over `SyncClient`. | Cross-machine claim arbitration, the worker loop, the `WorkExecutor` seam where your real work runs. |

Rule of thumb: state and tables → engine. Two workers racing for the same job → server.

## Job state is computed, never stored

A job's state — `Pending`, `Leased`, `Done`, `Failed`, `Cancelled` — is **derived from the append-only
rows** by `job_state(&db, job_id, now_ms)`. There is no status column to update, and therefore no
way for two machines to disagree about a status column.

The lifecycle events:

- `submit_job` — the work exists
- `insert_claim` — someone took it under a lease
- `record_result` — completed, **exactly once**: a second call when a result row already exists is a
  no-op `Ok`, so duplicate work is caught rather than double-recorded
- `record_failure` — the failure row is what **legalizes the next attempt**
- `cancel_job` — withdrawn

**Lease expiry is advisory wall-clock time supplied by the caller.** The engine never trusts a clock
on its own, and an expired lease does **not** by itself mean the job was abandoned — that judgment
belongs to the caller deciding whether to re-claim.

## Install the ledger schema

The seven tables are not created for you. Call this at open; it is idempotent and safe when the
schema already arrived via sync:

```rust
use contextdb_engine::work_ledger::install_work_ledger_schema;

install_work_ledger_schema(&db)?;
```

Each ledger table carries a deliberate conflict policy — read them from
`work_ledger_conflict_policy_entries()`, or apply them onto an existing `ConflictPolicies` with
`apply_work_ledger_policy_overrides(&mut policies)` before standing up a hub.

## Submit a job

`JobSpec` is builder-only. Required identity is the job id, its work class, its mode and the
submitting node; everything else defaults (no requirement tags, no inputs, priority `0`, no
deadline, `max_attempts` 2).

```rust
use contextdb_engine::work_ledger::{JobSpec, submit_job};

let spec = JobSpec::builder("job-1", "describe-image", "once", "node-a")
    .requirement_tags(vec!["gpu".to_string()])
    .max_attempts(3)
    .submitted_at_ms(now_ms)
    .build();

let no_direct_inputs: [&[u8]; 0] = [];
submit_job(&db, &spec, &no_direct_inputs)?;
```

Requirement tags are open vocabulary and **every one must be advertised by a node to claim the
job** — that is how a CPU-only node stays out of the way of GPU work. `claimable_jobs(&db, node_id,
&advertised_tags, &policy, now_ms)` is the matching read.

Inputs are either carried inline (the `&[I]` argument, small payloads) or referenced:
`InputRef::local_path(...)` or `InputRef::blob_ref(hash)`.

**Inline (`ledger_input`) inputs are NOT kept until the job is terminal.** `work_inputs` declares
`RETAIN 7 DAYS`, so a job's ledger-carried input copies age out on that window regardless of
whether the job was ever claimed. `materialize_inputs` distinguishes a job that never declared
ledger input (returns `Ok` with an empty result) from one whose input aged out (returns
`Err(Error::WorkInputExpired { job_id })`) — a worker must handle the typed refusal rather than
executing on silently-empty input. Submit and claim promptly if the job's inputs are inline.

**That `RETAIN 7 DAYS` window is engine-owned policy, not something you tune with `ALTER TABLE`
on `work_inputs`** — nor is `work_capabilities`' (or `peer_directory`'s / `work_node_contacts`')
`HISTORY` / `SYNC CONFLICT` / `SYNC ...` / `SYNC SAFE` declaration. All four are built-in tables
this module and its siblings install, and each one's own bookkeeping depends on staying at the
shape declared in its own `CREATE TABLE` text. A locally-typed `ALTER TABLE ... DROP RETAIN` /
`SET RETAIN <other window>` / `SET HISTORY ALL` / `SET SYNC CONFLICT KEEP FIRST` / `SET SYNC ...`
against one of these four tables refuses loudly instead of silently taking effect and then being
silently reverted by the next installer call. You also can't get around any of this by typing your
own `CREATE TABLE` for one of these four names: it refuses unless your columns structurally match
the owning installer's own declaration, and unless any policy clause you DO write matches what
that installer declares (silence on policy is fine — that is what a pre-declaration root looks
like before its first reconcile). The identical mutation arriving from a PEER over sync is
held to the same bar: an explicit differing value — whether spelled as an `ALTER TABLE`, as a
`CREATE TABLE` adopting one of these tables from a peer that already has it, or as a fresh
`CREATE TABLE` of a reserved name from a peer that has already dropped it (guarding against
DROP + CREATE circumvention) — refuses the whole sync batch; an axis the arriving DDL is
simply silent on preserves this table's current declared value rather than clearing it, so a
half-healed peer converging on the same declaration interoperates instead of wedging. If your workload needs a different input-copy lifetime than 7
days, that is not a schema knob on this table — it is `run_input_retention`'s `grace_ms`
argument, which you call on your own
schedule.

## Claim across machines

`contextdb_server::work_ledger::claim_job` **claims by push**: it inserts a local claim row, pushes
it, and the hub's conflict reply on the claim key *is* the arbitration verdict. Two workers racing
for the same job resolve at the hub, not by a local guess.

```rust
use contextdb_server::work_ledger::claim_job;

let outcome = claim_job(&sync_client, "job-1", 1, "node-b", lease_deadline_ms, now_ms).await?;
```

**If the hub is unreachable the claim is held locally as `Won { synced: false }` rather than
blocking.** That is deliberate — an offline node keeps working — but it means the claim is not yet
arbitrated, and you should treat it as provisional.

To run work rather than hand-drive it, implement `WorkExecutor` and use `poll_and_execute_once` or
`run_worker_loop` with a `WorkerConfig`. The executor trait is the seam where your real work
happens; everything around it is ledger bookkeeping.

## Move the bytes — the blob plane

`contextdb_server::BlobStore` moves opaque, content-addressed bytes node to node.

```rust
use contextdb_server::BlobStore;

let blob_store = BlobStore::new(db.clone(), policy, identity_path);

// Holder: ingest (idempotent — the hash IS the content) and start serving.
let blob_hash = blob_store.ingest_bytes(&std::fs::read("frame.jpg")?)?;
blob_store.serve_on(&iroh_server);
```

`ingest_file(&Path)` is the streaming sibling of `ingest_bytes`. Because the returned `BlobHash` is
the hash of the bytes, ingesting the same content twice is a no-op.

On the consumer side, fetching is async, node-to-node and **hash-verified — only bytes that hash to
the requested `BlobHash` ever reach your sink**:

```rust
let mut sink = Vec::new();
let bytes_written = blob_store
    .resolve_blob_ref(&blob_hash, &holder_ticket, &mut sink)
    .await?;
```

Failures are a matchable `ResolveError`, not a string: `Unentitled`, `PolicyForbidden`,
`HashMismatch`, `HolderUnreachable`, `LocalStoreUnavailable`, `BlobNotFound`, `TransferAborted`,
`SinkWrite`.

## The entitlement rule — the actual security boundary

**A consumer may fetch a blob only while it holds a *live* claim** — lease still ahead, that attempt
not failed — **on a job whose inputs reference that blob's hash.**

This is checked **holder-side at serve time, before any payload bytes move**. The check is the gate,
not an after-the-fact audit, and it is never trusted from the requester.

There is also a local, identity-blind pre-check the consumer can run before dialing. **That is a
round-trip optimization and nothing more — never treat it as the entitlement boundary.**

If the holder's local mirror of the ledger can lag behind a claim that landed on the hub, wire
`set_claim_refresh` to this node's own `SyncClient::pull_default()` so the serve-time check
refreshes instead of polling.

## Wiring the two together

```rust
// Node A (holder): ingest a blob, serve it, and submit a job that references it.
use contextdb_engine::work_ledger::{InputRef, JobSpec, submit_job};
use contextdb_server::{BlobStore, work_ledger::claim_job};

let bytes = std::fs::read("frame.jpg")?;
let blob_hash = blob_store.ingest_bytes(&bytes)?;
blob_store.serve_on(&iroh_server);

let spec = JobSpec::builder("job-1", "describe-image", "once", "node-a")
    .input_refs(vec![InputRef::blob_ref(blob_hash.clone())])
    .build();
let no_direct_inputs: [&[u8]; 0] = [];
submit_job(&db, &spec, &no_direct_inputs)?;

// Node B (worker): claim the job under a lease, then resolve the referenced blob.
// Holding a live claim on this job is what entitles Node B to fetch the bytes.
let claim = claim_job(&sync_client, "job-1", 1, "node-b", lease_deadline_ms, now_ms).await?;
let mut sink = Vec::new();
let bytes_written = blob_store
    .resolve_blob_ref(&blob_hash, &holder_ticket, &mut sink)
    .await?;
```

**The ledger reserves `blob_ref` but does not resolve it.** `materialize_inputs` refuses a
`blob_ref` input with `Error::InputRequiresBlobResolver` and points you at the blob resolver. The
ledger tracks that a job depends on a blob; the media plane moves the bytes.

## Reclaiming space

`reclaim_unreferenced(now_ms, grace_ms)` frees a blob once every job referencing it is terminal past
the grace window, or once no job references it at all. A later resolve against a reclaimed blob
returns `BlobNotFound` — **even from an otherwise-entitled caller.** Budget your grace window
against your slowest worker, not your fastest.

## Transport

Direct or relayed is a **serve-time choice on the holder's endpoint spec**, and by default it is
direct only — contextdb contacts no relay unless asked. The operator opts into the public relays or
a self-hosted relay URL, the same relay configuration the `sync` skill describes. The choice rides
in the serve ticket, so a consumer behind NAT that cannot reach the holder directly is bridged
automatically.

## Depth

- [`docs/architecture.md`](../../docs/architecture.md#work-ledger-and-media-plane) — the module
  disambiguation, entitlement rule, and the worked two-node example above
- [`skills/sync/SKILL.md`](../sync/SKILL.md) — the `SyncClient`/hub the execution layer rides on
