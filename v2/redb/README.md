# contextdb-redb

redb 4.1.0 with changes for contextdb. Upstream version: [redb](https://github.com/cberner/redb)
4.1.0, as published on crates.io. The library is still named `redb`. Its leaves are written
differently (below), so it opens only files that it wrote itself: upstream's tests of opening
files written by redb 2.6 are removed, and every other upstream test passes unchanged.

Backends may reject allocator growth through `StorageBackend::admit_growth` before cache
invalidation, file resizing or allocator changes. A refusal takes the clean abort path;
errors from the subsequent physical I/O retain the normal storage-failure behavior.

`Table::insert_discard` replaces a value without copying its old bytes for a discarded
return value. Ordinary `insert` keeps its existing return contract.

`DRAIN_BYTES` describes the required bounded-drain workspace: one lent list, at most 4,096
extracted entries, and deferred frees bounded by entries plus height. The recovery integration must enforce and witness that bound.

This version changes the following and nothing else. Each change is written so that it can be
proposed upstream.

Page alignment uses `is_multiple_of` for its nonzero page size, preserving the
rounding while passing the current strict clippy check. The example requiring the
separate, unshipped `redb_derive` crate is removed.

Checking a handle after an I/O failure:

- `Database::check_io_errors` forwards the backend failure check; after failed physical I/O,
  it returns `PreviousIo` until the database is reopened.

Reading a file while its writer has it open:

- `Builder::open_read_only_with_backend` is new: it opens a database for reading whether or not
  another handle has it open for writing, takes no lock and writes nothing, and each
  `begin_read()` on it reads the commit that is newest in the file at that moment.
- `ReadTransaction::commit_id` is new: it returns the id of the commit a read transaction reads,
  and ids rise with every commit of a file.
- `Builder::set_held_commits_callback` is new: before a writer reuses freed pages it asks the
  given function for the oldest commit that a reader it cannot see still holds and keeps every
  page that this commit or a later one can reach, and `Database::compact` returns
  `TransactionInProgress` while such a commit is held.
- Every commit now writes its pages to the file before it writes the header that names them, so
  a reader of the file never finds a header ahead of its pages.

Reading a write transaction's current tables and reserving room:

- `WriteTransaction::peek_table` reads a closed table at its current uncommitted root and refuses
  one open for writing. Drop the table and all its guards before any mutation or transaction exit.
- `WriteTransaction::reserve(pages)` allocates and frees that many pages within the transaction,
  so later work can reuse them. The declared file-size limit still applies.
- Normal tables release pages while walking branches; file growth reserves blocks or writes zeros.

Checking pages when they are read:

- A committed page that is read from the file is now checked, before it is cached or returned,
  against the checksum that its parent page or the root of its tree holds for it, and a mismatch
  is `StorageError::Corrupted`; upstream compares these checksums only during repair.
- The page walks of repair and compaction, which have no checksum at hand, no longer put the
  pages they read into the page cache, so the cache only holds pages that were checked or that
  were written through the same handle.

Keeping a file close to the size of what it holds:

- `Builder::set_manual_compaction(true)` leaves ordinary commits untrimmed.
  `WriteTransaction::compact_pages(allowance, from)` bounds candidate reads and relocation,
  returns `(moved, next)` with an opaque position; commit and clean close trim.
  `compaction_space(&mut allowance)` returns free, total, eligible pending, last freed pages and held readers;
  its pending scan shares the read allowance and stops at churn. `compaction_state()` reads no tree.
  Relocation uses free-now minus churn; one scoped file-size clip covers moves and metadata.
  `compaction_position` and `set_compaction_position` keep the opaque position in a system table;
  a clipped copy leaves an in-memory hint. Every immediate commit with quick repair disabled
  prepares metadata before publication. Preparation reuses released pages and keeps their list;
  a refused extension rolls back, restores those pages to allocated and leaves the handle writable.
  `prepare_compaction_commit` applies the compaction-only growth clip, admits metadata before
  releasing pending pages and selects maximum trimming for move and drain commits; the clip
  is restored on transaction exit. `pending_free_pages` excludes the newest retired proof,
  which each publication replaces, while retaining older chains and ordinary pending frees.
  `compaction_backlog` applies the same exclusion to its pending batch count;
  `has_free_tail` exposes the allocator's trim check. Drains commit only for those pending
  entries or a trimmable tail. A drain admits iterations only while pending entries at or
  before its starting physical commit remain, stopping once those are gone or a pass makes
  no progress. An admitted pass still trims a free tail and may release newer frees.
  `RELEASE_PAGES` bounds compaction's staged addresses at 4,096 (32 KiB encoded); ordinary release
  uses its commit's allocated-page count clamped to 4,096..=131,072 (at most 1 MiB encoded).
  Remaining entries wait. Failed writes and syncs still require reopening the handle.
  Settings and position metadata have a bounded lookup cost; release has at most the bounded
  entries' root-to-leaf paths. Discovery and pending inspection still share the work allowance.

- A key type answers one new question, `Key::compares_as_bytes`: whether it orders keys exactly
  as their bytes. Only `&[u8]` answers yes.
- A leaf of such keys keeps once the leading bytes that all its keys share (at most 255): its
  second header byte, which upstream leaves unwritten, holds their count; its first key is kept
  whole and every later key without them. A tree whose values are the unit type, such as the
  value sets of a multimap table or a table used as a set of keys, shares nothing and is written
  as upstream writes it. An edit in place leaves the shared bytes where they are, also for a
  leaf's first pair: a key that no longer begins with them rebuilds the leaf instead.
- A leaf that must split is cut at the first of a few places that leaves both halves within one
  page, else at the one that takes the fewest pages: after a pair that ends a run of inserted
  keys (so a table written in key order has full leaves), before it, or where half the bytes
  are. A leaf is joined with a neighbour when what is left, keys counted whole, is under a third
  of a page; two leaves joined try half, then the place nearest half towards where they met
  that leaves both within a page, then where they met. The key that a branch
  keeps between two leaves of byte keys is the shortest beginning of the right leaf's first key
  that is above the left leaf's last key; upstream keeps the left leaf's whole last key.
- `Range::next_lent` is new: it answers the next pair borrowed until the walk moves on, with no
  key copied to the heap. A key of a leaf that shares bytes is rebuilt in one buffer the range
  owns, whose shared bytes stay while the walk is on that leaf.
- The file grows in small steps. Once its usable bytes pass 1 MiB the file grows by a sixteenth
  of them, and never by less than twice the allocation that needed the room; upstream doubles
  the file while it has one region and adds a whole region at a time after that. A file under
  1 MiB still doubles, and a region is still filled before the next one is started.

Everything below is upstream's README.

# redb

![CI](https://github.com/cberner/redb/actions/workflows/ci.yml/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/redb.svg)](https://crates.io/crates/redb)
[![Documentation](https://docs.rs/redb/badge.svg)](https://docs.rs/redb)
[![License](https://img.shields.io/crates/l/redb)](https://crates.io/crates/redb)
[![dependency status](https://deps.rs/repo/github/cberner/redb/status.svg)](https://deps.rs/repo/github/cberner/redb)

A simple, portable, high-performance, ACID, embedded key-value store.

redb is written in pure Rust and is loosely inspired by [lmdb](http://www.lmdb.tech/doc/). Data is stored in a collection
of copy-on-write B-trees. For more details, see the [design doc](docs/design.md)

```rust
use redb::{Database, Error, ReadableDatabase, TableDefinition};

const TABLE: TableDefinition<&str, u64> = TableDefinition::new("my_data");

fn main() -> Result<(), Error> {
    let db = Database::create("my_db.redb")?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.insert("my_key", &123)?;
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    assert_eq!(table.get("my_key")?.unwrap().value(), 123);

    Ok(())
}
```

## Status
Stable and maintained.

The file format is stable, and a reasonable effort will be made to provide an upgrade path if there
are any future changes to it.

## Features
* Zero-copy, thread-safe, `BTreeMap` based API
* Fully ACID-compliant transactions
* MVCC support for concurrent readers & writer, without blocking
* Crash-safe by default
* Savepoints and rollbacks

## Development
To run all the tests and benchmarks a few extra dependencies are required:
* `cargo install cargo-deny --locked`
* `cargo install cargo-fuzz --locked`
* `apt install libclang-dev`

## Benchmarks
redb has similar performance to other top embedded key-value stores such as lmdb and rocksdb

|                           | redb      | lmdb       | rocksdb        | sled     | fjall       | sqlite     |
|---------------------------|-----------|------------|----------------|----------|-------------|------------|
| bulk load                 | 17063ms   | **9232ms** | 13969ms        | 24971ms  | 18619ms     | 15341ms    |
| individual writes         | **920ms** | 1598ms     | 2432ms         | 2701ms   | 3488ms      | 7040ms     |
| batch writes              | 1595ms    | 942ms      | 451ms          | 853ms    | **353ms**   | 2625ms     |
| len()                     | **0ms**   | **0ms**    | 749ms          | 1573ms   | 1181ms      | 30ms       |
| random reads              | 1138ms    | **637ms**  | 2911ms         | 1601ms   | 2177ms      | 4283ms     |
| random reads              | 934ms     | **631ms**  | 2884ms         | 1592ms   | 2357ms      | 4281ms     |
| random range reads        | 1174ms    | **565ms**  | 2734ms         | 1992ms   | 2564ms      | 8431ms     |
| random range reads        | 1173ms    | **565ms**  | 2742ms         | 1993ms   | 2690ms      | 8449ms     |
| random reads (4 threads)  | 1390ms    | **840ms**  | 3995ms         | 1913ms   | 2606ms      | 7000ms     |
| random reads (8 threads)  | 757ms     | **427ms**  | 2147ms         | 1019ms   | 1352ms      | 8123ms     |
| random reads (16 threads) | 652ms     | **216ms**  | 1478ms         | 690ms    | 963ms       | 23022ms    |
| random reads (32 threads) | 410ms     | **125ms**  | 1100ms         | 444ms    | 576ms       | 26536ms    |
| removals                  | 23297ms   | 10435ms    | 6900ms         | 11088ms  | **6004ms**  | 10323ms    |
| uncompacted size          | 4.00 GiB  | 2.61 GiB   | **893.18 MiB** | 2.13 GiB | 1000.95 MiB | 1.09 GiB   |
| compacted size            | 1.69 GiB  | 1.26 GiB   | **454.71 MiB** | N/A      | 1000.95 MiB | 556.85 MiB |

Source code for benchmark [here](./crates/redb-bench/benches/lmdb_benchmark.rs). Results collected on a Ryzen 9950X3D with Samsung 9100 PRO NVMe.

## License

Licensed under either of

* [Apache License, Version 2.0](LICENSE-APACHE)
* [MIT License](LICENSE-MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.

`Database::set_file_size_limit` clips allocator growth and returns typed `FileSizeLimit` before
backend resize when the requested pages cannot fit, leaving the handle usable.

Recovering the last durable commit:

- `Builder::set_incremental_recovery(true)` stores checksummed fixed-size chains of changed
  pages. Reopen verifies that chain and the changed tree pages with file-sized bitmaps;
  a broken newest commit falls back to the preceding complete state, including on the reader path.
- Allocated-page tracking uses bitmaps. `flush_freed_pages` writes pending freed pages at
  4,096 entries and appends across calls. Old proof chains enter the same bounded waiting-page
  release as ordinary freed pages. Non-durable commits assert while this option is enabled.

- An enabled unclean writing open forgets free space and pins its selected roots. It reads
  the header and last commit proof; system/waiting trees and data branches are found later
  by `Database::recovery_step(allowance)`. `recovery_state()` reports remaining work,
  file bytes and effective growth ceiling; known space has no remaining-work value.
  The same page visitor serves discovery and compaction. Old pages remain taken until the
  checked marking pass completes, then holes are released in bounded chunks. Clean close
  completes discovery before saving allocator state; a failed checked walk frees nothing.
- `DISK_LIMIT` remains a hard physical ceiling. The engine gates host writes during
  recovery at a reserve of `min(limit, max(limit / 100, 64 MiB))` inside that ceiling.
  Below 64 MiB, writers wait for engine maintenance or receive the caller-maintenance
  refusal until recovery ends. Healthy stores retain their configured ceiling and
  fitting writes remain allowed.
