# contextdb-redb

redb 4.1.0 with changes for contextdb. Upstream version: [redb](https://github.com/cberner/redb)
4.1.0, as published on crates.io. The library is still named `redb`. Its leaves are written
differently (below), so it opens only files that it wrote itself: upstream's tests of opening
files written by redb 2.6 are removed, and every other upstream test passes unchanged.

This version changes the following and nothing else. Each change is written so that it can be
proposed upstream.

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

Checking pages when they are read:

- A committed page that is read from the file is now checked, before it is cached or returned,
  against the checksum that its parent page or the root of its tree holds for it, and a mismatch
  is `StorageError::Corrupted`; upstream compares these checksums only during repair.
- The page walks of repair and compaction, which have no checksum at hand, no longer put the
  pages they read into the page cache, so the cache only holds pages that were checked or that
  were written through the same handle.

Keeping a file close to the size of what it holds:

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
