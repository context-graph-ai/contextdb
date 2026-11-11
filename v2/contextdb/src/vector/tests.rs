//! The pure functions of the vector part: the range a slot proves for its vector's similarity,
//! the bytes of a block, and the record of a partition. And what sealing makes of a store whose
//! records do not agree, and what a write and sealing make of a schema record whose VECTOR
//! column is not one: no public call writes such a store, so the last tests here write one
//! through the store itself.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::block::{BLOCK_BYTES, BlockReader, BlockWriter, takes};
use super::directory::Partition;
use super::seal::seal;
use super::slot::{Screen, distance, slot_len, write_slot};
use super::stage::stage;
use crate::Database;
use crate::memory::Budget;
use crate::schema::Catalog;
use crate::store::{ChangeSet, Key, Store, TableId};
use crate::types::{Error, Value};

/// A fixed scatter of numbers: a xorshift that starts from a seed.
struct Fixed(u64);

impl Fixed {
    fn seeded(seed: u64) -> Fixed {
        let spread = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD6E8_FEB8_6659_FD93;
        let mut fixed = Fixed(if spread == 0 { 1 } else { spread });
        for _ in 0..8 {
            fixed.unit();
        }
        fixed
    }

    /// The next number from -1 up to, not including, 1.
    fn unit(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 40) as f32 / (1_u64 << 23) as f32 - 1.0
    }
}

fn vector(seed: u64, dimension: usize) -> Vec<f32> {
    let mut fixed = Fixed::seeded(seed);
    (0..dimension).map(|_| fixed.unit()).collect()
}

/// The distance as the design writes it: each number widened to 64 bits, the three sums added
/// in the order of the numbers, and 1 for a vector of zeros.
fn written_out(stored: &[f32], query: &[f32]) -> f64 {
    let (mut dot, mut aa, mut bb) = (0.0_f64, 0.0_f64, 0.0_f64);
    for (x, y) in stored.iter().zip(query) {
        let (x, y) = (f64::from(*x), f64::from(*y));
        dot += x * y;
        aa += x * x;
        bb += y * y;
    }
    if aa == 0.0 || bb == 0.0 {
        1.0
    } else {
        1.0 - dot / (aa.sqrt() * bb.sqrt())
    }
}

/// Asserts that the slot written from `stored` gives a range that holds the similarity of
/// `stored` to `query`, one minus their distance, and that the distance is the design's to
/// the bit.
fn the_range_holds(stored: &[f32], query: &[f32], what: &str) {
    let found = distance(stored, query);
    assert_eq!(
        found.to_bits(),
        written_out(stored, query).to_bits(),
        "{what}: the distance {found:e}"
    );
    let mut slot = vec![0_u8; slot_len(stored.len())];
    write_slot(stored, &mut slot);
    let (low, high) = Screen::new(query).range(&slot);
    let similarity = 1.0 - found;
    assert!(
        f64::from(low) <= similarity && similarity <= f64::from(high),
        "{what}: the similarity {similarity:e} is outside the range from {low:e} to {high:e}"
    );
}

#[test]
fn the_range_of_a_slot_holds_the_similarity_of_its_vector() {
    for dimension in [1, 7, 16, 33, 768] {
        // Two hundred pairs: scattered, nearly the same direction, nearly opposite, and of
        // lengths a million million apart.
        for pair in 0..200_u64 {
            let seed = dimension as u64 * 1000 + 2 * pair;
            let mut stored = vector(seed, dimension);
            let mut query = vector(seed + 1, dimension);
            let nudges = query.iter().zip(&stored);
            let nudged: Vec<f32> = nudges
                .map(|(nudge, number)| number + 0.001 * nudge)
                .collect();
            match pair % 4 {
                1 => query = nudged,
                2 => query = nudged.iter().map(|number| -number).collect(),
                3 => {
                    stored.iter_mut().for_each(|number| *number *= 1e-6);
                    query.iter_mut().for_each(|number| *number *= 1e6);
                }
                _ => {}
            }
            the_range_holds(
                &stored,
                &query,
                &format!("dimension {dimension}, pair {pair}"),
            );
        }
        // A vector of zeros, on either side, is at distance 1; and the largest numbers a
        // vector can hold.
        let zeros = vec![0.0_f32; dimension];
        let largest = vec![f32::MAX; dimension];
        let other = vector(dimension as u64 * 7919, dimension);
        for (stored, query, what) in [
            (&zeros, &other, "a stored vector of zeros"),
            (&other, &zeros, "a search for a vector of zeros"),
            (&zeros, &zeros, "zeros on both sides"),
            (&largest, &other, "a stored vector of the largest number"),
            (
                &other,
                &largest,
                "a search for a vector of the largest number",
            ),
            (&largest, &largest, "the largest number on both sides"),
            (&other, &other, "a vector and itself"),
        ] {
            the_range_holds(stored, query, &format!("dimension {dimension}, {what}"));
        }
        assert_eq!(distance(&zeros, &other), 1.0);
        assert_eq!(distance(&other, &zeros), 1.0);
    }
}

/// A key of one of several lengths: an INTEGER, or a TEXT of 1 to 40 letters.
fn key(n: i64) -> Key {
    let value = if n % 3 == 0 {
        Value::Text("k".repeat((n % 40) as usize + 1))
    } else {
        Value::Int64(n)
    };
    Key::from_values(&[value], &[]).expect("a key of one value")
}

#[test]
fn a_block_reads_back_every_slot_and_key_and_stays_within_its_bytes() {
    const DIMENSION: usize = 16;
    // Every slot and key of a block, as they were pushed.
    let reads_back = |block: &[u8], pushed: &[(Key, Vec<f32>)]| {
        let reader = BlockReader::new(block, DIMENSION);
        assert_eq!(reader.len(), pushed.len());
        for (at, (key, vector)) in pushed.iter().enumerate() {
            let mut slot = vec![0_u8; slot_len(DIMENSION)];
            write_slot(vector, &mut slot);
            assert_eq!(reader.slot(at), slot.as_slice(), "slot {at}");
            assert_eq!(reader.key(at), *key, "key {at}");
        }
    };

    // Slots are pushed for as many keys as the writer says its block takes, of more keys than
    // a block has room for.
    let offered: Vec<Key> = (0..(BLOCK_BYTES / slot_len(DIMENSION)) as i64)
        .map(key)
        .collect();
    let (mut writer, taken) = BlockWriter::new(DIMENSION, None, &offered);
    assert!(writer.is_empty());
    let mut pushed: Vec<(Key, Vec<f32>)> = Vec::new();
    let refused = loop {
        let n = pushed.len() as i64;
        let key = key(n);
        if pushed.len() == taken {
            break key;
        }
        let vector = vector(n as u64, DIMENSION);
        writer.push(&key, &vector);
        assert!(!writer.is_empty());
        pushed.push((key, vector));
    };
    let block = writer.finish();
    // A block is its count, the slots, one key end for each slot, and the keys.
    let keys: usize = pushed.iter().map(|(key, _)| key.as_bytes().len()).sum();
    assert_eq!(
        block.len(),
        4 + pushed.len() * (slot_len(DIMENSION) + 4) + keys
    );
    assert_eq!(block[..4], (pushed.len() as u32).to_le_bytes());
    // It is within the most a block may be, and the slot that was refused would have passed it.
    assert!(block.len() <= BLOCK_BYTES);
    assert!(block.len() + slot_len(DIMENSION) + 4 + refused.as_bytes().len() > BLOCK_BYTES);
    reads_back(&block, &pushed);

    // A writer that starts from a block goes on where the block ends.
    let (mut first, _) = BlockWriter::new(DIMENSION, None, &offered[..10]);
    for (key, vector) in &pushed[..10] {
        first.push(key, vector);
    }
    let first = first.finish();
    reads_back(&first, &pushed[..10]);
    let (mut second, room) = BlockWriter::new(DIMENSION, Some(&first), &offered[10..15]);
    assert!(!second.is_empty());
    for (at, (key, vector)) in pushed[10..15].iter().enumerate() {
        assert!(at < room);
        second.push(key, vector);
    }
    let second = second.finish();
    reads_back(&second, &pushed[..15]);
    let (mut at_once, _) = BlockWriter::new(DIMENSION, None, &offered[..15]);
    for (key, vector) in &pushed[..15] {
        at_once.push(key, vector);
    }
    assert!(
        second == at_once.finish(),
        "a block written in two goes is the same block"
    );

    // A writer with no slot takes any key, so that a row is never left without a block.
    let long = Key::from_values(&[Value::Text("k".repeat(BLOCK_BYTES))], &[]).expect("a key");
    assert_eq!(takes(DIMENSION, 4, &[long]).0, 1);
}

#[test]
fn a_partition_record_reads_back() {
    let partitions = [
        Partition {
            number: 0,
            live: 1,
            blocks: 0,
        },
        Partition {
            number: 7,
            live: 300,
            blocks: 2,
        },
        Partition {
            number: 0x0102_0304,
            live: 0x0506_0708_090A_0B0C,
            blocks: 0x0D0E_0F10,
        },
        Partition {
            number: u32::MAX,
            live: u64::MAX,
            blocks: u32::MAX,
        },
    ];
    for partition in partitions {
        // Sixteen bytes: the number, the live vectors and the blocks, low byte first.
        let record = partition.encode();
        let mut wanted = partition.number.to_le_bytes().to_vec();
        wanted.extend(partition.live.to_le_bytes());
        wanted.extend(partition.blocks.to_le_bytes());
        assert_eq!(record, wanted);
        assert_eq!(Partition::decode(&record), partition);
    }
}

#[test]
fn a_pending_record_of_a_row_with_no_vector_is_reported_as_damage() {
    // Which of three pending rows is left with no vector: the first, which a cycle reads to
    // find the partition, and a later one, which it reads to write a slot.
    for damaged in [1_i64, 2] {
        let folder = std::env::temp_dir().join(format!(
            "contextdb-pending-without-vector-{damaged}-{}",
            std::process::id()
        ));
        if folder.exists() {
            std::fs::remove_dir_all(&folder).expect("clear the folder an earlier run left");
        }
        // A sound store: three rows of one partition, each with a vector and each waiting on
        // the pending list.
        let db = Database::open(&folder).expect("open a fresh store");
        for sql in [
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(4))",
            "INSERT INTO docs (id, v) VALUES (1, [1.0, 0.0, 0.0, 0.0]), \
             (2, [0.0, 1.0, 0.0, 0.0]), (3, [0.0, 0.0, 1.0, 0.0])",
        ] {
            db.execute(sql, &HashMap::new()).expect(sql);
        }
        db.close().expect("close");

        // The damage: one row is written again with no vector by the store alone. The vector
        // part is not asked what that means, so the row's pending record stays.
        let budget = Budget::new(None).expect("a budget");
        let store = Store::open_for_writing(&folder, &budget, None).expect("open the store");
        let view = store.view().expect("the newest commit");
        let catalog = Catalog::load(&view).expect("the tables");
        let table = catalog.table("docs").expect("the table").id;
        let mut changes = ChangeSet::new(view, &budget).expect("a change set");
        let key = Key::from_values(&[Value::Int64(damaged)], &[]).expect("a key");
        let row = vec![Value::Int64(damaged), Value::Null];
        changes.put_row(table, key, row, false).expect("stage");
        store.commit(changes).expect("commit the damage");

        // A cycle over it is told the store is damaged, by name and naming the folder. It
        // does not panic, and it leaves nothing staged and nothing counted.
        let view = store.view().expect("the newest commit");
        let mut changes = ChangeSet::new(view, &budget).expect("a change set");
        let counted = budget.used();
        match seal(&mut changes, &catalog, &budget, 4096) {
            Err(Error::StoreCorrupted { path, .. }) => assert_eq!(path, folder),
            other => panic!(
                "with no vector in row {damaged}, which has a pending record, a cycle gave \
                 {other:?}"
            ),
        }
        assert!(changes.is_empty(), "the refused cycle left records staged");
        assert_eq!(budget.used(), counted, "memory counted after the refusal");
        drop(changes);
        drop(store);
        std::fs::remove_dir_all(&folder).expect("remove the test's folder");
    }
}

/// What a test of a wrongly named VECTOR column works on: the folder of its store, the store,
/// its budget, and the number of the table `docs`.
type Misnamed = (PathBuf, Store, Budget, TableId);

/// A store whose schema record names, as the VECTOR column of `docs`, the column `named`.
///
/// The store is written sound through the public calls: `docs` with the key `id`, an INTEGER
/// column `n` and the VECTOR column `v`, and three rows, each with a vector waiting on the
/// pending list. Then the store alone writes the table's schema record again with one byte
/// changed: the name of the column the record's VECTOR entry is for. No declaration writes such
/// a record.
fn a_vector_entry_named(test: &str, named: u8) -> Misnamed {
    let folder = std::env::temp_dir().join(format!("contextdb-{test}-{}", std::process::id()));
    if folder.exists() {
        std::fs::remove_dir_all(&folder).expect("clear the folder an earlier run left");
    }
    let db = Database::open(&folder).expect("open a fresh store");
    for sql in [
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, v VECTOR(4))",
        "INSERT INTO docs (id, n, v) VALUES (1, 10, [1.0, 0.0, 0.0, 0.0]), \
         (2, 20, [0.0, 1.0, 0.0, 0.0]), (3, 30, [0.0, 0.0, 1.0, 0.0])",
    ] {
        db.execute(sql, &HashMap::new()).expect(sql);
    }
    db.close().expect("close");

    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(&folder, &budget, None).expect("open the store");
    let view = store.view().expect("the newest commit");
    let table = Catalog::load(&view).expect("the tables");
    let table = table.table("docs").expect("the table").id;
    let mut records = view.schema_records().expect("the schema records");
    let mut record = records.pop().expect("the record of the one table");
    assert!(records.is_empty(), "one table has one schema record");
    // A schema record ends with its VECTOR entries, and the one entry here ends with its
    // column's name, a text of one byte, and then nine bytes: no PARTITION_KEY column, no
    // MAX_PARTITIONS, no SEARCH_MODE.
    let name = record.len() - 10;
    assert_eq!(
        record[name - 4..],
        [1, 0, 0, 0, b'v', 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    record[name] = named;
    let mut changes = ChangeSet::new(view, &budget).expect("a change set");
    changes.put_schema("docs", record).expect("stage");
    store.commit(changes).expect("commit the damage");
    (folder, store, budget, table)
}

/// Asserts that a call over such a store told the store's damage, by name and naming the
/// folder. `what` says which call.
fn told_as_damage<T: std::fmt::Debug>(given: crate::types::Result<T>, folder: &Path, what: &str) {
    match given {
        Err(Error::StoreCorrupted { path, .. }) => assert_eq!(path, folder, "{what}"),
        other => panic!("{what} gave {other:?}"),
    }
}

/// A commit that puts a fourth row with a vector into `docs` reaches the vector part, which is
/// asked what the row means for the column. Damage may be told when the tables are read or
/// when the vector part is asked; either way it is damage, and nothing panics.
fn a_write_is_reported_as_damage(test: &str, named: u8, what: &str) {
    let (folder, store, budget, table) = a_vector_entry_named(test, named);
    let view = store.view().expect("the newest commit");
    let mut changes = ChangeSet::new(view.clone(), &budget).expect("a change set");
    let key = Key::from_values(&[Value::Int64(4)], &[]).expect("a key");
    let numbers = Value::Vector(vec![0.0, 0.0, 0.0, 1.0]);
    let row = vec![Value::Int64(4), Value::Int64(40), numbers];
    changes.put_row(table, key, row, true).expect("stage");
    let staged = Catalog::load(&view).and_then(|catalog| stage(&mut changes, &catalog, &budget));
    told_as_damage(staged, &folder, what);
    drop(changes);
    drop((view, store));
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

/// A sealing cycle over the three pending vectors of `docs`. Damage may be told when the
/// tables are read or by the cycle; either way it is damage, nothing panics, and the cycle
/// leaves nothing staged and nothing counted.
fn a_cycle_is_reported_as_damage(test: &str, named: u8, what: &str) {
    let (folder, store, budget, _) = a_vector_entry_named(test, named);
    let view = store.view().expect("the newest commit");
    let mut changes = ChangeSet::new(view.clone(), &budget).expect("a change set");
    let counted = budget.used();
    let sealed =
        Catalog::load(&view).and_then(|catalog| seal(&mut changes, &catalog, &budget, 4096));
    told_as_damage(sealed, &folder, what);
    assert!(changes.is_empty(), "the refused cycle left records staged");
    assert_eq!(budget.used(), counted, "memory counted after the refusal");
    drop(changes);
    drop((view, store));
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_write_to_a_vector_column_that_is_no_column_of_its_table_is_reported_as_damage() {
    let what = "a commit of a row, with a VECTOR entry for the column `w`, which `docs` has not,";
    a_write_is_reported_as_damage("vector-entry-no-column-write", b'w', what);
}

#[test]
fn a_cycle_over_a_vector_column_that_is_no_column_of_its_table_is_reported_as_damage() {
    let what = "a cycle, with a VECTOR entry for the column `w`, which `docs` has not,";
    a_cycle_is_reported_as_damage("vector-entry-no-column-cycle", b'w', what);
}

#[test]
fn a_write_to_a_vector_column_that_is_not_of_the_vector_type_is_reported_as_damage() {
    let what = "a commit of a row, with a VECTOR entry for the INTEGER column `n`,";
    a_write_is_reported_as_damage("vector-entry-not-vector-write", b'n', what);
}

#[test]
fn a_cycle_over_a_vector_column_that_is_not_of_the_vector_type_is_reported_as_damage() {
    let what = "a cycle, with a VECTOR entry for the INTEGER column `n`,";
    a_cycle_is_reported_as_damage("vector-entry-not-vector-cycle", b'n', what);
}
