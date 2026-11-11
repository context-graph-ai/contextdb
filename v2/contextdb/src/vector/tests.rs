//! The pure functions of the vector part: the range a slot proves for its vector's similarity,
//! the bytes of a block, and the record of a partition. And what sealing makes of a store whose
//! records do not agree, and what a write and sealing make of a schema record whose VECTOR
//! column is not one: no public call writes such a store, so the last tests here write one
//! through the store itself.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::block::{BLOCK_BYTES, BlockReader, BlockWriter, takes};
use super::build::{Build, Phase};
use super::directory::{Grouped, Partition, partition};
use super::grouping::{
    Grouping, Place, centres, centres_to_a_record, mean, nearest as nearest_groups, put_centres,
    put_mean,
};
use super::seal::seal;
use super::sketch::{Estimate, Sketches, WIDEST, sketch_len, sketches_of, write_sketch};
use super::slot::{Screen, distance, slot_len, write_slot};
use super::stage::stage;
use super::train::{Mean, Round, first_centres, groups_for, nearest, product, sampled};
use crate::Database;
use crate::memory::Budget;
use crate::schema::Catalog;
use crate::store::{ChangeSet, Key, Read, Store, TableId, VectorId, VectorRecord, View};
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
        changes
            .put_row(table, key, row, Vec::new(), false)
            .expect("stage");
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
    // The VECTOR entry is the last thing in the record, and in it the column's name follows
    // the length of the name: the last place the record holds that length and `v`.
    let entry = [1, 0, 0, 0, b'v'];
    let at = record
        .windows(entry.len())
        .rposition(|window| window == entry)
        .expect("the VECTOR entry of the column v");
    let name = at + entry.len() - 1;
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
    changes
        .put_row(table, key, row, Vec::new(), true)
        .expect("stage");
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

// ---- the records of a grouping ----

/// The column every record of the tests below is kept for.
const COLUMN: VectorId = VectorId(3);

/// An empty store in a folder of its own.
fn a_store(test: &str) -> (PathBuf, Store, Budget) {
    let folder = std::env::temp_dir().join(format!("contextdb-{test}-{}", std::process::id()));
    if folder.exists() {
        std::fs::remove_dir_all(&folder).expect("clear the folder an earlier run left");
    }
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(&folder, &budget, None).expect("open a fresh store");
    (folder, store, budget)
}

/// Commits one record of the vector part and returns the store as of that commit.
fn put(store: &Store, budget: &Budget, record: VectorRecord<'_>, value: Vec<u8>) -> View {
    let mut changes = ChangeSet::new(store.view().expect("a view"), budget).expect("begin");
    changes.put_vector(record, value).expect("stage");
    store.commit(changes).expect("commit");
    store.view().expect("the newest commit")
}

/// A record with four of its bytes, from `at` on, set to this number.
fn with(record: &[u8], at: usize, number: u32) -> Vec<u8> {
    let mut record = record.to_vec();
    record[at..at + 4].copy_from_slice(&number.to_le_bytes());
    record
}

fn unit(vector: &[f32]) -> Vec<f32> {
    let length = vector
        .iter()
        .map(|x| f64::from(*x).powi(2))
        .sum::<f64>()
        .sqrt();
    vector
        .iter()
        .map(|x| (f64::from(*x) / length) as f32)
        .collect()
}

fn slot_of(vector: &[f32]) -> Vec<u8> {
    let mut slot = vec![0_u8; slot_len(vector.len())];
    write_slot(vector, &mut slot);
    slot
}

/// A slot written by hand: this scale, no rounding error, these codes.
fn hand_slot(scale: f32, codes: &[i8]) -> Vec<u8> {
    let mut slot = scale.to_le_bytes().to_vec();
    slot.extend(0.0_f32.to_le_bytes());
    slot.extend(codes.iter().map(|code| code.cast_unsigned()));
    slot
}

/// The vector a slot keeps: its scale times each code.
fn kept_by(slot: &[u8]) -> Vec<f32> {
    let scale = f32::from_le_bytes(std::array::from_fn(|at| slot[at]));
    let codes = slot[8..].iter();
    codes
        .map(|code| scale * f32::from(code.cast_signed()))
        .collect()
}

fn close_to(found: &[f32], wanted: &[f32], what: &str) {
    assert_eq!(found.len(), wanted.len(), "{what}");
    for (at, (found, wanted)) in found.iter().zip(wanted).enumerate() {
        assert!(
            (found - wanted).abs() < 0.01,
            "{what}, number {at}: {found:e}, not {wanted:e}"
        );
    }
}

/// A partition with every field set: three tail blocks from the third on, a grouping of 17
/// groups in force, another being built, and a mean.
fn a_partition() -> Partition {
    Partition {
        number: 7,
        live: 300,
        blocks: 6,
        first: 2,
        closed: 4,
        slots: 500,
        sealed: 9000,
        grouping: Some(Grouped {
            number: 9,
            blocks: 3,
            groups: 17,
            slots: 290,
            upto: 8500,
        }),
        building: Some(11),
        mean: true,
    }
}

#[test]
fn a_partition_record_reads_back() {
    // The 73 bytes: the number, the live rows, the tail's last, first and closed blocks, its
    // slots, the slots ever sealed, the flags, the grouping in force and the one being built.
    let whole = a_partition();
    let mut wanted = Vec::new();
    wanted.extend(7_u32.to_le_bytes());
    wanted.extend(300_u64.to_le_bytes());
    for number in [6_u32, 2, 4] {
        wanted.extend(number.to_le_bytes());
    }
    wanted.extend(500_u64.to_le_bytes());
    wanted.extend(9000_u64.to_le_bytes());
    wanted.push(1 | 2 | 4);
    for number in [9_u32, 3, 17] {
        wanted.extend(number.to_le_bytes());
    }
    wanted.extend(290_u64.to_le_bytes());
    wanted.extend(8500_u64.to_le_bytes());
    wanted.extend(11_u32.to_le_bytes());
    assert_eq!(wanted.len(), 73);
    assert_eq!(whole.encode(), wanted);

    // A field its flag does not set is zero.
    let bare = Partition {
        number: 0x0102_0304,
        live: 0x0506_0708_090A_0B0C,
        blocks: 0x0D0E_0F10,
        sealed: u64::MAX,
        ..Partition::default()
    };
    let mut wanted = vec![
        4, 3, 2, 1, 0x0C, 0x0B, 0x0A, 9, 8, 7, 6, 5, 0x10, 0x0F, 0x0E, 0x0D,
    ];
    wanted.extend([0; 16]);
    wanted.extend([0xFF; 8]);
    wanted.extend([0; 33]);
    assert_eq!(bare.encode(), wanted);

    let (folder, store, budget) = a_store("partition-record");
    for (at, entry) in [whole, bare].into_iter().enumerate() {
        let tuple = key(at as i64 + 1);
        let record = VectorRecord::Partition(COLUMN, &tuple);
        let view = put(&store, &budget, record, entry.encode());
        assert_eq!(partition(&view, COLUMN, &tuple).expect("read"), Some(entry));
        let other = key(50);
        assert_eq!(partition(&view, COLUMN, &other).expect("read"), None);
    }
    drop(store);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_partition_entry_that_disagrees_with_itself_is_damage() {
    let (folder, store, budget) = a_store("partition-damage");
    let good = a_partition().encode();
    let flags = |flags: u8| {
        let mut record = good.clone();
        record[40] = flags;
        record
    };
    let cases = [
        ("of sixteen bytes", good[..16].to_vec()),
        ("one byte longer", [good.as_slice(), &[0]].concat()),
        ("with a flag above the three", flags(15)),
        (
            "whose tail holds more slots than were ever sealed",
            with(&good, 32, 499),
        ),
        ("with a grouping of 17 groups and no mean", flags(3)),
        (
            "whose first tail block is after its closed one",
            with(&good, 16, 5),
        ),
        ("whose closed block is after its last", with(&good, 20, 7)),
        ("with a grouping of no group", with(&good, 49, 0)),
    ];
    let tuple = key(1);
    for (what, record) in cases {
        let view = put(
            &store,
            &budget,
            VectorRecord::Partition(COLUMN, &tuple),
            record,
        );
        let what = format!("a partition entry {what}");
        told_as_damage(partition(&view, COLUMN, &tuple), &folder, &what);
    }
    drop(store);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

/// What a partition entry says of the grouping of the next tests, and that grouping: three
/// groups in two blocks.
const GROUPED: Grouped = Grouped {
    number: 9,
    blocks: 2,
    groups: 3,
    slots: 500,
    upto: 500,
};

fn a_grouping() -> Grouping {
    let place = |block, slot| Place { block, slot };
    Grouping {
        gap: 780,
        probes: 2,
        width: 4,
        starts: vec![place(0, 0), place(0, 200), place(1, 50), place(1, 300)],
    }
}

#[test]
fn a_grouping_record_reads_back() {
    let grouping = a_grouping();
    // The gap, the width and the probes, then each place as its block and its slot.
    let numbers = [780_u32, 4, 2, 0, 0, 0, 200, 1, 50, 1, 300];
    let wanted: Vec<u8> = numbers.iter().flat_map(|n| n.to_le_bytes()).collect();
    assert_eq!(wanted.len(), 12 + 8 * 4);
    assert_eq!(grouping.encode(), wanted);
    let (folder, store, budget) = a_store("grouping-record");
    let view = put(&store, &budget, VectorRecord::Grouping(COLUMN, 9), wanted);
    assert_eq!(
        Grouping::read(&view, COLUMN, GROUPED).expect("read"),
        grouping
    );
    drop((view, store));
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_grouping_that_disagrees_with_its_partition_is_damage() {
    let (folder, store, budget) = a_store("grouping-damage");
    let good = a_grouping().encode();
    let view = store.view().expect("a view");
    let read = Grouping::read(&view, COLUMN, GROUPED);
    told_as_damage(read, &folder, "a grouping with no record");
    drop(view);
    let cases = [
        (
            "of the places of two groups",
            good[..good.len() - 8].to_vec(),
        ),
        ("with a gap above 20,000", with(&good, 0, 20_001)),
        ("with a width of one quarter", with(&good, 4, 1)),
        ("with a width above sixteen quarters", with(&good, 4, 17)),
        ("that scans no group", with(&good, 8, 0)),
        ("that scans more groups than it has", with(&good, 8, 4)),
        ("that does not begin at the first slot", with(&good, 16, 1)),
        (
            "with a place before the one ahead of it",
            with(&with(&good, 28, 0), 32, 100),
        ),
        ("that ends past its blocks", with(&good, 36, 5)),
    ];
    for (what, record) in cases {
        let view = put(&store, &budget, VectorRecord::Grouping(COLUMN, 9), record);
        let what = format!("a grouping {what}");
        told_as_damage(Grouping::read(&view, COLUMN, GROUPED), &folder, &what);
    }
    drop(store);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn centres_read_back_in_records_of_a_block() {
    const DIMENSION: usize = 16;
    const GROUPS: u32 = 5000;
    assert_eq!(centres_to_a_record(768), 85);
    assert_eq!(centres_to_a_record(DIMENSION), 4095);
    let all: Vec<f32> = (0..u64::from(GROUPS))
        .flat_map(|group| unit(&vector(group, DIMENSION)))
        .collect();
    let (folder, store, budget) = a_store("centres");
    let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
    put_centres(&mut changes, COLUMN, 9, DIMENSION, &all).expect("stage the centres");
    store.commit(changes).expect("commit");

    // One full record and the rest in the next.
    let view = store.view().expect("the newest commit");
    let mut records = Vec::new();
    let from = VectorRecord::Centres(COLUMN, 9, 0);
    view.vectors(from, &mut |record, value| match record {
        VectorRecord::Centres(COLUMN, 9, place) => {
            records.push((place, value.len()));
            Ok(true)
        }
        _ => Ok(false),
    })
    .expect("read the records");
    assert_eq!(records, [(0, 4095 * 64), (1, 905 * 64)]);

    let mut held = budget.reserve(0, "query").expect("a reservation");
    let read = centres(&view, COLUMN, 9, GROUPS, DIMENSION, &mut held).expect("read the centres");
    assert!(read == all, "the centres read back");
    assert_eq!(held.bytes(), 4 * DIMENSION as u64 * u64::from(GROUPS));

    // Ranked for the vector that is centre 1,234: that group first, then by product, groups
    // of one product in the order of their numbers.
    let grouped = Grouped {
        groups: GROUPS,
        ..GROUPED
    };
    let asked = &all[1234 * DIMENSION..1235 * DIMENSION];
    let ranked = nearest_groups(&view, COLUMN, grouped, asked, &mut held).expect("rank");
    assert_eq!(ranked.len(), GROUPS as usize);
    assert_eq!(ranked[0].0, 1234);
    for pair in ranked.windows(2) {
        let ((first, high), (second, low)) = (pair[0], pair[1]);
        assert!(high > low || high == low && first < second, "{pair:?}");
    }
    drop(held);
    drop((view, store));
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn centres_that_do_not_come_to_their_groups_are_damage() {
    const DIMENSION: usize = 16;
    let (folder, store, budget) = a_store("centres-damage");
    let three: Vec<f32> = (0..3)
        .flat_map(|group| unit(&vector(group, DIMENSION)))
        .collect();
    let bytes: Vec<u8> = three
        .iter()
        .flat_map(|number| number.to_le_bytes())
        .collect();
    let mut not_finite = bytes.clone();
    not_finite[4..8].copy_from_slice(&f32::NAN.to_le_bytes());
    // Each under a number of its own, read as the centres of this many groups.
    let cases = [
        ("three centres for a grouping of four", 20, 4, bytes.clone()),
        (
            "a record that is no whole number of centres",
            21,
            3,
            bytes[..bytes.len() - 1].to_vec(),
        ),
        (
            "a centre with a number that is not finite",
            22,
            3,
            not_finite,
        ),
        ("no record", 23, 3, Vec::new()),
    ];
    for (what, number, groups, record) in cases {
        let view = if record.is_empty() {
            store.view().expect("a view")
        } else {
            put(
                &store,
                &budget,
                VectorRecord::Centres(COLUMN, number, 0),
                record,
            )
        };
        let mut held = budget.reserve(0, "query").expect("a reservation");
        let read = centres(&view, COLUMN, number, groups, DIMENSION, &mut held);
        told_as_damage(read, &folder, what);
    }
    drop(store);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_mean_reads_back_and_one_of_another_length_is_damage() {
    const DIMENSION: usize = 16;
    let (folder, store, budget) = a_store("mean");
    let kept = vector(77, DIMENSION);
    let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
    put_mean(&mut changes, COLUMN, 7, &kept).expect("stage the mean");
    store.commit(changes).expect("commit");
    let view = store.view().expect("the newest commit");
    let mut held = budget.reserve(0, "query").expect("a reservation");
    let read = mean(&view, COLUMN, 7, DIMENSION, &mut held).expect("read the mean");
    assert!(read == kept, "the mean read back");
    drop(view);

    let bytes: Vec<u8> = kept
        .iter()
        .flat_map(|number| number.to_le_bytes())
        .collect();
    let mut not_finite = bytes.clone();
    not_finite[..4].copy_from_slice(&f32::INFINITY.to_le_bytes());
    let cases = [
        (
            "a mean of fifteen numbers",
            8,
            bytes[..bytes.len() - 4].to_vec(),
        ),
        ("a mean with a number that is not finite", 9, not_finite),
        ("a mean with no record", 10, Vec::new()),
    ];
    for (what, number, record) in cases {
        let view = if record.is_empty() {
            store.view().expect("a view")
        } else {
            put(&store, &budget, VectorRecord::Mean(COLUMN, number), record)
        };
        told_as_damage(
            mean(&view, COLUMN, number, DIMENSION, &mut held),
            &folder,
            what,
        );
    }
    drop(held);
    drop(store);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

// ---- finding centres ----

#[test]
fn the_groups_of_a_partition_follow_the_square_root() {
    let wanted = [
        (0, 1),
        (1, 1),
        (2, 1),
        (3, 2),
        (2000, 45),
        (100_008, 316),
        (1_000_000, 1000),
        (5_000_000, 2236),
        (u64::MAX, 65_535),
    ];
    for (slots, groups) in wanted {
        assert_eq!(groups_for(slots), groups, "{slots} slots");
    }
}

#[test]
fn the_first_centres_are_the_same_for_the_same_numbers() {
    let first = first_centres(1000, 10);
    assert_eq!(first, first_centres(1000, 10));
    assert_eq!(first.len(), 10);
    assert!(first.windows(2).all(|pair| pair[0] < pair[1]), "{first:?}");
    assert!(first[9] < 1000, "{first:?}");
    assert_eq!(first_centres(10, 10), (0..10).collect::<Vec<u64>>());
    // The sample: every slot of a small partition, and 64 a group evenly spread of a large.
    assert_eq!(
        sampled(10, 1).collect::<Vec<u64>>(),
        (0..10).collect::<Vec<u64>>()
    );
    let spread: Vec<u64> = sampled(1000, 2).collect();
    assert_eq!(spread.len(), 128);
    assert_eq!((&spread[..4], spread[127]), (&[0, 7, 15, 23][..], 992));
}

#[test]
fn a_round_moves_each_centre_to_its_slots() {
    const DIMENSION: usize = 4;
    let budget = Budget::new(None).expect("a budget");
    let mut held = budget.reserve(0, "maintenance").expect("a reservation");
    let mut found = vec![
        1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0_f32,
    ];
    let near_first = [
        slot_of(&[1.0, 0.2, 0.0, 0.0]),
        slot_of(&[1.0, -0.2, 0.0, 0.0]),
    ];
    let near_second = slot_of(&[0.0, 1.0, 0.5, 0.0]);
    // The nearest centre is the one with the greatest product, the lower number of two alike.
    assert_eq!(nearest(&found, DIMENSION, &near_first[0]).0, 0);
    let (group, greatest) = nearest(&found, DIMENSION, &near_second);
    assert_eq!(group, 1);
    assert!((greatest - 0.894).abs() < 0.01, "the product {greatest:e}");
    assert_eq!(
        product(&found[4..8], &near_second).to_bits(),
        greatest.to_bits()
    );
    assert_eq!(
        nearest(&found, DIMENSION, &slot_of(&[1.0, 1.0, 0.0, 0.0])).0,
        0
    );

    let mut round = Round::new(3, DIMENSION, &mut held).expect("a round");
    assert_eq!(held.bytes(), 8 * 4 * 3 + 4 * 3);
    for slot in near_first.iter().chain([&near_second]) {
        round.take(&found, slot);
    }
    round.finish(&mut found);
    // Each centre that drew a slot is the direction of its slots' sum; the third drew none.
    close_to(&found[..4], &[1.0, 0.0, 0.0, 0.0], "the first centre");
    close_to(&found[4..8], &[0.0, 0.894, 0.447, 0.0], "the second centre");
    assert_eq!(found[8..], [0.0, 0.0, 1.0, 0.0]);
    for centre in found.chunks(DIMENSION) {
        let length: f32 = centre.iter().map(|x| x * x).sum();
        assert!(
            (length - 1.0).abs() < 1e-5,
            "a centre of squared length {length:e}"
        );
    }

    // The mean is not brought to length one, and is zeros when nothing was taken.
    let mut middle = Mean::new(DIMENSION, &mut held).expect("a mean");
    middle.take(&near_first[0]);
    middle.take(&near_second);
    close_to(&middle.finish(), &[0.490, 0.545, 0.224, 0.0], "the mean");
    let nothing = Mean::new(DIMENSION, &mut held).expect("a mean");
    assert_eq!(nothing.finish(), [0.0; DIMENSION]);
}

// ---- the record of a build ----

/// A build of 45 groups over the one tail block of 2,000 slots, at this phase.
fn a_build(phase: Phase) -> Build {
    Build {
        number: 12,
        steps: 5,
        from: None,
        tail: (0, 1),
        slots: 2000,
        upto: 2000,
        stale: false,
        groups: 45,
        phase,
        counts: vec![0; 45],
        starts: Vec::new(),
        tens: Vec::new(),
        needing: 0,
    }
}

/// The slots each group was given, and where each group then begins in the one new block.
fn given() -> (Vec<u32>, Vec<Place>) {
    let mut counts = vec![44_u32; 45];
    counts[0] = 64;
    let mut starts = vec![Place::default()];
    for count in &counts {
        let slot = starts[starts.len() - 1].slot + count;
        starts.push(Place { block: 0, slot });
    }
    (counts, starts)
}

#[test]
fn a_build_record_reads_back() {
    let (counts, starts) = given();
    let tens = |questions: usize| vec![Place { block: 0, slot: 7 }; 10 * questions];
    let packed = |phase: Phase, questions: usize| Build {
        counts: counts.clone(),
        starts: starts.clone(),
        tens: tens(questions),
        ..a_build(phase)
    };
    let builds = [
        a_build(Phase::Train { round: 3 }),
        Build {
            from: Some(Grouped {
                number: 9,
                blocks: 1,
                groups: 40,
                slots: 1600,
                upto: 1600,
            }),
            tail: (2, 3),
            stale: true,
            needing: 4096,
            ..a_build(Phase::Assign { next: 1 })
        },
        Build {
            counts: counts.clone(),
            starts: starts[..3].to_vec(),
            ..a_build(Phase::Pack { group: 2 })
        },
        packed(Phase::Measure { done: 2, most: 150 }, 2),
        packed(
            Phase::Width {
                gap: 150,
                probes: 3,
                done: 1,
                asked: 10,
                kept: [9; 15],
            },
            128,
        ),
        packed(
            Phase::Switch {
                gap: 150,
                probes: 3,
                width: 4,
            },
            128,
        ),
        Build {
            stale: true,
            ..packed(Phase::Sweep, 0)
        },
    ];
    let (folder, store, budget) = a_store("build-record");
    let view = store.view().expect("a view");
    assert_eq!(Build::read(&view, COLUMN, 7).expect("read"), None);
    drop(view);
    for build in builds {
        let view = put(
            &store,
            &budget,
            VectorRecord::Build(COLUMN, 7),
            build.encode(),
        );
        let read = Build::read(&view, COLUMN, 7).expect("read the build");
        assert_eq!(read, Some(build));
    }
    drop(store);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_build_whose_cursor_is_past_its_blocks_is_damage() {
    let (counts, _) = given();
    let whole = a_build(Phase::Train { round: 3 }).encode();
    let cases = [
        (
            "a build that gives groups from a block it does not read",
            a_build(Phase::Assign { next: 5 }).encode(),
        ),
        (
            "a build that packs a group it does not have",
            Build {
                counts,
                ..a_build(Phase::Pack { group: 46 })
            }
            .encode(),
        ),
        (
            "a build's record cut short",
            whole[..whole.len() - 1].to_vec(),
        ),
    ];
    let (folder, store, budget) = a_store("build-damage");
    for (what, record) in cases {
        let view = put(&store, &budget, VectorRecord::Build(COLUMN, 7), record);
        told_as_damage(Build::read(&view, COLUMN, 7), &folder, what);
    }
    drop(store);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

// ---- sketches ----

#[test]
fn a_sketch_holds_the_signs_and_the_factor_of_its_slot() {
    // Sixteen numbers worked out by hand. The slot keeps an eighth of each code; the
    // reference is a half for the first eight numbers and zero for the rest.
    let codes = [8, -8, 4, -4, 2, -2, 1, -1, 0, 0, 8, 8, -8, -8, 4, 4];
    let slot = hand_slot(0.125, &codes);
    let reference = [[0.5_f32; 8], [0.0; 8]].concat();
    // The differences are 0.5, -1.5, 0, -1, -0.25, -0.75, -0.375, -0.625, then 0, 0, 1, 1,
    // -1, -1, 0.5, 0.5: bits 0 and 2 of the first byte, and all but bits 4 and 5 of the
    // second. Their squares come to 9.15625 and their sizes to 10: the factor is 30,003.
    assert_eq!(sketch_len(16), 4);
    let mut sketch = [0xAA_u8; 4];
    write_sketch(&slot, &reference, &mut sketch);
    assert_eq!(sketch, [0b0000_0101, 0b1100_1111, 0x33, 0x75]);

    // No difference at all: every bit, and a factor of zero.
    write_sketch(&slot, &kept_by(&slot), &mut sketch);
    assert_eq!(sketch, [0xFF, 0xFF, 0, 0]);

    // Twelve numbers: the four bits past them stay zero. Differences of one give a factor
    // of 32,768, and differences of two one that is held to 65,535.
    assert_eq!(sketch_len(12), 4);
    for (code, factor) in [(8_i8, 32_768_u16), (16, u16::MAX)] {
        let mut sketch = [0xAA_u8; 4];
        write_sketch(&hand_slot(0.125, &[code; 12]), &[0.0; 12], &mut sketch);
        let [low, high] = factor.to_le_bytes();
        assert_eq!(sketch, [0xFF, 0x0F, low, high]);
    }
}

/// The vector of a search at length one, as numbers of 32 bits.
fn asked_unit(query: &[f32]) -> Vec<f32> {
    unit(query)
}

/// The record of the sketches of these slots, each from this reference.
fn record_of(slots: &[Vec<u8>], reference: &[f32]) -> Vec<u8> {
    let mut record = (slots.len() as u32).to_le_bytes().to_vec();
    for slot in slots {
        let mut sketch = vec![0_u8; sketch_len(reference.len())];
        write_sketch(slot, reference, &mut sketch);
        record.extend(sketch);
    }
    record
}

#[test]
fn a_sketch_estimates_a_product_within_its_factor() {
    const DIMENSION: usize = 16;
    for pair in 0..20_u64 {
        let slot = slot_of(&vector(3 * pair, DIMENSION));
        let reference: Vec<f32> = vector(3 * pair + 1, DIMENSION)
            .iter()
            .map(|number| 0.1 * number)
            .collect();
        let query = vector(3 * pair + 2, DIMENSION);
        let asked = asked_unit(&query);
        let dot = |a: &[f32], b: &[f32]| -> f64 {
            a.iter()
                .zip(b)
                .map(|(x, y)| f64::from(*x) * f64::from(*y))
                .sum()
        };
        let truth = dot(&kept_by(&slot), &asked);
        let base = dot(&reference, &asked) as f32;

        let record = record_of(std::slice::from_ref(&slot), &reference);
        let factor = f64::from(u16::from_le_bytes([record[6], record[7]])) / 32_768.0;
        let mut ranges = Vec::new();
        let sketches = Sketches::new(&record, DIMENSION);
        Estimate::new(&query).ranges(&sketches, 0..1, base, WIDEST, &mut ranges);
        let [(low, high)] = ranges[..] else {
            panic!("pair {pair}: {} ranges for one sketch", ranges.len())
        };
        // At the widest, a range is four factors to each side of the estimate.
        let (low, high) = (f64::from(low), f64::from(high));
        assert!(
            ((high - low) - 8.0 * factor).abs() < 1e-4,
            "pair {pair}: a range from {low:e} to {high:e} with the factor {factor:e}"
        );
        assert!(
            low - 1e-5 <= truth && truth <= high + 1e-5,
            "pair {pair}: the product {truth:e} is outside the range from {low:e} to {high:e}"
        );
    }
}

#[test]
fn the_ranges_of_a_run_of_sketches_are_those_of_the_plain_arithmetic() {
    // A hundred numbers: a sketch's second word is completed with zeros.
    const DIMENSION: usize = 100;
    const WIDTH: u8 = 6;
    let reference: Vec<f32> = vector(500, DIMENSION).iter().map(|x| 0.1 * x).collect();
    let slots: Vec<Vec<u8>> = (0..40)
        .map(|n| slot_of(&vector(600 + n, DIMENSION)))
        .collect();
    let record = record_of(&slots, &reference);
    let query = vector(700, DIMENSION);
    let base = 0.125_f32;
    let mut ranges = Vec::new();
    let sketches = Sketches::new(&record, DIMENSION);
    Estimate::new(&query).ranges(&sketches, 3..37, base, WIDTH, &mut ranges);
    assert_eq!(ranges.len(), 34);

    // The same worked out bit by bit: the search's vector at length one in whole steps of a
    // seventh of its largest number, and for each sketch the steps under its set bits.
    let asked = asked_unit(&query);
    let step = asked.iter().fold(0.0_f32, |most, x| most.max(x.abs())) / 7.0;
    let steps: Vec<i32> = asked
        .iter()
        .map(|x| ((x / step).round() as i32).clamp(-7, 7))
        .collect();
    let sum: i32 = steps.iter().sum();
    for (place, (low, high)) in (3..37).zip(ranges) {
        let sketch = sketches.at(place);
        let set = |at: usize| sketch[at / 8] >> (at % 8) & 1 == 1;
        let under: i32 = (0..DIMENSION)
            .filter(|at| set(*at))
            .map(|at| steps[at])
            .sum();
        let factor = f32::from(u16::from_le_bytes([sketch[13], sketch[14]])) / 32_768.0;
        let estimate = base + factor * step * (2 * under - sum) as f32;
        let wide = f32::from(WIDTH) / 4.0 * factor;
        for (found, wanted) in [(low, estimate - wide), (high, estimate + wide)] {
            assert!(
                (found - wanted).abs() <= 1e-5 * wanted.abs().max(1.0),
                "sketch {place}: {found:e} where the plain arithmetic gives {wanted:e}"
            );
        }
    }
}

#[test]
fn the_sketches_of_a_block_read_back() {
    const DIMENSION: usize = 16;
    let keys: Vec<Key> = (0..5).map(key).collect();
    let (mut writer, taken) = BlockWriter::new(DIMENSION, None, &keys);
    assert_eq!(taken, 5);
    for (n, key) in keys.iter().enumerate() {
        writer.push(key, &vector(800 + n as u64, DIMENSION));
    }
    let block = writer.finish();
    // Slot `place` is sketched from reference `place % 2`.
    let references = [vector(900, DIMENSION), vector(901, DIMENSION)];
    let record = sketches_of(&block, DIMENSION, &mut |place| &references[place % 2]);
    assert_eq!(record.len(), 4 + 5 * sketch_len(DIMENSION));
    assert_eq!(record[..4], 5_u32.to_le_bytes());
    let sketches = Sketches::new(&record, DIMENSION);
    assert!(sketches.is_whole(5) && !sketches.is_whole(4) && !sketches.is_whole(6));
    assert!(!Sketches::new(&record[..record.len() - 1], DIMENSION).is_whole(5));
    let reader = BlockReader::new(&block, DIMENSION);
    for place in 0..5 {
        let mut wanted = vec![0_u8; sketch_len(DIMENSION)];
        write_sketch(reader.slot(place), &references[place % 2], &mut wanted);
        assert_eq!(sketches.at(place), wanted.as_slice(), "sketch {place}");
    }
}

// ---- damage in a built store, through the public calls ----

const BUILT: usize = 16;

/// Row `n` of a built store: one of eight directions at right angles, moved a little.
fn gathered(n: u64) -> Vec<f32> {
    let moved = vector(n, BUILT);
    let direction = (n % 8) as usize;
    let lifted = |at: usize| f32::from(u8::from(at / 2 == direction));
    (0..BUILT).map(|at| lifted(at) + 0.05 * moved[at]).collect()
}

fn fresh(test: &str) -> PathBuf {
    let folder = std::env::temp_dir().join(format!("contextdb-{test}-{}", std::process::id()));
    if folder.exists() {
        std::fs::remove_dir_all(&folder).expect("clear the folder an earlier run left");
    }
    folder
}

fn insert_gathered(db: &Database, ids: std::ops::RangeInclusive<u64>) {
    let tx = db.transaction().expect("begin");
    for id in ids {
        tx.execute(
            "INSERT INTO docs (id, v) VALUES ($id, $v)",
            &crate::params! { "id" => id as i64, "v" => gathered(id) },
        )
        .expect("insert a row");
    }
    tx.commit().expect("commit");
}

/// An open store whose `docs.v` holds 2,000 vectors in a column that is at its size, after
/// this many maintenance cycles: sixteen build its grouping of 45 groups.
fn a_store_with_vectors(test: &str, cycles: usize) -> (PathBuf, Database) {
    let folder = fresh(test);
    let db = Database::open(&folder).expect("open a fresh store");
    let table = "CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(16) AUTO_INDEX_AT 1000)";
    db.execute(table, &HashMap::new()).expect(table);
    insert_gathered(&db, 1..=2000);
    for _ in 0..cycles {
        db.run_maintenance_cycle().expect("a maintenance cycle");
    }
    (folder, db)
}

/// The first record of the kind of `from` that the column of `from` keeps, with its value.
fn first_record(view: &View, from: VectorRecord<'static>) -> (VectorRecord<'static>, Vec<u8>) {
    let mut found = None;
    view.vectors(from, &mut |record, value| {
        found = Some(match record {
            VectorRecord::Grouping(column, number) => VectorRecord::Grouping(column, number),
            VectorRecord::Sketch(column, under, block) => {
                VectorRecord::Sketch(column, under, block)
            }
            VectorRecord::Assigned(column, number, place) => {
                VectorRecord::Assigned(column, number, place)
            }
            other => panic!("looked for {from:?} and the next record is {other:?}"),
        })
        .map(|record| (record, value.to_vec()));
        Ok(false)
    })
    .expect("read the records");
    let (record, value) = found.unwrap_or_else(|| panic!("no record from {from:?} on"));
    assert_eq!(
        std::mem::discriminant(&record),
        std::mem::discriminant(&from),
        "the first record from {from:?} on"
    );
    (record, value)
}

/// Closes the store, changes the first record from `from` on with `damage`, and opens it.
fn damaged(
    folder: &Path,
    db: Database,
    from: fn(VectorId) -> VectorRecord<'static>,
    damage: fn(&mut Vec<u8>),
) -> Database {
    db.close().expect("close");
    drop(db);
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, None).expect("open the store");
    let view = store.view().expect("the newest commit");
    let catalog = Catalog::load(&view).expect("the tables");
    let column = catalog.table("docs").expect("the table").vectors[0].id;
    let (record, mut value) = first_record(&view, from(column));
    damage(&mut value);
    drop(view);
    put(&store, &budget, record, value);
    drop(store);
    Database::open(folder).expect("open the damaged store")
}

/// The ids a search of ten returns under this mode.
fn ten(db: &Database, mode: &str) -> crate::types::Result<Vec<Value>> {
    let sql = format!(
        "SELECT id, v <=> $q AS distance FROM docs ORDER BY distance USE VECTOR {mode} LIMIT 10"
    );
    let result = db.execute(&sql, &crate::params! { "q" => gathered(40_003) })?;
    Ok(result.rows().iter().map(|row| row[0].clone()).collect())
}

/// Damages the index of a built store and asserts what its searches then give: INDEXED the
/// store's damage error, AUTO and EXACT the same ten rows.
fn a_damaged_index_refuses_indexed_and_auto_answers_exactly(
    test: &str,
    from: fn(VectorId) -> VectorRecord<'static>,
    damage: fn(&mut Vec<u8>),
    what: &str,
) {
    let (folder, db) = a_store_with_vectors(test, 16);
    assert_eq!(ten(&db, "INDEXED").expect("the built store").len(), 10);
    let db = damaged(&folder, db, from, damage);
    told_as_damage(ten(&db, "INDEXED"), &folder, what);
    let exact = ten(&db, "EXACT").expect("EXACT reads no record of the index");
    assert_eq!(exact.len(), 10);
    match ten(&db, "AUTO") {
        Ok(auto) => assert_eq!(auto, exact, "AUTO over {what}"),
        Err(error) => panic!("AUTO over {what} gave {error:?}"),
    }
    db.close().expect("close");
    drop(db);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_sketch_record_that_does_not_match_its_block_is_damage() {
    // The record of the one block's sketches, holding one sketch fewer than the block has
    // slots, and saying so.
    a_damaged_index_refuses_indexed_and_auto_answers_exactly(
        "sketch-count",
        |column| VectorRecord::Sketch(column, 0, 0),
        |record| {
            let count = u32::from_le_bytes(std::array::from_fn(|at| record[at])) - 1;
            record[..4].copy_from_slice(&count.to_le_bytes());
            record.truncate(record.len() - sketch_len(BUILT));
        },
        "a record of sketches whose count is not its block's",
    );
}

#[test]
fn auto_answers_exactly_where_a_grouping_is_damaged() {
    a_damaged_index_refuses_indexed_and_auto_answers_exactly(
        "grouping-gap",
        |column| VectorRecord::Grouping(column, 0),
        |record| record[..4].copy_from_slice(&20_001_u32.to_le_bytes()),
        "a grouping whose gap is above 20,000",
    );
}

#[test]
fn assigned_groups_that_do_not_match_their_block_are_damage() {
    // Twelve cycles: sealed and begun, ten rounds, and every slot given its group.
    let (folder, db) = a_store_with_vectors("assigned-count", 12);
    let db = damaged(
        &folder,
        db,
        |column| VectorRecord::Assigned(column, 0, 0),
        |record| record.extend([0, 0]),
    );
    told_as_damage(
        db.run_maintenance_cycle(),
        &folder,
        "a cycle that packs from the groups of one slot more than the block has",
    );
    db.close().expect("close");
    drop(db);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

/// Closes the store and opens it as these tests write to it: the store, its budget, the
/// column of `docs.v` and the entry of its one partition.
fn beneath(folder: &Path, db: Database) -> (Store, Budget, VectorId, Partition) {
    db.close().expect("close");
    drop(db);
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, None).expect("open the store");
    let view = store.view().expect("the newest commit");
    let catalog = Catalog::load(&view).expect("the tables");
    let column = catalog.table("docs").expect("the table").vectors[0].id;
    let none = Key::from_values(&[], &[]).expect("the key of no values");
    let entry = partition(&view, column, &none).expect("read the partition");
    drop(view);
    (store, budget, column, entry.expect("the one partition"))
}

/// The build the one partition of a store is making, read as [`beneath`] opens the store.
fn build_under_way(store: &Store, column: VectorId, entry: &Partition) -> Build {
    let view = store.view().expect("the newest commit");
    let build = Build::read(&view, column, entry.number).expect("read the build");
    build.expect("a build under way")
}

/// Puts this record of a build in place of the partition's, opens the store, and asserts that
/// the next cycle tells the damage.
fn a_cycle_over_this_build_is_damage(test: &str, cycles: usize, damage: fn(Build) -> Build) {
    let (folder, db) = a_store_with_vectors(test, cycles);
    let (store, budget, column, entry) = beneath(&folder, db);
    let build = damage(build_under_way(&store, column, &entry));
    let record = VectorRecord::Build(column, entry.number);
    put(&store, &budget, record, build.encode());
    drop(store);
    let db = Database::open(&folder).expect("open the damaged store");
    told_as_damage(db.run_maintenance_cycle(), &folder, test);
    db.close().expect("close");
    drop(db);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_build_of_more_groups_than_slots_is_damage() {
    // One cycle: sealed and begun. The record then says 2,001 groups for its 2,000 slots.
    a_cycle_over_this_build_is_damage("build-more-groups-than-slots", 1, |build| {
        assert_eq!((build.slots, build.groups), (2000, 45));
        Build {
            groups: 2001,
            counts: vec![0; 2001],
            ..build
        }
    });
}

#[test]
fn a_packed_build_whose_groups_begin_out_of_order_is_damage() {
    // Thirteen cycles: every group is packed and its beginning recorded. The second group
    // then begins after the third.
    a_cycle_over_this_build_is_damage("build-starts-out-of-order", 13, |mut build| {
        assert_eq!(build.starts.len(), 46);
        assert!(build.starts[1] < build.starts[2], "{:?}", build.starts);
        build.starts.swap(1, 2);
        build
    });
}

#[test]
fn assigned_groups_of_an_odd_length_are_damage() {
    // Twelve cycles: sealed and begun, ten rounds, and every slot given its group in two
    // bytes. The record then ends half way through a slot's.
    let (folder, db) = a_store_with_vectors("assigned-odd", 12);
    let db = damaged(
        &folder,
        db,
        |column| VectorRecord::Assigned(column, 0, 0),
        |record| record.push(0),
    );
    told_as_damage(
        db.run_maintenance_cycle(),
        &folder,
        "a cycle that packs from groups recorded in an odd number of bytes",
    );
    db.close().expect("close");
    drop(db);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

/// A store whose grouping is being built again from slots one of which its row has left, so
/// that the partition keeps one `Left` record. Once the build has reached `sweeping` (or at
/// once, for `false`) that record's value is made `bytes` long, and the cycles that follow
/// must tell the damage: the build reads the record for the slot's key before it gives the
/// slot a group, and the steps after the switch read it to remove it.
fn a_left_record_of_this_length_is_damage(test: &str, sweeping: bool, bytes: usize) {
    let (folder, mut db) = a_store_with_vectors(test, 16);
    insert_gathered(&db, 2001..=2400);
    let changed = crate::params! { "v" => gathered(50_000) };
    db.execute("UPDATE docs SET v = $v WHERE id = 5", &changed)
        .expect("change a vector");
    db.run_maintenance_cycle().expect("the cycle that begins");
    let none = Key::from_values(&[], &[]).expect("the key of no values");
    for cycle in 0.. {
        let (store, budget, column, entry) = beneath(&folder, db);
        let phase = build_under_way(&store, column, &entry).phase;
        if sweeping && phase != Phase::Sweep {
            assert!(cycle < 40, "40 cycles and the build is at {phase:?}");
            drop(store);
            db = Database::open(&folder).expect("open");
            db.run_maintenance_cycle().expect("a cycle of the build");
            continue;
        }
        let view = store.view().expect("the newest commit");
        let mut left = None;
        view.vectors(
            VectorRecord::Left(column, entry.number, &none),
            &mut |record, value| {
                if let VectorRecord::Left(_, _, key) = record {
                    left = Some((key.clone(), value.to_vec()));
                }
                Ok(false)
            },
        )
        .expect("read the records");
        drop(view);
        let (key, mut value) = left.expect("a record of the row that left its slot");
        assert_eq!(value.len(), 8);
        value.resize(bytes, 0);
        put(
            &store,
            &budget,
            VectorRecord::Left(column, entry.number, &key),
            value,
        );
        drop(store);
        db = Database::open(&folder).expect("open the damaged store");
        break;
    }
    let told = (0..40).find_map(|_| db.run_maintenance_cycle().err());
    told_as_damage(told.map_or(Ok(()), Err), &folder, test);
    db.close().expect("close");
    drop(db);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_left_record_that_is_not_eight_bytes_is_damage() {
    a_left_record_of_this_length_is_damage("left-short-while-building", false, 7);
    a_left_record_of_this_length_is_damage("left-short-while-sweeping", true, 7);
    a_left_record_of_this_length_is_damage("left-long-while-sweeping", true, 9);
}

/// How many records of the kind of `from` the column of `from` keeps, from `from` on.
fn records_from(view: &View, from: VectorRecord<'_>) -> usize {
    let column = |record: VectorRecord<'_>| match record {
        VectorRecord::Partition(column, _)
        | VectorRecord::Pending(column, ..)
        | VectorRecord::Block(column, ..)
        | VectorRecord::Grouping(column, _)
        | VectorRecord::Centres(column, ..)
        | VectorRecord::Build(column, _)
        | VectorRecord::Assigned(column, ..)
        | VectorRecord::Sketch(column, ..)
        | VectorRecord::Mean(column, _)
        | VectorRecord::Left(column, ..) => column,
    };
    let mut count = 0;
    view.vectors(from, &mut |record, _| {
        let same = std::mem::discriminant(&record) == std::mem::discriminant(&from)
            && column(record) == column(from);
        count += usize::from(same);
        Ok(same)
    })
    .expect("read the records");
    count
}

#[test]
fn a_partition_that_empties_leaves_no_record_of_its_grouping_or_its_build() {
    // A grouping in force; then a fifth more vectors sealed into its tail, a row that left
    // its slot, and the regrouping those make due begun.
    let (folder, db) = a_store_with_vectors("emptied-partition", 16);
    insert_gathered(&db, 2001..=2400);
    let changed = crate::params! { "v" => gathered(50_000) };
    db.execute("UPDATE docs SET v = $v WHERE id = 5", &changed)
        .expect("change a vector");
    db.run_maintenance_cycle().expect("a maintenance cycle");
    // Every kind of record the column may keep, and how many of each it keeps at a moment.
    let counts = |folder: &Path| -> Vec<usize> {
        let budget = Budget::new(None).expect("a budget");
        let store = Store::open_for_writing(folder, &budget, None).expect("open the store");
        let view = store.view().expect("the newest commit");
        let catalog = Catalog::load(&view).expect("the tables");
        let column = catalog.table("docs").expect("the table").vectors[0].id;
        let none = Key::from_values(&[], &[]).expect("the key of no values");
        let kinds = [
            VectorRecord::Partition(column, &none),
            VectorRecord::Pending(column, 0, &none),
            VectorRecord::Block(column, 0, 0),
            VectorRecord::Grouping(column, 0),
            VectorRecord::Centres(column, 0, 0),
            VectorRecord::Build(column, 0),
            VectorRecord::Assigned(column, 0, 0),
            VectorRecord::Sketch(column, 0, 0),
            VectorRecord::Mean(column, 0),
            VectorRecord::Left(column, 0, &none),
        ];
        kinds.map(|from| records_from(&view, from)).to_vec()
    };
    db.close().expect("close");
    drop(db);
    let before = counts(&folder);
    for (kind, count) in [
        "partition",
        "pending",
        "block",
        "grouping",
        "centres",
        "build",
    ]
    .iter()
    .zip(&before)
    {
        // Nothing is pending after the cycle; of every other kind there is a record to lose.
        assert_eq!(
            *count == 0,
            *kind == "pending",
            "{kind} records before: {before:?}"
        );
    }
    assert!(
        before[7] > 0 && before[8] == 1 && before[9] == 1,
        "{before:?}"
    );

    let db = Database::open(&folder).expect("open");
    db.execute("DELETE FROM docs WHERE id > 0", &HashMap::new())
        .expect("delete every row");
    db.close().expect("close");
    drop(db);
    assert_eq!(
        counts(&folder),
        [0; 10],
        "records left by the emptied partition"
    );
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}
