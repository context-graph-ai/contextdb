//! The pure functions of the vector part: the range a slot proves for its vector's similarity,
//! the bytes of a block, and the record of a partition. And what sealing makes of a store whose
//! records do not agree, and what a write and sealing make of a schema record whose VECTOR
//! column is not one: no public call writes such a store, so the last tests here write one
//! through the store itself.

#[path = "../../tests/support/report.rs"]
pub(crate) mod report;
use report::count;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::binary::{Codes, Estimate, WIDEST, code_len, write_code};
use super::block::{BLOCK_BYTES, BlockReader, BlockWriter, takes};
use super::build::{Build, Phase, unindexed_work};
use super::directory::{Grouped, Partition, partition};
use super::ivf::{
    Ivf, Place, centroids, centroids_per_record, mean, nearest as nearest_groups, put_centroids,
    put_mean,
};
use super::seal::seal;
use super::slot::{Screen, distance, slot_len, write_slot};
use super::train::{Mean, Round, first_centroids, groups_for, nearest, product, sampled};
use super::unindexed::track_unindexed;
use crate::memory::{Budget, Reservation};
use crate::schema::Catalog;
use crate::store::{Cell, ChangeSet, Key, Read, Store, VectorId, VectorRecord, View};
use crate::types::{Error, Value};
use crate::{Database, DatabaseOpenOptions, Maintenance, StorageBackend, WrapStorage};

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

// ---- the records of an IVF ----

/// The column every record of the tests below is kept for.
const COLUMN: VectorId = VectorId(3);

/// An empty store in a folder of its own.
fn a_store(test: &str) -> (PathBuf, Store, Budget) {
    let folder = fresh(test);
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(&folder, &budget, &DatabaseOpenOptions::default())
        .expect("open a fresh store");
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
        .sqrt()
        .max(f64::MIN_POSITIVE);
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

/// A partition with every field set: three tail blocks from the third on, an IVF of 17
/// groups in force, another being built, and 23 outside slots assigned.
fn a_partition() -> Partition {
    Partition {
        number: 7,
        live: 300,
        unindexed: 0,
        pending_bytes: 0,
        tail_room: 0,
        blocks: 6,
        first: 2,
        closed: 4,
        slots: 500,
        sealed: 9000,
        ivf: Some(Grouped {
            number: 9,
            blocks: 3,
            groups: 17,
            slots: 290,
            upto: 8500,
        }),
        building: Some((11, 0)),
        assigned: 23,
    }
}

/// What a partition entry says of the IVF of the next tests, and that IVF: three
/// groups in two blocks.
const GROUPED: Grouped = Grouped {
    number: 9,
    blocks: 2,
    groups: 3,
    slots: 500,
    upto: 500,
};

fn an_ivf() -> Ivf {
    let place = |block, slot| Place { block, slot };
    Ivf {
        gap: 780,
        probes: 2,
        width: 4,
        starts: vec![place(0, 0), place(0, 200), place(1, 50), place(1, 300)],
    }
}

#[test]
fn centroids_read_back_in_records_of_a_block() {
    const DIMENSION: usize = 16;
    const GROUPS: u32 = 5000;
    assert_eq!(centroids_per_record(768), 85);
    assert_eq!(centroids_per_record(DIMENSION), 4095);
    let all: Vec<f32> = (0..u64::from(GROUPS))
        .flat_map(|group| unit(&vector(group, DIMENSION)))
        .collect();
    let (folder, store, budget) = a_store("centroids");
    let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
    put_centroids(&mut changes, COLUMN, 9, DIMENSION, &all).expect("stage the centroids");
    store.commit(changes).expect("commit");

    // One full record and the rest in the next.
    let view = store.view().expect("the newest commit");
    let mut records = Vec::new();
    let from = VectorRecord::Centroids(COLUMN, 9, 0);
    view.vectors(from, &mut |record, value| match record {
        VectorRecord::Centroids(COLUMN, 9, place) => {
            records.push((place, value.len()));
            Ok(true)
        }
        _ => Ok(false),
    })
    .expect("read the records");
    assert_eq!(records, [(0, 4095 * 64), (1, 905 * 64)]);

    let mut held = budget.reserve(0, "query").expect("a reservation");
    let read = centroids(&view, COLUMN, 9, GROUPS, DIMENSION, &mut held).expect("read centroids");
    assert!(read == all, "the centroids read back");
    assert_eq!(held.bytes(), 4 * DIMENSION as u64 * u64::from(GROUPS));

    // Ranked for the vector that is centroid 1,234: that group first, then by product, groups
    // of one product in the order of their numbers.
    let grouped = Grouped {
        groups: GROUPS,
        ..GROUPED
    };
    let asked = &all[1234 * DIMENSION..1235 * DIMENSION];
    let ranked = nearest_groups(&view, COLUMN, grouped, asked, &mut held, None).expect("rank");
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

// ---- finding centroids ----

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
fn the_first_centroids_are_the_same_for_the_same_numbers() {
    let first = first_centroids(1000, 10);
    assert_eq!(first, first_centroids(1000, 10));
    assert_eq!(first.len(), 10);
    assert!(first.windows(2).all(|pair| pair[0] < pair[1]), "{first:?}");
    assert!(first[9] < 1000, "{first:?}");
    assert_eq!(first_centroids(10, 10), (0..10).collect::<Vec<u64>>());
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
fn a_round_moves_each_centroid_to_its_slots() {
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
    // The nearest centroid is the one with the greatest product, the lower number of two alike.
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
    // Each centroid that drew a slot is the direction of its slots' sum; the third drew none.
    close_to(&found[..4], &[1.0, 0.0, 0.0, 0.0], "the first centroid");
    close_to(&found[4..8], &[0.0, 0.894, 0.447, 0.0], "second centroid");
    assert_eq!(found[8..], [0.0, 0.0, 1.0, 0.0]);
    for centroid in found.chunks(DIMENSION) {
        let length: f32 = centroid.iter().map(|x| x * x).sum();
        assert!(
            (length - 1.0).abs() < 1e-5,
            "a centroid of squared length {length:e}"
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
        reusing: false,
        outside: 0,
        sum: Vec::new(),
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

// ---- binary codes ----

#[test]
fn a_code_holds_the_signs_and_the_factor_of_its_slot() {
    // Sixteen numbers worked out by hand. The slot keeps an eighth of each code; the
    // reference is a half for the first eight numbers and zero for the rest.
    let codes = [8, -8, 4, -4, 2, -2, 1, -1, 0, 0, 8, 8, -8, -8, 4, 4];
    let slot = hand_slot(0.125, &codes);
    let reference = [[0.5_f32; 8], [0.0; 8]].concat();
    // The differences are 0.5, -1.5, 0, -1, -0.25, -0.75, -0.375, -0.625, then 0, 0, 1, 1,
    // -1, -1, 0.5, 0.5: bits 0 and 2 of the first byte, and all but bits 4 and 5 of the
    // second. Their squares come to 9.15625 and their sizes to 10: the factor is 30,003.
    assert_eq!(code_len(16), 4);
    let mut code = [0xAA_u8; 4];
    write_code(&slot, &reference, &mut code);
    assert_eq!(code, [0b0000_0101, 0b1100_1111, 0x33, 0x75]);

    // No difference at all: every bit, and a factor of zero.
    write_code(&slot, &kept_by(&slot), &mut code);
    assert_eq!(code, [0xFF, 0xFF, 0, 0]);

    // Twelve numbers: the four bits past them stay zero. Differences of one give a factor
    // of 32,768, and differences of two one that is held to 65,535.
    assert_eq!(code_len(12), 4);
    for (code, factor) in [(8_i8, 32_768_u16), (16, u16::MAX)] {
        let mut binary = [0xAA_u8; 4];
        write_code(&hand_slot(0.125, &[code; 12]), &[0.0; 12], &mut binary);
        let [low, high] = factor.to_le_bytes();
        assert_eq!(binary, [0xFF, 0x0F, low, high]);
    }
}

/// The vector of a search at length one, as numbers of 32 bits.
fn asked_unit(query: &[f32]) -> Vec<f32> {
    unit(query)
}

/// The record of the binary codes of these slots, each from this reference.
fn record_of(slots: &[Vec<u8>], reference: &[f32]) -> Vec<u8> {
    let mut record = (slots.len() as u32).to_le_bytes().to_vec();
    for slot in slots {
        let mut code = vec![0_u8; code_len(reference.len())];
        write_code(slot, reference, &mut code);
        record.extend(code);
    }
    record
}

#[test]
fn a_code_estimates_a_product_within_its_factor() {
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
        let codes = Codes::new(&record, DIMENSION);
        Estimate::new(&query).ranges(&codes, 0..1, base, WIDEST, &mut ranges);
        let [(low, high)] = ranges[..] else {
            panic!("pair {pair}: {} ranges for one code", ranges.len())
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
fn the_ranges_of_a_run_of_codes_are_those_of_the_plain_arithmetic() {
    // A hundred numbers: a binary code's second word is completed with zeros.
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
    let codes = Codes::new(&record, DIMENSION);
    Estimate::new(&query).ranges(&codes, 3..37, base, WIDTH, &mut ranges);
    assert_eq!(ranges.len(), 34);

    // The same worked out bit by bit: the search's vector at length one in whole steps of a
    // seventh of its largest number, and for each binary code the steps under its set bits.
    let asked = asked_unit(&query);
    let step = asked.iter().fold(0.0_f32, |most, x| most.max(x.abs())) / 7.0;
    let steps: Vec<i32> = asked
        .iter()
        .map(|x| ((x / step).round() as i32).clamp(-7, 7))
        .collect();
    let sum: i32 = steps.iter().sum();
    for (place, (low, high)) in (3..37).zip(ranges) {
        let code = codes.at(place);
        let set = |at: usize| code[at / 8] >> (at % 8) & 1 == 1;
        let under: i32 = (0..DIMENSION)
            .filter(|at| set(*at))
            .map(|at| steps[at])
            .sum();
        let factor = f32::from(u16::from_le_bytes([code[13], code[14]])) / 32_768.0;
        let estimate = base + factor * step * (2 * under - sum) as f32;
        let wide = f32::from(WIDTH) / 4.0 * factor;
        for (found, wanted) in [(low, estimate - wide), (high, estimate + wide)] {
            assert!(
                (found - wanted).abs() <= 1e-5 * wanted.abs().max(1.0),
                "code {place}: {found:e} where the plain arithmetic gives {wanted:e}"
            );
        }
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

pub(crate) fn fresh(test: &str) -> PathBuf {
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

/// Opens the store in a folder with every maintenance cycle left to the test.
fn opened(folder: &Path) -> Database {
    let options = crate::DatabaseOpenOptions {
        maintenance: crate::Maintenance::Caller,
        ..Default::default()
    };
    Database::open_with_options(folder, options).expect("open the store")
}

/// Whether the one partition, with its build's phase (`None` with no build), is as wanted.
type Reached = fn(&Partition, Option<Phase>) -> bool;

/// The IVF of 45 groups is in force and no build is under way.
const IN_FORCE: Reached = |entry, phase| entry.ivf.is_some() && phase.is_none();

/// A 2,000-vector store advanced to the requested maintenance state.
fn a_store_with_vectors(test: &str, reached: Reached) -> (PathBuf, Database) {
    let (folder, db) = vectors_for_damage(test, 2000);
    let db = advance_to(&folder, db, reached);
    (folder, db)
}

fn vectors_for_damage(test: &str, count: u64) -> (PathBuf, Database) {
    let folder = fresh(test);
    let db = opened(&folder);
    let threshold = if count == 0 {
        String::new()
    } else {
        format!(" INDEXING_THRESHOLD {}", count.min(1000))
    };
    let table =
        format!("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, v VECTOR(16){threshold})");
    db.execute(&table, &HashMap::new()).expect("declare");
    insert_gathered(&db, 1..=count);
    (folder, db)
}

fn advance_to(folder: &Path, mut db: Database, reached: Reached) -> Database {
    for cycle in 1..=40 {
        db.run_maintenance_cycle().expect("a maintenance cycle");
        let (store, budget, column, entry) = beneath(folder, db);
        let view = store.view().expect("the newest commit");
        let held = &mut budget.reserve(0, "query").expect("a reservation");
        let build = Build::read(&view, column, entry.number, BUILT, held).expect("read the build");
        let now = reached(&entry, build.map(|build| build.phase));
        drop((view, store));
        db = opened(folder);
        if now {
            return db;
        }
        assert!(cycle < 40, "40 cycles and the partition is at {entry:?}");
    }
    unreachable!("the loop returns or fails")
}

// Extends the centroid record roundtrip to records written by first builds and rebuilds.
#[test]
fn every_build_renews_the_mean_and_each_centroid_from_its_members() {
    let (folder, mut db) = a_store_with_vectors("member-means", IN_FORCE);
    for last in [2000, 2400] {
        insert_gathered(&db, 2001..=last);
        for _ in 0..20 {
            db.run_maintenance_cycle().expect("finish the build");
        }
        let (store, budget, column, entry) = beneath(&folder, db);
        let view = store.view().expect("a view");
        let g = entry.ivf.expect("an IVF");
        assert_eq!((g.blocks, g.slots), (1, last));
        let held = &mut budget.reserve(0, "query").expect("a reservation");
        let centroids = centroids(&view, column, g.number, g.groups, BUILT, held).expect("centres");
        let shape = Ivf::read(&view, column, g, held).expect("group boundaries");
        let (_, bytes) = first_record(&view, VectorRecord::Block(column, g.number, 0));
        let block = BlockReader::new(&bytes, BUILT);
        let mut average = |from, to| {
            let mut sum = Mean::new(BUILT, held).expect("a sum");
            for n in from..to {
                sum.take(block.slot(n));
            }
            sum.finish()
        };
        let at = |p: Place| p.block as usize * block.len() + p.slot as usize;
        for (places, found) in shape.starts.windows(2).zip(centroids.chunks(BUILT)) {
            let wanted = unit(&average(at(places[0]), at(places[1])));
            assert!(found.iter().zip(wanted).all(|(a, b)| (a - b).abs() < 1e-6));
        }
        let wanted = average(0, block.len());
        let found = mean(&view, column, g.number, BUILT, held).expect("the IVF's mean");
        assert!(found.iter().zip(wanted).all(|(a, b)| (a - b).abs() < 1e-6));
        drop((view, store));
        db = opened(&folder);
    }
}
/// The first record of the kind of `from` that the column of `from` keeps, with its value.
fn first_record(view: &View, from: VectorRecord<'static>) -> (VectorRecord<'static>, Vec<u8>) {
    let mut found = None;
    view.vectors(from, &mut |record, value| {
        let record = match record {
            VectorRecord::Block(c, n, at) => VectorRecord::Block(c, n, at),
            VectorRecord::Ivf(c, n) => VectorRecord::Ivf(c, n),
            VectorRecord::Mean(c, n) => VectorRecord::Mean(c, n),
            VectorRecord::Centroids(c, n, at) => VectorRecord::Centroids(c, n, at),
            VectorRecord::Binary(c, n, at) => VectorRecord::Binary(c, n, at),
            VectorRecord::Assigned(c, n, at) => VectorRecord::Assigned(c, n, at),
            other => panic!("looked for {from:?} and the next record is {other:?}"),
        };
        found = Some((record, value.to_vec()));
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
    // So that the check's word on the damaged store is the damage's, not a store it never passes.
    db.verify().expect("the check passes the sound built store");
    let (store, budget, column, _) = beneath(folder, db);
    let view = store.view().expect("the newest commit");
    let (record, mut value) = first_record(&view, from(column));
    damage(&mut value);
    drop(view);
    put(&store, &budget, record, value);
    drop(store);
    opened(folder)
}

/// The ids a search of ten returns under this mode.
fn ten(db: &Database, mode: &str) -> crate::types::Result<Vec<Value>> {
    let sql = format!(
        "SELECT id, v <=> $q AS distance FROM docs ORDER BY distance USE VECTOR {mode} LIMIT 10"
    );
    let result = db.execute(&sql, &crate::params! { "q" => gathered(40_003) })?;
    Ok(result.rows().iter().map(|row| row[0].clone()).collect())
}

/// Closes the store and opens it as these tests write to it: the store, its budget, the
/// column of `docs.v` and the entry of its one partition.
fn beneath(folder: &Path, db: Database) -> (Store, Budget, VectorId, Partition) {
    db.close().expect("close");
    drop(db);
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, &DatabaseOpenOptions::default())
        .expect("open the store");
    let view = store.view().expect("the newest commit");
    let catalog = Catalog::load(&view, &budget).expect("the tables");
    let declared = &catalog.table("docs").expect("the table").vectors[0];
    let column = declared.id;
    let none = Key::from_values(&[], &[]).expect("the key of no values");
    let entry = partition(&view, declared, &none).expect("read the partition");
    drop(view);
    (store, budget, column, entry.expect("the one partition"))
}

/// The build the one partition of a store is making, read as [`beneath`] opens the store.
fn build_under_way(
    store: &Store,
    column: VectorId,
    entry: &Partition,
    held: &mut Reservation,
) -> Build {
    let view = store.view().expect("the newest commit");
    let build = Build::read(&view, column, entry.number, BUILT, held).expect("read the build");
    build.expect("a build under way")
}

/// How many records of the kind of `from` the column of `from` keeps, from `from` on.
fn records_from(view: &View, from: VectorRecord<'_>) -> usize {
    let column = |record: VectorRecord<'_>| match record {
        VectorRecord::Partition(column, _)
        | VectorRecord::Pending(column, ..)
        | VectorRecord::Block(column, ..)
        | VectorRecord::Ivf(column, _)
        | VectorRecord::Centroids(column, ..)
        | VectorRecord::Build(column, _)
        | VectorRecord::Assigned(column, ..)
        | VectorRecord::Binary(column, ..)
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
fn a_partition_that_empties_leaves_no_record_of_its_ivf_or_its_build() {
    for later in [false, true] {
        // An IVF in force; then more vectors sealed into its tail, a row that left
        // its slot, and the regrouping those make due begun.
        let (folder, mut db) =
            a_store_with_vectors(&format!("emptied-partition-{later}"), IN_FORCE);
        insert_gathered(&db, 2001..=5000);
        let changed = crate::params! { "v" => gathered(50_000) };
        db.execute("UPDATE docs SET v = $v WHERE id = 5", &changed)
            .expect("change a vector");
        // A cycle advances builds before it seals what commits left (K1): the first seals the
        // new rows, the second begins the regrouping it made due.
        db.run_maintenance_cycle().expect("a maintenance cycle");
        db.run_maintenance_cycle().expect("a maintenance cycle");
        if later {
            db = advance_to(&folder, db, |_, p| {
                matches!(p, Some(Phase::Train { round: 1 }))
            });
            db.execute("DELETE FROM docs WHERE id = 5", &HashMap::new())
                .unwrap();
        }
        // Every kind of record the column may keep, and how many of each it keeps at a moment.
        let counts = |folder: &Path| -> Vec<usize> {
            let budget = Budget::new(None).expect("a budget");
            let store = Store::open_for_writing(folder, &budget, &DatabaseOpenOptions::default())
                .expect("open the store");
            let view = store.view().expect("the newest commit");
            let catalog = Catalog::load(&view, &budget).expect("the tables");
            let column = catalog.table("docs").expect("the table").vectors[0].id;
            let none = Key::from_values(&[], &[]).expect("the key of no values");
            let kinds = [
                VectorRecord::Partition(column, &none),
                VectorRecord::Pending(column, 0, &none),
                VectorRecord::Block(column, 0, 0),
                VectorRecord::Ivf(column, 0),
                VectorRecord::Centroids(column, 0, 0),
                VectorRecord::Build(column, 0),
                VectorRecord::Assigned(column, 0, 0),
                VectorRecord::Binary(column, 0, 0),
                VectorRecord::Mean(column, 0),
                VectorRecord::Left(column, 0, &none),
            ];
            kinds.map(|from| records_from(&view, from)).to_vec()
        };
        db.close().expect("close");
        drop(db);
        let before = counts(&folder);
        for (kind, count) in before[..6].iter().enumerate() {
            assert_eq!(
                *count == 0,
                kind == 1,
                "kind {kind} records before: {before:?}"
            );
        }
        assert!(
            before[7] > 0 && before[8] == 1 && before[9] == 1,
            "{before:?}"
        );

        let db = opened(&folder);
        db.execute("DELETE FROM docs WHERE id > 0", &HashMap::new())
            .expect("delete every row");
        db.close().expect("close");
        drop(db);
        let after = counts(&folder);
        assert_eq!(after, [0; 10], "records left by the emptied partition");
        std::fs::remove_dir_all(&folder).expect("remove the test's folder");
    }
}

/// Corruption retrains under the same limit; centroid I/O must escape without a commit.
#[test]
fn a_damaged_ivf_is_rebuilt_by_the_cycles_under_the_same_limit() {
    let (folder, db) = a_store_with_vectors("rebuilt-ivf", IN_FORCE);
    let wide = |record: &mut Vec<u8>| record[..4].copy_from_slice(&20_001_u32.to_le_bytes());
    let in_force = |column| VectorRecord::Ivf(column, 0);
    let db = damaged(&folder, db, in_force, wide);
    let exact = ten(&db, "EXACT").expect("EXACT reads no record of the index");
    let auto = ten(&db, "AUTO").expect("AUTO answers over a damaged IVF");
    assert!(auto == exact, "AUTO {auto:?}, EXACT {exact:?}");
    for ran in 1..=100 {
        let cycle = db.run_maintenance_cycle().expect("a cycle").vector;
        assert!(count(cycle.stalled_partitions) == 0, "cycle {ran} stalled");
        if count(cycle.maintenance_vectors_remaining) + count(cycle.remaining_partitions) == 0 {
            break;
        }
        assert!(ran < 100, "100 cycles left the damaged IVF unbuilt");
    }
    let indexed = ten(&db, "INDEXED").expect("the rebuilt IVF answers USE VECTOR INDEXED");
    assert_eq!(indexed.len(), exact.len(), "the rebuilt IVF's answer");
    let durable = db
        .execute("SELECT id FROM docs", &crate::params! {})
        .unwrap();
    let durable: Vec<_> = durable.rows().iter().map(|row| &row[0]).collect();
    assert!(
        indexed.iter().all(|id| durable.contains(&id)),
        "durable ids"
    );
    insert_gathered(&db, 2001..=2400);
    let (store, budget, column, entry) = beneath(&folder, db);
    let ivf = entry.ivf.unwrap();
    assert!(ivf.groups > 1);
    let held = &mut budget.reserve(0, "query").expect("a reservation");
    Ivf::read(&store.view().unwrap(), column, ivf, held).unwrap();
    assert_eq!(
        ivf.slots + entry.slots + u64::from(entry.unindexed),
        entry.live
    );
    assert!(entry.assigned + entry.slots < u64::from(ivf.groups).pow(2));
    let from = VectorRecord::Centroids(column, ivf.number, 0);
    let (record, centres) = first_record(&store.view().unwrap(), from);
    drop(store);
    let saved = std::fs::read(folder.join("data")).unwrap();
    let fault = report::Fault::new("reuse-centroid-sentinel");
    let options = || fault.options(Default::default());
    let db = Database::open_with_options(&folder, options()).unwrap();
    fault.clear();
    db.run_maintenance_cycle().unwrap();
    fault.identify(&centres);
    drop(db);
    for io_failure in [false, true] {
        std::fs::write(folder.join("data"), &saved).unwrap();
        if !io_failure {
            let store =
                Store::open_for_writing(&folder, &budget, &DatabaseOpenOptions::default()).unwrap();
            let mut bad = centres.clone();
            bad[..4].copy_from_slice(&f32::NAN.to_le_bytes());
            put(&store, &budget, record, bad);
        }
        let mut db = Database::open_with_options(&folder, options()).unwrap();
        let answer = ten(&db, "EXACT").unwrap();
        if io_failure {
            db = fault.refuse_cycle(db, || {
                Database::open_with_options(&folder, options()).unwrap()
            });
            let sql = "SHOW VECTOR_PARTITIONS";
            let shown = db.execute(sql, &HashMap::new()).unwrap();
            assert_eq!(shown.rows()[0][10], Value::Text("pending".into()));
            db.run_maintenance_cycle().unwrap();
        } else {
            db.run_maintenance_cycle().unwrap();
            let (store, budget, column, entry) = beneath(&folder, db);
            let held = &mut budget.reserve(0, "query").expect("a reservation");
            let build = build_under_way(&store, column, &entry, held);
            assert!(matches!(build.phase, Phase::Train { round: 0 }) && !build.reusing);
            drop(store);
            db = opened(&folder);
        }
        assert_eq!(ten(&db, "EXACT").unwrap(), answer);
        db.run_maintenance_cycle().unwrap();
        db.close().unwrap();
    }
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

// ---- damaged records ----

/// Damage that no public call writes: a standalone record, a pending row without its vector,
/// a schema naming the wrong column, or one record changed in a saved maintenance state.
type RecordFrom = fn(VectorId) -> VectorRecord<'static>;
enum Damage {
    Alone(Kind, Vec<u8>),
    Tuple,
    NoVector(i64),
    Named(u8),
    Built(&'static str, RecordFrom, fn(&mut Vec<u8>)),
    Building(&'static str, fn(Build) -> Build),
    Place(&'static str, RecordFrom, Option<u32>),
    Left(&'static str, usize),
}

/// The record a [`Damage::Alone`] is, and for centroids how many groups it is read as.
#[derive(Clone, Copy)]
enum Kind {
    Partition,
    Ivf,
    Centroids(u32),
    Mean,
    Build,
}

/// Sound stores saved once, with one vector except where binary codes and two nonempty
/// groups need three. Each damaged case gets its own copy of a closed data file.
fn damage_stores() -> HashMap<&'static str, Vec<u8>> {
    let mut saved = HashMap::new();
    for count in [0, 1, 3, 2000] {
        let states: &[&str] = match count {
            0 => &["pending", "later-pending"],
            1 => &["begun", "assigned", "left-building", "left-sweeping"],
            3 => &["packed", "width", "switch", "built"],
            _ => &["training"],
        };
        let (folder, mut db) = vectors_for_damage(&format!("damage-source-{count}"), count);
        for (at, &name) in states.iter().enumerate() {
            if count == 0 {
                insert_gathered(&db, at as u64 + 1..=at as u64 + 1);
            } else {
                if name == "left-building" {
                    db = advance_to(&folder, db, IN_FORCE);
                    db.execute(
                        "UPDATE docs SET v = $v WHERE id = 1",
                        &crate::params! { "v" => gathered(50_000) },
                    )
                    .expect("leave one stale slot");
                }
                let reached: Reached = match name {
                    "begun" | "left-building" => {
                        |_, p| matches!(p, Some(Phase::Assign { next: 0 }))
                    }
                    "training" => |_, p| matches!(p, Some(Phase::Train { round: 1 })),
                    "assigned" => |_, p| matches!(p, Some(Phase::Pack { group: 0 })),
                    "packed" => |_, p| matches!(p, Some(Phase::Measure { done: 0, .. })),
                    "width" => |_, p| matches!(p, Some(Phase::Width { .. })),
                    "switch" => |_, p| matches!(p, Some(Phase::Switch { .. })),
                    "left-sweeping" => |_, p| p == Some(Phase::Sweep),
                    _ => IN_FORCE,
                };
                db = advance_to(&folder, db, reached);
            }
            if name == "assigned" || name == "built" {
                db.verify().expect("the check passes the sound built store");
            }
            if name == "built" {
                assert_eq!(ten(&db, "INDEXED").expect("the built store").len(), 3);
            }
            db.close().expect("close");
            drop(db);
            let bytes = std::fs::read(folder.join("data")).expect("save the state");
            saved.insert(name, bytes);
            db = opened(&folder);
        }
        db.close().expect("close");
        std::fs::remove_dir_all(folder).expect("remove the source");
    }
    saved
}

/// Runs a step, checking that a refusal leaves no staged records or memory charge.
fn staged(store: &Store, budget: &Budget, stage: StageWith) -> crate::types::Result<()> {
    let view = store.view().expect("the newest commit");
    let mut changes = ChangeSet::new(view.clone(), budget).expect("a change set");
    let counted = budget.used();
    let given =
        Catalog::load(&view, budget).and_then(|catalog| stage(&mut changes, &catalog, budget));
    assert!(
        given.is_ok() || changes.is_empty(),
        "the refusal left records staged"
    );
    // A refusal before any write holds nothing; after its first write this transaction
    // retains exactly the drain workspace, including when its staged records rewind.
    let held = budget.used() - counted;
    assert!(
        given.is_ok() || [0, redb::WriteTransaction::DRAIN_BYTES as u64].contains(&held),
        "memory counted after the refusal beyond the transaction's drain workspace"
    );
    drop(changes);
    assert_eq!(
        budget.used(),
        counted,
        "abort releases the transaction's workspace"
    );
    given
}

type StageWith<'a> = &'a dyn Fn(&mut ChangeSet, &Catalog, &Budget) -> crate::types::Result<()>;

type Paths = Vec<(&'static str, crate::types::Result<()>)>;

/// Each path's answer after changing one record in its own copy of a sound store.
fn met(test: &str, damage: Damage, saved: &HashMap<&str, Vec<u8>>) -> (PathBuf, Paths) {
    let state = match &damage {
        Damage::Alone(..) => None,
        Damage::Tuple => Some("built"),
        Damage::NoVector(2) => Some("later-pending"),
        Damage::NoVector(_) | Damage::Named(_) => Some("pending"),
        Damage::Built(state, ..)
        | Damage::Building(state, _)
        | Damage::Left(state, _)
        | Damage::Place(state, ..) => Some(*state),
    };
    let folder = fresh(test);
    if let Some(state) = state {
        std::fs::create_dir(&folder).expect("a case's folder");
        std::fs::write(folder.join("data"), &saved[state]).expect("copy the sound state");
    }
    let budget = Budget::new(None).expect("a budget");
    let store =
        Store::open_for_writing(&folder, &budget, &DatabaseOpenOptions::default()).expect("open");
    let view = store.view().expect("the newest commit");
    let catalog = Catalog::load(&view, &budget).expect("tables");
    let cycle: StageWith =
        &|changes, catalog, budget| seal(changes, catalog, budget, 4096).map(drop);
    let given = match damage {
        Damage::Tuple => {
            let declared = &catalog.table("docs").unwrap().vectors[0];
            let empty = Key::from_values(&[], &[]).unwrap();
            let extra = key(1);
            let p = partition(&view, declared, &empty).unwrap().unwrap();
            let record = VectorRecord::Partition(declared.id, &extra);
            put(&store, &budget, record, p.encode());
            vec![]
        }
        Damage::Alone(kind, bytes) => {
            let (tuple, number) = (key(1), GROUPED.number);
            let record = match kind {
                Kind::Partition => VectorRecord::Partition(COLUMN, &tuple),
                Kind::Ivf => VectorRecord::Ivf(COLUMN, number),
                Kind::Centroids(_) => VectorRecord::Centroids(COLUMN, number, 0),
                Kind::Mean => VectorRecord::Mean(COLUMN, number),
                Kind::Build => VectorRecord::Build(COLUMN, number),
            };
            if !bytes.is_empty() {
                put(&store, &budget, record, bytes);
            }
            let view = store.view().expect("the newest commit");
            let held = &mut budget.reserve(0, "query").expect("a reservation");
            let declared = crate::schema::VectorColumn {
                id: COLUMN,
                column: "v".into(),
                partition_key: vec!["ctx".into()],
                max_partitions: None,
                search_mode: None,
                indexing_threshold: None,
                probes: None,
                compaction: None,
                quantization: Default::default(),
            };
            let read = match kind {
                Kind::Partition => partition(&view, &declared, &tuple).map(drop),
                Kind::Ivf => Ivf::read(&view, COLUMN, GROUPED, held).map(drop),
                Kind::Centroids(groups) => {
                    centroids(&view, COLUMN, number, groups, 16, held).map(drop)
                }
                Kind::Mean => mean(&view, COLUMN, number, 16, held).map(drop),
                Kind::Build => Build::read(&view, COLUMN, number, BUILT, held).map(drop),
            };
            vec![("its reader", read)]
        }
        Damage::NoVector(id) => {
            let table = catalog.table("docs").expect("docs").id;
            let mut changes = ChangeSet::new(view, &budget).expect("begin");
            let key = Key::from_values(&[Value::Int64(id)], &[]).expect("a key");
            let row = [Value::Int64(id), Value::Null, Value::Null]
                .map(Cell::Here)
                .to_vec();
            changes
                .put_row(table, key, row, Vec::new(), false)
                .expect("stage");
            store.commit(changes).expect("commit the damage");
            vec![("a cycle", staged(&store, &budget, cycle))]
        }
        Damage::Named(column) => {
            let table = catalog.table("docs").expect("docs").id;
            let mut record = None;
            view.each_schema(|bytes| {
                if record.is_none() {
                    record = Some(bytes.to_vec());
                }
                Ok(())
            })
            .expect("the schema records");
            let mut record = record.expect("the table's record");
            // The VECTOR entry names `v`, followed by six zero bytes for its undeclared policies.
            // The column declaration also names `v`, but is followed by its nonzero type.
            let entry = [1, 0, 0, 0, b'v', 0, 0, 0, 0, 0, 0];
            let found = (0..record.len()).filter(|at| record[*at..].starts_with(&entry));
            let [at] = found.collect::<Vec<_>>()[..] else {
                panic!("one VECTOR entry for `v`")
            };
            record[at + 4] = column;
            let mut changes = ChangeSet::new(view, &budget).expect("a change set");
            changes.put_schema("docs", record).expect("stage");
            store.commit(changes).expect("commit the damage");
            let write: StageWith = &|changes, catalog, budget| {
                let key = Key::from_values(&[Value::Int64(2)], &[]).expect("a key");
                let (id, n) = (Value::Int64(2), Value::Int64(20));
                let row = [id, n, Value::Vector(gathered(2))];
                changes.put_row(table, key, row.map(Cell::Here).to_vec(), Vec::new(), true)?;
                let totals = super::Totals::load(changes.base(), catalog)?;
                track_unindexed(changes, catalog, budget, &totals).map(drop)
            };
            vec![
                ("a write", staged(&store, &budget, write)),
                ("a cycle", staged(&store, &budget, cycle)),
            ]
        }
        damage => {
            let declared = &catalog.table("docs").expect("docs").vectors[0];
            let column = declared.id;
            let none = Key::from_values(&[], &[]).expect("the partition key");
            let entry = partition(&view, declared, &none)
                .expect("read the partition")
                .expect("one partition");
            let left = matches!(damage, Damage::Left(..));
            let mut check_only = matches!(&damage, Damage::Built(_, from, _) if matches!(from(column), VectorRecord::Block(..)));
            match damage {
                Damage::Built(_, from, change) => {
                    let (record, mut value) = first_record(&view, from(column));
                    check_only |= matches!(record, VectorRecord::Mean(..));
                    change(&mut value);
                    put(&store, &budget, record, value);
                }
                Damage::Place(_, from, place) => {
                    let (record, value) = first_record(&view, from(column));
                    let mut changes = ChangeSet::new(store.view().unwrap(), &budget).unwrap();
                    changes.delete_vector(record).unwrap();
                    if let VectorRecord::Block(v, n, at) = record {
                        changes
                            .delete_vector(VectorRecord::Binary(v, n, at))
                            .unwrap();
                    }
                    check_only |=
                        matches!(record, VectorRecord::Block(..) | VectorRecord::Mean(..));
                    if let Some(at) = place {
                        let moved = match record {
                            VectorRecord::Binary(v, n, _) => VectorRecord::Binary(v, n, at),
                            VectorRecord::Assigned(v, n, _) => VectorRecord::Assigned(v, n, at),
                            _ => unreachable!(),
                        };
                        changes.put_vector(moved, value).unwrap();
                    }
                    store.commit(changes).unwrap();
                }
                Damage::Building(_, change) => {
                    let held = &mut budget.reserve(0, "query").expect("a reservation");
                    let original = if state == Some("built") {
                        a_build(Phase::Train { round: 0 })
                    } else {
                        build_under_way(&store, column, &entry, held)
                    };
                    let build = change(original);
                    check_only |= Some(build.number) != entry.building.map(|(number, _)| number);
                    put(
                        &store,
                        &budget,
                        VectorRecord::Build(column, entry.number),
                        build.encode(),
                    );
                }
                Damage::Left(_, bytes) => {
                    let key = Key::from_values(&[Value::Int64(1)], &[]).expect("the stale row");
                    let record = VectorRecord::Left(column, entry.number, &key);
                    put(&store, &budget, record, vec![0; bytes]);
                }
                _ => unreachable!(),
            };
            drop((view, store));
            if check_only {
                return (folder, Vec::new());
            }
            let mut db = opened(&folder);
            let given = if state == Some("built") {
                let exact = ten(&db, "EXACT").expect("EXACT reads no record of the index");
                let auto = ten(&db, "AUTO").expect("AUTO answers over a damaged index");
                assert!(
                    exact.len() == 3 && auto == exact,
                    "AUTO {auto:?}, EXACT {exact:?}"
                );
                db.close().expect("close");
                db = opened(&folder);
                ("a search USE VECTOR INDEXED", ten(&db, "INDEXED").map(drop))
            } else {
                let (path, cycles) = if left {
                    ("the cycles that follow", 40)
                } else {
                    ("a cycle", 1)
                };
                let told = (0..cycles).find_map(|_| db.run_maintenance_cycle().err());
                (path, told.map_or(Ok(()), Err))
            };
            db.close().expect("close");
            vec![given]
        }
    };
    (folder, given)
}

#[test]
fn every_damaged_vector_record_is_told_as_damage_by_each_path_that_meets_it() {
    use {Damage::*, Kind::Centroids as Groups};
    let (e, g) = (a_partition().encode(), an_ivf().encode());
    let flags = |flags: u8| [&e[..40], &[flags], &e[41..]].concat();
    let back = with(&g, 28, 0);
    let bytes = |numbers: Vec<f32>| numbers.into_iter().flat_map(f32::to_le_bytes).collect();
    let c: Vec<u8> = bytes((0..3).flat_map(|group| unit(&vector(group, 16))).collect());
    let m: Vec<u8> = bytes(vector(77, 16));
    let not_finite = |bytes: &[u8], at: usize, n: f32| {
        [&bytes[..at], &n.to_le_bytes(), &bytes[at + 4..]].concat()
    };
    let (entry, ivf) = (|b| Alone(Kind::Partition, b), |b| Alone(Kind::Ivf, b));
    let (means, builds) = (|b| Alone(Kind::Mean, b), |b| Alone(Kind::Build, b));
    let assign = a_build(Phase::Assign { next: 5 }).encode();
    let mut pack = a_build(Phase::Pack { group: 46 });
    pack.counts = given().0;
    let train = a_build(Phase::Train { round: 3 }).encode();
    let binary = |column| VectorRecord::Binary(column, 0, 0);
    let in_force = |column| VectorRecord::Ivf(column, 0);
    let assigned = |column| VectorRecord::Assigned(column, 0, 0);
    // One binary code fewer than the block has slots, and the record saying so.
    let fewer = |record: &mut Vec<u8>| {
        let count = u32::from_le_bytes(std::array::from_fn(|at| record[at])) - 1;
        record[..4].copy_from_slice(&count.to_le_bytes());
        record.truncate(record.len() - code_len(BUILT));
    };
    let wide = |record: &mut Vec<u8>| record[..4].copy_from_slice(&20_001_u32.to_le_bytes());
    // Every slot is given its group in two bytes.
    let two = |record: &mut Vec<u8>| record.extend([0, 0]);
    let one = |record: &mut Vec<u8>| record.push(0);
    // The build as it begins.
    let groups = |mut build: Build| {
        assert_eq!((build.slots, build.groups), (1, 1));
        (build.groups, build.counts) = (2, vec![0; 2]);
        build
    };
    // The build once it has packed every group and recorded where each begins.
    let swapped = |mut build: Build| {
        assert!(build.starts.len() == 3 && build.starts[1] < build.starts[2]);
        build.starts.swap(1, 2);
        build
    };
    let block = |v| VectorRecord::Block(v, 0, 0);
    let framing = |b: &mut Vec<u8>| b[..4].fill(255);
    let orphan = |b: &mut Vec<u8>| *b.last_mut().unwrap() = 127;
    let past_build = Place("assigned", assigned, Some(u32::MAX));
    let no_centroids = Place("training", |v| VectorRecord::Centroids(v, 0, 0), None);
    let wrong_build = Building("begun", |mut b| {
        b.number += 1;
        b
    });
    let overflow = |record: &mut Vec<u8>| {
        let q = unit(&gathered(40_003));
        let c: Vec<_> = q.iter().map(|x| f32::MAX.copysign(*x)).collect();
        assert!(c.iter().chain(&q).all(|x| x.is_finite()));
        assert!(q.iter().map(|x| x.abs()).sum::<f32>() > 1.0);
        assert!(
            !c.iter()
                .zip(&q)
                .map(|(x, y)| x * y)
                .sum::<f32>()
                .is_finite()
        );
        for (bytes, n) in record.chunks_exact_mut(4).zip(c.iter().cycle()) {
            bytes.copy_from_slice(&n.to_le_bytes());
        }
    };
    let bad_product = Built("built", |v| VectorRecord::Centroids(v, 0, 0), overflow);
    let extra_sum = |mut b: Build| {
        b.sum.push(0.0);
        b
    };
    let saved = damage_stores();
    let mut rows = vec![
        ("partition tuple has an extra cell", Tuple),
        (
            "missing grouped block and codes",
            Place("built", block, None),
        ),
        ("stray build on idle partition", Building("built", |b| b)),
        ("finite centroid product overflows", bad_product),
        ("entry of 16 bytes", entry(e[..16].to_vec())),
        ("entry a byte longer", entry([&e[..], &[0]].concat())),
        ("entry, a flag above three", entry(flags(15))),
        ("entry, tail past sealed", entry(with(&e, 32, 499))),
        ("entry, first block past closed", entry(with(&e, 16, 5))),
        ("entry, closed block past last", entry(with(&e, 20, 7))),
        ("entry, IVF of no group", entry(with(&e, 49, 0))),
        ("no IVF", ivf(Vec::new())),
        ("IVF, two groups' places", ivf(g[..g.len() - 8].to_vec())),
        ("IVF, gap above 20,000", ivf(with(&g, 0, 20_001))),
        ("IVF, width a quarter", ivf(with(&g, 4, 1))),
        ("IVF, width past 16", ivf(with(&g, 4, 17))),
        ("IVF, scans no group", ivf(with(&g, 8, 0))),
        ("IVF, scans past groups", ivf(with(&g, 8, 4))),
        ("IVF, not from slot 0", ivf(with(&g, 16, 1))),
        ("IVF, a place going back", ivf(with(&back, 32, 100))),
        ("IVF, ends past blocks", ivf(with(&g, 36, 5))),
        ("three centroids, four groups", Alone(Groups(4), c.clone())),
        (
            "centroids, part of one",
            Alone(Groups(3), c[..c.len() - 1].to_vec()),
        ),
        (
            "centroid not finite",
            Alone(Groups(3), not_finite(&c, 4, f32::NAN)),
        ),
        ("no centroids", Alone(Groups(3), Vec::new())),
        ("mean of fifteen numbers", means(m[..m.len() - 4].to_vec())),
        ("mean not finite", means(not_finite(&m, 0, f32::INFINITY))),
        ("no mean", means(Vec::new())),
        ("build assigning past blocks", builds(assign)),
        ("build packing a missing group", builds(pack.encode())),
        ("build cut short", builds(train[..train.len() - 1].to_vec())),
        // The first, which a cycle reads to find the partition, and one it reads for a slot.
        ("first pending row, no vector", NoVector(1)),
        ("later pending row, no vector", NoVector(2)),
        ("VECTOR entry of no column", Named(b'w')),
        ("VECTOR entry of an INTEGER", Named(b'n')),
        (
            "codes fewer than the block's",
            Built("built", binary, fewer),
        ),
        (
            "code cut short",
            Built("built", binary, |b| b.truncate(b.len() - 1)),
        ),
        ("code count", Built("built", binary, |b| b[..4].fill(255))),
        ("missing codes", Place("built", binary, None)),
        ("codes out of order", Place("built", binary, Some(1))),
        ("assigned beyond build", past_build),
        ("later training lacks centroids", no_centroids),
        ("build identity", wrong_build),
        ("block framing", Built("built", block, framing)),
        ("block orphan slot", Built("built", block, orphan)),
        ("build sum dimension", Building("begun", extra_sum)),
        ("packed sum dimension", Building("packed", extra_sum)),
        ("a built IVF too wide", Built("built", in_force, wide)),
        ("assigned, a slot more", Built("assigned", assigned, two)),
        ("assigned, odd bytes", Built("assigned", assigned, one)),
        ("build, more groups than slots", Building("begun", groups)),
        ("build, groups out of order", Building("packed", swapped)),
        ("left of 15 bytes, building", Left("left-building", 15)),
        ("left of 15 bytes, sweeping", Left("left-sweeping", 15)),
        ("left of 17 bytes, sweeping", Left("left-sweeping", 17)),
    ];
    for (state, name) in [
        ("packed", "Measure build mean"),
        ("width", "Width build mean"),
        ("switch", "Switch build mean"),
    ] {
        let build_mean = |v| VectorRecord::Mean(v, 0);
        rows.extend([
            (name, Built(state, build_mean, |b| b.truncate(1))),
            (name, Place(state, build_mean, None)),
        ]);
    }
    let mut failures = Vec::new();
    for (at, (what, damage)) in rows.into_iter().enumerate() {
        // A record never written leaves an empty store, which is sound: its reader alone meets it.
        let query_only = matches!(&damage, Built(_, from, _) if matches!(from(COLUMN), VectorRecord::Centroids(..)));
        let unwritten = matches!(&damage, Alone(_, bytes) if bytes.is_empty());
        let (folder, mut given) = met(&format!("vector-damage-{at}"), damage, &saved);
        if !unwritten && !query_only {
            let check = Database::open(&folder).and_then(|db| db.verify());
            given.push(("the whole-store check", check.map(drop)));
        }
        for (path, given) in given {
            if !matches!(&given, Err(Error::StoreCorrupted { path, .. }) if *path == folder) {
                failures.push(format!("{what}, met by {path}: {given:?}"));
            }
        }
        std::fs::remove_dir_all(&folder).expect("remove the test's folder");
    }

    assert!(failures.is_empty(), "{failures:#?}");

    // A mean as it is put reads back.
    let (folder, store, budget) = a_store("mean");
    let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
    put_mean(&mut changes, COLUMN, 7, &vector(77, 16)).expect("stage the mean");
    store.commit(changes).expect("commit");
    let held = &mut budget.reserve(0, "query").expect("a reservation");
    let read = mean(&store.view().expect("a view"), COLUMN, 7, 16, held);
    assert_eq!(read.expect("read the mean"), vector(77, 16));
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn the_work_list_puts_a_partition_never_built_before_those_with_an_ivf() {
    let (f, b) = (fresh("work-order"), Budget::new(None).unwrap());
    let (db, p) = (opened(&f), crate::params! { "v" => gathered(1) });
    let run = |s| drop(db.execute(s, &p).unwrap());
    run("CREATE TABLE d (id INT PRIMARY KEY, c INT NOT NULL, \
         v VECTOR(16) PARTITION_KEY (c) INDEXING_THRESHOLD 4)");
    run("INSERT INTO d VALUES (1,1,$v),(2,1,$v),(3,2,$v),(4,2,$v)");
    while db.run_maintenance_cycle().unwrap().work_remaining == Some(true) {}
    run("INSERT INTO d VALUES (5,1,$v),(6,2,$v),(7,4,$v)");
    db.close().unwrap();
    let s = Store::open_for_writing(&f, &b, &Default::default()).unwrap();
    let (v, h) = (s.view().unwrap(), &mut b.reserve(0, "q").unwrap());
    let w = unindexed_work(&v, &Catalog::load(&v, &b).unwrap(), h, &Default::default()).unwrap();
    let got: Vec<_> = w.iter().map(|w| &w.tuple).collect();
    assert_eq!(got, [&key(4), &key(1), &key(2)]);
}
