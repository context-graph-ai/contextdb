//! How a key is written as bytes, and which bytes lie between two keys.
//!
//! Each value of a key is written as one tag byte and then the value, in a form whose bytes
//! compare as the values do and that no other value's form begins with. A key of several
//! columns is its values one after another, so it compares column by column, and a key of
//! fewer columns is the beginning of every key that starts with the same values.
//!
//! NULL has the greatest tag, so it comes after every value. A value in a descending column,
//! NULL too, has every bit of its form turned, which is the same order backwards. No tag is
//! 0x00 or 0xFF, so no form begins with 0xFF whichever way its column runs.

use std::ops::Bound;

use super::{Key, VectorId, VectorRecord};
use crate::types::{ColumnType, Error, Result, SortDirection, TxId, Uuid, Value};

/// No value's form begins with this byte, turned or not, so a key followed by it sorts after
/// every key that begins with that key and before every other key above it.
const PAST: u8 = 0xFF;

/// The bytes from the first, which a span includes, to the second, which it does not.
pub(super) type Span = (Vec<u8>, Vec<u8>);

/// The form [`Key::from_values`] gives, where `room` is `usize::MAX`: every value whole.
///
/// A text whose form is longer than `room` bytes is cut to exactly `room`: its leading bytes,
/// the mark and the digest of the whole text. The second answer is where in the key the kept
/// bytes of the first cut text end.
pub(super) fn encode(
    values: &[Value],
    directions: &[SortDirection],
    room: usize,
) -> Result<(Vec<u8>, Option<usize>)> {
    let mut key = Vec::new();
    let mut first_cut = None;
    for (column, value) in values.iter().enumerate() {
        let at = key.len();
        match value {
            Value::Null => key.push(0xFE),
            Value::Bool(flag) => key.extend([1, u8::from(*flag)]),
            Value::Int64(number) => put(&mut key, 2, &signed(*number)),
            Value::Float64(number) => put(&mut key, 3, &real(*number)),
            Value::Text(text) => {
                key.push(4);
                // A zero byte inside the text is followed by 0xFF, and two zero bytes end it:
                // a text sorts before every longer text it begins. The mark of a cut text
                // sorts after that end and before any further byte.
                let end = at.saturating_add(room - CUT_BYTES);
                let mut cut = None;
                for byte in text.bytes() {
                    if key.len() + 1 + usize::from(byte == 0) > end {
                        cut = Some(key.len());
                        break;
                    }
                    key.push(byte);
                    if byte == 0 {
                        key.push(0xFF);
                    }
                }
                match cut {
                    None => key.extend([0, 0]),
                    Some(_) => {
                        key.extend([0, 1]);
                        key.extend(digest(text));
                    }
                }
                first_cut = first_cut.or(cut);
            }
            Value::Uuid(id) => put(&mut key, 5, id.as_bytes()),
            Value::Timestamp(millis) => put(&mut key, 6, &signed(*millis)),
            Value::TxId(id) => put(&mut key, 7, &id.0.to_be_bytes()),
            Value::Json(_) | Value::Vector(_) => {
                return Err(Error::PlanError(
                    "a JSON or vector value cannot be part of a key".to_owned(),
                ));
            }
        }
        if directions.get(column) == Some(&SortDirection::Desc) {
            key[at..].iter_mut().for_each(|byte| *byte = !*byte);
        }
    }
    Ok((key, first_cut))
}

/// The bytes the form of a cut text ends with: its mark and its digest.
const CUT_BYTES: usize = 2 + 16;

/// The FNV-1a 128-bit digest of a text, most significant byte first. It is written out here
/// because a stored digest must be the same bytes in every build.
fn digest(text: &str) -> [u8; 16] {
    let mut digest: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    for byte in text.bytes() {
        digest ^= u128::from(byte);
        digest = digest.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b);
    }
    digest.to_be_bytes()
}

fn put(key: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    key.push(tag);
    key.extend(bytes);
}

/// A signed number with its sign bit turned, most significant byte first.
fn signed(number: i64) -> [u8; 8] {
    (number.cast_unsigned() ^ (1 << 63)).to_be_bytes()
}

/// A real number in one total order: negative numbers, zero, positive numbers, then NaN. The
/// two zeros are one key, and so is every NaN.
fn real(number: f64) -> [u8; 8] {
    let number = if number.is_nan() {
        f64::NAN
    } else if number == 0.0 {
        0.0
    } else {
        number
    };
    let bits = number.to_bits();
    let ordered = if number.is_sign_negative() {
        !bits
    } else {
        bits ^ (1 << 63)
    };
    ordered.to_be_bytes()
}

/// The span of keys between two bounds. A bound stands for every key that begins with it: an
/// included bound takes them all in, an excluded bound leaves them all out.
pub(super) fn span(from: Bound<&Key>, to: Bound<&Key>) -> Span {
    let past = |key: &Key| [key.0.as_slice(), &[PAST]].concat();
    let start = match from {
        Bound::Unbounded => Vec::new(),
        Bound::Included(key) => key.0.clone(),
        Bound::Excluded(key) => past(key),
    };
    let end = match to {
        Bound::Unbounded => vec![PAST],
        Bound::Included(key) => past(key),
        Bound::Excluded(key) => key.0.clone(),
    };
    // Bounds that cross hold nothing; a span never ends before it starts.
    let end = end.max(start.clone());
    (start, end)
}

/// A key as the data file holds it: the number of its table or index, then the key.
pub(super) fn stored(id: u32, key: &[u8]) -> Vec<u8> {
    [&id.to_be_bytes(), key].concat()
}

/// The first byte of the stored key of a block of the vector part. Blocks are kept in a table
/// of their own.
pub(super) const BLOCK: u8 = 0;
const PARTITION: u8 = 1;
pub(super) const PENDING: u8 = 2;
const GROUPING: u8 = 3;
/// The first byte of the stored key of a record of centres, which is kept with the blocks.
pub(super) const CENTRES: u8 = 4;
const BUILD: u8 = 5;
const ASSIGNED: u8 = 6;
/// The first byte of the stored key of a record of sketches, which is kept with the blocks.
pub(super) const SKETCH: u8 = 7;
const MEAN: u8 = 8;
pub(super) const LEFT: u8 = 9;

/// True for the kinds of record that are kept in the table of blocks.
pub(super) fn with_blocks(kind: u8) -> bool {
    kind == BLOCK || kind == CENTRES || kind == SKETCH
}

/// A record of the vector part as the data file keys it: one byte for its kind, the column,
/// and then the rest of its fields in order, every number most significant byte first. So the
/// records of one kind are one run of keys in the order of their fields: the columns follow
/// each other by number, and within a column a partition's pending rows or blocks are one run.
pub(super) fn vector(record: VectorRecord<'_>) -> Vec<u8> {
    let number = u32::to_be_bytes;
    match record {
        VectorRecord::Block(column, partition, block) => {
            let numbers = [number(column.0), number(partition), number(block)];
            [&[BLOCK][..], numbers.as_flattened()].concat()
        }
        VectorRecord::Partition(column, tuple) => {
            [&[PARTITION][..], &number(column.0), &tuple.0].concat()
        }
        VectorRecord::Pending(column, partition, row) => {
            let numbers = [number(column.0), number(partition)];
            [&[PENDING][..], numbers.as_flattened(), &row.0].concat()
        }
        VectorRecord::Grouping(column, grouping) => {
            let numbers = [number(column.0), number(grouping)];
            [&[GROUPING][..], numbers.as_flattened()].concat()
        }
        VectorRecord::Centres(column, grouping, place) => {
            let numbers = [number(column.0), number(grouping), number(place)];
            [&[CENTRES][..], numbers.as_flattened()].concat()
        }
        VectorRecord::Build(column, partition) => {
            let numbers = [number(column.0), number(partition)];
            [&[BUILD][..], numbers.as_flattened()].concat()
        }
        VectorRecord::Assigned(column, grouping, place) => {
            let numbers = [number(column.0), number(grouping), number(place)];
            [&[ASSIGNED][..], numbers.as_flattened()].concat()
        }
        VectorRecord::Sketch(column, under, block) => {
            let numbers = [number(column.0), number(under), number(block)];
            [&[SKETCH][..], numbers.as_flattened()].concat()
        }
        VectorRecord::Mean(column, partition) => {
            let numbers = [number(column.0), number(partition)];
            [&[MEAN][..], numbers.as_flattened()].concat()
        }
        VectorRecord::Left(column, partition, row) => {
            let numbers = [number(column.0), number(partition)];
            [&[LEFT][..], numbers.as_flattened(), &row.0].concat()
        }
    }
}

/// The record a stored key of the vector part stands for. A key tuple or a row key is read
/// into `key`, which the record then borrows.
pub(super) fn vector_of<'a>(stored: &[u8], key: &'a mut Key) -> Option<VectorRecord<'a>> {
    let number = |at: usize| Some(u32::from_be_bytes(*stored.get(at..)?.first_chunk()?));
    let column = VectorId(number(1)?);
    key.0.clear();
    Some(match stored[0] {
        BLOCK => VectorRecord::Block(column, number(5)?, number(9)?),
        PARTITION => {
            key.0.extend_from_slice(&stored[5..]);
            VectorRecord::Partition(column, key)
        }
        PENDING => {
            let partition = number(5)?;
            key.0.extend_from_slice(&stored[9..]);
            VectorRecord::Pending(column, partition, key)
        }
        GROUPING => VectorRecord::Grouping(column, number(5)?),
        CENTRES => VectorRecord::Centres(column, number(5)?, number(9)?),
        BUILD => VectorRecord::Build(column, number(5)?),
        ASSIGNED => VectorRecord::Assigned(column, number(5)?, number(9)?),
        SKETCH => VectorRecord::Sketch(column, number(5)?, number(9)?),
        MEAN => VectorRecord::Mean(column, number(5)?),
        LEFT => {
            let partition = number(5)?;
            key.0.extend_from_slice(&stored[9..]);
            VectorRecord::Left(column, partition, key)
        }
        _ => return None,
    })
}

/// The most bytes a row key, a partition-key tuple, a kept count's values or an index entry
/// takes.
pub(crate) const KEY_LIMIT: usize = 2704;

/// The most bytes a TEXT value takes in an index entry. A longer one is cut and given a digest;
/// the row is read again wherever the entry cannot decide, so this sets the index's size and
/// how many rows an order over texts that begin alike reads together, never an answer.
const ENTRY_TEXT: usize = 256;

/// How the entries of one index are made: its columns' directions, and the room each of its
/// TEXT columns has. The columns of fixed size take what they take, and the TEXT columns share
/// the rest of [`KEY_LIMIT`] equally, each at most [`ENTRY_TEXT`], so no entry is longer than the
/// limit.
pub(crate) struct EntryShape {
    directions: Vec<SortDirection>,
    room: usize,
}

/// One index entry, and where its first cut value's kept bytes end when it holds one.
pub(crate) struct Entry {
    pub key: Key,
    pub cut: Option<usize>,
}

impl EntryShape {
    /// The shape of the entries of an index over columns of these types and directions.
    pub(crate) fn new(types: &[ColumnType], directions: &[SortDirection]) -> EntryShape {
        let texts = types.iter().filter(|column| **column == ColumnType::Text);
        let fixed = types.iter().map(|column| match column {
            ColumnType::Text => 0,
            ColumnType::Uuid => 17,
            ColumnType::Boolean => 2,
            _ => 9,
        });
        EntryShape {
            directions: directions.to_vec(),
            room: ((KEY_LIMIT - fixed.sum::<usize>()) / texts.count().max(1)).min(ENTRY_TEXT),
        }
    }

    /// The entry of a row's values in the index's columns; given only leading values, the bytes
    /// every entry with those values begins with. This is the only place an entry is made.
    ///
    /// A text that fits its column's room is whole, as [`Key::from_values`] writes it. A longer
    /// one is cut, so entries are in the order of their values except that long texts with
    /// the same kept bytes follow each other in the order of their digests.
    pub(crate) fn entry(&self, values: &[Value]) -> Result<Entry> {
        let (key, cut) = encode(values, &self.directions, self.room)?;
        Ok(Entry { key: Key(key), cut })
    }
}

/// The values a key of ascending columns was made from; `None` for bytes that are no such key.
pub(super) fn decode(mut key: &[u8]) -> Option<Vec<Value>> {
    let whole = |bytes: [u8; 8]| i64::from_be_bytes(bytes) ^ i64::MIN;
    let mut values = Vec::new();
    while let Some([tag]) = take(&mut key) {
        values.push(match tag {
            0xFE => Value::Null,
            1 => Value::Bool(take::<1>(&mut key)? != [0]),
            2 => Value::Int64(whole(take(&mut key)?)),
            3 => {
                let ordered = u64::from_be_bytes(take(&mut key)?);
                let bits = if ordered >> 63 == 0 {
                    !ordered
                } else {
                    ordered ^ (1 << 63)
                };
                Value::Float64(f64::from_bits(bits))
            }
            4 => {
                let mut text = Vec::new();
                loop {
                    match take(&mut key)? {
                        [0] => match take(&mut key)? {
                            [0] => break,
                            [0xFF] => text.push(0),
                            _ => return None,
                        },
                        [byte] => text.push(byte),
                    }
                }
                Value::Text(String::from_utf8(text).ok()?)
            }
            5 => Value::Uuid(Uuid::from_bytes(take(&mut key)?)),
            6 => Value::Timestamp(whole(take(&mut key)?)),
            7 => Value::TxId(TxId(u64::from_be_bytes(take(&mut key)?))),
            _ => return None,
        });
    }
    Some(values)
}

/// The next bytes of a key, `None` when it is cut short.
fn take<const N: usize>(key: &mut &[u8]) -> Option<[u8; N]> {
    let (taken, rest) = key.split_first_chunk()?;
    *key = rest;
    Some(*taken)
}
