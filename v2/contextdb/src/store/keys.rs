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
use crate::types::{Error, Result, SortDirection, Value};

/// No value's form begins with this byte, turned or not, so a key followed by it sorts after
/// every key that begins with that key and before every other key above it.
const PAST: u8 = 0xFF;

/// The bytes from the first, which a span includes, to the second, which it does not.
pub(super) type Span = (Vec<u8>, Vec<u8>);

/// The form [`Key::from_values`] gives.
pub(super) fn encode(values: &[Value], directions: &[SortDirection]) -> Result<Vec<u8>> {
    let mut key = Vec::new();
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
                // a text sorts before every longer text it begins.
                for byte in text.bytes() {
                    key.push(byte);
                    if byte == 0 {
                        key.push(0xFF);
                    }
                }
                key.extend([0, 0]);
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
    Ok(key)
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
const PENDING: u8 = 2;

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
        _ => {
            let partition = number(5)?;
            key.0.extend_from_slice(&stored[9..]);
            VectorRecord::Pending(column, partition, key)
        }
    })
}
