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

use super::Key;
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
