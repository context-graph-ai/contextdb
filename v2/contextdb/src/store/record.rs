//! How a row and its values are written as bytes.
//!
//! A row is its cells one after another. A cell is one tag byte and then the value; text, JSON
//! and vectors carry their length in bytes first. A large value is written the same way, but
//! beside the row, and the row keeps a cell that says where and how many bytes.

use super::{Beside, Cell};
use crate::types::{TxId, Uuid, Value};

/// A text, JSON or vector value that takes more bytes than this is kept beside its row.
const BESIDE_FROM: usize = 1024;

const NULL: u8 = 0;
const BOOL: u8 = 1;
const INT64: u8 = 2;
const FLOAT64: u8 = 3;
const TEXT: u8 = 4;
const UUID: u8 = 5;
const TIMESTAMP: u8 = 6;
const JSON: u8 = 7;
const VECTOR: u8 = 8;
const TXID: u8 = 9;
const BESIDE: u8 = 10;

/// Writes a row. Each large value is handed, already written, to `beside`, which keeps it and
/// answers with its place.
pub(super) fn encode_row<E>(
    values: &[Value],
    mut beside: impl FnMut(&[u8]) -> Result<u64, E>,
) -> Result<Vec<u8>, E> {
    let mut row = Vec::new();
    for value in values {
        let at = row.len();
        put_value(&mut row, value);
        let grows = matches!(value, Value::Text(_) | Value::Json(_) | Value::Vector(_));
        if grows && row.len() - at > BESIDE_FROM {
            let bytes = (row.len() - at) as u64;
            let place = beside(&row[at..])?;
            row.truncate(at);
            row.push(BESIDE);
            row.extend(bytes.to_le_bytes());
            row.extend(place.to_le_bytes());
        }
    }
    Ok(row)
}

fn put_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.push(NULL),
        Value::Bool(flag) => out.extend([BOOL, u8::from(*flag)]),
        Value::Int64(number) => put(out, INT64, &number.to_le_bytes()),
        Value::Float64(number) => put(out, FLOAT64, &number.to_bits().to_le_bytes()),
        Value::Text(text) => put_long(out, TEXT, text.as_bytes()),
        Value::Uuid(id) => put(out, UUID, id.as_bytes()),
        Value::Timestamp(millis) => put(out, TIMESTAMP, &millis.to_le_bytes()),
        Value::Json(json) => {
            let text = serde_json::to_vec(json).expect("a JSON value always writes as text");
            put_long(out, JSON, &text);
        }
        Value::Vector(numbers) => {
            put(out, VECTOR, &(4 * numbers.len() as u64).to_le_bytes());
            for number in numbers {
                out.extend(number.to_le_bytes());
            }
        }
        Value::TxId(id) => put(out, TXID, &id.0.to_le_bytes()),
    }
}

fn put(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    out.push(tag);
    out.extend(bytes);
}

fn put_long(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    put(out, tag, &(bytes.len() as u64).to_le_bytes());
    out.extend(bytes);
}

/// Reads a row back. The bytes are a row this module wrote: the storage library checks every
/// page it reads, so nothing else reaches here.
pub(super) fn decode_row(mut row: &[u8]) -> Vec<Cell> {
    let mut cells = Vec::new();
    while !row.is_empty() {
        let tag = take(&mut row, 1)[0];
        cells.push(if tag == BESIDE {
            Cell::Beside(Beside {
                bytes: u64::from_le_bytes(take_array(&mut row)),
                place: u64::from_le_bytes(take_array(&mut row)),
            })
        } else {
            Cell::Here(take_value(tag, &mut row))
        });
    }
    cells
}

/// Reads back one value that was kept beside its row.
pub(super) fn decode_value(bytes: &[u8]) -> Value {
    take_value(bytes[0], &mut &bytes[1..])
}

fn take_value(tag: u8, bytes: &mut &[u8]) -> Value {
    match tag {
        NULL => Value::Null,
        BOOL => Value::Bool(take(bytes, 1)[0] != 0),
        INT64 => Value::Int64(i64::from_le_bytes(take_array(bytes))),
        FLOAT64 => Value::Float64(f64::from_bits(u64::from_le_bytes(take_array(bytes)))),
        TEXT => {
            let text = String::from_utf8(take_long(bytes).to_vec());
            Value::Text(text.expect("stored text was written from a string"))
        }
        UUID => Value::Uuid(Uuid::from_bytes(take_array(bytes))),
        TIMESTAMP => Value::Timestamp(i64::from_le_bytes(take_array(bytes))),
        JSON => {
            let json = serde_json::from_slice(take_long(bytes));
            Value::Json(json.expect("stored JSON was written from a JSON value"))
        }
        VECTOR => {
            let numbers = take_long(bytes).chunks_exact(4);
            Value::Vector(
                numbers
                    .map(|number| f32::from_le_bytes(std::array::from_fn(|at| number[at])))
                    .collect(),
            )
        }
        // TXID, the one tag of a value that is left.
        _ => Value::TxId(TxId(u64::from_le_bytes(take_array(bytes)))),
    }
}

fn take<'a>(bytes: &mut &'a [u8], count: usize) -> &'a [u8] {
    let (taken, rest) = bytes.split_at(count);
    *bytes = rest;
    taken
}

fn take_array<const N: usize>(bytes: &mut &[u8]) -> [u8; N] {
    let taken = take(bytes, N);
    std::array::from_fn(|at| taken[at])
}

fn take_long<'a>(bytes: &mut &'a [u8]) -> &'a [u8] {
    let count = u64::from_le_bytes(take_array(bytes));
    take(bytes, count as usize)
}
