//! How a row and its values are written as bytes.
//!
//! A row is its cells one after another. A cell is one tag byte and then the value; text, JSON
//! and vectors carry their length in bytes first, as a `u64`. That is a value's record. A
//! text, JSON or vector value whose record is more than 1,024 bytes is kept beside the row, and
//! the row keeps a cell that says where and how many bytes are stored for it.
//!
//! What is stored for a large value is its record in one of two forms, and the first byte says
//! which. The plain form is the record itself. The compressed form is the tag with its top bit
//! set, the same eight bytes of length, and one LZ4 block that decodes to exactly that many
//! bytes and runs to the end. A text or JSON value of at most 1 MiB is stored compressed when
//! that saves an eighth of its bytes; a longer one and a vector are always stored plain, and a
//! value that stays in its row is never touched. Either form reads back as the same value.

use lz4_flex::block::{compress_into, decompress_into, get_maximum_output_size};

use super::{Beside, Cell};
use crate::memory::Reservation;
use crate::types::{self, TxId, Uuid, Value};

/// A text, JSON or vector value that takes more bytes than this is kept beside its row.
const BESIDE_FROM: usize = 1024;

/// The tag and the length in front of a text, JSON or vector value.
const FRONT: usize = 9;

/// Set in the first byte of a stored record whose value is one LZ4 block.
const COMPRESSED: u8 = 0x80;

/// The longest text or JSON value that is compressed. Reading a compressed value holds its
/// stored bytes and its real bytes together for a moment, so this keeps that extra under 1 MiB
/// and a longer value reads holding no more than its own bytes.
const COMPRESS_TO: usize = 1 << 20;

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

/// Writes a row. The record of each large value is handed to `beside`, which may put the
/// record's other form in its place, keeps it, and answers with its place. The row's cell
/// holds the length of what was kept.
pub(super) fn encode_row<E>(
    values: &[Value],
    mut beside: impl FnMut(&mut Vec<u8>) -> Result<u64, E>,
) -> Result<Vec<u8>, E> {
    let mut row = Vec::new();
    // The record of one value that can be large, so that a large one never passes through
    // the row.
    let mut record = Vec::new();
    for value in values {
        if !matches!(value, Value::Text(_) | Value::Json(_) | Value::Vector(_)) {
            put_value(&mut row, value);
            continue;
        }
        record.clear();
        put_value(&mut record, value);
        if record.len() > BESIDE_FROM {
            let place = beside(&mut record)?;
            row.push(BESIDE);
            row.extend((record.len() as u64).to_le_bytes());
            row.extend(place.to_le_bytes());
        } else {
            row.extend(&record);
        }
    }
    Ok(row)
}

/// Puts the compressed form of a large value's plain record in its place, when the value is
/// one that is compressed and that saves an eighth of its bytes.
///
/// The compressor works in a buffer of 20 bytes more than 1.1 times the value. `reservation`
/// is grown by that first, and the bytes it was grown by are returned for the caller to give
/// back once the record is written. When the budget has no room for the buffer the record
/// stays plain: compression never refuses a commit.
pub(super) fn compress(record: &mut Vec<u8>, reservation: &mut Reservation) -> u64 {
    let len = record.len() - FRONT;
    let room = get_maximum_output_size(len);
    if record[0] == VECTOR || len > COMPRESS_TO || reservation.grow(room as u64).is_err() {
        return 0;
    }
    let mut block = vec![0; room];
    // A buffer of that size holds whatever the value compresses to, so nothing fails here.
    if let Ok(size) = compress_into(&record[FRONT..], &mut block)
        && size <= len - len / 8
    {
        // The block moves up to let the tag and the length in before it. They fit: the block
        // is no longer than the value, and the buffer is at least 20 bytes longer.
        block.copy_within(..size, FRONT);
        block[..FRONT].copy_from_slice(&record[..FRONT]);
        block[0] |= COMPRESSED;
        block.truncate(FRONT + size);
        *record = block;
    }
    room as u64
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

/// Makes a large value from its stored record, in either form. `reservation` already holds
/// the record's bytes, and holds the bytes of the value's plain record when the value is made.
///
/// A compressed record is decoded first. The reservation is grown by the bytes of the plain
/// record before one of them exists, so a value that does not fit under the memory limit is
/// refused and never produced; the stored bytes are given back once it is decoded. A text
/// keeps the plain record's buffer as its own. While a JSON or vector value is made, the
/// record and the value both exist, and the reservation holds the bytes twice for that moment.
///
/// The inner error is the reason a record's own bytes do not support the length it gives:
/// the store is damaged. Nothing is allocated from such a length.
pub(super) fn load(
    mut stored: Vec<u8>,
    reservation: &mut Reservation,
) -> types::Result<Result<Value, &'static str>> {
    let Some(front) = stored.first_chunk::<FRONT>() else {
        return Ok(Err("a large value is shorter than its tag and length"));
    };
    if front[0] & COMPRESSED != 0 {
        let len = u64::from_le_bytes(take_array(&mut &front[1..]));
        // No LZ4 block decodes to more than 255 times its own bytes.
        if len > 255 * (stored.len() - FRONT) as u64 {
            return Ok(Err(
                "a compressed large value gives a length its bytes cannot decode to",
            ));
        }
        reservation.grow(FRONT as u64 + len)?;
        let mut plain = vec![0; FRONT + len as usize];
        plain[..FRONT].copy_from_slice(front);
        plain[0] &= !COMPRESSED;
        let decoded = decompress_into(&stored[FRONT..], &mut plain[FRONT..]);
        if decoded.ok() != Some(len as usize) {
            return Ok(Err("a compressed large value decodes to another length"));
        }
        reservation.shrink(stored.len() as u64);
        stored = plain;
    }
    let len = u64::from_le_bytes(take_array(&mut &stored[1..]));
    if len != (stored.len() - FRONT) as u64 {
        return Ok(Err("a large value gives a length its bytes are not"));
    }
    if stored[0] == TEXT {
        stored.drain(..FRONT);
        let text = String::from_utf8(stored).map(Value::Text);
        return Ok(text.map_err(|_| "a large text is not text"));
    }
    let twice = stored.len() as u64;
    reservation.grow(twice)?;
    let json = &stored[FRONT..];
    let value = match stored[0] {
        JSON => serde_json::from_slice(json).ok().map(Value::Json),
        VECTOR => Some(decode_value(&stored)),
        _ => None,
    };
    reservation.shrink(twice);
    Ok(value.ok_or("a large value is not text, JSON or a vector"))
}

/// Reads one value back from its plain record.
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
