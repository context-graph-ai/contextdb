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
//! that saves an eighth of its bytes; a longer one and a vector are always stored plain. Either
//! form reads back as the same value. A value that stays in its row is compressed only with it:
//! a row's record of 256 bytes or more is stored, when that saves an eighth, as the byte `0xFF`
//! (which begins no cell), its length as a `u32` and one LZ4 block.
//!
//! A vector cell that comes with a scale and codes is written in a third form, in its row or
//! beside it: the tag `0x88`, the count of its numbers as a `u64`, the scale as an `f32` and
//! one signed byte a number. It reads back as each code times the scale.

use lz4_flex::block::{compress_into, decompress_into, get_maximum_output_size};

use super::disk::{Stored, unsound};
use super::{Beside, Cell, Compact};
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
/// A vector as a scale and one signed byte a number.
const REDUCED: u8 = 0x88;
/// The first byte of a row's record that is stored as one block.
const PACKED: u8 = 0xFF;

/// The tag, the count and the scale in front of the codes of such a vector.
const REDUCED_FRONT: u64 = 13;

/// Writes a row. The record of each large value is handed to `beside`, which may put the
/// record's other form in its place, keeps it, and answers with its place. The row's cell
/// holds the length of what was kept. A cell that `compact` names is written as its scale and
/// codes.
pub(super) fn encode_row<E>(
    values: &[Value],
    compact: &[Compact],
    mut beside: impl FnMut(&mut Vec<u8>) -> Result<u64, E>,
) -> Result<Vec<u8>, E> {
    let mut row = Vec::new();
    // The record of one value that can be large, so that a large one never passes through
    // the row.
    let mut record = Vec::new();
    for (at, value) in values.iter().enumerate() {
        if !matches!(value, Value::Text(_) | Value::Json(_) | Value::Vector(_)) {
            put_value(&mut row, value);
            continue;
        }
        record.clear();
        match compact.iter().find(|cell| cell.at == at) {
            Some(cell) => {
                let count = cell.codes.len() as u64;
                put(&mut record, REDUCED, &count.to_le_bytes());
                record.extend(cell.scale.to_le_bytes());
                record.extend(cell.codes.iter().map(|code| code.cast_unsigned()));
            }
            None => put_value(&mut record, value),
        }
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
pub(super) fn compress(record: &mut Vec<u8>, reservation: &mut Reservation) -> u64 {
    if matches!(record[0], VECTOR | REDUCED) {
        return 0;
    }
    let front = [&[record[0] | COMPRESSED][..], &record[1..FRONT]].concat();
    squeeze(record, FRONT, &front, reservation)
}

/// Puts a row's record of 256 bytes or more in its stored form, when that saves an eighth.
pub(super) fn pack(row: &mut Vec<u8>, reservation: &mut Reservation) -> u64 {
    if row.len() < 256 {
        return 0;
    }
    let front = [&[PACKED][..], &(row.len() as u32).to_le_bytes()].concat();
    squeeze(row, 0, &front, reservation)
}

/// Puts `front` and one LZ4 block of the record's bytes from `from` on in its place, when they
/// are at most 1 MiB and that saves an eighth of them. The compressor's buffer (1.1 times them
/// and 20 bytes) is asked of `reservation` first and returned, to give back once the record is
/// written; without room for it the record stays plain: compression never refuses a commit.
fn squeeze(record: &mut Vec<u8>, from: usize, front: &[u8], reservation: &mut Reservation) -> u64 {
    let len = record.len() - from;
    let room = get_maximum_output_size(len);
    if len > COMPRESS_TO || reservation.grow(room as u64).is_err() {
        return 0;
    }
    let mut block = vec![0; room];
    // A buffer of that size holds whatever the bytes compress to, so nothing fails here.
    if let Ok(size) = compress_into(&record[from..], &mut block)
        && front.len() + size <= record.len() - len / 8
    {
        // The block moves up to let the front in; the buffer is 11 bytes longer than both.
        block.copy_within(..size, front.len());
        block[..front.len()].copy_from_slice(front);
        block.truncate(front.len() + size);
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

/// A stored row's record as it was written, the one way a stored row is read; a block that does
/// not decode to its length is damage. Like a plain row, it is not counted against the limit.
pub(super) fn plain(row: &[u8]) -> Stored<std::borrow::Cow<'_, [u8]>> {
    let Some(([PACKED, len @ ..], block)) = row.split_first_chunk::<5>() else {
        return Ok(row.into());
    };
    let plain = unpack(&[], block, u32::from_le_bytes(*len).into(), |_| Ok(()));
    Ok(plain.ok().flatten().ok_or_else(unsound)?.into())
}

/// Reads a row back from its plain record. The bytes are a row this module wrote: the storage
/// library checks every page it reads, and `plain` every block, so nothing else reaches here.
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

/// Steps over the first cell of a stored row without making its value, and answers with the
/// large value the cell keeps beside the row, when it keeps one.
fn skip_cell(row: &mut &[u8]) -> Option<Beside> {
    let width = match take(row, 1)[0] {
        NULL => 0,
        BOOL => 1,
        UUID => 16,
        TEXT | JSON | VECTOR => u64::from_le_bytes(take_array(row)) as usize,
        REDUCED => u64::from_le_bytes(take_array(row)) as usize + 4,
        BESIDE => {
            return Some(Beside {
                bytes: u64::from_le_bytes(take_array(row)),
                place: u64::from_le_bytes(take_array(row)),
            });
        }
        _ => 8,
    };
    take(row, width);
    None
}

/// The bytes of a stored row from its cell at `slot` on: none for a row with no cell there,
/// which was written before the column was added.
fn from_cell(mut row: &[u8], slot: usize) -> &[u8] {
    for _ in 0..slot {
        if !row.is_empty() {
            skip_cell(&mut row);
        }
    }
    row
}

/// The large values a row's plain record keeps beside it. None of the row's values is made.
pub(super) fn besides(mut row: &[u8]) -> impl Iterator<Item = Beside> {
    std::iter::from_fn(move || {
        while !row.is_empty() {
            if let Some(kept) = skip_cell(&mut row) {
                return Some(kept);
            }
        }
        None
    })
}

/// A row's plain record without its cell at `slot`, and the large value that cell kept beside
/// the row. `None` for a row with no cell there.
pub(super) fn without_cell(row: &[u8], slot: usize) -> Option<(Vec<u8>, Option<Beside>)> {
    let mut rest = from_cell(row, slot);
    let start = row.len() - rest.len();
    if rest.is_empty() {
        return None;
    }
    let kept = skip_cell(&mut rest);
    Some(([&row[..start], rest].concat(), kept))
}

/// Says again, in a row just written, where each large value that was left where it is is
/// kept. `left` names them by cell, and NULL was written in the cell of each.
pub(super) fn leave(row: &mut Vec<u8>, left: &[(usize, Beside)]) {
    for (slot, kept) in left {
        let at = row.len() - from_cell(row, *slot).len();
        let place = [kept.bytes.to_le_bytes(), kept.place.to_le_bytes()];
        row.splice(at..=at, [&[BESIDE][..], place.as_flattened()].concat());
    }
}

/// Makes a large value from its stored record, in either form. `reservation` already holds
/// the record's bytes, and holds the bytes of the value's plain record when the value is made.
///
/// A compressed record is decoded first. The reservation is grown by the bytes of the plain
/// record before one of them exists, so a value that does not fit under the memory limit is
/// refused and never produced; the stored bytes are given back once it is decoded. A text
/// keeps the plain record's buffer as its own. A JSON tree is counted, before it is made, as
/// the most its text can make. While a vector is made, the record and the vector both exist,
/// and the reservation holds the bytes twice for that moment.
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
    if front[0] == REDUCED {
        let count = u64::from_le_bytes(take_array(&mut &front[1..]));
        if (stored.len() as u64).checked_sub(REDUCED_FRONT) != Some(count) {
            return Ok(Err("a reduced vector gives a length its bytes are not"));
        }
        // The numbers are four bytes each where the record has one.
        reservation.grow(4 * count)?;
        let value = decode_value(&stored);
        reservation.shrink(REDUCED_FRONT - FRONT as u64 + count);
        return Ok(Ok(value));
    }
    if front[0] & COMPRESSED != 0 {
        let len = u64::from_le_bytes(take_array(&mut &front[1..]));
        let grow = |bytes| reservation.grow(bytes);
        let Some(mut plain) = unpack(front, &stored[FRONT..], len, grow)? else {
            return Ok(Err(
                "a compressed large value does not decode to the length it gives",
            ));
        };
        plain[0] &= !COMPRESSED;
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
    if stored[0] == JSON {
        // The tree is counted at the most its text can make before it is made, and is held
        // by the reservation as long as the value is.
        reservation.grow(types::json_bytes(&stored[FRONT..]))?;
        let json = serde_json::from_slice(&stored[FRONT..]).map(Value::Json);
        return Ok(json.map_err(|_| "a large JSON value is not JSON"));
    }
    let twice = stored.len() as u64;
    reservation.grow(twice)?;
    let value = (stored[0] == VECTOR).then(|| decode_value(&stored));
    reservation.shrink(twice);
    Ok(value.ok_or("a large value is not text, JSON or a vector"))
}

/// `front` and the `len` bytes an LZ4 block decodes to, or `None`. `grow` is asked for them before
/// they are made, and only when no more than 255 times the block, the most a block decodes to.
fn unpack(
    front: &[u8],
    block: &[u8],
    len: u64,
    grow: impl FnOnce(u64) -> types::Result<()>,
) -> types::Result<Option<Vec<u8>>> {
    if len > 255 * block.len() as u64 {
        return Ok(None);
    }
    grow(front.len() as u64 + len)?;
    let mut plain = vec![0; front.len() + len as usize];
    plain[..front.len()].copy_from_slice(front);
    let decoded = decompress_into(block, &mut plain[front.len()..]);
    Ok((decoded.ok() == Some(len as usize)).then_some(plain))
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
        REDUCED => {
            let count = u64::from_le_bytes(take_array(bytes));
            let scale = f32::from_le_bytes(take_array(bytes));
            let codes = take(bytes, count as usize).iter();
            Value::Vector(
                codes
                    .map(|code| scale * f32::from(code.cast_signed()))
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
