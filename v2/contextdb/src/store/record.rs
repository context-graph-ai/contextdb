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

use std::io::Write;

use lz4_flex::block::{compress_into, decompress_into, get_maximum_output_size};

use super::disk::{Stored, unsound};
use super::{Beside, Cell, Compact, Row};
use crate::memory::{Passing, Reservation};
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
pub(super) const WRITTEN: u8 = 11;
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
pub(super) fn encode_row<E: From<types::Error>>(
    values: &[Value],
    compact: &[Compact],
    (written, left): (Option<u32>, &[(usize, Beside)]),
    cover: &mut Passing,
    mut beside: impl FnMut(&mut Vec<u8>) -> Result<u64, E>,
) -> Result<Vec<u8>, E> {
    let width = |at, value: &Value| {
        compact.iter().find(|c| c.at == at).map_or_else(
            || {
                let mut n = Length(0);
                put_value(&mut n, value);
                n.0
            },
            |c| REDUCED_FRONT as usize + c.codes.len(),
        )
    };
    let (size, largest) = values.iter().enumerate().fold(
        (5 * usize::from(written.is_some()), 0),
        |(size, largest), (at, value)| {
            let len = width(at, value);
            let kept = left.iter().any(|(slot, _)| *slot == at);
            (
                size + if kept || len > BESIDE_FROM { 17 } else { len },
                largest.max(if kept { 0 } else { len }),
            )
        },
    );
    cover.cover((size + largest) as u64)?;
    let mut row = Vec::with_capacity(size);
    if let Some(written) = written {
        put(&mut row, WRITTEN, &written.to_le_bytes());
    }
    let mut record = Vec::with_capacity(largest);
    for (at, value) in values.iter().enumerate() {
        if let Some((_, kept)) = left.iter().find(|(slot, _)| *slot == at) {
            put(&mut row, BESIDE, &kept.bytes.to_le_bytes());
            row.extend(kept.place.to_le_bytes());
            continue;
        }
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

/// Delivery framing: opaque identity, then an inline body or the same address a row keeps.
pub(crate) fn delivery_parts(bytes: &[u8]) -> Stored<(&[u8], &[u8], Option<Beside>)> {
    let (length, bytes) = bytes.split_first_chunk::<4>().ok_or_else(unsound)?;
    let (identity, mut body) = bytes
        .split_at_checked(u32::from_le_bytes(*length) as usize)
        .ok_or_else(unsound)?;
    if !matches!(body.first(), Some(&NULL | &BESIDE)) {
        return Err(unsound());
    }
    let kept = skip_cell(&mut body).ok_or_else(unsound)?;
    if kept.is_some() && !body.is_empty() {
        return Err(unsound());
    }
    Ok((identity, body, kept))
}

pub(super) fn keep_delivery(
    bytes: &mut Vec<u8>,
    save: impl FnOnce(&[u8]) -> Stored<u64>,
) -> Stored<()> {
    let (identity, body, _) = delivery_parts(bytes)?;
    if body.len() > BESIDE_FROM {
        let (at, length, place) = (4 + identity.len(), body.len() as u64, save(body)?);
        bytes.truncate(at);
        bytes.push(BESIDE);
        bytes.extend(length.to_le_bytes());
        bytes.extend(place.to_le_bytes());
    }
    Ok(())
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

pub(crate) struct Length(pub(crate) usize);
impl Write for Length {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn put_value(out: &mut impl Write, value: &Value) {
    match value {
        Value::Null => out.write_all(&[NULL]).unwrap(),
        Value::Bool(flag) => out.write_all(&[BOOL, u8::from(*flag)]).unwrap(),
        Value::Int64(number) => put(out, INT64, &number.to_le_bytes()),
        Value::Float64(number) => put(out, FLOAT64, &number.to_bits().to_le_bytes()),
        Value::Text(text) => put_long(out, TEXT, text.as_bytes()),
        Value::Uuid(id) => put(out, UUID, id.as_bytes()),
        Value::Timestamp(millis) => put(out, TIMESTAMP, &millis.to_le_bytes()),
        Value::Json(json) => {
            let mut len = Length(0);
            serde_json::to_writer(&mut len, json).expect("a JSON value writes as text");
            put(out, JSON, &(len.0 as u64).to_le_bytes());
            serde_json::to_writer(out, json).expect("a JSON value writes as text");
        }
        Value::Vector(numbers) => {
            put(out, VECTOR, &(4 * numbers.len() as u64).to_le_bytes());
            for number in numbers {
                out.write_all(&number.to_le_bytes()).unwrap();
            }
        }
        Value::TxId(id) => put(out, TXID, &id.0.to_le_bytes()),
    }
}

/// Serialized size, measured by the same encoder without allocating another value.
pub(crate) fn value_len(value: &Value) -> usize {
    let mut count = Length(0);
    put_value(&mut count, value);
    count.0
}
pub(crate) fn put_text(out: &mut Vec<u8>, text: &str) {
    put_long(out, TEXT, text.as_bytes());
}

fn put(out: &mut impl Write, tag: u8, bytes: &[u8]) {
    out.write_all(&[tag]).unwrap();
    out.write_all(bytes).unwrap();
}

fn put_long(out: &mut impl Write, tag: u8, bytes: &[u8]) {
    put(out, tag, &(bytes.len() as u64).to_le_bytes());
    out.write_all(bytes).unwrap();
}

/// A stored row's record as it was written, the one way a stored row is read; a block that does
/// not decode to its length is damage. Its buffer is admitted before expansion.
pub(super) fn plain<'a>(row: &'a [u8], cover: &mut Passing) -> Stored<std::borrow::Cow<'a, [u8]>> {
    let Some(([PACKED, len @ ..], block)) = row.split_first_chunk::<5>() else {
        return Ok(row.into());
    };
    let len = u64::from(u32::from_le_bytes(*len));
    if !(256..=COMPRESS_TO as u64).contains(&len) {
        return Err(unsound());
    }
    Ok(unpack(&[], block, len, |bytes| cover.cover(bytes))?
        .ok_or_else(unsound)?
        .into())
}

/// Reads cells only after their lengths, tags and payloads have been checked.
pub(super) fn decode_record(row: &[u8], cover: &mut Passing) -> Stored<Row> {
    let row = plain(row, cover)?;
    let (written, cells) = match row.split_first_chunk::<5>() {
        Some(([WRITTEN, s @ ..], rest)) => (Some(u32::from_le_bytes(*s)), rest),
        _ => (None, row.as_ref()),
    };
    let (count, bytes) = super::most(cells).ok_or_else(unsound)?;
    let bytes = bytes + row.len() as u64 * u64::from(matches!(row, std::borrow::Cow::Owned(_)));
    cover.cover(bytes)?;
    Ok(Row {
        cells: decode_row(cells, count)?,
        written,
        bytes,
    })
}

pub(super) fn decode_row(mut row: &[u8], count: usize) -> Stored<Vec<Cell>> {
    let mut cells = Vec::with_capacity(count);
    while !row.is_empty() {
        let tag = take(&mut row, 1).ok_or_else(unsound)?[0];
        let cell = if tag == BESIDE {
            Cell::Beside(Beside {
                bytes: u64::from_le_bytes(take_array(&mut row).ok_or_else(unsound)?),
                place: u64::from_le_bytes(take_array(&mut row).ok_or_else(unsound)?),
            })
        } else {
            Cell::Here(take_value(tag, &mut row).ok_or_else(unsound)?)
        };
        cells.push(cell);
    }
    Ok(cells)
}

/// Walks the framing without allocating, bounding the cells and their decoded heap together.
pub(crate) fn most(mut row: &[u8]) -> Option<(usize, u64)> {
    let (mut count, mut bytes) = (0, size_of::<Row>() as u64);
    while !row.is_empty() {
        let cell = row;
        skip_cell(&mut row)?;
        let cell = &cell[..cell.len() - row.len()];
        let heap = match cell[0] {
            TEXT | VECTOR => (cell.len() - FRONT) as u64,
            JSON => 2 * types::json_bytes(&cell[FRONT..]) + (cell.len() - FRONT) as u64,
            REDUCED => 4 * (cell.len() - REDUCED_FRONT as usize) as u64,
            _ => 0,
        };
        bytes += size_of::<Cell>() as u64 + heap;
        count += 1;
    }
    Some((count, bytes))
}

/// Steps over one checked cell, returning its external value's address if present.
fn skip_cell(row: &mut &[u8]) -> Option<Option<Beside>> {
    let tag = take(row, 1)?[0];
    let width = match tag {
        NULL => 0,
        BOOL => 1,
        WRITTEN => 4,
        UUID => 16,
        TEXT | JSON | VECTOR => usize::try_from(u64::from_le_bytes(take_array(row)?)).ok()?,
        REDUCED => usize::try_from(u64::from_le_bytes(take_array(row)?))
            .ok()?
            .checked_add(4)?,
        BESIDE => {
            return Some(Some(Beside {
                bytes: u64::from_le_bytes(take_array(row)?),
                place: u64::from_le_bytes(take_array(row)?),
            }));
        }
        INT64 | FLOAT64 | TIMESTAMP | TXID => 8,
        _ => return None,
    };
    take(row, width)?;
    Some(None)
}

fn from_cell(mut row: &[u8], slot: usize) -> Stored<&[u8]> {
    if row.first() == Some(&WRITTEN) {
        skip_cell(&mut row).ok_or_else(unsound)?;
    }
    for _ in 0..slot {
        if !row.is_empty() {
            skip_cell(&mut row).ok_or_else(unsound)?;
        }
    }
    Ok(row)
}

/// A row without one cell, and the external value removed with it.
pub(super) fn without_cell(row: &[u8], slot: usize) -> Stored<Option<(Vec<u8>, Option<Beside>)>> {
    let mut rest = from_cell(row, slot)?;
    let start = row.len() - rest.len();
    if rest.is_empty() {
        return Ok(None);
    }
    let kept = skip_cell(&mut rest).ok_or_else(unsound)?;
    Ok(Some(([&row[..start], rest].concat(), kept)))
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
        let count = u64::from_le_bytes(front[1..].try_into().expect("a checked front"));
        if (stored.len() as u64).checked_sub(REDUCED_FRONT) != Some(count) {
            return Ok(Err("a reduced vector gives a length its bytes are not"));
        }
        // The numbers are four bytes each where the record has one.
        reservation.grow(4 * count)?;
        let value = decode_value(&stored);
        reservation.shrink(REDUCED_FRONT - FRONT as u64 + count);
        return Ok(value.ok_or("a reduced vector is not a value"));
    }
    if front[0] & COMPRESSED != 0 {
        let len = u64::from_le_bytes(front[1..].try_into().expect("a checked front"));
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
    let len = u64::from_le_bytes(stored[1..FRONT].try_into().expect("a checked front"));
    if len != (stored.len() - FRONT) as u64 {
        return Ok(Err("a large value gives a length its bytes are not"));
    }
    if stored[0] == TEXT {
        stored.drain(..FRONT);
        let text = String::from_utf8(stored).map(Value::Text);
        return Ok(text.map_err(|_| "a large text is not text"));
    }
    let bytes = stored.len() as u64;
    let value = if matches!(stored[0], JSON | VECTOR) {
        read_value(&mut stored.as_slice(), reservation)?
    } else {
        None
    };
    drop(stored);
    reservation.shrink(bytes);
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

/// Reads one complete standalone value.
pub(super) fn decode_value(mut bytes: &[u8]) -> Option<Value> {
    let tag = take(&mut bytes, 1)?[0];
    take_value(tag, &mut bytes).filter(|_| bytes.is_empty())
}

pub(crate) fn read_value(
    bytes: &mut &[u8],
    held: &mut Reservation,
) -> types::Result<Option<Value>> {
    if bytes.first() == Some(&REDUCED) {
        return Ok(None);
    }
    let before = held.bytes();
    let value = read_cell(bytes, |cell, _| {
        // The encoded buffer and Value slot are already held by this caller.
        held.grow(match cell[0] {
            JSON => types::json_bytes(&cell[FRONT..]),
            TEXT | VECTOR => (cell.len() - FRONT) as u64,
            _ => 0,
        })
    })?;
    if let Some(value) = &value {
        held.shrink(held.bytes() - before - types::heap_bytes(value));
    }
    Ok(value)
}

/// The shared value codec used by rows and stored declarations.
pub(crate) fn value_prefix(bytes: &mut &[u8], cover: &mut Passing) -> types::Result<Option<Value>> {
    read_cell(bytes, |_, bound| cover.cover(bound))
}

fn read_cell(
    bytes: &mut &[u8],
    admit: impl FnOnce(&[u8], u64) -> types::Result<()>,
) -> types::Result<Option<Value>> {
    let cell = *bytes;
    let mut rest = cell;
    if skip_cell(&mut rest).is_none() {
        return Ok(None);
    }
    let cell = &cell[..cell.len() - rest.len()];
    let Some((_, bound)) = most(cell) else {
        return Ok(None);
    };
    admit(cell, bound)?;
    let tag = take(bytes, 1).expect("a checked cell")[0];
    Ok(take_value(tag, bytes))
}

fn take_value(tag: u8, bytes: &mut &[u8]) -> Option<Value> {
    Some(match tag {
        NULL => Value::Null,
        BOOL => match take(bytes, 1)?[0] {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            _ => return None,
        },
        INT64 => Value::Int64(i64::from_le_bytes(take_array(bytes)?)),
        FLOAT64 => Value::Float64(f64::from_le_bytes(take_array(bytes)?)),
        TEXT => Value::Text(take_text(bytes)?),
        UUID => Value::Uuid(Uuid::from_bytes(take_array(bytes)?)),
        TIMESTAMP => Value::Timestamp(i64::from_le_bytes(take_array(bytes)?)),
        JSON => Value::Json(serde_json::from_slice(take_long(bytes)?).ok()?),
        VECTOR => {
            let numbers = take_long(bytes)?;
            if !numbers.len().is_multiple_of(4) {
                return None;
            }
            Value::Vector(
                numbers
                    .chunks_exact(4)
                    .map(|n| f32::from_le_bytes(n.try_into().expect("four bytes")))
                    .collect(),
            )
        }
        REDUCED => {
            let count = usize::try_from(u64::from_le_bytes(take_array(bytes)?)).ok()?;
            let scale = f32::from_le_bytes(take_array(bytes)?);
            Value::Vector(
                take(bytes, count)?
                    .iter()
                    .map(|c| scale * f32::from(c.cast_signed()))
                    .collect(),
            )
        }
        TXID => Value::TxId(TxId(u64::from_le_bytes(take_array(bytes)?))),
        _ => return None,
    })
}

/// A stored text's characters, copied out in a function of its own: inside [`take_value`] the
/// copy's allocation was inlined or not as the code around it grew, which every row pays for.
#[inline(never)]
fn take_text(bytes: &mut &[u8]) -> Option<String> {
    Some(std::str::from_utf8(take_long(bytes)?).ok()?.to_owned())
}

fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Option<&'a [u8]> {
    let (taken, rest) = bytes.split_at_checked(count)?;
    *bytes = rest;
    Some(taken)
}

fn take_array<const N: usize>(bytes: &mut &[u8]) -> Option<[u8; N]> {
    take(bytes, N)?.try_into().ok()
}

fn take_long<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
    let count = usize::try_from(u64::from_le_bytes(take_array(bytes)?)).ok()?;
    take(bytes, count)
}
