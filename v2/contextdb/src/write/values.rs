//! What a written value has to be before it is stored: of its column's type, by one of the few
//! documented conversions or not at all.

use std::ops::Range;

use crate::types::{ColumnDef, ColumnType, Error, Result, Uuid, Value};

/// Brings a value to its column's type, or refuses it with `ColumnTypeMismatch`.
///
/// NULL fits every type here; whether the column takes it is the NOT NULL rule's business. The
/// only conversions are quoted text into a UUID, TIMESTAMP, JSON or VECTOR column and an INTEGER
/// into a REAL column.
pub(super) fn typed(table: &str, column: &ColumnDef, value: Value) -> Result<Value> {
    let converted = match (column.column_type, &value) {
        (_, Value::Null)
        | (ColumnType::Integer, Value::Int64(_))
        | (ColumnType::Real, Value::Float64(_))
        | (ColumnType::Text, Value::Text(_))
        | (ColumnType::Boolean, Value::Bool(_))
        | (ColumnType::Json, Value::Json(_))
        | (ColumnType::Uuid, Value::Uuid(_))
        | (ColumnType::Timestamp, Value::Timestamp(_)) => return Ok(value),
        (ColumnType::Vector(length), Value::Vector(vector)) if vector.len() == length => {
            return Ok(value);
        }
        (ColumnType::Real, Value::Int64(whole)) => Some(Value::Float64(*whole as f64)),
        (ColumnType::Uuid, Value::Text(text)) => Uuid::parse_str(text).ok().map(Value::Uuid),
        (ColumnType::Timestamp, Value::Text(text)) => timestamp_millis(text).map(Value::Timestamp),
        (ColumnType::Json, Value::Text(text)) => serde_json::from_str(text).ok().map(Value::Json),
        (ColumnType::Vector(length), Value::Text(text)) => vector_from_text(text)
            .filter(|vector| vector.len() == length)
            .map(Value::Vector),
        _ => None,
    };
    converted.ok_or_else(|| Error::ColumnTypeMismatch {
        table: table.to_owned(),
        column: column.name.clone(),
        expected: format!("{:?}", column.column_type).to_uppercase(),
        actual: kind(&value),
    })
}

/// What a refusal calls the value it was given. A vector says how long it is, since its length
/// is what a VECTOR column refuses it for.
fn kind(value: &Value) -> String {
    match value {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int64(_) => "Int64",
        Value::Float64(_) => "Float64",
        Value::Text(_) => "Text",
        Value::Uuid(_) => "Uuid",
        Value::Timestamp(_) => "Timestamp",
        Value::Json(_) => "Json",
        Value::Vector(vector) => return format!("Vector({})", vector.len()),
        Value::TxId(_) => "TxId",
    }
    .to_owned()
}

/// Reads an ISO 8601 moment with its offset (`2026-01-01T02:00:00+02:00`,
/// `1970-01-01T00:00:00.250Z`) as milliseconds since the first moment of 1970, UTC. Digits past
/// the millisecond are dropped. `None` when the text is not such a moment.
pub(super) fn timestamp_millis(text: &str) -> Option<i64> {
    let (year, month, day) = (
        digits(text, 0..4)?,
        digits(text, 5..7)?,
        digits(text, 8..10)?,
    );
    let (hour, minute, second) = (
        digits(text, 11..13)?,
        digits(text, 14..16)?,
        digits(text, 17..19)?,
    );
    let marks = text.as_bytes();
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = match month {
        2 => 28 + i64::from(leap),
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if [marks[4], marks[7], marks[13], marks[16]] != *b"--::"
        || !matches!(marks[10], b'T' | b't')
        || !(1..=12).contains(&month)
        || !(1..=month_days).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }

    let mut rest = &text[19..];
    let mut millis = 0;
    if let Some(fraction) = rest.strip_prefix('.') {
        let end = fraction
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(fraction.len());
        if end == 0 {
            return None;
        }
        millis = format!("{:0<3}", &fraction[..end.min(3)]).parse().ok()?;
        rest = &fraction[end..];
    }
    let offset_minutes = match rest.as_bytes() {
        b"Z" | b"z" => 0,
        [sign @ (b'+' | b'-'), _, _, b':', _, _] => {
            let minutes = digits(rest, 1..3)? * 60 + digits(rest, 4..6)?;
            if *sign == b'-' { -minutes } else { minutes }
        }
        _ => return None,
    };

    // Days since 1970 of a date in the Gregorian calendar, counting years from March so that
    // the leap day is the last day of its year.
    let (year, month) = if month > 2 {
        (year, month - 3)
    } else {
        (year - 1, month + 9)
    };
    let leap_days = year.div_euclid(4) - year.div_euclid(100) + year.div_euclid(400);
    let days = year * 365 + leap_days + (153 * month + 2) / 5 + day - 719_469;
    Some(((days * 24 + hour) * 60 + minute - offset_minutes) * 60_000 + second * 1_000 + millis)
}

/// The number written in these positions of the text, when every one of them is a digit.
fn digits(text: &str, positions: Range<usize>) -> Option<i64> {
    let part = text.get(positions)?;
    if part.bytes().all(|byte| byte.is_ascii_digit()) {
        part.parse().ok()
    } else {
        None
    }
}

/// Reads the text spelling of a vector, `[0.1, 0.2, 0.3]`.
fn vector_from_text(text: &str) -> Option<Vec<f32>> {
    let inside = text.trim().strip_prefix('[')?.strip_suffix(']')?;
    inside
        .split(',')
        .map(|number| number.trim().parse().ok())
        .collect()
}
