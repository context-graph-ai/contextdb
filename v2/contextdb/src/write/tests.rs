//! Tests of this part's two pure functions: reading a moment, and bringing a value to a type.

use serde_json::json;

use super::values::{timestamp_millis, typed};
use crate::types::{ColumnDef, ColumnType, Error, Uuid, Value};

#[test]
fn an_iso_8601_moment_is_read_as_milliseconds_since_1970_utc() {
    assert_eq!(timestamp_millis("1970-01-01T00:00:00Z"), Some(0));
    assert_eq!(timestamp_millis("1970-01-01T00:00:01Z"), Some(1_000));
    assert_eq!(timestamp_millis("1969-12-31T23:59:59.5Z"), Some(-500));
    assert_eq!(
        timestamp_millis("2024-02-29T12:00:00Z"),
        Some(1_709_208_000_000)
    );
    // One moment written with three offsets, the last with digits past the millisecond.
    let new_year = 1_767_225_600_000;
    assert_eq!(timestamp_millis("2026-01-01T00:00:00Z"), Some(new_year));
    assert_eq!(
        timestamp_millis("2026-01-01T02:00:00+02:00"),
        Some(new_year)
    );
    assert_eq!(
        timestamp_millis("2025-12-31T19:00:00.123456-05:00"),
        Some(new_year + 123)
    );

    for not_a_moment in [
        "",
        "2026-01-01",
        "2026-01-01T00:00:00",
        "2026-01-01 00:00:00Z",
        "2026-13-01T00:00:00Z",
        "2025-02-29T00:00:00Z",
        "2026-01-01T24:00:00Z",
        "2026-01-01T00:00:00.Z",
        "2026-01-01T00:00:00+0200",
        "+026-01-01T00:00:00Z",
        "yesterday at about ten",
    ] {
        assert_eq!(timestamp_millis(not_a_moment), None, "{not_a_moment:?}");
    }
}

#[test]
fn a_value_is_brought_to_its_columns_type_or_refused() {
    let typed = |column_type, value| {
        let column = ColumnDef {
            name: "c".to_owned(),
            column_type,
            nullable: true,
            primary_key: false,
            unique: false,
            default: None,
        };
        typed("t", &column, value)
    };
    let text = |text: &str| Value::Text(text.to_owned());

    // The documented conversions, and a value that already is of the type.
    for (column_type, given, stored) in [
        (ColumnType::Real, Value::Int64(2), Value::Float64(2.0)),
        (
            ColumnType::Uuid,
            text("550E8400-E29B-41D4-A716-446655440000"),
            Value::Uuid(Uuid::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000)),
        ),
        (
            ColumnType::Timestamp,
            text("1970-01-01T00:00:01Z"),
            Value::Timestamp(1_000),
        ),
        (
            ColumnType::Json,
            text(r#"[1, "two"]"#),
            Value::Json(json!([1, "two"])),
        ),
        (
            ColumnType::Vector(3),
            text("[1, 2.5, -3]"),
            Value::Vector(vec![1.0, 2.5, -3.0]),
        ),
        (ColumnType::Text, text("kept"), text("kept")),
        (ColumnType::Integer, Value::Null, Value::Null),
    ] {
        assert_eq!(
            typed(column_type, given).expect("a value that fits"),
            stored
        );
    }

    // Anything else is refused by the column's name, and says what was expected and given.
    for (column_type, given, expected, actual) in [
        (ColumnType::Integer, text("three"), "INTEGER", "Text"),
        (
            ColumnType::Integer,
            Value::Float64(1.5),
            "INTEGER",
            "Float64",
        ),
        (ColumnType::Text, Value::Int64(1), "TEXT", "Int64"),
        (ColumnType::Timestamp, Value::Int64(5), "TIMESTAMP", "Int64"),
        (ColumnType::Uuid, text("not a uuid"), "UUID", "Text"),
        (ColumnType::Json, text("not json"), "JSON", "Text"),
        (ColumnType::Vector(3), text("[1, 2]"), "VECTOR(3)", "Text"),
        (
            ColumnType::Vector(3),
            Value::Vector(vec![1.0]),
            "VECTOR(3)",
            "Vector(1)",
        ),
    ] {
        match typed(column_type, given) {
            Err(Error::ColumnTypeMismatch {
                table,
                column,
                expected: said_expected,
                actual: said_actual,
            }) => assert_eq!(
                (&*table, &*column, &*said_expected, &*said_actual),
                ("t", "c", expected, actual)
            ),
            other => panic!("a {actual} for a {expected} column gave {other:?}"),
        }
    }
}
