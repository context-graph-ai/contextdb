//! Checks of the pure parts of the store: the order keys take as bytes, the span a pair of
//! bounds stands for, and a row written and read back.

use std::ops::Bound;

use super::{Cell, Key, keys, record};
use crate::types::{SortDirection, TxId, Uuid, Value};

fn key(values: &[Value]) -> Key {
    Key::from_values(values, &[]).expect("these values can be part of a key")
}

/// A key of two columns, the second one descending.
fn second_descending(first: &Value, second: &Value) -> Key {
    let directions = [SortDirection::Asc, SortDirection::Desc];
    Key::from_values(&[first.clone(), second.clone()], &directions)
        .expect("these values can be part of a key")
}

fn text(text: &str) -> Value {
    Value::Text(text.to_owned())
}

#[test]
fn keys_compare_as_their_values_do_with_null_last() {
    // Each list is in rising order of value, with NULL after every value.
    let rising = [
        vec![
            Value::Int64(i64::MIN),
            Value::Int64(-1),
            Value::Int64(0),
            Value::Int64(i64::MAX),
            Value::Null,
        ],
        vec![
            Value::Float64(f64::NEG_INFINITY),
            Value::Float64(-1.5),
            Value::Float64(0.0),
            Value::Float64(f64::MIN_POSITIVE),
            Value::Float64(f64::INFINITY),
            Value::Float64(f64::NAN),
            Value::Null,
        ],
        vec![
            text(""),
            text("a"),
            text("a\0"),
            text("a\0b"),
            text("ab"),
            text("b"),
            Value::Null,
        ],
        vec![Value::Bool(false), Value::Bool(true), Value::Null],
        vec![Value::Timestamp(-1), Value::Timestamp(1), Value::Null],
        vec![
            Value::Uuid(Uuid::from_u128(1)),
            Value::Uuid(Uuid::from_u128(u128::MAX)),
            Value::Null,
        ],
        vec![Value::TxId(TxId(1)), Value::TxId(TxId(256)), Value::Null],
    ];
    for values in rising {
        let keys: Vec<Key> = values.iter().map(std::slice::from_ref).map(key).collect();
        assert!(keys.is_sorted(), "{values:?}");
        // The second column decides only between equal first columns.
        let first = key(&[values[0].clone(), Value::Int64(9)]);
        let second = key(&[values[1].clone(), Value::Int64(0)]);
        assert!(first < second, "{values:?}");

        // A descending column is the same order backwards: NULL first, then the greatest.
        let mut falling: Vec<Key> = values
            .iter()
            .map(|value| second_descending(&values[0], value))
            .collect();
        falling.reverse();
        assert!(falling.is_sorted(), "{values:?}, descending");
    }

    // The two zeros are one key, and so is every NaN.
    assert_eq!(key(&[Value::Float64(-0.0)]), key(&[Value::Float64(0.0)]));
    assert_eq!(
        key(&[Value::Float64(-f64::NAN)]),
        key(&[Value::Float64(f64::NAN)])
    );
    assert!(Key::from_values(&[Value::Vector(vec![1.0])], &[]).is_err());
    assert!(Key::from_values(&[Value::Json(serde_json::Value::Null)], &[]).is_err());
}

#[test]
fn a_bound_of_fewer_columns_stands_for_every_key_that_begins_with_it() {
    let inside = |from: Bound<&Key>, to: Bound<&Key>, values: &[Value]| {
        let (start, end) = keys::span(from, to);
        (start..end).contains(&key(values).0)
    };
    let k = key(&[text("k")]);
    let rows = [
        (vec![text("j"), Value::Int64(9)], false, true, false),
        (vec![text("k")], true, false, false),
        (vec![text("k"), Value::Null], true, false, false),
        (vec![text("k"), Value::Int64(9)], true, false, false),
        (vec![text("k\0"), Value::Int64(0)], false, false, true),
        (vec![text("l")], false, false, true),
    ];
    for (values, within, before, after) in rows {
        let both = inside(Bound::Included(&k), Bound::Included(&k), &values);
        assert_eq!(both, within, "{values:?}");
        let below = inside(Bound::Unbounded, Bound::Excluded(&k), &values);
        assert_eq!(below, before, "{values:?}");
        let above = inside(Bound::Excluded(&k), Bound::Unbounded, &values);
        assert_eq!(above, after, "{values:?}");
        // The same keys lie from the bound to the end of what begins with it.
        let end = k.prefix_end().expect("a key of one text has an end");
        let to_end = inside(Bound::Included(&k), Bound::Excluded(&end), &values);
        assert_eq!(to_end, within, "{values:?}");
    }
    assert!(key(&[]).prefix_end().is_none());
    // Bounds that cross hold nothing.
    let (start, end) = keys::span(Bound::Excluded(&k), Bound::Excluded(&k));
    assert_eq!(start, end);
}

#[test]
fn a_row_reads_back_as_written_with_its_large_values_beside_it() {
    let long = "long text, ".repeat(400);
    let values = vec![
        Value::Null,
        Value::Bool(true),
        Value::Int64(i64::MIN),
        Value::Float64(f64::MIN_POSITIVE),
        text("it's \"quoted\"\0 \u{65e5}\u{672c}"),
        Value::Uuid(Uuid::from_u128(7)),
        Value::Timestamp(-1),
        // A number that the JSON package reads back wrongly unless it is told to be exact.
        Value::Json(serde_json::json!({ "k": [1, -1.1193133179981887e-17, "x", null] })),
        Value::Vector(vec![f32::MAX, f32::MIN_POSITIVE, -1.0]),
        Value::TxId(TxId(u64::MAX)),
        text(&long),
        Value::Vector(vec![0.25; 384]),
    ];

    let mut kept = Vec::new();
    let row = record::encode_row(&values, |large| {
        kept.push(large.to_vec());
        Ok::<_, ()>(kept.len() as u64 - 1)
    })
    .expect("nothing refuses");
    assert_eq!(kept.len(), 2, "the long text and the long vector");
    assert!(row.len() < 400, "the row holds {} bytes", row.len());

    let cells = record::decode_row(&row);
    let read: Vec<Value> = cells
        .into_iter()
        .map(|cell| match cell {
            Cell::Here(value) => value,
            Cell::Beside(beside) => {
                let large = &kept[beside.place as usize];
                assert_eq!(beside.bytes, large.len() as u64);
                record::decode_value(large)
            }
        })
        .collect();
    assert_eq!(read, values);
}
