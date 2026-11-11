//! Checks of the pure parts of the store: the order keys take as bytes, the span a pair of
//! bounds stands for, and a row written and read back. And the store's own check of a
//! compressed value's recorded length: the storage library checks every page it reads, so a
//! length changed on its way from the file never reaches that check, and the tests of damage
//! here write the wrong bytes into the data file as a commit of the storage library: a wrong
//! length, a large value with a slice missing, and a block that decodes to the recorded length
//! of bytes that are no value of its kind.

use std::ops::Bound;
use std::path::{Path, PathBuf};

use lz4_flex::block::{compress_into, get_maximum_output_size};
use redb::ReadableTable;

use super::{
    Cell, ChangeSet, Compact, Key, Read, Store, TableId, VectorId, VectorRecord, keys, record,
};
use crate::memory::Budget;
use crate::types::{Error, SortDirection, TxId, Uuid, Value};

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
    let row = record::encode_row(&values, &[], |large| {
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

/// The storage library's table of large values, as `disk` declares it: place to slice.
const BESIDE: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("beside");

#[test]
fn a_compressed_value_whose_recorded_length_is_wrong_is_damage_and_takes_no_memory() {
    const MIB: u64 = 1 << 20;
    // 16 KiB of one sentence, which the store keeps compressed in a block of about a hundredth
    // of that.
    const LEN: u64 = 16 << 10;
    let folder =
        std::env::temp_dir().join(format!("contextdb-wrong-length-{}", std::process::id()));
    if folder.exists() {
        std::fs::remove_dir_all(&folder).expect("clear the folder an earlier run left");
    }
    let sentence = "A store keeps what it is given and hands it back as it was. ";
    let body: String = sentence.chars().cycle().take(LEN as usize).collect();
    let (table, row_key) = (TableId(1), key(&[Value::Int64(1)]));

    // One row whose text goes beside it, written under no limit so that it is compressed.
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(&folder, &budget, None).expect("open a fresh store");
    let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
    let row = vec![Value::Int64(1), text(&body)];
    changes
        .put_row(table, row_key.clone(), row, Vec::new(), true)
        .expect("stage");
    store.commit(changes).expect("commit");
    drop(store);

    // Opens the store under a limit that leaves `room` bytes once it is open, and loads the
    // text. Returns what the load gave, where the value is kept, the bytes the statement
    // holds after the load, and whether what the budget counts is back where it was once
    // the statement has let go.
    let load = |room: u64| {
        let budget = Budget::new(Some(9 * MIB + room)).expect("a budget");
        let store = Store::open_for_writing(&folder, &budget, None).expect("open the store");
        let view = store.view().expect("a view");
        let row = view.row(table, &row_key).expect("read the row");
        let Some(Cell::Beside(beside)) = row.expect("the row is there").cells.pop() else {
            panic!("a text of {LEN} bytes was kept in its row");
        };
        let before = budget.used();
        let mut reservation = budget.reserve(0, "query").expect("a reservation");
        let loaded = view.load(&beside, &mut reservation);
        let held = reservation.bytes();
        drop(reservation);
        (loaded, beside, held, budget.used() == before)
    };
    let (loaded, beside, held, given_back) = load(MIB);
    assert!(loaded.expect("load the value") == text(&body));
    assert_eq!(held, 9 + LEN, "held for the loaded value");
    assert!(given_back, "counted once the value is let go");
    // The compressed record: nine bytes, then one block.
    let block = beside.bytes - 9;
    assert!(
        beside.bytes < LEN / 50,
        "{} bytes are stored for a repeating text of {LEN}: it is not compressed",
        beside.bytes
    );
    // The room every load below is given: what reading the value takes at its most, the
    // stored record and the value's own record together, one byte and a little more. No
    // block decodes to more than 255 times its own bytes, and that much does not fit.
    let room = beside.bytes + 9 + LEN + 1 + 64;
    assert!(LEN <= 255 * block && 9 + 255 * block > room);

    // Writes this length where the record keeps its own, as a commit of the storage library,
    // which then hands the slice over as a page that passes its check.
    let record_length = |wrong: u64| {
        let data = redb::Database::open(folder.join("data")).expect("open the data file");
        let write = data.begin_write().expect("begin");
        {
            let mut slices = write.open_table(BESIDE).expect("the large values");
            let first = slices.get(beside.place).expect("read the first slice");
            let mut first = first.expect("the first slice is there").value().to_vec();
            assert_eq!(first[0], 0x84, "the first byte of a compressed text");
            first[1..9].copy_from_slice(&wrong.to_le_bytes());
            slices
                .insert(beside.place, first.as_slice())
                .expect("write the first slice");
        }
        write.commit().expect("commit");
    };
    // The outcome of a load that must make no value: the reason of the store's damage, which
    // names the folder and the large value, or `None` for the memory limit's refusal. Either
    // way the statement holds nothing afterwards and nothing stays counted.
    let damage = |wrong: u64| -> Option<String> {
        record_length(wrong);
        let (loaded, _, held, given_back) = load(room);
        assert_eq!(held, 0, "held after a recorded length of {wrong}");
        assert!(given_back, "counted after a recorded length of {wrong}");
        match loaded {
            Err(Error::StoreCorrupted { path, reason }) => {
                assert_eq!(path, folder, "a recorded length of {wrong}");
                assert!(reason.contains("large value"), "{wrong}: {reason}");
                Some(reason)
            }
            Err(Error::MemoryBudgetExceeded { operation, .. }) => {
                assert_eq!(operation, "query", "a recorded length of {wrong}");
                None
            }
            other => panic!("a recorded length of {wrong} gave {other:?}"),
        }
    };

    // The value reads under that room as it is.
    let (loaded, _, held, _) = load(room);
    assert!(loaded.expect("load the value under the room") == text(&body));
    assert_eq!(held, 9 + LEN);

    // A length the block cannot decode to, from one more than 255 times its bytes to the
    // largest number there is, is damage before any memory is asked for. The limit has room
    // for none of them: a length that was asked for would be the limit's refusal, and one
    // that was taken without asking would end the test.
    for wrong in [255 * block + 1, 1 << 40, 1 << 62, u64::MAX - 8, u64::MAX] {
        assert!(
            damage(wrong).is_some(),
            "memory was asked for a recorded length of {wrong}"
        );
    }
    // The most a wrong length can ask for is 255 times the block, and it is asked for before
    // it is taken: with no room for it, it is the limit's refusal, as for any value that
    // does not fit.
    assert!(
        damage(255 * block).is_none(),
        "no memory was asked for a recorded length of 255 times the block"
    );
    // A length there is room for, and that the block does not decode to, is damage once the
    // block is decoded: one byte more than the value has, one byte less, half of it, none.
    for wrong in [LEN + 1, LEN - 1, LEN / 2, 0] {
        assert!(
            damage(wrong).is_some(),
            "a recorded length of {wrong} was refused for want of room"
        );
    }

    // With its own length back, the value is as it was written.
    record_length(LEN);
    let (loaded, _, held, _) = load(room);
    assert!(loaded.expect("load the value") == text(&body));
    assert_eq!(held, 9 + LEN);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

/// The bytes of one slice of a large value, as `disk` cuts them.
const SLICE: u64 = 243;

/// The one row of the stores the tests below write: table 1, key 1.
fn the_row() -> (TableId, Key) {
    (TableId(1), key(&[Value::Int64(1)]))
}

/// A folder of this name that holds nothing, for one test.
fn fresh_folder(name: &str) -> PathBuf {
    let folder = std::env::temp_dir().join(format!("contextdb-{name}-{}", std::process::id()));
    if folder.exists() {
        std::fs::remove_dir_all(&folder).expect("clear the folder an earlier run left");
    }
    folder
}

/// Writes a store of one row, the number 1 and then `value`, under no memory limit, so that a
/// value the store compresses is compressed, and closes it.
fn store_one_row(folder: &Path, value: Value) {
    let (table, row_key) = the_row();
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, None).expect("open a fresh store");
    let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
    let row = vec![Value::Int64(1), value];
    changes
        .put_row(table, row_key, row, Vec::new(), true)
        .expect("stage");
    store.commit(changes).expect("commit");
}

/// Opens the store and loads the large value of its one row. Returns what the load gave, where
/// the value is kept, the bytes the statement holds after the load, and whether what the budget
/// counts is back where it was once the statement has let go.
fn load_the_value(folder: &Path) -> (crate::types::Result<Value>, super::Beside, u64, bool) {
    let (table, row_key) = the_row();
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, None).expect("open the store");
    let view = store.view().expect("a view");
    let row = view.row(table, &row_key).expect("read the row");
    let Some(Cell::Beside(beside)) = row.expect("the row is there").cells.pop() else {
        panic!("the value was kept in its row");
    };
    let before = budget.used();
    let mut reservation = budget.reserve(0, "query").expect("a reservation");
    let loaded = view.load(&beside, &mut reservation);
    let held = reservation.bytes();
    drop(reservation);
    (loaded, beside, held, budget.used() == before)
}

/// Asserts that loading the large value of the store's one row is the store's damage, naming
/// the folder, and that the statement then holds nothing and nothing stays counted. `what`
/// says what was done to the value.
fn loading_is_damage(folder: &Path, what: &str) {
    let (loaded, _, held, given_back) = load_the_value(folder);
    match loaded {
        Err(Error::StoreCorrupted { path, .. }) => assert_eq!(path, folder, "{what}"),
        Ok(Value::Text(text)) => panic!("{what} was read as a text of {} bytes", text.len()),
        Ok(_) => panic!("{what} was read as a value"),
        Err(other) => panic!("{what} gave {other:?}"),
    }
    assert_eq!(held, 0, "held after the refusal: {what}");
    assert!(given_back, "counted after the refusal: {what}");
}

/// Letters in a fixed scatter, which no compressor makes shorter, so that a text or JSON value
/// of them is stored plain.
fn scattered(count: usize) -> String {
    const LETTERS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let letters = (0..count).map(|_| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        char::from(LETTERS[(state >> 58) as usize])
    });
    letters.collect()
}

/// Stores `value`, whose plain record is nine bytes and then `plain` more and which is stored
/// as that, removes the last of its slices as a commit of the storage library, and asserts that
/// reading the value is then damage: the row still says how many bytes are stored for the
/// value, and the slices that are left hold fewer.
fn a_value_without_its_last_slice_is_damage(name: &str, value: &Value, plain: usize) {
    let folder = fresh_folder(name);
    store_one_row(&folder, value.clone());
    let (loaded, beside, _, _) = load_the_value(&folder);
    assert!(loaded.expect("load the value as it was written") == *value);
    assert_eq!(beside.bytes, 9 + plain as u64, "the value is stored plain");
    let slices = beside.bytes.div_ceil(SLICE);
    assert!(slices > 1 && beside.bytes % SLICE != 0, "{slices} slices");

    let data = redb::Database::open(folder.join("data")).expect("open the data file");
    let write = data.begin_write().expect("begin");
    {
        let mut kept = write.open_table(BESIDE).expect("the large values");
        let last = kept.remove(beside.place + slices - 1);
        assert!(last.expect("remove the last slice").is_some());
    }
    write.commit().expect("commit");
    drop(data);

    loading_is_damage(&folder, "a large value whose last slice is gone");
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_plain_text_whose_slices_hold_fewer_bytes_than_its_row_records_is_damage() {
    let letters = scattered(2000);
    a_value_without_its_last_slice_is_damage("short-text", &text(&letters), 2000);
}

#[test]
fn a_plain_json_whose_slices_hold_fewer_bytes_than_its_row_records_is_damage() {
    // A JSON text is its letters between two quotation marks.
    let json = Value::Json(serde_json::Value::String(scattered(2000)));
    a_value_without_its_last_slice_is_damage("short-json", &json, 2002);
}

#[test]
fn a_vector_whose_slices_hold_fewer_bytes_than_its_row_records_is_damage() {
    let numbers: Vec<f32> = (0..384_u16).map(|n| f32::from(n) / 8.0).collect();
    a_value_without_its_last_slice_is_damage("short-vector", &Value::Vector(numbers), 4 * 384);
}

/// Stores `value`, which the store compresses, and puts in the place of its block, as a commit
/// of the storage library, the block of `wrong`: as many bytes as the value has, which are no
/// value of its kind. The stored record keeps its tag, its recorded length and its own number
/// of bytes, so the block decodes to exactly the length the record gives. Asserts that reading
/// the value is then damage.
fn a_block_of_other_bytes_is_damage(name: &str, value: &Value, wrong: &[u8]) {
    let folder = fresh_folder(name);
    store_one_row(&folder, value.clone());
    let (loaded, beside, _, _) = load_the_value(&folder);
    assert!(loaded.expect("load the value as it was written") == *value);

    let data = redb::Database::open(folder.join("data")).expect("open the data file");
    let write = data.begin_write().expect("begin");
    {
        let mut kept = write.open_table(BESIDE).expect("the large values");
        let first = kept.get(beside.place).expect("read the first slice");
        let first = first.expect("the first slice is there").value().to_vec();
        assert!(first[0] & 0x80 != 0, "the value is stored compressed");
        let recorded = u64::from_le_bytes(std::array::from_fn(|at| first[1 + at]));
        assert_eq!(recorded, wrong.len() as u64, "the length the record gives");

        // The same tag and length, and then the block of the wrong bytes.
        let mut record = vec![0; 9 + get_maximum_output_size(wrong.len())];
        record[..9].copy_from_slice(&first[..9]);
        let block = compress_into(wrong, &mut record[9..]).expect("the buffer holds any block");
        record.truncate(9 + block);
        assert_eq!(
            record.len() as u64,
            beside.bytes,
            "the wrong bytes make a record as long as the value's"
        );
        for (place, slice) in (beside.place..).zip(record.chunks(SLICE as usize)) {
            kept.insert(place, slice).expect("write a slice");
        }
    }
    write.commit().expect("commit");
    drop(data);

    loading_is_damage(&folder, "a block that decodes to bytes of another kind");
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_compressed_text_that_decodes_to_bytes_that_are_not_text_is_damage() {
    const LEN: usize = 16 << 10;
    // No text holds the byte 0xFF.
    a_block_of_other_bytes_is_damage("not-text", &text(&"a".repeat(LEN)), &[0xFF; LEN]);
}

#[test]
fn a_compressed_json_that_decodes_to_bytes_that_are_not_json_is_damage() {
    const LEN: usize = 16 << 10;
    // The value is one JSON text; the wrong bytes have a brace where each of its quotation
    // marks is, which is no JSON.
    let json = Value::Json(serde_json::Value::String("a".repeat(LEN - 2)));
    let wrong = format!("{{{}}}", "a".repeat(LEN - 2));
    a_block_of_other_bytes_is_damage("not-json", &json, wrong.as_bytes());
}

#[test]
fn a_key_gives_its_values_back() {
    let lists = [
        vec![],
        vec![Value::Bool(false), Value::Bool(true)],
        vec![
            Value::Int64(i64::MIN),
            Value::Int64(-1),
            Value::Int64(i64::MAX),
        ],
        vec![
            Value::Float64(-1.5),
            Value::Float64(0.0),
            Value::Float64(f64::INFINITY),
        ],
        vec![
            text(""),
            text("a"),
            text("a\0b\0"),
            text("\u{65e5}\u{672c}"),
        ],
        vec![Value::Uuid(Uuid::from_u128(7)), Value::Timestamp(-1)],
        vec![
            Value::TxId(TxId(u64::MAX)),
            Value::Null,
            text("after a NULL"),
        ],
    ];
    for values in lists {
        let made = key(&values);
        assert_eq!(made.values(), Some(values.clone()), "{values:?}");
        assert_eq!(key(&made.values().expect("the values")), made);
    }
    // Bytes that are no key of ascending columns: an unknown tag, and a value cut short.
    assert_eq!(Key::from_bytes(&[0x7F]).values(), None);
    for whole in [key(&[Value::Int64(5)]), key(&[text("cut")])] {
        let bytes = whole.as_bytes();
        let short = Key::from_bytes(&bytes[..bytes.len() - 1]);
        assert_eq!(short.values(), None, "{bytes:?} without its last byte");
    }
}

/// `count` numbers that are each a whole code from -127 to 127 times one scale, with the
/// scale and the codes.
fn reduced(count: usize) -> (Vec<f32>, f32, Vec<i8>) {
    let scale = 0.031_25_f32;
    let codes: Vec<i8> = (0..count)
        .map(|at| ((at % 255) as i16 - 127) as i8)
        .collect();
    let numbers = codes.iter().map(|code| scale * f32::from(*code)).collect();
    (numbers, scale, codes)
}

/// Writes a store of rows 1 to `rows`, each the number and then one vector of `count` reduced
/// numbers, handed to the store with its scale and codes when `compact`. Returns the bytes of
/// the data file once the store is closed.
fn store_vectors(folder: &Path, rows: i64, count: usize, compact: bool) -> u64 {
    let (numbers, scale, codes) = reduced(count);
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, None).expect("open a fresh store");
    let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
    for id in 1..=rows {
        let row = vec![Value::Int64(id), Value::Vector(numbers.clone())];
        let kept = if compact {
            vec![Compact {
                at: 1,
                scale,
                codes: codes.clone(),
            }]
        } else {
            Vec::new()
        };
        changes
            .put_row(TableId(1), key(&[Value::Int64(id)]), row, kept, true)
            .expect("stage");
    }
    store.commit(changes).expect("commit");
    drop(store);
    std::fs::metadata(folder.join("data"))
        .expect("the data file")
        .len()
}

#[test]
fn a_reduced_vector_reads_back_number_for_number_in_a_quarter_of_the_room() {
    let folder = fresh_folder("reduced-vector");
    // 4,000 rows, so that both files are well past the size a data file starts at.
    let compact = store_vectors(&folder, 4000, 768, true);
    // 781 bytes stay in the row, and read back as exactly the numbers that were put.
    let (numbers, ..) = reduced(768);
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(&folder, &budget, None).expect("open the store");
    let view = store.view().expect("a view");
    for id in [1, 250, 500] {
        let row = view.row(TableId(1), &key(&[Value::Int64(id)]));
        let row = row.expect("read the row").expect("the row is there");
        match &row.cells[1] {
            Cell::Here(Value::Vector(read)) => {
                let bits = |numbers: &[f32]| -> Vec<u32> {
                    numbers.iter().map(|number| number.to_bits()).collect()
                };
                assert_eq!(bits(read), bits(&numbers), "row {id}");
            }
            other => panic!("the vector of row {id} was read as {other:?}"),
        }
    }
    drop((view, store));
    let plain_folder = fresh_folder("reduced-vector-plain");
    let plain = store_vectors(&plain_folder, 4000, 768, false);
    assert!(
        3 * compact < plain,
        "4,000 vectors of 768 numbers take {compact} bytes of file reduced and {plain} as written"
    );
    for folder in [folder, plain_folder] {
        std::fs::remove_dir_all(&folder).expect("remove the test's folder");
    }
}

#[test]
fn a_reduced_record_of_another_length_is_damage() {
    // 2,000 reduced numbers are a record of 2,013 bytes, kept beside the row.
    const COUNT: usize = 2000;
    let folder = fresh_folder("reduced-length");
    store_vectors(&folder, 1, COUNT, true);
    let (loaded, beside, _, _) = load_the_value(&folder);
    assert!(loaded.expect("load the value as it was written") == Value::Vector(reduced(COUNT).0));
    assert_eq!(
        beside.bytes,
        13 + COUNT as u64,
        "the record of a reduced vector"
    );

    // The record now says it holds one number more than its bytes do.
    let data = redb::Database::open(folder.join("data")).expect("open the data file");
    let write = data.begin_write().expect("begin");
    {
        let mut kept = write.open_table(BESIDE).expect("the large values");
        let first = kept.get(beside.place).expect("read the first slice");
        let mut first = first.expect("the first slice is there").value().to_vec();
        assert_eq!(first[0], 0x88, "the tag of a reduced vector");
        assert_eq!(first[1..9], (COUNT as u64).to_le_bytes());
        first[1..9].copy_from_slice(&(COUNT as u64 + 1).to_le_bytes());
        kept.insert(beside.place, first.as_slice())
            .expect("write the slice");
    }
    write.commit().expect("commit");
    drop(data);

    loading_is_damage(&folder, "a reduced vector whose count is not its bytes'");
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn the_records_of_a_grouping_keep_their_keys_apart() {
    use VectorRecord::*;
    let column = VectorId(3);
    let (tuple, row) = (key(&[text("a")]), key(&[Value::Int64(1)]));
    // A partition numbered 5 and its grouping numbered 6: every kind of record of the two.
    let records = [
        Partition(column, &tuple),
        Pending(column, 5, &row),
        Block(column, 5, 0),
        Block(column, 6, 0),
        Grouping(column, 6),
        Centres(column, 6, 0),
        Build(column, 5),
        Assigned(column, 6, 0),
        Sketch(column, 5, 0),
        Sketch(column, 6, 0),
        Mean(column, 5),
        Left(column, 5, &row),
    ];
    let stored = records.map(keys::vector);
    let apart: std::collections::BTreeSet<&Vec<u8>> = stored.iter().collect();
    assert_eq!(apart.len(), records.len(), "two records share a key");
    for (record, stored) in records.iter().zip(&stored) {
        let mut held = key(&[]);
        assert_eq!(keys::vector_of(stored, &mut held), Some(*record));
        // Blocks, centres and sketches are kept together; everything else is small.
        let large = matches!(record, Block(..) | Centres(..) | Sketch(..));
        assert_eq!(keys::with_blocks(stored[0]), large, "{record:?}");
    }
    // The sketches of a grouping's blocks come in the order of the blocks, before the next
    // number's.
    let sketch = |under, block| keys::vector(Sketch(column, under, block));
    assert!(sketch(6, 0) < sketch(6, 1) && sketch(6, 1) < sketch(7, 0));
}
