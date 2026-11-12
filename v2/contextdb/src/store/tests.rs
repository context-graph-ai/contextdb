//! Checks of the pure parts of the store: the order keys take as bytes, the span a pair of
//! bounds stands for, and a row written and read back. And the store's own checks of stored
//! bytes that disagree with themselves: the storage library checks every page it reads, so these
//! tests write the wrong bytes into the data file as a commit of the storage library, which no
//! public call can: a large value with a slice missing, a block that decodes to the recorded
//! length of bytes that are no value of its kind, a reduced vector of another length, a kept
//! count, and an index entry with the place its first long value is cut at. And the one rule by
//! which an index entry is made: never longer than the limit, in the order of its values, and
//! the same bytes every time.

use std::ops::Bound;
use std::path::{Path, PathBuf};

use lz4_flex::block::{compress_into, get_maximum_output_size};
use redb::{ReadableDatabase, ReadableTable};

use super::{
    Cell, ChangeSet, Compact, CountId, Dropped, EntryShape, IndexId, KEY_LIMIT, Key, Read, Store,
    TableId, VectorId, VectorRecord, keys, record,
};
use crate::memory::Budget;
use crate::types::{ColumnType, Error, Result, SortDirection, TxId, Uuid, Value};

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
fn a_row_reads_back_as_written_packed_or_not_with_its_large_values_beside_it() {
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

    // Through a store. A row's record (35 bytes, the text, and a large text beside the row) of
    // 256 bytes or more is kept as one block when that saves an eighth; a shorter one, and one
    // that does not shrink, as is.
    let (same, scatter, long) = ("packs well, ".repeat(100), scattered(600), scattered(2000));
    let cases = [
        (&same[..220], false),
        (&same[..221], true),
        (&scatter[..], false),
    ];
    let folder = fresh_folder("packed-rows");
    let put = |changes: &mut ChangeSet, id: i64, cells: Vec<Cell>, new_key: bool| {
        let key = key(&[Value::Int64(id)]);
        let staged = changes.put_row(TableId(1), key, cells, Vec::new(), new_key);
        staged.expect("stage");
    };
    let here = |id: i64, middle: &str| {
        vec![
            Cell::Here(Value::Int64(id)),
            Cell::Here(text(middle)),
            Cell::Here(text(&long)),
        ]
    };
    commit_staged(&folder, |changes| {
        for (id, (body, _)) in (1..).zip(cases) {
            put(changes, id, here(id, body), true);
        }
    });
    for ((_, bytes), (body, packed)) in records(&folder, ROWS).iter().zip(cases) {
        let (plain, stored) = (35 + body.len(), bytes.len());
        let kept = if packed {
            stored <= plain - plain / 8
        } else {
            stored == plain
        };
        assert!(
            kept,
            "a record of {plain} bytes (packed: {packed}) is stored as {stored}"
        );
    }
    // The large text a row's cell keeps beside it, loaded.
    let large = |read: &dyn Read, cell: &Cell| {
        let Cell::Beside(at) = cell else {
            panic!("the large text is kept beside its row")
        };
        let budget = Budget::new(None).expect("a budget");
        let loaded = read.load(at, &mut budget.reserve(0, "query").expect("a reservation"));
        assert_eq!(loaded.expect("load"), text(&long), "the large text");
    };
    let before = on_the_newest_moment(&folder, |read| {
        let scan = read.rows(TableId(1), Bound::Unbounded, Bound::Unbounded);
        let scan = scan
            .and_then(Iterator::collect::<Result<Vec<_>>>)
            .expect("scan");
        for ((key, row), (id, (body, _))) in scan.iter().zip((1..).zip(cases)) {
            let by_key = read.row(TableId(1), key).expect("read");
            assert_eq!(by_key.as_ref(), Some(row), "row {id} by key and by scan");
            assert_eq!(row.cells[..2], here(id, body)[..2], "row {id} as written");
            large(read, &row.cells[2]);
        }
        scan
    });

    // An UPDATE that does not assign the large text hands its cell back as read: the text stays
    // where it is, inside a row that is packed.
    let (second, kept) = (&before[1].0, &before[1].1.cells[2]);
    commit_staged(&folder, |changes| {
        let cells = [&here(2, &same[..300])[..2], std::slice::from_ref(kept)].concat();
        put(changes, 2, cells, false);
    });
    let after = on_the_newest_moment(&folder, |read| {
        let row = read
            .row(TableId(1), second)
            .expect("read")
            .expect("the row");
        large(read, &row.cells[2]);
        row
    });
    assert_eq!(
        after.cells[1],
        Cell::Here(text(&same[..300])),
        "the assigned cell is new"
    );
    assert_eq!(&after.cells[2], kept, "the large text is kept where it was");

    // Dropping the text column rewrites each row, packed or not, to the cells that stay.
    let drop = Dropped::Cell(TableId(1), 1);
    commit_staged(&folder, |changes| changes.stage_drop(drop).expect("stage"));
    on_the_newest_moment(&folder, |read| {
        for id in 1..=3 {
            let row = read.row(TableId(1), &key(&[Value::Int64(id)]));
            let cells = row.expect("read").expect("the row").cells;
            assert_eq!(cells[0], Cell::Here(Value::Int64(id)), "row {id}");
            assert_eq!(cells.len(), 2, "row {id} lost its text cell only");
            large(read, &cells[1]);
        }
    });
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

/// The storage library's table of large values, as `disk` declares it: place to slice.
const BESIDE: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("beside");

/// The storage library's table of rows, as `disk` declares it.
const ROWS: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("rows");

/// The bytes of one slice of a large value, as `disk` cuts them.
const SLICE: u64 = 243;

/// Changes one of the storage library's tables as a commit of that library, so the page
/// checksums are right for what it leaves.
fn tamper<K: redb::Key + 'static, V: redb::Value + 'static>(
    folder: &Path,
    table: redb::TableDefinition<K, V>,
    change: impl FnOnce(&mut redb::Table<'_, K, V>),
) {
    let data = redb::Database::open(folder.join("data")).expect("open the data file");
    let write = data.begin_write().expect("begin");
    change(&mut write.open_table(table).expect("the table"));
    write.commit().expect("commit");
}

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

/// Writes a store of this one row (table 1, key 1) with these vector cells kept reduced, under
/// no memory limit, so that a value the store compresses is compressed, and closes it.
fn store_row(folder: &Path, cells: Vec<Cell>, reduced: Vec<Compact>) {
    let (table, row_key) = the_row();
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, None).expect("open a fresh store");
    let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
    let staged = changes.put_row(table, row_key, cells, reduced, true);
    staged.expect("stage");
    store.commit(changes).expect("commit");
}

/// Writes a store of one row, the number 1 and then `value`.
fn store_one_row(folder: &Path, value: Value) {
    store_row(
        folder,
        vec![Cell::Here(Value::Int64(1)), Cell::Here(value)],
        Vec::new(),
    );
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

    tamper(&folder, BESIDE, |kept| {
        let last = kept.remove(beside.place + slices - 1);
        assert!(last.expect("remove the last slice").is_some());
    });

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

    tamper(&folder, BESIDE, |kept| {
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
    });

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

// ---- the kept count and the index entry, as records ----

/// The storage library's tables of kept counts and of index entries, as `disk` declares them.
const COUNTS: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("counts");
const INDEX: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("index");

/// Every record of a kept count, in key order, or the refusal of the read.
fn kept(read: &dyn Read, count: CountId) -> Result<Vec<(Key, u64)>> {
    read.kept(count, Bound::Unbounded, Bound::Unbounded)
        .and_then(|counts| counts.collect())
}

/// Every entry of an index, in entry order, each with its row key and the place its first long
/// value is cut at, or the refusal of the read.
fn entries(read: &dyn Read, index: IndexId) -> Result<Vec<(Key, Key, Option<usize>)>> {
    read.index(index, Bound::Unbounded, Bound::Unbounded)
        .and_then(|entries| entries.collect())
}

/// Opens the store in this folder under no memory limit and runs `read` on its newest moment.
fn on_the_newest_moment<T>(folder: &Path, read: impl FnOnce(&dyn Read) -> T) -> T {
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, None).expect("open the store");
    let view = store.view().expect("a view");
    read(&view)
}

/// Stages with `stage` on the store's newest moment and commits, or says why it could not.
fn try_commit(
    folder: &Path,
    stage: impl FnOnce(&mut ChangeSet) -> Result<()>,
) -> Result<crate::types::TxId> {
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, None).expect("open the store");
    let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
    stage(&mut changes)?;
    store.commit(changes)
}

/// Stages with `stage` on the store's newest moment and commits.
fn commit_staged(folder: &Path, stage: impl FnOnce(&mut ChangeSet)) {
    let staged = |changes: &mut ChangeSet| {
        stage(changes);
        Ok(())
    };
    try_commit(folder, staged).expect("commit");
}

/// Asserts that a read was refused as damage of the store in this folder.
fn assert_damage<T: std::fmt::Debug>(read: Result<T>, folder: &Path, what: &str) {
    match read {
        Err(Error::StoreCorrupted { path, .. }) => assert_eq!(path, folder, "{what}"),
        other => panic!("{what} was read as {other:?}"),
    }
}

/// Every record of one of the storage library's tables, as (key, value).
fn records(folder: &Path, table: redb::TableDefinition<&[u8], &[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = redb::Database::open(folder.join("data")).expect("open the data file");
    let read = data.begin_read().expect("begin");
    let table = read.open_table(table).expect("the table");
    table
        .iter()
        .expect("read the table")
        .map(|record| {
            let (key, value) = record.expect("read a record");
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect()
}

/// Writes `value` under `key` in one of the storage library's tables, as a commit of that
/// library, which then hands the record over as a page that passes its check.
fn write_record(
    folder: &Path,
    table: redb::TableDefinition<&[u8], &[u8]>,
    key: &[u8],
    value: &[u8],
) {
    tamper(folder, table, |records| {
        records.insert(key, value).expect("write the record");
    });
}

#[test]
fn a_kept_count_that_reaches_zero_leaves_no_record() {
    // No public call tells a zero record from none: the count reads 0 either way.
    let folder = fresh_folder("kept-count-zero");
    let (count, group) = (CountId(3), key(&[Value::Int64(1)]));
    commit_staged(&folder, |changes| {
        changes.add_kept(count, group.clone(), 2).expect("stage");
    });
    assert_eq!(records(&folder, COUNTS).len(), 1);
    commit_staged(&folder, |changes| {
        changes.add_kept(count, group.clone(), -2).expect("stage");
    });
    assert!(
        records(&folder, COUNTS).is_empty(),
        "a count taken down to zero keeps no record"
    );
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn a_kept_count_whose_record_is_not_eight_bytes_is_damage() {
    let folder = fresh_folder("kept-count-damaged");
    let count = CountId(1);
    let group = key(&[text("group")]);
    commit_staged(&folder, |changes| {
        changes.add_kept(count, group.clone(), 7).expect("stage");
    });
    let [(stored_key, stored_value)] = records(&folder, COUNTS)
        .try_into()
        .expect("one count was written, so one record is there");
    assert_eq!(stored_value.len(), 8, "a kept count is eight bytes");

    for wrong in [
        &stored_value[..7],
        &[stored_value.as_slice(), &[0]].concat(),
        &[],
    ] {
        write_record(&folder, COUNTS, &stored_key, wrong);
        let read = on_the_newest_moment(&folder, |view| kept(view, count));
        assert_damage(
            read,
            &folder,
            &format!("a kept count of {} bytes", wrong.len()),
        );
    }
    // With its own bytes back, the count is what was written.
    write_record(&folder, COUNTS, &stored_key, &stored_value);
    let read = on_the_newest_moment(&folder, |view| kept(view, count));
    assert_eq!(read.expect("read the count"), [(group, 7)]);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

/// The most bytes a TEXT column takes in an index entry (the engine's `ENTRY_TEXT`, which tests
/// cannot name). Every shape below leaves each of its TEXT columns at least this much.
const TEXT_ROOM: usize = 256;

/// The longest text an entry keeps whole: of its room a cut text gives 19 to its tag, its mark
/// and its digest, and a longer one is kept as this many bytes and those.
const WHOLE: usize = TEXT_ROOM - 19;

/// An index over an INTEGER and a TEXT column, both ascending.
fn integer_and_text() -> EntryShape {
    let directions = [SortDirection::Asc, SortDirection::Asc];
    EntryShape::new(&[ColumnType::Integer, ColumnType::Text], &directions)
}

/// Commits two entries of index 2: one whose text is short, for row 1, and one whose text is
/// cut, for row 2. Returns what reading the index must give back.
fn store_two_entries(folder: &Path) -> Vec<(Key, Key, Option<usize>)> {
    let shape = integer_and_text();
    let values = [
        [Value::Int64(1), text("short")],
        [Value::Int64(1), text(&"x".repeat(5000))],
    ];
    let rows = [key(&[Value::Int64(1)]), key(&[Value::Int64(2)])];
    let entry = |at: usize| shape.entry(&values[at]).expect("an entry");
    assert_eq!(entry(0).cut, None, "a short text is kept whole");
    // Nine bytes of the INTEGER, the text's tag, and the bytes kept of the text.
    let cut = Some(9 + 1 + WHOLE);
    assert_eq!(entry(1).cut, cut, "where the long text is cut");
    let wanted = vec![
        (entry(0).key, rows[0].clone(), None),
        (entry(1).key, rows[1].clone(), cut),
    ];
    commit_staged(folder, |changes| {
        for (at, row) in rows.iter().enumerate() {
            changes
                .put_index(IndexId(2), entry(at), row.clone())
                .expect("stage");
        }
        // A change set reads the entries it has staged, each with its place.
        assert_eq!(entries(changes, IndexId(2)).expect("read"), wanted);
    });
    wanted
}

#[test]
fn an_index_record_whose_value_does_not_fit_its_key_is_damage() {
    let folder = fresh_folder("index-entry-damaged");
    let wanted = store_two_entries(&folder);
    let stored = records(&folder, INDEX);
    // The whole entry records its length in four bytes; the cut one records its place as well.
    let lengths: Vec<usize> = stored.iter().map(|(_, value)| value.len()).collect();
    assert_eq!(lengths, [4, 8]);

    for (stored_key, stored_value) in &stored {
        let mut wrongs = vec![
            stored_value[..3].to_vec(),
            [stored_value.as_slice(), &[0]].concat(),
            Vec::new(),
            [stored_value.as_slice(), &[0; 8]].concat(),
        ];
        if stored_value.len() == 8 {
            // A place past the end of the entry, whichever way its four bytes are read.
            wrongs.push([&stored_value[..4], &[0xFF; 4][..]].concat());
        }
        for wrong in wrongs {
            write_record(&folder, INDEX, stored_key, &wrong);
            let read = on_the_newest_moment(&folder, |view| entries(view, IndexId(2)));
            let what = format!(
                "an index record of {} bytes where {} were written",
                wrong.len(),
                stored_value.len()
            );
            assert_damage(read, &folder, &what);
        }
        write_record(&folder, INDEX, stored_key, stored_value);
    }
    // With their own bytes back, the entries are what was written.
    let read = on_the_newest_moment(&folder, |view| entries(view, IndexId(2)));
    assert_eq!(read.expect("read the entries"), wanted);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

// ---- how an index entry is made ----

/// The 128-bit FNV-1a digest of a text, most significant byte first.
fn digest(text: &str) -> [u8; 16] {
    let mut digest: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    for byte in text.bytes() {
        digest ^= u128::from(byte);
        digest = digest.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b);
    }
    digest.to_be_bytes()
}

/// The bytes of the entry these values make.
fn entry_bytes(shape: &EntryShape, values: &[Value]) -> Vec<u8> {
    let entry = shape.entry(values).expect("an entry");
    entry.key.as_bytes().to_vec()
}

#[test]
fn an_entry_is_never_longer_than_the_limit_and_is_the_same_bytes_every_time() {
    use ColumnType::{Boolean, Integer, Text, Timestamp, Uuid as UuidType};
    use SortDirection::{Asc, Desc};
    assert_eq!(KEY_LIMIT, 2704);
    // Texts that an entry keeps whole and texts it must cut, for any share of an entry a TEXT
    // column can have: short ones, zero bytes (each is two bytes in an entry), characters of
    // several bytes, and lengths on both sides of every place a text is cut at below.
    let mut texts = vec![
        String::new(),
        "a".to_owned(),
        "a\0b".to_owned(),
        "\0".repeat(3000),
        format!("{}\0{}", "z".repeat(60), "z".repeat(5000)),
        "\u{65e5}\u{672c}".repeat(1000),
        scattered(60_000),
    ];
    for len in [
        64,
        65,
        66,
        WHOLE - 1,
        WHOLE,
        WHOLE + 1,
        TEXT_ROOM,
        KEY_LIMIT,
    ] {
        texts.push("x".repeat(len));
    }
    let other = |column_type: ColumnType| match column_type {
        Integer => Value::Int64(-7),
        Boolean => Value::Bool(true),
        Timestamp => Value::Timestamp(1),
        _ => Value::Uuid(Uuid::from_u128(7)),
    };
    let shapes: Vec<(Vec<ColumnType>, Vec<SortDirection>)> = vec![
        (vec![Text], vec![Asc]),
        (vec![Text], vec![Desc]),
        (vec![Text, Text], vec![Asc, Desc]),
        (vec![UuidType, Text, Text, Text], vec![Asc; 4]),
        (
            vec![Integer, Text, Timestamp, Text, Boolean],
            vec![Desc, Asc, Asc, Desc, Asc],
        ),
        (
            vec![UuidType, UuidType, Text, UuidType, Text, Text],
            vec![Asc; 6],
        ),
        (vec![Text; 32], vec![Asc; 32]),
    ];
    for (types, directions) in shapes {
        let shape = EntryShape::new(&types, &directions);
        // Every text in every TEXT column at once, the same text in each, then each text
        // beside the one after it, then NULL in every column.
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for at in 0..texts.len() {
            let mut next = at;
            let mut row = Vec::new();
            let mut same = Vec::new();
            for column_type in &types {
                if *column_type == Text {
                    row.push(text(&texts[next % texts.len()]));
                    same.push(text(&texts[at]));
                    next += 1;
                } else {
                    row.push(other(*column_type));
                    same.push(other(*column_type));
                }
            }
            rows.extend([row, same]);
        }
        rows.push(vec![Value::Null; types.len()]);
        for row in rows {
            let entry = shape.entry(&row).expect("an entry");
            let bytes = entry.key.as_bytes();
            assert!(
                bytes.len() <= KEY_LIMIT,
                "an entry of {} bytes over {types:?}",
                bytes.len()
            );
            // The same values give the same bytes and the same place.
            let again = shape.entry(&row).expect("an entry");
            assert!(
                bytes == again.key.as_bytes(),
                "two entries of one row differ"
            );
            assert_eq!(entry.cut, again.cut);
            let longest = row
                .iter()
                .any(|value| matches!(value, Value::Text(text) if text.len() > WHOLE));
            assert!(
                !longest || entry.cut.is_some(),
                "a text longer than any share of an entry was kept whole over {types:?}"
            );
            // Given only its leading values, the entry begins every entry with those values.
            for leading in 0..row.len() {
                let begins = entry_bytes(&shape, &row[..leading]);
                assert!(
                    bytes.starts_with(&begins),
                    "{leading} leading values of {types:?}"
                );
            }
        }
    }
}

#[test]
fn an_entry_keeps_a_text_whole_up_to_its_share_and_cuts_a_longer_one_to_its_digest() {
    // One TEXT column has the room of an entry: a text of `WHOLE` bytes is kept whole, as a key
    // keeps it, and one byte more is cut to `WHOLE` bytes, the mark and the digest of the whole
    // text.
    let shape = EntryShape::new(&[ColumnType::Text], &[SortDirection::Asc]);
    let whole = "w".repeat(WHOLE);
    let entry = shape.entry(&[text(&whole)]).expect("an entry");
    assert_eq!(entry.cut, None);
    assert!(
        entry.key == key(&[text(&whole)]),
        "a whole text is not as a key keeps it"
    );

    let long = format!("{whole}w");
    let entry = shape.entry(&[text(&long)]).expect("an entry");
    let bytes = entry.key.as_bytes();
    let kept = 1 + WHOLE;
    assert_eq!((bytes.len(), entry.cut), (TEXT_ROOM, Some(kept)));
    assert!(bytes[..kept] == key(&[text(&whole)]).as_bytes()[..kept]);
    assert_eq!(bytes[kept..kept + 2], [0, 1], "the mark of a cut text");
    assert_eq!(
        bytes[kept + 2..],
        digest(&long),
        "the digest of the whole text"
    );
    // A descending column turns every bit of the same form.
    let descending = EntryShape::new(&[ColumnType::Text], &[SortDirection::Desc]);
    let turned: Vec<u8> = bytes.iter().map(|byte| !byte).collect();
    assert!(entry_bytes(&descending, &[text(&long)]) == turned);

    // A zero byte is two bytes in an entry, and a cut never falls between the two.
    let zeros = "\0".repeat(3000);
    let entry = shape.entry(&[text(&zeros)]).expect("an entry");
    let cut = entry.cut.expect("3,000 zero bytes are cut");
    assert_eq!(
        cut,
        1 + WHOLE / 2 * 2,
        "whole zero bytes are kept, two bytes each"
    );
    assert_eq!(entry.key.as_bytes().len(), cut + 2 + 16);
    assert_eq!(entry.key.as_bytes()[cut + 2..], digest(&zeros));

    // Three TEXT columns after a UUID each have `TEXT_ROOM` too, so a text of `WHOLE` bytes is
    // whole and one byte more is cut. Two texts of 60,000 bytes after a short one make an entry
    // of two full rooms after the short one, and the place it records is that of the first cut
    // text.
    let types = [
        ColumnType::Uuid,
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Text,
    ];
    let shape = EntryShape::new(&types, &[SortDirection::Asc; 4]);
    let row = |value: &str| {
        let id = Value::Uuid(Uuid::from_u128(7));
        [id, text("command"), text(value), text(value)]
    };
    assert_eq!(
        shape.entry(&row(&"v".repeat(WHOLE))).expect("an entry").cut,
        None
    );
    let entry = shape.entry(&row(&"v".repeat(WHOLE + 1))).expect("an entry");
    assert_eq!(entry.cut, Some(17 + 10 + 1 + WHOLE));
    let entry = shape.entry(&row(&scattered(60_000))).expect("an entry");
    assert_eq!(
        (entry.key.as_bytes().len(), entry.cut),
        (17 + 10 + 2 * TEXT_ROOM, Some(17 + 10 + 1 + WHOLE))
    );
}

#[test]
fn entries_are_in_the_order_of_their_values_whole_or_cut() {
    // Rising texts, whole and cut: no two of the cut ones begin with the same kept bytes.
    let rising = [
        Value::Text(String::new()),
        text("a"),
        text("a\0"),
        text("a\0b"),
        text("ab"),
        text("b"),
        text(&"b".repeat(WHOLE)),
        text(&"b".repeat(5000)),
        text("c"),
        text(&"c".repeat(2001)),
        text(&format!("{}{}", "c".repeat(2500), "a".repeat(3000))),
        text(&format!("{}{}", "c".repeat(2000), "d".repeat(3000))),
        text("d"),
        Value::Null,
    ];
    let ascending = EntryShape::new(&[ColumnType::Text], &[SortDirection::Asc]);
    let descending = EntryShape::new(&[ColumnType::Text], &[SortDirection::Desc]);
    let up: Vec<Vec<u8>> = rising
        .iter()
        .map(|value| entry_bytes(&ascending, std::slice::from_ref(value)))
        .collect();
    assert!(
        up.windows(2).all(|pair| pair[0] < pair[1]),
        "entries of rising values do not rise"
    );
    let down: Vec<Vec<u8>> = rising
        .iter()
        .map(|value| entry_bytes(&descending, std::slice::from_ref(value)))
        .collect();
    assert!(
        down.windows(2).all(|pair| pair[0] > pair[1]),
        "entries of rising values in a descending column do not fall"
    );
    // The column after a cut text decides only between rows whose text is the same text.
    let two = EntryShape::new(
        &[ColumnType::Text, ColumnType::Integer],
        &[SortDirection::Asc, SortDirection::Asc],
    );
    let long = "b".repeat(5000);
    let longer = format!("{long}b");
    let pair = |value: &str, number: i64| entry_bytes(&two, &[text(value), Value::Int64(number)]);
    assert!(pair(&long, 1) < pair(&long, 2));
    assert!(pair("b", 9) < pair(&long, 0) && pair(&long, 9) < pair("c", 0));
    assert!(pair(&long, 1) != pair(&longer, 1));

    // Texts that begin with the same kept bytes differ by their digests alone, and follow each
    // other in the order of those.
    let alike: Vec<String> = (0..6)
        .map(|n| format!("{}{}", "m".repeat(3000), scattered(100 + n)))
        .collect();
    let mut by_entry: Vec<&String> = alike.iter().collect();
    by_entry.sort_by_key(|value| entry_bytes(&ascending, &[text(value)]));
    let mut by_digest: Vec<&String> = alike.iter().collect();
    by_digest.sort_by_key(|value| digest(value));
    assert!(
        by_entry == by_digest,
        "a run of cut entries is not in digest order"
    );
    let first = ascending.entry(&[text(&alike[0])]).expect("an entry");
    let cut = first.cut.expect("a text of more than 3,000 bytes is cut");
    for value in &alike {
        let entry = ascending.entry(&[text(value)]).expect("an entry");
        assert_eq!(entry.cut, Some(cut));
        assert!(entry.key.as_bytes()[..cut] == first.key.as_bytes()[..cut]);
    }
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

/// Writes a store of one row, the number 1 and then a vector of `count` reduced numbers, handed
/// to the store with its scale and codes.
fn store_reduced_vector(folder: &Path, count: usize) {
    let (numbers, scale, codes) = reduced(count);
    let row = vec![
        Cell::Here(Value::Int64(1)),
        Cell::Here(Value::Vector(numbers)),
    ];
    store_row(
        folder,
        row,
        vec![Compact {
            at: 1,
            scale,
            codes,
        }],
    );
}

#[test]
fn a_reduced_record_of_another_length_is_damage() {
    // 2,000 reduced numbers are a record of 2,013 bytes, kept beside the row.
    const COUNT: usize = 2000;
    let folder = fresh_folder("reduced-length");
    store_reduced_vector(&folder, COUNT);
    let (loaded, beside, _, _) = load_the_value(&folder);
    assert!(loaded.expect("load the value as it was written") == Value::Vector(reduced(COUNT).0));
    assert_eq!(
        beside.bytes,
        13 + COUNT as u64,
        "the record of a reduced vector"
    );

    // The record now says it holds one number more than its bytes do.
    record_length(&folder, beside.place, COUNT as u64 + 1);
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

#[test]
fn a_recorded_length_of_255_times_the_block_is_the_limits_refusal_and_one_more_is_damage() {
    // No public call writes a wrong recorded length. The most a block can decode to is 255
    // times its bytes: that much is asked of the memory limit before it is taken, and one byte
    // more is damage before any memory is asked for.
    const LEN: usize = 16 << 10;
    let folder = fresh_folder("recorded-length-bound");
    store_one_row(
        &folder,
        text(&"A store keeps what it is given. ".repeat(LEN / 32)),
    );
    let (_, beside, _, _) = load_the_value(&folder);
    let block = beside.bytes - 9;
    // Room for the stored record and a value of LEN bytes, and for no more than that.
    let room = beside.bytes + 9 + LEN as u64 + 1 + 64;
    assert!(
        9 + 255 * block > room,
        "255 times the block does not fit the room"
    );
    // A length one more or one less than the bytes the block decodes to is damage as well,
    // though the page checksum holds: only the check of the decoded length can tell.
    let cases = [
        (255 * block, false),
        (255 * block + 1, true),
        (LEN as u64 + 1, true),
        (LEN as u64 - 1, true),
    ];
    for (wrong, damaged) in cases {
        record_length(&folder, beside.place, wrong);
        let (table, row_key) = the_row();
        let budget = Budget::new(Some((9 << 20) + room)).expect("a budget");
        let store = Store::open_for_writing(&folder, &budget, None).expect("open the store");
        let view = store.view().expect("a view");
        let row = view.row(table, &row_key).expect("read").expect("the row");
        let Some(Cell::Beside(at)) = row.cells.into_iter().last() else {
            panic!("the text was kept beside its row");
        };
        let before = budget.used();
        let mut reservation = budget.reserve(0, "query").expect("a reservation");
        let loaded = view.load(&at, &mut reservation);
        assert_eq!(reservation.bytes(), 0, "held after a length of {wrong}");
        drop(reservation);
        assert_eq!(budget.used(), before, "counted after a length of {wrong}");
        match loaded {
            Err(Error::StoreCorrupted { path, .. }) if damaged => assert_eq!(path, folder),
            Err(Error::MemoryBudgetExceeded { operation, .. }) if !damaged => {
                assert_eq!(operation, "query");
            }
            other => panic!("a recorded length of {wrong} gave {other:?}"),
        }
    }
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");

    // A plain record whose recorded length is one more or one less than its bytes is damage.
    let letters = scattered(2000);
    for wrong in [2001_u64, 1999] {
        let folder = fresh_folder("recorded-length-plain");
        store_one_row(&folder, text(&letters));
        let (_, beside, _, _) = load_the_value(&folder);
        assert_eq!(beside.bytes, 9 + 2000, "the value is stored plain");
        record_length(&folder, beside.place, wrong);
        loading_is_damage(
            &folder,
            &format!("a plain record that gives the length {wrong}"),
        );
        std::fs::remove_dir_all(&folder).expect("remove the test's folder");
    }

    // A packed row records the length it unpacks to, in four bytes after its marker byte: one
    // more or less than the block decodes to, or more than a block can, is damage on every path
    // that reads a stored row, and the refused change leaves the row as it was.
    let folder = fresh_folder("recorded-length-packed");
    let (table, row_key) = the_row();
    let cells = vec![
        Cell::Here(Value::Int64(1)),
        Cell::Here(text(&"A packed row repeats itself. ".repeat(10))),
        Cell::Here(text(&scattered(2000))),
    ];
    store_row(&folder, cells.clone(), Vec::new());
    let [(stored_key, packed)] = <[_; 1]>::try_from(records(&folder, ROWS)).expect("one row");
    let plain = 35 + 290_u32;
    assert_eq!(
        packed[1..5],
        plain.to_le_bytes(),
        "the row is packed and records its length"
    );
    for wrong in [plain + 1, plain - 1, 255 * (packed.len() as u32 - 5) + 1] {
        let mut bytes = packed.clone();
        bytes[1..5].copy_from_slice(&wrong.to_le_bytes());
        write_record(&folder, ROWS, &stored_key, &bytes);
        let by_key = on_the_newest_moment(&folder, |read| read.row(table, &row_key).map(drop));
        let scan = on_the_newest_moment(&folder, |read| {
            let scan = read.rows(table, Bound::Unbounded, Bound::Unbounded);
            scan.and_then(Iterator::collect::<Result<Vec<_>>>).map(drop)
        });
        let change =
            |stage: &dyn Fn(&mut ChangeSet) -> Result<()>| try_commit(&folder, stage).map(drop);
        let update = change(&|c| c.put_row(table, row_key.clone(), cells.clone(), vec![], false));
        let delete = change(&|c| c.delete_row(table, &row_key));
        let drop_table = change(&|c| c.stage_drop(Dropped::Rows(table)));
        let drop_column = change(&|c| c.stage_drop(Dropped::Cell(table, 1)));
        let paths = [
            ("a read by key", by_key),
            ("a scan", scan),
            ("an update", update),
            ("a delete", delete),
            ("a drop of the table", drop_table),
            ("a drop of a column", drop_column),
        ];
        for (path, outcome) in paths {
            assert_damage(
                outcome,
                &folder,
                &format!("{path} of a packed row that records {wrong}"),
            );
        }
    }
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

/// Puts `wrong` as the recorded length in the first slice of the large value at `place`, as a
/// commit of the storage library, so the page checksum is right for it.
fn record_length(folder: &Path, place: u64, wrong: u64) {
    tamper(folder, BESIDE, |kept| {
        let first = kept.get(place).expect("read the first slice");
        let mut first = first.expect("the first slice is there").value().to_vec();
        first[1..9].copy_from_slice(&wrong.to_le_bytes());
        kept.insert(place, first.as_slice()).expect("write");
    });
}
