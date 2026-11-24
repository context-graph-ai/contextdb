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
use crate::DatabaseOpenOptions;
use crate::memory::{Budget, Passing};
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

    let budget = Budget::new(None).expect("a budget");
    let mut cover = budget.passing("encode fixture").expect("a cover");
    let mut kept = Vec::new();
    let row = record::encode_row(&values, &[], (None, &[]), &mut cover, |large| {
        kept.push(large.to_vec());
        Ok::<_, crate::types::Error>(kept.len() as u64 - 1)
    })
    .expect("nothing refuses");
    assert_eq!(kept.len(), 2, "the long text and the long vector");
    assert!(row.len() < 400, "the row holds {} bytes", row.len());

    let cells = record::decode_row(&row, values.len()).expect("the row just written is sound");
    let read: Vec<Value> = cells
        .into_iter()
        .map(|cell| match cell {
            Cell::Here(value) => value,
            Cell::Beside(beside) => {
                let large = &kept[beside.place as usize];
                assert_eq!(beside.bytes, large.len() as u64);
                record::decode_value(large).expect("the value just written is sound")
            }
        })
        .collect();
    assert_eq!(read, values);
    let reduced = [
        &[0x88][..],
        &2_u64.to_le_bytes(),
        &1_f32.to_le_bytes(),
        &[1, 255],
        &row[row.len() - 17..],
    ]
    .concat();
    let (_, beside) = record::without_cell(&reduced, 1).unwrap().unwrap();
    assert!(beside.is_some());
    for at in [0, 1] {
        let mut broken = reduced.clone();
        broken[at] = 255;
        assert!(record::without_cell(&broken, 1).is_err());
    }

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
    let before = on_the_newest_moment(&folder, |read, cover| {
        let scan = read.rows(TableId(1), Bound::Unbounded, Bound::Unbounded, false, cover);
        let scan = scan
            .and_then(Iterator::collect::<Result<Vec<_>>>)
            .expect("scan");
        for ((key, row), (id, (body, _))) in scan.iter().zip((1..).zip(cases)) {
            let by_key = read.row(TableId(1), key, cover).expect("read");
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
    let after = on_the_newest_moment(&folder, |read, cover| {
        let row = read
            .row(TableId(1), second, cover)
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
    on_the_newest_moment(&folder, |read, cover| {
        for id in 1..=3 {
            let row = read.row(TableId(1), &key(&[Value::Int64(id)]), cover);
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
    let store = Store::open_for_writing(folder, &budget, &DatabaseOpenOptions::default())
        .expect("open a fresh store");
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
    read.index(index, Bound::Unbounded, Bound::Unbounded, false)
        .and_then(|entries| entries.collect())
}

/// Opens the store in this folder under no memory limit and runs `read` on its newest moment.
fn on_the_newest_moment<T>(folder: &Path, read: impl FnOnce(&dyn Read, &mut Passing) -> T) -> T {
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, &DatabaseOpenOptions::default())
        .expect("open the store");
    let view = store.view().expect("a view");
    let mut cover = budget.passing("query").expect("a cover");
    read(&view, &mut cover)
}

/// Stages with `stage` on the store's newest moment and commits, or says why it could not.
fn try_commit(
    folder: &Path,
    stage: impl FnOnce(&mut ChangeSet) -> Result<()>,
) -> Result<crate::types::TxId> {
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, &DatabaseOpenOptions::default())
        .expect("open the store");
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
        let laid = kept(&*changes, count).expect("read");
        assert!(
            laid.is_empty(),
            "a staged -2 is laid over the stored 2: {laid:?}"
        );
    });
    assert!(
        records(&folder, COUNTS).is_empty(),
        "a count taken down to zero keeps no record"
    );
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
fn the_records_of_an_ivf_keep_their_keys_apart() {
    use VectorRecord::*;
    let column = VectorId(3);
    let (tuple, row) = (key(&[text("a")]), key(&[Value::Int64(1)]));
    // A partition numbered 5 and its IVF numbered 6: every kind of record of the two.
    let records = [
        Partition(column, &tuple),
        Pending(column, 5, &row),
        Block(column, 5, 0),
        Block(column, 6, 0),
        Ivf(column, 6),
        Centroids(column, 6, 0),
        Build(column, 5),
        Assigned(column, 6, 0),
        Binary(column, 5, 0),
        Binary(column, 6, 0),
        Mean(column, 5),
        Left(column, 5, &row),
    ];
    let stored = records.map(keys::vector);
    let apart: std::collections::BTreeSet<&Vec<u8>> = stored.iter().collect();
    assert_eq!(apart.len(), records.len(), "two records share a key");
    for (record, stored) in records.iter().zip(&stored) {
        let mut held = key(&[]);
        assert_eq!(keys::vector_of(stored, &mut held), Some(*record));
        // Blocks, centroids and binary codes are kept together; everything else is small.
        let large = matches!(record, Block(..) | Centroids(..) | Binary(..));
        assert_eq!(keys::with_blocks(stored[0]), large, "{record:?}");
    }
    // The binary codes of an IVF's blocks come in the order of the blocks, before the next
    // number's.
    let code = |under, block| keys::vector(Binary(column, under, block));
    assert!(code(6, 0) < code(6, 1) && code(6, 1) < code(7, 0));
}

// ---- damaged records ----

/// A record the store writes sound. The large value of the one row: a text or JSON of letters
/// no compressor makes shorter, kept plain; a vector of 384 numbers; a text or JSON of 16 KiB
/// of one letter, or a text of a sentence repeated to 16 KiB, kept compressed; a vector of
/// 2,000 numbers kept reduced. Or the plain text's row, the row packed into one block, the row
/// with a short text or JSON as its last cell, a kept count, or the short (0) or the cut (1) of
/// two index entries. Or, of a store declared through SQL, the record of the named table at this
/// place: the count of `c`, the row of `i` or the entry of `i`'s index, after `c`'s.
#[derive(Clone, Copy)]
enum Sound {
    Plain,
    PlainJson,
    Vector,
    Letters,
    JsonLetters,
    Repeated,
    Reduced,
    Packed,
    PlainRow,
    InRow,
    JsonInRow,
    Count,
    Entry(usize),
    Declared(&'static str, usize),
}

/// What is done to a sound record as a commit of the storage library: the last slice of a
/// large value taken away; the block of a compressed value replaced by the block of these
/// bytes, as many as the value has; the recorded length made this, from the recorded length
/// and the bytes of the block; the record's bytes made these; the record taken away.
#[derive(Clone, Copy)]
enum Fault<'a> {
    LastSlice,
    Gone,
    Block(&'a [u8]),
    Length(fn(u64, u64) -> u64),
    Bytes(fn(&[u8]) -> Vec<u8>),
    Place(&'static str, bool, fn(&[u8]) -> Vec<u8>),
}

/// The bytes of the 16 KiB values.
const LEN: usize = 16 << 10;

/// The cells of the packed row: 35 bytes, a text that packs well, and a text kept beside it.
fn packed() -> Vec<Cell> {
    let well = text(&"A packed row repeats itself. ".repeat(10));
    let cells = [Value::Int64(1), well, text(&scattered(2000))];
    cells.map(Cell::Here).to_vec()
}

fn plant(folder: &Path, sound: Sound) {
    let json = |text: String| Value::Json(serde_json::Value::String(text));
    let value = match sound {
        Sound::Reduced => return store_reduced_vector(folder, 2000),
        Sound::Packed => return store_row(folder, packed(), Vec::new()),
        Sound::Entry(_) => return drop(store_two_entries(folder)),
        Sound::Declared(..) => {
            let db = crate::Database::open(folder).expect("open the store");
            let sql = "CREATE TABLE c (id INTEGER PRIMARY KEY, g INTEGER, UNIQUE (g, id)) \
                       MAINTAIN COUNT (g); \
                       CREATE TABLE i (id INTEGER PRIMARY KEY, g INTEGER); \
                       CREATE INDEX i_g ON i (g); INSERT INTO c (id, g) VALUES (1, 1); \
                       INSERT INTO i (id, g) VALUES (1, 1); CREATE TABLE v (id INTEGER PRIMARY KEY, v VECTOR(2)); \
                       INSERT INTO v VALUES (1, [1.0, 0.0])";
            let none = std::collections::HashMap::new();
            for one in sql.split("; ") {
                db.execute(one, &none).expect(one);
            }
            return db.close().expect("close");
        }
        Sound::Count => {
            let group = key(&[text("group")]);
            return commit_staged(folder, |changes| {
                changes.add_kept(CountId(1), group, 7).unwrap()
            });
        }
        Sound::Plain | Sound::PlainRow => text(&scattered(2000)),
        Sound::PlainJson => json(scattered(2000)),
        Sound::Vector => Value::Vector((0..384_u16).map(|n| f32::from(n) / 8.0).collect()),
        Sound::Letters => text(&"a".repeat(LEN)),
        Sound::JsonLetters => json("a".repeat(LEN - 2)),
        Sound::Repeated => text(&"A store keeps what it is given. ".repeat(512)),
        Sound::InRow => text(&scattered(20)),
        Sound::JsonInRow => json(scattered(20)),
    };
    store_one_row(folder, value);
}

/// The record with the fault, which for a packed row records its length in four bytes and for
/// a large value in eight.
fn changed(record: &[u8], width: usize, fault: Fault) -> Vec<u8> {
    if let Fault::Bytes(wrong) = fault {
        return wrong(record);
    }
    let mut recorded = [0; 8];
    recorded[..width].copy_from_slice(&record[1..=width]);
    let (recorded, block) = (u64::from_le_bytes(recorded), record.len() - 1 - width);
    match fault {
        Fault::Length(wrong) => {
            assert!(width == 8 || block < recorded as usize, "stored packed");
            let wrong = wrong(recorded, block as u64).to_le_bytes();
            [&record[..1], &wrong[..width], &record[1 + width..]].concat()
        }
        Fault::Block(wrong) => {
            assert!(record[0] & 0x80 != 0, "the value is stored compressed");
            assert_eq!(recorded, wrong.len() as u64, "the value's length");
            let mut block = vec![0; get_maximum_output_size(wrong.len())];
            let length = compress_into(wrong, &mut block).expect("any block fits");
            let whole = [&record[..9], &block[..length]].concat();
            assert_eq!(whole.len(), record.len(), "a record as long as the value's");
            whole
        }
        _ => panic!("only a value kept in slices loses its last"),
    }
}

fn damage(folder: &Path, sound: Sound, fault: Fault) {
    use Sound::{Count, Declared, Entry, InRow, JsonInRow, Packed, PlainRow};
    if let Declared("numbers", _) = sound {
        let numbers = redb::TableDefinition::<u64, u64>::new("numbers");
        return tamper(folder, numbers, |t| {
            t.insert(super::disk::count_of(1), 2).unwrap();
        });
    }
    let (table, at) = match sound {
        Count => (COUNTS, 0),
        Entry(at) => (INDEX, at),
        Declared(name, at) => (redb::TableDefinition::new(name), at),
        _ => (ROWS, 0),
    };
    if let Packed | PlainRow | InRow | JsonInRow | Count | Entry(_) | Declared(..) = sound {
        let (key, bytes) = records(folder, table).swap_remove(at);
        if let Fault::Place(name, copy, change) = fault {
            write_record(
                folder,
                redb::TableDefinition::new(name),
                &change(&key),
                &bytes,
            );
            if copy {
                return;
            }
            return tamper(folder, table, |t| drop(t.remove(&*key).unwrap()));
        }
        if let Fault::Gone = fault {
            return tamper(folder, table, |t| drop(t.remove(&*key).expect("remove")));
        }
        return write_record(folder, table, &key, &changed(&bytes, 4, fault));
    }
    tamper(folder, BESIDE, |kept| {
        let slices: Vec<(u64, Vec<u8>)> = (kept.iter().expect("read the slices"))
            .map(|slice| slice.expect("a slice"))
            .map(|(place, slice)| (place.value(), slice.value().to_vec()))
            .collect();
        let (first, last) = (slices[0].0, slices[slices.len() - 1].0);
        if let Fault::LastSlice = fault {
            assert!(slices[0].1[0] & 0x80 == 0, "the value is stored plain");
            assert!(last > first, "the value is kept in more than one slice");
            return drop(kept.remove(last).expect("remove the last slice"));
        }
        let record: Vec<u8> = slices.into_iter().flat_map(|(_, slice)| slice).collect();
        let record = changed(&record, 8, fault);
        for (place, slice) in (first..).zip(record.chunks(SLICE as usize)) {
            kept.insert(place, slice).expect("write a slice");
        }
    });
}

/// What the reader that meets the record first gave. A large value is loaded with `room` under
/// the memory limit, or with no limit, and once the load is over the statement holds nothing and nothing stays
/// counted.
fn read(folder: &Path, sound: Sound, room: Option<u64>) -> Result<()> {
    if let Sound::Declared(..) = sound {
        // Records no reader meets: the check, which passes the sound store.
        let db = crate::Database::open(folder)?;
        return db.verify().map(drop);
    }
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, &DatabaseOpenOptions::default())
        .expect("open the store");
    budget.set_limit(room.map(|room| budget.used() + room))?;
    let view = store.view().expect("a view");
    let (table, row_key) = the_row();
    let mut cover = budget.passing("query")?;
    match sound {
        Sound::Packed | Sound::InRow | Sound::JsonInRow => {
            view.row(table, &row_key, &mut cover).map(drop)
        }
        Sound::Count => kept(&view, CountId(1)).map(drop),
        Sound::Entry(_) => entries(&view, IndexId(2)).map(drop),
        _ => {
            let row = view
                .row(table, &row_key, &mut cover)
                .expect("read")
                .expect("the row");
            let Some(Cell::Beside(beside)) = row.cells.last() else {
                panic!("the value was kept in its row");
            };
            let before = budget.used();
            let mut held = budget.reserve(0, "query").expect("a reservation");
            let loaded = view.load(beside, &mut held).map(drop);
            assert!(loaded.is_ok() || held.bytes() == 0, "held after {loaded:?}");
            drop(held);
            assert_eq!(budget.used(), before, "counted after the load");
            loaded
        }
    }
}

/// What every path that meets the record gave: the whole-store check (which walks every record,
/// owned by a declared table or not), the first reader, for a row's own record a scan,
/// and for the packed row an update, a delete and a drop of the table and of a column.
fn met(folder: &Path, sound: Sound) -> Vec<(&'static str, Result<()>)> {
    let check = crate::Database::open(folder).and_then(|db| db.verify());
    let mut given = vec![("the whole-store check", check.map(drop))];
    if let Sound::Declared(..) = sound {
        return given;
    }
    // Room for a value of 16 KiB, its record and what is read from it.
    given.push(("the reader", read(folder, sound, Some(64 << 10))));
    if let Sound::PlainRow = sound {
        given.push(("the reader with no memory limit", read(folder, sound, None)));
    }
    let (table, row) = the_row();
    if let Sound::Packed | Sound::InRow | Sound::JsonInRow = sound {
        let scan = on_the_newest_moment(folder, |read, cover| {
            let scan = read.rows(table, Bound::Unbounded, Bound::Unbounded, false, cover);
            scan.and_then(Iterator::collect::<Result<Vec<_>>>).map(drop)
        });
        given.push(("a scan", scan));
    }
    if let Sound::Packed = sound {
        let change =
            |stage: &dyn Fn(&mut ChangeSet) -> Result<()>| try_commit(folder, stage).map(drop);
        let update = change(&|c| c.put_row(table, row.clone(), packed(), vec![], false));
        let delete = change(&|c| c.delete_row(table, &row));
        let drop_table = change(&|c| c.stage_drop(Dropped::Rows(table)));
        let drop_column = change(&|c| c.stage_drop(Dropped::Cell(table, 1)));
        given.extend([
            ("an update", update),
            ("a delete", delete),
            ("a drop of the table", drop_table),
            ("a drop of a column", drop_column),
        ]);
    }
    given
}

#[test]
fn every_damaged_record_is_told_as_damage_by_each_path_that_meets_it() {
    use {Fault::*, Sound::*};
    // The JSON text with a brace where each of its quotation marks is.
    let braced = format!("{{{}}}", "a".repeat(LEN - 2)).into_bytes();
    let (more, less) = (Length(|n, _| n + 1), Length(|n, _| n - 1));
    // 255 times its bytes is the most a block decodes to; past it is damage before any memory
    // is asked for.
    let past = Length(|_, b| 255 * b + 1);
    let (three, longer) = (Bytes(|b| b[..3].to_vec()), Bytes(|b| [b, &[0]].concat()));
    let (none, eight) = (Bytes(|_| Vec::new()), Bytes(|b| [b, &[0; 8]].concat()));
    // A row's own record: its last byte made one no text or JSON ends with, or taken away, or
    // a cell after its last whose tag names no kind of value.
    let unended = Bytes(|b| [&b[..b.len() - 1], &[0xFF]].concat());
    let cut = Bytes(|b| b[..b.len() - 1].to_vec());
    let unknown = Bytes(|b| [b, &[0xEE; 9]].concat());
    // The plain text's row, the length it records of its large value made 2^63 bytes more,
    // the last byte of the eight before the value's place: within no memory limit, past what
    // any machine can hold.
    let longest = Bytes(|b| [&b[..b.len() - 9], &[0x80], &b[b.len() - 8..]].concat());
    // A kept count's first byte one more.
    let one = Bytes(|b| [&[b[0] + 1], &b[1..]].concat());
    let orphan_key = |k: &[u8]| [&k[..k.len() - 1], &[2]].concat();
    let orphan = Place("counts", true, orphan_key);
    let wrong_table = Place("vector_blocks", false, |k| k.to_vec());
    let wrong_key = Place("vectors", false, |k| [k, &[0]].concat());
    let extra_cell = Bytes(|b| [b, &[0]].concat());
    let wrong_index = Bytes(|b| [&b[..b.len() - 1], &[2]].concat());
    let bad_vector = Bytes(|_| vec![8, 1, 0, 0, 0, 0, 0, 0, 0, 0]);
    let rows = [
        ("plain text, last slice gone", Plain, LastSlice),
        ("plain JSON, last slice gone", PlainJson, LastSlice),
        ("vector, last slice gone", Vector, LastSlice),
        ("text, decoding to no text", Letters, Block(&[0xFF; LEN])),
        ("JSON, decoding to no JSON", JsonLetters, Block(&braced)),
        ("reduced vector, a number more", Reduced, more),
        ("plain text, a byte more", Plain, more),
        ("plain text, a byte less", Plain, less),
        ("compressed text, a byte more", Repeated, more),
        ("compressed text, a byte less", Repeated, less),
        ("compressed text, past 255 blocks", Repeated, past),
        ("packed row, a byte more", Packed, more),
        ("packed row, a byte less", Packed, less),
        ("packed row, past 255 blocks", Packed, past),
        (
            "kept count of seven bytes",
            Count,
            Bytes(|b| b[..7].to_vec()),
        ),
        ("kept count of a byte more", Count, longer),
        ("kept count of no bytes", Count, none),
        ("entry cut to three bytes", Entry(0), three),
        ("entry of a byte more", Entry(0), longer),
        ("entry of no bytes", Entry(0), none),
        ("entry of eight bytes more", Entry(0), eight),
        ("cut entry cut to three bytes", Entry(1), three),
        ("cut entry of a byte more", Entry(1), longer),
        ("cut entry of no bytes", Entry(1), none),
        ("cut entry of eight bytes more", Entry(1), eight),
        // A place past the end of the entry, whichever way its four bytes are read.
        (
            "cut entry cut past its end",
            Entry(1),
            Bytes(|b| [&b[..4], &[0xFF; 4]].concat()),
        ),
        ("text in its row, not text", InRow, unended),
        ("JSON in its row, not JSON", JsonInRow, unended),
        ("row, its last cell cut short", InRow, cut),
        ("row, a cell of no known tag", InRow, unknown),
        ("plain text, 2^63 bytes more in its row", PlainRow, longest),
        // Records that agree with themselves and not with each other, met by the check alone.
        ("table count differs", Declared("numbers", 0), one),
        ("count, one more than its rows", Declared("counts", 0), one),
        ("deleted counted row", Declared("rows", 0), Gone),
        ("orphan count", Declared("counts", 0), orphan),
        (
            "vector in blocks table",
            Declared("vectors", 0),
            wrong_table,
        ),
        (
            "partition key has extra cell",
            Declared("vectors", 0),
            wrong_key,
        ),
        ("undeclared cell", Declared("rows", 1), extra_cell),
        ("wrong index value", Declared("rows", 1), wrong_index),
        ("invalid boolean", InRow, Bytes(|_| vec![1, 2])),
        ("vector length", InRow, bad_vector),
        ("index entry, its row gone", Declared("rows", 1), Gone),
        ("row, its index entry gone", Declared("index", 1), Gone),
    ];
    let mut failures = Vec::new();
    for (at, (what, sound, fault)) in rows.into_iter().enumerate() {
        let folder = fresh_folder(&format!("damaged-{at}"));
        plant(&folder, sound);
        read(&folder, sound, Some(64 << 10)).unwrap_or_else(|error| panic!("{what}: {error:?}"));
        damage(&folder, sound, fault);
        for (path, given) in met(&folder, sound) {
            match given {
                Err(Error::StoreCorrupted { path: told, .. }) => assert_eq!(told, folder, "{what}"),
                other => failures.push(format!("{what}, met by {path}, gave {other:?}")),
            }
        }
        std::fs::remove_dir_all(&folder).expect("remove the test's folder");
    }

    // 255 times its block is asked of the memory limit before it is taken, and is no damage.
    let folder = fresh_folder("damaged-limit");
    plant(&folder, Repeated);
    damage(&folder, Repeated, Length(|_, block| 255 * block));
    // Room for the value and its record, and for no more than that.
    match read(&folder, Repeated, Some(17 << 10)) {
        Err(Error::MemoryBudgetExceeded { operation, .. }) => assert_eq!(operation, "query"),
        other => panic!("a length of 255 times the block gave {other:?}"),
    }
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A pinned commit is charged for the roots of its tables at their own size, before anything of
/// it is opened, and every pin, however many, is given back exactly: a pin that is dropped is
/// paid back when the next pin is inserted, never left charged.
#[test]
fn each_pinned_commit_is_charged_its_roots_and_given_back_when_pruned() {
    use redb::ReadableTableMetadata;
    const LEN: usize = 300_000;
    const PINS: usize = 3;
    let folder = fresh_folder("pin-charge");
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(&folder, &budget, &DatabaseOpenOptions::default())
        .expect("open a fresh store");
    let (table, row_key) = the_row();
    let put = |n: i64| {
        let view = store.view().expect("a view");
        let mut changes = ChangeSet::new(view, &budget).expect("begin");
        let cells = vec![
            Cell::Here(Value::Int64(n)),
            Cell::Here(text(&scattered(LEN))),
        ];
        changes
            .put_row(table, row_key.clone(), cells, Vec::new(), n == 0)
            .expect("stage");
        store.commit(changes).expect("commit");
    };
    put(0);
    let super::HeldFor::Writing { data, .. } = &store.held else {
        unreachable!("opened for writing")
    };
    let rows = data
        .begin_read()
        .unwrap()
        .open_table(ROWS)
        .unwrap()
        .stats()
        .unwrap();
    assert_eq!(
        rows.tree_height(),
        1,
        "the one row is held by a root of one page"
    );
    // The rows table's root is one page of the store, and a pin retains at least that page.
    let root = 4096;
    let dropped = budget.used();
    let mut pins = Vec::new();
    let mut charges = Vec::new();
    for n in 1..=PINS as i64 {
        put(n);
        let before = budget.used();
        pins.push(store.view().expect("a pin"));
        charges.push(budget.used() - before);
    }
    assert!(
        charges[0] >= root,
        "a pin was charged {} bytes, less than the {root} bytes of the root it retains: roots are \
         charged at their own size",
        charges[0]
    );
    assert!(
        charges.iter().all(|c| *c == charges[0]),
        "pins of one shape were charged {charges:?}"
    );
    assert_eq!(
        budget.used() - dropped,
        charges.iter().sum::<u64>(),
        "the pins' charges were not all held while the pins were"
    );
    // A pin that does not fit is refused before it opens anything, and charges nothing.
    put(PINS as i64 + 1);
    let used = budget.used();
    budget
        .set_limit(Some(used + charges[0] - 1))
        .expect("a limit above what is in use");
    assert!(
        matches!(store.view(), Err(Error::MemoryBudgetExceeded { .. })),
        "a pin one byte over the budget was admitted"
    );
    assert_eq!(budget.used(), used, "a refused pin left memory charged");
    budget.set_limit(None).expect("lift the limit");
    // Dropped pins are paid back, and what is left over is pruned when the next pin goes in.
    drop(pins);
    assert_eq!(
        budget.used(),
        dropped,
        "dropping the pins did not give back their charge"
    );
    put(PINS as i64 + 2);
    let before = budget.used();
    let _pin = store.view().expect("a pin");
    assert_eq!(
        budget.used() - before,
        charges[0],
        "the new pin was charged another amount"
    );
    let super::HeldFor::Writing { pins, .. } = &store.held else {
        unreachable!()
    };
    let alive = pins
        .lock()
        .unwrap()
        .values()
        .filter(|pin| pin.strong_count() > 0)
        .count();
    assert_eq!(
        pins.lock().unwrap().len(),
        alive,
        "dead pins were left in the map: they are pruned when a new pin is inserted"
    );
}

#[test]
fn a_range_read_from_the_far_end_is_the_forward_read_reversed_on_the_file_and_in_a_transaction() {
    // 2,000-byte keys that differ in their last four bytes: every page shares a prefix.
    let at = |n: i64| key(&[Value::Text(format!("{:k<2000}{n:04}", ""))]);
    let put = |changes: &mut ChangeSet, n: i64| {
        let cells = vec![Cell::Here(Value::Int64(n))];
        changes
            .put_row(TableId(1), at(n), cells, Vec::new(), true)
            .unwrap();
    };
    let folder = fresh_folder("range-far-end");
    commit_staged(&folder, |c| (0..800).step_by(2).for_each(|n| put(c, n)));
    let ends = [
        (Bound::Unbounded, Bound::Unbounded),
        (Bound::Included(at(101)), Bound::Excluded(at(699))),
        (Bound::Included(at(900)), Bound::Excluded(at(901))),
    ];
    let check = |read: &dyn Read, cover: &mut Passing, what: &str, sizes: [usize; 3]| {
        for ((from, to), size) in ends.iter().zip(sizes) {
            let mut scan = |back| {
                let rows = read.rows(TableId(1), from.as_ref(), to.as_ref(), back, cover);
                rows.and_then(Iterator::collect::<Result<Vec<_>>>)
                    .expect("scan")
            };
            let (forward, mut backward) = (scan(false), scan(true));
            backward.reverse();
            assert_eq!(forward.len(), size, "{what}: the rows of {from:?}");
            assert!(
                forward == backward,
                "{what}: far end is the forward read reversed"
            );
        }
    };
    on_the_newest_moment(&folder, |read, cover| {
        check(read, cover, "file", [400, 299, 0])
    });
    // The transaction's own rows lie between the file's.
    let budget = Budget::new(None).unwrap();
    let store = Store::open_for_writing(&folder, &budget, &DatabaseOpenOptions::default()).unwrap();
    let mut changes = ChangeSet::new(store.view().unwrap(), &budget).unwrap();
    (1..800).step_by(2).for_each(|n| put(&mut changes, n));
    let mut cover = budget.passing("query").unwrap();
    check(&changes, &mut cover, "transaction", [800, 598, 0]);
}
