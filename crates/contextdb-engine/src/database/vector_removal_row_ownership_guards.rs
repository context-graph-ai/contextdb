// Included inside received_legacy_growth_guards. The vector-removal loop
// decodes every stored entry it visits. Each decoded row must stay charged
// only while that row lives, even though the operation running the walk
// outlives it. A charge left with that operation grows the removal peak with
// the rows removed. Local DROP TABLE reaches the loop's identity-retiring
// variant (persistence.rs remove_table_rewrite_aux_*, retire_identity = true). The received stage reaches the same loop at
// redb/pending_directory, which is the fixed-budget refusal in the
// population guards above.
const OWNERSHIP_DIM: usize = 1024;
const OWNERSHIP_BODY: usize = OWNERSHIP_DIM * std::mem::size_of::<f32>();
const OWNERSHIP_LIMIT: usize = 64 * 1024 * 1024;
const OWNERSHIP_KEEP: usize = 4;
// A per-row charge left behind for 960 extra rows is about 3.9 MiB. The
// bound allows sixteen bodies of unrelated page work, and nothing near a
// per-row term.
const OWNERSHIP_PEAK_SLACK: usize = 16 * OWNERSHIP_BODY;

struct DropMeasure {
    rows: usize,
    peak_rise: usize,
    decoded_bodies: usize,
}

fn ownership_row(i: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("id".into(), Value::Int64(i as i64)),
        (
            "embedding".into(),
            Value::Vector(vec![(i + 1) as f32; OWNERSHIP_DIM]),
        ),
    ])
}

fn ownership_table(db: &Database, table: &str, count: usize) {
    sql(
        db,
        &format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, embedding VECTOR({OWNERSHIP_DIM}) SEARCH_MODE EXACT)"
        ),
    );
    for first in (0..count).step_by(256) {
        let tx = db.begin().unwrap();
        for i in first..(first + 256).min(count) {
            db.insert_row(tx, table, ownership_row(i)).unwrap();
        }
        db.commit(tx).unwrap();
    }
}

fn stored_vectors(db: &Database, table: &str, count: usize) -> usize {
    (0..count)
        .filter(|i| {
            db.point_lookup(table, "id", &Value::Int64(*i as i64), db.snapshot())
                .unwrap()
                .is_some_and(|row| {
                    db.live_vector_entry(row.row_id, db.snapshot())
                        .unwrap()
                        .is_some_and(|entry| entry.vector == vec![(*i + 1) as f32; OWNERSHIP_DIM])
                })
        })
        .count()
}

fn dropped_inside_enclosing_owner(rows: usize) -> DropMeasure {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("ownership.db");
    let db = open(&path, OWNERSHIP_LIMIT);
    ownership_table(&db, "warm", 2);
    ownership_table(&db, "sel", rows);
    ownership_table(&db, "keep", OWNERSHIP_KEEP);
    assert_eq!(stored_vectors(&db, "sel", rows), rows, "every selected row is a real stored vector");
    assert_eq!(stored_vectors(&db, "keep", OWNERSHIP_KEEP), OWNERSHIP_KEEP);
    // Pay first-use caches outside the measured window with one drop of the
    // same shape.
    sql(&db, "DROP TABLE warm");
    let before = db.accountant.usage().used;
    db.accountant.reset_high_water_for_test();
    crate::memory_accounting::watch_budget_ledger_for_test();
    sql(&db, "DROP TABLE sel");
    let ledger = crate::memory_accounting::take_budget_ledger_for_test();
    let peak_rise = db.accountant.high_water_for_test().saturating_sub(before);
    let decoded_bodies = ledger
        .iter()
        .filter(|entry| entry.what == "writer_source/decode_stored_value" && entry.bytes >= OWNERSHIP_BODY)
        .count();
    // The removal is real: the selected table and its vectors are gone, and
    // the unselected table keeps every vector, live and after reopen.
    assert!(db.table_meta("sel").is_none());
    assert_eq!(stored_vectors(&db, "keep", OWNERSHIP_KEEP), OWNERSHIP_KEEP);
    db.close().unwrap();
    drop(db);
    let db = open(&path, OWNERSHIP_LIMIT);
    assert!(db.table_meta("sel").is_none());
    assert_eq!(stored_vectors(&db, "keep", OWNERSHIP_KEEP), OWNERSHIP_KEEP);
    db.close().unwrap();
    DropMeasure {
        rows,
        peak_rise,
        decoded_bodies,
    }
}

#[test]
#[serial]
fn removing_vector_entries_releases_each_decoded_row_inside_the_operation() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let small = dropped_inside_enclosing_owner(64);
    let large = dropped_inside_enclosing_owner(1024);
    for measure in [&small, &large] {
        eprintln!(
            "VECTOR_REMOVAL_OWNERSHIP rows={} decoded_bodies={} peak_rise={}",
            measure.rows,
            measure.decoded_bodies,
            measure.peak_rise
        );
        assert!(
            measure.decoded_bodies >= measure.rows,
            "every visited row's stored body is decoded under admission, not exempted from it"
        );
    }
    assert!(
        large.peak_rise <= small.peak_rise + OWNERSHIP_PEAK_SLACK,
        "removal peak must not grow with the rows removed: {} rows {} B, {} rows {} B",
        small.rows,
        small.peak_rise,
        large.rows,
        large.peak_rise
    );
}
