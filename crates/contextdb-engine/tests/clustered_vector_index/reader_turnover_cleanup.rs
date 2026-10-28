//! Current-tree retirement advances through overlapping finite readers.
//! Removing those entries does not assert that storage pages behind retained roots are reusable.
use super::support::{
    PhysicalPlace, SwitchPhase, empty, int_ids, maintenance_cycles, open_caller_driven, params,
    physical_pages, remap_progress, serving, switch_state, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;

const ROWS: usize = 64;
const GROWTH: usize = 16;
const BATCH: usize = 2;
const DIM: usize = 8;
const SQL: &str = "SELECT id FROM items ORDER BY embedding <=> $v USE VECTOR INDEXED LIMIT 5";

fn bound() -> std::collections::HashMap<String, Value> {
    params([("v", Value::Vector(unit_vector(7, DIM)))])
}

fn old_list_entries(db: &Database, generation: u64) -> usize {
    // This enumerates payload entries in the current native tree. It does not
    // enumerate retained COW roots or measure physical allocator page release.
    physical_pages(db, "items", "embedding").iter().filter(|page|
        matches!(page.place, PhysicalPlace::ListPage { generation: found } if found == generation)
    ).count()
}

#[test]
fn obsolete_current_tree_entries_are_removed_while_finite_readers_overlap() {
    let directory = tempfile::tempdir().unwrap();
    let db = open_caller_driven(&directory.path().join("turnover.db"));
    db.execute("CREATE TABLE writes (id INT PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(8) SEARCH_MODE INDEXED SEARCH (EXPECTED_VECTORS = 64, TARGET_ACCURACY = 95) IVF (LISTS = 4, PROBES = 4, REFINE_FACTOR = 64.00) CONSOLIDATION (CHANGE_PERCENT = 10) WITH (training_samples = 64, training_iterations = 1, build_batch_entries = 2, remap_batch_entries = 2))",
        &empty(),
    ).unwrap();
    for id in 0..ROWS {
        db.execute(
            "INSERT INTO items VALUES ($id, $v)",
            &params([
                ("id", Value::Int64(id as i64)),
                ("v", Value::Vector(unit_vector(id as u64, DIM))),
            ]),
        )
        .unwrap();
    }
    let mut first = None;
    for _ in 0..maintenance_cycles(ROWS, 1, 1) {
        let report = db.run_maintenance_cycle().unwrap();
        assert!(
            report.vector.first_failure.is_none(),
            "initial maintenance refused: {report:?}"
        );
        if report.vector.ready_partitions == 1 && report.vector.remaining_partitions == 0 {
            first = Some(serving(&db, "items", "embedding", &[]).unwrap().generation);
            break;
        }
    }
    let first = first.expect("the initial native generation becomes ready");
    let old_snapshot = db.snapshot();
    let old_reader = db
        .pin_snapshot(old_snapshot)
        .expect("open the pre-replacement reader");
    let original = int_ids(&db.execute_at_snapshot(SQL, &bound(), old_snapshot).unwrap());
    assert_eq!(original[0], 7);

    let tx = db.begin().unwrap();
    db.execute_in_tx(tx, "DELETE FROM items WHERE id = 7", &empty())
        .unwrap();
    for offset in 0..GROWTH {
        let vector = if offset == 0 {
            unit_vector(7, DIM)
        } else {
            unit_vector(1000 + offset as u64, DIM)
        };
        db.execute_in_tx(
            tx,
            "INSERT INTO items VALUES ($id, $v)",
            &params([
                ("id", Value::Int64(1000 + offset as i64)),
                ("v", Value::Vector(vector)),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    let mut replaced_and_remapped = false;
    for _ in 0..maintenance_cycles(ROWS + GROWTH, 1, 1) {
        let report = db.run_maintenance_cycle().unwrap();
        assert!(
            report.vector.first_failure.is_none(),
            "replacement maintenance refused: {report:?}"
        );
        let now = serving(&db, "items", "embedding", &[]).unwrap();
        if now.generation != first
            && switch_state(&db, "items", "embedding", &[]).phase == SwitchPhase::Serving
            && remap_progress(&db, "items", "embedding", &[]).entries_remaining == 0
        {
            replaced_and_remapped = true;
            break;
        }
    }
    assert!(
        replaced_and_remapped,
        "replacement and its remap finish while the old reader is retained"
    );
    assert_eq!(
        int_ids(&db.execute_at_snapshot(SQL, &bound(), old_snapshot).unwrap()),
        original,
        "the reader opened before replacement keeps its original indexed answer"
    );
    let current = int_ids(&db.execute(SQL, &bound()).unwrap());
    assert_eq!(current[0], 1000);
    assert!(!current.contains(&7));
    let obsolete = old_list_entries(&db, first);
    assert!(
        obsolete > BATCH,
        "obsolete current-tree list entries span multiple cleanup batches: {obsolete}"
    );
    let allocated_before = db.__durable_page_population_for_test().unwrap();

    // A new reader starts before the old reader finishes. Each following
    // reader starts before its predecessor ends, so no maintenance call sees
    // a global gap. Every individual reader still has a finite lifetime.
    let mut reader_snapshot = db.snapshot();
    let mut reader = db.pin_snapshot(reader_snapshot).unwrap();
    drop(old_reader);
    let mut remaining = obsolete;
    let mut positive_batches = 0;
    for cycle in 0..maintenance_cycles(ROWS + GROWTH, 1, 1) {
        assert_eq!(
            int_ids(
                &db.execute_at_snapshot(SQL, &bound(), reader_snapshot)
                    .unwrap()
            ),
            current
        );
        db.execute(
            "INSERT INTO writes VALUES ($id)",
            &params([("id", Value::Int64(cycle as i64))]),
        )
        .expect("ordinary writes stay admitted under reader turnover");
        let next_snapshot = db.snapshot();
        let next = db
            .pin_snapshot(next_snapshot)
            .expect("the next reader overlaps its predecessor");
        drop(reader);
        reader = next;
        reader_snapshot = next_snapshot;
        let report = db
            .run_maintenance_cycle()
            .expect("one finite cleanup cycle under an active reader");
        assert!(
            report.vector.first_failure.is_none(),
            "cleanup refused: {report:?}"
        );
        let after = old_list_entries(&db, first);
        assert!(
            after <= remaining,
            "obsolete current-tree entries never grow"
        );
        assert!(
            remaining - after <= BATCH,
            "each cycle removes at most the declared batch"
        );
        positive_batches += usize::from(after < remaining);
        remaining = after;
        if after == 0 {
            break;
        }
    }
    let allocated_during_turnover = db.__durable_page_population_for_test().unwrap();
    eprintln!(
        "current_tree_obsolete_list_entries_before={obsolete} after={remaining} positive_batches={positive_batches}; durable_allocator_population_before={allocated_before:?} during_turnover={allocated_during_turnover:?}; physical_release_not_inferred_from_current_tree_removal=true"
    );
    assert_eq!(
        int_ids(
            &db.execute_at_snapshot(SQL, &bound(), reader_snapshot)
                .unwrap()
        ),
        current,
        "the last overlapping reader retains the replacement's answer"
    );
    drop(reader);
    let mut without_readers = remaining;
    for _ in 0..maintenance_cycles(ROWS + GROWTH, 1, 1) {
        if without_readers == 0 {
            break;
        }
        let report = db
            .run_maintenance_cycle()
            .expect("reader-free cleanup positive control");
        assert!(report.vector.first_failure.is_none());
        without_readers = old_list_entries(&db, first);
    }
    let allocated_after_reader_release = db.__durable_page_population_for_test().unwrap();
    eprintln!(
        "reader_free_control_obsolete_entries={without_readers}; durable_allocator_population_after_reader_release={allocated_after_reader_release:?}; physical_release_of_specific_index_pages_not_measured=true"
    );
    assert_eq!(
        without_readers, 0,
        "the obsolete entries are eligible for cleanup when readers are absent"
    );
    assert_eq!(int_ids(&db.execute(SQL, &bound()).unwrap()), current);
    assert_eq!(
        remaining, 0,
        "obsolete current-tree list entries must drain without a database-wide reader gap"
    );
    assert!(
        positive_batches >= 2,
        "cleanup progresses in multiple bounded batches"
    );
}
