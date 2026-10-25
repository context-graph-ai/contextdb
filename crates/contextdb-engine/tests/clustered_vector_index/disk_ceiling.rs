//! Building or replacing a clustered generation respects the store's
//! declared disk ceiling, phase by phase. Each persisted build write (training
//! reservoir, build accumulator, unpublished list run, prepared replacement)
//! passes disk admission before any byte of it reaches the store. When a
//! write's projected store size would pass the ceiling, that write is refused
//! and never executed; the refusal surfaces as the existing typed disk-budget
//! error naming the ceiling; every index reservation is returned; the old
//! generation keeps serving every committed row; and once the ceiling is
//! lifted the build finishes and the store reopens cleanly.

use super::support::{
    BuildWriteClass, BuildWriteEvent, MAX_MAINTENANCE_CYCLES, PhysicalPage, build_progress, count,
    drive_until_serving, empty, int_ids, ivf, last_build_refusal, observe_build_writes,
    observe_search, open_caller_driven, open_with_build_batch_rows, outstanding_index_reservations,
    params, physical_pages, serving, unit_vector,
};
use contextdb_core::{Error, Value, Wallclock};
use contextdb_engine::Database;
use std::path::Path;
use std::sync::{Arc, Mutex};

const DIMENSIONS: usize = 64;
const FIRST_ROWS: std::ops::Range<i64> = 0..120;
const SECOND_ROWS: std::ops::Range<i64> = 1_000..1_240;
const BUILD_BATCH_ROWS: u64 = 16;
const REFUSED_CYCLES: usize = 3;

fn insert_rows(db: &Database, ids: std::ops::Range<i64>) {
    let tx = db.begin_or_panic();
    for id in ids {
        db.execute_in_tx(
            tx,
            "INSERT INTO ceiling_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(id as u64, DIMENSIONS)),
                ),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit the rows");
}

/// A store serving its first generation, with a doubling of rows committed
/// after it so a multi-batch replacement is due. Every step is the same for
/// every store built by this function, so two such stores reach the same
/// state through the same writes. Returns the first generation.
fn store_with_due_replacement(path: &Path) -> (Database, u64) {
    let db = open_with_build_batch_rows(path, BUILD_BATCH_ROWS);
    db.execute(
        &format!(
            "CREATE TABLE ceiling_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {} CONSOLIDATION (CHANGE_PERCENT = 10))",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    insert_rows(&db, FIRST_ROWS);
    drive_until_serving(&db);
    let first = serving(&db, "ceiling_items", "embedding", &[])
        .expect("the first generation is serving")
        .generation;
    insert_rows(&db, SECOND_ROWS);
    (db, first)
}

fn ids() -> Vec<i64> {
    FIRST_ROWS.chain(SECOND_ROWS).collect()
}

/// The contents of every unpublished build-state page, prepared replacement
/// included, as a sorted list of (place, bytes).
fn build_state_contents(db: &Database) -> Vec<(String, Vec<u8>)> {
    let mut pages = physical_pages(db, "ceiling_items", "embedding")
        .into_iter()
        .filter(|page: &PhysicalPage| page.place.is_unpublished_build_state())
        .map(|page| (format!("{:?}", page.place), page.bytes))
        .collect::<Vec<_>>();
    pages.sort();
    pages
}

fn assert_every_row_is_served(db: &Database, stage: &str) {
    let ids = ids();
    assert_eq!(
        count(db, "ceiling_items"),
        ids.len() as i64,
        "{stage}: every row survives"
    );
    for id in ids {
        let (result, work) = observe_search(db, || {
            db.execute(
                "SELECT id FROM ceiling_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
                &params([("query", Value::Vector(unit_vector(id as u64, DIMENSIONS)))]),
            )
            .expect("indexed search")
        });
        assert!(
            work.clustered_route,
            "{stage}: row {id} was searched on the clustered index"
        );
        assert_eq!(int_ids(&result), vec![id], "{stage}: row {id} is served");
    }
}

fn assert_disk_refusal(error: &Error, ceiling: u64, stage: &str) {
    match error {
        Error::DiskBudgetExceeded {
            budget_limit_bytes, ..
        } => assert_eq!(
            *budget_limit_bytes, ceiling,
            "{stage}: the refusal names the declared ceiling"
        ),
        other => {
            panic!("{stage}: the build is refused with the typed disk-budget error: {other:?}")
        }
    }
}

/// Runs one maintenance cycle and returns every build write event it made.
fn observed_cycle(db: &Database) -> (contextdb_core::Result<()>, Vec<BuildWriteEvent>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    let result = observe_build_writes(
        Arc::new(move |event: &BuildWriteEvent| sink.lock().unwrap().push(event.clone())),
        || db.run_maintenance_cycle().map(|_| ()),
    );
    let events = events.lock().unwrap().clone();
    (result, events)
}

/// The refusal surfaced by a refused cycle: its own error, or the Context's
/// recorded build refusal. The recorded refusal is a diagnostic record, not
/// build payload.
fn surfaced_refusal(db: &Database, result: contextdb_core::Result<()>, stage: &str) -> Error {
    match result {
        Err(error) => error,
        Ok(()) => last_build_refusal(db, "ceiling_items", "embedding", &[])
            .unwrap_or_else(|| panic!("{stage}: the refused build records why it stopped")),
    }
}

fn written_classes(events: &[BuildWriteEvent]) -> Vec<BuildWriteClass> {
    events
        .iter()
        .filter_map(|event| match event {
            BuildWriteEvent::Written { class, .. } => Some(*class),
            BuildWriteEvent::Admission { .. } => None,
        })
        .collect()
}

fn assert_still_serving(db: &Database, first: u64, stage: &str) {
    assert_eq!(
        outstanding_index_reservations(db),
        0,
        "{stage}: the refused cycle returned every index reservation"
    );
    assert_eq!(
        serving(db, "ceiling_items", "embedding", &[]).map(|serving| serving.generation),
        Some(first),
        "{stage}: the old generation keeps serving"
    );
}

fn assert_finishes_once_lifted(db: Database, path: &Path, first: u64, stage: &str) {
    db.set_disk_limit(None).expect("lift the disk ceiling");
    drive_until_serving(&db);
    assert_ne!(
        serving(&db, "ceiling_items", "embedding", &[])
            .expect("a generation is serving")
            .generation,
        first,
        "{stage}: with room again the replacement publishes"
    );
    assert_every_row_is_served(&db, &format!("{stage}, after the replacement"));
    db.close().expect("close the store");
    let db = open_caller_driven(path);
    assert_every_row_is_served(&db, &format!("{stage}, after reopen"));
    db.close().expect("close the reopened store");
}

/// A store already past its ceiling: every build write is refused at
/// admission and none executes.
#[test]
fn a_replacement_build_under_an_exhausted_ceiling_is_refused_and_writes_nothing() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("ceiling-exhausted.db");
    let (db, first) = store_with_due_replacement(&path);
    let ceiling = 1;
    db.set_disk_limit(Some(ceiling))
        .expect("declare a ceiling the store is already past");
    let file_before = db.disk_file_size().expect("a file-backed store has a size");
    let contents_before = build_state_contents(&db);
    for cycle in 0..REFUSED_CYCLES {
        let stage = format!("exhausted ceiling, refused cycle {cycle}");
        let (result, events) = observed_cycle(&db);
        assert_disk_refusal(&surfaced_refusal(&db, result, &stage), ceiling, &stage);
        assert_eq!(
            written_classes(&events),
            Vec::new(),
            "{stage}: no build write executed: {events:?}"
        );
        for event in &events {
            if let BuildWriteEvent::Admission { admitted, .. } = event {
                assert!(!admitted, "{stage}: no build write is admitted: {event:?}");
            }
        }
        assert_eq!(
            db.disk_file_size(),
            Some(file_before),
            "{stage}: the store file did not grow"
        );
        assert_eq!(
            build_state_contents(&db),
            contents_before,
            "{stage}: the contents of the build-state pages did not change"
        );
        assert_still_serving(&db, first, &stage);
    }
    assert_every_row_is_served(&db, "exhausted ceiling");
    assert_finishes_once_lifted(db, &path, first, "exhausted ceiling");
}

/// Drives `db`'s replacement, cycle by cycle with no ceiling, until its next
/// step writes `class`.
fn drive_until_next_write(db: &Database, class: BuildWriteClass) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if build_progress(db, "ceiling_items", "embedding", &[]).next_write == Some(class) {
            return;
        }
        db.run_maintenance_cycle().expect("one bounded build cycle");
    }
    panic!("the replacement never reached a {class:?} write");
}

fn first_admission(
    events: &[BuildWriteEvent],
    class: BuildWriteClass,
) -> Option<(usize, u64, u64, u64)> {
    events
        .iter()
        .enumerate()
        .find_map(|(index, event)| match event {
            BuildWriteEvent::Admission {
                class: seen,
                requested_bytes,
                current_store_bytes,
                projected_store_bytes,
                ..
            } if *seen == class => Some((
                index,
                *requested_bytes,
                *current_store_bytes,
                *projected_store_bytes,
            )),
            _ => None,
        })
}

/// From an admissible store, a ceiling one byte below the projected size of
/// the targeted phase's first write refuses exactly that write.
fn refused_at(class: BuildWriteClass) {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().expect("temporary store directory");

    // Unconstrained twin: learn the targeted write's admission at this step.
    let (twin, _) = store_with_due_replacement(&root.path().join("twin.db"));
    drive_until_next_write(&twin, class);
    let twin_size = twin
        .disk_file_size()
        .expect("a file-backed store has a size");
    let (twin_result, twin_events) = observed_cycle(&twin);
    twin_result.expect("the unconstrained step completes");
    let (twin_index, requested, current, projected) = first_admission(&twin_events, class)
        .unwrap_or_else(|| panic!("the step writes {class:?}: {twin_events:?}"));
    assert!(
        requested > 0 && projected > current,
        "the {class:?} write grows the store, so a ceiling can refuse it: {twin_events:?}"
    );
    for event in &twin_events[..twin_index] {
        if let BuildWriteEvent::Admission {
            projected_store_bytes,
            ..
        } = event
        {
            assert!(
                *projected_store_bytes < projected,
                "every write before the {class:?} write projects a smaller store, so one ceiling \
                 admits them and refuses it: {twin_events:?}"
            );
        }
    }
    assert!(
        written_classes(&twin_events).contains(&class),
        "unconstrained, the admitted {class:?} write executes"
    );
    twin.close().expect("close the twin");

    // Constrained store: the same steps, then a ceiling the store is still
    // inside but the targeted write would pass.
    let path = root.path().join("constrained.db");
    let (db, first) = store_with_due_replacement(&path);
    drive_until_next_write(&db, class);
    let size = db.disk_file_size().expect("a file-backed store has a size");
    assert_eq!(
        size, twin_size,
        "both stores reached the same step identically"
    );
    let ceiling = projected - 1;
    assert!(
        size <= ceiling,
        "the store is admissible under the ceiling it is given"
    );
    db.set_disk_limit(Some(ceiling))
        .expect("declare a ceiling the store is inside");

    let (result, events) = observed_cycle(&db);
    let stage = format!("{class:?} refused");
    let (index, _, _, refused_projection) = first_admission(&events, class)
        .unwrap_or_else(|| panic!("{stage}: the targeted write was reached: {events:?}"));
    match &events[index] {
        BuildWriteEvent::Admission {
            admitted,
            ceiling: seen,
            ..
        } => {
            assert!(
                !admitted,
                "{stage}: the targeted write is refused at admission"
            );
            assert_eq!(
                *seen,
                Some(ceiling),
                "{stage}: admission saw the declared ceiling"
            );
        }
        BuildWriteEvent::Written { .. } => unreachable!(),
    }
    assert!(
        refused_projection > ceiling,
        "{stage}: the refused write would have passed the ceiling"
    );
    for event in &events[..index] {
        if let BuildWriteEvent::Admission {
            admitted,
            projected_store_bytes,
            ..
        } = event
        {
            assert!(
                *admitted && *projected_store_bytes <= ceiling,
                "{stage}: writes before the targeted one fit and were admitted: {event:?}"
            );
        }
    }
    assert!(
        !written_classes(&events[index..]).contains(&class),
        "{stage}: the refused {class:?} write never executed: {events:?}"
    );
    assert_disk_refusal(&surfaced_refusal(&db, result, &stage), ceiling, &stage);
    assert!(
        db.disk_file_size().expect("a file-backed store has a size") <= ceiling,
        "{stage}: the store stays within its ceiling"
    );
    assert_eq!(
        build_progress(&db, "ceiling_items", "embedding", &[]).next_write,
        Some(class),
        "{stage}: the build did not advance past the refused write"
    );
    assert_still_serving(&db, first, &stage);
    assert_every_row_is_served(&db, &stage);
    assert_finishes_once_lifted(db, &path, first, &stage);
}

#[test]
fn a_training_reservoir_write_past_the_ceiling_is_refused_before_it_executes() {
    refused_at(BuildWriteClass::TrainingReservoir);
}

#[test]
fn a_build_accumulator_write_past_the_ceiling_is_refused_before_it_executes() {
    refused_at(BuildWriteClass::BuildAccumulator);
}

#[test]
fn an_unpublished_run_write_past_the_ceiling_is_refused_before_it_executes() {
    refused_at(BuildWriteClass::UnpublishedRun);
}

#[test]
fn a_prepared_replacement_write_past_the_ceiling_is_refused_before_it_executes() {
    refused_at(BuildWriteClass::PreparedReplacement);
}
