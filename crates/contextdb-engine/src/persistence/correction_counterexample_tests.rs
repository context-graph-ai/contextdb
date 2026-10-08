#![allow(dead_code)] // Existing compatibility and test seam shape.
//! Counterexamples for the durable ordering and proof-accounting invariants.

use super::*;
use crate::persistence::DurableRelationalIndexKey;
use crate::persistence::{
    ReadPersistenceLoad, load_read_image_with_memory, verified_ordered_run_test_seams,
};
use crate::{Database, MaintenancePolicy};
use contextdb_core::SnapshotId;
use contextdb_core::{DirectedValue, TotalOrdAsc, TotalOrdDesc};
use redb::TableDefinition;
use std::collections::HashMap;

fn posting_key(parts: Vec<DirectedValue>, row: u64) -> Vec<u8> {
    DurableRelationalIndexKey::new(parts, RowId(row), TxId(0))
        .expect("encode a durable posting key")
        .0
}

fn ascending_text(text: &str, row: u64) -> Vec<u8> {
    posting_key(
        vec![DirectedValue::Asc(TotalOrdAsc(Value::Text(
            text.to_owned(),
        )))],
        row,
    )
}

fn descending_text(text: &str, row: u64) -> Vec<u8> {
    posting_key(
        vec![DirectedValue::Desc(TotalOrdDesc(Value::Text(
            text.to_owned(),
        )))],
        row,
    )
}

fn ascending_int(value: i64, row: u64) -> Vec<u8> {
    posting_key(
        vec![DirectedValue::Asc(TotalOrdAsc(Value::Int64(value)))],
        row,
    )
}

fn compare(left: &[u8], right: &[u8]) -> Ordering {
    RedbPersistence::compare_relational_index_order_keys(left, right)
}

/// `aa` and `z` are valid
/// ascending TEXT keys, and `z` with one trailing byte no longer decodes
/// exactly, so a pair-dependent fallback orders it by raw bytes: `aa < z`
/// typed, `z < trailing` and `trailing < aa` raw.
#[test]
fn the_durable_order_has_no_cycle_over_a_valid_pair_and_a_trailing_byte() {
    let aa = ascending_text("aa", 0);
    let z = ascending_text("z", 0);
    let mut trailing = z.clone();
    trailing.push(0);

    assert_eq!(compare(&aa, &z), Ordering::Less, "valid SQL order");
    let first = compare(&z, &trailing);
    let second = compare(&trailing, &aa);
    assert!(
        !(first == Ordering::Less && second == Ordering::Less),
        "aa < z < z+trailing < aa is a cycle Redb can navigate: \
         z..trailing={first:?} trailing..aa={second:?}"
    );
}

/// Counterexample 1, general form: every ordered triple of stored byte strings
/// must be transitive, and valid keys must keep the order the declaration
/// states.
#[test]
fn the_durable_order_is_transitive_over_every_triple_of_stored_keys() {
    let mut keys = vec![
        ascending_text("aa", 0),
        ascending_text("z", 0),
        ascending_text("z", 1),
        ascending_int(7, 0),
        descending_text("m", 0),
        descending_text("z", 0),
        posting_key(vec![DirectedValue::Asc(TotalOrdAsc(Value::Null))], 0),
        Vec::new(),
        vec![0xff; 16],
    ];
    let valid = ascending_text("z", 0);
    let mut trailing = valid.clone();
    trailing.push(0);
    keys.push(trailing);
    keys.push(valid[..valid.len() - 1].to_vec());
    keys.push(valid[..4].to_vec());
    keys.push(posting_key(
        vec![
            DirectedValue::Asc(TotalOrdAsc(Value::Text("a".into()))),
            DirectedValue::Desc(TotalOrdDesc(Value::Int64(3))),
        ],
        2,
    ));

    for left in &keys {
        assert_eq!(compare(left, left), Ordering::Equal, "reflexive");
        for right in &keys {
            assert_eq!(
                compare(left, right).reverse(),
                compare(right, left),
                "antisymmetric"
            );
            if left != right {
                assert_ne!(compare(left, right), Ordering::Equal, "distinct keys tie");
            }
        }
    }
    for left in &keys {
        for middle in &keys {
            if compare(left, middle) != Ordering::Less {
                continue;
            }
            for right in &keys {
                if compare(middle, right) != Ordering::Less {
                    continue;
                }
                assert_eq!(
                    compare(left, right),
                    Ordering::Less,
                    "transitivity broken over a stored triple"
                );
            }
        }
    }

    // Valid keys keep exactly the declared order.
    assert_eq!(
        compare(&ascending_text("aa", 0), &ascending_text("z", 0)),
        Ordering::Less
    );
    assert_eq!(
        compare(&descending_text("z", 0), &descending_text("m", 0)),
        Ordering::Less,
        "a descending index stores the larger value first"
    );
    assert_eq!(
        compare(&ascending_text("z", 0), &ascending_text("z", 1)),
        Ordering::Less,
        "the row breaks a tie between equal values"
    );
    assert_eq!(
        compare(
            &ascending_text("z", 0),
            &posting_key(vec![DirectedValue::Asc(TotalOrdAsc(Value::Null))], 0)
        ),
        Ordering::Less,
        "NULL keeps its declared place after every value in an ascending index"
    );
}

fn tiny_database(path: &std::path::Path) -> Database {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO notes VALUES ($id, 'first')",
        &HashMap::from([("id".into(), Value::Int64(1))]),
    )
    .unwrap();
    db
}

/// Counterexample 3: a low durable limit must stay recoverable. The write that
/// records the new limit now maintains the committed proof, so persisting a
/// raise under the old tiny budget refuses and the operator is trapped.
#[test]
fn a_low_memory_limit_can_be_raised_and_removed_again() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("recoverable-limit.db");
    let db = tiny_database(&path);

    db.set_memory_limit(Some(1)).expect("lowering is allowed");
    assert_eq!(db.accountant().usage().limit, Some(1));
    db.set_memory_limit(Some(64 * 1024 * 1024))
        .expect("an operator can raise a limit that is too low to write under");
    assert_eq!(db.accountant().usage().limit, Some(64 * 1024 * 1024));
    db.set_memory_limit(Some(1)).expect("lowering again");
    db.set_memory_limit(None)
        .expect("an operator can remove a limit that is too low to write under");
    assert_eq!(db.accountant().usage().limit, None);
    db.close().unwrap();

    // The durable value agrees with the runtime one across a reopen.
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(db.accountant().usage().limit, None);
    db.execute(
        "INSERT INTO notes VALUES ($id, 'second')",
        &HashMap::from([("id".into(), Value::Int64(2))]),
    )
    .expect("the recovered database still writes");
    db.close().unwrap();
}

/// Counterexample 3, second half: a refused limit transition rolls back, and
/// leaves nothing admitted behind.
#[test]
fn a_refused_limit_transition_rolls_back_runtime_and_durable_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("rolled-back-limit.db");
    let db = tiny_database(&path);
    let before = db.accountant().usage().limit;
    let held = db.accountant().usage().used;

    // Below what the durable write itself needs: the transition must refuse
    // as a whole rather than half-applying.
    db.set_memory_limit(Some(1)).unwrap();
    let trapped = db.execute(
        "INSERT INTO notes VALUES ($id, 'blocked')",
        &HashMap::from([("id".into(), Value::Int64(3))]),
    );
    assert!(
        matches!(trapped, Err(Error::MemoryBudgetExceeded { .. })),
        "an impossible write is still refused: {trapped:?}"
    );
    db.set_memory_limit(before).expect("recovery is possible");
    assert_eq!(db.accountant().usage().limit, before);
    assert!(
        db.accountant().usage().used <= held.saturating_add(64 * 1024),
        "a refused write leaves nothing admitted behind"
    );
    db.execute(
        "INSERT INTO notes VALUES ($id, 'after recovery')",
        &HashMap::from([("id".into(), Value::Int64(4))]),
    )
    .expect("the database works again once the limit is back");
    db.close().unwrap();
}

/// Counterexample 4: the export destination writes a whole proof of its own.
/// Under a budget too small to admit it the export must refuse, not maintain
/// the destination proof outside `MEMORY_LIMIT`.
#[test]
fn export_admits_its_destination_proof_through_the_owning_accountant() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("exported.db");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..64 {
        db.execute(
            "INSERT INTO notes VALUES ($id, $body)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("body".into(), Value::Text(format!("body-{id:04}"))),
            ]),
        )
        .unwrap();
    }

    let generous = root.path().join("generous.cdb");
    db.export_snapshot(&generous)
        .expect("an admitted export succeeds");

    // What the export admitted through this database's accountant, and how it
    // grows with the content the destination has to build a proof over. A
    // destination that admits nothing cannot show this growth.
    let small = db.accountant().admitted_bytes_for_test();
    let again = root.path().join("again.cdb");
    db.export_snapshot(&again).unwrap();
    let per_small_export = db.accountant().admitted_bytes_for_test() - small;
    for id in 64..512 {
        db.execute(
            "INSERT INTO notes VALUES ($id, $body)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("body".into(), Value::Text(format!("body-{id:04}"))),
            ]),
        )
        .unwrap();
    }
    let large = db.accountant().admitted_bytes_for_test();
    let bigger = root.path().join("bigger.cdb");
    db.export_snapshot(&bigger).unwrap();
    let per_large_export = db.accountant().admitted_bytes_for_test() - large;
    eprintln!("export admission: 64 rows={per_small_export} 512 rows={per_large_export}");
    // Every row the larger export copies makes the destination maintain its
    // own committed proof over that row: at least one chunk decode and its
    // staged replacement. Below this the destination is building a proof
    // nobody admitted.
    const DESTINATION_PROOF_BYTES_PER_COPIED_ROW: usize = 5 * 1024;
    let copied = 512 - 64;
    assert!(
        per_large_export.saturating_sub(per_small_export)
            > copied * DESTINATION_PROOF_BYTES_PER_COPIED_ROW,
        "the destination's proof is admitted through this database's accountant: small={per_small_export} large={per_large_export} copied={copied}"
    );

    let used = db.accountant().usage().used;
    db.set_memory_limit(Some(used + 8 * 1024)).unwrap();
    let tight = root.path().join("tight.cdb");
    let refused = db.export_snapshot(&tight);
    assert!(
        matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
        "an export that cannot be admitted is refused: {refused:?}"
    );
    assert!(
        !tight.exists(),
        "a refused export publishes no artifact and cleans up after itself"
    );
    db.set_memory_limit(None).unwrap();
    db.close().unwrap();
}

fn vector_fixture(path: &std::path::Path) -> Database {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE points (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, v VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 8, EF_CONSTRUCTION = 32))",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX points_bucket ON points (bucket ASC)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..24i64 {
        db.execute(
            "INSERT INTO points VALUES ($id, $bucket, '[1,0,0]')",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("bucket".into(), Value::Int64(id % 2)),
            ]),
        )
        .unwrap();
    }
    db.run_maintenance_cycle().unwrap();
    db
}

/// Counterexample 5: when the authenticated run cannot be built, the identity
/// route must not answer from the physical index alone. A posting removed
/// behind the proof's back is a missing row nobody reports.
fn posting_fixture(path: &std::path::Path) {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE postings (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX by_bucket ON postings(bucket)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..5 {
        db.execute(
            "INSERT INTO postings VALUES ($id, 1)",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.close().unwrap();
}

fn identity_cursor(reverse: bool) -> contextdb_relational::mem::BoundedOrderedRowCursor {
    let mut cursor =
        contextdb_relational::mem::BoundedOrderedRowCursor::for_index("by_bucket".into(), reverse);
    cursor.select_identities_only();
    cursor.source_configure(1);
    cursor
}

fn pull_identity(
    load: &ReadPersistenceLoad,
    cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
) -> Result<Option<RowId>> {
    let meta = &load.image.table_meta["postings"];
    let declaration = RedbPersistence::relational_index_declarations(meta)
        .into_iter()
        .find(|declaration| declaration.name == "by_bucket")
        .unwrap();
    let snapshot = SnapshotId(
        load.image
            .newest_committed_tx_for_test()
            .map(|tx| tx.0)
            .unwrap(),
    );
    let persistence_fixture_parent_1 =
        crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
            load.source
                .verified_ordered_runs
                .memory
                .accountant
                .usage()
                .limit
                .map(|bytes| bytes as u64)
                .unwrap_or(u64::MAX),
        );
    let persistence_fixture_reader_1 = persistence_fixture_parent_1
        .child()
        .expect("admit the persistence fixture reader child");

    load.source
        .bounded_relational_ordered_next(
            "postings",
            meta,
            &declaration,
            snapshot,
            cursor,
            &mut || Ok(()),
            &mut || Ok(()),
            &mut |_| Ok(()),
            persistence_fixture_reader_1,
        )
        .map(|row| row.map(|row| row.row_id))
}

fn remove_one_physical_posting(path: &std::path::Path) {
    let file = redb::Database::open(path).unwrap();
    let write = file.begin_write().unwrap();
    {
        let name = RedbPersistence::relational_index_table_name("postings", "by_bucket");
        let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
            TableDefinition::new(name.as_str());
        let mut index = write.open_table(definition).unwrap();
        let key = index
            .iter()
            .unwrap()
            .next()
            .expect("a stored posting")
            .unwrap()
            .0
            .value()
            .0;
        assert!(
            index
                .remove(DurableRelationalIndexKey(key))
                .unwrap()
                .is_some()
        );
    }
    write.commit().unwrap();
}

#[test]
fn an_exhausted_proof_pool_never_answers_from_the_physical_index_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("exhausted-pool.db");
    posting_fixture(&path);
    remove_one_physical_posting(&path);

    for reverse in [false, true] {
        // With proof memory available the damage is corruption, and it is
        // reported before any row.
        let load = load_read_image_with_memory(&path, Some(8 * 1024 * 1024)).unwrap();
        let mut cursor = identity_cursor(reverse);
        let detected = pull_identity(&load, &mut cursor)
            .expect_err("a missing physical posting is corruption");
        assert!(
            matches!(detected, Error::StoreCorrupted { .. }),
            "reverse={reverse}: {detected:?}"
        );
        drop(load);

        // With no proof memory the answer cannot be proved complete, so it is
        // refused - never delivered from the physical index alone.
        let load = load_read_image_with_memory(&path, Some(8 * 1024 * 1024)).unwrap();
        let mut cursor = identity_cursor(reverse);
        verified_ordered_run_test_seams::arm_verified_run_memory_exhaustion_for_test(true);
        let refused = pull_identity(&load, &mut cursor);
        verified_ordered_run_test_seams::arm_verified_run_memory_exhaustion_for_test(false);
        match refused {
            Err(Error::MemoryBudgetExceeded { .. }) | Err(Error::StoreCorrupted { .. }) => {}
            other => {
                panic!("reverse={reverse}: an unprovable range must refuse, not answer: {other:?}")
            }
        }
    }

    // The boundary where a run is already part-delivered: losing the proof
    // mid-range must refuse too, not continue from the physical index.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("exhausted-resume.db");
    posting_fixture(&path);
    let load = load_read_image_with_memory(&path, Some(8 * 1024 * 1024)).unwrap();
    let mut cursor = identity_cursor(false);
    assert!(
        pull_identity(&load, &mut cursor).unwrap().is_some(),
        "the first identity comes from the authenticated run"
    );
    load.source.verified_ordered_runs.clear_for_test();
    verified_ordered_run_test_seams::arm_verified_run_memory_exhaustion_for_test(true);
    let resumed = pull_identity(&load, &mut cursor);
    verified_ordered_run_test_seams::arm_verified_run_memory_exhaustion_for_test(false);
    assert!(
        resumed.is_err(),
        "a resume that cannot rebuild its proof refuses: {resumed:?}"
    );
}

/// A memory-limit transition must never let a concurrent operation see capacity
/// the durable value has not committed to. These two observation points are the
/// whole contract: while the durable write is in flight, and immediately after
/// it commits, the published runtime limit must be the *smaller* of where the
/// transition started and where it is going, and an allocation that only the
/// larger limit would admit must be refused.
mod transition_observation {
    use super::*;
    use crate::database::memory_limit_transition_seam::{
        TransitionPhase, arm_memory_limit_transition_pause_for_test,
        disarm_memory_limit_transition_pause_for_test,
    };
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::mpsc;

    struct Disarm;
    impl Drop for Disarm {
        fn drop(&mut self) {
            disarm_memory_limit_transition_pause_for_test();
        }
    }

    #[derive(Debug)]
    pub(super) struct Observed {
        pub(super) phase: TransitionPhase,
        pub(super) published: Option<usize>,
        pub(super) admitted_beyond_the_smaller_limit: bool,
    }

    /// Run `transition` on another thread and sample the accountant at both
    /// observation points from this one. `probe` is the number of bytes only a
    /// limit above `floor` could admit.
    pub(super) fn observe_transition(
        database: &Database,
        floor: usize,
        transition: impl FnOnce(&Database) -> Result<()> + Send,
    ) -> (Result<()>, Vec<Observed>) {
        let (reached_tx, reached_rx) = mpsc::channel::<TransitionPhase>();
        let (resume_tx, resume_rx) = mpsc::channel::<()>();
        let reached_tx = Mutex::new(reached_tx);
        let resume_rx = Mutex::new(resume_rx);
        let hook: Arc<dyn Fn(TransitionPhase) + Send + Sync> = Arc::new(move |phase| {
            reached_tx
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .send(phase)
                .expect("the observing thread is waiting");
            resume_rx
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .recv()
                .expect("the observing thread releases the transition");
        });
        let mut samples = Vec::new();
        let outcome = std::thread::scope(|scope| {
            let worker = scope.spawn(move || {
                // The seam is armed on the transitioning thread itself, so no
                // other transition running beside this test is ever suspended.
                arm_memory_limit_transition_pause_for_test(hook);
                let _disarm = Disarm;
                transition(database)
            });
            while let Ok(phase) = reached_rx.recv() {
                let usage = database.accountant().usage();
                // One byte more than the floor still has room under the larger
                // limit and none under the smaller one.
                let probe = floor.saturating_sub(usage.used).saturating_add(1);
                let admitted = database.accountant().try_allocate(probe).is_ok();
                if admitted {
                    database.accountant().release(probe);
                }
                samples.push(Observed {
                    phase,
                    published: usage.limit,
                    admitted_beyond_the_smaller_limit: admitted,
                });
                if resume_tx.send(()).is_err() {
                    break;
                }
            }
            worker.join().expect("the transition thread does not panic")
        });
        (outcome, samples)
    }
}

/// Counterexample: widening publishes the target runtime budget before the
/// durable write that records it, so a concurrent operation can consume
/// capacity that never commits.
#[test]
fn a_widening_transition_never_publishes_capacity_its_durable_write_has_not_committed() {
    use crate::database::memory_limit_transition_seam::TransitionPhase;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("widening-transition.db");
    let db = tiny_database(&path);
    let start = 8 * 1024 * 1024;
    db.set_memory_limit(Some(start)).expect("start bounded");
    let target = 512 * 1024 * 1024;

    let (outcome, samples) = transition_observation::observe_transition(&db, start, |db| {
        db.set_memory_limit(Some(target))
    });
    outcome.expect("an ordinary widening still succeeds");
    assert!(!samples.is_empty(), "the transition was observed");
    for sample in &samples {
        if sample.phase == TransitionPhase::BeforeDurableWrite {
            assert_eq!(
                sample.published,
                Some(start),
                "the widened limit is published only after its durable write commits: {sample:?}"
            );
            assert!(
                !sample.admitted_beyond_the_smaller_limit,
                "a concurrent operation consumed capacity the durable value had not committed: \
                 {sample:?}"
            );
        }
    }
    assert_eq!(db.accountant().usage().limit, Some(target));
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        reopened.accountant().usage().limit,
        Some(target),
        "the durable value agrees with the published one"
    );
    reopened.close().unwrap();
}

/// Counterexample: narrowing commits the smaller durable value first and only
/// then publishes it, so a concurrent operation keeps allocating under the
/// larger limit after the smaller one is already the durable truth.
#[test]
fn a_narrowing_transition_never_leaves_the_old_capacity_published_after_its_durable_write() {
    use crate::database::memory_limit_transition_seam::TransitionPhase;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("narrowing-transition.db");
    let db = tiny_database(&path);
    let start = 512 * 1024 * 1024;
    db.set_memory_limit(Some(start)).expect("start wide");
    let target = 8 * 1024 * 1024;

    let (outcome, samples) = transition_observation::observe_transition(&db, target, |db| {
        db.set_memory_limit(Some(target))
    });
    outcome.expect("an ordinary narrowing still succeeds");
    assert!(!samples.is_empty(), "the transition was observed");
    for sample in &samples {
        assert_eq!(
            sample.published,
            Some(target),
            "a narrowing publishes the smaller limit before its durable write, not after: \
             {sample:?}"
        );
        assert!(
            !sample.admitted_beyond_the_smaller_limit,
            "a concurrent operation allocated above the limit already committed durably: \
             {sample:?}"
        );
        let _ = TransitionPhase::AfterDurableWrite;
    }
    assert_eq!(db.accountant().usage().limit, Some(target));
    db.close().unwrap();
}

/// A durable write that fails leaves runtime, durable and reopened state
/// agreeing, in both directions, and the limit still movable afterwards.
#[test]
fn an_injected_durable_write_failure_keeps_runtime_durable_and_reopen_coherent() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("failed-transition.db");
    let db = tiny_database(&path);
    let start = 64 * 1024 * 1024;
    db.set_memory_limit(Some(start)).expect("a settled start");

    for target in [Some(512 * 1024 * 1024), Some(4 * 1024 * 1024), None] {
        verified_ordered_run_test_seams::arm_config_write_failure_for_test(true);
        let refused = db.set_memory_limit(target);
        verified_ordered_run_test_seams::arm_config_write_failure_for_test(false);
        assert!(
            refused.is_err(),
            "an injected durable failure refuses the transition: target={target:?}"
        );
        assert_eq!(
            db.accountant().usage().limit,
            Some(start),
            "a refused transition leaves the runtime budget where the durable value still is: \
             target={target:?}"
        );
    }

    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        reopened.accountant().usage().limit,
        Some(start),
        "the durable value never moved"
    );
    reopened
        .set_memory_limit(Some(128 * 1024 * 1024))
        .expect("the limit is still movable after a refused transition");
    reopened.close().unwrap();
}

/// Counterexample: the export destination writes its format marker and its
/// initial content identity before the owning accountant is attached, so that
/// prefix is maintained outside `MEMORY_LIMIT` entirely.
#[test]
fn the_export_destination_admits_its_format_marker_and_initial_identity() {
    let root = tempfile::tempdir().unwrap();
    let budget = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        256 * 1024 * 1024,
    ));
    let before = budget.admitted_bytes_for_test();
    let artifact = RedbPersistence::create_with_accountant(
        &root.path().join("destination.db"),
        Some(Arc::clone(&budget)),
        crate::persistence::redb_cache_bytes_for_limit(budget.usage().limit),
    )
    .expect("create an export destination");
    let admitted = budget.admitted_bytes_for_test().saturating_sub(before);
    artifact.close();

    // The discriminator: attaching the accountant AFTER construction, the way
    // the export used to, leaves that whole prefix unaccounted.
    let control = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        256 * 1024 * 1024,
    ));
    let late =
        RedbPersistence::create(&root.path().join("late.db")).expect("create a second destination");
    late.attach_memory_accountant(Arc::clone(&control));
    let unaccounted = control.admitted_bytes_for_test();
    late.close();
    assert_eq!(
        unaccounted, 0,
        "attaching after construction accounts for none of the destination's first writes"
    );
    assert!(
        admitted > 0,
        "the destination's format marker and initial identity are admitted through the owning \
         accountant, not written outside it: admitted={admitted}"
    );
}

/// Admission success and limit publication must share ONE linearization
/// boundary. This forces the interleaving that proves it: the allocator reads
/// the OLD, wider limit and decides its request fits; the narrower limit is
/// published; only then does the allocator's `used` CAS run. No admission that
/// returns success may have been admitted under the superseded limit, and the
/// accountant must not be left holding more than the published ceiling.
const WIDE_LIMIT: usize = 64 * 1024 * 1024;
const NARROW_LIMIT: usize = 4 * 1024 * 1024;
const RACED_REQUEST: usize = 16 * 1024 * 1024;

#[test]
fn an_admission_that_read_the_old_limit_never_succeeds_under_the_published_narrower_one() {
    let accountant = std::sync::Arc::new(
        crate::memory_accounting::MemoryAccountant::with_runtime_budget(WIDE_LIMIT),
    );
    let publisher = std::sync::Arc::clone(&accountant);
    accountant.set_admission_race_hook(Box::new(move || {
        publisher
            .set_budget(Some(NARROW_LIMIT))
            .expect("the narrower limit publishes while the allocator is mid-admission");
    }));

    let outcome = accountant.try_allocate(RACED_REQUEST);
    let usage = accountant.usage();
    assert!(
        outcome.is_err(),
        "an allocation that read the old {WIDE_LIMIT}-byte limit must not be admitted \
         after the {NARROW_LIMIT}-byte limit is published: used={}",
        usage.used,
    );
    assert_eq!(
        usage.limit,
        Some(NARROW_LIMIT),
        "the published limit is the narrower one, with no headroom leaked into it"
    );
    assert!(
        usage.used <= NARROW_LIMIT,
        "usage is never left above the published ceiling: {} > {NARROW_LIMIT}",
        usage.used,
    );

    // The accountant still works under the narrower ceiling, and the refusal
    // was the ordinary typed one.
    accountant
        .try_allocate(1024)
        .expect("an allocation that fits the published limit is still admitted");
    accountant.release(1024);
    assert!(matches!(
        outcome,
        Err(contextdb_core::Error::MemoryBudgetExceeded { .. })
    ));
}

/// The same boundary, with the publication coming from the production
/// transition (`Database::set_memory_limit`) rather than a bare `set_budget`:
/// its durable write, its private transition headroom and its ordering are all
/// in play, and an admission that read the pre-transition limit still must not
/// succeed once the narrower one is published.
#[test]
fn the_production_limit_transition_shares_its_boundary_with_admission() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("admission-publication-boundary.db");
    let db = std::sync::Arc::new(tiny_database(&path));
    db.set_memory_limit(Some(WIDE_LIMIT))
        .expect("a wide runtime limit");

    let publisher = std::sync::Arc::clone(&db);
    db.accountant().set_admission_race_hook(Box::new(move || {
        publisher
            .set_memory_limit(Some(NARROW_LIMIT))
            .expect("the production transition publishes the narrower limit");
    }));

    let outcome = db.accountant().try_allocate(RACED_REQUEST);
    let usage = db.accountant().usage();
    assert!(
        outcome.is_err(),
        "an admission that read the pre-transition limit must not succeed after \
         the transition published the narrower one: used={}",
        usage.used,
    );
    assert_eq!(usage.limit, Some(NARROW_LIMIT));
    assert!(
        usage.used <= NARROW_LIMIT,
        "usage is never left above the newly published ceiling: {} > {NARROW_LIMIT}",
        usage.used,
    );
    // The transition thread's headroom stayed private: nothing above the
    // published ceiling is visible to anyone afterwards, and the database
    // still accepts ordinary work.
    db.execute(
        "INSERT INTO notes VALUES ($id, 'after the race')",
        &HashMap::from([("id".into(), Value::Int64(9))]),
    )
    .expect("the database still writes under the published limit");
}
