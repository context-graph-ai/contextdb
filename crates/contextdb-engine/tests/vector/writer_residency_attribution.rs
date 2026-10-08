//! A file-backed writer with the consumer's passage shape and a growing graph.
//! Per-row figures are attributed accountant charges, not distinct necessary
//! physical allocations or an irreducible minimum. The observed passage limit
//! applies to this measured workload; it is not a universal capacity.
//! Reconciliation samples are quiescent: a held cursor may retain an old
//! membership image outside the current and registered captured-root inventory.
//! The engine receipt is feature-gated. Its core and relational helpers are
//! compiled, doc-hidden passive methods with no production caller.
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{TxId, Value};
use contextdb_engine::database::{StagedDeleteRaceModeForTest, StagedDeleteRacePointForTest};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use uuid::Uuid;

const LIMIT: usize = 1_610_612_736;
const ROWS: usize = 1_000;
const PASSAGE_DDL: &str = "CREATE TABLE event_search_passages (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), generation_id UUID NOT NULL REFERENCES event_search_generations(id), connector_id TEXT NOT NULL, session_ref TEXT NOT NULL, source_instance_ref TEXT NOT NULL, passage_ordinal INTEGER NOT NULL, first_event_sequence INTEGER NOT NULL, last_event_sequence INTEGER NOT NULL, rendered_text TEXT NOT NULL, content_sha256 TEXT NOT NULL, vector_text VECTOR(768) PARTITION_KEY (context_id, generation_id) MAX_PARTITIONS 4096 SEARCH_MODE AUTO, dimension INTEGER NOT NULL, is_current BOOLEAN NOT NULL, is_searchable BOOLEAN NOT NULL DEFAULT true, created_at TXID NOT NULL, UNIQUE (context_id, id), UNIQUE (context_id, generation_id, id), UNIQUE (context_id, generation_id, connector_id, source_instance_ref, passage_ordinal), FOREIGN KEY (context_id, generation_id) REFERENCES event_search_generations(context_id, id)) HISTORY CURRENT ONLY SYNC OFF";
const INDEX_DDLS: &[&str] = &[
    "CREATE INDEX event_search_passages_active_idx ON event_search_passages (context_id, generation_id, is_current, is_searchable)",
    "CREATE INDEX event_search_passages_generation_ordinal_idx ON event_search_passages (generation_id, passage_ordinal)",
    "CREATE INDEX event_search_passages_connector_idx ON event_search_passages (context_id, generation_id, is_current, connector_id)",
    "CREATE INDEX event_search_passages_session_idx ON event_search_passages (context_id, generation_id, is_current, session_ref)",
    "CREATE INDEX event_search_passages_source_idx ON event_search_passages (context_id, generation_id, is_current, source_instance_ref)",
];

struct ReleaseOnceOutcome {
    first_charge: usize,
    second_charge: usize,
    removed_count: usize,
    accountant_released: usize,
    staged_controls_released: usize,
}

impl ReleaseOnceOutcome {
    fn holds(&self, admitted: usize) -> bool {
        self.first_charge == admitted
            && self.second_charge == 0
            && self.removed_count == 1
            && self.accountant_released == admitted + self.staged_controls_released
    }
}

#[test]
fn passage_writer_residency_has_named_owners_at_one_thousand_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        dir.path().join("writer.db"),
        DatabaseOpenOptions {
            memory_limit: Some(LIMIT),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE contexts (id UUID PRIMARY KEY CONTEXT_ID) SYNC OFF",
        &empty,
    )
    .unwrap();
    db.execute("CREATE TABLE event_search_generations (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), UNIQUE (context_id, id)) SYNC OFF", &empty).unwrap();
    db.execute(PASSAGE_DDL, &empty).unwrap();
    for ddl in INDEX_DDLS {
        db.execute(ddl, &empty).unwrap();
    }
    let context = Uuid::from_u128(1);
    let generation = Uuid::from_u128(2);
    db.execute(
        "INSERT INTO contexts (id) VALUES ($id)",
        &HashMap::from([("id".into(), Value::Uuid(context))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO event_search_generations (id, context_id) VALUES ($id, $context)",
        &HashMap::from([
            ("id".into(), Value::Uuid(generation)),
            ("context".into(), Value::Uuid(context)),
        ]),
    )
    .unwrap();
    let baseline = db.writer_memory_ownership_receipt_for_test();
    eprintln!("E20_PASSAGE rows=0 owners={baseline:?}");
    for first in (0..ROWS).step_by(100) {
        let tx = db.begin().unwrap();
        for ordinal in first..first + 100 {
            let vector = (0..768)
                .map(|index| ((index + ordinal) % 31) as f32 / 31.0)
                .collect();
            db.execute_in_tx(tx, "INSERT INTO event_search_passages (id, context_id, generation_id, connector_id, session_ref, source_instance_ref, passage_ordinal, first_event_sequence, last_event_sequence, rendered_text, content_sha256, vector_text, dimension, is_current, is_searchable, created_at) VALUES ($id, $context_id, $generation_id, $connector_id, $session_ref, $source_instance_ref, $passage_ordinal, $first_event_sequence, $last_event_sequence, $rendered_text, $content_sha256, $vector_text, $dimension, $is_current, $is_searchable, $created_at)", &HashMap::from([
                ("id".into(), Value::Uuid(Uuid::from_u128(100 + ordinal as u128))),
                ("context_id".into(), Value::Uuid(context)),
                ("generation_id".into(), Value::Uuid(generation)),
                ("connector_id".into(), Value::Text("sealed-corpus".into())),
                ("session_ref".into(), Value::Text("one-session".into())),
                ("source_instance_ref".into(), Value::Text("one-source".into())),
                ("passage_ordinal".into(), Value::Int64(ordinal as i64)),
                ("first_event_sequence".into(), Value::Int64(ordinal as i64)),
                ("last_event_sequence".into(), Value::Int64(ordinal as i64)),
                ("rendered_text".into(), Value::Text("P".repeat(3_000))),
                ("content_sha256".into(), Value::Text(format!("{ordinal:064x}"))),
                ("vector_text".into(), Value::Vector(vector)),
                ("dimension".into(), Value::Int64(768)),
                ("is_current".into(), Value::Bool(true)),
                ("is_searchable".into(), Value::Bool(true)),
                ("created_at".into(), Value::TxId(TxId(1))),
            ])).unwrap();
        }
        db.commit(tx).unwrap();
        if (first + 100) % 250 == 0 {
            db.run_maintenance_cycle().unwrap();
        }
        let owners = db.writer_memory_ownership_receipt_for_test();
        eprintln!("E20_PASSAGE rows={} owners={owners:?}", first + 100);
        assert_eq!(
            db.execute("SELECT COUNT(*) FROM event_search_passages", &empty)
                .unwrap()
                .rows,
            vec![vec![Value::Int64((first + 100) as i64)]],
            "the complete committed passage population is read from the durable store"
        );
        assert_eq!(owners.current_row_versions, 0);
        assert_eq!(owners.current_row_values, 0);
        assert_eq!(owners.current_row_index_postings, 0);
        assert_eq!(owners.current_membership_charge, 0);
        assert_eq!(owners.staged_row_versions, 0);
        assert_eq!(owners.staged_row_admissions, 0);
        assert_eq!(owners.reconciliation_gap, 0);
        assert_eq!(
            owners.current_membership_by_table.values().sum::<usize>(),
            owners.current_membership_charge,
        );
    }
    let final_owners = db.writer_memory_ownership_receipt_for_test();
    assert_eq!(final_owners.current_row_versions, 0);
    for table in ["contexts", "event_search_generations"] {
        assert_eq!(
            db.execute(&format!("SELECT COUNT(*) FROM {table}"), &empty)
                .unwrap()
                .rows,
            vec![vec![Value::Int64(1)]],
        );
    }
    db.close().unwrap();
}

#[test]
fn source_membership_commit_reconciles_with_writer_owners() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        dir.path().join("writer.db"),
        DatabaseOpenOptions {
            memory_limit: Some(LIMIT),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    let empty = HashMap::new();
    for ddl in [
        "CREATE TABLE contexts (id UUID PRIMARY KEY CONTEXT_ID) SYNC OFF",
        "CREATE TABLE event_search_generations (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), UNIQUE (context_id, id)) SYNC OFF",
        "CREATE TABLE events (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), UNIQUE (context_id, id)) SYNC OFF",
        "CREATE TABLE event_interpretation_values (context_id UUID CONTEXT_ID NOT NULL, generation_id UUID NOT NULL, event_id UUID NOT NULL, PRIMARY KEY (context_id, generation_id, event_id), UNIQUE (context_id, generation_id, event_id)) SYNC OFF",
        "CREATE TABLE event_search_source_events (membership_id UUID NOT NULL UNIQUE, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), generation_id UUID NOT NULL REFERENCES event_search_generations(id), ordinal INTEGER NOT NULL, event_id UUID NOT NULL REFERENCES events(id), connector_id TEXT NOT NULL, source_instance_ref TEXT NOT NULL, session_ref TEXT NOT NULL, source_sequence INTEGER NOT NULL, effective_interpretation_generation_id UUID, captured_source_sha256 TEXT NOT NULL, effective_source_sha256 TEXT NOT NULL, PRIMARY KEY (context_id, generation_id, ordinal), UNIQUE (context_id, generation_id, event_id), FOREIGN KEY (context_id, generation_id) REFERENCES event_search_generations(context_id, id), FOREIGN KEY (context_id, event_id) REFERENCES events(context_id, id), FOREIGN KEY (context_id, effective_interpretation_generation_id, event_id) REFERENCES event_interpretation_values(context_id, generation_id, event_id)) IMMUTABLE SYNC OFF",
        "CREATE INDEX idx_event_search_source_events_generation ON event_search_source_events (generation_id, context_id, ordinal)",
        "CREATE INDEX idx_event_search_source_events_cursor_range ON event_search_source_events (context_id, generation_id, connector_id, source_instance_ref, source_sequence, ordinal)",
    ] {
        db.execute(ddl, &empty).unwrap();
    }
    let context = Uuid::from_u128(1);
    let generation = Uuid::from_u128(2);
    db.execute(
        "INSERT INTO contexts (id) VALUES ($id)",
        &HashMap::from([("id".into(), Value::Uuid(context))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO event_search_generations (id, context_id) VALUES ($id, $context)",
        &HashMap::from([
            ("id".into(), Value::Uuid(generation)),
            ("context".into(), Value::Uuid(context)),
        ]),
    )
    .unwrap();
    for ordinal in 0..10i64 {
        let event = Uuid::from_u128(3 + ordinal as u128);
        db.execute(
            "INSERT INTO events (id, context_id) VALUES ($id, $context)",
            &HashMap::from([
                ("id".into(), Value::Uuid(event)),
                ("context".into(), Value::Uuid(context)),
            ]),
        )
        .unwrap();
        let before = db.writer_memory_ownership_receipt_for_test();
        db.execute("INSERT INTO event_search_source_events (membership_id, context_id, generation_id, ordinal, event_id, connector_id, source_instance_ref, session_ref, source_sequence, effective_interpretation_generation_id, captured_source_sha256, effective_source_sha256) VALUES ($membership_id, $context_id, $generation_id, $ordinal, $event_id, $connector_id, $source_instance_ref, $session_ref, $source_sequence, NULL, $captured_source_sha256, $effective_source_sha256)", &HashMap::from([
            ("membership_id".into(), Value::Uuid(Uuid::from_u128(100 + ordinal as u128))),
            ("context_id".into(), Value::Uuid(context)),
            ("generation_id".into(), Value::Uuid(generation)),
            ("ordinal".into(), Value::Int64(ordinal)),
            ("event_id".into(), Value::Uuid(event)),
            ("connector_id".into(), Value::Text("claude".into())),
            ("source_instance_ref".into(), Value::Text("source".into())),
            ("session_ref".into(), Value::Text("session".into())),
            ("source_sequence".into(), Value::Int64(ordinal)),
            ("captured_source_sha256".into(), Value::Text("a".repeat(64))),
            ("effective_source_sha256".into(), Value::Text("b".repeat(64))),
        ])).unwrap();
        let after = db.writer_memory_ownership_receipt_for_test();
        eprintln!(
            "E20_SOURCE row={ordinal} gap={} delta={} before={} after={}",
            after.reconciliation_gap,
            after.reconciliation_gap - before.reconciliation_gap,
            before.vector.accountant_total,
            after.vector.accountant_total
        );
        assert_eq!(after.reconciliation_gap, 0, "source row {ordinal}");
    }
    db.close().unwrap();
}

#[test]
fn repeated_sql_updates_release_superseded_staged_row_charges() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        dir.path().join("writer.db"),
        DatabaseOpenOptions {
            memory_limit: Some(LIMIT),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE generation (id UUID PRIMARY KEY, counter INTEGER NOT NULL) SYNC OFF",
        &empty,
    )
    .unwrap();
    let id = Uuid::from_u128(42);
    db.execute(
        "INSERT INTO generation (id, counter) VALUES ($id, $counter)",
        &HashMap::from([
            ("id".into(), Value::Uuid(id)),
            ("counter".into(), Value::Int64(0)),
        ]),
    )
    .unwrap();
    assert_eq!(
        db.writer_memory_ownership_receipt_for_test()
            .reconciliation_gap,
        0
    );
    let tx = db.begin().unwrap();
    for counter in 1..=3 {
        db.execute_in_tx(
            tx,
            "UPDATE generation SET counter = $counter WHERE id = $id",
            &HashMap::from([
                ("id".into(), Value::Uuid(id)),
                ("counter".into(), Value::Int64(counter)),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    let owners = db.writer_memory_ownership_receipt_for_test();
    assert_eq!(owners.current_row_versions, 0);
    assert_eq!(owners.staged_row_versions, 0);
    assert_eq!(owners.staged_row_admissions, 0);
    assert_eq!(owners.staged_index_postings, 0);
    assert_eq!(
        db.execute(
            "SELECT counter FROM generation WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(id)),])
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int64(3)]],
        "the committed durable row retains the last replacement"
    );
    assert_eq!(owners.reconciliation_gap, 0);
    db.close().unwrap();
}

#[test]
fn concurrent_deletes_retire_one_staged_admission() {
    let repaired = run_concurrent_staged_delete(StagedDeleteRaceModeForTest::Repaired);
    let split_lock = run_concurrent_staged_delete(StagedDeleteRaceModeForTest::SplitLock);
    assert_eq!(
        [
            repaired.0.holds(repaired.1),
            split_lock.0.holds(split_lock.1)
        ],
        [true, false],
        "the same release-once predicate must reject the old path",
    );
    assert_eq!(split_lock.0.first_charge, split_lock.1);
    assert_eq!(split_lock.0.second_charge, split_lock.1);
    assert_eq!(split_lock.0.removed_count, 1);
    assert_eq!(
        split_lock.0.accountant_released,
        2 * split_lock.1 + split_lock.0.staged_controls_released,
        "the bad path releases the same admission twice, in addition to its real declaration retirement"
    );
}

fn run_concurrent_staged_delete(mode: StagedDeleteRaceModeForTest) -> (ReleaseOnceOutcome, usize) {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE staged_once (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "INSERT INTO staged_once VALUES (1, $body)",
        &HashMap::from([("body".into(), Value::Text("held".repeat(128)))]),
    )
    .unwrap();
    let row_id = db
        .staged_row_id_for_test(tx, "staged_once")
        .unwrap()
        .unwrap();
    let before = db.writer_memory_ownership_receipt_for_test();
    assert_eq!(before.staged_row_versions, 1);
    assert_eq!(before.reconciliation_gap, 0);
    let admitted = before.staged_row_admissions;
    assert!(admitted > 0);
    let (first_read_tx, first_read_rx) = mpsc::sync_channel(0);
    let (release_first_tx, release_first_rx) = mpsc::sync_channel(0);
    let (second_arrived_tx, second_arrived_rx) = mpsc::sync_channel(0);
    let (second_read_tx, second_read_rx) = mpsc::sync_channel(0);
    let (first_retired_tx, first_retired_rx) = mpsc::sync_channel(1);
    let (second_retired_tx, second_retired_rx) = mpsc::sync_channel(1);
    std::thread::scope(|scope| {
        let first_db = db.clone();
        let first = scope.spawn(move || {
            let fixture_operation_reader_1 = first_db
                .declared_reader_memory()
                .expect("admit the actual fixture reader");

            first_db.delete_relational_row_with_race_hook_for_test(
                tx,
                "staged_once",
                row_id,
                mode,
                move |point| match point {
                    StagedDeleteRacePointForTest::AdmissionReadObserved => {
                        first_read_tx.send(()).unwrap();
                        release_first_rx.recv().unwrap();
                    }
                    StagedDeleteRacePointForTest::RetiredCharge(charge) => {
                        first_retired_tx.send(charge).unwrap();
                    }
                    StagedDeleteRacePointForTest::BeforeRetirement => {}
                },
                fixture_operation_reader_1.clone(),
            )
        });
        // The first caller has read admission through the real write set.
        // The old path drops its lock before this observation; the repaired path keeps it.
        first_read_rx.recv().unwrap();
        let second_db = db.clone();
        let probe_db = db.clone();
        let second = scope.spawn(move || {
            let fixture_operation_reader_2 = second_db
                .declared_reader_memory()
                .expect("admit the actual fixture reader");

            second_db.delete_relational_row_with_race_hook_for_test(
                tx,
                "staged_once",
                row_id,
                mode,
                move |point| match point {
                    StagedDeleteRacePointForTest::BeforeRetirement => {
                        second_arrived_tx
                            .send(probe_db.write_set_lock_is_held_for_test())
                            .unwrap();
                    }
                    StagedDeleteRacePointForTest::RetiredCharge(charge) => {
                        second_retired_tx.send(charge).unwrap();
                    }
                    StagedDeleteRacePointForTest::AdmissionReadObserved => {
                        if mode == StagedDeleteRaceModeForTest::SplitLock {
                            second_read_tx.send(()).unwrap();
                        } else {
                            panic!("the repaired second delete must not read a retired admission");
                        }
                    }
                },
                fixture_operation_reader_2.clone(),
            )
        });
        let real_lock_held = second_arrived_rx.recv().unwrap();
        match mode {
            StagedDeleteRaceModeForTest::Repaired => {
                release_first_tx.send(()).unwrap();
                assert!(real_lock_held);
            }
            StagedDeleteRaceModeForTest::SplitLock => {
                // Force the duplicate admission read before the first caller
                // can remove the row. The read itself used active_txs.
                if !real_lock_held {
                    second_read_rx.recv().unwrap();
                }
                release_first_tx.send(()).unwrap();
                if real_lock_held {
                    second_read_rx.recv().unwrap();
                }
                assert!(!real_lock_held);
            }
        }
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
    });
    let first_charge = first_retired_rx.recv().unwrap();
    let second_charge = second_retired_rx.recv().unwrap();
    let after = db.writer_memory_ownership_receipt_for_test();
    assert_eq!(after.staged_row_versions, 0);
    let outcome = ReleaseOnceOutcome {
        first_charge,
        second_charge,
        removed_count: before.staged_row_versions - after.staged_row_versions,
        accountant_released: before.vector.accountant_total - after.vector.accountant_total,
        staged_controls_released: before
            .staged_index_postings
            .checked_sub(after.staged_index_postings)
            .unwrap(),
    };
    assert_eq!(before.table_metadata, after.table_metadata);
    assert_eq!(
        before.named_owner_total - after.named_owner_total,
        admitted + outcome.staged_controls_released,
        "named owners lose one admission and the actual staged declaration controls"
    );
    if mode == StagedDeleteRaceModeForTest::Repaired {
        assert_eq!(after.reconciliation_gap, 0);
        assert_eq!(db.writer_memory_underflow_count_for_test(), 0);
    } else {
        assert_ne!(after.reconciliation_gap, 0);
    }
    db.rollback(tx).unwrap();
    let rolled_back = db.writer_memory_ownership_receipt_for_test();
    assert!(after.staged_index_postings > 0);
    assert_eq!(
        after
            .vector
            .accountant_total
            .checked_sub(rolled_back.vector.accountant_total),
        Some(after.staged_index_postings),
        "rollback releases the actual remaining staged declaration controls"
    );
    assert_eq!(
        after.named_owner_total - rolled_back.named_owner_total,
        after.staged_index_postings
    );
    assert_eq!(rolled_back.staged_row_versions, 0);
    assert_eq!(rolled_back.staged_row_admissions, 0);
    assert_eq!(rolled_back.staged_index_postings, 0);
    assert_eq!(after.active_transactions, 1);
    assert_eq!(rolled_back.active_transactions, 0);
    assert_eq!(rolled_back.reconciliation_gap, after.reconciliation_gap);
    if mode == StagedDeleteRaceModeForTest::Repaired {
        assert_eq!(db.writer_memory_underflow_count_for_test(), 0);
    }
    (outcome, admitted)
}

#[test]
fn repeated_replacement_rollback_reconciles_every_staged_charge() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE staged_rollback (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let baseline = db.writer_memory_ownership_receipt_for_test();
    let tx = db.begin().unwrap();
    let mut previous = None;
    for size in [64, 256, 1024, 4096] {
        if let Some(row_id) = previous {
            let fixture_operation_reader_3 = db
                .declared_reader_memory()
                .expect("admit the actual fixture reader");

            db.delete_relational_row_releasing_staged_for_test(
                tx,
                "staged_rollback",
                row_id,
                fixture_operation_reader_3.clone(),
            )
            .unwrap();
            let retired = db.writer_memory_ownership_receipt_for_test();
            assert_eq!(retired.staged_row_versions, 0);
            assert_eq!(retired.reconciliation_gap, 0);
        }
        db.execute_in_tx(
            tx,
            "INSERT INTO staged_rollback VALUES (1, $body)",
            &HashMap::from([("body".into(), Value::Text("r".repeat(size)))]),
        )
        .unwrap();
        previous = db.staged_row_id_for_test(tx, "staged_rollback").unwrap();
        assert_eq!(
            db.writer_memory_ownership_receipt_for_test()
                .staged_row_versions,
            1
        );
        assert_eq!(
            db.writer_memory_ownership_receipt_for_test()
                .reconciliation_gap,
            0
        );
    }
    db.rollback(tx).unwrap();
    let after = db.writer_memory_ownership_receipt_for_test();
    assert_eq!(after.current_row_versions, baseline.current_row_versions);
    assert_eq!(
        after.vector.accountant_total,
        baseline.vector.accountant_total
    );
    assert_eq!(after.reconciliation_gap, 0);
    assert_eq!(db.writer_memory_underflow_count_for_test(), 0);
}

#[test]
fn failed_delete_keeps_the_staged_row_and_its_charge() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE staged_failure (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "INSERT INTO staged_failure VALUES (1, $body)",
        &HashMap::from([("body".into(), Value::Text("held".repeat(128)))]),
    )
    .unwrap();
    let row_id = db
        .staged_row_id_for_test(tx, "staged_failure")
        .unwrap()
        .unwrap();
    let before = db.writer_memory_ownership_receipt_for_test();
    let fixture_operation_reader_4 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.delete_relational_row_releasing_staged_for_test(
            tx,
            "missing_table",
            row_id,
            fixture_operation_reader_4.clone()
        )
        .is_err()
    );
    let fixture_operation_reader_5 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.delete_relational_row_releasing_staged_for_test(
            TxId(u64::MAX),
            "staged_failure",
            row_id,
            fixture_operation_reader_5.clone()
        )
        .is_err()
    );
    let after = db.writer_memory_ownership_receipt_for_test();
    assert_eq!(after.staged_row_versions, 1);
    assert_eq!(after.staged_row_admissions, before.staged_row_admissions);
    assert_eq!(
        after.vector.accountant_total,
        before.vector.accountant_total
    );
    assert_eq!(after.reconciliation_gap, 0);
    db.rollback(tx).unwrap();
    assert_eq!(
        db.writer_memory_ownership_receipt_for_test()
            .reconciliation_gap,
        0
    );
    assert_eq!(db.writer_memory_underflow_count_for_test(), 0);
}

#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

#[test]
fn cursor_held_during_update_bounds_attribution_to_quiescent_samples() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE held_members (id INTEGER PRIMARY KEY, active BOOLEAN)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX held_active ON held_members(active)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..16 {
        db.execute(
            "INSERT INTO held_members VALUES ($id, TRUE)",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    assert_eq!(
        db.writer_memory_ownership_receipt_for_test()
            .reconciliation_gap,
        0
    );
    let limits = ReadLimits {
        result_rows: 1024,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 1,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    };
    let req = bounded::BoundedReadRequest::new(
        "SELECT id FROM held_members WHERE active = TRUE",
        HashMap::new(),
        limits,
        Arc::new(FrozenClock),
    );
    let opened = bounded::open_cursor(db.clone(), &req).unwrap();
    assert!(opened.first_page.has_more);
    let held_before = db.writer_memory_ownership_receipt_for_test();
    db.execute(
        "UPDATE held_members SET active = FALSE WHERE id = 12",
        &HashMap::new(),
    )
    .unwrap();
    let held = db.writer_memory_ownership_receipt_for_test();
    assert!(
        held.reconciliation_gap > held_before.reconciliation_gap,
        "held old cursor image must increase the inventory gap: before={held_before:?} after={held:?}"
    );
    drop(opened);
    assert_eq!(
        db.writer_memory_ownership_receipt_for_test()
            .reconciliation_gap,
        0
    );
}
