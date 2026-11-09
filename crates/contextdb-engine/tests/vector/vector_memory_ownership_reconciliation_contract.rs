//! Ownership receipts for retained vector memory.
//!
//! Passive per-owner receipts distinguish retained allocations from transient
//! workspace. The caller's accountant detects underflow, and one-shot failure
//! seams check rollback at real publication boundaries. State transitions use
//! Database, SQL, sync, PURGE, and maintenance; aggregate usage alone would hide
//! a leak in one category behind a release in another.

use contextdb_core::{Error, TenantId, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::plugin::CorePlugin;
use contextdb_engine::{Database, MaintenancePolicy, VectorMemoryOwnershipReceiptForTest};
use contextdb_server::subjects::pull_subject;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;
use uuid::Uuid;

#[path = "../fixtures/isolated_process.rs"]
mod isolated_process;

/// What the process's writers hold in all. A database's own counter dies with
/// the database, so a clause about its final release reads this instead, in a
/// journey that has the process to itself.
fn process_aggregate() -> usize {
    contextdb_engine::memory_accounting::process_aggregate_bytes_for_test()
}

const MAX_MAINTENANCE_CYCLES: usize = 16;
const RECEIVED_TENANT: &str = "vector-memory-ownership-reconciliation";
const RECEIVED_SCHEMA: &str = "CREATE TABLE received_items (
    id UUID PRIMARY KEY,
    scope TEXT NOT NULL,
    embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 8 SEARCH_MODE EXACT
) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn index(table: &str) -> VectorIndexRef {
    VectorIndexRef::new(table, "embedding")
}

fn partition(key: &str) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Text(key.to_owned())])
        .expect("TEXT keys have one canonical partition identity")
}

fn vector(axis: usize) -> Vec<f32> {
    match axis {
        0 => vec![1.0, 0.0, 0.0],
        1 => vec![0.0, 1.0, 0.0],
        _ => vec![0.0, 0.0, 1.0],
    }
}

fn create_table(db: &Database, table: &str) {
    db.execute(
        &format!(
            "CREATE TABLE {table} (\
             id UUID PRIMARY KEY, \
             scope TEXT NOT NULL, \
             embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 8 SEARCH_MODE EXACT\
             )"
        ),
        &empty(),
    )
    .expect("create the compact partitioned-vector fixture");
}

fn create_current_only_table(db: &Database, table: &str) {
    db.execute(
        &format!(
            "CREATE TABLE {table} (\
             id UUID PRIMARY KEY, \
             scope TEXT NOT NULL, \
             embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 8 SEARCH_MODE EXACT\
             ) HISTORY CURRENT ONLY SYNC OFF"
        ),
        &empty(),
    )
    .expect("create the explicitly current-only partitioned-vector fixture");
}

fn insert(db: &Database, table: &str, id: u128, scope: &str, axis: usize) {
    db.execute(
        &format!("INSERT INTO {table} (id, scope, embedding) VALUES ($id, $scope, $embedding)"),
        &params([
            ("id", Value::Uuid(Uuid::from_u128(id))),
            ("scope", Value::Text(scope.to_owned())),
            ("embedding", Value::Vector(vector(axis))),
        ]),
    )
    .expect("commit one deterministic vector row");
}

fn drive_maintenance(db: &Database, _table: &str, _scope: &str) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        let report = db
            .run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns")
            .vector;
        if report.remaining_partitions == 0 {
            return;
        }
    }
    panic!("maintenance left work remaining within the fixed finite cycle count");
}

fn indexed(db: &Database, table: &str, scope: &str, query: Vec<f32>) {
    let _ = indexed_result(db, table, scope, query);
}

fn indexed_result(
    db: &Database,
    table: &str,
    scope: &str,
    query: Vec<f32>,
) -> contextdb_engine::QueryResult {
    db.execute(
        &format!(
            "SELECT id, embedding FROM {table} WHERE scope = $scope \n             ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1"
        ),
        &params([
            ("scope", Value::Text(scope.to_owned())),
            ("query", Value::Vector(query)),
        ]),
    )
    .expect("the production indexed SQL door serves the selected partition")
}

fn assert_exact_vector(
    db: &Database,
    table: &str,
    scope: &str,
    expected_id: Uuid,
    expected_vector: Vec<f32>,
) {
    let result = db
        .execute(
            &format!(
                "SELECT id, embedding FROM {table} WHERE scope = $scope \
                 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1"
            ),
            &params([
                ("scope", Value::Text(scope.to_owned())),
                ("query", Value::Vector(expected_vector.clone())),
            ]),
        )
        .expect("the authoritative exact vector remains readable");
    assert_eq!(
        result.rows().len(),
        1,
        "exact lookup returns one current row"
    );
    let id_column = result
        .columns()
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("exact lookup projects id");
    let vector_column = result
        .columns()
        .iter()
        .position(|column| column == "embedding" || column.rsplit('.').next() == Some("embedding"))
        .expect("exact lookup projects embedding");
    assert_eq!(result.rows()[0][id_column], Value::Uuid(expected_id));
    assert_eq!(
        result.rows()[0][vector_column],
        Value::Vector(expected_vector),
        "received-image ownership transfer cannot change or drop the stored vector"
    );
}

// The passive test receipt samples charged vector owners. Current base/change
// graph fields exclude retired graphs,
// which have their own disjoint field; temporary workspace is likewise not
// part of retained_total. The receipt must never load, evict, reconcile, or
// otherwise mutate.
macro_rules! receipt {
    ($db:expr) => {
        $db.vector_memory_ownership_receipt_for_test()
    };
}

fn logical_non_vector(db: &Database) -> usize {
    receipt!(db)
        .non_vector_total
        .saturating_sub(db.__durable_writer_charged_bytes_for_test())
}

macro_rules! assert_same_receipt {
    ($actual:expr, $expected:expr, $message:literal) => {{
        let actual = &$actual;
        let expected = &$expected;
        assert_eq!(
            actual.raw_vector_bodies, expected.raw_vector_bodies,
            $message
        );
        assert_eq!(
            actual.raw_reverse_directory, expected.raw_reverse_directory,
            $message
        );
        assert_eq!(
            actual.partition_vectors, expected.partition_vectors,
            $message
        );
        assert_eq!(
            actual.vector_partition_controls, expected.vector_partition_controls,
            $message
        );
        assert_eq!(actual.base_graph, expected.base_graph, $message);
        assert_eq!(actual.change_graph, expected.change_graph, $message);
        assert_eq!(actual.mutable_tail, expected.mutable_tail, $message);
        assert_eq!(
            actual.typed_partition_keys, expected.typed_partition_keys,
            $message
        );
        assert_eq!(
            actual.retired_pinned_graph_bytes, expected.retired_pinned_graph_bytes,
            $message
        );
        assert_eq!(
            actual.temporary_workspace, expected.temporary_workspace,
            $message
        );
        assert_eq!(actual.retained_total, expected.retained_total, $message);
        assert_eq!(actual.non_vector_total, expected.non_vector_total, $message);
    }};
}

/// Same as `assert_same_receipt!` minus `non_vector_total`: proves no vector
/// ownership set grew or shrank while deliberately leaving room for a
/// legitimately larger non-vector schema charge (e.g. an added column) to
/// differ between the two receipts.
macro_rules! assert_same_vector_owners {
    ($actual:expr, $expected:expr, $message:literal) => {{
        let actual = &$actual;
        let expected = &$expected;
        assert_eq!(
            actual.raw_vector_bodies, expected.raw_vector_bodies,
            $message
        );
        assert_eq!(
            actual.raw_reverse_directory, expected.raw_reverse_directory,
            $message
        );
        assert_eq!(
            actual.partition_vectors, expected.partition_vectors,
            $message
        );
        assert_eq!(
            actual.vector_partition_controls, expected.vector_partition_controls,
            $message
        );
        assert_eq!(actual.base_graph, expected.base_graph, $message);
        assert_eq!(actual.change_graph, expected.change_graph, $message);
        assert_eq!(actual.mutable_tail, expected.mutable_tail, $message);
        assert_eq!(
            actual.typed_partition_keys, expected.typed_partition_keys,
            $message
        );
        assert_eq!(
            actual.retired_pinned_graph_bytes, expected.retired_pinned_graph_bytes,
            $message
        );
        assert_eq!(
            actual.temporary_workspace, expected.temporary_workspace,
            $message
        );
        assert_eq!(actual.retained_total, expected.retained_total, $message);
    }};
}

macro_rules! assert_exact_reconciliation {
    ($db:expr, $message:literal) => {{
        let receipt = receipt!($db);
        assert_eq!(
            receipt.retained_total,
            receipt.raw_vector_bodies
                + receipt.raw_reverse_directory
                + receipt.partition_vectors
                + receipt.vector_partition_controls
                + receipt.base_graph
                + receipt.change_graph
                + receipt.mutable_tail
                + receipt.typed_partition_keys
                + receipt.retired_pinned_graph_bytes
                + receipt.source_metadata,
            $message
        );
        assert_eq!(receipt.temporary_workspace, 0, $message);
        // The receipt includes the accountant total and the non-vector
        // remainder; compare both with the accountant of the database itself.
        assert_eq!(
            receipt.accountant_total,
            receipt.non_vector_total + receipt.retained_total + receipt.temporary_workspace,
            $message
        );
        assert_eq!(
            $db.accountant().usage().used,
            receipt.accountant_total,
            $message
        );
        // The accountant records over-release in this test-only count.
        assert_eq!($db.accountant().underflow_count_for_test(), 0, $message);
    }};
}

fn open_durable(path: &std::path::Path) -> Database {
    contextdb_engine::database::open_with_startup_limits(path, Arc::new(CorePlugin), None, None)
        .expect("open the durable fixture")
}

/// Restart must retain only dormant descriptors until one partition is used;
/// all subsequent churn and close/reopen cycles have the same retained owners
/// as a fresh database containing only the surviving current row.
#[test]
#[serial]
fn restart_churn_reconciles_each_vector_owner_once_and_final_drop_restores_the_caller_accountant() {
    let root = TempDir::new().expect("temporary durable store");
    let path = root.path().join("restart-churn.redb");
    let control_path = root.path().join("restart-churn-control.redb");
    if isolated_process::run_in_own_process(
        module_path!(),
        "restart_churn_reconciles_each_vector_owner_once_and_final_drop_restores_the_caller_accountant",
    ) {
        return;
    }
    let aggregate_before = process_aggregate();
    // Logical row/schema owners exclude real root and maintenance controls.
    // Full receipts and final drop still reconcile every control charge.
    let comparable_non_vector = |db: &Database| {
        let owners = db.writer_memory_ownership_receipt_for_test();
        assert_eq!(owners.reconciliation_gap, 0);
        owners
            .vector
            .non_vector_total
            .checked_sub(db.__durable_writer_charged_bytes_for_test())
            .unwrap()
            .checked_sub(owners.storage_maintenance_control_charge)
            .unwrap()
            .checked_sub(owners.writer_adapter_controls)
            .unwrap()
    };

    {
        let db = open_durable(&path);
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_table(&db, "ownership_items");
        insert(&db, "ownership_items", 0x101, "alpha", 0);
        insert(&db, "ownership_items", 0x102, "bravo", 1);
        drive_maintenance(&db, "ownership_items", "alpha");
        drive_maintenance(&db, "ownership_items", "bravo");
        db.close().expect("seal the initial durable fixture");
        assert_eq!(db.accountant().underflow_count_for_test(), 0);
    }
    assert_eq!(
        process_aggregate(),
        aggregate_before,
        "dropping the seed handle returns every initial fixture charge"
    );

    let mut stable_reopen_receipt: Option<VectorMemoryOwnershipReceiptForTest> = None;
    let mut fresh_control_receipt: Option<VectorMemoryOwnershipReceiptForTest> = None;
    let mut fresh_control_writer_retention: Option<usize> = None;
    let mut fresh_control_logical_non_vector: Option<usize> = None;
    // Independent of the accountant total: the fresh surviving-state
    // control's own physical row-version population, counted directly
    // through the same test seam used against the reopened store below.
    let mut control_physical_versions: Option<usize> = None;
    // Under HISTORY ALL (the default this table declares -- see
    // `create_table`), a deleted row's tombstoned version is an
    // engine-held copy retained by design, and every engine-held copy is
    // charged. Currency compaction only prunes a lineage's UPDATE-superseded
    // versions -- never a DELETE tombstone -- so the reopened store never
    // reconciles all the way down to the tombstone-free fresh control; it
    // reconciles to the control PLUS this permanently retained tombstone.
    // Measured once, in isolation, through the identical production
    // INSERT/UPDATE/UPDATE/DELETE/compaction doors used by the first iteration's own
    // churn below -- never a literal physical-version count or byte count.
    let mut retained_deleted_row_physical_versions: Option<usize> = None;
    let mut retained_deleted_row_non_vector_bytes: Option<usize> = None;
    for round in 0..3 {
        let db = open_durable(&path);
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let dormant = receipt!(&db);
        assert_eq!(
            dormant.raw_vector_bodies, 0,
            "open keeps raw bodies dormant"
        );
        assert_eq!(dormant.base_graph, 0, "open keeps saved graphs dormant");
        assert_eq!(
            dormant.change_graph, 0,
            "open keeps saved change graphs dormant"
        );
        assert_eq!(
            dormant.mutable_tail, 0,
            "open does not replay a dormant tail eagerly"
        );

        let selected = if round == 0 { "alpha" } else { "bravo" };
        indexed(&db, "ownership_items", selected, vector(round as usize));
        let loaded = receipt!(&db);
        assert_eq!(
            loaded.raw_vector_bodies, 0,
            "indexed F32 reads keep raw bodies dormant"
        );
        assert_eq!(
            loaded.raw_reverse_directory, 0,
            "durable membership keeps neither a resident reverse directory nor dormant body headers"
        );
        assert!(
            loaded.partition_vectors + loaded.vector_partition_controls > 0
                && loaded.typed_partition_keys > 0,
            "the live fixture still owns its partition registry and typed keys"
        );
        assert_exact_reconciliation!(
            &db,
            "lazy load releases decode/replay workspace without over-release"
        );

        if round == 0 {
            let before_embedding_receipt = receipt!(&db);
            let before_writer_retention = db.__durable_writer_charged_bytes_for_test();
            // The store's own public per-vector byte size (never a literal
            // byte count): the production F32 quantization sizing.
            let one_vector_payload_bytes = contextdb_core::VectorQuantization::F32.storage_bytes(3);
            // Independent of the accountant total: the relational store's own
            // physical row-version population (every superseded version plus
            // every keeper), counted directly rather than derived by
            // subtracting the accountant figure from itself.
            let before_physical_versions = db.__physical_version_count_for_test("ownership_items");
            // Isolated control measurement: what one row version of this
            // exact table shape (this row's id width, this scope string,
            // this vector dimension) costs the caller accountant, through
            // the identical production CREATE TABLE / INSERT door -- never a
            // literal byte count and never the engine's private sizing
            // helper directly.
            let _one_row_version_non_vector_bytes = {
                let control_root = TempDir::new().expect("temporary isolated control store");
                let aggregate_before_control = process_aggregate();
                let control = open_durable(&control_root.path().join("one-row-control.redb"));
                control.set_maintenance_policy(MaintenancePolicy::CallerDriven);
                create_table(&control, "ownership_items");
                let before_control = receipt!(&control);
                insert(&control, "ownership_items", 0x101, "alpha", 2);
                let after_control = receipt!(&control);
                let delta = after_control
                    .non_vector_total
                    .checked_sub(before_control.non_vector_total)
                    .expect("one isolated row insertion cannot reduce standing non-vector charge");
                control
                    .close()
                    .expect("close the isolated one-row-version control");
                assert_eq!(control.accountant().underflow_count_for_test(), 0);
                drop(control);
                assert_eq!(
                    process_aggregate(),
                    aggregate_before_control,
                    "isolated control final drop returns every charge"
                );
                delta
            };
            db.execute(
                "UPDATE ownership_items SET embedding = $embedding WHERE id = $id",
                &params([
                    ("id", Value::Uuid(Uuid::from_u128(0x101))),
                    ("embedding", Value::Vector(vector(2))),
                ]),
            )
            .expect("replace alpha's vector atomically");
            let after_embedding_receipt = receipt!(&db);
            eprintln!(
                "CHURN_REPLACEMENT_OWNERS before={before_embedding_receipt:?} after={after_embedding_receipt:?}"
            );
            let after_writer_retention = db.__durable_writer_charged_bytes_for_test();
            let after_physical_versions = db.__physical_version_count_for_test("ownership_items");
            assert_eq!(
                after_embedding_receipt.raw_vector_bodies,
                before_embedding_receipt.raw_vector_bodies,
                "a durable F32 replacement keeps raw cache bodies dormant; both physical \
                 row projections are counted below independently"
            );
            assert_eq!(
                after_embedding_receipt.raw_reverse_directory,
                before_embedding_receipt.raw_reverse_directory,
                "the dormant durable route adds no cached body header or reverse-directory owner"
            );
            assert_eq!(
                after_physical_versions,
                before_physical_versions + 1,
                "the replacement adds one physical row version and keeps the superseded \
                 version resident, never removing it in place"
            );
            assert_eq!(
                after_physical_versions * one_vector_payload_bytes,
                (before_physical_versions * one_vector_payload_bytes) + one_vector_payload_bytes,
                "the superseded row's F32 relational projection stays physically resident, \
                 counted directly from the physical row-version population, independent of \
                 the accountant"
            );
            assert_eq!(
                after_embedding_receipt
                    .non_vector_total
                    .saturating_sub(after_writer_retention),
                before_embedding_receipt
                    .non_vector_total
                    .saturating_sub(before_writer_retention),
                "an equal-shaped replacement leaves the retained non-vector charge \
                 unchanged: the superseded version's bytes are released at this same \
                 commit, exactly offsetting the new version's identical-size charge"
            );
            assert_exact_reconciliation!(&db, "a replacement is charged once before later churn");
            db.execute(
                "UPDATE ownership_items SET scope = $scope WHERE id = $id",
                &params([
                    ("id", Value::Uuid(Uuid::from_u128(0x101))),
                    (
                        "scope",
                        Value::Text("alpha-moved-with-a-longer-key".to_owned()),
                    ),
                ]),
            )
            .expect("move the vector and typed key atomically");
            db.execute(
                "DELETE FROM ownership_items WHERE id = $id",
                &params([("id", Value::Uuid(Uuid::from_u128(0x101)))]),
            )
            .expect("delete the moved vector atomically");
            db.compact_now()
                .expect("compact through the production door");
            indexed(&db, "ownership_items", "bravo", vector(1));
            let bravo_loaded = receipt!(&db);
            let bravo = partition("bravo");
            let raw_evicted = db
                .vector_store_for_test()
                .evict_raw_partition_for_test(&index("ownership_items"), &bravo);
            assert_eq!(
                raw_evicted,
                bravo_loaded.raw_vector_bodies > 0,
                "raw eviction reports exactly whether this F32 route owned a separate raw body"
            );
            let evicted = receipt!(&db);
            assert_eq!(
                evicted.raw_vector_bodies, 0,
                "idle eviction returns the selected raw body"
            );
            assert_exact_reconciliation!(
                &db,
                "churn, compaction, and eviction return temporary owners exactly once"
            );
        }
        db.close().expect("close one deterministic restart round");
        drop(db);

        if round == 0 {
            let aggregate_before_control = process_aggregate();
            let control = open_durable(&control_path);
            control.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            create_table(&control, "ownership_items");
            insert(&control, "ownership_items", 0x102, "bravo", 1);
            drive_maintenance(&control, "ownership_items", "bravo");
            control_physical_versions =
                Some(control.__physical_version_count_for_test("ownership_items"));
            control
                .close()
                .expect("close fresh surviving-state control");
            drop(control);
            let control = open_durable(&control_path);
            fresh_control_receipt = Some(receipt!(&control));
            fresh_control_logical_non_vector = Some(comparable_non_vector(&control));
            eprintln!(
                "FRESH_CONTROL_NON_VECTOR_OWNERS {:?}",
                control.writer_memory_ownership_receipt_for_test()
            );
            fresh_control_writer_retention =
                Some(control.__durable_writer_charged_bytes_for_test());
            assert_exact_reconciliation!(
                &control,
                "the fresh surviving-state control exactly reconciles every owner"
            );
            control
                .close()
                .expect("close fresh control verification reopen");
            assert_eq!(control.accountant().underflow_count_for_test(), 0);
            drop(control);
            assert_eq!(
                process_aggregate(),
                aggregate_before_control,
                "final fresh-control handle drop returns every charge"
            );

            // Isolated deleted-row-lineage control: replicate alpha's exact
            // journey (insert, replace the embedding, move the scope, then
            // delete) alone in its own store, through the identical
            // production doors the first iteration uses below, then run the same
            // currency-compaction test seam on it. This proves, in
            // isolation, that compaction prunes the lineage's two
            // UPDATE-superseded versions (a real prune) while the tombstone
            // itself is retained by design, and measures exactly how many
            // physical versions and how many non-vector bytes that retained
            // tombstone alone costs -- never a literal count.
            let lineage_root =
                TempDir::new().expect("temporary isolated deleted-row lineage control");
            let lineage =
                open_durable(&lineage_root.path().join("deleted-row-lineage-control.redb"));
            lineage.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            create_table(&lineage, "ownership_items");
            // Baseline: the bare table schema's own non-vector charge with
            // zero rows, so the retained-tombstone figure below isolates
            // only the incremental cost of the lineage's history, never the
            // schema's own standing overhead.
            let lineage_baseline_non_vector_total = comparable_non_vector(&lineage);
            insert(&lineage, "ownership_items", 0x101, "alpha", 0);
            lineage
                .execute(
                    "UPDATE ownership_items SET embedding = $embedding WHERE id = $id",
                    &params([
                        ("id", Value::Uuid(Uuid::from_u128(0x101))),
                        ("embedding", Value::Vector(vector(2))),
                    ]),
                )
                .expect("replicate the deleted row's embedding replacement in isolation");
            lineage
                .execute(
                    "UPDATE ownership_items SET scope = $scope WHERE id = $id",
                    &params([
                        ("id", Value::Uuid(Uuid::from_u128(0x101))),
                        (
                            "scope",
                            Value::Text("alpha-moved-with-a-longer-key".to_owned()),
                        ),
                    ]),
                )
                .expect("replicate the deleted row's scope move in isolation");
            lineage
                .execute(
                    "DELETE FROM ownership_items WHERE id = $id",
                    &params([("id", Value::Uuid(Uuid::from_u128(0x101)))]),
                )
                .expect("replicate the deleted row's delete in isolation");
            let lineage_versions_before_compaction =
                lineage.__physical_version_count_for_test("ownership_items");
            for _ in 0..MAX_MAINTENANCE_CYCLES {
                {
                    let seam_read_memory = lineage
                        .declared_reader_memory()
                        .expect("admit the fixture operation reader");
                    lineage.__compact_currency_versions_for_tables_for_test(
                        &["ownership_items"],
                        seam_read_memory,
                    )
                }
                .expect(
                    "prune the isolated lineage's update-superseded versions through the \
                         production door",
                );
            }
            let lineage_versions_after_compaction =
                lineage.__physical_version_count_for_test("ownership_items");
            assert!(
                lineage_versions_after_compaction < lineage_versions_before_compaction,
                "currency compaction prunes the isolated lineage's update-superseded versions, \
                 proving a real prune, even though the lineage ends in a delete"
            );
            let lineage_retained_non_vector_bytes = comparable_non_vector(&lineage)
                .checked_sub(lineage_baseline_non_vector_total)
                .expect(
                    "the retained tombstone's charge cannot be lower than the bare schema's \
                     own standing overhead",
                );
            lineage
                .close()
                .expect("close the isolated deleted-row-lineage control");
            drop(lineage);
            retained_deleted_row_physical_versions = Some(lineage_versions_after_compaction);
            retained_deleted_row_non_vector_bytes = Some(lineage_retained_non_vector_bytes);
        }

        let reopened = open_durable(&path);
        let expected_control = fresh_control_receipt
            .as_ref()
            .expect("the fresh surviving-state control was measured");
        let expected_control_physical_versions = control_physical_versions
            .expect("the fresh surviving-state control's physical version count was measured");
        // The deleted row's tombstone is retained forever under HISTORY
        // ALL (no production door short of dropping the table erases a
        // DELETE tombstone), so it stays physically resident in `path`
        // across every reopen in every round, not just the first iteration.
        let expected_reconciled_physical_versions = expected_control_physical_versions
            + retained_deleted_row_physical_versions.expect(
                "the isolated deleted-row-lineage control's retained version count was measured",
            );
        let expected_reconciled_non_vector_total = fresh_control_logical_non_vector
            .expect("the fresh control's independently attributed logical owners were measured")
            + retained_deleted_row_non_vector_bytes
                .expect("the isolated deleted-row-lineage control's retained non-vector bytes were measured");
        eprintln!(
            "REOPEN_NON_VECTOR_OWNERS actual={:?} control_vector={:?} control_writer_retention={:?} actual_vector={:?}",
            reopened.writer_memory_ownership_receipt_for_test(),
            expected_control,
            fresh_control_writer_retention,
            receipt!(&reopened)
        );
        if round == 0 {
            // Only this iteration's churn (the replaced, moved, and deleted
            // 0x101 row) leaves superseded history physically resident in
            // the reopened HISTORY ALL store; later rounds reopen a store
            // already pruned to the control by this iteration's compaction
            // below, so the residency demonstration below applies once.
            assert!(
                reopened.__physical_version_count_for_test("ownership_items")
                    > expected_control_physical_versions,
                "the reopened HISTORY ALL store still physically holds the superseded history"
            );
            assert_eq!(
                comparable_non_vector(&reopened),
                fresh_control_logical_non_vector.expect(
                    "the fresh control's logical owners exclude its actual maintenance control"
                ),
                "reopen's logical non-vector charge equals the fresh control without \
                 needing a compaction loop -- the retained tombstone's bytes were already \
                 released at the commit that superseded or deleted them, so reopen does \
                 not re-charge history that is still physically resident but logically \
                 released"
            );
        }
        let physical_versions_before_compaction =
            reopened.__physical_version_count_for_test("ownership_items");
        for _ in 0..MAX_MAINTENANCE_CYCLES {
            if reopened.__physical_version_count_for_test("ownership_items")
                == expected_reconciled_physical_versions
            {
                break;
            }
            // `ownership_items` declares no history clause (default HISTORY
            // ALL, see `create_table` above), so the production
            // `compact_currency_versions` entry point never touches it --
            // it scopes itself to declared-HISTORY-CURRENT-ONLY tables
            // (`database.rs:10444`). The test-only twin below runs the
            // identical unmodified pass (`compact_currency_versions_inner`)
            // against a caller-supplied table instead of the declared
            // eligibility set, so this exercises the same real mechanism.
            {
                let seam_read_memory = reopened
                    .declared_reader_memory()
                    .expect("admit the fixture operation reader");
                reopened.__compact_currency_versions_for_tables_for_test(
                    &["ownership_items"],
                    seam_read_memory,
                )
            }
            .expect("prune the retained history through the production door");
        }
        let physical_versions_after_compaction =
            reopened.__physical_version_count_for_test("ownership_items");
        if physical_versions_before_compaction > expected_reconciled_physical_versions {
            // Only the first iteration's reopen still carries alpha's two
            // UPDATE-superseded versions (the round that performed the
            // churn); later rounds reopen a store this iteration's compaction
            // already pruned, so there is nothing left to prune and this
            // proof does not re-apply.
            assert!(
                physical_versions_after_compaction < physical_versions_before_compaction,
                "currency compaction prunes the reopened store's real update-superseded \
                 versions, proving a real prune, on top of the permanently retained tombstone"
            );
        }
        assert_eq!(
            physical_versions_after_compaction, expected_reconciled_physical_versions,
            "compaction brings the reopened store to the control's physical version set plus \
             the deleted row's permanently retained tombstone (HISTORY ALL retains it by \
             design; no production door short of dropping the table erases a DELETE tombstone)"
        );
        let receipt = receipt!(&reopened);
        assert_same_vector_owners!(
            receipt,
            expected_control,
            "once the update-superseded history is physically pruned, the reopened store's \
             vector ownership categories reconcile to the fresh surviving-state control -- \
             the retained tombstone is relational-row history, not vector-index state"
        );
        assert_eq!(
            comparable_non_vector(&reopened),
            expected_reconciled_non_vector_total,
            "the reopened store's non-vector charge reconciles to the control plus the \
             deleted row's permanently retained tombstone bytes; every engine-held copy is \
             charged, and a HISTORY ALL tombstone stays an engine-held copy forever"
        );
        if let Some(previous) = stable_reopen_receipt.as_ref() {
            assert_same_receipt!(
                receipt,
                previous,
                "second and third reopen retain the exact same surviving ownership receipt"
            );
        }
        stable_reopen_receipt = Some(receipt);
        assert_exact_reconciliation!(
            &reopened,
            "each reopen exactly reconciles its vector categories and caller accountant"
        );
        reopened.close().expect("close verification reopen");
        drop(reopened);
    }

    {
        let db = open_durable(&path);
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_current_only_table(&db, "current_only_items");
        insert(&db, "current_only_items", 0x301, "gamma", 0);
        drive_maintenance(&db, "current_only_items", "gamma");

        // Hold an old read across the coming replacements: a caller-held
        // SnapshotPin still sees the version live when it was taken, so
        // currency compaction must defer pruning it rather than physically
        // removing a version out from under a registered reader.
        let held_snapshot = db.snapshot();
        let held_pin = db.pin_snapshot(held_snapshot).unwrap();
        let one_vector_payload_bytes = contextdb_core::VectorQuantization::F32.storage_bytes(3);
        let before_pinned_versions = db.__physical_version_count_for_test("current_only_items");

        db.execute(
            "UPDATE current_only_items SET embedding = $embedding WHERE id = $id",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0x301))),
                ("embedding", Value::Vector(vector(1))),
            ]),
        )
        .expect("replace the current-only-history vector a first time");
        db.execute(
            "UPDATE current_only_items SET embedding = $embedding WHERE id = $id",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0x301))),
                ("embedding", Value::Vector(vector(2))),
            ]),
        )
        .expect("replace the current-only-history vector a second time");

        let pinned_versions = db.__physical_version_count_for_test("current_only_items");
        assert_eq!(
            pinned_versions,
            before_pinned_versions + 2,
            "two replacements leave the live row plus two retained historical copies \
             physically resident, distinct from a single-version table"
        );
        let pinned_logical_non_vector = comparable_non_vector(&db);

        // Exact per-snapshot visibility (docs/architecture.md:337): the held
        // pin's registered snapshot was taken before either UPDATE, so it can
        // only ever see the ORIGINAL version (axis 0). Prove that read still
        // resolves correctly before compacting anything.
        let pinned_snapshot_read = db
            .execute_at_snapshot(
                "SELECT id, embedding FROM current_only_items WHERE id = $id",
                &params([("id", Value::Uuid(Uuid::from_u128(0x301)))]),
                held_snapshot,
            )
            .expect("the pin's own registered snapshot remains readable while it is held");
        assert_eq!(
            pinned_snapshot_read.rows().len(),
            1,
            "the pinned snapshot still resolves exactly one row for the held id"
        );
        let pinned_embedding_column = pinned_snapshot_read
            .columns()
            .iter()
            .position(|column| {
                column == "embedding" || column.rsplit('.').next() == Some("embedding")
            })
            .expect("the pinned-snapshot read projects embedding");
        assert_eq!(
            pinned_snapshot_read.rows()[0][pinned_embedding_column],
            Value::Vector(vector(0)),
            "the pin's registered snapshot still resolves to the value that was live when the \
             pin was taken -- the original, never-replaced version"
        );

        // Exact per-snapshot visibility means only the version created and
        // superseded BOTH after the pin's snapshot is prunable while the pin
        // is held: the first UPDATE's own version (created after the pin,
        // superseded by the second UPDATE) was never visible to the pin.
        // The original version (live when the pin was taken) is the one the
        // pin protects, so it is deferred; only that one intermediate
        // version is pruned now.
        // Cleanup runs in finite batches and may reach the intermediate
        // version on a later pass; drive passes until one prunes nothing,
        // all while the pin is held.
        let mut deferred_pruned = 0;
        for _ in 0..16 {
            let pass = db
                .compact_currency_versions()
                .expect("a deferred currency pass still returns cleanly");
            if pass.pruned_versions == 0 && deferred_pruned > 0 {
                break;
            }
            deferred_pruned += pass.pruned_versions;
        }
        assert_eq!(
            deferred_pruned, 1,
            "the documented exact-per-snapshot rule (docs/architecture.md:337) defers only the \
             version still visible to the held pin -- the original, pin-visible version -- and \
             prunes the intermediate version created and superseded entirely after the pin's \
             snapshot, even while the pin is held"
        );
        let versions_after_deferred_compaction =
            db.__physical_version_count_for_test("current_only_items");
        assert_eq!(
            pinned_versions - versions_after_deferred_compaction,
            1,
            "the deferred pass physically removes exactly the one invisible intermediate \
             version, leaving the pin-visible version and the live row resident"
        );
        let deferred_logical_non_vector = comparable_non_vector(&db);
        assert_eq!(
            deferred_logical_non_vector, pinned_logical_non_vector,
            "pruning the pin-invisible intermediate version while the pin is held leaves the \
             retained non-vector charge unchanged: that version's bytes were already released \
             at the commit that superseded it, so physically removing it is a logical no-op \
             and never credits the same bytes a second time"
        );
        assert_exact_reconciliation!(
            &db,
            "the partially deferred currency pass reconciles every vector category and the \
             caller accountant, exactly once"
        );

        // Release the pin, then close and reopen: the one retained
        // historical copy this test's pin still protects is still
        // physically resident purely because currency compaction deferred
        // it -- not because of the (now-released) pin.
        drop(held_pin);
        db.close()
            .expect("close with one retained, deferred historical copy");
        drop(db);

        let db = open_durable(&path);
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let reopened_versions = db.__physical_version_count_for_test("current_only_items");
        assert_eq!(
            reopened_versions, versions_after_deferred_compaction,
            "every physical row version, including the one still-deferred (tombstoned) \
             historical copy, survives a reopen intact -- reopen alone never prunes"
        );
        assert_eq!(
            reopened_versions * one_vector_payload_bytes,
            versions_after_deferred_compaction * one_vector_payload_bytes,
            "the resident row-held F32 projection bytes of the retained historical copy \
             persist across a reopen, counted independently of the accountant"
        );
        let reopened_logical_non_vector = comparable_non_vector(&db);
        assert_eq!(
            reopened_logical_non_vector, deferred_logical_non_vector,
            "reopen charges the identical retained non-vector total for the one \
             still-deferred historical copy, not merely the live row"
        );

        let before_release_logical_non_vector = comparable_non_vector(&db);
        let release_report = db
            .compact_currency_versions()
            .expect("prune the released historical copy through the production door");
        assert_eq!(
            release_report.pruned_versions, 1,
            "with the pin released, currency compaction prunes exactly the one historical \
             copy it deferred earlier (the original, pin-visible version), not the live row"
        );
        let versions_after_release_compaction =
            db.__physical_version_count_for_test("current_only_items");
        assert_eq!(
            reopened_versions - versions_after_release_compaction,
            1,
            "compaction physically removes exactly the one pruned historical copy, once"
        );
        let after_release_logical_non_vector = comparable_non_vector(&db);
        assert_eq!(
            before_release_logical_non_vector, after_release_logical_non_vector,
            "releasing the pin and compacting leaves the retained non-vector charge unchanged: \
             the one now-pruned historical copy's bytes were already released at the commit \
             that superseded it, so physically removing it is a logical no-op and never \
             credits the same bytes a second time"
        );
        assert_exact_reconciliation!(
            &db,
            "releasing the pin and pruning the deferred historical copies reconciles every \
             vector category and the caller accountant, exactly once"
        );

        db.close()
            .expect("close after releasing the pin and pruning deferred history");
        drop(db);
        let db = open_durable(&path);
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_eq!(
            db.__physical_version_count_for_test("current_only_items"),
            versions_after_release_compaction,
            "reopening again after the release-triggered compaction retains the same, \
             already-pruned physical version population"
        );
        assert_eq!(
            comparable_non_vector(&db),
            after_release_logical_non_vector,
            "reopening again after the release-triggered compaction retains the exact same \
             retained non-vector figure"
        );

        db.execute(
            "UPDATE current_only_items SET scope = $scope WHERE id = $id",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0x301))),
                (
                    "scope",
                    Value::Text("gamma-moved-with-a-longer-key".to_owned()),
                ),
            ]),
        )
        .expect("move the current-only-history vector and typed key atomically");
        db.execute(
            "DELETE FROM current_only_items WHERE id = $id",
            &params([("id", Value::Uuid(Uuid::from_u128(0x301)))]),
        )
        .expect("delete the moved current-only-history vector atomically");
        let before_currency_round = receipt!(&db);
        // Version cleanup runs in finite batches: drive passes until one prunes
        // nothing, keeping what every pass pruned and compacted.
        let mut currency_report = db.compact_currency_versions().expect(
            "prune the superseded current-only-history versions through the production door",
        );
        for _ in 0..16 {
            let pass = db
                .compact_currency_versions()
                .expect("a further finite cleanup pass returns cleanly");
            if pass.pruned_versions == 0 {
                break;
            }
            currency_report.pruned_versions += pass.pruned_versions;
            currency_report
                .compacted_tables
                .extend(pass.compacted_tables);
        }
        assert!(
            currency_report.pruned_versions > 0,
            "currency compaction prunes the superseded current-only-history version the \
             move-then-delete left behind, not zero"
        );
        assert!(
            currency_report
                .compacted_tables
                .iter()
                .any(|table| table == "current_only_items"),
            "currency compaction reports the current-only-history table it actually pruned"
        );
        assert_exact_reconciliation!(
            &db,
            "pruning a moved-then-deleted current-only-history row reconciles every vector \
             category and the caller accountant"
        );
        let after_currency_round = receipt!(&db);
        assert_eq!(
            after_currency_round.raw_vector_bodies, 0,
            "currency compaction returns the pruned current-only-history route's raw body"
        );
        // The reverse directory is paged, not resident; its durable entry's
        // removal is proven by the exact reconciliation above and the rewritten
        // vector key. Cleanup never grows the resident reverse directory.
        assert!(
            after_currency_round.raw_reverse_directory
                <= before_currency_round.raw_reverse_directory,
            "currency compaction never grows the resident reverse directory"
        );
        assert!(
            after_currency_round.base_graph == 0
                && after_currency_round.change_graph == 0
                && after_currency_round.mutable_tail == 0,
            "the pruned current-only-history route retains no maintained-graph owner"
        );
        db.close()
            .expect("close the current-only-history compaction round");
        assert_eq!(db.accountant().underflow_count_for_test(), 0);
        drop(db);
    }
    assert_eq!(
        process_aggregate(),
        aggregate_before,
        "dropping every final Database handle restores the process to its pre-open use"
    );
}

/// A complete received image transfers vector ownership rather than layering a
/// second raw body, directory, graph, or typed key copy over the first image.
#[tokio::test]
#[serial]
#[ignore = "Dropping the last received-image handle leaves the process memory aggregate far above its starting value."]
async fn received_image_replacement_is_idempotent_and_its_prepublication_failure_preserves_receipt()
{
    let root = TempDir::new().expect("temporary durable receiver");
    let edge_path = root.path().join("received-ownership.redb");
    if isolated_process::run_in_own_process(
        module_path!(),
        "received_image_replacement_is_idempotent_and_its_prepublication_failure_preserves_receipt",
    ) {
        return;
    }
    let aggregate_before = process_aggregate();
    let source = Arc::new(open_durable(&root.path().join("received-source.redb")));
    let edge = Arc::new(open_durable(&edge_path));
    source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    edge.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    source
        .execute(RECEIVED_SCHEMA, &empty())
        .expect("declare the authenticated source schema");
    insert(&source, "received_items", 0x201, "alpha", 0);

    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            source.clone(),
            broker.server_as(&hub_node_id),
            TenantId::from(RECEIVED_TENANT),
            hub_node_id,
            hub_identity,
        )
        .expect("construct the authenticated sync endpoint"),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&pull_subject(RECEIVED_TENANT))
        .await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_node_id),
        TenantId::from(RECEIVED_TENANT),
        edge_identity,
    )
    .expect("construct the authenticated sync endpoint");

    client
        .pull_default()
        .await
        .expect("authenticated pull installs the initial complete image");
    drive_maintenance(&edge, "received_items", "alpha");
    indexed(&edge, "received_items", "alpha", vector(0));
    let initial = receipt!(&edge);
    assert_exact_reconciliation!(
        &edge,
        "the initial received image exactly reconciles every owner"
    );

    client
        .pull_default()
        .await
        .expect("a no-change authenticated pull is accepted");
    assert_same_receipt!(
        receipt!(&edge),
        initial,
        "a no-change pull leaves every vector owner unchanged"
    );

    // Following the received-generation contract, the schema delta makes the
    // next authenticated pull enter received-schema image replacement rather
    // than incremental apply_changes.
    source
        .execute(
            "ALTER TABLE received_items ADD COLUMN source_note TEXT",
            &empty(),
        )
        .expect("make the next pull a received-schema replacement");
    source
        .execute(
            "UPDATE received_items SET embedding = $embedding WHERE id = $id",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0x201))),
                ("embedding", Value::Vector(vector(1))),
            ]),
        )
        .expect("change the received source image deterministically");
    // Fail the next received-schema publication and check its one-shot
    // consumed receipt before retrying the same public operation.
    edge.arm_received_vector_image_prepublication_failure_for_test();
    assert!(
        client.pull_default().await.is_err(),
        "the injected received-image prepublication failure refuses replacement"
    );
    assert!(
        edge.received_vector_image_prepublication_failure_consumed_for_test(),
        "the received-image prepublication fault is consumed exactly once"
    );
    assert_same_receipt!(
        receipt!(&edge),
        initial,
        "a failed received image preserves the prior owners exactly"
    );
    assert_exact_reconciliation!(
        &edge,
        "a failed received image returns every temporary owner"
    );

    client
        .pull_default()
        .await
        .expect("retry the changed image through authenticated received-schema pull");
    assert_exact_vector(
        &edge,
        "received_items",
        "alpha",
        Uuid::from_u128(0x201),
        vector(1),
    );
    let changed = receipt!(&edge);
    let changed_writer_retention = edge.__durable_writer_charged_bytes_for_test();
    assert!(
        changed.raw_vector_bodies <= initial.raw_vector_bodies,
        "same-size replacement cannot retain old plus new raw vector bodies: initial={} changed={}",
        initial.raw_vector_bodies,
        changed.raw_vector_bodies
    );
    assert!(
        changed.raw_reverse_directory <= initial.raw_reverse_directory,
        "same-shape replacement cannot retain two reverse directories"
    );
    assert!(
        changed.partition_vectors + changed.vector_partition_controls
            <= initial.partition_vectors + initial.vector_partition_controls,
        "same-shape replacement cannot retain two partition-vector registries"
    );
    assert!(
        changed.typed_partition_keys <= initial.typed_partition_keys,
        "same-key replacement cannot retain two typed partition-key owners"
    );
    assert_eq!(
        changed.base_graph + changed.change_graph + changed.mutable_tail,
        0,
        "received-image replacement releases the stale maintained route before finite maintenance"
    );
    assert!(matches!(
        edge.execute(
            "SELECT id FROM received_items WHERE scope = 'alpha' \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &params([("query", Value::Vector(vector(1)))]),
        ),
        Err(Error::VectorIndexedRouteUnavailable { .. })
    ));
    let meta_before_repeat = edge
        .table_meta("received_items")
        .expect("the received table is declared")
        .estimated_bytes();
    source
        .execute(
            "ALTER TABLE received_items ADD COLUMN repeated_image_note TEXT",
            &empty(),
        )
        .expect("force a second complete image with the same vector contents");
    client
        .pull_default()
        .await
        .expect("repeat the changed vector image through a second authenticated schema image");
    assert_exact_vector(
        &edge,
        "received_items",
        "alpha",
        Uuid::from_u128(0x201),
        vector(1),
    );
    let repeated = receipt!(&edge);
    let repeated_writer_retention = edge.__durable_writer_charged_bytes_for_test();
    assert_same_vector_owners!(
        repeated,
        changed,
        "repeating a changed image cannot grow a second ownership set"
    );
    assert_eq!(
        repeated
            .non_vector_total
            .saturating_sub(repeated_writer_retention),
        changed
            .non_vector_total
            .saturating_sub(changed_writer_retention)
            + (edge
                .table_meta("received_items")
                .expect("the received table is declared")
                .estimated_bytes()
                - meta_before_repeat),
        "a repeated image adds only the declared column's schema metadata, never a second row, \
         vector, or directory ownership set"
    );
    assert_exact_reconciliation!(
        &edge,
        "the final received image exactly reconciles every owner"
    );

    drop(client);
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("server task stops");
    drop(server);
    edge.close().expect("close the received-image receiver");
    assert_eq!(edge.accountant().underflow_count_for_test(), 0);
    drop(edge);
    // The source store is still open, so the process holds what it holds.
    assert_eq!(
        process_aggregate(),
        aggregate_before + source.accountant().usage().used,
        "dropping the final received-image handle returns every charge it took"
    );
}

/// A long canonical TEXT key is charged before a source snapshot can make the
/// old partition state survive; reclamation removes each old owner exactly once.
#[test]
#[serial]
#[ignore = "A refused one-byte-short key update leaves its reservation charged instead of releasing every owner."]
fn long_typed_key_move_and_snapshot_retirement_admit_before_mutation_and_release_once() {
    let root = TempDir::new().expect("temporary store");
    let path = root.path().join("long-key.redb");
    let control_path = root.path().join("long-key-control.redb");
    if isolated_process::run_in_own_process(
        module_path!(),
        "long_typed_key_move_and_snapshot_retirement_admit_before_mutation_and_release_once",
    ) {
        return;
    }
    let aggregate_before = process_aggregate();
    let long_key = "destination-key-".repeat(32);

    let admission = {
        let calibration = open_durable(&control_path);
        calibration.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_current_only_table(&calibration, "key_items");
        insert(&calibration, "key_items", 0x301, "source", 0);
        drive_maintenance(&calibration, "key_items", "source");
        let source_snapshot = calibration.snapshot();
        let source_pin = calibration.pin_snapshot(source_snapshot).unwrap();
        let before = receipt!(&calibration);
        let before_used = calibration.accountant().usage().used;
        calibration
            .execute(
                "UPDATE key_items SET scope = $scope WHERE id = $id",
                &params([
                    ("id", Value::Uuid(Uuid::from_u128(0x301))),
                    ("scope", Value::Text(long_key.clone())),
                ]),
            )
            .expect("measure one exact long-key admission");
        let after = receipt!(&calibration);
        let after_used = calibration.accountant().usage().used;
        let delta = after_used
            .checked_sub(before_used)
            .expect("long-key admission does not reduce the standing database charge");
        assert!(
            delta > 0,
            "the identical long-key lifecycle has positive total admission"
        );
        assert!(
            after.typed_partition_keys > before.typed_partition_keys,
            "the receipt separately identifies a positive typed-key charge"
        );
        assert_exact_reconciliation!(
            &calibration,
            "the calibrated long-key lifecycle exactly reconciles every owner"
        );
        drop(source_pin);
        calibration
            .close()
            .expect("close the admission calibration");
        assert_eq!(calibration.accountant().underflow_count_for_test(), 0);
        drop(calibration);
        assert_eq!(
            process_aggregate(),
            aggregate_before,
            "calibration final drop returns every charge"
        );
        delta
    };
    let db = open_durable(&path);
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_current_only_table(&db, "key_items");
    insert(&db, "key_items", 0x301, "source", 0);
    drive_maintenance(&db, "key_items", "source");
    let old_snapshot = db.snapshot();
    let pin = db.pin_snapshot(old_snapshot).unwrap();
    let before_move = receipt!(&db);
    let before_move_used = db.accountant().usage().used;
    db.set_memory_limit(Some(before_move_used + admission - 1))
        .expect("install a one-byte-short standing limit");
    let refusal = db
        .execute(
            "UPDATE key_items SET scope = $scope WHERE id = $id",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0x301))),
                ("scope", Value::Text(long_key.clone())),
            ]),
        )
        .expect_err("one byte below the measured long-key admission refuses atomically");
    assert!(
        matches!(refusal, Error::MemoryBudgetExceeded { .. }),
        "long-key admission uses the typed memory-budget refusal: {refusal:?}"
    );
    assert_same_receipt!(
        receipt!(&db),
        before_move,
        "the one-byte-short refusal preserves the pinned source and every owner"
    );
    db.set_memory_limit(None)
        .expect("restore admission for the successful measured move");
    db.execute(
        "UPDATE key_items SET scope = $scope WHERE id = $id",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x301))),
            ("scope", Value::Text(long_key.clone())),
        ]),
    )
    .expect("admit the long typed destination key before moving the row");
    let pinned = receipt!(&db);
    let actual_admission = db.accountant().usage().used - before_move_used;
    assert_eq!(
        actual_admission, admission,
        "the identical calibration and actual lifecycle have the same total admission"
    );
    assert!(
        pinned.typed_partition_keys > before_move.typed_partition_keys,
        "the long typed key has a separately positive charge; other owner growth is not excluded"
    );
    assert!(
        pinned.retired_pinned_graph_bytes > 0,
        "the snapshot keeps a positive retired graph owner before reclamation"
    );
    drop(pin);
    let mut after_pin_release = receipt!(&db);
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if after_pin_release.retired_pinned_graph_bytes == 0 {
            break;
        }
        db.run_maintenance_cycle()
            .expect("one finite snapshot-reclamation cycle returns");
        after_pin_release = receipt!(&db);
    }
    assert_eq!(
        after_pin_release.retired_pinned_graph_bytes, 0,
        "finite maintenance reclaims the positive retired graph after the pin releases"
    );
    assert!(
        pinned
            .retired_pinned_graph_bytes
            .checked_sub(after_pin_release.retired_pinned_graph_bytes)
            .expect("snapshot reclamation cannot grow retired ownership")
            > 0,
        "snapshot release returns the positive retired generation ownership"
    );
    assert!(
        pinned
            .retained_total
            .checked_sub(after_pin_release.retained_total)
            .expect("declared current-only cleanup cannot grow retained ownership")
            > 0,
        "declared current-only maintenance releases positive historical body, key, map, and retired-graph ownership"
    );
    assert_exact_reconciliation!(
        &db,
        "snapshot retirement exactly reconciles every owner without underflow"
    );
    db.close().expect("close long-key lifecycle");
    assert_eq!(db.accountant().underflow_count_for_test(), 0);
    drop(db);
    assert_eq!(
        process_aggregate(),
        aggregate_before,
        "final long-key handle drop returns every charge"
    );
}

/// Durable purge and compaction failures are separate one-shot journeys.  A
/// successful retry must reach the same exact ownership receipt as a fresh
/// database containing only the surviving partition.
#[test]
#[serial]
fn durable_purge_fault_journey_reconciles_to_a_fresh_control() {
    let root = TempDir::new().expect("temporary durable store");
    let path = root.path().join("purge-compaction-ownership.redb");
    let control_path = root.path().join("purge-compaction-control.redb");
    if isolated_process::run_in_own_process(
        module_path!(),
        "durable_purge_fault_journey_reconciles_to_a_fresh_control",
    ) {
        return;
    }
    let aggregate_before = process_aggregate();
    let db = open_durable(&path);
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_table(&db, "purge_items");
    insert(&db, "purge_items", 0x501, "alpha", 0);
    insert(&db, "purge_items", 0x502, "bravo", 1);
    drive_maintenance(&db, "purge_items", "alpha");
    drive_maintenance(&db, "purge_items", "bravo");
    let before = receipt!(&db);
    // Fail the next PURGE after workspace admission and before publication,
    // then check its one-shot consumed receipt.
    db.arm_vector_durable_purge_failure_for_test();
    assert!(
        db.execute(
            "PURGE FROM purge_items WHERE scope = $scope",
            &params([("scope", Value::Text("alpha".to_owned()))]),
        )
        .is_err(),
        "the purge-only durable fault is visible"
    );
    assert!(
        db.vector_durable_purge_failure_consumed_for_test(),
        "the purge-only durable fault is consumed exactly once"
    );
    assert_same_receipt!(
        receipt!(&db),
        before,
        "failed durable purge returns workspace and preserves every owner"
    );
    assert_exact_reconciliation!(
        &db,
        "failed durable purge exactly reconciles every owner without underflow"
    );

    db.execute(
        "PURGE FROM purge_items WHERE scope = $scope",
        &params([("scope", Value::Text("alpha".to_owned()))]),
    )
    .expect("retry purge through the public door");
    db.run_maintenance_cycle()
        .expect("reconcile eligible retired ownership after successful purge");
    assert_exact_reconciliation!(
        &db,
        "successful purge eventually exactly reconciles every eligible owner"
    );

    db.execute(
        "UPDATE purge_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x502))),
            ("embedding", Value::Vector(vector(2))),
        ]),
    )
    .expect("create committed bravo history");
    db.execute(
        "UPDATE purge_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x502))),
            ("embedding", Value::Vector(vector(0))),
        ]),
    )
    .expect("create later history");

    assert_exact_reconciliation!(
        &db,
        "the purge journey and later history exactly reconcile every owner"
    );
    db.close().expect("close durable purge/compaction journey");
    assert_eq!(db.accountant().underflow_count_for_test(), 0);
    drop(db);
    assert_eq!(
        process_aggregate(),
        aggregate_before,
        "closing the repaired purge/compaction database returns every owned charge before reopen"
    );

    let control = open_durable(&control_path);
    control.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_table(&control, "purge_items");
    insert(&control, "purge_items", 0x502, "bravo", 1);
    drive_maintenance(&control, "purge_items", "bravo");
    // Independent of the accountant total: the fresh surviving-state
    // control's own physical row-version population for bravo, counted
    // directly through the same test seam used against the reopened store.
    let control_physical_versions = control.__physical_version_count_for_test("purge_items");
    control.close().expect("seal fresh surviving-state control");
    drop(control);
    let control = open_durable(&control_path);
    let control_receipt = receipt!(&control);
    let control_writer_retention = control.__durable_writer_charged_bytes_for_test();
    let control_owners = control.writer_memory_ownership_receipt_for_test();
    let control_maintenance = control_owners.storage_maintenance_control_charge;
    assert_eq!(control_owners.reconciliation_gap, 0);
    assert_exact_reconciliation!(
        &control,
        "fresh surviving-state control exactly reconciles every owner"
    );
    control
        .close()
        .expect("close fresh surviving-state control reopen");
    assert_eq!(control.accountant().underflow_count_for_test(), 0);
    drop(control);
    assert_eq!(
        process_aggregate(),
        aggregate_before,
        "final purge/compaction control handle drop returns every charge"
    );

    let reopened = open_durable(&path);
    assert!(
        reopened.__physical_version_count_for_test("purge_items") > control_physical_versions,
        "the reopened HISTORY ALL store still physically holds bravo's superseded history"
    );
    let reopened_owners = reopened.writer_memory_ownership_receipt_for_test();
    assert_eq!(reopened_owners.reconciliation_gap, 0);
    eprintln!("purge restart ownership: control={control_owners:?}; reopened={reopened_owners:?}");
    assert_eq!(
        logical_non_vector(&reopened)
            .checked_sub(reopened_owners.storage_maintenance_control_charge)
            .unwrap(),
        control_receipt
            .non_vector_total
            .checked_sub(control_writer_retention)
            .unwrap()
            .checked_sub(control_maintenance)
            .unwrap(),
        "reopen's logical non-vector charge equals the fresh control without needing a \
         compaction loop -- the retained history's bytes were already released at the \
         commit that superseded or deleted them, so reopen does not re-charge history \
         that is still physically resident but logically released"
    );
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if reopened.__physical_version_count_for_test("purge_items") == control_physical_versions {
            break;
        }
        // `purge_items` declares no history clause (default HISTORY ALL, see
        // `create_table` above), so the production `compact_currency_versions`
        // entry point never touches it -- it scopes itself to declared-
        // HISTORY-CURRENT-ONLY tables (`database.rs:10444`). The test-only
        // twin below runs the identical unmodified pass
        // (`compact_currency_versions_inner`) against a caller-supplied
        // table instead of the declared eligibility set, so this exercises
        // the same real mechanism.
        {
            let seam_read_memory = reopened
                .declared_reader_memory()
                .expect("admit the fixture operation reader");
            reopened
                .__compact_currency_versions_for_tables_for_test(&["purge_items"], seam_read_memory)
        }
        .expect("prune the retained history through the production door");
    }
    assert_eq!(
        reopened.__physical_version_count_for_test("purge_items"),
        control_physical_versions,
        "compaction brings the reopened store to the control's physical version set"
    );
    assert_same_vector_owners!(
        receipt!(&reopened),
        control_receipt,
        "once the retained history is physically pruned, vector ownership matches the control"
    );
    assert_eq!(
        logical_non_vector(&reopened)
            .checked_sub(
                reopened
                    .writer_memory_ownership_receipt_for_test()
                    .storage_maintenance_control_charge
            )
            .unwrap(),
        control_receipt
            .non_vector_total
            .checked_sub(control_writer_retention)
            .unwrap()
            .checked_sub(control_maintenance)
            .unwrap(),
        "after compaction, logical row/schema ownership matches the fresh control while real finite maintenance controls remain independently charged"
    );
    assert_exact_reconciliation!(
        &reopened,
        "durable purge/compaction final reopen exactly reconciles every owner"
    );
    reopened.close().expect("close durable verification reopen");
    assert_eq!(reopened.accountant().underflow_count_for_test(), 0);
    drop(reopened);
    assert_eq!(
        process_aggregate(),
        aggregate_before,
        "final purge/compaction handle drop returns every charge"
    );
}

/// Renaming is identity-preserving; legal drops and final handle destruction
/// release every vector category once and never resurrect an old route.
#[test]
#[serial]
fn rename_drop_and_database_drop_release_vector_owners_once_without_old_identity_on_restart() {
    let root = TempDir::new().expect("temporary store");
    let path = root.path().join("rename-drop.redb");
    if isolated_process::run_in_own_process(
        module_path!(),
        "rename_drop_and_database_drop_release_vector_owners_once_without_old_identity_on_restart",
    ) {
        return;
    }
    let aggregate_before = process_aggregate();
    let db = open_durable(&path);
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_table(&db, "drop_items");
    insert(&db, "drop_items", 0x601, "alpha", 0);
    drive_maintenance(&db, "drop_items", "alpha");
    indexed(&db, "drop_items", "alpha", vector(0));
    let before_rename = receipt!(&db);
    db.execute(
        "ALTER TABLE drop_items RENAME COLUMN embedding TO vectoring",
        &empty(),
    )
    .expect("rename through the public DDL door");
    assert_same_receipt!(
        receipt!(&db),
        before_rename,
        "rename transfers each owner to the new vector identity without copying it"
    );
    db.execute("ALTER TABLE drop_items DROP COLUMN vectoring", &empty())
        .expect("drop the vector column through the public DDL door");
    let after_column_drop = receipt!(&db);
    assert_eq!(
        after_column_drop.retained_total, 0,
        "vector-column drop releases raw, directory, graphs, keys, and maps once"
    );
    assert_exact_reconciliation!(
        &db,
        "equal-length vectoring rename and vector-column drop exactly reconcile every owner"
    );
    create_table(&db, "table_drop_control");
    insert(&db, "table_drop_control", 0x602, "bravo", 1);
    drive_maintenance(&db, "table_drop_control", "bravo");
    indexed(&db, "table_drop_control", "bravo", vector(1));
    assert!(
        receipt!(&db).retained_total > 0,
        "the table-drop control owns vector memory before its public DDL drop"
    );
    db.execute("DROP TABLE table_drop_control", &empty())
        .expect("drop the independent vector table through the public DDL door");
    assert_eq!(
        receipt!(&db).retained_total,
        0,
        "table drop releases its raw body, directory, graphs, keys, and maps once"
    );
    assert_exact_reconciliation!(
        &db,
        "the independent table-drop control exactly reconciles every owner"
    );
    db.close().expect("close after vector-column drop");
    drop(db);

    let reopened = open_durable(&path);
    assert_eq!(
        receipt!(&reopened).retained_total,
        0,
        "restart exposes no dropped vector identity"
    );
    assert_exact_reconciliation!(
        &reopened,
        "the final no-vector restart exactly reconciles categories and caller accountant"
    );
    reopened.close().expect("close the no-vector restart");
    assert_eq!(
        reopened.accountant().underflow_count_for_test(),
        0,
        "all drop paths release exactly once"
    );
    drop(reopened);
    assert_eq!(
        process_aggregate(),
        aggregate_before,
        "dropping the final database handle returns every charge"
    );
}

/// The consumer marks every passage searchable after each extraction pass.
/// HISTORY ALL preserves those scalar/no-op versions; their immutable vector
/// bytes have one resident allocation while directory memberships keep growing.
#[test]
#[serial]
#[ignore = "Reading stored index entries fails because the engine decode requires an operation memory owner."]
fn retained_journey_scalar_publications_bound_unique_payloads_and_admit_a_small_write() {
    let root = TempDir::new().expect("task-scoped retained journey store");
    if isolated_process::run_in_own_process(
        module_path!(),
        "retained_journey_scalar_publications_bound_unique_payloads_and_admit_a_small_write",
    ) {
        return;
    }
    let aggregate_before = process_aggregate();
    let db = open_durable(&root.path().join("retained-scalar.redb"));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE passages (id UUID PRIMARY KEY, is_searchable BOOLEAN NOT NULL, embedding VECTOR(768) SEARCH_MODE EXACT AUTO_INDEX_AT 1 CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 1))", &empty()).unwrap();
    let mut embedding = vec![0.0; 768];
    embedding[0] = 1.0;
    for row in 1..=16 {
        db.execute(
            "INSERT INTO passages (id, is_searchable, embedding) VALUES ($id, false, $embedding)",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(row))),
                ("embedding", Value::Vector(embedding.clone())),
            ]),
        )
        .unwrap();
    }
    let mut interval_snapshots = vec![db.snapshot()];
    let initial = receipt!(&db);
    // Inserted bodies are durable and load on demand, so at most one copy of
    // each inserted payload is resident; the cycles below must not add any.
    assert!(
        initial.raw_vector_bodies <= 16 * 768 * 4,
        "resident vector bodies never exceed one copy of each inserted payload: {}",
        initial.raw_vector_bodies
    );
    // Keep one fixed finite limit for all publication/maintenance/steady cycles.
    db.set_memory_limit(Some(initial.accountant_total + 4 * 1024 * 1024))
        .unwrap();
    for cycle in 1..=64 {
        db.execute("UPDATE passages SET is_searchable = true", &empty())
            .unwrap();
        interval_snapshots.push(db.snapshot());
        let before = receipt!(&db);
        eprintln!("retained-scalar cycle={cycle} phase=before-maintenance owners={before:?}");
        let report = db.run_maintenance_cycle().unwrap();
        assert_eq!(
            report.vector.first_failure, None,
            "each publication cycle must maintain its partition"
        );
        assert_eq!(report.vector.first_failure_details, None);
        let after = receipt!(&db);
        eprintln!("retained-scalar cycle={cycle} phase=after-maintenance owners={after:?}");
        assert_eq!(before.raw_vector_bodies, initial.raw_vector_bodies);
        assert_eq!(after.raw_vector_bodies, initial.raw_vector_bodies);
        assert_eq!(after.temporary_workspace, 0);
        assert_exact_reconciliation!(&db, "retained scalar publication has exact disjoint owners");
    }
    let vector_fixture_reader_1 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_1 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_1.clone());

    let entries = db
        .vector_store_for_test()
        .entries_for_index(
            &VectorIndexRef::new("passages", "embedding"),
            &mut vector_fixture_copy_1,
        )
        .unwrap();
    assert_eq!(
        entries.len(),
        16 * 65,
        "all historical vector memberships remain present"
    );
    let identities = entries
        .iter()
        .map(|entry| (entry.row_id, entry.created_tx, entry.deleted_tx, entry.lsn))
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        identities.len(),
        16 * 65,
        "every retained interval has its distinct membership identity"
    );
    assert_eq!(interval_snapshots.len(), 65);
    for snapshot in interval_snapshots {
        let visible = entries
            .iter()
            .filter(|entry| {
                entry.created_tx.0 <= snapshot.0
                    && entry
                        .deleted_tx
                        .is_none_or(|deleted| deleted.0 > snapshot.0)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            visible.len(),
            16,
            "each of all 65 retained snapshot intervals still resolves all passages"
        );
        assert!(visible.iter().all(|entry| entry.vector == embedding));
    }
    drop(entries);
    // Complete the final publication's deterministic reclamation before the
    // quiet-cycle control, without retaining a reader snapshot.
    let settling = db.run_maintenance_cycle().unwrap();
    assert_eq!(settling.vector.first_failure, None);
    assert_eq!(settling.vector.first_failure_details, None);
    assert_eq!(settling.vector.remaining_partitions, 0);
    let quiet = receipt!(&db);
    for cycle in 1..=8 {
        let report = db.run_maintenance_cycle().unwrap();
        assert_eq!(report.vector.first_failure, None);
        assert_eq!(report.vector.first_failure_details, None);
        assert_eq!(report.vector.built_partitions, 0);
        assert_eq!(report.vector.remaining_partitions, 0);
        let after = receipt!(&db);
        eprintln!("retained-scalar steady={cycle} owners={after:?}");
        assert_eq!(after.raw_vector_bodies, initial.raw_vector_bodies);
        assert_same_receipt!(
            after,
            quiet,
            "quiet cycles retain exactly the same ownership receipt"
        );
        assert_eq!(after.accountant_total, quiet.accountant_total);
        assert_exact_reconciliation!(&db, "steady maintenance has exact disjoint owners");
    }
    let mut distinct_embedding = vec![0.0; 768];
    distinct_embedding[1] = 1.0;
    db.execute(
        "INSERT INTO passages (id, is_searchable, embedding) VALUES ($id, true, $embedding)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(17))),
            ("embedding", Value::Vector(distinct_embedding.clone())),
        ]),
    )
    .expect("the unchanged finite limit still admits a small write");
    let after_write = receipt!(&db).raw_vector_bodies;
    assert!(
        after_write <= 17 * 768 * 4,
        "the small write adds at most its own payload to the resident vector bodies: {after_write}"
    );
    let nearest = db
        .execute(
            "SELECT id FROM passages ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
            &params([("query", Value::Vector(distinct_embedding))]),
        )
        .expect("the fixed finite limit still admits indexed search after a small write");
    assert_eq!(
        nearest.rows(),
        vec![vec![Value::Uuid(Uuid::from_u128(17))]],
        "indexed search returns the distinct vector committed by the small foreground write"
    );
    assert_exact_reconciliation!(&db, "the small write and indexed read leave exact owners");
    db.close().unwrap();
    assert_eq!(db.accountant().underflow_count_for_test(), 0);
    drop(db);
    assert_eq!(process_aggregate(), aggregate_before);
}
