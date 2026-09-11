#![cfg(feature = "test-seams")]
//! Ownership receipts for retained vector memory.
//!
//! Passive per-owner receipts distinguish retained allocations from transient
//! workspace. The caller's accountant detects underflow, and one-shot failure
//! seams check rollback at real publication boundaries. State transitions use
//! Database, SQL, sync, PURGE, and maintenance; aggregate usage alone would hide
//! a leak in one category behind a release in another.

use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadFailureDetail, ReadFailureLimit,
    ReadLimits,
};
use contextdb_core::{Error, TenantId, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::memory_accounting::MemoryAccountant;
use contextdb_engine::plugin::CorePlugin;
use contextdb_engine::{
    Database, MaintenancePolicy, MaintenanceReport, VectorJournalTruncationPhaseForTest,
    VectorMemoryOwnershipReceiptForTest, VectorMemoryWorkspacePhaseForTest,
};
use contextdb_server::subjects::pull_subject;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use contextdb_vector::VectorRouteQuarantineReason;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const MAX_MAINTENANCE_CYCLES: usize = 16;
const SAFETY_TIMEOUT: Duration = Duration::from_secs(5);
const FINITE_CLEANUP_BUDGET: usize = 64 * 1024 * 1024;
const RECEIVED_TENANT: &str = "vector-memory-ownership-reconciliation";
const RECEIVED_SCHEMA: &str = "CREATE TABLE received_items (
    id UUID PRIMARY KEY,
    scope TEXT NOT NULL,
    embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 8 SEARCH_MODE INDEXED
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
             embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 8 SEARCH_MODE INDEXED\
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
             embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 8 SEARCH_MODE INDEXED\
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

fn drive_maintenance(db: &Database, table: &str, scope: &str) {
    let index = index(table);
    let partition = partition(scope);
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if db
            .vector_store_for_test()
            .partition_info(&index, &partition)
            .is_some_and(|info| info.graph_available)
        {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!("fixture route was not maintained within the fixed finite cycle count");
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
            "SELECT id, embedding FROM {table} WHERE scope = $scope \n             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1"
        ),
        &params([
            ("scope", Value::Text(scope.to_owned())),
            ("query", Value::Vector(query)),
        ]),
    )
    .expect("the production indexed SQL door serves the selected partition")
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

fn roomy_read_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000,
        result_bytes: 16 * 1024 * 1024,
        work: 10_000_000,
        active_ms: 1_000_000,
        memory: 128 * 1024 * 1024,
        cursor_page_rows: 128,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn bounded_indexed_request(query: Vec<f32>) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(
        "SELECT id, embedding FROM load_items WHERE scope = 'alpha' \
         ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
        params([("query", Value::Vector(query))]),
        roomy_read_limits(),
        Arc::new(FrozenClock),
    )
}

struct CancelOnDormantPrecharge {
    expected_bytes: u64,
    cancellation: OwnerReadCancellation,
    observed: AtomicBool,
    held_before: AtomicU64,
    cancellation_observed: AtomicBool,
}

impl bounded::ExecutionProbe for CancelOnDormantPrecharge {
    fn before_work(&self, _source: bounded::TestWorkSource, _completed_work: u64) {}

    fn after_temporary_reservation(
        &self,
        source: bounded::TestWorkSource,
        reserved_bytes: u64,
        held_temporary_bytes: u64,
    ) {
        if source == bounded::TestWorkSource::VectorCandidates
            && reserved_bytes == self.expected_bytes
            && !self.observed.swap(true, Ordering::SeqCst)
        {
            self.held_before.store(
                held_temporary_bytes.saturating_sub(reserved_bytes),
                Ordering::SeqCst,
            );
            self.cancellation.cancel();
        }
    }

    fn cancellation_observed(&self, _completed_work: u64) {
        self.cancellation_observed.store(true, Ordering::SeqCst);
    }
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
    assert_eq!(result.rows.len(), 1, "exact lookup returns one current row");
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("exact lookup projects id");
    let vector_column = result
        .columns
        .iter()
        .position(|column| column == "embedding" || column.rsplit('.').next() == Some("embedding"))
        .expect("exact lookup projects embedding");
    assert_eq!(result.rows[0][id_column], Value::Uuid(expected_id));
    assert_eq!(
        result.rows[0][vector_column],
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
    ($db:expr, $accountant:expr, $message:literal) => {{
        let receipt = receipt!($db);
        assert_eq!(
            receipt.retained_total,
            receipt.raw_vector_bodies
                + receipt.raw_reverse_directory
                + receipt.partition_vectors
                + receipt.base_graph
                + receipt.change_graph
                + receipt.mutable_tail
                + receipt.typed_partition_keys
                + receipt.retired_pinned_graph_bytes,
            $message
        );
        assert_eq!(receipt.temporary_workspace, 0, $message);
        // The receipt includes the accountant total and the non-vector
        // remainder; compare both with the caller-held accountant.
        assert_eq!(
            receipt.accountant_total,
            receipt.non_vector_total + receipt.retained_total + receipt.temporary_workspace,
            $message
        );
        assert_eq!($accountant.usage().used, receipt.accountant_total, $message);
        // The caller accountant records over-release in this test-only count.
        assert_eq!($accountant.underflow_count_for_test(), 0, $message);
    }};
}

fn open_with_accountant(path: &std::path::Path, accountant: Arc<MemoryAccountant>) -> Database {
    Database::open_with_config(path, Arc::new(CorePlugin), accountant)
        .expect("open the durable fixture with the caller accountant")
}

fn run_maintenance_with_journal_cleanup_paused(
    db: &Arc<Database>,
    accountant: &MemoryAccountant,
) -> (
    VectorMemoryOwnershipReceiptForTest,
    usize,
    MaintenanceReport,
) {
    let pause = db.__arm_one_shot_vector_journal_cleanup_pause_for_test();
    let worker_db = Arc::clone(db);
    let (result_tx, result_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        result_tx
            .send(worker_db.run_maintenance_cycle())
            .expect("cleanup result receiver remains live");
    });
    if !pause.wait_until_reached(SAFETY_TIMEOUT) {
        pause.release();
        let _ = result_rx.recv_timeout(SAFETY_TIMEOUT);
        worker
            .join()
            .expect("cleanup worker exits after an unreached pause is released");
        panic!("maintenance did not reach the live journal-cleanup allocation pause");
    }
    let active = receipt!(db);
    let accountant_used = accountant.usage().used;
    pause.release();
    let report = result_rx
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("released journal cleanup returns before the safety timeout")
        .expect("released journal cleanup completes its finite maintenance cycle");
    worker
        .join()
        .expect("journal-cleanup maintenance worker exits after release");
    (active, accountant_used, report)
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
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let before_open = accountant.usage().used;

    {
        let db = open_with_accountant(&path, accountant.clone());
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_table(&db, "ownership_items");
        insert(&db, "ownership_items", 0x101, "alpha", 0);
        insert(&db, "ownership_items", 0x102, "bravo", 1);
        drive_maintenance(&db, "ownership_items", "alpha");
        drive_maintenance(&db, "ownership_items", "bravo");
        db.close().expect("seal the initial durable fixture");
    }
    assert_eq!(
        accountant.usage().used,
        before_open,
        "dropping the seed handle returns every initial fixture charge"
    );

    let mut stable_reopen_receipt: Option<VectorMemoryOwnershipReceiptForTest> = None;
    let mut fresh_control_receipt: Option<VectorMemoryOwnershipReceiptForTest> = None;
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
    // INSERT/UPDATE/UPDATE/DELETE/compaction doors used by round 0's own
    // churn below -- never a literal physical-version count or byte count.
    let mut retained_deleted_row_physical_versions: Option<usize> = None;
    let mut retained_deleted_row_non_vector_bytes: Option<usize> = None;
    for round in 0..3 {
        let db = open_with_accountant(&path, accountant.clone());
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
        assert!(
            loaded.base_graph > dormant.base_graph,
            "the selected partition adds positive saved-graph ownership; F32 projection remains in the relational row"
        );
        assert!(
            loaded.raw_reverse_directory > 0
                && loaded.partition_vectors > 0
                && loaded.typed_partition_keys > 0,
            "the live fixture separately owns its reverse directory, partition registry, and typed keys"
        );
        assert_exact_reconciliation!(
            &db,
            accountant,
            "lazy load releases decode/replay workspace without over-release"
        );

        if round == 0 {
            let before_embedding_receipt = receipt!(&db);
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
                let control_accountant = Arc::new(MemoryAccountant::no_limit());
                let control = open_with_accountant(
                    &control_root.path().join("one-row-control.redb"),
                    control_accountant.clone(),
                );
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
                drop(control);
                assert_eq!(
                    control_accountant.usage().used,
                    0,
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
            let after_physical_versions = db.__physical_version_count_for_test("ownership_items");
            assert_eq!(
                after_embedding_receipt.raw_vector_bodies,
                before_embedding_receipt.raw_vector_bodies
                    + one_vector_payload_bytes
                    + one_vector_payload_bytes,
                "the replacement's raw storage owns exactly the new row's body plus the \
                 still-resident superseded body"
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
                after_embedding_receipt.non_vector_total, before_embedding_receipt.non_vector_total,
                "an equal-shaped replacement leaves the retained non-vector charge \
                 unchanged: the superseded version's bytes are released at this same \
                 commit, exactly offsetting the new version's identical-size charge"
            );
            let mut change_receipt = receipt!(&db);
            for _ in 0..MAX_MAINTENANCE_CYCLES {
                if change_receipt.change_graph > 0 {
                    break;
                }
                db.run_maintenance_cycle()
                    .expect("one finite change-generation maintenance batch returns");
                change_receipt = receipt!(&db);
            }
            assert!(
                change_receipt.change_graph > 0,
                "a replacement on a ready route has a separately positive sealed-change owner"
            );
            assert_exact_reconciliation!(
                &db,
                accountant,
                "the sealed change graph is charged once before later churn"
            );
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
            assert!(
                bravo_loaded.base_graph > 0,
                "bravo's saved graph is resident before its explicit eviction; F32 projection remains in the relational row"
            );
            let bravo = partition("bravo");
            let raw_evicted = db
                .vector_store_for_test()
                .evict_raw_partition_for_test(&index("ownership_items"), &bravo);
            assert_eq!(
                raw_evicted,
                bravo_loaded.raw_vector_bodies > 0,
                "raw eviction reports exactly whether this F32 route owned a separate raw body"
            );
            assert!(
                db.vector_store_for_test()
                    .evict_resident_partition_generation(
                        &contextdb_vector::VectorPartitionRef::new(index("ownership_items"), bravo),
                        true,
                    ),
                "the deterministic eviction seam evicts an idle saved graph"
            );
            let evicted = receipt!(&db);
            assert_eq!(
                evicted.raw_vector_bodies, 0,
                "idle eviction returns the selected raw body"
            );
            assert_eq!(
                evicted.base_graph, 0,
                "idle eviction returns the selected saved graph"
            );
            assert_exact_reconciliation!(
                &db,
                accountant,
                "churn, compaction, and eviction return temporary owners exactly once"
            );
        }
        db.close().expect("close one deterministic restart round");
        drop(db);

        if round == 0 {
            let control_accountant = Arc::new(MemoryAccountant::no_limit());
            let control = open_with_accountant(&control_path, control_accountant.clone());
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
            let control = open_with_accountant(&control_path, control_accountant.clone());
            fresh_control_receipt = Some(receipt!(&control));
            assert_exact_reconciliation!(
                &control,
                control_accountant,
                "the fresh surviving-state control exactly reconciles every owner"
            );
            control
                .close()
                .expect("close fresh control verification reopen");
            drop(control);
            assert_eq!(
                control_accountant.usage().used,
                0,
                "final fresh-control handle drop returns every charge"
            );
            assert_eq!(control_accountant.underflow_count_for_test(), 0);

            // Isolated deleted-row-lineage control: replicate alpha's exact
            // journey (insert, replace the embedding, move the scope, then
            // delete) alone in its own store, through the identical
            // production doors round 0 uses below, then run the same
            // currency-compaction test seam on it. This proves, in
            // isolation, that compaction prunes the lineage's two
            // UPDATE-superseded versions (a real prune) while the tombstone
            // itself is retained by design, and measures exactly how many
            // physical versions and how many non-vector bytes that retained
            // tombstone alone costs -- never a literal count.
            let lineage_root =
                TempDir::new().expect("temporary isolated deleted-row lineage control");
            let lineage_accountant = Arc::new(MemoryAccountant::no_limit());
            let lineage = open_with_accountant(
                &lineage_root.path().join("deleted-row-lineage-control.redb"),
                lineage_accountant.clone(),
            );
            lineage.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            create_table(&lineage, "ownership_items");
            // Baseline: the bare table schema's own non-vector charge with
            // zero rows, so the retained-tombstone figure below isolates
            // only the incremental cost of the lineage's history, never the
            // schema's own standing overhead.
            let lineage_baseline_non_vector_total = receipt!(&lineage).non_vector_total;
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
                lineage
                    .__compact_currency_versions_for_tables_for_test(&["ownership_items"])
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
            let lineage_retained_non_vector_bytes = receipt!(&lineage)
                .non_vector_total
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

        let reopened = open_with_accountant(&path, accountant.clone());
        let expected_control = fresh_control_receipt
            .as_ref()
            .expect("the fresh surviving-state control was measured");
        let expected_control_physical_versions = control_physical_versions
            .expect("the fresh surviving-state control's physical version count was measured");
        // The deleted row's tombstone is retained forever under HISTORY
        // ALL (no production door short of dropping the table erases a
        // DELETE tombstone), so it stays physically resident in `path`
        // across every reopen in every round, not just round 0's.
        let expected_reconciled_physical_versions = expected_control_physical_versions
            + retained_deleted_row_physical_versions.expect(
                "the isolated deleted-row-lineage control's retained version count was measured",
            );
        let expected_reconciled_non_vector_total = expected_control
            .non_vector_total
            + retained_deleted_row_non_vector_bytes
                .expect("the isolated deleted-row-lineage control's retained non-vector bytes were measured");
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
                receipt!(&reopened).non_vector_total,
                expected_control.non_vector_total,
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
            reopened
                .__compact_currency_versions_for_tables_for_test(&["ownership_items"])
                .expect("prune the retained history through the production door");
        }
        let physical_versions_after_compaction =
            reopened.__physical_version_count_for_test("ownership_items");
        if physical_versions_before_compaction > expected_reconciled_physical_versions {
            // Only round 0's reopen still carries alpha's two
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
            receipt.non_vector_total, expected_reconciled_non_vector_total,
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
            accountant,
            "each reopen exactly reconciles its vector categories and caller accountant"
        );
        reopened.close().expect("close verification reopen");
        drop(reopened);
    }

    {
        let db = open_with_accountant(&path, accountant.clone());
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_current_only_table(&db, "current_only_items");
        insert(&db, "current_only_items", 0x301, "gamma", 0);
        drive_maintenance(&db, "current_only_items", "gamma");

        // Hold an old read across the coming replacements: a caller-held
        // SnapshotPin still sees the version live when it was taken, so
        // currency compaction must defer pruning it rather than physically
        // removing a version out from under a registered reader.
        let held_snapshot = db.snapshot();
        let held_pin = db.pin_snapshot(held_snapshot);
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
        let pinned_receipt = receipt!(&db);

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
            pinned_snapshot_read.rows.len(),
            1,
            "the pinned snapshot still resolves exactly one row for the held id"
        );
        let pinned_embedding_column = pinned_snapshot_read
            .columns
            .iter()
            .position(|column| {
                column == "embedding" || column.rsplit('.').next() == Some("embedding")
            })
            .expect("the pinned-snapshot read projects embedding");
        assert_eq!(
            pinned_snapshot_read.rows[0][pinned_embedding_column],
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
        let deferred_report = db
            .compact_currency_versions()
            .expect("a deferred currency pass still returns cleanly");
        assert_eq!(
            deferred_report.pruned_versions, 1,
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
        let deferred_receipt = receipt!(&db);
        assert_eq!(
            deferred_receipt.non_vector_total, pinned_receipt.non_vector_total,
            "pruning the pin-invisible intermediate version while the pin is held leaves the \
             retained non-vector charge unchanged: that version's bytes were already released \
             at the commit that superseded it, so physically removing it is a logical no-op \
             and never credits the same bytes a second time"
        );
        assert_exact_reconciliation!(
            &db,
            accountant,
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

        let db = open_with_accountant(&path, accountant.clone());
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
        let reopened_receipt = receipt!(&db);
        assert_eq!(
            reopened_receipt.non_vector_total, deferred_receipt.non_vector_total,
            "reopen charges the identical retained non-vector total for the one \
             still-deferred historical copy, not merely the live row"
        );

        let before_release_compaction = receipt!(&db);
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
        let after_release_compaction = receipt!(&db);
        assert_eq!(
            before_release_compaction.non_vector_total, after_release_compaction.non_vector_total,
            "releasing the pin and compacting leaves the retained non-vector charge unchanged: \
             the one now-pruned historical copy's bytes were already released at the commit \
             that superseded it, so physically removing it is a logical no-op and never \
             credits the same bytes a second time"
        );
        assert_exact_reconciliation!(
            &db,
            accountant,
            "releasing the pin and pruning the deferred historical copies reconciles every \
             vector category and the caller accountant, exactly once"
        );

        db.close()
            .expect("close after releasing the pin and pruning deferred history");
        drop(db);
        let db = open_with_accountant(&path, accountant.clone());
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_eq!(
            db.__physical_version_count_for_test("current_only_items"),
            versions_after_release_compaction,
            "reopening again after the release-triggered compaction retains the same, \
             already-pruned physical version population"
        );
        assert_eq!(
            receipt!(&db).non_vector_total,
            after_release_compaction.non_vector_total,
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
        let currency_report = db.compact_currency_versions().expect(
            "prune the superseded current-only-history versions through the production door",
        );
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
            accountant,
            "pruning a moved-then-deleted current-only-history row reconciles every vector \
             category and the caller accountant"
        );
        let after_currency_round = receipt!(&db);
        assert_eq!(
            after_currency_round.raw_vector_bodies, 0,
            "currency compaction returns the pruned current-only-history route's raw body"
        );
        assert!(
            after_currency_round.raw_reverse_directory
                < before_currency_round.raw_reverse_directory,
            "currency compaction returns the moved-and-deleted row's reverse-directory entry"
        );
        assert!(
            after_currency_round.base_graph == 0
                && after_currency_round.change_graph == 0
                && after_currency_round.mutable_tail == 0,
            "the pruned current-only-history route retains no maintained-graph owner"
        );
        db.close()
            .expect("close the current-only-history compaction round");
        drop(db);
    }
    assert_eq!(
        accountant.usage().used,
        before_open,
        "dropping every final Database handle restores the caller accountant to pre-open use"
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

/// A complete received image transfers vector ownership rather than layering a
/// second raw body, directory, graph, or typed key copy over the first image.
#[tokio::test]
#[serial]
async fn received_image_replacement_is_idempotent_and_its_prepublication_failure_preserves_receipt()
{
    let root = TempDir::new().expect("temporary durable receiver");
    let edge_path = root.path().join("received-ownership.redb");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let before_open = accountant.usage().used;
    let source = Arc::new(Database::open_memory());
    let edge = Arc::new(open_with_accountant(&edge_path, accountant.clone()));
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
        ),
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
    );

    client
        .pull_default()
        .await
        .expect("authenticated pull installs the initial complete image");
    drive_maintenance(&edge, "received_items", "alpha");
    indexed(&edge, "received_items", "alpha", vector(0));
    let initial = receipt!(&edge);
    assert_exact_reconciliation!(
        &edge,
        accountant,
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
        accountant,
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
        changed.partition_vectors <= initial.partition_vectors,
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
    assert_same_vector_owners!(
        repeated,
        changed,
        "repeating a changed image cannot grow a second ownership set"
    );
    assert_eq!(
        repeated.non_vector_total,
        changed.non_vector_total
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
        accountant,
        "the final received image exactly reconciles every owner"
    );

    drop(client);
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("server task stops");
    drop(server);
    edge.close().expect("close the received-image receiver");
    drop(edge);
    assert_eq!(
        accountant.usage().used,
        before_open,
        "dropping the final received-image handle restores caller usage"
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

/// A long canonical TEXT key is charged before a source snapshot can make the
/// old partition state survive; reclamation removes each old owner exactly once.
#[test]
#[serial]
fn long_typed_key_move_and_snapshot_retirement_admit_before_mutation_and_release_once() {
    let root = TempDir::new().expect("temporary store");
    let path = root.path().join("long-key.redb");
    let control_path = root.path().join("long-key-control.redb");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let before_open = accountant.usage().used;
    let long_key = "destination-key-".repeat(32);

    let admission = {
        let calibration_accountant = Arc::new(MemoryAccountant::no_limit());
        let calibration = open_with_accountant(&control_path, calibration_accountant.clone());
        calibration.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_current_only_table(&calibration, "key_items");
        insert(&calibration, "key_items", 0x301, "source", 0);
        drive_maintenance(&calibration, "key_items", "source");
        let source_snapshot = calibration.snapshot();
        let source_pin = calibration.pin_snapshot(source_snapshot);
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
            calibration_accountant,
            "the calibrated long-key lifecycle exactly reconciles every owner"
        );
        drop(source_pin);
        calibration
            .close()
            .expect("close the admission calibration");
        drop(calibration);
        assert_eq!(
            calibration_accountant.usage().used,
            0,
            "calibration final drop returns every charge"
        );
        assert_eq!(calibration_accountant.underflow_count_for_test(), 0);
        delta
    };
    let db = open_with_accountant(&path, accountant.clone());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_current_only_table(&db, "key_items");
    insert(&db, "key_items", 0x301, "source", 0);
    drive_maintenance(&db, "key_items", "source");
    let old_snapshot = db.snapshot();
    let pin = db.pin_snapshot(old_snapshot);
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
        accountant,
        "snapshot retirement exactly reconciles every owner without underflow"
    );
    db.close().expect("close long-key lifecycle");
    drop(db);
    assert_eq!(
        accountant.usage().used,
        before_open,
        "final long-key handle drop restores caller usage"
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

/// Lazy graph admission must leave no partial route or fresh tail on every
/// decode, budget, and reconciliation failure; a retry owns one base/change/tail.
#[test]
#[serial]
fn graph_load_and_reconciliation_failures_restore_the_dormant_route_and_exact_receipt() {
    let root = TempDir::new().expect("temporary store");
    let path = root.path().join("load-failure.redb");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let before_open = accountant.usage().used;
    {
        let db = open_with_accountant(&path, accountant.clone());
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_table(&db, "load_items");
        insert(&db, "load_items", 0x401, "alpha", 0);
        drive_maintenance(&db, "load_items", "alpha");
        insert(&db, "load_items", 0x402, "alpha", 1);
        db.close().expect("close saved base plus journal suffix");
    }
    assert_eq!(
        accountant.usage().used,
        before_open,
        "seed handle drop returns every fixture charge"
    );

    let calibration = open_with_accountant(&path, accountant.clone());
    calibration.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let calibration_before = calibration.accountant().usage().used;
    let calibration_result = indexed_result(&calibration, "load_items", "alpha", vector(1));
    let calibration_receipt = receipt!(&calibration);
    let load_admission = calibration
        .accountant()
        .usage()
        .used
        .checked_sub(calibration_before)
        .expect("a successful lazy load does not reduce standing database use");
    assert!(
        load_admission > 0,
        "the full lazy-load lifecycle has positive total admission"
    );
    assert_exact_reconciliation!(
        &calibration,
        accountant,
        "the lazy-load calibration exactly reconciles every owner"
    );
    calibration.close().expect("close lazy-load calibration");
    drop(calibration);
    assert_eq!(
        accountant.usage().used,
        before_open,
        "calibration final drop returns every charge"
    );

    let db = open_with_accountant(&path, accountant.clone());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let before = receipt!(&db);
    let route = contextdb_vector::VectorPartitionRef::new(index("load_items"), partition("alpha"));
    let dormant = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route)
        .expect("reopen retains the dormant route descriptor");
    let dormant_load_bytes = db
        .vector_store_for_test()
        .dormant_partition_generation_load_bytes_for_test(&route, true)
        .expect("the dormant base declares its complete pre-I/O load admission");
    assert!(
        dormant_load_bytes > calibration_receipt.base_graph,
        "the caller precharge includes both final graph residency and the persisted decode buffer"
    );

    let before_used = db.accountant().usage().used;
    db.set_memory_limit(Some(before_used + load_admission - 1))
        .expect("set a one-byte-short store limit");
    let refusal = db
        .execute(
            "SELECT id FROM load_items WHERE scope = 'alpha' \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &params([("query", Value::Vector(vector(0)))]),
        )
        .expect_err("one byte below measured lazy-load ownership refuses");
    assert!(
        matches!(refusal, Error::MemoryBudgetExceeded { .. }),
        "lazy graph admission uses the typed memory-budget refusal: {refusal:?}"
    );
    assert_same_receipt!(
        receipt!(&db),
        before,
        "one-byte-short load returns every reservation"
    );
    assert_exact_reconciliation!(
        &db,
        accountant,
        "one-byte-short load restores every category and accountant charge"
    );
    db.set_memory_limit(None)
        .expect("clear the one-byte-short limit before phase-specific faults");

    // Decoder and reconciliation faults each fire once at their stated phase.
    // The following assertions check consumption and unchanged published state.
    db.arm_dormant_vector_decode_failure_for_test();
    let cancellation = OwnerReadCancellation::new();
    let precharge_probe = Arc::new(CancelOnDormantPrecharge {
        expected_bytes: u64::try_from(dormant_load_bytes)
            .expect("the fixture load admission fits the read contract"),
        cancellation: cancellation.clone(),
        observed: AtomicBool::new(false),
        held_before: AtomicU64::new(0),
        cancellation_observed: AtomicBool::new(false),
    });
    let mut cancelled_request = bounded_indexed_request(vector(1));
    cancelled_request.cancellation = cancellation;
    cancelled_request.probe =
        Some(Arc::clone(&precharge_probe) as Arc<dyn bounded::ExecutionProbe>);
    assert!(
        matches!(
            bounded::execute(&db, &cancelled_request),
            Err(bounded::TestError::Cancelled)
        ),
        "cancelling immediately after the dormant precharge returns the existing typed cancellation"
    );
    assert!(
        precharge_probe.observed.load(Ordering::SeqCst)
            && precharge_probe.cancellation_observed.load(Ordering::SeqCst),
        "the exact dormant load admission is reserved before the next cancellable load checkpoint"
    );
    assert!(
        !db.dormant_vector_decode_failure_consumed_for_test(),
        "pre-I/O cancellation leaves the later decode fault armed"
    );
    assert_same_receipt!(
        receipt!(&db),
        before,
        "cancelled dormant loading returns its whole caller reservation"
    );
    assert_exact_reconciliation!(
        &db,
        accountant,
        "cancelled dormant loading leaves no request or store owner behind"
    );

    let mut one_byte_short = bounded_indexed_request(vector(1));
    one_byte_short.limits.memory = precharge_probe
        .held_before
        .load(Ordering::SeqCst)
        .saturating_add(u64::try_from(dormant_load_bytes).expect("load bytes fit u64"))
        .saturating_sub(1);
    let refusal = bounded::execute(&db, &one_byte_short);
    assert!(
        matches!(
            &refusal,
            Err(bounded::TestError::Refused(refusal))
                if matches!(
                    refusal.detail(),
                    ReadFailureDetail::OwnerLimitExceeded(detail)
                        if detail.limit == ReadFailureLimit::Memory
                )
        ),
        "one byte below the observed caller precharge returns the typed memory refusal: {refusal:?}"
    );
    assert!(
        !db.dormant_vector_decode_failure_consumed_for_test(),
        "caller-budget refusal occurs before persistence/decode begins"
    );
    assert_same_receipt!(
        receipt!(&db),
        before,
        "one-byte-short caller admission leaves the dormant route unchanged"
    );

    assert!(bounded::execute(&db, &bounded_indexed_request(vector(1))).is_err());
    assert!(
        db.dormant_vector_decode_failure_consumed_for_test(),
        "the dormant decode fault is consumed exactly once"
    );
    assert_same_receipt!(
        receipt!(&db),
        before,
        "corrupt/incompatible decode returns every owner"
    );
    assert_exact_reconciliation!(
        &db,
        accountant,
        "the consumed dormant decode fault exactly reconciles every owner"
    );
    db.arm_vector_post_load_reconciliation_failure_for_test();
    assert!(bounded::execute(&db, &bounded_indexed_request(vector(1))).is_err());
    assert!(
        db.vector_post_load_reconciliation_failure_consumed_for_test(),
        "the post-load reconciliation fault is consumed exactly once"
    );
    assert_same_receipt!(
        receipt!(&db),
        before,
        "post-load reconciliation failure returns every owner"
    );
    assert_exact_reconciliation!(
        &db,
        accountant,
        "the consumed post-load reconciliation fault exactly reconciles every owner"
    );
    let after_failures = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route)
        .expect("failure preserves the dormant route");
    assert_eq!(after_failures.dormant_base, dormant.dormant_base);
    assert!(!after_failures.base_resident && !after_failures.change_resident);
    assert_eq!(
        after_failures.fresh_tail_entries, 0,
        "failed load creates no fresh tail"
    );

    let bounded_loaded = bounded::execute(&db, &bounded_indexed_request(vector(1)))
        .expect("an admitted bounded request loads and owns the dormant generation");
    assert_eq!(bounded_loaded.result.rows, calibration_result.rows);
    assert_eq!(bounded_loaded.result.columns, calibration_result.columns);
    assert!(
        bounded_loaded
            .telemetry
            .source_peak_temporary_bytes
            .get(&bounded::TestWorkSource::VectorCandidates)
            .is_some_and(|peak| *peak >= dormant_load_bytes as u64),
        "the bounded receipt includes the dormant generation's pre-I/O load admission"
    );
    let ordinary_loaded = indexed_result(&db, "load_items", "alpha", vector(1));
    assert_eq!(ordinary_loaded.rows, bounded_loaded.result.rows);
    assert_eq!(ordinary_loaded.columns, bounded_loaded.result.columns);
    let loaded = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route)
        .expect("successful retry publishes resident generation state");
    assert!(
        loaded.base_resident,
        "successful retry owns one resident sealed base"
    );
    assert_eq!(
        loaded.fresh_tail_entries, 1,
        "successful retry replays exactly the one post-base durable suffix"
    );
    let loaded_receipt = receipt!(&db);
    assert_eq!(
        loaded_receipt.base_graph, calibration_receipt.base_graph,
        "success transfers exactly the calibrated sealed-base owner from request to store"
    );
    assert!(
        loaded_receipt.base_graph > 0 && loaded_receipt.mutable_tail > 0,
        "successful retry owns the sealed base and one-entry fresh tail; F32 projection remains in the relational row"
    );
    assert_exact_reconciliation!(
        &db,
        accountant,
        "successful retry exactly reconciles base, change, tail, and all other owners"
    );
    db.close().expect("close lazy-load failure journey");
    drop(db);
    assert_eq!(
        accountant.usage().used,
        before_open,
        "final lazy-load handle drop restores caller usage"
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

#[test]
#[serial]
fn retired_graphs_release_between_partition_rebuilds_without_losing_pinned_reads() {
    let root = TempDir::new().unwrap();
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = open_with_accountant(
        &root.path().join("rebuild-retirement.redb"),
        accountant.clone(),
    );
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_table(&db, "rebuild_items");
    for (ordinal, scope) in ["alpha", "bravo", "charlie"].iter().enumerate() {
        insert(&db, "rebuild_items", 100 + ordinal as u128 * 2, scope, 0);
        insert(&db, "rebuild_items", 101 + ordinal as u128 * 2, scope, 1);
        drive_maintenance(&db, "rebuild_items", scope);
    }
    let snapshot = db.snapshot();
    let pin = db.pin_snapshot(snapshot);
    let sql = "SELECT id FROM rebuild_items WHERE scope = 'alpha' ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 2";
    let query = params([("query", Value::Vector(vector(0)))]);
    let old = db.execute_at_snapshot(sql, &query, snapshot).unwrap();
    for axis in [1, 2] {
        for ordinal in 0..3 {
            db.execute(
                "UPDATE rebuild_items SET embedding = $embedding WHERE id = $id",
                &params([
                    ("id", Value::Uuid(Uuid::from_u128(100 + ordinal * 2))),
                    ("embedding", Value::Vector(vector(axis))),
                ]),
            )
            .unwrap();
        }
        if axis == 1 {
            for _ in 0..3 {
                db.run_maintenance_cycle().unwrap();
            }
            assert!(receipt!(&db).change_graph > 0);
        }
    }
    db.run_maintenance_cycle().unwrap();
    assert!(receipt!(&db).retired_pinned_graph_bytes > 0);
    assert_eq!(
        db.execute_at_snapshot(sql, &query, snapshot).unwrap().rows,
        old.rows
    );
    let route =
        contextdb_vector::VectorPartitionRef::new(index("rebuild_items"), partition("alpha"));
    assert!(
        db.__debug_vector_partition_generation_retention_for_test(&route)
            .durable_superseded_generations
            > 0
    );
    let newer_snapshot = db.snapshot();
    let newer_pin = db.pin_snapshot(newer_snapshot);
    let newer_rows = db
        .execute_at_snapshot(sql, &query, newer_snapshot)
        .unwrap()
        .rows;
    drop(pin);
    db.run_maintenance_cycle().unwrap();
    let after = receipt!(&db);
    assert_eq!(
        db.__debug_vector_partition_generation_retention_for_test(&route)
            .durable_superseded_generations,
        0,
        "a newer reader must not retain obsolete durable generations either"
    );
    assert_eq!(
        after.retired_pinned_graph_bytes, 0,
        "a newer live reader and the next pending partition must not retain unneeded earlier graphs"
    );
    let rebuilt = db
        .vector_store_for_test()
        .partition_graph_generation_status(&contextdb_vector::VectorPartitionRef::new(
            index("rebuild_items"),
            partition("charlie"),
        ))
        .unwrap();
    assert!(
        rebuilt.base.is_some() || rebuilt.dormant_base.is_some(),
        "the all-needy wake leaves the third partition with a reloadable replacement base"
    );
    assert!(
        rebuilt.change.is_none() && rebuilt.dormant_change.is_none(),
        "the all-needy wake leaves no third-partition change generation awaiting replacement"
    );
    assert_exact_reconciliation!(
        &db,
        accountant,
        "each replacement releases only its obsolete graph owners"
    );
    assert_eq!(
        db.execute_at_snapshot(sql, &query, newer_snapshot)
            .unwrap()
            .rows,
        newer_rows
    );
    drop(newer_pin);
    db.close().unwrap();
    drop(db);
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

/// Compaction workspace is an owner while it exists, not an unreported peak
/// that disappears before the settled receipt is sampled. The typed pause is
/// a test-only observation after admission and before publication; it neither
/// allocates workspace nor chooses whether compaction runs.
#[test]
#[serial]
fn compaction_workspace_is_positively_charged_while_owned_and_released_after_publication() {
    let root = TempDir::new().expect("temporary durable workspace store");
    let path = root.path().join("compaction-workspace.redb");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let before_open = accountant.usage().used;
    let db = Arc::new(open_with_accountant(&path, accountant.clone()));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_table(&db, "workspace_items");
    insert(&db, "workspace_items", 0x481, "alpha", 0);
    drive_maintenance(&db, "workspace_items", "alpha");
    db.execute(
        "UPDATE workspace_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x481))),
            ("embedding", Value::Vector(vector(1))),
        ]),
    )
    .expect("create committed change work for the maintained route");
    let mut sealed = receipt!(&db);
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if sealed.change_graph > 0 {
            break;
        }
        db.run_maintenance_cycle()
            .expect("seal the first committed change in a finite maintenance batch");
        sealed = receipt!(&db);
    }
    assert!(
        sealed.change_graph > 0,
        "the compaction fixture owns a positive sealed change graph before replacement-base work"
    );
    db.execute(
        "UPDATE workspace_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x481))),
            ("embedding", Value::Vector(vector(2))),
        ]),
    )
    .expect("create a later fresh tail that requires maintained-vector compaction");

    // Observe workspace after compaction admission and before publication.
    let pause = db.__arm_one_shot_vector_memory_workspace_pause_for_test(
        VectorMemoryWorkspacePhaseForTest::CompactionPreparedBeforePublication,
    );
    let compaction_db = Arc::clone(&db);
    let (compaction_tx, compaction_rx) = mpsc::channel();
    let compaction_thread = thread::spawn(move || {
        compaction_tx
            .send(compaction_db.run_maintenance_cycle())
            .expect("compaction result receiver remains live");
    });
    if let Err(error) = pause.wait_until_reached_timeout(SAFETY_TIMEOUT) {
        pause.release();
        let _ = compaction_rx.recv_timeout(SAFETY_TIMEOUT);
        compaction_thread
            .join()
            .expect("compaction thread exits after an unreached pause is released");
        panic!("compaction did not reach its admitted-workspace pause: {error}");
    }
    let active = receipt!(&db);
    let active_accountant_used = accountant.usage().used;
    pause.release();
    let compaction_result = compaction_rx.recv_timeout(SAFETY_TIMEOUT);
    let compaction_finished =
        compaction_result.is_ok() || compaction_rx.recv_timeout(SAFETY_TIMEOUT).is_ok();
    if compaction_finished {
        compaction_thread
            .join()
            .expect("compaction thread exits after workspace publication is released");
    }

    assert!(
        active.temporary_workspace > 0,
        "the live compaction phase has a positive temporary-workspace owner"
    );
    assert_eq!(
        active.accountant_total,
        active.non_vector_total + active.retained_total + active.temporary_workspace,
        "the caller accountant includes live compaction workspace exactly once"
    );
    assert_eq!(
        active_accountant_used, active.accountant_total,
        "the supplied accountant and live category receipt agree while compaction is paused"
    );
    compaction_result
        .expect("released compaction returns before the safety timeout")
        .expect("released compaction publishes successfully");
    assert_exact_reconciliation!(
        &db,
        accountant,
        "published compaction returns its complete temporary workspace"
    );
    db.close().expect("close the compaction-workspace fixture");
    drop(db);
    assert_eq!(
        accountant.usage().used,
        before_open,
        "final workspace fixture drop restores caller usage"
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

/// Normal publication and the crash-retry owner both keep large physical
/// journal keys inside the finite shared budget until durable deletion ends.
#[test]
#[serial]
fn journal_cleanup_charges_large_keys_during_publication_refusal_and_retry() {
    let root = TempDir::new().expect("temporary journal-cleanup store");
    let path = root.path().join("journal-cleanup-ownership.redb");
    let accountant = Arc::new(MemoryAccountant::with_budget(FINITE_CLEANUP_BUDGET));
    let db = Arc::new(open_with_accountant(&path, accountant.clone()));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_table(&db, "cleanup_items");
    let long_key = "journal-cleanup-key-".repeat(1024);
    insert(&db, "cleanup_items", 0x491, &long_key, 0);
    let route =
        contextdb_vector::VectorPartitionRef::new(index("cleanup_items"), partition(&long_key));

    let (publishing, publishing_used, first) =
        run_maintenance_with_journal_cleanup_paused(&db, accountant.as_ref());
    assert_eq!(first.vector.built_partitions, 1);
    assert!(
        publishing.temporary_workspace >= long_key.len(),
        "ordinary publication charges at least the complete large physical journal key"
    );
    assert_eq!(publishing_used, publishing.accountant_total);
    assert_eq!(accountant.usage().limit, Some(FINITE_CLEANUP_BUDGET));
    assert!(publishing_used <= FINITE_CLEANUP_BUDGET);
    assert!(
        db.__debug_vector_partition_journal_file_for_test(&route)
            .physical_record_lsns
            .is_empty(),
        "released publication deletes its covered journal prefix"
    );

    db.execute(
        "UPDATE cleanup_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x491))),
            ("embedding", Value::Vector(vector(1))),
        ]),
    )
    .expect("commit replacement work for the post-publication retry");
    let crash = db.__arm_vector_journal_truncation_crash_for_test(
        &route,
        VectorJournalTruncationPhaseForTest::CatalogDurableBeforeCoveredJournalDelete,
    );
    let interrupted = db
        .run_maintenance_cycle()
        .expect("the partition-local post-catalog fault still closes the maintenance cycle");
    assert_eq!(
        interrupted.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::BuildFailure)
    );
    drop(crash);
    let pending_cleanup = db.__debug_vector_partition_journal_file_for_test(&route);
    assert!(
        !pending_cleanup.physical_record_lsns.is_empty(),
        "the injected boundary leaves a real covered journal record for retry"
    );
    let searchable_before_refusal = indexed_result(&db, "cleanup_items", &long_key, vector(1)).rows;

    let used_before_refusal = accountant.usage().used;
    let refusal_limit = used_before_refusal.saturating_add(long_key.len());
    assert!(refusal_limit < FINITE_CLEANUP_BUDGET);
    db.set_memory_limit(Some(refusal_limit))
        .expect("install a finite limit below the live cleanup ownership");
    let refusal = db
        .run_maintenance_cycle()
        .expect("cleanup refusal remains local while the finite cycle closes");
    assert_eq!(
        refusal.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
    );
    assert!(
        db.maintenance_cycle_stamp_is_set_for_test(),
        "partition-local cleanup refusal still records the closing cycle stamp"
    );
    assert_eq!(
        db.__debug_vector_partition_journal_file_for_test(&route),
        pending_cleanup,
        "typed cleanup refusal preserves the durable journal for an idempotent retry"
    );
    db.set_memory_limit(Some(FINITE_CLEANUP_BUDGET))
        .expect("restore the finite startup ceiling after the typed refusal");
    assert_eq!(
        indexed_result(&db, "cleanup_items", &long_key, vector(1)).rows,
        searchable_before_refusal,
        "cleanup refusal preserves the already-usable indexed route"
    );

    let (retrying, retrying_used, retry) =
        run_maintenance_with_journal_cleanup_paused(&db, accountant.as_ref());
    assert_eq!(retry.vector.first_failure, None);
    assert!(
        retrying.temporary_workspace >= long_key.len(),
        "the shared retry path charges the complete large physical journal key"
    );
    assert_eq!(retrying_used, retrying.accountant_total);
    assert!(retrying_used <= FINITE_CLEANUP_BUDGET);
    assert!(
        db.__debug_vector_partition_journal_file_for_test(&route)
            .physical_record_lsns
            .is_empty(),
        "the admitted retry deletes the interrupted covered prefix"
    );
    assert_exact_reconciliation!(
        &db,
        accountant,
        "publication and retry return every cleanup reservation exactly once"
    );
    db.close().expect("close journal-cleanup retry fixture");
    drop(db);
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

/// Repair retains an old base-plus-change chain while its snapshot sample
/// moves from the collection Vec into the guard's Arc slice. Both sample
/// allocations and every long physical generation key stay inside one finite
/// production workspace reservation.
#[test]
#[serial]
fn repair_pre_admits_snapshot_transition_and_retained_long_generation_keys() {
    const SAMPLE_PINS: usize = 65_536;

    let root = TempDir::new().expect("temporary retained-generation repair store");
    let path = root.path().join("repair-snapshot-key-admission.redb");
    let accountant = Arc::new(MemoryAccountant::with_budget(FINITE_CLEANUP_BUDGET));
    let db = Arc::new(open_with_accountant(&path, accountant.clone()));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE repair_admission_items (\
         id UUID PRIMARY KEY, \
         scope TEXT NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 8 SEARCH_MODE INDEXED \
             CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 1)\
         )",
        &empty(),
    )
    .expect("create the repair-admission fixture");
    let long_key = "repair-retained-generation-key-".repeat(512);
    let route = contextdb_vector::VectorPartitionRef::new(
        index("repair_admission_items"),
        partition(&long_key),
    );
    insert(&db, "repair_admission_items", 0x4A1, &long_key, 0);
    drive_maintenance(&db, "repair_admission_items", &long_key);
    let first_base = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route)
        .and_then(|status| status.base.or(status.dormant_base))
        .expect("the initial durable base generation is selected");

    insert(&db, "repair_admission_items", 0x4A2, &long_key, 1);
    let first_change = (0..MAX_MAINTENANCE_CYCLES)
        .find_map(|_| {
            db.run_maintenance_cycle()
                .expect("publish the bounded change generation");
            db.vector_store_for_test()
                .partition_graph_generation_status(&route)
                .and_then(|status| status.change.or(status.dormant_change))
        })
        .expect("the fixture publishes a durable change generation");
    assert!(
        first_change.generation_id > first_base.generation_id,
        "the retained historical chain contains distinct base and change keys"
    );

    let old_snapshot = db.snapshot();
    let old_pin = db.pin_snapshot(old_snapshot);
    db.execute(
        "UPDATE repair_admission_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x4A1))),
            ("embedding", Value::Vector(vector(2))),
        ]),
    )
    .expect("commit work that replaces the selected base-plus-change chain");
    let replacement_base = (0..MAX_MAINTENANCE_CYCLES)
        .find_map(|_| {
            db.run_maintenance_cycle()
                .expect("publish the replacement base generation");
            db.vector_store_for_test()
                .partition_graph_generation_status(&route)
                .and_then(|status| status.base.or(status.dormant_base))
                .filter(|base| base.generation_id > first_base.generation_id)
        })
        .expect("the newer vector work replaces the historical chain");
    assert!(
        db.vector_store_for_test()
            .make_retired_partition_generations_dormant_for_test(&route)
            > 0,
        "the retained base-plus-change chain becomes a durable dormant owner"
    );
    let retained = db.__debug_vector_partition_generation_retention_for_test(&route);
    assert!(
        retained.durable_superseded_generations >= 2,
        "the held snapshot retains both old durable generation records: {retained:?}"
    );
    assert_eq!(
        retained.resident_superseded_generations, 0,
        "repair must select the historical chain from dormant descriptors"
    );

    assert!(
        db.vector_store_for_test()
            .quarantine_partition_route(&route, VectorRouteQuarantineReason::CorruptBase,),
        "mark the replacement route for production repair"
    );
    let sampled_snapshot = db.snapshot();
    let sample_pins = (0..SAMPLE_PINS)
        .map(|_| db.pin_snapshot(sampled_snapshot))
        .collect::<Vec<_>>();
    let pause = db.__arm_one_shot_vector_repair_snapshot_sample_transition_pause_for_test();
    let repair_db = Arc::clone(&db);
    let (result_tx, result_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        result_tx
            .send(repair_db.run_maintenance_cycle())
            .expect("repair result receiver remains live");
    });
    if !pause.wait_until_reached(SAFETY_TIMEOUT) {
        pause.release();
        let _ = result_rx.recv_timeout(SAFETY_TIMEOUT);
        worker
            .join()
            .expect("repair worker exits after an unreached transition pause");
        panic!("repair did not reach the live snapshot-sample transition");
    }
    let active = receipt!(&db);
    let active_used = accountant.usage().used;
    let sampled_entries = sample_pins.len().saturating_add(1);
    let actual_overlap_payload = sampled_entries
        .saturating_mul(std::mem::size_of::<contextdb_core::SnapshotId>())
        .saturating_mul(2);
    assert!(
        active.temporary_workspace >= actual_overlap_payload,
        "the live workspace charges both the sampled Vec and Arc payloads during their actual overlap"
    );
    assert_eq!(
        active.accountant_total,
        active.non_vector_total + active.retained_total + active.temporary_workspace,
        "the transition reservation is present in the finite shared accountant"
    );
    assert_eq!(active_used, active.accountant_total);
    assert_eq!(accountant.usage().limit, Some(FINITE_CLEANUP_BUDGET));
    assert!(active_used <= FINITE_CLEANUP_BUDGET);
    pause.release();
    let report = result_rx
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("released repair returns before the safety timeout")
        .expect("admitted repair completes its finite maintenance cycle");
    worker
        .join()
        .expect("repair worker exits after transition release");
    assert_eq!(report.vector.first_failure, None);
    assert_eq!(report.vector.built_partitions, 1);
    let repaired_base = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route)
        .and_then(|status| status.base.or(status.dormant_base))
        .expect("repair publishes a selected replacement base");
    assert!(repaired_base.generation_id > replacement_base.generation_id);

    drop(sample_pins);
    let old_result = db
        .execute_at_snapshot(
            "SELECT id FROM repair_admission_items WHERE scope = $scope \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &params([
                ("scope", Value::Text(long_key.clone())),
                ("query", Value::Vector(vector(1))),
            ]),
            old_snapshot,
        )
        .expect("repair preserves indexed reads through the retained old chain");
    let id_column = old_result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("old-snapshot query projects id");
    assert_eq!(old_result.rows.len(), 1);
    assert_eq!(
        old_result.rows[0][id_column],
        Value::Uuid(Uuid::from_u128(0x4A2)),
        "the old snapshot still reads the row supplied by its retained change generation"
    );

    drop(old_pin);
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        let retained = db.__debug_vector_partition_generation_retention_for_test(&route);
        if retained.durable_superseded_generations == 0
            && retained.resident_superseded_generations == 0
        {
            break;
        }
        db.run_maintenance_cycle()
            .expect("released historical generations reclaim in a finite cycle");
    }
    let reclaimed = db.__debug_vector_partition_generation_retention_for_test(&route);
    assert_eq!(reclaimed.durable_superseded_generations, 0);
    assert_eq!(reclaimed.resident_superseded_generations, 0);
    assert_exact_reconciliation!(
        &db,
        accountant,
        "repair returns sample, key, retained-chain, and publication workspace exactly once"
    );
    db.close().expect("close snapshot/key admission fixture");
    drop(db);
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

/// Quarantine repair deliberately ignores a corrupt journal value, but its
/// physical large-key deletion remains a charged live cleanup allocation.
#[test]
#[serial]
fn repaired_journal_cleanup_charges_large_keys_under_a_finite_budget() {
    let root = TempDir::new().expect("temporary repaired-cleanup store");
    let path = root.path().join("repaired-journal-cleanup-ownership.redb");
    let accountant = Arc::new(MemoryAccountant::with_budget(FINITE_CLEANUP_BUDGET));
    let long_key = "repaired-journal-cleanup-key-".repeat(1024);
    let route =
        contextdb_vector::VectorPartitionRef::new(index("repair_items"), partition(&long_key));
    {
        let db = open_with_accountant(&path, accountant.clone());
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_table(&db, "repair_items");
        insert(&db, "repair_items", 0x492, &long_key, 0);
        drive_maintenance(&db, "repair_items", &long_key);
        db.execute(
            "UPDATE repair_items SET embedding = $embedding WHERE id = $id",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0x492))),
                ("embedding", Value::Vector(vector(2))),
            ]),
        )
        .expect("leave one uncovered large-key journal record for corruption");
        db.close()
            .expect("seal the repair fixture before corruption");
    }
    assert_eq!(accountant.usage().used, 0);
    Database::__corrupt_vector_partition_journal_record_for_test(&path, &route)
        .expect("corrupt the uncovered journal envelope while preserving its physical key");

    let db = Arc::new(open_with_accountant(&path, accountant.clone()));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let (repairing, repairing_used, report) =
        run_maintenance_with_journal_cleanup_paused(&db, accountant.as_ref());
    assert_eq!(report.vector.built_partitions, 1);
    assert!(
        repairing.temporary_workspace >= long_key.len(),
        "repair charges at least the complete corrupt record's physical large key"
    );
    assert_eq!(repairing_used, repairing.accountant_total);
    assert_eq!(accountant.usage().limit, Some(FINITE_CLEANUP_BUDGET));
    assert!(repairing_used <= FINITE_CLEANUP_BUDGET);
    indexed(&db, "repair_items", &long_key, vector(2));
    assert!(
        db.__debug_vector_partition_journal_file_for_test(&route)
            .physical_record_lsns
            .is_empty(),
        "repair physically deletes the covered corrupt journal record"
    );
    assert_exact_reconciliation!(
        &db,
        accountant,
        "repair returns decoded, key, retained-chain, and publication cleanup ownership"
    );
    db.close().expect("close repaired journal-cleanup fixture");
    drop(db);
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

/// Durable purge and compaction failures are separate one-shot journeys.  A
/// successful retry must reach the same exact ownership receipt as a fresh
/// database containing only the surviving partition.
#[test]
#[serial]
fn durable_purge_and_compaction_fault_journeys_reconcile_to_a_fresh_control() {
    let root = TempDir::new().expect("temporary durable store");
    let path = root.path().join("purge-compaction-ownership.redb");
    let control_path = root.path().join("purge-compaction-control.redb");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let before_open = accountant.usage().used;
    let db = open_with_accountant(&path, accountant.clone());
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
        accountant,
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
        accountant,
        "successful purge eventually exactly reconciles every eligible owner"
    );

    db.execute(
        "UPDATE purge_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x502))),
            ("embedding", Value::Vector(vector(2))),
        ]),
    )
    .expect("create committed bravo change work before its compaction fault");
    let mut sealed = receipt!(&db);
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if sealed.change_graph > 0 {
            break;
        }
        db.run_maintenance_cycle()
            .expect("seal bravo's first change in one finite maintenance batch");
        sealed = receipt!(&db);
    }
    assert!(
        sealed.change_graph > 0,
        "the durable-fault fixture owns a sealed change before replacement-base compaction"
    );
    db.execute(
        "UPDATE purge_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x502))),
            ("embedding", Value::Vector(vector(0))),
        ]),
    )
    .expect("create later tail work for replacement-base compaction");

    let before_compaction_fault = receipt!(&db);
    // The compaction fault has its own prepublication arm. Verify that failure
    // returns admitted workspace and leaves this handle usable for retry.
    db.arm_vector_durable_compaction_failure_for_test();
    let refused = db
        .run_maintenance_cycle()
        .expect("a partition-local compaction fault still closes the maintenance wake");
    assert_eq!(
        refused.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::BuildFailure),
        "the compaction-only durable fault is visible in the completed report"
    );
    assert!(
        db.vector_durable_compaction_failure_consumed_for_test(),
        "the compaction-only durable fault is consumed exactly once"
    );
    assert_same_receipt!(
        receipt!(&db),
        before_compaction_fault,
        "failed durable compaction returns workspace and preserves every owner"
    );
    assert_exact_reconciliation!(
        &db,
        accountant,
        "failed durable compaction exactly reconciles every owner without underflow"
    );
    let mut compacted = false;
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("retry one finite vector-compaction maintenance batch");
        if receipt!(&db).change_graph == 0 {
            compacted = true;
            break;
        }
    }
    assert!(
        compacted,
        "the successful retry publishes a replacement base within the finite maintenance bound"
    );
    assert_exact_reconciliation!(
        &db,
        accountant,
        "successful purge and compaction eventually exactly reconcile every owner"
    );
    db.close().expect("close durable purge/compaction journey");
    drop(db);
    assert_eq!(
        accountant.usage().used,
        before_open,
        "closing the repaired purge/compaction database returns every owned charge before reopen"
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);

    let control_accountant = Arc::new(MemoryAccountant::no_limit());
    let control = open_with_accountant(&control_path, control_accountant.clone());
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
    let control = open_with_accountant(&control_path, control_accountant.clone());
    let control_receipt = receipt!(&control);
    assert_exact_reconciliation!(
        &control,
        control_accountant,
        "fresh surviving-state control exactly reconciles every owner"
    );
    control
        .close()
        .expect("close fresh surviving-state control reopen");
    drop(control);
    assert_eq!(
        control_accountant.usage().used,
        0,
        "final purge/compaction control handle drop returns every charge"
    );
    assert_eq!(control_accountant.underflow_count_for_test(), 0);

    let reopened = open_with_accountant(&path, accountant.clone());
    assert!(
        reopened.__physical_version_count_for_test("purge_items") > control_physical_versions,
        "the reopened HISTORY ALL store still physically holds bravo's superseded history"
    );
    assert_eq!(
        receipt!(&reopened).non_vector_total,
        control_receipt.non_vector_total,
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
        reopened
            .__compact_currency_versions_for_tables_for_test(&["purge_items"])
            .expect("prune the retained history through the production door");
    }
    assert_eq!(
        reopened.__physical_version_count_for_test("purge_items"),
        control_physical_versions,
        "compaction brings the reopened store to the control's physical version set"
    );
    assert_same_receipt!(
        receipt!(&reopened),
        control_receipt,
        "once the retained history is physically pruned, the reopened store reconciles \
         to the fresh surviving-state control"
    );
    assert_exact_reconciliation!(
        &reopened,
        accountant,
        "durable purge/compaction final reopen exactly reconciles every owner"
    );
    reopened.close().expect("close durable verification reopen");
    drop(reopened);
    assert_eq!(
        accountant.usage().used,
        before_open,
        "final purge/compaction handle drop restores caller usage"
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

/// Renaming is identity-preserving; legal drops and final handle destruction
/// release every vector category once and never resurrect an old route.
#[test]
#[serial]
fn rename_drop_and_database_drop_release_vector_owners_once_without_old_identity_on_restart() {
    let root = TempDir::new().expect("temporary store");
    let path = root.path().join("rename-drop.redb");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let before_open = accountant.usage().used;
    let db = open_with_accountant(&path, accountant.clone());
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
        accountant,
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
        accountant,
        "the independent table-drop control exactly reconciles every owner"
    );
    db.close().expect("close after vector-column drop");
    drop(db);

    let reopened = open_with_accountant(&path, accountant.clone());
    assert_eq!(
        receipt!(&reopened).retained_total,
        0,
        "restart exposes no dropped vector identity"
    );
    assert_exact_reconciliation!(
        &reopened,
        accountant,
        "the final no-vector restart exactly reconciles categories and caller accountant"
    );
    reopened.close().expect("close the no-vector restart");
    drop(reopened);
    assert_eq!(
        accountant.usage().used,
        before_open,
        "dropping the final database handle restores caller usage"
    );
    assert_eq!(
        accountant.underflow_count_for_test(),
        0,
        "all drop paths release exactly once"
    );
}
