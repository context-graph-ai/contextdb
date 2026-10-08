//! An admitted one-table migration must not copy unrelated schema and provenance history.
//! The global allocator measures the actual live-heap peak around each received unit, after
//! the fixtures and signed wire inputs have already been built.

use contextdb_core::{Error, Incarnation, TenantId};
use contextdb_engine::database::ReceivedSchemaApplyObserverForTest;
use contextdb_engine::memory_accounting::MemoryAccountant;
use contextdb_engine::plugin::CorePlugin;
use contextdb_engine::protocol::WireDdlProvenance;
use contextdb_engine::sync_types::ChangeSet;
use contextdb_engine::{Database, MaintenancePolicy};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);
struct Meter;

fn grow(bytes: isize) {
    let live = LIVE.fetch_add(bytes, Ordering::SeqCst) + bytes;
    PEAK.fetch_max(live, Ordering::SeqCst);
}

unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grow(layout.size() as isize);
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grow(layout.size() as isize);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::SeqCst);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(ptr, layout, size) };
        if !next.is_null() {
            grow(size as isize - layout.size() as isize);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: Meter = Meter;

fn measured<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    let before = LIVE.load(Ordering::SeqCst);
    PEAK.store(before, Ordering::SeqCst);
    let result = operation();
    let growth = (PEAK.load(Ordering::SeqCst) - before).max(0) as usize;
    (result, growth)
}

type SignedUnit = (ChangeSet, Vec<WireDdlProvenance>);
const INCARNATION: Incarnation = Incarnation(311);
const SOURCE: &str = "metadata-peak-author";

fn author_unit(
    author: &Database,
    sql: &str,
    tenant: &TenantId,
    identity: &contextdb_engine::identity::FabricIdentity,
) -> SignedUnit {
    let before = author.current_lsn();
    author.execute(sql, &HashMap::new()).expect("author DDL");
    let changes = author
        .changes_since(before)
        .expect("read the declared changes");
    let provenance = {
        let seam_read_memory = author
            .declared_reader_memory()
            .expect("admit the fixture operation reader");
        author.outbound_ddl_provenance_for_test(
            &changes,
            tenant,
            &identity.node_id(),
            INCARNATION,
            &|bytes| Ok(identity.sign_for_test(bytes)),
            seam_read_memory,
        )
    }
    .expect("sign exact authored unit");
    (changes, provenance)
}

fn receive(receiver: &Database, unit: SignedUnit, tenant: &TenantId) -> contextdb_core::Result<()> {
    {
        let seam_read_memory = receiver
            .declared_reader_memory()
            .expect("admit the fixture operation reader");
        let mut seam_copy_owner =
            contextdb_engine::vector_read_memory::admission(seam_read_memory.clone());
        receiver.apply_received_schema_for_test(
            unit.0,
            unit.1,
            tenant.clone(),
            SOURCE,
            INCARNATION,
            &mut seam_copy_owner,
            seam_read_memory,
        )
    }
    .map(|_| ())
}

#[test]
fn one_table_migration_peak_ignores_unrelated_catalog_and_provenance() {
    const UNRELATED: usize = 96;
    // The fixed slack permits small bookkeeping variation from the second target ALTER.
    // A full clone of 96 wide TableMeta values is much larger than this.
    const PEAK_SLACK: usize = 96 * 1024;
    let root = tempfile::tempdir().expect("task-scoped stores");
    let tenant = TenantId::from("metadata-peak-tenant");
    let identity = contextdb_engine::identity::FabricIdentity::generate();
    let author = Database::open_memory();
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(
            "CREATE TABLE target (id INTEGER PRIMARY KEY) SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("author target");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        root.path().join("receiver.db"),
        Arc::new(CorePlugin),
        accountant.clone(),
    )
    .expect("file-backed receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(
            "CREATE TABLE target (id INTEGER PRIMARY KEY) SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("receiver target");

    let first = author_unit(
        &author,
        "ALTER TABLE target ADD COLUMN first TEXT",
        &tenant,
        &identity,
    );
    let (first_result, small_peak) = measured(|| receive(&receiver, first, &tenant));
    first_result.expect("first target migration");

    // Each unrelated declaration arrives through the authenticated received door. This grows
    // the table catalog, DDL history, generation map and received-author provenance together.
    for ordinal in 0..UNRELATED {
        let columns = (0..12)
            .map(|column| format!("history_column_{column:02}_with_a_long_name TEXT"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "CREATE TABLE unrelated_{ordinal:03} (id INTEGER PRIMARY KEY, {columns}) SYNC TWO WAY"
        );
        let unit = author_unit(&author, &sql, &tenant, &identity);
        receive(&receiver, unit, &tenant).expect("receive unrelated signed history");
    }

    let second = author_unit(
        &author,
        "ALTER TABLE target ADD COLUMN second TEXT",
        &tenant,
        &identity,
    );
    let replay = second.clone();
    let (second_result, large_peak) = measured(|| receive(&receiver, second, &tenant));
    second_result.expect("second one-table migration");
    assert!(
        large_peak <= small_peak + PEAK_SLACK,
        "one-table received migration copied unrelated metadata: small peak {small_peak} B, \
         peak after {UNRELATED} unrelated signed declarations {large_peak} B"
    );

    let trace = ReceivedSchemaApplyObserverForTest::new();
    let scope = receiver.observe_received_schema_applies_for_test(&trace);
    let (replay_result, replay_peak) = measured(|| receive(&receiver, replay.clone(), &tenant));
    replay_result.expect("identical authenticated replay");
    drop(scope);
    let observation = trace.storage_observation();
    assert_eq!(
        observation.staged_table_images_copied, 0,
        "replay must skip the working copy"
    );
    assert_eq!(
        observation.durable_table_rewrites, 0,
        "replay must not rewrite a table"
    );
    assert!(
        replay_peak <= small_peak + PEAK_SLACK,
        "identical replay copied unrelated metadata: {replay_peak} B versus {small_peak} B"
    );

    let third = author_unit(
        &author,
        "ALTER TABLE target ADD COLUMN third TEXT",
        &tenant,
        &identity,
    );
    let before_charge = accountant.usage().used;
    receiver
        .set_memory_limit(Some(before_charge + 1))
        .expect("tight declared limit");
    let (refusal, refusal_peak) = measured(|| receive(&receiver, third, &tenant));
    assert!(
        matches!(&refusal, Err(Error::MemoryBudgetExceeded { .. })),
        "tight migration must return a typed memory refusal: {refusal:?}"
    );
    assert_eq!(
        accountant.usage().used,
        before_charge,
        "refusal must release every temporary charge"
    );
    assert!(
        refusal_peak <= small_peak + PEAK_SLACK,
        "tight refusal allocated unrelated metadata before admission: {refusal_peak} B versus {small_peak} B"
    );
    assert!(
        receiver
            .table_meta("target")
            .unwrap()
            .columns
            .iter()
            .all(|column| column.name != "third"),
        "refused unit must not publish a schema prefix"
    );
    eprintln!(
        "received schema metadata peaks: small={small_peak} B, large={large_peak} B, replay={replay_peak} B, refusal={refusal_peak} B"
    );
    receiver.close().expect("close receiver");
}
