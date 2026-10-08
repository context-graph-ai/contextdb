use contextdb_core::{Error, TenantId, Value};
use contextdb_engine::memory_accounting::MemoryAccountant;
use contextdb_engine::plugin::CorePlugin;
use contextdb_engine::{Database, MaintenancePolicy};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);
static PEAK_BYTES: AtomicIsize = AtomicIsize::new(0);

struct CountingAllocator;

fn note_growth(delta: isize) {
    let live = LIVE_BYTES.fetch_add(delta, Ordering::Relaxed) + delta;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            note_growth(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            note_growth(layout.size() as isize);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(ptr, layout, new_size) };
        if !next.is_null() {
            note_growth(new_size as isize - layout.size() as isize);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn a_file_backed_received_migration_refuses_an_oversized_row_before_decoding_it() {
    const ROW_BYTES: usize = 12 * 1024 * 1024;
    const HEADROOM: usize = 1024 * 1024;
    const DDL: &str = "CREATE TABLE wide (id UUID PRIMARY KEY, body TEXT NOT NULL) SYNC TWO WAY";
    const TENANT: &str = "predecode-row-tenant";
    const SOURCE: &str = "predecode-row-author";
    const INCARNATION: contextdb_core::Incarnation = contextdb_core::Incarnation(23);

    let root = tempfile::tempdir().expect("fixture directory");
    let tenant = TenantId::from(TENANT);
    let identity = contextdb_engine::identity::FabricIdentity::generate();

    let author = Database::open(root.path().join("author.db")).expect("open author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(DDL, &HashMap::new())
        .expect("declare source table");
    let before_unit = author.current_lsn();
    author
        .execute("ALTER TABLE wide ADD COLUMN extra TEXT", &HashMap::new())
        .expect("author migration");
    let unit = author
        .changes_since(before_unit)
        .expect("read or construct the declared fixture result");
    let fixture_operation_reader_1 = author
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &tenant,
            &identity.node_id(),
            INCARNATION,
            &|bytes| Ok(identity.sign_for_test(bytes)),
            fixture_operation_reader_1.clone(),
        )
        .expect("sign migration");
    author.close().expect("close author");

    let receiver_path = root.path().join("receiver.db");
    let receiver = Database::open(&receiver_path).expect("open receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(DDL, &HashMap::new())
        .expect("declare target table");
    receiver
        .execute(
            "INSERT INTO wide (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(uuid::Uuid::from_u128(1))),
                ("body".to_owned(), Value::Text("x".repeat(ROW_BYTES))),
            ]),
        )
        .expect("persist one oversized row");
    receiver.close().expect("close filled receiver");

    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver =
        Database::open_with_config(receiver_path, Arc::new(CorePlugin), accountant.clone())
            .expect("reopen file-backed receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let before_charge = accountant.usage().used;
    let limit = before_charge + HEADROOM;
    assert!(ROW_BYTES > limit - before_charge);
    receiver
        .set_memory_limit(Some(limit))
        .expect("declare a limit smaller than one decoded row");
    assert_eq!(accountant.usage().used, before_charge);

    let before_heap = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(before_heap, Ordering::Relaxed);
    let fixture_operation_reader_2 = receiver
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_2 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_2.clone());

    let error = receiver
        .apply_received_schema_for_test(
            unit,
            provenance,
            tenant,
            SOURCE,
            INCARNATION,
            &mut fixture_operation_copy_2,
            fixture_operation_reader_2.clone(),
        )
        .expect_err("the affected row exceeds the remaining memory limit");
    let peak_growth = PEAK_BYTES.load(Ordering::Relaxed) - before_heap;
    assert!(
        matches!(&error, Error::MemoryBudgetExceeded { .. }),
        "received migration must return a typed memory refusal: {error:?}"
    );
    assert!(
        matches!(&error, Error::MemoryBudgetExceeded { operation, .. } if operation == "stage_received_unit_row_image"),
        "the refusal must occur at the affected row, after staging reaches its file-backed copy: {error:?}"
    );
    assert_eq!(
        accountant.usage().used,
        before_charge,
        "a refused migration must return every temporary charge"
    );
    assert!(
        peak_growth < (ROW_BYTES / 2) as isize,
        "the allocator grew by {peak_growth} bytes before refusing a {ROW_BYTES}-byte row"
    );
    assert!(
        receiver
            .table_meta("wide")
            .expect("table survives refusal")
            .columns
            .iter()
            .all(|c| c.name != "extra"),
        "a refused migration must leave the existing schema intact"
    );
    receiver.close().expect("close receiver");
}
