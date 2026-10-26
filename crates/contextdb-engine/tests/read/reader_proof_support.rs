#![allow(clippy::assertions_on_constants)] // Existing compatibility and test seam shape.
//! Shared production-path fixtures for the reader repair proofs.
use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, ReadLimits, ReadRoute};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{Database, MaintenancePolicy, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub const ROWS: usize = 10_000;
#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub const CONTEXTS: usize = 8;
#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub const DIM: usize = 768;
#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub const TEXT: usize = 512;
pub const SESSION_BYTES: usize = 24 * 1024 * 1024;
#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub const ROWS_PER_CONTEXT: usize = ROWS / CONTEXTS;

#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn runtime(root: &Path) -> PathBuf {
    let dir = root.join("reader-runtime");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

pub fn limits() -> ReadLimits {
    ReadLimits {
        result_rows: 10_000,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 10_000,
        memory: SESSION_BYTES as u64,
        cursor_page_rows: 2,
        cursor_page_bytes: 2 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 20_000,
    }
}

#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn options() -> ReadSessionOptions {
    ReadSessionOptions {
        memory_limit: Some(SESSION_BYTES),
        limits: limits(),
        ..Default::default()
    }
}

#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn row_id(row: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0x4a11_0000_0000_0000_0000_0000_0000_0000 + row as u128)
}
#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn context_id(context: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0xc017_0000_0000_0000_0000_0000_0000_0000 + context as u128)
}
#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn ordinal(id: uuid::Uuid) -> usize {
    usize::try_from(id.as_u128().checked_sub(row_id(0).as_u128()).unwrap()).unwrap()
}
#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn all_context_params() -> HashMap<String, Value> {
    let mut params = HashMap::from([("q".into(), Value::Vector(vector(0)))]);
    for context in 0..CONTEXTS {
        params.insert(format!("c{context}"), Value::Uuid(context_id(context)));
    }
    params
}

#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn payload(row: usize) -> String {
    format!("{row:08x}").repeat(TEXT / 8)
}
#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn vector(row: usize) -> Vec<f32> {
    // Fixed finite unit vectors with nontrivial neighbours; no runtime threshold override.
    let mut state = row as u64 + 0x9e37_79b9;
    let mut v: Vec<f32> = (0..DIM)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state as u32 as f64 / u32::MAX as f64 - 0.5) as f32
        })
        .collect();
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    for x in &mut v {
        *x /= norm;
    }
    v
}

#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn seed_working_set(path: &Path) {
    assert!(
        ROWS * (DIM * 4 + TEXT) > SESSION_BYTES,
        "the frozen logical payload alone must exceed the reader ceiling"
    );
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE reader_work (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, payload TEXT NOT NULL, embedding VECTOR(768) PARTITION_KEY (context_id) MAX_PARTITIONS 8)", &HashMap::new()).unwrap();
    db.execute(
        "CREATE INDEX reader_context ON reader_work (context_id, id)",
        &HashMap::new(),
    )
    .unwrap();
    for start in (0..ROWS).step_by(1000) {
        let tx = db.begin().unwrap();
        for row in start..start + 1000 {
            db.execute_in_tx(
                tx,
                "INSERT INTO reader_work VALUES ($id, $context, $payload, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Uuid(row_id(row))),
                    (
                        "context".into(),
                        Value::Uuid(context_id(row / ROWS_PER_CONTEXT)),
                    ),
                    ("payload".into(), Value::Text(payload(row))),
                    ("embedding".into(), Value::Vector(vector(row))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
    }
    finish_maintenance(&db, CONTEXTS);
    db.close().unwrap();
}

#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn finish_maintenance(db: &Database, partitions: usize) {
    for _ in 0..64 {
        db.run_maintenance_cycle()
            .expect("drive actual caller-owned generation build/reclaim");
        let s = db.vector_store_for_test().hnsw_maintenance_status();
        if s.ready_partitions == partitions && s.remaining_partitions == 0 {
            return;
        }
    }
    panic!("maintenance did not publish the fixture's {partitions} generations");
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by read, read_cache_share_option"
)]
#[derive(Default)]
pub struct Routes(pub Mutex<Vec<ReadSessionEvent>>);
impl ReadSessionTestObserver for Routes {
    fn observe_event(&self, e: ReadSessionEvent) {
        self.0.lock().unwrap().push(e);
    }
}
impl Routes {
    #[allow(
        dead_code,
        reason = "KEEP-SHARED: shared test helper used by read, read_cache_share_option"
    )]
    pub fn file_only(&self) {
        let events = self.0.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, ReadSessionEvent::RouteSelected(ReadRoute::File)))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, ReadSessionEvent::DirectBackendOpen))
                .count(),
            1
        );
        assert!(
            !events.iter().any(|e| matches!(
                e,
                ReadSessionEvent::LocalChannelOperation
                    | ReadSessionEvent::OwnershipRaceRetry { .. }
                    | ReadSessionEvent::RouteSelected(ReadRoute::Owner)
            )),
            "file session attached/retried: {events:?}"
        );
    }
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by read, read_cache_share_option"
)]
pub fn open_file(path: &Path, opts: ReadSessionOptions, routes: Arc<Routes>) -> ReadSession {
    let clock: Arc<dyn DeadlineClock> = Arc::new(ManualDeadlineClock::default());
    let s = ReadSession::open_with_observations_for_test(path, opts, clock, Some(routes), None)
        .expect("real file reader opens under its declared ceiling");
    assert_eq!(s.route(), ReadRoute::File);
    s
}

/// Open proves its history frontiers and checks the copied startup settings
/// against the ordered digest in the canonical Configuration root. These two
/// fixtures have distinct history depths, so pin their complete work signatures.
#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
pub fn assert_open_proves_history_and_startup_digest(
    open: contextdb_engine::direct_file_reader::test_seams::ReadWorkCounters,
    expected_ordered_nodes: u64,
    expected_hashed_bytes: u64,
) {
    assert_eq!(open.proof_nodes_read, expected_ordered_nodes, "{open:?}");
    assert_eq!(open.proof_nodes_hashed, expected_ordered_nodes, "{open:?}");
    assert_eq!(
        open.ordered_proof_nodes_read, expected_ordered_nodes,
        "{open:?}"
    );
    assert_eq!(
        contextdb_engine::direct_file_reader::test_seams::ReadWorkCounters {
            hashed_bytes: 0,
            proof_nodes_read: 0,
            proof_nodes_hashed: 0,
            ordered_proof_nodes_read: 0,
            ..open
        },
        contextdb_engine::direct_file_reader::test_seams::ReadWorkCounters::default(),
        "open does no row, vector or decode work: {open:?}"
    );
    assert_eq!(open.hashed_bytes, expected_hashed_bytes, "{open:?}");
}
