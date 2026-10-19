//! A hub serves a pull one page at a time: selection stops at the reader's
//! declared page budget, at a source-LSN group boundary, so its memory does not
//! grow with the history behind the cursor.

use super::*;
use crate::protocol::{DependencyCompletePullResponse, PullRequest, PullResponse, decode, encode};
use crate::subjects::pull_subject;
use crate::transport::in_process::InProcessBroker;
use crate::vector_read_memory::DeclaredReadMemory;
use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use std::time::Duration;

const TENANT: &str = "pull-page-cut";
const TABLE: &str = "embeddings";
const DIM: usize = 768;

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(600), future)
        .await
        .expect("bounded in-process exchange")
}

/// Funds every hub request from its own fresh declared reader at the shipped
/// default limit, and keeps each one, so a request's own peak is observable.
#[derive(Debug)]
struct PerRequestReader {
    parents: std::sync::Mutex<Vec<Arc<DeclaredReadMemory>>>,
}
impl PerRequestReader {
    fn next(&self) -> Arc<DeclaredReadMemory> {
        let parent = DeclaredReadMemory::from_reader_limit(ReadLimits::SHIPPED_MEMORY);
        self.parents.lock().unwrap().push(parent.clone());
        parent
    }
}
impl crate::vector_read_memory::ReadRequestBudgetProvider for PerRequestReader {
    fn request_budget(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        self.next().child()
    }
}

/// A hub store holding `rows` rows of one 768-wide vector each, written in
/// transactions of `per_tx` rows.
fn hub_store(dir: &std::path::Path, rows: usize, per_tx: usize) -> Arc<Database> {
    // The writer budget that funds a pull also funds the engine's background
    // maintenance (a vector build over these rows); the caller drives
    // maintenance here so a request's peak is the pull's own.
    let options = crate::DatabaseOpenOptions {
        maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
        ..Default::default()
    };
    let db = Arc::new(Database::open_with_options(dir.join("hub.db"), options).expect("open hub"));
    db.execute(
        &format!(
            "CREATE TABLE {TABLE} (id UUID PRIMARY KEY, v VECTOR({DIM})) SYNC TWO WAY SYNC CONFLICT KEEP FIRST"
        ),
        &HashMap::new(),
    )
    .expect("table");
    let mut written = 0;
    while written < rows {
        let tx = db.begin().expect("begin");
        for i in written..(written + per_tx).min(rows) {
            db.insert_row(
                tx,
                TABLE,
                HashMap::from([
                    (
                        "id".to_string(),
                        Value::Uuid(uuid::Uuid::from_u128(i as u128 + 1)),
                    ),
                    ("v".to_string(), Value::Vector(vec![i as f32; DIM])),
                ]),
            )
            .expect("row");
        }
        db.commit(tx).expect("commit");
        written = (written + per_tx).min(rows);
    }
    db
}

struct Pulled {
    rows: usize,
    page_rows: Vec<usize>,
    vectors: usize,
    pages: usize,
    peaks: Vec<u64>,
    cursors: Vec<Lsn>,
}

/// Pull everything from a hub over `db` with default pages, as a fresh edge
/// does, recording each request's own peak charge on the hub's writer budget,
/// which funds serving a pull.
async fn pull_all(db: Arc<Database>) -> contextdb_core::Result<Pulled> {
    let hub = db.clone();
    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(crate::identity::FabricIdentity::generate());
    let hub_node = hub_identity.node_id();
    let provider = Arc::new(PerRequestReader {
        parents: std::sync::Mutex::new(Vec::new()),
    });
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test_with_read_budget_provider(
            db,
            broker.server_as(&hub_node),
            TenantId::from(TENANT),
            hub_node,
            hub_identity,
            provider.clone(),
        )
        .expect("hub"),
    );
    let client = broker.client_as(&crate::identity::FabricIdentity::generate().node_id());
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    within(broker.wait_for_registered_route_for_test(&pull_subject(TENANT))).await;
    let mut pulled = Pulled {
        rows: 0,
        page_rows: Vec::new(),
        vectors: 0,
        pages: 0,
        peaks: Vec::new(),
        cursors: Vec::new(),
    };
    let mut since = Lsn(0);
    let outcome = loop {
        hub.writer_accountant_for_test().reset_high_water_for_test();
        let held_before = hub.writer_accountant_for_test().usage().used;
        let bytes = encode(
            MessageType::PullRequest,
            &PullRequest {
                since_lsn: since,
                max_entries: None,
                schema_recovery: None,
            },
        )
        .expect("encode pull");
        let reply =
            match within(client.request(&pull_subject(TENANT), bytes, Duration::from_secs(300)))
                .await
            {
                Ok(reply) => reply,
                Err(error) => break Err(contextdb_core::Error::SyncError(error.to_string().into())),
            };
        let envelope = decode(&reply).expect("decode reply");
        let response = match envelope.message_type {
            MessageType::PullResponse => {
                rmp_serde::from_slice::<PullResponse>(&envelope.payload).expect("pull response")
            }
            MessageType::DependencyCompletePullResponse => {
                rmp_serde::from_slice::<DependencyCompletePullResponse>(&envelope.payload)
                    .expect("dependency-complete response")
                    .ordinary
            }
            other => panic!("unexpected pull reply {other:?}"),
        };
        pulled.pages += 1;
        let page_rows = response
            .changeset
            .rows
            .iter()
            .filter(|row| row.table == TABLE)
            .count();
        pulled.rows += page_rows;
        pulled.page_rows.push(page_rows);
        pulled.vectors += response.changeset.vectors.len();
        let peak = hub
            .writer_accountant_for_test()
            .high_water_for_test()
            .saturating_sub(held_before) as u64;
        pulled.peaks.push(peak);
        if let Some(cursor) = response.cursor {
            pulled.cursors.push(cursor);
            since = cursor;
        }
        if !response.has_more {
            break Ok(());
        }
        assert!(pulled.pages < 10_000, "the pull makes progress");
    };
    shutdown.store(true, Ordering::SeqCst);
    within(task).await.expect("hub stops");
    outcome.map(|()| pulled)
}

// Bounded intent :29 — sync stays bounded as history grows. A fresh edge pulls
// a 10,000-row, 768-wide vector table (about 30 MB, written in 100-row
// transactions) to completion in pages, every row exactly once. Serving the
// pull is the hub's own sync work, funded by its writer budget; each request's
// peak charge there stays under the shipped 16 MiB reader ceiling and does not
// grow when the table doubles.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_two_way_table_pulls_in_pages_with_a_flat_peak() {
    let mut lines = Vec::new();
    let mut peaks = Vec::new();
    for rows in [5_000usize, 10_000] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = hub_store(dir.path(), rows, 100);
        let pulled = pull_all(db).await;
        let pulled = match pulled {
            Ok(pulled) => pulled,
            Err(error) => {
                panic!("{rows} rows: the pull completes under the default ceiling: {error:?}")
            }
        };
        let peak = pulled.peaks.iter().copied().max().unwrap_or(0);
        lines.push(format!(
            "rows={rows} pages={} rows_pulled={} vectors={} peak={peak}",
            pulled.pages, pulled.rows, pulled.vectors
        ));
        assert_eq!(pulled.rows, rows, "every row arrives exactly once");
        assert!(
            pulled.cursors.windows(2).all(|pair| pair[0] < pair[1]),
            "each page advances the cursor"
        );
        assert!(pulled.pages > 1, "the table is served in pages");
        peaks.push(peak);
    }
    println!("S4_PULL_PAGES {}", lines.join(" | "));
    let (small, large) = (peaks[0], peaks[1]);
    assert!(
        large <= small + small / 4,
        "the per-request peak does not grow with the table: {small} -> {large}"
    );
    assert!(large <= ReadLimits::SHIPPED_MEMORY);
}

// Bounded intent :29 on pruned history. When the change log no longer covers
// the cursor, the hub serves from persisted state; that pull too completes in
// pages, every row once, with a flat per-request peak on the hub's writer budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pull_served_from_persisted_state_pages_with_a_flat_peak() {
    let mut lines = Vec::new();
    let mut peaks = Vec::new();
    for rows in [2_500usize, 5_000] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = hub_store(dir.path(), rows, 100);
        db.__force_change_log_gap_for_test();
        let pulled = match pull_all(db).await {
            Ok(pulled) => pulled,
            Err(error) => panic!("{rows} rows: the persisted-state pull completes: {error:?}"),
        };
        let peak = pulled.peaks.iter().copied().max().unwrap_or(0);
        lines.push(format!(
            "rows={rows} pages={} rows_pulled={} peak={peak}",
            pulled.pages, pulled.rows
        ));
        assert_eq!(pulled.rows, rows, "every row arrives exactly once");
        assert!(pulled.pages > 1, "persisted state is served in pages");
        peaks.push(peak);
    }
    println!("S4_PULL_FALLBACK_PAGES {}", lines.join(" | "));
    assert!(
        peaks[1] <= peaks[0] + peaks[0] / 4,
        "the per-request peak does not grow with the table: {} -> {}",
        peaks[0],
        peaks[1]
    );
}

// Root intent :464-474: an authored transaction is never split. One 200-row
// transaction, twice the size of the others and larger than a page, arrives
// whole on its own page; the transactions after it follow on later pages.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_larger_than_a_page_arrives_whole() {
    let dir = tempfile::tempdir().expect("store directory");
    let db = hub_store(dir.path(), 200, 200);
    let tx_rows = 10;
    {
        let tx = db.begin().expect("begin");
        for i in 200..200 + tx_rows {
            db.insert_row(
                tx,
                TABLE,
                HashMap::from([
                    (
                        "id".to_string(),
                        Value::Uuid(uuid::Uuid::from_u128(i as u128 + 1)),
                    ),
                    ("v".to_string(), Value::Vector(vec![i as f32; DIM])),
                ]),
            )
            .expect("row");
        }
        db.commit(tx).expect("commit");
    }
    let pulled = pull_all(db).await.expect("the pull completes");
    println!(
        "S4_PULL_OVERSIZED pages={} page_rows={:?} peaks={:?}",
        pulled.pages, pulled.page_rows, pulled.peaks
    );
    assert_eq!(pulled.rows, 200 + tx_rows);
    assert!(
        pulled.page_rows.contains(&200),
        "the 200-row transaction arrives on one page: {:?}",
        pulled.page_rows
    );
}
