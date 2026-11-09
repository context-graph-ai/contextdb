//! Pull limits count work the receiver can actually see, while the cursor
//! consumes hub-local contact history without making an empty progress page.

use contextdb_core::{Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::protocol::{
    DependencyCompletePullResponse, MessageType, PullRequest, PullResponse, WireChangeSet,
    WireDdlChange, decode, encode,
};
use contextdb_server::subjects::pull_subject;
use contextdb_server::transport::ClientTransport;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const TENANT: &str = "pull-filter-contact-pagination";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

struct RunningHub {
    db: Arc<Database>,
    client: Arc<dyn ClientTransport>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn start() -> Self {
        let db = Arc::new(Database::open_memory());
        let broker = InProcessBroker::new();
        let hub_identity = Arc::new(FabricIdentity::generate());
        let hub_node = hub_identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                db.clone(),
                broker.server_as(&hub_node),
                TenantId::from(TENANT),
                hub_node,
                hub_identity,
            )
            .expect("read or construct the declared fixture result"),
        );
        let edge_node = FabricIdentity::generate().node_id();
        let client = broker.client_as(&edge_node);
        let shutdown = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let shutdown = shutdown.clone();
            async move { server.run_until(shutdown).await }
        });
        broker
            .wait_for_registered_route_for_test(&pull_subject(TENANT))
            .await;
        Self {
            db,
            client,
            shutdown,
            task,
        }
    }

    async fn pull(&self, since: Lsn) -> Page {
        let bytes = encode(
            MessageType::PullRequest,
            &PullRequest {
                since_lsn: since,
                max_entries: Some(1),
                schema_recovery: None,
            },
        )
        .unwrap();
        let reply = self
            .client
            .request(&pull_subject(TENANT), bytes, Duration::from_secs(10))
            .await
            .expect("authenticated pull receives a reply");
        let envelope = decode(&reply).expect("decode server reply");
        let (response, units) = match envelope.message_type {
            MessageType::PullResponse => (
                rmp_serde::from_slice::<PullResponse>(&envelope.payload).unwrap(),
                Vec::new(),
            ),
            MessageType::DependencyCompletePullResponse => {
                let response =
                    rmp_serde::from_slice::<DependencyCompletePullResponse>(&envelope.payload)
                        .unwrap();
                (response.ordinary, response.units)
            }
            other => panic!("unexpected pull response: {other:?}"),
        };
        let mut page = Page {
            cursor: response.cursor,
            has_more: response.has_more,
            ddl: Vec::new(),
            ddl_lsn: Vec::new(),
            row_lsn: Vec::new(),
        };
        page.add(response.changeset);
        for unit in units {
            page.add(unit);
        }
        page
    }

    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.await.expect("hub stops");
    }
}

#[derive(Debug)]
struct Page {
    cursor: Option<Lsn>,
    has_more: bool,
    ddl: Vec<String>,
    ddl_lsn: Vec<Lsn>,
    row_lsn: Vec<Lsn>,
}

impl Page {
    fn add(&mut self, changes: WireChangeSet) {
        assert!(changes.edges.is_empty() && changes.vectors.is_empty());
        assert!(changes.purges.is_empty());
        self.ddl_lsn.extend(changes.ddl_lsn);
        self.ddl
            .extend(changes.ddl.into_iter().map(|ddl| match ddl {
                WireDdlChange::CreateTable { name, .. } => name,
                other => panic!("unexpected DDL in fixture: {other:?}"),
            }));
        self.row_lsn.extend(changes.rows.into_iter().map(|row| {
            assert_eq!(row.table, "page_notes", "hub contacts never cross the wire");
            row.lsn
        }));
    }

    fn is_empty(&self) -> bool {
        self.ddl.is_empty() && self.row_lsn.is_empty()
    }
}

#[tokio::test]
async fn first_bounded_page_skips_hub_contacts_and_delivers_whole_source_groups_in_order() {
    let hub = RunningHub::start().await;
    // The first authenticated request creates and writes the hub's own contact
    // table before application work is authored.
    hub.pull(Lsn(0)).await;
    assert_eq!(hub.db.table_names(), vec!["work_node_contacts"]);

    let schema_tx = hub.db.begin().unwrap();
    hub.db
        .execute_in_tx(
            schema_tx,
            "CREATE TABLE page_notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &empty(),
        )
        .unwrap();
    hub.db
        .execute_in_tx(
            schema_tx,
            "CREATE TABLE page_tags (id INTEGER PRIMARY KEY) SYNC TWO WAY",
            &empty(),
        )
        .unwrap();
    hub.db.commit(schema_tx).unwrap();
    let rows_tx = hub.db.begin().unwrap();
    for id in [1, 2] {
        hub.db
            .execute_in_tx(
                rows_tx,
                "INSERT INTO page_notes (id, body) VALUES ($id, $body)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("body".to_owned(), Value::Text(format!("note-{id}"))),
                ]),
            )
            .unwrap();
    }
    hub.db.commit(rows_tx).unwrap();
    let source = hub
        .db
        .changes_since(Lsn(0))
        .expect("read or construct the declared fixture result");
    let schema_lsn = source
        .ddl_lsn
        .iter()
        .copied()
        .find(|lsn| source.ddl_lsn.iter().filter(|other| *other == lsn).count() == 2)
        .expect("both application declarations share a source LSN");
    let row_lsn = source
        .rows
        .iter()
        .find(|row| row.table == "page_notes")
        .unwrap()
        .lsn;
    assert!(schema_lsn < row_lsn);

    let first = hub.pull(Lsn(0)).await;
    assert!(
        !first.is_empty(),
        "a bounded pull must not return an empty progress page before deliverable schema: {first:?}"
    );
    assert_eq!(first.ddl, ["page_notes", "page_tags"]);
    assert_eq!(first.ddl_lsn, [schema_lsn, schema_lsn]);
    assert!(first.row_lsn.is_empty());
    assert_eq!(first.cursor, Some(schema_lsn));
    assert!(first.has_more);

    let second = hub.pull(first.cursor.unwrap()).await;
    assert!(second.ddl.is_empty(), "schema group must not replay");
    assert_eq!(second.row_lsn, [row_lsn, row_lsn]);
    assert!(
        second.cursor.is_some_and(|cursor| cursor >= row_lsn),
        "the final cursor consumes the row group and any later hub contact"
    );
    assert!(!second.has_more, "only hub-local contact work remains");
    hub.stop().await;
}

#[tokio::test]
async fn bookkeeping_only_history_terminates_and_consumes_its_source_cursor() {
    let hub = RunningHub::start().await;
    let page = hub.pull(Lsn(0)).await;
    assert!(page.is_empty(), "the hub's own contact is never delivered");
    assert!(
        !page.has_more,
        "bookkeeping alone cannot promise another page"
    );
    assert!(page.cursor.is_some_and(|cursor| cursor > Lsn(0)));
    hub.stop().await;
}
