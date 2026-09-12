//! A mixed-version table hold must preserve ordinary delivery, not turn its
//! cursor into permission to forget a page the receiver never saw.
//!
//! These are deliberately black-box sync journeys. The only transport wrapper
//! observes decoded pull pages (and, once, cuts a reply after the hub has
//! produced it); it neither fabricates a page nor changes sync state.

use contextdb_core::{Lsn, TenantId, Value};
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::protocol::{
    DependencyCompletePullResponse, MessageType, PullResponse, SchemaRecoveryPage, WireChangeSet,
    WireDdlChange, WireDdlProvenance, decode,
};
use contextdb_engine::sync_types::{SchemaSyncCapability, SchemaSyncHoldback};
use contextdb_engine::{Database, QueryResult};
use contextdb_server::protocol::PROTOCOL_VERSION;
use contextdb_server::subjects::pull_subject;
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const CURSOR_TENANT: &str = "vector-schema-recovery-cursor";
const CUT_TENANT: &str = "vector-schema-recovery-cut";
const SILENCE_TENANT: &str = "vector-schema-recovery-default-silence";
const ERASURE_TENANT: &str = "vector-schema-recovery-purge-order";
const TRIGGER_TENANT: &str = "vector-schema-recovery-trigger-bootstrap";
const REVERTED_BEFORE_HOLDBACK_TENANT: &str = "vector-schema-recovery-reverted-before-holdback";
const RELEASED_AFTER_ALTER_TENANT: &str = "vector-schema-recovery-released-after-alter";
const ALL_DEFAULT_HNSW_TENANT: &str = "vector-schema-recovery-all-default-hnsw";
const REVERTED_GRAPH_AND_CROSSOVER_TENANT: &str =
    "vector-schema-recovery-reverted-graph-and-crossover";
const DROPPED_PARTITIONED_TABLE_TENANT: &str = "vector-schema-recovery-dropped-partitioned-table";
const RECOVERY_ROWS: usize = 501;
const PULL_PAGE_ROWS: usize = 500;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn ids(result: QueryResult) -> Vec<Uuid> {
    result
        .rows
        .into_iter()
        .map(|row| match row.into_iter().next() {
            Some(Value::Uuid(id)) => id,
            value => panic!("expected one UUID value, got {value:?}"),
        })
        .collect()
}

fn graph_targets(db: &Database, source: Uuid) -> Vec<Uuid> {
    ids(db
        .execute(
            "SELECT target FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
             WHERE a.id = $source COLUMNS (b.id AS target))",
            &params([("source", Value::Uuid(source))]),
        )
        .expect("the graph edge is reachable"))
}

fn held(table: &str, capability: SchemaSyncCapability, node: &str) -> Vec<SchemaSyncHoldback> {
    vec![SchemaSyncHoldback {
        table: table.to_owned(),
        capability,
        node_to_upgrade: node.to_owned(),
    }]
}

struct RunningServer {
    server: Arc<SyncServer>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningServer {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.await.expect("server task stops");
    }
}

async fn start_server(
    broker: &InProcessBroker,
    tenant: &str,
    db: Arc<Database>,
) -> (RunningServer, Arc<FabricIdentity>) {
    let identity = Arc::new(FabricIdentity::generate());
    start_server_with_identity(broker, tenant, db, identity).await
}

async fn start_server_with_identity(
    broker: &InProcessBroker,
    tenant: &str,
    db: Arc<Database>,
    identity: Arc<FabricIdentity>,
) -> (RunningServer, Arc<FabricIdentity>) {
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
            broker.server_as(&node_id),
            TenantId::from(tenant),
            node_id,
            identity.clone(),
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&pull_subject(tenant))
        .await;
    (
        RunningServer {
            server,
            shutdown,
            task,
        },
        identity,
    )
}

#[derive(Debug, Clone)]
struct RecordedPullPage {
    protocol: u8,
    delivered_to_receiver: bool,
    cursor: Option<Lsn>,
    frontiers: Vec<Lsn>,
    row_tables: Vec<String>,
    edges: Vec<(Uuid, Uuid, String)>,
    ddl: Vec<WireDdlChange>,
    ddl_provenance: Vec<WireDdlProvenance>,
    has_later_docs_shape: bool,
    purges: Vec<String>,
    recovery: Option<SchemaRecoveryPage>,
}

impl RecordedPullPage {
    fn from_response(
        protocol: u8,
        delivered_to_receiver: bool,
        response: &PullResponse,
        units: &[WireChangeSet],
    ) -> Self {
        let mut page = Self {
            protocol,
            delivered_to_receiver,
            cursor: response.cursor,
            frontiers: Vec::new(),
            row_tables: Vec::new(),
            edges: Vec::new(),
            ddl: Vec::new(),
            ddl_provenance: Vec::new(),
            has_later_docs_shape: false,
            purges: Vec::new(),
            recovery: response.schema_recovery.clone(),
        };
        page.observe_changes(&response.changeset);
        for unit in units {
            page.observe_changes(unit);
        }
        page
    }

    fn observe_changes(&mut self, changes: &WireChangeSet) {
        self.ddl.extend(changes.ddl.iter().cloned());
        self.ddl_provenance
            .extend(changes.ddl_provenance.iter().cloned());
        self.frontiers.extend(changes.ddl_lsn.iter().copied());
        self.row_tables
            .extend(changes.rows.iter().map(|row| row.table.clone()));
        self.frontiers
            .extend(changes.rows.iter().map(|row| row.lsn));
        self.edges.extend(
            changes
                .edges
                .iter()
                .map(|edge| (edge.source, edge.target, edge.edge_type.clone())),
        );
        self.frontiers
            .extend(changes.edges.iter().map(|edge| edge.lsn));
        self.has_later_docs_shape |= changes.ddl.iter().any(|ddl| {
            matches!(
                ddl,
                WireDdlChange::AlterTable { name, columns, .. }
                    if name == "docs" && columns.iter().any(|(column, _)| column == "shape")
            )
        });
        self.purges
            .extend(changes.purges.iter().map(|purge| purge.table.clone()));
        self.frontiers
            .extend(changes.purges.iter().map(|purge| purge.purge_frontier));
    }
}

/// A read-only pull-page witness. `cut_one_old_reply` models a response that
/// the server completed but the old receiver never received.
struct ObservePullPages {
    inner: Arc<dyn ClientTransport>,
    subject: String,
    pages: Mutex<Vec<RecordedPullPage>>,
    cut_one_old_reply: AtomicBool,
}

impl ObservePullPages {
    fn pages(&self) -> Vec<RecordedPullPage> {
        self.pages.lock().expect("page witness lock").clone()
    }
}

impl ClientTransport for ObservePullPages {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let inner = self.inner.clone();
        let observe = subject == self.subject;
        let pages = &self.pages;
        let cut_one_old_reply = &self.cut_one_old_reply;
        Box::pin(async move {
            let request_protocol = decode(&request_bytes)
                .map_err(|error| TransportError::Other(format!("decode pull request: {error}")))?
                .version;
            let reply = inner.request(subject, request_bytes, timeout).await?;
            if observe {
                let cut_reply = request_protocol == PROTOCOL_VERSION
                    && cut_one_old_reply
                        .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok();
                let envelope = decode(&reply).map_err(|error| {
                    TransportError::Other(format!("decode inspected pull response: {error}"))
                })?;
                let page = match envelope.message_type {
                    MessageType::PullResponse => {
                        let response: PullResponse = rmp_serde::from_slice(&envelope.payload)
                            .map_err(|error| TransportError::Other(error.to_string()))?;
                        RecordedPullPage::from_response(
                            request_protocol,
                            !cut_reply,
                            &response,
                            &[],
                        )
                    }
                    MessageType::DependencyCompletePullResponse => {
                        let response: DependencyCompletePullResponse =
                            rmp_serde::from_slice(&envelope.payload)
                                .map_err(|error| TransportError::Other(error.to_string()))?;
                        RecordedPullPage::from_response(
                            request_protocol,
                            !cut_reply,
                            &response.ordinary,
                            &response.units,
                        )
                    }
                    other => {
                        return Err(TransportError::Other(format!(
                            "unexpected inspected pull response: {other:?}"
                        )));
                    }
                };
                pages.lock().expect("page witness lock").push(page);
                if cut_reply {
                    return Err(TransportError::IncompleteReply(
                        "deterministic cut of the final missing-capability pull response"
                            .to_owned(),
                    ));
                }
            }
            Ok(reply)
        })
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

fn assert_authored_table_history(source: &Database, pages: &[RecordedPullPage], table: &str) {
    let original = source.changes_since(Lsn(0));
    // Only DDL enters this existing test accessor: no row is present for its
    // lineage-binding branch to mutate. The signer must never be invoked.
    let schema = contextdb_engine::sync_types::ChangeSet {
        ddl: original.ddl,
        ddl_lsn: original.ddl_lsn,
        ..Default::default()
    };
    let (_, provenance) = source
        .outbound_row_lineages_and_ddl_provenance_for_test(
            &schema,
            &TenantId::from("authored-history-witness"),
            "unused",
            contextdb_core::Incarnation::mint(),
            &|_| panic!("DDL witness cannot sign row lineage"),
        )
        .unwrap();
    let expected = schema
        .ddl
        .into_iter()
        .map(WireDdlChange::from)
        .zip(provenance)
        .filter(|(_, entry)| entry.table.as_deref() == Some(table))
        .collect::<Vec<_>>();
    let observed = pages
        .iter()
        .filter(|page| page.delivered_to_receiver)
        .flat_map(|page| {
            page.ddl
                .iter()
                .cloned()
                .zip(page.ddl_provenance.iter().cloned())
        })
        .filter(|(_, entry)| entry.table.as_deref() == Some(table))
        .collect::<Vec<_>>();
    assert_eq!(
        observed, expected,
        "authored SQL, occurrence order, source LSN, ordinal, generation and digest remain exact"
    );
}

fn current_transport_pages(pages: &[RecordedPullPage]) -> Vec<&RecordedPullPage> {
    pages
        .iter()
        .filter(|page| page.protocol == PROTOCOL_VERSION)
        .collect()
}

fn row_packets(pages: &[RecordedPullPage], table: &str) -> usize {
    pages
        .iter()
        .flat_map(|page| &page.row_tables)
        .filter(|seen| seen.as_str() == table)
        .count()
}

fn edge_packets(pages: &[RecordedPullPage], source: Uuid, target: Uuid) -> usize {
    pages
        .iter()
        .flat_map(|page| &page.edges)
        .filter(|(seen_source, seen_target, edge_type)| {
            *seen_source == source && *seen_target == target && edge_type == "LINKS"
        })
        .count()
}

fn delivered_row_packets(pages: &[RecordedPullPage], table: &str) -> usize {
    pages
        .iter()
        .filter(|page| page.delivered_to_receiver)
        .flat_map(|page| &page.row_tables)
        .filter(|seen| seen.as_str() == table)
        .count()
}

fn delivered_edge_packets(pages: &[RecordedPullPage], source: Uuid, target: Uuid) -> usize {
    pages
        .iter()
        .filter(|page| page.delivered_to_receiver)
        .flat_map(|page| &page.edges)
        .filter(|(seen_source, seen_target, edge_type)| {
            *seen_source == source && *seen_target == target && edge_type == "LINKS"
        })
        .count()
}

fn expected_later_doc_ids() -> Vec<Uuid> {
    (0..RECOVERY_ROWS)
        .map(|ordinal| Uuid::from_u128(0xA400_1000 + ordinal as u128))
        .collect()
}

fn assert_all_later_docs_keep_their_post_shape(db: &Database, phase: &str) {
    assert_eq!(
        ids(db
            .execute(
                "SELECT id FROM docs WHERE shape = 'later' ORDER BY id",
                &empty(),
            )
            .expect("read all later rows with their post-shape value"),),
        expected_later_doc_ids(),
        "{phase}: every later row must arrive exactly once with shape = 'later'"
    );
}

fn assert_old_cursor_names_only_the_page_it_served(pages: &[RecordedPullPage]) {
    for page in current_transport_pages(pages)
        .into_iter()
        .filter(|page| page.recovery.is_none() && !page.frontiers.is_empty())
    {
        let served = page
            .frontiers
            .iter()
            .copied()
            .max()
            .expect("every checked missing-capability page carries compatible work");
        assert_eq!(
            page.cursor,
            Some(served),
            "ordinary durable progress must stop at the last frontier actually present in its page; a held table is recovered by its private recovery cursor, never by advancing this cursor past absent work"
        );
    }
}

fn declare_graph_controls_and_held_vector(db: &Database) {
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare a compatible node table");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare a compatible graph edge table");
    db.execute(
        "CREATE TABLE vector_docs (id UUID PRIMARY KEY, scope_id UUID NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id)) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare the protocol-7-only table");
}

fn insert_node(db: &Database, id: Uuid) {
    db.execute(
        "INSERT INTO nodes (id) VALUES ($id)",
        &params([("id", Value::Uuid(id))]),
    )
    .expect("insert compatible node");
}

fn insert_edge(db: &Database, id: Uuid, source: Uuid, target: Uuid) {
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) \
         VALUES ($id, $source, $target, 'LINKS')",
        &params([
            ("id", Value::Uuid(id)),
            ("source", Value::Uuid(source)),
            ("target", Value::Uuid(target)),
        ]),
    )
    .expect("insert compatible graph edge");
}

fn insert_vector_doc(db: &Database, id: Uuid, scope: Uuid) {
    db.execute(
        "INSERT INTO vector_docs (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("insert held vector row");
}

/// Clauses the approved-base grammar has no rule for. Base `vector_type` is
/// `VECTOR ( integer ) vector_quantization_clause?` and nothing more
/// (`git show 5cf9728c:crates/contextdb-parser/src/grammar.pest:190`); the
/// candidate adds these five as optional members of the same production
/// (`grammar.pest:212-223`). `column_constraint` is unchanged between the two
/// (base `:196-212`, candidate `:242-258`).
const UNSUPPORTED_VECTOR_CLAUSES: [&str; 5] = [
    "PARTITION_KEY",
    "MAX_PARTITIONS",
    "SEARCH_MODE",
    "AUTO_INDEX_AT",
    "HNSW",
];

/// `ALTER COLUMN` is a candidate-only `alter_action`
/// (base `grammar.pest:163` lists no `alter_column_action`). The wire carries a
/// structured `AlterTable` that the receiver validates by rendering a
/// `CREATE TABLE` (`database.rs:38561-38578`), so this phrase should never
/// travel; the guard is standing, not load-bearing.
const UNSUPPORTED_ALTER_PHRASE: &str = "ALTER COLUMN";

/// Split a rendered column-type string into grammar-shaped tokens: a bare run
/// of non-whitespace, non-`(` characters, or one balanced parenthesised group
/// (quote-aware, so a `'...'` value cannot desynchronize the paren count).
/// This is what lets `RANK_POLICY (JOIN hnsw ON search_mode, ...)` stay one
/// opaque token instead of exposing `hnsw` and `search_mode` as look-alike
/// keyword positions.
fn tokenize_rendered_column_type(rendered_type: &str) -> Vec<&str> {
    let bytes = rendered_type.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i] == b'(' {
            let start = i;
            let mut depth: i32 = 0;
            let mut in_quote = false;
            while i < bytes.len() {
                match bytes[i] {
                    b'\'' => in_quote = !in_quote,
                    b'(' if !in_quote => depth += 1,
                    b')' if !in_quote => {
                        depth -= 1;
                        if depth == 0 {
                            i += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            tokens.push(&rendered_type[start..i]);
        } else {
            let start = i;
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() && bytes[i] != b'(' {
                i += 1;
            }
            tokens.push(&rendered_type[start..i]);
        }
    }
    tokens
}

/// Consume the base-legal `vector_type` prefix -- `VECTOR(<integer>)` plus an
/// optional `WITH (...)` quantization group, or a non-vector type's single
/// token -- and return the index of the first remaining token. That token is
/// the one place a retired clause keyword can appear; everything after it is
/// `column_constraint*`, which the base grammar already accepts unconditionally.
fn base_legal_prefix_len(tokens: &[&str]) -> usize {
    if tokens
        .first()
        .is_some_and(|token| token.eq_ignore_ascii_case("VECTOR"))
    {
        let mut idx = 1;
        if tokens.get(idx).is_some_and(|token| token.starts_with('(')) {
            idx += 1;
        }
        if tokens
            .get(idx)
            .is_some_and(|token| token.eq_ignore_ascii_case("WITH"))
        {
            idx += 1;
            if tokens.get(idx).is_some_and(|token| token.starts_with('(')) {
                idx += 1;
            }
        }
        idx
    } else if tokens.is_empty() {
        0
    } else {
        1
    }
}

/// The shared proof: every `WireDdlChange` a page carries must be spelled in
/// vocabulary the base grammar (`5cf9728c:crates/contextdb-parser/src/grammar.pest`)
/// has a rule for. Written from the BASE grammar, not from the candidate's
/// renderer, so the two can disagree -- which is what makes the assertion
/// worth running.
fn assert_ddl_is_parseable_by_the_older_peer(ddl: &[WireDdlChange], phase: &str) {
    for change in ddl {
        let (table, columns, constraints) = match change {
            WireDdlChange::CreateTable {
                name,
                columns,
                constraints,
                ..
            }
            | WireDdlChange::AlterTable {
                name,
                columns,
                constraints,
                ..
            } => (name, columns, constraints),
            _ => continue,
        };
        for (column, rendered_type) in columns {
            let tokens = tokenize_rendered_column_type(rendered_type);
            let boundary = base_legal_prefix_len(&tokens);
            if let Some(next) = tokens.get(boundary)
                && let Some(clause) = UNSUPPORTED_VECTOR_CLAUSES
                    .iter()
                    .find(|clause| next.eq_ignore_ascii_case(clause))
            {
                panic!(
                    "{phase}: {table}.{column} carries {clause}, which the base grammar's \
                     vector_type has no rule for (5cf9728c:crates/contextdb-parser/src/grammar.pest:190); \
                     rendered type = {rendered_type:?}"
                );
            }
        }
        for constraint in constraints {
            assert!(
                !constraint.contains(UNSUPPORTED_ALTER_PHRASE),
                "{phase}: table {table} carries a constraint containing {UNSUPPORTED_ALTER_PHRASE:?}, \
                 which the base grammar's alter_action has no rule for: {constraint:?}"
            );
        }
    }
}

/// The over-removal detector: a scanner that ate a
/// character of a `REFERENCES` or `RANK_POLICY` clause produces a receiver
/// column shape that no longer matches the source. Same public door
/// `schema_render_carries_declared_access_control.rs` uses.
fn assert_schema_converged(source: &Database, receiver: &Database, table: &str, phase: &str) {
    let source_meta = source
        .table_meta(table)
        .unwrap_or_else(|| panic!("{phase}: source must still declare {table}"));
    let receiver_meta = receiver
        .table_meta(table)
        .unwrap_or_else(|| panic!("{phase}: receiver must have converged onto {table}"));
    assert_eq!(
        render_table_meta(table, &source_meta),
        render_table_meta(table, &receiver_meta),
        "{phase}: the receiver's rendered schema for {table} must match the source's"
    );
}

/// The column types a single `WireDdlChange` carries, or an empty slice for
/// every other kind of change.
fn ddl_column_types(ddl: &WireDdlChange) -> &[(String, String)] {
    match ddl {
        WireDdlChange::CreateTable { columns, .. } | WireDdlChange::AlterTable { columns, .. } => {
            columns
        }
        _ => &[],
    }
}

#[tokio::test]
async fn held_table_recovery_keeps_ordinary_rows_and_graph_edges_exactly_once_without_cursor_skip()
{
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let receiver = Arc::new(Database::open_memory());
    declare_graph_controls_and_held_vector(&source);
    source
        .execute(
            "CREATE TABLE custody_roots (id UUID PRIMARY KEY) \
             SYNC PUSH ONLY DELIVERY MANIFEST OVER custody_parts",
            &empty(),
        )
        .expect("declare a manifested root that enables custody pull filtering");
    source
        .execute(
            "CREATE TABLE custody_parts (id UUID PRIMARY KEY, root_id UUID REFERENCES custody_roots(id)) \
             SYNC PUSH ONLY",
            &empty(),
        )
        .expect("declare the manifested root's member table");
    let first = Uuid::from_u128(0xA100_0001);
    let second = Uuid::from_u128(0xA100_0002);
    let edge = Uuid::from_u128(0xA100_0003);
    let held_id = Uuid::from_u128(0xA100_0004);
    let scope = Uuid::from_u128(0xA100_00FF);
    insert_node(&source, first);
    insert_edge(&source, edge, first, second);
    insert_vector_doc(&source, held_id, scope);

    let (running, _) = start_server(&broker, CURSOR_TENANT, source.clone()).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&receiver_node),
        subject: pull_subject(CURSOR_TENANT),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(false),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        transport.clone(),
        TenantId::from(CURSOR_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    client
        .pull_default()
        .await
        .expect("only the incompatible table is held from the old receiver");
    assert_eq!(
        running.server.schema_sync_holdbacks(),
        held(
            "vector_docs",
            SchemaSyncCapability::VectorPartitioning,
            &receiver_node,
        )
    );
    let first_pages = transport.pages();
    assert_old_cursor_names_only_the_page_it_served(&first_pages);
    let durable_cursor = receiver
        .persisted_sync_pull_cursor(&TenantId::from(CURSOR_TENANT))
        .expect("read the receiver's durable ordinary pull cursor")
        .expect("the compatible page binds a source and cursor");
    assert_eq!(
        durable_cursor.1,
        current_transport_pages(&first_pages)
            .last()
            .expect("one old-protocol page was served")
            .cursor
            .expect("compatible page has a cursor"),
        "the durable ordinary cursor is the cursor of the compatible page, not the held table's hidden frontier"
    );
    assert_eq!(
        ids(receiver.execute("SELECT id FROM nodes", &empty()).unwrap()),
        vec![first]
    );
    assert_eq!(graph_targets(&receiver, first), vec![second]);

    insert_node(&source, second);
    client
        .pull_default()
        .await
        .expect("a later compatible node crosses while the vector table waits");
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM nodes ORDER BY id", &empty())
            .unwrap()),
        vec![first, second]
    );
    assert_eq!(graph_targets(&receiver, first), vec![second]);

    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("the same receiver recovers the held image without resetting ordinary progress");
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM vector_docs", &empty())
            .unwrap()),
        vec![held_id]
    );
    assert_eq!(running.server.schema_sync_holdbacks(), Vec::new());
    let pages = transport.pages();
    assert_old_cursor_names_only_the_page_it_served(&pages);
    let recovery_pages = pages
        .iter()
        .filter_map(|page| page.recovery.as_ref())
        .collect::<Vec<_>>();
    assert!(
        !recovery_pages.is_empty(),
        "the held table uses recovery pages"
    );
    assert!(
        recovery_pages
            .iter()
            .all(|page| page.next_lsn <= page.target_lsn),
        "custody filtering cannot advance a held-table recovery cursor beyond its frozen target: {recovery_pages:?}"
    );
    assert_eq!(
        row_packets(&pages, "nodes"),
        2,
        "each ordinary row arrived once"
    );
    assert_eq!(
        edge_packets(&pages, first, second),
        1,
        "the graph edge arrived once"
    );
    assert!(
        pages
            .iter()
            .filter(|page| page.recovery.is_some())
            .all(|page| {
                page.row_tables.iter().all(|table| table == "vector_docs") && page.edges.is_empty()
            }),
        "held-table recovery carries only its held table; it cannot consume unrelated rows or graph edges"
    );

    running.stop().await;
}

#[tokio::test]
async fn cut_last_held_back_page_then_upgrade_recovers_without_skipping_ordinary_or_graph_work() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let receiver = Arc::new(Database::open_memory());
    declare_graph_controls_and_held_vector(&source);
    let first = Uuid::from_u128(0xA200_0001);
    let second = Uuid::from_u128(0xA200_0002);
    let edge = Uuid::from_u128(0xA200_0003);
    let held_id = Uuid::from_u128(0xA200_0004);
    insert_node(&source, first);
    insert_node(&source, second);
    insert_edge(&source, edge, first, second);
    insert_vector_doc(&source, held_id, Uuid::from_u128(0xA200_00FF));

    let (running, _) = start_server(&broker, CUT_TENANT, source).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&receiver_node),
        subject: pull_subject(CUT_TENANT),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(true),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        transport.clone(),
        TenantId::from(CUT_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    assert!(
        client.pull_default().await.is_err(),
        "the deterministic cut leaves the old receiver without the final response"
    );
    assert_eq!(
        current_transport_pages(&transport.pages()).len(),
        1,
        "the compact missing-capability fixture has one final response to cut"
    );
    assert!(
        current_transport_pages(&transport.pages())
            .iter()
            .all(|page| !page.delivered_to_receiver),
        "the witness saw the sender's completed old-protocol reply, but the cut means the receiver did not"
    );
    assert!(
        receiver.table_meta("nodes").is_none(),
        "a reply cut before delivery cannot apply its ordinary table or rows"
    );
    assert!(
        receiver
            .persisted_sync_pull_cursor(&TenantId::from(CUT_TENANT))
            .expect("read the receiver cursor after the cut")
            .is_none(),
        "a reply the receiver never received cannot advance its ordinary cursor"
    );
    assert_eq!(
        running.server.schema_sync_holdbacks(),
        held(
            "vector_docs",
            SchemaSyncCapability::VectorPartitioning,
            &receiver_node,
        ),
        "the sender recorded the per-table gap before its reply was lost"
    );

    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("upgrading after a cut response replays every missing ordinary and held item");
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM nodes ORDER BY id", &empty())
            .unwrap()),
        vec![first, second]
    );
    assert_eq!(graph_targets(&receiver, first), vec![second]);
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM vector_docs", &empty())
            .unwrap()),
        vec![held_id]
    );
    let pages = transport.pages();
    assert_old_cursor_names_only_the_page_it_served(&pages);
    assert_eq!(
        delivered_row_packets(&pages, "nodes"),
        2,
        "the next exchange replays both ordinary rows exactly once; the sender's unseen reply is not receiver delivery"
    );
    assert_eq!(
        delivered_edge_packets(&pages, first, second),
        1,
        "the next exchange replays the graph edge exactly once; the sender's unseen reply is not receiver delivery"
    );
    assert!(running.server.schema_sync_holdbacks().is_empty());

    running.stop().await;
}

#[tokio::test]
async fn restoring_default_schema_keeps_authored_history_held_until_the_peer_upgrades() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let receiver = Arc::new(Database::open_memory());
    source
        .execute(
            "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare the compatible control table");
    source
        .execute(
            "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare the first table with protocol-7 schema vocabulary");
    source
        .execute(
            "CREATE TABLE still_held_docs (id UUID PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare the independently incompatible table");
    let note = Uuid::from_u128(0xA300_0001);
    let doc = Uuid::from_u128(0xA300_0002);
    let still_held_doc = Uuid::from_u128(0xA300_0003);
    source
        .execute(
            "INSERT INTO ordinary_notes (id, body) VALUES ($id, 'control')",
            &params([("id", Value::Uuid(note))]),
        )
        .expect("write compatible control data");
    source
        .execute(
            "INSERT INTO docs (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Uuid(doc)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("write the held table data");
    source
        .execute(
            "INSERT INTO still_held_docs (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Uuid(still_held_doc)),
                ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
            ]),
        )
        .expect("write the independently held table data");

    let (running, _) = start_server(&broker, SILENCE_TENANT, source.clone()).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        broker.client_as(&receiver_node),
        TenantId::from(SILENCE_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    client
        .pull_default()
        .await
        .expect("the ordinary table flows");
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM ordinary_notes", &empty())
            .unwrap()),
        vec![note]
    );
    assert!(receiver.table_meta("docs").is_none());
    assert_eq!(
        running.server.schema_sync_holdbacks(),
        vec![
            SchemaSyncHoldback {
                table: "docs".to_owned(),
                capability: SchemaSyncCapability::VectorSearchMode,
                node_to_upgrade: receiver_node.clone(),
            },
            SchemaSyncHoldback {
                table: "still_held_docs".to_owned(),
                capability: SchemaSyncCapability::VectorSearchMode,
                node_to_upgrade: receiver_node.clone(),
            },
        ]
    );

    source
        .execute(
            "ALTER TABLE docs ALTER COLUMN embedding SET SEARCH_MODE AUTO",
            &empty(),
        )
        .expect("the authorized ALTER restores the compatible default silence");
    client
        .pull_default()
        .await
        .expect("compatible tables continue after the local revert");
    assert!(receiver.table_meta("docs").is_none());
    assert!(receiver.table_meta("still_held_docs").is_none());
    assert_eq!(running.server.schema_sync_holdbacks().len(), 2);
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM ordinary_notes", &empty())
            .unwrap()),
        vec![note]
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("the upgraded peer replays both authored histories");
    assert_eq!(
        ids(receiver.execute("SELECT id FROM docs", &empty()).unwrap()),
        vec![doc]
    );
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM still_held_docs", &empty())
            .unwrap()),
        vec![still_held_doc]
    );
    assert!(running.server.schema_sync_holdbacks().is_empty());
    running.stop().await;
}

#[tokio::test]
async fn held_recovery_pages_purge_before_later_shape_work_and_keeps_erased_vectors_gone_after_restart()
 {
    let root = tempfile::TempDir::new().expect("temporary durable sync directory");
    let source_path = root.path().join("source.redb");
    let receiver_path = root.path().join("receiver.redb");
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open(&source_path).expect("open durable purge source"));
    let receiver = Arc::new(Database::open(&receiver_path).expect("open receiver"));
    source
        .execute(
            "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(3)) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare a missing-capability-compatible vector table");
    let erased = Uuid::from_u128(0xA400_0001);
    source
        .execute(
            "INSERT INTO docs (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Uuid(erased)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("seed the old receiver's future stale vector");

    let (running, _) = start_server(&broker, ERASURE_TENANT, source.clone()).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&receiver_node),
        subject: pull_subject(ERASURE_TENANT),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(false),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        transport.clone(),
        TenantId::from(ERASURE_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    client
        .pull_default()
        .await
        .expect("the old receiver first receives the compatible vector");
    assert_eq!(
        ids(receiver.execute("SELECT id FROM docs", &empty()).unwrap()),
        vec![erased]
    );

    source
        .execute(
            "ALTER TABLE docs ALTER COLUMN embedding SET SEARCH_MODE INDEXED",
            &empty(),
        )
        .expect("make docs incompatible with a peer lacking vector vocabulary");
    source
        .execute(
            "PURGE FROM docs WHERE id = $id",
            &params([("id", Value::Uuid(erased))]),
        )
        .expect("authoritatively erase the held vector");
    source
        .execute("ALTER TABLE docs ADD COLUMN shape TEXT", &empty())
        .expect("publish later schema-generation work after the purge");
    for ordinal in 0..RECOVERY_ROWS {
        source
            .execute(
                "INSERT INTO docs (id, embedding, shape) VALUES ($id, $embedding, 'later')",
                &params([
                    (
                        "id",
                        Value::Uuid(Uuid::from_u128(0xA400_1000 + ordinal as u128)),
                    ),
                    ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
                ]),
            )
            .expect("add a bounded recovery-page row");
    }
    client
        .pull_default()
        .await
        .expect("a peer lacking vector vocabulary holds only docs while its purge and later shape work wait");
    assert_eq!(
        running.server.schema_sync_holdbacks(),
        held(
            "docs",
            SchemaSyncCapability::VectorSearchMode,
            &receiver_node,
        )
    );

    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("the upgraded receiver recovers the bounded held image");
    let pages = transport.pages();
    let recovery_pages = pages
        .iter()
        .filter(|page| page.recovery.is_some())
        .collect::<Vec<_>>();
    assert!(
        recovery_pages.len() >= 2,
        "501 rows require finite recovery pages"
    );
    assert!(
        recovery_pages
            .iter()
            .all(|page| page.row_tables.len() <= PULL_PAGE_ROWS),
        "each recovery response respects the public 500-row pull-page bound"
    );
    let purge_page = recovery_pages
        .iter()
        .position(|page| page.purges.iter().any(|table| table == "docs"))
        .expect("the held recovery carries the authoritative purge");
    assert!(
        !recovery_pages[purge_page].has_later_docs_shape,
        "the page ending at the purge frontier must not also carry later schema shape work"
    );
    assert!(
        recovery_pages
            .iter()
            .skip(purge_page + 1)
            .any(|page| page.has_later_docs_shape),
        "the next bounded recovery request carries the later shape work"
    );
    assert!(
        ids(receiver
            .execute(
                "SELECT id FROM docs WHERE id = $id",
                &params([("id", Value::Uuid(erased))]),
            )
            .expect("row lookup after recovery"),)
        .is_empty()
    );
    assert!(
        !ids(receiver
            .execute(
                "SELECT id FROM docs ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 501",
                &params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .expect("vector search after recovery"),)
        .contains(&erased),
        "the purged vector is unreachable through the public vector door"
    );
    assert_all_later_docs_keep_their_post_shape(&receiver, "recovery before receiver restart");

    drop(client);
    running.stop().await;
    receiver.close().expect("close recovered receiver");
    drop(receiver);
    let reopened = Database::open(&receiver_path).expect("restart recovered receiver");
    assert!(
        ids(reopened
            .execute(
                "SELECT id FROM docs WHERE id = $id",
                &params([("id", Value::Uuid(erased))]),
            )
            .expect("row lookup after restart"),)
        .is_empty()
    );
    assert!(
        !ids(reopened
            .execute(
                "SELECT id FROM docs ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 501",
                &params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .expect("vector search after restart"),)
        .contains(&erased),
        "restart cannot resurrect a purged vector from an old recovery image"
    );
    assert_all_later_docs_keep_their_post_shape(&reopened, "recovery after receiver restart");
    reopened.close().expect("close restarted receiver");
}

#[tokio::test]
async fn held_recovery_bootstraps_trigger_ddl_before_later_rows() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let receiver = Arc::new(Database::open_memory());
    source
        .execute(
            "CREATE TABLE held_trigger_docs (id UUID PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare the table the old receiver cannot parse");
    source
        .execute(
            "CREATE TRIGGER held_docs_insert ON held_trigger_docs WHEN INSERT",
            &empty(),
        )
        .expect("declare the trigger before later data");
    source
        .register_trigger_callback("held_docs_insert", |_db, _context| Ok(()))
        .expect("initialize the source callback before writing its trigger table");
    source
        .complete_initialization()
        .expect("the source is ready to write trigger-attached rows");
    let later = Uuid::from_u128(0xA500_0001);
    source
        .execute(
            "INSERT INTO held_trigger_docs (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Uuid(later)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("write data after the trigger declaration");

    let (running, _) = start_server(&broker, TRIGGER_TENANT, source).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&receiver_node),
        subject: pull_subject(TRIGGER_TENANT),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(false),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        transport.clone(),
        TenantId::from(TRIGGER_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    client
        .pull_default()
        .await
        .expect("the old receiver holds only the incompatible trigger table");
    assert_eq!(
        running.server.schema_sync_holdbacks(),
        held(
            "held_trigger_docs",
            SchemaSyncCapability::VectorSearchMode,
            &receiver_node,
        )
    );

    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("the first recovery call applies only the trigger bootstrap page and returns");
    assert!(
        ids(receiver
            .execute("SELECT id FROM held_trigger_docs", &empty())
            .expect("read held table after trigger bootstrap"),)
        .is_empty(),
        "later data must remain behind the unregistered trigger callback boundary"
    );
    let first_recovery_pages = transport
        .pages()
        .into_iter()
        .filter(|page| page.recovery.is_some())
        .collect::<Vec<_>>();
    assert_eq!(
        first_recovery_pages.len(),
        1,
        "the first recovery call stops after the DDL-only trigger bootstrap page"
    );
    let trigger_bootstrap = &first_recovery_pages[0];
    assert!(
        trigger_bootstrap.row_tables.is_empty() && trigger_bootstrap.edges.is_empty(),
        "the trigger bootstrap recovery page applies DDL only, never later data"
    );
    assert!(
        trigger_bootstrap.ddl.iter().any(|ddl| {
            matches!(
                ddl,
                WireDdlChange::CreateTrigger { name, table, .. }
                    | WireDdlChange::CreateTriggerIncludingSync { name, table, .. }
                    if name == "held_docs_insert" && table == "held_trigger_docs"
            )
        }),
        "the first recovery page contains the trigger declaration before any row"
    );

    receiver
        .register_trigger_callback("held_docs_insert", |_db, _context| Ok(()))
        .expect("register the recovered trigger callback");
    receiver
        .complete_initialization()
        .expect("complete receiver initialization before later recovery data");
    client
        .pull_default()
        .await
        .expect("the next recovery call resumes with the trigger-attached data");
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM held_trigger_docs", &empty())
            .expect("read data after trigger initialization"),),
        vec![later]
    );
    let recovery_pages = transport
        .pages()
        .into_iter()
        .filter(|page| page.recovery.is_some())
        .collect::<Vec<_>>();
    assert!(
        recovery_pages.len() >= 2,
        "the registered callback allows a later recovery page after the DDL-only page"
    );
    assert!(
        recovery_pages[0].ddl.iter().any(|ddl| matches!(
            ddl,
            WireDdlChange::CreateTrigger { .. } | WireDdlChange::CreateTriggerIncludingSync { .. }
        )) && recovery_pages[0].row_tables.is_empty(),
        "recorded recovery order starts with trigger DDL and no data"
    );
    assert!(
        recovery_pages[1].row_tables.len() == 1
            && recovery_pages[1].row_tables[0] == "held_trigger_docs",
        "recorded recovery order puts held-table data on the next page after trigger bootstrap"
    );
    assert!(running.server.schema_sync_holdbacks().is_empty());

    running.stop().await;
}

#[tokio::test]
async fn a_reverted_declaration_keeps_its_authored_history_until_capability_catchup() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let receiver = Arc::new(Database::open_memory());
    source
        .execute(
            "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare the compatible control table");
    source
        .execute(
            "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare docs with a protocol-7-only search mode");
    let note = Uuid::from_u128(0xA600_0001);
    let doc = Uuid::from_u128(0xA600_0002);
    source
        .execute(
            "INSERT INTO ordinary_notes (id, body) VALUES ($id, 'control')",
            &params([("id", Value::Uuid(note))]),
        )
        .expect("write compatible control data");
    source
        .execute(
            "INSERT INTO docs (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Uuid(doc)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("write the row before the revert");
    source
        .execute(
            "ALTER TABLE docs ALTER COLUMN embedding SET SEARCH_MODE AUTO",
            &empty(),
        )
        .expect("revert to the compatible default before any old peer ever pulls");

    let (running, _) = start_server(&broker, REVERTED_BEFORE_HOLDBACK_TENANT, source.clone()).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&receiver_node),
        subject: pull_subject(REVERTED_BEFORE_HOLDBACK_TENANT),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(false),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        transport.clone(),
        TenantId::from(REVERTED_BEFORE_HOLDBACK_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    client
        .pull_default()
        .await
        .expect("only compatible tables reach the cold peer");

    assert!(receiver.table_meta("docs").is_none());
    assert!(!running.server.schema_sync_holdbacks().is_empty());
    let old_pages = transport.pages();
    for page in current_transport_pages(&old_pages) {
        assert_ddl_is_parseable_by_the_older_peer(&page.ddl, "before capability catchup");
    }
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("capability catchup replays immutable authored history");
    assert_authored_table_history(&source, &transport.pages(), "docs");
    let pages = old_pages;
    for page in current_transport_pages(&pages) {
        assert_ddl_is_parseable_by_the_older_peer(&page.ddl, "cold pull of a reverted declaration");
    }
    assert_eq!(running.server.schema_sync_holdbacks(), Vec::new());
    assert_schema_converged(
        &source,
        &receiver,
        "docs",
        "cold pull of a reverted declaration",
    );
    assert_eq!(
        ids(receiver.execute("SELECT id FROM docs", &empty()).unwrap()),
        vec![doc]
    );
    assert_eq!(
        ids(receiver
            .execute(
                "SELECT id FROM docs ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
                &params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .expect("the released vector is queryable through the public vector door"),),
        vec![doc]
    );
    assert_old_cursor_names_only_the_page_it_served(&pages);

    running.stop().await;
}

#[tokio::test]
async fn intermediate_alters_keep_exact_history_through_capability_catchup() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let receiver = Arc::new(Database::open_memory());
    source
        .execute(
            "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare the compatible control table");
    source
        .execute(
            "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare docs with a protocol-7-only search mode");
    let note = Uuid::from_u128(0xA700_0001);
    let doc = Uuid::from_u128(0xA700_0002);
    source
        .execute(
            "INSERT INTO ordinary_notes (id, body) VALUES ($id, 'control')",
            &params([("id", Value::Uuid(note))]),
        )
        .expect("write compatible control data");
    source
        .execute(
            "INSERT INTO docs (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Uuid(doc)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("write the held table's row");

    let (running, _) = start_server(&broker, RELEASED_AFTER_ALTER_TENANT, source.clone()).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&receiver_node),
        subject: pull_subject(RELEASED_AFTER_ALTER_TENANT),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(false),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        transport.clone(),
        TenantId::from(RELEASED_AFTER_ALTER_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    client
        .pull_default()
        .await
        .expect("the ordinary table flows while docs is held");
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM ordinary_notes", &empty())
            .unwrap()),
        vec![note]
    );
    assert_eq!(
        running.server.schema_sync_holdbacks(),
        held(
            "docs",
            SchemaSyncCapability::VectorSearchMode,
            &receiver_node
        )
    );

    source
        .execute("ALTER TABLE docs ADD COLUMN shape TEXT", &empty())
        .expect(
            "an intermediate ALTER while the column is still INDEXED re-renders the full column list",
        );
    source
        .execute(
            "ALTER TABLE docs ALTER COLUMN embedding SET SEARCH_MODE AUTO",
            &empty(),
        )
        .expect("revert to the compatible default");
    client
        .pull_default()
        .await
        .expect("the unsupported history remains held while ordinary work continues");

    assert!(receiver.table_meta("docs").is_none());
    assert!(!running.server.schema_sync_holdbacks().is_empty());
    let old_pages = transport.pages();
    for page in current_transport_pages(&old_pages) {
        assert_ddl_is_parseable_by_the_older_peer(&page.ddl, "before capability catchup");
    }
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("capability catchup replays immutable authored history");
    assert_authored_table_history(&source, &transport.pages(), "docs");
    let pages = old_pages;
    for page in current_transport_pages(&pages) {
        assert_ddl_is_parseable_by_the_older_peer(&page.ddl, "release after an intermediate ALTER");
    }
    assert_eq!(running.server.schema_sync_holdbacks(), Vec::new());
    assert_schema_converged(
        &source,
        &receiver,
        "docs",
        "release after an intermediate ALTER",
    );
    assert_eq!(
        ids(receiver.execute("SELECT id FROM docs", &empty()).unwrap()),
        vec![doc]
    );
    assert_eq!(
        ids(receiver
            .execute(
                "SELECT id FROM docs ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
                &params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .expect("the released vector is queryable through the public vector door"),),
        vec![doc]
    );

    running.stop().await;
}

#[tokio::test]
async fn an_all_default_graph_policy_is_spelled_as_silence() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    source
        .execute(
            "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(3) HNSW (M = DEFAULT)) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare docs with an all-default graph policy");
    let doc = Uuid::from_u128(0xA800_0001);
    source
        .execute(
            "INSERT INTO docs (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Uuid(doc)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("write the row");

    let (running, _) = start_server(&broker, ALL_DEFAULT_HNSW_TENANT, source.clone()).await;

    let old_receiver = Arc::new(Database::open_memory());
    let old_identity = Arc::new(FabricIdentity::generate());
    let old_node = old_identity.node_id();
    let old_transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&old_node),
        subject: pull_subject(ALL_DEFAULT_HNSW_TENANT),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(false),
    });
    let old_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        old_receiver.clone(),
        old_transport.clone(),
        TenantId::from(ALL_DEFAULT_HNSW_TENANT),
        old_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&old_node, false);
    old_client
        .pull_default()
        .await
        .expect("an all-default graph policy must never hold the table for a peer lacking vector vocabulary");

    let current_receiver = Arc::new(Database::open_memory());
    let current_identity = Arc::new(FabricIdentity::generate());
    let current_node = current_identity.node_id();
    let current_transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&current_node),
        subject: pull_subject(ALL_DEFAULT_HNSW_TENANT),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(false),
    });
    let current_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        current_receiver.clone(),
        current_transport.clone(),
        TenantId::from(ALL_DEFAULT_HNSW_TENANT),
        current_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&current_node, true);
    current_client
        .pull_default()
        .await
        .expect("a current-protocol receiver also pulls docs");

    let old_pages = old_transport.pages();
    let current_pages = current_transport.pages();
    for page in current_transport_pages(&old_pages) {
        assert_ddl_is_parseable_by_the_older_peer(
            &page.ddl,
            "all-default HNSW at a peer lacking vector vocabulary",
        );
    }
    for page in old_pages.iter().chain(current_pages.iter()) {
        assert!(
            !page.ddl.iter().any(|ddl| ddl_column_types(ddl)
                .iter()
                .any(|(_, rendered_type)| rendered_type.to_ascii_uppercase().contains("HNSW"))),
            "an all-default graph policy is spelled as silence; no page at either protocol may carry an HNSW clause"
        );
    }
    assert_eq!(running.server.schema_sync_holdbacks(), Vec::new());
    assert_schema_converged(
        &source,
        &old_receiver,
        "docs",
        "all-default HNSW, a peer lacking vector vocabulary",
    );
    assert_schema_converged(
        &source,
        &current_receiver,
        "docs",
        "all-default HNSW, current protocol",
    );
    assert_eq!(
        ids(old_receiver
            .execute("SELECT id FROM docs", &empty())
            .unwrap()),
        vec![doc]
    );
    assert_eq!(
        ids(current_receiver
            .execute("SELECT id FROM docs", &empty())
            .unwrap()),
        vec![doc]
    );

    running.stop().await;
}

#[tokio::test]
async fn reverted_graph_and_crossover_history_remains_authored_until_upgrade() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let receiver = Arc::new(Database::open_memory());
    source
        .execute(
            "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare the compatible control table");
    source
        .execute(
            "CREATE TABLE hnsw (id UUID PRIMARY KEY, decision_id UUID, search_mode BOOL)",
            &empty(),
        )
        .expect("declare a joined table whose name is a reserved-looking word");
    source
        .execute(
            "CREATE INDEX hnsw_decision_idx ON hnsw(decision_id)",
            &empty(),
        )
        .expect("index the joined table's join column");
    source
        .execute(
            "CREATE TABLE docs (
                id UUID PRIMARY KEY,
                weight REAL,
                embedding VECTOR(3) AUTO_INDEX_AT 1000 HNSW (M = 8, EF_CONSTRUCTION = 200)
                    RANK_POLICY (
                        JOIN hnsw ON decision_id,
                        FORMULA 'coalesce({weight}, 0.0) * coalesce({search_mode}, 0.0)',
                        SORT_KEY graph_relevance
                    )
            ) SYNC TWO WAY",
            &empty(),
        )
        .expect(
            "declare docs with a real graph and crossover policy beside a rank-policy join naming lookalike identifiers",
        );
    let note = Uuid::from_u128(0xA900_0001);
    let joined_row = Uuid::from_u128(0xA900_0002);
    let doc = Uuid::from_u128(0xA900_0003);
    source
        .execute(
            "INSERT INTO ordinary_notes (id, body) VALUES ($id, 'control')",
            &params([("id", Value::Uuid(note))]),
        )
        .expect("write compatible control data");
    source
        .execute(
            "INSERT INTO hnsw (id, decision_id, search_mode) VALUES ($id, $decision_id, $search_mode)",
            &params([
                ("id", Value::Uuid(joined_row)),
                ("decision_id", Value::Uuid(doc)),
                ("search_mode", Value::Bool(true)),
            ]),
        )
        .expect("write the joined row");
    source
        .execute(
            "INSERT INTO docs (id, weight, embedding) VALUES ($id, $weight, $embedding)",
            &params([
                ("id", Value::Uuid(doc)),
                ("weight", Value::Float64(1.0)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("write the row carrying the real graph and crossover policy");
    source
        .execute(
            "ALTER TABLE docs ALTER COLUMN embedding SET HNSW DEFAULT",
            &empty(),
        )
        .expect("revert the graph policy to the compatible default");
    source
        .execute(
            "ALTER TABLE docs ALTER COLUMN embedding SET AUTO_INDEX_AT DEFAULT",
            &empty(),
        )
        .expect("revert the crossover policy to the compatible default");

    let (running, _) =
        start_server(&broker, REVERTED_GRAPH_AND_CROSSOVER_TENANT, source.clone()).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&receiver_node),
        subject: pull_subject(REVERTED_GRAPH_AND_CROSSOVER_TENANT),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(false),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        transport.clone(),
        TenantId::from(REVERTED_GRAPH_AND_CROSSOVER_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    client
        .pull_default()
        .await
        .expect("historical graph and crossover clauses remain held");

    assert!(receiver.table_meta("docs").is_none());
    assert!(!running.server.schema_sync_holdbacks().is_empty());
    let old_pages = transport.pages();
    for page in current_transport_pages(&old_pages) {
        assert_ddl_is_parseable_by_the_older_peer(&page.ddl, "before capability catchup");
    }
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("capability catchup replays immutable authored history");
    assert_authored_table_history(&source, &transport.pages(), "docs");
    let pages = old_pages;
    for page in current_transport_pages(&pages) {
        assert_ddl_is_parseable_by_the_older_peer(
            &page.ddl,
            "cold pull of a reverted graph and crossover policy",
        );
    }
    assert_eq!(running.server.schema_sync_holdbacks(), Vec::new());
    assert_schema_converged(
        &source,
        &receiver,
        "docs",
        "cold pull of a reverted graph and crossover policy",
    );
    assert_eq!(
        ids(receiver
            .execute(
                "SELECT id FROM docs ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
                &params([("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .expect("the row answers through the public vector door"),),
        vec![doc]
    );

    running.stop().await;
}

#[tokio::test]
async fn a_dropped_partitioned_table_keeps_its_authored_create_and_drop() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let receiver = Arc::new(Database::open_memory());
    declare_graph_controls_and_held_vector(&source);
    let control = Uuid::from_u128(0xAA00_0001);
    let dropped_row = Uuid::from_u128(0xAA00_0002);
    let scope = Uuid::from_u128(0xAA00_00FF);
    insert_node(&source, control);
    insert_vector_doc(&source, dropped_row, scope);
    source
        .execute("DROP TABLE vector_docs", &empty())
        .expect("drop the partitioned table before any old peer ever pulls");

    let (running, _) =
        start_server(&broker, DROPPED_PARTITIONED_TABLE_TENANT, source.clone()).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&receiver_node),
        subject: pull_subject(DROPPED_PARTITIONED_TABLE_TENANT),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(false),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        transport.clone(),
        TenantId::from(DROPPED_PARTITIONED_TABLE_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    client
        .pull_default()
        .await
        .expect("the dropped table remains held while compatible work reaches the cold peer");

    assert!(receiver.table_meta("vector_docs").is_none());
    assert!(!running.server.schema_sync_holdbacks().is_empty());
    let old_pages = transport.pages();
    for page in current_transport_pages(&old_pages) {
        assert_ddl_is_parseable_by_the_older_peer(&page.ddl, "before capability catchup");
    }
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("capability catchup replays immutable authored history");
    assert_authored_table_history(&source, &transport.pages(), "vector_docs");
    let pages = old_pages;
    for page in current_transport_pages(&pages) {
        assert_ddl_is_parseable_by_the_older_peer(
            &page.ddl,
            "cold pull replaying a dropped partitioned table",
        );
    }
    assert_eq!(running.server.schema_sync_holdbacks(), Vec::new());
    assert!(receiver.table_meta("vector_docs").is_none());
    assert_schema_converged(
        &source,
        &receiver,
        "nodes",
        "cold pull replaying a dropped partitioned table",
    );
    assert_eq!(
        ids(receiver.execute("SELECT id FROM nodes", &empty()).unwrap()),
        vec![control]
    );
    assert_old_cursor_names_only_the_page_it_served(&pages);

    running.stop().await;
}

#[tokio::test]
async fn authored_schema_holdback_survives_both_nodes_reopening_before_capability_catchup() {
    let root = tempfile::tempdir().unwrap();
    let source_path = root.path().join("source.db");
    let receiver_path = root.path().join("receiver.db");
    let tenant = "authored-schema-reopen";
    let source = Arc::new(Database::open(&source_path).unwrap());
    source
        .execute(
            "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &empty(),
        )
        .unwrap();
    source.execute("CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED) SYNC TWO WAY", &empty()).unwrap();
    source
        .execute("ALTER TABLE docs ADD COLUMN label TEXT", &empty())
        .unwrap();
    source
        .execute(
            "ALTER TABLE docs ALTER COLUMN embedding SET SEARCH_MODE AUTO",
            &empty(),
        )
        .unwrap();
    let note = Uuid::from_u128(0xBB01);
    source
        .execute(
            "INSERT INTO ordinary_notes (id, body) VALUES ($id, 'first')",
            &params([("id", Value::Uuid(note))]),
        )
        .unwrap();
    let broker = InProcessBroker::new();
    let (running, server_identity) = start_server(&broker, tenant, source.clone()).await;
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let receiver = Arc::new(Database::open(&receiver_path).unwrap());
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        broker.client_as(&node_id),
        TenantId::from(tenant),
        identity.clone(),
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&node_id, false);
    client.pull_default().await.unwrap();
    assert!(receiver.table_meta("docs").is_none());
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM ordinary_notes", &empty())
            .unwrap()),
        vec![note]
    );
    assert!(!running.server.schema_sync_holdbacks().is_empty());
    client.shutdown().await;
    drop(client);
    running.stop().await;
    receiver.close().unwrap();
    source.close().unwrap();
    drop(receiver);
    drop(source);
    drop(broker);

    let source = Arc::new(Database::open(&source_path).unwrap());
    let receiver = Arc::new(Database::open(&receiver_path).unwrap());
    let broker = InProcessBroker::new();
    let (running, _) =
        start_server_with_identity(&broker, tenant, source.clone(), server_identity).await;
    let transport = Arc::new(ObservePullPages {
        inner: broker.client_as(&node_id),
        subject: pull_subject(tenant),
        pages: Mutex::new(Vec::new()),
        cut_one_old_reply: AtomicBool::new(false),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        transport.clone(),
        TenantId::from(tenant),
        identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&node_id, false);
    let later_note = Uuid::from_u128(0xBB02);
    source
        .execute(
            "INSERT INTO ordinary_notes (id, body) VALUES ($id, 'later')",
            &params([("id", Value::Uuid(later_note))]),
        )
        .unwrap();
    client.pull_default().await.unwrap();
    assert!(receiver.table_meta("docs").is_none());
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM ordinary_notes ORDER BY id", &empty())
            .unwrap()),
        vec![note, later_note]
    );
    let old_pages = transport.pages();
    for page in current_transport_pages(&old_pages) {
        assert_ddl_is_parseable_by_the_older_peer(&page.ddl, "reopened unsupported peer");
    }
    running
        .server
        .set_peer_vector_schema_support_for_test(&node_id, true);
    client.pull_default().await.unwrap();
    assert_authored_table_history(&source, &transport.pages(), "docs");
    assert_schema_converged(&source, &receiver, "docs", "reopened capability catchup");
    assert!(running.server.schema_sync_holdbacks().is_empty());
    running.stop().await;
}
