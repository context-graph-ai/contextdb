//! A newer edge sending schema to an older hub owns the same per-table compatibility gap.
//!
//! Ordinary tables keep moving once while the adopting table is held locally. Upgrading the same
//! hub resumes that table without resetting the edge's durable push progress.

use contextdb_core::{Lsn, TenantId, Value};
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::protocol::{MessageType, PushRequest, WireDdlChange, decode};
use contextdb_engine::sync_types::{SchemaSyncCapability, SchemaSyncHoldback};
use contextdb_engine::{Database, QueryResult};
use contextdb_server::protocol::PROTOCOL_VERSION;
use contextdb_server::subjects::push_subject;
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use contextdb_server::{
    FabricIdentity, InProcessBroker, SyncClient, SyncServer, TransferDirection, TransferPlane,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const TENANT: &str = "vector-schema-mixed-version-push";
const REVERTED_PUSH_TENANT: &str = "vector-schema-mixed-version-push-reverted";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn text_column(result: QueryResult) -> Vec<String> {
    result
        .rows
        .into_iter()
        .map(|row| match row.into_iter().next() {
            Some(Value::Text(value)) => value,
            value => panic!("expected one text value, got {value:?}"),
        })
        .collect()
}

fn sent_sync_rows(client: &SyncClient, peer: &str) -> u64 {
    client
        .transfer_receipts()
        .into_iter()
        .find(|receipt| {
            receipt.peer_node_id == peer
                && receipt.plane == TransferPlane::Sync
                && receipt.direction == TransferDirection::Sent
        })
        .map(|receipt| receipt.counters.items)
        .unwrap_or_default()
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

#[derive(Debug, Clone, Default)]
struct RecordedPushRequest {
    version: u8,
    ddl: Vec<WireDdlChange>,
    ddl_provenance: Vec<contextdb_engine::protocol::WireDdlProvenance>,
}

/// Decode one outgoing push request envelope and append it to the witness.
/// Shared by both transport methods a push can travel through.
fn record_push_request(
    requests: &Mutex<Vec<RecordedPushRequest>>,
    request_bytes: &[u8],
) -> TransportResult<()> {
    let envelope = decode(request_bytes)
        .map_err(|error| TransportError::Other(format!("decode outgoing push request: {error}")))?;
    match envelope.message_type {
        MessageType::PushRequest | MessageType::DependencyCompletePushRequest => {
            let push: PushRequest = rmp_serde::from_slice(&envelope.payload)
                .map_err(|error| TransportError::Other(error.to_string()))?;
            requests
                .lock()
                .expect("push request witness lock")
                .push(RecordedPushRequest {
                    version: envelope.version,
                    ddl: push.changeset.ddl,
                    ddl_provenance: push.changeset.ddl_provenance,
                });
            Ok(())
        }
        other => Err(TransportError::Other(format!(
            "unexpected observed push request: {other:?}"
        ))),
    }
}

/// A read-only push-request witness. It decodes the outgoing request and
/// forwards it unchanged; it never fabricates a request or changes sync
/// state. It records the REQUEST, not the reply, so no reply shape is
/// assumed.
struct ObservePushRequests {
    inner: Arc<dyn ClientTransport>,
    subject: String,
    requests: Mutex<Vec<RecordedPushRequest>>,
}

impl ObservePushRequests {
    fn requests(&self) -> Vec<RecordedPushRequest> {
        self.requests
            .lock()
            .expect("push request witness lock")
            .clone()
    }
}

impl ClientTransport for ObservePushRequests {
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
        let requests = &self.requests;
        Box::pin(async move {
            if observe {
                record_push_request(requests, &request_bytes)?;
            }
            inner.request(subject, request_bytes, timeout).await
        })
    }

    // `request_push` (`sync_client.rs`) sends a retry-safe push through THIS
    // method, not `request`; only the non-retry-safe fallback uses `request`.
    // Both must be observed or a retry-safe push (the common case) records no
    // requests at all.
    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let inner = self.inner.clone();
        let observe = subject == self.subject;
        let requests = &self.requests;
        Box::pin(async move {
            if observe {
                record_push_request(requests, &request_bytes)?;
            }
            inner
                .request_single_reply(subject, request_bytes, timeout)
                .await
        })
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

#[tokio::test]
async fn newer_edge_keeps_ordinary_tables_flowing_then_resumes_the_held_table_after_hub_upgrade() {
    let broker = InProcessBroker::new();
    let edge = Arc::new(Database::open_memory());
    let hub = Arc::new(Database::open_memory());
    let first_note = Uuid::from_u128(0xD001);
    let second_note = Uuid::from_u128(0xD002);
    let vector_id = Uuid::from_u128(0xD003);
    let scope_id = Uuid::from_u128(0xD004);

    edge.execute(
        "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT NOT NULL) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare the table both nodes understand");
    edge.execute(
        "CREATE TABLE partitioned_notes (id UUID PRIMARY KEY, scope_id UUID NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id)) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare the table that needs the newer schema capability");
    edge.execute(
        "INSERT INTO ordinary_notes (id, body) VALUES ($id, $body)",
        &params([
            ("id", Value::Uuid(first_note)),
            ("body", Value::Text("first".to_string())),
        ]),
    )
    .expect("write the first ordinary row");
    edge.execute(
        "INSERT INTO partitioned_notes (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(vector_id)),
            ("scope", Value::Uuid(scope_id)),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("write the held table's row");

    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub.clone(),
            broker.server_as(&hub_node_id),
            TenantId::from(TENANT),
            hub_node_id.clone(),
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
        .wait_for_registered_route_for_test(&push_subject(TENANT))
        .await;

    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_node_id),
        TenantId::from(TENANT),
        edge_identity,
    );
    client.set_peer_vector_schema_support_for_test(false);

    client
        .push()
        .await
        .expect("newer vocabulary must not refuse the whole push connection");
    assert_eq!(
        text_column(
            hub.execute("SELECT body FROM ordinary_notes ORDER BY body", &empty())
                .expect("the ordinary table and first row arrive")
        ),
        vec!["first".to_string()]
    );
    assert!(
        hub.table_meta("partitioned_notes").is_none(),
        "the older hub parser must never be handed SQL words it cannot understand"
    );
    assert_eq!(
        client.schema_sync_holdbacks(),
        vec![SchemaSyncHoldback {
            table: "partitioned_notes".to_string(),
            capability: SchemaSyncCapability::VectorPartitioning,
            node_to_upgrade: hub_node_id.clone(),
        }]
    );

    edge.execute(
        "INSERT INTO ordinary_notes (id, body) VALUES ($id, $body)",
        &params([
            ("id", Value::Uuid(second_note)),
            ("body", Value::Text("second".to_string())),
        ]),
    )
    .expect("write ordinary work while the other table remains held");
    client
        .push()
        .await
        .expect("the unaffected table keeps syncing during the version gap");
    assert_eq!(
        text_column(
            hub.execute("SELECT body FROM ordinary_notes ORDER BY body", &empty())
                .expect("both ordinary rows are readable")
        ),
        vec!["first".to_string(), "second".to_string()]
    );
    assert_eq!(sent_sync_rows(&client, &hub_node_id), 2);

    client.set_peer_vector_schema_support_for_test(true);
    client
        .push()
        .await
        .expect("upgrading the hub resumes the held table without resetting push progress");
    assert!(hub.table_meta("partitioned_notes").is_some());
    let vector_rows = hub
        .execute("SELECT id FROM partitioned_notes", &empty())
        .expect("the held row arrives with its declaration");
    assert_eq!(vector_rows.rows, vec![vec![Value::Uuid(vector_id)]]);
    assert!(client.schema_sync_holdbacks().is_empty());

    drop(client);
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("server task stops");
}

#[tokio::test]
async fn a_newer_edge_holds_authored_history_until_the_hub_can_replay_it() {
    let broker = InProcessBroker::new();
    let edge = Arc::new(Database::open_memory());
    let hub = Arc::new(Database::open_memory());
    let note = Uuid::from_u128(0xD100);
    let doc = Uuid::from_u128(0xD101);

    edge.execute(
        "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT NOT NULL) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare the compatible control table");
    edge.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare docs with a protocol-7-only search mode");
    edge.execute(
        "INSERT INTO ordinary_notes (id, body) VALUES ($id, $body)",
        &params([
            ("id", Value::Uuid(note)),
            ("body", Value::Text("control".to_string())),
        ]),
    )
    .expect("write the control row");
    edge.execute(
        "INSERT INTO docs (id, embedding) VALUES ($id, $embedding)",
        &params([
            ("id", Value::Uuid(doc)),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("write the row before the revert");
    edge.execute(
        "ALTER TABLE docs ALTER COLUMN embedding SET SEARCH_MODE AUTO",
        &empty(),
    )
    .expect("revert to the compatible default before any push to the older hub");

    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub.clone(),
            broker.server_as(&hub_node_id),
            TenantId::from(REVERTED_PUSH_TENANT),
            hub_node_id.clone(),
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
        .wait_for_registered_route_for_test(&push_subject(REVERTED_PUSH_TENANT))
        .await;

    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let transport = Arc::new(ObservePushRequests {
        inner: broker.client_as(&edge_node_id),
        subject: push_subject(REVERTED_PUSH_TENANT),
        requests: Mutex::new(Vec::new()),
    });
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        transport.clone(),
        TenantId::from(REVERTED_PUSH_TENANT),
        edge_identity,
    );
    client.set_peer_vector_schema_support_for_test(false);

    client
        .push()
        .await
        .expect("only compatible work reaches the older hub");

    let requests = transport.requests();
    for request in requests
        .iter()
        .filter(|request| request.version == PROTOCOL_VERSION)
    {
        assert_ddl_is_parseable_by_the_older_peer(
            &request.ddl,
            "push of a reverted declaration to an older hub",
        );
    }
    assert!(!client.schema_sync_holdbacks().is_empty());
    assert!(hub.table_meta("docs").is_none());
    assert_eq!(sent_sync_rows(&client, &hub_node_id), 1);
    client.set_peer_vector_schema_support_for_test(true);
    client
        .push()
        .await
        .expect("an upgraded hub receives exact authored history");
    assert!(client.schema_sync_holdbacks().is_empty());
    let original = edge.changes_since(Lsn(0));
    let schema = contextdb_engine::sync_types::ChangeSet {
        ddl: original.ddl,
        ddl_lsn: original.ddl_lsn,
        ..Default::default()
    };
    let (_, provenance) = edge
        .outbound_row_lineages_and_ddl_provenance_for_test(
            &schema,
            &TenantId::from(REVERTED_PUSH_TENANT),
            &edge_node_id,
            contextdb_core::Incarnation::mint(),
            &|_| panic!("DDL witness cannot sign row lineage"),
        )
        .unwrap();
    let expected = schema
        .ddl
        .into_iter()
        .map(WireDdlChange::from)
        .zip(provenance)
        .filter(|(_, entry)| entry.table.as_deref() == Some("docs"))
        .collect::<Vec<_>>();
    let observed = transport
        .requests()
        .into_iter()
        .flat_map(|request| request.ddl.into_iter().zip(request.ddl_provenance))
        .filter(|(_, entry)| entry.table.as_deref() == Some("docs"))
        .collect::<Vec<_>>();
    assert_eq!(
        observed, expected,
        "push preserves exact authored DDL, order and all provenance fields"
    );
    assert_eq!(
        text_column(
            hub.execute("SELECT body FROM ordinary_notes", &empty())
                .expect("the control table and row arrive")
        ),
        vec!["control".to_string()]
    );
    assert_eq!(
        hub.execute("SELECT id FROM docs", &empty())
            .expect("the released vector row arrives")
            .rows,
        vec![vec![Value::Uuid(doc)]]
    );
    assert_schema_converged(
        &edge,
        &hub,
        "docs",
        "push of a reverted declaration to an older hub",
    );
    assert_eq!(sent_sync_rows(&client, &hub_node_id), 2);

    drop(client);
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("server task stops");
}
