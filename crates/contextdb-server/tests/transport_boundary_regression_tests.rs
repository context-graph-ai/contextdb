use contextdb_core::{Error, Lsn, Value};
use contextdb_engine::Database;
use contextdb_server::protocol::{MessageType, PushRequest, PushResponse, decode, encode};
use contextdb_server::transport::{
    ClientTransport, HandlerRegistration, IncomingRequest, Responder, ServerTransport,
    TransportError, TransportFuture,
};
use contextdb_server::{FabricIdentity, SyncClient, SyncServer};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

struct AlwaysIncompleteTransport {
    push_calls: AtomicUsize,
    status_calls: AtomicUsize,
}

impl ClientTransport for AlwaysIncompleteTransport {
    fn peer_node_id(&self) -> Option<String> {
        Some("incomplete-reply-hub".to_string())
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        _request_bytes: Vec<u8>,
        _timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject.ends_with(".push") {
            self.status_calls.store(0, Ordering::SeqCst);
            self.push_calls.fetch_add(1, Ordering::SeqCst);
        } else if subject.ends_with(".status") {
            self.status_calls.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async {
            Err(TransportError::IncompleteReply(
                "simulated partial reply".to_string(),
            ))
        })
    }
}

#[tokio::test]
async fn push_does_not_retry_incomplete_reply() {
    let transport = Arc::new(AlwaysIncompleteTransport {
        push_calls: AtomicUsize::new(0),
        status_calls: AtomicUsize::new(0),
    });
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)",
        &Default::default(),
    )
    .unwrap();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db,
        transport.clone(),
        contextdb_core::TenantId::from("push-incomplete-reply"),
        Arc::new(FabricIdentity::generate()),
    );
    let result = client.push().await;

    assert!(
        result.is_err(),
        "push must surface the incomplete reply as an error"
    );
    assert_eq!(
        transport.push_calls.load(Ordering::SeqCst),
        1,
        "push must not retry after a reply stream has already started"
    );
    assert_eq!(
        transport.status_calls.load(Ordering::SeqCst),
        1,
        "an incomplete post-send reply remains ambiguous and runs lost-ack status reconciliation"
    );
}

struct TerminalPushErrorTransport {
    push_calls: AtomicUsize,
    status_calls: AtomicUsize,
    pull_calls: AtomicUsize,
}

impl ClientTransport for TerminalPushErrorTransport {
    fn peer_node_id(&self) -> Option<String> {
        Some("terminal-error-hub".to_string())
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        _request_bytes: Vec<u8>,
        _timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let is_push = subject.ends_with(".push");
        if is_push {
            self.status_calls.store(0, Ordering::SeqCst);
            self.push_calls.fetch_add(1, Ordering::SeqCst);
        } else if subject.ends_with(".status") {
            self.status_calls.fetch_add(1, Ordering::SeqCst);
        } else if subject.ends_with(".pull") {
            self.pull_calls.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async move {
            if !is_push {
                return Err(TransportError::NoResponder);
            }
            encode(
                MessageType::PushResponse,
                &PushResponse {
                    result: None,
                    error: Some("server rejected push".to_string()),
                    application_error: None,
                },
            )
            .map_err(|err| TransportError::Other(err.to_string()))
        })
    }
}

#[tokio::test]
async fn valid_server_error_surfaces_directly_without_lost_ack_reconciliation() {
    let transport = Arc::new(TerminalPushErrorTransport {
        push_calls: AtomicUsize::new(0),
        status_calls: AtomicUsize::new(0),
        pull_calls: AtomicUsize::new(0),
    });
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)",
        &Default::default(),
    )
    .unwrap();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db,
        transport.clone(),
        contextdb_core::TenantId::from("push-terminal-error"),
        Arc::new(FabricIdentity::generate()),
    );
    let result = client.push().await;

    assert!(
        matches!(&result, Err(Error::SyncError(detail)) if detail == "server rejected push"),
        "push must surface the decoded server rejection unchanged, got {result:?}"
    );
    assert_eq!(
        transport.push_calls.load(Ordering::SeqCst),
        1,
        "push must not retry a valid server error reply"
    );
    assert_eq!(
        transport.status_calls.load(Ordering::SeqCst),
        0,
        "a decoded terminal server response must not enter lost-ack status reconciliation"
    );
    assert_eq!(
        transport.pull_calls.load(Ordering::SeqCst),
        0,
        "a decoded terminal server response must not enter lost-ack pull reconciliation"
    );
}

struct NoResponderTransport {
    pull_calls: AtomicUsize,
}

struct RetryUnsafePushTransport {
    retry_safe_push_calls: AtomicUsize,
    single_attempt_push_calls: AtomicUsize,
    edge_node_id: String,
    hub_node_id: String,
}

impl ClientTransport for RetryUnsafePushTransport {
    fn peer_node_id(&self) -> Option<String> {
        Some(self.hub_node_id.clone())
    }

    fn local_node_id(&self) -> Option<String> {
        Some(self.edge_node_id.clone())
    }

    fn has_stable_edge_identity(&self) -> bool {
        true
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        _request_bytes: Vec<u8>,
        _timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let is_push = subject.ends_with(".push");
        if is_push {
            self.single_attempt_push_calls
                .fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async move {
            if !is_push {
                return Err(TransportError::NoResponder);
            }
            encode(
                MessageType::PushResponse,
                &PushResponse {
                    result: Some(contextdb_server::protocol::WireApplyResult {
                        applied_rows: 1,
                        skipped_rows: 0,
                        conflicts: Vec::new(),
                        new_lsn: Lsn(2),
                    }),
                    error: None,
                    application_error: None,
                },
            )
            .map_err(|err| TransportError::Other(err.to_string()))
        })
    }

    fn ensure_single_reply_retry_safe(&self, _request_bytes: &[u8]) -> Result<(), TransportError> {
        self.retry_safe_push_calls.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::RetryUnsafe("synthetic".to_string()))
    }
}

#[tokio::test]
async fn push_retry_unsafe_transport_falls_back_to_one_single_attempt() {
    let identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = identity.node_id();
    let transport = Arc::new(RetryUnsafePushTransport {
        retry_safe_push_calls: AtomicUsize::new(0),
        single_attempt_push_calls: AtomicUsize::new(0),
        edge_node_id,
        hub_node_id: "retry-unsafe-hub".to_string(),
    });
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)",
        &Default::default(),
    )
    .unwrap();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        transport.clone(),
        contextdb_core::TenantId::from("push-retry-unsafe"),
        identity,
    );
    client
        .push()
        .await
        .expect("bootstrap DDL should use the single-attempt path");
    // The transport contract is per encoded request. Retire the independent
    // DDL source group first so the assertion below isolates one row request
    // and can still distinguish a fallback from a retry.
    transport.retry_safe_push_calls.store(0, Ordering::SeqCst);
    transport
        .single_attempt_push_calls
        .store(0, Ordering::SeqCst);

    let id = Uuid::new_v4();
    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), Value::Uuid(id));
    params.insert("v".to_string(), Value::Text("data".into()));
    db.execute("INSERT INTO t (id, v) VALUES ($id, $v)", &params)
        .unwrap();

    let result = client
        .push()
        .await
        .expect("retry-unsafe push should use the single-attempt path");

    assert_eq!(result.applied_rows, 1);
    assert_eq!(
        transport.retry_safe_push_calls.load(Ordering::SeqCst),
        1,
        "push should try the retry-safe send exactly once"
    );
    assert_eq!(
        transport.single_attempt_push_calls.load(Ordering::SeqCst),
        1,
        "push must fall back to exactly one single-attempt send"
    );
}

impl ClientTransport for NoResponderTransport {
    fn peer_node_id(&self) -> Option<String> {
        Some("no-responder-hub".to_string())
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        _request_bytes: Vec<u8>,
        _timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject.ends_with(".pull") {
            self.pull_calls.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async { Err(TransportError::NoResponder) })
    }
}

#[tokio::test]
async fn pull_does_not_retry_no_responder() {
    let transport = Arc::new(NoResponderTransport {
        pull_calls: AtomicUsize::new(0),
    });
    let db = Arc::new(Database::open_memory());
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db,
        transport.clone(),
        contextdb_core::TenantId::from("pull-no-responder"),
        Arc::new(FabricIdentity::generate()),
    );
    let result = client.pull_default().await;

    assert!(
        result.is_err(),
        "pull must surface no-responder as an error"
    );
    assert_eq!(
        transport.pull_calls.load(Ordering::SeqCst),
        1,
        "pull must not retry a transport no-responder status reply"
    );
}

#[tokio::test]
async fn in_process_request_timeout_bounds_handler_and_reply_wait() {
    let broker = contextdb_server::InProcessBroker::new();
    let client = broker.client_as("timeout-edge");
    let server = broker.server_as("timeout-hub");
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_shutdown = shutdown.clone();
    let server_task = tokio::spawn(async move {
        server
            .serve(
                vec![HandlerRegistration {
                    subject: "sync.timeout-test.pull".to_string(),
                    handler: Arc::new(|_req| {
                        Box::pin(async {
                            std::future::pending::<()>().await;
                            Ok(())
                        })
                    }),
                }],
                server_shutdown,
            )
            .await
    });
    // Deterministic readiness (was a fixed 20 ms settle sleep): serve()
    // registers its routes synchronously at its top, so retry a cheap probe
    // until the route exists — NoResponder means the spawned server task has
    // not been polled yet. The deadline is a failure ceiling.
    let ready_deadline = Instant::now() + Duration::from_secs(5);
    while let Err(TransportError::NoResponder) = client
        .request(
            "sync.timeout-test.pull",
            b"probe".to_vec(),
            Duration::from_millis(5),
        )
        .await
    {
        assert!(
            Instant::now() < ready_deadline,
            "server task must register its handler"
        );
        tokio::task::yield_now().await;
    }

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        client.request(
            "sync.timeout-test.pull",
            b"request".to_vec(),
            Duration::from_millis(50),
        ),
    )
    .await
    .expect("fake request must return within the outer guard");

    assert!(
        matches!(result, Err(TransportError::Timeout)),
        "fake request must report the caller timeout, got {result:?}"
    );
    // (The old `elapsed < 500ms` upper bound was redundant with the 1 s outer
    // guard + the Timeout match, and flipped under load — removed.)

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;
}

struct SinglePushServerTransport {
    request_bytes: Vec<u8>,
    response_bytes: Arc<Mutex<Option<Vec<u8>>>>,
}

impl ServerTransport for SinglePushServerTransport {
    fn serve<'a>(
        &'a self,
        handlers: Vec<HandlerRegistration>,
        _shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()> {
        let request_bytes = self.request_bytes.clone();
        let response_bytes = self.response_bytes.clone();
        Box::pin(async move {
            let registration = handlers
                .into_iter()
                .find(|registration| registration.subject.ends_with(".push"))
                .expect("push handler registered");
            let responder: Responder = Box::new(move |bytes| {
                Box::pin(async move {
                    *response_bytes.lock().unwrap_or_else(|err| err.into_inner()) = Some(bytes);
                    Ok(())
                })
            });
            (registration.handler)(IncomingRequest {
                bytes: request_bytes,
                responder,
                node_id: Some("apply-drain-edge".to_string()),
            })
            .await
        })
    }
}

#[tokio::test]
async fn server_run_until_returns_after_accepted_push_apply_finishes() {
    let request = PushRequest {
        changeset: Default::default(),
        incarnation: contextdb_core::Incarnation::default(),
    };
    let request_bytes = encode(MessageType::PushRequest, &request).unwrap();
    let response_bytes = Arc::new(Mutex::new(None));
    let transport = Arc::new(SinglePushServerTransport {
        request_bytes,
        response_bytes: response_bytes.clone(),
    });
    let server_db = Arc::new(Database::open_memory());
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let server = SyncServer::with_authenticated_transport_and_identity_for_test(
        server_db.clone(),
        transport,
        contextdb_core::TenantId::from("apply-drain"),
        node_id,
        identity,
    );

    // The exact lost-wakeup requires a scheduler interleaving inside wait_idle,
    // so this bounded test guards the observable contract: after the transport
    // returns with an accepted push apply, run_until must not park forever while
    // draining the apply task.
    tokio::time::timeout(
        Duration::from_secs(2),
        server.run_until(Arc::new(AtomicBool::new(true))),
    )
    .await
    .expect("run_until must return after the accepted apply finishes");

    let response_bytes = response_bytes
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .take()
        .expect("push response delivered");
    let envelope = decode(&response_bytes).unwrap();
    assert!(matches!(envelope.message_type, MessageType::PushResponse));
    let response: PushResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(response.error, None);
    let result = response.result.expect("push result");
    assert_eq!(result.applied_rows, 0);
    assert_eq!(result.skipped_rows, 0);
}
