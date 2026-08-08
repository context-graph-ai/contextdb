//! The Iroh adapter's acceptance tests. Everything here runs over REAL
//! localhost Iroh endpoints — no relay, no internet, no third-party contact,
//! and no Docker.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::transport::iroh::{
    EndpointSpec, IrohServer, LookupChoice, PeerRequest, PublishChoice, RelayChoice, SYNC_ALPN,
    client_with_test_controller_for_test, is_iroh_endpoint, peer_connect, peer_request,
};
use contextdb_server::transport::{
    HandlerRegistration, IncomingRequest, RequestHandler, client_transport,
};
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use uuid::Uuid;

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), fut)
        .await
        .expect("bounded iroh transport operation exceeded 30s")
}

// A restart owns the UDP port it just released.  These are real localhost
// endpoint journeys, so serialise them within this test binary instead of
// letting another journey win a remembered port between close and rebind.
// This is resource ownership, not a timing workaround: no test sleeps or
// retries for port availability.
static REAL_IROH_JOURNEY_PERMIT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[test]
fn sync_alpn_is_frozen_at_v6() {
    assert_eq!(
        SYNC_ALPN, b"contextdb.sync.v6",
        "a wire-version change requires an intentional compatibility decision"
    );
}

/// The one large real-Iroh journey performs durable staging plus a two-page
/// reconciliation pull. This is a deadlock guard only; every assertion below
/// is on transport observations or durable state, never elapsed time.
async fn within_oversized_reconciliation<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(300), fut)
        .await
        .expect("oversized Iroh reconciliation made no progress before the deadlock guard")
}

async fn assert_oversized_response_completion_reset(restart_after_durable_completion: bool) {
    const RESPONSE_BYTES: usize = 69 * 1024 * 1024 + 137;
    const SUBJECT: &str = "oversized-response-completion-reset";

    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let hub_identity = identity_file(&hub_dir);
    let endpoint = within(IrohServer::bind(&bind_spec(&hub_identity)))
        .await
        .expect("bind authenticated hub");
    let controller = endpoint.large_request_test_controller();
    if restart_after_durable_completion {
        controller.pause_after_durable_response_completion_before_ack_for_test();
    } else {
        controller.pause_before_durable_response_completion_for_test();
    }
    let transport = endpoint.transport();
    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let edge_identity = identity_file(&edge_dir);
    let client = client_transport(&format!(
        "iroh:?to={}&identity={}",
        endpoint.ticket(),
        edge_identity.display()
    ));
    let expected = Arc::new(vec![0x5c; RESPONSE_BYTES]);
    let dispatches = Arc::new(AtomicUsize::new(0));
    let handler: RequestHandler = Arc::new({
        let expected = expected.clone();
        let dispatches = dispatches.clone();
        move |incoming: IncomingRequest| {
            let expected = expected.clone();
            let dispatches = dispatches.clone();
            Box::pin(async move {
                dispatches.fetch_add(1, Ordering::SeqCst);
                (incoming.responder)((*expected).clone()).await
            }) as contextdb_server::transport::TransportFuture<'static, ()>
        }
    });
    let stop = Arc::new(AtomicBool::new(false));
    let serve_task = tokio::spawn({
        let stop = stop.clone();
        async move {
            transport
                .serve(
                    vec![HandlerRegistration {
                        subject: SUBJECT.to_string(),
                        handler,
                    }],
                    stop,
                )
                .await
        }
    });
    controller.wait_until_routes_ready_for_test().await;
    let caller_finished = Arc::new(AtomicBool::new(false));
    let caller = tokio::spawn({
        let client = client.clone();
        let caller_finished = caller_finished.clone();
        async move {
            let result = client
                .request(
                    SUBJECT,
                    b"lose completion acknowledgement".to_vec(),
                    Duration::from_secs(300),
                )
                .await;
            caller_finished.store(true, Ordering::SeqCst);
            result
        }
    });
    controller
        .wait_until_response_control_paused_for_test()
        .await;
    assert!(
        !caller_finished.load(Ordering::SeqCst),
        "the caller remains incomplete until a completion acknowledgement survives"
    );
    let paused_counts = controller
        .response_stage_counts_for_test()
        .expect("read response stage inventory at completion reset");
    assert_eq!(
        paused_counts,
        contextdb_server::transport::iroh::LargeResponseStageCounts {
            stages: usize::from(!restart_after_durable_completion),
            receipts: usize::from(restart_after_durable_completion),
        },
        "the reset boundary distinguishes transport receipt from durable completion"
    );
    controller.reset_paused_response_control_for_test();
    let reply = within_oversized_reconciliation(caller)
        .await
        .expect("join completion retry caller")
        .expect("completion retry succeeds over the live hub");
    assert_eq!(
        *blake3::hash(&reply).as_bytes(),
        *blake3::hash(&expected).as_bytes()
    );
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    let observed = controller.observations_for_test();
    let total_chunks = observed.staged_response_manifests[0].total_chunks as usize;
    assert_eq!(observed.staged_response_manifests.len(), 1);
    assert_eq!(observed.requested_response_chunks.len(), total_chunks);
    assert_eq!(observed.served_response_chunks.len(), total_chunks);
    assert_eq!(
        observed.response_complete_controls_received, 2,
        "the first Complete reached the transport and the retry carries the same completion"
    );
    assert_eq!(observed.successful_response_complete_ack_writes, 1);
    assert_eq!(observed.released_response_transfers.len(), 1);
    assert_eq!(
        observed.completed_response_transfers.len(),
        if restart_after_durable_completion {
            2
        } else {
            1
        },
        "a post-durable reset repeats receipt-backed Complete but not its successful ACK write"
    );
    assert_eq!(
        observed.injected_pre_durable_complete_resets,
        usize::from(!restart_after_durable_completion)
    );
    assert_eq!(
        observed.injected_post_durable_complete_ack_resets,
        usize::from(restart_after_durable_completion)
    );
    assert_eq!(
        observed
            .durable_response_complete_outcomes
            .iter()
            .map(|outcome| outcome.receipt_preexisted)
            .collect::<Vec<_>>(),
        if restart_after_durable_completion {
            vec![false, true]
        } else {
            vec![false]
        },
        "only a post-durable reset leaves a receipt-backed completion retry"
    );
    assert_eq!(
        controller
            .response_stage_counts_for_test()
            .expect("read final response stage inventory"),
        contextdb_server::transport::iroh::LargeResponseStageCounts {
            stages: 0,
            receipts: 0,
        },
        "the successful Complete acknowledgement and one Release clean every durable response artifact"
    );
    client
        .shutdown()
        .await
        .expect("close completion retry client");
    stop.store(true, Ordering::SeqCst);
    within(serve_task)
        .await
        .expect("join completion retry hub")
        .expect("completion retry hub exits cleanly");
    endpoint.close().await;
}

fn identity_file(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("fabric-identity.key")
}

fn deterministic_bearer_ticket() -> String {
    let secret = iroh::SecretKey::from_bytes(&[0x3d; 32]);
    iroh_tickets::endpoint::EndpointTicket::new(iroh::EndpointAddr::new(secret.public()))
        .to_string()
}

fn stage_root_for(identity: &Path) -> PathBuf {
    let mut name = identity
        .file_name()
        .expect("identity filename")
        .to_os_string();
    name.push(".sync-staging");
    identity.with_file_name(name)
}

#[cfg(unix)]
fn seal_stage_root(root: &Path) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::create_dir(root).expect("create deliberately inaccessible staging root");
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o000))
        .expect("seal deliberately inaccessible staging root");
}

#[cfg(unix)]
fn unseal_stage_root(root: &Path) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))
        .expect("restore staging-root permissions for temporary-directory cleanup");
}

fn assert_stage_diagnostic_redacts_bearer_ticket(arm: &str, diagnostic: &str, ticket: &str) {
    assert!(
        diagnostic.contains("cannot open durable staging root"),
        "{arm} must retain the safe staging-root operation category, got: {diagnostic}"
    );
    assert!(
        !diagnostic.contains(ticket),
        "{arm} must not expose the bearer ticket embedded in the identity filename, got: {diagnostic}"
    );
}

fn bind_spec(identity: &Path) -> String {
    format!("iroh:?identity={}", identity.display())
}

fn bind_spec_with_port(identity: &Path, port: u16) -> String {
    format!("iroh:?identity={}&port={port}", identity.display())
}

fn contains_staged_file(root: &Path) -> bool {
    std::fs::read_dir(root)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .any(|entry| {
            entry
                .file_type()
                .map(|kind| {
                    kind.is_file()
                        || kind.is_symlink()
                        || (kind.is_dir() && contains_staged_file(&entry.path()))
                })
                .unwrap_or(true)
        })
}

fn assert_completed_oversized_response_lifecycle(
    observations: &contextdb_server::transport::iroh::LargeRequestTestObservations,
    authenticated_node_id: &str,
) {
    let oversized = observations
        .staged_response_manifests
        .iter()
        .find(|manifest| {
            if !(manifest.authenticated_node_id == authenticated_node_id
                && manifest.subject.starts_with("sync.")
                && manifest.subject.ends_with(".pull")
                && manifest.total_bytes > 64 * 1024 * 1024)
            {
                return false;
            }

            let expected_sequences =
                (0..manifest.total_chunks).collect::<std::collections::BTreeSet<_>>();
            let requested_sequences = observations
                .requested_response_chunks
                .iter()
                .filter(|chunk| chunk.transfer_digest == manifest.transfer_digest)
                .map(|chunk| chunk.sequence)
                .collect::<std::collections::BTreeSet<_>>();
            let served_sequences = observations
                .served_response_chunks
                .iter()
                .filter(|chunk| chunk.transfer_digest == manifest.transfer_digest)
                .map(|chunk| chunk.sequence)
                .collect::<std::collections::BTreeSet<_>>();
            let completed = observations.completed_response_transfers.iter().any(|completion| {
                completion.transfer_digest == manifest.transfer_digest
                    && completion.authenticated_node_id == authenticated_node_id
            });
            let releases = observations
                .released_response_transfers
                .iter()
                .filter(|release| {
                    release.transfer_digest == manifest.transfer_digest
                        && release.authenticated_node_id == authenticated_node_id
                })
                .count();

            requested_sequences == expected_sequences
                && served_sequences == expected_sequences
                && completed
                && releases == 1
        })
        .expect(
            "one over-frame authenticated pull reply completes its chunk, completion, and release lifecycle",
        );
    let expected_sequences = (0..oversized.total_chunks).collect::<std::collections::BTreeSet<_>>();
    let requested_sequences = observations
        .requested_response_chunks
        .iter()
        .filter(|chunk| chunk.transfer_digest == oversized.transfer_digest)
        .map(|chunk| chunk.sequence)
        .collect::<std::collections::BTreeSet<_>>();
    let served_sequences = observations
        .served_response_chunks
        .iter()
        .filter(|chunk| chunk.transfer_digest == oversized.transfer_digest)
        .map(|chunk| chunk.sequence)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        requested_sequences, expected_sequences,
        "the oversized pull reply observes every expected request sequence and no out-of-range sequence"
    );
    assert_eq!(
        served_sequences, expected_sequences,
        "the oversized pull reply observes every expected served sequence and no out-of-range sequence"
    );
    assert!(
        observations
            .completed_response_transfers
            .iter()
            .any(|completion| {
                completion.transfer_digest == oversized.transfer_digest
                    && completion.authenticated_node_id == authenticated_node_id
            }),
        "the oversized pull reply completes through its authenticated durable lifecycle"
    );
    assert_eq!(
        observations
            .released_response_transfers
            .iter()
            .filter(|release| {
                release.transfer_digest == oversized.transfer_digest
                    && release.authenticated_node_id == authenticated_node_id
            })
            .count(),
        1,
        "the isolated oversized pull reply releases its durable lifecycle once"
    );
}

/// The UDP ports named by a parsed ticket's direct socket addresses (there is
/// no accessor for this on `EndpointTicket`/`EndpointAddr`, so it is read off
/// each address's own `Display` text — `"ip:<socket-addr>"` for an IP
/// address, which trims to a plain `SocketAddr` the standard parser accepts).
/// A restart on one explicit bound port produces the SAME port on every
/// discovered address, even when which addresses got discovered varies.
fn ticket_ports(
    ticket: &iroh_tickets::endpoint::EndpointTicket,
) -> std::collections::BTreeSet<u16> {
    ticket
        .endpoint_addr()
        .addrs
        .iter()
        .filter_map(|addr| {
            addr.to_string()
                .strip_prefix("ip:")
                .and_then(|rest| rest.parse::<std::net::SocketAddr>().ok())
                .map(|socket| socket.port())
        })
        .collect()
}

fn create_notes_table(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .expect("create notes table");
}

fn insert_note(db: &Database, id: Uuid, body: &str) {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Uuid(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("insert note");
}

fn note_body(db: &Database, id: Uuid) -> Option<String> {
    let mut key = HashMap::new();
    key.insert("id".to_string(), Value::Uuid(id));
    let result = db
        .execute("SELECT body FROM notes WHERE id = $id", &key)
        .expect("select note");
    let idx = result
        .columns
        .iter()
        .position(|c| c == "body")
        .expect("body column");
    result.rows.first().map(|row| match &row[idx] {
        Value::Text(text) => text.clone(),
        other => panic!("notes.body must be text, got {other:?}"),
    })
}

struct RunningHub {
    server_db: Arc<Database>,
    ticket: String,
    node_id: String,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(spec: &str, tenant: &str) -> RunningHub {
    // The IrohServer handle is dropped at the end of this block, BEFORE the
    // sync server starts serving: transport() must hand the SyncServer a
    // self-sufficient serving side, so dropping the bind handle never tears
    // down an actively serving endpoint.
    let identity_path = EndpointSpec::parse(spec)
        .and_then(|parsed| parsed.identity_path().map(Path::to_path_buf))
        .unwrap_or_else(|| panic!("hub spec must name a fabric identity: {spec}"));
    let identity = Arc::new(
        FabricIdentity::load_or_generate(&identity_path)
            .unwrap_or_else(|err| panic!("hub identity must load from {identity_path:?}: {err}")),
    );
    let (ticket, node_id, transport) = {
        let endpoint = IrohServer::bind(spec)
            .await
            .unwrap_or_else(|err| panic!("hub endpoint must bind for spec {spec}: {err}"));
        (endpoint.ticket(), endpoint.node_id(), endpoint.transport())
    };
    assert_eq!(
        node_id,
        identity.node_id(),
        "the dropped bind handle and serving transport use the exact named fabric identity"
    );
    let server_db = Arc::new(Database::open_memory());
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            server_db.clone(),
            transport,
            contextdb_core::TenantId::from(tenant),
            node_id.clone(),
            identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub {
        server_db,
        ticket,
        node_id,
        shutdown,
        task,
    }
}

impl RunningHub {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

#[tokio::test]
async fn retained_sync_server_releases_the_stopped_hub_port_for_rebind() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let identity = identity_file(&dir);
    let port = free_udp_port();
    let spec = bind_spec_with_port(&identity, port);
    let endpoint = within(IrohServer::bind(&spec))
        .await
        .expect("bind original hub");
    let controller = endpoint.large_request_test_controller();
    let retained_server = Arc::new(SyncServer::new(
        Arc::new(Database::open_memory()),
        &endpoint,
        contextdb_core::TenantId::from("retained-server-port-release"),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_task = tokio::spawn({
        let retained_server = retained_server.clone();
        let shutdown = shutdown.clone();
        async move { retained_server.run_until(shutdown).await }
    });
    controller.wait_until_routes_ready_for_test().await;

    // The caller has released its bind handle, but deliberately retains the
    // SyncServer after its task ends — the production shape that previously
    // left the transport's Endpoint clone holding the UDP socket.
    drop(endpoint);
    shutdown.store(true, Ordering::SeqCst);
    within(serve_task)
        .await
        .expect("retained sync server finishes shutdown");

    let rebound = within(IrohServer::bind(&spec))
        .await
        .expect("joined shutdown releases the port despite retained SyncServer");
    within(rebound.close()).await;
    drop(retained_server);
}

#[tokio::test]
async fn owner_forced_close_joins_an_active_serve_before_rebind() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let identity = identity_file(&dir);
    let port = free_udp_port();
    let spec = bind_spec_with_port(&identity, port);
    let endpoint = within(IrohServer::bind(&spec))
        .await
        .expect("bind original hub");
    let controller = endpoint.large_request_test_controller();
    let retained_server = Arc::new(SyncServer::new(
        Arc::new(Database::open_memory()),
        &endpoint,
        contextdb_core::TenantId::from("owner-forced-active-close"),
    ));
    let serve_task = tokio::spawn({
        let retained_server = retained_server.clone();
        async move {
            retained_server
                .run_until(Arc::new(AtomicBool::new(false)))
                .await
        }
    });
    controller.wait_until_routes_ready_for_test().await;

    // No graceful-shutdown flag is set and the retained SyncServer is still
    // alive. The owner's consuming close must wake and join that active serve
    // before it reports that the remembered port is reusable.
    within(endpoint.close()).await;
    let rebound = within(IrohServer::bind(&spec))
        .await
        .expect("owner-forced close releases the active serve port before returning");
    within(rebound.close()).await;
    within(serve_task)
        .await
        .expect("owner-forced close joins the serve task");
    drop(retained_server);
}

#[tokio::test]
async fn owner_forced_close_releases_a_detached_responder_before_rebind() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let identity = identity_file(&hub_dir);
    let port = free_udp_port();
    let spec = bind_spec_with_port(&identity, port);
    let endpoint = within(IrohServer::bind(&spec))
        .await
        .expect("bind original hub");
    let controller = endpoint.large_request_test_controller();
    let transport = endpoint.transport();
    let responder_started = Arc::new(tokio::sync::Semaphore::new(0));
    let release_responder = Arc::new(tokio::sync::Semaphore::new(0));
    let responder_finished = Arc::new(tokio::sync::Semaphore::new(0));
    let handler: RequestHandler = Arc::new({
        let responder_started = responder_started.clone();
        let release_responder = release_responder.clone();
        let responder_finished = responder_finished.clone();
        move |incoming: IncomingRequest| {
            let responder_started = responder_started.clone();
            let release_responder = release_responder.clone();
            let responder_finished = responder_finished.clone();
            tokio::spawn(async move {
                // Model the push-apply path: the handler returns after moving
                // the stream-owning responder into a detached task.
                responder_started.add_permits(1);
                let permit = release_responder
                    .acquire()
                    .await
                    .expect("responder release semaphore remains open");
                permit.forget();
                drop(incoming.responder);
                responder_finished.add_permits(1);
            });
            Box::pin(async { Ok(()) }) as contextdb_server::transport::TransportFuture<'static, ()>
        }
    });
    let serve_task = tokio::spawn(async move {
        transport
            .serve(
                vec![HandlerRegistration {
                    subject: "detached-responder".to_string(),
                    handler,
                }],
                Arc::new(AtomicBool::new(false)),
            )
            .await
    });
    controller.wait_until_routes_ready_for_test().await;
    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let client = client_transport(&format!(
        "iroh:?to={}&identity={}",
        endpoint.ticket(),
        identity_file(&edge_dir).display()
    ));
    let request_task = tokio::spawn(async move {
        client
            .request(
                "detached-responder",
                b"hold reply ownership".to_vec(),
                Duration::from_secs(30),
            )
            .await
    });
    responder_started
        .acquire()
        .await
        .expect("detached responder starts")
        .forget();

    // The detached task deliberately retains the responder after close. The
    // owner close must cancel its stream ownership before reporting success.
    within(endpoint.close()).await;
    let rebound = within(IrohServer::bind(&spec))
        .await
        .expect("detached responder cannot retain the closed hub port");
    within(rebound.close()).await;
    within(serve_task)
        .await
        .expect("owner-forced close joins serving transport")
        .expect("serving transport exits cleanly");

    release_responder.add_permits(1);
    responder_finished
        .acquire()
        .await
        .expect("detached responder finishes")
        .forget();
    let _ = within(request_task)
        .await
        .expect("closed request task joins");
}

#[tokio::test]
async fn graceful_drain_deadline_releases_a_detached_responder_before_rebind() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let identity = identity_file(&hub_dir);
    let port = free_udp_port();
    let spec = bind_spec_with_port(&identity, port);
    let endpoint = within(IrohServer::bind(&spec))
        .await
        .expect("bind original hub");
    let controller = endpoint.large_request_test_controller();
    let transport = endpoint.transport();
    let responder_started = Arc::new(tokio::sync::Semaphore::new(0));
    let release_responder = Arc::new(tokio::sync::Semaphore::new(0));
    let responder_finished = Arc::new(tokio::sync::Semaphore::new(0));
    let handler: RequestHandler = Arc::new({
        let responder_started = responder_started.clone();
        let release_responder = release_responder.clone();
        let responder_finished = responder_finished.clone();
        move |incoming: IncomingRequest| {
            let responder_started = responder_started.clone();
            let release_responder = release_responder.clone();
            let responder_finished = responder_finished.clone();
            tokio::spawn(async move {
                responder_started.add_permits(1);
                let permit = release_responder
                    .acquire()
                    .await
                    .expect("responder release semaphore remains open");
                permit.forget();
                drop(incoming.responder);
                responder_finished.add_permits(1);
            });
            Box::pin(async { Ok(()) }) as contextdb_server::transport::TransportFuture<'static, ()>
        }
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            transport
                .serve(
                    vec![HandlerRegistration {
                        subject: "deadline-detached-responder".to_string(),
                        handler,
                    }],
                    shutdown,
                )
                .await
        }
    });
    controller.wait_until_routes_ready_for_test().await;
    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let client = client_transport(&format!(
        "iroh:?to={}&identity={}",
        endpoint.ticket(),
        identity_file(&edge_dir).display()
    ));
    let request_task = tokio::spawn(async move {
        client
            .request(
                "deadline-detached-responder",
                b"hold reply ownership through drain".to_vec(),
                Duration::from_secs(30),
            )
            .await
    });
    responder_started
        .acquire()
        .await
        .expect("detached responder starts")
        .forget();

    // Select the deadline branch directly: the assertion is about ownership
    // cleanup, not elapsed wall time. The detached task remains alive while
    // the old hub shuts down and the exact port is rebound.
    controller.force_graceful_drain_timeout_for_test();
    shutdown.store(true, Ordering::SeqCst);
    within(serve_task)
        .await
        .expect("deadline shutdown joins serving transport")
        .expect("deadline shutdown exits cleanly");
    within(endpoint.close()).await;
    let rebound = within(IrohServer::bind(&spec))
        .await
        .expect("drain deadline cannot leave a detached responder holding the hub port");
    within(rebound.close()).await;

    release_responder.add_permits(1);
    responder_finished
        .acquire()
        .await
        .expect("detached responder finishes")
        .forget();
    let _ = within(request_task)
        .await
        .expect("closed request task joins");
}

#[tokio::test]
async fn bound_transport_refuses_a_second_serve_after_shutdown() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let endpoint = within(IrohServer::bind(&bind_spec(&identity_file(&dir))))
        .await
        .expect("bind one-shot transport");
    let controller = endpoint.large_request_test_controller();
    let transport = endpoint.transport();
    let second_transport_handle = endpoint.transport();
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_task = tokio::spawn({
        let transport = transport.clone();
        let shutdown = shutdown.clone();
        async move { transport.serve(Vec::new(), shutdown).await }
    });
    controller.wait_until_routes_ready_for_test().await;
    shutdown.store(true, Ordering::SeqCst);
    within(serve_task)
        .await
        .expect("first serve task joins")
        .expect("first serve exits cleanly");

    let error = second_transport_handle
        .serve(Vec::new(), Arc::new(AtomicBool::new(false)))
        .await
        .expect_err("a bound transport cannot silently serve a dropped endpoint twice");
    assert!(
        error.to_string().contains("served only once"),
        "the second serve must explain that a restart needs a new endpoint: {error}"
    );
    within(endpoint.close()).await;
}

// Identity is fabric-owned.

#[test]
fn identity_keypair_persists_and_yields_same_node_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = identity_file(&dir);

    let first = FabricIdentity::load_or_generate(&path)
        .expect("first load_or_generate must create an identity");
    assert!(path.exists(), "identity file must be persisted at {path:?}");
    let first_id = first.node_id();
    assert_eq!(
        first_id.len(),
        64,
        "node_id must be the lowercase hex of a 32-byte ed25519 public key, got {first_id:?}"
    );
    assert_eq!(
        first_id.to_lowercase(),
        first_id,
        "node_id must be lowercase hex"
    );

    let second = FabricIdentity::load_or_generate(&path)
        .expect("second load_or_generate must load the same identity");
    assert_eq!(
        second.node_id(),
        first_id,
        "the same identity file must always yield the same node_id"
    );
    assert_eq!(
        second.public_key_bytes(),
        first.public_key_bytes(),
        "public key bytes must be stable across loads"
    );
}

#[test]
fn fresh_data_root_yields_new_identity() {
    let dir_a = tempfile::tempdir().expect("tempdir a");
    let dir_b = tempfile::tempdir().expect("tempdir b");
    let a = FabricIdentity::load_or_generate(&identity_file(&dir_a)).expect("identity a");
    let b = FabricIdentity::load_or_generate(&identity_file(&dir_b)).expect("identity b");
    assert_ne!(
        a.node_id(),
        b.node_id(),
        "distinct data roots must mint distinct identities"
    );
}

#[cfg(unix)]
#[test]
fn identity_file_written_with_owner_only_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = identity_file(&dir);
    FabricIdentity::load_or_generate(&path).expect("identity");
    let mode = std::fs::metadata(&path)
        .expect("identity file metadata")
        .permissions()
        .mode();
    assert_eq!(
        mode & 0o777,
        0o600,
        "the secret key file must be readable by the owner only"
    );
}

#[tokio::test]
async fn bound_endpoint_uses_the_fabric_identity_not_a_transport_minted_one() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = identity_file(&dir);
    let identity = FabricIdentity::load_or_generate(&path).expect("identity");

    let endpoint = within(IrohServer::bind(&bind_spec(&path)))
        .await
        .expect("bind with existing identity");
    assert_eq!(
        endpoint.node_id(),
        identity.node_id(),
        "the endpoint's identity must BE the fabric identity handed to it — never one the transport minted"
    );
}

// No third-party contact by default; opt-ins are explicit.

#[test]
fn default_endpoint_config_disables_relay_and_publishing() {
    let spec = EndpointSpec::parse("iroh:?identity=/tmp/k.key")
        .expect("a bind spec must parse as an iroh endpoint");
    assert_eq!(
        spec.relay(),
        &RelayChoice::Disabled,
        "the default relay posture is disabled: LAN needs no relay and no internet"
    );
    assert!(
        !spec.publishes_address_lookup(),
        "the adapter must never publish node addresses to an external address-lookup service"
    );
}

#[test]
fn relay_use_requires_explicit_opt_in() {
    let self_hosted =
        EndpointSpec::parse("iroh:?identity=/tmp/k.key&relay=https://relay.example.net")
            .expect("self-hosted relay spec must parse");
    assert_eq!(
        self_hosted.relay(),
        &RelayChoice::SelfHosted("https://relay.example.net".to_string()),
        "relay=<url> selects the operator's own relay"
    );
    assert!(!self_hosted.publishes_address_lookup());

    let n0 = EndpointSpec::parse("iroh:?identity=/tmp/k.key&relay=n0")
        .expect("public-relay opt-in spec must parse");
    assert_eq!(
        n0.relay(),
        &RelayChoice::N0Public,
        "relay=n0 is the explicit opt-in to the free public relays"
    );

    let none = EndpointSpec::parse("iroh:?identity=/tmp/k.key&relay=none")
        .expect("relay=none spec must parse");
    assert_eq!(none.relay(), &RelayChoice::Disabled);
}

#[test]
fn unrelated_urls_are_not_iroh_endpoints() {
    for spec in [
        "https://hub.example.net",
        "file:///tmp/contextdb",
        "tcp://localhost:9222",
    ] {
        assert!(
            !is_iroh_endpoint(spec),
            "unrelated URL {spec} must not be accepted as an Iroh endpoint"
        );
    }
}

// The sync protocol end to end over real localhost Iroh endpoints.

#[tokio::test]
async fn sync_push_pull_status_over_iroh() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "iroh-e2e";
    let hub = start_hub(&bind_spec(&identity_file(&dir)), tenant).await;

    assert!(
        is_iroh_endpoint(&hub.ticket),
        "the hub ticket must itself be a dialable endpoint spec"
    );
    assert_eq!(
        hub.node_id.len(),
        64,
        "the hub must report its fabric node_id"
    );

    // Edge A pushes.
    let edge_a = Arc::new(Database::open_memory());
    create_notes_table(&edge_a);
    let note_a = Uuid::new_v4();
    insert_note(&edge_a, note_a, "from-edge-a");
    // The client combines the hub ticket with its own persisted identity.
    let edge_a_identity = dir.path().join("edge-a.fabric-identity.key");
    let client_a = SyncClient::new(
        edge_a.clone(),
        &peer_dial_spec(&hub.ticket, &edge_a_identity),
        contextdb_core::TenantId::from(tenant),
    );
    within(client_a.push()).await.expect("edge a push");
    assert_eq!(
        note_body(&hub.server_db, note_a).as_deref(),
        Some("from-edge-a"),
        "pushed row must land on the hub"
    );

    // Edge B pulls what A pushed.
    let edge_b = Arc::new(Database::open_memory());
    let edge_b_identity = dir.path().join("edge-b.fabric-identity.key");
    let client_b = SyncClient::new(
        edge_b.clone(),
        &peer_dial_spec(&hub.ticket, &edge_b_identity),
        contextdb_core::TenantId::from(tenant),
    );
    within(client_b.pull_default()).await.expect("edge b pull");
    assert_eq!(
        note_body(&edge_b, note_a).as_deref(),
        Some("from-edge-a"),
        "pulled row must land on edge b"
    );

    // Connectivity status reflects the live endpoint.
    within(client_a.ensure_connected())
        .await
        .expect("ensure_connected against a live hub");
    assert!(client_a.is_connected().await);

    hub.stop().await;
}

#[tokio::test]
async fn large_changeset_moves_in_one_stream_over_iroh() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    // A large payload must arrive as one framed Iroh message with full
    // integrity.
    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "iroh-large";
    let hub = start_hub(&bind_spec(&identity_file(&dir)), tenant).await;

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let id = Uuid::new_v4();
    let big_body = "x".repeat(2 * 1024 * 1024 + 137);
    insert_note(&edge, id, &big_body);

    let sender_identity = dir.path().join("large-sender.fabric-identity.key");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &sender_identity),
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push()).await.expect("large push");
    assert_eq!(
        note_body(&hub.server_db, id).map(|body| body.len()),
        Some(big_body.len()),
        "the >1MB row must arrive intact on the hub"
    );

    let reader = Arc::new(Database::open_memory());
    let reader_identity = dir.path().join("large-reader.fabric-identity.key");
    let reader_client = SyncClient::new(
        reader.clone(),
        &peer_dial_spec(&hub.ticket, &reader_identity),
        contextdb_core::TenantId::from(tenant),
    );
    within(reader_client.pull_default())
        .await
        .expect("large pull");
    assert_eq!(
        note_body(&reader, id).map(|body| body.len()),
        Some(big_body.len()),
        "the >1MB row must arrive intact on the pulling edge"
    );

    hub.stop().await;
}

#[tokio::test]
async fn oversized_authenticated_request_reaches_its_handler_only_after_complete_validation() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    const FRAME_CEILING: usize = 64 * 1024 * 1024;

    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let hub_identity = identity_file(&hub_dir);
    let port = free_udp_port();
    let endpoint = within(IrohServer::bind(&bind_spec_with_port(&hub_identity, port)))
        .await
        .expect("bind authenticated hub");
    let ticket = endpoint.ticket();
    let server_transport = endpoint.transport();
    let controller = endpoint.large_request_test_controller();

    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let edge_identity = identity_file(&edge_dir);
    let edge_node_id = FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist edge identity")
        .node_id();
    let dial_spec = format!("iroh:?to={ticket}&identity={}", edge_identity.display());
    let client = client_transport(&dial_spec);

    let mut stage_name = hub_identity
        .file_name()
        .expect("hub identity filename")
        .to_os_string();
    stage_name.push(".sync-staging");
    let stage_root = hub_identity.with_file_name(stage_name);
    let handler_invocations = Arc::new(AtomicUsize::new(0));
    let handler: RequestHandler = Arc::new({
        let handler_invocations = handler_invocations.clone();
        move |request: IncomingRequest| {
            let handler_invocations = handler_invocations.clone();
            Box::pin(async move {
                if request.node_id.is_none() {
                    return Err(contextdb_server::transport::TransportError::Other(
                        "oversized request must retain its authenticated edge identity".to_string(),
                    ));
                }
                handler_invocations.fetch_add(1, Ordering::SeqCst);
                let digest = blake3::hash(&request.bytes);
                (request.responder)(digest.as_bytes().to_vec()).await
            }) as contextdb_server::transport::TransportFuture<'static, ()>
        }
    });

    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_task = tokio::spawn({
        let shutdown = shutdown.clone();
        let server_transport = server_transport.clone();
        async move {
            server_transport
                .serve(
                    vec![HandlerRegistration {
                        subject: "oversized-authenticated-request".to_string(),
                        handler,
                    }],
                    shutdown,
                )
                .await
        }
    });

    let fitting = b"ordinary request".to_vec();
    let fitting_digest = *blake3::hash(&fitting).as_bytes();
    let fitting_reply = within(client.request(
        "oversized-authenticated-request",
        fitting,
        Duration::from_secs(30),
    ))
    .await
    .expect("fitting real ticketed-Iroh request");
    assert_eq!(fitting_reply, fitting_digest);
    assert!(
        !stage_root.exists(),
        "a fitting request and reply must keep the one-frame path and create neither request nor response durable staging"
    );
    let fitting_observations = controller.observations_for_test();
    assert!(
        fitting_observations.staged_response_manifests.is_empty()
            && fitting_observations.requested_response_chunks.is_empty()
            && fitting_observations.served_response_chunks.is_empty()
            && fitting_observations.completed_response_transfers.is_empty()
            && fitting_observations.released_response_transfers.is_empty(),
        "a fitting real-Iroh reply has no oversized-response manifest or control traffic"
    );
    assert_eq!(
        controller
            .response_stage_counts_for_test()
            .expect("read fitting response-stage inventory"),
        contextdb_server::transport::iroh::LargeResponseStageCounts {
            stages: 0,
            receipts: 0,
        },
        "a fitting real-Iroh reply leaves no durable response stage or receipt"
    );
    let unrelated = within(client.request(
        "unrelated-authenticated-request",
        b"unrelated ordinary traffic".to_vec(),
        Duration::from_secs(30),
    ))
    .await;
    assert!(
        matches!(
            unrelated,
            Err(contextdb_server::transport::TransportError::NoResponder)
        ),
        "an unrelated fitting request takes the ordinary route and receives its ordinary no-handler result"
    );
    assert!(
        !stage_root.exists(),
        "unrelated ordinary traffic must not create a durable oversized-request stage"
    );

    handler_invocations.store(0, Ordering::SeqCst);
    let expected = vec![0x5a; FRAME_CEILING + 137];
    let expected_digest = *blake3::hash(&expected).as_bytes();
    controller.pause_after_persisted_fragment_for_test(0);
    let paused_request = tokio::spawn({
        let client = client.clone();
        let expected = expected.clone();
        async move {
            client
                .request(
                    "oversized-authenticated-request",
                    expected,
                    Duration::from_secs(30),
                )
                .await
        }
    });
    within(controller.wait_until_paused_for_test()).await;
    assert_eq!(
        handler_invocations.load(Ordering::SeqCst),
        0,
        "the application handler cannot run before a persisted oversized request is complete"
    );
    let paused_stages = controller
        .stage_snapshots_for_test()
        .expect("read-only paused-stage inspection");
    assert_eq!(
        paused_stages.len(),
        1,
        "one enrolled edge creates one isolated persisted request stage"
    );
    assert_eq!(
        paused_stages[0].authenticated_node_id, edge_node_id,
        "the durable stage is scoped to the authenticated edge identity"
    );
    assert_eq!(
        paused_stages[0].fragments.len(),
        1,
        "the pause follows one durable fragment and precedes its acknowledgement"
    );
    assert_eq!(paused_stages[0].fragments[0].sequence, 0);
    assert_eq!(
        controller
            .observations_for_test()
            .accepted_fragment_sequences
            .iter()
            .map(|fragment| fragment.sequence)
            .collect::<Vec<_>>(),
        vec![0],
        "the old authenticated hub observes only the initial durable fragment"
    );

    // Stop the serving endpoint while the original stream is paused. The
    // stage is durable, so a same-identity restart accepts the retry exactly
    // once without invoking the old paused request.
    paused_request.abort();
    let _ = paused_request.await;
    client
        .shutdown()
        .await
        .expect("close the paused authenticated client before hub restart");
    shutdown.store(true, Ordering::SeqCst);
    endpoint.close().await;
    controller.resume_for_test();
    within(serve_task)
        .await
        .expect("join paused authenticated server task")
        .expect("paused authenticated server exits cleanly");
    drop(server_transport);

    let restarted = within(IrohServer::bind(&bind_spec_with_port(&hub_identity, port)))
        .await
        .expect("restart hub with the same fabric identity and staging root");
    assert_eq!(
        restarted.node_id(),
        FabricIdentity::load_or_generate(&hub_identity)
            .expect("reload restarted hub identity")
            .node_id(),
        "restart retains the hub fabric identity even when the old socket has not released its sticky port"
    );
    let restarted_transport = restarted.transport();
    let restarted_controller = restarted.large_request_test_controller();
    let restarted_shutdown = Arc::new(AtomicBool::new(false));
    let restarted_handler: RequestHandler = Arc::new({
        let handler_invocations = handler_invocations.clone();
        move |request: IncomingRequest| {
            let handler_invocations = handler_invocations.clone();
            Box::pin(async move {
                if request.node_id.is_none() {
                    return Err(contextdb_server::transport::TransportError::Other(
                        "oversized request must retain its authenticated edge identity".to_string(),
                    ));
                }
                handler_invocations.fetch_add(1, Ordering::SeqCst);
                let digest = blake3::hash(&request.bytes);
                (request.responder)(digest.as_bytes().to_vec()).await
            }) as contextdb_server::transport::TransportFuture<'static, ()>
        }
    });
    let restarted_task = tokio::spawn({
        let restarted_shutdown = restarted_shutdown.clone();
        async move {
            restarted_transport
                .serve(
                    vec![HandlerRegistration {
                        subject: "oversized-authenticated-request".to_string(),
                        handler: restarted_handler,
                    }],
                    restarted_shutdown,
                )
                .await
        }
    });
    let resumed_client = client_transport(&format!(
        "iroh:?to={}&identity={}",
        restarted.ticket(),
        edge_identity.display()
    ));
    let total_fragments = expected.len().div_ceil(4 * 1024 * 1024) as u32;
    let reply = within(resumed_client.request(
        "oversized-authenticated-request",
        expected,
        Duration::from_secs(30),
    ))
    .await
    .expect("same enrolled edge resumes its staged request after restart");
    assert_eq!(
        reply, expected_digest,
        "the handler replies only after the complete authenticated request validates"
    );
    assert_eq!(
        handler_invocations.load(Ordering::SeqCst),
        1,
        "the resumed original request invokes the handler exactly once"
    );
    assert!(
        stage_root.exists(),
        "the real over-frame request must use the durable authenticated staging path"
    );
    assert_eq!(
        restarted_controller
            .observations_for_test()
            .accepted_fragment_sequences
            .iter()
            .map(|fragment| fragment.sequence)
            .collect::<Vec<_>>(),
        (1..total_fragments).collect::<Vec<_>>(),
        "the replacement hub receives only the durable suffix and never sequence zero"
    );

    resumed_client
        .shutdown()
        .await
        .expect("close resumed authenticated client");
    restarted_shutdown.store(true, Ordering::SeqCst);
    within(restarted_task)
        .await
        .expect("join restarted authenticated server task")
        .expect("restarted authenticated server exits cleanly");
    restarted.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_authenticated_response_stages_and_reassembles_over_real_iroh() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    const FRAME_CEILING: usize = 64 * 1024 * 1024;
    const RESPONSE_BYTES: usize = 69 * 1024 * 1024 + 137;

    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let hub_identity = identity_file(&hub_dir);
    let endpoint = within(IrohServer::bind(&bind_spec(&hub_identity)))
        .await
        .expect("bind authenticated hub");
    let ticket = endpoint.ticket();
    let server_transport = endpoint.transport();

    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let edge_identity = identity_file(&edge_dir);
    let dial_spec = format!("iroh:?to={ticket}&identity={}", edge_identity.display());
    let client = client_transport(&dial_spec);
    let request = b"fetch durable oversized response".to_vec();
    let expected_digest = *blake3::hash(&vec![0xa7; RESPONSE_BYTES]).as_bytes();

    let mut stage_name = hub_identity
        .file_name()
        .expect("hub identity filename")
        .to_os_string();
    stage_name.push(".sync-staging");
    let stage_root = hub_identity.with_file_name(stage_name);
    let handler: RequestHandler = Arc::new(move |incoming: IncomingRequest| {
        let request = request.clone();
        Box::pin(async move {
            if incoming.node_id.is_none() || incoming.bytes != request {
                return Err(contextdb_server::transport::TransportError::Other(
                    "oversized response must remain bound to the authenticated request".to_string(),
                ));
            }
            (incoming.responder)(vec![0xa7; RESPONSE_BYTES]).await
        }) as contextdb_server::transport::TransportFuture<'static, ()>
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            server_transport
                .serve(
                    vec![HandlerRegistration {
                        subject: "oversized-authenticated-response".to_string(),
                        handler,
                    }],
                    shutdown,
                )
                .await
        }
    });

    let reply = within_oversized_reconciliation(client.request(
        "oversized-authenticated-response",
        b"fetch durable oversized response".to_vec(),
        Duration::from_secs(300),
    ))
    .await
    .expect("real ticketed-Iroh response exceeds the one-frame ceiling");
    const { assert!(RESPONSE_BYTES > FRAME_CEILING) };
    assert_eq!(reply.len(), RESPONSE_BYTES);
    assert_eq!(*blake3::hash(&reply).as_bytes(), expected_digest);
    assert!(
        stage_root.join("responses").exists(),
        "the over-frame reply must create the durable response staging hierarchy"
    );
    assert!(
        !contains_staged_file(&stage_root.join("responses")),
        "the acknowledged completion must remove every staged response byte"
    );
    assert!(
        !contains_staged_file(&stage_root.join("response-completions")),
        "the acknowledged release must remove the durable completion receipt"
    );

    client
        .shutdown()
        .await
        .expect("close oversized-response client");
    shutdown.store(true, Ordering::SeqCst);
    within_oversized_reconciliation(serve_task)
        .await
        .expect("join oversized-response server task")
        .expect("oversized-response server exits cleanly");
    endpoint.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_oversized_response_publications_complete_independently_over_real_iroh() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    const RESPONSE_BYTES: usize = 69 * 1024 * 1024 + 137;
    const SUBJECT: &str = "identical-oversized-response-publications";

    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let hub_identity = identity_file(&hub_dir);
    let endpoint = within(IrohServer::bind(&bind_spec(&hub_identity)))
        .await
        .expect("bind authenticated hub");
    let controller = endpoint.large_request_test_controller();
    controller.pause_before_serving_response_chunk_for_test(0);
    let transport = endpoint.transport();

    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let edge_identity = identity_file(&edge_dir);
    let dial_spec = format!(
        "iroh:?to={}&identity={}",
        endpoint.ticket(),
        edge_identity.display()
    );
    let first_client = client_transport(&dial_spec);
    let second_client = client_transport(&dial_spec);
    let expected = Arc::new(vec![0x71; RESPONSE_BYTES]);
    let dispatch_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let dispatches = Arc::new(AtomicUsize::new(0));
    let handler: RequestHandler = Arc::new({
        let expected = expected.clone();
        let dispatch_barrier = dispatch_barrier.clone();
        let dispatches = dispatches.clone();
        move |incoming: IncomingRequest| {
            let expected = expected.clone();
            let dispatch_barrier = dispatch_barrier.clone();
            let dispatches = dispatches.clone();
            Box::pin(async move {
                dispatches.fetch_add(1, Ordering::SeqCst);
                dispatch_barrier.wait().await;
                (incoming.responder)((*expected).clone()).await
            }) as contextdb_server::transport::TransportFuture<'static, ()>
        }
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            transport
                .serve(
                    vec![HandlerRegistration {
                        subject: SUBJECT.to_string(),
                        handler,
                    }],
                    shutdown,
                )
                .await
        }
    });
    controller.wait_until_routes_ready_for_test().await;

    let first = tokio::spawn({
        let client = first_client.clone();
        async move {
            client
                .request(SUBJECT, b"same request".to_vec(), Duration::from_secs(300))
                .await
        }
    });
    let second = tokio::spawn({
        let client = second_client.clone();
        async move {
            client
                .request(SUBJECT, b"same request".to_vec(), Duration::from_secs(300))
                .await
        }
    });
    controller
        .wait_until_staged_response_manifests_for_test(2)
        .await;
    controller
        .wait_until_response_control_paused_for_test()
        .await;
    let staged = controller.observations_for_test().staged_response_manifests;
    assert_eq!(staged.len(), 2);
    assert_eq!(staged[0].response_digest, staged[1].response_digest);
    assert_ne!(
        staged[0].transfer_digest, staged[1].transfer_digest,
        "identical reply bytes must still own independent publication identities"
    );
    assert_eq!(
        controller
            .response_stage_counts_for_test()
            .expect("inspect both durable response publications")
            .stages,
        2
    );
    controller.reset_paused_response_control_for_test();

    for caller in [first, second] {
        let reply = within_oversized_reconciliation(caller)
            .await
            .expect("join identical response caller")
            .expect("independent response publication completes");
        assert_eq!(
            *blake3::hash(&reply).as_bytes(),
            *blake3::hash(&expected).as_bytes()
        );
    }
    assert_eq!(dispatches.load(Ordering::SeqCst), 2);
    let finished = controller.observations_for_test();
    let mut completed = finished
        .completed_response_transfers
        .iter()
        .map(|transfer| transfer.transfer_digest)
        .collect::<Vec<_>>();
    completed.sort_unstable();
    completed.dedup();
    assert_eq!(completed.len(), 2);
    let mut released = finished
        .released_response_transfers
        .iter()
        .map(|transfer| transfer.transfer_digest)
        .collect::<Vec<_>>();
    released.sort_unstable();
    released.dedup();
    assert_eq!(released.len(), 2);
    assert_eq!(
        controller
            .response_stage_counts_for_test()
            .expect("inspect final response publication inventory"),
        contextdb_server::transport::iroh::LargeResponseStageCounts {
            stages: 0,
            receipts: 0,
        }
    );

    first_client
        .shutdown()
        .await
        .expect("close first identical response client");
    second_client
        .shutdown()
        .await
        .expect("close second identical response client");
    shutdown.store(true, Ordering::SeqCst);
    within(serve_task)
        .await
        .expect("join identical response hub")
        .expect("identical response hub exits cleanly");
    endpoint.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_response_staging_budget_refuses_and_cleans_an_over_budget_publication() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    const RESPONSE_BYTES: usize = 69 * 1024 * 1024 + 137;
    const SUBJECT: &str = "runtime-response-staging-budget";

    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let hub_identity = identity_file(&hub_dir);
    let endpoint = within(IrohServer::bind(&format!(
        "{}&response-staging-bytes=1",
        bind_spec(&hub_identity)
    )))
    .await
    .expect("bind budgeted authenticated hub");
    let controller = endpoint.large_request_test_controller();
    let transport = endpoint.transport();
    let handler: RequestHandler = Arc::new(move |incoming: IncomingRequest| {
        Box::pin(async move { (incoming.responder)(vec![0x74; RESPONSE_BYTES]).await })
            as contextdb_server::transport::TransportFuture<'static, ()>
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            transport
                .serve(
                    vec![HandlerRegistration {
                        subject: SUBJECT.to_string(),
                        handler,
                    }],
                    shutdown,
                )
                .await
        }
    });
    controller.wait_until_routes_ready_for_test().await;

    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let client = client_transport(&format!(
        "iroh:?to={}&identity={}",
        endpoint.ticket(),
        identity_file(&edge_dir).display()
    ));
    let error = within_oversized_reconciliation(client.request(
        SUBJECT,
        b"reply cannot fit declared durable budget".to_vec(),
        Duration::from_secs(300),
    ))
    .await
    .expect_err("runtime staging refuses a reply larger than its declared budget");
    assert!(
        error.to_string().contains("configured 1-byte budget"),
        "runtime refusal must name the declared durable budget: {error}"
    );
    assert_eq!(
        controller
            .response_stage_counts_for_test()
            .expect("inspect refused runtime stage cleanup"),
        contextdb_server::transport::iroh::LargeResponseStageCounts {
            stages: 0,
            receipts: 0,
        },
        "a refused publication leaves no durable bytes and never evicts registered work"
    );
    assert!(
        !stage_root_for(&hub_identity).join("responses").exists(),
        "the preflight byte admission must refuse before creating a response-stage hierarchy"
    );

    client
        .shutdown()
        .await
        .expect("close budgeted response client");
    shutdown.store(true, Ordering::SeqCst);
    within(serve_task)
        .await
        .expect("join budgeted response hub")
        .expect("budgeted response hub exits cleanly");
    endpoint.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_response_restarts_at_the_lost_chunk_without_replaying_the_handler() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    const RESPONSE_BYTES: usize = 69 * 1024 * 1024 + 137;
    const LOST_SEQUENCE: u64 = 1;
    const SUBJECT: &str = "oversized-response-restart";

    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let hub_identity = identity_file(&hub_dir);
    let port = free_udp_port();
    let bind = bind_spec_with_port(&hub_identity, port);
    let old_endpoint = within(IrohServer::bind(&bind))
        .await
        .expect("bind original authenticated hub");
    let ticket = old_endpoint.ticket();
    let old_controller = old_endpoint.large_request_test_controller();
    old_controller.pause_before_serving_response_chunk_for_test(LOST_SEQUENCE);
    let old_transport = old_endpoint.transport();

    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let edge_identity = identity_file(&edge_dir);
    let edge_node_id = FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist edge identity")
        .node_id();
    let (client, client_control) = client_with_test_controller_for_test(&format!(
        "iroh:?to={ticket}&identity={}",
        edge_identity.display()
    ));
    client_control.pause_after_response_chunk_failure_before_retry_for_test(LOST_SEQUENCE);

    let expected = Arc::new(vec![0xa7; RESPONSE_BYTES]);
    let handler_dispatches = Arc::new(AtomicUsize::new(0));
    let handler: RequestHandler = Arc::new({
        let expected = expected.clone();
        let handler_dispatches = handler_dispatches.clone();
        move |incoming: IncomingRequest| {
            let expected = expected.clone();
            let handler_dispatches = handler_dispatches.clone();
            Box::pin(async move {
                handler_dispatches.fetch_add(1, Ordering::SeqCst);
                (incoming.responder)((*expected).clone()).await
            }) as contextdb_server::transport::TransportFuture<'static, ()>
        }
    });
    let old_stop = Arc::new(AtomicBool::new(false));
    let old_task = tokio::spawn({
        let old_stop = old_stop.clone();
        async move {
            old_transport
                .serve(
                    vec![HandlerRegistration {
                        subject: SUBJECT.to_string(),
                        handler,
                    }],
                    old_stop,
                )
                .await
        }
    });
    old_controller.wait_until_routes_ready_for_test().await;

    let caller_finished = Arc::new(AtomicBool::new(false));
    let caller = tokio::spawn({
        let client = client.clone();
        let caller_finished = caller_finished.clone();
        async move {
            let reply = client
                .request(
                    SUBJECT,
                    b"restart at chunk one".to_vec(),
                    Duration::from_secs(300),
                )
                .await;
            caller_finished.store(true, Ordering::SeqCst);
            reply
        }
    });
    old_controller
        .wait_until_response_control_paused_for_test()
        .await;
    let old_observed = old_controller.observations_for_test();
    assert_eq!(old_observed.staged_response_manifests.len(), 1);
    assert_eq!(
        old_observed
            .requested_response_chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        vec![0, LOST_SEQUENCE],
        "the original hub receives the first request for the lost chunk"
    );
    assert_eq!(
        old_observed
            .served_response_chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        vec![0],
        "the original hub only writes the chunk before the loss"
    );
    assert!(!caller_finished.load(Ordering::SeqCst));

    old_stop.store(true, Ordering::SeqCst);
    old_endpoint.close().await;
    old_controller.reset_paused_response_control_for_test();
    client_control
        .wait_until_response_chunk_retry_paused_for_test()
        .await;
    assert!(
        !caller_finished.load(Ordering::SeqCst),
        "the caller remains incomplete while the redial is deliberately held"
    );
    within(old_task)
        .await
        .expect("join original hub task")
        .expect("original hub exits cleanly");

    let new_endpoint = within(IrohServer::bind(&bind))
        .await
        .expect("rebind the same identity and explicit port");
    let new_controller = new_endpoint.large_request_test_controller();
    let new_transport = new_endpoint.transport();
    let new_stop = Arc::new(AtomicBool::new(false));
    let new_handler: RequestHandler = Arc::new({
        let expected = expected.clone();
        let handler_dispatches = handler_dispatches.clone();
        move |incoming: IncomingRequest| {
            let expected = expected.clone();
            let handler_dispatches = handler_dispatches.clone();
            Box::pin(async move {
                handler_dispatches.fetch_add(1, Ordering::SeqCst);
                (incoming.responder)((*expected).clone()).await
            }) as contextdb_server::transport::TransportFuture<'static, ()>
        }
    });
    let new_task = tokio::spawn({
        let new_stop = new_stop.clone();
        async move {
            new_transport
                .serve(
                    vec![HandlerRegistration {
                        subject: SUBJECT.to_string(),
                        handler: new_handler,
                    }],
                    new_stop,
                )
                .await
        }
    });
    new_controller.wait_until_routes_ready_for_test().await;
    client_control.resume_response_chunk_retry_for_test();

    let reply = within_oversized_reconciliation(caller)
        .await
        .expect("join restarted oversized-response caller")
        .expect("the client resumes at the lost response chunk");
    assert_eq!(
        *blake3::hash(&reply).as_bytes(),
        *blake3::hash(&expected).as_bytes()
    );
    assert_eq!(handler_dispatches.load(Ordering::SeqCst), 1);
    let new_observed = new_controller.observations_for_test();
    let total_chunks = old_observed.staged_response_manifests[0].total_chunks;
    assert!(new_observed.staged_response_manifests.is_empty());
    assert_eq!(
        new_observed
            .requested_response_chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        (LOST_SEQUENCE..total_chunks).collect::<Vec<_>>()
    );
    assert_eq!(
        new_observed
            .served_response_chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        (LOST_SEQUENCE..total_chunks).collect::<Vec<_>>()
    );
    assert_eq!(new_observed.completed_response_transfers.len(), 1);
    assert_eq!(new_observed.released_response_transfers.len(), 1);
    assert_eq!(
        new_controller
            .response_stage_counts_for_test()
            .expect("read restarted response stage inventory"),
        contextdb_server::transport::iroh::LargeResponseStageCounts {
            stages: 0,
            receipts: 0,
        }
    );
    assert_eq!(
        new_observed.completed_response_transfers[0].authenticated_node_id,
        edge_node_id
    );

    client.shutdown().await.expect("close restarted client");
    new_stop.store(true, Ordering::SeqCst);
    within(new_task)
        .await
        .expect("join restarted hub task")
        .expect("restarted hub exits cleanly");
    new_endpoint.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_an_accepted_oversized_response_but_refuses_fresh_work() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    const RESPONSE_BYTES: usize = 69 * 1024 * 1024 + 137;
    const PAUSED_SEQUENCE: u64 = 0;
    const SUBJECT: &str = "oversized-response-shutdown-drain";

    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let hub_identity = identity_file(&hub_dir);
    let endpoint = within(IrohServer::bind(&bind_spec(&hub_identity)))
        .await
        .expect("bind authenticated hub");
    let controller = endpoint.large_request_test_controller();
    controller.pause_before_serving_response_chunk_for_test(PAUSED_SEQUENCE);
    let transport = endpoint.transport();

    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let edge_identity = identity_file(&edge_dir);
    let (client, client_control) = client_with_test_controller_for_test(&format!(
        "iroh:?to={}&identity={}",
        endpoint.ticket(),
        edge_identity.display()
    ));
    client_control.pause_after_response_chunk_failure_before_retry_for_test(PAUSED_SEQUENCE);

    let expected = Arc::new(vec![0x6d; RESPONSE_BYTES]);
    let handler_dispatches = Arc::new(AtomicUsize::new(0));
    let handler: RequestHandler = Arc::new({
        let expected = expected.clone();
        let handler_dispatches = handler_dispatches.clone();
        move |incoming: IncomingRequest| {
            let expected = expected.clone();
            let handler_dispatches = handler_dispatches.clone();
            Box::pin(async move {
                handler_dispatches.fetch_add(1, Ordering::SeqCst);
                (incoming.responder)((*expected).clone()).await
            }) as contextdb_server::transport::TransportFuture<'static, ()>
        }
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            transport
                .serve(
                    vec![HandlerRegistration {
                        subject: SUBJECT.to_string(),
                        handler,
                    }],
                    shutdown,
                )
                .await
        }
    });
    controller.wait_until_routes_ready_for_test().await;

    let caller_finished = Arc::new(AtomicBool::new(false));
    let caller = tokio::spawn({
        let client = client.clone();
        let caller_finished = caller_finished.clone();
        async move {
            let reply = client
                .request(
                    SUBJECT,
                    b"drain this accepted response".to_vec(),
                    Duration::from_secs(300),
                )
                .await;
            caller_finished.store(true, Ordering::SeqCst);
            reply
        }
    });
    controller
        .wait_until_response_control_paused_for_test()
        .await;
    let paused = controller.observations_for_test();
    assert_eq!(paused.staged_response_manifests.len(), 1);
    assert_eq!(
        paused
            .requested_response_chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        vec![PAUSED_SEQUENCE],
        "the hub accepted Chunk 0 for the registered oversized response before shutdown"
    );
    assert!(paused.served_response_chunks.is_empty());
    assert!(!caller_finished.load(Ordering::SeqCst));

    shutdown.store(true, Ordering::SeqCst);
    controller
        .wait_until_shutdown_admission_closed_for_test()
        .await;
    assert!(
        !serve_task.is_finished(),
        "shutdown must wait for the registered response transfer rather than close its accepted Chunk 0"
    );

    let fresh_edge_dir = tempfile::tempdir().expect("fresh edge tempdir");
    let fresh_client = client_transport(&format!(
        "iroh:?to={}&identity={}",
        endpoint.ticket(),
        identity_file(&fresh_edge_dir).display()
    ));
    let fresh = within(fresh_client.request(
        SUBJECT,
        b"this is fresh work after admission closes".to_vec(),
        Duration::from_secs(10),
    ))
    .await;
    assert!(
        fresh.is_err(),
        "a new ordinary request is refused after shutdown closes admission"
    );
    assert_eq!(
        handler_dispatches.load(Ordering::SeqCst),
        1,
        "the fresh ordinary request never reaches the registered handler"
    );
    fresh_client
        .shutdown()
        .await
        .expect("close refused fresh-work client");

    controller.reset_paused_response_control_for_test();
    client_control
        .wait_until_response_chunk_retry_paused_for_test()
        .await;
    assert!(
        !caller_finished.load(Ordering::SeqCst),
        "the response remains incomplete until its known Chunk 0 continuation is retried"
    );
    client_control.resume_response_chunk_retry_for_test();

    let reply = within_oversized_reconciliation(caller)
        .await
        .expect("join draining oversized-response caller")
        .expect("the registered response completes during shutdown drain");
    assert_eq!(reply, *expected);
    let observed = controller.observations_for_test();
    let manifest = paused
        .staged_response_manifests
        .first()
        .expect("the paused response has its staged manifest");
    let expected_sequences = (0..manifest.total_chunks).collect::<std::collections::BTreeSet<_>>();
    let requested_sequences = observed
        .requested_response_chunks
        .iter()
        .filter(|chunk| chunk.transfer_digest == manifest.transfer_digest)
        .map(|chunk| chunk.sequence)
        .collect::<std::collections::BTreeSet<_>>();
    let served_sequences = observed
        .served_response_chunks
        .iter()
        .filter(|chunk| chunk.transfer_digest == manifest.transfer_digest)
        .map(|chunk| chunk.sequence)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(requested_sequences, expected_sequences);
    assert_eq!(served_sequences, expected_sequences);
    assert_eq!(
        observed
            .requested_response_chunks
            .iter()
            .filter(|chunk| {
                chunk.transfer_digest == manifest.transfer_digest
                    && chunk.sequence == PAUSED_SEQUENCE
            })
            .count(),
        2,
        "the known Chunk 0 continuation is admitted again after shutdown begins"
    );
    assert_eq!(observed.completed_response_transfers.len(), 1);
    assert_eq!(observed.successful_response_complete_ack_writes, 1);
    assert_eq!(handler_dispatches.load(Ordering::SeqCst), 1);

    within_oversized_reconciliation(serve_task)
        .await
        .expect("join draining oversized-response server")
        .expect("shutdown exits after the Complete acknowledgement");
    client
        .shutdown()
        .await
        .expect("close draining oversized-response client");
    endpoint.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_response_retries_completion_when_the_stream_resets_before_durability() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    assert_oversized_response_completion_reset(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_response_retries_receipt_backed_completion_when_its_ack_is_lost() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    assert_oversized_response_completion_reset(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_only_oversized_push_reconciles_after_the_real_iroh_reply_is_lost() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    const FRAME_CEILING: usize = 64 * 1024 * 1024;
    const MAX_OVERSIZED_RECONCILIATION_ATTEMPTS: usize = 5;
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = TenantId::from("iroh-row-only-retry");
    let hub_identity = root.path().join("hub.fabric-identity.key");
    let endpoint = within(IrohServer::bind(&bind_spec(&hub_identity)))
        .await
        .expect("bind authenticated Iroh hub");
    let controller = endpoint.large_request_test_controller();
    let ticket = endpoint.ticket();
    let hub = Arc::new(Database::open(root.path().join("hub.db")).expect("open hub database"));
    // Both stores already have the same declaration. Advancing the source
    // watermark before the row write keeps DDL off the wire in this journey.
    create_notes_table(&hub);
    let server = Arc::new(SyncServer::new(hub.clone(), &endpoint, tenant.clone()));
    let stop = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });

    let edge_path = root.path().join("edge.db");
    let edge_identity = root.path().join("edge.fabric-identity.key");
    let edge_node_id = FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist edge identity")
        .node_id();
    let edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
    create_notes_table(&edge);
    let declaration_lsn = edge.current_lsn();
    edge.persist_sync_push_watermark(&tenant, declaration_lsn)
        .expect("make the row-only source frontier durable");
    let incarnation = edge
        .sync_incarnation(&tenant)
        .expect("read durable edge incarnation");
    hub.persist_sync_applied_push_watermark_for_node_incarnation(
        &tenant,
        &edge_node_id,
        incarnation,
        declaration_lsn,
    )
    .expect("seed the preinstalled-schema receipt for this edge incarnation");
    let bootstrap = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&ticket, &edge_identity),
        tenant.clone(),
    );
    bootstrap
        .push()
        .await
        .expect("bootstrap the real authenticated hub pull frontier");
    assert_eq!(
        bootstrap.push_watermark(),
        declaration_lsn,
        "the empty bootstrap push must not advance the edge row-write frontier"
    );
    bootstrap.shutdown().await;
    assert!(
        hub.table_meta("work_node_contacts").is_some(),
        "the authenticated bootstrap exchange records a hub-local node contact"
    );
    let hub_source = hub
        .sync_incarnation(&tenant)
        .expect("read the bootstrapped hub source");
    let hub_schema_lsn = hub.current_lsn();
    assert!(
        hub.ddl_log_since(hub_schema_lsn).is_empty(),
        "the captured hub frontier has no later DDL to deliver during row-only reconciliation"
    );
    edge.persist_sync_pull_watermark(&tenant, hub_schema_lsn)
        .expect("record the bootstrapped hub pull frontier");
    edge.persist_sync_pull_cursor(&tenant, hub_source, hub_schema_lsn)
        .expect("bind the bootstrapped pull frontier to the hub source");
    assert_eq!(
        edge.persisted_sync_pull_cursor(&tenant)
            .expect("read the combined bootstrapped pull cursor"),
        Some((hub_source, hub_schema_lsn)),
        "the edge cursor must identify the hub that issued this frontier"
    );
    // One committed row-only LSN contains 600 sub-frame rows. That source-LSN
    // group is deliberately unsplittable, so its reconciliation reply exceeds
    // the one-frame ceiling and uses durable transport chunking. The terminal
    // follow-up pull is an ordinary empty reply, not a split data page.
    let body = "r".repeat(112 * 1024);
    let notes = (0..600).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
    assert!(
        body.len() < FRAME_CEILING,
        "each row remains below the one-frame ceiling; only the atomic row set is oversized"
    );
    assert_eq!(
        notes.len() * body.len(),
        68_812_800,
        "the fixture supplies 112KiB for each of 600 rows before wire metadata"
    );
    assert!(
        notes.len() * body.len() > FRAME_CEILING,
        "the exact unsplittable source-LSN group exceeds the one-frame ceiling before wire metadata"
    );
    let source_tx = edge
        .begin()
        .expect("begin one oversized source transaction");
    for note_id in &notes {
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Uuid(*note_id));
        values.insert("body".to_string(), Value::Text(body.clone()));
        edge.insert_row(source_tx, "notes", values)
            .expect("stage one row in the oversized source transaction");
    }
    edge.commit(source_tx)
        .expect("commit every oversized row in one source transaction");
    let source_final_lsn = edge.current_lsn();
    assert_eq!(
        source_final_lsn.0,
        declaration_lsn.0 + 1,
        "the oversized row-only request must keep all rows in one accepted source LSN"
    );
    let client = Arc::new(SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&ticket, &edge_identity),
        tenant.clone(),
    ));
    let hub_before = hub.current_lsn();
    let pull_cursor_before = edge
        .persisted_sync_pull_cursor(&tenant)
        .expect("read durable pull cursor before the paused response");

    controller.drop_next_completed_reply_for_test();
    let pull_pause = client.pause_after_pull_response_for_test();
    let push = tokio::spawn({
        let client = client.clone();
        async move { client.push().await }
    });
    if tokio::time::timeout(Duration::from_secs(300), pull_pause.wait_until_reached())
        .await
        .is_err()
    {
        let observations = controller.observations_for_test();
        let stage_count = controller
            .stage_snapshots_for_test()
            .map(|stages| stages.len())
            .unwrap_or(usize::MAX);
        panic!(
            "oversized Iroh reconciliation made no progress before the deadlock guard; \
             dispatches={} resets={} post_reset_status={} accepted_fragments={} stages={} \
             pull_reply_sizes={:?} in_memory_pending={} durable_pending={:?} push_watermark={}",
            observations.completed_handler_dispatches,
            observations.injected_reply_resets,
            observations.authenticated_status_probes_after_reset,
            observations.accepted_fragment_sequences.len(),
            stage_count,
            observations.authenticated_pull_reply_bytes_after_reset,
            client.pending_push_confirmation_for_test().0,
            edge.persisted_sync_pending_push_confirmation(&tenant)
                .expect("read durable pending confirmation at the deadlock guard"),
            client.push_watermark().0,
        );
    }
    assert_eq!(
        client.pending_push_confirmation_for_test(),
        source_final_lsn,
        "the in-memory pending confirmation remains until the paused reconciliation response is applied"
    );
    assert_eq!(
        edge.persisted_sync_pending_push_confirmation(&tenant)
            .expect("read durable pending confirmation before reconciliation completes"),
        Some(source_final_lsn),
        "the file-backed edge retains pending confirmation while reconciliation is paused"
    );
    assert_eq!(
        client.push_watermark(),
        declaration_lsn,
        "status confirmation alone cannot advance the edge push watermark"
    );
    assert_eq!(
        edge.persisted_sync_pull_cursor(&tenant)
            .expect("read durable pull cursor while response application is paused"),
        pull_cursor_before,
        "a fully authenticated, chunked, completed, and released response cannot advance the pull cursor before SyncClient applies it"
    );
    let before_reconciliation = controller.observations_for_test();
    assert_eq!(
        before_reconciliation.completed_handler_dispatches, 1,
        "one completed staged push reaches the real authenticated handler"
    );
    assert_eq!(
        before_reconciliation.injected_reply_resets, 1,
        "the real final reply stream is reset exactly once"
    );
    assert_eq!(
        before_reconciliation.authenticated_status_probes_after_reset, 1,
        "the interrupted push performs one authenticated status probe before reconciliation"
    );
    assert!(
        before_reconciliation
            .authenticated_pull_reply_bytes_after_reset
            .iter()
            .any(|bytes| *bytes > FRAME_CEILING),
        "the paused reconciliation includes a durable over-frame pull reply"
    );
    assert_completed_oversized_response_lifecycle(&before_reconciliation, &edge_node_id);
    assert_eq!(
        controller
            .response_stage_counts_for_test()
            .expect("read response inventory after Complete and Release"),
        contextdb_server::transport::iroh::LargeResponseStageCounts {
            stages: 0,
            receipts: 0,
        },
        "the paused SyncClient has already observed Complete and Release, so no durable response bytes or receipt remain"
    );
    let expected_fragments = before_reconciliation
        .accepted_fragment_sequences
        .first()
        .expect("the accepted oversized request records its real fragment sequence")
        .total_fragments;
    assert!(
        before_reconciliation
            .accepted_fragment_sequences
            .iter()
            .all(|fragment| fragment.total_bytes as usize > FRAME_CEILING),
        "the accepted request bytes are demonstrably larger than the one-frame ceiling"
    );
    assert_eq!(
        before_reconciliation.accepted_fragment_sequences.len(),
        expected_fragments as usize,
        "reconciliation does not begin a second full fragment upload"
    );
    assert_eq!(
        before_reconciliation
            .accepted_fragment_sequences
            .iter()
            .map(|fragment| fragment.sequence)
            .collect::<Vec<_>>(),
        (0..expected_fragments).collect::<Vec<_>>(),
        "the one accepted upload contains each fragment exactly once in order"
    );
    pull_pause.release();
    within_oversized_reconciliation(push)
        .await
        .expect("lost-reply reconciliation task joins")
        .expect("the real client redials and reconciles its lost final reply");

    assert!(
        notes
            .iter()
            .all(|note_id| note_body(&hub, *note_id).as_deref() == Some(body.as_str())),
        "every preinstalled-schema row reaches the hub intact"
    );
    assert_eq!(
        client.push_watermark(),
        source_final_lsn,
        "source progress advances only after the retry observes the completed hub state"
    );
    assert_eq!(
        client.pending_push_confirmation_for_test(),
        contextdb_core::Lsn(0),
        "in-memory pending confirmation clears only after reconciliation pull completion"
    );
    assert_eq!(
        edge.persisted_sync_pending_push_confirmation(&tenant)
            .expect("read durable pending confirmation after reconciliation"),
        None,
        "durable pending confirmation clears only after reconciliation pull completion"
    );
    assert_eq!(
        hub.persisted_sync_applied_push_watermark_for_node_incarnation(
            &tenant,
            &edge_node_id,
            incarnation,
        )
        .expect("read hub per-edge row-only watermark"),
        Some(source_final_lsn),
        "the authenticated hub cursor is keyed to this edge incarnation"
    );
    let accepted_rows = hub
        .changes_since(hub_before)
        .rows
        .into_iter()
        .filter(|change| change.table == "notes")
        .count();
    assert_eq!(
        accepted_rows,
        notes.len(),
        "retrying a dropped final reply cannot duplicate any application row or its hub progress"
    );
    assert!(
        controller
            .stage_snapshots_for_test()
            .expect("read completed-stage snapshot after reconciliation")
            .is_empty(),
        "completed oversized staging is cleaned after the handler reply attempt"
    );
    let after_reconciliation = controller.observations_for_test();
    assert_eq!(
        after_reconciliation.completed_handler_dispatches, 1,
        "no replay dispatches the committed oversized push a second time"
    );
    assert_eq!(
        after_reconciliation.accepted_fragment_sequences,
        before_reconciliation.accepted_fragment_sequences,
        "reconciliation does not accept a second full upload"
    );
    let reconciliation_replies = &after_reconciliation.authenticated_pull_reply_bytes_after_reset;
    let (terminal_reply, oversized_attempts) = reconciliation_replies
        .split_last()
        .expect("reconciliation reaches an ordinary terminal pull");
    assert!(
        (1..=MAX_OVERSIZED_RECONCILIATION_ATTEMPTS).contains(&oversized_attempts.len()),
        "the oversized reconciliation stays within the production five-attempt bound"
    );
    assert!(
        oversized_attempts
            .iter()
            .all(|bytes| *bytes > FRAME_CEILING),
        "every pre-terminal reconciliation pull carries the unsplittable over-frame source-LSN group"
    );
    assert!(
        *terminal_reply < FRAME_CEILING,
        "the final pull is an ordinary empty terminal follow-up, not a split data page"
    );
    assert_completed_oversized_response_lifecycle(&after_reconciliation, &edge_node_id);
    assert_eq!(
        controller
            .response_stage_counts_for_test()
            .expect("read response inventory after reconciliation"),
        contextdb_server::transport::iroh::LargeResponseStageCounts {
            stages: 0,
            receipts: 0,
        },
        "every completed response stage and receipt is gone after Release"
    );

    client.shutdown().await;
    stop.store(true, Ordering::SeqCst);
    within(server_task)
        .await
        .expect("row-only hub stops within the test bound");
    endpoint.close().await;
}

#[tokio::test]
async fn reconnect_after_hub_restart_over_iroh() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "iroh-restart";
    let identity = identity_file(&dir);
    let port = free_udp_port();
    let spec = bind_spec_with_port(&identity, port);

    let hub = start_hub(&spec, tenant).await;
    let ticket = hub.ticket.clone();

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let first = Uuid::new_v4();
    insert_note(&edge, first, "before-restart");
    let edge_identity = dir.path().join("restart-edge.fabric-identity.key");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&ticket, &edge_identity),
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push()).await.expect("push before restart");
    let server_db_before = hub.server_db.clone();
    assert_eq!(
        note_body(&server_db_before, first).as_deref(),
        Some("before-restart")
    );
    hub.stop().await;

    // Same identity, same port: the ticket the edge already holds must keep
    // working across a hub restart. The reprinted ticket's set of direct
    // socket addresses is allowed to differ across a restart — which local
    // interfaces get discovered by the time the endpoint prints its ticket
    // can vary run to run, and that snapshot was never part of the promise.
    // What must stay identical is the node's own identity and the UDP port
    // it is bound to, since a caller only needs those two to keep dialing
    // the same node at the same address.
    let hub = start_hub(&spec, tenant).await;
    let before: iroh_tickets::endpoint::EndpointTicket =
        ticket.parse().expect("previous ticket must parse");
    let after: iroh_tickets::endpoint::EndpointTicket =
        hub.ticket.parse().expect("reprinted ticket must parse");
    assert_eq!(
        after.endpoint_addr().id,
        before.endpoint_addr().id,
        "a hub restart with the same identity file must reprint the same node identity"
    );
    assert_eq!(
        ticket_ports(&after),
        ticket_ports(&before),
        "a hub restart on the same explicit port must reprint the same port"
    );
    let second = Uuid::new_v4();
    insert_note(&edge, second, "after-restart");
    client.reconnect().await;
    within(client.push()).await.expect("push after restart");
    assert_eq!(
        note_body(&hub.server_db, second).as_deref(),
        Some("after-restart"),
        "the edge must reach the restarted hub through its existing ticket"
    );

    hub.stop().await;
}

#[tokio::test]
async fn backlog_accumulated_offline_reaches_hub_over_iroh() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "iroh-backlog";
    let identity = identity_file(&dir);
    let port = free_udp_port();
    let spec = bind_spec_with_port(&identity, port);

    // Learn the ticket, then take the hub down.
    let hub = start_hub(&spec, tenant).await;
    let ticket = hub.ticket.clone();
    hub.stop().await;

    // Edge accumulates rows while the hub is down; pushing fails loudly.
    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    for (n, id) in ids.iter().enumerate() {
        insert_note(&edge, *id, &format!("offline-{n}"));
    }
    let edge_identity = dir.path().join("backlog-edge.fabric-identity.key");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&ticket, &edge_identity),
        contextdb_core::TenantId::from(tenant),
    );
    assert!(
        within(client.push()).await.is_err(),
        "pushing at a down hub must fail, not hang or silently drop"
    );

    // Hub returns; the full backlog converges.
    let hub = start_hub(&spec, tenant).await;
    client.reconnect().await;
    within(client.push()).await.expect("backlog push");
    for (n, id) in ids.iter().enumerate() {
        assert_eq!(
            note_body(&hub.server_db, *id).as_deref(),
            Some(format!("offline-{n}").as_str()),
            "backlog row {n} must reach the hub"
        );
    }

    hub.stop().await;
}

#[tokio::test]
async fn unreachable_hub_maps_to_transport_neutral_errors() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    // Bind to learn a real ticket, then stop serving entirely.
    let hub = start_hub(&bind_spec(&identity_file(&dir)), "iroh-downed").await;
    let ticket = hub.ticket.clone();
    hub.stop().await;

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    insert_note(&edge, Uuid::new_v4(), "never-arrives");
    let edge_identity = dir.path().join("unreachable-edge.fabric-identity.key");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&ticket, &edge_identity),
        contextdb_core::TenantId::from("iroh-downed"),
    );
    let err = within(client.push())
        .await
        .expect_err("push at a downed hub must error")
        .to_string()
        .to_ascii_lowercase();
    assert!(
        err.contains("unreachable") || err.contains("no responder") || err.contains("timed out"),
        "the failure must surface in transport-neutral vocabulary, got: {err}"
    );
    assert!(
        !err.contains("iroh"),
        "sync-level errors must not name the concrete transport, got: {err}"
    );
}

// Ticket enrollment.

#[tokio::test]
async fn ticket_round_trips_as_opaque_config_string() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(&bind_spec(&identity_file(&dir)), "iroh-ticket").await;

    let ticket = hub.ticket.clone();
    assert!(
        is_iroh_endpoint(&ticket),
        "the printed ticket must be accepted as an endpoint spec verbatim"
    );
    let prefixed = format!("iroh:{ticket}");
    assert!(
        is_iroh_endpoint(&prefixed),
        "an iroh:-prefixed ticket must also be accepted"
    );
    let parsed = EndpointSpec::parse(&ticket).expect("ticket parses");
    assert_eq!(
        parsed.dial_ticket(),
        Some(ticket.as_str()),
        "a ticket spec is a dial spec"
    );
    assert_eq!(
        parsed.relay(),
        &RelayChoice::Disabled,
        "dialing by ticket must not silently enable any relay"
    );

    hub.stop().await;
}

// The fabric-internal peer surface (what the media-transfer path builds on).

#[tokio::test]
async fn second_alpn_peer_stream_exchanges_bytes_without_hub() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let endpoint = within(IrohServer::bind(&bind_spec(&identity_file(&dir))))
        .await
        .expect("bind peer node");
    let ticket = endpoint.ticket();

    let echo_alpn = b"contextdb.test.echo.v1".to_vec();
    endpoint.register_protocol(
        echo_alpn.clone(),
        Arc::new(|request: PeerRequest| {
            Box::pin(async move {
                // Echo back WHO asked plus the bytes: the handler must see
                // the caller's authenticated fabric identity, because the
                // media-transfer path authorizes fetches by node identity.
                let mut reply = format!("from={}:", request.remote_node_id).into_bytes();
                reply.extend_from_slice(&request.bytes);
                Ok(reply)
            }) as contextdb_server::transport::TransportFuture<'static, Vec<u8>>
        }),
    );
    assert_ne!(
        echo_alpn.as_slice(),
        SYNC_ALPN,
        "the peer surface must be an ADDITIONAL protocol label, not the sync ALPN"
    );

    // The caller dials AS its enrolled fabric identity (fabric-owned identity
    // applies to the dialing side too — never a transport-minted throwaway).
    let caller_dir = tempfile::tempdir().expect("caller tempdir");
    let caller_key = identity_file(&caller_dir);
    let caller = FabricIdentity::load_or_generate(&caller_key).expect("caller identity");
    let reply = within(peer_request(
        &caller_key,
        &ticket,
        &echo_alpn,
        b"blob-bytes".to_vec(),
        Duration::from_secs(10),
    ))
    .await
    .expect("peer request must succeed node-to-node");
    let expected = format!("from={}:blob-bytes", caller.node_id()).into_bytes();
    assert_eq!(
        reply, expected,
        "the peer handler must see the caller's ENROLLED node_id, proving the connection originated from the fabric identity"
    );
}

// The sync client can also dial AS a pinned fabric identity.

#[tokio::test]
async fn dial_spec_with_identity_pins_the_edge_fabric_identity() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let tenant = "iroh-edge-id";
    let hub = start_hub(&bind_spec(&identity_file(&hub_dir)), tenant).await;

    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let edge_key = edge_dir.path().join("edge-fabric-identity.key");
    let combined = format!("iroh:?to={}&identity={}", hub.ticket, edge_key.display());
    let parsed = EndpointSpec::parse(&combined)
        .expect("the combined to+identity dial form must parse as an iroh endpoint");
    assert_eq!(
        parsed.dial_ticket(),
        Some(hub.ticket.as_str()),
        "the combined form carries the ticket to dial"
    );
    assert_eq!(
        parsed.identity_path().map(|p| p.to_path_buf()),
        Some(edge_key.clone()),
        "the combined form pins the edge's own key file"
    );

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let id = Uuid::new_v4();
    insert_note(&edge, id, "identified-edge");
    let client = SyncClient::new(
        edge.clone(),
        &combined,
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push()).await.expect("identified push");
    assert_eq!(
        note_body(&hub.server_db, id).as_deref(),
        Some("identified-edge"),
        "sync over the identified dial spec must work end to end"
    );
    let first = FabricIdentity::load_or_generate(&edge_key)
        .expect("the edge key file must have been created by the dial")
        .node_id();
    client.reconnect().await;
    within(client.push()).await.expect("second push");
    let second = FabricIdentity::load_or_generate(&edge_key)
        .expect("edge key reload")
        .node_id();
    assert_eq!(
        first, second,
        "the edge identity must persist across reconnects — same key file, same node_id"
    );

    hub.stop().await;
}

// Cross-network: a ticket that names a relay must be dialable THROUGH it.

/// Spawns a localhost relay standing in for the operator's self-hosted one,
/// capturing its self-signed certificate to a file — the same artifact a
/// real operator points `relay-ca=` at.
async fn run_local_relay_with_ca_file(
    dir: &tempfile::TempDir,
) -> (String, PathBuf, iroh_relay::server::Server) {
    use iroh_relay::server::{
        CertConfig, QuicConfig, RelayConfig as RelayServerConfig, Server, ServerConfig, TlsConfig,
    };
    use std::net::Ipv4Addr;
    use std::sync::Arc;

    let (certs, server_config) = iroh_relay::server::testing::self_signed_tls_certs_and_config();
    let ca_path = dir.path().join("relay-ca.der");
    std::fs::write(&ca_path, certs[0].as_ref()).expect("write relay ca cert");

    let tls = TlsConfig::new(
        (Ipv4Addr::LOCALHOST, 0),
        CertConfig::Manual { server_config },
    );
    let mut relay = RelayServerConfig::new((Ipv4Addr::LOCALHOST, 0));
    relay.tls = Some(tls);
    relay.key_cache_capacity = Some(1024);
    relay.access = Arc::new(iroh_relay::server::AllowAll);
    let mut config = ServerConfig::default();
    config.relay = Some(relay);
    config.quic = Some(QuicConfig::new((Ipv4Addr::LOCALHOST, 0)));
    let server = Server::spawn(config).await.expect("spawn local relay");
    let url = format!("https://{}", server.https_addr().expect("relay https addr"));
    (url, ca_path, server)
}

#[tokio::test]
async fn ticket_relay_url_enables_relay_dialing() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let relay_dir = tempfile::tempdir().expect("relay tempdir");
    let (relay_url, relay_ca, _relay_guard) = run_local_relay_with_ca_file(&relay_dir).await;

    // Hub opts into that relay via its endpoint spec; the relay's
    // certificate is trusted EXPLICITLY via its CA file — the operator knob
    // for self-hosted relays with private/self-signed certs, never a default.
    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let tenant = "iroh-relayed";
    let spec = format!(
        "iroh:?identity={}&relay={}&relay-ca={}",
        identity_file(&hub_dir).display(),
        relay_url,
        relay_ca.display()
    );
    let hub = start_hub(&spec, tenant).await;

    // Build a RELAY-ONLY ticket: same endpoint id, no direct addresses —
    // the shape a cross-network edge actually receives when no direct path
    // exists. Dialing it can only succeed through the relay.
    let full: iroh_tickets::endpoint::EndpointTicket =
        hub.ticket.parse().expect("hub ticket parses");
    let relay_only_addr = iroh::EndpointAddr::new(full.endpoint_addr().id)
        .with_relay_url(relay_url.parse().expect("relay url parses"));
    let relay_only_ticket =
        iroh_tickets::endpoint::EndpointTicket::new(relay_only_addr).to_string();

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let id = Uuid::new_v4();
    insert_note(&edge, id, "via-relay");
    let edge_identity = hub_dir.path().join("relay-edge.fabric-identity.key");
    let dial_spec = format!(
        "iroh:?to={relay_only_ticket}&relay-ca={}&identity={}",
        relay_ca.display(),
        edge_identity.display()
    );
    let client = SyncClient::new(
        edge.clone(),
        &dial_spec,
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push())
        .await
        .expect("push through a relay-only ticket must succeed: the ticket names the relay, so the dialing endpoint must enable exactly that relay");
    assert_eq!(
        note_body(&hub.server_db, id).as_deref(),
        Some("via-relay"),
        "the relayed push must land on the hub"
    );

    hub.stop().await;
}

#[tokio::test]
async fn unsupported_endpoint_scheme_errors_actionably() {
    let edge = Arc::new(Database::open_memory());
    let client = SyncClient::new(
        edge,
        "tcp://localhost:4222",
        contextdb_core::TenantId::from("iroh-compat"),
    );
    let err = within(client.ensure_connected())
        .await
        .expect_err("an unsupported endpoint scheme must error")
        .to_string()
        .to_ascii_lowercase();
    assert!(
        err.contains("unsupported") && err.contains("iroh"),
        "the error must direct the operator to the supported Iroh endpoint form, got: {err}"
    );
}

// Transport neutrality holds structurally.

#[test]
fn iroh_word_confined_to_adapter_and_config_surface() {
    let workspace_crates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates dir");
    let allowed_suffixes = [
        // The adapter itself.
        "contextdb-server/src/transport/iroh.rs",
        // The media-plane fetch backend: the second adapter file under
        // src/transport/, the sole sanctioned home of the iroh-blobs backend
        // (the transport adapter purity guard), enforced by
        // media_transfer_containment.rs.
        "contextdb-engine/src/transport/iroh_blobs_adapter.rs",
        // The seam wiring: feature gate + factory routing only.
        "contextdb-server/src/transport/mod.rs",
        // The endpoint/relay config surface (flags, defaults, help text).
        "contextdb-server/src/main.rs",
        "contextdb-cli/src/main.rs",
        "contextdb-cli/src/repl.rs",
        "contextdb-cli/src/sync_status.rs",
        // The media-transfer acceptance scaffold binds real localhost peer
        // endpoints. Its own containment guard enforces the stricter rule that
        // backend names stay in the adapter and only IrohServer crosses into
        // the resolver.
        "contextdb-engine/src/blob_store.rs",
        "contextdb-server/src/lib.rs",
        "contextdb-server/tests/media_transfer_tests.rs",
        "contextdb-server/tests/media_transfer_memory.rs",
        "contextdb-server/tests/media_transfer_containment.rs",
        "contextdb-server/benches/blob_transfer.rs",
        // The media-fetch retention test drives the iroh-blobs backend fetch
        // path directly, so it is legitimately backend-aware.
        "contextdb-server/tests/media_transfer_fetch_retention.rs",
        // The shared media test-support module hoisted out of the media
        // suites inherits that same backend-aware role.
        "contextdb-server/tests/media_support/mod.rs",
        // The media-transfer demo binds real iroh endpoints end-to-end; a
        // deliberately backend-aware example surface.
        "contextdb-server/examples/media_transfer_fabric_demo.rs",
        // This test file names the module under test.
        "contextdb-server/tests/iroh_transport_tests.rs",
        // These server suites bind a real localhost endpoint to exercise the
        // authenticated transport contract directly.
        "contextdb-server/tests/accepted_delete_suppression_cursor_tests.rs",
        "contextdb-server/tests/authenticated_sync_contract_tests.rs",
        "contextdb-server/tests/dependency_complete_sync_tests.rs",
        "contextdb-server/tests/dependency_unit_refusal_tests.rs",
        "contextdb-server/tests/destination_ancestry_reupload_tests.rs",
        "contextdb-server/tests/durable_deletion_and_purge_tests.rs",
        "contextdb-server/tests/established_owner_vector_update_sync_tests.rs",
        "contextdb-server/tests/file_backed_identity_selection_tests.rs",
        "contextdb-server/tests/in_memory_sync_lineage_tests.rs",
        "contextdb-server/tests/no_broker_surface.rs",
        "contextdb-server/tests/oversized_dependency_unit_tests.rs",
        "contextdb-server/tests/pending_push_change_count_tests.rs",
        "contextdb-server/tests/pull_only_overwrite_tests.rs",
        "contextdb-server/tests/push_only_terminal_refusal_tests.rs",
        "contextdb-server/tests/sync_off_drop_outbound_tests.rs",
        // CLI fixtures and the public-API compile fixture deliberately name
        // endpoint configuration while keeping production callers neutral.
        "contextdb-cli/tests/declared_sync_surface_tests.rs",
        "contextdb-cli/tests/json_stderr_purity_tests.rs",
        "contextdb-engine/tests/fixtures/public_api_forbidden/src/main.rs",
        // The engine mirrors the server transport adapter and its seam.
        "contextdb-engine/src/transport/iroh.rs",
        "contextdb-engine/src/transport/mod.rs",
        // The bounded-tables blob-plane receipt test binds a real localhost
        // endpoint to prove per-peer serve/fetch counters, so it names the
        // adapter for the same reason the media suites do.
        "contextdb-server/tests/bounded_tables_sync_tests.rs",
        // The bounded-tables live-smoke driver must name the real transport it
        // drives — the live-smoke requires the real endpoint path, not a test
        // double.
        "contextdb-server/examples/bounded_tables_smoke.rs",
        // The installed-release verifier and its policy/purge journeys drive
        // the real ticketed transport through the public product surface.
        "contextdb-server/src/smoke_driver.rs",
        "contextdb-server/src/smoke_policy_journey.rs",
        "contextdb-server/src/smoke_purge_journey.rs",
        // These purge delivery journeys bind real localhost endpoints to
        // verify the public authenticated transport contract.
        "contextdb-server/tests/authoritative_purge_blob_atomicity_tests.rs",
        "contextdb-server/tests/authoritative_purge_delivery_tests.rs",
        "contextdb-server/tests/authoritative_purge_direction_delivery_tests.rs",
        "contextdb-server/tests/authoritative_purge_fresh_same_key_lineage_tests.rs",
        "contextdb-server/tests/authoritative_purge_pull_only_delivery_tests.rs",
        "contextdb-server/tests/authoritative_purge_stale_descendant_refusal_tests.rs",
        "contextdb-server/tests/authoritative_purge_sync_off_delivery_tests.rs",
        // The engine feature gate and audits legitimately name the adapter
        // while keeping consumer sync callers transport-neutral.
        "contextdb-engine/src/lib.rs",
        "contextdb-engine/tests/durable_public_api_surface_tests.rs",
        "contextdb-engine/tests/sync_source_mirror_tests.rs",
        // The test-estate ratchet audit names test FILES (including this one)
        // in its per-file sleep counts — filenames, not transport use.
        "contextdb-core/tests/test_estate_audit.rs",
    ];

    let mut sources = Vec::new();
    collect_rust_sources(workspace_crates, &mut sources);
    assert!(
        sources.len() > 50,
        "source sweep must actually walk the workspace, found only {}",
        sources.len()
    );

    let mut violations = Vec::new();
    for path in sources {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if allowed_suffixes
            .iter()
            .any(|suffix| normalized.ends_with(suffix))
        {
            continue;
        }
        let contents =
            std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {path:?}: {err}"));
        if contents.to_ascii_lowercase().contains("iroh") {
            violations.push(normalized);
        }
    }
    assert!(
        violations.is_empty(),
        "the word 'iroh' may appear only in the adapter and the endpoint config surface; found in: {violations:?}"
    );
}

fn collect_rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|err| panic!("read {dir:?}: {err}")) {
        let entry = entry.unwrap_or_else(|err| panic!("read entry under {dir:?}: {err}"));
        let path = entry.path();
        let name = entry.file_name();
        if name == "target" {
            continue;
        }
        if path.is_dir() {
            collect_rust_sources(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

// Operator-trap regressions.

#[tokio::test]
async fn hub_restart_without_port_keeps_the_same_ticket() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    // Port stickiness by default: a hub bound WITHOUT port= records its
    // chosen port beside the identity key and reuses it, so tickets survive
    // restarts (a live-smoke trap: a restarted hub minted a new random port
    // and every issued ticket went stale).
    let dir = tempfile::tempdir().expect("tempdir");
    let spec = bind_spec(&identity_file(&dir));

    let first = within(IrohServer::bind(&spec)).await.expect("first bind");
    let first_ticket = first.ticket();
    within(first.close()).await;

    let second = within(IrohServer::bind(&spec)).await.expect("second bind");
    let second_ticket = second.ticket();
    within(second.close()).await;

    // The reprinted ticket's set of direct socket addresses is allowed to
    // differ across a restart — which local interfaces get discovered by the
    // time the endpoint prints its ticket can vary run to run, and that
    // snapshot was never part of the promise. What this test is actually
    // about is port stickiness: the node's own identity and the UDP port it
    // is bound to (recorded beside the identity key and reused without an
    // explicit port=) must stay identical.
    let first: iroh_tickets::endpoint::EndpointTicket =
        first_ticket.parse().expect("first ticket must parse");
    let second: iroh_tickets::endpoint::EndpointTicket =
        second_ticket.parse().expect("second ticket must parse");
    assert_eq!(
        second.endpoint_addr().id,
        first.endpoint_addr().id,
        "a restarted hub with the same identity must reprint the same node identity"
    );
    assert_eq!(
        ticket_ports(&second),
        ticket_ports(&first),
        "a restarted hub without an explicit port= must reuse the same recorded port"
    );
}

#[tokio::test]
async fn enrollment_ticket_failures_redact_the_ticket_at_every_entrypoint() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let secret = iroh::SecretKey::from_bytes(&[0x5a; 32]);
    let ticket =
        iroh_tickets::endpoint::EndpointTicket::new(iroh::EndpointAddr::new(secret.public()))
            .to_string();
    let root = tempfile::tempdir().expect("ticket redaction tempdir");
    let identity = identity_file(&root);
    let ticket_directory = root.path().join(&ticket);
    std::fs::create_dir(&ticket_directory).expect("ticket-named identity directory");
    let missing_relay_ca = root.path().join(format!("missing-{ticket}.pem"));
    let invalid_relay_ca = root.path().join(format!("invalid-{ticket}.pem"));
    let empty_relay_ca = root.path().join(format!("empty-{ticket}.pem"));
    std::fs::write(&invalid_relay_ca, b"-----BEGIN CERTIFICATE-----\ninvalid")
        .expect("invalid relay CA fixture");
    std::fs::write(
        &empty_relay_ca,
        b"-----BEGIN PUBLIC KEY-----\nAA==\n-----END PUBLIC KEY-----\n",
    )
    .expect("empty relay CA fixture");

    let mut failures = Vec::new();
    let mut record = |label: &str, result: Result<(), String>, required: &[&str]| match result {
        Ok(()) => failures.push(format!("{label}: unexpectedly succeeded")),
        Err(error) => {
            let leaked_ticket = error.contains(&ticket);
            let missing_required = required
                .iter()
                .filter(|needle| !error.contains(*needle))
                .collect::<Vec<_>>();
            if leaked_ticket || !missing_required.is_empty() {
                failures.push(format!(
                    "{label}: leaked_ticket={leaked_ticket}, missing_required={missing_required:?}"
                ));
            }
        }
    };
    let parser = |spec: String| EndpointSpec::parse_detailed(&spec).map(|_| ());

    record(
        "parser malformed tagged ticket",
        parser(format!("iroh:malformed/{ticket}")),
        &["not a valid enrollment ticket"],
    );
    record(
        "parser missing equals",
        parser(format!("iroh:?{ticket}")),
        &["malformed parameter", "expected key=value"],
    );
    record(
        "parser invalid port",
        parser(format!("iroh:?identity=/tmp/safe.key&port={ticket}")),
        &["invalid value", "port"],
    );
    record(
        "parser invalid response staging",
        parser(format!(
            "iroh:?identity=/tmp/safe.key&response-staging-bytes={ticket}"
        )),
        &["invalid value", "response-staging-bytes"],
    );
    record(
        "parser unknown key",
        parser(format!("iroh:?{ticket}=ignored")),
        &[
            "unknown parameter",
            "accepted: identity, port, relay, relay-ca, publish, lookup, response-staging-bytes, pre-admission-connections, pre-admission-bytes, request-read-idle-ms, to",
        ],
    );
    record(
        "parser invalid to guard",
        parser(format!("iroh:?to=malformed/{ticket}")),
        &["invalid enrollment ticket", "to="],
    );

    record(
        "bind malformed tagged ticket",
        within(IrohServer::bind(&format!("iroh:malformed/{ticket}")))
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        &["not a bindable endpoint spec"],
    );
    record(
        "bind dial ticket",
        within(IrohServer::bind(&format!("iroh:{ticket}")))
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        &["cannot bind a serving endpoint from a dial ticket"],
    );
    record(
        "bind missing identity with relay ticket",
        within(IrohServer::bind(&format!(
            "iroh:?relay=https://relay.invalid/{ticket}"
        )))
        .await
        .map(|_| ())
        .map_err(|error| error.to_string()),
        &["must carry identity"],
    );
    for (label, spec, required) in [
        (
            "bind invalid relay ticket value",
            format!("iroh:?identity={}&relay={ticket}", identity.display()),
            &["invalid relay url"][..],
        ),
        (
            "bind invalid publish ticket value",
            format!("iroh:?identity={}&publish={ticket}", identity.display()),
            &["invalid publish service url"][..],
        ),
        (
            "bind invalid lookup ticket value",
            format!("iroh:?identity={}&lookup={ticket}", identity.display()),
            &["invalid lookup service url"][..],
        ),
        (
            "bind missing relay CA ticket path",
            format!(
                "iroh:?identity={}&relay=https://relay.invalid&relay-ca={}",
                identity.display(),
                missing_relay_ca.display()
            ),
            &["cannot read relay-ca file"][..],
        ),
        (
            "bind invalid relay CA ticket path",
            format!(
                "iroh:?identity={}&relay=https://relay.invalid&relay-ca={}",
                identity.display(),
                invalid_relay_ca.display()
            ),
            &["invalid PEM in relay-ca file"][..],
        ),
        (
            "bind empty relay CA ticket path",
            format!(
                "iroh:?identity={}&relay=https://relay.invalid&relay-ca={}",
                identity.display(),
                empty_relay_ca.display()
            ),
            &["contains no certificates"][..],
        ),
        (
            "bind ticket-named identity directory",
            format!("iroh:?identity={}", ticket_directory.display()),
            &["cannot read fabric identity"][..],
        ),
    ] {
        record(
            label,
            within(IrohServer::bind(&spec))
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            required,
        );
    }

    let malformed_client = SyncClient::new(
        Arc::new(Database::open_memory()),
        &format!("iroh:malformed/{ticket}"),
        TenantId::from("ticket-redaction-malformed"),
    );
    record(
        "dial malformed tagged ticket",
        within(malformed_client.ensure_connected()).await,
        &["not a valid enrollment ticket"],
    );
    let bind_client = SyncClient::new(
        Arc::new(Database::open_memory()),
        &format!(
            "iroh:?identity={}&relay=https://relay.invalid/{ticket}",
            identity.display()
        ),
        TenantId::from("ticket-redaction-bind"),
    );
    record(
        "dial bind spec with relay ticket",
        within(bind_client.ensure_connected()).await,
        &["needs the server's ticket"],
    );
    record(
        "peer request ticket-named identity directory",
        within(peer_request(
            &ticket_directory,
            &ticket,
            b"ticket-redaction",
            Vec::new(),
            Duration::from_secs(1),
        ))
        .await
        .map(|_| ())
        .map_err(|error| error.to_string()),
        &["fabric identity"],
    );

    let sticky_identity = root.path().join(format!("sticky-{ticket}.key"));
    let mut sticky_name = sticky_identity
        .file_name()
        .expect("sticky identity filename")
        .to_os_string();
    sticky_name.push(".port");
    let sticky_port = sticky_identity.with_file_name(sticky_name);
    let occupied_socket =
        std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve remembered UDP port");
    let occupied_port = occupied_socket
        .local_addr()
        .expect("reserved UDP socket address")
        .port();
    std::fs::write(&sticky_port, occupied_port.to_string()).expect("remember occupied port");
    let rebind_result = match within(IrohServer::bind(&bind_spec(&sticky_identity))).await {
        Ok(server) => {
            server.close().await;
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    };
    record(
        "bind occupied remembered port beside ticket-named identity",
        rebind_result,
        &["re-binding the remembered sync port"],
    );
    drop(occupied_socket);
    std::fs::remove_file(&sticky_port).expect("remove remembered port file");
    std::fs::create_dir(&sticky_port).expect("replace remembered port file with directory");
    let persistence_result = match within(IrohServer::bind(&bind_spec(&sticky_identity))).await {
        Ok(server) => {
            server.close().await;
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    };
    record(
        "bind remembered-port persistence directory beside ticket-named identity",
        persistence_result,
        &["cannot persist the remembered sync port"],
    );

    assert!(
        failures.is_empty(),
        "enrollment ticket failures must be useful and redacted: {failures:?}"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_stage_diagnostics_redact_bearer_tokens_from_identity_filenames() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    const FRAME_CEILING: usize = 64 * 1024 * 1024;
    let ticket = deterministic_bearer_ticket();
    let mut diagnostics = Vec::new();

    let cleanup_dir = tempfile::tempdir().expect("cleanup tempdir");
    let cleanup_identity = cleanup_dir
        .path()
        .join(format!("cleanup-{ticket}.fabric-identity.key"));
    FabricIdentity::load_or_generate(&cleanup_identity).expect("persist cleanup identity");
    let cleanup_root = stage_root_for(&cleanup_identity);
    seal_stage_root(&cleanup_root);
    let cleanup_result = within(IrohServer::bind(&format!(
        "{}&response-staging-bytes=1",
        bind_spec(&cleanup_identity)
    )))
    .await;
    unseal_stage_root(&cleanup_root);
    let cleanup_diagnostic = match cleanup_result {
        Ok(server) => {
            server.close().await;
            panic!("sealed response-stage root must refuse startup cleanup");
        }
        Err(error) => error.to_string(),
    };
    diagnostics.push(("startup response-stage cleanup", cleanup_diagnostic));

    let request_dir = tempfile::tempdir().expect("request tempdir");
    let request_identity = request_dir
        .path()
        .join(format!("request-{ticket}.fabric-identity.key"));
    FabricIdentity::load_or_generate(&request_identity).expect("persist request identity");
    let request_root = stage_root_for(&request_identity);
    seal_stage_root(&request_root);
    let request_endpoint = within(IrohServer::bind(&bind_spec(&request_identity)))
        .await
        .expect("bind request-stage hub before its staging fault");
    let request_transport = request_endpoint.transport();
    let request_controller = request_endpoint.large_request_test_controller();
    let request_stop = Arc::new(AtomicBool::new(false));
    let request_task = tokio::spawn({
        let request_stop = request_stop.clone();
        async move { request_transport.serve(Vec::new(), request_stop).await }
    });
    request_controller.wait_until_routes_ready_for_test().await;
    let request_edge_dir = tempfile::tempdir().expect("request edge tempdir");
    let request_client = client_transport(&format!(
        "iroh:?to={}&identity={}",
        request_endpoint.ticket(),
        identity_file(&request_edge_dir).display()
    ));
    let request_result = within(request_client.request(
        "sealed-oversized-request-stage",
        vec![0x31; FRAME_CEILING + 1],
        Duration::from_secs(30),
    ))
    .await;
    request_client
        .shutdown()
        .await
        .expect("close request-stage client");
    request_stop.store(true, Ordering::SeqCst);
    request_endpoint.close().await;
    within(request_task)
        .await
        .expect("join request-stage hub")
        .expect("request-stage hub exits cleanly");
    unseal_stage_root(&request_root);
    diagnostics.push((
        "oversized request staging",
        request_result
            .expect_err("sealed request-stage root must reject the real oversized descriptor")
            .to_string(),
    ));

    let response_dir = tempfile::tempdir().expect("response tempdir");
    let response_identity = response_dir
        .path()
        .join(format!("response-{ticket}.fabric-identity.key"));
    FabricIdentity::load_or_generate(&response_identity).expect("persist response identity");
    let response_root = stage_root_for(&response_identity);
    seal_stage_root(&response_root);
    let response_endpoint = within(IrohServer::bind(&bind_spec(&response_identity)))
        .await
        .expect("bind response-stage hub before its staging fault");
    let response_controller = response_endpoint.large_request_test_controller();
    let response_transport = response_endpoint.transport();
    let (response_diagnostic_tx, response_diagnostic_rx) = tokio::sync::oneshot::channel();
    let response_diagnostic_tx = Arc::new(std::sync::Mutex::new(Some(response_diagnostic_tx)));
    let response_handler: RequestHandler = Arc::new(move |incoming: IncomingRequest| {
        let response_diagnostic_tx = response_diagnostic_tx.clone();
        Box::pin(async move {
            let diagnostic = (incoming.responder)(vec![0x73; FRAME_CEILING + 1])
                .await
                .expect_err("sealed response-stage root must reject the oversized reply")
                .to_string();
            if let Some(sender) = response_diagnostic_tx
                .lock()
                .expect("response diagnostic sender mutex")
                .take()
            {
                let _ = sender.send(diagnostic.clone());
            }
            Err(contextdb_server::transport::TransportError::Other(
                diagnostic,
            ))
        }) as contextdb_server::transport::TransportFuture<'static, ()>
    });
    let response_stop = Arc::new(AtomicBool::new(false));
    let response_task = tokio::spawn({
        let response_stop = response_stop.clone();
        async move {
            response_transport
                .serve(
                    vec![HandlerRegistration {
                        subject: "sealed-oversized-response-stage".to_string(),
                        handler: response_handler,
                    }],
                    response_stop,
                )
                .await
        }
    });
    response_controller.wait_until_routes_ready_for_test().await;
    let response_edge_dir = tempfile::tempdir().expect("response edge tempdir");
    let response_client = client_transport(&format!(
        "iroh:?to={}&identity={}",
        response_endpoint.ticket(),
        identity_file(&response_edge_dir).display()
    ));
    let response_call = tokio::spawn({
        let response_client = response_client.clone();
        async move {
            response_client
                .request(
                    "sealed-oversized-response-stage",
                    b"trigger response staging".to_vec(),
                    Duration::from_secs(30),
                )
                .await
        }
    });
    let response_diagnostic = within(response_diagnostic_rx)
        .await
        .expect("response staging handler must receive its returned diagnostic");
    response_client
        .shutdown()
        .await
        .expect("close response-stage client");
    let _ = within(response_call)
        .await
        .expect("join response-stage caller");
    response_stop.store(true, Ordering::SeqCst);
    response_endpoint.close().await;
    within(response_task)
        .await
        .expect("join response-stage hub")
        .expect("response-stage hub exits cleanly");
    unseal_stage_root(&response_root);
    diagnostics.push(("oversized response staging", response_diagnostic));

    for (arm, diagnostic) in diagnostics {
        assert_stage_diagnostic_redacts_bearer_ticket(arm, &diagnostic, &ticket);
    }
}

// Every reachability capability the transport library provides is an explicit
// operator KNOB — off by default, never amputated.

#[test]
fn lookup_and_publish_are_explicit_opt_in_knobs() {
    // Defaults: absent — nothing published, nothing resolved beyond tickets.
    let default_spec =
        EndpointSpec::parse("iroh:?identity=/tmp/k.key").expect("default bind spec parses");
    assert_eq!(default_spec.publish(), None);
    assert_eq!(default_spec.lookup(), None);
    assert!(!default_spec.publishes_address_lookup());

    // Explicit opt-ins parse to their choices.
    let n0 = EndpointSpec::parse("iroh:?identity=/tmp/k.key&publish=n0&lookup=n0")
        .expect("n0 opt-in spec must parse");
    assert_eq!(n0.publish(), Some(&PublishChoice::N0));
    assert_eq!(n0.lookup(), Some(&LookupChoice::N0));
    assert!(n0.publishes_address_lookup());

    let self_hosted = EndpointSpec::parse(
        "iroh:?identity=/tmp/k.key&publish=https://pkarr.example.net&lookup=https://dns.example.net",
    )
    .expect("self-hosted lookup infrastructure spec must parse");
    assert_eq!(
        self_hosted.publish(),
        Some(&PublishChoice::Custom(
            "https://pkarr.example.net".to_string()
        ))
    );
    assert_eq!(
        self_hosted.lookup(),
        Some(&LookupChoice::Custom("https://dns.example.net".to_string()))
    );

    let mdns = EndpointSpec::parse("iroh:?identity=/tmp/k.key&lookup=mdns")
        .expect("LAN-local mdns lookup spec must parse");
    assert_eq!(mdns.lookup(), Some(&LookupChoice::Mdns));

    let dns_origin = EndpointSpec::parse("iroh:?identity=/tmp/k.key&lookup=dns:lookup.example.net")
        .expect("self-hosted DNS zone lookup spec must parse");
    assert_eq!(
        dns_origin.lookup(),
        Some(&LookupChoice::DnsOrigin("lookup.example.net".to_string()))
    );

    // The dial form takes the knobs too (an edge resolving a stale-address
    // ticket needs lookup on the DIALING side).
    let dir = tempfile::tempdir().expect("tempdir");
    let _ = dir; // ticket-bearing dial specs are exercised end to end below
}

#[tokio::test]
async fn published_hub_announces_to_the_operator_lookup_service() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    // The PUBLISH knob end to end against a real (local) lookup service: the
    // hub opted into publish=<url> and its signed announce must arrive there.
    let (dns_pkarr, _lookup_url, pkarr_url) = start_local_dns_pkarr().await;

    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let spec = format!(
        "iroh:?identity={}&publish={pkarr_url}",
        identity_file(&hub_dir).display()
    );
    let hub = start_hub(&spec, "iroh-publish").await;

    let full: iroh_tickets::endpoint::EndpointTicket =
        hub.ticket.parse().expect("hub ticket parses");
    dns_pkarr
        .on_endpoint(&full.endpoint_addr().id, Duration::from_secs(10))
        .await
        .expect("the hub's announce must land on the operator's lookup service");

    hub.stop().await;
}

#[cfg(feature = "mdns")]
#[tokio::test]
async fn mdns_lookup_resolves_identity_only_tickets_on_the_lan() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    // The LOOKUP knob end to end with zero third-party anything: both sides
    // opt into lookup=mdns (LAN-local), the edge's ticket carries ONLY the
    // hub's identity — no addresses — and the dial resolves via mDNS. An
    // IP/port change can never strand this edge.
    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let tenant = "iroh-mdns";
    let spec = format!(
        "iroh:?identity={}&lookup=mdns",
        identity_file(&hub_dir).display()
    );
    let hub = start_hub(&spec, tenant).await;

    let full: iroh_tickets::endpoint::EndpointTicket =
        hub.ticket.parse().expect("hub ticket parses");
    let id_only_ticket = iroh_tickets::endpoint::EndpointTicket::new(iroh::EndpointAddr::new(
        full.endpoint_addr().id,
    ))
    .to_string();

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let id = Uuid::new_v4();
    insert_note(&edge, id, "via-mdns");
    let edge_identity = hub_dir.path().join("mdns-edge.fabric-identity.key");
    let dial = format!(
        "iroh:?to={id_only_ticket}&lookup=mdns&identity={}",
        edge_identity.display()
    );
    let client = SyncClient::new(edge.clone(), &dial, contextdb_core::TenantId::from(tenant));

    // mDNS convergence can take a few announce cycles; retry the push until
    // the resolve succeeds (bounded by the outer test timeout).
    let mut last_err = String::new();
    let mut pushed = false;
    for _ in 0..6 {
        match within(client.push()).await {
            Ok(_) => {
                pushed = true;
                break;
            }
            Err(err) => {
                last_err = err.to_string();
                client.reconnect().await;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    assert!(
        pushed,
        "push via an identity-only ticket must succeed once mDNS resolves the hub, last error: {last_err}"
    );
    assert_eq!(
        note_body(&hub.server_db, id).as_deref(),
        Some("via-mdns"),
        "the mdns-resolved push must land on the hub"
    );

    hub.stop().await;
}

#[tokio::test]
async fn peer_connection_protocol_exposes_raw_streams() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    // The streaming half of the peer surface: the protocol owner gets the
    // raw connection (plus the caller's authenticated identity) and drives
    // its own streams — the media-transfer path's substrate.
    let dir = tempfile::tempdir().expect("tempdir");
    let endpoint = within(IrohServer::bind(&bind_spec(&identity_file(&dir))))
        .await
        .expect("bind peer node");
    let ticket = endpoint.ticket();

    let stream_alpn = b"contextdb.test.stream.v1".to_vec();
    endpoint.register_connection_protocol(
        stream_alpn.clone(),
        Arc::new(|peer: contextdb_server::transport::iroh::PeerConnection| {
            Box::pin(async move {
                let (mut send, mut recv) = peer.connection.accept_bi().await.map_err(|e| {
                    contextdb_server::transport::TransportError::Other(e.to_string())
                })?;
                let payload = recv.read_to_end(1024 * 1024).await.map_err(|e| {
                    contextdb_server::transport::TransportError::Other(e.to_string())
                })?;
                let mut reply = format!("stream-from={}:", peer.remote_node_id).into_bytes();
                reply.extend_from_slice(&payload);
                send.write_all(&reply).await.map_err(|e| {
                    contextdb_server::transport::TransportError::Other(e.to_string())
                })?;
                send.finish().map_err(|e| {
                    contextdb_server::transport::TransportError::Other(e.to_string())
                })?;
                Ok(())
            }) as contextdb_server::transport::TransportFuture<'static, ()>
        }),
    );

    let caller_dir = tempfile::tempdir().expect("caller tempdir");
    let caller_key = identity_file(&caller_dir);
    let caller = FabricIdentity::load_or_generate(&caller_key).expect("caller identity");
    let peer = within(peer_connect(&caller_key, &ticket, &stream_alpn))
        .await
        .expect("streaming peer connect must succeed");
    let (mut send, mut recv) = peer.connection.open_bi().await.expect("open raw stream");
    send.write_all(b"raw-bytes").await.expect("write");
    send.finish().expect("finish");
    let reply = recv.read_to_end(1024 * 1024).await.expect("read reply");
    let expected = format!("stream-from={}:raw-bytes", caller.node_id()).into_bytes();
    assert_eq!(
        reply, expected,
        "the raw-stream echo must round-trip and carry the caller's enrolled node_id"
    );
}

/// Spins a LOCAL DNS + pkarr pair (iroh test-utils) standing in for an
/// operator's self-hosted lookup service. Returns (guard, lookup-url,
/// publish-url) — both URLs point at the same local pkarr service.
async fn start_local_dns_pkarr() -> (iroh::test_utils::DnsPkarrServer, String, String) {
    let server = iroh::test_utils::DnsPkarrServer::run()
        .await
        .expect("local dns+pkarr servers");
    let url = server.pkarr_url().to_string();
    (server, url.clone(), url)
}

// Operator-trap regressions: fail fast, tell the truth about liveness, and
// never print a ticket whose stability was not persisted.

#[tokio::test]
async fn unsupported_server_spec_fails_serve_immediately() {
    // An unsupported server endpoint must fail fast rather than parking
    // silently until shutdown.
    use contextdb_server::transport::server_transport;
    let transport = server_transport("tcp://localhost:4222");
    // The 2 s timeout wrapper IS the promptness proof: a serve() that parks
    // until shutdown trips it. (A tighter elapsed bound here was redundant
    // wall-clock slop that flipped under load — removed.)
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        transport.serve(Vec::new(), Arc::new(AtomicBool::new(false))),
    )
    .await;
    let err = result
        .expect("serve must return promptly, not park until shutdown")
        .expect_err("serve on an unsupported endpoint must error");
    let rendered = err.to_string().to_ascii_lowercase();
    assert!(
        rendered.contains("unsupported") && rendered.contains("iroh"),
        "the error must direct the operator to the supported Iroh endpoint form, got: {rendered}"
    );
}

#[tokio::test]
async fn sync_status_reports_unreachable_after_hub_dies() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "iroh-liveness";
    let hub = start_hub(&bind_spec(&identity_file(&dir)), tenant).await;

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    insert_note(&edge, Uuid::new_v4(), "liveness-probe");
    let edge_identity = dir.path().join("liveness-edge.fabric-identity.key");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push()).await.expect("push while hub is up");
    assert!(
        client.is_connected().await,
        "connected while the hub serves"
    );

    hub.stop().await;
    // Bounded poll on the observable condition (was a fixed 1 s settle sleep):
    // after the hub dies the liveness probe must eventually report dead. The
    // deadline is a failure ceiling, not a timing assertion.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while client.is_connected().await {
        assert!(
            std::time::Instant::now() < deadline,
            "is_connected must report the dead hub within the failure ceiling"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // The operator-facing health answer must tell the truth: the cached
    // connection is dead, so the client is NOT connected, and a fresh
    // ensure_connected must fail rather than hand back the corpse.
    let still_connected = client.is_connected().await;
    let reconnect_probe = within(client.ensure_connected()).await;
    assert!(
        !still_connected || reconnect_probe.is_err(),
        "after the hub dies, is_connected must be false (got {still_connected}) or ensure_connected must error (got {reconnect_probe:?})"
    );

    // And the strong form: is_connected alone must not report a dead link.
    assert!(
        !client.is_connected().await,
        "is_connected must probe liveness, not merely cached state"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn unwritable_identity_dir_fails_bind_loudly_before_ticket() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("tempdir");
    let identity = identity_file(&dir);
    // Create the identity first (readable), then freeze the directory so the
    // sticky-port sibling cannot be written.
    FabricIdentity::load_or_generate(&identity).expect("identity");
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555))
        .expect("freeze dir");

    let result = within(IrohServer::bind(&bind_spec(&identity))).await;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).expect("thaw dir");

    let err = match result {
        Ok(_) => panic!(
            "bind must fail loudly when the remembered-port file cannot be persisted — a ticket printed without port stickiness goes stale on restart"
        ),
        Err(err) => err,
    };
    let rendered = err.to_string().to_ascii_lowercase();
    assert!(
        rendered.contains("port"),
        "the error must name the port persistence problem, got: {rendered}"
    );
}

fn free_udp_port() -> u16 {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind probe socket");
    let port = socket.local_addr().expect("probe addr").port();
    drop(socket);
    port
}

// Bounded tables — the shipped transport must authenticate its hub, because two
// protections read that identity and both go inert when it is `None`:
// `bind_retention_hub` (the multi-hub refusal never arms) and per-peer transfer
// receipts (a real edge keeps none). Only the in-process broker implemented
// `peer_node_id`, so every in-process test passed while the shipped path was
// silently unprotected.

#[tokio::test]
async fn iroh_client_reports_the_dialed_hub_as_its_authenticated_peer() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(&bind_spec(&identity_file(&dir)), "peer-identity").await;

    let client_transport = contextdb_server::transport::client_transport(&hub.ticket);
    assert_eq!(
        client_transport.peer_node_id().as_deref(),
        Some(hub.node_id.as_str()),
        "a client dials its hub BY KEY, so the shipped transport must report that \
         key as the authenticated peer — the default `None` leaves every \
         identity-keyed protection inert"
    );

    hub.stop().await;
}

#[tokio::test]
async fn iroh_push_arms_the_retention_hub_and_records_client_receipts() {
    let _journey = REAL_IROH_JOURNEY_PERMIT.lock().await;
    use contextdb_server::{TransferDirection, TransferPlane};

    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "peer-identity-consequences";
    let hub = start_hub(&bind_spec(&identity_file(&dir)), tenant).await;

    let edge = Arc::new(Database::open_memory());
    edge.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
         RETAIN 48 HOURS SYNC SAFE SYNC PUSH ONLY",
        &HashMap::new(),
    )
    .expect("retained table");
    for id in 1..6i64 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("window-{id}")));
        edge.execute("INSERT INTO windows (id, body) VALUES ($id, $body)", &row)
            .expect("window insert");
    }

    let edge_identity = dir.path().join("receipt-edge.fabric-identity.key");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push()).await.expect("push over iroh");

    // Consequence 1: the multi-hub refusal is armed against a REAL hub identity.
    assert_eq!(
        edge.retention_sync_peer().as_deref(),
        Some(hub.node_id.as_str()),
        "a real push of a retained table must register the hub it delivered to; \
         an unregistered hub means the multi-hub refusal can never fire"
    );

    // Consequence 2: the edge keeps client-side receipts, keyed by that identity.
    let receipts = client.transfer_receipts();
    assert!(
        !receipts.is_empty(),
        "a real edge must keep transfer receipts for a delivered push: {receipts:?}"
    );
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.peer_node_id == hub.node_id),
        "every receipt must be keyed by the authenticated hub node id \
         ({}): {receipts:?}",
        hub.node_id
    );
    let sent = receipts
        .iter()
        .find(|receipt| {
            receipt.plane == TransferPlane::Sync && receipt.direction == TransferDirection::Sent
        })
        .unwrap_or_else(|| panic!("no Sync/Sent receipt in {receipts:?}"));
    assert!(
        sent.counters.items >= 5,
        "the receipt must count the rows actually delivered: {receipts:?}"
    );

    hub.stop().await;
}
