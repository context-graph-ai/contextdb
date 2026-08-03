use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

fn create_observation_tables(db: &Database) {
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(384)) IMMUTABLE SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .unwrap();
}

fn create_items_table(db: &Database) {
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT) SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .unwrap();
}

struct PushFixture {
    client: SyncClient,
    server_db: Arc<Database>,
    server_task: JoinHandle<()>,
}

struct PullFixture {
    client: SyncClient,
    edge_db: Arc<Database>,
    server_task: JoinHandle<()>,
}

struct MultiEdgeFixture {
    edge_a: SyncClient,
    edge_b: SyncClient,
    verifier: SyncClient,
    verifier_db: Arc<Database>,
    server_task: JoinHandle<()>,
}

fn setup_chunked_mixed_push(rt: &tokio::runtime::Runtime) -> PushFixture {
    let fabric = Arc::new(InProcessBroker::new());
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    create_observation_tables(&edge_db);
    create_observation_tables(&server_db);

    let known_vector: Vec<f32> = (0..384).map(|i| (i as f32) / 384.0).collect();
    let tx = edge_db.begin_or_panic();
    for i in 0..400usize {
        let uuid = Uuid::new_v4();
        let vec: Vec<f32> = if i == 0 {
            known_vector.clone()
        } else {
            (0..384).map(|j| ((i * 384 + j) as f32).sin()).collect()
        };
        let row_id = edge_db
            .insert_row(
                tx,
                "observations",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid)),
                    ("data".to_string(), Value::Text("x".repeat(4_000))),
                    ("embedding".to_string(), Value::Vector(vec.clone())),
                ]),
            )
            .unwrap();
        edge_db
            .insert_vector(
                tx,
                contextdb_core::VectorIndexRef::new("observations", "embedding"),
                row_id,
                vec,
            )
            .unwrap();
    }
    edge_db.commit(tx).unwrap();

    let tenant = format!("sync-pr-push-{}", Uuid::new_v4());
    let server = Arc::new(SyncServer::with_authenticated_transport_for_test(
        server_db.clone(),
        fabric.server_as("sync-pr-push-hub"),
        contextdb_core::TenantId::from(&tenant),
    ));
    let server_handle = rt.spawn({
        let server = server.clone();
        async move { server.run().await }
    });
    rt.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            fabric.wait_for_registered_route_for_test(&contextdb_server::subjects::status_subject(
                &tenant,
            )),
        )
        .await
        .expect("sync-pr push server route")
    });

    PushFixture {
        client: SyncClient::with_authenticated_transport_for_test(
            edge_db,
            fabric.client_as("sync-pr-push-edge"),
            contextdb_core::TenantId::from(&tenant),
        ),
        server_db,
        server_task: server_handle,
    }
}

fn setup_chunked_large_pull(rt: &tokio::runtime::Runtime) -> PullFixture {
    let fabric = Arc::new(InProcessBroker::new());
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    create_observation_tables(&server_db);
    let tx = server_db.begin_or_panic();
    for _ in 0..600usize {
        server_db
            .insert_row(
                tx,
                "observations",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("data".to_string(), Value::Text("x".repeat(2_500))),
                ]),
            )
            .unwrap();
    }
    server_db.commit(tx).unwrap();

    let tenant = format!("sync-pr-pull-{}", Uuid::new_v4());
    let server = Arc::new(SyncServer::with_authenticated_transport_for_test(
        server_db,
        fabric.server_as("sync-pr-pull-hub"),
        contextdb_core::TenantId::from(&tenant),
    ));
    let server_handle = rt.spawn({
        let server = server.clone();
        async move { server.run().await }
    });
    rt.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            fabric.wait_for_registered_route_for_test(&contextdb_server::subjects::status_subject(
                &tenant,
            )),
        )
        .await
        .expect("sync-pr pull server route")
    });

    PullFixture {
        client: SyncClient::with_authenticated_transport_for_test(
            edge_db.clone(),
            fabric.client_as("sync-pr-pull-edge"),
            contextdb_core::TenantId::from(&tenant),
        ),
        edge_db,
        server_task: server_handle,
    }
}

fn setup_multi_edge_converge(rt: &tokio::runtime::Runtime) -> MultiEdgeFixture {
    let fabric = Arc::new(InProcessBroker::new());
    let edge_a_db = Arc::new(Database::open_memory());
    let edge_b_db = Arc::new(Database::open_memory());
    let verifier_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    create_items_table(&edge_a_db);
    create_items_table(&edge_b_db);
    create_items_table(&server_db);

    let tx_a = edge_a_db.begin_or_panic();
    for i in 0..100usize {
        edge_a_db
            .insert_row(
                tx_a,
                "items",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("name".to_string(), Value::Text(format!("a-{i}"))),
                ]),
            )
            .unwrap();
    }
    edge_a_db.commit(tx_a).unwrap();

    let tx_b = edge_b_db.begin_or_panic();
    for i in 0..100usize {
        edge_b_db
            .insert_row(
                tx_b,
                "items",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("name".to_string(), Value::Text(format!("b-{i}"))),
                ]),
            )
            .unwrap();
    }
    edge_b_db.commit(tx_b).unwrap();

    let tenant = format!("sync-pr-multiedge-{}", Uuid::new_v4());
    let server = Arc::new(SyncServer::with_authenticated_transport_for_test(
        server_db,
        fabric.server_as("sync-pr-multiedge-hub"),
        contextdb_core::TenantId::from(&tenant),
    ));
    let server_handle = rt.spawn({
        let server = server.clone();
        async move { server.run().await }
    });
    rt.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            fabric.wait_for_registered_route_for_test(&contextdb_server::subjects::status_subject(
                &tenant,
            )),
        )
        .await
        .expect("sync-pr multiedge server route")
    });

    MultiEdgeFixture {
        edge_a: SyncClient::with_authenticated_transport_for_test(
            edge_a_db,
            fabric.client_as("sync-pr-edge-a"),
            contextdb_core::TenantId::from(&tenant),
        ),
        edge_b: SyncClient::with_authenticated_transport_for_test(
            edge_b_db,
            fabric.client_as("sync-pr-edge-b"),
            contextdb_core::TenantId::from(&tenant),
        ),
        verifier: SyncClient::with_authenticated_transport_for_test(
            verifier_db.clone(),
            fabric.client_as("sync-pr-verifier"),
            contextdb_core::TenantId::from(&tenant),
        ),
        verifier_db,
        server_task: server_handle,
    }
}

fn sync_pr(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("sync_pr");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_millis(500));

    group.bench_function("chunked_mixed_push_400_rows", |b| {
        b.iter_batched(
            || setup_chunked_mixed_push(&rt),
            |fixture| {
                rt.block_on(async {
                    let result = fixture.client.push().await.unwrap();
                    assert_eq!(result.applied_rows, 400);
                    assert_eq!(
                        fixture
                            .server_db
                            .scan("observations", fixture.server_db.snapshot())
                            .unwrap()
                            .len(),
                        400
                    );
                    fixture.server_task.abort();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("chunked_large_pull_600_rows", |b| {
        b.iter_batched(
            || setup_chunked_large_pull(&rt),
            |fixture| {
                rt.block_on(async {
                    let result = fixture.client.pull_default().await.unwrap();
                    assert_eq!(result.applied_rows, 600);
                    assert_eq!(
                        fixture
                            .edge_db
                            .scan("observations", fixture.edge_db.snapshot())
                            .unwrap()
                            .len(),
                        600
                    );
                    fixture.server_task.abort();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("multi_edge_push_pull_converge_2x100", |b| {
        b.iter_batched(
            || setup_multi_edge_converge(&rt),
            |fixture| {
                rt.block_on(async {
                    let (a, b) = tokio::join!(fixture.edge_a.push(), fixture.edge_b.push());
                    a.unwrap();
                    b.unwrap();
                    let pulled = fixture.verifier.pull_default().await.unwrap();
                    assert_eq!(pulled.applied_rows, 200);
                    assert_eq!(
                        fixture
                            .verifier_db
                            .scan("items", fixture.verifier_db.snapshot())
                            .unwrap()
                            .len(),
                        200
                    );
                    fixture.server_task.abort();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, sync_pr);
criterion_main!(benches);
