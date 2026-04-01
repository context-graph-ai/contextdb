use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::{SyncClient, SyncServer};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::task::JoinHandle;
use uuid::Uuid;

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
    ws_url: String,
}

async fn start_nats() -> NatsFixture {
    let conf = format!(
        "{}/../contextdb-server/tests/nats.conf",
        env!("CARGO_MANIFEST_DIR")
    );
    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_exposed_port(9222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
    let request = image
        .with_mount(Mount::bind_mount(&conf, "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);
    let container: ContainerAsync<GenericImage> = request.start().await.expect("start NATS");
    let nats_port = container
        .get_host_port_ipv4(4222.tcp())
        .await
        .expect("NATS port");
    let ws_port = container
        .get_host_port_ipv4(9222.tcp())
        .await
        .expect("NATS websocket port");
    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
        ws_url: format!("ws://127.0.0.1:{ws_port}"),
    }
}

async fn wait_for_server_ready(edge_url: &str, tenant: &str, policies: &ConflictPolicies) {
    let probe_db = Arc::new(Database::open_memory());
    let probe_client = SyncClient::new(probe_db, edge_url, tenant);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match probe_client.pull(policies).await {
            Ok(_) => return,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => panic!("server did not become ready for tenant {tenant}: {err}"),
        }
    }
}

fn create_observation_tables(db: &Database) {
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(384)) IMMUTABLE",
        &HashMap::new(),
    )
    .unwrap();
}

fn create_items_table(db: &Database) {
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
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

fn setup_chunked_mixed_push(
    rt: &tokio::runtime::Runtime,
    nats_url: &str,
    edge_url: &str,
) -> PushFixture {
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    create_observation_tables(&edge_db);
    create_observation_tables(&server_db);

    let known_vector: Vec<f32> = (0..384).map(|i| (i as f32) / 384.0).collect();
    let tx = edge_db.begin();
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
        edge_db.insert_vector(tx, row_id, vec).unwrap();
    }
    edge_db.commit(tx).unwrap();

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let tenant = format!("sync-pr-push-{}", Uuid::new_v4());
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        nats_url,
        &tenant,
        policies.clone(),
    ));
    let server_handle = rt.spawn({
        let server = server.clone();
        async move { server.run().await }
    });
    rt.block_on(wait_for_server_ready(edge_url, &tenant, &policies));

    PushFixture {
        client: SyncClient::new(edge_db, edge_url, &tenant),
        server_db,
        server_task: server_handle,
    }
}

fn setup_chunked_large_pull(
    rt: &tokio::runtime::Runtime,
    nats_url: &str,
    edge_url: &str,
) -> PullFixture {
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    create_observation_tables(&server_db);
    let tx = server_db.begin();
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

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let tenant = format!("sync-pr-pull-{}", Uuid::new_v4());
    let server = Arc::new(SyncServer::new(
        server_db,
        nats_url,
        &tenant,
        policies.clone(),
    ));
    let server_handle = rt.spawn({
        let server = server.clone();
        async move { server.run().await }
    });
    rt.block_on(wait_for_server_ready(edge_url, &tenant, &policies));

    PullFixture {
        client: SyncClient::new(edge_db.clone(), edge_url, &tenant),
        edge_db,
        server_task: server_handle,
    }
}

fn setup_multi_edge_converge(
    rt: &tokio::runtime::Runtime,
    nats_url: &str,
    edge_url: &str,
) -> MultiEdgeFixture {
    let edge_a_db = Arc::new(Database::open_memory());
    let edge_b_db = Arc::new(Database::open_memory());
    let verifier_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    create_items_table(&edge_a_db);
    create_items_table(&edge_b_db);
    create_items_table(&server_db);

    let tx_a = edge_a_db.begin();
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

    let tx_b = edge_b_db.begin();
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

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let tenant = format!("sync-pr-multiedge-{}", Uuid::new_v4());
    let server = Arc::new(SyncServer::new(
        server_db,
        nats_url,
        &tenant,
        policies.clone(),
    ));
    let server_handle = rt.spawn({
        let server = server.clone();
        async move { server.run().await }
    });
    rt.block_on(wait_for_server_ready(edge_url, &tenant, &policies));

    MultiEdgeFixture {
        edge_a: SyncClient::new(edge_a_db, edge_url, &tenant),
        edge_b: SyncClient::new(edge_b_db, edge_url, &tenant),
        verifier: SyncClient::new(verifier_db.clone(), edge_url, &tenant),
        verifier_db,
        server_task: server_handle,
    }
}

fn sync_pr(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let nats = rt.block_on(start_nats());
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    let mut group = c.benchmark_group("sync_pr");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_millis(500));

    group.bench_function("chunked_mixed_push_400_rows", |b| {
        b.iter_batched(
            || setup_chunked_mixed_push(&rt, &nats.nats_url, &nats.ws_url),
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
            || setup_chunked_large_pull(&rt, &nats.nats_url, &nats.ws_url),
            |fixture| {
                rt.block_on(async {
                    let result = fixture.client.pull(&policies).await.unwrap();
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
            || setup_multi_edge_converge(&rt, &nats.nats_url, &nats.ws_url),
            |fixture| {
                rt.block_on(async {
                    let (a, b) = tokio::join!(fixture.edge_a.push(), fixture.edge_b.push());
                    a.unwrap();
                    b.unwrap();
                    let pulled = fixture.verifier.pull(&policies).await.unwrap();
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
    rt.block_on(async { drop(nats) });
}

criterion_group!(benches, sync_pr);
criterion_main!(benches);
