#[path = "common/mod.rs"]
mod common;

use common::process::{run_cli_script, spawn_server, stop_child};
use common::sync::{start_nats, wait_for_server_ready};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::path::Path;
use std::process::Child;
use tempfile::TempDir;
use uuid::Uuid;

struct PushFixture {
    _tmp: TempDir,
    server_path: std::path::PathBuf,
    edge_path: std::path::PathBuf,
    tenant: String,
    ws_url: String,
    server: Child,
}

struct PullFixture {
    _tmp: TempDir,
    fresh_path: std::path::PathBuf,
    tenant: String,
    ws_url: String,
    server: Child,
}

struct FanInFixture {
    _tmp: TempDir,
    verify_path: std::path::PathBuf,
    tenant: String,
    ws_url: String,
    edge_paths: Vec<std::path::PathBuf>,
    scripts: Vec<String>,
    server: Child,
}

struct BacklogFixture {
    _tmp: TempDir,
    server_path: std::path::PathBuf,
    edge_path: std::path::PathBuf,
    tenant: String,
    ws_url: String,
    server: Child,
}

fn count_rows_from_file(path: &Path, table: &str) -> usize {
    let db = Database::open(path).expect("open db for verification");
    db.scan(table, db.snapshot()).expect("scan table").len()
}

fn build_insert_script(count: usize) -> String {
    let mut script = String::from("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n");
    for i in 0..count {
        script.push_str(&format!(
            "INSERT INTO items (id, name) VALUES ('{}', 'item-{}')\n",
            Uuid::new_v4(),
            i
        ));
    }
    script.push_str(".sync push\n.quit\n");
    script
}

fn append_offline_rows(path: &Path, start: usize, count: usize) {
    let db = Database::open(path).expect("open edge db for offline backlog append");
    let tx = db.begin();
    for i in start..start + count {
        db.insert_row(
            tx,
            "items",
            std::collections::HashMap::from([
                (
                    "id".to_string(),
                    contextdb_core::Value::Uuid(Uuid::new_v4()),
                ),
                (
                    "name".to_string(),
                    contextdb_core::Value::Text(format!("item-{i}")),
                ),
            ]),
        )
        .expect("offline row insert");
    }
    db.commit(tx).expect("offline backlog commit");
    db.close().expect("close edge db after offline append");
}

fn setup_push_fixture(
    rt: &tokio::runtime::Runtime,
    nats_url: &str,
    ws_url: &str,
    policies: &ConflictPolicies,
) -> PushFixture {
    let tmp = TempDir::new().unwrap();
    let edge_path = tmp.path().join("edge.db");
    let server_path = tmp.path().join("server.db");
    let tenant = format!("system-push-{}", Uuid::new_v4());
    let server = spawn_server(&server_path, &tenant, nats_url);
    rt.block_on(wait_for_server_ready(ws_url, &tenant, policies));

    PushFixture {
        _tmp: tmp,
        server_path,
        edge_path,
        tenant,
        ws_url: ws_url.to_string(),
        server,
    }
}

fn setup_pull_fixture(
    rt: &tokio::runtime::Runtime,
    nats_url: &str,
    ws_url: &str,
    policies: &ConflictPolicies,
    count: usize,
) -> PullFixture {
    let tmp = TempDir::new().unwrap();
    let source_path = tmp.path().join("source.db");
    let fresh_path = tmp.path().join("fresh.db");
    let server_path = tmp.path().join("server.db");
    let tenant = format!("system-pull-{}", Uuid::new_v4());
    let server = spawn_server(&server_path, &tenant, nats_url);
    rt.block_on(wait_for_server_ready(ws_url, &tenant, policies));

    let output = run_cli_script(
        &source_path,
        &["--tenant-id", &tenant, "--nats-url", ws_url],
        &build_insert_script(count),
    );
    assert!(output.status.success(), "setup push failed");

    PullFixture {
        _tmp: tmp,
        fresh_path,
        tenant,
        ws_url: ws_url.to_string(),
        server,
    }
}

fn setup_fan_in_fixture(
    rt: &tokio::runtime::Runtime,
    nats_url: &str,
    ws_url: &str,
    policies: &ConflictPolicies,
) -> FanInFixture {
    let tmp = TempDir::new().unwrap();
    let server_path = tmp.path().join("server.db");
    let verify_path = tmp.path().join("verify.db");
    let tenant = format!("system-fanin-{}", Uuid::new_v4());
    let server = spawn_server(&server_path, &tenant, nats_url);
    rt.block_on(wait_for_server_ready(ws_url, &tenant, policies));

    let scripts: Vec<String> = (0..4).map(|_| build_insert_script(50)).collect();
    let edge_paths: Vec<_> = (0..4)
        .map(|idx| tmp.path().join(format!("edge-{idx}.db")))
        .collect();

    FanInFixture {
        _tmp: tmp,
        verify_path,
        tenant,
        ws_url: ws_url.to_string(),
        edge_paths,
        scripts,
        server,
    }
}

fn setup_backlog_fixture(
    rt: &tokio::runtime::Runtime,
    nats_url: &str,
    ws_url: &str,
    policies: &ConflictPolicies,
) -> BacklogFixture {
    let tmp = TempDir::new().unwrap();
    let edge_path = tmp.path().join("edge.db");
    let server_path = tmp.path().join("server.db");
    let tenant = format!("system-backlog-{}", Uuid::new_v4());

    let mut server = spawn_server(&server_path, &tenant, nats_url);
    rt.block_on(wait_for_server_ready(ws_url, &tenant, policies));
    let initial = run_cli_script(
        &edge_path,
        &["--tenant-id", &tenant, "--nats-url", ws_url],
        &build_insert_script(100),
    );
    assert!(initial.status.success(), "initial push should succeed");
    stop_child(&mut server);

    append_offline_rows(&edge_path, 100, 100);

    let server = spawn_server(&server_path, &tenant, nats_url);
    rt.block_on(wait_for_server_ready(ws_url, &tenant, policies));

    BacklogFixture {
        _tmp: tmp,
        server_path,
        edge_path,
        tenant,
        ws_url: ws_url.to_string(),
        server,
    }
}

fn server_sync_system(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let nats = rt.block_on(start_nats());
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let mut group = c.benchmark_group("sync_system");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_millis(500));

    group.bench_function("push_100_rows_single_edge", |b| {
        b.iter_batched(
            || setup_push_fixture(&rt, &nats.nats_url, &nats.ws_url, &policies),
            |mut fixture| {
                let output = run_cli_script(
                    &fixture.edge_path,
                    &[
                        "--tenant-id",
                        &fixture.tenant,
                        "--nats-url",
                        &fixture.ws_url,
                    ],
                    &build_insert_script(100),
                );
                assert!(output.status.success(), "push failed");
                stop_child(&mut fixture.server);
                assert_eq!(count_rows_from_file(&fixture.server_path, "items"), 100);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("pull_100_rows_single_edge", |b| {
        b.iter_batched(
            || setup_pull_fixture(&rt, &nats.nats_url, &nats.ws_url, &policies, 100),
            |mut fixture| {
                let output = run_cli_script(
                    &fixture.fresh_path,
                    &["--tenant-id", &fixture.tenant, "--nats-url", &fixture.ws_url],
                    "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n.sync pull\nSELECT count(*) FROM items\n.quit\n",
                );
                assert!(output.status.success(), "pull failed");
                let stdout = String::from_utf8_lossy(&output.stdout);
                assert!(stdout.contains("100"), "expected 100 synced rows, got {stdout}");
                stop_child(&mut fixture.server);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("fan_in_4_edges_x_50_rows", |b| {
        b.iter_batched(
            || setup_fan_in_fixture(&rt, &nats.nats_url, &nats.ws_url, &policies),
            |mut fixture| {
                let handles: Vec<_> = fixture
                    .scripts
                    .iter()
                    .enumerate()
                    .map(|(idx, script)| {
                        let edge_path = fixture.edge_paths[idx].clone();
                        let tenant = fixture.tenant.clone();
                        let ws_url = fixture.ws_url.clone();
                        let script = script.clone();
                        std::thread::spawn(move || {
                            run_cli_script(
                                &edge_path,
                                &["--tenant-id", &tenant, "--nats-url", &ws_url],
                                &script,
                            )
                        })
                    })
                    .collect();

                for handle in handles {
                    let output = handle.join().expect("edge worker");
                    assert!(output.status.success(), "edge push failed");
                }

                let verify = run_cli_script(
                    &fixture.verify_path,
                    &["--tenant-id", &fixture.tenant, "--nats-url", &fixture.ws_url],
                    "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n.sync pull\nSELECT count(*) FROM items\n.quit\n",
                );
                assert!(verify.status.success(), "verification pull failed");
                let stdout = String::from_utf8_lossy(&verify.stdout);
                assert!(stdout.contains("200"), "expected 200 synced rows, got {stdout}");
                stop_child(&mut fixture.server);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("backlog_push_after_server_restart_100_rows", |b| {
        b.iter_batched(
            || setup_backlog_fixture(&rt, &nats.nats_url, &nats.ws_url, &policies),
            |mut fixture| {
                let output = run_cli_script(
                    &fixture.edge_path,
                    &[
                        "--tenant-id",
                        &fixture.tenant,
                        "--nats-url",
                        &fixture.ws_url,
                    ],
                    ".sync push\n.quit\n",
                );
                assert!(output.status.success(), "backlog push failed");
                stop_child(&mut fixture.server);
                assert_eq!(count_rows_from_file(&fixture.server_path, "items"), 200);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
    rt.block_on(async { drop(nats) });
}

criterion_group!(benches, server_sync_system);
criterion_main!(benches);
