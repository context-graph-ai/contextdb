use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::SyncClient;
use criterion::{Criterion, criterion_group, criterion_main};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Once;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

static BUILD_RELEASE_BINARIES: Once = Once::new();

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
    ws_url: String,
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn target_dir() -> PathBuf {
    workspace_root().join("target").join("release")
}

fn cli_bin() -> PathBuf {
    let mut path = target_dir().join("contextdb-cli");
    if cfg!(windows) {
        path.set_extension("exe");
    }
    path
}

fn server_bin() -> PathBuf {
    let mut path = target_dir().join("contextdb-server");
    if cfg!(windows) {
        path.set_extension("exe");
    }
    path
}

fn ensure_release_binaries() {
    BUILD_RELEASE_BINARIES.call_once(|| {
        let status = Command::new("cargo")
            .current_dir(workspace_root())
            .args([
                "build",
                "--release",
                "-p",
                "contextdb-cli",
                "-p",
                "contextdb-server",
            ])
            .status()
            .expect("release build command should start");
        assert!(status.success(), "release build should succeed");
    });
}

fn run_cli_script_with_args(db_path: &Path, args: &[&str], script: &str) -> Output {
    ensure_release_binaries();
    let mut child = Command::new(cli_bin())
        .arg(db_path)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should spawn");
    child
        .stdin
        .as_mut()
        .expect("stdin pipe")
        .write_all(script.as_bytes())
        .expect("write script");
    child.wait_with_output().expect("CLI should finish")
}

fn spawn_server(db_path: &Path, tenant_id: &str, nats_url: &str) -> Child {
    ensure_release_binaries();
    Command::new(server_bin())
        .args([
            "--db-path",
            db_path.to_str().expect("utf-8 path"),
            "--tenant-id",
            tenant_id,
            "--nats-url",
            nats_url,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("server should spawn")
}

async fn start_nats() -> NatsFixture {
    let conf = workspace_root()
        .join("crates/contextdb-engine/tests/nats.conf")
        .to_string_lossy()
        .into_owned();
    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_exposed_port(9222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
    let request = image
        .with_mount(Mount::bind_mount(conf, "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);
    let container: ContainerAsync<GenericImage> =
        request.start().await.expect("NATS container should start");
    let nats_port = container
        .get_host_port_ipv4(4222.tcp())
        .await
        .expect("NATS port should be mapped");
    let ws_port = container
        .get_host_port_ipv4(9222.tcp())
        .await
        .expect("NATS websocket port should be mapped");
    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
        ws_url: format!("ws://127.0.0.1:{ws_port}"),
    }
}

async fn wait_for_server_ready(edge_url: &str, tenant: &str, policies: &ConflictPolicies) {
    let probe_db = std::sync::Arc::new(Database::open_memory());
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

fn sync_push_and_pull(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let nats = rt.block_on(start_nats());
    let mut group = c.benchmark_group("sync_system");
    ensure_release_binaries();
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    group.bench_function("push_100_rows_single_edge", |b| {
        let script = build_insert_script(100);
        b.iter(|| {
            let tmp = TempDir::new().unwrap();
            let edge_path = tmp.path().join("edge.db");
            let server_path = tmp.path().join("server.db");
            let tenant = format!("bench-{}", Uuid::new_v4());

            let mut server = spawn_server(&server_path, &tenant, &nats.nats_url);
            rt.block_on(wait_for_server_ready(&nats.ws_url, &tenant, &policies));

            let output = run_cli_script_with_args(
                &edge_path,
                &["--tenant-id", &tenant, "--nats-url", &nats.ws_url],
                &script,
            );
            assert!(output.status.success(), "push failed");

            let _ = server.kill();
            let _ = server.wait();
        });
    });

    group.bench_function("pull_100_rows_single_edge", |b| {
        let setup_script = build_insert_script(100);
        let tmp = TempDir::new().unwrap();
        let source_path = tmp.path().join("source.db");
        let server_path = tmp.path().join("server.db");
        let tenant = format!("bench-pull-{}", Uuid::new_v4());

        let mut server = spawn_server(&server_path, &tenant, &nats.nats_url);
        rt.block_on(wait_for_server_ready(&nats.ws_url, &tenant, &policies));

        let output = run_cli_script_with_args(
            &source_path,
            &["--tenant-id", &tenant, "--nats-url", &nats.ws_url],
            &setup_script,
        );
        assert!(output.status.success(), "setup push failed");

        b.iter(|| {
            let pull_tmp = TempDir::new().unwrap();
            let fresh_path = pull_tmp.path().join("fresh.db");
            let output = run_cli_script_with_args(
                &fresh_path,
                &["--tenant-id", &tenant, "--nats-url", &nats.ws_url],
                "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n.sync pull\n.quit\n",
            );
            assert!(output.status.success(), "pull failed");
        });

        let _ = server.kill();
        let _ = server.wait();
    });

    group.finish();
    rt.block_on(async { drop(nats) });
}

fn multi_edge_fan_in(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let nats = rt.block_on(start_nats());
    let mut group = c.benchmark_group("multi_edge");
    ensure_release_binaries();
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    group.bench_function("fan_in_4_edges_x_50_rows", |b| {
        let scripts: Vec<String> = (0..4).map(|_| build_insert_script(50)).collect();
        b.iter(|| {
            let tmp = TempDir::new().unwrap();
            let server_path = tmp.path().join("server.db");
            let tenant = format!("bench-fanin-{}", Uuid::new_v4());
            let mut server = spawn_server(&server_path, &tenant, &nats.nats_url);
            rt.block_on(wait_for_server_ready(&nats.ws_url, &tenant, &policies));

            let handles: Vec<_> = scripts
                .iter()
                .enumerate()
                .map(|(idx, script)| {
                    let edge_path = tmp.path().join(format!("edge-{idx}.db"));
                    let tenant = tenant.clone();
                    let nats_url = nats.ws_url.clone();
                    let script = script.clone();
                    thread::spawn(move || {
                        run_cli_script_with_args(
                            &edge_path,
                            &["--tenant-id", &tenant, "--nats-url", &nats_url],
                            &script,
                        )
                    })
                })
                .collect();

            for handle in handles {
                let output = handle.join().expect("edge worker");
                assert!(output.status.success(), "edge push failed");
            }

            let verify_path = tmp.path().join("verify.db");
            let verify = run_cli_script_with_args(
                &verify_path,
                &["--tenant-id", &tenant, "--nats-url", &nats.ws_url],
                "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n.sync pull\nSELECT count(*) FROM items\n.quit\n",
            );
            assert!(verify.status.success(), "verification pull failed");
            let stdout = String::from_utf8_lossy(&verify.stdout);
            assert!(stdout.contains("200"), "expected 200 synced rows, got {stdout}");

            let _ = server.kill();
            let _ = server.wait();
        });
    });

    group.finish();
    rt.block_on(async { drop(nats) });
}

criterion_group!(benches, sync_push_and_pull, multi_edge_fan_in);
criterion_main!(benches);
