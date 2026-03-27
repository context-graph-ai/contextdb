use criterion::{Criterion, criterion_group, criterion_main};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Once;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

static BUILD_RELEASE_BINARIES: Once = Once::new();

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
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

fn run_cli_script(db_path: &Path, script: &str) -> Output {
    ensure_release_binaries();
    let mut child = Command::new(cli_bin())
        .arg(db_path)
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
    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
    }
}

// --- CLI benchmarks ---

fn cli_insert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("cli");

    for count in [100, 1_000] {
        group.bench_function(format!("pipe_{count}_inserts"), |b| {
            b.iter(|| {
                let tmp = tempfile::TempDir::new().unwrap();
                let db_path = tmp.path().join("bench.db");

                let mut script =
                    String::from("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n");
                for i in 0..count {
                    script.push_str(&format!(
                        "INSERT INTO items (id, name) VALUES ('{}', 'item-{}')\n",
                        Uuid::new_v4(),
                        i
                    ));
                }
                script.push_str(".quit\n");

                let output = run_cli_script(&db_path, &script);
                assert!(output.status.success(), "CLI failed");
            });
        });
    }

    group.finish();
}

fn cli_query_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("cli_query");

    group.bench_function("select_after_1000_rows", |b| {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("bench-query.db");

        let mut setup_script =
            String::from("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, seq INTEGER)\n");
        for i in 0..1_000 {
            setup_script.push_str(&format!(
                "INSERT INTO items (id, name, seq) VALUES ('{}', 'item-{i}', {i})\n",
                Uuid::new_v4(),
            ));
        }
        setup_script.push_str(".quit\n");
        let output = run_cli_script(&db_path, &setup_script);
        assert!(output.status.success(), "setup failed");

        b.iter(|| {
            let mut query_script = String::new();
            for _ in 0..100 {
                query_script.push_str("SELECT id, name FROM items WHERE seq > 500 LIMIT 10\n");
            }
            query_script.push_str(".quit\n");
            let output = run_cli_script(&db_path, &query_script);
            assert!(output.status.success(), "query failed");
        });
    });

    group.finish();
}

// --- Sync benchmarks (real NATS via testcontainers) ---

fn sync_push_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let nats = rt.block_on(start_nats());

    let mut group = c.benchmark_group("sync");

    group.bench_function("push_1000_rows", |b| {
        b.iter(|| {
            let tmp = tempfile::TempDir::new().unwrap();
            let edge_path = tmp.path().join("edge.db");
            let server_path = tmp.path().join("server.db");
            let tenant = format!("bench-{}", Uuid::new_v4());

            let mut server = spawn_server(&server_path, &tenant, &nats.nats_url);
            std::thread::sleep(std::time::Duration::from_millis(500));

            let mut script = String::from("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n");
            for i in 0..1_000 {
                script.push_str(&format!(
                    "INSERT INTO items (id, name) VALUES ('{}', 'item-{}')\n",
                    Uuid::new_v4(),
                    i
                ));
            }
            script.push_str(".sync push\n.quit\n");

            let output = run_cli_script_with_args(
                &edge_path,
                &["--tenant-id", &tenant, "--nats-url", &nats.nats_url],
                &script,
            );
            assert!(output.status.success(), "push failed");

            let _ = server.kill();
            let _ = server.wait();
        });
    });

    group.bench_function("pull_1000_rows", |b| {
        // Setup: push 1000 rows to server once
        let tmp = tempfile::TempDir::new().unwrap();
        let source_path = tmp.path().join("source.db");
        let server_path = tmp.path().join("server.db");
        let tenant = format!("bench-pull-{}", Uuid::new_v4());

        let mut server = spawn_server(&server_path, &tenant, &nats.nats_url);
        std::thread::sleep(std::time::Duration::from_millis(500));

        let mut setup = String::from("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n");
        for i in 0..1_000 {
            setup.push_str(&format!(
                "INSERT INTO items (id, name) VALUES ('{}', 'item-{}')\n",
                Uuid::new_v4(),
                i
            ));
        }
        setup.push_str(".sync push\n.quit\n");
        let output = run_cli_script_with_args(
            &source_path,
            &["--tenant-id", &tenant, "--nats-url", &nats.nats_url],
            &setup,
        );
        assert!(output.status.success(), "setup push failed");

        b.iter(|| {
            let pull_tmp = tempfile::TempDir::new().unwrap();
            let fresh_path = pull_tmp.path().join("fresh.db");
            let output = run_cli_script_with_args(
                &fresh_path,
                &["--tenant-id", &tenant, "--nats-url", &nats.nats_url],
                "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)\n.sync pull\n.quit\n",
            );
            assert!(output.status.success(), "pull failed");
        });

        let _ = server.kill();
        let _ = server.wait();
    });

    group.finish();
}

criterion_group!(
    benches,
    cli_insert_throughput,
    cli_query_throughput,
    sync_push_throughput,
);
criterion_main!(benches);
