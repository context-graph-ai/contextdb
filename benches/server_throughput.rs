use criterion::{Criterion, criterion_group, criterion_main};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Once;
use uuid::Uuid;

static BUILD_RELEASE_BINARIES: Once = Once::new();

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

// --- CLI benchmarks ---

fn cli_insert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("cli");
    ensure_release_binaries();

    for count in [25, 100] {
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
    ensure_release_binaries();

    group.bench_function("select_after_1000_rows", |b| {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("bench-query.db");

        let mut setup_script =
            String::from("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, seq INTEGER)\n");
        for i in 0..250 {
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
            for _ in 0..25 {
                query_script.push_str("SELECT id, name FROM items WHERE seq > 100 LIMIT 10\n");
            }
            query_script.push_str(".quit\n");
            let output = run_cli_script(&db_path, &query_script);
            assert!(output.status.success(), "query failed");
        });
    });

    group.finish();
}

criterion_group!(benches, cli_insert_throughput, cli_query_throughput,);
criterion_main!(benches);
