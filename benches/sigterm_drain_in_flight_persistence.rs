#[path = "common/mod.rs"]
mod common;

use common::process::{spawn_server_with_barrier, wait_for_child_output};
use common::sync::{
    push_change_through_server, push_many_changes_through_server, start_sync_fixture,
};
use contextdb_core::Value;
use contextdb_engine::Database;
use criterion::{Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Child;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

const BENCH_BATCH_ROWS: usize = 4096;
const TENANT: &str = "sigterm_drain_bench";

struct DrainFixture {
    _tmp: TempDir,
    db_path: PathBuf,
    barrier_path: PathBuf,
    release_path: PathBuf,
    stderr_path: PathBuf,
    child: Option<Child>,
    in_flight_id: Uuid,
    batch_ids: Vec<Uuid>,
}

struct ServerStderrDump<'a> {
    path: &'a std::path::Path,
}

impl<'a> Drop for ServerStderrDump<'a> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let s = std::fs::read_to_string(self.path).unwrap_or_default();
            eprintln!("--- server.stderr ---\n{s}\n--- end ---");
        }
    }
}

fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

async fn build_fixture(bind_spec: &str, ticket: &str) -> DrainFixture {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("drain.db");
    let barrier_path = tmp.path().join("push-started.barrier");
    let release_path = tmp.path().join("push-release.barrier");
    let stderr_path = tmp.path().join("server.stderr");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&release_path)
            .status()
            .expect("mkfifo must execute")
            .success(),
        "create deterministic push-release channel"
    );

    {
        let db = Database::open(&db_path).expect("seed open");
        db.execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4)) SYNC CONFLICT KEEP FIRST",
            &empty_params(),
        )
        .expect("create");
        drop(db);
    }

    let child = spawn_server_with_barrier(
        &db_path,
        TENANT,
        bind_spec,
        &barrier_path,
        &release_path,
        100,
        &stderr_path,
    );
    let in_flight_id = Uuid::new_v4();
    let in_flight_vec = vec![0.5_f32, 0.5, 0.5, 0.5];
    push_change_through_server(ticket, TENANT, "evidence", in_flight_id, in_flight_vec).await;

    let batch_ids: Vec<Uuid> = (0..BENCH_BATCH_ROWS).map(|_| Uuid::new_v4()).collect();

    DrainFixture {
        _tmp: tmp,
        db_path,
        barrier_path,
        release_path,
        stderr_path,
        child: Some(child),
        in_flight_id,
        batch_ids,
    }
}

fn assert_durable(fixture: &DrainFixture) {
    let reopened = (0..5)
        .find_map(|attempt| match Database::open(&fixture.db_path) {
            Ok(db) => Some(db),
            Err(_) if attempt < 4 => {
                std::thread::sleep(Duration::from_millis(50 * (attempt as u64 + 1)));
                None
            }
            Err(e) => panic!("reopen after drain failed: {e:?}"),
        })
        .expect("reopen after drain succeeds within 5 attempts");

    let r = reopened
        .execute("SELECT id FROM evidence", &empty_params())
        .expect("select");
    let id_idx = r.columns.iter().position(|c| c == "id").unwrap();
    let returned: Vec<Value> = r.rows.iter().map(|row| row[id_idx].clone()).collect();
    assert!(
        returned.contains(&Value::Uuid(fixture.in_flight_id)),
        "first in-flight commit must survive graceful drain"
    );
    for id in &fixture.batch_ids {
        assert!(
            returned.contains(&Value::Uuid(*id)),
            "every row from the in-flight batch must survive graceful drain; missing {id}"
        );
    }
}

/// Run one full SIGTERM-drain iteration and return the kill-to-exit elapsed.
/// Re-asserts durability after the timed window.
async fn run_drain_iteration(bind_spec: &str, ticket: &str) -> Duration {
    use std::process::Command;

    let mut fixture = build_fixture(bind_spec, ticket).await;
    let _stderr_dump = ServerStderrDump {
        path: &fixture.stderr_path,
    };

    let batch_for_task = fixture.batch_ids.clone();
    let ticket = ticket.to_string();
    let task_barrier_path = fixture.barrier_path.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let pending_push = tokio::spawn(async move {
        push_many_changes_through_server(
            &ticket,
            TENANT,
            "evidence",
            batch_for_task,
            4,
            Some(started_tx),
            Some(task_barrier_path),
        )
        .await;
    });
    started_rx
        .await
        .expect("in-flight push task must reach the server-side SIGTERM barrier");

    let real_child = fixture.child.take().expect("child present before drain");
    let pid = real_child.id().to_string();

    let kt0 = Instant::now();
    let kill_status = Command::new("kill")
        .args(["-TERM", pid.as_str()])
        .status()
        .expect("kill -TERM must execute");
    assert!(
        kill_status.success(),
        "kill -TERM must signal the server; status: {kill_status:?}"
    );
    std::fs::write(&fixture.release_path, b"release").expect("release in-flight push barrier");

    let exit = wait_for_child_output(real_child, Duration::from_secs(30), "graceful drain").status;
    let elapsed = kt0.elapsed();

    assert!(
        exit.success(),
        "contextdb-server must exit 0 on SIGTERM; status: {exit:?}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "drain budget exceeded: {elapsed:?}"
    );

    tokio::time::timeout(Duration::from_secs(10), pending_push)
        .await
        .expect("in-flight push must complete during graceful drain")
        .expect("in-flight push task must not panic");

    assert_durable(&fixture);

    elapsed
}

fn bench_sigterm_drain_in_flight_persistence(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let sync = rt.block_on(start_sync_fixture());
    let bind_spec = sync.bind_spec.clone();
    let ticket = sync.ticket.clone();

    // Pre-timing correctness gate: run one full drain at 4096-row scale before any timed sample.
    rt.block_on(async {
        let _ = run_drain_iteration(&bind_spec, &ticket).await;
    });

    let mut group = c.benchmark_group("sigterm_drain");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    group.bench_function("drain_4096_rows", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let elapsed = rt.block_on(run_drain_iteration(&bind_spec, &ticket));
                total += elapsed;
            }
            total
        });
    });

    group.finish();
    drop(sync);
}

criterion_group!(benches, bench_sigterm_drain_in_flight_persistence);
criterion_main!(benches);
