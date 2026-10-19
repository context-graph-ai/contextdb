#![allow(clippy::type_complexity)] // Existing compatibility and test seam shape.
//! Ordinary reads, writes, generation maintenance and restarts on disposable DBs.
//! Pipes and existing kernel/publication pauses order the overlap. The timeout
//! below detects a stuck process; it never decides a latency/product verdict.
use super::reader_proof_support as fixture;
use contextdb_core::{Incarnation, Lsn, TenantId, Value, VectorIndexRef, VectorSearchMode};
use contextdb_engine::database::ReceivedSchemaApplyObserverForTest;
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::protocol::WireDdlProvenance;
use contextdb_engine::read_session::{
    ReadKernelCancellationEvent, ReadKernelSourceEvent, ReadKernelTestObserver,
};
use contextdb_engine::sync_types::ChangeSet;
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadCancellation, OwnerReadConfig,
    ReadSession,
};
use std::collections::{BTreeSet, HashMap};
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;

const CHILD: &str = "CONTEXTDB_READER_CONTINUITY_CHILD";
const PATH: &str = "CONTEXTDB_READER_CONTINUITY_PATH";
const RUNTIME: &str = "CONTEXTDB_READER_CONTINUITY_RUNTIME";
const MIXED_ROOT_CHILD: &str = "CONTEXTDB_CONTINUITY_MIXED_ROOT_CHILD";
const WATCH: Duration = Duration::from_secs(600);
const SCHEMA_TENANT: &str = "reader-continuity-schema";
const SCHEMA_SOURCE: &str = "reader-continuity-source";
const SCHEMA_SOURCE_INCARNATION: Incarnation = Incarnation(17);
const SQL: &str =
    "SELECT id FROM continuity_vectors ORDER BY embedding <=> $q USE VECTOR AUTO LIMIT 10";

fn run_with_platform_runtime(test: &str) -> bool {
    if std::env::var_os(MIXED_ROOT_CHILD).is_some() {
        return false;
    }
    let directory = tempfile::tempdir().unwrap();
    let platform = directory.path().join("platform-runtime");
    std::fs::create_dir(&platform).unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&platform, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(crate::child_test_filter(module_path!(), test))
        .arg("--nocapture")
        .env("XDG_RUNTIME_DIR", &platform)
        .env(MIXED_ROOT_CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "mixed-root child failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    true
}
fn q() -> HashMap<String, Value> {
    HashMap::from([("q".into(), Value::Vector(vec![1., 0., 0.]))])
}
#[derive(Default)]
struct GraphObservation {
    builds: AtomicU64,
}
impl contextdb_vector::hnsw::HnswBuildObserver for GraphObservation {
    fn before_hnsw_build(&self) {
        self.builds.fetch_add(1, Ordering::SeqCst);
    }
}
fn seed(path: &Path) {
    seed_rows(path, 1250);
}
fn seed_rows(path: &Path, rows: i64) {
    seed_rows_with_observer(path, rows, Arc::new(GraphObservation::default()));
}
fn seed_rows_with_observer(path: &Path, rows: i64, builds: Arc<GraphObservation>) {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE continuity_vectors (id INTEGER PRIMARY KEY, payload TEXT, embedding VECTOR(3)) HISTORY ALL", &HashMap::new()).unwrap();
    let tx = db.begin().unwrap();
    for id in 0..rows {
        let x = 1. - id as f32 * 1.5 / rows as f32;
        db.execute_in_tx(
            tx,
            "INSERT INTO continuity_vectors VALUES ($id, 'old', $v)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("v".into(), Value::Vector(vec![x, (1. - x * x).sqrt(), 0.])),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    contextdb_vector::hnsw::with_hnsw_build_observer(builds.clone(), || {
        fixture::finish_maintenance(&db, 1)
    });
    assert!(
        builds.builds.load(Ordering::SeqCst) > 0,
        "the observer witnesses actual fixture construction"
    );
    db.close().unwrap();
}
fn writer(path: &Path, runtime: &Path) -> Database {
    let db = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            owner_reads: OwnerReadConfig {
                runtime_dir: (!runtime.as_os_str().is_empty()).then(|| runtime.to_owned()),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .expect("writer acquires the store while the earlier file session is alive");
    initialize_replay_callbacks(&db);
    db
}

fn initialize_replay_callbacks(db: &Database) {
    let registered = db.registered_trigger_callbacks();
    for trigger in db.list_triggers() {
        if !registered.contains(&trigger.name) {
            db.register_trigger_callback(&trigger.name, |_db, _context| Ok(()))
                .expect("register the fixture's declared trigger callback");
        }
    }
    db.complete_initialization()
        .expect("initialize the fixture only after every callback is registered");
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SchemaReplayUnit {
    changes: ChangeSet,
    ddl_provenance: Vec<WireDdlProvenance>,
}

/// The identity the fixture author signs its schema with: a real ed25519 key,
/// so a receiver checks authorship against the author's public key.
fn schema_author() -> &'static contextdb_engine::identity::FabricIdentity {
    static AUTHOR: std::sync::OnceLock<contextdb_engine::identity::FabricIdentity> =
        std::sync::OnceLock::new();
    AUTHOR.get_or_init(contextdb_engine::identity::FabricIdentity::generate)
}

fn schema_replay_unit(author: &Database, changes: ChangeSet) -> SchemaReplayUnit {
    let ddl_provenance = {
        let seam_read_memory = author
            .declared_reader_memory()
            .expect("admit the fixture operation reader");
        author.outbound_ddl_provenance_for_test(
            &changes,
            &TenantId::from(SCHEMA_TENANT),
            &schema_author().node_id(),
            SCHEMA_SOURCE_INCARNATION,
            &|bytes| Ok(schema_author().sign_for_test(bytes)),
            seam_read_memory,
        )
    }
    .expect("source produces canonical DDL provenance for the exact schema unit");
    SchemaReplayUnit {
        changes,
        ddl_provenance,
    }
}

fn apply_received_schema(db: &Database, unit: SchemaReplayUnit) {
    {
        let seam_read_memory = db
            .declared_reader_memory()
            .expect("admit the fixture operation reader");
        let mut seam_copy_owner =
            contextdb_engine::vector_read_memory::admission(seam_read_memory.clone());
        db.apply_received_schema_for_test(
            unit.changes,
            unit.ddl_provenance,
            TenantId::from(SCHEMA_TENANT),
            SCHEMA_SOURCE,
            SCHEMA_SOURCE_INCARNATION,
            &mut seam_copy_owner,
            seam_read_memory,
        )
    }
    .expect("authenticated dependency-complete received schema applies");
}

fn expected_replay_tables() -> BTreeSet<String> {
    (0..100)
        .map(|ordinal| format!("schema_{ordinal:03}"))
        .collect()
}
fn replace_generation(db: &Database) {
    let index = VectorIndexRef::new("continuity_vectors", "embedding");
    let generation = || {
        let state = db
            .vector_store_for_test()
            .partition_graph_generation_status(&contextdb_vector::VectorPartitionRef::new(
                index.clone(),
                contextdb_core::VectorPartitionKey::unpartitioned(),
            ))
            .unwrap();
        (
            state.base.or(state.dormant_base).map(|g| g.generation_id),
            state
                .change
                .or(state.dormant_change)
                .map(|g| g.generation_id),
        )
    };
    let before = generation();
    assert!(
        before.0.is_some(),
        "the fixture has a healthy saved generation"
    );
    let count = db
        .execute("SELECT COUNT(*) FROM continuity_vectors", &HashMap::new())
        .unwrap();
    let Value::Int64(rows) = count.rows[0][0] else {
        panic!("integer row count")
    };
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "UPDATE continuity_vectors SET payload = 'new', embedding = $v WHERE id < $changed",
        &HashMap::from([
            ("v".into(), Value::Vector(vec![-1., 0., 0.])),
            ("changed".into(), Value::Int64(rows / 3)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();
    for _ in 0..64 {
        db.run_maintenance_cycle().unwrap();
        if generation() != before {
            break;
        }
    }
    assert_ne!(
        generation(),
        before,
        "the writer really publishes a replacement generation"
    );
    db.run_pruning_cycle_checked().unwrap();
    db.run_maintenance_cycle().unwrap(); // actual old-generation reclamation attempt
    assert_eq!(
        db.execute(
            "SELECT payload FROM continuity_vectors WHERE id = 0",
            &HashMap::new()
        )
        .unwrap()
        .rows,
        vec![vec![Value::Text("new".into())]]
    );
}
fn receipt(s: &str) {
    println!("READER_PROOF:{s}");
    std::io::stdout().flush().unwrap();
}
fn next(input: &mut impl Iterator<Item = std::io::Result<String>>) -> String {
    input
        .next()
        .expect("parent command is present")
        .expect("parent pipe remains open")
}
fn child_dispatch() -> bool {
    let Ok(role) = std::env::var(CHILD) else {
        return false;
    };
    let path = std::path::PathBuf::from(std::env::var(PATH).unwrap());
    let runtime = std::path::PathBuf::from(std::env::var(RUNTIME).unwrap());
    let stdin = std::io::stdin();
    let mut input = stdin.lock().lines();
    if role == "reader" {
        let session = fixture::open_file(&path, roomy(), Arc::new(fixture::Routes::default()));
        assert!(session.file_snapshot_for_test().is_some());
        receipt("reader-open");
        assert_eq!(next(&mut input), "close");
        drop(session);
        receipt("reader-closed");
        return true;
    }
    assert_eq!(role, "writer");
    let mut db = Some(writer(&path, &runtime));
    receipt("writer-open");
    while let Some(command) = input.next() {
        match command.unwrap().as_str() {
            "replace" => {
                replace_generation(db.as_ref().unwrap());
                receipt("replaced");
            }
            "restart" => {
                db.take().unwrap().close().unwrap();
                receipt("writer-closed");
                db = Some(writer(&path, &runtime));
                receipt("writer-restarted");
            }
            command if command.starts_with("identical ") => {
                let unit: SchemaReplayUnit =
                    serde_json::from_slice(&std::fs::read(&command[10..]).unwrap()).unwrap();
                let db = db.as_ref().unwrap();
                let trace = ReceivedSchemaApplyObserverForTest::new();
                let scope = db.observe_received_schema_applies_for_test(&trace);
                apply_received_schema(db, unit);
                drop(scope);
                let observation = trace
                    .observation()
                    .expect("identical apply reached schema kernel");
                assert_eq!(observation.staged_table_images_copied, 0);
                assert_eq!(observation.durable_table_rewrites, 0);
                receipt("identical-published");
            }
            command if command.starts_with("replay ") => {
                let unit: SchemaReplayUnit =
                    serde_json::from_slice(&std::fs::read(&command[7..]).unwrap()).unwrap();
                let db = db.as_ref().unwrap();
                let trace = ReceivedSchemaApplyObserverForTest::new();
                let _trace_scope = db.observe_received_schema_applies_for_test(&trace);
                let pause = db.pause_before_received_schema_durable_commit_for_test();
                std::thread::scope(|scope| {
                    let applying = scope.spawn(|| apply_received_schema(db, unit));
                    assert!(
                        pause.wait_until_reached(WATCH),
                        "real received-schema apply reaches durable commit"
                    );
                    let staged = trace
                        .observation()
                        .expect("received apply attached its physical work observer");
                    assert_eq!(staged.staged_table_names, expected_replay_tables());
                    assert_eq!(staged.staged_table_images_copied, 100);
                    assert_eq!(staged.staged_rows_copied, 100);
                    assert_eq!(staged.staged_graph_edges_copied, 0);
                    assert_eq!(staged.staged_vectors_copied, 0);
                    assert_eq!(staged.durable_table_rewrites, 0);
                    receipt("schema-parked");
                    assert_eq!(next(&mut input), "publish");
                    pause.release();
                    applying.join().unwrap();
                });
                let observation = trace
                    .observation()
                    .expect("completed received apply retains its physical work receipt");
                assert_eq!(observation.durable_table_names, expected_replay_tables());
                assert_eq!(observation.durable_table_rewrites, 100);
                assert_eq!(observation.durable_rows_rewritten, 100);
                let relational_artifact_operations = observation
                    .artifact_writes
                    .iter()
                    .filter(|(event, _)| {
                        event.table.starts_with("rel_schema_")
                            || (event.table == "__redb_user_table_directory"
                                && event.key.starts_with(b"rel_schema_"))
                    })
                    .map(|(_, count)| *count)
                    .sum::<u64>();
                assert!(
                    relational_artifact_operations > 0,
                    "the Redb observer must witness actual affected-table artifact writes"
                );
                eprintln!(
                    "SCHEMA_REPLAY_WORK staged_tables={} staged_rows={} durable_tables={} durable_rows={} relational_artifact_operations={} profile_nanos={:?} profile_work={:?}",
                    observation.staged_table_images_copied,
                    observation.staged_rows_copied,
                    observation.durable_table_rewrites,
                    observation.durable_rows_rewritten,
                    relational_artifact_operations,
                    observation.profile_nanos,
                    observation.profile_work,
                );
                receipt("schema-published");
            }
            "close" => {
                db.take().unwrap().close().unwrap();
                receipt("closed");
                break;
            }
            other => panic!("unknown disposable fixture command {other}"),
        }
    }
    true
}
struct Process {
    child: Child,
    input: ChildStdin,
    lines: mpsc::Receiver<String>,
    output_thread: Option<std::thread::JoinHandle<()>>,
}
impl Process {
    fn start(test: &str, role: &str, path: &Path, runtime: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &crate::child_test_filter(module_path!(), test),
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, role)
            .env(PATH, path)
            .env(RUNTIME, runtime)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::channel();
        let output_thread = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else {
                    break;
                };
                if let Some(event) = line.split_once("READER_PROOF:").map(|(_, event)| event) {
                    let _ = tx.send(event.to_owned());
                }
            }
        });
        Self {
            child,
            input,
            lines,
            output_thread: Some(output_thread),
        }
    }
    fn send(&mut self, command: &str) {
        writeln!(self.input, "{command}").unwrap();
        self.input.flush().unwrap();
    }
    fn expect(&self, expected: &str) {
        assert_eq!(
            self.lines
                .recv_timeout(WATCH)
                .unwrap_or_else(|_| {
                    panic!(
                        "child did not reach {expected}; watchdog detects stalls but is not synchronization"
                    )
                }),
            expected
        );
    }
    fn finish(mut self) {
        self.send("close");
        self.expect("closed");
        assert!(self.child.wait().unwrap().success());
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        if let Some(output) = self.output_thread.take() {
            let _ = output.join();
        }
    }
}
fn roomy() -> contextdb_engine::ReadSessionOptions {
    let mut o = fixture::options();
    // This overlap proof measures snapshot continuity, not the separate
    // 24 MiB lazy-working-set wall. Give both the session and statement the
    // declared CG reader ceiling so setup reaches the writer overlap.
    o.memory_limit = None;
    o.limits.memory = 1024 * 1024 * 1024;
    o
}
struct Park {
    armed: AtomicBool,
    reached: mpsc::Sender<()>,
    release: (Mutex<bool>, Condvar),
}
impl Park {
    fn free(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }
}
struct Release(Arc<Park>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.free();
    }
}
impl ReadKernelTestObserver for Park {
    fn before_source_touch(&self, _: ReadKernelSourceEvent, _: OwnerReadCancellation) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.reached.send(()).unwrap();
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
        }
    }
    fn cancellation_observed(&self, _: ReadKernelCancellationEvent, _: OwnerReadCancellation) {}
}
#[test]
fn a_file_reader_keeps_its_snapshot_after_a_writer_takes_the_store() {
    if run_with_platform_runtime("a_file_reader_keeps_its_snapshot_after_a_writer_takes_the_store")
    {
        return;
    }
    // Fixed file-route continuity under the shared selected runtime root:
    if child_dispatch() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("overlap.db");
    let runtime = fixture::runtime(root.path());
    seed(&path);
    let routes = Arc::new(fixture::Routes::default());
    let (tx, reached) = mpsc::channel();
    let park = Arc::new(Park {
        armed: AtomicBool::new(true),
        reached: tx,
        release: (Mutex::new(false), Condvar::new()),
    });
    let reader = ReadSession::open_with_observations_for_test(
        &path,
        roomy(),
        Arc::new(ManualDeadlineClock::default()),
        Some(routes.clone()),
        Some(park.clone()),
    )
    .unwrap();
    let snapshot = reader.file_snapshot_for_test().unwrap();
    std::thread::scope(|scope| {
        let reading = scope.spawn(|| {
            reader.execute(
                "SELECT id, payload FROM continuity_vectors ORDER BY id",
                &HashMap::new(),
            )
        });
        let release = Release(park.clone()); // releases even if writer admission fails
        reached.recv_timeout(WATCH).unwrap();
        let mut writer = Process::start(
            "a_file_reader_keeps_its_snapshot_after_a_writer_takes_the_store",
            "writer",
            &path,
            &runtime,
        );
        writer.expect("writer-open");
        writer.send("replace");
        writer.expect("replaced");
        drop(release);
        let answer = reading
            .join()
            .unwrap()
            .expect("active original-snapshot statement survives writer commit/reclamation");
        assert_eq!(answer.rows.len(), 1250);
        assert!(
            answer
                .rows
                .iter()
                .all(|r| r[1] == Value::Text("old".into()))
        );
        assert_eq!(reader.file_snapshot_for_test(), Some(snapshot));
        writer.send("restart");
        writer.expect("writer-closed");
        writer.expect("writer-restarted");
        assert_eq!(
            reader
                .execute(
                    "SELECT payload FROM continuity_vectors WHERE id = 0",
                    &HashMap::new()
                )
                .unwrap()
                .rows,
            vec![vec![Value::Text("old".into())]]
        );
        assert_eq!(reader.file_snapshot_for_test(), Some(snapshot));
        routes.file_only();
        writer.finish();
    });
    let witness = reader.file_memory_for_test().unwrap().3;
    drop(reader);
    assert!(witness.upgrade().is_none());
    let latest = fixture::open_file(&path, roomy(), Arc::new(fixture::Routes::default()));
    assert_ne!(latest.file_snapshot_for_test(), Some(snapshot));
    assert_eq!(
        latest
            .execute(
                "SELECT payload FROM continuity_vectors WHERE id = 0",
                &HashMap::new()
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("new".into())]]
    );
}

#[test]
fn lazy_generation_load_beside_a_writer_keeps_serving() {
    if run_with_platform_runtime("lazy_generation_load_beside_a_writer_keeps_serving") {
        return;
    }
    // The selected root holds both the writer's and readers' snapshot registrations:
    if child_dispatch() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("lazy.db");
    let runtime = fixture::runtime(root.path());
    let graph = Arc::new(GraphObservation::default());
    seed_rows_with_observer(&path, 10_000, graph.clone());
    assert!(
        graph.builds.load(Ordering::SeqCst) > 0,
        "the exact observer used below sees a real fixture graph build"
    );
    graph.builds.store(0, Ordering::SeqCst);
    // Independent baseline reader closes before the tested readers open; neither
    // tested session has loaded the saved graph when the writer takes ownership.
    let baseline = ReadSession::with_runtime_directory_for_test(&runtime, || {
        fixture::open_file(&path, roomy(), Arc::new(fixture::Routes::default()))
    });
    let before_auto = baseline.execute(SQL, &q()).unwrap().rows;
    let before_indexed = baseline
        .execute(&SQL.replace("AUTO", "INDEXED"), &q())
        .unwrap()
        .rows;
    drop(baseline);
    let auto_routes = Arc::new(fixture::Routes::default());
    let indexed_routes = Arc::new(fixture::Routes::default());
    let (auto, indexed) = ReadSession::with_runtime_directory_for_test(&runtime, || {
        contextdb_vector::hnsw::with_hnsw_build_observer(graph.clone(), || {
            (
                fixture::open_file(&path, roomy(), auto_routes.clone()),
                fixture::open_file(&path, roomy(), indexed_routes.clone()),
            )
        })
    });
    assert_eq!(graph.builds.load(Ordering::SeqCst), 0);
    let snapshot = auto.file_snapshot_for_test();
    assert_eq!(snapshot, indexed.file_snapshot_for_test());
    let mut writer = Process::start(
        "lazy_generation_load_beside_a_writer_keeps_serving",
        "writer",
        &path,
        Path::new(""),
    );
    writer.expect("writer-open");
    writer.send("replace");
    writer.expect("replaced");
    for (reader, mode) in [(&auto, "AUTO"), (&indexed, "INDEXED")] {
        let backend_bytes = Arc::new(AtomicU64::new(0));
        let observed_bytes = backend_bytes.clone();
        let backend_observation = redb::observe_backend_reads(Arc::new(move |bytes| {
            observed_bytes.fetch_add(bytes as u64, Ordering::SeqCst);
        }));
        let answer = contextdb_vector::hnsw::with_hnsw_build_observer(graph.clone(), || {
            reader.execute(&SQL.replace("AUTO", mode), &q())
        })
        .expect("first lazy graph load under writer ownership must serve the original snapshot");
        drop(backend_observation);
        assert!(
            backend_bytes.load(Ordering::SeqCst) > 0,
            "the original retained snapshot faults actual backend pages under {mode}"
        );
        assert_eq!(
            graph.builds.load(Ordering::SeqCst),
            0,
            "read-open/query never rebuilds the graph"
        );
        let expected = if mode == "AUTO" {
            &before_auto
        } else {
            &before_indexed
        };
        assert_eq!(
            &answer.rows, expected,
            "the identical query on the original snapshot retains its answer"
        );
        assert_eq!(
            answer.trace.vector_search.unwrap().resolved_mode,
            VectorSearchMode::Indexed
        );
        assert_eq!(reader.file_snapshot_for_test(), snapshot);
    }
    writer.send("restart");
    writer.expect("writer-closed");
    writer.expect("writer-restarted");
    for reader in [&auto, &indexed] {
        assert_eq!(reader.execute(SQL, &q()).unwrap().rows, before_auto);
    }
    auto_routes.file_only();
    indexed_routes.file_only();
    writer.finish();
}

fn declare_replay_schema(db: &Database) {
    for ordinal in 0..100 {
        let ddl = if ordinal == 0 {
            "CREATE TABLE schema_000 (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF".into()
        } else {
            format!(
                "CREATE TABLE schema_{ordinal:03} (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES schema_{:03}(id), body TEXT, FOREIGN KEY (parent_id) REFERENCES schema_{:03}(id)) SYNC OFF",
                ordinal - 1,
                ordinal - 1
            )
        };
        db.execute(&ddl, &HashMap::new()).unwrap();
    }
    db.execute(
        "CREATE EVENT TYPE schema_099_insert WHEN INSERT ON schema_099",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER schema_099_insert_trigger ON schema_099 WHEN INSERT INCLUDING SYNC",
        &HashMap::new(),
    )
    .unwrap();
}
fn read_journey(reader: &ReadSession) -> (Vec<Vec<Value>>, Vec<Vec<Value>>, Vec<Vec<Value>>) {
    let find = reader.execute("SELECT id FROM reader_work WHERE context_id = $context ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 10",
        &HashMap::from([("q".into(), Value::Vector(fixture::vector(0))), ("context".into(), Value::Uuid(fixture::context_id(0)))])).unwrap();
    assert_eq!(
        find.trace.vector_search.unwrap().resolved_mode,
        VectorSearchMode::Indexed
    );
    let show = reader
        .execute(
            "SELECT id, payload FROM reader_work WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(fixture::row_id(0)))]),
        )
        .unwrap();
    // Engine metadata is the read-only status door. The separate ledger proof
    // owns its four consumer status surfaces; no missing ledger API is called here.
    let status = reader
        .execute("SHOW VECTOR_INDEXES", &HashMap::new())
        .unwrap();
    assert!(!find.rows.is_empty() && !show.rows.is_empty() && !status.rows.is_empty());
    (find.rows, show.rows, status.rows)
}
#[test]
fn schema_replay_does_not_interrupt_active_readers() {
    if child_dispatch() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("schema-overlap.db");
    let runtime = fixture::runtime(root.path());
    // Keep the frozen resident fixture; no reduced-vector debug alternative.
    fixture::seed_working_set(&path);
    let author = Database::open_memory();
    declare_replay_schema(&author);
    let initial = author
        .changes_since(Lsn(0))
        .expect("read the declared changes");
    assert_eq!(initial.ddl.len(), 102);
    let initial = schema_replay_unit(&author, initial);
    let receiver = writer(&path, &runtime);
    // Establish already-held declarations through ordinary local DDL, so setup
    // does not pay the very received-schema rewrite whose overlap is tested.
    declare_replay_schema(&receiver);
    initialize_replay_callbacks(&receiver);
    // The foreign-key chain requires one parent row per table. Seed it in one
    // transaction so setup publishes the fixture once rather than 100 times.
    let seed_tx = receiver.begin().unwrap();
    for ordinal in 0..100 {
        let sql = if ordinal == 0 {
            "INSERT INTO schema_000 VALUES (1, 'kept')".into()
        } else {
            format!("INSERT INTO schema_{ordinal:03} VALUES (1, 1, 'kept')")
        };
        receiver
            .execute_in_tx(seed_tx, &sql, &HashMap::new())
            .unwrap();
    }
    receiver.commit(seed_tx).unwrap();
    receiver.close().unwrap();
    let identical_path = root.path().join("identical.json");
    std::fs::write(&identical_path, serde_json::to_vec(&initial).unwrap()).unwrap();
    let since = author.current_lsn();
    for ordinal in 0..100 {
        author
            .execute(
                &format!("ALTER TABLE schema_{ordinal:03} ADD COLUMN revised TEXT"),
                &HashMap::new(),
            )
            .unwrap();
    }
    let migration = author
        .changes_since(since)
        .expect("read the declared changes");
    assert_eq!(
        migration.ddl.len(),
        100,
        "largest affected set remains all 100 tables"
    );
    let migration = schema_replay_unit(&author, migration);
    let migration_path = root.path().join("migration.json");
    std::fs::write(&migration_path, serde_json::to_vec(&migration).unwrap()).unwrap();

    let routes = Arc::new(fixture::Routes::default());
    let reader = fixture::open_file(&path, roomy(), routes.clone());
    let snapshot = reader.file_snapshot_for_test().unwrap();
    let before = read_journey(&reader);
    let mut cursor = reader
        .open_cursor(
            "SELECT id, payload FROM reader_work ORDER BY id",
            &HashMap::new(),
        )
        .unwrap();
    assert!(cursor.first_page().has_more);
    let mut writer = Process::start(
        "schema_replay_does_not_interrupt_active_readers",
        "writer",
        &path,
        &runtime,
    );
    writer.expect("writer-open");
    writer.send(&format!("identical {}", identical_path.display()));
    writer.expect("identical-published");
    assert_eq!(read_journey(&reader), before);
    writer.send(&format!("replay {}", migration_path.display()));
    writer.expect("schema-parked");
    assert_eq!(
        read_journey(&reader),
        before,
        "find/show/status while received-schema publication is parked"
    );
    let page = cursor.fetch(std::num::NonZeroUsize::new(2)).unwrap();
    assert_eq!(page.rows[0][0], Value::Uuid(fixture::row_id(2)));
    writer.send("publish");
    writer.expect("schema-published");
    assert_eq!(read_journey(&reader), before);
    assert_eq!(reader.file_snapshot_for_test(), Some(snapshot));
    assert!(
        reader
            .execute("SELECT revised FROM schema_099", &HashMap::new())
            .is_err(),
        "an old file snapshot must not acquire the new schema mid-session"
    );
    assert_eq!(
        reader
            .execute("SELECT body FROM schema_099", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Text("kept".into())]]
    );
    cursor.close().unwrap();
    routes.file_only();
    writer.finish();
    drop(reader);
    let restarted = fixture::open_file(&path, roomy(), Arc::new(fixture::Routes::default()));
    assert_ne!(restarted.file_snapshot_for_test(), Some(snapshot));
    assert_eq!(
        restarted
            .execute("SELECT body, revised FROM schema_099", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Text("kept".into()), Value::Null]]
    );
    assert_eq!(
        restarted
            .execute("SELECT body, revised FROM schema_000", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Text("kept".into()), Value::Null]]
    );
}

#[test]
fn a_dead_reader_process_does_not_prevent_later_write_maintenance_and_restart() {
    if child_dispatch() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("reader-death.db");
    let runtime = fixture::runtime(root.path());
    seed(&path);
    let mut child = Process::start(
        "a_dead_reader_process_does_not_prevent_later_write_maintenance_and_restart",
        "reader",
        &path,
        &runtime,
    );
    child.expect("reader-open");
    // Kill only the explicitly spawned disposable test process. Waiting reaps
    // it before subsequent admission; no heartbeat delay stands in for death.
    child.child.kill().unwrap();
    child.child.wait().unwrap();
    drop(child);
    let db = writer(&path, &runtime);
    replace_generation(&db);
    db.close().unwrap();
    let reader = fixture::open_file(&path, roomy(), Arc::new(fixture::Routes::default()));
    assert_eq!(
        reader
            .execute(
                "SELECT payload FROM continuity_vectors WHERE id = 0",
                &HashMap::new()
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("new".into())]]
    );
}

#[test]
fn an_owner_query_keeps_its_registered_snapshot_while_the_writer_commits() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("owner-overlap.db");
    let runtime = fixture::runtime(root.path());
    seed(&path);
    let baseline = fixture::open_file(&path, roomy(), Arc::new(fixture::Routes::default()));
    let expected = baseline
        .execute(&SQL.replace("AUTO", "EXACT"), &q())
        .unwrap()
        .rows;
    drop(baseline);
    let (tx, reached) = mpsc::channel();
    let park = Arc::new(Park {
        armed: AtomicBool::new(true),
        reached: tx,
        release: (Mutex::new(false), Condvar::new()),
    });
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..Default::default()
            },
            test_kernel_observer: Some(park.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        let mut options = roomy();
        // This fixture synchronizes at a source barrier; the writer, not
        // elapsed kernel time, controls when the reader may continue.
        options.limits.active_ms = u64::MAX;
        ReadSession::open_with_options(&path, options)
    })
    .unwrap();
    assert_eq!(
        reader.route(),
        contextdb_core::read_contract::ReadRoute::Owner
    );
    std::thread::scope(|scope| {
        let reading = scope.spawn(|| reader.execute(SQL, &q()));
        let release = Release(park.clone());
        reached.recv_timeout(WATCH).unwrap();
        // Ordinary writes and an actual generation publication complete before
        // the active reader is released. No user retry is issued on either side.
        let (done, completed) = mpsc::channel();
        let writable = &db;
        let writing = scope.spawn(move || {
            replace_generation(writable);
            done.send(()).unwrap();
        });
        let progress = completed.recv_timeout(WATCH);
        drop(release);
        writing.join().unwrap();
        let answer = reading
            .join()
            .unwrap()
            .expect("owner query completes its registered snapshot");
        progress.expect("the writer made progress before the reader was released");
        assert_eq!(answer.rows, expected);
    });
    assert_eq!(
        reader
            .execute(
                "SELECT payload FROM continuity_vectors WHERE id = 0",
                &HashMap::new()
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("new".into())]],
        "later owner statements correctly see the later committed state"
    );
    drop(reader);
    db.close().unwrap();
}
