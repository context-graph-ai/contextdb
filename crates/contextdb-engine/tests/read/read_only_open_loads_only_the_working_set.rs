//! Independent accounting proof for the file route's actual decode and digest work.
//!
//! These existing loop-local controls remain diagnostics. The fixed-budget,
//! eight-Context production-path acceptance lives in working_set_bounded_contract.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::direct_file_reader::test_seams::{
    DirectImageMetadataKind, DirectImageState, DirectMetadataBody, DirectMetadataRequest,
    DirectReaderConfig, ReadWorkCounters, ReadWorkObservationScope, ReadWorkPhase, open_for_test,
    owned_image_for_test, typed_digest_for_test,
};
use contextdb_engine::persistence::RedbPersistence;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;

const VECTOR_DIMENSIONS: usize = 768;
const TEXT_BYTES: usize = 512;
const SMALL_ROWS: usize = 10;
const LARGE_ROWS: usize = 10_000;
const COMMIT_ROWS: usize = 1_000;

struct FixedClock;

impl DeadlineClock for FixedClock {
    fn now_ms(&self) -> u64 {
        1
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn reader_config() -> DirectReaderConfig {
    DirectReaderConfig::new(
        ReadLimits {
            result_rows: 100,
            result_bytes: 2 * 1024 * 1024,
            work: 1_000_000,
            active_ms: 10_000,
            memory: 128 * 1024 * 1024,
            cursor_page_rows: 100,
            cursor_page_bytes: 2 * 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 20_000,
        },
        Arc::new(FixedClock),
    )
}

fn payload(row: usize) -> String {
    let seed = format!("{row:08x}");
    seed.repeat(TEXT_BYTES / seed.len())
}

fn vector(row: usize) -> Vec<f32> {
    (0..VECTOR_DIMENSIONS)
        .map(|component| (row * VECTOR_DIMENSIONS + component) as f32)
        .collect()
}

fn row_id(row: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0x4a11_0000_0000_0000_0000_0000_0000_0000 + row as u128)
}

fn seed(path: &Path, rows: usize) {
    let database = Database::open(path)
        .unwrap_or_else(|error| panic!("open fixture {}: {error}", path.display()));
    database
        .execute(
            "CREATE TABLE reader_work (id UUID PRIMARY KEY, payload TEXT NOT NULL, embedding VECTOR(768))",
            &HashMap::new(),
        )
        .expect("create read-work fixture table");

    for start in (0..rows).step_by(COMMIT_ROWS) {
        let tx = database.begin().expect("begin fixed-size fixture commit");
        for row in start..rows.min(start + COMMIT_ROWS) {
            database
                .execute_in_tx(
                    tx,
                    "INSERT INTO reader_work (id, payload, embedding) VALUES ($id, $payload, $embedding)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Uuid(row_id(row))),
                        ("payload".to_owned(), Value::Text(payload(row))),
                        ("embedding".to_owned(), Value::Vector(vector(row))),
                    ]),
                )
                .unwrap_or_else(|error| panic!("insert fixture row {row}: {error}"));
        }
        database
            .commit(tx)
            .unwrap_or_else(|error| panic!("commit fixture rows {start}: {error}"));
    }
    database.close().expect("close fixture writer");
}

#[derive(Debug)]
struct Measurement {
    rows: usize,
    open: ReadWorkCounters,
    query: ReadWorkCounters,
}

fn measure(path: &Path, rows: usize) -> Measurement {
    let open_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    assert_eq!(open_scope.phase(), ReadWorkPhase::Open);
    let reader = open_for_test(path, reader_config())
        .unwrap_or_else(|error| panic!("open file reader {}: {error}", path.display()));
    let open = open_scope.counters();
    drop(open_scope);

    let query_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    assert_eq!(query_scope.phase(), ReadWorkPhase::Query);
    let answer = reader
        .execute(
            "SELECT payload FROM reader_work WHERE id = $id",
            &HashMap::from([("id".to_owned(), Value::Uuid(row_id(0)))]),
        )
        .expect("bounded selected-key query");
    assert_eq!(
        answer.result.rows.len(),
        1,
        "selected key remains answerable"
    );
    let query = query_scope.counters();
    drop(query_scope);
    drop(reader);
    Measurement { rows, open, query }
}

fn observe_open(path: &Path) -> ReadWorkCounters {
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    let reader = open_for_test(path, reader_config()).expect("open bounded config fixture");
    let counters = scope.counters();
    drop(reader);
    counters
}

fn copied_startup_setting_count(path: &Path) -> usize {
    let raw = redb::Database::open(path).expect("inspect startup settings copy");
    let read = raw.begin_read().expect("read startup settings copy");
    let copied: TableDefinition<&str, &[u8]> = TableDefinition::new("read_open_config");
    let table = read.open_table(copied).expect("startup settings table");
    let root = table
        .get("")
        .expect("startup settings root read")
        .expect("startup settings root");
    let root_count = u64::from_be_bytes(
        root.value()[..8]
            .try_into()
            .expect("startup settings count"),
    );
    let copied_count = table
        .iter()
        .expect("iterate startup settings")
        .map(|entry| entry.expect("copied startup setting"))
        .filter(|(key, _)| !key.value().is_empty())
        .count();
    assert_eq!(
        root_count, copied_count as u64,
        "ordered startup digest count covers each copied setting"
    );
    copied_count
}

#[test]
fn open_configuration_proofs_follow_needed_settings_not_unrelated_config_entries() {
    let root = tempfile::tempdir().expect("configuration proof fixtures");
    let lean = root.path().join("lean.db");
    let crowded = root.path().join("crowded.db");
    let routed = root.path().join("routed.db");
    for path in [&lean, &crowded, &routed] {
        seed(path, SMALL_ROWS);
    }
    let persistence = RedbPersistence::open(&crowded).expect("open crowded config writer");
    for index in 0..96 {
        persistence
            .flush_config_value(&format!("unrelated_blob_{index:03}"), &"x".repeat(1024))
            .expect("write unrelated config entry");
    }
    drop(persistence);

    let db = Database::open(&routed).expect("open routed fixture writer");
    for statement in [
        "CREATE EVENT TYPE reader_added WHEN INSERT ON reader_work",
        "CREATE SINK reader_sink TYPE callback",
        "CREATE ROUTE reader_to_sink EVENT reader_added TO reader_sink",
        "CREATE TRIGGER reader_trigger ON reader_work WHEN INSERT INCLUDING SYNC",
    ] {
        db.execute(statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    db.close().expect("seal routed fixture");

    // Making `DURABLE_LINEAGE_TABLE_GENERATION_PREFIX` a bound open-config key,
    // needed for pending-delete lineage, makes open prove one more needed
    // setting than before: settings (1, 1, 6), not (0, 0, 5). Open defers the
    // history proof to the first read that needs it, so it reads no proof
    // node; what it hashes follows the needed settings, never the unrelated
    // config entries the crowded fixture adds.
    let lean_settings = copied_startup_setting_count(&lean);
    let crowded_settings = copied_startup_setting_count(&crowded);
    let routed_settings = copied_startup_setting_count(&routed);
    assert_eq!(
        (lean_settings, crowded_settings, routed_settings),
        (1, 1, 6)
    );
    let lean_open = observe_open(&lean);
    let crowded_open = observe_open(&crowded);
    let routed_open = observe_open(&routed);
    assert_eq!(crowded_open, lean_open);
    for (label, open) in [("lean", &lean_open), ("routed", &routed_open)] {
        assert_eq!(
            (open.proof_nodes_read, open.ordered_proof_nodes_read),
            (0, 0),
            "{label}: open defers the history proof: {open:?}"
        );
    }
    assert!(
        routed_open.hashed_bytes >= lean_open.hashed_bytes,
        "the routed fixture's needed settings are authenticated at open: lean={lean_open:?} routed={routed_open:?}"
    );
    for open in [lean_open, crowded_open, routed_open] {
        assert_eq!(open.proof_nodes_read - open.ordered_proof_nodes_read, 0);
    }
    assert_eq!(
        routed_settings - lean_settings,
        5,
        "the ordered startup digest includes each added needed setting"
    );
    for open in [lean_open, crowded_open, routed_open] {
        assert_eq!(open.decoded_rows, 0);
        assert_eq!(open.decoded_vectors, 0);
        assert_eq!(open.stored_row_bytes, 0);
        assert_eq!(open.vector_body_accesses, 0);
    }
}

fn assert_digest_positive_control(path: &Path) {
    let reader = open_for_test(path, reader_config()).expect("materialize an actual reader image");
    let image = owned_image_for_test(&reader).expect("observe production reader image");
    drop(reader);

    let outer = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    let _digest = typed_digest_for_test(&image).expect("digest actual reader image");
    let once = outer.counters();
    assert_eq!(once.hashed_rows, 0);
    assert_eq!(once.hashed_vectors, 0);
    assert!(once.hashed_bytes > 0);

    let nested = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let _digest = typed_digest_for_test(&image).expect("digest inside nested scope");
    let nested_counts = nested.counters();
    assert!(nested_counts.hashed_bytes > 0);
    drop(nested);

    let panic_result = catch_unwind(AssertUnwindSafe(|| {
        let _panic_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Commit);
        let _digest = typed_digest_for_test(&image).expect("digest before panic");
        panic!("exercise scoped observer unwind restoration");
    }));
    assert!(
        panic_result.is_err(),
        "the nested observation must actually unwind"
    );
    let _digest = typed_digest_for_test(&image).expect("digest after nested unwind");
    let restored = outer.counters();
    assert_eq!(restored.hashed_bytes, once.hashed_bytes * 2);
    println!(
        "read-work actual digest counters: first={once:?}; nested={nested_counts:?}; restored={restored:?}"
    );
}

fn mutation_cost_control(path: &Path) -> ReadWorkCounters {
    let commit_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Commit);
    assert_eq!(commit_scope.phase(), ReadWorkPhase::Commit);
    let database = Database::open(path).expect("reopen writer for mutation cost control");
    let noop = database.begin().expect("begin no-op transaction");
    database.commit(noop).expect("commit no-op transaction");
    database
        .execute(
            "INSERT INTO reader_work (id, payload, embedding) VALUES ($id, $payload, $embedding)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(row_id(100_001))),
                ("payload".to_owned(), Value::Text(payload(100_001))),
                ("embedding".to_owned(), Value::Vector(vector(100_001))),
            ]),
        )
        .expect("commit small append");
    database.close().expect("close append writer");
    let replay = Database::open(path).expect("reopen committed append");
    replay.close().expect("close replay opener");
    let counts = commit_scope.counters();
    drop(commit_scope);
    counts
}

#[test]
fn lazy_typed_image_digest_does_not_rehydrate_rows_or_vectors() {
    let root = tempfile::tempdir().expect("digest-control fixture directory");
    let path = root.path().join("digest-control.db");
    seed(&path, SMALL_ROWS);
    assert_digest_positive_control(&path);
}

#[test]
fn small_mutation_noop_and_reopen_do_not_run_file_reader_decode_loops() {
    let root = tempfile::tempdir().expect("commit-control fixture directory");
    let path = root.path().join("commit-control.db");
    seed(&path, SMALL_ROWS);
    let commit = mutation_cost_control(&path);
    println!("read-work actual commit counters: {commit:?}");
    assert_eq!(
        ReadWorkCounters {
            hashed_vectors: 0,
            hashed_bytes: 0,
            proof_nodes_read: 0,
            proof_nodes_hashed: 0,
            ordered_proof_nodes_read: 0,
            ..commit
        },
        ReadWorkCounters::default(),
        "no-op, one-row append, and durable reopen must not decode reader rows or vectors: {commit:?}"
    );
    assert!(
        commit.hashed_vectors <= SMALL_ROWS as u64,
        "writer reopen verifies at most the seeded vectors: {commit:?}"
    );
    // Every proof node read from the store is authenticated. The writer also
    // authenticates the chunks it builds before it writes them, so hashed
    // nodes exceed read nodes by exactly that self-check.
    assert!(
        commit.proof_nodes_hashed >= commit.proof_nodes_read,
        "every proof node read is authenticated: {commit:?}"
    );
    assert!(
        commit.ordered_proof_nodes_read <= 16,
        "writable recovery proves only the bounded history frontier: {commit:?}"
    );
}

#[test]
fn legacy_decode_counters_remain_a_read_open_diagnostic() {
    let root = tempfile::tempdir().expect("read-work fixture directory");
    let small = root.path().join("working-set-small.db");
    let large = root.path().join("working-set-large.db");
    seed(&small, SMALL_ROWS);
    seed(&large, LARGE_ROWS);

    let small_measurement = measure(&small, SMALL_ROWS);
    let large_measurement = measure(&large, LARGE_ROWS);
    assert_eq!(small_measurement.rows, SMALL_ROWS);
    assert_eq!(large_measurement.rows, LARGE_ROWS);
    println!("read-work actual counters: small={small_measurement:?}; large={large_measurement:?}");

    // Open authenticates the fixed history root controls and the ordered
    // startup digest; the history proof itself is read by the first request
    // that needs it. So open costs the same for 10 rows and 10,000, decodes no
    // row or vector body, and performs no full-image hash.
    let without_open_proof = |open: &ReadWorkCounters| ReadWorkCounters {
        hashed_bytes: 0,
        proof_nodes_read: 0,
        proof_nodes_hashed: 0,
        ordered_proof_nodes_read: 0,
        ..*open
    };
    for (label, measurement) in [("small", &small_measurement), ("large", &large_measurement)] {
        assert_eq!(measurement.open.proof_nodes_read, 0, "{label}");
        assert_eq!(measurement.open.proof_nodes_hashed, 0, "{label}");
        assert_eq!(measurement.open.ordered_proof_nodes_read, 0, "{label}");
        assert_eq!(
            without_open_proof(&measurement.open),
            ReadWorkCounters::default(),
            "open must load catalog/schema only and must not grow with unrelated stored \
             rows/vectors; observed {label}-file work is {measurement:?}"
        );
    }
    assert_eq!(
        small_measurement.open.hashed_bytes, large_measurement.open.hashed_bytes,
        "open authenticates the same fixed controls whatever the history depth"
    );
    // Deferring the proof keeps its refusal: damaged history still opens, and
    // the first request that depends on it is refused as typed damage, never
    // answered short.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    crate::history_answer_is_complete_and_in_author_order::assert_reported_as_damage(
        crate::history_answer_is_complete_and_in_author_order::damaged_history(
            contextdb_engine::persistence::read_persistence_test_scaffold::HistoryDamage::RemovedCommitIndexMember,
        ),
        "a missing commit-index member behind a bounded open",
    );
    assert!(
        large_measurement.query.decoded_rows > 0 || large_measurement.query.decoded_vectors > 0,
        "the bounded selected-key query must load its needed pages after open: {large_measurement:?}"
    );
    assert!(
        large_measurement.query.hashed_rows > 0,
        "the selected row must be authenticated by actual relational hashing: {large_measurement:?}"
    );
    assert!(
        large_measurement.query.hashed_vectors > 0,
        "the selected row's vector must be authenticated by actual vector hashing: {large_measurement:?}"
    );
    assert!(
        large_measurement.query.decoded_rows <= SMALL_ROWS as u64
            && large_measurement.query.decoded_vectors <= SMALL_ROWS as u64,
        "the selected-key query must not hydrate unrelated 10k rows/vectors: {large_measurement:?}"
    );
}

// Opening a store for one small selected query must not pay for history the
// query never asks about. Three stores share the same working set; the second
// adds unrelated history (committed rows on another table, their change log
// and commit index), the third adds the same history with every row also
// queued for delivery. Each store is opened in its own child process so its
// resident and cgroup peaks belong to that open alone.

const HISTORY_CHILD_ROLE: &str = "CONTEXTDB_OPEN_HISTORY_CHILD";
const HISTORY_CHILD_STORE: &str = "CONTEXTDB_OPEN_HISTORY_STORE";
const HISTORY_ROWS: usize = 1_000;
const HISTORY_ROWS_PER_COMMIT: usize = 2;
const HISTORY_BODY_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum History {
    Quiet,
    Committed,
    CommittedAndQueued,
}

fn seed_with_history(path: &Path, history: History) {
    seed(path, SMALL_ROWS);
    let database = Database::open(path).expect("reopen fixture writer for history");
    // Every store declares the same schema, event type, sink and route, so
    // their catalogs and schema-change logs match. Only the queued store's
    // event type watches the table the history lands in; no callback is
    // registered, so each of its events stays queued durably.
    let watched = if history == History::CommittedAndQueued {
        "ledger_a"
    } else {
        "ledger_b"
    };
    for statement in [
        "CREATE TABLE ledger_a (id UUID PRIMARY KEY, body TEXT NOT NULL)".to_owned(),
        "CREATE TABLE ledger_b (id UUID PRIMARY KEY, body TEXT NOT NULL)".to_owned(),
        format!("CREATE EVENT TYPE ledger_added WHEN INSERT ON {watched}"),
        "CREATE SINK ledger_sink TYPE callback".to_owned(),
        "CREATE ROUTE ledger_to_sink EVENT ledger_added TO ledger_sink".to_owned(),
    ] {
        database
            .execute(&statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    if history != History::Quiet {
        for start in (0..HISTORY_ROWS).step_by(HISTORY_ROWS_PER_COMMIT) {
            let tx = database.begin().expect("begin history commit");
            for row in start..start + HISTORY_ROWS_PER_COMMIT {
                database
                    .execute_in_tx(
                        tx,
                        "INSERT INTO ledger_a (id, body) VALUES ($id, $body)",
                        &HashMap::from([
                            (
                                "id".to_owned(),
                                Value::Uuid(uuid::Uuid::from_u128(0x1ed9_e700 + row as u128)),
                            ),
                            (
                                "body".to_owned(),
                                Value::Text(format!("{row:08x}").repeat(HISTORY_BODY_BYTES / 8)),
                            ),
                        ]),
                    )
                    .unwrap_or_else(|error| panic!("insert history row {row}: {error}"));
            }
            database
                .commit(tx)
                .unwrap_or_else(|error| panic!("commit history rows {start}: {error}"));
        }
    }
    database.close().expect("close history writer");
}

#[derive(Debug, Clone, Copy, Default)]
struct OpenFootprint {
    /// Every history entry open touched: decoded, plus commit-index entries
    /// visited without being decoded. Zero means open did no history work.
    history_records: u64,
    /// History entries open decoded into a value.
    history_decoded: u64,
    /// Commit-index entries open visited without decoding or keeping them.
    history_scanned: u64,
    history_bytes: u64,
    open_ms: u64,
    charged_bytes: u64,
    charged_peak_bytes: u64,
    resident_peak_growth_bytes: u64,
    cgroup_peak_growth_bytes: Option<u64>,
    /// Commit-index entries in the change history read on request after open.
    history_commits: u64,
    /// The reader's reported queue depth for the test sink.
    queued: u64,
}

fn proc_status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .expect("read process status")
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap_or_else(|| panic!("process status carries {field}"))
}

/// This process's cgroup peak, reset for this descriptor so later reads report
/// the peak since the reset (Linux 6.12 and later). `None` where the cgroup
/// does not allow it.
fn reset_cgroup_peak() -> Option<(std::fs::File, u64)> {
    use std::io::{Read as _, Seek as _, Write as _};
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = cgroup.lines().find_map(|line| line.strip_prefix("0::"))?;
    let base = Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'));
    let mut peak = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(base.join("memory.peak"))
        .ok()?;
    peak.write_all(b"reset\n").ok()?;
    let current = std::fs::read_to_string(base.join("memory.current"))
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let mut probe = String::new();
    peak.rewind().ok()?;
    peak.read_to_string(&mut probe).ok()?;
    Some((peak, current))
}

fn read_cgroup_peak(peak: &mut std::fs::File) -> Option<u64> {
    use std::io::{Read as _, Seek as _};
    let mut value = String::new();
    peak.rewind().ok()?;
    peak.read_to_string(&mut value).ok()?;
    value.trim().parse().ok()
}

fn sink_queued(
    reader: &contextdb_engine::direct_file_reader::test_seams::DirectFileReader,
    sink: &str,
) -> u64 {
    match reader
        .metadata(DirectMetadataRequest::EventsStatus)
        .expect("events status answers")
        .body
    {
        DirectMetadataBody::EventsStatus { status, .. } => status
            .sinks
            .iter()
            .find(|status| status.name == sink)
            .map(|status| status.queued)
            .unwrap_or_else(|| panic!("sink {sink} is declared")),
        other => panic!("events status answers as events status: {other:?}"),
    }
}

fn open_footprint_in_child(path: &Path) -> OpenFootprint {
    let filter = crate::child_test_filter(module_path!(), "open_history_footprint_child");
    let output = std::process::Command::new(std::env::current_exe().expect("current test binary"))
        .args(["--exact", &filter, "--nocapture", "--test-threads=1"])
        .env(HISTORY_CHILD_ROLE, "1")
        .env(HISTORY_CHILD_STORE, path)
        .output()
        .expect("run the open-footprint child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "open-footprint child failed: {}\n{stdout}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let line = stdout
        .lines()
        .find_map(|line| line.split_once("OPEN_FOOTPRINT ").map(|(_, rest)| rest))
        .unwrap_or_else(|| panic!("child reports its footprint: {stdout}"));
    let on_request = stdout
        .lines()
        .find_map(|line| {
            line.split_once("OPEN_HISTORY_ON_REQUEST ")
                .map(|(_, rest)| rest)
        })
        .unwrap_or_else(|| panic!("child reports its on-request history: {stdout}"));
    let requested = |name: &str| -> u64 {
        on_request
            .split_whitespace()
            .find_map(|pair| pair.strip_prefix(&format!("{name}=")))
            .and_then(|raw| raw.parse().ok())
            .unwrap_or_else(|| panic!("child reports {name}"))
    };
    let value = |name: &str| -> Option<u64> {
        line.split_whitespace()
            .find_map(|pair| pair.strip_prefix(&format!("{name}=")))
            .and_then(|raw| raw.parse().ok())
    };
    OpenFootprint {
        history_records: value("history_records").expect("history records"),
        history_decoded: value("history_decoded").expect("history decoded"),
        history_scanned: value("history_scanned").expect("history scanned"),
        history_bytes: value("history_bytes").expect("history bytes"),
        open_ms: value("open_ms").expect("open time"),
        charged_bytes: value("charged_bytes").expect("charged bytes"),
        charged_peak_bytes: value("charged_peak_bytes").expect("charged peak"),
        resident_peak_growth_bytes: value("resident_peak_growth_bytes")
            .expect("resident peak growth"),
        cgroup_peak_growth_bytes: value("cgroup_peak_growth_bytes"),
        history_commits: requested("history_commits"),
        queued: requested("queued"),
    }
}

#[test]
fn open_history_footprint_child() {
    use contextdb_engine::direct_file_reader::test_seams::{
        charged_high_water_for_test, image_memory_for_test,
    };
    use contextdb_engine::persistence::read_persistence_test_scaffold::observe_open_history_work_for_test;

    if std::env::var_os(HISTORY_CHILD_ROLE).is_none() {
        return;
    }
    let path =
        std::path::PathBuf::from(std::env::var_os(HISTORY_CHILD_STORE).expect("child store path"));
    // Start the resident peak from here: writing 5 resets VmHWM.
    std::fs::write("/proc/self/clear_refs", "5").expect("reset the resident peak");
    let resident_before = proc_status_kib("VmHWM:") * 1024;
    let mut cgroup = reset_cgroup_peak();
    let started = std::time::Instant::now();
    let (reader, history) = observe_open_history_work_for_test(|| {
        open_for_test(&path, reader_config())
            .unwrap_or_else(|error| panic!("open file reader {}: {error}", path.display()))
    });
    let open_ms = started.elapsed().as_millis() as u64;
    let resident_after = proc_status_kib("VmHWM:") * 1024;
    let cgroup_growth = cgroup.as_mut().and_then(|(peak, baseline)| {
        read_cgroup_peak(peak).map(|peak| peak.saturating_sub(*baseline))
    });
    let (usage, _, _) = image_memory_for_test(&reader);
    let charged_peak = charged_high_water_for_test(&reader);
    let answer = reader
        .execute(
            "SELECT payload FROM reader_work WHERE id = $id",
            &HashMap::from([("id".to_owned(), Value::Uuid(row_id(0)))]),
        )
        .expect("the selected working-set key still answers");
    assert_eq!(answer.result.rows.len(), 1);
    // History left in the store at open is still complete when asked for.
    // The whole history is one answer, larger than the working-set reader's
    // result ceiling, so it is asked of a reader declared for it -- after
    // the measured open is over.
    let mut history_config = reader_config();
    history_config.limits.result_bytes = 256 * 1024 * 1024;
    history_config.limits.memory = 1024 * 1024 * 1024;
    history_config.memory_limit = None;
    let history_reader =
        open_for_test(&path, history_config).expect("open a reader declared for history");
    let history_commits = match history_reader
        .metadata(DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::ChangeLog,
        })
        .expect("the change history answers on request")
        .body
    {
        DirectMetadataBody::ImageState {
            state: DirectImageState::ChangeLog(changes),
        } => changes.commit_index.len(),
        other => panic!("change history answers as change-log state: {other:?}"),
    };
    let queued = sink_queued(&reader, "ledger_sink");
    println!("OPEN_HISTORY_ON_REQUEST history_commits={history_commits} queued={queued}");
    let decoded = history.change_log_records
        + history.ddl_log_records
        + history.commit_index_records
        + history.audit_records
        + history.queued_records;
    println!(
        "OPEN_FOOTPRINT history_records={} history_decoded={decoded} history_scanned={} history_bytes={} open_ms={open_ms} charged_bytes={} charged_peak_bytes={charged_peak} resident_peak_growth_bytes={}{}",
        decoded + history.commit_index_scanned,
        history.commit_index_scanned,
        history.bytes,
        usage.used,
        resident_after.saturating_sub(resident_before),
        cgroup_growth
            .map(|growth| format!(" cgroup_peak_growth_bytes={growth}"))
            .unwrap_or_default(),
    );
    println!("OPEN_HISTORY_DETAIL {history:?}");
}

/// Resident growth an open may add for unrelated queued payloads an order of
/// magnitude larger: allocator and page-table noise, not a copy of what it
/// skipped.
const UNRELATED_RESIDENT_SLACK_BYTES: u64 = 2 * 1024 * 1024;

fn footprints(histories: [History; 2]) -> [OpenFootprint; 2] {
    let root = tempfile::tempdir().expect("history fixture directory");
    histories.map(|history| {
        let path = root.path().join(format!("{history:?}.db"));
        seed_with_history(&path, history);
        let footprint = open_footprint_in_child(&path);
        let store_bytes = std::fs::metadata(&path).expect("stat history store").len();
        println!(
            "open footprint {history:?}: store_bytes={store_bytes} history_records={} history_decoded={} history_scanned={} history_bytes={} open_ms={} charged_bytes={} charged_peak_bytes={} resident_peak_growth_bytes={} cgroup_peak_growth_bytes={:?} history_commits={} queued={}",
            footprint.history_records,
            footprint.history_decoded,
            footprint.history_scanned,
            footprint.history_bytes,
            footprint.open_ms,
            footprint.charged_bytes,
            footprint.charged_peak_bytes,
            footprint.resident_peak_growth_bytes,
            footprint.cgroup_peak_growth_bytes,
            footprint.history_commits,
            footprint.queued,
        );
        footprint
    })
}

#[test]
fn opening_does_not_copy_unrelated_queued_payloads() {
    let [committed, queued] = footprints([History::Committed, History::CommittedAndQueued]);
    assert_eq!(
        (queued.history_records, queued.history_bytes),
        (committed.history_records, committed.history_bytes),
        "opening must not copy queued payloads nobody asked for: committed={committed:?} queued={queued:?}"
    );
    assert!(
        queued.resident_peak_growth_bytes
            <= committed
                .resident_peak_growth_bytes
                .saturating_add(UNRELATED_RESIDENT_SLACK_BYTES),
        "queued payloads must not grow the open's resident peak: committed={committed:?} queued={queued:?}"
    );
    assert_eq!(
        (committed.queued, queued.queued),
        (0, HISTORY_ROWS as u64),
        "the reader still reports every queued entry it left in the store"
    );
}

#[test]
fn opening_does_not_decode_unrelated_committed_history() {
    let [quiet, committed] = footprints([History::Quiet, History::Committed]);
    assert_eq!(
        (quiet.history_records, committed.history_records),
        (0, 0),
        "open touches no history entry at all -- none decoded, none visited -- however much \
         committed history the store holds: quiet={quiet:?} committed={committed:?}"
    );
    assert_eq!(
        (committed.history_records, committed.history_bytes),
        (quiet.history_records, quiet.history_bytes),
        "opening must not decode unrelated committed history: quiet={quiet:?} committed={committed:?}"
    );
    assert!(
        committed.resident_peak_growth_bytes
            <= quiet
                .resident_peak_growth_bytes
                .saturating_add(UNRELATED_RESIDENT_SLACK_BYTES),
        "unrelated committed history must not grow the open's resident peak: quiet={quiet:?} committed={committed:?}"
    );
    assert_eq!(
        committed.history_commits,
        quiet.history_commits + (HISTORY_ROWS / HISTORY_ROWS_PER_COMMIT) as u64,
        "history left in the store at open is complete when asked for: quiet={quiet:?} committed={committed:?}"
    );
}

/// A reader reports the queue depth of the snapshot it opened, not of the
/// store as a writer later changes it.
#[test]
fn a_reader_reports_the_queued_count_of_its_own_snapshot() {
    let root = tempfile::tempdir().expect("queued snapshot fixture directory");
    let path = root.path().join("queued-snapshot.db");
    seed(&path, SMALL_ROWS);
    let insert = |database: &Database, rows: std::ops::Range<usize>| {
        for row in rows {
            database
                .execute(
                    "INSERT INTO ledger_a (id, body) VALUES ($id, $body)",
                    &HashMap::from([
                        (
                            "id".to_owned(),
                            Value::Uuid(uuid::Uuid::from_u128(0x5e_0000 + row as u128)),
                        ),
                        ("body".to_owned(), Value::Text(format!("queued-{row}"))),
                    ]),
                )
                .unwrap_or_else(|error| panic!("queue row {row}: {error}"));
        }
    };
    let database = Database::open(&path).expect("writer declares the queue");
    for statement in [
        "CREATE TABLE ledger_a (id UUID PRIMARY KEY, body TEXT NOT NULL)",
        "CREATE EVENT TYPE ledger_added WHEN INSERT ON ledger_a",
        "CREATE SINK ledger_sink TYPE callback",
        "CREATE ROUTE ledger_to_sink EVENT ledger_added TO ledger_sink",
    ] {
        database
            .execute(statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    insert(&database, 0..10);
    database.close().expect("close first writer");

    let reader = open_for_test(&path, reader_config()).expect("open the snapshot reader");
    assert_eq!(sink_queued(&reader, "ledger_sink"), 10);

    let database = Database::open(&path).expect("reopen the writer while the reader is open");
    insert(&database, 10..15);
    database.close().expect("close second writer");

    assert_eq!(
        sink_queued(&reader, "ledger_sink"),
        10,
        "the open reader keeps its snapshot's queue depth"
    );
    let later = open_for_test(&path, reader_config()).expect("open a later reader");
    assert_eq!(
        sink_queued(&later, "ledger_sink"),
        15,
        "a later reader sees the writer's queued entries"
    );
}
