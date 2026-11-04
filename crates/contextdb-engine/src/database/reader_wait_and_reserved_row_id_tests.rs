//! Refusal contracts use real public operations; no producer is replaced.
use super::*;
use crate::read_session::{DatabaseOpenOptions, ReaderWaitDeadline};
use contextdb_core::read_contract::{
    HeldByReadersDetail, OwnerReadCancellation, ReadFailureDetail, ReadFailureKind,
};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;

const BOUND: Duration = Duration::from_secs(10);

struct Task<T> {
    result: Receiver<T>,
    handle: JoinHandle<()>,
}
fn task<T: Send + 'static>(operation: impl FnOnce() -> T + Send + 'static) -> Task<T> {
    let (send, result) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _ = send.send(operation());
    });
    Task { result, handle }
}
impl<T> Task<T> {
    fn finish(self, what: &str) -> T {
        let value = self
            .result
            .recv_timeout(BOUND)
            .unwrap_or_else(|error| panic!("{what} blocked or failed to finish: {error}"));
        self.join_completed();
        value
    }
    fn join_completed(self) {
        let (send, joined) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = send.send(self.handle.join());
        });
        joined
            .recv_timeout(BOUND)
            .expect("operation thread blocked during exit")
            .expect("operation thread panicked");
    }
}
struct PassedDeadlineClock;
impl contextdb_core::read_contract::DeadlineClock for PassedDeadlineClock {
    fn now_ms(&self) -> u64 {
        1
    }
    fn wait_until(&self, _: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(async {})
    }
}
fn options() -> DatabaseOpenOptions {
    let mut options = DatabaseOpenOptions::default();
    options.owner_reads.enabled = false;
    options.maintenance_policy = MaintenancePolicy::CallerDriven;
    options
}
fn held_by_readers(result: Result<()>, what: &str) -> HeldByReadersDetail {
    let error = result.expect_err(what);
    let Error::ReadFailure(failure) = error.engine_diagnostic() else {
        panic!("{what}: expected typed HeldByReaders, got {error:?}");
    };
    assert_eq!(failure.kind(), ReadFailureKind::HeldByReaders, "{what}");
    let ReadFailureDetail::HeldByReaders(detail) = failure.detail() else {
        panic!("{what}: missing holder detail");
    };
    detail.validate().unwrap();
    assert!(
        detail.observed_direct_readers > 0,
        "{what}: reader count was omitted"
    );
    assert!(
        detail
            .verified_readers
            .iter()
            .any(|reader| reader.process_id == std::process::id()),
        "{what}: the actual reader's identity was omitted: {detail:?}"
    );
    detail.clone()
}

#[test]
fn cdb_239_cancelled_writer_wait_keeps_typed_reader_holder_detail() {
    use crate::direct_file_reader::{DirectFileReader, DirectReaderConfig};
    use crate::persistence::read_persistence_test_scaffold::{
        ReaderPausePoint, WriterAdmissionEvent, pause_readers_at_for_test,
        watch_writer_admission_for_test,
    };
    use contextdb_core::read_contract::ReadLimits;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cancelled-reader-wait.db");
    let seed = Database::open_with_options(&path, options()).unwrap();
    seed.execute(
        "CREATE TABLE small (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    seed.execute("INSERT INTO small VALUES (7)", &HashMap::new())
        .unwrap();
    seed.close().unwrap();
    drop(seed);
    let pause = pause_readers_at_for_test(&path, ReaderPausePoint::SourceStart);
    let reader_path = path.clone();
    let reader = task(move || {
        let config =
            DirectReaderConfig::new(ReadLimits::default(), crate::read_session::read_clock());
        let reader = DirectFileReader::open(reader_path, config).unwrap();
        assert_eq!(
            reader
                .execute("SELECT id FROM small", &HashMap::new())
                .unwrap()
                .result
                .rows(),
            &[vec![Value::Int64(7)]]
        );
    });
    assert!(
        pause.wait_for_paused_reader(BOUND).is_some(),
        "reader never reached SourceStart before exact registration"
    );
    let probe_path = path.clone();
    let deadline = task(move || {
        let mut configuration = options();
        configuration.reader_wait_deadline = Some(ReaderWaitDeadline {
            clock: Arc::new(PassedDeadlineClock),
            deadline_ms: 0,
        });
        Database::open_with_options(&probe_path, configuration).map(|db| db.close().unwrap())
    })
    .finish("expired-deadline reader-holder control");
    let expected = held_by_readers(deadline, "expired-deadline control");
    let watch = watch_writer_admission_for_test(&path);
    let stop = OwnerReadCancellation::new();
    let writer_stop = stop.clone();
    let writer_path = path.clone();
    let writer = task(move || {
        let mut configuration = options();
        configuration.reader_wait_stop = Some(writer_stop);
        Database::open_with_options(&writer_path, configuration).map(|db| db.close().unwrap())
    });
    let waiting = watch.next(BOUND);
    // Cancellation follows a real admission acknowledgment, with the SAME
    // reader still paused; it cannot accidentally test a pre-cancelled open.
    task(move || stop.cancel()).finish("caller cancellation notification");
    let cancelled = writer.result.recv_timeout(BOUND);
    let answered_before_release = cancelled.is_ok();
    drop(pause);
    reader.finish("reader after SourceStart release");
    let cancelled = match cancelled {
        Ok(value) => {
            writer.join_completed();
            value
        }
        Err(_) => writer.finish("cancelled writer after reader release"),
    };
    let Some(WriterAdmissionEvent::Waiting { store, readers }) = waiting else {
        panic!("writer did not acknowledge a real reader wait: {waiting:?}");
    };
    assert_eq!(store, path);
    assert_eq!(readers, expected.verified_readers);
    assert!(
        answered_before_release,
        "caller cancellation did not finish the writer while its reader was still paused"
    );
    eprintln!(
        "CDB239_CANCEL result={:?} deadline_detail={expected:?}",
        cancelled
            .as_ref()
            .map_err(|error| error.engine_diagnostic())
    );
    let actual = held_by_readers(cancelled, "cancelled in-progress writer wait");
    assert_eq!(
        actual, expected,
        "cancellation must preserve the same reader evidence as deadline expiry"
    );
}

fn memory_database() -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}
fn reserved_column(result: Result<QueryResult>, table: &str) {
    let error = result.expect_err("DDL accepted a reserved row_id column instead of refusing it");
    assert!(
        matches!(error.engine_diagnostic(), Error::ReservedColumnName { table: found, column } if found == table && column == "row_id"),
        "expected typed ReservedColumnName for {table}.row_id, got {error:?}"
    );
}
#[test]
fn cdb_240_create_refuses_reserved_row_id_without_creating_the_table() {
    let db = memory_database();
    db.execute(
        "CREATE TABLE allowed_create (id INTEGER PRIMARY KEY, graph_row_id INTEGER)",
        &HashMap::new(),
    )
    .unwrap();
    let before = db.current_lsn();
    let result = db.execute(
        "CREATE TABLE reserved_create (id INTEGER PRIMARY KEY, row_id INTEGER)",
        &HashMap::new(),
    );
    eprintln!("CDB240_CREATE result={result:?}");
    reserved_column(result, "reserved_create");
    assert!(
        db.table_meta("reserved_create").is_none(),
        "refused CREATE installed a schema"
    );
    assert_eq!(
        db.current_lsn(),
        before,
        "refused CREATE advanced the published LSN"
    );
}
#[test]
fn cdb_240_alter_refuses_reserved_row_id_without_changing_the_table() {
    let db = memory_database();
    db.execute(
        "CREATE TABLE reserved_alter (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "ALTER TABLE reserved_alter ADD COLUMN graph_row_id INTEGER",
        &HashMap::new(),
    )
    .unwrap();
    let before = db.current_lsn();
    let schema = db.table_meta("reserved_alter");
    let result = db.execute(
        "ALTER TABLE reserved_alter ADD COLUMN row_id INTEGER",
        &HashMap::new(),
    );
    eprintln!("CDB240_ALTER result={result:?}");
    reserved_column(result, "reserved_alter");
    assert_eq!(
        db.table_meta("reserved_alter"),
        schema,
        "refused ALTER changed the schema"
    );
    assert_eq!(
        db.current_lsn(),
        before,
        "refused ALTER advanced the published LSN"
    );
}
#[test]
fn cdb_228_select_row_id_returns_the_actual_engine_id() {
    let db = memory_database();
    db.execute(
        "CREATE TABLE ordinary (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin().unwrap();
    let actual = db
        .insert_row(
            tx,
            "ordinary",
            HashMap::from([("id".into(), Value::Int64(9001))]),
        )
        .unwrap();
    db.commit(tx).unwrap();
    let expected = i64::try_from(actual.0).unwrap();
    assert_ne!(
        expected, 9001,
        "natural key must differ from the engine's actual allocated ID"
    );
    let bare = db.execute("SELECT row_id FROM ordinary", &HashMap::new());
    let mixed = db.execute("SELECT row_id, id FROM ordinary", &HashMap::new());
    eprintln!(
        "CDB228_SELECT engine_row_id={expected} bare={:?} mixed={:?}",
        bare.as_ref().map(|r| (r.columns(), r.rows())),
        mixed.as_ref().map(|r| (r.columns(), r.rows()))
    );
    let bare = bare.expect("SELECT row_id must answer");
    let mixed = mixed.expect("mixed SELECT row_id must answer");
    assert_eq!(
        bare.columns(),
        &["row_id"],
        "SELECT dropped the engine identifier"
    );
    assert_eq!(bare.rows(), &[vec![Value::Int64(expected)]]);
    assert_eq!(
        mixed.columns(),
        &["row_id", "id"],
        "mixed SELECT dropped the engine identifier"
    );
    assert_eq!(
        mixed.rows(),
        &[vec![Value::Int64(expected), Value::Int64(9001)]]
    );
}
