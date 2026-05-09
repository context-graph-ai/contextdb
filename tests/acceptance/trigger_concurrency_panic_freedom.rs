//! Trigger-active tx-control symmetry — typed-variant escape across all
//! tx-control surfaces under all four distinct callback-active conditions.

use contextdb_core::{CallbackKind, Error, Lsn, Result, RowId, TxId, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::plugin::DatabasePlugin;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, EdgeChange, VectorChange,
};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

// ---------- Shared helpers ----------

type ApplyShapeResult = (&'static str, Result<contextdb_engine::ApplyResult>);
type ClosedSurfaceResult = (&'static str, Result<()>);
type ForbiddenStatementResult = (&'static str, Result<contextdb_engine::QueryResult>);

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn forbidden_statement_samples() -> &'static [(&'static str, &'static str)] {
    &[
        ("begin", "BEGIN"),
        ("commit", "COMMIT"),
        ("rollback", "ROLLBACK"),
        (
            "create_table",
            "CREATE TABLE blocked_table (id UUID PRIMARY KEY)",
        ),
        (
            "alter_table",
            "ALTER TABLE helper_base ADD COLUMN extra TEXT",
        ),
        ("drop_table", "DROP TABLE helper_base"),
        (
            "create_index",
            "CREATE INDEX blocked_idx ON helper_base(id)",
        ),
        ("drop_index", "DROP INDEX existing_idx ON helper_base"),
        (
            "create_schedule",
            "CREATE SCHEDULE blocked_schedule EVERY '1 SECOND' TX (cb)",
        ),
        ("drop_schedule", "DROP SCHEDULE existing_schedule"),
        (
            "create_trigger",
            "CREATE TRIGGER blocked_trigger ON helper_base WHEN INSERT",
        ),
        ("drop_trigger", "DROP TRIGGER existing_trigger"),
        (
            "create_event_type",
            "CREATE EVENT TYPE blocked_event WHEN INSERT ON helper_base",
        ),
        ("create_sink", "CREATE SINK blocked_sink TYPE callback"),
        (
            "create_route",
            "CREATE ROUTE blocked_route EVENT existing_event TO existing_sink",
        ),
        ("drop_route", "DROP ROUTE existing_route"),
        ("set_memory_limit", "SET MEMORY_LIMIT '1G'"),
        ("set_disk_limit", "SET DISK_LIMIT '1G'"),
        (
            "set_sync_conflict_policy",
            "SET SYNC_CONFLICT_POLICY 'latest_wins'",
        ),
    ]
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

/// 30-second wall-clock timeout wrapper. Spawns the body in a thread and polls
/// the JoinHandle so assertion panics are rethrown as assertion panics, not
/// misreported as timeouts.
fn run_with_timeout<F, T>(body: F) -> std::result::Result<T, &'static str>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let handle = thread::spawn(body);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            return Err("test deadlocked (>30s)");
        }
        thread::sleep(Duration::from_millis(10));
    }
    match handle.join() {
        Ok(value) => Ok(value),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

/// Non-allocating `core::fmt::Write` adapter. Asserts no allocation by writing
/// into a fixed-size on-stack buffer; panics if the buffer overflows. Used by
/// t29 to verify the typed-Err Display impls are alloc-free.
struct NoAllocSink {
    buf: [u8; 256],
    len: usize,
}

impl NoAllocSink {
    fn new() -> Self {
        Self {
            buf: [0u8; 256],
            len: 0,
        }
    }
    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buf[..self.len]).expect("utf8")
    }
    fn reset(&mut self) {
        self.len = 0;
    }
}

impl std::fmt::Write for NoAllocSink {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let bytes = s.as_bytes();
        if self.len + bytes.len() > self.buf.len() {
            return Err(std::fmt::Error);
        }
        self.buf[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
        Ok(())
    }
}

/// Build a database with a trigger registered on table `t`. The callback parks
/// at `entered_cb.wait()` (signaling its body is active) and continues only
/// after `done_cb.wait()` (the racing thread has finished). Returns the
/// database, plus the tx-id of the firing INSERT once executed.
///
/// Caller drives the firing INSERT on a separate thread.
fn build_trigger_parked_db(entered: Arc<Barrier>, done: Arc<Barrier>) -> Arc<Database> {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute("CREATE TRIGGER probe_trigger ON t WHEN INSERT", &empty())
        .unwrap();
    let entered_cb = entered.clone();
    let done_cb = done.clone();
    db.register_trigger_callback("probe_trigger", move |_db, _ctx| {
        entered_cb.wait();
        done_cb.wait();
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();
    db
}

/// Build a database with a cron schedule whose callback parks identically.
/// The background tickler is paused; tests drive single dispatches via
/// `dispatch_cron_now_in_thread` so timing is deterministic.
fn build_cron_parked_db(
    entered: Arc<Barrier>,
    done: Arc<Barrier>,
) -> (Arc<Database>, contextdb_engine::CronPauseGuard) {
    let db = Arc::new(Database::open_memory());
    let pause = db.pause_cron_tickler_for_test();
    db.execute("CREATE TABLE c (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE SCHEDULE probe_cron EVERY '1 MILLISECONDS' TX (probe_cron_cb)",
        &empty(),
    )
    .unwrap();
    let entered_cb = entered.clone();
    let done_cb = done.clone();
    db.register_cron_callback("probe_cron_cb", move |_db_handle| {
        entered_cb.wait();
        done_cb.wait();
        Ok(())
    })
    .unwrap();
    (db, pause)
}

/// Spawn a thread that dispatches a single cron tick. The 5ms sleep lets
/// 1ms-interval schedules become due.
fn dispatch_cron_now_in_thread(db: Arc<Database>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        let _ = db.cron_run_due_now_for_test();
    })
}

/// Drive a single cron tick. Sleeps 5ms first so 1ms-interval schedules become due.
fn fire_cron_now(db: &Database) {
    thread::sleep(Duration::from_millis(5));
    let _ = db.cron_run_due_now_for_test();
}

fn fire_trigger_in_thread(db: Arc<Database>, key: u128) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        let tx = db.begin()?;
        db.execute_in_tx(
            tx,
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(key)))]),
        )?;
        db.commit(tx)
    })
}

/// Build a single-row `ChangeSet` against table `table` with id = `uuid(key)`.
/// Used by t13–t16 / t47 to verify that `apply_changes` trips its callback-active
/// gate BEFORE consuming the changeset (post-Err `db.scan(table, snap)` is empty).
fn populated_changeset(table: &str, key: u128) -> ChangeSet {
    use contextdb_engine::sync_types::{NaturalKey, RowChange};
    let mut values = HashMap::new();
    values.insert("id".to_string(), Value::Uuid(uuid(key)));
    ChangeSet {
        rows: vec![RowChange {
            table: table.to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(uuid(key)),
            },
            values,
            deleted: false,
            lsn: Lsn(1),
        }],
        edges: vec![],
        vectors: vec![],
        ddl: vec![],
        ddl_lsn: vec![],
    }
}

/// Build an N-row `ChangeSet` against table `table` with ids `uuid(key_base + i)`
/// for i in 0..n. Used by t47 for the 100-row fail-early-and-rebuild probe.
fn populated_changeset_n(table: &str, key_base: u128, n: usize) -> ChangeSet {
    use contextdb_engine::sync_types::{NaturalKey, RowChange};
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        let key = key_base + i as u128;
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Uuid(uuid(key)));
        rows.push(RowChange {
            table: table.to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(uuid(key)),
            },
            values,
            deleted: false,
            lsn: Lsn(1 + i as u64),
        });
    }
    ChangeSet {
        rows,
        edges: vec![],
        vectors: vec![],
        ddl: vec![],
        ddl_lsn: vec![],
    }
}

fn edge_only_changeset(lsn: u64) -> ChangeSet {
    ChangeSet {
        rows: vec![],
        edges: vec![EdgeChange {
            source: uuid(0xE0_0001),
            target: uuid(0xE0_0002),
            edge_type: "related".to_string(),
            properties: HashMap::new(),
            lsn: Lsn(lsn),
        }],
        vectors: vec![],
        ddl: vec![],
        ddl_lsn: vec![],
    }
}

fn vector_only_changeset(lsn: u64, row_id: RowId) -> ChangeSet {
    ChangeSet {
        rows: vec![],
        edges: vec![],
        vectors: vec![VectorChange {
            index: VectorIndexRef::new("vectors", "embedding"),
            row_id,
            vector: vec![0.1, 0.2, 0.3],
            lsn: Lsn(lsn),
        }],
        ddl: vec![],
        ddl_lsn: vec![],
    }
}

fn ddl_only_changeset(lsn: u64) -> ChangeSet {
    ChangeSet {
        rows: vec![],
        edges: vec![],
        vectors: vec![],
        ddl: vec![DdlChange::CreateTable {
            name: format!("ddl_shape_{lsn}"),
            columns: vec![("id".to_string(), "UUID PRIMARY KEY".to_string())],
            constraints: vec![],
            foreign_keys: vec![],
            composite_foreign_keys: vec![],
            composite_unique: vec![],
        }],
        ddl_lsn: vec![Lsn(lsn)],
    }
}

fn invalid_ddl_lsn_changeset(lsn: u64) -> ChangeSet {
    let mut changes = ddl_only_changeset(lsn);
    changes.ddl_lsn.clear();
    changes
}

fn row_id_for_uuid(db: &Database, table: &str, key: u128) -> RowId {
    let expected = Value::Uuid(uuid(key));
    db.scan(table, db.snapshot())
        .unwrap_or_else(|err| panic!("scan {table} for row_id failed: {err:?}"))
        .into_iter()
        .find(|row| row.values.get("id") == Some(&expected))
        .unwrap_or_else(|| panic!("row {key:x} not found in {table}"))
        .row_id
}

fn table_contains_uuid(db: &Database, table: &str, key: u128) -> bool {
    let expected = Value::Uuid(uuid(key));
    db.scan(table, db.snapshot())
        .unwrap_or_else(|err| panic!("scan {table} failed: {err:?}"))
        .iter()
        .any(|row| row.values.get("id") == Some(&expected))
}

fn prepare_vector_shape_target(db: &Database, key: u128) -> RowId {
    db.execute(
        "CREATE TABLE vectors (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO vectors (id) VALUES ($id)",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(key)))]),
    )
    .unwrap();
    row_id_for_uuid(db, "vectors", key)
}

fn assert_non_row_apply_shapes_left_no_side_effects(
    db: &Database,
    vector_row_id: RowId,
    ddl_lsns: &[u64],
    context: &str,
) {
    let snap = db.snapshot();
    assert_eq!(
        db.edge_count(uuid(0xE0_0001), "related", snap)
            .unwrap_or_else(|err| panic!("{context}: edge_count failed: {err:?}")),
        0,
        "{context}: edge-only ChangeSet must not stage or commit an edge before returning typed Err"
    );

    let vector_hits = db
        .query_vector(
            VectorIndexRef::new("vectors", "embedding"),
            &[0.1, 0.2, 0.3],
            10,
            None,
            snap,
        )
        .unwrap_or_else(|err| panic!("{context}: vector query failed: {err:?}"));
    assert!(
        vector_hits
            .iter()
            .all(|(row_id, _score)| *row_id != vector_row_id),
        "{context}: vector-only ChangeSet must not stage or commit vector row {vector_row_id:?} before returning typed Err; hits={vector_hits:?}"
    );

    for ddl_lsn in ddl_lsns {
        let ddl_table = format!("ddl_shape_{ddl_lsn}");
        let scan = db.scan(&ddl_table, snap);
        assert!(
            matches!(scan, Err(Error::TableNotFound(ref table)) if table == &ddl_table),
            "{context}: DDL-only ChangeSet must not create {ddl_table} before returning typed Err; got {scan:?}"
        );
    }
}

fn stage_commit_probe_row(db: &Database, table: &str, key: u128) -> TxId {
    db.execute(
        &format!("CREATE TABLE {table} (id UUID PRIMARY KEY)"),
        &empty(),
    )
    .unwrap();
    let tx = db.begin().expect("setup begin");
    db.execute_in_tx(
        tx,
        &format!("INSERT INTO {table} (id) VALUES ($id)"),
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(key)))]),
    )
    .unwrap();
    tx
}

fn assert_commit_probe_survives_typed_err(db: &Database, table: &str, tx: TxId, key: u128) {
    let snap = db.snapshot();
    assert_eq!(
        db.scan(table, snap).unwrap().len(),
        0,
        "commit() must return the callback-active typed Err before committing staged data"
    );
    db.commit(tx)
        .expect("staged tx must remain commit-able after callback-active commit Err");
    let snap = db.snapshot();
    let rows = db.scan(table, snap).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "commit() must not discard or rollback the staged tx before returning the typed Err"
    );
    assert_eq!(
        rows[0].values.get("id"),
        Some(&Value::Uuid(uuid(key))),
        "commit() typed Err must preserve the exact staged row, not only row cardinality"
    );
}

fn assert_rollback_probe_survives_typed_err(db: &Database, table: &str, tx: TxId, key: u128) {
    let snap = db.snapshot();
    assert_eq!(
        db.scan(table, snap).unwrap().len(),
        0,
        "rollback() must return the callback-active typed Err before committing or discarding staged data"
    );
    db.commit(tx)
        .expect("staged tx must remain commit-able after callback-active rollback Err");
    let snap = db.snapshot();
    let rows = db.scan(table, snap).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "rollback() must not discard or mutate the staged tx before returning the typed Err"
    );
    assert_eq!(
        rows[0].values.get("id"),
        Some(&Value::Uuid(uuid(key))),
        "rollback() typed Err must preserve the exact staged row, not only row cardinality"
    );
}

fn assert_handle_still_open(db: &Database, context: &str) {
    let tx = db.begin().unwrap_or_else(|err| {
        panic!("{context}: handle should remain open after typed Err: {err:?}")
    });
    db.rollback(tx)
        .unwrap_or_else(|err| panic!("{context}: rollback after open probe failed: {err:?}"));
}

fn prepare_forbidden_statement_side_effect_probe(db: &Database) {
    db.execute("CREATE TABLE helper_base (id UUID PRIMARY KEY)", &empty())
        .expect("helper_base setup");
    db.execute(
        "CREATE TABLE autocommit_probe (id UUID PRIMARY KEY)",
        &empty(),
    )
    .expect("autocommit_probe setup");
    db.execute("CREATE INDEX existing_idx ON helper_base(id)", &empty())
        .expect("existing index setup");
    db.execute(
        "CREATE SCHEDULE existing_schedule EVERY '60 SECONDS' TX (cb)",
        &empty(),
    )
    .expect("existing schedule setup");
    db.execute(
        "CREATE TRIGGER existing_trigger ON helper_base WHEN INSERT",
        &empty(),
    )
    .expect("existing trigger setup");
    db.register_trigger_callback("existing_trigger", |_db, _ctx| Ok(()))
        .expect("existing trigger callback setup");
    db.execute(
        "CREATE EVENT TYPE existing_event WHEN INSERT ON helper_base",
        &empty(),
    )
    .expect("existing event setup");
    db.execute("CREATE SINK existing_sink TYPE callback", &empty())
        .expect("existing sink setup");
    db.execute(
        "CREATE ROUTE existing_route EVENT existing_event TO existing_sink",
        &empty(),
    )
    .expect("existing route setup");
    db.execute("SET SYNC_CONFLICT_POLICY 'server_wins'", &empty())
        .expect("baseline conflict policy setup");
}

fn assert_forbidden_statement_family_left_no_side_effects(
    db: &Database,
    since: Lsn,
    context: &str,
    autocommit_probe_key: u128,
) {
    let leaked_ddl = db.ddl_log_since(since);
    assert!(
        leaked_ddl.is_empty(),
        "{context}: forbidden helper SQL leaked DDL entries after typed Err: {leaked_ddl:?}"
    );
    assert!(
        db.table_meta("blocked_table").is_none(),
        "{context}: CREATE TABLE side effect leaked"
    );
    let helper_meta = db
        .table_meta("helper_base")
        .unwrap_or_else(|| panic!("{context}: DROP TABLE side effect leaked"));
    assert!(
        helper_meta
            .columns
            .iter()
            .all(|column| column.name != "extra"),
        "{context}: ALTER TABLE side effect leaked: {helper_meta:?}"
    );
    db.execute("DROP INDEX existing_idx ON helper_base", &empty())
        .unwrap_or_else(|err| panic!("{context}: existing index was dropped: {err}"));
    db.execute("CREATE INDEX blocked_idx ON helper_base(id)", &empty())
        .unwrap_or_else(|err| panic!("{context}: blocked index already existed: {err}"));
    db.execute("DROP SCHEDULE existing_schedule", &empty())
        .unwrap_or_else(|err| panic!("{context}: existing schedule was dropped: {err}"));
    db.execute(
        "CREATE SCHEDULE blocked_schedule EVERY '1 SECOND' TX (cb)",
        &empty(),
    )
    .unwrap_or_else(|err| panic!("{context}: blocked schedule already existed: {err}"));
    db.execute("DROP TRIGGER existing_trigger", &empty())
        .unwrap_or_else(|err| panic!("{context}: existing trigger was dropped: {err}"));
    db.execute(
        "CREATE TRIGGER blocked_trigger ON helper_base WHEN INSERT",
        &empty(),
    )
    .unwrap_or_else(|err| panic!("{context}: blocked trigger already existed: {err}"));
    db.execute("DROP ROUTE existing_route", &empty())
        .unwrap_or_else(|err| panic!("{context}: existing route was dropped: {err}"));
    db.execute(
        "CREATE EVENT TYPE blocked_event WHEN INSERT ON helper_base",
        &empty(),
    )
    .unwrap_or_else(|err| panic!("{context}: blocked event type already existed: {err}"));
    db.execute("CREATE SINK blocked_sink TYPE callback", &empty())
        .unwrap_or_else(|err| panic!("{context}: blocked sink already existed: {err}"));
    db.execute(
        "CREATE ROUTE blocked_route EVENT existing_event TO existing_sink",
        &empty(),
    )
    .unwrap_or_else(|err| panic!("{context}: blocked route already existed: {err}"));
    assert_eq!(
        db.accountant().usage().limit,
        None,
        "{context}: SET MEMORY_LIMIT side effect leaked"
    );
    assert_eq!(
        db.disk_limit(),
        None,
        "{context}: SET DISK_LIMIT side effect leaked"
    );
    assert_eq!(
        db.conflict_policies().default,
        ConflictPolicy::ServerWins,
        "{context}: SET SYNC_CONFLICT_POLICY side effect leaked"
    );

    db.execute(
        "INSERT INTO autocommit_probe (id) VALUES ($id)",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(autocommit_probe_key)))]),
    )
    .unwrap_or_else(|err| {
        panic!("{context}: normal autocommit insert failed after forbidden SQL: {err}")
    });
    assert!(
        table_contains_uuid(db, "autocommit_probe", autocommit_probe_key),
        "{context}: forbidden transaction-control SQL left a session transaction active"
    );
}

#[derive(Debug)]
struct DirectApiSideEffectProbe {
    delete_row_key: u128,
    delete_row_id: RowId,
    insert_row_key: u128,
    upsert_row_key: u128,
    insert_edge_source: Uuid,
    insert_edge_target: Uuid,
    delete_edge_source: Uuid,
    delete_edge_target: Uuid,
    insert_vector_row_id: RowId,
    delete_vector_row_id: RowId,
}

fn prepare_direct_api_side_effect_probe(db: &Database, key_base: u128) -> DirectApiSideEffectProbe {
    db.execute("CREATE TABLE w (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE v (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let delete_row_key = key_base + 1;
    let insert_row_key = key_base + 2;
    let upsert_row_key = key_base + 3;
    let insert_vector_key = key_base + 4;
    let delete_vector_key = key_base + 5;

    for (table, key) in [
        ("w", delete_row_key),
        ("v", insert_vector_key),
        ("v", delete_vector_key),
    ] {
        db.execute(
            &format!("INSERT INTO {table} (id) VALUES ($id)"),
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(key)))]),
        )
        .unwrap();
    }

    let delete_row_id = row_id_for_uuid(db, "w", delete_row_key);
    let insert_vector_row_id = row_id_for_uuid(db, "v", insert_vector_key);
    let delete_vector_row_id = row_id_for_uuid(db, "v", delete_vector_key);
    let delete_edge_source = uuid(key_base + 0xE01);
    let delete_edge_target = uuid(key_base + 0xE02);

    let seed_tx = db.begin().expect("direct side-effect seed begin");
    db.insert_edge(
        seed_tx,
        delete_edge_source,
        delete_edge_target,
        "direct".to_string(),
        HashMap::new(),
    )
    .expect("seed delete-edge target");
    db.insert_vector(
        seed_tx,
        VectorIndexRef::new("v", "embedding"),
        delete_vector_row_id,
        vec![0.9, 0.0, 0.0],
    )
    .expect("seed delete-vector target");
    db.commit(seed_tx).expect("seed direct side-effect data");

    DirectApiSideEffectProbe {
        delete_row_key,
        delete_row_id,
        insert_row_key,
        upsert_row_key,
        insert_edge_source: uuid(key_base + 0xE03),
        insert_edge_target: uuid(key_base + 0xE04),
        delete_edge_source,
        delete_edge_target,
        insert_vector_row_id,
        delete_vector_row_id,
    }
}

fn assert_direct_api_typed_err_left_no_side_effects(
    db: &Database,
    probe: &DirectApiSideEffectProbe,
    context: &str,
) {
    assert!(
        table_contains_uuid(db, "w", probe.delete_row_key),
        "{context}: delete_row typed Err must leave the pre-existing row visible"
    );
    assert!(
        !table_contains_uuid(db, "w", probe.insert_row_key),
        "{context}: insert_row typed Err must not stage a row that becomes commit-able later"
    );
    assert!(
        !table_contains_uuid(db, "w", probe.upsert_row_key),
        "{context}: upsert_row typed Err must not stage a row that becomes commit-able later"
    );

    let snap = db.snapshot();
    assert_eq!(
        db.edge_count(probe.insert_edge_source, "direct", snap)
            .unwrap_or_else(|err| panic!("{context}: inserted-edge count failed: {err:?}")),
        0,
        "{context}: insert_edge typed Err must not stage an edge that becomes commit-able later"
    );
    assert_eq!(
        db.edge_count(probe.delete_edge_source, "direct", snap)
            .unwrap_or_else(|err| panic!("{context}: seeded-edge count failed: {err:?}")),
        1,
        "{context}: delete_edge typed Err must not delete the pre-existing edge"
    );

    let inserted_vector_hits = db
        .query_vector(
            VectorIndexRef::new("v", "embedding"),
            &[0.1, 0.2, 0.3],
            10,
            None,
            snap,
        )
        .unwrap_or_else(|err| panic!("{context}: inserted-vector query failed: {err:?}"));
    assert!(
        inserted_vector_hits
            .iter()
            .all(|(row_id, _score)| *row_id != probe.insert_vector_row_id),
        "{context}: insert_vector typed Err must not stage a vector that becomes commit-able later; hits={inserted_vector_hits:?}"
    );

    let seeded_vector_hits = db
        .query_vector(
            VectorIndexRef::new("v", "embedding"),
            &[0.9, 0.0, 0.0],
            10,
            None,
            snap,
        )
        .unwrap_or_else(|err| panic!("{context}: seeded-vector query failed: {err:?}"));
    assert!(
        seeded_vector_hits
            .iter()
            .any(|(row_id, _score)| *row_id == probe.delete_vector_row_id),
        "{context}: delete_vector typed Err must leave the pre-existing vector searchable; hits={seeded_vector_hits:?}"
    );
}

#[derive(Clone, Copy, Debug)]
enum TxControlSurface {
    Begin,
    Commit,
    Rollback,
    ApplyChanges,
    Close,
}

impl TxControlSurface {
    const ALL: [Self; 5] = [
        Self::Begin,
        Self::Commit,
        Self::Rollback,
        Self::ApplyChanges,
        Self::Close,
    ];

    const OPEN_OPERATION_ONLY: [Self; 4] = [
        Self::Begin,
        Self::Commit,
        Self::Rollback,
        Self::ApplyChanges,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Begin => "begin",
            Self::Commit => "commit",
            Self::Rollback => "rollback",
            Self::ApplyChanges => "apply_changes",
            Self::Close => "close",
        }
    }
}

#[derive(Clone, Debug)]
struct PreparedTxSurface {
    surface: TxControlSurface,
    tx: Option<TxId>,
    table: Option<String>,
}

fn prepare_tx_surface(db: &Database, surface: TxControlSurface, key: u128) -> PreparedTxSurface {
    let tx = match surface {
        TxControlSurface::Commit | TxControlSurface::Rollback => Some(db.begin_or_panic()),
        _ => None,
    };
    let table = match surface {
        TxControlSurface::ApplyChanges => {
            let table = format!("priority_apply_{key:x}");
            db.execute(
                &format!("CREATE TABLE {table} (id UUID PRIMARY KEY)"),
                &empty(),
            )
            .unwrap();
            Some(table)
        }
        _ => None,
    };
    PreparedTxSurface { surface, tx, table }
}

fn run_tx_surface(db: &Database, probe: &PreparedTxSurface, key: u128) -> Result<()> {
    match probe.surface {
        TxControlSurface::Begin => match db.begin() {
            Ok(tx) => {
                let _ = db.rollback(tx);
                Ok(())
            }
            Err(err) => Err(err),
        },
        TxControlSurface::Commit => db.commit(probe.tx.expect("commit probe tx")),
        TxControlSurface::Rollback => db.rollback(probe.tx.expect("rollback probe tx")),
        TxControlSurface::ApplyChanges => db
            .apply_changes(
                populated_changeset(probe.table.as_deref().expect("apply table"), key),
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            )
            .map(|_| ()),
        TxControlSurface::Close => db.close(),
    }
}

fn cleanup_tx_surface(db: &Database, probe: &PreparedTxSurface) {
    if let Some(tx) = probe.tx {
        let _ = db.rollback(tx);
    }
}

fn run_closed_tx_surface(db: &Database, surface: TxControlSurface, key: u128) -> Result<()> {
    match surface {
        TxControlSurface::Begin => db.begin().map(|tx| {
            let _ = db.rollback(tx);
        }),
        TxControlSurface::Commit => db.commit(TxId::from_raw_wire(key as u64)),
        TxControlSurface::Rollback => db.rollback(TxId::from_raw_wire(key as u64)),
        TxControlSurface::ApplyChanges => db
            .apply_changes(
                populated_changeset("closed_table", key),
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            )
            .map(|_| ()),
        TxControlSurface::Close => db.close(),
    }
}

fn is_reentry_kind<T>(result: &Result<T>, expected: CallbackKind) -> bool {
    matches!(
        result,
        Err(Error::CallbackReentry { kind }) if *kind == expected
    )
}

fn is_cross_thread_kind<T>(result: &Result<T>, expected: CallbackKind) -> bool {
    matches!(
        result,
        Err(Error::CallbackActiveCrossThread { kind }) if *kind == expected
    )
}

fn observe_surface_inside_fresh_trigger(
    target: Arc<Database>,
    probe: Arc<PreparedTxSurface>,
    key: u128,
) -> Result<()> {
    let db_x = Arc::new(Database::open_memory());
    db_x.execute("CREATE TABLE x (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db_x.execute("CREATE TRIGGER tr_x ON x WHEN INSERT", &empty())
        .unwrap();
    let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
    let observed_cb = observed.clone();
    db_x.register_trigger_callback("tr_x", move |_db_handle, _ctx| {
        let attempt = run_tx_surface(&target, &probe, key);
        *observed_cb.lock().unwrap() = Some(attempt);
        Ok(())
    })
    .unwrap();
    db_x.complete_initialization().unwrap();
    db_x.execute(
        "INSERT INTO x (id) VALUES ($id)",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(key)))]),
    )
    .unwrap();
    observed.lock().unwrap().take().expect("trigger ran")
}

#[derive(Default)]
struct CloseOnQueryPlugin {
    db: Mutex<Option<Arc<Database>>>,
    observed: Mutex<Option<Result<()>>>,
    armed: AtomicBool,
}

impl CloseOnQueryPlugin {
    fn attach(&self, db: Arc<Database>) {
        *self.db.lock().unwrap() = Some(db);
    }

    fn arm(&self) {
        *self.observed.lock().unwrap() = None;
        self.armed.store(true, AtomicOrdering::SeqCst);
    }

    fn take_observed(&self) -> Option<Result<()>> {
        self.observed.lock().unwrap().take()
    }
}

impl DatabasePlugin for CloseOnQueryPlugin {
    fn on_query(&self, _sql: &str) -> Result<()> {
        if self.armed.swap(false, AtomicOrdering::SeqCst) {
            let db = self
                .db
                .lock()
                .unwrap()
                .as_ref()
                .expect("plugin db attached")
                .clone();
            let result = db.close();
            *self.observed.lock().unwrap() = Some(result);
            return Err(Error::Other(
                "plugin close precedence probe abort".to_string(),
            ));
        }
        Ok(())
    }
}

fn plugin_db_with_read_table() -> (Arc<Database>, Arc<CloseOnQueryPlugin>) {
    let plugin = Arc::new(CloseOnQueryPlugin::default());
    let db = Arc::new(Database::open_memory_with_plugin(plugin.clone()).unwrap());
    plugin.attach(db.clone());
    db.execute("CREATE TABLE read_probe (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    (db, plugin)
}

fn assert_display_impl_does_not_allocate(iterations_per_case: usize, profile_label: &str) {
    use crate::test_alloc_counter::{ALLOC_COUNT, AllocCounter, COUNT_ENABLED};

    let cases = [
        Error::CallbackActiveCrossThread {
            kind: CallbackKind::Trigger,
        },
        Error::CallbackActiveCrossThread {
            kind: CallbackKind::Cron,
        },
        Error::CallbackReentry {
            kind: CallbackKind::Trigger,
        },
        Error::CallbackReentry {
            kind: CallbackKind::Cron,
        },
    ];
    let mut sink = NoAllocSink::new();

    // Enable the counter before the first Display call. This is intentionally a
    // cold-call probe as well as a steady-state probe: a lazy OnceLock/String
    // cache in Display would allocate on first use and must fail this contract.
    COUNT_ENABLED.with(|enabled| enabled.set(true));
    let before = ALLOC_COUNT.load(AtomicOrdering::SeqCst);
    for _ in 0..iterations_per_case {
        for err in &cases {
            sink.reset();
            std::fmt::write(&mut sink, format_args!("{}", err)).expect("write to NoAllocSink");
        }
    }
    let after = ALLOC_COUNT.load(AtomicOrdering::SeqCst);
    COUNT_ENABLED.with(|enabled| enabled.set(false));

    let total_writes = iterations_per_case * cases.len();
    let delta = after.saturating_sub(before);
    assert_eq!(
        delta, 0,
        "Display impl on Error::CallbackActiveCrossThread / Error::CallbackReentry must be alloc-free in {profile_label}; observed {delta} allocations across {total_writes} writes. A `format!`/`String` inside Display would push this above zero."
    );

    // Suppress unused-impl warning if some path isn't reached.
    let _: AllocCounter = AllocCounter;
}

// ============================================================================
// t01_begin_a1_cron_reentry — RED
// ============================================================================
#[test]
#[serial]
fn t01_begin_a1_cron_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        let observed: Arc<Mutex<Option<Result<contextdb_core::TxId>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |_db_handle| {
            let attempt = db_for_cb.begin();
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        fire_cron_now(&db);
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Cron
                })
            ),
            "expected Err(CallbackReentry {{ kind: Cron }}), got {:?}",
            observed
        );
    })
    .expect("t01 deadlocked");
}

// ============================================================================
// t02_begin_a2_trigger_reentry — RED
// ============================================================================
#[test]
#[serial]
fn t02_begin_a2_trigger_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let observed: Arc<Mutex<Option<Result<contextdb_core::TxId>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            let attempt = db_for_cb.begin();
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(1)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected Err(CallbackReentry {{ kind: Trigger }}), got {:?}",
            observed
        );
    })
    .expect("t02 deadlocked");
}

// ============================================================================
// t03_begin_b1_cron_cross_thread — RED
// ============================================================================
#[test]
#[serial]
fn t03_begin_b1_cron_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered.clone(), done.clone());
        let dispatch = dispatch_cron_now_in_thread(db.clone());
        entered.wait();
        let result = db.begin();
        done.wait();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Cron
                })
            ),
            "expected Err(CallbackActiveCrossThread {{ kind: Cron }}), got {:?}",
            result
        );
        let _ = dispatch.join();
    })
    .expect("t03 deadlocked");
}

// ============================================================================
// t04_begin_b2_trigger_cross_thread — RED
// ============================================================================
#[test]
#[serial]
fn t04_begin_b2_trigger_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        let fire = fire_trigger_in_thread(db.clone(), 0xB2);
        entered.wait();
        let result = db.begin();
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected Err(CallbackActiveCrossThread {{ kind: Trigger }}), got {:?}",
            result
        );
    })
    .expect("t04 deadlocked");
}

// ============================================================================
// t05_commit_a1_cron_reentry — RED
// ============================================================================
#[test]
#[serial]
fn t05_commit_a1_cron_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        let setup_table = "commit_probe_05";
        let setup_key = 0xC05;
        let setup_tx = stage_commit_probe_row(&db, setup_table, setup_key);
        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |_db_handle| {
            let attempt = db_for_cb.commit(setup_tx);
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        fire_cron_now(&db);
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Cron
                })
            ),
            "expected Err(CallbackReentry {{ kind: Cron }}), got {:?}",
            observed
        );
        assert_commit_probe_survives_typed_err(&db, setup_table, setup_tx, setup_key);
    })
    .expect("t05 deadlocked");
}

// ============================================================================
// t06_commit_a2_trigger_reentry — RED
// ============================================================================
#[test]
#[serial]
fn t06_commit_a2_trigger_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let setup_table = "commit_probe_06";
        let setup_key = 0xC06;
        let setup_tx = stage_commit_probe_row(&db, setup_table, setup_key);
        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            let attempt = db_for_cb.commit(setup_tx);
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(2)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected Err(CallbackReentry {{ kind: Trigger }}), got {:?}",
            observed
        );
        assert_commit_probe_survives_typed_err(&db, setup_table, setup_tx, setup_key);
    })
    .expect("t06 deadlocked");
}

// ============================================================================
// t07_commit_b1_cron_cross_thread — RED
// ============================================================================
#[test]
#[serial]
fn t07_commit_b1_cron_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered.clone(), done.clone());
        let setup_table = "commit_probe_07";
        let setup_key = 0xC07;
        let setup_tx = stage_commit_probe_row(&db, setup_table, setup_key);
        let dispatch = dispatch_cron_now_in_thread(db.clone());
        entered.wait();
        let result = db.commit(setup_tx);
        done.wait();
        let _ = dispatch.join();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Cron
                })
            ),
            "expected Err(CallbackActiveCrossThread {{ kind: Cron }}), got {:?}",
            result
        );
        assert_commit_probe_survives_typed_err(&db, setup_table, setup_tx, setup_key);
    })
    .expect("t07 deadlocked");
}

// ============================================================================
// t08_commit_b2_trigger_cross_thread — RED
// ============================================================================
#[test]
#[serial]
fn t08_commit_b2_trigger_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        let setup_table = "commit_probe_08";
        let setup_key = 0xC08;
        let setup_tx = stage_commit_probe_row(&db, setup_table, setup_key);
        let fire = fire_trigger_in_thread(db.clone(), 0xC8);
        entered.wait();
        let result = db.commit(setup_tx);
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected Err(CallbackActiveCrossThread {{ kind: Trigger }}), got {:?}",
            result
        );
        assert_commit_probe_survives_typed_err(&db, setup_table, setup_tx, setup_key);
    })
    .expect("t08 deadlocked");
}

// ============================================================================
// t09_rollback_a1_cron_reentry — RED
// ============================================================================
#[test]
#[serial]
fn t09_rollback_a1_cron_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        let setup_table = "rollback_probe_09";
        let setup_key = 0xC09;
        let setup_tx = stage_commit_probe_row(&db, setup_table, setup_key);
        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |_db_handle| {
            let attempt = db_for_cb.rollback(setup_tx);
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        fire_cron_now(&db);
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Cron
                })
            ),
            "got {:?}",
            observed
        );
        assert_rollback_probe_survives_typed_err(&db, setup_table, setup_tx, setup_key);
    })
    .expect("t09 deadlocked");
}

// ============================================================================
// t10_rollback_a2_trigger_reentry — RED
// ============================================================================
#[test]
#[serial]
fn t10_rollback_a2_trigger_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let setup_table = "rollback_probe_10";
        let setup_key = 0xC10;
        let setup_tx = stage_commit_probe_row(&db, setup_table, setup_key);
        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            let attempt = db_for_cb.rollback(setup_tx);
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA10)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "got {:?}",
            observed
        );
        assert_rollback_probe_survives_typed_err(&db, setup_table, setup_tx, setup_key);
    })
    .expect("t10 deadlocked");
}

// ============================================================================
// t11_rollback_b1_cron_cross_thread — RED
// ============================================================================
#[test]
#[serial]
fn t11_rollback_b1_cron_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered.clone(), done.clone());
        let setup_table = "rollback_probe_11";
        let setup_key = 0xC11;
        let setup_tx = stage_commit_probe_row(&db, setup_table, setup_key);
        let dispatch = dispatch_cron_now_in_thread(db.clone());
        entered.wait();
        let result = db.rollback(setup_tx);
        done.wait();
        let _ = dispatch.join();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Cron
                })
            ),
            "got {:?}",
            result
        );
        assert_rollback_probe_survives_typed_err(&db, setup_table, setup_tx, setup_key);
    })
    .expect("t11 deadlocked");
}

// ============================================================================
// t12_rollback_b2_trigger_cross_thread — RED
// ============================================================================
#[test]
#[serial]
fn t12_rollback_b2_trigger_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        let setup_table = "rollback_probe_12";
        let setup_key = 0xC12;
        let setup_tx = stage_commit_probe_row(&db, setup_table, setup_key);
        let fire = fire_trigger_in_thread(db.clone(), 0xC12);
        entered.wait();
        let result = db.rollback(setup_tx);
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                })
            ),
            "got {:?}",
            result
        );
        assert_rollback_probe_survives_typed_err(&db, setup_table, setup_tx, setup_key);
    })
    .expect("t12 deadlocked");
}

// ============================================================================
// t_apply_changes_positive_control — REGRESSION GUARD
// Pins that populated_changeset_n produces a well-formed ChangeSet that
// inserts every requested row when uncontended. Without this control, t13–t16
// and t47 could pass vacuously: a malformed ChangeSet builder would cause
// apply_changes to return Err for builder reasons, and the post-Err scan
// (asserting 0 rows) would also be satisfied for the wrong reason.
// ============================================================================
#[test]
#[serial]
fn t_apply_changes_positive_control_helper_inserts_when_uncontended() {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE applied (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    let cs = populated_changeset_n("applied", 0xC47_0000, 100);
    db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .expect("uncontended apply_changes must succeed");
    let snap = db.snapshot();
    assert_eq!(
        db.scan("applied", snap).unwrap().len(),
        100,
        "populated_changeset_n must insert all 100 rows when uncontended; otherwise t13–t16/t47 post-Err absence assertions are not load-bearing"
    );
}

// ============================================================================
// t13_apply_changes_a1_cron_reentry — RED
// Populated ChangeSet (1 row) — pins fail-early-and-rebuild semantics: the
// gate trips BEFORE consuming the changeset (post-Err scan returns 0 rows).
// ============================================================================
#[test]
#[serial]
fn t13_apply_changes_a1_cron_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let observed: Arc<Mutex<Option<Result<contextdb_engine::ApplyResult>>>> =
            Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |_db_handle| {
            let cs = populated_changeset("t", 0xA13);
            let attempt =
                db_for_cb.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins));
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        fire_cron_now(&db);
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Cron
                })
            ),
            "got {:?}",
            observed
        );
        // Fail-early-and-rebuild: row from the changeset must NOT have been
        // consumed before the gate tripped.
        let snap = db.snapshot();
        assert_eq!(
            db.scan("t", snap).unwrap().len(),
            0,
            "apply_changes must trip its callback-active gate BEFORE consuming the changeset"
        );
    })
    .expect("t13 deadlocked");
}

// ============================================================================
// t13b_apply_changes_non_row_shapes_a1_cron_reentry — RED
// Empty, edge-only, vector-only, valid DDL-only, and malformed-ddl-lsn
// ChangeSets still trip the surface gate before apply_changes inspects or
// validates content. Prevents a row-only/empty-only typed implementation.
// ============================================================================
#[test]
#[serial]
fn t13b_apply_changes_non_row_shapes_a1_cron_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        let vector_row_id = prepare_vector_shape_target(&db, 0x13B2_0000);
        let observed: Arc<Mutex<Vec<ApplyShapeResult>>> = Arc::new(Mutex::new(Vec::new()));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |_db_handle| {
            let shapes = [
                ("empty", ChangeSet::default()),
                ("edge-only", edge_only_changeset(0x13B1)),
                ("vector-only", vector_only_changeset(0x13B2, vector_row_id)),
                ("ddl-only", ddl_only_changeset(0x13B3)),
                ("invalid-ddl-lsn", invalid_ddl_lsn_changeset(0x13B4)),
            ];
            let mut observed = observed_cb.lock().unwrap();
            for (name, changes) in shapes {
                observed.push((
                    name,
                    db_for_cb.apply_changes(
                        changes,
                        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
                    ),
                ));
            }
            Ok(())
        })
        .unwrap();
        fire_cron_now(&db);
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 5, "callback must exercise every A1 shape");
        for (name, result) in observed.iter() {
            assert!(
                matches!(
                    result,
                    Err(Error::CallbackReentry {
                        kind: CallbackKind::Cron
                    })
                ),
                "{name} ChangeSet under A1 must return CallbackReentry{{Cron}}, got {:?}",
                result
            );
        }
        assert_non_row_apply_shapes_left_no_side_effects(
            &db,
            vector_row_id,
            &[0x13B3, 0x13B4],
            "A1",
        );
    })
    .expect("t13b deadlocked");
}

// ============================================================================
// t14_apply_changes_a2_trigger_reentry — RED
// Populated ChangeSet (1 row); post-Err absence assertion.
// ============================================================================
#[test]
#[serial]
fn t14_apply_changes_a2_trigger_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        // Second table for the apply_changes target — separate from the trigger's
        // firing table to keep the typed-Err caused by A2 distinct from any
        // trigger-DDL/changeset-conflict semantics.
        db.execute("CREATE TABLE applied (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let observed: Arc<Mutex<Option<Result<contextdb_engine::ApplyResult>>>> =
            Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            let cs = populated_changeset("applied", 0xA14B);
            let attempt =
                db_for_cb.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins));
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA14)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "got {:?}",
            observed
        );
        let snap = db.snapshot();
        assert_eq!(
            db.scan("applied", snap).unwrap().len(),
            0,
            "apply_changes must trip its callback-active gate BEFORE consuming the changeset"
        );
    })
    .expect("t14 deadlocked");
}

// ============================================================================
// t14b_apply_changes_non_row_shapes_a2_trigger_reentry — RED
// Empty, edge-only, vector-only, valid DDL-only, and malformed-ddl-lsn
// ChangeSets still trip the surface gate before apply_changes inspects or
// validates content under pure trigger reentry.
// ============================================================================
#[test]
#[serial]
fn t14b_apply_changes_non_row_shapes_a2_trigger_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let vector_row_id = prepare_vector_shape_target(&db, 0x14B2_0000);
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let observed: Arc<Mutex<Vec<ApplyShapeResult>>> = Arc::new(Mutex::new(Vec::new()));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            let shapes = [
                ("empty", ChangeSet::default()),
                ("edge-only", edge_only_changeset(0x14B1)),
                ("vector-only", vector_only_changeset(0x14B2, vector_row_id)),
                ("ddl-only", ddl_only_changeset(0x14B3)),
                ("invalid-ddl-lsn", invalid_ddl_lsn_changeset(0x14B4)),
            ];
            let mut observed = observed_cb.lock().unwrap();
            for (name, changes) in shapes {
                observed.push((
                    name,
                    db_for_cb.apply_changes(
                        changes,
                        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
                    ),
                ));
            }
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA14B0)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 5, "callback must exercise every A2 shape");
        for (name, result) in observed.iter() {
            assert!(
                matches!(
                    result,
                    Err(Error::CallbackReentry {
                        kind: CallbackKind::Trigger
                    })
                ),
                "{name} ChangeSet under A2 must return CallbackReentry{{Trigger}}, got {:?}",
                result
            );
        }
        assert_non_row_apply_shapes_left_no_side_effects(
            &db,
            vector_row_id,
            &[0x14B3, 0x14B4],
            "A2",
        );
    })
    .expect("t14b deadlocked");
}

// ============================================================================
// t15_apply_changes_b1_cron_cross_thread — RED
// Populated ChangeSet (1 row); post-Err absence assertion.
// ============================================================================
#[test]
#[serial]
fn t15_apply_changes_b1_cron_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered.clone(), done.clone());
        // Apply target table — separate from `c` (the schedule's no-op table).
        db.execute("CREATE TABLE applied (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let dispatch = dispatch_cron_now_in_thread(db.clone());
        entered.wait();
        let cs = populated_changeset("applied", 0xC15);
        let result = db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins));
        done.wait();
        let _ = dispatch.join();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Cron
                })
            ),
            "got {:?}",
            result
        );
        let snap = db.snapshot();
        assert_eq!(
            db.scan("applied", snap).unwrap().len(),
            0,
            "apply_changes must trip its callback-active gate BEFORE consuming the changeset"
        );
    })
    .expect("t15 deadlocked");
}

// ============================================================================
// t15b_apply_changes_non_row_shapes_b1_cron_cross_thread — RED
// B1 gate is content-independent: empty, edge-only, vector-only, valid DDL-only,
// and malformed-ddl-lsn changesets must all fail before apply_changes inspects
// or validates them.
// ============================================================================
#[test]
#[serial]
fn t15b_apply_changes_non_row_shapes_b1_cron_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered.clone(), done.clone());
        let vector_row_id = prepare_vector_shape_target(&db, 0x15B2_0000);
        let dispatch = dispatch_cron_now_in_thread(db.clone());
        entered.wait();
        let shapes = [
            ("empty", ChangeSet::default()),
            ("edge-only", edge_only_changeset(0x15B1)),
            ("vector-only", vector_only_changeset(0x15B2, vector_row_id)),
            ("ddl-only", ddl_only_changeset(0x15B3)),
            ("invalid-ddl-lsn", invalid_ddl_lsn_changeset(0x15B4)),
        ];
        let mut observed = Vec::with_capacity(shapes.len());
        for (name, changes) in shapes {
            let result = db.apply_changes(
                changes,
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            );
            observed.push((name, result));
        }
        done.wait();
        let _ = dispatch.join();
        for (name, result) in observed {
            assert!(
                matches!(
                    result,
                    Err(Error::CallbackActiveCrossThread {
                        kind: CallbackKind::Cron
                    })
                ),
                "{name} ChangeSet under B1 must return CallbackActiveCrossThread{{Cron}}, got {:?}",
                result
            );
        }
        assert_non_row_apply_shapes_left_no_side_effects(
            &db,
            vector_row_id,
            &[0x15B3, 0x15B4],
            "B1",
        );
    })
    .expect("t15b deadlocked");
}

// ============================================================================
// t16_apply_changes_b2_trigger_cross_thread — RED
// Populated ChangeSet (1 row); post-Err absence assertion.
// ============================================================================
#[test]
#[serial]
fn t16_apply_changes_b2_trigger_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        // Apply target table — separate from `t` (the trigger's firing table).
        db.execute("CREATE TABLE applied (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let fire = fire_trigger_in_thread(db.clone(), 0xC16);
        entered.wait();
        let cs = populated_changeset("applied", 0xC16B);
        let result = db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins));
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                })
            ),
            "got {:?}",
            result
        );
        let snap = db.snapshot();
        assert_eq!(
            db.scan("applied", snap).unwrap().len(),
            0,
            "apply_changes must trip its callback-active gate BEFORE consuming the changeset"
        );
    })
    .expect("t16 deadlocked");
}

// ============================================================================
// t16b_apply_changes_non_row_shapes_b2_trigger_cross_thread — RED
// B2 gate is content-independent: empty, edge-only, vector-only, valid DDL-only,
// and malformed-ddl-lsn changesets must all fail before apply_changes inspects
// or validates them.
// ============================================================================
#[test]
#[serial]
fn t16b_apply_changes_non_row_shapes_b2_trigger_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        let vector_row_id = prepare_vector_shape_target(&db, 0x16B2_0000);
        let fire = fire_trigger_in_thread(db.clone(), 0xC16B0);
        entered.wait();
        let shapes = [
            ("empty", ChangeSet::default()),
            ("edge-only", edge_only_changeset(0x16B1)),
            ("vector-only", vector_only_changeset(0x16B2, vector_row_id)),
            ("ddl-only", ddl_only_changeset(0x16B3)),
            ("invalid-ddl-lsn", invalid_ddl_lsn_changeset(0x16B4)),
        ];
        let mut observed = Vec::with_capacity(shapes.len());
        for (name, changes) in shapes {
            let result =
                db.apply_changes(changes, &ConflictPolicies::uniform(ConflictPolicy::LatestWins));
            observed.push((name, result));
        }
        done.wait();
        fire.join().unwrap().unwrap();
        for (name, result) in observed {
            assert!(
                matches!(
                    result,
                    Err(Error::CallbackActiveCrossThread {
                        kind: CallbackKind::Trigger
                    })
                ),
                "{name} ChangeSet under B2 must return CallbackActiveCrossThread{{Trigger}}, got {:?}",
                result
            );
        }
        assert_non_row_apply_shapes_left_no_side_effects(
            &db,
            vector_row_id,
            &[0x16B3, 0x16B4],
            "B2",
        );
    })
    .expect("t16b deadlocked");
}

// ============================================================================
// t17_close_a1_cron_reentry — RED
// ============================================================================
#[test]
#[serial]
fn t17_close_a1_cron_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |_db_handle| {
            let attempt = db_for_cb.close();
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        fire_cron_now(&db);
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Cron
                })
            ),
            "got {:?}",
            observed
        );
        assert_handle_still_open(&db, "close A1");
    })
    .expect("t17 deadlocked");
}

// ============================================================================
// t18_close_a2_trigger_reentry — RED (NEW check; surface previously missing)
// ============================================================================
#[test]
#[serial]
fn t18_close_a2_trigger_reentry() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            let attempt = db_for_cb.close();
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA18)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected new A2 check on close() to return CallbackReentry {{ Trigger }}, got {:?}",
            observed
        );
        assert_handle_still_open(&db, "close A2");
    })
    .expect("t18 deadlocked");
}

// ============================================================================
// t19_close_b1_cron_cross_thread — RED
// ============================================================================
#[test]
#[serial]
fn t19_close_b1_cron_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered.clone(), done.clone());
        let dispatch = dispatch_cron_now_in_thread(db.clone());
        entered.wait();
        let result = db.close();
        done.wait();
        let _ = dispatch.join();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Cron
                })
            ),
            "got {:?}",
            result
        );
        assert_handle_still_open(&db, "close B1");
    })
    .expect("t19 deadlocked");
}

// ============================================================================
// t20_close_b2_trigger_cross_thread — RED
// ============================================================================
#[test]
#[serial]
fn t20_close_b2_trigger_cross_thread() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        let fire = fire_trigger_in_thread(db.clone(), 0xC20);
        entered.wait();
        let result = db.close();
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                })
            ),
            "got {:?}",
            result
        );
        assert_handle_still_open(&db, "close B2");
    })
    .expect("t20 deadlocked");
}

// ============================================================================
// t21_helper_inside_cron_a1 — RED — execute() forbidden-statement gate
// ============================================================================
#[test]
#[serial]
fn t21_helper_inside_cron_a1() {
    run_with_timeout(|| {
        for (idx, &(name, sql)) in forbidden_statement_samples().iter().enumerate() {
            let tmp = tempfile::TempDir::new().expect("tempdir");
            let db = Arc::new(
                Database::open(tmp.path().join(format!("t21_{idx}_{name}.redb"))).expect("open db"),
            );
            let _pause = db.pause_cron_tickler_for_test();
            prepare_forbidden_statement_side_effect_probe(&db);
            let observed: Arc<Mutex<Option<ForbiddenStatementResult>>> = Arc::new(Mutex::new(None));
            let observed_cb = observed.clone();
            let db_for_cb = db.clone();
            db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
                .unwrap();
            let since = db.current_lsn();
            db.register_cron_callback("cb", move |_db_handle| {
                *observed_cb.lock().unwrap() = Some((name, db_for_cb.execute(sql, &empty())));
                Ok(())
            })
            .unwrap();
            fire_cron_now(&db);
            let (observed_name, result) = observed
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| panic!("cron callback did not execute {name}"));
            assert_eq!(observed_name, name, "cron sample name drift");
            assert!(
                matches!(
                    result,
                    Err(Error::CallbackReentry {
                        kind: CallbackKind::Cron
                    })
                ),
                "cron forbidden statement {name} must return CallbackReentry{{Cron}}, got {:?}",
                result
            );
            assert_forbidden_statement_family_left_no_side_effects(
                &db,
                since,
                &format!("cron same-thread forbidden SQL {name}"),
                0xA2100 + idx as u128,
            );
        }
    })
    .expect("t21 deadlocked");
}

// ============================================================================
// t22_helper_inside_trigger_a2 — RED
// ============================================================================
#[test]
#[serial]
fn t22_helper_inside_trigger_a2() {
    run_with_timeout(|| {
        for (idx, &(name, sql)) in forbidden_statement_samples().iter().enumerate() {
            let tmp = tempfile::TempDir::new().expect("tempdir");
            let db = Arc::new(
                Database::open(tmp.path().join(format!("t22_{idx}_{name}.redb")))
                    .expect("open db"),
            );
            prepare_forbidden_statement_side_effect_probe(&db);
            db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
                .unwrap();
            db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
                .unwrap();
            let observed: Arc<Mutex<Option<ForbiddenStatementResult>>> =
                Arc::new(Mutex::new(None));
            let observed_cb = observed.clone();
            let db_for_cb = db.clone();
            db.register_trigger_callback("tr", move |_db, _ctx| {
                *observed_cb.lock().unwrap() = Some((name, db_for_cb.execute(sql, &empty())));
                Ok(())
            })
            .unwrap();
            db.complete_initialization().unwrap();
            let since = db.current_lsn();
            db.execute(
                "INSERT INTO t (id) VALUES ($id)",
                &HashMap::from([(
                    "id".to_string(),
                    Value::Uuid(uuid(0xA2200 + idx as u128)),
                )]),
            )
            .unwrap();
            let (observed_name, result) = observed
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| panic!("trigger callback did not execute {name}"));
            assert_eq!(observed_name, name, "trigger sample name drift");
            assert!(
                matches!(
                    result,
                    Err(Error::CallbackReentry {
                        kind: CallbackKind::Trigger
                    })
                ),
                "trigger forbidden statement {name} must return CallbackReentry{{Trigger}}, got {:?}",
                result
            );
            assert_forbidden_statement_family_left_no_side_effects(
                &db,
                since,
                &format!("trigger same-thread forbidden SQL {name}"),
                0xA2200 + idx as u128,
            );
        }
    })
    .expect("t22 deadlocked");
}

// ============================================================================
// t23_helper_cross_thread_b1 — RED
// ============================================================================
#[test]
#[serial]
fn t23_helper_cross_thread_b1() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered.clone(), done.clone());
        // Add a writeable table for the cross-thread INSERT attempt.
        db.execute("CREATE TABLE w (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let dispatch = dispatch_cron_now_in_thread(db.clone());
        entered.wait();
        let result = db.execute(
            "INSERT INTO w (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xC23)))]),
        );
        done.wait();
        let _ = dispatch.join();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Cron
                })
            ),
            "got {:?}",
            result
        );
        assert!(
            !table_contains_uuid(&db, "w", 0xC23),
            "execute() under B1 must return before staging the cross-thread INSERT"
        );
    })
    .expect("t23 deadlocked");
}

// ============================================================================
// t24_helper_cross_thread_b2 — RED
// ============================================================================
#[test]
#[serial]
fn t24_helper_cross_thread_b2() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        db.execute("CREATE TABLE w (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let fire = fire_trigger_in_thread(db.clone(), 0xC24);
        entered.wait();
        let result = db.execute(
            "INSERT INTO w (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xC24A)))]),
        );
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                })
            ),
            "got {:?}",
            result
        );
        assert!(
            !table_contains_uuid(&db, "w", 0xC24A),
            "execute() under B2 must return before staging the cross-thread INSERT"
        );
    })
    .expect("t24 deadlocked");
}

// ============================================================================
// t25_helper_tx_bound_b1 — RED — execute_in_tx vs assert_cron_callback_tx_bound_handle
// ============================================================================
#[test]
#[serial]
fn t25_helper_tx_bound_b1() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered.clone(), done.clone());
        db.execute("CREATE TABLE w (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let setup_tx = db.begin().expect("setup begin");
        let dispatch = dispatch_cron_now_in_thread(db.clone());
        entered.wait();
        let result = db.execute_in_tx(
            setup_tx,
            "INSERT INTO w (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xC25)))]),
        );
        done.wait();
        let _ = dispatch.join();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Cron
                })
            ),
            "got {:?}",
            result
        );
        db.commit(setup_tx)
            .expect("execute_in_tx setup tx must remain commit-able after B1 typed Err");
        assert!(
            !table_contains_uuid(&db, "w", 0xC25),
            "execute_in_tx under B1 must return before staging the INSERT"
        );
    })
    .expect("t25 deadlocked");
}

// ============================================================================
// t25b_direct_api_tx_bound_b1 — RED
// Direct library APIs (`insert_row`, `upsert_row`, graph/vector helpers) route
// through `assert_cron_callback_tx_bound_handle`, not the SQL helper gate. This
// pins B1 on that direct API guard instead of only covering execute_in_tx().
// ============================================================================
#[test]
#[serial]
fn t25b_direct_api_tx_bound_b1() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let (db, _pause) = build_cron_parked_db(entered.clone(), done.clone());
        let probe = prepare_direct_api_side_effect_probe(&db, 0xC25B00);
        let setup_tx = db.begin().expect("setup begin");
        let dispatch = dispatch_cron_now_in_thread(db.clone());
        entered.wait();
        let results = [
            (
                "insert_row",
                db.insert_row(
                    setup_tx,
                    "w",
                    HashMap::from([("id".to_string(), Value::Uuid(uuid(probe.insert_row_key)))]),
                )
                .map(|_| ()),
            ),
            (
                "upsert_row",
                db.upsert_row(
                    setup_tx,
                    "w",
                    "id",
                    HashMap::from([("id".to_string(), Value::Uuid(uuid(probe.upsert_row_key)))]),
                )
                .map(|_| ()),
            ),
            (
                "delete_row",
                db.delete_row(setup_tx, "w", probe.delete_row_id),
            ),
            (
                "insert_edge",
                db.insert_edge(
                    setup_tx,
                    probe.insert_edge_source,
                    probe.insert_edge_target,
                    "direct".to_string(),
                    HashMap::new(),
                )
                .map(|_| ()),
            ),
            (
                "delete_edge",
                db.delete_edge(
                    setup_tx,
                    probe.delete_edge_source,
                    probe.delete_edge_target,
                    "direct",
                ),
            ),
            (
                "insert_vector",
                db.insert_vector(
                    setup_tx,
                    VectorIndexRef::new("v", "embedding"),
                    probe.insert_vector_row_id,
                    vec![0.1, 0.2, 0.3],
                ),
            ),
            (
                "delete_vector",
                db.delete_vector(
                    setup_tx,
                    VectorIndexRef::new("v", "embedding"),
                    probe.delete_vector_row_id,
                ),
            ),
        ];
        done.wait();
        let _ = dispatch.join();
        for (name, result) in results {
            assert!(
                matches!(
                    result,
                    Err(Error::CallbackActiveCrossThread {
                        kind: CallbackKind::Cron
                    })
                ),
                "direct {name} under B1 must return CallbackActiveCrossThread{{Cron}}, got {:?}",
                result
            );
        }
        db.commit(setup_tx)
            .expect("direct API tx must remain commit-able after B1 typed Errs");
        assert_direct_api_typed_err_left_no_side_effects(&db, &probe, "B1 direct API");
    })
    .expect("t25b deadlocked");
}

// ============================================================================
// t26b_direct_api_tx_bound_b2 — RED
// Symmetric direct library API coverage for trigger-active cross-thread gating.
// ============================================================================
#[test]
#[serial]
fn t26b_direct_api_tx_bound_b2() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        let probe = prepare_direct_api_side_effect_probe(&db, 0xC26B00);
        let setup_tx = db.begin().expect("setup begin");
        let fire = fire_trigger_in_thread(db.clone(), 0xC26B);
        entered.wait();
        let results = [
            (
                "insert_row",
                db.insert_row(
                    setup_tx,
                    "w",
                    HashMap::from([("id".to_string(), Value::Uuid(uuid(probe.insert_row_key)))]),
                )
                .map(|_| ()),
            ),
            (
                "upsert_row",
                db.upsert_row(
                    setup_tx,
                    "w",
                    "id",
                    HashMap::from([("id".to_string(), Value::Uuid(uuid(probe.upsert_row_key)))]),
                )
                .map(|_| ()),
            ),
            (
                "delete_row",
                db.delete_row(setup_tx, "w", probe.delete_row_id),
            ),
            (
                "insert_edge",
                db.insert_edge(
                    setup_tx,
                    probe.insert_edge_source,
                    probe.insert_edge_target,
                    "direct".to_string(),
                    HashMap::new(),
                )
                .map(|_| ()),
            ),
            (
                "delete_edge",
                db.delete_edge(
                    setup_tx,
                    probe.delete_edge_source,
                    probe.delete_edge_target,
                    "direct",
                ),
            ),
            (
                "insert_vector",
                db.insert_vector(
                    setup_tx,
                    VectorIndexRef::new("v", "embedding"),
                    probe.insert_vector_row_id,
                    vec![0.1, 0.2, 0.3],
                ),
            ),
            (
                "delete_vector",
                db.delete_vector(
                    setup_tx,
                    VectorIndexRef::new("v", "embedding"),
                    probe.delete_vector_row_id,
                ),
            ),
        ];
        done.wait();
        fire.join().unwrap().unwrap();
        for (name, result) in results {
            assert!(
                matches!(
                    result,
                    Err(Error::CallbackActiveCrossThread {
                        kind: CallbackKind::Trigger
                    })
                ),
                "direct {name} under B2 must return CallbackActiveCrossThread{{Trigger}}, got {:?}",
                result
            );
        }
        db.commit(setup_tx)
            .expect("direct API tx must remain commit-able after B2 typed Errs");
        assert_direct_api_typed_err_left_no_side_effects(&db, &probe, "B2 direct API");
    })
    .expect("t26b deadlocked");
}

// ============================================================================
// t26_helper_tx_bound_b2 — RED
// ============================================================================
#[test]
#[serial]
fn t26_helper_tx_bound_b2() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        db.execute("CREATE TABLE w (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let setup_tx = db.begin().expect("setup begin");
        let fire = fire_trigger_in_thread(db.clone(), 0xC26);
        entered.wait();
        let result = db.execute_in_tx(
            setup_tx,
            "INSERT INTO w (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xC26A)))]),
        );
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                })
            ),
            "got {:?}",
            result
        );
        db.commit(setup_tx)
            .expect("execute_in_tx setup tx must remain commit-able after B2 typed Err");
        assert!(
            !table_contains_uuid(&db, "w", 0xC26A),
            "execute_in_tx under B2 must return before staging the INSERT"
        );
    })
    .expect("t26 deadlocked");
}

// ============================================================================
// t27_execute_in_tx_inline_a1 — RED
// ============================================================================
#[test]
#[serial]
fn t27_execute_in_tx_inline_a1() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        db.execute("CREATE TABLE w (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let setup_tx = db.begin().expect("setup begin");
        let observed: Arc<Mutex<Option<Result<contextdb_engine::QueryResult>>>> =
            Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |_db_handle| {
            let attempt = db_for_cb.execute_in_tx(
                setup_tx,
                "INSERT INTO w (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA27)))]),
            );
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        fire_cron_now(&db);
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Cron
                })
            ),
            "got {:?}",
            observed
        );
    })
    .expect("t27 deadlocked");
}

// ============================================================================
// t28_display_strings_exact — REGRESSION GUARD
// ============================================================================
#[test]
#[serial]
fn t28_display_strings_exact() {
    use crate::test_alloc_counter::{ALLOC_COUNT, COUNT_ENABLED};

    let cases = [
        (
            Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            },
            "trigger callback active on another thread; retry once the callback completes",
        ),
        (
            Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            },
            "cron callback active on another thread; retry once the callback completes",
        ),
        (
            Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            },
            "operation not allowed inside a trigger callback (API misuse — fix the call site)",
        ),
        (
            Error::CallbackReentry {
                kind: CallbackKind::Cron,
            },
            "operation not allowed inside a cron callback (API misuse — fix the call site)",
        ),
    ];
    let mut sink = NoAllocSink::new();
    COUNT_ENABLED.with(|enabled| enabled.set(true));
    let before = ALLOC_COUNT.load(AtomicOrdering::SeqCst);
    for (err, _) in &cases {
        sink.reset();
        std::fmt::write(&mut sink, format_args!("{}", err)).expect("Display alloc-free");
    }
    let after = ALLOC_COUNT.load(AtomicOrdering::SeqCst);
    COUNT_ENABLED.with(|enabled| enabled.set(false));
    assert_eq!(
        after - before,
        0,
        "first Display call for each typed callback error must be alloc-free; lazy String/OnceLock caches are not allowed"
    );

    for (err, expected) in &cases {
        let mut sink = NoAllocSink::new();
        std::fmt::write(&mut sink, format_args!("{}", err)).expect("Display alloc-free");
        assert_eq!(sink.as_str(), *expected, "Display drift on {:?}", err);
    }
    // Kind-agnostic substrings — both class greppable substrings appear.
    let mut sink = NoAllocSink::new();
    std::fmt::write(
        &mut sink,
        format_args!(
            "{}",
            Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron
            }
        ),
    )
    .unwrap();
    assert!(sink.as_str().contains("callback active on another thread"));
    sink.reset();
    std::fmt::write(
        &mut sink,
        format_args!(
            "{}",
            Error::CallbackReentry {
                kind: CallbackKind::Trigger
            }
        ),
    )
    .unwrap();
    assert!(sink.as_str().contains("operation not allowed inside"));
}

// ============================================================================
// t29_display_impl_does_not_allocate — RED — counts allocator activity directly
// Pins behavior contract §3 alloc-freeness against Display. Wraps the global
// allocator with a counter; takes a delta snapshot around a 1M-iteration
// `write!` loop on a single thread (so the delta attributes only to this
// test's loop, not to other threads in the test binary). Asserts delta == 0.
// A Display impl that internally uses `format!`/`String` would push delta > 0
// and fail. The output-overflow check on NoAllocSink (256-byte budget) is
// retained as a secondary signal in `t29_display_output_fits_256_bytes`.
// ============================================================================
#[test]
#[serial]
fn t29_display_impl_does_not_allocate() {
    assert_display_impl_does_not_allocate(250_000, "the active test profile");
}

// ============================================================================
// t29b_display_impl_does_not_allocate_release_profile — RED in release
// Dedicated release-profile gate for the alloc-free Display contract. The main
// t29 probe also runs in debug, but this release-only test prevents a
// cfg(debug_assertions)-only implementation from satisfying the suite.
// ============================================================================
#[cfg(not(debug_assertions))]
#[test]
#[serial]
fn t29b_display_impl_does_not_allocate_release_profile() {
    assert_display_impl_does_not_allocate(250_000, "release");
}

// ============================================================================
// t29_display_output_fits_256_bytes — REGRESSION GUARD
// Output-budget check retained from the original NoAllocSink probe. If a
// future edit makes the Display string longer than 256 bytes the no-alloc
// sink would overflow; this test captures that drift independently of the
// allocator probe above.
// ============================================================================
#[test]
#[serial]
fn t29_display_output_fits_256_bytes() {
    for err in [
        Error::CallbackActiveCrossThread {
            kind: CallbackKind::Trigger,
        },
        Error::CallbackActiveCrossThread {
            kind: CallbackKind::Cron,
        },
        Error::CallbackReentry {
            kind: CallbackKind::Trigger,
        },
        Error::CallbackReentry {
            kind: CallbackKind::Cron,
        },
    ] {
        let mut sink = NoAllocSink::new();
        std::fmt::write(&mut sink, format_args!("{}", err))
            .expect("Display output must fit in the 256-byte NoAllocSink");
    }
}

// ============================================================================
// t30_class_distinguishability — REGRESSION GUARD
// ============================================================================
#[test]
#[serial]
fn t30_class_distinguishability() {
    fn classify(err: &Error) -> &'static str {
        match err {
            Error::CallbackActiveCrossThread { .. } => "ClassB",
            Error::CallbackReentry { .. } => "ClassA",
            _ => "Other",
        }
    }
    assert_eq!(
        classify(&Error::CallbackActiveCrossThread {
            kind: CallbackKind::Cron
        }),
        "ClassB"
    );
    assert_eq!(
        classify(&Error::CallbackActiveCrossThread {
            kind: CallbackKind::Trigger
        }),
        "ClassB"
    );
    assert_eq!(
        classify(&Error::CallbackReentry {
            kind: CallbackKind::Cron
        }),
        "ClassA"
    );
    assert_eq!(
        classify(&Error::CallbackReentry {
            kind: CallbackKind::Trigger
        }),
        "ClassA"
    );
    // Class is independent of kind: substring-only match works for cross-process consumers.
    let display = format!(
        "{}",
        Error::CallbackActiveCrossThread {
            kind: CallbackKind::Trigger
        }
    );
    assert!(display.contains("callback active on another thread"));
    let display = format!(
        "{}",
        Error::CallbackReentry {
            kind: CallbackKind::Cron
        }
    );
    assert!(display.contains("operation not allowed inside"));
}

// ============================================================================
// t31_retry_backoff_shape — RED
// ============================================================================
#[test]
#[serial]
fn t31_retry_backoff_shape() {
    run_with_timeout(|| {
        // Two-barrier harness: fire thread enters callback (signals `entered`),
        // then exits when every retrying thread has reported a typed contention
        // observation (signaled by `done`).
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let entered_cb = entered.clone();
        let done_cb = done.clone();
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            entered_cb.wait();              // signal: callback body active, parking
            done_cb.wait();                  // wait for retry threads to observe contention
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        // Fire the trigger on a dedicated thread.
        let fire = fire_trigger_in_thread(db.clone(), 0xC31);
        // Wait until the callback is parked. From this point, every begin()
        // call observes B2 (cross-thread trigger active).
        entered.wait();

        // 8 retrying threads. Each retries on CallbackActiveCrossThread{Trigger}
        // with exponential backoff (1ms to 50ms cap), counting attempts AND counting
        // typed-Err observations. Both counters PIN that the contention path was
        // actually exercised, defeating a no-op `begin() -> Ok(_)` regression.
        #[derive(Debug)]
        enum RetryObservation {
            FirstTypedErr(usize),
            SucceededBeforeTypedErr { thread: usize, attempts: usize },
            UnexpectedErr { thread: usize, error: String },
            RetryLimitExceeded { thread: usize, attempts: usize },
        }

        let n = 8usize;
        let attempts_per_thread = Arc::new(Mutex::new(vec![0usize; n]));
        let observed_typed_err_count = Arc::new(AtomicUsize::new(0));
        let (obs_tx, obs_rx) = mpsc::channel();
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let db = db.clone();
            let attempts_per_thread = attempts_per_thread.clone();
            let observed_typed_err_count = observed_typed_err_count.clone();
            let obs_tx = obs_tx.clone();
            handles.push(thread::spawn(move || -> Result<()> {
                let mut backoff_ms: u64 = 1;
                let mut attempts = 0usize;
                let mut observed_first_typed_err = false;
                loop {
                    attempts += 1;
                    if attempts > 50 {
                        attempts_per_thread.lock().unwrap()[i] = attempts;
                        let _ = obs_tx.send(RetryObservation::RetryLimitExceeded {
                            thread: i,
                            attempts,
                        });
                        panic!("thread {i} exceeded 50 retries");
                    }
                    match db.begin() {
                        Ok(tx) => {
                            // Drop the tx; we are testing retry shape, not commit.
                            let _ = db.rollback(tx);
                            attempts_per_thread.lock().unwrap()[i] = attempts;
                            if !observed_first_typed_err {
                                let _ = obs_tx.send(RetryObservation::SucceededBeforeTypedErr {
                                    thread: i,
                                    attempts,
                                });
                            }
                            return Ok(());
                        }
                        Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger }) => {
                            observed_typed_err_count.fetch_add(1, AtomicOrdering::SeqCst);
                            if !observed_first_typed_err {
                                observed_first_typed_err = true;
                                obs_tx.send(RetryObservation::FirstTypedErr(i)).unwrap();
                            }
                            thread::sleep(Duration::from_millis(backoff_ms));
                            backoff_ms = (backoff_ms.saturating_mul(2)).min(50);
                        }
                        Err(other) => {
                            attempts_per_thread.lock().unwrap()[i] = attempts;
                            let error = format!("{other:?}");
                            let _ = obs_tx.send(RetryObservation::UnexpectedErr {
                                thread: i,
                                error,
                            });
                            return Err(other);
                        }
                    }
                }
            }));
        }
        drop(obs_tx);

        let mut first_typed_err_by_thread = vec![false; n];
        while !first_typed_err_by_thread.iter().all(|observed| *observed) {
            match obs_rx
                .recv()
                .expect("retry workers exited before reporting their first observation")
            {
                RetryObservation::FirstTypedErr(thread) => {
                    first_typed_err_by_thread[thread] = true;
                }
                RetryObservation::SucceededBeforeTypedErr { thread, attempts } => {
                    done.wait();
                    for h in handles {
                        let _ = h.join();
                    }
                    let _ = fire.join();
                    panic!(
                        "thread {thread} succeeded after {attempts} attempts before observing CallbackActiveCrossThread{{Trigger}}"
                    );
                }
                RetryObservation::UnexpectedErr { thread, error } => {
                    done.wait();
                    for h in handles {
                        let _ = h.join();
                    }
                    let _ = fire.join();
                    panic!("thread {thread} returned unexpected retry error before typed contention observation: {error}");
                }
                RetryObservation::RetryLimitExceeded { thread, attempts } => {
                    done.wait();
                    for h in handles {
                        let _ = h.join();
                    }
                    let _ = fire.join();
                    panic!("thread {thread} ran {attempts} retries before all threads observed typed contention");
                }
            }
        }
        done.wait();

        for h in handles {
            h.join().unwrap().unwrap();
        }
        fire.join().unwrap().unwrap();

        let attempts = attempts_per_thread.lock().unwrap().clone();
        for (i, count) in attempts.iter().enumerate() {
            assert!(*count <= 50, "thread {i} ran {count} retries; cap is 50");
        }
        // Contention proof — every thread must have observed contention at
        // least once. With 8 threads × a single 50ms-parked callback × backoff,
        // every thread that wins past attempt 1 has seen the typed-Err arm at
        // least once before that. The stricter "all >= 2" form catches a
        // partial-no-op regression where one thread happened to win on attempt
        // 1 (which "any > 1" would silently accept).
        assert!(
            attempts.iter().all(|c| *c >= 2),
            "every thread must retry at least once: {:?}",
            attempts
        );
        // Typed-Err proof — the contention path returned the typed variant at
        // least once. A no-op gate would never produce the typed Err.
        assert!(
            observed_typed_err_count.load(AtomicOrdering::SeqCst) >= 1,
            "no thread observed CallbackActiveCrossThread{{Trigger}} — typed-Err contract not exercised"
        );
    })
    .expect("t31 deadlocked");
}

// ============================================================================
// t32_priority_a1_over_a2 — RED — source order: A1 > A2
// ============================================================================
#[test]
#[serial]
fn t32_priority_a1_over_a2() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        // Register a trigger callback. It will be active when we then call begin
        // from inside a cron callback (which sets CRON_CALLBACK_ACTIVE on top of
        // TRIGGER_CALLBACK_ACTIVE on the same thread). To exercise priority we
        // pre-set CRON_CALLBACK_ACTIVE through a nested cron callback that fires
        // a trigger insert which itself hosts the begin() probe.
        //
        // Setup: a cron callback writes through its tx-bound handle to a table
        // that has a trigger registered. Inside the trigger callback (thread is
        // both CRON_CALLBACK_ACTIVE and TRIGGER_CALLBACK_ACTIVE), call db.begin()
        // and observe which Class A variant returns.
        let observed: Arc<Mutex<Option<Result<contextdb_core::TxId>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_outer = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            let attempt = db_outer.begin();
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        db.register_cron_callback("cb", move |db_handle| {
            // Insert through the cron's tx-bound handle. This fires the trigger,
            // whose callback runs synchronously on this same thread — A1 (cron
            // active) AND A2 (trigger active) are both true at that moment.
            db_handle.execute(
                "INSERT INTO t (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xC32)))]),
            )?;
            Ok(())
        })
        .unwrap();
        fire_cron_now(&db);

        let observed = observed.lock().unwrap().take().expect("trigger ran");
        // Source order: A1 (cron) is checked before A2 (trigger).
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Cron
                })
            ),
            "expected A1 priority (Cron) over A2 (Trigger), got {:?}",
            observed
        );
    })
    .expect("t32 deadlocked");
}

// ============================================================================
// t32b_priority_a2_over_b1_and_b2 — RED — source order: A2 > B1 > B2
// Covers the middle priority pair from the contract's A1 > A2 > B1 > B2
// source order. T1 is inside a trigger callback (A2 set), while another
// thread holds either a cron callback parked (B1 visible from T1) or a trigger
// callback parked (B2 visible from T1). T1's begin() must return
// CallbackReentry{Trigger} in both probes (A2 wins).
// ============================================================================
#[test]
#[serial]
fn t32b_priority_a2_over_b1_and_b2_when_trigger_active_and_other_callback_parked() {
    run_with_timeout(|| {
        let begin_inside_trigger = |probe_db: Arc<Database>, key: u128| {
            let db_x = Arc::new(Database::open_memory());
            db_x.execute("CREATE TABLE x (id UUID PRIMARY KEY)", &empty())
                .unwrap();
            db_x.execute("CREATE TRIGGER tr_x ON x WHEN INSERT", &empty())
                .unwrap();
            let observed: Arc<Mutex<Option<Result<contextdb_core::TxId>>>> =
                Arc::new(Mutex::new(None));
            let observed_cb = observed.clone();
            db_x.register_trigger_callback("tr_x", move |_db_handle, _ctx| {
                let attempt = probe_db.begin();
                *observed_cb.lock().unwrap() = Some(attempt);
                Ok(())
            })
            .unwrap();
            db_x.complete_initialization().unwrap();
            db_x.execute(
                "INSERT INTO x (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(key)))]),
            )
            .unwrap();
            observed.lock().unwrap().take().expect("trigger ran")
        };

        // B1 probe: db_y has a parked cron callback on another thread. T1's
        // db_x trigger callback probes db_y.begin(), so A2 and B1 are both
        // true. A2 must win over B1.
        let entered_cron = Arc::new(Barrier::new(2));
        let done_cron = Arc::new(Barrier::new(2));
        let (db_y_cron, _pause) = build_cron_parked_db(entered_cron.clone(), done_cron.clone());
        let fire_cron = dispatch_cron_now_in_thread(db_y_cron.clone());
        entered_cron.wait();
        let observed_b1 = begin_inside_trigger(db_y_cron.clone(), 0xA32B1);
        done_cron.wait();
        fire_cron.join().unwrap();
        assert!(
            matches!(
                observed_b1,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected A2 priority (Trigger reentry) over B1 (cross-thread Cron), got {:?}",
            observed_b1
        );

        // B2 probe: db_y has a parked trigger callback on another thread.
        // T1's db_x trigger callback probes db_y.begin(), so A2 and B2 are
        // both true. A2 must win over B2.
        let entered_y = Arc::new(Barrier::new(2));
        let done_y = Arc::new(Barrier::new(2));
        let db_y = build_trigger_parked_db(entered_y.clone(), done_y.clone());

        // T2: drive an INSERT on db_y so its callback parks. Wait until parked.
        let fire_y = fire_trigger_in_thread(db_y.clone(), 0xC32B);
        entered_y.wait();
        // Now B2 is true on db_y (from any other thread's perspective).

        let observed_b2 = begin_inside_trigger(db_y.clone(), 0xA32B2);
        done_y.wait();
        fire_y.join().unwrap().unwrap();

        assert!(
            matches!(
                observed_b2,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected A2 priority (Trigger reentry) over B2 (cross-thread Trigger), got {:?}",
            observed_b2
        );
    })
    .expect("t32b deadlocked");
}

// ============================================================================
// t32c_priority_b1_over_b2 — RED — source order: B1 > B2
// T1 holds a parked cron callback; T2 holds a parked trigger callback;
// T3 calls db.begin() — sees BOTH B1 and B2 cross-thread, must return
// the cron variant (source order B1 before B2).
// ============================================================================
#[test]
#[serial]
fn t32c_priority_b1_over_b2_when_cron_and_trigger_both_parked_cross_thread() {
    run_with_timeout(|| {
        let entered_cron = Arc::new(Barrier::new(2));
        let done_cron = Arc::new(Barrier::new(2));
        let entered_trigger = Arc::new(Barrier::new(2));
        let done_trigger = Arc::new(Barrier::new(2));

        // One Database hosts the parked cron callback while another hosts the
        // parked trigger callback. A single Database cannot park the trigger
        // after the cron callback is already active because trigger firing also
        // begins a transaction and would hit B1 before entering the callback.
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();

        let trigger_db = Arc::new(Database::open_memory());
        trigger_db
            .execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        trigger_db
            .execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        // Cron callback parks at the cron barriers.
        let entered_cron_cb = entered_cron.clone();
        let done_cron_cb = done_cron.clone();
        db.register_cron_callback("cb", move |_db_handle| {
            entered_cron_cb.wait();
            done_cron_cb.wait();
            Ok(())
        })
        .unwrap();
        // Trigger callback parks at the trigger barriers.
        let entered_trigger_cb = entered_trigger.clone();
        let done_trigger_cb = done_trigger.clone();
        trigger_db
            .register_trigger_callback("tr", move |_db, _ctx| {
                entered_trigger_cb.wait();
                done_trigger_cb.wait();
                Ok(())
            })
            .unwrap();
        trigger_db.complete_initialization().unwrap();

        // T1: dispatch the cron tick. Wait until parked.
        let cron_dispatch = dispatch_cron_now_in_thread(db.clone());
        entered_cron.wait();
        // T2: fire the trigger. Wait until parked.
        let trigger_fire = fire_trigger_in_thread(trigger_db.clone(), 0xC32C);
        entered_trigger.wait();

        // T3 (this thread): both B1 and B2 are true. begin() must return
        // CallbackActiveCrossThread{Cron} (B1 before B2 in source order).
        let result = db.begin();

        // Unblock both parked callbacks.
        done_cron.wait();
        done_trigger.wait();
        let _ = cron_dispatch.join();
        trigger_fire.join().unwrap().unwrap();

        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Cron
                })
            ),
            "expected B1 priority (Cron) over B2 (Trigger) by source order, got {:?}",
            result
        );
    })
    .expect("t32c deadlocked");
}

// ============================================================================
// t32d_priority_source_order_is_symmetric_across_all_tx_control_surfaces — RED
// The focused t32/t32b/t32c probes are intentionally readable begin() examples.
// This matrix blocks a shallow implementation that wires the priority order only
// for begin() while leaving commit/rollback/apply_changes/close asymmetric.
// ============================================================================
#[test]
#[serial]
fn t32d_priority_source_order_is_symmetric_across_all_tx_control_surfaces() {
    run_with_timeout(|| {
        let mut failures = Vec::new();
        for (i, surface) in TxControlSurface::ALL.iter().copied().enumerate() {
            let base = 0xD320 + (i as u128) * 0x100;

            // A1>A2: a cron callback writes through its tx-bound handle, firing
            // a trigger synchronously on the same thread. The probe surface runs
            // inside the trigger callback while both Class A thread-locals are set.
            let db = Arc::new(Database::open_memory());
            let _pause = db.pause_cron_tickler_for_test();
            db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
                .unwrap();
            db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
                .unwrap();
            let probe = Arc::new(prepare_tx_surface(&db, surface, base));
            let observed: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
            let observed_cb = observed.clone();
            let db_outer = db.clone();
            let probe_cb = probe.clone();
            db.register_trigger_callback("tr", move |_db, _ctx| {
                let attempt = run_tx_surface(&db_outer, &probe_cb, base + 1);
                *observed_cb.lock().unwrap() = Some(attempt);
                Ok(())
            })
            .unwrap();
            db.complete_initialization().unwrap();
            db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
                .unwrap();
            db.register_cron_callback("cb", move |db_handle| {
                db_handle.execute(
                    "INSERT INTO t (id) VALUES ($id)",
                    &HashMap::from([("id".to_string(), Value::Uuid(uuid(base + 2)))]),
                )?;
                Ok(())
            })
            .unwrap();
            fire_cron_now(&db);
            let result = observed.lock().unwrap().take().expect("trigger ran");
            cleanup_tx_surface(&db, &probe);
            if !is_reentry_kind(&result, CallbackKind::Cron) {
                failures.push(format!(
                    "{} A1>A2 expected CallbackReentry{{Cron}}, got {result:?}",
                    surface.label()
                ));
            }

            // A2>B1: the current thread is inside a trigger callback while the
            // target database has a cron callback parked on another thread.
            let entered_cron = Arc::new(Barrier::new(2));
            let done_cron = Arc::new(Barrier::new(2));
            let (db_y_cron, _pause) = build_cron_parked_db(entered_cron.clone(), done_cron.clone());
            let probe = Arc::new(prepare_tx_surface(&db_y_cron, surface, base + 0x10));
            let fire_cron = dispatch_cron_now_in_thread(db_y_cron.clone());
            entered_cron.wait();
            let result =
                observe_surface_inside_fresh_trigger(db_y_cron.clone(), probe.clone(), base + 0x11);
            done_cron.wait();
            let _ = fire_cron.join();
            cleanup_tx_surface(&db_y_cron, &probe);
            if !is_reentry_kind(&result, CallbackKind::Trigger) {
                failures.push(format!(
                    "{} A2>B1 expected CallbackReentry{{Trigger}}, got {result:?}",
                    surface.label()
                ));
            }

            // A2>B2: the current thread is inside one trigger callback while the
            // target database has another trigger callback parked on a different
            // thread.
            let entered_trigger = Arc::new(Barrier::new(2));
            let done_trigger = Arc::new(Barrier::new(2));
            let db_y = build_trigger_parked_db(entered_trigger.clone(), done_trigger.clone());
            let probe = Arc::new(prepare_tx_surface(&db_y, surface, base + 0x20));
            let fire_trigger = fire_trigger_in_thread(db_y.clone(), base + 0x21);
            entered_trigger.wait();
            let result =
                observe_surface_inside_fresh_trigger(db_y.clone(), probe.clone(), base + 0x22);
            done_trigger.wait();
            fire_trigger.join().unwrap().unwrap();
            cleanup_tx_surface(&db_y, &probe);
            if !is_reentry_kind(&result, CallbackKind::Trigger) {
                failures.push(format!(
                    "{} A2>B2 expected CallbackReentry{{Trigger}}, got {result:?}",
                    surface.label()
                ));
            }

            // B1>B2: probe from a third thread so the target database sees both
            // its own parked cron callback and the global trigger callback owner.
            let entered_cron = Arc::new(Barrier::new(2));
            let done_cron = Arc::new(Barrier::new(2));
            let entered_trigger = Arc::new(Barrier::new(2));
            let done_trigger = Arc::new(Barrier::new(2));
            let (db_b1, _pause) = build_cron_parked_db(entered_cron.clone(), done_cron.clone());
            let probe = Arc::new(prepare_tx_surface(&db_b1, surface, base + 0x30));
            let trigger_db = build_trigger_parked_db(entered_trigger.clone(), done_trigger.clone());
            let cron_dispatch = dispatch_cron_now_in_thread(db_b1.clone());
            entered_cron.wait();
            let trigger_fire = fire_trigger_in_thread(trigger_db.clone(), base + 0x31);
            entered_trigger.wait();
            let db_probe = db_b1.clone();
            let probe_for_thread = probe.clone();
            let result =
                thread::spawn(move || run_tx_surface(&db_probe, &probe_for_thread, base + 0x32))
                    .join()
                    .unwrap();
            done_cron.wait();
            done_trigger.wait();
            let _ = cron_dispatch.join();
            trigger_fire.join().unwrap().unwrap();
            cleanup_tx_surface(&db_b1, &probe);
            if !is_cross_thread_kind(&result, CallbackKind::Cron) {
                failures.push(format!(
                    "{} B1>B2 expected CallbackActiveCrossThread{{Cron}}, got {result:?}",
                    surface.label()
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "source-order priority must be symmetric across every tx-control surface:\n{}",
            failures.join("\n")
        );
    })
    .expect("t32d deadlocked");
}

// ============================================================================
// Priority-pair coverage note
// t32/t32b/t32c provide simple begin() probes for the adjacent source-order
// pairs. t32d repeats the same priority matrix across begin/commit/rollback/
// apply_changes/close so the product contract cannot regress on non-begin
// surfaces.
// ============================================================================

// ============================================================================
// t33_close_then_begin_returns_closed_database — REGRESSION GUARD
// ============================================================================
#[test]
#[serial]
fn t33_close_then_begin_returns_closed_database() {
    let db = Arc::new(Database::open_memory());
    db.close().expect("close");
    let result = db.begin();
    assert!(
        matches!(
            &result,
            Err(Error::Other(message)) if message == "database handle is closed"
        ),
        "begin() after close() must return the closed-database error, got {:?}",
        result
    );
}

// ============================================================================
// t33b_closed_database_wins_over_trigger_contention_for_open_operations
// REGRESSION GUARD. Pins the open_operation() precedence used by begin/commit/
// rollback/apply_changes: a closed handle must not be hidden by global trigger
// contention from another thread. Cron B1 is per-database rather than global,
// so a pre-closed handle cannot also own a running cron callback through public
// API; this test additionally pins that an attempted close under B1 does not set
// the closed flag before returning its callback-active error.
// ============================================================================
#[test]
#[serial]
fn t33b_closed_database_wins_over_trigger_contention_for_open_operations() {
    run_with_timeout(|| {
        let closed_inside_cron: Arc<Mutex<Vec<ClosedSurfaceResult>>> =
            Arc::new(Mutex::new(Vec::new()));
        let closed_inside_cron_cb = closed_inside_cron.clone();
        let closed_cron_probe = Arc::new(Database::open_memory());
        closed_cron_probe.close().expect("close cron probe handle");
        let active_cron_db = Arc::new(Database::open_memory());
        let _active_cron_pause = active_cron_db.pause_cron_tickler_for_test();
        active_cron_db
            .execute("CREATE SCHEDULE closed_cron EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        let closed_cron_probe_cb = closed_cron_probe.clone();
        active_cron_db
            .register_cron_callback("cb", move |_db_handle| {
                let mut results = closed_inside_cron_cb.lock().unwrap();
                for (i, surface) in TxControlSurface::OPEN_OPERATION_ONLY
                    .iter()
                    .copied()
                    .enumerate()
                {
                    results.push((
                        surface.label(),
                        run_closed_tx_surface(&closed_cron_probe_cb, surface, 0xC33C00 + i as u128),
                    ));
                }
                Ok(())
            })
            .unwrap();
        fire_cron_now(&active_cron_db);
        let cron_results = std::mem::take(&mut *closed_inside_cron.lock().unwrap());
        assert_eq!(
            cron_results.len(),
            TxControlSurface::OPEN_OPERATION_ONLY.len(),
            "closed cron callback must probe every open-operation surface"
        );
        for (surface, result) in cron_results {
            assert!(
                matches!(
                    &result,
                    Err(Error::Other(message)) if message == "database handle is closed"
                ),
                "{surface} on a closed handle inside cron callback must return the exact closed-database error, got {:?}",
                result
            );
        }

        let closed_inside_trigger: Arc<Mutex<Vec<ClosedSurfaceResult>>> =
            Arc::new(Mutex::new(Vec::new()));
        let closed_inside_trigger_cb = closed_inside_trigger.clone();
        let closed_trigger_probe = Arc::new(Database::open_memory());
        closed_trigger_probe
            .close()
            .expect("close trigger probe handle");
        let active_trigger_db = Arc::new(Database::open_memory());
        active_trigger_db
            .execute("CREATE TABLE closed_t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        active_trigger_db
            .execute("CREATE TRIGGER closed_tr ON closed_t WHEN INSERT", &empty())
            .unwrap();
        let closed_trigger_probe_cb = closed_trigger_probe.clone();
        active_trigger_db
            .register_trigger_callback("closed_tr", move |_db, _ctx| {
                let mut results = closed_inside_trigger_cb.lock().unwrap();
                for (i, surface) in TxControlSurface::OPEN_OPERATION_ONLY
                    .iter()
                    .copied()
                    .enumerate()
                {
                    results.push((
                        surface.label(),
                        run_closed_tx_surface(&closed_trigger_probe_cb, surface, 0xC33D00 + i as u128),
                    ));
                }
                Ok(())
            })
            .unwrap();
        active_trigger_db.complete_initialization().unwrap();
        active_trigger_db
            .execute(
                "INSERT INTO closed_t (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xC33D)))]),
            )
            .unwrap();
        let trigger_results = std::mem::take(&mut *closed_inside_trigger.lock().unwrap());
        assert_eq!(
            trigger_results.len(),
            TxControlSurface::OPEN_OPERATION_ONLY.len(),
            "closed trigger callback must probe every open-operation surface"
        );
        for (surface, result) in trigger_results {
            assert!(
                matches!(
                    &result,
                    Err(Error::Other(message)) if message == "database handle is closed"
                ),
                "{surface} on a closed handle inside trigger callback must return the exact closed-database error, got {:?}",
                result
            );
        }

        let entered_cron = Arc::new(Barrier::new(2));
        let done_cron = Arc::new(Barrier::new(2));
        let (cron_db, _pause) = build_cron_parked_db(entered_cron.clone(), done_cron.clone());
        let cron_dispatch = dispatch_cron_now_in_thread(cron_db.clone());
        entered_cron.wait();
        let close_under_b1 = cron_db.close();
        assert!(
            close_under_b1.is_err(),
            "close() under B1 must return an error before doing close side effects"
        );
        done_cron.wait();
        let _ = cron_dispatch.join();
        assert_handle_still_open(&cron_db, "close under B1 precedence");

        let entered_trigger = Arc::new(Barrier::new(2));
        let done_trigger = Arc::new(Barrier::new(2));
        let trigger_db = build_trigger_parked_db(entered_trigger.clone(), done_trigger.clone());
        let fire_trigger = fire_trigger_in_thread(trigger_db.clone(), 0xC33B);
        entered_trigger.wait();

        for (i, surface) in TxControlSurface::OPEN_OPERATION_ONLY
            .iter()
            .copied()
            .enumerate()
        {
            let closed_db = Arc::new(Database::open_memory());
            closed_db.close().expect("close");
            let db_probe = closed_db.clone();
            let result =
                thread::spawn(move || run_closed_tx_surface(&db_probe, surface, 0xC33B00 + i as u128))
                    .join()
                    .unwrap();
            assert!(
                matches!(
                    &result,
                    Err(Error::Other(message)) if message == "database handle is closed"
                ),
                "{} on a closed handle under trigger contention must return the exact closed-database error, got {:?}",
                surface.label(),
                result
            );
        }

        done_trigger.wait();
        fire_trigger.join().unwrap().unwrap();
    })
    .expect("t33b deadlocked");
}

// ============================================================================
// t33c_close_active_operation_wins_over_cross_thread_callback_contention
// REGRESSION GUARD. `close()` intentionally checks A1/A2 first, then active
// operation, then B1/B2. These probes pin the middle precedence so a close()
// invoked from a plugin hook cannot hide "active operation" behind cross-thread
// callback contention.
// ============================================================================
#[test]
#[serial]
fn t33c_close_active_operation_wins_over_cross_thread_callback_contention() {
    run_with_timeout(|| {
        // B1 overlap: the probed database owns a parked cron callback, while a
        // third thread executes a read query. The plugin hook calls close()
        // while execute() has the operation stack populated.
        let (db, plugin) = plugin_db_with_read_table();
        let _pause = db.pause_cron_tickler_for_test();
        let entered_cron = Arc::new(Barrier::new(2));
        let done_cron = Arc::new(Barrier::new(2));
        db.execute("CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)", &empty())
            .unwrap();
        let entered_cron_cb = entered_cron.clone();
        let done_cron_cb = done_cron.clone();
        db.register_cron_callback("cb", move |_db_handle| {
            entered_cron_cb.wait();
            done_cron_cb.wait();
            Ok(())
        })
        .unwrap();
        let cron_dispatch = dispatch_cron_now_in_thread(db.clone());
        entered_cron.wait();
        plugin.arm();
        let db_probe = db.clone();
        let plugin_probe = plugin.clone();
        let observed_b1 = thread::spawn(move || {
            let _ = db_probe.execute("SELECT * FROM read_probe", &empty());
            plugin_probe.take_observed()
        })
        .join()
        .unwrap()
        .expect("plugin close result under B1");
        done_cron.wait();
        let _ = cron_dispatch.join();
        assert!(
            matches!(
                &observed_b1,
                Err(Error::Other(message))
                    if message == "cannot close database from inside an active operation"
            ),
            "close() inside an active operation under B1 must return the active-operation error, got {:?}",
            observed_b1
        );

        // B2 overlap: a separate trigger callback is parked globally. The probe
        // runs on a non-owner thread so the target database sees global trigger
        // contention as well as its active operation stack.
        let (db, plugin) = plugin_db_with_read_table();
        let entered_trigger = Arc::new(Barrier::new(2));
        let done_trigger = Arc::new(Barrier::new(2));
        let trigger_db = build_trigger_parked_db(entered_trigger.clone(), done_trigger.clone());
        let fire_trigger = fire_trigger_in_thread(trigger_db.clone(), 0xC33C);
        entered_trigger.wait();
        plugin.arm();
        let db_probe = db.clone();
        let plugin_probe = plugin.clone();
        let observed_b2 = thread::spawn(move || {
            let _ = db_probe.execute("SELECT * FROM read_probe", &empty());
            plugin_probe.take_observed()
        })
        .join()
        .unwrap()
        .expect("plugin close result under B2");
        done_trigger.wait();
        fire_trigger.join().unwrap().unwrap();
        assert!(
            matches!(
                &observed_b2,
                Err(Error::Other(message))
                    if message == "cannot close database from inside an active operation"
            ),
            "close() inside an active operation under B2 must return the active-operation error, got {:?}",
            observed_b2
        );
    })
    .expect("t33c deadlocked");
}

// ============================================================================
// t34_drop_with_active_callback_does_not_panic — REGRESSION GUARD
// Drops the outer Arc AFTER the callback has exited. Adds a positive Drop-
// effect control: a no-op Drop would also satisfy "no panic"; the positive
// control proves Drop actually performs work (releases global state) by
// requiring a fresh Database::open_memory() after the drop succeeds — a
// regression that left a global lock or thread alive would fail this.
// ============================================================================
#[test]
#[serial]
fn t34_drop_with_active_callback_does_not_panic() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        let fire = fire_trigger_in_thread(db.clone(), 0xC34);
        entered.wait();
        // Drop the local Arc reference. Other clones (the callback closure
        // capturing db_for_cb in earlier tests; not here — we only drop the
        // outer Arc) should not trigger a panic in Drop. The dispatch thread
        // still owns a clone via fire.
        done.wait();
        fire.join().unwrap().unwrap();
        // Now drop the outer reference explicitly. Drop must not panic.
        drop(db);
        // Positive Drop-effect control — fresh open_memory must succeed and
        // execute a happy-path begin/commit. A regression where Drop leaked
        // a global lock or stranded a tickler thread would deadlock or fail
        // here; a no-op Drop replacement would also fail because the prior
        // resources would still be held.
        let fresh = Arc::new(Database::open_memory());
        let tx = fresh
            .begin()
            .expect("fresh open_memory must succeed post-drop");
        fresh.rollback(tx).expect("fresh rollback must succeed");
    })
    .expect("t34 deadlocked");
}

// ============================================================================
// t34b_database_drop_while_callback_parked_does_not_panic — REGRESSION GUARD
// Drops the outer Arc WHILE the callback is STILL parked at the barrier.
// The dispatch thread holds another Arc clone (via `fire`'s captured db),
// so the inner Database is not deallocated until the callback exits — but
// the outer Arc's drop is exercised concurrently with the callback's runner.
// Pins that no Drop wrapper takes a lock that contends with the callback's
// runner thread.
// ============================================================================
#[test]
#[serial]
fn t34b_database_drop_while_callback_parked_does_not_panic() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        // Spawn the firing thread; it holds its own Arc clone via fire_trigger_in_thread's
        // `move || { ... }`, so the inner Database survives the outer drop below.
        let fire = fire_trigger_in_thread(db.clone(), 0xC34B);
        entered.wait();
        // Callback is parked. Drop the outer Arc NOW, while the callback body is
        // still active on the firing thread. The inner Database survives because
        // the dispatch thread (firing closure) holds a clone, but Arc-decrement
        // semantics on the outer reference must not panic.
        drop(db);
        // Allow the parked callback to exit, then verify the firing thread's
        // commit completed cleanly (no panic propagated).
        done.wait();
        fire.join().unwrap().unwrap();
        // Positive Drop-effect control — once the inner Database is finally
        // deallocated (after `fire.join` released the last clone), a fresh
        // open_memory must succeed. A no-op Drop replacement would leave the
        // inner instance alive (no resource release), but more importantly a
        // Drop wrapper that took a write-lock on shutdown would deadlock here.
        let fresh = Arc::new(Database::open_memory());
        let tx = fresh
            .begin()
            .expect("fresh open_memory must succeed post-drop");
        fresh.rollback(tx).expect("fresh rollback must succeed");
    })
    .expect("t34b deadlocked");
}

// ============================================================================
// t35_tx_bound_handle_writes_inside_callback_succeed — REGRESSION GUARD
// ============================================================================
#[test]
#[serial]
fn t35_tx_bound_handle_writes_inside_callback_succeed() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE host (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute(
            "CREATE TABLE audit (id UUID PRIMARY KEY, host_id UUID)",
            &empty(),
        )
        .unwrap();
        db.execute("CREATE TRIGGER tr ON host WHEN INSERT", &empty())
            .unwrap();
        db.register_trigger_callback("tr", move |db_handle, ctx| {
            // Writes through the tx-bound handle MUST succeed (firing tx).
            let host_id = ctx
                .row_values
                .get("id")
                .cloned()
                .unwrap_or(Value::Uuid(uuid(0xA35A)));
            db_handle.insert_row(
                ctx.tx,
                "audit",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(0xA35B))),
                    ("host_id".to_string(), host_id),
                ]),
            )?;
            // begin() through the tx-bound handle must FAIL with CallbackReentry{Trigger}.
            let begin_attempt = db_handle.begin();
            assert!(
                matches!(
                    begin_attempt,
                    Err(Error::CallbackReentry {
                        kind: CallbackKind::Trigger
                    })
                ),
                "tx-bound handle's begin() must reject with CallbackReentry{{Trigger}}; got {:?}",
                begin_attempt
            );
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO host (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA35C)))]),
        )
        .unwrap();
        // Audit must contain exactly 1 row — the tx-bound write succeeded.
        let snap = db.snapshot();
        assert_eq!(db.scan("audit", snap).unwrap().len(), 1);
        assert_eq!(db.scan("host", snap).unwrap().len(), 1);
    })
    .expect("t35 deadlocked");
}

// ============================================================================
// t36_callback_uses_outer_handle_outer_commit_fails — RED
// ============================================================================
#[test]
#[serial]
fn t36_callback_uses_outer_handle_outer_commit_fails() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let db_outer = db.clone();
        db.register_trigger_callback("tr", move |_db_handle, _ctx| {
            // Outer-handle begin(): misuse. Returns Err but the callback Ok-returns.
            let _ = db_outer.begin();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        // Drive an outer tx, INSERT, then commit — commit dispatches the trigger,
        // whose callback misuses begin() on the outer handle. Commit must surface
        // CallbackReentry{Trigger} and the outer tx must NOT be committed.
        let tx = db.begin().expect("setup begin");
        db.execute_in_tx(
            tx,
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA36)))]),
        )
        .unwrap();
        let commit_result = db.commit(tx);
        assert!(
            matches!(
                commit_result,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "outer commit must surface CallbackReentry{{Trigger}}; got {:?}",
            commit_result
        );
        // Outer tx must not be committed: the row is absent from a fresh snapshot.
        let snap = db.snapshot();
        assert_eq!(db.scan("t", snap).unwrap().len(), 0);
    })
    .expect("t36 deadlocked");
}

// ============================================================================
// t36b_reentry_retry_never_helps_inside_trigger_callback — RED
// CallbackReentry is misuse (Class A) — retrying inside the same callback body
// must NOT make progress. Pins that Class A is not retry-safe and that no
// future regression silently downgrades A2 to B2 (which would be retry-safe)
// mid-callback. All 5 attempts return CallbackReentry{Trigger}.
// ============================================================================
#[test]
#[serial]
fn t36b_reentry_retry_never_helps_inside_trigger_callback() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let observed: Arc<Mutex<Vec<Result<contextdb_core::TxId>>>> =
            Arc::new(Mutex::new(Vec::with_capacity(5)));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            let mut results = observed_cb.lock().unwrap();
            for _ in 0..5 {
                // No sleep between attempts — pin that retrying inside the
                // callback body is structurally unable to make progress.
                results.push(db_for_cb.begin());
            }
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA36B)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 5, "callback must have run all 5 attempts");
        for (i, attempt) in observed.iter().enumerate() {
            assert!(
                matches!(
                    attempt,
                    Err(Error::CallbackReentry { kind: CallbackKind::Trigger })
                ),
                "attempt {i} returned {:?} — every attempt inside a trigger callback must be CallbackReentry{{Trigger}} (no retry can help)",
                attempt
            );
        }
    })
    .expect("t36b deadlocked");
}

// ============================================================================
// t37_no_engine_logging_on_typed_err_production_path — RED
// Drives the engine production path that returns the typed Err across all five
// tx-control surfaces, counts engine-emitted tracing events PROCESS-WIDE, and
// asserts zero events were produced by the engine. A positive-control event
// emitted from inside the test (NOT from the engine) proves the subscriber
// pipeline is live — without it a misconfigured subscriber would let the
// assertion pass vacuously.
//
// Process-wide capture (via `set_global_default`) is required because the
// retry threads cross thread boundaries and `subscriber::with_default` is
// thread-local. The global subscriber is installed exactly once for the
// test-binary lifetime via OnceLock; counting is gated by an AtomicBool so
// other tests do not contribute events.
// ============================================================================
use std::sync::OnceLock;

static T37_TRACING_COUNT: AtomicUsize = AtomicUsize::new(0);
static T37_COUNT_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static T37_SUBSCRIBER_INIT: OnceLock<()> = OnceLock::new();

fn t37_install_global_subscriber() {
    T37_SUBSCRIBER_INIT.get_or_init(|| {
        use tracing::subscriber::set_global_default;
        use tracing_subscriber::Registry;
        use tracing_subscriber::layer::SubscriberExt;

        struct CountingLayer;
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountingLayer {
            fn on_event(
                &self,
                _event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if T37_COUNT_ENABLED.load(AtomicOrdering::SeqCst) {
                    T37_TRACING_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
                }
            }
        }
        let _ = set_global_default(Registry::default().with(CountingLayer));
    });
}

#[test]
#[serial]
fn t37_no_engine_logging_on_typed_err_production_path() {
    t37_install_global_subscriber();

    run_with_timeout(|| {
        // Drive B2 on every tx-control surface. Behavior contract §17 forbids
        // tracing emission on typed-Err construction for the whole surface
        // family, not only begin().
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let entered_cb = entered.clone();
        let done_cb = done.clone();
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            entered_cb.wait();
            thread::sleep(Duration::from_millis(50));
            done_cb.wait();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        let commit_tx = db.begin().expect("setup commit tx");
        let rollback_tx = db.begin().expect("setup rollback tx");

        let fire = fire_trigger_in_thread(db.clone(), 0xC37);
        entered.wait();

        // Reset and enable global counting AFTER setup is complete so setup
        // events (DDL, trigger registration) do not contribute.
        T37_TRACING_COUNT.store(0, AtomicOrdering::SeqCst);
        T37_COUNT_ENABLED.store(true, AtomicOrdering::SeqCst);

        let mut typed_err_seen = 0usize;
        let mut failures = Vec::new();
        let mut cleanup_txs = Vec::new();

        match db.begin() {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => typed_err_seen += 1,
            Ok(tx) => {
                cleanup_txs.push(tx);
                failures.push("begin returned Ok under B2".to_string());
            }
            Err(other) => failures.push(format!("begin returned unexpected {other:?}")),
        }
        match db.commit(commit_tx) {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => typed_err_seen += 1,
            Ok(()) => failures.push("commit returned Ok under B2".to_string()),
            Err(other) => failures.push(format!("commit returned unexpected {other:?}")),
        }
        match db.rollback(rollback_tx) {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => typed_err_seen += 1,
            Ok(()) => failures.push("rollback returned Ok under B2".to_string()),
            Err(other) => failures.push(format!("rollback returned unexpected {other:?}")),
        }
        match db.apply_changes(
            ChangeSet::default(),
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        ) {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => typed_err_seen += 1,
            Ok(result) => failures.push(format!("apply_changes returned Ok({result:?}) under B2")),
            Err(other) => failures.push(format!("apply_changes returned unexpected {other:?}")),
        }
        match db.close() {
            Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            }) => typed_err_seen += 1,
            Ok(()) => failures.push("close returned Ok under B2".to_string()),
            Err(other) => failures.push(format!("close returned unexpected {other:?}")),
        }

        // Snapshot the engine-attributed event count BEFORE emitting the
        // positive-control event so we can attribute the two cleanly.
        let engine_events = T37_TRACING_COUNT.load(AtomicOrdering::SeqCst);

        // Positive control — proves the global subscriber pipeline is live.
        // Without this, a misconfigured subscriber would silently let the
        // engine_events == 0 assertion pass vacuously.
        tracing::error!("positive_control_event");
        let after_control = T37_TRACING_COUNT.load(AtomicOrdering::SeqCst);
        T37_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);

        done.wait();
        fire.join().unwrap().unwrap();
        for tx in cleanup_txs {
            let _ = db.rollback(tx);
        }
        let _ = db.rollback(commit_tx);
        let _ = db.rollback(rollback_tx);

        // Must have actually exercised the production typed-Err path.
        assert!(
            failures.is_empty(),
            "typed-Err precondition failed across tx-control surfaces: {failures:?}"
        );
        assert_eq!(
            typed_err_seen, 5,
            "expected one typed B2 error from each tx-control surface"
        );
        assert!(
            after_control > engine_events,
            "positive-control tracing event was not captured — subscriber wiring is misconfigured; the engine_events == 0 assertion below is therefore not load-bearing (engine_events={engine_events}, after_control={after_control})"
        );
        assert_eq!(
            engine_events,
            0,
            "engine emitted {engine_events} tracing event(s) on the typed-Err production path across begin/commit/rollback/apply_changes/close; behavior contract §17 forbids any engine logging on Error construction. typed_err_seen={typed_err_seen}"
        );
    })
    .expect("t37 deadlocked");
}

// ============================================================================
// t38_multi_handle_same_thread_reentry — RED
// ============================================================================
#[test]
#[serial]
fn t38_multi_handle_same_thread_reentry() {
    run_with_timeout(|| {
        let db_x = Arc::new(Database::open_memory());
        db_x.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db_x.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        // db_y is a different Database instance.
        let db_y = Arc::new(Database::open_memory());
        let observed: Arc<Mutex<Option<Result<contextdb_core::TxId>>>> = Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_y_for_cb = db_y.clone();
        db_x.register_trigger_callback("tr", move |_db_x_handle, _ctx| {
            // Inside the callback registered on db_x, call begin() on db_y.
            let attempt = db_y_for_cb.begin();
            *observed_cb.lock().unwrap() = Some(attempt);
            Ok(())
        })
        .unwrap();
        db_x.complete_initialization().unwrap();
        db_x.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA38)))]),
        )
        .unwrap();
        let observed = observed.lock().unwrap().take().expect("callback ran");
        assert!(
            matches!(
                observed,
                Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger
                })
            ),
            "expected CallbackReentry{{Trigger}} on cross-handle, got {:?}",
            observed
        );
    })
    .expect("t38 deadlocked");
}

// ============================================================================
// t39_cron_tickler_resilience_after_callback_active — RED
// ============================================================================
#[test]
#[serial]
fn t39_cron_tickler_resilience_after_callback_active() {
    use contextdb_engine::CronAuditKind;
    run_with_timeout(|| {
        // Park a trigger callback on a user thread; cron tickler concurrent
        // tick attempts to begin() and observes CallbackActiveCrossThread{Trigger};
        // audit logs Failed; next tick succeeds after the trigger callback exits.
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        // Add a cron schedule whose callback writes one row. Pause the tickler
        // BEFORE creating the schedule so the background loop does not fire
        // a "no callback registered" Failed audit between CREATE SCHEDULE and
        // register_cron_callback.
        let _pause = db.pause_cron_tickler_for_test();
        db.execute("CREATE TABLE cron_log (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute(
            "CREATE SCHEDULE every_short EVERY '50 MILLISECONDS' TX (every_short_cb)",
            &empty(),
        )
        .unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_cb = counter.clone();
        db.register_cron_callback("every_short_cb", move |db_handle| {
            let n = counter_cb.fetch_add(1, AtomicOrdering::SeqCst);
            db_handle.execute(
                "INSERT INTO cron_log (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA390 + n as u128)))]),
            )?;
            Ok(())
        })
        .unwrap();

        // Audit must be empty before contention.
        let baseline = db.cron_audit_log_for_test();
        assert!(
            baseline.is_empty(),
            "audit must start empty (paused tickler); got {:?}",
            baseline
        );

        let fire = fire_trigger_in_thread(db.clone(), 0xC39);
        entered.wait();
        // Drive a cron tick while the trigger callback is parked. begin()
        // sees B2 and propagates Err via Stub 5's `?` on cron.rs:464.
        let _ = db.cron_run_due_now_for_test();
        let audit_after_contention = db.cron_audit_log_for_test();
        // Pin BOTH the schedule_name AND the inner Failed(String) — accept
        // only failures attributed to the typed trigger-cross-thread cause.
        // Matching the bare Failed(_) variant would let any cause (e.g.,
        // permission denied, schedule-not-found) pass.
        let has_failed_for_trigger = audit_after_contention.iter().any(|entry| {
            entry.schedule_name == "every_short"
                && matches!(
                    &entry.kind,
                    CronAuditKind::Failed(s) if s.contains("trigger callback active on another thread")
                )
        });
        // Unblock the trigger callback; next tick succeeds.
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            has_failed_for_trigger,
            "cron audit must contain a Failed entry for schedule 'every_short' whose inner string contains 'trigger callback active on another thread'; audit={:?}",
            audit_after_contention
        );
        // Drive another tick now that the trigger callback has exited.
        fire_cron_now(&db);
        let post_audit = db.cron_audit_log_for_test();
        let has_fired_after = post_audit.iter().any(|entry| {
            entry.schedule_name == "every_short"
                && matches!(entry.kind, CronAuditKind::Fired)
        });
        assert!(
            has_fired_after,
            "next cron tick must succeed (Fired entry for schedule 'every_short') after trigger callback exits; audit={:?}",
            post_audit
        );
    })
    .expect("t39 deadlocked");
}

// ============================================================================
// t39b coverage note — B1 (cross-thread cron callback) racing the tickler is
// not physically reachable: cron callbacks only run on the tickler thread,
// and the tickler does not call begin() concurrent with its own callback.
// Therefore t39b (tickler vs B1) is dropped; t39c (sustained B2 contention)
// extends t39's resilience-under-contention coverage instead.
// ============================================================================

// ============================================================================
// t39c_cron_tickler_survives_sustained_b2_contention — RED
// A trigger callback parked across 5 consecutive cron ticks. All 5 audit
// entries must be Failed; the schedule must NOT have stopped; post-unblock
// the next tick must log Fired. Pins that the tickler does not give up
// after a single failure (or any fixed N failures).
// ============================================================================
#[test]
#[serial]
fn t39c_cron_tickler_survives_sustained_b2_contention() {
    use contextdb_engine::CronAuditKind;
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());

        let _pause = db.pause_cron_tickler_for_test();
        db.execute("CREATE TABLE cron_log (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute(
            "CREATE SCHEDULE every_short EVERY '50 MILLISECONDS' TX (every_short_cb)",
            &empty(),
        )
        .unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_cb = counter.clone();
        db.register_cron_callback("every_short_cb", move |db_handle| {
            let n = counter_cb.fetch_add(1, AtomicOrdering::SeqCst);
            db_handle.execute(
                "INSERT INTO cron_log (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA39C0 + n as u128)))]),
            )?;
            Ok(())
        })
        .unwrap();

        // Park the trigger callback.
        let fire = fire_trigger_in_thread(db.clone(), 0xC39C);
        entered.wait();

        // Drive 5 consecutive cron ticks while the trigger callback remains parked.
        // Each tick's begin() observes B2 and propagates the typed Err via Stub 5's
        // `?` on cron.rs:464; run_cron_callback_transaction records Failed.
        for _ in 0..5 {
            let _ = db.cron_run_due_now_for_test();
        }
        let audit_during = db.cron_audit_log_for_test();
        // Pin BOTH the schedule_name AND the inner Failed(String) on every
        // counted entry. Matching the bare Failed(_) variant would accept
        // any failure cause; matching a different schedule would conflate
        // unrelated regressions.
        let failed_count = audit_during
            .iter()
            .filter(|e| {
                e.schedule_name == "every_short"
                    && matches!(
                        &e.kind,
                        CronAuditKind::Failed(s) if s.contains("trigger callback active on another thread")
                    )
            })
            .count();
        let counter_during = counter.load(AtomicOrdering::SeqCst);

        // Unblock the parked trigger callback and let the firing thread complete
        // before asserting the RED condition, so a stub-time failure cannot leave
        // global trigger callback owner state parked for the next test.
        done.wait();
        fire.join().unwrap().unwrap();

        assert!(
            failed_count >= 5,
            "expected ≥5 Failed audit entries for schedule 'every_short' whose inner string contains 'trigger callback active on another thread' during sustained B2 contention; got {failed_count}; audit={:?}",
            audit_during
        );
        // Schedule must still be live — counter is still 0 because every callback
        // begin() failed before the closure ran.
        assert_eq!(
            counter_during,
            0,
            "cron callback closure must NOT have run while begin() was failing"
        );

        // Drive one more tick — the schedule must still be live and produce Fired.
        fire_cron_now(&db);
        let audit_post = db.cron_audit_log_for_test();
        let post_fired = audit_post
            .iter()
            .skip(audit_during.len())
            .any(|e| e.schedule_name == "every_short" && matches!(e.kind, CronAuditKind::Fired));
        assert!(
            post_fired,
            "next cron tick after sustained contention must succeed (Fired entry for 'every_short'); post-audit tail={:?}",
            &audit_post[audit_during.len()..]
        );
        assert!(
            counter.load(AtomicOrdering::SeqCst) >= 1,
            "post-unblock callback closure must have run at least once"
        );
    })
    .expect("t39c deadlocked");
}

// ============================================================================
// t40_sync_wire_substring_contract — REGRESSION GUARD
// In-process variant-level Display contract. Pairs with t40b which exercises
// the same substrings end-to-end across the contextdb-server wire.
// ============================================================================
#[test]
#[serial]
fn t40_sync_wire_substring_contract() {
    // Cross-process consumers receive Display strings. The kind-agnostic
    // greppable substring "callback active on another thread" + the kind
    // disambiguator "trigger callback" both appear in the wire string.
    let err = Error::CallbackActiveCrossThread {
        kind: CallbackKind::Trigger,
    };
    let wire = err.to_string();
    assert!(
        wire.contains("callback active on another thread"),
        "wire substring missing kind-agnostic class B greppable: {wire:?}"
    );
    assert!(
        wire.contains("trigger callback"),
        "wire substring missing kind disambiguator: {wire:?}"
    );
    // Class A symmetric check.
    let err = Error::CallbackReentry {
        kind: CallbackKind::Trigger,
    };
    let wire = err.to_string();
    assert!(wire.contains("operation not allowed inside"));
    assert!(wire.contains("trigger callback"));
}

// ============================================================================
// t40b_sync_push_response_error_carries_typed_substrings_over_wire — RED
// End-to-end wire test. A real contextdb-server child process is spawned
// alongside testcontainers NATS; a SyncClient pushes a ChangeSet whose
// receiver-side apply_changes hits B2 (parked trigger callback on the
// server-side database). The PushResponse.error wraps the engine's typed
// Display string; SyncClient::push surfaces that string via Err(SyncError(_)).
// We assert the received error string contains BOTH "callback active on
// another thread" (kind-agnostic) AND "trigger callback" (kind disambiguator).
// Without this, a future engine PR that redacts the error string at
// sync_server.rs:237/256/294 would silently break the wire substring while
// the in-process t40 still passed.
// ============================================================================
//
// Harness shape mirrors tests/acceptance/sync.rs::sync_e2e_value_txid_round_trip
// (uses common::start_nats and SyncClient/SyncServer in-process via tokio
// rather than spawn_server, since we need to register a trigger callback on
// the server-side Database — spawn_server uses the contextdb-server binary
// which has no trigger-callback registration surface across the process boundary).
//
// IMPORTANT: this test runs the server-side Database in-process under a
// tokio runtime, which is sufficient to exercise the wire path (NATS round
// trip + PushResponse encoding/decoding). It is functionally equivalent to
// spawning the server binary for the purposes of the wire substring contract.
#[tokio::test]
#[serial]
async fn t40b_sync_push_response_error_carries_typed_substrings_over_wire() {
    use super::common::{start_nats, wait_for_sync_server_ready};
    use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
    use contextdb_server::{SyncClient, SyncServer};

    let nats = start_nats().await;
    let tenant = "t40b_wire_substring";

    // Server-side database: register a trigger that parks at a barrier so
    // any apply_changes call on the server hits B2.
    let server_db = Arc::new(Database::open_memory());
    server_db
        .execute("CREATE TABLE applied (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    server_db
        .execute("CREATE TABLE host (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    server_db
        .execute("CREATE TRIGGER tr ON host WHEN INSERT", &empty())
        .unwrap();
    let entered = Arc::new(Barrier::new(2));
    let done = Arc::new(Barrier::new(2));
    let entered_cb = entered.clone();
    let done_cb = done.clone();
    server_db
        .register_trigger_callback("tr", move |_db, _ctx| {
            entered_cb.wait();
            done_cb.wait();
            Ok(())
        })
        .unwrap();
    server_db.complete_initialization().unwrap();

    // Edge-side database: same DDL so ChangeSet conversion succeeds.
    let edge_db = Arc::new(Database::open_memory());
    edge_db
        .execute("CREATE TABLE applied (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    edge_db
        .execute("CREATE TABLE host (id UUID PRIMARY KEY)", &empty())
        .unwrap();

    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        tenant,
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    assert!(
        wait_for_sync_server_ready(&nats.nats_url, tenant, Duration::from_secs(15)).await,
        "sync server must be ready before t40b parks the trigger callback"
    );

    // Park the server-side trigger callback by inserting into `host`.
    // Run the firing INSERT on a dedicated blocking thread so the tokio
    // runtime is not blocked by the trigger barrier.
    let server_db_for_fire = server_db.clone();
    let fire_handle = tokio::task::spawn_blocking(move || {
        let tx = server_db_for_fire.begin().expect("server begin");
        server_db_for_fire
            .execute_in_tx(
                tx,
                "INSERT INTO host (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xC40B0)))]),
            )
            .expect("server insert");
        server_db_for_fire.commit(tx).expect("server commit")
    });
    // Wait for the callback to be parked. entered.wait() blocks; do this on
    // a blocking task too.
    let entered_for_wait = entered.clone();
    tokio::task::spawn_blocking(move || entered_for_wait.wait())
        .await
        .unwrap();

    // Now push from the edge. The server-side apply_changes will hit B2 and
    // return Err(CallbackActiveCrossThread{Trigger}); SyncServer wraps the
    // Display string as PushResponse.error; SyncClient surfaces it as
    // Err(Error::SyncError(string)).
    let edge_client = SyncClient::new(edge_db.clone(), &nats.nats_url, tenant);
    // Insert a row on the edge so push has something to send.
    {
        let tx = edge_db.begin().expect("edge begin");
        edge_db
            .execute_in_tx(
                tx,
                "INSERT INTO applied (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xC40B1)))]),
            )
            .expect("edge insert");
        edge_db.commit(tx).expect("edge commit");
    }

    let push_result = tokio::time::timeout(Duration::from_secs(30), edge_client.push())
        .await
        .expect("push should complete or return the typed sync error within 30s");

    // Unblock the server-side trigger callback so the firing task can complete.
    let done_for_wait = done.clone();
    tokio::task::spawn_blocking(move || done_for_wait.wait())
        .await
        .unwrap();
    fire_handle.await.unwrap();

    // The push must have failed with the typed string preserved inside
    // SyncClient's fixed "sync error: ..." wrapper.
    let err = push_result.expect_err("push must fail under server-side B2 contention");
    let wire = err.to_string();
    let canonical = "trigger callback active on another thread; retry once the callback completes";
    let expected_wire = format!("sync error: {canonical}");
    assert!(
        wire.contains("callback active on another thread"),
        "wire substring missing kind-agnostic Class B greppable in PushResponse.error path: wire={wire:?}"
    );
    assert!(
        wire.contains("trigger callback"),
        "wire substring missing kind disambiguator in PushResponse.error path: wire={wire:?}"
    );
    assert_eq!(
        wire, expected_wire,
        "wire did not preserve the exact SyncClient wrapper plus canonical engine Display template"
    );
}

// t41 lives in tests/acceptance/observation_trigger.rs — see Stub 6.
// At stub time t41 is RED (substring drift). After Step 5 implementation,
// the rewrite asserts pass against the typed-variant Display strings.

// ============================================================================
// t42_begin_or_panic_helper_visibility — REGRESSION GUARD post-stubs
// ============================================================================
#[test]
#[serial]
fn t42_begin_or_panic_helper_visibility() {
    let db = Database::open_memory();
    // begin_or_panic must be a regular pub fn — callable from this integration
    // test without #[cfg(test)] visibility.
    let tx = db.begin_or_panic();
    db.rollback(tx).unwrap();
}

// ============================================================================
// t43_surface_string_no_begin_token — REGRESSION GUARD
// ============================================================================
#[test]
#[serial]
fn t43_surface_string_no_begin_token() {
    for err in [
        Error::CallbackActiveCrossThread {
            kind: CallbackKind::Trigger,
        },
        Error::CallbackActiveCrossThread {
            kind: CallbackKind::Cron,
        },
        Error::CallbackReentry {
            kind: CallbackKind::Trigger,
        },
        Error::CallbackReentry {
            kind: CallbackKind::Cron,
        },
    ] {
        let display = err.to_string();
        assert!(
            !display.contains("begin()"),
            "Display must be surface-agnostic; found 'begin()' in: {display:?}"
        );
    }
}

// ============================================================================
// t44b_post_stub_begin_returns_typed_err_under_b2_cross_thread_contention
// NOT feature-gated. STRICT: asserts catch_unwind returns
// Ok(Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger })).
// Stays after Step 5 as a regression guard against any future panic regression
// in the typed-Err path.
// ============================================================================
#[test]
#[serial]
fn t44b_post_stub_begin_returns_typed_err_under_b2_cross_thread_contention() {
    use std::panic::catch_unwind;
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        let fire = fire_trigger_in_thread(db.clone(), 0xC44B);
        entered.wait();
        let db_probe = db.clone();
        let outcome = catch_unwind(std::panic::AssertUnwindSafe(|| db_probe.begin()));
        done.wait();
        fire.join().unwrap().unwrap();
        // Strict: post-stub (and post-Step-5) begin() must NOT panic AND must
        // return the typed variant. Any Ok(Ok(_)) or Err(_) (panic regression)
        // fails. At stub time the typed variant is not yet wired (Stub 2
        // returns Error::Other) so this test is RED until Step 5 lands.
        assert!(
            matches!(
                outcome,
                Ok(Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger }))
            ),
            "expected Ok(Err(CallbackActiveCrossThread{{Trigger}})) under cross-thread trigger contention; got {:?}",
            outcome
        );
    })
    .expect("t44b deadlocked");
}

// ============================================================================
// t44b sibling family — RED at stub time, regression guard post-Step-5
// Each sibling catch_unwind-wraps a distinct tx-control surface under B2
// trigger contention and strictly asserts the typed Err. A panic regression
// in commit/rollback/apply_changes/close would manifest only as a 30-second
// run_with_timeout deadlock (the spawned thread's panic would prevent the
// channel send) without these sibling probes — t44b alone covers begin().
// ============================================================================

/// Helper for the t44b sibling family. Drives a B2 (parked-trigger cross-thread)
/// scenario, captures the surface call inside `catch_unwind`, and returns the
/// outcome for the caller to assert on. The parked callback is unblocked after
/// the surface call records its result. AssertUnwindSafe wraps the inner call
/// because Database holds interior mutability and is not UnwindSafe.
fn t44b_capture_under_b2<F, T>(
    surface: F,
) -> std::result::Result<Result<T>, Box<dyn std::any::Any + Send>>
where
    F: FnOnce(Arc<Database>) -> Result<T>,
    T: Send + 'static,
{
    let entered = Arc::new(Barrier::new(2));
    let done = Arc::new(Barrier::new(2));
    let db = build_trigger_parked_db(entered.clone(), done.clone());
    let fire = fire_trigger_in_thread(db.clone(), 0xC44B5);
    entered.wait();
    let db_probe = db.clone();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || surface(db_probe)));
    done.wait();
    fire.join().unwrap().unwrap();
    outcome
}

#[test]
#[serial]
fn t44b_commit_post_stub_returns_typed_err_under_b2_no_panic() {
    run_with_timeout(|| {
        // Pre-allocate a tx for commit() to operate on. begin() is itself
        // gated, so do this BEFORE entering the parked-trigger window.
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        let setup_tx = db.begin().expect("setup begin");
        let fire = fire_trigger_in_thread(db.clone(), 0xC44B6);
        entered.wait();
        let db_probe = db.clone();
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db_probe.commit(setup_tx)));
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            matches!(
                outcome,
                Ok(Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                }))
            ),
            "commit() under B2 must NOT panic AND must return typed Err; got {:?}",
            outcome
        );
    })
    .expect("t44b_commit deadlocked");
}

#[test]
#[serial]
fn t44b_rollback_post_stub_returns_typed_err_under_b2_no_panic() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        let setup_tx = db.begin().expect("setup begin");
        let fire = fire_trigger_in_thread(db.clone(), 0xC44B7);
        entered.wait();
        let db_probe = db.clone();
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db_probe.rollback(setup_tx)));
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            matches!(
                outcome,
                Ok(Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                }))
            ),
            "rollback() under B2 must NOT panic AND must return typed Err; got {:?}",
            outcome
        );
    })
    .expect("t44b_rollback deadlocked");
}

#[test]
#[serial]
fn t44b_apply_changes_post_stub_returns_typed_err_under_b2_no_panic() {
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = build_trigger_parked_db(entered.clone(), done.clone());
        // apply target — separate from the trigger's firing table `t`.
        db.execute("CREATE TABLE applied (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let fire = fire_trigger_in_thread(db.clone(), 0xC44B8);
        entered.wait();
        let db_probe = db.clone();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db_probe.apply_changes(
                populated_changeset("applied", 0xC44B_8000),
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            )
        }));
        done.wait();
        fire.join().unwrap().unwrap();
        assert!(
            matches!(
                outcome,
                Ok(Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                }))
            ),
            "apply_changes() under B2 must NOT panic AND must return typed Err; got {:?}",
            outcome
        );
    })
    .expect("t44b_apply_changes deadlocked");
}

#[test]
#[serial]
fn t44b_close_post_stub_returns_typed_err_under_b2_no_panic() {
    run_with_timeout(|| {
        let outcome = t44b_capture_under_b2(|db| db.close());
        assert!(
            matches!(
                outcome,
                Ok(Err(Error::CallbackActiveCrossThread {
                    kind: CallbackKind::Trigger
                }))
            ),
            "close() under B2 must NOT panic AND must return typed Err; got {:?}",
            outcome
        );
    })
    .expect("t44b_close deadlocked");
}

// ============================================================================
// t45_constrained_handle_classifies_typed_err_identically_to_unconstrained — RED
// Database opened via Database::open_with_contexts after an admin handle has
// declared the trigger. Both A2 (same-thread reentry inside the callback) and
// B2 (cross-thread begin on the constrained handle while its callback is parked)
// must classify identically (same typed variant, same kind) to the unconstrained
// handle case.
// ============================================================================
#[test]
#[serial]
fn t45_constrained_handle_classifies_typed_err_identically_to_unconstrained() {
    use std::collections::BTreeSet;
    run_with_timeout(|| {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let path = tmp.path().join("t45-constrained.db");
        // ContextId has only `pub fn new(uuid: Uuid) -> Self`; no From<&str> impl.
        let ctx = contextdb_core::types::ContextId::new(Uuid::from_u128(0x00C0FFEE_0000_4045_A045_000000000045_u128));
        let mut contexts = BTreeSet::new();
        contexts.insert(ctx.clone());

        // Trigger DDL requires an admin handle. Declare schema first, then open
        // the constrained handle against the same file-backed database.
        let admin = Database::open(&path).expect("open admin");
        admin
            .execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        admin
            .execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        admin.close().unwrap();

        let db_a = Arc::new(
            Database::open_with_contexts(&path, contexts.clone()).expect("open_with_contexts a"),
        );

        // A2 probe — record the result of begin() called from inside the callback.
        let observed_a2: Arc<Mutex<Option<Result<contextdb_core::TxId>>>> = Arc::new(Mutex::new(None));
        let observed_a2_cb = observed_a2.clone();
        // Two-barrier handshake for the cross-thread B2 probe; the callback
        // captures the A2 result, signals `entered`, waits at `done` so the
        // B2 racer thread can run while the callback body is still active.
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let entered_cb = entered.clone();
        let done_cb = done.clone();
        let db_a_for_cb = db_a.clone();
        db_a.register_trigger_callback("tr", move |_db_handle, _ctx| {
            // A2: same-thread, same-handle reentry.
            *observed_a2_cb.lock().unwrap() = Some(db_a_for_cb.begin());
            // Park so the B2 racer can run.
            entered_cb.wait();
            done_cb.wait();
            Ok(())
        })
        .unwrap();
        db_a.complete_initialization().unwrap();

        // Drive the firing INSERT on a dedicated thread.
        let fire = fire_trigger_in_thread(db_a.clone(), 0xC45);

        // Wait for the callback to be parked, then run the B2 probe on the same
        // constrained handle from the owning test thread.
        entered.wait();
        let result_b2 = db_a.begin();
        done.wait();
        fire.join().unwrap().unwrap();

        let result_a2 = observed_a2.lock().unwrap().take().expect("A2 callback ran");
        assert!(
            matches!(
                result_a2,
                Err(Error::CallbackReentry { kind: CallbackKind::Trigger })
            ),
            "constrained-handle A2 must classify identically to unconstrained: expected CallbackReentry{{Trigger}}, got {:?}",
            result_a2
        );
        assert!(
            matches!(
                result_b2,
                Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger })
            ),
            "constrained-handle B2 must classify identically to unconstrained: expected CallbackActiveCrossThread{{Trigger}}, got {:?}",
            result_b2
        );
    })
    .expect("t45 deadlocked");
}

// ============================================================================
// t46_sink_callback_calling_tx_control_inside_parked_trigger_returns_typed_err_to_dispatcher — RED
// EventBus sink callback calls trigger_db.begin() on an externally captured
// handle while a trigger callback is parked on a separate thread (B2 active). The
// sink callback receives Err(CallbackActiveCrossThread{Trigger}), converts
// to SinkError::Transient, and the dispatcher's existing retry loop runs.
//
// The sink is wired on event_db, while the parked trigger lives on trigger_db.
// This keeps event delivery independent of the B2 guard being probed by the
// sink callback.
// ============================================================================
#[test]
#[serial]
fn t46_sink_callback_calling_tx_control_inside_parked_trigger_returns_typed_err_to_dispatcher() {
    use contextdb_engine::{SinkError, SinkEvent};
    run_with_timeout(|| {
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let trigger_db = build_trigger_parked_db(entered.clone(), done.clone());
        let event_db = Arc::new(Database::open_memory());

        // Sink event source table + EventBus DDL (CREATE EVENT TYPE / CREATE SINK
        // / CREATE ROUTE) wires INSERTs on `invalidations` to the `retry_probe`
        // sink. Pattern lifted from tests/acceptance/event_bus.rs::declare_inv_schema.
        event_db
            .execute(
            "CREATE TABLE invalidations (id UUID PRIMARY KEY, severity TEXT, reason TEXT)",
            &empty(),
        )
        .unwrap();
        event_db
            .execute(
            "CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations",
            &empty(),
        )
        .unwrap();
        event_db
            .execute("CREATE SINK retry_probe TYPE callback", &empty())
            .unwrap();
        event_db
            .execute(
            "CREATE ROUTE inv_to_retry_probe EVENT inv_match TO retry_probe",
            &empty(),
        )
        .unwrap();

        // Sink callback: probes trigger_db.begin() on the externally captured handle.
        // Under B2 (parked trigger) returns Transient → dispatcher retries.
        let attempts = Arc::new(AtomicUsize::new(0));
        let saw_typed_err = Arc::new(AtomicUsize::new(0));
        let attempts_cb = attempts.clone();
        let saw_typed_err_cb = saw_typed_err.clone();
        let db_for_sink = trigger_db.clone();
        event_db
            .register_sink("retry_probe", None, move |_evt: &SinkEvent| {
            let n = attempts_cb.fetch_add(1, AtomicOrdering::SeqCst);
            match db_for_sink.begin() {
                Ok(tx) => {
                    let _ = db_for_sink.rollback(tx);
                    Ok(())
                }
                Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger }) => {
                    saw_typed_err_cb.fetch_add(1, AtomicOrdering::SeqCst);
                    Err(SinkError::Transient(format!(
                        "callback active on another thread; sink attempt {n}"
                    )))
                }
                Err(other) => Err(SinkError::Permanent(format!("unexpected: {other:?}"))),
            }
        })
        .unwrap();

        // Park the trigger callback BEFORE driving the sink event, so the sink
        // dispatcher's first attempt hits B2.
        let fire = fire_trigger_in_thread(trigger_db.clone(), 0xC46);
        entered.wait();

        // Drive the sink — INSERT into invalidations emits the routed event on
        // event_db's dispatcher thread; that thread's sink callback probes
        // trigger_db and hits B2.
        let sink_drive_result = event_db.execute(
            "INSERT INTO invalidations (id, severity, reason) VALUES ($id, $sev, $rsn)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(0xA46))),
                ("sev".to_string(), Value::Text("warning".to_string())),
                ("rsn".to_string(), Value::Text("t46".to_string())),
            ]),
        );

        // Wait for the sink dispatcher to attempt and observe the typed Err.
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && saw_typed_err.load(AtomicOrdering::SeqCst) == 0 {
            thread::sleep(Duration::from_millis(20));
        }
        let saw_typed_before_unblock = saw_typed_err.load(AtomicOrdering::SeqCst);

        // Unblock the trigger callback so subsequent sink retries succeed.
        done.wait();
        fire.join().unwrap().unwrap();
        sink_drive_result.expect("sink-driving INSERT must succeed");
        assert!(
            saw_typed_before_unblock >= 1,
            "sink callback must have observed CallbackActiveCrossThread{{Trigger}} during B2 contention"
        );

        // Wait for the sink retry loop to deliver successfully.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let m = event_db.sink_metrics_for_test("retry_probe");
            if m.delivered >= 1 && m.retried >= 1 {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let m = event_db.sink_metrics_for_test("retry_probe");
        assert!(m.retried >= 1, "sink dispatcher's retry loop must have run; metrics={:?}", m);
        assert!(m.delivered >= 1, "sink must eventually deliver after contention clears; metrics={:?}", m);
    })
    .expect("t46 deadlocked");
}

// ============================================================================
// t47_apply_changes_100_row_changeset_fails_early_under_b2_contention — RED
// 100-row apply_changes under B2 (parked trigger callback). Asserts:
// (a) typed Err returned, (b) in-memory post-Err scan returns 0 rows, AND
// (c) a fresh open of the same on-disk file post-drop also reports 0 rows.
// Pins fail-early-and-rebuild at scale AND that no rows leaked into durable
// redb state. Row count is 100 (not 4096) — the H2 lesson is wall-clock-
// sensitive large-N tests live in benches.
// ============================================================================
#[test]
#[serial]
fn t47_apply_changes_100_row_changeset_fails_early_under_b2_contention() {
    run_with_timeout(|| {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let path = tmp.path().join("t47.db");

        // Build the trigger-parked DB on disk so we can reopen post-drop.
        let entered = Arc::new(Barrier::new(2));
        let done = Arc::new(Barrier::new(2));
        let db = Arc::new(Database::open(&path).expect("open"));
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        // Apply target — separate from `t` (the trigger's firing table).
        db.execute("CREATE TABLE applied (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        let entered_cb = entered.clone();
        let done_cb = done.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            entered_cb.wait();
            done_cb.wait();
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        let fire = fire_trigger_in_thread(db.clone(), 0xC47);
        entered.wait();
        let cs = populated_changeset_n("applied", 0xC47_0000, 100);
        let result =
            db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins));
        done.wait();
        fire.join().unwrap().unwrap();

        assert!(
            matches!(
                result,
                Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger })
            ),
            "expected typed CallbackActiveCrossThread{{Trigger}} on 100-row apply_changes under B2; got {:?}",
            result
        );
        let snap = db.snapshot();
        let scanned = db.scan("applied", snap).unwrap();
        assert_eq!(
            scanned.len(),
            0,
            "fail-early-and-rebuild: in-memory post-Err scan must be empty; saw {} rows present",
            scanned.len()
        );

        // Durable-persistence pin — drop the Database, reopen the same path,
        // assert the `applied` table is still empty. Catches any regression
        // where apply_changes leaks rows into durable redb state before
        // returning the typed Err.
        drop(db);
        let reopened = Database::open(&path).expect("reopen post-drop");
        let snap = reopened.snapshot();
        assert_eq!(
            reopened.scan("applied", snap).unwrap().len(),
            0,
            "fail-early-and-rebuild: durable post-Err scan after reopen must be empty"
        );
    })
    .expect("t47 deadlocked");
}

// ============================================================================
// t48_eventual_progress_under_alternating_cron_and_trigger_contention — RED
// A retry loop alternates against B1 (cron callback parked on T1) and B2
// (trigger callback parked on T2). Both are unblocked at staggered times;
// the loop must eventually succeed within ≤100 retry attempts. Pins the
// eventual-progress guarantee under cycling Cron↔Trigger contention
// (Behavior contract §10).
// ============================================================================
#[test]
#[serial]
fn t48_eventual_progress_under_alternating_cron_and_trigger_contention() {
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        let _pause = db.pause_cron_tickler_for_test();
        db.execute(
            "CREATE SCHEDULE c EVERY '1 MILLISECONDS' TX (cb)",
            &empty(),
        )
        .unwrap();
        let trigger_db = Arc::new(Database::open_memory());
        trigger_db
            .execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        trigger_db
            .execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();

        let entered_cron = Arc::new(Barrier::new(2));
        let done_cron = Arc::new(Barrier::new(2));
        let entered_trigger = Arc::new(Barrier::new(2));
        let done_trigger = Arc::new(Barrier::new(2));
        let entered_cron_cb = entered_cron.clone();
        let done_cron_cb = done_cron.clone();
        db.register_cron_callback("cb", move |_db_handle| {
            entered_cron_cb.wait();
            done_cron_cb.wait();
            Ok(())
        })
        .unwrap();
        let entered_trigger_cb = entered_trigger.clone();
        let done_trigger_cb = done_trigger.clone();
        trigger_db
            .register_trigger_callback("tr", move |_db, _ctx| {
                entered_trigger_cb.wait();
                done_trigger_cb.wait();
                Ok(())
            })
            .unwrap();
        trigger_db.complete_initialization().unwrap();

        // T1 parks the cron callback first.
        let cron_dispatch = dispatch_cron_now_in_thread(db.clone());
        entered_cron.wait();

        // T2 parks a trigger callback on a separate Database while the retrying
        // handle's cron callback is still parked. The retrying `db.begin()` sees
        // handle-local B1 first, then process-wide B2 after B1 clears.
        let trigger_fire = fire_trigger_in_thread(trigger_db.clone(), 0xC48);
        entered_trigger.wait();

        // Spawn a "stagger" thread that releases cron first, then trigger
        // ~10ms later. The retry loop on the main thread must observe at
        // least one of each typed-variant kind during the cycling and
        // eventually succeed once both are released.
        let done_cron_release = done_cron.clone();
        let done_trigger_release = done_trigger.clone();
        let stagger = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            done_cron_release.wait(); // releases the cron callback
            thread::sleep(Duration::from_millis(20));
            done_trigger_release.wait(); // releases the trigger callback
        });

        let mut attempts = 0usize;
        let mut saw_cron = false;
        let mut saw_trigger = false;
        let mut unexpected_error: Option<String> = None;
        let final_tx = loop {
            attempts += 1;
            if attempts > 100 {
                unexpected_error = Some(format!(
                    "eventual-progress contract violated: ≥100 retries without success (saw_cron={saw_cron}, saw_trigger={saw_trigger})"
                ));
                break None;
            }
            match db.begin() {
                Ok(tx) => break Some(tx),
                Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Cron }) => {
                    saw_cron = true;
                    thread::sleep(Duration::from_millis(2));
                }
                Err(Error::CallbackActiveCrossThread { kind: CallbackKind::Trigger }) => {
                    saw_trigger = true;
                    thread::sleep(Duration::from_millis(2));
                }
                Err(other) => {
                    unexpected_error =
                        Some(format!("unexpected error during cycling B1↔B2: {other:?}"));
                    break None;
                }
            }
        };
        if let Some(final_tx) = final_tx {
            let _ = db.rollback(final_tx);
        }

        stagger.join().unwrap();
        let _ = cron_dispatch.join();
        trigger_fire.join().unwrap().unwrap();

        if let Some(message) = unexpected_error {
            panic!("{message}");
        }

        // The retry loop must have observed BOTH kinds during the cycling
        // (proves the alternation actually happened, not that one side was
        // released too quickly to ever observe).
        assert!(
            saw_cron,
            "retry loop must have observed at least one CallbackActiveCrossThread{{Cron}} during cycling"
        );
        assert!(
            saw_trigger,
            "retry loop must have observed at least one CallbackActiveCrossThread{{Trigger}} during cycling"
        );
    })
    .expect("t48 deadlocked");
}

// ============================================================================
// t49_begin_or_panic_panics_with_typed_display_when_inside_trigger_callback — RED
// Pins that begin_or_panic surfaces the typed Err's Display in the panic
// payload, not a generic "begin failed" message.
// ============================================================================
#[test]
#[serial]
fn t49_begin_or_panic_panics_with_typed_display_when_inside_trigger_callback() {
    use std::panic::catch_unwind;
    run_with_timeout(|| {
        let db = Arc::new(Database::open_memory());
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &empty())
            .unwrap();
        let observed: Arc<Mutex<Option<Box<dyn std::any::Any + Send>>>> =
            Arc::new(Mutex::new(None));
        let observed_cb = observed.clone();
        let db_for_cb = db.clone();
        db.register_trigger_callback("tr", move |_db, _ctx| {
            let outcome = catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = db_for_cb.begin_or_panic();
            }));
            *observed_cb.lock().unwrap() = Some(outcome.expect_err("begin_or_panic must panic"));
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        db.execute(
            "INSERT INTO t (id) VALUES ($id)",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xA49)))]),
        )
        .unwrap();

        let payload = observed.lock().unwrap().take().expect("callback ran");
        // Downcast the panic payload to &str or String. expect() typically
        // produces a String; std panic!("...") produces &'static str. Try both.
        let payload_str: String = if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            panic!("panic payload was neither &str nor String");
        };
        let expected =
            "operation not allowed inside a trigger callback (API misuse — fix the call site)";
        assert!(
            payload_str == expected,
            "begin_or_panic panic payload must be exactly the typed Err Display; got {:?}",
            payload_str
        );
    })
    .expect("t49 deadlocked");
}
