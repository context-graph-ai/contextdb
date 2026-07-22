//! Snapshot read stability under concurrent replacement.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, MutexGuard};
use std::thread::{self, yield_now};

use contextdb_core::types::{ContextId, TxId};
use contextdb_core::{Error, Result, Value};
use contextdb_engine::{Database, QueryResult};
use tempfile::{TempDir, tempdir};
use uuid::Uuid;

const WRITERS: usize = 12;
const READERS: usize = 4;
const ROUNDS: usize = 50;
const K: usize = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
struct TargetRow {
    id: Uuid,
    group_id: Uuid,
    slot: i64,
    item_ref: Uuid,
    payload: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TextTargetRow {
    id: String,
    group_id: Uuid,
    slot: i64,
    item_ref: String,
    payload: String,
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(values: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect()
}

fn row_id(group: Uuid, slot: i64) -> Uuid {
    Uuid::from_u128(group.as_u128() ^ ((slot as u128) << 96) ^ 0x9C42)
}

fn item_ref(group: Uuid, slot: i64, writer: usize) -> Uuid {
    Uuid::from_u128(group.as_u128() ^ ((slot as u128) << 64) ^ ((writer as u128) << 32) ^ 0xEF)
}

fn text_row_id(group: Uuid, slot: i64) -> String {
    format!("{group}-{slot}")
}

fn text_item_ref(group: Uuid, slot: i64, writer: usize) -> String {
    format!("{group}-{slot}-{writer}")
}

fn classify_query(outcome: Result<QueryResult>) -> Option<QueryResult> {
    match outcome {
        Ok(result) => Some(result),
        Err(Error::UniqueViolation { .. })
        | Err(Error::ConditionalUpdateConflict { .. })
        | Err(Error::ForeignKeyViolation { .. })
        | Err(Error::InvalidStateTransition(_)) => None,
        Err(other) => panic!("illegitimate error under replace race: {other:?}"),
    }
}

fn classify_commit(outcome: Result<()>) -> bool {
    match outcome {
        Ok(()) => true,
        Err(Error::UniqueViolation { .. })
        | Err(Error::ConditionalUpdateConflict { .. })
        | Err(Error::ForeignKeyViolation { .. })
        | Err(Error::InvalidStateTransition(_)) => false,
        Err(other) => panic!("illegitimate error under replace race: {other:?}"),
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send + 'static>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_else(|| "non-string panic".to_string())
}

fn lock_unpoisoned(lock: &Mutex<()>) -> MutexGuard<'_, ()> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn must_query(outcome: Result<QueryResult>, context: &str) -> QueryResult {
    match outcome {
        Ok(result) => result,
        Err(other) => panic!("{context}: expected Ok result, got {other:?}"),
    }
}

fn must_commit(outcome: Result<()>, context: &str) {
    match outcome {
        Ok(()) => {}
        Err(other) => panic!("{context}: expected commit success, got {other:?}"),
    }
}

fn create_replace_targets(db: &Database) {
    db.execute(
        "CREATE TABLE replace_targets (
          id UUID PRIMARY KEY,
          group_id UUID NOT NULL,
          slot INTEGER NOT NULL,
          item_ref UUID NOT NULL,
          payload TEXT NOT NULL,
          UNIQUE (group_id, slot),
          UNIQUE (group_id, item_ref)
        )",
        &empty(),
    )
    .expect("create replace_targets");
    db.execute(
        "CREATE TABLE replace_markers (
          id UUID PRIMARY KEY,
          writer INTEGER NOT NULL,
          payload TEXT NOT NULL
        )",
        &empty(),
    )
    .expect("create replace_markers");
}

fn create_scoped_replace_targets(db: &Database) {
    db.execute(
        "CREATE TABLE replace_targets (
          id UUID PRIMARY KEY,
          group_id UUID NOT NULL,
          slot INTEGER NOT NULL,
          item_ref UUID NOT NULL,
          payload TEXT NOT NULL,
          context_id UUID CONTEXT_ID,
          UNIQUE (group_id, slot),
          UNIQUE (group_id, item_ref)
        )",
        &empty(),
    )
    .expect("create scoped replace_targets");
    db.execute(
        "CREATE TABLE replace_markers (
          id UUID PRIMARY KEY,
          writer INTEGER NOT NULL,
          payload TEXT NOT NULL
        )",
        &empty(),
    )
    .expect("create scoped replace_markers");
}

fn insert_target_in_tx(
    db: &Database,
    tx: contextdb_core::types::TxId,
    group: Uuid,
    slot: i64,
    writer: usize,
    payload: &str,
) -> Result<QueryResult> {
    db.execute_in_tx(
        tx,
        "INSERT INTO replace_targets (id, group_id, slot, item_ref, payload)
         VALUES ($id, $g, $slot, $ref, $p)",
        &params([
            ("id", Value::Uuid(row_id(group, slot))),
            ("g", Value::Uuid(group)),
            ("slot", Value::Int64(slot)),
            ("ref", Value::Uuid(item_ref(group, slot, writer))),
            ("p", Value::Text(payload.to_string())),
        ]),
    )
}

fn insert_scoped_target_in_tx(
    db: &Database,
    tx: contextdb_core::types::TxId,
    group: Uuid,
    slot: i64,
    writer: usize,
    payload: &str,
    ctx: Uuid,
) -> Result<QueryResult> {
    db.execute_in_tx(
        tx,
        "INSERT INTO replace_targets (id, group_id, slot, item_ref, payload, context_id)
         VALUES ($id, $g, $slot, $ref, $p, $ctx)",
        &params([
            ("id", Value::Uuid(row_id(group, slot))),
            ("g", Value::Uuid(group)),
            ("slot", Value::Int64(slot)),
            ("ref", Value::Uuid(item_ref(group, slot, writer))),
            ("p", Value::Text(payload.to_string())),
            ("ctx", Value::Uuid(ctx)),
        ]),
    )
}

fn seed_group(db: &Database, group: Uuid, writer: usize, payload: &str) {
    let tx = db.begin().expect("seed begin");
    for slot in 0..(K as i64) {
        let result =
            insert_target_in_tx(db, tx, group, slot, writer, payload).expect("seed insert");
        assert_eq!(result.rows_affected, 1);
    }
    db.commit(tx).expect("seed commit");
}

fn seed_scoped_group(db: &Database, group: Uuid, writer: usize, payload: &str, ctx: Uuid) {
    let tx = db.begin().expect("scoped seed begin");
    for slot in 0..(K as i64) {
        let result =
            insert_scoped_target_in_tx(db, tx, group, slot, writer, payload, ctx).expect("seed");
        assert_eq!(result.rows_affected, 1);
    }
    db.commit(tx).expect("scoped seed commit");
}

fn replace_round_with_verify_lock(
    db: &Database,
    group: Uuid,
    writer: usize,
    verify_lock: &Mutex<()>,
) {
    replace_round_inner(db, group, writer, None, None, Some(verify_lock));
}

fn replace_scoped_round_with_verify_lock(
    db: &Database,
    group: Uuid,
    writer: usize,
    ctx: Uuid,
    verify_lock: &Mutex<()>,
) {
    replace_round_inner(db, group, writer, Some(ctx), None, Some(verify_lock));
}

fn trigger_replace_round_logged(
    db: &Database,
    tx: TxId,
    group: Uuid,
    writer: usize,
    violations: &Mutex<Vec<String>>,
) {
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let del = db.execute_in_tx(
            tx,
            "DELETE FROM replace_targets WHERE group_id = $g",
            &params([("g", Value::Uuid(group))]),
        );
        let _ = classify_query(del);
        yield_now();
        for slot in 0..(K as i64) {
            let result = insert_target_in_tx(db, tx, group, slot, writer, &format!("w{writer}"));
            match classify_query(result) {
                Some(result) => assert_eq!(result.rows_affected, 1),
                None => return,
            }
            yield_now();
        }
    }));
    if let Err(panic) = attempt {
        violations.lock().unwrap().push(panic_message(panic));
    }
}

fn replace_round_inner(
    db: &Database,
    group: Uuid,
    writer: usize,
    ctx: Option<Uuid>,
    max_attempts: Option<usize>,
    verify_lock: Option<&Mutex<()>>,
) {
    let attempts = max_attempts.unwrap_or(2048);
    for attempt in 0..attempts {
        let tx = db.begin().expect("begin replace tx");
        let marker_id = Uuid::new_v4();
        let del_results: Vec<_> = if attempt % 2 == 0 {
            vec![db.execute_in_tx(
                tx,
                "DELETE FROM replace_targets WHERE group_id = $g",
                &params([("g", Value::Uuid(group))]),
            )]
        } else {
            (0..(K as i64))
                .map(|slot| {
                    db.execute_in_tx(
                        tx,
                        "DELETE FROM replace_targets WHERE id = $id",
                        &params([("id", Value::Uuid(row_id(group, slot)))]),
                    )
                })
                .collect()
        };
        for del in del_results {
            let _ = classify_query(del);
        }
        yield_now();

        let mut conflicted = false;
        for slot in 0..(K as i64) {
            let payload = format!("w{writer}");
            let ins = match ctx {
                Some(ctx) => insert_scoped_target_in_tx(db, tx, group, slot, writer, &payload, ctx),
                None => insert_target_in_tx(db, tx, group, slot, writer, &payload),
            };
            match classify_query(ins) {
                Some(result) => assert_eq!(result.rows_affected, 1),
                None => {
                    conflicted = true;
                    break;
                }
            }
            yield_now();
        }
        if conflicted {
            let _ = db.rollback(tx);
            yield_now();
            continue;
        }
        let staged_rows = db
            .execute_in_tx(
                tx,
                "SELECT id, group_id, slot, item_ref, payload
                 FROM replace_targets WHERE group_id = $g ORDER BY slot",
                &params([("g", Value::Uuid(group))]),
            )
            .expect("read own staged replacement rows");
        let staged_rows = parse_target_rows(staged_rows.rows);
        assert_eq!(
            staged_rows.len(),
            K,
            "writer {writer} must read back its full staged replacement before commit"
        );
        for row in staged_rows {
            assert_eq!(row.payload, format!("w{writer}"));
            assert_eq!(row.item_ref, item_ref(group, row.slot, writer));
        }
        db.execute_in_tx(
            tx,
            "INSERT INTO replace_markers (id, writer, payload) VALUES ($id, $writer, $payload)",
            &params([
                ("id", Value::Uuid(marker_id)),
                ("writer", Value::Int64(writer as i64)),
                ("payload", Value::Text(format!("w{writer}"))),
            ]),
        )
        .expect("insert replacement commit marker");
        let _verify_guard = verify_lock.map(lock_unpoisoned);
        if classify_commit(db.commit(tx)) {
            let committed_rows = select_group(db, group);
            assert_eq!(
                committed_rows.len(),
                K,
                "commit returned Ok but writer {writer}'s replacement row count was not visible"
            );
            let slots = committed_rows
                .iter()
                .map(|row| row.slot)
                .collect::<BTreeSet<_>>();
            assert_eq!(
                slots,
                BTreeSet::from([0, 1, 2]),
                "commit returned Ok but writer {writer}'s slot set was not exact"
            );
            for row in committed_rows {
                assert_eq!(
                    row.id,
                    row_id(group, row.slot),
                    "commit returned Ok but writer {writer}'s deterministic id was not visible"
                );
                assert_eq!(
                    row.payload,
                    format!("w{writer}"),
                    "commit returned Ok but another payload was visible before verification"
                );
                assert_eq!(
                    row.item_ref,
                    item_ref(group, row.slot, writer),
                    "commit returned Ok but writer {writer}'s item_ref was not visible"
                );
            }
            let marker = db
                .execute(
                    "SELECT writer, payload FROM replace_markers WHERE id = $id",
                    &params([("id", Value::Uuid(marker_id))]),
                )
                .expect("read committed replacement marker");
            assert_eq!(
                marker.rows,
                vec![vec![
                    Value::Int64(writer as i64),
                    Value::Text(format!("w{writer}"))
                ]],
                "commit returned Ok but did not publish the transaction's marker write"
            );
            return;
        }
        yield_now();
    }
    panic!("writer {writer} on group {group} exhausted retries; replacement race livelocked");
}

fn select_group(db: &Database, group: Uuid) -> Vec<TargetRow> {
    let rows = must_query(
        db.execute(
            "SELECT id, group_id, slot, item_ref, payload
             FROM replace_targets WHERE group_id = $g ORDER BY slot",
            &params([("g", Value::Uuid(group))]),
        ),
        "select group",
    )
    .rows;
    parse_target_rows(rows)
}

fn parse_target_rows(rows: Vec<Vec<Value>>) -> Vec<TargetRow> {
    rows.into_iter()
        .map(|row| match row.as_slice() {
            [
                Value::Uuid(id),
                Value::Uuid(group_id),
                Value::Int64(slot),
                Value::Uuid(item_ref),
                Value::Text(payload),
            ] => TargetRow {
                id: *id,
                group_id: *group_id,
                slot: *slot,
                item_ref: *item_ref,
                payload: payload.clone(),
            },
            other => panic!("unexpected replace_targets row shape: {other:?}"),
        })
        .collect()
}

fn select_group_payload_refs(db: &Database, group: Uuid) -> Vec<TargetRow> {
    let rows = must_query(
        db.execute(
            "SELECT id, group_id, slot, item_ref, payload
             FROM replace_targets WHERE group_id = $g ORDER BY slot",
            &params([("g", Value::Uuid(group))]),
        ),
        "select group payload refs",
    )
    .rows;
    parse_target_rows(rows)
}

fn writer_from_payload(payload: &str) -> usize {
    payload[1..]
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("payload did not encode a writer id: {payload}"))
}

fn assert_complete_writer_set(rows: &[TargetRow], group: Uuid, context: &str) -> String {
    assert_eq!(
        rows.len(),
        K,
        "{context}: expected exactly {K} rows, got {rows:?}"
    );
    let payloads: BTreeSet<_> = rows.iter().map(|row| row.payload.as_str()).collect();
    assert_eq!(
        payloads.len(),
        1,
        "{context}: replacement set must contain exactly one writer payload, got {rows:?}"
    );
    let payload = rows[0].payload.clone();
    assert!(
        payload == "bystander"
            || payload == "A"
            || payload == "B"
            || payload == "S0"
            || payload == "S1"
            || payload.starts_with('w')
            || payload.starts_with('u')
            || payload.starts_with('x'),
        "{context}: unexpected payload tag {payload:?}"
    );
    for (expected_slot, row) in (0..(K as i64)).zip(rows.iter()) {
        assert_eq!(row.group_id, group, "{context}: wrong group");
        assert_eq!(row.slot, expected_slot, "{context}: wrong slot order");
        assert_eq!(row.id, row_id(group, expected_slot), "{context}: wrong id");
        if let Some(writer) = payload
            .strip_prefix('w')
            .and_then(|rest| rest.parse::<usize>().ok())
        {
            assert_eq!(
                row.item_ref,
                item_ref(group, expected_slot, writer),
                "{context}: item_ref did not match writer {writer}"
            );
        }
    }
    payload
}

fn assert_seed_rows(rows: &[TargetRow], group: Uuid, writer: usize, payload: &str) {
    assert_eq!(
        rows.len(),
        K,
        "seeded bystander row count changed: {rows:?}"
    );
    for (slot, row) in (0..(K as i64)).zip(rows.iter()) {
        assert_eq!(
            *row,
            TargetRow {
                id: row_id(group, slot),
                group_id: group,
                slot,
                item_ref: item_ref(group, slot, writer),
                payload: payload.to_string(),
            }
        );
    }
}

fn assert_final_replace_state(db: &Database, group: Uuid, context: &str) -> String {
    let rows = select_group(db, group);
    let payload = assert_complete_writer_set(&rows, group, context);
    if payload.starts_with('w') {
        let winner = writer_from_payload(&payload);
        for row in rows {
            assert_eq!(
                row.item_ref,
                item_ref(group, row.slot, winner),
                "{context}: winner item_ref mismatch"
            );
        }
    }
    payload
}

fn open_file_db(name: &str) -> (TempDir, std::path::PathBuf, Database) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join(name);
    let db = Database::open(&path).expect("open file db");
    (dir, path, db)
}

fn setup_standard_file(name: &str) -> (TempDir, std::path::PathBuf, Arc<Database>, Uuid, Uuid) {
    let (dir, path, db) = open_file_db(name);
    create_replace_targets(&db);
    let group = Uuid::from_u128(0x6700);
    let bystander = Uuid::from_u128(0xB457);
    seed_group(&db, group, usize::MAX, "seed");
    seed_group(&db, bystander, usize::MAX - 1, "bystander");
    (dir, path, Arc::new(db), group, bystander)
}

fn run_replace_race(db: Arc<Database>, group: Uuid) {
    run_replace_race_rounds(db, group, ROUNDS);
}

fn run_replace_race_rounds(db: Arc<Database>, group: Uuid, rounds: usize) {
    let barrier = Arc::new(Barrier::new(WRITERS + 1));
    let abort = Arc::new(AtomicBool::new(false));
    let verify_lock = Arc::new(Mutex::new(()));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for writer in 0..WRITERS {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let abort = Arc::clone(&abort);
            let verify_lock = Arc::clone(&verify_lock);
            handles.push(scope.spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    barrier.wait();
                    for _ in 0..rounds {
                        if abort.load(Ordering::SeqCst) {
                            return;
                        }
                        replace_round_with_verify_lock(&db, group, writer, &verify_lock);
                    }
                }));
                if outcome.is_err() {
                    abort.store(true, Ordering::SeqCst);
                }
                outcome
            }));
        }
        barrier.wait();
        for handle in handles {
            match handle.join().expect("writer thread join failed") {
                Ok(()) => {}
                Err(panic) => {
                    abort.store(true, Ordering::SeqCst);
                    panic!("writer thread panicked: {}", panic_message(panic));
                }
            }
        }
    });
}

fn run_reader_probe(db: &Database, group: Uuid) {
    run_reader_probe_iterations(db, group, ROUNDS * 4);
}

fn run_reader_probe_iterations(db: &Database, group: Uuid, iterations: usize) {
    for _ in 0..iterations {
        for slot in 0..(K as i64) {
            let result = must_query(
                db.execute(
                    "SELECT id, payload FROM replace_targets WHERE id = $id",
                    &params([("id", Value::Uuid(row_id(group, slot)))]),
                ),
                "point reader must not error",
            );
            assert_eq!(
                result.rows.len(),
                1,
                "point reader saw a hole or duplicate for slot {slot}: {:?}",
                result.rows
            );
            match result.rows[0].as_slice() {
                [Value::Uuid(id), Value::Text(payload)] => {
                    assert_eq!(*id, row_id(group, slot));
                    assert!(
                        payload == "seed"
                            || payload.starts_with('w')
                            || payload.starts_with('u')
                            || payload.starts_with('x'),
                        "point reader saw unexpected payload {payload:?}"
                    );
                }
                other => panic!("unexpected point reader row: {other:?}"),
            }
            yield_now();
        }

        let rows = select_group_payload_refs(db, group);
        assert_eq!(rows.len(), K, "reader saw empty/partial set: {rows:?}");
        let slots: BTreeSet<_> = rows.iter().map(|row| row.slot).collect();
        assert_eq!(
            slots,
            BTreeSet::from([0, 1, 2]),
            "reader saw duplicate/out-of-range slots: {rows:?}"
        );
        let payloads: BTreeSet<_> = rows.iter().map(|row| row.payload.as_str()).collect();
        assert_eq!(payloads.len(), 1, "reader saw a mixed-writer set: {rows:?}");
        let payload = rows[0].payload.clone();
        if payload.starts_with('w') {
            let writer = writer_from_payload(&payload);
            for row in rows {
                assert_eq!(row.item_ref, item_ref(group, row.slot, writer));
            }
        }
        yield_now();
    }
}

fn run_replace_race_with_readers(db: Arc<Database>, group: Uuid, ctx: Option<Uuid>) {
    run_replace_race_with_readers_rounds(db, group, ctx, ROUNDS);
}

fn run_replace_race_with_readers_rounds(
    db: Arc<Database>,
    group: Uuid,
    ctx: Option<Uuid>,
    rounds: usize,
) {
    let barrier = Arc::new(Barrier::new(WRITERS + READERS + 1));
    let abort = Arc::new(AtomicBool::new(false));
    let verify_lock = Arc::new(Mutex::new(()));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for writer in 0..WRITERS {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let abort = Arc::clone(&abort);
            let verify_lock = Arc::clone(&verify_lock);
            handles.push(scope.spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    barrier.wait();
                    for _ in 0..rounds {
                        if abort.load(Ordering::SeqCst) {
                            return;
                        }
                        match ctx {
                            Some(ctx) => replace_scoped_round_with_verify_lock(
                                &db,
                                group,
                                writer,
                                ctx,
                                &verify_lock,
                            ),
                            None => {
                                replace_round_with_verify_lock(&db, group, writer, &verify_lock)
                            }
                        }
                    }
                }));
                if outcome.is_err() {
                    abort.store(true, Ordering::SeqCst);
                }
                outcome
            }));
        }
        for _ in 0..READERS {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let abort = Arc::clone(&abort);
            handles.push(scope.spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    barrier.wait();
                    if !abort.load(Ordering::SeqCst) {
                        run_reader_probe_iterations(&db, group, rounds * 4);
                    }
                }));
                if outcome.is_err() {
                    abort.store(true, Ordering::SeqCst);
                }
                outcome
            }));
        }
        barrier.wait();
        for handle in handles {
            match handle.join().expect("race participant join failed") {
                Ok(()) => {}
                Err(panic) => {
                    abort.store(true, Ordering::SeqCst);
                    panic!("race participant panicked: {}", panic_message(panic));
                }
            }
        }
    });
}

#[test]
fn sr_01_core_replace_race_file_backed() {
    // RED: current code can surface NotFound during a concurrent replace race.
    let (_dir, _path, db, group, bystander) = setup_standard_file("sr_01.db");
    run_replace_race(Arc::clone(&db), group);
    assert_final_replace_state(&db, group, "sr_01 final state");
    assert_seed_rows(
        &select_group(&db, bystander),
        bystander,
        usize::MAX - 1,
        "bystander",
    );
}

#[test]
fn sr_02_concurrent_readers_during_race() {
    // RED: readers must never observe NotFound, empty, partial, or mixed snapshots.
    let (_dir, _path, db, group, _bystander) = setup_standard_file("sr_02.db");
    run_replace_race_with_readers(db, group, None);
}

#[test]
fn sr_03_core_replace_race_in_memory() {
    // RED: the in-memory store has the same replacement contract.
    let db = Arc::new(Database::open_memory());
    create_replace_targets(&db);
    let group = Uuid::from_u128(0x6700);
    seed_group(&db, group, usize::MAX, "seed");
    run_replace_race(Arc::clone(&db), group);
    assert_final_replace_state(&db, group, "sr_03 final state");
}

#[test]
fn sr_04_update_replacement_race() {
    // RED: UPDATE/UPSERT races may downgrade to zero rows or typed conflicts, never NotFound.
    let (_dir, _path, db, group, _bystander) = setup_standard_file("sr_04.db");
    let barrier = Arc::new(Barrier::new(WRITERS + 1));
    let update_successes = Arc::new(AtomicUsize::new(0));
    let upsert_successes = Arc::new(AtomicUsize::new(0));
    let abort = Arc::new(AtomicBool::new(false));
    let verify_lock = Arc::new(Mutex::new(()));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for writer in 0..WRITERS {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let update_successes = Arc::clone(&update_successes);
            let upsert_successes = Arc::clone(&upsert_successes);
            let abort = Arc::clone(&abort);
            let verify_lock = Arc::clone(&verify_lock);
            handles.push(scope.spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    barrier.wait();
                    for _ in 0..ROUNDS {
                        if abort.load(Ordering::SeqCst) {
                            return;
                        }
                        match writer % 3 {
                            0 => replace_round_with_verify_lock(&db, group, writer, &verify_lock),
                            1 => {
                                for slot in 0..(K as i64) {
                                    let result = {
                                        let _verify_guard = lock_unpoisoned(&verify_lock);
                                        classify_query(db.execute(
                                            "UPDATE replace_targets SET payload = $p WHERE id = $id",
                                            &params([
                                                ("id", Value::Uuid(row_id(group, slot))),
                                                ("p", Value::Text(format!("u{writer}"))),
                                            ]),
                                        ))
                                    };
                                    if let Some(result) = result {
                                        assert!(
                                            matches!(result.rows_affected, 0 | 1),
                                            "UPDATE rows_affected must be 0 or 1, got {result:?}"
                                        );
                                        if result.rows_affected == 1 {
                                            update_successes.fetch_add(1, Ordering::SeqCst);
                                        }
                                    }
                                    yield_now();
                                }
                            }
                            _ => {
                                for slot in 0..(K as i64) {
                                    let result = {
                                        let _verify_guard = lock_unpoisoned(&verify_lock);
                                        classify_query(db.execute(
                                            "INSERT INTO replace_targets (id, group_id, slot, item_ref, payload)
                                     VALUES ($id, $g, $slot, $ref, $p)
                                     ON CONFLICT (id) DO UPDATE SET payload = $p",
                                            &params([
                                                ("id", Value::Uuid(row_id(group, slot))),
                                                ("g", Value::Uuid(group)),
                                                ("slot", Value::Int64(slot)),
                                                ("ref", Value::Uuid(item_ref(group, slot, writer))),
                                                ("p", Value::Text(format!("x{writer}"))),
                                            ]),
                                        ))
                                    };
                                    if let Some(result) = result {
                                        assert!(
                                            matches!(result.rows_affected, 0 | 1),
                                            "UPSERT rows_affected must be 0 or 1, got {result:?}"
                                        );
                                        if result.rows_affected == 1 {
                                            upsert_successes.fetch_add(1, Ordering::SeqCst);
                                        }
                                    }
                                    yield_now();
                                }
                            }
                        }
                    }
                }));
                if outcome.is_err() {
                    abort.store(true, Ordering::SeqCst);
                }
                outcome
            }));
        }
        barrier.wait();
        for handle in handles {
            match handle.join().expect("sr_04 writer join failed") {
                Ok(()) => {}
                Err(panic) => {
                    abort.store(true, Ordering::SeqCst);
                    panic!("sr_04 writer panicked: {}", panic_message(panic));
                }
            }
        }
    });

    assert!(
        update_successes.load(Ordering::SeqCst) > 0,
        "UPDATE writers must visibly mutate at least once"
    );
    assert!(
        upsert_successes.load(Ordering::SeqCst) > 0,
        "UPSERT writers must visibly mutate at least once"
    );
    let rows = select_group(&db, group);
    assert_eq!(rows.len(), K, "sr_04 final row count: {rows:?}");
    let slots: BTreeSet<_> = rows.iter().map(|row| row.slot).collect();
    assert_eq!(
        slots,
        BTreeSet::from([0, 1, 2]),
        "sr_04 final slot set must be exact: {rows:?}"
    );
    for row in rows {
        assert_eq!(row.id, row_id(group, row.slot));
        assert!(
            row.payload.starts_with('w')
                || row.payload.starts_with('u')
                || row.payload.starts_with('x'),
            "unexpected final payload after update race: {row:?}"
        );
        if row.payload.starts_with('w') {
            let writer = writer_from_payload(&row.payload);
            assert_eq!(row.item_ref, item_ref(group, row.slot, writer));
        } else {
            let allowed_refs = std::iter::once(item_ref(group, row.slot, usize::MAX))
                .chain(
                    (0..WRITERS)
                        .filter(|writer| writer % 3 != 1)
                        .map(|writer| item_ref(group, row.slot, writer)),
                )
                .collect::<BTreeSet<_>>();
            assert!(
                allowed_refs.contains(&row.item_ref),
                "UPDATE/UPSERT final row carried an impossible item_ref: {row:?}"
            );
        }
    }
}

#[test]
fn sr_05_same_tx_readback_under_committed_replacement() {
    // RED: current code returns a mixed committed+staged set instead of A's staged overlay.
    let (_dir, _path, db) = open_file_db("sr_05.db");
    create_replace_targets(&db);
    let group = Uuid::from_u128(0x6705);
    seed_group(&db, group, 0, "seed");

    let tx_a = db.begin().expect("tx_a begin");
    let tx_b = db.begin().expect("tx_b begin");
    let del_a = db
        .execute_in_tx(
            tx_a,
            "DELETE FROM replace_targets WHERE group_id = $g",
            &params([("g", Value::Uuid(group))]),
        )
        .expect("tx_a delete");
    assert_eq!(del_a.rows_affected, K as u64);
    for slot in 0..(K as i64) {
        insert_target_in_tx(&db, tx_a, group, slot, 10, "A").expect("tx_a insert");
    }

    db.execute_in_tx(
        tx_b,
        "DELETE FROM replace_targets WHERE group_id = $g",
        &params([("g", Value::Uuid(group))]),
    )
    .expect("tx_b delete");
    for slot in 0..(K as i64) {
        insert_target_in_tx(&db, tx_b, group, slot, 11, "B").expect("tx_b insert");
    }
    must_commit(db.commit(tx_b), "tx_b commit");

    let rows = must_query(
        db.execute_in_tx(
            tx_a,
            "SELECT id, group_id, slot, item_ref, payload
             FROM replace_targets WHERE group_id = $g ORDER BY slot",
            &params([("g", Value::Uuid(group))]),
        ),
        "tx_a read-your-own-writes",
    )
    .rows;
    let rows = parse_target_rows(rows);
    assert_eq!(rows.len(), K);
    for row in &rows {
        assert_eq!(row.payload, "A");
        assert_eq!(row.item_ref, item_ref(group, row.slot, 10));
    }

    match db.commit(tx_a) {
        Err(Error::UniqueViolation { table, column }) => {
            assert_eq!(table, "replace_targets");
            assert!(
                column.contains("id") || column.contains("group_id") || column.contains("slot"),
                "UniqueViolation must name a colliding key, got column={column:?}"
            );
            assert!(
                !column.contains("item_ref"),
                "item_ref is writer-distinct and must not be reported as the collision"
            );
        }
        other => panic!("tx_a commit must yield typed UniqueViolation, got {other:?}"),
    }
    let final_rows = select_group(&db, group);
    assert_eq!(final_rows.len(), K);
    assert!(final_rows.iter().all(|row| row.payload == "B"));
}

#[test]
fn sr_06_stale_staged_delete_is_commit_noop() {
    // RED: current file-backed persist path errors on stale staged deletes.
    let (_dir, _path, db) = open_file_db("sr_06.db");
    create_replace_targets(&db);
    let group_x = Uuid::from_u128(0x6706);
    let group_y = Uuid::from_u128(0x7606);
    seed_group(&db, group_x, 1, "seed");

    let tx_a = db.begin().expect("tx_a begin");
    let del_a = db
        .execute_in_tx(
            tx_a,
            "DELETE FROM replace_targets WHERE group_id = $gx",
            &params([("gx", Value::Uuid(group_x))]),
        )
        .expect("tx_a delete");
    assert_eq!(del_a.rows_affected, K as u64);
    for slot in 0..(K as i64) {
        insert_target_in_tx(&db, tx_a, group_y, slot, 10, "A").expect("tx_a insert group_y");
    }

    let tx_b = db.begin().expect("tx_b begin");
    db.execute_in_tx(
        tx_b,
        "DELETE FROM replace_targets WHERE group_id = $gx",
        &params([("gx", Value::Uuid(group_x))]),
    )
    .expect("tx_b delete");
    for offset in 0..(K as i64) {
        let slot = offset + K as i64;
        db.execute_in_tx(
            tx_b,
            "INSERT INTO replace_targets (id, group_id, slot, item_ref, payload)
             VALUES ($id, $g, $slot, $ref, $p)",
            &params([
                ("id", Value::Uuid(row_id(group_x, slot))),
                ("g", Value::Uuid(group_x)),
                ("slot", Value::Int64(slot)),
                ("ref", Value::Uuid(item_ref(group_x, slot, 11))),
                ("p", Value::Text("B".to_string())),
            ]),
        )
        .expect("tx_b replacement insert");
    }
    must_commit(db.commit(tx_b), "tx_b commit");
    must_commit(db.commit(tx_a), "tx_a stale delete commit");

    let group_y_rows = select_group(&db, group_y);
    assert_eq!(group_y_rows.len(), K);
    assert!(group_y_rows.iter().all(|row| row.payload == "A"));

    let group_x_rows = select_group(&db, group_x);
    assert_eq!(group_x_rows.len(), K);
    for (offset, row) in (0..(K as i64)).zip(group_x_rows.iter()) {
        let slot = offset + K as i64;
        assert_eq!(row.slot, slot);
        assert_eq!(row.payload, "B");
        assert_eq!(row.id, row_id(group_x, slot));
        assert_eq!(row.item_ref, item_ref(group_x, slot, 11));
    }
}

#[test]
fn sr_07_scoped_handle_replace_race() {
    // RED: scoped handles must not mask a replace-race NotFound as scope behavior.
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("sr_07.db");
    let group = Uuid::from_u128(0x6707);
    let ctx = Uuid::from_u128(0x42C7);
    {
        let admin = Database::open(&path).expect("open admin");
        create_scoped_replace_targets(&admin);
        seed_scoped_group(&admin, group, usize::MAX, "seed", ctx);
        admin.close().expect("close admin");
    }

    let scoped = Arc::new(
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx)]))
            .expect("open scoped"),
    );
    run_replace_race_with_readers_rounds(Arc::clone(&scoped), group, Some(ctx), 10);
    let winner = assert_final_replace_state(&scoped, group, "sr_07 scoped final");
    scoped.close().expect("close scoped");

    let admin = Database::open(&path).expect("reopen admin");
    let admin_rows = select_group(&admin, group);
    assert_complete_writer_set(&admin_rows, group, "sr_07 admin final");
    admin.close().expect("close admin after sanity read");

    let other_ctx = Uuid::from_u128(0x42C8);
    let hidden = Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(other_ctx)]))
        .expect("open other scoped");
    let winner_id = row_id(group, 0);
    match hidden.execute(
        "SELECT payload FROM replace_targets WHERE id = $id",
        &params([("id", Value::Uuid(winner_id))]),
    ) {
        Err(Error::ContextScopeViolation { .. }) => {}
        other => panic!(
            "hidden explicit-anchor read must return ContextScopeViolation, not NotFound; got {other:?}"
        ),
    }
    let list = hidden
        .execute(
            "SELECT payload FROM replace_targets WHERE group_id = $g",
            &params([("g", Value::Uuid(group))]),
        )
        .expect("hidden list read should silently filter");
    assert_eq!(list.rows.len(), 0);
    assert!(winner.starts_with('w') || winner == "seed");
}

#[test]
fn sr_08_sequential_replacement_convergence() {
    // REGRESSION GUARD: atomic replacement visibility without concurrency.
    let (_dir, _path, db) = open_file_db("sr_08.db");
    create_replace_targets(&db);
    let group = Uuid::from_u128(0x6708);
    seed_group(&db, group, 20, "S0");
    let tx = db.begin().expect("replace begin");
    db.execute_in_tx(
        tx,
        "DELETE FROM replace_targets WHERE group_id = $g",
        &params([("g", Value::Uuid(group))]),
    )
    .expect("delete S0");
    for slot in 0..(K as i64) {
        insert_target_in_tx(&db, tx, group, slot, 21, "S1").expect("insert S1");
    }
    db.commit(tx).expect("replace commit");
    let rows = select_group(&db, group);
    assert_eq!(rows.len(), K);
    for row in rows {
        assert_eq!(row.payload, "S1");
        assert_eq!(row.item_ref, item_ref(group, row.slot, 21));
    }
    let old = db
        .execute(
            "SELECT id FROM replace_targets WHERE payload = 'S0'",
            &empty(),
        )
        .expect("select old rows");
    assert_eq!(old.rows.len(), 0);

    db.execute(
        "CREATE TABLE replace_targets_text (
          id TEXT PRIMARY KEY,
          group_id UUID NOT NULL,
          slot INTEGER NOT NULL,
          item_ref TEXT NOT NULL,
          payload TEXT NOT NULL,
          UNIQUE (group_id, slot),
          UNIQUE (group_id, item_ref)
        )",
        &empty(),
    )
    .expect("create text table");
    for writer_payload in ["S0", "S1"] {
        let tx = db.begin().expect("text replace begin");
        db.execute_in_tx(
            tx,
            "DELETE FROM replace_targets_text WHERE group_id = $g",
            &params([("g", Value::Uuid(group))]),
        )
        .expect("text delete");
        let writer = if writer_payload == "S0" { 20 } else { 21 };
        for slot in 0..(K as i64) {
            db.execute_in_tx(
                tx,
                "INSERT INTO replace_targets_text (id, group_id, slot, item_ref, payload)
                 VALUES ($id, $g, $slot, $ref, $p)",
                &params([
                    ("id", Value::Text(text_row_id(group, slot))),
                    ("g", Value::Uuid(group)),
                    ("slot", Value::Int64(slot)),
                    ("ref", Value::Text(text_item_ref(group, slot, writer))),
                    ("p", Value::Text(writer_payload.to_string())),
                ]),
            )
            .expect("text insert");
        }
        db.commit(tx).expect("text commit");
    }
    let text_rows = db
        .execute(
            "SELECT id, group_id, slot, item_ref, payload
             FROM replace_targets_text WHERE group_id = $g ORDER BY slot",
            &params([("g", Value::Uuid(group))]),
        )
        .expect("select text rows")
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [
                Value::Text(id),
                Value::Uuid(group_id),
                Value::Int64(slot),
                Value::Text(item_ref),
                Value::Text(payload),
            ] => TextTargetRow {
                id: id.clone(),
                group_id: *group_id,
                slot: *slot,
                item_ref: item_ref.clone(),
                payload: payload.clone(),
            },
            other => panic!("unexpected text row shape: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(text_rows.len(), K);
    for row in text_rows {
        assert_eq!(row.payload, "S1");
        assert_eq!(row.id, text_row_id(group, row.slot));
        assert_eq!(row.item_ref, text_item_ref(group, row.slot, 21));
    }
}

#[test]
fn sr_09_reopen_after_race_winner_survives() {
    // RED: same root race as sr_01, plus durability of the converged winner.
    let (dir, path, db, group, _bystander) = setup_standard_file("sr_09.db");
    run_replace_race_rounds(Arc::clone(&db), group, 10);
    let captured = select_group(&db, group);
    assert_complete_writer_set(&captured, group, "sr_09 captured winner");
    db.close().expect("close before reopen");
    let reopened = Database::open(&path).expect("reopen");
    assert_eq!(select_group(&reopened, group), captured);
    drop(reopened);
    drop(dir);
}

#[test]
fn sr_10_trigger_callback_writes_during_race() {
    // REGRESSION GUARD: callback-initiated replacements obey the same no-NotFound contract.
    let (_dir, _path, db, group, bystander) = setup_standard_file("sr_10.db");
    db.execute(
        "CREATE TABLE signals (id UUID PRIMARY KEY, payload TEXT NOT NULL)",
        &empty(),
    )
    .expect("create signals");
    db.execute("CREATE TRIGGER sr_trigger ON signals WHEN INSERT", &empty())
        .expect("create trigger");

    let violations = Arc::new(Mutex::new(Vec::new()));
    let callback_counter = Arc::new(AtomicUsize::new(100));
    let violations_cb = Arc::clone(&violations);
    let callback_counter_cb = Arc::clone(&callback_counter);
    db.register_trigger_callback("sr_trigger", move |db_handle, ctx| {
        let writer = callback_counter_cb.fetch_add(1, Ordering::SeqCst);
        trigger_replace_round_logged(db_handle, ctx.tx, group, writer, &violations_cb);
        Ok(())
    })
    .expect("register trigger callback");
    db.complete_initialization()
        .expect("complete trigger initialization");

    let barrier = Arc::new(Barrier::new(WRITERS + READERS + 1));
    let abort = Arc::new(AtomicBool::new(false));
    let signal_successes = Arc::new(AtomicUsize::new(0));
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for writer in 0..WRITERS {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let abort = Arc::clone(&abort);
            let signal_successes = Arc::clone(&signal_successes);
            handles.push(scope.spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    barrier.wait();
                    for round in 0..ROUNDS {
                        if abort.load(Ordering::SeqCst) {
                            return;
                        }
                        let id = Uuid::from_u128(
                            0x5100_0000_0000_0000_0000_0000_0000_0000
                                ^ ((writer as u128) << 64)
                                ^ round as u128,
                        );
                        if let Some(result) = classify_query(db.execute(
                            "INSERT INTO signals (id, payload) VALUES ($id, $p)",
                            &params([
                                ("id", Value::Uuid(id)),
                                ("p", Value::Text(format!("s{writer}-{round}"))),
                            ]),
                        )) {
                            assert_eq!(result.rows_affected, 1);
                            signal_successes.fetch_add(1, Ordering::SeqCst);
                        }
                        yield_now();
                    }
                }));
                if outcome.is_err() {
                    abort.store(true, Ordering::SeqCst);
                }
                outcome
            }));
        }
        for _ in 0..READERS {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let abort = Arc::clone(&abort);
            handles.push(scope.spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    barrier.wait();
                    if !abort.load(Ordering::SeqCst) {
                        run_reader_probe(&db, group);
                    }
                }));
                if outcome.is_err() {
                    abort.store(true, Ordering::SeqCst);
                }
                outcome
            }));
        }
        barrier.wait();
        for handle in handles {
            match handle.join().expect("trigger race participant join failed") {
                Ok(()) => {}
                Err(panic) => {
                    abort.store(true, Ordering::SeqCst);
                    panic!(
                        "trigger race participant panicked: {}",
                        panic_message(panic)
                    );
                }
            }
        }
    });

    let logged = violations.lock().unwrap();
    assert!(
        logged.is_empty(),
        "trigger callback observed illegitimate replace-race errors: {logged:?}"
    );
    drop(logged);
    assert!(
        callback_counter.load(Ordering::SeqCst) > 100,
        "trigger callback must have executed replacement work"
    );
    assert!(
        signal_successes.load(Ordering::SeqCst) > 0,
        "at least one trigger-firing signal insert must commit"
    );
    let final_rows = select_group(&db, group);
    let winner = assert_complete_writer_set(&final_rows, group, "sr_10 final state");
    let writer = writer_from_payload(&winner);
    assert!(
        writer >= 100,
        "sr_10 final winner must be a trigger callback writer, got {winner}"
    );
    assert_seed_rows(
        &select_group(&db, bystander),
        bystander,
        usize::MAX - 1,
        "bystander",
    );
}
