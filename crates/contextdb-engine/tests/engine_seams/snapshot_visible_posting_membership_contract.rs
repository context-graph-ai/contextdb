//! A current indexed membership read must not re-check every obsolete TRUE
//! posting retained for historical snapshots.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, QueryResult};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

const LIVE_MEMBERS: i64 = 16;
const REPLACEMENT_ROUNDS: i64 = 48;

const EQUALITY_SQL: &str =
    "SELECT id, revision FROM posting_members WHERE active = TRUE ORDER BY slot";
const RANGE_SQL: &str = "SELECT id, revision FROM posting_members \
                         WHERE slot >= $low AND slot < $high ORDER BY slot";

#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn limits() -> ReadLimits {
    ReadLimits {
        result_rows: 1_024,
        result_bytes: 16 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 64,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn request(sql: &str, bound: HashMap<String, Value>) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, bound, limits(), Arc::new(FrozenClock))
}

fn ids_and_revisions(result: &QueryResult) -> BTreeSet<(i64, i64)> {
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id), Value::Int64(revision)] => (*id, *revision),
            other => panic!("expected id and revision from the production query, got {other:?}"),
        })
        .collect()
}

fn expected_members(first_id: i64, revision: i64) -> BTreeSet<(i64, i64)> {
    (0..LIVE_MEMBERS)
        .map(|slot| (first_id + slot, revision))
        .collect()
}

fn index_entries_touched(outcome: &bounded::TestResult) -> u64 {
    outcome
        .telemetry
        .source_work
        .get(&bounded::TestWorkSource::IndexRange)
        .copied()
        .expect("index work telemetry is mandatory for this declared-index query")
}

fn assert_current_membership(label: &str, result: &QueryResult, expected: &BTreeSet<(i64, i64)>) {
    assert_eq!(
        ids_and_revisions(result),
        *expected,
        "{label} must return exactly the fixed current TRUE members at their current versions"
    );
}

#[test]
fn current_index_membership_does_not_walk_retained_true_postings_after_replacements_and_reopen() {
    let root = tempfile::tempdir().expect("create durable posting fixture directory");
    let path = root.path().join("posting-members.redb");
    let db = Database::open(&path).expect("open durable posting fixture");
    db.execute(
        "CREATE TABLE posting_members (id INTEGER PRIMARY KEY, slot INTEGER, active BOOLEAN, revision INTEGER) HISTORY ALL",
        &HashMap::new(),
    )
    .expect("create HISTORY ALL membership table");
    db.execute(
        "CREATE INDEX posting_members_active_idx ON posting_members(active)",
        &HashMap::new(),
    )
    .expect("index current TRUE membership");
    db.execute(
        "CREATE INDEX posting_members_slot_idx ON posting_members(slot)",
        &HashMap::new(),
    )
    .expect("index the bounded membership range");

    for slot in 0..LIVE_MEMBERS {
        db.execute(
            "INSERT INTO posting_members (id, slot, active, revision) VALUES ($id, $slot, TRUE, 0)",
            &params([("id", Value::Int64(slot)), ("slot", Value::Int64(slot))]),
        )
        .expect("insert one initial TRUE member");
    }
    let initial_snapshot = db.snapshot();
    let initial_members = expected_members(0, 0);

    let baseline_equality = bounded::execute(&db, &request(EQUALITY_SQL, HashMap::new()))
        .expect("read initial TRUE membership through the bounded production path");
    assert_current_membership(
        "initial equality",
        &baseline_equality.result,
        &initial_members,
    );

    let mut current_ids = (0..LIVE_MEMBERS).collect::<Vec<_>>();
    let mut current_revisions = vec![0; LIVE_MEMBERS as usize];
    let mut middle_snapshot = None;
    let mut middle_members = None;
    for round in 0..REPLACEMENT_ROUNDS {
        let slot = round % LIVE_MEMBERS;
        let retired_id = current_ids[slot as usize];
        let tx = db.begin().expect("begin atomic membership replacement");
        db.execute_in_tx(
            tx,
            "UPDATE posting_members SET active = FALSE, slot = slot + 1000 WHERE id = $id",
            &params([("id", Value::Int64(retired_id))]),
        )
        .expect("replace one currently TRUE member with FALSE");
        let replacement_id = LIVE_MEMBERS + round;
        db.execute_in_tx(tx,
            "INSERT INTO posting_members (id, slot, active, revision) VALUES ($id, $slot, TRUE, $revision)",
            &params([
                ("id", Value::Int64(replacement_id)),
                ("slot", Value::Int64(slot)),
                ("revision", Value::Int64(round + 1)),
            ]),
        )
        .expect("insert one new TRUE member");
        db.commit(tx)
            .expect("publish both membership changes atomically");
        current_ids[slot as usize] = replacement_id;
        current_revisions[slot as usize] = round + 1;
        if round == REPLACEMENT_ROUNDS / 2 - 1 {
            middle_snapshot = Some(db.snapshot());
            middle_members = Some(
                current_ids
                    .iter()
                    .copied()
                    .zip(current_revisions.iter().copied())
                    .collect::<BTreeSet<_>>(),
            );
        }
    }
    let middle_snapshot = middle_snapshot.expect("capture a middle historical view");
    let middle_members = middle_members.expect("record the middle member versions");
    let current_members = current_ids
        .iter()
        .copied()
        .zip(current_revisions.iter().copied())
        .collect::<BTreeSet<_>>();

    assert_current_membership(
        "captured initial equality",
        &db.execute_at_snapshot(EQUALITY_SQL, &HashMap::new(), initial_snapshot)
            .expect("query the captured initial membership view"),
        &initial_members,
    );
    assert_current_membership(
        "captured middle equality",
        db.execute_at_snapshot(EQUALITY_SQL, &HashMap::new(), middle_snapshot)
            .as_ref()
            .expect("query the captured middle membership view"),
        &middle_members,
    );

    for (sql, bound, index) in [
        (EQUALITY_SQL, HashMap::new(), "posting_members_active_idx"),
        (
            RANGE_SQL,
            params([
                ("low", Value::Int64(0)),
                ("high", Value::Int64(LIVE_MEMBERS)),
            ]),
            "posting_members_slot_idx",
        ),
    ] {
        let warm =
            bounded::execute(&db, &request(sql, bound.clone())).expect("warm current membership");
        assert_eq!(warm.result.trace.index_used.as_deref(), Some(index));
        assert_current_membership("warm current", &warm.result, &current_members);
        assert_eq!(index_entries_touched(&warm), LIVE_MEMBERS as u64);
        let ordinary = db
            .execute(sql, &bound)
            .expect("ordinary current membership");
        assert_eq!(ordinary.trace.index_used.as_deref(), Some(index));
        assert_current_membership("ordinary current", &ordinary, &current_members);
        assert_eq!(ordinary.trace.rows_examined, LIVE_MEMBERS as u64);
    }

    db.close()
        .expect("close before reopening the durable fixture");
    drop(db);
    let db = Database::open(&path).expect("reopen durable posting fixture");

    assert_current_membership(
        "reopened captured initial equality",
        &db.execute_at_snapshot(EQUALITY_SQL, &HashMap::new(), initial_snapshot)
            .expect("reopen must preserve the captured initial membership view"),
        &initial_members,
    );

    let equality = bounded::execute(&db, &request(EQUALITY_SQL, HashMap::new()))
        .expect("read current TRUE membership through the bounded production path after reopen");
    assert_current_membership("reopened equality", &equality.result, &current_members);

    let range = bounded::execute(
        &db,
        &request(
            RANGE_SQL,
            params([
                ("low", Value::Int64(0)),
                ("high", Value::Int64(LIVE_MEMBERS)),
            ]),
        ),
    )
    .expect("read current TRUE membership range through the bounded production path after reopen");
    assert_current_membership("reopened range", &range.result, &current_members);

    assert_eq!(
        equality.result.trace.index_used.as_deref(),
        Some("posting_members_active_idx")
    );
    assert_eq!(
        range.result.trace.index_used.as_deref(),
        Some("posting_members_slot_idx")
    );
    assert_current_membership(
        "reopened middle",
        &db.execute_at_snapshot(EQUALITY_SQL, &HashMap::new(), middle_snapshot)
            .unwrap(),
        &middle_members,
    );
    let equality_work = index_entries_touched(&equality);
    let range_work = index_entries_touched(&range);
    println!(
        "OBSERVED initial_index_work={} equality_index_work={} range_index_work={} live_members={} replacement_rounds={}",
        index_entries_touched(&baseline_equality),
        equality_work,
        range_work,
        LIVE_MEMBERS,
        REPLACEMENT_ROUNDS,
    );
    assert!(
        equality_work == LIVE_MEMBERS as u64 && range_work == LIVE_MEMBERS as u64,
        "current equality and range membership each have {} live TRUE members but inspected {equality_work} and {range_work} index entries after {REPLACEMENT_ROUNDS} TRUE-to-FALSE replacements; obsolete TRUE postings are still physical query work",
        LIVE_MEMBERS,
    );
}

#[test]
fn captured_current_cursor_keeps_its_image_across_same_id_changes_and_cancellation() {
    use contextdb_core::read_contract::OwnerReadCancellation;
    use std::num::NonZeroUsize;
    let root = tempfile::tempdir().unwrap();
    let accountant = Arc::new(contextdb_engine::memory_accounting::MemoryAccountant::no_limit());
    let db = Arc::new(
        Database::open_with_config(
            root.path().join("pinned-members.redb"),
            Arc::new(contextdb_engine::plugin::CorePlugin),
            accountant.clone(),
        )
        .unwrap(),
    );
    db.execute(
        "CREATE TABLE pinned_members (id INTEGER PRIMARY KEY, active BOOLEAN, revision INTEGER)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX pinned_active ON pinned_members(active)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX pinned_revision ON pinned_members(revision DESC)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..16 {
        db.execute(
            "INSERT INTO pinned_members VALUES ($id, TRUE, 0)",
            &params([("id", Value::Int64(id))]),
        )
        .unwrap();
    }
    // No sort materializer: this cursor must still owe physical index reads.
    let sql = "SELECT id, revision FROM pinned_members WHERE active = TRUE";
    let mut req = request(sql, HashMap::new());
    req.limits.cursor_page_rows = 1;
    let opened = bounded::open_cursor(db.clone(), &req).unwrap();
    assert!(opened.first_page.has_more);
    assert!(opened.telemetry.source_work[&bounded::TestWorkSource::IndexRange] < 16);
    let mut ordered_request = request(
        "SELECT id, revision FROM pinned_members ORDER BY revision DESC",
        HashMap::new(),
    );
    ordered_request.limits.cursor_page_rows = 1;
    let ordered = bounded::open_cursor(db.clone(), &ordered_request).unwrap();
    assert!(ordered.first_page.has_more);
    assert!(ordered.telemetry.source_work[&bounded::TestWorkSource::IndexRange] < 16);
    for revision in 1..=48 {
        db.execute(
            "UPDATE pinned_members SET active = FALSE, revision = $revision WHERE id = 12",
            &params([("revision", Value::Int64(revision))]),
        )
        .unwrap();
        db.execute(
            "UPDATE pinned_members SET active = TRUE WHERE id = 12",
            &HashMap::new(),
        )
        .unwrap();
    }
    let latest = bounded::execute(&db, &request(sql, HashMap::new())).unwrap();
    assert_eq!(index_entries_touched(&latest), 16);
    assert!(ids_and_revisions(&latest.result).contains(&(12, 48)));
    db.execute(
        "DROP INDEX pinned_active ON pinned_members",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX pinned_active ON pinned_members(revision)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "DROP INDEX pinned_revision ON pinned_members",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX pinned_revision ON pinned_members(active)",
        &HashMap::new(),
    )
    .unwrap();
    for mut opened in [opened, ordered] {
        let mut rows = opened.first_page.rows.clone();
        let cancel = OwnerReadCancellation::new();
        cancel.cancel();
        assert!(matches!(
            opened.cursor.fetch(NonZeroUsize::new(1), cancel),
            Err(bounded::TestError::Cancelled)
        ));
        let mut work = opened.telemetry.source_work[&bounded::TestWorkSource::IndexRange];
        loop {
            let fetched = opened
                .cursor
                .fetch(NonZeroUsize::new(1), OwnerReadCancellation::new())
                .unwrap();
            rows.extend(fetched.page.rows);
            work += fetched
                .telemetry
                .source_work
                .get(&bounded::TestWorkSource::IndexRange)
                .copied()
                .unwrap_or(0);
            if !fetched.page.has_more {
                break;
            }
        }
        assert_eq!(rows.len(), 16);
        assert_eq!(
            rows.into_iter()
                .map(|row| match row.as_slice() {
                    [Value::Int64(id), Value::Int64(rev)] => (*id, *rev),
                    other => panic!("{other:?}"),
                })
                .collect::<BTreeSet<_>>(),
            expected_members(0, 0)
        );
        assert_eq!(
            work, 16,
            "a captured current image survives writes and index replacement without walking new history"
        );
        opened.cursor.close().unwrap();
    }
    db.close().unwrap();
    drop(db);
    assert_eq!(
        accountant.usage().used,
        0,
        "the database and last cursor release every owned membership node"
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

#[test]
fn same_id_membership_and_vector_replacements_compose_with_rollback_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("vector-members.redb");
    let db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE vector_members (id INTEGER PRIMARY KEY, active BOOLEAN, revision INTEGER, embedding VECTOR(2)) HISTORY ALL", &HashMap::new()).unwrap();
    db.execute(
        "CREATE INDEX vector_members_active ON vector_members(active)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO vector_members VALUES (1, TRUE, 0, $vector)",
        &params([("vector", Value::Vector(vec![1.0, 0.0]))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO vector_members VALUES (2, TRUE, 0, $vector)",
        &params([("vector", Value::Vector(vec![0.5, 0.5]))]),
    )
    .unwrap();
    let sql = "SELECT id, revision FROM vector_members WHERE active = TRUE ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1";
    let bound = params([("query", Value::Vector(vec![1.0, 0.0]))]);
    let old = db.snapshot();
    assert_eq!(
        db.execute(sql, &bound).unwrap().rows,
        vec![vec![Value::Int64(1), Value::Int64(0)]]
    );
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "UPDATE vector_members SET active = FALSE, revision = 99, embedding = $vector WHERE id = 1",
        &params([("vector", Value::Vector(vec![0.0, 1.0]))]),
    )
    .unwrap();
    assert_eq!(
        db.execute_in_tx(tx, sql, &bound).unwrap().rows[0][0],
        Value::Int64(2)
    );
    assert_eq!(db.execute(sql, &bound).unwrap().rows[0][0], Value::Int64(1));
    db.rollback(tx).unwrap();
    assert_eq!(db.execute(sql, &bound).unwrap().rows[0][0], Value::Int64(1));
    for revision in 1..=32 {
        db.execute(
            "UPDATE vector_members SET revision = $revision, embedding = $vector WHERE id = 1",
            &params([
                ("revision", Value::Int64(revision)),
                ("vector", Value::Vector(vec![0.0, 1.0])),
            ]),
        )
        .unwrap();
    }
    for reopened in [false, true] {
        let owned;
        let current = if reopened {
            db.close().unwrap();
            owned = Database::open(&path).unwrap();
            &owned
        } else {
            &db
        };
        assert_eq!(
            current.execute(sql, &bound).unwrap().rows[0][0],
            Value::Int64(2)
        );
        let bounded = bounded::execute(current, &request(sql, bound.clone())).unwrap();
        assert_eq!(bounded.result.rows[0][0], Value::Int64(2));
        assert_eq!(index_entries_touched(&bounded), 2);
        assert_eq!(
            current.execute_at_snapshot(sql, &bound, old).unwrap().rows,
            vec![vec![Value::Int64(1), Value::Int64(0)]]
        );
        let historical = current
            .__with_snapshot_override_for_test(old, || {
                bounded::execute(current, &request(sql, bound.clone()))
            })
            .expect("bounded EXACT keeps the old vector identity after reopen");
        assert_eq!(
            historical.result.rows,
            vec![vec![Value::Int64(1), Value::Int64(0)]]
        );
        assert_eq!(
            current
                .execute(
                    "SELECT revision FROM vector_members WHERE id = 1",
                    &HashMap::new()
                )
                .unwrap()
                .rows[0][0],
            Value::Int64(32)
        );
    }
}

#[test]
fn membership_admission_refusal_publishes_no_row_or_vector_state() {
    use contextdb_core::Error;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("refused-members.redb");
    let db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE refused_members (id INTEGER PRIMARY KEY, active BOOLEAN, embedding VECTOR(2))", &HashMap::new()).unwrap();
    db.execute(
        "CREATE INDEX refused_active ON refused_members(active)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO refused_members VALUES (1, TRUE, $vector)",
        &params([("vector", Value::Vector(vec![1.0, 0.0]))]),
    )
    .unwrap();
    let snapshot = db.snapshot();
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "UPDATE refused_members SET active = FALSE, embedding = $vector WHERE id = 1",
        &params([("vector", Value::Vector(vec![0.0, 1.0]))]),
    )
    .unwrap();
    db.accountant()
        .set_budget(Some(db.accountant().usage().used))
        .unwrap();
    let result = db.commit(tx);
    assert!(
        matches!(result, Err(Error::MemoryBudgetExceeded { ref subsystem, ref operation, .. }) if subsystem == "relational_index" && operation == "membership"),
        "membership must be admitted before durability: {result:?}"
    );
    db.accountant().set_budget(None).unwrap();
    let _ = db.rollback(tx);
    assert_eq!(db.snapshot(), snapshot);
    let sql = "SELECT id, embedding FROM refused_members WHERE active = TRUE";
    let expected = vec![vec![Value::Int64(1), Value::Vector(vec![1.0, 0.0])]];
    assert_eq!(db.execute(sql, &HashMap::new()).unwrap().rows, expected);
    db.close().unwrap();
    drop(db);
    let db = Database::open(&path).unwrap();
    assert_eq!(db.execute(sql, &HashMap::new()).unwrap().rows, expected);
}

#[test]
fn new_membership_root_cannot_expose_half_applied_row_vector_commit() {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE atomic_members (id INTEGER PRIMARY KEY, active BOOLEAN, revision INTEGER, embedding VECTOR(2))", &HashMap::new()).unwrap();
    db.execute(
        "CREATE INDEX atomic_active ON atomic_members(active)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX atomic_revision ON atomic_members(revision)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO atomic_members VALUES (1, TRUE, 0, $vector)",
        &params([("vector", Value::Vector(vec![1.0, 0.0]))]),
    )
    .unwrap();
    let old = db.snapshot();
    let before_lsn = db.current_lsn();
    let pause = db.pause_after_relational_apply_for_test();
    let writer_db = db.clone();
    let writer = std::thread::spawn(move || {
        let tx = writer_db.begin().unwrap();
        writer_db
            .execute_in_tx(
                tx,
                "UPDATE atomic_members SET active = FALSE WHERE id = 1",
                &HashMap::new(),
            )
            .unwrap();
        writer_db
            .execute_in_tx(
                tx,
                "INSERT INTO atomic_members VALUES (2, TRUE, 1, $vector)",
                &params([("vector", Value::Vector(vec![0.0, 1.0]))]),
            )
            .unwrap();
        writer_db.commit(tx).unwrap();
    });
    assert!(
        pause.wait_until_reached(std::time::Duration::from_secs(30)),
        "writer reaches the interval after relational publication and before vector publication"
    );
    assert_eq!(db.current_lsn(), before_lsn);
    let vector_sql = "SELECT id, revision FROM atomic_members WHERE active = TRUE ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 2";
    let bound = params([("query", Value::Vector(vec![1.0, 0.0]))]);
    assert_eq!(
        db.execute(vector_sql, &bound).unwrap().rows,
        vec![vec![Value::Int64(1), Value::Int64(0)]]
    );
    assert_eq!(
        bounded::execute(&db, &request(vector_sql, bound.clone()))
            .unwrap()
            .result
            .rows,
        vec![vec![Value::Int64(1), Value::Int64(0)]]
    );
    assert!(
        db.execute(
            "SELECT id FROM atomic_members WHERE revision >= 1",
            &HashMap::new()
        )
        .unwrap()
        .rows
        .is_empty()
    );
    pause.release();
    writer.join().unwrap();
    assert_eq!(db.current_lsn().0, before_lsn.0 + 1);
    assert_eq!(
        db.execute(vector_sql, &bound).unwrap().rows,
        vec![vec![Value::Int64(2), Value::Int64(1)]]
    );
    assert_eq!(
        db.execute_at_snapshot(vector_sql, &bound, old)
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1), Value::Int64(0)]]
    );
}

#[test]
fn retired_partitions_keep_old_exact_membership_after_moves_delete_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("moved-members.redb");
    let db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE moved_members (id INTEGER PRIMARY KEY, scope TEXT NOT NULL, active BOOLEAN, embedding VECTOR(2) PARTITION_KEY (scope) MAX_PARTITIONS 4) HISTORY ALL", &HashMap::new()).unwrap();
    db.execute(
        "CREATE INDEX moved_active ON moved_members(active)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO moved_members VALUES (1, 'alpha', TRUE, $vector)",
        &params([("vector", Value::Vector(vec![1.0, 0.0]))]),
    )
    .unwrap();
    let original = db.snapshot();
    db.execute(
        "UPDATE moved_members SET scope = 'beta' WHERE id = 1",
        &HashMap::new(),
    )
    .unwrap();
    let moved = db.snapshot();
    db.execute(
        "UPDATE moved_members SET embedding = $vector WHERE id = 1",
        &params([("vector", Value::Vector(vec![0.0, 1.0]))]),
    )
    .unwrap();
    let replaced = db.snapshot();
    db.execute("DELETE FROM moved_members WHERE id = 1", &HashMap::new())
        .unwrap();
    for reopened in [false, true] {
        let owned;
        let current = if reopened {
            db.close().unwrap();
            owned = Database::open(&path).unwrap();
            &owned
        } else {
            &db
        };
        let sql = "SELECT id, scope, embedding FROM moved_members WHERE active = TRUE ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1";
        let bound = params([("query", Value::Vector(vec![1.0, 0.0]))]);
        assert!(current.execute(sql, &bound).unwrap().rows.is_empty());
        assert!(
            bounded::execute(current, &request(sql, bound.clone()))
                .unwrap()
                .result
                .rows
                .is_empty()
        );
        for (snapshot, scope, vector) in [
            (original, "alpha", vec![1.0, 0.0]),
            (moved, "beta", vec![1.0, 0.0]),
            (replaced, "beta", vec![0.0, 1.0]),
        ] {
            let expected = vec![vec![
                Value::Int64(1),
                Value::Text(scope.to_owned()),
                Value::Vector(vector),
            ]];
            assert_eq!(
                current
                    .execute_at_snapshot(sql, &bound, snapshot)
                    .unwrap()
                    .rows,
                expected,
                "ordinary reopened={reopened} snapshot={snapshot:?}"
            );
            let result = current
                .__with_snapshot_override_for_test(snapshot, || {
                    bounded::execute(current, &request(sql, bound.clone()))
                })
                .unwrap();
            assert_eq!(
                result.result.rows, expected,
                "bounded reopened={reopened} snapshot={snapshot:?}"
            );
        }
    }
}

/// Registration alone must protect a read that has not pulled any source.
#[test]
fn registered_snapshot_survives_commit_before_capture_and_index_replacement_before_first_pull() {
    use contextdb_core::read_contract::OwnerReadCancellation;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Interleave {
        db: Arc<Database>,
        committed: AtomicBool,
        replaced: AtomicBool,
    }
    impl bounded::ExecutionProbe for Interleave {
        fn before_work(&self, _: bounded::TestWorkSource, _: u64) {}
        fn cancellation_observed(&self, _: u64) {}
        fn after_snapshot_registration(&self) {
            assert!(!self.committed.swap(true, Ordering::SeqCst));
            self.db
                .execute(
                    "UPDATE captured_members SET active = FALSE, revision = 9 WHERE id = 12",
                    &HashMap::new(),
                )
                .unwrap();
        }
        fn after_source_capture(&self) {
            assert!(self.committed.load(Ordering::SeqCst));
            assert!(!self.replaced.swap(true, Ordering::SeqCst));
            self.db
                .execute(
                    "DROP INDEX captured_index ON captured_members",
                    &HashMap::new(),
                )
                .unwrap();
            self.db
                .execute(
                    "CREATE INDEX captured_index ON captured_members(id DESC)",
                    &HashMap::new(),
                )
                .unwrap();
        }
    }
    for ordered in [false, true] {
        let accountant =
            Arc::new(contextdb_engine::memory_accounting::MemoryAccountant::no_limit());
        let directory = tempfile::tempdir().unwrap();
        let db = Arc::new(
            Database::open_with_config(
                directory.path().join("capture.redb"),
                Arc::new(contextdb_engine::plugin::CorePlugin),
                accountant.clone(),
            )
            .unwrap(),
        );
        db.execute("CREATE TABLE captured_members (id INTEGER PRIMARY KEY, active BOOLEAN, revision INTEGER) HISTORY ALL", &HashMap::new()).unwrap();
        db.execute(
            if ordered {
                "CREATE INDEX captured_index ON captured_members(revision DESC)"
            } else {
                "CREATE INDEX captured_index ON captured_members(active)"
            },
            &HashMap::new(),
        )
        .unwrap();
        for id in 0..16 {
            db.execute(
                "INSERT INTO captured_members VALUES ($id, TRUE, 0)",
                &params([("id", Value::Int64(id))]),
            )
            .unwrap();
        }
        for revision in 1..=96 {
            db.execute(
                "UPDATE captured_members SET revision = $revision WHERE id = 12",
                &params([("revision", Value::Int64(revision))]),
            )
            .unwrap();
        }
        db.execute(
            "UPDATE captured_members SET revision = 0 WHERE id = 12",
            &HashMap::new(),
        )
        .unwrap();
        let probe = Arc::new(Interleave {
            db: db.clone(),
            committed: AtomicBool::new(false),
            replaced: AtomicBool::new(false),
        });
        let mut req = request(
            if ordered {
                "SELECT id, revision FROM captured_members ORDER BY revision DESC"
            } else {
                "SELECT id, revision FROM captured_members WHERE active = TRUE"
            },
            HashMap::new(),
        );
        req.limits.cursor_page_rows = 1;
        req.probe = Some(probe.clone());
        let mut opened = bounded::open_cursor(db.clone(), &req).unwrap();
        assert!(probe.replaced.load(Ordering::SeqCst));
        assert!(opened.first_page.has_more);
        let mut posting_work = opened
            .telemetry
            .source_work
            .get(&bounded::TestWorkSource::IndexRange)
            .copied()
            .unwrap_or_default();
        let mut rows = std::mem::take(&mut opened.first_page.rows);
        let cancel = OwnerReadCancellation::new();
        cancel.cancel();
        assert!(matches!(
            opened.cursor.fetch(NonZeroUsize::new(1), cancel),
            Err(bounded::TestError::Cancelled)
        ));
        loop {
            let page = opened
                .cursor
                .fetch(NonZeroUsize::new(1), OwnerReadCancellation::new())
                .unwrap();
            posting_work += page
                .telemetry
                .source_work
                .get(&bounded::TestWorkSource::IndexRange)
                .copied()
                .unwrap_or_default();
            rows.extend(page.page.rows);
            if !page.page.has_more {
                break;
            }
        }
        assert!(
            posting_work <= 18,
            "registered current membership must not walk retired postings: {posting_work}"
        );
        assert_eq!(rows.len(), 16);
        let actual = rows
            .into_iter()
            .map(|row| match row.as_slice() {
                [Value::Int64(id), Value::Int64(revision)] => (*id, *revision),
                other => panic!("{other:?}"),
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected_members(0, 0));
        let new = db
            .execute(
                "SELECT id, revision FROM captured_members WHERE active = TRUE",
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(new.rows.len(), 15);
        opened.cursor.close().unwrap();
        drop(opened);
        drop(req);
        drop(probe);
        db.close().unwrap();
        drop(db);
        assert_eq!(accountant.usage().used, 0);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }
}
