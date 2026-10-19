//! The consumer's per-context session limits keep exact ranks and fixed index work.
#![cfg(feature = "test-seams")]
use contextdb_core::{Error, Value};
use contextdb_engine::database::GroupRoute;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use uuid::Uuid;

const NEWEST: &str = "WITH ranked_sessions AS (SELECT context_id, connector_id, session_ref, event_count, latest_occurred_at, ROW_NUMBER() OVER (PARTITION BY context_id ORDER BY latest_occurred_at DESC, connector_id ASC, session_ref ASC) AS session_rank FROM event_context_sessions WHERE context_id IN ($recent_context_0, $recent_context_1) AND latest_occurred_at IS NOT NULL) SELECT context_id, connector_id, session_ref, event_count, latest_occurred_at FROM ranked_sessions WHERE (context_id = $rank_context_0 AND session_rank <= $rank_limit_0) OR (context_id = $rank_context_1 AND session_rank <= $rank_limit_1) ORDER BY context_id ASC, session_rank ASC";
fn bindings() -> HashMap<String, Value> {
    HashMap::from([
        ("recent_context_0".into(), context(1)),
        ("recent_context_1".into(), context(2)),
        ("rank_context_0".into(), context(1)),
        ("rank_context_1".into(), context(2)),
        ("rank_limit_0".into(), Value::Int64(2)),
        ("rank_limit_1".into(), Value::Int64(3)),
    ])
}
fn context(id: u128) -> Value {
    Value::Uuid(Uuid::from_u128(id))
}
fn execute(db: &Database, sql: &str) -> QueryResult {
    let answer = db
        .execute(sql, &bindings())
        .unwrap_or_else(|error| panic!("session statement {sql}: {error}"));
    if sql.starts_with("CREATE TABLE event_context_sessions ") {
        // Exercise the ranked Sort recognition boundaries before the unchanged
        // large-history fixture. These rows use the same public reader budget.
        use contextdb_core::read_memory::ReadMemoryBudget;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        #[derive(Debug)]
        struct RankedPeakBudget {
            source: Arc<dyn ReadMemoryBudget>,
            live: AtomicUsize,
            peak: AtomicUsize,
        }
        impl ReadMemoryBudget for RankedPeakBudget {
            fn try_reserve(
                &self,
                bytes: usize,
                subsystem: &'static str,
                operation: &'static str,
                hint: &'static str,
            ) -> contextdb_core::Result<()> {
                self.source.try_reserve(bytes, subsystem, operation, hint)?;
                let live = self.live.fetch_add(bytes, Ordering::SeqCst) + bytes;
                self.peak.fetch_max(live, Ordering::SeqCst);
                Ok(())
            }
            fn release(&self, bytes: usize) {
                self.live.fetch_sub(bytes, Ordering::SeqCst);
                self.source.release(bytes);
            }
        }
        let probe_directory = tempfile::tempdir().unwrap();
        let probe = Database::open(probe_directory.path().join("rank-sort-shapes.redb")).unwrap();
        probe.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        probe
            .execute(
                "CREATE TABLE rank_real_probe (id INT PRIMARY KEY, p REAL NOT NULL)",
                &HashMap::new(),
            )
            .unwrap();
        for id in 1..=2 {
            probe
                .execute(
                    &format!("INSERT INTO rank_real_probe VALUES ({id},1.0)"),
                    &HashMap::new(),
                )
                .unwrap();
        }
        probe
            .execute(
                "CREATE TABLE rank_int_probe (id INT PRIMARY KEY, p INT NOT NULL)",
                &HashMap::new(),
            )
            .unwrap();
        for start in (1..=256_i64).step_by(64) {
            let tx = probe.begin().unwrap();
            for id in start..(start + 64).min(257) {
                probe
                    .execute_in_tx(
                        tx,
                        "INSERT INTO rank_int_probe VALUES ($id,1)",
                        &HashMap::from([("id".into(), Value::Int64(id))]),
                    )
                    .unwrap();
            }
            probe.commit(tx).unwrap();
        }
        let real_sql = "WITH ranked AS (SELECT id,p,ROW_NUMBER() OVER (PARTITION BY p ORDER BY id) AS rn FROM rank_real_probe) SELECT id,p,rn FROM ranked WHERE p = 1.0 AND rn <= 1";
        let real_answer = probe
            .execute(real_sql, &HashMap::new())
            .map(|answer| answer.rows)
            .map_err(|error| error.to_string());
        let int_prefix = "WITH ranked AS (SELECT id,p,ROW_NUMBER() OVER (PARTITION BY p ORDER BY id) AS rn FROM rank_int_probe) SELECT id,p,rn FROM ranked";
        let no_ceiling = (0..256)
            .map(|_| "(rn >= 1)")
            .collect::<Vec<_>>()
            .join(" OR ");
        let no_ceiling_answer = probe
            .execute(
                &format!("{int_prefix} WHERE {no_ceiling} LIMIT 1"),
                &HashMap::new(),
            )
            .map(|answer| answer.rows)
            .map_err(|error| error.to_string());
        let mut peak_observations = Vec::new();
        for tail in [
            " WHERE p = 1 AND rn <= 256 LIMIT 1",
            " WHERE p = 1 AND id <= 256 LIMIT 1",
        ] {
            let peak_owner = Arc::new(RankedPeakBudget {
                source: probe.declared_reader_memory().unwrap(),
                live: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            });
            let peak_answer = probe
                .execute_with_read_memory(
                    &format!("{int_prefix}{tail}"),
                    &HashMap::new(),
                    peak_owner.clone(),
                )
                .map(|answer| answer.rows)
                .map_err(|error| error.to_string());
            peak_observations.push((peak_answer, peak_owner.peak.load(Ordering::SeqCst)));
        }
        let division_answer = probe
            .execute(
                &format!("{int_prefix} WHERE 10 / (2 - id) > 0 AND rn <= 1"),
                &HashMap::new(),
            )
            .map(|answer| answer.rows)
            .map_err(|error| error.to_string());
        let division_control = probe
            .execute(
                &format!("{int_prefix} WHERE 10 / (2 - id) > 0"),
                &HashMap::new(),
            )
            .map(|answer| answer.rows)
            .map_err(|error| error.to_string());
        // All shapes are observed before any regression assertion can stop the row.
        assert_eq!(
            (real_answer, no_ceiling_answer),
            (
                Ok(vec![vec![
                    Value::Int64(1),
                    Value::Float64(1.0),
                    Value::Int64(1)
                ]]),
                Ok(vec![vec![
                    Value::Int64(1),
                    Value::Int64(1),
                    Value::Int64(1)
                ]])
            ),
            "collect REAL and no-ceiling outcomes before the shared matrix oracle"
        );
        assert_eq!(
            peak_observations[0].0,
            Ok(vec![vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1)
            ]])
        );
        assert_eq!(peak_observations[0].0, peak_observations[1].0);
        assert!(
            peak_observations[0].1 <= peak_observations[1].1,
            "a cap that never binds has no greater actual reader peak than full Sort: {peak_observations:?}"
        );
        assert!(
            matches!(&division_control, Err(error) if error.to_ascii_lowercase().contains("division by zero")),
            "the unpruned control reaches the second row's division: {division_control:?}"
        );
        assert_eq!(
            division_answer, division_control,
            "rank recognition must preserve a later row's evaluation error"
        );
        probe.close().unwrap();
    }
    answer
}
fn session(id: u128, ordinal: usize) -> Vec<Value> {
    let (connector, name, stamp) = match ordinal {
        0 => ("a".into(), "b".into(), Value::Timestamp(1000)),
        1 => ("a".into(), "a".into(), Value::Timestamp(1000)),
        2 => ("b".into(), "a".into(), Value::Timestamp(1000)),
        3 => ("a".into(), "null-a".into(), Value::Null),
        4 => ("a".into(), "null-b".into(), Value::Null),
        5 => ("a".into(), "old-a".into(), Value::Timestamp(10)),
        6 => ("b".into(), "old-b".into(), Value::Timestamp(10)),
        7 => ("c".into(), "old-c".into(), Value::Timestamp(20)),
        8 => ("d".into(), "old-d".into(), Value::Timestamp(30)),
        9 => ("e".into(), "old-e".into(), Value::Timestamp(40)),
        n => ("z".into(), format!("filler-{n:05}"), Value::Timestamp(100)),
    };
    vec![
        context(id),
        Value::Text(connector),
        Value::Text(name),
        Value::Int64(1),
        stamp,
    ]
}
fn seed(db: &Database, id: u128, from: usize, to: usize) {
    for start in (from..to).step_by(256) {
        let tx = db.begin().expect("begin a finite session seed batch");
        for ordinal in start..(start + 256).min(to) {
            let row = session(id, ordinal);
            let params = ["context", "connector", "session", "count", "stamp"]
                .into_iter()
                .zip(row)
                .map(|(name, value)| (name.into(), value))
                .collect();
            db.execute_in_tx(tx,
                "INSERT INTO event_context_sessions VALUES ($context, $connector, $session, $count, $stamp)",
                &params).expect("stage the actual session row");
        }
        db.commit(tx)
            .expect("publish the finite session seed batch");
    }
}
fn expected(ordinals: &[usize]) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for (id, limit) in [(1, 2), (2, 3)] {
        rows.extend(
            ordinals
                .iter()
                .take(limit)
                .map(|ordinal| session(id, *ordinal)),
        );
    }
    rows
}
fn check(answer: QueryResult, rows: &[Vec<Value>], route: GroupRoute) -> Vec<u64> {
    assert_eq!(
        answer.rows, rows,
        "rows follow context ASC then exact session rank ASC"
    );
    assert_eq!(answer.trace.groups.len(), 2);
    if route == GroupRoute::Index {
        assert_eq!(answer.trace.physical_plan, "IndexScan");
        assert!(answer.trace.sort_elided);
    }
    answer
        .trace
        .groups
        .iter()
        .enumerate()
        .map(|(ordinal, group)| {
            assert_eq!(group.partition_key, "<redacted>");
            assert_eq!(group.route, route);
            let delivered = rows
                .iter()
                .filter(|row| row[0] == context(ordinal as u128 + 1))
                .count() as u64;
            assert_eq!(group.returned_rows, delivered);
            if route == GroupRoute::Index {
                assert_eq!(
                    group.examined_entries, delivered,
                    "the writable ordered walk touches exactly its delivered posting prefix"
                );
            }
            group.examined_entries
        })
        .collect()
}
fn oldest() -> String {
    NEWEST.replace("latest_occurred_at DESC", "latest_occurred_at ASC")
}
fn undated() -> String {
    NEWEST.replace("latest_occurred_at DESC, ", "").replace(
        "latest_occurred_at IS NOT NULL",
        "latest_occurred_at IS NULL",
    )
}

#[test]
fn grouped_session_top_n_has_exact_context_ranks_and_fixed_declared_index_work() {
    let directory = tempfile::tempdir().expect("writable ranked session fixture");
    let path = directory.path().join("sessions.db");
    let db = Database::open(&path).expect("open the writable file store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    execute(
        &db,
        "CREATE TABLE event_context_sessions (context_id UUID NOT NULL, connector_id TEXT NOT NULL, session_ref TEXT NOT NULL, event_count INTEGER NOT NULL, latest_occurred_at TIMESTAMP, PRIMARY KEY (context_id, connector_id, session_ref)) SYNC OFF",
    );
    execute(
        &db,
        "CREATE INDEX idx_event_context_sessions_recency ON event_context_sessions (context_id, latest_occurred_at DESC, connector_id ASC, session_ref ASC)",
    );
    for id in [1, 2] {
        seed(&db, id, 0, 10);
    }
    let newest_rows = expected(&[1, 0, 2]);
    let dated_rows = expected(&[5, 6, 7]);
    let null_rows = expected(&[3, 4]);
    let small_newest = check(execute(&db, NEWEST), &newest_rows, GroupRoute::Index);
    let small_undated = check(execute(&db, &undated()), &null_rows, GroupRoute::Index);
    check(execute(&db, &oldest()), &dated_rows, GroupRoute::Sort);
    execute(
        &db,
        "CREATE INDEX idx_event_context_sessions_oldest ON event_context_sessions (context_id, latest_occurred_at ASC, connector_id ASC, session_ref ASC)",
    );
    let small_oldest = check(execute(&db, &oldest()), &dated_rows, GroupRoute::Index);
    for id in [1, 2] {
        seed(&db, id, 10, 20_010);
    }
    assert_eq!(
        check(execute(&db, NEWEST), &newest_rows, GroupRoute::Index),
        small_newest
    );
    assert_eq!(
        check(execute(&db, &oldest()), &dated_rows, GroupRoute::Index),
        small_oldest
    );
    assert_eq!(
        check(execute(&db, &undated()), &null_rows, GroupRoute::Index),
        small_undated
    );
    seed(&db, 3, 0, 20_010);
    assert_eq!(
        check(execute(&db, NEWEST), &newest_rows, GroupRoute::Index),
        small_newest,
        "unselected contexts never enter the per-context newest prefix walk"
    );
    assert_eq!(
        check(execute(&db, &oldest()), &dated_rows, GroupRoute::Index),
        small_oldest
    );
    assert_eq!(
        check(execute(&db, &undated()), &null_rows, GroupRoute::Index),
        small_undated
    );
    let fallback = NEWEST.replace(
        "latest_occurred_at IS NOT NULL",
        "latest_occurred_at IS NOT NULL AND event_count > 0",
    );
    check(execute(&db, &fallback), &newest_rows, GroupRoute::Sort);
    assert_window_refusals(&db);
    assert_ranked_cursor_pages(&path, &newest_rows);
    let tx = db.begin().expect("begin ranked staged-row control");
    let mut staged = bindings();
    staged.insert("context".into(), context(1));
    db.execute_in_tx(
        tx,
        "INSERT INTO event_context_sessions VALUES ($context, 'staged', 'new', 1, $stamp)",
        &{
            let mut p = staged.clone();
            p.insert("stamp".into(), Value::Timestamp(2000));
            p
        },
    )
    .expect("stage a new first-ranked session");
    let mut inserted_rows = vec![
        vec![
            context(1),
            Value::Text("staged".into()),
            Value::Text("new".into()),
            Value::Int64(1),
            Value::Timestamp(2000),
        ],
        session(1, 1),
    ];
    inserted_rows.extend([1, 0, 2].into_iter().map(|n| session(2, n)));
    assert_eq!(
        db.execute_in_tx(tx, NEWEST, &bindings())
            .expect("rank with staged insert")
            .rows,
        inserted_rows
    );
    db.execute_in_tx(tx, "DELETE FROM event_context_sessions WHERE context_id = $context AND connector_id = 'a' AND session_ref = 'a'", &staged)
        .expect("stage deletion of the former first-ranked session");
    inserted_rows[1] = session(1, 0);
    assert_eq!(
        db.execute_in_tx(tx, NEWEST, &bindings())
            .expect("rank with staged insert and delete")
            .rows,
        inserted_rows
    );
    db.rollback(tx).expect("release the staged ranked control");
    check(execute(&db, NEWEST), &newest_rows, GroupRoute::Index);
    db.close().expect("close the writable ranked fixture");
}

fn assert_window_refusals(db: &Database) {
    for (sql, expected_form) in [
        (
            "SELECT RANK() OVER (PARTITION BY context_id ORDER BY latest_occurred_at DESC) FROM event_context_sessions",
            "RANK window",
        ),
        (
            "SELECT SUM(event_count) OVER (PARTITION BY context_id ORDER BY latest_occurred_at DESC) FROM event_context_sessions",
            "SUM window",
        ),
        (
            "SELECT ROW_NUMBER() OVER (PARTITION BY context_id) FROM event_context_sessions",
            "ROW_NUMBER without ORDER BY",
        ),
        (
            "SELECT ROW_NUMBER() OVER (PARTITION BY context_id ORDER BY latest_occurred_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM event_context_sessions",
            "explicit window frame",
        ),
        (
            "SELECT ROW_NUMBER() OVER named_rank FROM event_context_sessions WINDOW named_rank AS (PARTITION BY context_id ORDER BY latest_occurred_at DESC)",
            "named window",
        ),
    ] {
        assert!(
            matches!(db.execute(sql, &bindings()), Err(Error::WindowFunctionNotSupported { form }) if form == expected_form),
            "unsupported window form must have the exact typed refusal: {expected_form}"
        );
    }
}

fn assert_ranked_cursor_pages(path: &std::path::Path, expected_rows: &[Vec<Value>]) {
    let mut options = ReadSessionOptions::default();
    options.limits.cursor_page_rows = 1;
    let work_limit = options.limits.work;
    let reader = ReadSession::open_with_options(path, options)
        .expect("open the actual consumer reader on the writable store");
    let mut cursor = reader
        .open_cursor(NEWEST, &bindings())
        .expect("open ranked cursor with one-row pages");
    let mut rows = cursor.first_page().rows.clone();
    let mut more = cursor.first_page().has_more;
    let mut examined = HashMap::new();
    loop {
        let groups = &cursor.trace().groups;
        assert!(
            !groups.is_empty(),
            "group disclosures survive every delivered page"
        );
        assert_eq!(
            groups.iter().map(|group| group.returned_rows).sum::<u64>(),
            rows.len() as u64,
            "per-group returned rows count actual delivered rows through this page"
        );
        for group in groups {
            assert_eq!(group.route, GroupRoute::Index);
            assert_eq!(group.partition_key, "<redacted>");
            let previous = examined
                .insert(group.ordinal, group.examined_entries)
                .unwrap_or(0);
            assert!(
                group.examined_entries >= previous,
                "group examined work is cumulative across pages"
            );
            assert!(
                group.examined_entries <= work_limit,
                "proof descent and posting work stay inside the declared read work limit"
            );
        }
        if !more {
            break;
        }
        let page = cursor.fetch(None).expect("fetch the next ranked page");
        assert!(page.rows.len() <= 1);
        rows.extend(page.rows);
        more = page.has_more;
    }
    assert_eq!(rows, expected_rows);
    assert_eq!(cursor.trace().groups.len(), 2);
    cursor.close().expect("close the ranked cursor");
}
