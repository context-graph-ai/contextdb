//! Rows that store the same vector share one graph point, and each is still
//! answered as its own row: INDEXED returns them in row order at their shared
//! score, filters each by the reader's snapshot, and never hides one behind
//! another's deletion. Rows that join a point later, in the tail, are answered
//! from the graph too. The answers equal EXACT's, resident and reopened.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

const DIMENSION: usize = 8;
/// Rows stored with one identical vector, nearest to the query.
const SHARED: usize = 30;
const DISTINCT: usize = 100;

fn vector(seed: usize) -> Vec<f32> {
    (0..DIMENSION)
        .map(|axis| {
            let mixed = (seed as u64 + 11)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .wrapping_add((axis as u64 + 5).wrapping_mul(0xbf58_476d_1ce4_e5b9));
            ((mixed ^ (mixed >> 29)) % 10_007) as f32 / 10_007.0 - 0.5
        })
        .collect()
}

/// The shared vector, and the query, which is close to it.
fn shared() -> Vec<f32> {
    vec![1.0; DIMENSION]
}

fn query() -> Vec<f32> {
    let mut query = shared();
    query[0] = 1.01;
    query
}

fn insert(db: &Database, seed: usize, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO shared_rows (id, embedding) VALUES ($id, $embedding)",
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
            ("embedding".to_owned(), Value::Vector(vector)),
        ]),
    )
    .expect("insert one row");
}

fn settle(db: &Database) {
    let mut last = None;
    for _ in 0..64 {
        last = Some(format!(
            "{:?}",
            db.run_maintenance_cycle()
                .expect("one caller-driven maintenance cycle")
        ));
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == 1 && status.remaining_partitions == 0 {
            return;
        }
    }
    panic!(
        "maintenance did not publish the graph: {last:?}; {:?}",
        db.vector_store_for_test().hnsw_maintenance_status()
    );
}

fn fixture(db: &Database) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE shared_rows (id UUID PRIMARY KEY, \
             embedding VECTOR({DIMENSION}) SEARCH_MODE INDEXED AUTO_INDEX_AT 1)"
        ),
        &HashMap::new(),
    )
    .expect("declare the table");
    for seed in 1..=SHARED {
        insert(db, seed, shared());
    }
    for seed in SHARED + 1..=SHARED + DISTINCT {
        insert(db, seed, vector(seed));
    }
    settle(db);
}

fn search_at(
    db: &Database,
    mode: &str,
    snapshot: Option<contextdb_core::SnapshotId>,
) -> QueryResult {
    search_labelled(db, mode, snapshot, "")
}

fn search_labelled(
    db: &Database,
    mode: &str,
    snapshot: Option<contextdb_core::SnapshotId>,
    label: &str,
) -> QueryResult {
    let sql = format!(
        "SELECT id FROM shared_rows ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
    );
    let params = HashMap::from([("query".to_owned(), Value::Vector(query()))]);
    match snapshot {
        Some(snapshot) => db.execute_at_snapshot(&sql, &params, snapshot),
        None => db.execute(&sql, &params),
    }
    .unwrap_or_else(|error| {
        let (_, receipts) = contextdb_vector::hnsw::with_union_search_receipts(|| match snapshot {
            Some(snapshot) => db.execute_at_snapshot(&sql, &params, snapshot),
            None => db.execute(&sql, &params),
        });
        panic!("{label}: the {mode} search executes: {error:?}; receipts {receipts:?}")
    })
}

fn ids(result: &QueryResult) -> Vec<Value> {
    result.rows.iter().map(|row| row[0].clone()).collect()
}

fn assert_indexed_equals_exact(
    db: &Database,
    snapshot: Option<contextdb_core::SnapshotId>,
    label: &str,
) {
    assert_mode_equals_exact(db, "INDEXED", snapshot, label);
}

/// An earlier reader is answered from the graph its snapshot can walk, on
/// an in-memory store as on a saved one, so it is checked under INDEXED.
fn earlier_reader_mode(_label: &str) -> &'static str {
    "INDEXED"
}

fn assert_mode_equals_exact(
    db: &Database,
    mode: &str,
    snapshot: Option<contextdb_core::SnapshotId>,
    label: &str,
) {
    assert_eq!(
        ids(&search_labelled(db, mode, snapshot, label)),
        ids(&search_labelled(db, "EXACT", snapshot, label)),
        "{label}: INDEXED answers each shared row as EXACT does"
    );
}

fn delete(db: &Database, seed: usize) {
    db.execute(
        "DELETE FROM shared_rows WHERE id = $id",
        &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128)))]),
    )
    .expect("delete one row");
}

fn assert_rows_share_and_answer(db: &Database, label: &str) {
    assert_indexed_equals_exact(db, None, label);
    // Deleting the smallest shared rows leaves the next ones answered; a
    // reader that captured the store before the deletes still sees them.
    let before = db.snapshot();
    let _pin = db.pin_snapshot(before).expect("pin the earlier reader");
    for seed in 1..=5 {
        delete(db, seed);
    }
    assert_indexed_equals_exact(db, None, &format!("{label} after deletes"));
    assert_mode_equals_exact(
        db,
        earlier_reader_mode(label),
        Some(before),
        &format!("{label} earlier reader"),
    );
    assert_ne!(
        ids(&search_at(db, "INDEXED", None)),
        ids(&search_at(db, earlier_reader_mode(label), Some(before))),
        "{label}: the deletes change the current answer and not the earlier one"
    );
    // Rows that join the shared point later, in the tail, are answered too.
    for seed in 10_000..10_010 {
        insert(db, seed, shared());
    }
    assert_indexed_equals_exact(db, None, &format!("{label} tail members"));
}

#[test]
fn rows_sharing_one_graph_point_are_answered_per_row_and_per_snapshot() {
    let db = Database::open_memory();
    fixture(&db);
    assert_rows_share_and_answer(&db, "resident");

    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("shared-rows.redb");
    {
        let db = Database::open(&path).expect("open the durable fixture");
        fixture(&db);
        db.close().expect("close the fixture");
    }
    let reopened = Database::open(&path).expect("reopen the saved graph");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_rows_share_and_answer(&reopened, "reopened");
}

/// The column clauses each family below is declared with: F32, SQ8 and SQ4.
const REPRESENTATIONS: [&str; 3] = [
    "",
    " WITH (quantization = 'SQ8')",
    " WITH (quantization = 'SQ4')",
];

fn declare(db: &Database, table: &str, representation: &str, policy: &str) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE {table} (id UUID PRIMARY KEY, tag TEXT, \
             embedding VECTOR({DIMENSION}){representation} SEARCH_MODE INDEXED \
             AUTO_INDEX_AT 1{policy})"
        ),
        &HashMap::new(),
    )
    .expect("declare the table");
    db.execute(
        &format!("CREATE INDEX {table}_tag ON {table}(tag)"),
        &HashMap::new(),
    )
    .expect("index the filter column");
}

fn put(db: &Database, table: &str, seed: usize, vector: Vec<f32>) {
    db.execute(
        &format!("INSERT INTO {table} (id, tag, embedding) VALUES ($id, $tag, $embedding)"),
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
            (
                "tag".to_owned(),
                Value::Text(
                    if seed.is_multiple_of(2) {
                        "even"
                    } else {
                        "odd"
                    }
                    .to_owned(),
                ),
            ),
            ("embedding".to_owned(), Value::Vector(vector)),
        ]),
    )
    .expect("insert one row");
}

fn replace(db: &Database, table: &str, seed: usize, vector: Vec<f32>) {
    db.execute(
        &format!("UPDATE {table} SET embedding = $embedding WHERE id = $id"),
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
            ("embedding".to_owned(), Value::Vector(vector)),
        ]),
    )
    .expect("replace one row's vector");
}

fn remove(db: &Database, table: &str, seed: usize) {
    db.execute(
        &format!("DELETE FROM {table} WHERE id = $id"),
        &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128)))]),
    )
    .expect("delete one row");
}

fn settle_table(db: &Database, table: &str) {
    let mut last = None;
    for _ in 0..64 {
        last = Some(format!(
            "{:?}",
            db.run_maintenance_cycle()
                .expect("one caller-driven maintenance cycle")
        ));
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.remaining_partitions == 0 && status.ready_partitions >= 1 {
            return;
        }
    }
    let shown = db
        .execute(
            &format!("SHOW VECTOR_PARTITIONS FOR {table}.embedding"),
            &HashMap::new(),
        )
        .map(|shown| {
            shown
                .columns
                .iter()
                .cloned()
                .zip(shown.rows.first().cloned().unwrap_or_default())
                .collect::<Vec<_>>()
        });
    panic!(
        "maintenance did not publish the graph: {last:?}; {:?}; {shown:?}",
        db.vector_store_for_test().hnsw_maintenance_status()
    );
}

fn answer(
    db: &Database,
    table: &str,
    mode: &str,
    filter: &str,
    query: &[f32],
    snapshot: Option<contextdb_core::SnapshotId>,
    label: &str,
) -> Vec<Value> {
    let sql = format!(
        "SELECT id FROM {table} {filter} ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
    );
    let params = HashMap::from([("query".to_owned(), Value::Vector(query.to_vec()))]);
    let result = match snapshot {
        Some(snapshot) => db.execute_at_snapshot(&sql, &params, snapshot),
        None => db.execute(&sql, &params),
    }
    .unwrap_or_else(|error| {
        let (_, receipts) = contextdb_vector::hnsw::with_union_search_receipts(|| match snapshot {
            Some(snapshot) => db.execute_at_snapshot(&sql, &params, snapshot),
            None => db.execute(&sql, &params),
        });
        panic!("{label}: the {mode} search executes: {error:?}; receipts {receipts:?}")
    });
    ids(&result)
}

fn assert_complete_and_equal(
    db: &Database,
    table: &str,
    query: &[f32],
    snapshot: Option<contextdb_core::SnapshotId>,
    label: &str,
) {
    let mode = if snapshot.is_some() {
        earlier_reader_mode(label)
    } else {
        "INDEXED"
    };
    for filter in ["", "WHERE tag = 'even'"] {
        let indexed = answer(db, table, mode, filter, query, snapshot, label);
        let exact = answer(db, table, "EXACT", filter, query, snapshot, label);
        assert_eq!(
            indexed.len(),
            10,
            "{label} [{filter}]: INDEXED returns k rows"
        );
        assert_eq!(
            indexed, exact,
            "{label} [{filter}]: INDEXED answers as EXACT does"
        );
    }
}

/// One graph point can hold far more rows than the answer asks for. The rows
/// the walk keeps are rows, not points: a partition of forty rows storing one
/// vector — and forty all-zero rows — answers a complete ten-row INDEXED
/// answer equal to EXACT's, filtered and unfiltered, in each representation,
/// resident and reopened.
#[test]
fn a_point_shared_by_more_rows_than_the_answer_returns_every_row_asked_for() {
    let query = query();
    for representation in REPRESENTATIONS {
        for (family, stored) in [("one vector", shared()), ("all zero", vec![0.0; DIMENSION])] {
            let fill = |db: &Database| {
                declare(db, "one_point", representation, "");
                for seed in 1..=40 {
                    put(db, "one_point", seed, stored.clone());
                }
                settle_table(db, "one_point");
            };
            let label = format!("{family}{representation}");
            let db = Database::open_memory();
            fill(&db);
            assert_complete_and_equal(&db, "one_point", &query, None, &format!("{label} resident"));

            let root = TempDir::new().expect("create the durable fixture directory");
            let path = root.path().join("one-point.redb");
            {
                let db = Database::open(&path).expect("open the durable fixture");
                fill(&db);
                db.close().expect("close the fixture");
            }
            let reopened = Database::open(&path).expect("reopen the saved graph");
            reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            assert_complete_and_equal(
                &reopened,
                "one_point",
                &query,
                None,
                &format!("{label} reopened"),
            );
        }
    }
}

/// A row replaced since its graph point was built is answered at its current
/// vector, never at the shared point's distance. Sixty rows share one point,
/// more than the walk's breadth (EF_SEARCH 16); the eight smallest rows — the
/// point's own row among them — are replaced by far vectors. Before, their
/// old memberships were kept at the shared distance ahead of the unchanged
/// rows, and rescoring them left the answer short of the rows EXACT returns.
/// An earlier reader, pinned before the replacements, still sees the old
/// vectors.
#[test]
fn a_replaced_row_is_answered_at_its_current_vector_not_its_old_point() {
    let query = query();
    let far = vec![-1.0; DIMENSION];
    for representation in REPRESENTATIONS {
        let check = |db: &Database, label: &str| {
            let before = db.snapshot();
            let _pin = db.pin_snapshot(before).expect("pin the earlier reader");
            for seed in 1..=8 {
                replace(db, "replaced", seed, far.clone());
            }
            assert_complete_and_equal(db, "replaced", &query, None, &format!("{label} current"));
            assert_complete_and_equal(
                db,
                "replaced",
                &query,
                Some(before),
                &format!("{label} earlier reader"),
            );
            assert!(
                !answer(db, "replaced", "EXACT", "", &query, None, label)
                    .contains(&Value::Uuid(Uuid::from_u128(1))),
                "{label}: the replaced rows left the current answer"
            );
        };
        let fill = |db: &Database| {
            declare(db, "replaced", representation, " HNSW (EF_SEARCH = 16)");
            for seed in 1..=60 {
                put(db, "replaced", seed, shared());
            }
            for seed in 61..=100 {
                put(db, "replaced", seed, vector(seed));
            }
            settle_table(db, "replaced");
        };
        let db = Database::open_memory();
        fill(&db);
        check(&db, &format!("resident{representation}"));

        let root = TempDir::new().expect("create the durable fixture directory");
        let path = root.path().join("replaced.redb");
        {
            let db = Database::open(&path).expect("open the durable fixture");
            fill(&db);
            db.close().expect("close the fixture");
        }
        let reopened = Database::open(&path).expect("reopen the saved graph");
        reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        check(&reopened, &format!("reopened{representation}"));
    }
}

/// Deleted and replaced rows that shared a point are retained for older
/// readers exactly as deleted graph points are, and count toward the column's
/// declared CONSOLIDATION tombstone share. At a fixed fifty live rows on one
/// point, with twenty deleted and twenty new each round and maintenance driven
/// under the default share (10%), each pass a search makes over the point's
/// members (keeping rows, and counting the rows a walk that ran out of
/// frontier reached) reads at most live × (1 + share) of them, so a search
/// reads at most two such passes plus k, however much history accumulates.
#[test]
fn member_reads_stay_within_the_declared_tombstone_share_as_history_grows() {
    const LIVE: usize = 50;
    const K: usize = 10;
    let query = query();
    let db = Database::open_memory();
    declare(&db, "churn", "", " HNSW (EF_SEARCH = 64)");
    for seed in 1..=LIVE {
        put(&db, "churn", seed, shared());
    }
    settle_table(&db, "churn");
    let mut next = LIVE + 1;
    let mut oldest = 1;
    for round in 1..=8 {
        for _ in 0..20 {
            remove(&db, "churn", oldest);
            oldest += 1;
            put(&db, "churn", next, shared());
            next += 1;
        }
        settle_table(&db, "churn");
        contextdb_vector::hnsw::take_member_visits_for_test();
        let indexed = answer(&db, "churn", "INDEXED", "", &query, None, "churn");
        let visits = contextdb_vector::hnsw::take_member_visits_for_test() as usize;
        assert_eq!(
            indexed,
            answer(&db, "churn", "EXACT", "", &query, None, "churn"),
            "round {round}: INDEXED answers as EXACT does"
        );
        // An unfiltered walk that exhausts its frontier reads the point's
        // members twice: once to keep rows, once to count the rows it
        // reached against the live count. Each pass is bounded alike.
        println!("round {round}: {visits} member reads");
        assert!(
            visits >= K - 1,
            "round {round}: the members were read ({visits})"
        );
        assert!(
            visits <= 2 * (LIVE + LIVE / 10) + K,
            "round {round}: {visits} member reads exceed two passes of live x (1 + 10%) + k"
        );
    }
}

/// A partition with a row its graph does not connect is counted row by row,
/// members included, through the walk's one admitted row list: a large shared
/// point beside a cut-off row reaches that counting, INDEXED refuses with the
/// typed disconnected error, AUTO answers as EXACT does, and every byte the
/// counting took is given back.
#[test]
fn a_large_shared_point_beside_a_cut_off_row_is_counted_and_given_back() {
    let query = query();
    let db = Database::open_memory();
    declare(&db, "cut", "", " HNSW (EF_SEARCH = 400)");
    for seed in 1..=250 {
        put(&db, "cut", seed, shared());
    }
    for seed in 251..=300 {
        put(&db, "cut", seed, vector(seed));
    }
    settle_table(&db, "cut");
    answer(&db, "cut", "AUTO", "", &query, None, "warm");
    let settled = db.accountant().usage().used;
    let cut_row = db
        .scan("cut", db.snapshot())
        .expect("scan the table")
        .into_iter()
        .find_map(|row| match row.values.get("id") {
            Some(Value::Uuid(id)) if *id == Uuid::from_u128(300) => Some(row.row_id.0),
            _ => None,
        })
        .expect("the cut row is visible");
    let refused = contextdb_vector::hnsw::with_hnsw_unlinked_rows_for_test(&[cut_row], || {
        db.execute(
            "SELECT id FROM cut ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
            &HashMap::from([("query".to_owned(), Value::Vector(query.clone()))]),
        )
        .expect_err("INDEXED refuses a partition its graph does not connect")
    });
    assert!(
        matches!(
            refused,
            contextdb_core::Error::VectorIndexedPartitionDisconnected { .. }
        ),
        "{refused:?}"
    );
    assert_eq!(
        db.accountant().usage().used,
        settled,
        "the refusal gives back every byte"
    );
    let auto = contextdb_vector::hnsw::with_hnsw_unlinked_rows_for_test(&[cut_row], || {
        answer(&db, "cut", "AUTO", "", &query, None, "cut")
    });
    assert_eq!(auto, answer(&db, "cut", "EXACT", "", &query, None, "cut"));
    assert_eq!(
        db.accountant().usage().used,
        settled,
        "the answer gives back every byte"
    );
    // A member-caused growth of the counted rows that the budget refuses
    // refuses the statement, typed, sets nothing aside and charges nothing; a
    // retry answers completely. The cut found above set the partition aside;
    // one maintenance cycle repairs it so the walk reaches the counting again.
    db.run_maintenance_cycle()
        .expect("one caller-driven maintenance cycle");
    let settled = db.accountant().usage().used;
    contextdb_vector::hnsw::refuse_member_growth_once_for_test();
    let refused = contextdb_vector::hnsw::with_hnsw_unlinked_rows_for_test(&[cut_row], || {
        db.execute(
            "SELECT id FROM cut ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
            &HashMap::from([("query".to_owned(), Value::Vector(query.clone()))]),
        )
    });
    // Refused by the budget, not answered and not reported disconnected.
    match &refused {
        Err(contextdb_core::Error::VectorIndexedPartitionDisconnected { .. }) | Ok(_) => {
            panic!("the refused growth refuses the statement by type: {refused:?}")
        }
        Err(error) => assert!(
            format!("{error:?}").contains("Memory") || format!("{error:?}").contains("memory"),
            "a memory refusal: {error:?}"
        ),
    }
    assert_eq!(
        db.accountant().usage().used,
        settled,
        "the refusal charges nothing"
    );
    let shown = db
        .execute("SHOW VECTOR_PARTITIONS FOR cut.embedding", &HashMap::new())
        .expect("show the partition");
    let state = shown
        .columns
        .iter()
        .position(|column| column == "query_state")
        .unwrap();
    assert_eq!(
        shown.rows[0][state],
        Value::Text("ready".into()),
        "nothing is set aside"
    );
    let retried = contextdb_vector::hnsw::with_hnsw_unlinked_rows_for_test(&[cut_row], || {
        answer(&db, "cut", "AUTO", "", &query, None, "retry")
    });
    assert_eq!(
        retried,
        answer(&db, "cut", "EXACT", "", &query, None, "cut")
    );
}

/// A shared point is published in finite steps: a resident graph whose one
/// point holds six hundred rows, too large for a reader to load whole (the
/// fixture's reader memory is 1 MiB), is exported in batches of at most the
/// export's point limit (256 records), points and member identities together,
/// and the reopened store answers every row.
#[test]
fn a_point_shared_by_more_rows_than_one_batch_is_published_in_finite_batches() {
    let query = query();
    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("big-group.redb");
    {
        let mut options = contextdb_engine::DatabaseOpenOptions::default();
        options.owner_reads.limits.limits.memory = 1024 * 1024;
        let db = Database::open_with_options(&path, options).expect("open the durable fixture");
        declare(&db, "big_group", "", "");
        for seed in 1..=600 {
            put(&db, "big_group", seed, shared());
        }
        for seed in 601..=640 {
            put(&db, "big_group", seed, vector(seed));
        }
        contextdb_vector::hnsw::take_resident_export_batches_for_test();
        settle_table(&db, "big_group");
        let batches = contextdb_vector::hnsw::take_resident_export_batches_for_test();
        assert!(
            batches.len() >= 3,
            "the group is published over several batches: {batches:?}"
        );
        assert!(
            batches.iter().all(|records| *records <= 256),
            "no batch stores more than its limit: {batches:?}"
        );
        assert_eq!(
            batches.iter().sum::<usize>(),
            640,
            "every row is published once"
        );
        db.close().expect("close the fixture");
    }
    let reopened = Database::open(&path).expect("reopen the saved graph");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_complete_and_equal(&reopened, "big_group", &query, None, "reopened");
}

/// Replaced rows never crowd current rows out of the answer, whether the
/// replaced row was a point's own row or a row sharing it. Nine groups of two
/// identical rows sit near the query, nearer groups first, beside farther
/// rows; the walk keeps EF_SEARCH 16 rows. The point's own row of each of the
/// eight nearest groups is replaced by a far vector. Their old points still
/// hold the old distances: kept there, the eight stale rows and the eight
/// unchanged rows of those groups would fill the walk's sixteen places and
/// push the ninth group out, leaving rescoring with eight near rows where
/// EXACT has ten. Only the version a snapshot sees competes, so INDEXED
/// equals EXACT, current and for a reader pinned before the replacements,
/// resident and reopened, in each representation.
#[test]
fn replaced_point_rows_of_many_groups_do_not_crowd_out_current_rows() {
    let query = query();
    let near = |group: usize| {
        let mut vector = shared();
        vector[1] = 1.0 + 0.01 * (group as f32 + 1.0);
        vector
    };
    let far = vec![-1.0; DIMENSION];
    for representation in REPRESENTATIONS {
        let fill = |db: &Database| {
            declare(db, "groups", representation, " HNSW (EF_SEARCH = 16)");
            for group in 0..9 {
                // The first row of a group is its point's own row.
                put(db, "groups", group * 2 + 1, near(group));
                put(db, "groups", group * 2 + 2, near(group));
            }
            for seed in 100..140 {
                put(db, "groups", seed, vector(seed));
            }
            settle_table(db, "groups");
        };
        let check = |db: &Database, label: &str| {
            let before = db.snapshot();
            let _pin = db.pin_snapshot(before).expect("pin the earlier reader");
            for group in 0..8 {
                replace(db, "groups", group * 2 + 1, far.clone());
            }
            assert_complete_and_equal(db, "groups", &query, None, &format!("{label} current"));
            assert_complete_and_equal(
                db,
                "groups",
                &query,
                Some(before),
                &format!("{label} earlier reader"),
            );
        };
        let db = Database::open_memory();
        fill(&db);
        check(&db, &format!("resident{representation}"));

        let root = TempDir::new().expect("create the durable fixture directory");
        let path = root.path().join("groups.redb");
        {
            let db = Database::open(&path).expect("open the durable fixture");
            fill(&db);
            db.close().expect("close the fixture");
        }
        let reopened = Database::open(&path).expect("reopen the saved graph");
        reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        check(&reopened, &format!("reopened{representation}"));
    }
}

/// A filtered walk over a graph where no rows share a point reads no
/// membership at all: the candidates the filter supplies are never probed
/// against points that have no members. Resident and reopened.
#[test]
fn a_filtered_walk_over_a_graph_without_shared_points_reads_no_membership() {
    let query = query();
    let fill = |db: &Database| {
        declare(db, "distinct_rows", "", "");
        for seed in 1..=300 {
            put(db, "distinct_rows", seed, vector(seed));
        }
        settle_table(db, "distinct_rows");
    };
    let check = |db: &Database, label: &str| {
        contextdb_vector::hnsw::take_member_visits_for_test();
        let indexed = answer(
            db,
            "distinct_rows",
            "INDEXED",
            "WHERE tag = 'even'",
            &query,
            None,
            label,
        );
        assert_eq!(
            contextdb_vector::hnsw::take_member_visits_for_test(),
            0,
            "{label}: no membership is read"
        );
        assert_eq!(
            indexed,
            answer(
                db,
                "distinct_rows",
                "EXACT",
                "WHERE tag = 'even'",
                &query,
                None,
                label
            ),
            "{label}: the filtered answer equals EXACT"
        );
    };
    let db = Database::open_memory();
    fill(&db);
    check(&db, "resident");
    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("distinct.redb");
    {
        let db = Database::open(&path).expect("open the durable fixture");
        fill(&db);
        db.close().expect("close the fixture");
    }
    let reopened = Database::open(&path).expect("reopen the saved graph");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    check(&reopened, "reopened");
}

/// A filtered search over a saved graph reaches a shared point whose own row
/// the filter keeps and whose many rows it drops, beside a row the graph does
/// not connect. Counting the reached rows probes the filter's candidates
/// against that point and finds no member: each probe -- a miss too -- is a
/// work checkpoint of the statement. INDEXED refuses the cut partition, typed,
/// with nothing left charged, and AUTO answers as EXACT does.
#[test]
fn unsuccessful_member_probes_of_a_reopened_filtered_count_are_each_charged() {
    let query = query();
    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("probes.redb");
    {
        let db = Database::open(&path).expect("open the durable fixture");
        declare(&db, "probes", "", " HNSW (EF_SEARCH = 400)");
        let put_tagged = |seed: usize, tag: &str, vector: Vec<f32>| {
            db.execute(
                "INSERT INTO probes (id, tag, embedding) VALUES ($id, $tag, $embedding)",
                &HashMap::from([
                    ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
                    ("tag".to_owned(), Value::Text(tag.to_owned())),
                    ("embedding".to_owned(), Value::Vector(vector)),
                ]),
            )
            .expect("insert one row");
        };
        put_tagged(1, "keep", shared());
        for seed in 2..=250 {
            put_tagged(seed, "drop", shared());
        }
        for seed in 251..=300 {
            put_tagged(seed, "keep", vector(seed));
        }
        settle_table(&db, "probes");
        db.close().expect("close the fixture");
    }
    let db = Database::open(&path).expect("reopen the saved graph");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let cut_row = db
        .scan("probes", db.snapshot())
        .expect("scan the table")
        .into_iter()
        .find_map(|row| match row.values.get("id") {
            Some(Value::Uuid(id)) if *id == Uuid::from_u128(300) => Some(row.row_id.0),
            _ => None,
        })
        .expect("the cut row is visible");
    let filter = "WHERE tag = 'keep'";
    // A warm graph search loads what a search keeps resident; the ledger is
    // read after it.
    answer(&db, "probes", "INDEXED", filter, &query, None, "warm");
    let settled = db.accountant().usage().used;
    contextdb_vector::hnsw::take_most_member_reads_between_checkpoints_for_test();
    contextdb_vector::hnsw::take_member_visits_for_test();
    let refused = contextdb_vector::hnsw::with_hnsw_unlinked_rows_for_test(&[cut_row], || {
        db.execute(
            &format!(
                "SELECT id FROM probes {filter} ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10"
            ),
            &HashMap::from([("query".to_owned(), Value::Vector(query.clone()))]),
        )
    });
    assert!(
        matches!(
            refused,
            Err(contextdb_core::Error::VectorIndexedPartitionDisconnected { .. })
        ),
        "{refused:?}"
    );
    assert!(
        contextdb_vector::hnsw::take_member_visits_for_test() > 0,
        "the count probed the shared point"
    );
    assert_eq!(
        contextdb_vector::hnsw::take_most_member_reads_between_checkpoints_for_test(),
        1,
        "a work checkpoint precedes every probe, a miss included"
    );
    assert_eq!(
        db.accountant().usage().used,
        settled,
        "nothing stays charged"
    );
    let auto = contextdb_vector::hnsw::with_hnsw_unlinked_rows_for_test(&[cut_row], || {
        answer(&db, "probes", "AUTO", filter, &query, None, "auto")
    });
    assert_eq!(
        auto,
        answer(&db, "probes", "EXACT", filter, &query, None, "exact")
    );
}

fn retag(db: &Database, table: &str, seed: usize, tag: &str) {
    db.execute(
        &format!("UPDATE {table} SET tag = $tag WHERE id = $id"),
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
            ("tag".to_owned(), Value::Text(tag.to_owned())),
        ]),
    )
    .expect("change one row's scalar column");
}

/// A scalar-only UPDATE stores a row's unchanged vector again at the row's new
/// version; a replacement stores a new vector. Across nine three-row groups,
/// more rows than EF_SEARCH keeps, point rows and members are each restamped
/// or replaced -- one member nearer the query than any group, none equal to
/// it. Every row answers at the score of the vector its snapshot sees: INDEXED
/// returns k rows equal to EXACT's, filtered and not, for the current reader
/// and one pinned before the writes, resident, reopened, and after a restart
/// that leaves the writes unmaintained, in each representation.
#[test]
fn restamped_and_replaced_rows_of_shared_points_answer_at_their_visible_scores() {
    let query = query();
    let near = |group: usize| {
        let mut vector = shared();
        vector[1] = 1.0 + 0.01 * (group as f32 + 1.0);
        vector
    };
    let far = vec![-1.0; DIMENSION];
    let nearer = {
        let mut vector = shared();
        vector[0] = 1.005;
        vector
    };
    for representation in REPRESENTATIONS {
        let fill = |db: &Database| {
            declare(db, "restamped", representation, " HNSW (EF_SEARCH = 16)");
            for group in 0..9 {
                // The first row of a group is its point's own row.
                for row in 1..=3 {
                    put(db, "restamped", group * 3 + row, near(group));
                }
            }
            for seed in 100..140 {
                put(db, "restamped", seed, vector(seed));
            }
            settle_table(db, "restamped");
        };
        let write = |db: &Database| {
            for group in 0..9 {
                let point_row = group * 3 + 1;
                match group % 3 {
                    0 => {
                        retag(db, "restamped", point_row, "even");
                        retag(db, "restamped", point_row + 1, "even");
                    }
                    1 => {
                        replace(db, "restamped", point_row, far.clone());
                        retag(db, "restamped", point_row + 2, "even");
                    }
                    _ => replace(db, "restamped", point_row + 1, nearer.clone()),
                }
            }
        };
        let check = |db: &Database, label: &str| {
            let before = db.snapshot();
            let _pin = db.pin_snapshot(before).expect("pin the earlier reader");
            write(db);
            assert_complete_and_equal(db, "restamped", &query, None, &format!("{label} current"));
            assert_complete_and_equal(
                db,
                "restamped",
                &query,
                Some(before),
                &format!("{label} earlier reader"),
            );
        };
        let db = Database::open_memory();
        fill(&db);
        check(&db, &format!("resident{representation}"));

        let root = TempDir::new().expect("create the durable fixture directory");
        let path = root.path().join("restamped.redb");
        {
            let db = Database::open(&path).expect("open the durable fixture");
            fill(&db);
            db.close().expect("close the fixture");
        }
        let reopened = Database::open(&path).expect("reopen the saved graph");
        reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        check(&reopened, &format!("reopened{representation}"));
        reopened.close().expect("close after the writes");
        let restarted = Database::open(&path).expect("restart with the writes unmaintained");
        restarted.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_complete_and_equal(
            &restarted,
            "restamped",
            &query,
            None,
            &format!("restarted{representation}"),
        );
    }
}
