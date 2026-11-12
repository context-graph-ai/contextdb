//! Propagation: rows that declared they follow a transition make it inside the statement that
//! makes the transition. So the transaction that ran the statement reads the rows as moved,
//! nobody else does until it commits, and a rollback undoes all of it together.

mod support;

use contextdb::{Database, Error, Uuid, Value, params};
use support::Run;

/// The status of every row of a table, in key order.
fn statuses(on: &impl Run, table: &str) -> Vec<String> {
    support::rows(on, &format!("SELECT status FROM {table} ORDER BY id"))
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Text(status)] => status.clone(),
            other => panic!("a status of {table} came back as {other:?}"),
        })
        .collect()
}

/// `count` times one status.
fn all(status: &str, count: usize) -> Vec<String> {
    vec![status.to_owned(); count]
}

/// A store with projects 1 to 3, all active, and 3,000 tasks: task `n` names project
/// `n % 3 + 1`. A task follows its project into the archive. `clause` is written after the
/// reference's `PROPAGATE SET invalidated`.
fn store_of_projects(name: &str, clause: &str) -> Database {
    let db = support::open(&support::folder(name));
    support::run(
        &db,
        "CREATE TABLE projects (id INTEGER PRIMARY KEY, status TEXT NOT NULL) \
         STATE MACHINE (status: active -> [archived])",
    );
    support::run(
        &db,
        &format!(
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, status TEXT NOT NULL, \
             project_id INTEGER REFERENCES projects(id) ON STATE archived PROPAGATE SET invalidated \
             {clause}) STATE MACHINE (status: active -> [invalidated, superseded])"
        ),
    );
    support::run(
        &db,
        "INSERT INTO projects (id, status) VALUES (1, 'active'), (2, 'active'), (3, 'active')",
    );
    let tx = db.transaction().expect("begin");
    for n in 0..3000_i64 {
        tx.execute(
            "INSERT INTO tasks (id, status, project_id) VALUES ($id, 'active', $project)",
            &params! { "id" => n, "project" => n % 3 + 1 },
        )
        .expect("insert a task");
    }
    tx.commit().expect("commit the tasks");
    db
}

/// The statuses the tasks have once the projects in `archived` are archived and nothing else
/// has happened.
fn tasks_after(archived: &[i64]) -> Vec<String> {
    (0..3000_i64)
        .map(|n| match archived.contains(&(n % 3 + 1)) {
            true => "invalidated".to_owned(),
            false => "active".to_owned(),
        })
        .collect()
}

#[test]
fn rows_follow_inside_the_statement_and_are_seen_and_undone_with_its_transaction() {
    let db = store_of_projects("propagation-in-transaction", "");

    // Rolled back: the project and every task that followed it are as they were.
    let tx = db.transaction().expect("begin");
    let archived = support::run(&tx, "UPDATE projects SET status = 'archived' WHERE id = 2");
    assert_eq!(archived.rows_affected(), 1, "the rows the statement named");
    // The transaction reads its own propagation, a thousand rows of it and no other row.
    assert_eq!(statuses(&tx, "tasks"), tasks_after(&[2]));
    assert_eq!(
        support::int(
            &tx,
            "SELECT COUNT(*) FROM tasks WHERE status = 'invalidated'"
        ),
        1000
    );
    // Nobody else does.
    assert_eq!(statuses(&db, "tasks"), all("active", 3000));
    assert_eq!(statuses(&db, "projects"), all("active", 3));
    tx.rollback();
    assert_eq!(statuses(&db, "tasks"), all("active", 3000));
    assert_eq!(statuses(&db, "projects"), all("active", 3));

    // Committed: the transition and the rows that followed it arrive together.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "UPDATE projects SET status = 'archived' WHERE id = 2");
    // A task that follows may still be changed by the transaction's next statement.
    support::run(&tx, "UPDATE tasks SET status = 'superseded' WHERE id = 0");
    tx.commit().expect("commit the transition");
    let mut wanted = tasks_after(&[2]);
    wanted[0] = "superseded".to_owned();
    assert_eq!(statuses(&db, "tasks"), wanted);
    assert_eq!(statuses(&db, "projects"), ["active", "archived", "active"]);

    // One statement that moves two projects moves the tasks of both. Task 0, of project 1, was
    // superseded, which lists no next state: it is left as it is and the rest follow.
    let both = support::run(&db, "UPDATE projects SET status = 'archived' WHERE id <> 2");
    assert_eq!(both.rows_affected(), 2);
    let mut wanted = all("invalidated", 3000);
    wanted[0] = "superseded".to_owned();
    assert_eq!(statuses(&db, "tasks"), wanted);

    assert_eq!(
        support::int(
            &db,
            "SELECT COUNT(*) FROM tasks WHERE status = 'invalidated'"
        ),
        2999
    );
}

#[test]
fn an_aborting_propagation_names_its_table_and_undoes_its_statement_and_no_other() {
    let db = store_of_projects("propagation-aborted", "ABORT ON FAILURE");
    // One task of project 1 cannot follow: superseded lists no next state.
    support::run(
        &db,
        "UPDATE tasks SET status = 'superseded' WHERE id = 1500",
    );
    let mut before = all("active", 3000);
    before[1500] = "superseded".to_owned();

    let aborted = |error: Error, how: &str| match error {
        Error::PropagationAborted { table, reason } => {
            assert_eq!(table, "tasks", "{how}");
            assert!(!reason.is_empty(), "{how}: the refusal gives no reason");
        }
        other => panic!("a propagation that cannot move a row, {how}, gave {other:?}"),
    };
    // On the writer: the project has not moved and neither has any of its 999 other tasks.
    aborted(
        support::refusal(&db, "UPDATE projects SET status = 'archived' WHERE id = 1"),
        "on the writer",
    );
    assert_eq!(statuses(&db, "projects"), all("active", 3));
    assert_eq!(statuses(&db, "tasks"), before);

    // Inside a transaction the refused statement is undone whole, the statements before it
    // stand, and the transaction commits them.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "UPDATE projects SET status = 'archived' WHERE id = 2");
    aborted(
        support::refusal(
            &tx,
            "UPDATE projects SET status = 'archived' WHERE id IN (1, 3)",
        ),
        "inside a transaction",
    );
    assert_eq!(statuses(&tx, "projects"), ["active", "archived", "active"]);
    let mut wanted = tasks_after(&[2]);
    wanted[1500] = "superseded".to_owned();
    assert_eq!(statuses(&tx, "tasks"), wanted);
    tx.commit().expect("commit the statement that stood");
    assert_eq!(statuses(&db, "projects"), ["active", "archived", "active"]);
    assert_eq!(statuses(&db, "tasks"), wanted);

    // A project all of whose tasks can follow is archived with them.
    support::run(&db, "UPDATE projects SET status = 'archived' WHERE id = 3");
    let mut wanted = tasks_after(&[2, 3]);
    wanted[1500] = "superseded".to_owned();
    assert_eq!(statuses(&db, "tasks"), wanted);
}

/// Declares `claims`, whose rows follow each other into `invalidated` along incoming `CITES`
/// edges, with this `clause` after the declaration, and the edge table beside it. Claim `n`
/// has the id `Uuid::from_u128(n)`.
fn store_of_claims(name: &str, claims: u128, clause: &str) -> Database {
    let db = support::open(&support::folder(name));
    support::run(
        &db,
        "CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, \
         target_id UUID NOT NULL, edge_type TEXT NOT NULL)",
    );
    support::run(
        &db,
        &format!(
            "CREATE TABLE claims (id UUID PRIMARY KEY, status TEXT NOT NULL) \
             STATE MACHINE (status: active -> [invalidated]) \
             PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated {clause}"
        ),
    );
    let tx = db.transaction().expect("begin");
    for n in 1..=claims {
        tx.execute(
            "INSERT INTO claims (id, status) VALUES ($id, 'active')",
            &params! { "id" => Uuid::from_u128(n) },
        )
        .expect("insert a claim");
    }
    tx.commit().expect("commit the claims");
    db
}

/// Writes the edge `id`: claim `source` cites claim `target`.
fn cites(on: &impl Run, id: i64, source: u128, target: u128) {
    on.run(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, 'CITES')",
        &params! {
            "id" => id, "source" => Uuid::from_u128(source), "target" => Uuid::from_u128(target),
        },
    )
    .expect("insert an edge");
}

fn invalidate(on: &impl Run, claim: u128) {
    on.run(
        "UPDATE claims SET status = 'invalidated' WHERE id = $id",
        &params! { "id" => Uuid::from_u128(claim) },
    )
    .expect("invalidate a claim");
}

#[test]
fn max_depth_reaches_rows_at_most_that_many_edges_from_the_changed_row() {
    // A chain of three: claim 2 cites claim 1, and claim 3 cites claim 2.
    let chain = |name: &str, clause: &str| {
        let db = store_of_claims(name, 3, clause);
        cites(&db, 1, 2, 1);
        cites(&db, 2, 3, 2);
        invalidate(&db, 1);
        statuses(&db, "claims")
    };
    // One edge away follows; two edges away is past MAX DEPTH 1 and does not.
    assert_eq!(
        chain("propagation-depth-one", "MAX DEPTH 1"),
        ["invalidated", "invalidated", "active"]
    );
    // With MAX DEPTH 2 both follow.
    assert_eq!(
        chain("propagation-depth-two", "MAX DEPTH 2"),
        ["invalidated", "invalidated", "invalidated"]
    );
}

/// Row `n` of a table whose rows follow each other along edges.
fn node(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

/// The table of edges, and `nodes` with rows 1 to `count` in the state `first`, declared with
/// `rules` after its columns.
fn store_of_nodes(name: &str, rules: &str, first: &str, count: u128) -> Database {
    let db = support::open(&support::folder(name));
    support::run(
        &db,
        "CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, \
         target_id UUID NOT NULL, edge_type TEXT NOT NULL)",
    );
    support::run(
        &db,
        &format!(
            "CREATE TABLE nodes (id UUID PRIMARY KEY, status TEXT NOT NULL, \
             batch INTEGER NOT NULL) {rules}"
        ),
    );
    for n in 1..=count {
        db.execute(
            "INSERT INTO nodes (id, status, batch) VALUES ($id, $status, 0)",
            &params! { "id" => node(n), "status" => first },
        )
        .expect("insert a node");
    }
    db
}

/// Joins node `from` to node `to` by an edge of type FEEDS.
fn feed(db: &Database, edge: i64, from: u128, to: u128) {
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) \
         VALUES ($id, $from, $to, 'FEEDS')",
        &params! { "id" => edge, "from" => node(from), "to" => node(to) },
    )
    .expect("insert an edge");
}

#[test]
fn a_follower_reachable_from_two_changed_rows_changes_once_by_the_shorter_way() {
    let db = store_of_nodes(
        "propagation-shorter-way",
        "STATE MACHINE (status: running -> [stopped]) \
         PROPAGATE ON EDGE FEEDS OUTGOING STATE stopped SET stopped MAX DEPTH 2",
        "running",
        6,
    );
    support::run(&db, "CREATE INDEX nodes_status ON nodes (status)");
    support::run(&db, "ALTER TABLE nodes SET MAINTAIN COUNT (status)");
    // 1 feeds 2, 2 feeds 3, 3 feeds 4 and 4 feeds 5; 6 feeds 3 as well. So node 3 is two
    // edges from node 1 and one edge from node 6.
    for (edge, from, to) in [(1, 1, 2), (2, 2, 3), (3, 3, 4), (4, 4, 5), (5, 6, 3)] {
        feed(&db, edge, from, to);
    }
    for n in [1, 6] {
        db.execute(
            "UPDATE nodes SET batch = 1 WHERE id = $id",
            &params! { "id" => node(n) },
        )
        .expect("mark a node");
    }
    let stopped = support::run(&db, "UPDATE nodes SET status = 'stopped' WHERE batch = 1");
    assert_eq!(stopped.rows_affected(), 2, "the rows the statement named");

    // Node 3 is one edge from a changed row, so node 4 is two and follows under MAX DEPTH 2.
    // Had node 3 been reached by way of node 2, node 4 would be three edges away and stay.
    // Node 5 is three edges away by the shortest way and stays.
    assert_eq!(
        statuses(&db, "nodes"),
        [
            "stopped", "stopped", "stopped", "stopped", "running", "stopped"
        ]
    );
    // Node 3 changed once: the kept count of each state is the number of rows in it.
    for (status, rows) in [("stopped", 5), ("running", 1)] {
        let count = support::run(
            &db,
            &format!("SELECT COUNT(*) FROM nodes WHERE status = '{status}'"),
        );
        assert_eq!(
            count.rows(),
            [[Value::Int64(rows)]],
            "rows that are {status}"
        );
        assert_eq!(count.rows_examined(), 0, "rows read to count {status}");
    }
}

#[test]
fn a_follower_the_handle_may_not_write_refuses_the_statement_by_kind_and_nothing_changes() {
    let (mine, theirs) = (Uuid::from_u128(1), Uuid::from_u128(2));
    let db = support::open(&support::folder("propagation-narrowed-writer"));
    support::run(
        &db,
        "CREATE TABLE projects (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, \
         status TEXT NOT NULL) STATE MACHINE (status: active -> [archived])",
    );
    support::run(
        &db,
        "CREATE TABLE tasks (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, \
         status TEXT NOT NULL, \
         project_id INTEGER REFERENCES projects(id) ON STATE archived PROPAGATE SET invalidated) \
         STATE MACHINE (status: active -> [invalidated])",
    );
    db.execute(
        "INSERT INTO projects (id, context_id, status) VALUES (1, $mine, 'active')",
        &params! { "mine" => mine },
    )
    .expect("insert the project");
    // Task 1 is in the Context the narrowed handle is given and task 2 is in another.
    for (id, context) in [(1_i64, mine), (2, theirs)] {
        db.execute(
            "INSERT INTO tasks (id, context_id, status, project_id) \
             VALUES ($id, $context, 'active', 1)",
            &params! { "id" => id, "context" => context },
        )
        .expect("insert a task");
    }

    let contexts = std::collections::BTreeSet::from([contextdb::ContextId::new(mine)]);
    let narrowed = db.scoped_with_contexts(contexts);
    match support::refusal(
        &narrowed,
        "UPDATE projects SET status = 'archived' WHERE id = 1",
    ) {
        Error::ContextScopeViolation { table } => assert_eq!(table, "tasks"),
        other => panic!("a follower in another Context refused the statement as {other:?}"),
    }
    assert_eq!(statuses(&db, "projects"), all("active", 1));
    assert_eq!(statuses(&db, "tasks"), all("active", 2));
}
