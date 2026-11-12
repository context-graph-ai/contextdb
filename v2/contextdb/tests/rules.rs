//! The rules a table declares, as a host meets them: each refusal comes as its kind with the
//! fields that say what was refused, and leaves the store as it was. A reference is checked
//! when its transaction commits.

mod support;

use contextdb::{Database, Error, Uuid, Value, params};
use support::Run;

/// A statement and the values it binds, run on a writer or inside one of its transactions.
fn bound(
    on: &impl Run,
    sql: &str,
    values: &std::collections::HashMap<String, Value>,
) -> contextdb::Result<contextdb::QueryResult> {
    on.run(sql, values)
}

/// Runs `refused` three ways, each of which must be refused, and hands every refusal to `check`:
/// as text on the writer, with its values bound on the writer, and inside a transaction that
/// stays usable and is then rolled back. A rule is a rule about rows, whichever way a write
/// arrives.
fn refused_every_way(
    db: &Database,
    text: &str,
    with_values: (&str, std::collections::HashMap<String, Value>),
    check: impl Fn(Error, &str),
) {
    check(support::refusal(db, text), "written into the statement");
    let (sql, values) = with_values;
    match bound(db, sql, &values) {
        Err(error) => check(error, "with its values bound"),
        Ok(result) => panic!("{sql}\nwas not refused with its values bound: {result:?}"),
    }
    let tx = db.transaction().expect("begin");
    check(support::refusal(&tx, text), "inside a transaction");
    // The transaction is as it was: it reads, and it has staged nothing.
    tx.rollback();
}

const TASKS: &str = "CREATE TABLE tasks (id INTEGER PRIMARY KEY, kind TEXT NOT NULL IMMUTABLE, \
                     status TEXT NOT NULL, note TEXT) \
                     STATE MACHINE (status: draft -> [active, rejected], active -> [superseded])";
const ALL_TASKS: &str = "SELECT id, kind, status, note FROM tasks ORDER BY id";

/// A store with `tasks` (a frozen column and a state machine) holding three rows, and `log`, a
/// table that is IMMUTABLE as a whole, holding one.
fn store_of_tasks(name: &str) -> Database {
    let db = support::open(&support::folder(name));
    support::run(&db, TASKS);
    support::run(
        &db,
        "INSERT INTO tasks (id, kind, status, note) VALUES (1, 'plan', 'draft', 'a'), \
         (2, 'plan', 'active', 'b'), (3, 'fix', 'rejected', 'c')",
    );
    support::run(
        &db,
        "CREATE TABLE log (id INTEGER PRIMARY KEY, entry TEXT NOT NULL) IMMUTABLE",
    );
    support::run(&db, "INSERT INTO log (id, entry) VALUES (1, 'first')");
    db
}

#[test]
fn an_immutable_refusal_names_the_table_or_the_column_and_changes_nothing() {
    let db = store_of_tasks("immutable-refusals");
    let tasks = support::rows(&db, ALL_TASKS);
    let log = support::rows(&db, "SELECT id, entry FROM log ORDER BY id");
    let declared = db.declaration("tasks").expect("the declaration of tasks");

    let whole_table = |error: Error, how: &str| match error {
        Error::ImmutableTable { table } => assert_eq!(table, "log", "{how}"),
        other => panic!("a change to an IMMUTABLE table, {how}, was refused as {other:?}"),
    };
    refused_every_way(
        &db,
        "UPDATE log SET entry = 'changed' WHERE id = 1",
        (
            "UPDATE log SET entry = $entry WHERE id = $id",
            params! { "entry" => "changed", "id" => 1_i64 },
        ),
        whole_table,
    );
    refused_every_way(
        &db,
        "DELETE FROM log WHERE id = 1",
        ("DELETE FROM log WHERE id = $id", params! { "id" => 1_i64 }),
        whole_table,
    );
    refused_every_way(
        &db,
        "INSERT INTO log (id, entry) VALUES (1, 'again') ON CONFLICT (id) DO UPDATE SET entry = 'again'",
        (
            "INSERT INTO log (id, entry) VALUES ($id, $entry) ON CONFLICT (id) DO UPDATE SET entry = $entry",
            params! { "id" => 1_i64, "entry" => "again" },
        ),
        whole_table,
    );

    let one_column = |error: Error, how: &str| match error {
        Error::ImmutableColumn { table, column } => {
            assert_eq!(
                (table.as_str(), column.as_str()),
                ("tasks", "kind"),
                "{how}"
            );
        }
        other => panic!("a change to an IMMUTABLE column, {how}, was refused as {other:?}"),
    };
    refused_every_way(
        &db,
        "UPDATE tasks SET note = 'z', kind = 'other' WHERE id = 1",
        (
            "UPDATE tasks SET note = $note, kind = $kind WHERE id = $id",
            params! { "note" => "z", "kind" => "other", "id" => 1_i64 },
        ),
        one_column,
    );
    refused_every_way(
        &db,
        "INSERT INTO tasks (id, kind, status) VALUES (1, 'plan', 'draft') \
         ON CONFLICT (id) DO UPDATE SET kind = 'other'",
        (
            "INSERT INTO tasks (id, kind, status) VALUES ($id, $kind, $status) \
             ON CONFLICT (id) DO UPDATE SET kind = $kind",
            params! { "id" => 1_i64, "kind" => "other", "status" => "draft" },
        ),
        one_column,
    );
    // The column cannot be taken out of the table or renamed either.
    for sql in [
        "ALTER TABLE tasks DROP COLUMN kind",
        "ALTER TABLE tasks DROP COLUMN kind CASCADE",
        "ALTER TABLE tasks RENAME COLUMN kind TO sort",
    ] {
        one_column(support::refusal(&db, sql), sql);
    }

    assert_eq!(support::rows(&db, ALL_TASKS), tasks);
    assert_eq!(
        support::rows(&db, "SELECT id, entry FROM log ORDER BY id"),
        log
    );
    assert_eq!(
        db.declaration("tasks").expect("the declaration of tasks"),
        declared
    );
}

#[test]
fn a_state_refusal_names_the_table_the_column_and_both_states() {
    let db = store_of_tasks("state-refusals");
    let tasks = support::rows(&db, ALL_TASKS);

    // (the row, the state asked for, the state the row is in)
    for (id, to, from) in [
        (2_i64, "draft", "active"),
        (1, "superseded", "draft"),
        (3, "active", "rejected"),
        (1, "paused", "draft"),
        (2, "active", "active"),
    ] {
        refused_every_way(
            &db,
            &format!("UPDATE tasks SET status = '{to}' WHERE id = {id}"),
            (
                "UPDATE tasks SET status = $to WHERE id = $id",
                params! { "to" => to, "id" => id },
            ),
            |error, how| {
                // The sentence a person reads names the table and the column too.
                assert_eq!(
                    error.to_string(),
                    format!("invalid state transition of tasks.status: {from} -> {to}"),
                    "row {id}, {how}"
                );
                match error {
                    Error::InvalidStateTransition {
                        table,
                        column,
                        from: said_from,
                        to: said_to,
                    } => assert_eq!(
                        (
                            table.as_str(),
                            column.as_str(),
                            said_from.as_str(),
                            said_to.as_str()
                        ),
                        ("tasks", "status", from, to),
                        "row {id}, {how}"
                    ),
                    other => panic!("moving row {id} from {from} to {to}, {how}, gave {other:?}"),
                }
            },
        );
    }
    assert_eq!(support::rows(&db, ALL_TASKS), tasks);

    // A statement over several rows is refused by the one that may not move, and names it.
    match support::refusal(&db, "UPDATE tasks SET status = 'active' WHERE id IN (1, 3)") {
        Error::InvalidStateTransition { from, to, .. } => {
            assert_eq!((from.as_str(), to.as_str()), ("rejected", "active"));
        }
        other => panic!("moving a rejected row was refused as {other:?}"),
    }
    assert_eq!(support::rows(&db, ALL_TASKS), tasks);

    // Inside a transaction a refused move leaves the moves before it standing.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "UPDATE tasks SET status = 'active' WHERE id = 1");
    assert!(matches!(
        support::refusal(&tx, "UPDATE tasks SET status = 'draft' WHERE id = 1"),
        Error::InvalidStateTransition { .. }
    ));
    support::run(&tx, "UPDATE tasks SET status = 'superseded' WHERE id = 1");
    tx.commit().expect("commit the two listed moves");
    assert_eq!(
        support::rows(&db, "SELECT status FROM tasks WHERE id = 1"),
        [[support::text("superseded")]]
    );

    // A declaration prints the states a row may leave, and those it may go to, in the order
    // they were written.
    support::run(
        &db,
        "CREATE TABLE jobs (id INTEGER PRIMARY KEY, status TEXT NOT NULL) \
         STATE MACHINE (status: zeta -> [b, a], draft -> active, active -> [superseded, archived])",
    );
    let declared = db.declaration("jobs").expect("the declaration of jobs");
    let wanted = "STATE MACHINE (status: zeta -> [b, a], draft -> [active], \
                  active -> [superseded, archived])";
    assert!(declared.contains(wanted), "not\n{wanted}\nbut\n{declared}");
}

const PROJECTS: &str = "CREATE TABLE projects (id INTEGER PRIMARY KEY, title TEXT)";
const WORK: &str = "CREATE TABLE work (id INTEGER PRIMARY KEY, project_id INTEGER REFERENCES projects(id), \
                    note TEXT)";

/// A store with `projects` 1 and 2 and `work`, whose rows reference a project: work 1 names
/// project 1.
fn store_of_work(name: &str) -> Database {
    let db = support::open(&support::folder(name));
    support::run(&db, PROJECTS);
    support::run(&db, WORK);
    support::run(
        &db,
        "INSERT INTO projects (id, title) VALUES (1, 'one'), (2, 'two')",
    );
    support::run(&db, "INSERT INTO work (id, project_id) VALUES (1, 1)");
    db
}

/// Asserts that an outcome is the refusal of a reference from `work`: with `still_referenced`,
/// a project that a row of `work` still names; without, a row of `work` whose project is not
/// there.
fn assert_reference_refused<T: std::fmt::Debug>(
    outcome: contextdb::Result<T>,
    still_referenced: bool,
    when: &str,
) {
    match outcome {
        Err(Error::ForeignKeyViolation {
            table,
            columns,
            parent,
            still_referenced: said,
            ..
        }) => assert_eq!(
            (table.as_str(), columns.as_str(), parent.as_str(), said),
            ("work", "project_id", "projects", still_referenced),
            "{when}"
        ),
        other => panic!("{when}: {other:?}"),
    }
}

const ALL_WORK: &str = "SELECT id, project_id FROM work ORDER BY id";
const ALL_PROJECTS: &str = "SELECT id FROM projects ORDER BY id";

#[test]
fn a_reference_is_checked_when_its_transaction_commits_and_a_refused_commit_leaves_nothing() {
    let db = store_of_work("reference-at-commit");

    // The row that references comes first and its parent after it: the commit sees both.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "INSERT INTO work (id, project_id) VALUES (2, 3)");
    support::run(&tx, "INSERT INTO projects (id, title) VALUES (3, 'three')");
    tx.commit().expect("commit a row and the parent it names");

    // A row whose parent never comes is taken by its statement, with the rows around it, and
    // refused by the commit. The transaction is gone and none of it was written.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "INSERT INTO projects (id, title) VALUES (4, 'four')");
    support::run(&tx, "INSERT INTO work (id, project_id) VALUES (3, 4)");
    support::run(&tx, "INSERT INTO work (id, project_id) VALUES (4, 404)");
    support::run(&tx, "UPDATE projects SET title = 'changed' WHERE id = 1");
    assert_reference_refused(
        tx.commit(),
        false,
        "a commit with a row whose parent is missing",
    );
    assert_eq!(
        support::rows(&db, ALL_WORK),
        [
            [Value::Int64(1), Value::Int64(1)],
            [Value::Int64(2), Value::Int64(3)]
        ]
    );
    assert_eq!(
        support::rows(&db, "SELECT id, title FROM projects ORDER BY id"),
        [
            vec![Value::Int64(1), support::text("one")],
            vec![Value::Int64(2), support::text("two")],
            vec![Value::Int64(3), support::text("three")],
        ]
    );

    // A statement on the writer is a transaction of its own, so it brings the refusal itself.
    let refused = db.execute(
        "INSERT INTO work (id, project_id) VALUES ($id, $project)",
        &params! { "id" => 9_i64, "project" => 404_i64 },
    );
    assert_eq!(
        refused.as_ref().err().map(ToString::to_string).as_deref(),
        Some("foreign key violation: work (project_id) = (404) has no row in projects"),
        "the refusal says which value has no parent"
    );
    assert_reference_refused(
        refused,
        false,
        "an insert on the writer of a row whose parent is missing",
    );
    // A reference that is NULL names nothing.
    support::run(&db, "INSERT INTO work (id, project_id) VALUES (9, NULL)");

    // A parent that another commit removed while this transaction was open is missing too.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "INSERT INTO work (id, project_id) VALUES (10, 2)");
    support::run(&db, "DELETE FROM projects WHERE id = 2");
    assert_reference_refused(
        tx.commit(),
        false,
        "a commit of a row whose parent was deleted meanwhile",
    );
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM work"), 3);
}

#[test]
fn a_parent_is_not_removed_or_given_another_key_under_a_row_that_names_it() {
    let db = store_of_work("parent-under-child");
    let work = support::rows(&db, ALL_WORK);
    let projects = support::rows(&db, ALL_PROJECTS);

    assert_reference_refused(
        db.execute("DELETE FROM projects WHERE id = 1", &params! {}),
        true,
        "deleting a project that work names",
    );
    assert_reference_refused(
        db.execute("UPDATE projects SET id = 7 WHERE id = 1", &params! {}),
        true,
        "giving a project that work names another key",
    );
    // In a transaction the statement is taken and the commit refuses.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "DELETE FROM projects WHERE id = 1");
    support::run(&tx, "INSERT INTO projects (id, title) VALUES (5, 'five')");
    assert_reference_refused(tx.commit(), true, "a commit that deletes a named project");
    assert_eq!(support::rows(&db, ALL_WORK), work);
    assert_eq!(support::rows(&db, ALL_PROJECTS), projects);

    // A project nothing named when it was deleted, and that a row committed meanwhile names.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "DELETE FROM projects WHERE id = 2");
    support::run(&db, "INSERT INTO work (id, project_id) VALUES (2, 2)");
    assert_reference_refused(
        tx.commit(),
        true,
        "a commit that deletes a project a row was pointed at meanwhile",
    );
    assert_eq!(support::rows(&db, ALL_PROJECTS), projects);

    // The rows that name it and the parent go together, in either order, in one transaction;
    // and a parent may be moved to another key together with the rows that name it.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "DELETE FROM projects WHERE id = 2");
    support::run(&tx, "DELETE FROM work WHERE id = 2");
    support::run(&tx, "UPDATE projects SET id = 7 WHERE id = 1");
    support::run(&tx, "UPDATE work SET project_id = 7 WHERE id = 1");
    tx.commit()
        .expect("commit a parent with the rows that name it");
    assert_eq!(
        support::rows(&db, ALL_WORK),
        [[Value::Int64(1), Value::Int64(7)]]
    );
    assert_eq!(support::rows(&db, ALL_PROJECTS), [[Value::Int64(7)]]);
}

#[test]
fn a_reference_refusal_names_every_column_of_the_reference() {
    // The kind is the same for every shape of reference (the scripts in `tests/sql/` hold the
    // shapes); the fields that say which reference was refused are checked here.
    let db = support::open(&support::folder("reference-columns-named"));
    support::run(
        &db,
        "CREATE TABLE windows (machine TEXT NOT NULL, slot INTEGER NOT NULL, \
         PRIMARY KEY (machine, slot))",
    );
    support::run(
        &db,
        "CREATE TABLE samples (id INTEGER PRIMARY KEY, machine TEXT, slot INTEGER, \
         FOREIGN KEY (machine, slot) REFERENCES windows(machine, slot))",
    );
    support::run(
        &db,
        "CREATE TABLE parts (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parts(id))",
    );
    support::run(
        &db,
        "INSERT INTO windows (machine, slot) VALUES ('m1', 1), ('m2', 2)",
    );
    support::run(
        &db,
        "INSERT INTO samples (id, machine, slot) VALUES (1, 'm1', 1)",
    );
    for (sql, table, columns, parent, still) in [
        // Each value has a parent of its own; the pair has none.
        (
            "INSERT INTO samples VALUES (4, 'm1', 2)",
            "samples",
            "machine, slot",
            "windows",
            false,
        ),
        (
            "DELETE FROM windows WHERE machine = 'm1'",
            "samples",
            "machine, slot",
            "windows",
            true,
        ),
        (
            "INSERT INTO parts VALUES (3, 404)",
            "parts",
            "parent_id",
            "parts",
            false,
        ),
    ] {
        match support::refusal(&db, sql) {
            Error::ForeignKeyViolation {
                table: said_table,
                columns: said_columns,
                parent: said_parent,
                still_referenced,
                ..
            } => assert_eq!(
                (
                    said_table.as_str(),
                    said_columns.as_str(),
                    said_parent.as_str(),
                    still_referenced
                ),
                (table, columns, parent, still),
                "{sql}"
            ),
            other => panic!("{sql}\nwas refused as {other:?}"),
        }
    }
}

const EDGES: &str = "CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, \
                     target_id UUID NOT NULL, edge_type TEXT NOT NULL) DAG('DEPENDS_ON')";

/// Inserts the edge `id`, from node `source` to node `target`.
fn insert_edge(
    on: &impl Run,
    id: i64,
    source: u128,
    target: u128,
    edge_type: &str,
) -> contextdb::Result<contextdb::QueryResult> {
    on.run(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, $type)",
        &params! {
            "id" => id, "source" => Uuid::from_u128(source), "target" => Uuid::from_u128(target),
            "type" => edge_type,
        },
    )
}

fn assert_cycle<T: std::fmt::Debug>(outcome: contextdb::Result<T>, when: &str) {
    match outcome {
        Err(Error::CycleDetected { table, edge_type }) => assert_eq!(
            (table.as_str(), edge_type.as_str()),
            ("edges", "DEPENDS_ON"),
            "{when}"
        ),
        other => panic!("{when}: {other:?}"),
    }
}

#[test]
fn a_cycle_is_refused_by_the_statement_and_by_the_commit_that_would_close_it() {
    let db = support::open(&support::folder("cycles"));
    support::run(&db, EDGES);
    insert_edge(&db, 1, 1, 2, "DEPENDS_ON").expect("1 -> 2");
    insert_edge(&db, 2, 2, 3, "DEPENDS_ON").expect("2 -> 3");
    let ids = "SELECT id FROM edges ORDER BY id";

    assert_cycle(
        insert_edge(&db, 3, 3, 1, "DEPENDS_ON"),
        "an edge back to the start, on the writer",
    );
    // Inside a transaction the statement that would close the cycle is refused, over the edges
    // the transaction itself has staged, and the transaction goes on.
    let tx = db.transaction().expect("begin");
    insert_edge(&tx, 3, 3, 4, "DEPENDS_ON").expect("3 -> 4");
    assert_cycle(
        insert_edge(&tx, 4, 4, 1, "DEPENDS_ON"),
        "an edge that closes a cycle over a staged edge",
    );
    insert_edge(&tx, 4, 4, 5, "DEPENDS_ON").expect("4 -> 5");
    tx.commit().expect("commit the edges that close no cycle");
    assert_eq!(
        support::rows(&db, ids),
        [1_i64, 2, 3, 4].map(|id| [Value::Int64(id)])
    );

    // Two transactions that each add one edge, neither of which closes a cycle where it was
    // written: the second to commit would close one, and is refused whole.
    let tx = db.transaction().expect("begin");
    insert_edge(&tx, 5, 5, 6, "DEPENDS_ON").expect("5 -> 6, staged");
    insert_edge(&tx, 6, 8, 9, "DEPENDS_ON").expect("8 -> 9, staged");
    insert_edge(&db, 7, 6, 1, "DEPENDS_ON").expect("6 -> 1, committed meanwhile");
    assert_cycle(
        tx.commit(),
        "a commit whose edge closes a cycle with a newer commit",
    );
    assert_eq!(
        support::rows(&db, ids),
        [1_i64, 2, 3, 4, 7].map(|id| [Value::Int64(id)])
    );
    // The same edges, written now, are refused by the statement.
    assert_cycle(
        insert_edge(&db, 5, 5, 6, "DEPENDS_ON"),
        "the same edge on the writer",
    );
}

fn insert_fan(db: &Database, hub: u128, first_leaf: u128, first_id: i64, count: usize) {
    let numbers: Vec<usize> = (0..count).collect();
    for batch in numbers.chunks(1000) {
        let rows: Vec<String> = batch
            .iter()
            .map(|n| {
                format!(
                    "({}, '{}', '{}', 'DEPENDS_ON')",
                    first_id + *n as i64,
                    Uuid::from_u128(hub),
                    Uuid::from_u128(first_leaf + *n as u128)
                )
            })
            .collect();
        support::run(
            db,
            &format!(
                "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES {}",
                rows.join(", ")
            ),
        );
    }
}

#[test]
fn a_cycle_through_a_node_with_1_500_edges_is_refused_and_an_edge_that_closes_none_is_accepted() {
    // Node 2 has 1,500 edges out, to the nodes 1000..2500, and node 1 has one edge, to node 2.
    const FAN: usize = 1_500;
    const LAST_LEAF: u128 = 1000 + FAN as u128 - 1;
    let db = support::open(&support::folder("cycle-through-wide-node"));
    support::run(&db, EDGES);
    insert_fan(&db, 2, 1000, 1000, FAN);
    insert_edge(&db, 1, 1, 2, "DEPENDS_ON").expect("1 -> 2");
    let count = || support::int(&db, "SELECT COUNT(*) FROM edges");
    assert_eq!(count(), FAN as i64 + 1);

    // The way round goes through the wide node and out by the last of its edges.
    assert_cycle(
        insert_edge(&db, 5000, LAST_LEAF, 1, "DEPENDS_ON"),
        "an edge from the last node the wide node reaches, back to the node before it",
    );
    assert_cycle(
        insert_edge(&db, 5000, LAST_LEAF, 2, "DEPENDS_ON"),
        "an edge from the last node the wide node reaches, back to the wide node",
    );
    assert_eq!(count(), FAN as i64 + 1);

    // An edge on from that node, and one more edge into the wide node, close no cycle.
    insert_edge(&db, 5000, LAST_LEAF, 9000, "DEPENDS_ON").expect("on from the last node");
    insert_edge(&db, 5001, 9001, 2, "DEPENDS_ON").expect("one more edge into the wide node");
    insert_edge(&db, 5002, 9000, 9001 + 1, "DEPENDS_ON").expect("on again");
    assert_eq!(count(), FAN as i64 + 4);
    assert_eq!(
        support::rows(&db, "SELECT id FROM edges WHERE id >= 5000 ORDER BY id"),
        [5000_i64, 5001, 5002].map(|id| [Value::Int64(id)])
    );
}

#[test]
fn a_cycle_check_too_large_for_the_memory_limit_is_refused_by_the_limit_and_writes_nothing() {
    // Node 2 reaches 20,000 nodes. An edge into node 2 is checked by going over all of them,
    // and what that holds does not fit in the room the limit leaves.
    const FAN: usize = 20_000;
    const ROOM: usize = 64 * 1024;
    let db = support::open(&support::folder("cycle-check-memory-limit"));
    support::run(&db, EDGES);
    insert_fan(&db, 2, 1000, 1000, FAN);
    let count = || support::int(&db, "SELECT COUNT(*) FROM edges");
    assert_eq!(count(), FAN as i64);

    db.set_memory_limit(Some(db.memory_used() + ROOM))
        .expect("set the limit");
    // The limit has room for an edge whose check goes nowhere.
    insert_edge(&db, 1, 500, 501, "DEPENDS_ON").expect("an edge between two new nodes");
    match insert_edge(&db, 2, 1, 2, "DEPENDS_ON") {
        Err(Error::MemoryBudgetExceeded { .. }) => {}
        other => panic!(
            "an edge whose check goes over {FAN} nodes with {ROOM} bytes of room gave {other:?}"
        ),
    }
    assert_eq!(count(), FAN as i64 + 1);
    assert!(support::rows(&db, "SELECT id FROM edges WHERE id = 2").is_empty());

    // It was the limit that refused it: the edge closes no cycle, and is written without one.
    db.set_memory_limit(None).expect("lift the limit");
    insert_edge(&db, 2, 1, 2, "DEPENDS_ON").expect("the same edge with no limit");
    assert_eq!(count(), FAN as i64 + 2);
}

#[test]
fn a_table_another_table_references_is_not_dropped_until_the_reference_is_gone() {
    let db = store_of_work("drop-referenced-parent");
    let declared = db
        .declaration("projects")
        .expect("the declaration of projects");
    let projects = support::rows(&db, "SELECT id FROM projects ORDER BY id");
    let work = support::int(&db, "SELECT COUNT(*) FROM work");

    for sql in ["DROP TABLE projects", "DROP TABLE IF EXISTS projects"] {
        match support::refusal(&db, sql) {
            Error::SchemaInvalid { .. } => {}
            other => panic!("{sql}\nwhile work references projects was refused as {other:?}"),
        }
    }
    assert_eq!(
        db.declaration("projects")
            .expect("the declaration of projects"),
        declared
    );
    assert_eq!(
        support::rows(&db, "SELECT id FROM projects ORDER BY id"),
        projects
    );
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM work"), work);
    // The reference still stands over the table that was not dropped.
    assert!(matches!(
        support::refusal(&db, "INSERT INTO work (id, project_id) VALUES (90, 404)"),
        Error::ForeignKeyViolation { .. }
    ));

    // With the referencing table gone, the table goes.
    support::run(&db, "DROP TABLE work");
    support::run(&db, "DROP TABLE projects");
    assert!(matches!(
        support::refusal(&db, "SELECT id FROM projects"),
        Error::TableNotFound(table) if table == "projects"
    ));
}
