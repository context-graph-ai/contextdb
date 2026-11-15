//! Graph patterns through the library: the work a pattern does and the memory it may take, the
//! rule that keeps a DAG's edges free of cycles, and what a narrowed handle is shown. A node is
//! hidden from a handle when a table with a Context column, labels or an access-control column
//! has a row whose `id` is the node's id and that row is hidden from the handle; a node no row
//! carries is hidden from nobody. Whatever is hidden is taken out before the pattern is matched.

mod support;

use contextdb::{Database, Error, Principal, Uuid, Value, params};

use support::{Run, context, in_contexts, label_set};

/// An edge table whose rows are each under a Context.
const EDGES: &str = "CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, \
                     target_id UUID NOT NULL, edge_type TEXT NOT NULL, context_id UUID CONTEXT_ID)";

/// A table of nodes, each under a Context.
const THINGS: &str = "CREATE TABLE things (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID)";

const GRANTS: &str = "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, \
                      principal_id TEXT, acl_id UUID)";
const FILES: &str =
    "CREATE TABLE files (id UUID PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id))";

fn node(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

/// A fresh store with these tables.
fn store(name: &str, tables: &[&str]) -> Database {
    let db = support::open(&support::folder(name));
    for table in tables {
        support::run(&db, table);
    }
    db
}

/// Writes edge `id`, of type `E`, from one node to another, under Context `of`.
fn edge(on: &impl Run, id: i64, from: u128, to: u128, of: u128) {
    typed_edge(on, id, from, to, "E", of);
}

fn typed_edge(on: &impl Run, id: i64, from: u128, to: u128, edge_type: &str, of: u128) {
    let values = params! {
        "id" => id, "from" => node(from), "to" => node(to), "type" => edge_type,
        "context" => context(of),
    };
    let sql = "INSERT INTO edges (id, source_id, target_id, edge_type, context_id) \
               VALUES ($id, $from, $to, $type, $context)";
    if let Err(error) = on.run(sql, &values) {
        panic!("edge {id} from {from} to {to} was refused: {error:?}");
    }
}

/// A pattern over `edges` that names its far node `b`, with the id and the depth of each node
/// reached, nearest first.
fn pattern(pattern: &str, filter: &str) -> String {
    format!(
        "SELECT b_id, depth FROM GRAPH_TABLE(edges MATCH {pattern} {filter} \
         COLUMNS (b.id AS b_id, depth AS depth)) ORDER BY depth, b_id"
    )
}

/// The same pattern, started at one node.
fn from(start: u128, pattern_text: &str) -> String {
    pattern(pattern_text, &format!("WHERE a.id = '{}'", node(start)))
}

/// The rows a pattern must return: each node's number and its depth.
fn reached(wanted: &[(u128, i64)]) -> Vec<Vec<Value>> {
    wanted
        .iter()
        .map(|(n, depth)| vec![Value::Uuid(node(*n)), Value::Int64(*depth)])
        .collect()
}

/// A store whose nodes 1, 3, 4, 5 and 7 are under Context 1 and whose node 2 is under Context
/// 2; node 6 has no row. Every edge row is under Context 1 except the edge from 1 to 7.
///
/// ```text
/// 1 -> 2 -> 3        1 -> 6
///      2 -> 5        1 -> 7   (the edge row is under Context 2)
/// 1 -> 4 -> 5        4 -> 1
/// 1 -> 4 again, of type F
/// ```
fn partly_hidden(name: &str) -> Database {
    let db = store(name, &[EDGES, THINGS]);
    for (n, of) in [(1, 1), (2, 2), (3, 1), (4, 1), (5, 1), (7, 1)] {
        db.execute(
            "INSERT INTO things (id, context_id) VALUES ($id, $context)",
            &params! { "id" => node(n), "context" => context(of) },
        )
        .expect("insert a node");
    }
    let edges = [
        (1, 1, 2, 1),
        (2, 2, 3, 1),
        (3, 1, 4, 1),
        (4, 4, 5, 1),
        (5, 2, 5, 1),
        (6, 1, 6, 1),
        (7, 1, 7, 2),
        (8, 4, 1, 1),
    ];
    for (id, from, to, of) in edges {
        edge(&db, id, from, to, of);
    }
    typed_edge(&db, 9, 1, 4, "F", 1);
    db
}

// ---- the work and the memory of a pattern ----

#[test]
fn a_pinned_single_hop_reads_the_nodes_degree_in_rows() {
    let db = store("graph-pinned-degree", &[EDGES]);
    let tx = db.transaction().expect("begin");
    for n in 0..200 {
        edge(&tx, n, 1_000 + n as u128, 2_000 + n as u128, 1);
    }
    // Node 1 has three edges out and two in.
    for (id, from, to) in [
        (500, 1, 2),
        (501, 1, 3),
        (502, 1, 4),
        (503, 5, 1),
        (504, 6, 1),
    ] {
        edge(&tx, id, from, to, 1);
    }
    tx.commit().expect("commit the edges");

    let out = support::run(&db, &from(1, "(a)-[:E]->(b)"));
    assert_eq!(out.rows(), reached(&[(2, 1), (3, 1), (4, 1)]));
    assert_eq!(out.rows_examined(), 3, "rows read for three edges out");
    let into = support::run(&db, &from(1, "(a)<-[:E]-(b)"));
    assert_eq!(into.rows(), reached(&[(5, 1), (6, 1)]));
    assert_eq!(into.rows_examined(), 2, "rows read for two edges in");
}

#[test]
fn a_walk_whose_visited_set_is_over_the_limit_is_refused_and_one_that_fits_is_answered() {
    // Node 1 has 8,000 edges out, and one of the nodes they reach has an edge to node 2. Node 3
    // has three edges out, and one of the nodes they reach has an edge to node 4. A walk of
    // exactly two steps returns one row from either start, and remembers every node it met.
    const WIDE: u128 = 8_000;
    let db = store("graph-walk-memory", &[EDGES]);
    let tx = db.transaction().expect("begin");
    for n in 0..WIDE {
        edge(&tx, n as i64, 1, 10_000 + n, 1);
    }
    edge(&tx, 20_000, 10_000, 2, 1);
    for n in 0..3 {
        edge(&tx, 30_000 + n as i64, 3, 40_000 + n, 1);
    }
    edge(&tx, 50_000, 40_000, 4, 1);
    tx.commit().expect("commit the edges");

    // 64 KiB of room: the 8,001 nodes of the wide walk take more than 300 KiB to remember.
    let limit = db.memory_used() + (64 << 10);
    db.set_memory_limit(Some(limit)).expect("set the limit");
    let wide = from(1, "(a)-[:E]->{2,2}(b)");
    match db.execute(&wide, &params! {}) {
        Err(Error::MemoryBudgetExceeded { .. }) => {}
        other => panic!("a walk over 8,001 nodes in 64 KiB got {other:?}"),
    }
    assert_eq!(
        support::rows(&db, &from(3, "(a)-[:E]->{2,2}(b)")),
        reached(&[(4, 2)]),
        "a walk over five nodes under the same limit"
    );
    db.set_memory_limit(None).expect("lift the limit");
    assert_eq!(support::rows(&db, &wide), reached(&[(2, 2)]));
}

#[test]
fn a_transaction_walks_the_edges_it_staged_and_not_the_ones_it_deleted() {
    let db = store("graph-staged-edges", &[EDGES]);
    let tx = db.transaction().expect("begin");
    edge(&tx, 1, 1, 2, 1);
    edge(&tx, 2, 2, 3, 1);
    let hop = from(1, "(a)-[:E]->(b)");
    let walk = from(1, "(a)-[:E]->{1,2}(b)");
    let every = pattern("(a)-[:E]->(b)", "");
    assert_eq!(support::rows(&tx, &hop), reached(&[(2, 1)]));
    assert_eq!(support::rows(&tx, &walk), reached(&[(2, 1), (3, 2)]));
    assert_eq!(support::rows(&tx, &every), reached(&[(2, 1), (3, 1)]));
    assert_eq!(
        support::rows(&db, &walk),
        reached(&[]),
        "outside the transaction"
    );

    support::run(&tx, "DELETE FROM edges WHERE id = 2");
    assert_eq!(support::rows(&tx, &walk), reached(&[(2, 1)]));
    assert_eq!(support::rows(&tx, &every), reached(&[(2, 1)]));
    support::run(&tx, "DELETE FROM edges WHERE id = 1");
    assert_eq!(support::rows(&tx, &hop), reached(&[]));
}

// ---- a DAG's edges ----

fn dag(name: &str) -> Database {
    let table = format!("{EDGES} DAG('E')");
    store(name, &[table.as_str()])
}

fn assert_cycle(result: contextdb::Result<impl std::fmt::Debug>, what: &str) {
    match result {
        Err(Error::CycleDetected { table, edge_type }) => {
            assert_eq!(
                (table.as_str(), edge_type.as_str()),
                ("edges", "E"),
                "{what}"
            );
        }
        other => panic!("{what} got {other:?}"),
    }
}

/// An INSERT of one edge of type `E`, written out.
fn insert_edge(id: i64, from: u128, to: u128) -> String {
    format!(
        "INSERT INTO edges (id, source_id, target_id, edge_type, context_id) \
         VALUES ({id}, '{}', '{}', 'E', '{}')",
        node(from),
        node(to),
        context(1)
    )
}

#[test]
fn an_edge_that_closes_a_cycle_with_another_commits_edge_is_refused_at_commit() {
    let db = dag("graph-dag-commit");
    let first = db.transaction().expect("begin");
    let second = db.transaction().expect("begin");
    // Neither transaction sees the other's edge, so each statement is taken.
    support::run(&first, &insert_edge(1, 1, 2));
    support::run(&second, &insert_edge(2, 2, 1));
    first
        .commit()
        .expect("the first of two transactions commits");
    assert_cycle(second.commit(), "the commit of the other half of a cycle");
    assert_eq!(
        support::rows(&db, "SELECT id FROM edges"),
        [[Value::Int64(1)]]
    );
}

// ---- what a narrowed handle is shown ----

#[test]
fn a_handle_allowed_to_see_everything_gets_the_rows_an_unconstrained_handle_gets() {
    let db = partly_hidden("graph-same-rows");
    let all = in_contexts(&db, &[1, 2]);
    let asked = [
        pattern("(a)-[]->(b)", ""),
        pattern("(a)-[:E]-(b)", ""),
        from(1, "(a)-[]->(b)"),
        from(1, "(a)-[:E]->{1,3}(b)"),
        from(5, "(a)<-[:E]-(b)"),
        pattern("(a)-[:E]->{1,2}(b)", ""),
    ];
    for sql in &asked {
        let wanted = support::rows(&db, sql);
        assert!(!wanted.is_empty(), "{sql}\nreturns no row to compare");
        assert_eq!(support::rows(&all, sql), wanted, "{sql}");
    }
    // What both are held to: one row for each edge, and the start again when a cycle returns.
    assert_eq!(
        support::rows(&all, &from(1, "(a)-[]->(b)")),
        reached(&[(2, 1), (4, 1), (4, 1), (6, 1), (7, 1)])
    );
    assert_eq!(
        support::rows(&all, &from(1, "(a)-[:E]->{1,3}(b)")),
        reached(&[(2, 1), (4, 1), (6, 1), (7, 1), (1, 2), (3, 2), (5, 2)])
    );
}

#[test]
fn a_handle_narrowed_to_a_context_is_shown_the_graph_without_what_it_may_not_see() {
    // One store, four rules of what a narrowed handle may follow, each over the same graph.
    let db = partly_hidden("graph-hidden-nodes");
    let narrowed = in_contexts(&db, &[1]);
    a_hidden_edge_row_is_not_followed(&db, &narrowed);
    an_edge_whose_row_is_visible_and_whose_target_is_hidden_is_not_reported(&narrowed);
    a_walk_does_not_pass_through_a_hidden_node_to_a_visible_one_behind_it(&db, &narrowed);
    a_hidden_node_reached_on_the_way_is_left_out_with_no_error(&db, &narrowed);
}

/// Over [`partly_hidden`], read through a handle narrowed to Context 1.
fn a_hidden_edge_row_is_not_followed(db: &Database, narrowed: &Database) {
    // Node 7 is the handle's to see, and the only edge to it is not.
    assert_eq!(
        support::rows(
            narrowed,
            &format!("SELECT id FROM things WHERE id = '{}'", node(7))
        ),
        [[Value::Uuid(node(7))]]
    );
    let seven = [Value::Uuid(node(7)), Value::Int64(1)];
    for sql in [from(1, "(a)-[:E]->(b)"), from(1, "(a)-[:E]->{1,3}(b)")] {
        assert!(support::rows(db, &sql).contains(&seven.to_vec()), "{sql}");
        assert!(
            !support::rows(narrowed, &sql).contains(&seven.to_vec()),
            "{sql}"
        );
    }
    assert_eq!(
        support::rows(narrowed, &from(7, "(a)<-[:E]-(b)")),
        reached(&[]),
        "against the hidden edge"
    );
}

/// Over [`partly_hidden`], read through a handle narrowed to Context 1.
fn an_edge_whose_row_is_visible_and_whose_target_is_hidden_is_not_reported(narrowed: &Database) {
    // The handle reads the row of the edge from node 1 to node 2, and may not see node 2.
    assert_eq!(
        support::int(narrowed, "SELECT id FROM edges WHERE id = 1"),
        1
    );
    assert_eq!(
        support::rows(narrowed, &from(1, "(a)-[:E]->(b)")),
        reached(&[(4, 1), (6, 1)])
    );
    // Nor is an edge reported whose hidden node is the one it leaves.
    assert_eq!(
        support::rows(narrowed, &from(3, "(a)<-[:E]-(b)")),
        reached(&[])
    );
    assert_eq!(
        support::rows(narrowed, &pattern("(a)-[:E]->(b)", "")),
        reached(&[(1, 1), (4, 1), (5, 1), (6, 1)])
    );
}

/// Over [`partly_hidden`], read through a handle narrowed to Context 1.
fn a_walk_does_not_pass_through_a_hidden_node_to_a_visible_one_behind_it(
    db: &Database,
    narrowed: &Database,
) {
    let walk = from(1, "(a)-[:E]->{1,3}(b)");
    // Node 3 is reached only through node 2; node 5 through node 2 and through node 4.
    let (three, five) = (reached(&[(3, 2)]), reached(&[(5, 2)]));
    let unconstrained = support::rows(db, &walk);
    assert!(unconstrained.contains(&three[0]) && unconstrained.contains(&five[0]));
    assert_eq!(
        support::rows(narrowed, &walk),
        reached(&[(4, 1), (6, 1), (1, 2), (5, 2)])
    );
    // Written out step by step it is the same: no row goes through node 2.
    let steps = format!(
        "SELECT b_id, c_id FROM GRAPH_TABLE(edges MATCH (a)-[:E]->(b)-[:E]->(c) \
         WHERE a.id = '{}' COLUMNS (b.id AS b_id, c.id AS c_id)) ORDER BY c_id",
        node(1)
    );
    assert_eq!(
        support::rows(narrowed, &steps),
        [
            [Value::Uuid(node(4)), Value::Uuid(node(1))],
            [Value::Uuid(node(4)), Value::Uuid(node(5))]
        ]
    );
}

/// Over [`partly_hidden`], read through a handle narrowed to Context 1.
fn a_hidden_node_reached_on_the_way_is_left_out_with_no_error(db: &Database, narrowed: &Database) {
    // Back from node 5: nodes 2 and 4 are one step away, node 1 two.
    let back = from(5, "(a)<-[:E]-{1,2}(b)");
    assert_eq!(support::rows(db, &back), reached(&[(2, 1), (4, 1), (1, 2)]));
    match narrowed.execute(&back, &params! {}) {
        Ok(result) => assert_eq!(result.rows(), reached(&[(4, 1), (1, 2)])),
        Err(error) => panic!("a walk that meets a hidden node was refused: {error:?}"),
    }
}

#[test]
fn a_pattern_pinned_to_a_hidden_node_is_refused_by_the_rule_that_hid_it() {
    // Hidden by its Context.
    let db = partly_hidden("graph-pinned-context");
    let narrowed = in_contexts(&db, &[1]);
    let by_subquery = format!(
        "WHERE a.id = (SELECT id FROM things WHERE id = '{}')",
        node(2)
    );
    for sql in [
        from(2, "(a)-[:E]->(b)"),
        from(2, "(a)-[:E]->{1,3}(b)"),
        pattern("(a)-[:E]->(b)", &by_subquery),
    ] {
        match narrowed.execute(&sql, &params! {}) {
            Err(Error::ContextScopeViolation { .. }) => {}
            other => panic!("{sql}\ngot {other:?}"),
        }
    }
    // A node no row carries is hidden from nobody: the pattern is answered.
    assert_eq!(
        support::rows(&narrowed, &from(6, "(a)<-[:E]-(b)")),
        reached(&[(1, 1)])
    );
    assert_eq!(
        support::rows(&narrowed, &from(99, "(a)-[:E]->(b)")),
        reached(&[])
    );
    // A list of keys is a list read: the hidden key is left out of it.
    let (one, two) = (node(1), node(2));
    for filter in [
        format!("a.id = '{one}' OR a.id = '{two}'"),
        format!("a.id IN ('{one}', '{two}')"),
    ] {
        let sql = pattern("(a)-[:E]->(b)", &format!("WHERE {filter}"));
        assert_eq!(support::rows(&narrowed, &sql), reached(&[(4, 1), (6, 1)]));
    }

    // A table of nodes under a Context whose key and indexes do not begin with `id` cannot be
    // asked who may see a node, so a handle narrowed to Contexts is refused the pattern.
    let db = store(
        "graph-no-id-key",
        &[
            EDGES,
            "CREATE TABLE nodes (id UUID NOT NULL, context_id UUID CONTEXT_ID, PRIMARY KEY (context_id, id))",
        ],
    );
    let asked = in_contexts(&db, &[1]).execute(&from(1, "(a)-[:E]->(b)"), &params! {});
    assert!(matches!(asked, Err(Error::PlanError(_))), "{asked:?}");

    // Hidden by a grant the principal does not hold: node 11 is granted, node 12 is not.
    let db = store("graph-pinned-grant", &[EDGES, GRANTS, FILES]);
    db.execute(
        "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) \
         VALUES ($id, 'Agent', 'ann', $acl)",
        &params! { "id" => node(900), "acl" => node(801) },
    )
    .expect("grant");
    for (n, acl) in [(11, 801), (12, 802)] {
        db.execute(
            "INSERT INTO files (id, acl_id) VALUES ($id, $acl)",
            &params! { "id" => node(n), "acl" => node(acl) },
        )
        .expect("insert a file");
    }
    edge(&db, 1, 11, 12, 1);
    edge(&db, 2, 11, 13, 1);
    edge(&db, 3, 12, 13, 1);
    let ann = db.scoped_with_constraints(None, None, Some(Principal::Agent("ann".to_owned())));
    match ann.execute(&from(12, "(a)-[:E]->(b)"), &params! {}) {
        Err(Error::AclDenied { .. }) => {}
        other => panic!("a pattern pinned to a node with no grant got {other:?}"),
    }
    assert_eq!(
        support::rows(&ann, &from(11, "(a)-[:E]->(b)")),
        reached(&[(13, 1)])
    );

    // Hidden by its label: node 21 is read under 'public', node 22 under 'internal'.
    let db = store(
        "graph-pinned-label",
        &[
            EDGES,
            "CREATE TABLE papers (id UUID PRIMARY KEY, label TEXT SCOPE_LABEL_READ ('public', \
             'internal') WRITE ('internal'))",
        ],
    );
    for (n, label) in [(21, "public"), (22, "internal")] {
        db.execute(
            "INSERT INTO papers (id, label) VALUES ($id, $label)",
            &params! { "id" => node(n), "label" => label },
        )
        .expect("insert a paper");
    }
    edge(&db, 1, 21, 22, 1);
    edge(&db, 2, 22, 23, 1);
    let labels = label_set(&["public"]);
    let public = db.scoped_with_constraints(None, Some(labels), None);
    match public.execute(&from(22, "(a)-[:E]->(b)"), &params! {}) {
        Err(Error::ScopeLabelViolation { .. }) => {}
        other => panic!("a pattern pinned to a node under another label got {other:?}"),
    }
    assert_eq!(
        support::rows(&public, &from(21, "(a)-[:E]->{1,2}(b)")),
        reached(&[])
    );
}

#[test]
fn no_refusal_text_and_no_count_tells_of_a_hidden_node() {
    let db = partly_hidden("graph-tells-nothing");
    let narrowed = in_contexts(&db, &[1]);
    let error = support::refusal(&narrowed, &from(2, "(a)-[:E]->(b)"));
    let words = format!("{error} {error:?}");
    for hidden in [node(2), context(2), node(3), node(5)] {
        assert!(
            !words.contains(&hidden.to_string()),
            "the refusal names {hidden}: {words}"
        );
    }
    // A count over a walk counts what the handle is shown: nodes 4, 6, 1 and 5 of the seven.
    let count = format!(
        "WITH met AS (SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:E]->{{1,3}}(b) \
         WHERE a.id = '{}' COLUMNS (b.id AS b_id))) SELECT COUNT(*) FROM met",
        node(1)
    );
    assert_eq!(support::int(&db, &count), 7);
    assert_eq!(support::int(&narrowed, &count), 4);
}

#[test]
fn a_pattern_that_meets_a_row_needing_a_grant_is_refused_unless_another_rule_hid_the_row() {
    let db = store(
        "graph-principal-required",
        &[
            EDGES,
            GRANTS,
            FILES,
            "CREATE TABLE docs (id UUID PRIMARY KEY, \
             acl_id UUID ACL REFERENCES acl_grants(acl_id), context_id UUID CONTEXT_ID)",
        ],
    );
    // Node 30 is a file; node 41 is a document of Context 2, nodes 51 and 62 of Context 1. Every
    // edge is under Context 1. The nodes 10, 11, 20, 40, 50 and 60 have no row.
    db.execute(
        "INSERT INTO files (id, acl_id) VALUES ($id, $acl)",
        &params! { "id" => node(30), "acl" => node(801) },
    )
    .expect("a file");
    for (n, of) in [(41, 2), (51, 1), (62, 1)] {
        db.execute(
            "INSERT INTO docs (id, acl_id, context_id) VALUES ($id, $acl, $context)",
            &params! { "id" => node(n), "acl" => node(801), "context" => context(of) },
        )
        .expect("a document");
    }
    for (id, (a, b)) in [(10, 11), (20, 30), (40, 41), (50, 51), (60, 61), (61, 62)]
        .into_iter()
        .enumerate()
    {
        edge(&db, id as i64 + 1, a, b, 1);
    }
    let narrowed = in_contexts(&db, &[1]);
    let system = db.scoped_with_constraints(None, None, Some(Principal::System));
    let required = |table: &str| Error::PrincipalRequired {
        table: table.to_owned(),
    };
    let step = "(a)-[:E]->(b)";
    let walk = "(a)-[:E]->{1,2}(b)";

    assert_eq!(
        support::rows(&narrowed, &from(10, step)),
        reached(&[(11, 1)]),
        "a pattern that reaches only nodes no such row carries is answered"
    );
    for (reader, start, pattern_text, wanted, why) in [
        (
            &narrowed,
            20,
            step,
            required("files"),
            "a reach to a row of an access-controlled table",
        ),
        (
            &narrowed,
            50,
            step,
            required("docs"),
            "a document of the reader's Context at depth 1",
        ),
        (
            &narrowed,
            60,
            walk,
            required("docs"),
            "a document of the reader's Context at depth 2",
        ),
        (
            &system,
            20,
            step,
            required("files"),
            "the principal System holds no grants",
        ),
    ] {
        assert_eq!(
            support::refusal(reader, &from(start, pattern_text)).to_string(),
            wanted.to_string(),
            "{why} needs a handle that names a principal able to hold grants"
        );
    }
    assert_eq!(
        support::rows(&narrowed, &from(40, step)),
        reached(&[]),
        "a document outside the reader's Contexts is hidden by that rule: no row and no error"
    );
    assert!(
        matches!(
            support::refusal(&narrowed, &from(41, step)),
            Error::ContextScopeViolation { .. }
        ),
        "a start named that the Context rule hides is that rule's refusal"
    );
}
