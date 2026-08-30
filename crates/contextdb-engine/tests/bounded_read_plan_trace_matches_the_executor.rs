//! A bounded read describes its plan in the same words the executor does.
//!
//! A query's trace is how an operator answers "why was that slow?" -- which
//! plan ran, which index it used, which predicates reached the source, which
//! indexes were considered and rejected. The same statement asked through a
//! bounded read view is the same question about the same data, so it has to
//! come back with the same plan description. A door that answers "a full edge
//! scan" where the other answers "an adjacency probe" tells the operator the
//! store is doing work it is not doing, and no amount of correct rows makes
//! that answer true.
//!
//! The ORACLE here is the executor's own answer, never a value restated by
//! hand: each statement is asked through both doors and the two descriptions
//! are compared. The one thing this file restates is the VOCABULARY -- the
//! set of plan words the executor publishes -- so a plan word that stops
//! being covered is caught rather than quietly dropped.
//!
//! Rows examined has its own pin and is deliberately not part of the
//! description compared here, so that a disagreement about counting cannot be
//! mistaken for a disagreement about plans.

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{Database, QueryResult, QueryTrace};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// Ceilings far above anything these statements need, so a ceiling can never
/// be what makes the two doors disagree. The shipped defaults are not roomy
/// enough: a thousand-document neighbour search crosses the default work
/// ceiling, and a refusal would hide the plan this file is about.
fn roomy() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: 100_000,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn node(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x00D0_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

/// Everything a trace says about the PLAN, rendered so two traces can be
/// compared and so a disagreement reads as prose in the failure.
fn described(trace: &QueryTrace) -> String {
    let pushed: Vec<&str> = trace
        .predicates_pushed
        .iter()
        .map(|predicate| predicate.as_ref())
        .collect();
    let considered: Vec<(&str, &str)> = trace
        .indexes_considered
        .iter()
        .map(|candidate| (candidate.name.as_str(), candidate.rejected_reason.as_ref()))
        .collect();
    format!(
        "plan={:?} index_used={:?} predicates_pushed={pushed:?} \
         indexes_considered={considered:?} sort_elided={} query_vector_source={:?}",
        trace.physical_plan, trace.index_used, trace.sort_elided, trace.query_vector_source
    )
}

/// The answer, rendered for comparison. A statement with no ORDER BY promises
/// a SET of rows and nothing about their sequence, so comparing those two
/// doors row-by-row would fail on something neither of them promises; those
/// answers are compared as sets and the ordered ones as sequences.
fn answer(result: &QueryResult, ordered: bool) -> String {
    let mut rows: Vec<String> = result.rows.iter().map(|row| format!("{row:?}")).collect();
    if !ordered {
        rows.sort();
    }
    format!("columns={:?} rows={rows:?}", result.columns)
}

struct Case {
    name: &'static str,
    database: Database,
    sql: String,
    params: HashMap<String, Value>,
    /// Whether the statement asked for an order. Only then is the sequence
    /// part of the answer.
    ordered: bool,
}

fn relational_store() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, bucket TEXT, score INTEGER)",
            &empty(),
        )
        .expect("create the relational fixture table");
    for ordinal in 0..64_u128 {
        database
            .execute(
                "INSERT INTO items (id, bucket, score) VALUES ($id, $bucket, $score)",
                &params(vec![
                    ("id", Value::Uuid(node(ordinal))),
                    ("bucket", Value::Text(format!("bucket-{}", ordinal % 4))),
                    ("score", Value::Int64((ordinal % 16) as i64)),
                ]),
            )
            .expect("insert a relational row");
    }
    database
}

fn indexed_relational_store() -> Database {
    let database = relational_store();
    database
        .execute("CREATE INDEX items_score ON items (score)", &empty())
        .expect("create the score index");
    database
}

fn graph_store() -> Database {
    let database = Database::open_memory();
    database
        .execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &empty())
        .expect("create the node table");
    database
        .execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
             edge_type TEXT)",
            &empty(),
        )
        .expect("create the edge table");
    // A hub with five outbound neighbours of the type under test, a chain
    // three hops deep beyond it, and inbound edges of another type, so a
    // probe, a scan and a walk each have something to distinguish them.
    let hub = node(1);
    let chain: Vec<Uuid> = (0..3).map(|hop| node(10 + hop)).collect();
    let leaves: Vec<Uuid> = (0..5).map(|leaf| node(20 + leaf)).collect();
    let admirers: Vec<Uuid> = (0..4).map(|admirer| node(30 + admirer)).collect();
    for id in std::iter::once(hub)
        .chain(chain.iter().copied())
        .chain(leaves.iter().copied())
        .chain(admirers.iter().copied())
    {
        database
            .execute(
                "INSERT INTO nodes (id) VALUES ($id)",
                &params(vec![("id", Value::Uuid(id))]),
            )
            .expect("insert a node");
    }
    let insert_edge = |source: Uuid, target: Uuid, edge_type: &str| {
        database
            .execute(
                "INSERT INTO edges (id, source_id, target_id, edge_type) \
                 VALUES ($id, $source, $target, $edge_type)",
                &params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("source", Value::Uuid(source)),
                    ("target", Value::Uuid(target)),
                    ("edge_type", Value::Text(edge_type.to_owned())),
                ]),
            )
            .expect("insert an edge");
    };
    for leaf in &leaves {
        insert_edge(hub, *leaf, "LINKS");
    }
    insert_edge(hub, chain[0], "LINKS");
    insert_edge(chain[0], chain[1], "LINKS");
    insert_edge(chain[1], chain[2], "LINKS");
    for admirer in &admirers {
        insert_edge(*admirer, hub, "SERVES");
    }
    database
}

const VECTOR_DIMENSIONS: usize = 3;

/// A vector fixture of `rows` documents. Below a thousand the engine answers
/// a neighbour query by brute force; above it the approximate index takes
/// over, and both plan words have to be covered.
fn vector_store(rows: u128) -> Database {
    let database = Database::open_memory();
    database
        .execute(
            &format!(
                "CREATE TABLE docs (id UUID PRIMARY KEY, bucket TEXT, \
                 embedding VECTOR({VECTOR_DIMENSIONS}))"
            ),
            &empty(),
        )
        .expect("create the vector fixture table");
    for ordinal in 0..rows {
        let angle = ordinal as f32 / 97.0;
        database
            .execute(
                "INSERT INTO docs (id, bucket, embedding) VALUES ($id, $bucket, $embedding)",
                &params(vec![
                    ("id", Value::Uuid(node(1_000 + ordinal))),
                    ("bucket", Value::Text(format!("bucket-{}", ordinal % 4))),
                    (
                        "embedding",
                        Value::Vector(vec![angle.cos(), angle.sin(), 0.25]),
                    ),
                ]),
            )
            .expect("insert a document");
    }
    database
}

fn indexed_vector_store(rows: u128) -> Database {
    let database = vector_store(rows);
    database
        .execute("CREATE INDEX docs_bucket ON docs (bucket)", &empty())
        .expect("create the bucket index");
    database
}

fn query_vector() -> Value {
    Value::Vector(vec![1.0, 0.0, 0.25])
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "a scan of a column with no index",
            database: relational_store(),
            sql: "SELECT id, score FROM items WHERE score = $score".to_owned(),
            params: params(vec![("score", Value::Int64(3))]),
            ordered: false,
        },
        Case {
            name: "a scan the caller asked to be ordered",
            database: relational_store(),
            sql: "SELECT id, score FROM items ORDER BY score, id".to_owned(),
            params: empty(),
            ordered: true,
        },
        Case {
            name: "a lookup an index can answer",
            database: indexed_relational_store(),
            sql: "SELECT id, score FROM items WHERE score = $score ORDER BY id".to_owned(),
            params: params(vec![("score", Value::Int64(3))]),
            ordered: true,
        },
        Case {
            name: "one hop out of a pinned node",
            database: graph_store(),
            sql: "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $hub \
                  COLUMNS (b.id AS t))"
                .to_owned(),
            params: params(vec![("hub", Value::Uuid(node(1)))]),
            ordered: false,
        },
        Case {
            name: "one hop into a pinned node",
            database: graph_store(),
            sql: "SELECT d FROM GRAPH_TABLE(edges MATCH (a)<-[:SERVES]-(b) WHERE a.id = $hub \
                  COLUMNS (b.id AS d))"
                .to_owned(),
            params: params(vec![("hub", Value::Uuid(node(1)))]),
            ordered: false,
        },
        Case {
            name: "one hop between two pinned nodes",
            database: graph_store(),
            sql: "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $hub \
                  AND b.id = $leaf COLUMNS (b.id AS t))"
                .to_owned(),
            params: params(vec![
                ("hub", Value::Uuid(node(1))),
                ("leaf", Value::Uuid(node(21))),
            ]),
            ordered: false,
        },
        Case {
            name: "every edge of a type, pinned to nothing",
            database: graph_store(),
            sql: "SELECT s, t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                  COLUMNS (a.id AS s, b.id AS t))"
                .to_owned(),
            params: empty(),
            ordered: false,
        },
        Case {
            name: "a walk several hops deep",
            database: graph_store(),
            sql: "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,3}(b) WHERE a.id = $hub \
                  COLUMNS (b.id AS t))"
                .to_owned(),
            params: params(vec![("hub", Value::Uuid(node(1)))]),
            ordered: false,
        },
        Case {
            name: "nearest neighbours found by looking at every document",
            database: vector_store(64),
            sql: "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 3".to_owned(),
            params: params(vec![("query", query_vector())]),
            ordered: true,
        },
        Case {
            name: "nearest neighbours among documents an index narrowed",
            database: indexed_vector_store(64),
            sql: "SELECT id FROM docs WHERE bucket = $bucket ORDER BY embedding <=> $query LIMIT 3"
                .to_owned(),
            params: params(vec![
                ("bucket", Value::Text("bucket-1".to_owned())),
                ("query", query_vector()),
            ]),
            ordered: true,
        },
        Case {
            name: "nearest neighbours found through the approximate index",
            database: vector_store(1_024),
            sql: "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 3".to_owned(),
            params: params(vec![("query", query_vector())]),
            ordered: true,
        },
        Case {
            name: "nearest neighbours through the approximate index after an index narrowed",
            database: indexed_vector_store(1_024),
            sql: "SELECT id FROM docs WHERE bucket = $bucket ORDER BY embedding <=> $query LIMIT 3"
                .to_owned(),
            params: params(vec![
                ("bucket", Value::Text("bucket-1".to_owned())),
                ("query", query_vector()),
            ]),
            ordered: true,
        },
        Case {
            name: "a neighbour query whose vector is read from a stored row",
            database: vector_store(64),
            sql: "SELECT id FROM docs ORDER BY embedding <=> \
                  ROW_VECTOR('docs','embedding',$anchor) LIMIT 3"
                .to_owned(),
            params: params(vec![("anchor", Value::Uuid(node(1_000)))]),
            ordered: true,
        },
    ]
}

/// The plan words the executor publishes. A run that stops producing one of
/// these has stopped covering it, which is how a plan quietly loses its pin.
const PLAN_VOCABULARY: [&str; 10] = [
    "Scan",
    "Sort",
    "IndexScan",
    "AdjacencyProbe",
    "EdgesScan",
    "GraphBfs",
    "Scan -> VectorSearch",
    "IndexScan -> VectorSearch",
    "Scan -> HNSWSearch",
    "IndexScan -> HNSWSearch",
];

#[test]
fn a_bounded_read_describes_its_plan_the_way_the_executor_does() {
    let mut disagreements = Vec::new();
    let mut covered = BTreeSet::new();

    for case in cases() {
        let eager = case
            .database
            .execute(&case.sql, &case.params)
            .unwrap_or_else(|error| panic!("the executor answers {}: {error}", case.name));
        let bounded = case
            .database
            .read_session(roomy())
            .expect("open a bounded read view")
            .execute(&case.sql, &case.params)
            .unwrap_or_else(|error| panic!("a bounded read answers {}: {error}", case.name));

        covered.insert(eager.trace.physical_plan);
        println!(
            "OBSERVED {}: executor {} | bounded {}",
            case.name,
            described(&eager.trace),
            described(&bounded.trace)
        );

        if answer(&bounded, case.ordered) != answer(&eager, case.ordered) {
            disagreements.push(format!(
                "{}: the executor answers {} and a bounded read answers {}",
                case.name,
                answer(&eager, case.ordered),
                answer(&bounded, case.ordered)
            ));
        }
        if described(&bounded.trace) != described(&eager.trace) {
            disagreements.push(format!(
                "{}: the executor describes the plan as {} and a bounded read describes it as {}",
                case.name,
                described(&eager.trace),
                described(&bounded.trace)
            ));
        }
    }

    let uncovered: Vec<&str> = PLAN_VOCABULARY
        .into_iter()
        .filter(|plan| !covered.contains(plan))
        .collect();
    assert!(
        uncovered.is_empty(),
        "every plan word the executor publishes needs a statement that produces it, and these \
         produced none: {uncovered:?} (observed {covered:?})"
    );

    assert!(
        disagreements.is_empty(),
        "one store, one statement, one answer: a bounded read that describes a different plan \
         tells an operator the store is doing work it is not doing:\n{}",
        disagreements.join("\n")
    );
}

/// A vector query's trace names the stored column its query vector came from.
/// It is part of the plan description above; this pins the field itself, so a
/// door that drops it fails on the field rather than on a rendered string.
#[test]
fn a_bounded_read_names_the_stored_column_a_query_vector_came_from() {
    let database = vector_store(64);
    let sql = "SELECT id FROM docs ORDER BY embedding <=> ROW_VECTOR('docs','embedding',$anchor) \
               LIMIT 3";
    let anchor = params(vec![("anchor", Value::Uuid(node(1_000)))]);

    let eager = database
        .execute(sql, &anchor)
        .expect("the executor answers");
    assert_eq!(
        eager.trace.query_vector_source,
        Some(VectorIndexRef::new("docs", "embedding")),
        "the executor names the column the query vector was read from"
    );

    let bounded = database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(sql, &anchor)
        .expect("a bounded read answers");
    assert_eq!(
        bounded.trace.query_vector_source, eager.trace.query_vector_source,
        "a bounded read names the same stored column the executor names"
    );
}

/// A graph whose only edge of the type under test runs from one named node to
/// another, buried in a crowd of unrelated edges of the SAME type. The crowd
/// is what makes the two plans tell different stories: a probe touches one
/// entry, a scan walks every edge in the store.
const UNRELATED_EDGES: u128 = 1_000;

fn target_pinned_store() -> Database {
    let database = Database::open_memory();
    database
        .execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &empty())
        .expect("create the node table");
    database
        .execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
             edge_type TEXT)",
            &empty(),
        )
        .expect("create the edge table");
    let insert_node = |id: uuid::Uuid| {
        database
            .execute(
                "INSERT INTO nodes (id) VALUES ($id)",
                &params(vec![("id", Value::Uuid(id))]),
            )
            .expect("insert a node");
    };
    insert_node(node(5_000));
    insert_node(node(5_001));
    let insert_edge = |source: Uuid, target: Uuid| {
        database
            .execute(
                "INSERT INTO edges (id, source_id, target_id, edge_type) \
                 VALUES ($id, $source, $target, 'LINKS')",
                &params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("source", Value::Uuid(source)),
                    ("target", Value::Uuid(target)),
                ]),
            )
            .expect("insert an edge");
    };
    insert_edge(node(5_000), node(5_001));
    for ordinal in 0..UNRELATED_EDGES {
        insert_edge(node(6_000 + ordinal * 2), node(6_000 + ordinal * 2 + 1));
    }
    database
}

/// The pinned node in each pattern is the SECOND one named, so each statement
/// pins only where the hop ENDS.
const TARGET_ONLY_OUTGOING: &str = "SELECT s FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                                    WHERE b.id = $pinned COLUMNS (a.id AS s))";
const TARGET_ONLY_INCOMING: &str = "SELECT s FROM GRAPH_TABLE(edges MATCH (a)<-[:LINKS]-(b) \
                                    WHERE b.id = $pinned COLUMNS (a.id AS s))";

fn target_only_cases() -> [(&'static str, &'static str, Uuid); 2] {
    [
        (
            "a hop pinned only where it ends",
            TARGET_ONLY_OUTGOING,
            node(5_001),
        ),
        (
            "a hop written the other way round, pinned only where it ends",
            TARGET_ONLY_INCOMING,
            node(5_000),
        ),
    ]
}

#[test]
fn a_bounded_read_probes_adjacency_when_only_the_end_of_a_hop_is_pinned() {
    let mut disagreements = Vec::new();

    for (what, sql, pinned) in target_only_cases() {
        let database = target_pinned_store();
        let pinned_params = params(vec![("pinned", Value::Uuid(pinned))]);

        let eager = database
            .execute(sql, &pinned_params)
            .unwrap_or_else(|error| panic!("the executor answers {what}: {error}"));
        // The oracle itself is checked, so this case cannot degrade into two
        // doors agreeing on a full scan.
        assert_eq!(
            eager.trace.physical_plan, "AdjacencyProbe",
            "the executor probes adjacency for {what} rather than walking every edge: {:?}",
            eager.trace
        );
        assert_eq!(
            eager.rows.len(),
            1,
            "exactly one edge of this type ends at the pinned node"
        );

        let bounded = match database
            .read_session(roomy())
            .expect("open a bounded read view")
            .execute(sql, &pinned_params)
        {
            Ok(bounded) => bounded,
            Err(error) => {
                disagreements.push(format!(
                    "{what}: the executor answers {} and a bounded read refuses: {error}",
                    answer(&eager, false)
                ));
                continue;
            }
        };
        println!(
            "OBSERVED {what}: executor {} examined={} | bounded {} examined={}",
            described(&eager.trace),
            eager.trace.rows_examined,
            described(&bounded.trace),
            bounded.trace.rows_examined
        );

        if answer(&bounded, false) != answer(&eager, false) {
            disagreements.push(format!(
                "{what}: the executor answers {} and a bounded read answers {}",
                answer(&eager, false),
                answer(&bounded, false)
            ));
        }
        if described(&bounded.trace) != described(&eager.trace) {
            disagreements.push(format!(
                "{what}: the executor describes the plan as {} and a bounded read describes it \
                 as {}",
                described(&eager.trace),
                described(&bounded.trace)
            ));
        }
        if bounded.trace.rows_examined != eager.trace.rows_examined {
            disagreements.push(format!(
                "{what}: the executor examined {} rows and a bounded read reports {}",
                eager.trace.rows_examined, bounded.trace.rows_examined
            ));
        }
    }

    assert!(
        disagreements.is_empty(),
        "pinning where a hop ENDS is as much a pin as pinning where it starts: a door that calls \
         it unpinned walks every edge in the store to answer a one-entry probe, and tells the \
         operator it did:\n{}",
        disagreements.join("\n")
    );
}

#[test]
fn a_bounded_probe_pinned_only_at_the_end_fits_a_ceiling_a_full_scan_would_cross() {
    // Room for a probe that touches a single adjacency entry, and nowhere near
    // enough for a walk over a thousand unrelated edges. A door that answers
    // this by scanning does not merely describe itself wrongly -- it cannot
    // answer at all under a ceiling the real work fits inside.
    let ceiling = ReadLimits {
        work: 64,
        ..roomy()
    };

    let mut refusals = Vec::new();
    for (what, sql, pinned) in target_only_cases() {
        let database = target_pinned_store();
        let pinned_params = params(vec![("pinned", Value::Uuid(pinned))]);
        let expected = database
            .execute(sql, &pinned_params)
            .unwrap_or_else(|error| panic!("the executor answers {what}: {error}"));

        match database
            .read_session(ceiling)
            .expect("open a bounded read view")
            .execute(sql, &pinned_params)
        {
            Ok(bounded) => {
                println!(
                    "OBSERVED {what} under a probe-sized ceiling: {}",
                    answer(&bounded, false)
                );
                if answer(&bounded, false) != answer(&expected, false) {
                    refusals.push(format!(
                        "{what}: under a probe-sized ceiling the executor answers {} and a \
                         bounded read answers {}",
                        answer(&expected, false),
                        answer(&bounded, false)
                    ));
                }
            }
            Err(error) => refusals.push(format!(
                "{what}: a probe that touches one adjacency entry is refused by a ceiling that \
                 fits it, because the door is walking every edge instead: {error}"
            )),
        }
    }

    assert!(
        refusals.is_empty(),
        "a ceiling is set for the work a statement really needs, so a door that turns a \
         one-entry probe into a full walk turns an answerable question into a refusal:\n{}",
        refusals.join("\n")
    );
}

/// A graph whose starting node is named by something OTHER than its
/// identifier -- a column on the node table, or a column on a second table
/// that also describes nodes. The executor resolves those to identifiers at
/// execution time and then probes adjacency; a door that cannot follow it
/// there has no pinned vertex to probe from and walks every edge instead.
fn metadata_started_store() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT, kind TEXT)",
            &empty(),
        )
        .expect("create the node table");
    database
        .execute(
            "CREATE TABLE alt_nodes (id UUID PRIMARY KEY, kind TEXT)",
            &empty(),
        )
        .expect("create the second table that describes nodes");
    database
        .execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
             edge_type TEXT)",
            &empty(),
        )
        .expect("create the edge table");

    let insert_node = |id: Uuid, name: &str, kind: &str| {
        database
            .execute(
                "INSERT INTO nodes (id, name, kind) VALUES ($id, $name, $kind)",
                &params(vec![
                    ("id", Value::Uuid(id)),
                    ("name", Value::Text(name.to_owned())),
                    ("kind", Value::Text(kind.to_owned())),
                ]),
            )
            .expect("insert a node");
    };
    let insert_edge = |source: Uuid, target: Uuid| {
        database
            .execute(
                "INSERT INTO edges (id, source_id, target_id, edge_type) \
                 VALUES ($id, $source, $target, 'LINKS')",
                &params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("source", Value::Uuid(source)),
                    ("target", Value::Uuid(target)),
                ]),
            )
            .expect("insert an edge");
    };

    insert_node(node(7_000), "seed", "root");
    insert_node(node(7_010), "named-target", "target");
    insert_node(node(7_011), "kind-target", "target");
    // A start that only the SECOND table knows about, so a door that reads
    // one metadata table and not the other answers with half the walk.
    database
        .execute(
            "INSERT INTO alt_nodes (id, kind) VALUES ($id, 'root')",
            &params(vec![("id", Value::Uuid(node(7_001)))]),
        )
        .expect("describe a node in the second table only");
    insert_edge(node(7_000), node(7_010));
    insert_edge(node(7_001), node(7_011));
    // Unrelated edges of the same type, so a probe and a scan are different
    // amounts of work rather than the same handful of entries.
    for ordinal in 0..UNRELATED_EDGES {
        insert_node(node(8_000 + ordinal * 2), "other", "other");
        insert_node(node(8_000 + ordinal * 2 + 1), "other", "other");
        insert_edge(node(8_000 + ordinal * 2), node(8_000 + ordinal * 2 + 1));
    }
    database
}

/// A start named by a node column no index covers, and a start named across
/// two tables that both describe nodes.
const STARTED_BY_KIND: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                               WHERE a.kind = 'root' COLUMNS (b.id AS t))";
const STARTED_ACROSS_TABLES: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                                     WHERE a.name = 'seed' OR a.kind = 'root' \
                                     COLUMNS (b.id AS t))";

#[test]
fn a_bounded_read_probes_adjacency_when_the_start_is_named_by_something_other_than_its_id() {
    let mut disagreements = Vec::new();

    for (what, sql) in [
        ("a start named by a node column", STARTED_BY_KIND),
        (
            "a start named across two tables that describe nodes",
            STARTED_ACROSS_TABLES,
        ),
    ] {
        let database = metadata_started_store();
        let eager = database
            .execute(sql, &empty())
            .unwrap_or_else(|error| panic!("the executor answers {what}: {error}"));
        // The oracle itself is checked, so this case cannot degrade into two
        // doors agreeing on a full scan.
        assert_eq!(
            eager.trace.physical_plan, "AdjacencyProbe",
            "the executor resolves the start and then probes adjacency for {what}: {:?}",
            eager.trace
        );

        let bounded = match database
            .read_session(roomy())
            .expect("open a bounded read view")
            .execute(sql, &empty())
        {
            Ok(bounded) => bounded,
            Err(error) => {
                disagreements.push(format!(
                    "{what}: the executor answers {} and a bounded read refuses: {error}",
                    answer(&eager, false)
                ));
                continue;
            }
        };
        println!(
            "OBSERVED {what}: executor {} examined={} | bounded {} examined={}",
            described(&eager.trace),
            eager.trace.rows_examined,
            described(&bounded.trace),
            bounded.trace.rows_examined
        );

        if answer(&bounded, false) != answer(&eager, false) {
            disagreements.push(format!(
                "{what}: the executor answers {} and a bounded read answers {}",
                answer(&eager, false),
                answer(&bounded, false)
            ));
        }
        if described(&bounded.trace) != described(&eager.trace) {
            disagreements.push(format!(
                "{what}: the executor describes the plan as {} and a bounded read describes it \
                 as {}",
                described(&eager.trace),
                described(&bounded.trace)
            ));
        }
        if bounded.trace.rows_examined != eager.trace.rows_examined {
            disagreements.push(format!(
                "{what}: the executor examined {} rows and a bounded read reports {}",
                eager.trace.rows_examined, bounded.trace.rows_examined
            ));
        }
    }

    assert!(
        disagreements.is_empty(),
        "a start the executor resolves at execution time is still a pinned start: a door that \
         calls it unpinned walks every edge in the store to answer a two-entry probe, and tells \
         the operator it did:\n{}",
        disagreements.join("\n")
    );
}

/// How many live edges of the type under test leave the hub, how many were
/// removed (their entries stay in the adjacency list, tombstoned), and how
/// many leave it under a type nobody asked for.
const LIVE_LINKS: u128 = 5;
const REMOVED_LINKS: u128 = 4;
const OTHER_TYPE_EDGES: u128 = 7;

/// A hub whose adjacency list holds three kinds of entry: ones the probe must
/// return, ones it must skip because they were removed, and ones it must skip
/// because they are the wrong type. A door that reports every entry it stepped
/// over is describing work the answer does not contain.
fn crowded_adjacency_store() -> Database {
    let database = Database::open_memory();
    database
        .execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &empty())
        .expect("create the node table");
    database
        .execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
             edge_type TEXT)",
            &empty(),
        )
        .expect("create the edge table");

    let hub = node(9_000);
    let connect = |target: Uuid, edge_type: &str| {
        let tx = database.begin().expect("begin an edge write");
        database
            .insert_edge(tx, hub, target, edge_type.to_owned(), HashMap::new())
            .expect("connect the hub to a node");
        database.commit(tx).expect("commit an edge write");
    };

    for ordinal in 0..LIVE_LINKS {
        connect(node(9_100 + ordinal), "LINKS");
    }
    for ordinal in 0..REMOVED_LINKS {
        let target = node(9_200 + ordinal);
        connect(target, "LINKS");
        let tx = database.begin().expect("begin a removal");
        database
            .delete_edge(tx, hub, target, "LINKS")
            .expect("remove an edge the probe must not return");
        database.commit(tx).expect("commit a removal");
    }
    for ordinal in 0..OTHER_TYPE_EDGES {
        connect(node(9_300 + ordinal), "MENTIONS");
    }
    database
}

#[test]
fn a_bounded_probe_counts_the_entries_in_its_answer_not_the_ones_it_stepped_over() {
    let database = crowded_adjacency_store();
    let sql = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $hub \
               COLUMNS (b.id AS t))";
    let hub = params(vec![("hub", Value::Uuid(node(9_000)))]);

    let eager = database
        .execute(sql, &hub)
        .expect("the executor answers the probe");
    assert_eq!(
        eager.rows.len() as u128,
        LIVE_LINKS,
        "the probe returns only the live edges of the type it asked for"
    );
    assert_eq!(
        eager.trace.rows_examined as u128, LIVE_LINKS,
        "the executor counts what it returned, not the removed and wrong-typed entries it \
         skipped past: {:?}",
        eager.trace
    );

    let bounded = database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(sql, &hub)
        .expect("a bounded read answers the probe");
    println!(
        "OBSERVED a crowded adjacency list ({LIVE_LINKS} live, {REMOVED_LINKS} removed, \
         {OTHER_TYPE_EDGES} of another type): executor examined {} | bounded examined {}",
        eager.trace.rows_examined, bounded.trace.rows_examined
    );

    assert_eq!(
        answer(&bounded, false),
        answer(&eager, false),
        "both doors return the same live edges"
    );
    assert_eq!(
        bounded.trace.rows_examined, eager.trace.rows_examined,
        "rows examined is what an operator reads to tell a probe that touched a handful of edges \
         from one that walked a list: the executor reports {} and a bounded read reports {}, \
         which is every entry it stepped over rather than the ones its answer is made of",
        eager.trace.rows_examined, bounded.trace.rows_examined
    );
}
