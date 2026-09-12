use super::common::*;
use contextdb_core::{Error, Value};
use contextdb_engine::cli_render::render_explain;
use contextdb_engine::{Database, QueryResult};
use std::panic::{AssertUnwindSafe, catch_unwind};
use uuid::Uuid;

fn ce_uuid(n: u128) -> Uuid {
    Uuid::from_u128(0xCE00_0000_0000_0000_0000_0000_0000_0000 + n)
}

fn pushed(r: &QueryResult) -> Vec<&str> {
    r.trace
        .predicates_pushed
        .iter()
        .map(|c| c.as_ref())
        .collect()
}

const FEWER: &str = "fewer predicate columns matched than chosen index";
const FIRST: &str = "first column not in WHERE";
const TIED: &str = "tied with chosen index; lost by creation order";

fn reason<'a>(r: &'a QueryResult, name: &str) -> Option<&'a str> {
    r.trace
        .indexes_considered
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.rejected_reason.as_ref())
}

fn seed_edge(
    db: &Database,
    id_n: u128,
    source_id: Uuid,
    context_id: Uuid,
    edge_type: &str,
    target_id: Uuid,
) {
    db.execute(
        "INSERT INTO edges (id, source_id, context_id, edge_type, target_id) VALUES ($id, $source, $ctx, $etype, $target)",
        &params(vec![
            ("id", Value::Uuid(ce_uuid(id_n))),
            ("source", Value::Uuid(source_id)),
            ("ctx", Value::Uuid(context_id)),
            ("etype", Value::Text(edge_type.into())),
            ("target", Value::Uuid(target_id)),
        ]),
    )
    .unwrap();
}

/// EXPLAIN tells a consumer why each competing index lost. On a DAG edges table with a composite
/// UNIQUE constraint, the hop composite beats both automatic indexes and a hand-made index
/// (fewer predicate columns) and the primary key (first column not in WHERE), and pushes every
/// predicate so only the matching rows are read. When two composites tie, the one created later
/// says it lost by creation order.
#[test]
fn acceptance_explain_says_why_each_competing_composite_index_lost() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE edges (
            id UUID PRIMARY KEY,
            source_id UUID,
            context_id UUID,
            edge_type TEXT,
            target_id UUID,
            UNIQUE (source_id, target_id, edge_type)
        ) DAG('HOP')",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_hop ON edges (source_id, context_id, edge_type)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_alt ON edges (source_id, target_id)",
        &empty_params(),
    )
    .unwrap();

    let ctx = ce_uuid(0xC);
    let other_ctx = ce_uuid(0xD);
    let sources = [ce_uuid(1), ce_uuid(2), ce_uuid(3)];
    let mut id_n = 10;
    for (s_idx, source) in sources.iter().copied().enumerate() {
        for m in 0..4 {
            seed_edge(
                &db,
                id_n,
                source,
                ctx,
                "HOP",
                ce_uuid(0x100 + s_idx as u128 * 10 + m),
            );
            id_n += 1;
        }
        for noise in 0..20 {
            let context = if noise % 2 == 0 { other_ctx } else { ctx };
            let edge_type = if noise % 2 == 0 { "HOP" } else { "OTHER" };
            seed_edge(&db, id_n, source, context, edge_type, ce_uuid(0x500 + id_n));
            id_n += 1;
        }
    }

    let sql = "SELECT target_id FROM edges WHERE source_id IN ($s, $s2, $s3) AND context_id = $ctx AND edge_type = $et";
    let query_params = params(vec![
        ("s", Value::Uuid(sources[0])),
        ("s2", Value::Uuid(sources[1])),
        ("s3", Value::Uuid(sources[2])),
        ("ctx", Value::Uuid(ctx)),
        ("et", Value::Text("HOP".into())),
    ]);
    db.__reset_rows_examined();
    let r = db.execute(sql, &query_params).unwrap();

    assert_eq!(r.trace.index_used.as_deref(), Some("idx_hop"));
    assert_eq!(pushed(&r), vec!["source_id", "context_id", "edge_type"]);
    assert_eq!(
        reason(&r, "__unique_source_id_target_id_edge_type"),
        Some(FEWER)
    );
    assert_eq!(reason(&r, "__graph_edge_source_target_type"), Some(FEWER));
    assert_eq!(reason(&r, "idx_alt"), Some(FEWER));
    assert_eq!(reason(&r, "__pk_id"), Some(FIRST));
    assert_eq!(r.rows.len(), 12);
    assert_eq!(db.__rows_examined(), 12);

    let explain = render_explain(&db, sql, &query_params).unwrap();
    assert!(explain.contains(
        "__unique_source_id_target_id_edge_type: fewer predicate columns matched than chosen index"
    ));
    assert!(explain.contains(
        "__graph_edge_source_target_type: fewer predicate columns matched than chosen index"
    ));
    assert!(explain.contains("idx_alt: fewer predicate columns matched than chosen index"));
    assert!(explain.contains("__pk_id: first column not in WHERE"));
    assert!(explain.contains("idx_hop"));
    assert!(explain.contains("source_id"));
    assert!(explain.contains("context_id"));
    assert!(explain.contains("edge_type"));

    let tie = Database::open_memory();
    tie.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, context_id UUID, weight INTEGER)",
        &empty_params(),
    )
    .unwrap();
    tie.execute(
        "CREATE INDEX idx_long ON edges (source_id, context_id, weight)",
        &empty_params(),
    )
    .unwrap();
    tie.execute(
        "CREATE INDEX idx_short ON edges (source_id, context_id)",
        &empty_params(),
    )
    .unwrap();
    let source = ce_uuid(0x21);
    let cutoff = ce_uuid(0x30);
    for i in 0..20 {
        tie.execute(
            "INSERT INTO edges (id, source_id, context_id, weight) VALUES ($id, $source, $ctx, $weight)",
            &params(vec![
                ("id", Value::Uuid(ce_uuid(0x1000 + i))),
                ("source", Value::Uuid(source)),
                ("ctx", Value::Uuid(ce_uuid(0x31 + i))),
                ("weight", Value::Int64(i as i64)),
            ]),
        )
        .unwrap();
    }
    let tie_sql =
        "SELECT id FROM edges WHERE source_id IN ($s) AND context_id > $c ORDER BY id ASC";
    let tie_params = params(vec![("s", Value::Uuid(source)), ("c", Value::Uuid(cutoff))]);
    let r = tie.execute(tie_sql, &tie_params).unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_long"));
    assert_eq!(pushed(&r), vec!["source_id"]);
    assert_eq!(reason(&r, "idx_short"), Some(TIED));

    let explain = render_explain(&tie, tie_sql, &tie_params).unwrap();
    assert!(explain.contains("idx_short: tied with chosen index; lost by creation order"));
    assert!(explain.contains("idx_long"));
}

#[test]
fn acceptance_first_composite_index_explain_walkthrough() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, tag TEXT, score INTEGER)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_tag_score ON notes (tag, score)",
        &empty_params(),
    )
    .unwrap();
    db.execute("ALTER TABLE notes ADD COLUMN tag2 TEXT", &empty_params())
        .unwrap();
    for (n, tag, score) in [(1, "work", 5), (2, "work", 9), (3, "home", 5)] {
        db.execute(
            "INSERT INTO notes (id, tag, score, tag2) VALUES ($id, $tag, $score, $tag2)",
            &params(vec![
                ("id", Value::Uuid(ce_uuid(n))),
                ("tag", Value::Text(tag.into())),
                ("score", Value::Int64(score)),
                ("tag2", Value::Text("x".into())),
            ]),
        )
        .unwrap();
    }
    let explain = render_explain(
        &db,
        "SELECT id FROM notes WHERE tag = 'work' AND score = 5",
        &empty_params(),
    )
    .unwrap();
    assert!(explain.contains("IndexScan"));
    assert!(explain.contains("idx_tag_score"));
    assert!(explain.contains("predicates_pushed: [tag, score]"));

    let scan_explain = render_explain(
        &db,
        "SELECT id FROM notes WHERE tag2 = 'x'",
        &empty_params(),
    )
    .unwrap();
    assert!(scan_explain.contains("Scan"));
}

#[test]
fn acceptance_degenerate_in_queries_graceful() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, tag TEXT, score INTEGER)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_tag_score ON notes (tag, score)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO notes (id, tag, score) VALUES ($id, 'work', 5)",
        &params(vec![("id", Value::Uuid(ce_uuid(1)))]),
    )
    .unwrap();

    let empty_in = db.execute(
        "SELECT id FROM notes WHERE tag IN () AND score = 5",
        &empty_params(),
    );
    assert!(matches!(empty_in, Err(Error::ParseError(_))));

    let plain = Database::open_memory();
    plain
        .execute(
            "CREATE TABLE plain_notes (id UUID PRIMARY KEY, tag TEXT)",
            &empty_params(),
        )
        .unwrap();
    plain
        .execute(
            "INSERT INTO plain_notes (id, tag) VALUES ($id, 'work')",
            &params(vec![("id", Value::Uuid(ce_uuid(2)))]),
        )
        .unwrap();
    let unindexed = plain
        .execute(
            "SELECT id FROM plain_notes WHERE tag IN (1, 'work')",
            &empty_params(),
        )
        .unwrap();
    assert_eq!(unindexed.trace.physical_plan, "Scan");
    assert_eq!(unindexed.rows, vec![vec![Value::Uuid(ce_uuid(2))]]);

    db.__reset_rows_examined();
    let indexed = catch_unwind(AssertUnwindSafe(|| {
        db.execute(
            "SELECT id FROM notes WHERE tag IN (1, 'work') AND score = 5",
            &empty_params(),
        )
    }));
    assert!(
        indexed.is_ok(),
        "mixed-type IN on indexed TEXT column must not panic"
    );
    let indexed = indexed.unwrap().unwrap();
    assert_eq!(indexed.trace.physical_plan, "IndexScan");
    assert_eq!(indexed.trace.index_used.as_deref(), Some("idx_tag_score"));
    assert_eq!(pushed(&indexed), vec!["tag", "score"]);
    assert_eq!(indexed.rows, vec![vec![Value::Uuid(ce_uuid(1))]]);
    assert_eq!(db.__rows_examined(), 1);
}
