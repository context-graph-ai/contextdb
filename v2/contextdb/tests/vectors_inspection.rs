//! Inspection of VECTOR columns: `SHOW VECTOR_INDEXES`, `SHOW VECTOR_PARTITIONS` and `explain`
//! say what the vector part keeps and how a search would go, from its small records alone:
//! they read no block and change nothing.

mod support;

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

use contextdb::{ContextId, DatabaseOpenOptions, Error, ReadSession, Uuid, Value, VectorIndexRef};

use support::{Run, Shown};

fn int(number: i64) -> Value {
    Value::Int64(number)
}

/// Asserts the fields of an inspection row that hold text, numbers, flags and NULL. The two
/// JSON fields are compared apart.
fn assert_fields(row: &Shown, wanted: &[(&str, Value)], what: &str) {
    for (name, value) in wanted {
        assert_eq!(support::field(row, name), value, "{what}: {name}");
    }
}

/// Two columns: `docs.v`, keyed by context, with 600 sealed vectors under `a` and 400 pending
/// under `b`, which together bring the column to its declared size; and `plain.e`, with no
/// key and no vector.
fn two_columns(name: &str) -> (std::path::PathBuf, contextdb::Database) {
    let clauses = "PARTITION_KEY (ctx) AUTO_INDEX_AT 1000 CONSOLIDATION (CHANGE_PERCENT = 50)";
    let (folder, db) = support::built(name, 8, clauses, 0);
    support::run(
        &db,
        "CREATE TABLE plain (id INTEGER PRIMARY KEY, e VECTOR(4))",
    );
    support::load_docs(&db, 1..=600, "a", |n| support::vector(n, 8));
    assert_eq!(support::upkeep(&db), (1, 600));
    support::load_docs(&db, 601..=1000, "b", |n| support::vector(n, 8));
    (folder, db)
}

#[test]
fn show_vector_indexes_describes_every_vector_column() {
    let (folder, db) = two_columns("show-vector-indexes");
    // The names of the fields are held by `tests/sql/vector_inspection.sql`, through the
    // writer: every other door gives the same.
    let fields = support::run(&db, "SHOW VECTOR_INDEXES").columns().to_vec();
    let through = |on: &dyn Run, door: &str| {
        let result = support::run(&on, "SHOW VECTOR_INDEXES");
        assert_eq!(result.columns(), fields, "through {door}");
        let rows = support::shown(&on, "SHOW VECTOR_INDEXES");
        assert_eq!(rows.len(), 2, "through {door}");
        let text = support::text;
        assert_fields(
            &rows[0],
            &[
                ("table", text("docs")),
                ("column", text("v")),
                ("dimension", int(8)),
                ("quantization", text("F32")),
                ("vector_count", int(1000)),
                ("bytes", int(600 * 16)),
                ("max_partitions", int(256)),
                ("live_partitions", int(2)),
                ("search_mode", text("AUTO")),
                ("declared_auto_index_at", int(1000)),
                ("effective_auto_index_at", int(1000)),
                ("declared_consolidation_mode", text("PERCENT")),
                ("declared_consolidation_change_percent", int(50)),
                ("declared_consolidation_tombstone_percent", Value::Null),
                ("effective_consolidation_mode", text("PERCENT")),
                ("effective_consolidation_change_percent", int(50)),
                ("effective_consolidation_tombstone_percent", int(10)),
                ("pending_inserts", int(400)),
                ("tombstones", int(0)),
                ("durable_vector_bytes", int(600 * 16)),
                ("durable_index_bytes", int(0)),
                ("indexed_available", Value::Bool(true)),
                ("maintenance_state", text("pending")),
                ("maintenance_vectors_total", int(600)),
                ("maintenance_vectors_done", int(0)),
                ("maintenance_vectors_remaining", int(600)),
                ("unavailable_partitions", int(0)),
                ("stalled_partitions", int(0)),
                ("broad_route", text("fan_out")),
                ("recovery_action", text("run a maintenance cycle")),
            ],
            door,
        );
        assert_eq!(
            support::json(&rows[0], "partition_key_columns"),
            contextdb::serde_json::json!(["ctx"])
        );
        assert_fields(
            &rows[1],
            &[
                ("table", text("plain")),
                ("column", text("e")),
                ("dimension", int(4)),
                ("quantization", text("F32")),
                ("vector_count", int(0)),
                ("bytes", int(0)),
                ("max_partitions", Value::Null),
                ("live_partitions", int(0)),
                ("search_mode", text("AUTO")),
                ("declared_auto_index_at", Value::Null),
                ("effective_auto_index_at", int(100_000)),
                ("declared_consolidation_mode", Value::Null),
                ("declared_consolidation_change_percent", Value::Null),
                ("declared_consolidation_tombstone_percent", Value::Null),
                ("effective_consolidation_mode", text("PERCENT")),
                ("effective_consolidation_change_percent", int(20)),
                ("effective_consolidation_tombstone_percent", int(10)),
                ("pending_inserts", int(0)),
                ("indexed_available", Value::Bool(true)),
                ("maintenance_state", text("idle")),
                ("maintenance_vectors_total", int(0)),
                ("broad_route", text("fan_out")),
                ("recovery_action", text("none")),
            ],
            door,
        );
        assert_eq!(
            support::json(&rows[1], "partition_key_columns"),
            contextdb::serde_json::json!([])
        );
    };
    through(&db, "the writer");
    let tx = db.transaction().expect("begin");
    through(&tx, "a transaction");
    tx.rollback();
    through(
        &ReadSession::open(&folder).expect("open for reading"),
        "a read session",
    );
}

#[test]
fn show_vector_partitions_lists_partitions_by_key_with_limit_and_offset() {
    let (folder, db) = two_columns("show-vector-partitions");
    let reader = ReadSession::open(&folder).expect("open for reading");
    // The names of the fields are held by `tests/sql/vector_inspection.sql`, through the
    // writer: a reader gives the same.
    let result = support::run(&reader, "SHOW VECTOR_PARTITIONS");
    let by_writer = support::run(&db, "SHOW VECTOR_PARTITIONS");
    assert_eq!(result.columns(), by_writer.columns());
    // The key of each row a statement returns, with its column.
    let keys = |sql: &str| -> Vec<String> {
        support::shown(&reader, sql)
            .iter()
            .map(|row| {
                format!(
                    "{}.{} {}",
                    support::word(row, "table"),
                    support::word(row, "column"),
                    support::json(row, "partition_key")
                )
            })
            .collect()
    };
    let (a, b, plain) = (
        r#"docs.v {"ctx":"a"}"#,
        r#"docs.v {"ctx":"b"}"#,
        "plain.e {}",
    );
    assert_eq!(keys("SHOW VECTOR_PARTITIONS"), [a, b, plain]);
    assert_eq!(keys("SHOW VECTOR_PARTITIONS FOR docs.v"), [a, b]);
    assert_eq!(keys("SHOW VECTOR_PARTITIONS FOR docs.v LIMIT 1"), [a]);
    assert_eq!(
        keys("SHOW VECTOR_PARTITIONS FOR docs.v LIMIT 1 OFFSET 1"),
        [b]
    );
    assert_eq!(keys("SHOW VECTOR_PARTITIONS LIMIT 5 OFFSET 2"), [plain]);
    assert_eq!(keys("SHOW VECTOR_PARTITIONS LIMIT 2"), [a, b]);
    assert_eq!(keys("SHOW VECTOR_PARTITIONS FOR plain.e"), [plain]);

    let rows = support::shown(&db, "SHOW VECTOR_PARTITIONS");
    let text = support::text;
    // Sealed, in a column that has just come to its size: its first grouping is due.
    assert_fields(
        &rows[0],
        &[
            ("live_rows", int(600)),
            ("pending_inserts", int(0)),
            ("tombstones", int(0)),
            ("durable_vector_bytes", int(600 * 16)),
            ("durable_index_bytes", int(0)),
            ("indexed_available", Value::Bool(true)),
            ("availability_reason", text("none")),
            ("maintenance_state", text("pending")),
            ("maintenance_reason", text("first_grouping")),
            ("maintenance_vectors_total", int(600)),
            ("maintenance_vectors_done", int(0)),
            ("maintenance_vectors_remaining", int(600)),
            ("lists", Value::Null),
            ("probes", Value::Null),
            ("recovery_action", text("run a maintenance cycle")),
        ],
        "the sealed partition",
    );
    assert_fields(
        &rows[1],
        &[
            ("live_rows", int(400)),
            ("pending_inserts", int(400)),
            ("durable_vector_bytes", int(0)),
            ("lists", Value::Null),
        ],
        "the pending partition",
    );
    // A column with no key has one row before its first vector.
    assert_fields(
        &rows[2],
        &[
            ("live_rows", int(0)),
            ("pending_inserts", int(0)),
            ("tombstones", int(0)),
            ("durable_vector_bytes", int(0)),
            ("durable_index_bytes", int(0)),
            ("maintenance_state", text("idle")),
            ("maintenance_reason", text("none")),
            ("lists", Value::Null),
            ("recovery_action", text("none")),
        ],
        "the column with no vector",
    );

    // The two refusals.
    match support::refusal(&db, "SHOW VECTOR_PARTITIONS FOR nowhere.v") {
        Error::TableNotFound(table) => assert_eq!(table, "nowhere"),
        other => panic!("a table that is not there gave {other:?}"),
    }
    match support::refusal(&db, "SHOW VECTOR_PARTITIONS FOR docs.tag") {
        Error::UnknownVectorIndex { index } => {
            let wanted = VectorIndexRef {
                table: "docs".to_owned(),
                column: "tag".to_owned(),
            };
            assert_eq!(index, wanted);
        }
        other => panic!("a column that is no VECTOR column gave {other:?}"),
    }
}

/// A search of `docs` with this filter and mode, with nothing bound.
fn searched(filter: Option<&str>, mode: &str) -> String {
    support::by_mode(filter, mode, 10)
}

#[test]
fn inspection_and_explain_read_no_block_and_change_nothing() {
    // One block of 2,000 slots of 72 bytes with keys of 9: 170,004 bytes.
    const BLOCK: u64 = 4 + 2000 * (72 + 4 + 9);
    let (folder, db) = support::built("inspection-reads-no-block", 64, "AUTO_INDEX_AT 1000", 2000);
    support::close(db);
    let (door, bytes) = support::counting_bytes_read();
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(door);
    let db = support::open_with(&folder, options).expect("open");
    let (opened, before) = (bytes.load(Ordering::SeqCst), db.committed_watermark());
    assert_eq!(support::shown(&db, "SHOW VECTOR_INDEXES").len(), 1);
    assert_eq!(
        support::field(&support::the_partition(&db), "lists"),
        &int(45)
    );
    let lines = db.explain(&searched(None, "INDEXED")).expect("explain");
    assert!(lines.contains("route: indexed"), "{lines}");
    let read = bytes.load(Ordering::SeqCst) - opened;
    assert!(
        read < BLOCK,
        "inspection and explain read {read} bytes; the block is {BLOCK}"
    );
    assert_eq!(db.committed_watermark(), before);
}

/// The lines of an explanation, each trimmed of the spaces it begins with.
fn lines_of(explained: &str) -> Vec<&str> {
    explained.lines().map(str::trim_start).collect()
}

#[test]
fn explain_names_how_each_partition_is_searched() {
    // Partition a is built; b has 2,000 vectors outside any grouping, twice the declared size.
    let clauses = "PARTITION_KEY (ctx) AUTO_INDEX_AT 1000";
    let (folder, db) = support::built("explain-routes", 16, clauses, 2000);
    support::load_docs(&db, 2001..=4000, "b", |n| support::gathered(n, 16));
    let reader = ReadSession::open(&folder).expect("open for reading");
    let explain = |sql: &str| -> String {
        let from_writer = db.explain(sql).expect("the writer explains");
        assert_eq!(reader.explain(sql).expect("a reader explains"), from_writer);
        from_writer
    };
    let routes = |lines: &[&str]| -> Vec<String> {
        let routes = lines.iter().filter(|line| line.starts_with("route: "));
        routes
            .map(|line| line["route: ".len()..].to_owned())
            .collect()
    };

    let explained = explain(&searched(None, "EXACT"));
    let lines = lines_of(&explained);
    for wanted in [
        "requested_mode: EXACT",
        "resolved_mode: EXACT",
        "effective_auto_index_at: 1000",
        "aggregate_allowed_vectors: 4000",
        r#"partition_key_columns: ["ctx"]"#,
        "partitions: 2",
        r#"partition: {"ctx":"a"}"#,
        r#"partition: {"ctx":"b"}"#,
        "refusal: none",
    ] {
        assert!(
            lines.contains(&wanted),
            "no line {wanted:?} in\n{explained}"
        );
    }
    assert_eq!(routes(&lines), ["exact", "exact"]);
    assert_eq!(lines.last(), Some(&"refusal: none"));

    // The built partition alone, under INDEXED: its own six lines.
    let explained = explain(&searched(Some("ctx = 'a'"), "INDEXED"));
    let lines = lines_of(&explained);
    let at = lines
        .iter()
        .position(|line| *line == r#"partition: {"ctx":"a"}"#)
        .unwrap_or_else(|| panic!("no partition line in\n{explained}"));
    assert_eq!(lines[at + 1..at + 3], ["route: indexed", "lists: 45"]);
    assert!(lines[at + 3].starts_with("probes: "), "{explained}");
    assert_eq!(
        lines[at + 4..at + 6],
        ["live_rows: 2000", "outside_grouping: 0"]
    );
    assert!(explained.contains("\n  route: indexed\n"), "{explained}");
    assert!(lines.contains(&"partitions: 1") && lines.contains(&"refusal: none"));

    // Both under INDEXED: the partition that is behind refuses the search, in its sentence.
    let explained = explain(&searched(None, "INDEXED"));
    let refusal = format!("refusal: docs.v{}", support::INDEXED_REFUSED);
    assert_eq!(lines_of(&explained).last(), Some(&refusal.as_str()));
    assert!(explained.contains("outside_grouping: 2000"), "{explained}");

    // Under AUTO each partition goes its own way and nothing is refused.
    let explained = explain(&searched(None, "AUTO"));
    let lines = lines_of(&explained);
    assert!(lines.contains(&"requested_mode: AUTO") && lines.contains(&"resolved_mode: AUTO"));
    assert_eq!(routes(&lines), ["indexed", "exact"]);
    assert_eq!(lines.last(), Some(&"refusal: none"));

    // A partition named through a parameter is not known: every partition is listed.
    let explained = explain(&searched(Some("ctx = $c"), "EXACT"));
    let lines = lines_of(&explained);
    let unbound = |line: &&str| {
        line.starts_with("partitions: every partition (") && line.ends_with("is not bound)")
    };
    assert!(lines.iter().any(unbound), "{explained}");
    assert_eq!(routes(&lines).len(), 2);

    // A handle narrowed to Context 1 is told nothing of the partitions of other Contexts: no
    // partition line, no count, no id of Context 2; the unnarrowed handle's text is unchanged.
    support::run(
        &db,
        "CREATE TABLE scoped (id INTEGER PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
         v VECTOR(2) PARTITION_KEY (context_id))",
    );
    let (one, two) = (Uuid::from_u128(0x00C0_0001), Uuid::from_u128(0x00C0_0002));
    for (id, context) in [(1, one), (2, two)] {
        let sql = format!("INSERT INTO scoped VALUES ({id}, '{context}', [1.0, 0.0])");
        support::run(&db, &sql);
    }
    let sql = "SELECT id, v <=> [1.0, 0.0] AS distance FROM scoped ORDER BY distance LIMIT 10";
    let whole = db.explain(sql).expect("explain");
    assert!(whole.contains(&two.to_string()), "{whole}");
    let narrowed = db.scoped_with_contexts(BTreeSet::from([ContextId::new(one)]));
    let seen = narrowed.explain(sql).expect("a narrowed handle explains");
    assert!(
        !seen.contains("partition:")
            && !seen.contains("partitions:")
            && !seen.contains(&two.to_string()),
        "a handle narrowed to Context 1 was told of other partitions:\n{seen}"
    );
    assert_eq!(db.explain(sql).expect("explain"), whole);

    // Any other statement is a later version's to explain.
    match db.explain("SELECT id FROM docs") {
        Err(error) => assert!(
            error.to_string().ends_with(
                "explaining this statement is not supported in this version of the library"
            ),
            "{error}"
        ),
        Ok(explained) => panic!("a plain SELECT was explained:\n{explained}"),
    }
}

#[test]
fn explain_says_that_auto_below_the_columns_size_resolves_to_exact() {
    // Fifty vectors in a column whose size is the 100,000 of no AUTO_INDEX_AT.
    let (_, db) = support::built("explain-auto-below-its-size", 16, "", 50);
    let explained = db.explain(&searched(None, "AUTO")).expect("explain");
    let lines = lines_of(&explained);
    for wanted in [
        "requested_mode: AUTO",
        "resolved_mode: EXACT",
        "effective_auto_index_at: 100000",
        "aggregate_allowed_vectors: 50",
        "refusal: none",
    ] {
        assert!(
            lines.contains(&wanted),
            "no line {wanted:?} in\n{explained}"
        );
    }
}
