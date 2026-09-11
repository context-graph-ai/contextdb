//! SQL spellings for declared vector partitions, search modes, and inspection.

use contextdb_parser::ast::Statement;
use contextdb_parser::parse;

fn parse_sql(sql: &str) -> Statement {
    parse(sql).unwrap_or_else(|error| panic!("SQL must parse: {sql}\nerror: {error:?}"))
}

#[test]
fn existing_vector_declarations_and_nearest_neighbor_queries_still_parse() {
    assert!(matches!(
        parse_sql(
            "CREATE TABLE vector_control (id INTEGER PRIMARY KEY, embedding VECTOR(3) WITH (quantization = 'SQ8'))"
        ),
        Statement::CreateTable(_)
    ));
    assert!(matches!(
        parse_sql("ALTER TABLE vector_control ADD COLUMN fallback VECTOR(3)"),
        Statement::AlterTable(_)
    ));
    assert!(matches!(
        parse_sql("SELECT id FROM vector_control ORDER BY embedding <=> $query LIMIT 10"),
        Statement::Select(_)
    ));
}

#[test]
fn create_table_accepts_defaulted_and_explicit_partition_declarations() {
    let declarations = [
        "CREATE TABLE defaulted_vectors (id INTEGER PRIMARY KEY, scope_id UUID NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id))",
        "CREATE TABLE automatic_vectors (id INTEGER PRIMARY KEY, scope_id UUID NOT NULL, kind TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id, kind) SEARCH_MODE AUTO)",
        "CREATE TABLE exact_vectors (id INTEGER PRIMARY KEY, scope_id UUID NOT NULL, embedding VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope_id) MAX_PARTITIONS 32 SEARCH_MODE EXACT)",
        "CREATE TABLE indexed_vectors (id INTEGER PRIMARY KEY, scope_id UUID NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 64 SEARCH_MODE INDEXED)",
    ];

    for sql in declarations {
        assert!(matches!(parse_sql(sql), Statement::CreateTable(_)));
    }
}

#[test]
fn add_column_accepts_defaulted_and_explicit_partition_declarations() {
    let declarations = [
        "ALTER TABLE vector_items ADD COLUMN embedding VECTOR(3) PARTITION_KEY (scope_id)",
        "ALTER TABLE vector_items ADD COLUMN compact_embedding VECTOR(3) WITH (quantization = 'SQ4') PARTITION_KEY (scope_id, kind) MAX_PARTITIONS 128 SEARCH_MODE INDEXED",
    ];

    for sql in declarations {
        assert!(matches!(parse_sql(sql), Statement::AlterTable(_)));
    }
}

#[test]
fn max_partitions_without_a_partition_key_reaches_semantic_validation() {
    assert!(matches!(
        parse_sql(
            "CREATE TABLE invalid_limit_scope (id INTEGER PRIMARY KEY, embedding VECTOR(3) MAX_PARTITIONS 8)"
        ),
        Statement::CreateTable(_)
    ));
}

#[test]
fn unpartitioned_vector_columns_accept_every_search_mode() {
    for mode in ["AUTO", "EXACT", "INDEXED"] {
        let sql = format!(
            "CREATE TABLE unpartitioned_{mode} (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE {mode})"
        );
        assert!(matches!(parse_sql(&sql), Statement::CreateTable(_)));
    }
}

#[test]
fn alter_column_accepts_online_partition_limit_and_search_mode_changes() {
    assert!(matches!(
        parse_sql("ALTER TABLE vector_items ALTER COLUMN embedding SET MAX_PARTITIONS 512"),
        Statement::AlterTable(_)
    ));

    for mode in ["AUTO", "EXACT", "INDEXED"] {
        let sql = format!("ALTER TABLE vector_items ALTER COLUMN embedding SET SEARCH_MODE {mode}");
        assert!(matches!(parse_sql(&sql), Statement::AlterTable(_)));
    }
}

#[test]
fn vector_search_override_follows_vector_ordering_and_precedes_rank_and_limit() {
    for mode in ["AUTO", "EXACT", "INDEXED"] {
        let sql = format!(
            "SELECT passage_id FROM event_search_passages WHERE context_id = $context AND generation_id = $generation AND is_current = TRUE ORDER BY vector_text <=> $query USE VECTOR {mode} LIMIT 10"
        );
        assert!(matches!(parse_sql(&sql), Statement::Select(_)));
    }

    assert!(matches!(
        parse_sql(
            "SELECT passage_id FROM event_search_passages ORDER BY vector_text <=> $query USE VECTOR INDEXED USE RANK relevance LIMIT 10"
        ),
        Statement::Select(_)
    ));
}

#[test]
fn vector_partition_inspection_accepts_global_targeted_and_paged_forms() {
    for sql in [
        "SHOW VECTOR_PARTITIONS;",
        "SHOW VECTOR_PARTITIONS FOR vector_items.embedding;",
        "SHOW VECTOR_PARTITIONS FOR vector_items.embedding LIMIT 50 OFFSET 100;",
    ] {
        let _ = parse_sql(sql);
    }
}

#[test]
fn partition_inspection_requires_both_table_and_column_after_for() {
    for sql in [
        "SHOW VECTOR_PARTITIONS FOR vector_items;",
        "SHOW VECTOR_PARTITIONS FOR vector_items LIMIT 2;",
        "SHOW VECTOR_PARTITIONS FOR vector_items.;",
        "SHOW VECTOR_PARTITIONS FOR vector_items.embedding.extra;",
    ] {
        assert!(
            contextdb_parser::parse(sql).is_err(),
            "unexpectedly accepted {sql}"
        );
    }
    for sql in [
        "SHOW VECTOR_PARTITIONS;",
        "SHOW VECTOR_PARTITIONS LIMIT 2;",
        "SHOW VECTOR_PARTITIONS FOR vector_items.embedding;",
        "SHOW VECTOR_PARTITIONS FOR vector_items.embedding LIMIT 2 OFFSET 1;",
    ] {
        assert!(
            contextdb_parser::parse(sql).is_ok(),
            "unexpectedly refused {sql}"
        );
    }
}

#[test]
fn vector_policy_spellings_preserve_values_and_defaults_on_create_add_and_alter() {
    use contextdb_parser::{AlterAction, VectorPolicyValue, VectorSearchMode};

    for declaration in [
        "VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 17 HNSW (EF_SEARCH = DEFAULT, M = 12, EF_CONSTRUCTION = 64)",
        "vector(3) with (quantization = 'SQ8') partition_key (scope_id) max_partitions 8 search_mode indexed auto_index_at 17 hnsw (ef_search = default, m = 12, ef_construction = 64)",
    ] {
        for sql in [
            format!(
                "CREATE TABLE vector_items (scope_id INTEGER NOT NULL, embedding {declaration})"
            ),
            format!("ALTER TABLE vector_items ADD COLUMN embedding {declaration}"),
        ] {
            let column = match parse_sql(&sql) {
                Statement::CreateTable(table) => table.columns.into_iter().last().unwrap(),
                Statement::AlterTable(table) => match table.action {
                    AlterAction::AddColumn(column) => *column,
                    action => panic!("expected ADD COLUMN, got {action:?}"),
                },
                statement => panic!("expected column declaration, got {statement:?}"),
            };
            assert_eq!(column.partition_key_columns, vec!["scope_id"]);
            assert_eq!(column.max_partitions.as_deref(), Some("8"));
            assert_eq!(column.search_mode, Some(VectorSearchMode::Indexed));
            assert_eq!(column.auto_index_at.as_deref(), Some("17"));
            assert_eq!(column.hnsw.m, Some(VectorPolicyValue::Value("12".into())));
            assert_eq!(
                column.hnsw.ef_construction,
                Some(VectorPolicyValue::Value("64".into()))
            );
            assert_eq!(column.hnsw.ef_search, Some(VectorPolicyValue::Default));
        }
    }
    for (suffix, expected) in [
        ("AUTO_INDEX_AT 17", Some("17")),
        ("auto_index_at default", None),
    ] {
        let Statement::AlterTable(table) = parse_sql(&format!(
            "ALTER TABLE vector_items ALTER COLUMN embedding SET {suffix}"
        )) else {
            panic!("expected ALTER TABLE")
        };
        let AlterAction::SetVectorAutoIndexAt { auto_index_at, .. } = table.action else {
            panic!("expected AUTO_INDEX_AT change")
        };
        assert_eq!(auto_index_at.as_deref(), expected);
    }
    let Statement::AlterTable(table) = parse_sql(
        "ALTER TABLE vector_items ALTER COLUMN embedding SET hnsw (ef_search = default, M = 12)",
    ) else {
        panic!("expected ALTER TABLE")
    };
    let AlterAction::SetVectorHnsw {
        hnsw: Some(policy), ..
    } = table.action
    else {
        panic!("expected grouped HNSW change")
    };
    assert_eq!(policy.m, Some(VectorPolicyValue::Value("12".into())));
    assert_eq!(
        policy.ef_construction, None,
        "omission keeps the existing declaration"
    );
    assert_eq!(policy.ef_search, Some(VectorPolicyValue::Default));
    let Statement::AlterTable(table) =
        parse_sql("ALTER TABLE vector_items ALTER COLUMN embedding SET HNSW DEFAULT")
    else {
        panic!("expected ALTER TABLE")
    };
    assert!(matches!(
        table.action,
        AlterAction::SetVectorHnsw { hnsw: None, .. }
    ));
}

#[test]
fn vector_declarations_and_queries_reject_reordered_or_repeated_clauses() {
    for declaration in [
        "PARTITION_KEY (scope_id) WITH (quantization = 'SQ8')",
        "MAX_PARTITIONS 8 PARTITION_KEY (scope_id)",
        "PARTITION_KEY (scope_id) SEARCH_MODE AUTO MAX_PARTITIONS 8",
        "AUTO_INDEX_AT 17 SEARCH_MODE AUTO",
        "HNSW (M = 12) AUTO_INDEX_AT 17",
        "AUTO_INDEX_AT 17 AUTO_INDEX_AT 18",
        "HNSW (M = 12) HNSW (EF_SEARCH = 17)",
        "HNSW ()",
        "HNSW (M = 12, m = 16)",
        "HNSW (UNKNOWN = 12)",
        "HNSW (M 12)",
        "HNSW (M = 12 EF_SEARCH = 17)",
    ] {
        for sql in [
            format!(
                "CREATE TABLE vector_items (scope_id INTEGER NOT NULL, embedding VECTOR(3) {declaration})"
            ),
            format!("ALTER TABLE vector_items ADD COLUMN embedding VECTOR(3) {declaration}"),
        ] {
            assert!(
                parse(&sql).is_err(),
                "out-of-order or malformed declaration accepted: {sql}"
            );
        }
    }
    for sql in [
        "SELECT id FROM vector_items USE VECTOR EXACT ORDER BY embedding <=> $query LIMIT 3",
        "SELECT id FROM vector_items ORDER BY embedding <=> $query LIMIT 3 USE VECTOR EXACT",
        "SELECT id FROM vector_items ORDER BY embedding <=> $query USE RANK relevance USE VECTOR EXACT LIMIT 3",
        "SELECT id FROM vector_items ORDER BY embedding <=> $query USE VECTOR EXACT USE VECTOR AUTO LIMIT 3",
    ] {
        assert!(
            parse(sql).is_err(),
            "invalid vector clause order accepted: {sql}"
        );
    }
}

#[test]
fn shared_vector_query_detection_follows_nested_queries_and_ignores_text() {
    use contextdb_parser::classification::select_contains_vector_similarity;

    for (sql, expected) in [
        (
            "SELECT id FROM vector_items ORDER BY embedding <=> $query LIMIT 3",
            true,
        ),
        (
            "WITH nearest AS (SELECT id FROM vector_items ORDER BY embedding <=> $query LIMIT 3) SELECT id FROM nearest",
            true,
        ),
        (
            "SELECT id FROM vector_items WHERE id IN (SELECT id FROM vector_items ORDER BY embedding <=> $query LIMIT 3)",
            true,
        ),
        (
            "SELECT id FROM vector_items WHERE id IN (SELECT id FROM vector_items ORDER BY id LIMIT 3)",
            false,
        ),
        ("SELECT '<=>' FROM vector_items ORDER BY id", false),
        (
            "SELECT id FROM vector_items /* ORDER BY embedding <=> $query */ ORDER BY id",
            false,
        ),
    ] {
        let Statement::Select(select) = parse_sql(sql) else {
            panic!("expected SELECT")
        };
        assert_eq!(
            select_contains_vector_similarity(&select),
            expected,
            "{sql}"
        );
    }
}
