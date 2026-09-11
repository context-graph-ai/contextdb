use super::*;

/// Every machine that renders the same declared state machine must produce
/// the same constraint text: the spelling participates in schema identity
/// comparisons across sync, so a per-process ordering would make two
/// machines disagree about DDL they both authored from identical SQL.
/// `HashMap` gives every instance its own iteration order, so building the
/// same transitions in different insertion orders across many instances
/// exposes any order-dependent rendering with overwhelming probability.
#[test]
fn state_machine_constraint_rendering_is_deterministic() {
    let states = [
        "pending",
        "acknowledged",
        "resolved",
        "dismissed",
        "archived",
        "escalated",
    ];
    let mut renderings = std::collections::HashSet::new();
    for rotation in 0..states.len() {
        let mut transitions = HashMap::new();
        for offset in 0..states.len() {
            let from = states[(rotation + offset) % states.len()];
            transitions.insert(from.to_string(), vec!["resolved".to_string()]);
        }
        let meta = TableMeta {
            state_machine: Some(contextdb_core::StateMachineConstraint {
                column: "status".to_string(),
                transitions,
            }),
            ..TableMeta::default()
        };
        renderings.insert(create_table_constraints_from_meta(&meta).join("; "));
    }
    assert_eq!(
        renderings.len(),
        1,
        "identical state machines must render one constraint spelling, got {renderings:?}"
    );
    let only = renderings.into_iter().next().unwrap();
    let sorted_expectation = "STATE MACHINE (status: acknowledged -> [resolved], \
         archived -> [resolved], dismissed -> [resolved], escalated -> [resolved], \
         pending -> [resolved], resolved -> [resolved])";
    assert_eq!(
        only, sorted_expectation,
        "the one spelling is the transitions sorted by source state"
    );
}

#[test]
fn projected_vector_defaults_render_like_the_durable_declaration() {
    for (declaration, expected) in [
        (
            "VECTOR(3) AUTO_INDEX_AT DEFAULT HNSW (M = DEFAULT, EF_CONSTRUCTION = DEFAULT, EF_SEARCH = DEFAULT)",
            "VECTOR(3)",
        ),
        (
            "VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope) MAX_PARTITIONS 0008 SEARCH_MODE AUTO AUTO_INDEX_AT 0005001 HNSW (M = DEFAULT, EF_CONSTRUCTION = 00064, EF_SEARCH = DEFAULT)",
            "VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope) MAX_PARTITIONS 8 AUTO_INDEX_AT 5001 HNSW (EF_CONSTRUCTION = 64)",
        ),
        (
            "VECTOR(3) AUTO_INDEX_AT 0001000 HNSW (M = 00016, EF_CONSTRUCTION = 00200, EF_SEARCH = 00200)",
            "VECTOR(3) AUTO_INDEX_AT 1000 HNSW (M = 16, EF_CONSTRUCTION = 200, EF_SEARCH = 200)",
        ),
    ] {
        let db = Database::open_memory();
        let sql = format!(
            "CREATE TABLE projected_vectors (id INTEGER PRIMARY KEY, scope TEXT NOT NULL, embedding {declaration})"
        );
        let statement = contextdb_parser::parse(&sql).expect("parse vector declaration");
        let projected = db
            .ddl_change_for_statement(&statement, None)
            .expect("valid vector declaration has a DDL projection");
        db.execute(&sql, &HashMap::new())
            .expect("install the same declaration");
        let meta = db
            .table_meta("projected_vectors")
            .expect("installed metadata");
        assert_eq!(
            projected,
            ddl_change_from_meta("projected_vectors", &meta),
            "projection and durable DDL have one normalized identity: {declaration}"
        );
        let column = meta
            .columns
            .iter()
            .find(|column| column.name == "embedding")
            .unwrap();
        assert_eq!(sql_type_for_meta_column(column, &[]), expected);
    }
}
