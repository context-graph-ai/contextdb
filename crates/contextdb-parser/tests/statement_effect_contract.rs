use contextdb_parser::{Statement, StatementEffect, parse, statement_effect};
use std::collections::HashSet;

const STATEMENT_VARIANT_COUNT: usize = 27;

fn statement_variant(statement: &Statement) -> &'static str {
    match statement {
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::AlterTable(_) => "ALTER TABLE",
        Statement::DropTable(_) => "DROP TABLE",
        Statement::CreateIndex(_) => "CREATE INDEX",
        Statement::DropIndex(_) => "DROP INDEX",
        Statement::Insert(_) => "INSERT",
        Statement::Purge(_) => "PURGE",
        Statement::Delete(_) => "DELETE",
        Statement::Update(_) => "UPDATE",
        Statement::Select(_) => "SELECT",
        Statement::Begin => "BEGIN",
        Statement::Commit => "COMMIT",
        Statement::Rollback => "ROLLBACK",
        Statement::SetMemoryLimit(_) => "SET MEMORY_LIMIT",
        Statement::ShowMemoryLimit => "SHOW MEMORY_LIMIT",
        Statement::SetDiskLimit(_) => "SET DISK_LIMIT",
        Statement::ShowDiskLimit => "SHOW DISK_LIMIT",
        Statement::ShowSyncConflictPolicy => "SHOW SYNC_CONFLICT_POLICY",
        Statement::ShowVectorIndexes => "SHOW VECTOR_INDEXES",
        Statement::CreateSchedule { .. } => "CREATE SCHEDULE",
        Statement::DropSchedule { .. } => "DROP SCHEDULE",
        Statement::CreateTrigger { .. } => "CREATE TRIGGER",
        Statement::DropTrigger { .. } => "DROP TRIGGER",
        Statement::CreateEventType { .. } => "CREATE EVENT TYPE",
        Statement::CreateSink { .. } => "CREATE SINK",
        Statement::CreateRoute { .. } => "CREATE ROUTE",
        Statement::DropRoute { .. } => "DROP ROUTE",
    }
}

#[test]
fn statement_effect_marks_only_the_five_inspection_variants_as_reads() {
    let cases = [
        (
            "CREATE TABLE",
            "CREATE TABLE entries (id UUID PRIMARY KEY)",
            StatementEffect::Write,
        ),
        (
            "ALTER TABLE",
            "ALTER TABLE entries ADD COLUMN note TEXT",
            StatementEffect::Write,
        ),
        ("DROP TABLE", "DROP TABLE entries", StatementEffect::Write),
        (
            "CREATE INDEX",
            "CREATE INDEX entries_by_id ON entries (id)",
            StatementEffect::Write,
        ),
        (
            "DROP INDEX",
            "DROP INDEX entries_by_id ON entries",
            StatementEffect::Write,
        ),
        (
            "INSERT",
            "INSERT INTO entries (id) VALUES ($id)",
            StatementEffect::Write,
        ),
        (
            "PURGE",
            "PURGE FROM entries WHERE id = $id",
            StatementEffect::Write,
        ),
        (
            "DELETE",
            "DELETE FROM entries WHERE id = $id",
            StatementEffect::Write,
        ),
        (
            "UPDATE",
            "UPDATE entries SET id = $replacement WHERE id = $id",
            StatementEffect::Write,
        ),
        ("SELECT", "SELECT 1", StatementEffect::Read),
        ("BEGIN", "BEGIN", StatementEffect::Write),
        ("COMMIT", "COMMIT", StatementEffect::Write),
        ("ROLLBACK", "ROLLBACK", StatementEffect::Write),
        (
            "SET MEMORY_LIMIT",
            "SET MEMORY_LIMIT '1M'",
            StatementEffect::Write,
        ),
        (
            "SHOW MEMORY_LIMIT",
            "SHOW MEMORY_LIMIT",
            StatementEffect::Read,
        ),
        (
            "SET DISK_LIMIT",
            "SET DISK_LIMIT '1M'",
            StatementEffect::Write,
        ),
        ("SHOW DISK_LIMIT", "SHOW DISK_LIMIT", StatementEffect::Read),
        (
            "SHOW SYNC_CONFLICT_POLICY",
            "SHOW SYNC_CONFLICT_POLICY",
            StatementEffect::Read,
        ),
        (
            "SHOW VECTOR_INDEXES",
            "SHOW VECTOR_INDEXES",
            StatementEffect::Read,
        ),
        (
            "CREATE SCHEDULE",
            "CREATE SCHEDULE refresh EVERY '1h' TX (refresh_entries)",
            StatementEffect::Write,
        ),
        (
            "DROP SCHEDULE",
            "DROP SCHEDULE refresh",
            StatementEffect::Write,
        ),
        (
            "CREATE TRIGGER",
            "CREATE TRIGGER entry_change ON entries WHEN INSERT",
            StatementEffect::Write,
        ),
        (
            "DROP TRIGGER",
            "DROP TRIGGER entry_change",
            StatementEffect::Write,
        ),
        (
            "CREATE EVENT TYPE",
            "CREATE EVENT TYPE entry_created WHEN INSERT ON entries",
            StatementEffect::Write,
        ),
        (
            "CREATE SINK",
            "CREATE SINK entry_sink TYPE callback",
            StatementEffect::Write,
        ),
        (
            "CREATE ROUTE",
            "CREATE ROUTE entry_route EVENT entry_created TO entry_sink",
            StatementEffect::Write,
        ),
        (
            "DROP ROUTE",
            "DROP ROUTE entry_route",
            StatementEffect::Write,
        ),
    ];

    let parsed_cases = cases
        .iter()
        .map(|(variant, sql, expected_effect)| {
            let statement = parse(sql).unwrap_or_else(|error| {
                panic!("{variant} fixture must parse before classification: {error}")
            });
            (*variant, *expected_effect, statement)
        })
        .collect::<Vec<_>>();

    assert_eq!(
        parsed_cases.len(),
        cases.len(),
        "the parse pass must materialize every declared fixture"
    );
    assert_eq!(
        parsed_cases.len(),
        STATEMENT_VARIANT_COUNT,
        "the fixture table must contain every current Statement variant"
    );

    let mut parsed_variants = HashSet::new();
    for (declared_variant, _, statement) in &parsed_cases {
        let parsed_variant = statement_variant(statement);
        assert_eq!(
            parsed_variant, *declared_variant,
            "{declared_variant} fixture parsed as {parsed_variant}"
        );
        assert!(
            parsed_variants.insert(parsed_variant),
            "{parsed_variant} must have exactly one fixture"
        );
    }
    assert_eq!(
        parsed_variants.len(),
        STATEMENT_VARIANT_COUNT,
        "the parsed fixture set must exhaust every current Statement variant"
    );
    let declared_read_count = parsed_cases
        .iter()
        .filter(|(_, expected_effect, _)| *expected_effect == StatementEffect::Read)
        .count();
    assert_eq!(
        declared_read_count, 5,
        "exactly five current Statement variants are declared reads"
    );
    assert_eq!(
        parsed_cases.len() - declared_read_count,
        22,
        "every other current Statement variant is declared a write"
    );

    for (variant, expected_effect, statement) in &parsed_cases {
        assert_eq!(
            statement_effect(statement),
            *expected_effect,
            "{variant} must keep its declared store effect"
        );
    }
}
