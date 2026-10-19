//! Schema migration units may contain only schema declarations.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn assert_mixed_refusal(error: contextdb_core::Error, phase: &str) {
    assert!(
        error
            .to_string()
            .contains("mixed DML and transactional local schema DDL"),
        "{phase}: schema DDL and a row write must keep the established refusal: {error}"
    );
}

#[test]
fn schema_ddl_and_rows_in_one_transaction_are_still_refused() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE existing_rows (id INTEGER PRIMARY KEY, body TEXT)",
        &empty(),
    )
    .expect("create the pre-existing row table outside the migration");

    let ddl_first = db.begin().expect("begin DDL-first transaction");
    db.execute_in_tx(
        ddl_first,
        "CREATE TABLE ddl_first_schema (id INTEGER PRIMARY KEY)",
        &empty(),
    )
    .expect("stage schema before the prohibited row write");
    assert_mixed_refusal(
        db.execute_in_tx(
            ddl_first,
            "INSERT INTO existing_rows VALUES (1, 'must-not-commit')",
            &empty(),
        )
        .expect_err("DDL followed by DML must be refused"),
        "DDL then DML",
    );
    db.rollback(ddl_first)
        .expect("roll back the refused DDL-first transaction");
    assert!(
        db.table_meta("ddl_first_schema").is_none(),
        "rollback after the DDL-first refusal must not publish the staged table"
    );
    assert!(
        db.execute("SELECT * FROM existing_rows", &empty())
            .expect("inspect rows after DDL-first rollback")
            .rows
            .is_empty(),
        "rollback after the DDL-first refusal must not publish the rejected row"
    );

    let dml_first = db.begin().expect("begin DML-first transaction");
    db.execute_in_tx(
        dml_first,
        "INSERT INTO existing_rows VALUES (2, 'must-not-commit')",
        &empty(),
    )
    .expect("stage row before the prohibited schema declaration");
    assert_mixed_refusal(
        db.execute_in_tx(
            dml_first,
            "CREATE TABLE dml_first_schema (id INTEGER PRIMARY KEY)",
            &empty(),
        )
        .expect_err("DML followed by DDL must be refused"),
        "DML then DDL",
    );
    db.rollback(dml_first)
        .expect("roll back the refused DML-first transaction");
    assert!(
        db.table_meta("dml_first_schema").is_none(),
        "rollback after the DML-first refusal must not publish the rejected table"
    );
    assert!(
        db.execute("SELECT * FROM existing_rows", &empty())
            .expect("inspect rows after DML-first rollback")
            .rows
            .is_empty(),
        "rollback after the DML-first refusal must not publish the staged row"
    );
}

#[path = "../fixtures/schema_wall/mod.rs"]
mod support;

fn every_row_write_is_refused(ddl: &str) {
    for dml in [
        "INSERT INTO mixed_rows VALUES ('00000000-0000-0000-0000-000000000002', 'insert')",
        "UPDATE mixed_rows SET body = 'update' WHERE id = '00000000-0000-0000-0000-000000000001'",
        "DELETE FROM mixed_rows WHERE id = '00000000-0000-0000-0000-000000000001'",
        "INSERT INTO mixed_rows VALUES ('00000000-0000-0000-0000-000000000001', 'upsert') ON CONFLICT (id) DO UPDATE SET body = 'upsert'",
    ] {
        for ddl_first in [true, false] {
            let db = Database::open_memory();
            support::declare(
                &db,
                &[
                    "CREATE TABLE mixed_rows (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
                    "CREATE TABLE mixed_weights (id UUID PRIMARY KEY, passage_id UUID NOT NULL, weight REAL NOT NULL)",
                    "CREATE INDEX mixed_weights_passage_id_idx ON mixed_weights(passage_id)",
                    "CREATE EVENT TYPE mixed_existing_event WHEN INSERT ON mixed_rows",
                    "CREATE SINK mixed_existing_sink TYPE callback",
                ],
            );
            db.execute("INSERT INTO mixed_rows VALUES ('00000000-0000-0000-0000-000000000001', 'original')", &empty()).unwrap();
            let before_schema = support::schema(&db);
            let before_rows = db
                .execute("SELECT * FROM mixed_rows ORDER BY id", &empty())
                .unwrap()
                .rows;
            let before_changes = db
                .changes_since(contextdb_core::Lsn(0))
                .expect("read the declared changes");
            let tx = db.begin().unwrap();
            let (first, second) = if ddl_first { (ddl, dml) } else { (dml, ddl) };
            db.execute_in_tx(tx, first, &empty()).unwrap_or_else(|error| panic!("newly admitted DDL prerequisite must execute, rather than manufacture a mixed-write rejection: {first}: {error}"));
            let error = db.execute_in_tx(tx, second, &empty()).expect_err(
                "every newly admitted DDL form keeps the mixed-write refusal in both orders",
            );
            assert_mixed_refusal(
                error,
                &format!("DDL={ddl}, DML={dml}, DDL-first={ddl_first}"),
            );
            db.rollback(tx).unwrap();
            assert_eq!(
                support::schema(&db),
                before_schema,
                "refusal/rollback leaves every catalog unchanged"
            );
            assert_eq!(
                db.execute("SELECT * FROM mixed_rows ORDER BY id", &empty())
                    .unwrap()
                    .rows,
                before_rows
            );
            assert_eq!(
                db.changes_since(contextdb_core::Lsn(0))
                    .expect("read the declared changes")
                    .ddl,
                before_changes.ddl
            );
            assert_eq!(
                db.changes_since(contextdb_core::Lsn(0))
                    .expect("read the declared changes")
                    .max_lsn(),
                before_changes.max_lsn()
            );
        }
    }
}

// Independent registered cases: a current CREATE INDEX prerequisite refusal
// cannot hide event/trigger/ALTER cases from the mandatory runner's inventory.
macro_rules! mixed_form {
    ($name:ident, $ddl:literal) => {
        #[test]
        fn $name() {
            every_row_write_is_refused($ddl);
        }
    };
}

mixed_form!(
    mixed_rows_refuse_create_index,
    "CREATE INDEX mixed_body_index ON mixed_rows(body)"
);
mixed_form!(
    mixed_rows_refuse_create_vector,
    "CREATE TABLE mixed_vector (id UUID PRIMARY KEY, embedding VECTOR(2))"
);
mixed_form!(
    mixed_rows_refuse_create_rank,
    "CREATE TABLE mixed_rank (id UUID PRIMARY KEY, embedding VECTOR(2) RANK_POLICY (JOIN mixed_weights ON passage_id, FORMULA '{vector_score} * coalesce({weight}, 1.0)', SORT_KEY mixed_score))"
);
mixed_form!(
    mixed_rows_refuse_create_partitioned_vector,
    "CREATE TABLE mixed_partitioned (id UUID PRIMARY KEY, context_id UUID NOT NULL, embedding VECTOR(2) PARTITION_KEY (context_id))"
);
mixed_form!(
    mixed_rows_refuse_create_dag,
    "CREATE TABLE mixed_edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL) DAG('LINK')"
);
mixed_form!(
    mixed_rows_refuse_alter_vector,
    "ALTER TABLE mixed_rows ADD COLUMN embedding VECTOR(2)"
);
mixed_form!(
    mixed_rows_refuse_alter_rank,
    "ALTER TABLE mixed_rows ADD COLUMN embedding VECTOR(2) RANK_POLICY (JOIN mixed_weights ON passage_id, FORMULA '{vector_score} * coalesce({weight}, 1.0)', SORT_KEY mixed_altered_score)"
);
mixed_form!(
    mixed_rows_refuse_alter_partitioned_vector,
    "ALTER TABLE mixed_rows ADD COLUMN embedding VECTOR(2) PARTITION_KEY (id)"
);
mixed_form!(
    mixed_rows_refuse_create_event_type,
    "CREATE EVENT TYPE mixed_new_event WHEN INSERT ON mixed_rows"
);
mixed_form!(
    mixed_rows_refuse_create_trigger,
    "CREATE TRIGGER mixed_new_trigger ON mixed_rows WHEN INSERT INCLUDING SYNC"
);
mixed_form!(
    mixed_rows_refuse_create_sink,
    "CREATE SINK mixed_new_sink TYPE callback"
);
mixed_form!(
    mixed_rows_refuse_create_route,
    "CREATE ROUTE mixed_new_route EVENT mixed_existing_event TO mixed_existing_sink"
);
