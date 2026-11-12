//! A count through a handle that may not see every row: it is the number of rows the handle
//! sees, never the number there are, and the handle reads the rows to know. A count with no
//! row read is for a reader that sees them all.

mod support;

use std::collections::BTreeSet;

use contextdb::{ContextId, Uuid, Value, params};

#[test]
fn a_handle_that_may_not_see_every_row_counts_the_rows_it_sees_by_reading_them() {
    let (mine, theirs) = (Uuid::from_u128(1), Uuid::from_u128(2));
    // Once with a kept count over the filter's column and once with only an index: sixty
    // rows of kind `k`, twenty of them in the Context the narrowed handle is given.
    for (name, clause) in [
        ("narrowed-kept-count", "MAINTAIN COUNT (kind)"),
        ("narrowed-index-count", ""),
    ] {
        let db = support::open(&support::folder(name));
        support::run(
            &db,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, \
             kind TEXT NOT NULL)",
        );
        support::run(&db, "CREATE INDEX notes_kind ON notes (kind)");
        if !clause.is_empty() {
            support::run(&db, &format!("ALTER TABLE notes SET {clause}"));
        }
        let tx = db.transaction().expect("begin");
        for n in 1..=90_i64 {
            tx.execute(
                "INSERT INTO notes (id, context_id, kind) VALUES ($id, $context, $kind)",
                &params! {
                    "id" => n, "context" => if n % 3 == 0 { mine } else { theirs },
                    "kind" => if n <= 60 { "k" } else { "other" },
                },
            )
            .expect("insert a note");
        }
        tx.commit().expect("commit the notes");
        let count = "SELECT COUNT(*) FROM notes WHERE kind = 'k'";

        // The writer sees every row and reads none to count them.
        let all = support::run(&db, count);
        assert_eq!(all.rows(), [[Value::Int64(60)]], "{name}");
        assert_eq!(all.rows_examined(), 0, "{name}");

        // The narrowed handle is told the number of rows it may see, never the number there
        // are, and reads the rows to know.
        let narrowed = db.scoped_with_contexts(BTreeSet::from([ContextId::new(mine)]));
        let seen = support::rows(&narrowed, "SELECT id FROM notes WHERE kind = 'k'");
        assert_eq!(seen.len(), 20, "{name}");
        let some = support::run(&narrowed, count);
        assert_eq!(some.rows(), [[Value::Int64(20)]], "{name}");
        assert!(
            some.rows_examined() >= 20,
            "{name}: a narrowed count of 20 rows read {} rows",
            some.rows_examined()
        );
        assert_eq!(
            support::int(&narrowed, "SELECT COUNT(*) FROM notes"),
            30,
            "{name}: the whole table through the narrowed handle"
        );
    }
}
