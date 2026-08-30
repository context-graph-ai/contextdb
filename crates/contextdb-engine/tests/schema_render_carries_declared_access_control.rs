//! `.schema`'s printed DDL must carry a column's declared access control.
//!
//! `docs/cli.md` promises that `.schema` "reflects the table's full *enforced*
//! policy" and that "its printed DDL re-parses to a table with the same policy,
//! so `.schema` output remains a valid way to snapshot or replay a definition."
//! It names exactly two things the printed DDL does not reproduce literally:
//! column `DEFAULT` clauses and `STATE MACHINE` from-state ordering.
//!
//! A column declared `ACL REFERENCES acl_grants(acl_id)` is a silent third.
//! `sql_type_for_meta_column` renders type, quantization, foreign-key rules and
//! `col.references`, but never `col.acl_ref` — which is persisted on the column
//! metadata (`contextdb_core::table_meta::ColumnDef::acl_ref`). So the policy
//! survives in the store, but an operator who follows the documented
//! snapshot/replay path rebuilds the table with row-level authorization
//! removed, and nothing warns them. This is a render-and-replay defect, not a
//! lost constraint.
//!
//! The discriminating control below: an ordinary foreign key on the same shape
//! already renders and must keep rendering exactly as it does today, so the gap
//! is specific to the ACL declaration rather than a general REFERENCES gap.

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::cli_render::render_table_meta;
use std::collections::HashMap;

const GRANT_TABLE: &str = "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, \
                           principal_id TEXT, acl_id UUID)";
const CONTROLLED_TABLE: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, acl_id UUID ACL \
                                REFERENCES acl_grants(acl_id), payload TEXT)";

fn declared_acl(
    db: &Database,
    table: &str,
    column: &str,
) -> Option<contextdb_core::table_meta::AclRef> {
    db.table_meta(table)
        .unwrap_or_else(|| panic!("table_meta({table:?}) must be Some after CREATE TABLE"))
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .and_then(|candidate| candidate.acl_ref.clone())
}

#[test]
fn schema_render_carries_the_declared_acl_reference() {
    let db = Database::open_memory();
    let empty: HashMap<String, Value> = HashMap::new();
    db.execute(GRANT_TABLE, &empty)
        .expect("the grant table an ACL column references must be declarable");
    db.execute(CONTROLLED_TABLE, &empty)
        .expect("an access-controlled table must be declarable");

    let declared = declared_acl(&db, "notes", "acl_id")
        .expect("sanity: the declared ACL reference must be persisted on the column metadata");
    assert_eq!(declared.ref_table, "acl_grants");
    assert_eq!(declared.ref_column, "acl_id");

    let meta = db.table_meta("notes").expect("source metadata must exist");
    let rendered = render_table_meta("notes", &meta);
    assert!(
        rendered.contains("ACL REFERENCES acl_grants(acl_id)"),
        "the rendered .schema DDL must carry the column's ACL REFERENCES clause -- without it the \
         printed DDL replays as a table with row-level authorization removed, and docs/cli.md \
         names only DEFAULT and STATE MACHINE ordering as exceptions to the replay promise. \
         Rendered:\n{rendered}"
    );
}

#[test]
fn schema_render_round_trips_the_acl_reference_into_a_fresh_store() {
    let empty: HashMap<String, Value> = HashMap::new();

    let db_a = Database::open_memory();
    db_a.execute(GRANT_TABLE, &empty)
        .expect("the grant table an ACL column references must be declarable");
    db_a.execute(CONTROLLED_TABLE, &empty)
        .expect("an access-controlled table must be declarable");
    let acl_a = declared_acl(&db_a, "notes", "acl_id");
    assert!(
        acl_a.is_some(),
        "sanity: the source store must hold the declared ACL reference"
    );

    let meta_a = db_a
        .table_meta("notes")
        .expect("source metadata must exist");
    let rendered = render_table_meta("notes", &meta_a);

    let db_b = Database::open_memory();
    db_b.execute(GRANT_TABLE, &empty)
        .expect("the grant table must exist on the replay store too");
    db_b.execute(&rendered, &empty).unwrap_or_else(|error| {
        panic!("the rendered DDL must parse on a fresh store: {error:?}\nrendered:\n{rendered}")
    });

    let acl_b = declared_acl(&db_b, "notes", "acl_id");
    assert_eq!(
        acl_b, acl_a,
        "replaying the printed DDL must rebuild the same access-control policy; a table replayed \
         without its ACL reference is readable by principals the original refused. \
         Rendered:\n{rendered}"
    );
}

#[test]
fn schema_render_of_a_plain_foreign_key_is_unchanged() {
    let db = Database::open_memory();
    let empty: HashMap<String, Value> = HashMap::new();
    db.execute("CREATE TABLE parent (id UUID PRIMARY KEY)", &empty)
        .expect("the referenced table must be declarable");
    db.execute(
        "CREATE TABLE child (id UUID PRIMARY KEY, p UUID REFERENCES parent(id))",
        &empty,
    )
    .expect("an ordinary foreign key must be declarable");

    let meta = db.table_meta("child").expect("source metadata must exist");
    let rendered = render_table_meta("child", &meta);
    assert!(
        rendered.contains("p UUID REFERENCES parent(id)"),
        "control: an ordinary foreign key already renders and must keep rendering exactly as it \
         does today. Rendered:\n{rendered}"
    );
    assert!(
        !rendered.contains("ACL REFERENCES"),
        "control: a column with no ACL declaration must not gain an ACL clause. \
         Rendered:\n{rendered}"
    );
}
