//! A read-door consumer can learn a table's scope-label form from the schema
//! answer, so it can check its own declaration against the store.
//!
//! A caller that opens a read session declaring `scope_labels` is asserting
//! something about the store it has not read: that the tables it is about to
//! query actually carry a scope-label column, and that the labels it declared
//! are labels those tables know. Today it cannot check that. The persisted
//! column carries the constraint --- `ColumnDef.scope_label`, a
//! `ScopeLabelKind` that is either the Simple form (one label set governing
//! writes) or the Split form (a read set and a write set) --- but the schema
//! projection the read door hands back drops it. So the consumer either
//! declares blind and finds out by seeing zero rows, or it re-parses DDL text.
//!
//! The read schema answer therefore carries the scope-label constraint per
//! column, typed and whole: `DirectSchemaColumn::scope_label`, an
//! `Option<DirectScopeLabelKind>` mirroring the persisted `ScopeLabelKind` ---
//! `Simple { write_labels }` and `Split { read_labels, write_labels }` --- with
//! the label sets exactly as declared. It is present on both read routes,
//! because a consumer must not have to know whether a writer happens to be
//! holding the store to get the same answer about it.
//!
//! Two things it is NOT. It is not a change to the rendered `.schema` DDL
//! string: that render is a doc-frozen surface whose round-trip losses are
//! already ledgered (DL-CDB-38), and the control below pins it byte-identical
//! to the render of the same table without the declaration. And it is not a
//! change to any other schema fact: declaring a scope label alters nothing
//! else the schema answer says about that column, which the same control pins
//! field by field.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::read_contract::{OwnerReadLimits, ReadLimits, ReadRoute};
use contextdb_engine::{
    Database, DatabaseOpenOptions, DirectSchema, DirectSchemaColumn, DirectScopeLabelKind,
    MetadataBody, MetadataRequest, OwnerReadConfig, ReadSession,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// The table whose scope column is declared in the Simple form: one label set,
/// governing writes.
const SIMPLE_TABLE: &str = "simple_scoped_rows";
/// The table whose scope column is declared in the Split form: a read set and
/// a separate write set, so a projection that collapses the two is visibly
/// wrong.
const SPLIT_TABLE: &str = "split_scoped_rows";
/// A table with no scope-label declaration at all.
const UNSCOPED_TABLE: &str = "unscoped_rows";
/// The same column shape as `SIMPLE_TABLE`, declared WITHOUT the scope-label
/// constraint. It is the control: every schema fact except the new one must
/// read identically on both tables, and the DDL render must be identical too.
const RENDER_CONTROL_TABLE: &str = "render_control_rows";

/// The scope-carrying column, named the same in every fixture table so the
/// control comparison is column-for-column.
const SCOPE_COLUMN: &str = "scope";

/// The two declared labels. The Split table reads under the first and writes
/// under the second, so a projection that copied one set into both fields, or
/// swapped them, fails rather than passes.
const READ_LABEL: &str = "a";
const WRITE_LABEL: &str = "b";

fn secure_runtime_root(directory: &Path, name: &str) -> PathBuf {
    let root = directory.join(name);
    std::fs::create_dir(&root).expect("create the task-scoped owner runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped owner runtime root");
    root
}

fn seed(database: &Database) {
    for statement in [
        format!(
            "CREATE TABLE {SIMPLE_TABLE} (id INTEGER PRIMARY KEY, \
             {SCOPE_COLUMN} TEXT SCOPE_LABEL ('{READ_LABEL}', '{WRITE_LABEL}'), payload TEXT)"
        ),
        format!(
            "CREATE TABLE {SPLIT_TABLE} (id INTEGER PRIMARY KEY, \
             {SCOPE_COLUMN} TEXT SCOPE_LABEL_READ ('{READ_LABEL}') WRITE ('{WRITE_LABEL}'), \
             payload TEXT)"
        ),
        format!("CREATE TABLE {UNSCOPED_TABLE} (id INTEGER PRIMARY KEY, payload TEXT)"),
        format!(
            "CREATE TABLE {RENDER_CONTROL_TABLE} (id INTEGER PRIMARY KEY, \
             {SCOPE_COLUMN} TEXT, payload TEXT)"
        ),
    ] {
        database
            .execute(&statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("seed {statement}: {error}"));
    }
}

/// A store with no writer holding it, so the reading route is the committed
/// file.
fn idle_store(directory: &Path) -> PathBuf {
    let path = directory.join("scope-schema.db");
    let database = Database::open(&path).expect("open the scope-schema fixture writer");
    seed(&database);
    database.close().expect("the fixture writer closes cleanly");
    path
}

/// A live writer serving owner reads, so the reading route is that owner over
/// a channel.
fn served_store(directory: &Path, runtime_root: PathBuf) -> (Database, PathBuf) {
    let path = directory.join("scope-schema-served.db");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root),
                limits: OwnerReadLimits {
                    limits: ReadLimits::default(),
                    concurrency: 4,
                },
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open a writer that serves owner reads");
    seed(&database);
    (database, path)
}

/// Ask the read door what a table's schema is, through the public metadata
/// door both routes answer.
fn schema_of(session: &ReadSession, table: &str) -> DirectSchema {
    let answer = session
        .metadata(
            MetadataRequest::Schema {
                table: table.to_owned(),
            },
            None,
        )
        .unwrap_or_else(|error| panic!("the read door answers the schema of {table}: {error}"));
    match answer.body {
        MetadataBody::Schema { schema } => schema,
        other => panic!("asked for the schema of {table} and got {other:?}"),
    }
}

fn column_named<'a>(schema: &'a DirectSchema, name: &str) -> &'a DirectSchemaColumn {
    schema
        .columns
        .iter()
        .find(|column| column.name == name)
        .unwrap_or_else(|| {
            panic!(
                "table {} names a column {name}: {:?}",
                schema.table, schema.columns
            )
        })
}

fn simple_form() -> DirectScopeLabelKind {
    DirectScopeLabelKind::Simple {
        write_labels: vec![READ_LABEL.to_owned(), WRITE_LABEL.to_owned()],
    }
}

fn split_form() -> DirectScopeLabelKind {
    DirectScopeLabelKind::Split {
        read_labels: vec![READ_LABEL.to_owned()],
        write_labels: vec![WRITE_LABEL.to_owned()],
    }
}

/// Proof 1 --- the schema answer carries each table's scope form and its label
/// sets exactly, telling the Simple declaration apart from the Split one.
#[test]
fn the_schema_answer_carries_each_table_s_declared_scope_label_form() {
    let directory = tempfile::TempDir::new().expect("task-scoped scope-schema directory");
    let path = idle_store(directory.path());
    let session = ReadSession::open(&path).expect("open the idle scope-schema store");
    assert_eq!(session.route(), ReadRoute::File);

    let simple = schema_of(&session, SIMPLE_TABLE);
    assert_eq!(
        column_named(&simple, SCOPE_COLUMN).scope_label,
        Some(simple_form()),
        "a column declared SCOPE_LABEL ('{READ_LABEL}', '{WRITE_LABEL}') answers the Simple \
         form with both labels, in the order they were declared"
    );

    let split = schema_of(&session, SPLIT_TABLE);
    assert_eq!(
        column_named(&split, SCOPE_COLUMN).scope_label,
        Some(split_form()),
        "a column declared SCOPE_LABEL_READ ('{READ_LABEL}') WRITE ('{WRITE_LABEL}') answers \
         the Split form with the read set and the write set kept apart"
    );
}

/// Proof 2 --- the same schema answer on the direct-file route and the
/// live-owner route. Parity alone would be satisfied by both routes dropping
/// the scope form, so the owner route is also pinned to the declared value
/// before the two answers are compared.
#[test]
fn both_read_routes_answer_the_same_scope_label_form() {
    let directory = tempfile::TempDir::new().expect("task-scoped scope-schema directory");
    let runtime_root = secure_runtime_root(directory.path(), "scope-schema-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    let asked_the_owner = {
        let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
            ReadSession::open(&path)
        })
        .expect("a live owner is reachable");
        assert_eq!(session.route(), ReadRoute::Owner);

        let simple = schema_of(&session, SIMPLE_TABLE);
        let split = schema_of(&session, SPLIT_TABLE);
        let unscoped = schema_of(&session, UNSCOPED_TABLE);

        assert_eq!(
            column_named(&simple, SCOPE_COLUMN).scope_label,
            Some(simple_form()),
            "the Simple form survives the owner channel rather than being dropped in transit"
        );
        assert_eq!(
            column_named(&split, SCOPE_COLUMN).scope_label,
            Some(split_form()),
            "the Split form survives the owner channel with its two label sets intact"
        );

        (simple, split, unscoped)
    };

    // The same store, now idle, asked the same questions.
    database.close().expect("the writer closes cleanly");
    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("the store is still readable once its owner is gone");
    assert_eq!(session.route(), ReadRoute::File);

    let asked_the_file = (
        schema_of(&session, SIMPLE_TABLE),
        schema_of(&session, SPLIT_TABLE),
        schema_of(&session, UNSCOPED_TABLE),
    );

    assert_eq!(
        asked_the_owner, asked_the_file,
        "a consumer verifying its scope declaration gets the same schema whether or not a \
         writer happens to be holding the store"
    );
}

/// Proof 3 --- a table with no scope-label declaration says so: no column
/// answers a scope constraint, and neither do the ordinary columns of the
/// tables that do declare one.
#[test]
fn a_column_with_no_scope_label_declaration_answers_no_scope_constraint() {
    let directory = tempfile::TempDir::new().expect("task-scoped scope-schema directory");
    let path = idle_store(directory.path());
    let session = ReadSession::open(&path).expect("open the idle scope-schema store");

    let unscoped = schema_of(&session, UNSCOPED_TABLE);
    for column in &unscoped.columns {
        assert_eq!(
            column.scope_label, None,
            "no column of {UNSCOPED_TABLE} was declared with a scope label, so none answers \
             one: {column:?}"
        );
    }

    for table in [SIMPLE_TABLE, SPLIT_TABLE] {
        let schema = schema_of(&session, table);
        for column in schema.columns.iter().filter(|c| c.name != SCOPE_COLUMN) {
            assert_eq!(
                column.scope_label, None,
                "the scope-label answer belongs to the column that declared it, not to every \
                 column of {table}: {column:?}"
            );
        }
    }
}

/// Proof 4 --- the control. Declaring a scope label changes nothing else the
/// schema answer says: every other field of the column reads exactly as it
/// does on the same column without the declaration, and the rendered DDL --- a
/// doc-frozen surface whose scope-label round-trip loss is ledgered as
/// DL-CDB-38 --- is byte-identical.
#[test]
fn declaring_a_scope_label_changes_no_other_schema_fact() {
    let directory = tempfile::TempDir::new().expect("task-scoped scope-schema directory");
    let path = idle_store(directory.path());
    let session = ReadSession::open(&path).expect("open the idle scope-schema store");

    let scoped = schema_of(&session, SIMPLE_TABLE);
    let control = schema_of(&session, RENDER_CONTROL_TABLE);

    let scoped_column = column_named(&scoped, SCOPE_COLUMN);
    let control_column = column_named(&control, SCOPE_COLUMN);

    // The facts the schema answer already carried, still carried and still
    // saying what they said before there was a scope-label field at all.
    assert_eq!(scoped_column.name, SCOPE_COLUMN);
    assert_eq!(scoped_column.data_type, "TEXT");
    assert!(scoped_column.nullable, "the scope column stays nullable");
    assert!(!scoped_column.primary_key);
    assert!(!scoped_column.unique);
    assert!(!scoped_column.immutable);
    assert!(!scoped_column.expires);
    assert_eq!(scoped_column.default, None);
    assert_eq!(scoped_column.references, None);
    assert_eq!(scoped_column.quantization, None);
    assert_eq!(scoped_column.rank, None);

    // Said another way, with no hand-written expectation to drift: the scoped
    // column is the unscoped column plus the new field, and nothing else.
    assert_eq!(
        scoped_column,
        &DirectSchemaColumn {
            scope_label: Some(simple_form()),
            ..control_column.clone()
        },
        "the scope-label declaration is the ONLY difference between the two columns"
    );

    // The rest of the table's schema answer is the control's too.
    assert_eq!(scoped.immutable, control.immutable);
    assert_eq!(scoped.primary_key, control.primary_key);
    assert_eq!(scoped.indexes, control.indexes);
    assert_eq!(scoped.state_machine, control.state_machine);
    assert_eq!(scoped.retain, control.retain);
    assert_eq!(scoped.history, control.history);
    assert_eq!(scoped.sync_direction, control.sync_direction);
    assert_eq!(scoped.conflict_policy, control.conflict_policy);
    assert_eq!(scoped.dag_edge_types, control.dag_edge_types);
    assert_eq!(scoped.propagate, control.propagate);

    // The doc-frozen render: identical to the unscoped table's render once the
    // table name is accounted for. It does not gain a scope-label clause here
    // (DL-CDB-38 owns that gap), and it does not lose anything either.
    assert_eq!(
        scoped.ddl,
        control.ddl.replace(RENDER_CONTROL_TABLE, SIMPLE_TABLE),
        "the `.schema` render is a doc-frozen surface and stays exactly what it was"
    );
    assert!(
        !scoped.ddl.contains("SCOPE_LABEL"),
        "the render still omits the scope-label clause: {}",
        scoped.ddl
    );
}
