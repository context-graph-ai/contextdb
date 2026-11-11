use std::collections::BTreeMap;

use super::{Catalog, Table, check, record, render};
use crate::sql::ast::{self, DataType, Expr, Literal};
use crate::sql::plan::{CreateIndexPlan, CreateTablePlan};
use crate::store::{IndexId, TableId};
use crate::types::{
    ColumnDef, ColumnDefault, ColumnType, Error, IndexDecl, SortDirection, TableMeta, Value,
};

fn column(name: &str, column_type: ColumnType) -> ColumnDef {
    ColumnDef {
        name: name.to_owned(),
        column_type,
        nullable: true,
        primary_key: false,
        unique: false,
        default: None,
    }
}

fn index(name: &str, columns: &[(&str, SortDirection)]) -> IndexDecl {
    IndexDecl {
        name: name.to_owned(),
        columns: columns
            .iter()
            .map(|(column, direction)| ((*column).to_owned(), *direction))
            .collect(),
    }
}

/// A table with every kind of clause this version keeps, as CREATE TABLE and two CREATE INDEX
/// statements leave it.
fn notes() -> Table {
    use SortDirection::{Asc, Desc};
    let literal = |value| Some(ColumnDefault::Literal(value));
    Table {
        id: TableId(4),
        name: "notes".to_owned(),
        meta: TableMeta {
            columns: vec![
                ColumnDef {
                    primary_key: true,
                    ..column("id", ColumnType::Integer)
                },
                ColumnDef {
                    nullable: false,
                    default: literal(Value::Text("it's".to_owned())),
                    ..column("title", ColumnType::Text)
                },
                ColumnDef {
                    default: literal(Value::Null),
                    ..column("body", ColumnType::Text)
                },
                ColumnDef {
                    default: literal(Value::Int64(3)),
                    ..column("stars", ColumnType::Integer)
                },
                ColumnDef {
                    unique: true,
                    ..column("slug", ColumnType::Text)
                },
                ColumnDef {
                    nullable: false,
                    default: Some(ColumnDefault::Now),
                    ..column("made at", ColumnType::Timestamp)
                },
                ColumnDef {
                    default: literal(Value::Float64(-2.0)),
                    ..column("score", ColumnType::Real)
                },
                ColumnDef {
                    default: literal(Value::Bool(true)),
                    ..column("kept", ColumnType::Boolean)
                },
                column("embedding", ColumnType::Vector(384)),
            ],
            unique_constraints: vec![vec!["title".to_owned(), "stars".to_owned()]],
            indexes: vec![
                index("__pk_id", &[("id", Asc)]),
                index("__unique_slug", &[("slug", Asc)]),
                index("__unique_title_stars", &[("title", Asc), ("stars", Asc)]),
                index("by_stars", &[("stars", Desc), ("made at", Asc)]),
                index("by_body", &[("body", Asc)]),
            ],
        },
        index_ids: [7, 8, 9, 12, 15].map(IndexId).to_vec(),
        vectors: Vec::new(),
    }
}

#[test]
fn a_record_reads_back_as_the_table_it_was_written_from() {
    let table = notes();
    let back = record::decode(&record::encode(&table));
    assert_eq!(back.id, table.id);
    assert_eq!(back.name, table.name);
    assert_eq!(back.meta, table.meta);
    assert_eq!(back.index_ids, table.index_ids);
}

#[test]
fn render_prints_what_the_author_declared_and_nothing_else() {
    assert_eq!(
        render(&notes()),
        "\
CREATE TABLE notes (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL DEFAULT 'it''s',
  body TEXT DEFAULT NULL,
  stars INTEGER DEFAULT 3,
  slug TEXT UNIQUE,
  \"made at\" TIMESTAMP NOT NULL DEFAULT NOW(),
  score REAL DEFAULT -2.0,
  kept BOOLEAN DEFAULT TRUE,
  embedding VECTOR(384),
  UNIQUE (title, stars)
);
CREATE INDEX by_stars ON notes (stars DESC, \"made at\");
CREATE INDEX by_body ON notes (body);
"
    );
}

/// The DEFAULT of a column as the parser hands it over, for this text after the word.
fn written_default(text: &str) -> Expr {
    let statement = format!("CREATE TABLE t (c INTEGER DEFAULT {text})");
    let parsed = match crate::sql::parse(&statement) {
        Ok(ast::Statement::CreateTable(create)) => create,
        other => panic!("{statement}\nwas read as {other:?}"),
    };
    let [column] = parsed.columns.as_slice() else {
        panic!(
            "{statement}\nwas read with {} columns",
            parsed.columns.len()
        );
    };
    column
        .default
        .clone()
        .unwrap_or_else(|| panic!("{statement}\nwas read with no default"))
}

#[test]
fn a_default_is_kept_in_the_type_the_author_wrote() {
    for (text, value) in [
        ("NULL", Value::Null),
        ("TRUE", Value::Bool(true)),
        ("3", Value::Int64(3)),
        ("0.0", Value::Float64(0.0)),
        ("'it''s'", "it's".into()),
        ("-1", Value::Int64(-1)),
        ("-2.5", Value::Float64(-2.5)),
    ] {
        assert_eq!(
            check::default(&written_default(text)),
            Some(ColumnDefault::Literal(value)),
            "DEFAULT {text}"
        );
    }
    assert_eq!(
        check::default(&written_default("NOW()")),
        Some(ColumnDefault::Now)
    );
    // Anything that is not a literal or NOW() is no default this version keeps.
    assert_eq!(check::default(&written_default("$p")), None);
    assert_eq!(check::default(&written_default("-'x'")), None);
}

/// A column as the parser hands over one that wrote no clause.
fn declared(name: &str, data_type: DataType) -> ast::ColumnDef {
    ast::ColumnDef {
        name: name.to_owned(),
        data_type,
        nullable: true,
        primary_key: false,
        unique: false,
        default: None,
        vector: ast::VectorClauses::default(),
    }
}

/// A CREATE TABLE as the planner hands over one that wrote no table option.
fn create(name: &str, columns: Vec<ast::ColumnDef>, unique: &[&[&str]]) -> CreateTablePlan {
    CreateTablePlan {
        name: name.to_owned(),
        columns,
        unique_constraints: unique
            .iter()
            .map(|rule| rule.iter().map(|column| (*column).to_owned()).collect())
            .collect(),
        if_not_exists: false,
    }
}

#[test]
fn a_declared_table_gets_its_numbers_and_the_index_behind_each_rule() {
    let key = |name: &str| ast::ColumnDef {
        primary_key: true,
        ..declared(name, DataType::Integer)
    };
    let people = create(
        "people",
        vec![
            ast::ColumnDef {
                nullable: false,
                unique: true,
                ..key("id")
            },
            ast::ColumnDef {
                unique: true,
                ..declared("email", DataType::Text)
            },
            ast::ColumnDef {
                default: Some(Expr::Literal(Literal::Real(0.0))),
                ..declared("score", DataType::Real)
            },
            declared("born", DataType::Integer),
            declared("doc", DataType::Json),
        ],
        &[&["email", "born"]],
    );
    let mut catalog = Catalog {
        tables: BTreeMap::new(),
    };
    let table = check::new_table(&people, &catalog).expect("a plain table");
    let names = |table: &Table| -> Vec<String> {
        let indexes = table.meta.indexes.iter();
        indexes.map(|index| index.name.clone()).collect()
    };
    assert_eq!(table.id, TableId(0));
    // The key column keeps the UNIQUE its author wrote and gets no second index for it.
    assert_eq!(
        names(&table),
        ["__pk_id", "__unique_email", "__unique_email_born"]
    );
    assert_eq!(table.index_ids, [0, 1, 2].map(IndexId));
    assert!(render(&table).starts_with(
        "CREATE TABLE people (\n  id INTEGER PRIMARY KEY NOT NULL UNIQUE,\n  email TEXT UNIQUE,\n  \
         score REAL DEFAULT 0.0,\n"
    ));
    catalog.tables.insert(table.name.clone(), table);

    // The next table and the next index count on from the ones the catalog holds.
    let tags = create("tags", vec![key("id")], &[]);
    let tags = check::new_table(&tags, &catalog).expect("a second table");
    assert_eq!(
        (tags.id, &tags.index_ids[..]),
        (TableId(1), &[IndexId(3)][..])
    );
    catalog.tables.insert(tags.name.clone(), tags);
    let index = |name: &str, column: &str| CreateIndexPlan {
        name: name.to_owned(),
        table: "people".to_owned(),
        columns: vec![(column.to_owned(), SortDirection::Desc)],
    };
    let indexed = check::with_index(&index("by_born", "born"), &catalog).expect("an index");
    assert_eq!(names(&indexed)[3], "by_born");
    assert_eq!(indexed.index_ids, [0, 1, 2, 4].map(IndexId));
    catalog.tables.insert(indexed.name.clone(), indexed);

    // What is refused, and as what.
    assert!(matches!(
        check::with_index(&index("__unique_born", "born"), &catalog),
        Err(Error::ReservedIndexName { prefix, .. }) if prefix == "__unique_"
    ));
    assert!(matches!(
        check::with_index(&index("by_age", "age"), &catalog),
        Err(Error::ColumnNotFound { table, column }) if table == "people" && column == "age"
    ));
    assert!(matches!(
        check::with_index(&index("by_doc", "doc"), &catalog),
        Err(Error::ColumnNotIndexable { column, column_type: ColumnType::Json, .. })
            if column == "doc"
    ));
    // A REAL may be indexed, and may not be a key or under a UNIQUE rule.
    check::with_index(&index("by_score", "score"), &catalog).expect("an index over a REAL");
    for unique in [&[][..], &[&["id", "ratio"][..]][..]] {
        let ratio = ast::ColumnDef {
            unique: unique.is_empty(),
            ..declared("ratio", DataType::Real)
        };
        assert!(matches!(
            check::new_table(&create("ratios", vec![key("id"), ratio], unique), &catalog),
            Err(Error::ColumnNotIndexable { column, column_type: ColumnType::Real, .. })
                if column == "ratio"
        ));
    }
    assert!(matches!(
        check::with_index(&index("by_born", "email"), &catalog),
        Err(Error::DuplicateIndex { table, index }) if table == "people" && index == "by_born"
    ));
    // Rules whose columns spell one name stay separate rules, each with its own index.
    let pairs = create(
        "pairs",
        vec![
            key("id"),
            declared("a", DataType::Integer),
            declared("b", DataType::Integer),
            declared("a_b", DataType::Integer),
        ],
        &[&["a_b"], &["a", "b"], &["a", "b"]],
    );
    let pairs = check::new_table(&pairs, &catalog).expect("three rules");
    assert_eq!(
        names(&pairs),
        [
            "__pk_id",
            "__unique_a_b",
            "__unique_a_b_2",
            "__unique_a_b_3"
        ]
    );
    assert_eq!(pairs.meta.indexes[1].columns.len(), 1);
    assert_eq!(pairs.meta.indexes[2].columns.len(), 2);
    let not_supported = |plan: &CreateTablePlan, clause: &str| {
        matches!(
            check::new_table(plan, &catalog),
            Err(Error::PlanError(text)) if text.starts_with(clause) && text.contains("not supported")
        )
    };
    let keyless = create("keyless", vec![declared("word", DataType::Text)], &[]);
    assert!(not_supported(&keyless, "a table without a PRIMARY KEY"));
}

#[test]
fn the_index_clauses_of_a_vector_column_are_stored_and_give_the_numbers_in_force() {
    use super::{Consolidation, Quantization, VectorColumn};
    use crate::store::VectorId;
    let column = |id, auto_index_at, consolidation, quantization| VectorColumn {
        id: VectorId(id),
        column: format!("v{id}"),
        partition_key: Vec::new(),
        max_partitions: None,
        search_mode: None,
        auto_index_at,
        consolidation,
        quantization,
    };
    let percent = |change, tombstone| Some(Consolidation::Percent { change, tombstone });
    let vectors = vec![
        column(0, None, None, Quantization::F32),
        column(1, Some(1000), Some(Consolidation::None), Quantization::Sq8),
        column(
            2,
            Some(u64::MAX),
            percent(Some(50), None),
            Quantization::F32,
        ),
        column(3, Some(1), percent(None, Some(100)), Quantization::Sq8),
        column(4, Some(50), percent(Some(1), Some(2)), Quantization::F32),
    ];
    // The size from which the column is grouped, the size from which a partition of it is,
    // and the two shares at which a partition is grouped again.
    let in_force: Vec<_> = vectors
        .iter()
        .map(|vector| {
            (
                vector.index_at(),
                vector.grouped_from(),
                vector.consolidation(),
            )
        })
        .collect();
    assert_eq!(
        in_force,
        [
            (100_000, 1000, Some((20, 10))),
            (1000, 10, None),
            (u64::MAX, u64::MAX / 100, Some((50, 10))),
            (1, 1, Some((20, 100))),
            (50, 1, Some((1, 2))),
        ]
    );
    // What was written is what the record gives back: a clause that was not stays unwritten.
    let table = Table {
        vectors: vectors.clone(),
        ..notes()
    };
    assert_eq!(record::decode(&record::encode(&table)).vectors, vectors);
}
